//! `legion daemon`: on a manager node the manager, and on FreeBSD the agent
//! with its own Podman service; on a worker node only the agent. The agent
//! follows its work through the managers' dispatcher either way, its own
//! manager first.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig};
use legion_api::node::{Capacity, NodeState, NodeStatus, Platform};
use legion_api::quantity::{Bytes, Cpu};
use legion_podman::service::{PodmanService, ServiceConfig};

pub struct Options {
    pub data_dir: PathBuf,
    pub run_dir: PathBuf,
    pub listen: SocketAddr,
    pub advertise: Option<String>,
    pub agent: bool,
    pub node_name: Option<String>,
    pub infra_image: Option<String>,
    pub zfs_root: Option<String>,
    /// Workers: managers to call instead of those recorded at join.
    pub managers: Vec<String>,
    /// Where to serve Prometheus metrics, if anywhere.
    pub metrics_listen: Option<SocketAddr>,
}

pub async fn run(opts: Options) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,openraft=warn".into()),
        )
        // Colours only on a terminal, not in log files.
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .init();
    let advertise = opts
        .advertise
        .clone()
        .unwrap_or_else(|| opts.listen.to_string());
    let hostname = hostname()?;
    let pki = crate::pki::Pki::new(&opts.data_dir);
    if pki.role()? == legion_pki::Role::Worker {
        return run_worker(&opts, &pki, hostname).await;
    }
    let node = match &opts.node_name {
        Some(n) => n.clone(),
        None => node_name(&hostname)?,
    };
    legion_api::validate_name(&node).context("node name")?;
    let tls = pki
        .ensure(&node, &opts.data_dir)
        .context("setting up certificates")?;
    // The agent calls this manager as the node.
    let control = GrpcControl::new(&node, vec![advertise.clone()], Some(tls.client()))
        .map_err(|e| anyhow::anyhow!(e))?;
    let manager = legion_manager::Manager::open(legion_manager::ManagerConfig {
        tls: Some(tls),
        ..legion_manager::ManagerConfig::new(&opts.data_dir, &advertise)
    })
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("opening the store")?;
    let listener = tokio::net::TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("listening on {}", opts.listen))?;
    tracing::info!(listen = %opts.listen, node_id = manager.id(), "serving");
    let metrics_listener = bind_metrics(&opts).await?;

    let (stop, stopped) = tokio::sync::watch::channel(false);
    let agent = if opts.agent && cfg!(target_os = "freebsd") {
        Some(start_agent(&opts, &node, status(hostname, &advertise), control, stopped).await?)
    } else {
        tracing::info!("agent disabled; this daemon runs the manager only");
        None
    };

    // The agent holds store handles, so it stops before the store does.
    let (agent_task, podman, agent_metrics) = match agent {
        Some(a) => (Some(a.task), Some(a.podman), Some(a.metrics)),
        None => (None, None, None),
    };
    let metrics = metrics_listener.map(|l| {
        tokio::spawn(crate::metrics::serve(
            l,
            crate::metrics::collect(Some(manager.metrics()), agent_metrics),
        ))
    });
    let shutdown = async move {
        crate::shutdown_signal().await;
        let _ = stop.send(true);
        if let Some(task) = agent_task {
            let _ = task.await;
        }
    };
    let served = manager
        .serve(listener, shutdown)
        .await
        .map_err(|e| anyhow::anyhow!(e));
    if let Some(metrics) = metrics {
        metrics.abort();
    }
    if let Some(podman) = podman {
        podman.shutdown().await;
    }
    served
}

/// A worker node: the agent alone, calling the cluster's managers.
async fn run_worker(opts: &Options, pki: &crate::pki::Pki, hostname: String) -> anyhow::Result<()> {
    let (node, tls) = pki.worker()?;
    if let Some(name) = &opts.node_name
        && *name != node
    {
        bail!("this worker joined as {node}; its certificate does not cover --node-name {name}");
    }
    if !(opts.agent && cfg!(target_os = "freebsd")) {
        bail!("a worker runs tasks only: it needs the agent, on FreeBSD");
    }
    let managers = if opts.managers.is_empty() {
        crate::pki::read_managers(&opts.data_dir)?
    } else {
        opts.managers.clone()
    };
    if managers.is_empty() {
        bail!("no manager to call: pass --manager or join again");
    }
    let control = GrpcControl::new(&node, managers, Some(tls)).map_err(|e| anyhow::anyhow!(e))?;
    let metrics_listener = bind_metrics(opts).await?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let advertise = opts.advertise.clone().unwrap_or_default();
    let agent = start_agent(
        opts,
        &node,
        status(hostname, &advertise),
        control.clone(),
        stopped,
    )
    .await?;
    let metrics = metrics_listener.map(|l| {
        tokio::spawn(crate::metrics::serve(
            l,
            crate::metrics::collect(None, Some(agent.metrics)),
        ))
    });
    tracing::info!(%node, "worker running");
    crate::shutdown_signal().await;
    let _ = stop.send(true);
    let _ = agent.task.await;
    if let Some(metrics) = metrics {
        metrics.abort();
    }
    agent.podman.shutdown().await;
    // The managers the agent learned of, for the next start.
    crate::pki::write_managers(&opts.data_dir, &control.managers())
}

/// `legion join`: certifies this node with the cluster and stores its
/// credentials in `data_dir`, before the daemon first starts there.
pub async fn join(
    token: &str,
    manager: &str,
    data_dir: &Path,
    advertise: Option<&str>,
    name: Option<String>,
) -> anyhow::Result<()> {
    let pki = crate::pki::Pki::new(data_dir);
    if pki.exists() || data_dir.join("store.redb").exists() {
        bail!(
            "{} already holds a node (its own CA or a store); join from an empty data directory",
            data_dir.display()
        );
    }
    let node = match name {
        Some(n) => n,
        None => node_name(&hostname()?)?,
    };
    legion_api::validate_name(&node).context("node name")?;
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("creating {}", data_dir.display()))?;
    let id = legion_manager::node_id(data_dir).map_err(|e| anyhow::anyhow!(e))?;
    let joined = legion_manager::join(manager, token, &node, id, advertise.unwrap_or(""))
        .await
        .map_err(|e| anyhow::anyhow!("joining through {manager}: {e}"))?;
    pki.write_joined(&joined)?;
    if joined.role == legion_pki::Role::Worker {
        crate::pki::write_managers(data_dir, &[manager.to_owned()])?;
    }
    println!(
        "joined as {} {node}; start the daemon with --data-dir {}",
        joined.role,
        data_dir.display()
    );
    Ok(())
}

/// Binds the metrics endpoint's listener, if asked for one.
async fn bind_metrics(opts: &Options) -> anyhow::Result<Option<tokio::net::TcpListener>> {
    let Some(addr) = opts.metrics_listen else {
        return Ok(None);
    };
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("listening for metrics on {addr}"))?;
    tracing::info!(%addr, "serving metrics at /metrics");
    Ok(Some(listener))
}

/// A running agent and what it needs.
struct Running {
    task: tokio::task::JoinHandle<()>,
    podman: PodmanService,
    metrics: legion_agent::metrics::AgentMetrics<legion_podman::Podman>,
}

async fn start_agent(
    opts: &Options,
    node: &str,
    status: NodeStatus,
    control: GrpcControl,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<Running> {
    let podman_config = ServiceConfig::new(&opts.data_dir, &opts.run_dir);
    if let Some(dir) = &podman_config.hooks_dir {
        legion_agent::hook::install(dir).context("installing Legion's jail hook")?;
    }
    let podman = PodmanService::start(podman_config, Duration::from_secs(60))
        .await
        .context("starting Legion's Podman service")?;

    let mut config = AgentConfig::new(node, opts.run_dir.join("tasks"));
    config.logs_dir = Some(opts.data_dir.join("logs"));
    if let Some(image) = &opts.infra_image {
        config.infra_image = image.clone();
    }
    config.zfs_root = opts.zfs_root.clone();
    config.wireguard = Some(WireGuardConfig {
        key_file: opts.data_dir.join("wireguard.key"),
        port: legion_net::mesh::PORT,
    });
    let agent = Agent::new(
        config,
        Arc::new(podman.client().clone()),
        Arc::new(legion_sys::Shell::new()),
        Arc::new(control),
    );
    let metrics = agent.metrics();
    tracing::info!(%node, "starting the agent");
    let task = tokio::spawn(agent.run(status, async move {
        let _ = stopped.wait_for(|s| *s).await;
    }));
    Ok(Running {
        task,
        podman,
        metrics,
    })
}

/// What the node registers with: `advertise` is `host:port` or a host.
fn status(hostname: String, advertise: &str) -> NodeStatus {
    NodeStatus {
        state: NodeState::Ready,
        hostname,
        addr: advertise
            .rsplit_once(':')
            .map_or(advertise, |(h, _)| h)
            .to_owned(),
        platform: Platform {
            os: std::env::consts::OS.to_owned(),
            arch: oci_arch().to_owned(),
        },
        allocatable: Capacity {
            cpu: Cpu(std::thread::available_parallelism().map_or(1, |n| n.get()) as u64 * 1000),
            memory: Bytes(physical_memory().unwrap_or(0)),
        },
        version: env!("CARGO_PKG_VERSION").to_owned(),
        // Assigned by the manager on registration.
        pod_cidr: None,
        // Filled in by the agent.
        wireguard: None,
    }
}

fn hostname() -> anyhow::Result<String> {
    let out = std::process::Command::new("hostname")
        .output()
        .context("running hostname")?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// The first label of the hostname, as a valid object name.
fn node_name(hostname: &str) -> anyhow::Result<String> {
    let label = hostname
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if legion_api::validate_name(&label).is_err() {
        bail!("hostname {hostname:?} does not make a valid node name; pass --node-name");
    }
    Ok(label)
}

/// OCI architecture names, which images are published under.
fn oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn physical_memory() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.physmem"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_names() {
        assert_eq!(node_name("Web01.example.org").unwrap(), "web01");
        assert!(node_name("under_score").is_err());
    }
}
