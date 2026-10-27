//! `legion daemon`: the manager, and on FreeBSD the agent with its own
//! Podman service.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use legion_agent::{Agent, AgentConfig, StoreControl};
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
    let node = match &opts.node_name {
        Some(n) => n.clone(),
        None => node_name(&hostname)?,
    };
    legion_api::validate_name(&node).context("node name")?;
    let tls = crate::pki::Pki::new(&opts.data_dir)
        .ensure(&node, &opts.data_dir)
        .context("setting up certificates")?;
    let manager = legion_manager::Manager::open(legion_manager::ManagerConfig {
        data_dir: opts.data_dir.clone(),
        advertise: advertise.clone(),
        raft: legion_manager::default_raft_config(),
        tls: Some(tls),
    })
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("opening the store")?;
    let listener = tokio::net::TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("listening on {}", opts.listen))?;
    tracing::info!(listen = %opts.listen, node_id = manager.id(), "serving");

    let (stop, stopped) = tokio::sync::watch::channel(false);
    let agent = if opts.agent && cfg!(target_os = "freebsd") {
        Some(start_agent(&opts, &node, hostname, &advertise, &manager, stopped).await?)
    } else {
        tracing::info!("agent disabled; this daemon runs the manager only");
        None
    };

    // The agent holds store handles, so it stops before the store does.
    let (agent_task, podman) = match agent {
        Some((task, podman)) => (Some(task), Some(podman)),
        None => (None, None),
    };
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
    if let Some(podman) = podman {
        podman.shutdown().await;
    }
    served
}

async fn start_agent(
    opts: &Options,
    node: &str,
    hostname: String,
    advertise: &str,
    manager: &legion_manager::Manager,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, PodmanService)> {
    let podman = PodmanService::start(
        ServiceConfig::new(&opts.data_dir, &opts.run_dir),
        Duration::from_secs(60),
    )
    .await
    .context("starting Legion's Podman service")?;

    let mut config = AgentConfig::new(node, opts.run_dir.join("tasks"));
    if let Some(image) = &opts.infra_image {
        config.infra_image = image.clone();
    }
    config.zfs_root = opts.zfs_root.clone();
    let status = NodeStatus {
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
    };
    let agent = Agent::new(
        config,
        Arc::new(podman.client().clone()),
        Arc::new(legion_sys::Shell::new()),
        Arc::new(StoreControl::new(manager.store().clone())),
    );
    tracing::info!(%node, "starting the agent");
    let task = tokio::spawn(agent.run(status, async move {
        let _ = stopped.wait_for(|s| *s).await;
    }));
    Ok((task, podman))
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
