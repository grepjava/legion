//! `legion daemon`: on a manager node the manager, and on FreeBSD the agent
//! running tasks as jails; on a worker node only the agent. The agent follows its work through the
//! managers' dispatcher either way, its own manager first.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig};
use legion_api::node::{Capacity, NodeState, NodeStatus, Platform};
use legion_api::quantity::{Bytes, Cpu};

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
    /// Managers: the floating address.
    pub vip: Option<legion_manager::vip::VipConfig>,
    /// Where the ingress proxy listens, if anywhere: HTTP and HTTPS.
    pub ingress_listen: Option<SocketAddr>,
    pub ingress_tls_listen: Option<SocketAddr>,
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
    let mut pki = crate::pki::Pki::new(&opts.data_dir);
    if pki.role()? == legion_pki::Role::Worker {
        return run_worker(&opts, &pki, hostname).await;
    }
    if pki.locked() {
        let key = crate::autolock::wait(opts.listen, &opts.data_dir).await?;
        pki = pki.unlock(key)?;
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
    let vip = match &opts.vip {
        Some(_) if !cfg!(target_os = "freebsd") => bail!("--vip needs CARP: FreeBSD only"),
        Some(v) => {
            let sys: Arc<dyn legion_sys::Sys> = Arc::new(legion_sys::Shell::new());
            Some((sys, v.clone()))
        }
        None => None,
    };
    let store_key = pki.store_key().context("the store's key")?;
    let manager = legion_manager::Manager::open(legion_manager::ManagerConfig {
        tls: Some(tls),
        vip,
        store_key: Some(store_key),
        ..legion_manager::ManagerConfig::new(&opts.data_dir, &advertise)
    })
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("opening the store")?;
    let autolock = tokio::spawn(crate::autolock::follow(manager.store().clone(), pki));
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
    let (agent_task, agent_metrics) = match agent {
        Some(a) => (Some(a.task), Some(a.metrics)),
        None => (None, None),
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
    autolock.abort();
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
    if opts.vip.is_some() {
        bail!("--vip is the managers' address; this node is a worker");
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
    metrics: crate::metrics::Collect,
}

async fn start_agent(
    opts: &Options,
    node: &str,
    status: NodeStatus,
    control: GrpcControl,
    stopped: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<Running> {
    let mut config = AgentConfig::new(node, opts.run_dir.join("tasks"));
    config.logs_dir = Some(opts.data_dir.join("logs"));
    if let Some(image) = &opts.infra_image {
        config.infra_image = image.clone();
    }
    config.zfs_root = opts.zfs_root.clone();
    config.ingress = opts.ingress_listen;
    config.ingress_tls = opts.ingress_tls_listen;
    config.wireguard = Some(WireGuardConfig {
        key_file: opts.data_dir.join("wireguard.key"),
        port: legion_net::mesh::PORT,
    });
    let sys: Arc<dyn legion_sys::Sys> = Arc::new(legion_sys::Shell::new());
    let runtime = Arc::new(jail_runtime(opts, &sys)?);
    let (task, metrics) = spawn_agent(config, runtime, sys, control, node, status, stopped);
    Ok(Running { task, metrics })
}

fn image_store(
    sys: Arc<dyn legion_sys::Sys>,
    data_dir: &Path,
    zfs_root: &str,
) -> anyhow::Result<legion_image::Store> {
    let client = legion_image::Client::new().map_err(|e| anyhow::anyhow!(e))?;
    legion_image::Store::open(
        sys,
        &format!("{zfs_root}/images"),
        &data_dir.join("images"),
        client,
    )
    .map_err(|e| anyhow::anyhow!("opening the image store: {e}"))
}

/// `legion image load`: an OCI archive into this node's image store.
pub async fn load_image(
    file: &Path,
    name: Option<&str>,
    data_dir: &Path,
    zfs_root: &str,
) -> anyhow::Result<()> {
    let store = image_store(Arc::new(legion_sys::Shell::new()), data_dir, zfs_root)?;
    let image = store
        .load(file, name)
        .await
        .map_err(|e| anyhow::anyhow!("loading {}: {e}", file.display()))?;
    println!("loaded {} ({})", image.references.join(", "), image.id);
    Ok(())
}

#[cfg(unix)]
fn jail_runtime(
    opts: &Options,
    sys: &Arc<dyn legion_sys::Sys>,
) -> anyhow::Result<legion_jail::JailRuntime> {
    let Some(zfs) = opts.zfs_root.as_deref() else {
        bail!("the agent keeps images and containers in ZFS: pass --zfs-root");
    };
    let images = image_store(sys.clone(), &opts.data_dir, zfs)?;
    let exe = std::env::current_exe().context("finding this binary, for the supervisor")?;
    legion_jail::JailRuntime::new(
        sys.clone(),
        Arc::new(images),
        legion_jail::Config {
            state_dir: opts.data_dir.join("jails"),
            dataset: format!("{zfs}/jails"),
            shim: vec![exe.into(), crate::JAIL_SHIM.into()],
        },
    )
    .context("starting the jail runtime")
}

/// Elsewhere the agent does not run; this only keeps the code building.
#[cfg(not(unix))]
fn jail_runtime(
    _: &Options,
    _: &Arc<dyn legion_sys::Sys>,
) -> anyhow::Result<legion_agent::fake::FakeRuntime> {
    bail!("the agent runs tasks as jails, on FreeBSD")
}

fn spawn_agent<R: legion_agent::Runtime>(
    config: AgentConfig,
    runtime: Arc<R>,
    sys: Arc<dyn legion_sys::Sys>,
    control: GrpcControl,
    node: &str,
    status: NodeStatus,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) -> (tokio::task::JoinHandle<()>, crate::metrics::Collect) {
    let agent = Agent::new(config, runtime, sys, Arc::new(control));
    let metrics = crate::metrics::agent(agent.metrics());
    tracing::info!(%node, "starting the agent");
    let task = tokio::spawn(agent.run(status, async move {
        let _ = stopped.wait_for(|s| *s).await;
    }));
    (task, metrics)
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

/// `legion restore`: replaces a stopped manager's store with one holding
/// `file`'s backup, or what its own store holds, in which this manager is
/// the only member (DESIGN.md §3.7).
pub fn restore(
    file: Option<&Path>,
    data_dir: &Path,
    advertise: Option<String>,
    unlock_key_file: Option<&Path>,
) -> anyhow::Result<()> {
    let mut pki = crate::pki::Pki::new(data_dir);
    if pki.locked() {
        let Some(f) = unlock_key_file else {
            bail!("this manager is locked: pass --unlock-key-file");
        };
        let text =
            std::fs::read_to_string(f).with_context(|| format!("reading {}", f.display()))?;
        let key = legion_manager::autolock::parse(&text).map_err(|e| anyhow::anyhow!(e))?;
        pki = pki.unlock(key)?;
    }
    if !pki.exists() || pki.role()? != legion_pki::Role::Manager {
        bail!(
            "{} holds no manager of a cluster: restore where a manager ran, with its pki",
            data_dir.display()
        );
    }
    let id_file = data_dir.join("node-id");
    if !id_file.exists() {
        bail!("{} is missing", id_file.display());
    }
    let id = legion_manager::node_id(data_dir).map_err(|e| anyhow::anyhow!(e))?;
    let key = pki.store_key()?;
    let backup = match file {
        Some(file) => {
            let json =
                std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
            serde_json::from_slice(&json)
                .with_context(|| format!("{} is not a Legion backup", file.display()))?
        }
        None => legion_store::Backup::of_store(data_dir, Some(&key))?,
    };
    backup.check()?;
    let Some(addr) = advertise.or_else(|| backup.managers.get(&id).cloned()) else {
        bail!("the backup does not record this manager's address: pass --advertise");
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_string();
    let aside = legion_store::set_aside(data_dir, &suffix)?;
    legion_store::restore(data_dir, id, &addr, &backup, Some(&key))?;
    println!(
        "restored {} objects at version {}; this manager ({addr}) is the cluster's only member",
        backup.objects.len(),
        backup.version
    );
    if let Some(aside) = aside {
        println!("the old store is at {}", aside.display());
    }
    println!("start the daemon; other managers join again from empty data directories");
    Ok(())
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
