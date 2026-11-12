//! `legion daemon`: on a manager node the manager, and on FreeBSD the agent
//! running tasks as jails; on a worker node only the agent. The agent follows its work through the
//! managers' dispatcher either way, its own manager first.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig, WireGuardKey};
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
    /// Where load balancer addresses are held; the default route's
    /// interface if unset.
    pub load_balancer_interface: Option<String>,
    /// Run Linux images too, through the Linux ABI.
    pub linux: bool,
    /// Behind a NAT no other node gets through: traffic to other such
    /// nodes goes through a relay.
    pub mesh_nat: bool,
    /// Relays between nodes behind NAT.
    pub mesh_relay: bool,
    /// Where the volume plugins are, if not in the default place.
    pub volume_plugins: Option<PathBuf>,
    /// Registries (`host[:port]`) reached over plain HTTP: by the managers
    /// resolving tags, and by the node pulling.
    pub insecure_registries: Vec<String>,
    /// How long the manager keeps audit events, and how many.
    pub audit: legion_manager::AuditRetention,
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
    let hostname = hostname()?;
    private_dir(&opts.data_dir)?;
    let mut pki = crate::pki::Pki::new(&opts.data_dir);
    if pki.role()? == legion_pki::Role::Worker {
        return run_worker(&opts, &pki, hostname).await;
    }
    if pki.locked() {
        let key = crate::autolock::wait(opts.listen, &opts.data_dir).await?;
        pki = pki.unlock(key)?;
    }
    let node = manager_node(opts.node_name.as_deref(), &pki, &hostname)?;
    legion_api::validate_name(&node).context("node name")?;
    let tls = pki
        .ensure(&node, &opts.data_dir)
        .context("setting up certificates")?;
    let advertise = manager_address(opts.advertise.as_deref(), opts.listen);
    let vip = match &opts.vip {
        Some(_) if !cfg!(target_os = "freebsd") => bail!("--vip needs CARP: FreeBSD only"),
        Some(v) => {
            let sys: Arc<dyn legion_sys::Sys> = Arc::new(legion_sys::Shell::new());
            Some((sys, v.clone()))
        }
        None => None,
    };
    let store_key = pki.store_key().context("the store's key")?;
    // Before the store opens: failing these leaves nothing to close.
    let listener = tokio::net::TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("listening on {}", opts.listen))?;
    let metrics_listener = bind_metrics(&opts).await?;
    let manager = legion_manager::Manager::open(legion_manager::ManagerConfig {
        tls: Some(tls),
        vip,
        store_key: Some(store_key),
        insecure_registries: opts.insecure_registries.clone(),
        audit: opts.audit,
        ..legion_manager::ManagerConfig::new(&opts.data_dir, &advertise)
    })
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("opening the store")?;
    let credentials = manager.credentials().expect("a manager serves with TLS");
    // The agent calls this manager as the node, with the same credentials.
    let control = GrpcControl::new(&node, vec![advertise.clone()], Some(credentials.clone()))
        .map_err(|e| anyhow::anyhow!(e))?;
    let run_agent = opts.agent && cfg!(target_os = "freebsd");
    // Kept with the other private keys, so that autolock seals it too.
    let wireguard = run_agent.then(|| {
        let sys = legion_sys::Shell::new();
        let genkey = || Ok(legion_sys::Sys::wg_genkey(&sys)?);
        match pki.wireguard_key(&opts.data_dir, genkey) {
            Ok(key) => WireGuardKey::Given(key),
            Err(e) => {
                tracing::warn!("{e:#}; the agent tries for one itself");
                WireGuardKey::File(opts.data_dir.join("wireguard.key"))
            }
        }
    });
    let pki = Arc::new(std::sync::Mutex::new(pki));
    let autolock = tokio::spawn(crate::autolock::follow(
        manager.store().clone(),
        pki.clone(),
    ));
    let demoted = manager.demoted();
    let pki_after = pki.clone();
    let rotation = manager.authority().map(|authority| {
        tokio::spawn(crate::ca::follow(
            manager.store().clone(),
            pki.clone(),
            node.clone(),
            opts.data_dir.clone(),
            credentials.clone(),
            authority,
        ))
    });
    let renewal = tokio::spawn(crate::pki::renew_manager(
        pki,
        node.clone(),
        opts.data_dir.clone(),
        credentials,
    ));
    tracing::info!(listen = %opts.listen, node_id = manager.id(), "serving");

    let (stop, stopped) = tokio::sync::watch::channel(false);
    let agent = if let Some(wireguard) = wireguard {
        let started = start_agent(
            &opts,
            &node,
            status(hostname, &advertise),
            control,
            wireguard,
            stopped,
        )
        .await;
        match started {
            Ok(agent) => Some(agent),
            Err(e) => {
                // Close the store as a stop does, its handles first.
                let closed = async move {
                    autolock.abort();
                    let _ = autolock.await;
                    if let Some(r) = rotation {
                        r.abort();
                        let _ = r.await;
                    }
                    renewal.abort();
                };
                if let Err(close) = manager.serve(listener, closed).await {
                    tracing::warn!("closing the store: {close}");
                }
                return Err(e);
            }
        }
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
    let demoting = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown = {
        let demoting = demoting.clone();
        let mut demoted = demoted.clone();
        async move {
            tokio::select! {
                _ = crate::shutdown_signal() => {}
                // Demoted: stopped to start again as a worker.
                _ = demoted.wait_for(Option::is_some) => {
                    demoting.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _ = stop.send(true);
            if let Some(task) = agent_task {
                let _ = task.await;
            }
            // It holds a store handle as well, and the store closes only once
            // every handle is gone: it must end before serving does.
            autolock.abort();
            let _ = autolock.await;
            if let Some(r) = rotation {
                r.abort();
                let _ = r.await;
            }
            renewal.abort();
        }
    };
    let served = manager
        .serve(listener, shutdown)
        .await
        .map_err(|e| anyhow::anyhow!(e));
    if let Some(metrics) = metrics {
        metrics.abort();
    }
    served?;
    if demoting.load(std::sync::atomic::Ordering::SeqCst)
        && let Some(d) = demoted.borrow().clone()
    {
        pki_after
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .write_demoted(&opts.data_dir, &d)
            .context("writing the worker's credentials")?;
        tracing::info!("demoted: starting again as a worker");
        return Err(start_again());
    }
    Ok(())
}

/// Starts this daemon again, in the role its data directory now says, with
/// the arguments it was started with; returns only if that fails. Its pods
/// run on and are adopted.
fn start_again() -> anyhow::Error {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let exe = match std::env::current_exe() {
            Ok(e) => e,
            Err(e) => return anyhow::anyhow!("starting again in the new role: {e}"),
        };
        let e = std::process::Command::new(exe)
            .args(std::env::args_os().skip(1))
            .exec();
        anyhow::anyhow!("starting again in the new role: {e}")
    }
    #[cfg(not(unix))]
    anyhow::anyhow!("starting again in the new role: Unix only")
}

/// A worker's half of a promotion: once its heartbeats ask it to become a
/// manager, it asks to be certified as one and writes what it got. Retries
/// until it has; returns once written.
async fn promote_when_asked(data_dir: std::path::PathBuf, advertise: String, control: GrpcControl) {
    let mut wanted = control.wanted_role();
    loop {
        if wanted.wait_for(|r| r == "manager").await.is_err() {
            return std::future::pending().await;
        }
        match promote_once(&data_dir, &advertise, &control).await {
            Ok(()) => return,
            Err(e) => tracing::warn!("asking to be promoted: {e:#}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

async fn promote_once(
    data_dir: &Path,
    advertise: &str,
    control: &GrpcControl,
) -> anyhow::Result<()> {
    let node_id = legion_manager::node_id(data_dir).map_err(|e| anyhow::anyhow!(e))?;
    let (key_pem, csr_pem) = legion_pki::request()?;
    let protocol = legion_api::protocol::Protocol::current();
    let r = control
        .promote(legion_proto::v1::PromoteRequest {
            csr_pem,
            node_id,
            advertise: advertise.to_owned(),
            protocol: protocol.speaks,
            oldest_protocol: protocol.oldest,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    crate::pki::Pki::new(data_dir).write_promoted(
        &r.cert_pem,
        &key_pem,
        &r.ca_pem,
        &r.ca_key_pem,
    )?;
    tracing::info!(node_id, %advertise, "promoted: starting again as a manager");
    Ok(())
}

/// A worker node: the agent alone, calling the cluster's managers.
async fn run_worker(opts: &Options, pki: &crate::pki::Pki, hostname: String) -> anyhow::Result<()> {
    let (node, bundle) = pki.worker()?;
    let credentials =
        legion_manager::Credentials::new(&bundle.ca_pem, &bundle.cert_pem, &bundle.key_pem)
            .map_err(|e| anyhow::anyhow!(e))
            .context("loading the worker's certificate")?;
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
    let control = GrpcControl::new(&node, managers.clone(), Some(credentials.clone()))
        .map_err(|e| anyhow::anyhow!(e))?;
    let renewal = tokio::spawn(crate::pki::renew_worker(
        crate::pki::Pki::new(&opts.data_dir),
        credentials,
        control.clone(),
    ));
    let metrics_listener = bind_metrics(opts).await?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let advertise = match &opts.advertise {
        Some(a) => a.clone(),
        None => worker_address(&managers),
    };
    let agent = start_agent(
        opts,
        &node,
        status(hostname, &advertise),
        control.clone(),
        WireGuardKey::File(opts.data_dir.join("wireguard.key")),
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
    // As a manager, it is reached where its API listens.
    let manager_advertise = manager_address(opts.advertise.as_deref(), opts.listen);
    let promoted = tokio::select! {
        _ = crate::shutdown_signal() => false,
        _ = promote_when_asked(opts.data_dir.clone(), manager_advertise, control.clone()) => true,
    };
    renewal.abort();
    let _ = stop.send(true);
    let _ = agent.task.await;
    if let Some(metrics) = metrics {
        metrics.abort();
    }
    // The managers the agent learned of, for the next start.
    crate::pki::write_managers(&opts.data_dir, &control.managers())?;
    if promoted {
        return Err(start_again());
    }
    Ok(())
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
    private_dir(data_dir)?;
    let id = legion_manager::node_id(data_dir).map_err(|e| anyhow::anyhow!(e))?;
    // A manager needs one; a worker's is ignored.
    let advertise = match advertise {
        Some(a) => with_port(a, DEFAULT_PORT),
        None => reaching(manager)
            .map(|ip| SocketAddr::new(ip, DEFAULT_PORT).to_string())
            .unwrap_or_default(),
    };
    let joined = legion_manager::join(manager, token, &node, id, &advertise)
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

/// The API's port unless --listen says otherwise.
const DEFAULT_PORT: u16 = 7470;

/// The address this host sends from toward `to`: its address on the route
/// there. Connecting a UDP socket picks it and sends nothing.
fn source_address(to: SocketAddr) -> Option<IpAddr> {
    let any: SocketAddr = if to.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = std::net::UdpSocket::bind(any).ok()?;
    socket.connect(to).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified()).then_some(ip)
}

/// The address this host reaches `manager` (`host:port`) from.
fn reaching(manager: &str) -> Option<IpAddr> {
    use std::net::ToSocketAddrs;
    let to = manager.to_socket_addrs().ok()?.next()?;
    source_address(to)
}

/// Where other managers reach this one: `--advertise`, or `--listen` when
/// it names an address, or this host's address on its default route.
fn manager_address(advertise: Option<&str>, listen: SocketAddr) -> String {
    if let Some(a) = advertise {
        return with_port(a, listen.port());
    }
    if !listen.ip().is_unspecified() {
        return listen.to_string();
    }
    // Any address outside this host's networks takes the default route.
    let outside = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 9));
    match source_address(outside) {
        Some(ip) => {
            let addr = SocketAddr::new(ip, listen.port()).to_string();
            tracing::info!(%addr, "advertising the default route's address; --advertise chooses another");
            addr
        }
        None => {
            tracing::warn!(
                "no default route: advertising 127.0.0.1, which no other node reaches; pass --advertise"
            );
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), listen.port()).to_string()
        }
    }
}

/// Where other nodes' WireGuard reaches a worker: the address it reaches
/// its first manager from. Empty, with a warning, if there is none: the
/// node then has no WireGuard endpoint.
fn worker_address(managers: &[String]) -> String {
    match managers.iter().find_map(|m| reaching(m)) {
        Some(ip) => {
            tracing::info!(%ip, "advertising the address the managers are reached from; --advertise chooses another");
            ip.to_string()
        }
        None => {
            tracing::warn!(
                "no route to a manager: no WireGuard endpoint to advertise; pass --advertise"
            );
            String::new()
        }
    }
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
    mut status: NodeStatus,
    control: GrpcControl,
    wireguard: WireGuardKey,
    stopped: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<Running> {
    status.platform.linux = opts.linux;
    let mut config = AgentConfig::new(node, opts.run_dir.join("tasks"));
    config.logs_dir = Some(opts.data_dir.join("logs"));
    if let Some(image) = &opts.infra_image {
        config.infra_image = image.clone();
    }
    config.zfs_root = opts.zfs_root.clone();
    if let Some(dir) = &opts.volume_plugins {
        config.plugin_dir = dir.clone();
    }
    config.ingress = opts.ingress_listen;
    config.ingress_tls = opts.ingress_tls_listen;
    config.load_balancer_interface = opts.load_balancer_interface.clone();
    // Its own process, which pods' lookups need through a restart.
    config.resolver = Some(legion_agent::ExternalResolver {
        command: vec![
            std::env::current_exe()
                .context("finding this binary, for the node resolver")?
                .into(),
            "node-resolver".into(),
        ],
        dir: opts.run_dir.join("resolver"),
    });
    config.wireguard = Some(WireGuardConfig {
        key: wireguard,
        port: legion_net::mesh::PORT,
        nat: opts.mesh_nat,
        relay: opts.mesh_relay,
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
    insecure_registries: &[String],
) -> anyhow::Result<legion_image::Store> {
    let client = insecure_registries.iter().fold(
        legion_image::Client::new().map_err(|e| anyhow::anyhow!(e))?,
        |c, r| c.plain_http(r),
    );
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
    // Loading reaches no registry.
    let store = image_store(Arc::new(legion_sys::Shell::new()), data_dir, zfs_root, &[])?;
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
    let mut images = image_store(sys.clone(), &opts.data_dir, zfs, &opts.insecure_registries)?;
    if opts.linux {
        let note = legion_jail::enable_linux(sys.as_ref())
            .context("readying the host for Linux images")?;
        if let Some(note) = note {
            tracing::warn!("{note}");
        }
        images = images.with_linux(true);
    }
    let exe = std::env::current_exe().context("finding this binary, for the supervisor")?;
    legion_jail::JailRuntime::new(
        sys.clone(),
        Arc::new(images),
        legion_jail::Config {
            state_dir: opts.data_dir.join("jails"),
            dataset: Some(format!("{zfs}/jails")),
            shim: vec![exe.into(), crate::JAIL_SHIM.into()],
            prune_images: true,
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

/// `advertise` as `host:port`: as given if it has a port, else with
/// `port` (a worker's `--advertise` is often a bare address, and a manager
/// it becomes is reached at one with a port).
fn with_port(advertise: &str, port: u16) -> String {
    if advertise.parse::<SocketAddr>().is_ok() {
        return advertise.to_owned();
    }
    if let Ok(ip) = advertise.parse::<std::net::IpAddr>() {
        return SocketAddr::new(ip, port).to_string();
    }
    let bare = advertise.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<std::net::Ipv6Addr>() {
        return SocketAddr::new(ip.into(), port).to_string();
    }
    match advertise.rsplit_once(':') {
        Some((_, p)) if p.parse::<u16>().is_ok() => advertise.to_owned(),
        _ => format!("{advertise}:{port}"),
    }
}

/// The host of `advertise`: `host:port`, `[v6]:port` or a bare address
/// (a v6 one has colons of its own).
fn host_of(advertise: &str) -> String {
    if let Ok(addr) = advertise.parse::<std::net::SocketAddr>() {
        return addr.ip().to_string();
    }
    if advertise.parse::<std::net::IpAddr>().is_ok() {
        return advertise.to_owned();
    }
    advertise
        .rsplit_once(':')
        .map_or(advertise, |(h, _)| h)
        .to_owned()
}

/// What the node registers with: `advertise` is `host:port` or a host.
fn status(hostname: String, advertise: &str) -> NodeStatus {
    NodeStatus {
        state: NodeState::Ready,
        hostname,
        addr: host_of(advertise),
        platform: Platform {
            os: std::env::consts::OS.to_owned(),
            arch: legion_image::oci::arch().to_owned(),
            // Set by the agent's start.
            linux: false,
            no_limits: false,
        },
        allocatable: Capacity {
            cpu: Cpu(std::thread::available_parallelism().map_or(1, |n| n.get()) as u64 * 1000),
            memory: Bytes(physical_memory().unwrap_or(0)),
        },
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: legion_api::protocol::Protocol::current(),
        image_pins: true,
        // Assigned by the manager on registration.
        pod_cidr: None,
        // Filled in by the agent.
        wireguard: None,
        // The leader's, kept by registration.
        drain: None,
    }
}

fn hostname() -> anyhow::Result<String> {
    let out = std::process::Command::new("hostname")
        .output()
        .context("running hostname")?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// A manager's node name: the one given, else the one its certificate was
/// issued to (`join --node-name` names it once), else the hostname's.
fn manager_node(
    given: Option<&str>,
    pki: &crate::pki::Pki,
    hostname: &str,
) -> anyhow::Result<String> {
    match given {
        Some(n) => Ok(n.to_owned()),
        None => match pki.manager_name()? {
            Some(n) => Ok(n),
            None => node_name(hostname),
        },
    }
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
    // The private keys sealed as the restored store says, so that the next
    // start takes its unlock key (or none), not the one before.
    let wanted = backup
        .objects
        .iter()
        .find(|o| {
            o.kind == legion_api::Kind::Secret
                && o.metadata.namespace == legion_manager::SYSTEM_NAMESPACE
                && o.metadata.name == "autolock"
        })
        .map(|o| {
            let text = o
                .spec
                .get("key")
                .and_then(|k| k.as_str())
                .unwrap_or_default();
            legion_manager::autolock::parse(text).map_err(|e| anyhow::anyhow!(e))
        })
        .transpose()?;
    if wanted.as_ref() != pki.lock() {
        let sealing = wanted.is_some();
        pki.reseal(wanted)
            .context("sealing the private keys as the backup says")?;
        if sealing {
            println!("the private keys are sealed with the backup's unlock key");
        } else {
            println!("autolock is off in the backup: the private keys are in the clear");
        }
    }
    if let Some(aside) = aside {
        println!("the old store is at {}", aside.display());
    }
    println!("start the daemon; other managers join again from empty data directories");
    Ok(())
}

/// Makes the node's data directory, its owner's alone whatever the umask
/// or however it was started: it holds keys and containers' secrets.
fn private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("making {} private", dir.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_addresses_get_a_port() {
        for (advertise, want) in [
            ("10.0.0.5", "10.0.0.5:7470"),
            ("10.0.0.5:7000", "10.0.0.5:7000"),
            ("fd00::5", "[fd00::5]:7470"),
            ("[fd00::5]", "[fd00::5]:7470"),
            ("[fd00::5]:7000", "[fd00::5]:7000"),
            ("m1.example", "m1.example:7470"),
            ("m1.example:7000", "m1.example:7000"),
        ] {
            assert_eq!(with_port(advertise, 7470), want, "{advertise}");
        }
    }

    #[test]
    fn advertised_hosts() {
        for (advertise, host) in [
            ("10.0.0.1:7470", "10.0.0.1"),
            ("10.0.0.1", "10.0.0.1"),
            ("[fd00::1]:7470", "fd00::1"),
            ("fd00::1", "fd00::1"),
            ("node.example:7470", "node.example"),
        ] {
            assert_eq!(host_of(advertise), host, "{advertise}");
        }
    }

    #[test]
    fn advertised_addresses() {
        let any: SocketAddr = "0.0.0.0:7470".parse().unwrap();
        assert_eq!(manager_address(Some("10.0.0.5:7470"), any), "10.0.0.5:7470");
        let one: SocketAddr = "10.0.0.6:7471".parse().unwrap();
        assert_eq!(manager_address(None, one), "10.0.0.6:7471");
        // Every address: one this host has, on the listening port.
        let picked: SocketAddr = manager_address(None, any).parse().unwrap();
        assert_eq!(picked.port(), 7470);
        assert!(!picked.ip().is_unspecified());
        // A manager on this host is reached from loopback.
        assert_eq!(
            reaching("127.0.0.1:7470"),
            Some(IpAddr::from(Ipv4Addr::LOCALHOST))
        );
        assert_eq!(worker_address(&["127.0.0.1:7470".into()]), "127.0.0.1");
        assert_eq!(worker_address(&[]), "");
    }

    #[test]
    fn node_names() {
        assert_eq!(node_name("Web01.example.org").unwrap(), "web01");
        assert!(node_name("under_score").is_err());
    }

    #[test]
    fn a_manager_keeps_the_name_it_joined_as() {
        let dir = tempfile::tempdir().unwrap();
        let pki = crate::pki::Pki::new(dir.path());
        assert_eq!(manager_node(None, &pki, "host.example").unwrap(), "host");
        pki.ensure("custom", dir.path()).unwrap();
        assert_eq!(manager_node(None, &pki, "host.example").unwrap(), "custom");
        assert_eq!(manager_node(Some("other"), &pki, "host").unwrap(), "other");
    }
}
