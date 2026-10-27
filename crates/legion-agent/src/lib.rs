//! The Legion agent (DESIGN.md §3.4): registers its node and runs the tasks
//! assigned to it as pods of jails through the Legion-owned Podman.
//!
//! The agent compares three views and converges them: the assignments from
//! the manager, its task workers, and the containers Podman holds with this
//! node's label. It reconciles whenever the assignments change, on every
//! container event, and periodically in case an event was missed. Pods
//! outlive the agent: when it restarts, workers adopt the running pods.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use legion_api::node::NodeStatus;
use legion_api::task::DesiredState;
use legion_sys::Sys;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

pub mod control;
pub mod fake;
mod files;
pub mod pod;
mod probe;
pub mod runtime;
mod task;
mod volumes;

pub use control::{
    Assignment, Assignments, Control, ControlError, EndpointPort, Endpoints, Registration,
    ServiceEndpoints, StoreControl, VolumeClaim,
};
pub use runtime::Runtime;

use crate::pod::{LABEL_NODE, LABEL_TASK};
use crate::task::{Ctx, Msg};

/// Modules the node needs: bridges and epairs for pod networking, pf for
/// services and policy. Jails cannot load them on demand.
pub const MODULES: [&str; 3] = ["if_bridge", "if_epair", "pf"];

#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// The node's name; tasks name it in `spec.node`.
    pub node: String,
    /// Image of the infra container: anything with `/bin/sleep`.
    pub infra_image: String,
    /// How often to check containers against assignments regardless of
    /// events.
    pub resync: Duration,
    /// Holds a tmpfs per task with its secrets and configs. The agent owns
    /// it: entries no task accounts for are removed.
    pub files_dir: PathBuf,
    /// The ZFS dataset under which volumes are created (`<pool>/legion`);
    /// without one, tasks using volumes are rejected.
    pub zfs_root: Option<String>,
}

impl AgentConfig {
    pub fn new(node: &str, files_dir: impl Into<PathBuf>) -> Self {
        AgentConfig {
            node: node.to_owned(),
            infra_image: "docker.io/freebsd/freebsd-runtime:15.1".to_owned(),
            resync: Duration::from_secs(30),
            files_dir: files_dir.into(),
            zfs_root: None,
        }
    }
}

pub struct Agent<R, C> {
    ctx: Ctx<R, C>,
    resync: Duration,
}

/// A running task worker.
struct Worker {
    tx: mpsc::Sender<Msg>,
    stopping: bool,
}

impl<R: Runtime, C: Control> Agent<R, C> {
    pub fn new(config: AgentConfig, runtime: Arc<R>, sys: Arc<dyn Sys>, control: Arc<C>) -> Self {
        Agent {
            ctx: Ctx {
                runtime,
                sys,
                control,
                node: config.node,
                infra_image: config.infra_image,
                files_dir: config.files_dir,
                zfs_root: config.zfs_root,
                net: Arc::default(),
                ips: Arc::default(),
            },
            resync: config.resync,
        }
    }

    /// Runs until `shutdown` resolves. Pods keep running afterwards.
    pub async fn run(self, status: NodeStatus, shutdown: impl Future<Output = ()> + Send) {
        tokio::pin!(shutdown);
        let ctx = self.ctx;
        let modules = task::blocking(&ctx.sys, |sys| {
            MODULES
                .iter()
                .filter_map(|m| sys.kld_load(m).err().map(|e| format!("{m}: {e}")))
                .collect::<Vec<_>>()
        })
        .await;
        for e in modules {
            tracing::warn!("loading kernel module {e}");
        }

        let registration = tokio::select! {
            r = register(&ctx, &status) => r,
            _ = &mut shutdown => return,
        };
        let net = legion_net::PodNetwork::new(registration.pod_cidr);
        tracing::info!(subnet = %net.subnet, "pod network");
        let bridge = net.clone();
        if let Err(e) = task::blocking(&ctx.sys, move |sys| bridge.ensure_bridge(sys)).await {
            tracing::error!("setting up the pod bridge: {e}; pods will fail to start");
        }
        let (gateway, subnet) = (net.gateway(), net.subnet);
        let _ = ctx.net.set(net);
        let endpoints = ctx.control.endpoints();
        let dns = dns(endpoints.clone(), gateway).await;
        let lb = tokio::spawn(load_balancing(ctx.sys.clone(), endpoints, subnet));

        let mut assignments = ctx.control.assignments(&ctx.node);
        // Nothing is reconciled before the first complete set of
        // assignments: against an empty one, every running pod would look
        // like an orphan.
        tokio::select! {
            loaded = assignments.changed() => if loaded.is_err() {
                tracing::warn!("assignments ended");
                return;
            },
            _ = &mut shutdown => return,
        }
        // Addresses held by tasks that may still run, so new pods do not
        // take them.
        {
            let mut ips = ctx.ips.lock().unwrap_or_else(|e| e.into_inner());
            for (uid, a) in assignments.borrow().iter() {
                if a.status.state.is_terminal() {
                    continue;
                }
                if let Some(ip) = a.status.ip.as_deref().and_then(|s| s.parse().ok()) {
                    ips.insert(uid.clone(), ip);
                }
            }
        }

        let (events_tx, mut events) = mpsc::channel(256);
        let forwarder = tokio::spawn(forward_events(ctx.clone(), events_tx));
        let mut workers: BTreeMap<String, Worker> = BTreeMap::new();
        let mut running: JoinSet<String> = JoinSet::new();
        // Uids whose leftover containers are being removed.
        let mut cleaning: BTreeSet<String> = BTreeSet::new();
        let mut cleanups: JoinSet<String> = JoinSet::new();
        let mut tick = tokio::time::interval(self.resync);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                changed = assignments.changed() => {
                    if changed.is_err() {
                        tracing::warn!("assignments ended");
                        break;
                    }
                    let current = assignments.borrow_and_update().clone();
                    reconcile(&ctx, &current, &mut workers, &mut running, &mut cleaning, &mut cleanups).await;
                }
                Some(event) = events.recv() => match event {
                    Forwarded::Event { uid, msg } => {
                        if let Some(w) = workers.get(&uid) {
                            let _ = w.tx.send(msg).await;
                        }
                    }
                    Forwarded::Reconnected => {
                        for w in workers.values() {
                            let _ = w.tx.send(Msg::Resync).await;
                        }
                    }
                },
                _ = tick.tick() => {
                    let current = assignments.borrow().clone();
                    reconcile(&ctx, &current, &mut workers, &mut running, &mut cleaning, &mut cleanups).await;
                    for w in workers.values() {
                        let _ = w.tx.try_send(Msg::Resync);
                    }
                }
                Some(done) = running.join_next() => {
                    match done {
                        Ok(uid) => { workers.remove(&uid); }
                        Err(e) if e.is_panic() => tracing::error!("task worker panicked: {e}"),
                        Err(_) => {}
                    }
                }
                Some(done) = cleanups.join_next() => {
                    if let Ok(uid) = done {
                        cleaning.remove(&uid);
                    }
                }
            }
        }

        // Dropping the senders ends each worker without touching its pod.
        forwarder.abort();
        if let Some(dns) = dns {
            dns.abort();
        }
        // Rules and tables stay: the pods keep running.
        lb.abort();
        workers.clear();
        let deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                next = running.join_next() => if next.is_none() { break },
                _ = &mut deadline => {
                    running.abort_all();
                    break;
                }
            }
        }
        cleanups.abort_all();
        tracing::info!("agent stopped");
    }
}

/// Runs the node resolver on the pod gateway, answering from the service
/// endpoints and relaying the rest to the host's resolvers. Pods still
/// start without it, with names outside the cluster unresolvable.
async fn dns(
    mut endpoints: watch::Receiver<Endpoints>,
    gateway: Ipv4Addr,
) -> Option<tokio::task::JoinHandle<()>> {
    use legion_net::dns::{self, ServiceAddrs, Zone};

    let addr = SocketAddr::from((gateway, 53));
    let (udp, tcp) = match (
        tokio::net::UdpSocket::bind(addr).await,
        tokio::net::TcpListener::bind(addr).await,
    ) {
        (Ok(u), Ok(t)) => (u, t),
        (Err(e), _) | (_, Err(e)) => {
            tracing::warn!(%addr, "starting the node resolver: {e}");
            return None;
        }
    };
    let upstream: Vec<SocketAddr> = std::fs::read_to_string("/etc/resolv.conf")
        .map(|t| dns::resolv_conf_servers(&t))
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.ip() != gateway)
        .collect();
    if upstream.is_empty() {
        tracing::warn!("no name servers in /etc/resolv.conf: only cluster names will resolve");
    }
    tracing::info!(%addr, ?upstream, "node resolver");
    let (tx, rx) = watch::channel(Arc::new(Zone::default()));
    Some(tokio::spawn(async move {
        let follow = async move {
            while endpoints.changed().await.is_ok() {
                let zone = Zone::new(endpoints.borrow_and_update().iter().map(|e| ServiceAddrs {
                    namespace: e.namespace.clone(),
                    name: e.name.clone(),
                    vip: e.vip,
                    tasks: e.ready.clone(),
                }));
                tx.send_replace(Arc::new(zone));
            }
        };
        tokio::select! {
            () = dns::serve(udp, tcp, rx, upstream) => {}
            () = follow => {}
        }
    }))
}

/// Programs pf from the service endpoints (see [`legion_net::lb`]): VIPs,
/// published ports, the hairpin NAT and NAT for pods going out. Failures
/// are retried a few seconds later.
async fn load_balancing(
    sys: Arc<dyn Sys>,
    mut endpoints: watch::Receiver<Endpoints>,
    subnet: legion_sys::Ipv4Net,
) {
    use legion_net::lb::{self, LoadBalancer, Port, Proto, Service};

    let checks = task::blocking(&sys, |sys| {
        if let Err(e) = sys.sysctl_set("net.inet.ip.forwarding", "1") {
            tracing::warn!("enabling IP forwarding: {e}; VIPs and NAT will not work");
        }
        match sys.pf_main_rules() {
            Ok(rules) => lb::missing_hooks(&rules),
            Err(e) => {
                tracing::warn!("reading pf's main ruleset: {e}");
                Vec::new()
            }
        }
    })
    .await;
    if !checks.is_empty() {
        tracing::warn!(
            "pf's main ruleset does not call Legion's anchors, so VIPs, published ports \
             and NAT for pods do nothing; add to /etc/pf.conf and reload: {}",
            checks.join("; ")
        );
    }

    let mut balancer = LoadBalancer::default();
    let mut retry = false;
    loop {
        if retry {
            tokio::select! {
                r = endpoints.changed() => if r.is_err() { return },
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        } else if endpoints.changed().await.is_err() {
            return;
        }
        let services: Vec<Service> = endpoints
            .borrow_and_update()
            .iter()
            .map(|e| Service {
                uid: e.uid.clone(),
                vip: e.vip,
                ports: e
                    .ports
                    .iter()
                    .map(|p| Port {
                        proto: match p.protocol {
                            legion_api::service::Protocol::Tcp => Proto::Tcp,
                            legion_api::service::Protocol::Udp => Proto::Udp,
                        },
                        port: p.port,
                        target: p.target,
                        published: p.published,
                    })
                    .collect(),
                ready: e.ready.clone(),
                alive: e.alive.clone(),
            })
            .collect();
        let (b, result) = task::blocking(&sys, move |sys| {
            let mut b = balancer;
            let egress = sys.default_interface().unwrap_or_else(|e| {
                tracing::warn!("finding the egress interface: {e}");
                None
            });
            let r = b.apply(sys, &services, subnet, egress.as_deref());
            (b, r)
        })
        .await;
        balancer = b;
        retry = match result {
            Ok(()) => false,
            Err(e) => {
                tracing::warn!("programming pf for services: {e}; retrying");
                // Start over: the anchor's state is unknown.
                balancer = LoadBalancer::default();
                true
            }
        };
    }
}

/// Registers the node, retrying until the manager takes it.
async fn register<R, C: Control>(ctx: &Ctx<R, C>, status: &NodeStatus) -> control::Registration {
    let mut warned = false;
    loop {
        match ctx.control.register(&ctx.node, status).await {
            Ok(r) => {
                tracing::info!(node = %ctx.node, "node registered");
                return r;
            }
            Err(e) => {
                if !warned {
                    tracing::info!(
                        "waiting for the manager to register the node ({e}); run `legion init` on a new cluster"
                    );
                    warned = true;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Starts workers for new tasks, stops workers for tasks that went away or
/// should stop, and removes containers no task accounts for.
async fn reconcile<R: Runtime, C: Control>(
    ctx: &Ctx<R, C>,
    assignments: &control::Assignments,
    workers: &mut BTreeMap<String, Worker>,
    running: &mut JoinSet<String>,
    cleaning: &mut BTreeSet<String>,
    cleanups: &mut JoinSet<String>,
) {
    for (uid, a) in assignments {
        let terminal = a.status.state.is_terminal();
        let wanted = a.spec.desired_state == DesiredState::Running && !terminal;
        match workers.get_mut(uid) {
            Some(w) => {
                if !wanted && !w.stopping && a.spec.desired_state == DesiredState::Shutdown {
                    w.stopping = w.tx.send(Msg::Stop).await.is_ok();
                }
            }
            // A task to shut down that never reached an end state still
            // needs its pod removed and its state reported.
            None if wanted || (a.spec.desired_state == DesiredState::Shutdown && !terminal) => {
                let (tx, rx) = mpsc::channel(64);
                let (worker_ctx, a, key) = (ctx.clone(), a.clone(), uid.clone());
                running.spawn(async move {
                    task::run(worker_ctx, a, rx).await;
                    key
                });
                workers.insert(
                    uid.clone(),
                    Worker {
                        tx,
                        stopping: false,
                    },
                );
            }
            None => {}
        }
    }
    for (uid, w) in workers.iter_mut() {
        if !assignments.contains_key(uid) && !w.stopping {
            w.stopping = w.tx.send(Msg::Stop).await.is_ok();
        }
    }

    let found = match ctx.runtime.list(&[(LABEL_NODE, &ctx.node)]).await {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("listing containers: {e}");
            return;
        }
    };
    let root = ctx.files_dir.clone();
    let with_files = tokio::task::spawn_blocking(move || files::present(&root))
        .await
        .unwrap_or_default();
    let orphans: BTreeSet<String> = found
        .iter()
        .filter_map(|c| c.labels.get(LABEL_TASK).cloned())
        .chain(with_files)
        .filter(|uid| !workers.contains_key(uid) && !cleaning.contains(uid))
        .collect();
    for uid in orphans {
        let grace = assignments.get(&uid).map_or(Duration::from_secs(10), |a| {
            a.spec.pod.stop_grace_period.as_std()
        });
        tracing::info!(uid, "removing what is left of a task not assigned here");
        cleaning.insert(uid.clone());
        let ctx = ctx.clone();
        cleanups.spawn(async move {
            task::teardown(&ctx, &uid, grace).await;
            uid
        });
    }
}

enum Forwarded {
    Event {
        uid: String,
        msg: Msg,
    },
    /// The event stream was re-established; events may have been missed.
    Reconnected,
}

/// Follows Podman's events for this node's containers, reconnecting when
/// the stream ends.
async fn forward_events<R: Runtime, C>(ctx: Ctx<R, C>, tx: mpsc::Sender<Forwarded>) {
    let mut first = true;
    loop {
        match ctx.runtime.events(&[(LABEL_NODE, &ctx.node)]).await {
            Ok(mut stream) => {
                if !first && tx.send(Forwarded::Reconnected).await.is_err() {
                    return;
                }
                while let Some(event) = stream.next().await {
                    let event = match event {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::debug!("podman events: {e}");
                            break;
                        }
                    };
                    if event.action != "died" {
                        continue;
                    }
                    let Some(uid) = event.actor.attributes.get(LABEL_TASK).cloned() else {
                        continue;
                    };
                    let msg = Msg::Died {
                        id: event.actor.id.clone(),
                        code: event.exit_code(),
                    };
                    if tx.send(Forwarded::Event { uid, msg }).await.is_err() {
                        return;
                    }
                }
            }
            Err(e) => tracing::debug!("podman events: {e}"),
        }
        first = false;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
