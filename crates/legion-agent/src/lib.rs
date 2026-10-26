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
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use legion_api::node::NodeStatus;
use legion_api::task::DesiredState;
use legion_sys::Sys;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub mod control;
pub mod fake;
pub mod pod;
pub mod runtime;
mod task;

pub use control::{Assignment, Assignments, Control, ControlError, StoreControl};
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
}

impl AgentConfig {
    pub fn new(node: &str) -> Self {
        AgentConfig {
            node: node.to_owned(),
            infra_image: "docker.io/freebsd/freebsd-runtime:15.1".to_owned(),
            resync: Duration::from_secs(30),
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

        tokio::select! {
            _ = register(&ctx, &status) => {}
            _ = &mut shutdown => return,
        }

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

/// Registers the node, retrying until the manager takes it.
async fn register<R, C: Control>(ctx: &Ctx<R, C>, status: &NodeStatus) {
    let mut warned = false;
    loop {
        match ctx.control.register(&ctx.node, status).await {
            Ok(()) => {
                tracing::info!(node = %ctx.node, "node registered");
                return;
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
    let orphans: BTreeSet<String> = found
        .iter()
        .filter_map(|c| c.labels.get(LABEL_TASK).cloned())
        .filter(|uid| !workers.contains_key(uid) && !cleaning.contains(uid))
        .collect();
    for uid in orphans {
        let grace = assignments.get(&uid).map_or(Duration::from_secs(10), |a| {
            a.spec.pod.stop_grace_period.as_std()
        });
        tracing::info!(uid, "removing containers of a task not assigned here");
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
