//! Controllers (DESIGN.md §3.3). They run on the leader only: each pass
//! reads Services, Tasks, Nodes and Volumes from the local store, plans the writes
//! that converge them ([`plan`]) and makes them with version
//! preconditions, so a write based on stale state fails and the next pass
//! sees the newer state.
//!
//! A pass runs when the store changes (debounced), when a planned wait ends
//! (restart backoff), and every few seconds regardless.

pub mod plan;
#[cfg(test)]
mod plan_tests;

use std::time::Duration;

use legion_api::{Kind, Object};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use tokio::sync::watch;
use tokio::time::Instant;

use plan::{Action, Homes, Memory, Node, Service, Task};

/// Changes arriving within this window are handled in one pass.
const DEBOUNCE: Duration = Duration::from_millis(50);
const PERIOD: Duration = Duration::from_secs(5);

/// Runs the controllers whenever this manager leads, until `closing` turns
/// true.
pub async fn run(store: Store, id: NodeId, mut closing: watch::Receiver<bool>) {
    let mut metrics = store.raft.metrics();
    loop {
        // Wait for leadership.
        loop {
            if *closing.borrow() {
                return;
            }
            if metrics.borrow().current_leader == Some(id) {
                break;
            }
            tokio::select! {
                changed = metrics.changed() => if changed.is_err() { return },
                _ = closing.changed() => {}
            }
        }
        tracing::info!("leading: controllers started");
        tokio::select! {
            _ = lead(&store) => {}
            _ = lost(&mut metrics, id) => tracing::info!("no longer leading: controllers stopped"),
            _ = closing.wait_for(|c| *c) => return,
        }
    }
}

async fn lost(
    metrics: &mut watch::Receiver<openraft::RaftMetrics<NodeId, openraft::BasicNode>>,
    id: NodeId,
) {
    while metrics.borrow().current_leader == Some(id) {
        if metrics.changed().await.is_err() {
            return;
        }
    }
}

async fn lead(store: &Store) {
    let mut memory = Memory::default();
    loop {
        let from = match store.applied_version() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("reading the store: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let wake = pass(store, &mut memory).await;
        // Sleep until something changes, the planned wake-up, or the period.
        let mut watcher = match store.watch(from, None) {
            Ok(w) => w,
            Err(_) => continue,
        };
        let deadline = wake.map_or(Instant::now() + PERIOD, |w| w.min(Instant::now() + PERIOD));
        tokio::select! {
            ev = watcher.next() => if ev.is_ok() {
                tokio::time::sleep(DEBOUNCE).await;
            },
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

/// One reconcile pass; returns when the next one is due at the latest.
async fn pass(store: &Store, memory: &mut Memory) -> Option<Instant> {
    let (services, tasks, nodes, homes) = match read(store) {
        Ok(state) => state,
        Err(e) => {
            tracing::warn!("reading the store: {e}");
            return Some(Instant::now() + Duration::from_secs(1));
        }
    };
    let plan = plan::plan(&services, &tasks, &nodes, &homes, memory, Instant::now());
    for action in plan.actions {
        if let Err(e) = apply(store, action).await {
            tracing::debug!("controller write: {e}");
        }
    }
    plan.wake
}

type State = (Vec<Service>, Vec<Task>, Vec<Node>, Homes);

fn read(store: &Store) -> Result<State, legion_store::Error> {
    let services = store
        .list(Kind::Service, None)?
        .objects
        .into_iter()
        .filter_map(|o| {
            let spec = parsed(&o)?;
            Some(Service { object: o, spec })
        })
        .collect();
    let tasks = store
        .list(Kind::Task, None)?
        .objects
        .into_iter()
        .filter_map(|o| {
            let spec = parsed(&o)?;
            let status = serde_json::from_value(o.status.clone()).unwrap_or_default();
            Some(Task {
                object: o,
                spec,
                status,
            })
        })
        .collect();
    let nodes = store
        .list(Kind::Node, None)?
        .objects
        .into_iter()
        .filter_map(|o| {
            let spec = parsed(&o)?;
            let status = serde_json::from_value(o.status.clone()).unwrap_or_default();
            Some(Node {
                object: o,
                spec,
                status,
            })
        })
        .collect();
    let homes = store
        .list(Kind::Volume, None)?
        .objects
        .into_iter()
        .filter_map(|o| {
            let status: legion_api::volume::VolumeStatus = serde_json::from_value(o.status).ok()?;
            (!status.node.is_empty())
                .then_some(((o.metadata.namespace, o.metadata.name), status.node))
        })
        .collect();
    Ok((services, tasks, nodes, homes))
}

fn parsed<T: legion_api::Spec>(o: &Object) -> Option<T> {
    match legion_api::parse(&o.spec) {
        Ok(spec) => Some(spec),
        Err(e) => {
            tracing::warn!(object = %o.key(), "unreadable spec: {e}");
            None
        }
    }
}

async fn apply(store: &Store, action: Action) -> Result<(), String> {
    let command = match action {
        Action::Create(object) => Command::Put {
            object,
            precondition: Precondition::MustNotExist,
        },
        Action::Update(object, version) => Command::Put {
            object,
            precondition: Precondition::Version(version),
        },
        Action::Delete(key, version) => Command::Delete {
            key,
            precondition: Precondition::Version(version),
        },
    };
    let out = store
        .raft
        .client_write(command)
        .await
        .map_err(|e| e.to_string())?;
    match out.data {
        Outcome::Written(_) | Outcome::Deleted { .. } => Ok(()),
        other => Err(format!("{other:?}")),
    }
}
