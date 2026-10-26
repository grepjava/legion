//! The agent's view of the manager: register the node, follow the tasks
//! assigned to it, report their status.
//!
//! In phase 1 the agent runs in the manager's process and [`StoreControl`]
//! reads and writes the local store. Phase 2 replaces it with the
//! dispatcher stream; the agent is written against [`Control`] only.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use legion_api::node::{NodeSpec, NodeStatus};
use legion_api::task::{TaskSpec, TaskStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, Event, Outcome, Precondition, Store, WatchError};
use legion_sys::Ipv4Net;
use openraft::error::{ClientWriteError, RaftError};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// The manager cannot take writes right now (no cluster yet, no leader);
    /// retry later.
    #[error("manager unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Other(String),
}

/// A task assigned to this node.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub key: ObjectKey,
    /// Identifies this run; a task recreated under the same name gets a new
    /// uid.
    pub uid: String,
    pub spec: TaskSpec,
    pub status: TaskStatus,
}

/// What the manager gives a node when it registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The node's pod subnet.
    pub pod_cidr: Ipv4Net,
}

/// Every task assigned to the node, by uid.
pub type Assignments = BTreeMap<String, Assignment>;

pub trait Control: Send + Sync + 'static {
    /// Creates the node's `Node` object, or updates its status, keeping the
    /// spec and labels operators set and the pod subnet assigned on first
    /// registration.
    fn register(
        &self,
        name: &str,
        status: &NodeStatus,
    ) -> impl Future<Output = Result<Registration, ControlError>> + Send;

    /// The tasks assigned to `node`, kept current. The receiver's initial
    /// value is empty and not yet marked changed; the first change is the
    /// first complete set, which may be empty too.
    fn assignments(&self, node: &str) -> watch::Receiver<Assignments>;

    /// Writes a task's status. A task that is gone, or was replaced by one
    /// with another uid, is ignored.
    fn report(
        &self,
        key: &ObjectKey,
        uid: &str,
        status: &TaskStatus,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;
}

/// [`Control`] over the local store.
#[derive(Clone)]
pub struct StoreControl {
    store: Store,
}

/// Attempts at a read-modify-write that keeps losing to other writers.
const ATTEMPTS: usize = 5;

impl StoreControl {
    pub fn new(store: Store) -> Self {
        StoreControl { store }
    }

    async fn write(
        &self,
        object: Object,
        precondition: Precondition,
    ) -> Result<Outcome, ControlError> {
        match self
            .store
            .raft
            .client_write(Command::Put {
                object,
                precondition,
            })
            .await
        {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => Err(
                ControlError::Unavailable(format!("not the leader (leader: {:?})", f.leader_id)),
            ),
            Err(e) => Err(ControlError::Unavailable(e.to_string())),
        }
    }

    /// The lowest node subnet no node holds.
    fn free_subnet(&self) -> Result<Ipv4Net, ControlError> {
        let nodes = self
            .store
            .list(Kind::Node, None)
            .map_err(|e| ControlError::Other(e.to_string()))?;
        let taken = nodes
            .objects
            .iter()
            .filter_map(|n| {
                serde_json::from_value::<NodeStatus>(n.status.clone())
                    .ok()?
                    .pod_cidr
            })
            .filter_map(|c| c.parse::<Ipv4Net>().ok())
            .collect();
        legion_net::free_node_subnet(&taken)
            .ok_or_else(|| ControlError::Other("the cluster pod network is full".into()))
    }

    fn get(&self, key: &ObjectKey) -> Result<Option<Object>, ControlError> {
        self.store
            .get(key)
            .map_err(|e| ControlError::Other(e.to_string()))
    }
}

impl Control for StoreControl {
    async fn register(
        &self,
        name: &str,
        status: &NodeStatus,
    ) -> Result<Registration, ControlError> {
        let key = ObjectKey::new(Kind::Node, "", name);
        let other = |e: serde_json::Error| ControlError::Other(e.to_string());
        for _ in 0..ATTEMPTS {
            let existing = self.get(&key)?;
            let mut status = status.clone();
            status.pod_cidr = match existing
                .as_ref()
                .and_then(|n| serde_json::from_value::<NodeStatus>(n.status.clone()).ok())
                .and_then(|s| s.pod_cidr)
            {
                Some(cidr) => Some(cidr),
                None => Some(self.free_subnet()?.to_string()),
            };
            let registration = Registration {
                pod_cidr: status
                    .pod_cidr
                    .as_deref()
                    .unwrap_or_default()
                    .parse()
                    .map_err(ControlError::Other)?,
            };
            let status = serde_json::to_value(&status).map_err(other)?;
            let (object, precondition) = match existing {
                Some(mut node) => {
                    if node.status == status {
                        return Ok(registration);
                    }
                    node.status = status;
                    let version = node.metadata.version;
                    (node, Precondition::Version(version))
                }
                None => {
                    let spec = serde_json::to_value(NodeSpec::default()).map_err(other)?;
                    let mut node = Object::new(Kind::Node, "", name, spec);
                    node.status = status;
                    (node, Precondition::MustNotExist)
                }
            };
            match self.write(object, precondition).await? {
                Outcome::Written(_) => return Ok(registration),
                Outcome::Conflict { .. } => continue,
                other => return Err(ControlError::Other(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(ControlError::Unavailable(format!("{key} keeps changing")))
    }

    fn assignments(&self, node: &str) -> watch::Receiver<Assignments> {
        let (tx, rx) = watch::channel(Assignments::new());
        tokio::spawn(follow(self.store.clone(), node.to_owned(), tx));
        rx
    }

    async fn report(
        &self,
        key: &ObjectKey,
        uid: &str,
        status: &TaskStatus,
    ) -> Result<(), ControlError> {
        let status =
            serde_json::to_value(status).map_err(|e| ControlError::Other(e.to_string()))?;
        for _ in 0..ATTEMPTS {
            let Some(mut task) = self.get(key)? else {
                return Ok(());
            };
            if task.metadata.uid != uid || task.status == status {
                return Ok(());
            }
            task.status = status.clone();
            let version = task.metadata.version;
            match self.write(task, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(()),
                Outcome::Conflict { .. } => continue,
                other => return Err(ControlError::Other(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(ControlError::Unavailable(format!("{key} keeps changing")))
    }
}

/// Keeps `tx` equal to the node's tasks: list, then watch; list again
/// whenever the watch is lost. Ends when every receiver is gone.
async fn follow(store: Store, node: String, tx: watch::Sender<Assignments>) {
    loop {
        let listing = match store.list(Kind::Task, None) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("listing tasks: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let mut current: Assignments = listing
            .objects
            .iter()
            .filter_map(|o| assignment(o, &node))
            .map(|a| (a.uid.clone(), a))
            .collect();
        // A fresh listing is always published, even if equal: the first one
        // tells the agent the set is complete.
        tx.send_replace(current.clone());
        let mut watcher = match store.watch(listing.version, Some(Kind::Task)) {
            Ok(w) => w,
            Err(WatchError::Closed) => return,
            Err(e) => {
                tracing::debug!("watching tasks: {e}; listing again");
                continue;
            }
        };
        loop {
            let event = tokio::select! {
                ev = watcher.next() => ev,
                _ = tx.closed() => return,
            };
            match event {
                Ok(Event::Put(o)) => {
                    current.retain(|_, a| a.key != o.key());
                    if let Some(a) = assignment(&o, &node) {
                        current.insert(a.uid.clone(), a);
                    }
                }
                Ok(Event::Delete(o)) => current.retain(|_, a| a.key != o.key()),
                Err(WatchError::Closed) => return,
                Ok(Event::Reset(_)) | Err(_) => break,
            }
            tx.send_if_modified(|old| replace(old, &current));
        }
    }
}

fn replace(old: &mut Assignments, new: &Assignments) -> bool {
    if old == new {
        false
    } else {
        *old = new.clone();
        true
    }
}

fn assignment(o: &Object, node: &str) -> Option<Assignment> {
    let spec: TaskSpec = match legion_api::parse(&o.spec) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(task = %o.key(), "unreadable task spec: {e}");
            return None;
        }
    };
    if spec.node != node {
        return None;
    }
    let status = if o.status.is_null() {
        TaskStatus::default()
    } else {
        serde_json::from_value(o.status.clone()).unwrap_or_default()
    };
    Some(Assignment {
        key: o.key(),
        uid: o.metadata.uid.clone(),
        spec,
        status,
    })
}
