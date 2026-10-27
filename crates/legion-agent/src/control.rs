//! The agent's view of the manager: register the node, follow the tasks
//! assigned to it, report their status.
//!
//! In phase 1 the agent runs in the manager's process and [`StoreControl`]
//! reads and writes the local store. Phase 2 replaces it with the
//! dispatcher stream; the agent is written against [`Control`] only.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::node::{NodeSpec, NodeStatus};
use legion_api::service::{PortRef, Protocol, ServiceSpec, ServiceStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::volume::{VolumeSpec, VolumeStatus};
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

/// The outcome of [`Control::claim_volume`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeClaim {
    /// The volume is this node's now, or was already.
    Claimed(VolumeSpec),
    Missing,
    /// Another node holds the volume's data.
    Elsewhere(String),
}

/// A service's addresses, for DNS and, later, load balancing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceEndpoints {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub vip: Option<Ipv4Addr>,
    pub ports: Vec<EndpointPort>,
    /// Addresses of the service's ready tasks, in order.
    pub ready: Vec<Ipv4Addr>,
    /// Addresses of its running tasks, ready or not, in order.
    pub alive: Vec<Ipv4Addr>,
}

/// A service port, resolved against the pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointPort {
    pub protocol: Protocol,
    pub port: u16,
    /// The tasks' port.
    pub target: u16,
    /// Where it is published, if it is.
    pub published: Option<u16>,
}

/// Every service in the cluster, by namespace and name.
pub type Endpoints = Vec<ServiceEndpoints>;

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

    /// Every service's endpoints, kept current; empty until first known.
    fn endpoints(&self) -> watch::Receiver<Endpoints>;

    /// Writes a task's status. A task that is gone, or was replaced by one
    /// with another uid, is ignored.
    fn report(
        &self,
        key: &ObjectKey,
        uid: &str,
        status: &TaskStatus,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Binds a `Volume` to `node`, recording its dataset there, unless
    /// another node holds it.
    fn claim_volume(
        &self,
        namespace: &str,
        name: &str,
        node: &str,
        dataset: &str,
    ) -> impl Future<Output = Result<VolumeClaim, ControlError>> + Send;

    /// The value of a `Secret` or `Config`; `None` if it does not exist.
    fn data(
        &self,
        namespace: &str,
        kind: Kind,
        name: &str,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, ControlError>> + Send;
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

    fn endpoints(&self) -> watch::Receiver<Endpoints> {
        let (tx, rx) = watch::channel(Endpoints::new());
        tokio::spawn(follow_endpoints(self.store.clone(), tx));
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

    async fn claim_volume(
        &self,
        namespace: &str,
        name: &str,
        node: &str,
        dataset: &str,
    ) -> Result<VolumeClaim, ControlError> {
        let key = ObjectKey::new(Kind::Volume, namespace, name);
        let other = |e: serde_json::Error| ControlError::Other(e.to_string());
        for _ in 0..ATTEMPTS {
            let Some(mut volume) = self.get(&key)? else {
                return Ok(VolumeClaim::Missing);
            };
            let spec: VolumeSpec = serde_json::from_value(volume.spec.clone()).map_err(other)?;
            let status: VolumeStatus =
                serde_json::from_value(volume.status.clone()).unwrap_or_default();
            if !status.node.is_empty() && status.node != node {
                return Ok(VolumeClaim::Elsewhere(status.node));
            }
            if status.node == node && status.dataset == dataset {
                return Ok(VolumeClaim::Claimed(spec));
            }
            volume.status = serde_json::to_value(VolumeStatus {
                node: node.to_owned(),
                dataset: dataset.to_owned(),
            })
            .map_err(other)?;
            let version = volume.metadata.version;
            match self.write(volume, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(VolumeClaim::Claimed(spec)),
                Outcome::Conflict { .. } => continue,
                other => return Err(ControlError::Other(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(ControlError::Unavailable(format!("{key} keeps changing")))
    }

    async fn data(
        &self,
        namespace: &str,
        kind: Kind,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ControlError> {
        let Some(o) = self.get(&ObjectKey::new(kind, namespace, name))? else {
            return Ok(None);
        };
        let spec: DataSpec =
            serde_json::from_value(o.spec).map_err(|e| ControlError::Other(e.to_string()))?;
        spec.bytes()
            .map(Some)
            .map_err(|e| ControlError::Other(e.to_string()))
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

/// Keeps `tx` equal to the endpoints of every service: lists services and
/// tasks, then follows changes to both. Ends when every receiver is gone.
async fn follow_endpoints(store: Store, tx: watch::Sender<Endpoints>) {
    loop {
        let (services, tasks) = match (
            store.list(Kind::Service, None),
            store.list(Kind::Task, None),
        ) {
            (Ok(s), Ok(t)) => (s, t),
            (Err(e), _) | (_, Err(e)) => {
                tracing::warn!("listing services and tasks: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        // Changes after the older listing, so none is missed; replaying
        // some the newer listing already holds is harmless.
        let since = services.version.min(tasks.version);
        let by_key = |objects: Vec<Object>| -> BTreeMap<ObjectKey, Object> {
            objects.into_iter().map(|o| (o.key(), o)).collect()
        };
        let (mut services, mut tasks) = (by_key(services.objects), by_key(tasks.objects));
        tx.send_if_modified(|old| replace(old, &endpoints(&services, &tasks)));
        let mut watcher = match store.watch(since, None) {
            Ok(w) => w,
            Err(WatchError::Closed) => return,
            Err(e) => {
                tracing::debug!("watching services and tasks: {e}; listing again");
                continue;
            }
        };
        loop {
            let event = tokio::select! {
                ev = watcher.next() => ev,
                _ = tx.closed() => return,
            };
            let (o, put) = match event {
                Ok(Event::Put(o)) => (o, true),
                Ok(Event::Delete(o)) => (o, false),
                Err(WatchError::Closed) => return,
                Ok(Event::Reset(_)) | Err(_) => break,
            };
            let m = match o.kind {
                Kind::Service => &mut services,
                Kind::Task => &mut tasks,
                _ => continue,
            };
            if put {
                m.insert(o.key(), o);
            } else {
                m.remove(&o.key());
            }
            tx.send_if_modified(|old| replace(old, &endpoints(&services, &tasks)));
        }
    }
}

/// The ready tasks of each service: running, ready, with an address, and
/// of the service as it is now (not an earlier one of the same name).
fn endpoints(
    services: &BTreeMap<ObjectKey, Object>,
    tasks: &BTreeMap<ObjectKey, Object>,
) -> Endpoints {
    /// Ready and running task addresses.
    type Addrs = (Vec<Ipv4Addr>, Vec<Ipv4Addr>);
    let mut by_service: BTreeMap<(&str, String), Addrs> = BTreeMap::new();
    for t in tasks.values() {
        let Ok(spec) = legion_api::parse::<TaskSpec>(&t.spec) else {
            continue;
        };
        let status: TaskStatus = serde_json::from_value(t.status.clone()).unwrap_or_default();
        if status.state != TaskState::Running {
            continue;
        }
        let Some(ip) = status.ip.as_deref().and_then(|s| s.parse().ok()) else {
            continue;
        };
        let entry = by_service
            .entry((t.metadata.namespace.as_str(), spec.service_uid))
            .or_default();
        entry.1.push(ip);
        if spec.desired_state == DesiredState::Running && status.ready {
            entry.0.push(ip);
        }
    }
    services
        .values()
        .map(|s| {
            let status: ServiceStatus =
                serde_json::from_value(s.status.clone()).unwrap_or_default();
            let (mut ready, mut alive) = by_service
                .remove(&(s.metadata.namespace.as_str(), s.metadata.uid.clone()))
                .unwrap_or_default();
            ready.sort();
            alive.sort();
            let ports = legion_api::parse::<ServiceSpec>(&s.spec)
                .ok()
                .and_then(|spec| {
                    let pod = spec.pod;
                    let ports = spec.endpoint?.ports;
                    Some(
                        ports
                            .iter()
                            .filter_map(|p| {
                                let target =
                                    p.target_port.clone().unwrap_or(PortRef::Number(p.port));
                                Some(EndpointPort {
                                    protocol: p.protocol,
                                    port: p.port,
                                    target: pod.resolve_port(&target)?,
                                    published: status
                                        .published_ports
                                        .iter()
                                        .find(|o| o.port == p.port && o.protocol == p.protocol)
                                        .map(|o| o.published_port),
                                })
                            })
                            .collect(),
                    )
                })
                .unwrap_or_default();
            ServiceEndpoints {
                namespace: s.metadata.namespace.clone(),
                name: s.metadata.name.clone(),
                uid: s.metadata.uid.clone(),
                vip: status.virtual_ip.as_deref().and_then(|v| v.parse().ok()),
                ports,
                ready,
                alive,
            }
        })
        .collect()
}

fn replace<T: PartialEq + Clone>(old: &mut T, new: &T) -> bool {
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
