//! The dispatcher (DESIGN.md §3.4): agents follow the tasks assigned to
//! their node, the cluster's endpoints and the nodes of the WireGuard mesh,
//! and report back, through any manager.
//!
//! Streams and reads are served from the local replica. Writes go to the
//! leader, which a follower forwards them to, naming the node it acts for.
//! Heartbeats go to the leader too: it alone tracks when each node was last
//! heard from, and marks a node down after three silent periods. A new
//! leader gives every node three periods from the moment it took over.
//!
//! A node sees only its own work: it reports on its own tasks, and reads the
//! secrets and configs and claims the volumes its tasks use.

use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::dispatch::{
    Assignment, Assignments, EndpointPort, Endpoints, Peer, Peers, ServiceEndpoints, VolumeClaim,
};
use legion_api::node::{NodeSpec, NodeState, NodeStatus};
use legion_api::service::{PortRef, ServiceSpec, ServiceStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::volume::{VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_pki::Role;
use legion_proto::v1::dispatcher_client::DispatcherClient;
use legion_proto::v1::dispatcher_server::Dispatcher;
use legion_proto::v1::{
    AssignmentsRequest, AssignmentsUpdate, ClaimVolumeRequest, ClaimVolumeResponse, DataRequest,
    DataResponse, EndpointsRequest, EndpointsUpdate, HeartbeatRequest, HeartbeatResponse,
    PeersRequest, PeersUpdate, RegisterRequest, RegisterResponse, ReportRequest, ReportResponse,
};
use legion_store::{Command, Event, Outcome, Precondition, Store, WatchError};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::api::{ApiService, forwarded};
use crate::raft_net::{MAX_MESSAGE, connect_lazy};

/// How often agents send heartbeats unless configured otherwise.
pub const HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);

/// Silent periods after which a node is down.
const MISSED_HEARTBEATS: u32 = 3;

/// Attempts at a read-modify-write that keeps losing to other writers.
const ATTEMPTS: usize = 5;

#[derive(Clone)]
pub(crate) struct DispatcherService {
    api: ApiService,
    /// Whether callers present certificates; without TLS (tests) every
    /// caller names its node.
    tls: bool,
    period: Duration,
    /// When the leader last heard from each node.
    seen: Arc<Mutex<HashMap<String, Instant>>>,
    /// Every service's endpoints, once first listed.
    endpoints: watch::Receiver<Option<Endpoints>>,
    /// Every node of the mesh, once first listed.
    peers: watch::Receiver<Option<Peers>>,
}

impl DispatcherService {
    /// Starts following the cluster's endpoints and peers and, while this
    /// manager leads, the nodes' heartbeats. Both stop on `api.closing`.
    pub(crate) fn start(
        api: ApiService,
        tls: bool,
        period: Duration,
    ) -> (DispatcherService, tokio::task::JoinHandle<()>) {
        let (endpoints_tx, endpoints) = watch::channel(None);
        let (peers_tx, peers) = watch::channel(None);
        let service = DispatcherService {
            tls,
            period,
            seen: Arc::default(),
            endpoints,
            peers,
            api,
        };
        let follow = tokio::spawn(follow_cluster(
            service.api.store.clone(),
            endpoints_tx,
            peers_tx,
            service.api.closing.clone(),
        ));
        let monitor = tokio::spawn(service.clone().monitor());
        let tasks = tokio::spawn(async move {
            let _ = follow.await;
            let _ = monitor.await;
        });
        (service, tasks)
    }

    fn store(&self) -> &Store {
        &self.api.store
    }

    /// The node a request acts for. Workers act for themselves; managers
    /// for the node they name, or themselves.
    fn node<T>(&self, req: &Request<T>, named: &str) -> Result<String, Status> {
        let caller = crate::tls::caller(req, self.tls)?;
        let node = match caller {
            None if named.is_empty() => {
                return Err(Status::invalid_argument("node is required"));
            }
            None => named.to_owned(),
            Some(id) => match id.role {
                Role::Worker if named.is_empty() || named == id.name => id.name,
                Role::Worker => {
                    return Err(Status::permission_denied(format!(
                        "{id} may not act for node {named}"
                    )));
                }
                Role::Manager if named.is_empty() => id.name,
                Role::Manager => named.to_owned(),
                Role::Admin => {
                    return Err(Status::permission_denied(format!(
                        "{id} may not call the dispatcher"
                    )));
                }
            },
        };
        legion_api::validate_name(&node).map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(node)
    }

    /// The leader's dispatcher, or `None` if this manager leads.
    fn leader<T>(&self, req: &Request<T>) -> Result<Option<DispatcherClient<Channel>>, Status> {
        Ok(self.api.leader_addr(req)?.map(|addr| {
            DispatcherClient::new(connect_lazy(&addr, self.api.peer_tls.as_ref()))
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE)
        }))
    }

    async fn write(&self, object: Object, precondition: Precondition) -> Result<Outcome, Status> {
        self.api
            .write(Command::Put {
                object,
                precondition,
            })
            .await
    }

    fn get(&self, key: &ObjectKey) -> Result<Option<Object>, Status> {
        self.store().get(key).map_err(internal)
    }

    /// The node's tasks in `namespace`.
    fn tasks_of(&self, node: &str, namespace: &str) -> Result<Vec<Assignment>, Status> {
        Ok(self
            .store()
            .list(Kind::Task, Some(namespace))
            .map_err(internal)?
            .objects
            .iter()
            .filter_map(|o| assignment(o, node))
            .collect())
    }

    fn managers(&self) -> Vec<String> {
        let m = self.store().raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .nodes()
            .map(|(_, n)| n.addr.clone())
            .collect()
    }

    async fn register_local(
        &self,
        name: &str,
        status: NodeStatus,
    ) -> Result<RegisterResponse, Status> {
        let key = ObjectKey::new(Kind::Node, "", name);
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
            let response = RegisterResponse {
                pod_cidr: status.pod_cidr.clone().unwrap_or_default(),
            };
            let status = serde_json::to_value(&status).map_err(internal)?;
            let (object, precondition) = match existing {
                Some(mut node) => {
                    if node.status == status {
                        return Ok(response);
                    }
                    node.status = status;
                    let version = node.metadata.version;
                    (node, Precondition::Version(version))
                }
                None => {
                    let spec = serde_json::to_value(NodeSpec::default()).map_err(internal)?;
                    let mut node = Object::new(Kind::Node, "", name, spec);
                    node.status = status;
                    (node, Precondition::MustNotExist)
                }
            };
            match self.write(object, precondition).await? {
                Outcome::Written(_) => {
                    // A node that registers is alive.
                    self.seen
                        .lock()
                        .unwrap()
                        .insert(name.to_owned(), Instant::now());
                    return Ok(response);
                }
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
    }

    /// The lowest node subnet no node holds.
    fn free_subnet(&self) -> Result<legion_sys::Ipv4Net, Status> {
        let nodes = self.store().list(Kind::Node, None).map_err(internal)?;
        let taken = nodes
            .objects
            .iter()
            .filter_map(|n| {
                serde_json::from_value::<NodeStatus>(n.status.clone())
                    .ok()?
                    .pod_cidr
            })
            .filter_map(|c| c.parse().ok())
            .collect();
        legion_net::free_node_subnet(&taken)
            .ok_or_else(|| Status::resource_exhausted("the cluster pod network is full"))
    }

    async fn report_local(
        &self,
        node: &str,
        key: &ObjectKey,
        uid: &str,
        status: serde_json::Value,
    ) -> Result<(), Status> {
        for _ in 0..ATTEMPTS {
            let Some(mut task) = self.get(key)? else {
                return Ok(());
            };
            if task.metadata.uid != uid || task.status == status {
                return Ok(());
            }
            if assignment(&task, node).is_none() {
                return Err(Status::permission_denied(format!(
                    "{key} is not assigned to node {node}"
                )));
            }
            task.status = status.clone();
            let version = task.metadata.version;
            match self.write(task, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(()),
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
    }

    async fn claim_local(
        &self,
        node: &str,
        namespace: &str,
        name: &str,
        dataset: &str,
    ) -> Result<VolumeClaim, Status> {
        if !self
            .tasks_of(node, namespace)?
            .iter()
            .any(|a| a.spec.pod.uses_volume(name))
        {
            return Err(Status::permission_denied(format!(
                "no task of node {node} uses volume {namespace}/{name}"
            )));
        }
        let key = ObjectKey::new(Kind::Volume, namespace, name);
        for _ in 0..ATTEMPTS {
            let Some(mut volume) = self.get(&key)? else {
                return Ok(VolumeClaim::Missing);
            };
            let spec: VolumeSpec = serde_json::from_value(volume.spec.clone()).map_err(internal)?;
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
            .map_err(internal)?;
            let version = volume.metadata.version;
            match self.write(volume, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(VolumeClaim::Claimed(spec)),
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
    }

    /// While this manager leads, marks nodes down when they fall silent and
    /// up again when they are heard from.
    async fn monitor(self) {
        let mut closing = self.api.closing.clone();
        let grace = self.period * MISSED_HEARTBEATS;
        let tick = self.period.min(Duration::from_secs(1));
        let mut leading_since: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tick) => {}
                _ = closing.wait_for(|c| *c) => return,
            }
            let m = self.store().raft.metrics().borrow().clone();
            if m.current_leader != Some(self.api.id) {
                leading_since = None;
                self.seen.lock().unwrap().clear();
                continue;
            }
            let since = *leading_since.get_or_insert_with(Instant::now);
            let nodes = match self.store().list(Kind::Node, None) {
                Ok(l) => l.objects,
                Err(e) => {
                    tracing::warn!("listing nodes: {e}");
                    continue;
                }
            };
            let now = Instant::now();
            for node in nodes {
                let Ok(mut status) = serde_json::from_value::<NodeStatus>(node.status.clone())
                else {
                    continue;
                };
                let seen = self.seen.lock().unwrap().get(&node.metadata.name).copied();
                let alive = now.duration_since(seen.unwrap_or(since).max(since)) < grace;
                let state = match (alive, seen.is_some()) {
                    (true, true) => NodeState::Ready,
                    (false, _) => NodeState::Down,
                    // Not heard from yet under this leader, within grace.
                    (true, false) => continue,
                };
                if status.state == state {
                    continue;
                }
                let name = node.metadata.name.clone();
                status.state = state;
                let mut node = node;
                let version = node.metadata.version;
                node.status = match serde_json::to_value(&status) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                match self.write(node, Precondition::Version(version)).await {
                    Ok(Outcome::Written(_)) => match state {
                        NodeState::Down => tracing::warn!(node = %name, "node down"),
                        _ => tracing::info!(node = %name, "node up"),
                    },
                    // Changed meanwhile; the next tick looks again.
                    Ok(_) => {}
                    Err(e) => tracing::debug!(node = %name, "marking node {state:?}: {e}"),
                }
            }
        }
    }
}

#[tonic::async_trait]
impl Dispatcher for DispatcherService {
    async fn register(
        &self,
        req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        if let Some(mut leader) = self.leader(&req)? {
            let mut inner = req.into_inner();
            inner.node = node;
            return leader.register(forwarded(inner)).await;
        }
        let status: NodeStatus =
            serde_json::from_slice(&req.get_ref().status_json).map_err(invalid)?;
        Ok(Response::new(self.register_local(&node, status).await?))
    }

    type AssignmentsStream = ReceiverStream<Result<AssignmentsUpdate, Status>>;

    async fn assignments(
        &self,
        req: Request<AssignmentsRequest>,
    ) -> Result<Response<Self::AssignmentsStream>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        let (tx, rx) = watch::channel(None);
        tokio::spawn(follow_assignments(
            self.store().clone(),
            node,
            tx,
            self.api.closing.clone(),
        ));
        Ok(Response::new(stream(&self.api.closing, rx, |set| {
            let list: Vec<&Assignment> = set.values().collect();
            AssignmentsUpdate {
                assignments_json: serde_json::to_vec(&list).expect("assignments serialize"),
            }
        })))
    }

    type EndpointsStream = ReceiverStream<Result<EndpointsUpdate, Status>>;

    async fn endpoints(
        &self,
        req: Request<EndpointsRequest>,
    ) -> Result<Response<Self::EndpointsStream>, Status> {
        self.node(&req, &req.get_ref().node)?;
        let mut rx = self.endpoints.clone();
        // The current set first, if there is one.
        rx.mark_changed();
        Ok(Response::new(stream(&self.api.closing, rx, |set| {
            EndpointsUpdate {
                endpoints_json: serde_json::to_vec(set).expect("endpoints serialize"),
            }
        })))
    }

    type PeersStream = ReceiverStream<Result<PeersUpdate, Status>>;

    async fn peers(
        &self,
        req: Request<PeersRequest>,
    ) -> Result<Response<Self::PeersStream>, Status> {
        self.node(&req, &req.get_ref().node)?;
        let mut rx = self.peers.clone();
        rx.mark_changed();
        Ok(Response::new(stream(&self.api.closing, rx, |set| {
            PeersUpdate {
                peers_json: serde_json::to_vec(set).expect("peers serialize"),
            }
        })))
    }

    async fn report(
        &self,
        req: Request<ReportRequest>,
    ) -> Result<Response<ReportResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        if let Some(mut leader) = self.leader(&req)? {
            let mut inner = req.into_inner();
            inner.node = node;
            return leader.report(forwarded(inner)).await;
        }
        let r = req.into_inner();
        let key = ObjectKey::new(Kind::Task, &r.namespace, &r.name);
        key.validate().map_err(invalid)?;
        let status: TaskStatus = serde_json::from_slice(&r.status_json).map_err(invalid)?;
        let status = serde_json::to_value(status).map_err(internal)?;
        self.report_local(&node, &key, &r.uid, status).await?;
        Ok(Response::new(ReportResponse {}))
    }

    async fn claim_volume(
        &self,
        req: Request<ClaimVolumeRequest>,
    ) -> Result<Response<ClaimVolumeResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        if let Some(mut leader) = self.leader(&req)? {
            let mut inner = req.into_inner();
            inner.node = node;
            return leader.claim_volume(forwarded(inner)).await;
        }
        let r = req.into_inner();
        let claim = self
            .claim_local(&node, &r.namespace, &r.name, &r.dataset)
            .await?;
        Ok(Response::new(ClaimVolumeResponse {
            claim_json: serde_json::to_vec(&claim).map_err(internal)?,
        }))
    }

    async fn data(&self, req: Request<DataRequest>) -> Result<Response<DataResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        let r = req.into_inner();
        let kind: Kind = r.kind.parse().map_err(invalid)?;
        if !matches!(kind, Kind::Secret | Kind::Config) {
            return Err(Status::invalid_argument("kind is Secret or Config"));
        }
        let wanted = (kind, r.name.clone());
        if !self
            .tasks_of(&node, &r.namespace)?
            .iter()
            .any(|a| a.spec.pod.data_refs().contains(&wanted))
        {
            return Err(Status::permission_denied(format!(
                "no task of node {node} uses {} {}/{}",
                kind.as_str().to_lowercase(),
                r.namespace,
                r.name
            )));
        }
        let value = match self.get(&ObjectKey::new(kind, &r.namespace, &r.name))? {
            Some(o) => {
                let spec: DataSpec = serde_json::from_value(o.spec).map_err(internal)?;
                Some(spec.bytes().map_err(internal)?)
            }
            None => None,
        };
        Ok(Response::new(DataResponse { value }))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        if let Some(mut leader) = self.leader(&req)? {
            return leader.heartbeat(forwarded(HeartbeatRequest { node })).await;
        }
        self.seen.lock().unwrap().insert(node, Instant::now());
        Ok(Response::new(HeartbeatResponse {
            period_ms: self.period.as_millis() as u32,
            managers: self.managers(),
        }))
    }
}

/// A response stream with `item` of each new value of `rx`, once it has
/// one, until its sender is gone, the client goes away or the manager shuts
/// down.
fn stream<S, T>(
    closing: &watch::Receiver<bool>,
    mut rx: watch::Receiver<Option<S>>,
    item: impl Fn(&S) -> T + Send + 'static,
) -> ReceiverStream<Result<T, Status>>
where
    S: Send + Sync + 'static,
    T: Send + 'static,
{
    // Every item is a complete set, so a slow client needs only the latest.
    let (tx, out) = tokio::sync::mpsc::channel(1);
    let mut closing = closing.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = rx.changed() => if changed.is_err() {
                    return;
                },
                _ = tx.closed() => return,
                _ = closing.wait_for(|c| *c) => break,
            }
            let next = rx.borrow_and_update().as_ref().map(&item);
            if let Some(next) = next
                && tx.send(Ok(next)).await.is_err()
            {
                return;
            }
        }
        let _ = tx
            .send(Err(Status::unavailable("manager shutting down")))
            .await;
    });
    ReceiverStream::new(out)
}

/// Keeps `tx` equal to the node's tasks: list, then watch; list again
/// whenever the watch is lost. Ends when every receiver is gone or the
/// manager shuts down.
async fn follow_assignments(
    store: Store,
    node: String,
    tx: watch::Sender<Option<Assignments>>,
    mut closing: watch::Receiver<bool>,
) {
    loop {
        let listing = match store.list(Kind::Task, None) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("listing tasks: {e}");
                if pause(&mut closing).await {
                    return;
                }
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
        tx.send_replace(Some(current.clone()));
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
                _ = closing.wait_for(|c| *c) => return,
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
            tx.send_if_modified(|old| replace(old, Some(&current)));
        }
    }
}

/// Keeps `endpoints` equal to the endpoints of every service and `peers` to
/// the nodes of the mesh: lists services, tasks and nodes, then follows
/// changes to them. Ends when the manager shuts down.
async fn follow_cluster(
    store: Store,
    endpoints_tx: watch::Sender<Option<Endpoints>>,
    peers_tx: watch::Sender<Option<Peers>>,
    mut closing: watch::Receiver<bool>,
) {
    loop {
        let (services, tasks, nodes) = match (
            store.list(Kind::Service, None),
            store.list(Kind::Task, None),
            store.list(Kind::Node, None),
        ) {
            (Ok(s), Ok(t), Ok(n)) => (s, t, n),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                tracing::warn!("listing services, tasks and nodes: {e}");
                if pause(&mut closing).await {
                    return;
                }
                continue;
            }
        };
        // Changes after the oldest listing, so none is missed; replaying
        // some a newer listing already holds is harmless.
        let since = services.version.min(tasks.version).min(nodes.version);
        let by_key = |objects: Vec<Object>| -> BTreeMap<ObjectKey, Object> {
            objects.into_iter().map(|o| (o.key(), o)).collect()
        };
        let (mut services, mut tasks, mut nodes) = (
            by_key(services.objects),
            by_key(tasks.objects),
            by_key(nodes.objects),
        );
        endpoints_tx.send_if_modified(|old| replace(old, Some(&endpoints(&services, &tasks))));
        peers_tx.send_if_modified(|old| replace(old, Some(&peers(&nodes))));
        let mut watcher = match store.watch(since, None) {
            Ok(w) => w,
            Err(WatchError::Closed) => return,
            Err(e) => {
                tracing::debug!("watching services, tasks and nodes: {e}; listing again");
                continue;
            }
        };
        loop {
            let event = tokio::select! {
                ev = watcher.next() => ev,
                _ = closing.wait_for(|c| *c) => return,
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
                Kind::Node => &mut nodes,
                _ => continue,
            };
            let kind = o.kind;
            if put {
                m.insert(o.key(), o);
            } else {
                m.remove(&o.key());
            }
            if kind == Kind::Node {
                peers_tx.send_if_modified(|old| replace(old, Some(&peers(&nodes))));
            } else {
                endpoints_tx
                    .send_if_modified(|old| replace(old, Some(&endpoints(&services, &tasks))));
            }
        }
    }
}

/// The nodes with a pod subnet and a WireGuard key, down ones included:
/// they keep their place in the mesh until they are removed.
fn peers(nodes: &BTreeMap<ObjectKey, Object>) -> Peers {
    nodes
        .values()
        .filter_map(|n| {
            let status: NodeStatus = serde_json::from_value(n.status.clone()).ok()?;
            let wireguard = status.wireguard?;
            Some(Peer {
                node: n.metadata.name.clone(),
                public_key: wireguard.public_key,
                endpoint: wireguard.endpoint,
                pod_cidr: status.pod_cidr?,
            })
        })
        .collect()
}

/// Waits a second; true if the manager shuts down meanwhile.
async fn pause(closing: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(1)) => false,
        _ = closing.wait_for(|c| *c) => true,
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

fn replace<T: PartialEq + Clone>(old: &mut Option<T>, new: Option<&T>) -> bool {
    if old.as_ref() == new {
        false
    } else {
        *old = new.cloned();
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

fn invalid(e: impl std::fmt::Display) -> Status {
    Status::invalid_argument(e.to_string())
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn unexpected(outcome: Outcome) -> Status {
    Status::internal(format!("unexpected outcome {outcome:?}"))
}
