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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::dispatch::{
    Assignment, Assignments, EndpointPort, Endpoints, NamespacedPolicy, NamespacedRoute, Peer,
    Peers, Policies, Routes, ServiceEndpoints, VolumeClaim,
};
use legion_api::node::{NodeSpec, NodeState, NodeStatus, Role as NodeRole};
use legion_api::service::{PortRef, ServiceSpec, ServiceStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::volume::{Access, MigrationPhase, VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_pki::{Identity, Role};
use legion_proto::v1::dispatcher_client::DispatcherClient;
use legion_proto::v1::dispatcher_server::Dispatcher;
use legion_proto::v1::{
    AssignmentsRequest, AssignmentsUpdate, ClaimVolumeRequest, ClaimVolumeResponse, DataRequest,
    DataResponse, EndpointsRequest, EndpointsUpdate, HeartbeatRequest, HeartbeatResponse,
    PeersRequest, PeersUpdate, PoliciesRequest, PoliciesUpdate, RegisterRequest, RegisterResponse,
    RenewRequest, RenewResponse, ReportRequest, ReportResponse, RoutesRequest, RoutesUpdate,
    SessionFrame, SessionOffer, SessionsRequest,
};
use legion_store::{Command, Event, Outcome, Precondition, Store, WatchError};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::api::{ApiService, forwarded};
use crate::raft_net::{MAX_MESSAGE, connect_lazy};

/// How often agents send heartbeats unless configured otherwise.
pub const HEARTBEAT_PERIOD: Duration = Duration::from_secs(2);

/// Silent periods after which a node is down.
const MISSED_HEARTBEATS: u32 = 8;
/// Heartbeats missed before a node is suspect: its tasks leave the
/// services' endpoints, so other nodes stop sending them connections,
/// long before it is down and they are replaced.
const SUSPECT_HEARTBEATS: u32 = 3;

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
    /// Isolation and network policies, once first listed.
    policies: watch::Receiver<Option<Policies>>,
    /// Every route, once first listed.
    routes: watch::Receiver<Option<Routes>>,
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
        let (policies_tx, policies) = watch::channel(None);
        let (routes_tx, routes) = watch::channel(None);
        let service = DispatcherService {
            tls,
            period,
            seen: Arc::default(),
            endpoints,
            peers,
            policies,
            routes,
            api,
        };
        let follow = tokio::spawn(follow_cluster(
            service.api.store.clone(),
            Followed {
                endpoints: endpoints_tx,
                peers: peers_tx,
                policies: policies_tx,
                routes: routes_tx,
            },
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
                Role::Admin | Role::User => {
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

    /// Whether a route of `namespace` serves HTTPS with Secret `name`: any
    /// node's ingress proxy may need it.
    fn route_certificate(&self, namespace: &str, name: &str) -> Result<bool, Status> {
        Ok(self
            .store()
            .list(Kind::Route, Some(namespace))
            .map_err(internal)?
            .objects
            .iter()
            .filter_map(|o| legion_api::parse::<legion_api::route::RouteSpec>(&o.spec).ok())
            .any(|r| r.tls.is_some_and(|t| t.secret == name)))
    }

    /// Whether `node` may read routes' certificates: it is marked as
    /// serving HTTPS routes, or no node is.
    fn serves_routes(&self, node: &str) -> Result<bool, Status> {
        let nodes = self.store().list(Kind::Node, None).map_err(internal)?;
        let marked = |o: &Object| {
            o.metadata
                .labels
                .get(legion_api::node::LABEL_INGRESS)
                .is_some_and(|v| v == "true")
        };
        Ok(!nodes.objects.iter().any(marked)
            || nodes
                .objects
                .iter()
                .any(|o| o.metadata.name == node && marked(o)))
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
        role: NodeRole,
        status: NodeStatus,
    ) -> Result<RegisterResponse, Status> {
        let key = ObjectKey::new(Kind::Node, "", name);
        for _ in 0..ATTEMPTS {
            let existing = self.get(&key)?;
            if existing.is_none() {
                // The Node comes before its subnet's reservation, so a
                // reservation without a Node is never one being made and
                // may be reused. Its state is unknown, and it takes no
                // tasks, until the status below.
                let spec = NodeSpec {
                    role,
                    ..NodeSpec::default()
                };
                let spec = serde_json::to_value(spec).map_err(internal)?;
                let mut node = Object::new(Kind::Node, "", name, spec);
                node.status = serde_json::to_value(NodeStatus::default()).map_err(internal)?;
                match self.write(node, Precondition::MustNotExist).await? {
                    Outcome::Written(_) | Outcome::Conflict { .. } => continue,
                    other => return Err(unexpected(other)),
                }
            }
            let Some(mut node) = existing else {
                continue;
            };
            let mut status = status.clone();
            status.pod_cidr = match serde_json::from_value::<NodeStatus>(node.status.clone())
                .ok()
                .and_then(|s| s.pod_cidr)
            {
                Some(cidr) => Some(cidr),
                None => Some(self.reserve_subnet(name).await?.to_string()),
            };
            let response = RegisterResponse {
                pod_cidr: status.pod_cidr.clone().unwrap_or_default(),
            };
            let status = serde_json::to_value(&status).map_err(internal)?;
            // The role is the certificate's: a Node made before the role was
            // recorded, or edited since, gets it back.
            let mut spec: NodeSpec = legion_api::parse(&node.spec).map_err(internal)?;
            let spec = if spec.role == role {
                node.spec.clone()
            } else {
                spec.role = role;
                serde_json::to_value(spec).map_err(internal)?
            };
            if node.status == status && node.spec == spec {
                return Ok(response);
            }
            node.status = status;
            node.spec = spec;
            let precondition = Precondition::Version(node.metadata.version);
            match self.write(node, precondition).await? {
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

    /// The node's subnet: the one it reserved before, or the lowest free one,
    /// reserved now. Reservations share one object, so two nodes registering
    /// at once conflict there rather than both taking the same subnet (their
    /// Node objects are different keys).
    async fn reserve_subnet(&self, name: &str) -> Result<legion_sys::Ipv4Net, Status> {
        let key = subnets_key();
        for _ in 0..ATTEMPTS {
            let existing = self.get(&key)?;
            let mut held: BTreeMap<String, String> = match &existing {
                Some(o) => serde_json::from_value(o.spec.clone()).map_err(internal)?,
                None => BTreeMap::new(),
            };
            if let Some(cidr) = held.get(name).and_then(|c| c.parse().ok()) {
                return Ok(cidr);
            }
            // Nodes registered before reservations existed hold theirs in
            // their status only.
            let nodes = self.store().list(Kind::Node, None).map_err(internal)?;
            let in_status = nodes.objects.iter().filter_map(|n| {
                serde_json::from_value::<NodeStatus>(n.status.clone())
                    .ok()?
                    .pod_cidr
            });
            let in_status: Vec<String> = in_status.collect();
            let names: BTreeSet<&str> = nodes
                .objects
                .iter()
                .map(|n| n.metadata.name.as_str())
                .collect();
            let (subnet, released) = choose_subnet(&held, &in_status, &names)
                .ok_or_else(|| Status::resource_exhausted("the cluster pod network is full"))?;
            if let Some(gone) = released {
                tracing::info!(node = %name, subnet = %subnet, from = %gone, "reusing a deleted node's subnet");
                held.remove(&gone);
            }
            held.insert(name.to_owned(), subnet.to_string());
            let spec = serde_json::to_value(&held).map_err(internal)?;
            let precondition = existing.map_or(Precondition::MustNotExist, |o| {
                Precondition::Version(o.metadata.version)
            });
            let object = Object::new(key.kind, &key.namespace, &key.name, spec);
            match self.write(object, precondition).await? {
                Outcome::Written(_) => return Ok(subnet),
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
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
        release: bool,
    ) -> Result<VolumeClaim, Status> {
        if release {
            return self.release_local(node, namespace, name).await;
        }
        if !self
            .tasks_of(node, namespace)?
            .iter()
            .any(|a| a.spec.uses_volume(name))
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
            // A node without ZFS names no dataset, and must not hold one;
            // it refuses the volume itself.
            if spec.access() == Access::Shared
                || (spec.access() == Access::Local && dataset.is_empty())
            {
                return Ok(VolumeClaim::Claimed(Box::new(spec)));
            }
            if !status.node.is_empty() && status.node != node {
                return Ok(VolumeClaim::Elsewhere(status.node));
            }
            // Its last changes are on their way to another node.
            if let Some(m) = &status.migration
                && m.phase == MigrationPhase::Finishing
            {
                return Ok(VolumeClaim::Elsewhere(m.to.clone()));
            }
            if status.node == node && status.dataset == dataset {
                return Ok(VolumeClaim::Claimed(Box::new(spec)));
            }
            volume.status = serde_json::to_value(VolumeStatus {
                node: node.to_owned(),
                dataset: dataset.to_owned(),
                ..status
            })
            .map_err(internal)?;
            let version = volume.metadata.version;
            match self.write(volume, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(VolumeClaim::Claimed(Box::new(spec))),
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
    }

    /// Unbinds a volume used one node at a time from `node`, which has let
    /// go of it; one bound elsewhere, or to nothing, is left alone.
    async fn release_local(
        &self,
        node: &str,
        namespace: &str,
        name: &str,
    ) -> Result<VolumeClaim, Status> {
        let key = ObjectKey::new(Kind::Volume, namespace, name);
        for _ in 0..ATTEMPTS {
            let Some(mut volume) = self.get(&key)? else {
                return Ok(VolumeClaim::Missing);
            };
            let spec: VolumeSpec = serde_json::from_value(volume.spec.clone()).map_err(internal)?;
            let status: VolumeStatus =
                serde_json::from_value(volume.status.clone()).unwrap_or_default();
            if spec.access() != Access::Single || status.node != node {
                return Ok(VolumeClaim::Claimed(Box::new(spec)));
            }
            volume.status = serde_json::to_value(VolumeStatus::default()).map_err(internal)?;
            let version = volume.metadata.version;
            match self.write(volume, Precondition::Version(version)).await? {
                Outcome::Written(_) => return Ok(VolumeClaim::Claimed(Box::new(spec))),
                Outcome::Conflict { .. } => continue,
                other => return Err(unexpected(other)),
            }
        }
        Err(Status::unavailable(format!("{key} keeps changing")))
    }

    /// While this manager leads, marks nodes suspect and then down when
    /// they fall silent, and ready again when they are heard from.
    async fn monitor(self) {
        let mut closing = self.api.closing.clone();
        let grace = self.period * MISSED_HEARTBEATS;
        let suspect = self.period * SUSPECT_HEARTBEATS;
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
                let silent = now.duration_since(seen.unwrap_or(since).max(since));
                let state = match seen {
                    _ if silent >= grace => NodeState::Down,
                    Some(_) if silent >= suspect => NodeState::Suspect,
                    Some(_) => NodeState::Ready,
                    // Not heard from yet under this leader, within grace:
                    // after an election, nodes take a moment to find it.
                    None => continue,
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
                        NodeState::Suspect => {
                            tracing::warn!(node = %name, "node silent: suspect")
                        }
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
        let role = registering_role(
            crate::tls::caller(&req, self.tls)?,
            &node,
            &req.get_ref().role,
        );
        if let Some(mut leader) = self.leader(&req)? {
            let mut inner = req.into_inner();
            inner.node = node;
            inner.role = role.as_str().to_owned();
            return leader.register(forwarded(inner)).await;
        }
        let status: NodeStatus =
            serde_json::from_slice(&req.get_ref().status_json).map_err(invalid)?;
        Ok(Response::new(
            self.register_local(&node, role, status).await?,
        ))
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

    type PoliciesStream = ReceiverStream<Result<PoliciesUpdate, Status>>;

    async fn policies(
        &self,
        req: Request<PoliciesRequest>,
    ) -> Result<Response<Self::PoliciesStream>, Status> {
        self.node(&req, &req.get_ref().node)?;
        let mut rx = self.policies.clone();
        rx.mark_changed();
        Ok(Response::new(stream(&self.api.closing, rx, |set| {
            PoliciesUpdate {
                policies_json: serde_json::to_vec(set).expect("policies serialize"),
            }
        })))
    }

    type RoutesStream = ReceiverStream<Result<RoutesUpdate, Status>>;

    async fn routes(
        &self,
        req: Request<RoutesRequest>,
    ) -> Result<Response<Self::RoutesStream>, Status> {
        self.node(&req, &req.get_ref().node)?;
        let mut rx = self.routes.clone();
        rx.mark_changed();
        Ok(Response::new(stream(&self.api.closing, rx, |set| {
            RoutesUpdate {
                routes_json: serde_json::to_vec(set).expect("routes serialize"),
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
            .claim_local(&node, &r.namespace, &r.name, &r.dataset, r.release)
            .await?;
        Ok(Response::new(ClaimVolumeResponse {
            claim_json: serde_json::to_vec(&claim).map_err(internal)?,
        }))
    }

    async fn renew(&self, req: Request<RenewRequest>) -> Result<Response<RenewResponse>, Status> {
        let id = match crate::tls::caller(&req, self.tls)? {
            Some(id) if id.role == Role::Worker => id,
            Some(id) => {
                return Err(Status::permission_denied(format!(
                    "{id}: only workers renew here; managers issue their own"
                )));
            }
            None => {
                return Err(Status::failed_precondition(
                    "this manager serves without TLS",
                ));
            }
        };
        let cert_pem = self.api.renew(&req.get_ref().csr_pem, &id)?;
        tracing::info!(%id, "renewed a worker's certificate");
        Ok(Response::new(RenewResponse { cert_pem }))
    }

    async fn data(&self, req: Request<DataRequest>) -> Result<Response<DataResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        let r = req.into_inner();
        let kind: Kind = r.kind.parse().map_err(invalid)?;
        if !matches!(kind, Kind::Secret | Kind::Config) {
            return Err(Status::invalid_argument("kind is Secret or Config"));
        }
        let wanted = (kind, r.name.clone());
        let used = self
            .tasks_of(&node, &r.namespace)?
            .iter()
            .any(|a| a.spec.pod.data_refs().contains(&wanted));
        let route =
            !used && kind == Kind::Secret && self.route_certificate(&r.namespace, &r.name)?;
        if route && !self.serves_routes(&node)? {
            return Err(Status::permission_denied(format!(
                "node {node} is not marked to serve HTTPS routes: other nodes are \
                 (`legion node label {node} {}=true`)",
                legion_api::node::LABEL_INGRESS
            )));
        }
        if !used && !route {
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
        // Every node may read a route's certificate, so a route must not
        // hand them any other Secret of its namespace.
        if route && value.as_deref().is_some_and(|v| !certificate_and_key(v)) {
            return Err(Status::permission_denied(format!(
                "secret {}/{} is named by a route but is not a PEM certificate and key",
                r.namespace, r.name
            )));
        }
        Ok(Response::new(DataResponse { value }))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let node = self.node(&req, &req.get_ref().node)?;
        if let Some(mut leader) = self.leader(&req)? {
            let usage = req.into_inner().usage;
            return leader
                .heartbeat(forwarded(HeartbeatRequest { node, usage }))
                .await;
        }
        let reported = req.into_inner().usage;
        self.api.usage.record(
            &node,
            reported.into_iter().map(|u| {
                (
                    format!("{}/{}", u.namespace, u.task),
                    legion_api::dispatch::TaskUsage {
                        cpu: u.cpu_percent,
                        memory: u.memory_bytes,
                    },
                )
            }),
        );
        self.seen.lock().unwrap().insert(node, Instant::now());
        Ok(Response::new(HeartbeatResponse {
            period_ms: self.period.as_millis() as u32,
            managers: self.managers(),
        }))
    }

    type SessionsStream = ReceiverStream<Result<SessionOffer, Status>>;

    async fn sessions(
        &self,
        req: Request<SessionsRequest>,
    ) -> Result<Response<Self::SessionsStream>, Status> {
        use tokio_stream::StreamExt;
        let node = self.node(&req, &req.get_ref().node)?;
        let mut offers = self.api.sessions.follow(&node);
        // Relayed so the stream ends when the manager shuts down.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut closing = self.api.closing.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    offer = offers.next() => match offer {
                        Some(o) => if tx.send(o).await.is_err() {
                            return;
                        },
                        None => return,
                    },
                    _ = tx.closed() => return,
                    () = crate::session::shutdown(&mut closing) => break,
                }
            }
            let _ = tx
                .send(Err(Status::unavailable("manager shutting down")))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type AttachStream = crate::session::Frames;

    async fn attach(
        &self,
        req: Request<Streaming<SessionFrame>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        use legion_proto::v1::session_frame::Frame;
        use tokio_stream::StreamExt;
        // A worker attaches only to its own node's sessions.
        let node = crate::tls::caller(&req, self.tls)?
            .filter(|id| id.role == Role::Worker)
            .map(|id| id.name);
        let mut from_agent = req.into_inner();
        let id = match from_agent.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Attach(id)),
            })) => id,
            _ => {
                return Err(Status::invalid_argument(
                    "an attach starts with the session's id",
                ));
            }
        };
        Ok(Response::new(self.api.sessions.attach(
            node.as_deref(),
            &id,
            from_agent,
        )?))
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
/// Where [`follow_cluster`] publishes what it derives from the store.
struct Followed {
    endpoints: watch::Sender<Option<Endpoints>>,
    peers: watch::Sender<Option<Peers>>,
    policies: watch::Sender<Option<Policies>>,
    routes: watch::Sender<Option<Routes>>,
}

/// The kinds [`follow_cluster`] keeps, by key.
const FOLLOWED: [Kind; 6] = [
    Kind::Service,
    Kind::Task,
    Kind::Node,
    Kind::Namespace,
    Kind::NetworkPolicy,
    Kind::Route,
];

async fn follow_cluster(store: Store, out: Followed, mut closing: watch::Receiver<bool>) {
    loop {
        let mut objects: BTreeMap<Kind, BTreeMap<ObjectKey, Object>> = BTreeMap::new();
        let mut since = u64::MAX;
        let mut failed = None;
        for kind in FOLLOWED {
            match store.list(kind, None) {
                Ok(l) => {
                    // Changes after the oldest listing, so none is missed;
                    // replaying some a newer listing already holds is
                    // harmless.
                    since = since.min(l.version);
                    objects.insert(kind, l.objects.into_iter().map(|o| (o.key(), o)).collect());
                }
                Err(e) => failed = Some(e),
            }
        }
        if let Some(e) = failed {
            tracing::warn!("listing what the dispatcher follows: {e}");
            if pause(&mut closing).await {
                return;
            }
            continue;
        }
        let publish = |objects: &BTreeMap<Kind, BTreeMap<ObjectKey, Object>>, kind: Kind| {
            let of = |k: Kind| &objects[&k];
            match kind {
                // Nodes change the endpoints too: a suspect one's tasks
                // leave them.
                Kind::Node => {
                    let endpoints = out.endpoints.send_if_modified(|old| {
                        replace(
                            old,
                            Some(&endpoints(
                                of(Kind::Service),
                                of(Kind::Task),
                                of(Kind::Node),
                            )),
                        )
                    });
                    let peers = out
                        .peers
                        .send_if_modified(|old| replace(old, Some(&peers(of(Kind::Node)))));
                    endpoints || peers
                }
                Kind::Route => out
                    .routes
                    .send_if_modified(|old| replace(old, Some(&routes(of(Kind::Route))))),
                Kind::Namespace | Kind::NetworkPolicy => out.policies.send_if_modified(|old| {
                    replace(
                        old,
                        Some(&policies(of(Kind::Namespace), of(Kind::NetworkPolicy))),
                    )
                }),
                _ => out.endpoints.send_if_modified(|old| {
                    replace(
                        old,
                        Some(&endpoints(
                            of(Kind::Service),
                            of(Kind::Task),
                            of(Kind::Node),
                        )),
                    )
                }),
            }
        };
        for kind in [Kind::Service, Kind::Node, Kind::Namespace, Kind::Route] {
            publish(&objects, kind);
        }
        let mut watcher = match store.watch(since, None) {
            Ok(w) => w,
            Err(WatchError::Closed) => return,
            Err(e) => {
                tracing::debug!("watching what the dispatcher follows: {e}; listing again");
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
            let kind = o.kind;
            let Some(m) = objects.get_mut(&kind) else {
                continue;
            };
            if put {
                m.insert(o.key(), o);
            } else {
                m.remove(&o.key());
            }
            publish(&objects, kind);
        }
    }
}

fn routes(routes: &BTreeMap<ObjectKey, Object>) -> Routes {
    routes
        .values()
        .filter_map(|r| {
            Some(NamespacedRoute {
                namespace: r.metadata.namespace.clone(),
                name: r.metadata.name.clone(),
                spec: legion_api::parse(&r.spec).ok()?,
            })
        })
        .collect()
}

/// Namespaces' isolation, where it is not the default, and every policy.
fn policies(
    namespaces: &BTreeMap<ObjectKey, Object>,
    policies: &BTreeMap<ObjectKey, Object>,
) -> Policies {
    Policies {
        isolation: namespaces
            .values()
            .filter_map(|n| {
                let spec: legion_api::admit::NamespaceSpec = legion_api::parse(&n.spec).ok()?;
                (spec.isolation != Default::default())
                    .then(|| (n.metadata.name.clone(), spec.isolation))
            })
            .collect(),
        policies: policies
            .values()
            .filter_map(|p| {
                Some(NamespacedPolicy {
                    namespace: p.metadata.namespace.clone(),
                    name: p.metadata.name.clone(),
                    spec: legion_api::parse(&p.spec).ok()?,
                })
            })
            .collect(),
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

/// The ready tasks of each service: running, ready, with an address, of
/// the service as it is now (not an earlier one of the same name), and on
/// a node not suspect (or down).
fn endpoints(
    services: &BTreeMap<ObjectKey, Object>,
    tasks: &BTreeMap<ObjectKey, Object>,
    nodes: &BTreeMap<ObjectKey, Object>,
) -> Endpoints {
    let silent: BTreeSet<&str> = nodes
        .values()
        .filter(|n| {
            matches!(
                serde_json::from_value::<NodeStatus>(n.status.clone()).map(|s| s.state),
                Ok(NodeState::Suspect | NodeState::Down)
            )
        })
        .map(|n| n.metadata.name.as_str())
        .collect();
    /// Ready and running task addresses, and ready ones by ordinal.
    type Addrs = (Vec<Ipv4Addr>, Vec<Ipv4Addr>, BTreeMap<u32, Ipv4Addr>);
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
        let ordinal = spec.ordinal();
        let entry = by_service
            .entry((t.metadata.namespace.as_str(), spec.service_uid))
            .or_default();
        entry.1.push(ip);
        if spec.desired_state == DesiredState::Running
            && status.ready
            && !silent.contains(spec.node.as_str())
        {
            entry.0.push(ip);
            if let Some(n) = ordinal {
                entry.2.insert(n, ip);
            }
        }
    }
    services
        .values()
        .map(|s| {
            let status: ServiceStatus =
                serde_json::from_value(s.status.clone()).unwrap_or_default();
            let (mut ready, mut alive, ordinals) = by_service
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
                labels: s.metadata.labels.clone(),
                vip: status.virtual_ip.as_deref().and_then(|v| v.parse().ok()),
                ports,
                ready,
                alive,
                ordinals,
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
    // A global task names its node before the scheduler places it there
    // (it may lack room, or its namespace quota).
    if status.state == TaskState::Pending {
        return None;
    }
    Some(Assignment {
        key: o.key(),
        uid: o.metadata.uid.clone(),
        spec,
        status,
    })
}

/// The role of `node`, registering through `caller`: its certificate's, or,
/// in a request a manager forwards for another node (or without TLS), the
/// role the request names.
fn registering_role(caller: Option<Identity>, node: &str, named: &str) -> NodeRole {
    let named = || match named {
        "manager" => NodeRole::Manager,
        _ => NodeRole::Worker,
    };
    match caller {
        None => named(),
        Some(id) if id.role == Role::Manager && id.name != node => named(),
        Some(id) if id.role == Role::Manager => NodeRole::Manager,
        Some(_) => NodeRole::Worker,
    }
}

fn invalid(e: impl std::fmt::Display) -> Status {
    Status::invalid_argument(e.to_string())
}

/// Whether `pem` holds a certificate and a private key, as a route's
/// Secret must.
fn certificate_and_key(pem: &[u8]) -> bool {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    CertificateDer::pem_slice_iter(pem).any(|c| c.is_ok())
        && PrivateKeyDer::from_pem_slice(pem).is_ok()
}

/// A subnet for a node not in `held`: the lowest neither reserved (`held`)
/// nor in a Node's status (`in_status`); else, the network being full,
/// the lowest reserved for a node whose Node is gone (not in `nodes`),
/// with that node's name to release. A deleted node keeps its subnet until
/// then, so one that rejoins soon has it back.
fn choose_subnet(
    held: &BTreeMap<String, String>,
    in_status: &[String],
    nodes: &BTreeSet<&str>,
) -> Option<(legion_sys::Ipv4Net, Option<String>)> {
    let taken = held
        .values()
        .chain(in_status)
        .filter_map(|c| c.parse().ok())
        .collect();
    if let Some(free) = legion_net::free_node_subnet(&taken) {
        return Some((free, None));
    }
    let in_status: BTreeSet<legion_sys::Ipv4Net> =
        in_status.iter().filter_map(|c| c.parse().ok()).collect();
    held.iter()
        .filter(|(name, _)| !nodes.contains(name.as_str()))
        .filter_map(|(name, c)| Some((c.parse::<legion_sys::Ipv4Net>().ok()?, name)))
        .filter(|(c, _)| !in_status.contains(c))
        .min_by_key(|(c, _)| *c)
        .map(|(c, name)| (c, Some(name.clone())))
}

/// Node subnets by node name, in the reserved namespace.
fn subnets_key() -> ObjectKey {
    ObjectKey::new(Kind::Config, crate::join::SYSTEM_NAMESPACE, "node-subnets")
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn unexpected(outcome: Outcome) -> Status {
    Status::internal(format!("unexpected outcome {outcome:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subnet(n: u32) -> String {
        legion_net::node_subnet(n).unwrap().to_string()
    }

    #[test]
    fn a_deleted_nodes_subnet_is_reused_only_once_the_network_is_full() {
        let mut held: BTreeMap<String, String> = [("n0", 0), ("gone", 1), ("n2", 2)]
            .into_iter()
            .map(|(name, n)| (name.to_owned(), subnet(n)))
            .collect();
        let nodes: BTreeSet<&str> = ["n0", "n2", "new"].into();
        // Room left: the lowest free subnet, the deleted node's kept.
        let (free, released) = choose_subnet(&held, &[], &nodes).unwrap();
        assert_eq!((free.to_string(), released), (subnet(3), None));

        // Full: the deleted node's subnet, and its name to release.
        let count = (0..).map_while(legion_net::node_subnet).count() as u32;
        for n in 3..count {
            held.insert(format!("n{n}"), subnet(n));
        }
        let mut nodes: BTreeSet<&str> = held.keys().map(String::as_str).collect();
        nodes.remove("gone");
        let (reused, released) = choose_subnet(&held, &[], &nodes).unwrap();
        assert_eq!(
            (reused.to_string(), released.as_deref()),
            (subnet(1), Some("gone"))
        );

        // Not one a node still holds in its status, from before
        // reservations, and nothing when no node is gone.
        assert_eq!(choose_subnet(&held, &[subnet(1)], &nodes), None);
        nodes.insert("gone");
        assert_eq!(choose_subnet(&held, &[], &nodes), None);
    }
}
