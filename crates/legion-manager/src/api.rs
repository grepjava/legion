//! The public API: `Objects` and `Cluster` gRPC services over the store.
//!
//! Any manager accepts any request. Writes and consistent reads are served
//! by the leader, so a follower forwards them once (marking the request so it
//! is never forwarded twice). Watches are served from the local replica.

use legion_api::protocol::Protocol;
use std::collections::BTreeMap;

use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_pki::Role;
use legion_proto::v1::cluster_server::Cluster;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::objects_server::Objects;
use legion_proto::v1::{
    self as pb, ApplyRequest, ApplyResponse, ApplyResult, DeleteRequest, DeleteResponse,
    InitRequest, ListRequest, ListResponse, ObjectRef, StatusRequest, StatusResponse, WatchRequest,
    watch_event,
};
use legion_store::{Command, Event, NodeId, Outcome, Precondition, Store, WatchError};
use openraft::BasicNode;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::authz::{Caller, Verb};
use crate::join::SYSTEM_NAMESPACE;
use crate::raft_net::{MAX_MESSAGE, connect_lazy};

const FORWARDED: &str = "legion-forwarded";

/// Attempts at a server-side apply that keeps losing a race with other writers.
const APPLY_ATTEMPTS: usize = 5;

#[derive(Clone)]
pub struct ApiService {
    pub(crate) id: NodeId,
    /// `host:port` peers and forwarded requests reach this manager at.
    pub(crate) advertise: String,
    pub(crate) store: Store,
    /// Becomes true when the manager shuts down; open watches then end so
    /// the server's graceful shutdown can finish.
    pub(crate) closing: tokio::sync::watch::Receiver<bool>,
    /// For forwarding to the leader.
    pub(crate) peer_tls: Option<std::sync::Arc<crate::Credentials>>,
    /// Certifies joining nodes; managers without the CA key cannot.
    pub(crate) issuer: Option<crate::join::Issuer>,
    /// Held from quota or route host check to write, so two applies cannot
    /// both pass.
    pub(crate) quota_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// The agents connected to this manager, for sessions with their tasks.
    pub(crate) sessions: std::sync::Arc<crate::session::Hub>,
    /// What tasks use, as heartbeats report it.
    pub(crate) usage: std::sync::Arc<crate::usage::Usage>,
    /// What this manager speaks and works with, with agents.
    pub(crate) protocol: legion_api::protocol::Protocol,
    /// What it speaks and works with among managers: the floor counts.
    pub(crate) speaking: crate::upgrade::Speaking,
}

pub(crate) enum Route {
    Local,
    /// A client for the leader, and its address as routing saw it.
    Leader(ObjectsClient<Channel>, String),
}

/// The reserved namespace holds Legion's own records; no caller sees it.
pub(crate) fn check_namespace(namespace: &str) -> Result<(), Status> {
    if namespace == SYSTEM_NAMESPACE {
        Err(Status::permission_denied(format!(
            "namespace {SYSTEM_NAMESPACE} is reserved for Legion"
        )))
    } else {
        Ok(())
    }
}

impl ApiService {
    /// The leader's address, or `None` if this manager leads.
    pub(crate) fn leader_addr<T>(&self, req: &Request<T>) -> Result<Option<String>, Status> {
        match self.route(req)? {
            Route::Local => Ok(None),
            // The address of the leader routing found, not read again: an
            // election may have cleared it since.
            Route::Leader(_, addr) => Ok(Some(addr)),
        }
    }

    fn issuer(&self) -> Result<&crate::join::Issuer, Status> {
        self.issuer
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("this manager cannot issue certificates"))
    }

    /// A certificate for `id` again, for a new key: any manager with the CA
    /// signs it, leader or not, as nothing is written.
    pub(crate) fn renew(&self, csr_pem: &str, id: &legion_pki::Identity) -> Result<String, Status> {
        self.issuer()?
            .ca
            .sign(csr_pem, id, legion_pki::CERT_LIFETIME)
            .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))
    }

    pub(crate) async fn join(
        &self,
        req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        // Refused here as by the leader, before anything is signed; answered
        // with what both work with.
        let mine = self.protocol;
        let theirs = Protocol::said(req.get_ref().protocol, req.get_ref().oldest_protocol);
        mine.works_with(theirs, &format!("node {}", req.get_ref().name))
            .map_err(Status::unimplemented)?;
        let mut answer = if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::join_client::JoinClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            c.join(forwarded(req.into_inner())).await?.into_inner()
        } else {
            // The tokens and removals it checks are the latest: a worker's
            // join writes nothing that would fail on a deposed leader.
            self.consistent().await?;
            self.issuer()?.join(self, req.into_inner()).await?
        };
        let leader = Protocol::said(answer.protocol, answer.oldest_protocol);
        answer.protocol = leader.speaks.min(mine.speaks);
        answer.oldest_protocol = leader.oldest.max(mine.oldest);
        Ok(Response::new(answer))
    }

    /// Where a leader-only request should run.
    pub(crate) fn route<T>(&self, req: &Request<T>) -> Result<Route, Status> {
        let metrics = self.store.raft.metrics().borrow().clone();
        match metrics.current_leader {
            Some(leader) if leader == self.id => Ok(Route::Local),
            Some(leader) => {
                if req.metadata().contains_key(FORWARDED) {
                    return Err(Status::unavailable("leadership is changing; retry"));
                }
                let addr = metrics
                    .membership_config
                    .membership()
                    .get_node(&leader)
                    .map(|n| n.addr.clone())
                    .ok_or_else(|| {
                        Status::unavailable(format!("leader {leader} has no address"))
                    })?;
                Ok(Route::Leader(
                    ObjectsClient::new(connect_lazy(&addr, self.peer_tls.as_ref()))
                        .max_decoding_message_size(MAX_MESSAGE)
                        .max_encoding_message_size(MAX_MESSAGE),
                    addr,
                ))
            }
            None if metrics
                .membership_config
                .membership()
                .voter_ids()
                .next()
                .is_none() =>
            {
                Err(Status::failed_precondition(
                    "this manager is not part of a cluster; run `legion init`",
                ))
            }
            None => Err(Status::unavailable("no leader elected; retry")),
        }
    }

    /// Waits until this manager's store holds all the leader has committed
    /// (a linearizable read from then on). The leader checks it still
    /// leads; a follower, whose replica may lag or be cut off, asks the
    /// leader how far that is and waits to have applied as much.
    pub(crate) async fn consistent(&self) -> Result<(), Status> {
        tokio::time::timeout(crate::COMMIT_TIMEOUT, self.linearize())
            .await
            .map_err(|_| crate::no_quorum())?
    }

    async fn linearize(&self) -> Result<(), Status> {
        let Some(leader) = self.leader_addr(&Request::new(()))? else {
            return self
                .store
                .raft
                .ensure_linearizable()
                .await
                .map(|_| ())
                .map_err(|e| Status::unavailable(e.to_string()));
        };
        let index = crate::raft_net::read_index(&leader, self.peer_tls.as_ref()).await?;
        self.store
            .raft
            .wait(None)
            .applied_index_at_least(index, "the leader's read index")
            .await
            .map(drop)
            .map_err(|e| Status::unavailable(e.to_string()))
    }

    pub(crate) async fn write(&self, cmd: Command) -> Result<Outcome, Status> {
        let written =
            tokio::time::timeout(crate::COMMIT_TIMEOUT, self.store.raft.client_write(cmd))
                .await
                .map_err(|_| crate::no_quorum())?;
        match written {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id,
                ..
            }))) => Err(Status::unavailable(format!(
                "leadership moved to {leader_id:?}; retry"
            ))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn apply_local(&self, req: ApplyRequest) -> Result<ApplyResponse, Status> {
        let wire = req
            .object
            .ok_or_else(|| Status::invalid_argument("object is required"))?;
        let mut desired = Object::try_from(wire).map_err(invalid)?;
        default_namespace(desired.kind, &mut desired.metadata.namespace);
        check_namespace(&desired.metadata.namespace)?;
        if desired.kind == Kind::Namespace {
            check_namespace(&desired.metadata.name)?;
        }
        legion_api::admit(&mut desired).map_err(invalid)?;
        if desired.kind == Kind::ImagePolicy {
            check_policy_keys(&desired)?;
        }
        let key = desired.key();
        // Applies that check other objects than their own (quotas, a
        // route's host) take turns, so two cannot pass on the same state;
        // so do those of what they are checked against (a Namespace's
        // quota and security) and of Jobs, checked against those.
        let _counting = match crate::quota::counts(desired.kind)
            || matches!(
                desired.kind,
                Kind::Route | Kind::Namespace | Kind::Job | Kind::CronJob | Kind::Volume
            ) {
            true => Some(self.quota_lock.lock().await),
            false => None,
        };

        for _ in 0..APPLY_ATTEMPTS {
            let current = self.store.get(&key).map_err(internal)?;
            if let Some(expected) = req.expected_version
                && current.as_ref().map(|o| o.metadata.version) != Some(expected)
            {
                return Err(conflict(&key, current.as_ref()));
            }
            let (object, precondition, result) = match &current {
                Some(cur) => {
                    if cur.spec == desired.spec && cur.metadata.labels == desired.metadata.labels {
                        return Ok(ApplyResponse {
                            object: Some(cur.into()),
                            result: ApplyResult::Unchanged.into(),
                        });
                    }
                    let mut next = cur.clone();
                    next.spec = desired.spec.clone();
                    next.metadata.labels = desired.metadata.labels.clone();
                    (
                        next,
                        Precondition::Version(cur.metadata.version),
                        ApplyResult::Updated,
                    )
                }
                // Status is the cluster's: controllers trust it, so a
                // caller's own never goes in.
                None => (
                    Object {
                        status: serde_json::Value::Null,
                        ..desired.clone()
                    },
                    Precondition::MustNotExist,
                    ApplyResult::Created,
                ),
            };
            if let Some(cur) = &current {
                check_storage(&self.store, cur, &object)?;
            }
            crate::quota::check(&self.store, &object)?;
            crate::security::check(&self.store, &object)?;
            check_host(&self.store, &object)?;
            check_alias(&self.store, &object)?;
            if req.dry_run {
                return Ok(ApplyResponse {
                    object: Some((&object).into()),
                    result: result.into(),
                });
            }
            match self
                .write(Command::Put {
                    object,
                    precondition,
                })
                .await?
            {
                Outcome::Written(o) => {
                    return Ok(ApplyResponse {
                        object: Some((&o).into()),
                        result: result.into(),
                    });
                }
                // Someone else wrote in between; apply on top of their write.
                Outcome::Conflict { .. } if req.expected_version.is_none() => continue,
                Outcome::Conflict { .. } => {
                    return Err(conflict(
                        &key,
                        self.store.get(&key).map_err(internal)?.as_ref(),
                    ));
                }
                other => return Err(Status::internal(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(Status::aborted(format!(
            "{key} is changing too fast; retry"
        )))
    }
}

/// Refuses a route a host that routes of another namespace have, or that
/// covers or is covered by one of theirs: whoever routes a host gets its
/// requests, its certificate and its ACME orders.
fn check_host(store: &Store, next: &Object) -> Result<(), Status> {
    use legion_api::route::{RouteSpec, hosts_overlap};
    if next.kind != Kind::Route {
        return Ok(());
    }
    let spec: RouteSpec = legion_api::parse(&next.spec).map_err(invalid)?;
    let routes = store.list(Kind::Route, None).map_err(internal)?.objects;
    for r in routes {
        if r.metadata.namespace == next.metadata.namespace {
            continue;
        }
        let Ok(theirs) = legion_api::parse::<RouteSpec>(&r.spec) else {
            continue;
        };
        if hosts_overlap(&spec.host, &theirs.host) {
            return Err(Status::failed_precondition(format!(
                "host {}: namespace {} routes {}, and a host belongs to one namespace",
                spec.host, r.metadata.namespace, theirs.host
            )));
        }
    }
    Ok(())
}

/// Refuses a Volume naming storage one node at a time uses (an iSCSI LUN,
/// a plugin's volume) that another Volume, in any namespace, names already:
/// claims hold a Volume, and two of them could otherwise be held by two
/// nodes mounting the one LUN. The other is not named: it may be in a
/// namespace the caller cannot read. Claims check the same on their own.
fn check_alias(store: &Store, next: &Object) -> Result<(), Status> {
    use legion_api::volume::VolumeSpec;
    if next.kind != Kind::Volume {
        return Ok(());
    }
    let spec: VolumeSpec = legion_api::parse(&next.spec).map_err(invalid)?;
    let Some(storage) = spec.exclusive_storage() else {
        return Ok(());
    };
    let key = next.key();
    for other in store.list(Kind::Volume, None).map_err(internal)?.objects {
        if other.key() == key {
            continue;
        }
        let Ok(theirs) = legion_api::parse::<VolumeSpec>(&other.spec) else {
            continue;
        };
        if theirs.exclusive_storage().as_deref() == Some(storage.as_str()) {
            return Err(Status::already_exists(format!(
                "{key}: another Volume names this storage already, and it is used by one \
                 node at a time; use that Volume"
            )));
        }
    }
    Ok(())
}

/// Refuses a Volume another storage while it is in use: a node holds it
/// (its dataset, or the LUN or plugin volume it has attached) or a task
/// that has not ended mounts it. Agents keep what they mounted, so the
/// change would not reach them.
fn check_storage(store: &Store, cur: &Object, next: &Object) -> Result<(), Status> {
    use legion_api::volume::{VolumeSpec, VolumeStatus};
    if cur.kind != Kind::Volume {
        return Ok(());
    }
    let (before, after): (VolumeSpec, VolumeSpec) = (
        legion_api::parse(&cur.spec).map_err(internal)?,
        legion_api::parse(&next.spec).map_err(invalid)?,
    );
    if before.same_storage(&after) {
        return Ok(());
    }
    let key = cur.key();
    let refuse = |why: String| {
        Err(Status::failed_precondition(format!(
            "{key} {why}: its driver and source change only once nothing uses it"
        )))
    };
    let status: VolumeStatus = serde_json::from_value(cur.status.clone()).unwrap_or_default();
    if !status.node.is_empty() {
        return refuse(format!("is held by node {}", status.node));
    }
    if let Some(task) = mounted_by(store, cur)? {
        return refuse(format!("is mounted by task {task}"));
    }
    Ok(())
}

/// A task that has not ended and mounts the Volume `cur`, if any.
fn mounted_by(store: &Store, cur: &Object) -> Result<Option<String>, Status> {
    use legion_api::task::{TaskSpec, TaskStatus};
    let tasks = store
        .list(Kind::Task, Some(&cur.metadata.namespace))
        .map_err(internal)?;
    for t in &tasks.objects {
        let (Ok(spec), Ok(state)) = (
            serde_json::from_value::<TaskSpec>(t.spec.clone()),
            serde_json::from_value::<TaskStatus>(t.status.clone()),
        ) else {
            continue;
        };
        if !state.state.is_terminal() && spec.uses_volume(&cur.metadata.name) {
            return Ok(Some(t.metadata.name.clone()));
        }
    }
    Ok(None)
}

/// Refuses to delete a Volume in use, and returns the version checked, for
/// the delete to be made on: a task that has not ended mounts it, or a
/// node still in the cluster holds the LUN or plugin volume it attached.
/// Deleted then, it would come back unheld, for another node to attach
/// while the first still writes to it. (A ZFS volume's node keeps its
/// dataset whatever its tasks do, and the Volume may go once they have.)
/// An `ImagePolicy`'s keys must each read as a P-256 public key: admission
/// checks only that they are PEM, as the key reader lives in legion-image.
fn check_policy_keys(policy: &Object) -> Result<(), Status> {
    let spec: legion_api::image::ImagePolicySpec =
        legion_api::parse(&policy.spec).map_err(internal)?;
    for (i, k) in spec.keys.iter().enumerate() {
        if let Err(e) = legion_image::cosign::PublicKey::from_pem(k) {
            return Err(Status::invalid_argument(format!(
                "ImagePolicy {}: spec.keys[{i}]: {e}",
                policy.metadata.name
            )));
        }
    }
    Ok(())
}

fn check_volume_delete(store: &Store, key: &ObjectKey) -> Result<Option<u64>, Status> {
    use legion_api::volume::{Access, VolumeSpec, VolumeStatus};
    let Some(cur) = store.get(key).map_err(internal)? else {
        return Ok(None);
    };
    let refuse = |why: String| {
        Err(Status::failed_precondition(format!(
            "{key} {why}: it is deleted only once nothing uses it"
        )))
    };
    if let Some(task) = mounted_by(store, &cur)? {
        return refuse(format!("is mounted by task {task}"));
    }
    let spec: VolumeSpec = legion_api::parse(&cur.spec).map_err(internal)?;
    let status: VolumeStatus = serde_json::from_value(cur.status.clone()).unwrap_or_default();
    if spec.access() == Access::Single
        && !status.node.is_empty()
        && store
            .get(&ObjectKey::new(Kind::Node, CLUSTER_SCOPE, &status.node))
            .map_err(internal)?
            .is_some()
    {
        return refuse(format!(
            "is held by node {}, which lets go of it once its tasks have ended (or remove the node)",
            status.node
        ));
    }
    Ok(Some(cur.metadata.version))
}

fn default_namespace(kind: Kind, namespace: &mut String) {
    if kind.is_cluster_scoped() {
        if namespace.is_empty() {
            *namespace = CLUSTER_SCOPE.to_owned();
        }
    } else if namespace.is_empty() {
        *namespace = "default".to_owned();
    }
}

fn parse_ref(r: Option<ObjectRef>) -> Result<ObjectKey, Status> {
    let r = r.ok_or_else(|| Status::invalid_argument("ref is required"))?;
    let mut key = ObjectKey::try_from(r).map_err(invalid)?;
    default_namespace(key.kind, &mut key.namespace);
    key.validate().map_err(invalid)?;
    Ok(key)
}

fn parse_kind(kind: &str) -> Result<Kind, Status> {
    kind.parse().map_err(invalid)
}

fn invalid(e: impl std::fmt::Display) -> Status {
    Status::invalid_argument(e.to_string())
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn not_found(key: &ObjectKey) -> Status {
    Status::not_found(format!("{key} not found"))
}

fn conflict(key: &ObjectKey, current: Option<&Object>) -> Status {
    match current {
        Some(o) => Status::aborted(format!("{key} is at version {}", o.metadata.version)),
        None => Status::aborted(format!("{key} does not exist")),
    }
}

pub(crate) fn forwarded<T>(inner: T) -> Request<T> {
    let mut req = Request::new(inner);
    req.metadata_mut()
        .insert(FORWARDED, tonic::metadata::MetadataValue::from_static("1"));
    req
}

/// [`forwarded`], for `caller`: a user's name goes with it.
pub(crate) fn forwarded_for<T>(inner: T, caller: &Caller) -> Result<Request<T>, Status> {
    let mut req = forwarded(inner);
    if let Caller::User(name) = caller {
        let name = tonic::metadata::MetadataValue::try_from(name.as_str())
            .map_err(|_| Status::invalid_argument("a user name is not a header value"))?;
        req.metadata_mut().insert(crate::authz::FORWARDED_FOR, name);
    }
    Ok(req)
}

impl ApiService {
    pub(crate) fn caller<T>(&self, req: &Request<T>) -> Result<Caller, Status> {
        Caller::of(req, self.peer_tls.is_some())
    }

    /// Deleting a Task that has not ended asks it to stop instead: its
    /// pod may still run, and its owner replaces it only once the agent
    /// has torn it down (a stateful task's volumes are then free). The
    /// stopped task is then deleted as any other, or with its owner.
    /// Returns the version written, or `None` for a task to delete now.
    /// Deletes a Node, recording first that the node is removed: its
    /// certificate is refused from then, so it cannot come back between
    /// the two writes. Only once the delete would go through, not for a
    /// stale version; if it then does not (the Node changed meanwhile, or
    /// the write failed and the Node is still there), the node is let back
    /// in.
    async fn remove_node(
        &self,
        key: &ObjectKey,
        expected_version: Option<u64>,
    ) -> Result<Outcome, Status> {
        let precondition = expected_version.map_or(Precondition::None, Precondition::Version);
        let Some(node) = self.store.get(key).map_err(internal)? else {
            return Ok(Outcome::NotFound);
        };
        if expected_version.is_some_and(|v| v != node.metadata.version) {
            return Ok(Outcome::Conflict {
                current: Some(node.metadata.version),
            });
        }
        let recorded = crate::removed::record(self, &key.name).await?;
        let deleted = self
            .write(Command::Delete {
                key: key.clone(),
                precondition,
            })
            .await;
        // Not when the Node is gone: this delete or another took it, and
        // the removal stands.
        let stays = match &deleted {
            Ok(Outcome::Conflict { current: Some(_) }) => true,
            Ok(_) => false,
            // The delete may have gone through before the error: only what
            // the store holds once current says.
            Err(_) => {
                self.consistent().await?;
                self.store.get(key).map_err(internal)?.is_some()
            }
        };
        if stays {
            crate::removed::forget(self, &key.name, recorded).await?;
        }
        deleted
    }

    async fn stop_task(
        &self,
        key: &ObjectKey,
        expected: Option<u64>,
    ) -> Result<Option<u64>, Status> {
        use legion_api::task::{DesiredState, TaskSpec, TaskStatus};
        for _ in 0..APPLY_ATTEMPTS {
            let Some(mut task) = self.store.get(key).map_err(internal)? else {
                return Ok(None);
            };
            if expected.is_some_and(|v| v != task.metadata.version) {
                return Err(conflict(key, Some(&task)));
            }
            let status: TaskStatus =
                serde_json::from_value(task.status.clone()).unwrap_or_default();
            if status.state.is_terminal() {
                return Ok(None);
            }
            let mut spec: TaskSpec = serde_json::from_value(task.spec.clone()).map_err(internal)?;
            if spec.desired_state == DesiredState::Shutdown {
                return Ok(Some(task.metadata.version));
            }
            spec.desired_state = DesiredState::Shutdown;
            task.spec = serde_json::to_value(&spec).map_err(internal)?;
            let precondition = Precondition::Version(task.metadata.version);
            match self
                .write(Command::Put {
                    object: task,
                    precondition,
                })
                .await?
            {
                Outcome::Written(o) => return Ok(Some(o.metadata.version)),
                Outcome::Conflict { .. } if expected.is_none() => continue,
                Outcome::Conflict { .. } => {
                    return Err(conflict(
                        key,
                        self.store.get(key).map_err(internal)?.as_ref(),
                    ));
                }
                other => return Err(Status::internal(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(Status::aborted(format!(
            "{key} is changing too fast; retry"
        )))
    }

    async fn list_local(&self, req: ListRequest) -> Result<ListResponse, Status> {
        let kind = parse_kind(&req.kind)?;
        check_namespace(&req.namespace)?;
        let namespace = (!req.namespace.is_empty()).then_some(req.namespace.as_str());
        let listing = self.store.list(kind, namespace).map_err(internal)?;
        let selector: BTreeMap<_, _> = req.match_labels.into_iter().collect();
        let objects = listing
            .objects
            .iter()
            .filter(|o| o.metadata.namespace != SYSTEM_NAMESPACE)
            .filter(|o| {
                selector
                    .iter()
                    .all(|(k, v)| o.metadata.labels.get(k) == Some(v))
            })
            .map(pb::Object::from)
            .collect();
        Ok(ListResponse {
            objects,
            version: listing.version,
        })
    }
}

#[tonic::async_trait]
impl Objects for ApiService {
    async fn apply(&self, req: Request<ApplyRequest>) -> Result<Response<ApplyResponse>, Status> {
        // A user's rights are checked where the request is served: a
        // follower forwards them, its own copy may lag a new RoleBinding.
        let caller = self.caller(&req)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.apply(forwarded_for(req.into_inner(), &caller)?).await;
        }
        // The rights it checks, what it compares with, and the quotas and
        // policies, must be the latest, not what this replica has applied.
        self.consistent().await?;
        if let Some(o) = &req.get_ref().object {
            let kind = parse_kind(&o.kind)?;
            let mut namespace = o.namespace.clone();
            default_namespace(kind, &mut namespace);
            caller.check(&self.store, Verb::Write, kind, &namespace)?;
        }
        Ok(Response::new(self.apply_local(req.into_inner()).await?))
    }

    async fn get(&self, req: Request<ObjectRef>) -> Result<Response<pb::Object>, Status> {
        check_namespace(&req.get_ref().namespace)?;
        let key = parse_ref(Some(req.get_ref().clone()))?;
        let caller = self.caller(&req)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.get(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Read, key.kind, &key.namespace)?;
        let key = parse_ref(Some(req.into_inner()))?;
        match self.store.get(&key).map_err(internal)? {
            Some(o) => Ok(Response::new((&o).into())),
            None => Err(not_found(&key)),
        }
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let caller = self.caller(&req)?;
        let kind = parse_kind(&req.get_ref().kind)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.list(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        if !req.get_ref().namespace.is_empty() || kind.is_cluster_scoped() {
            let mut namespace = req.get_ref().namespace.clone();
            default_namespace(kind, &mut namespace);
            caller.check(&self.store, Verb::Read, kind, &namespace)?;
        }
        let mut listed = Response::new(self.list_local(req.into_inner()).await?);
        // Across namespaces, a user sees those they may read in.
        if caller != Caller::All {
            listed
                .get_mut()
                .objects
                .retain(|o| caller.may(&self.store, Verb::Read, kind, &o.namespace));
        }
        Ok(listed)
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = parse_ref(req.get_ref().r#ref.clone())?;
        let caller = self.caller(&req)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.delete(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Write, key.kind, &key.namespace)?;
        let req = req.into_inner();
        let key = parse_ref(req.r#ref)?;
        check_namespace(&key.namespace)?;
        if key.kind == Kind::Task
            && let Some(version) = self.stop_task(&key, req.expected_version).await?
        {
            return Ok(Response::new(DeleteResponse { version }));
        }
        let mut attempts = APPLY_ATTEMPTS;
        let deleted = loop {
            let mut precondition = req
                .expected_version
                .map_or(Precondition::None, Precondition::Version);
            // Deleted on the version checked: a node claiming it meanwhile
            // changes the version. The check is then made again, which
            // refuses it, as a caller who named no version asked for nothing
            // that changing the Volume made untrue.
            if key.kind == Kind::Volume
                && let Some(checked) = check_volume_delete(&self.store, &key)?
                && req.expected_version.is_none_or(|v| v == checked)
            {
                precondition = Precondition::Version(checked);
            }
            #[cfg(feature = "test-hooks")]
            crate::hooks::pause("volume-delete", &key.name).await;
            let deleted = if key.kind == Kind::Node {
                // Run to the end even if the caller goes: stopped between its
                // writes, it would leave a live node refused.
                let api = self.clone();
                let key = key.clone();
                tokio::spawn(async move { api.remove_node(&key, req.expected_version).await })
                    .await
                    .map_err(internal)??
            } else {
                self.write(Command::Delete {
                    key: key.clone(),
                    precondition,
                })
                .await?
            };
            attempts -= 1;
            let again = matches!(deleted, Outcome::Conflict { .. })
                && key.kind == Kind::Volume
                && req.expected_version.is_none();
            if !again || attempts == 0 {
                break deleted;
            }
        };
        match deleted {
            Outcome::Deleted { version } => Ok(Response::new(DeleteResponse { version })),
            Outcome::NotFound => Err(not_found(&key)),
            Outcome::Conflict { .. } => Err(conflict(
                &key,
                self.store.get(&key).map_err(internal)?.as_ref(),
            )),
            other => Err(Status::internal(format!("unexpected outcome {other:?}"))),
        }
    }

    type WatchStream = ReceiverStream<Result<pb::WatchEvent, Status>>;

    async fn watch(
        &self,
        req: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let caller = self.caller(&req)?;
        // Rights are checked against this replica's store, as each event
        // goes out: first brought up to what the leader has committed, so
        // a RoleBinding revoked there is revoked here before any of the
        // history is served. From then on the store is applied in order,
        // so no event comes out ahead of a revocation committed before it.
        self.consistent().await?;
        let req = req.into_inner();
        let kind = if req.kind.is_empty() {
            None
        } else {
            Some(parse_kind(&req.kind)?)
        };
        if let Some(k) = kind.filter(|k| k.is_cluster_scoped()) {
            caller.check(&self.store, Verb::Read, k, "")?;
        }
        let store = self.store.clone();
        let mut watcher = self
            .store
            .watch(req.from_version, kind)
            .map_err(|e| Status::aborted(e.to_string()))?;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut closing = self.closing.clone();
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    next = watcher.next() => Some(next),
                    _ = closing.wait_for(|c| *c) => None,
                    // Gone, though nothing it may see has happened since.
                    () = tx.closed() => return,
                };
                // A client too slow to take this ends with the stream.
                let Some(next) = next else {
                    let _ = tx.try_send(Err(Status::unavailable("manager shutting down")));
                    return;
                };
                let item = match next {
                    Ok(Event::Put(o) | Event::Delete(o))
                        if o.metadata.namespace == SYSTEM_NAMESPACE
                            || !caller.may(&store, Verb::Read, o.kind, &o.metadata.namespace) =>
                    {
                        continue;
                    }
                    Ok(Event::Put(o)) => Ok(pb::WatchEvent {
                        r#type: watch_event::Type::Put.into(),
                        object: Some((&o).into()),
                    }),
                    Ok(Event::Delete(o)) => Ok(pb::WatchEvent {
                        r#type: watch_event::Type::Delete.into(),
                        object: Some((&o).into()),
                    }),
                    Ok(Event::Reset(v)) => Err(Status::aborted(WatchError::Reset(v).to_string())),
                    Err(WatchError::Closed) => return,
                    Err(e) => Err(Status::aborted(e.to_string())),
                };
                let last = item.is_err();
                // The client went away, or stopped reading as the manager
                // shuts down: either way the stream ends.
                let sent = tokio::select! {
                    sent = tx.send(item) => sent.is_ok(),
                    _ = closing.wait_for(|c| *c) => false,
                };
                if !sent || last {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl Cluster for ApiService {
    async fn init(&self, req: Request<InitRequest>) -> Result<Response<StatusResponse>, Status> {
        self.caller(&req)?.require_all("initialise the cluster")?;
        let raft = &self.store.raft;
        if raft.is_initialized().await.map_err(internal)? {
            return Err(Status::already_exists(
                "this manager already belongs to a cluster",
            ));
        }
        let members = BTreeMap::from([(
            self.id,
            BasicNode {
                addr: self.advertise.clone(),
            },
        )]);
        raft.initialize(members).await.map_err(internal)?;
        raft.wait(Some(std::time::Duration::from_secs(10)))
            .current_leader(self.id, "become leader of the new cluster")
            .await
            .map_err(internal)?;
        Ok(Response::new(self.status()?))
    }

    async fn status(
        &self,
        _req: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(self.status()?))
    }

    async fn join_token(
        &self,
        req: Request<pb::JoinTokenRequest>,
    ) -> Result<Response<pb::JoinTokenResponse>, Status> {
        self.caller(&req)?.require_all("see join tokens")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.join_token(forwarded(req.into_inner())).await;
        }
        self.consistent().await?;
        let req = req.into_inner();
        let role = Role::parse(&req.role)
            .filter(|r| matches!(r, Role::Manager | Role::Worker))
            .ok_or_else(|| Status::invalid_argument("the role is `manager` or `worker`"))?;
        let token = self.issuer()?.token(role, req.rotate).await?;
        Ok(Response::new(pb::JoinTokenResponse {
            token: token.to_string(),
        }))
    }

    async fn autolock(
        &self,
        req: Request<pb::AutolockRequest>,
    ) -> Result<Response<pb::AutolockResponse>, Status> {
        self.caller(&req)?.require_all("change autolock")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.autolock(forwarded(req.into_inner())).await;
        }
        self.consistent().await?;
        let key = crate::autolock::apply(self, &req.into_inner().action).await?;
        Ok(Response::new(pb::AutolockResponse {
            key: key
                .as_ref()
                .map(crate::autolock::encode)
                .unwrap_or_default(),
        }))
    }

    async fn usage(
        &self,
        req: Request<pb::UsageRequest>,
    ) -> Result<Response<pb::UsageResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.get_ref().namespace.clone();
        check_namespace(&namespace)?;
        // Only the leader hears heartbeats; it checks the user's rights.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.usage(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        let mut usage = {
            let mut nodes: BTreeMap<String, Vec<pb::TaskUsage>> = BTreeMap::new();
            for (key, (node, u)) in self.usage.fresh() {
                let Some((ns, task)) = key.split_once('/') else {
                    continue;
                };
                if ns == SYSTEM_NAMESPACE || (!namespace.is_empty() && ns != namespace) {
                    continue;
                }
                nodes.entry(node).or_default().push(pb::TaskUsage {
                    namespace: ns.to_owned(),
                    task: task.to_owned(),
                    cpu_percent: u.cpu,
                    memory_bytes: u.memory,
                });
            }
            pb::UsageResponse {
                nodes: nodes
                    .into_iter()
                    .map(|(node, tasks)| pb::NodeUsage { node, tasks })
                    .collect(),
            }
        };
        // A user sees the tasks of the namespaces they may read in.
        if caller != Caller::All {
            for n in &mut usage.nodes {
                n.tasks
                    .retain(|t| caller.may(&self.store, Verb::Read, Kind::Task, &t.namespace));
            }
        }
        Ok(Response::new(usage))
    }

    type BackupStream = ReceiverStream<Result<pb::BackupChunk, Status>>;

    async fn backup(
        &self,
        req: Request<pb::BackupRequest>,
    ) -> Result<Response<Self::BackupStream>, Status> {
        // Secrets and join tokens included.
        self.caller(&req)?.require_all("back up the cluster")?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let mut chunks = c.backup(forwarded(req.into_inner())).await?.into_inner();
            tokio::spawn(async move {
                loop {
                    let chunk = chunks.message().await.transpose();
                    let Some(chunk) = chunk else { return };
                    let last = chunk.is_err();
                    if tx.send(chunk).await.is_err() || last {
                        return;
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        self.consistent().await?;
        let backup = self.store.backup().map_err(internal)?;
        let json = serde_json::to_vec(&backup).map_err(internal)?;
        tracing::info!(
            version = backup.version,
            objects = backup.objects.len(),
            bytes = json.len(),
            "backup"
        );
        tokio::spawn(async move {
            for data in json.chunks(1 << 20) {
                let chunk = pb::BackupChunk {
                    data: data.to_vec(),
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn issue_user(
        &self,
        req: Request<pb::IssueUserRequest>,
    ) -> Result<Response<pb::IssueUserResponse>, Status> {
        self.caller(&req)?.require_all("issue user certificates")?;
        let req = req.into_inner();
        legion_api::validate_name(&req.name)
            .map_err(|e| Status::invalid_argument(format!("user name: {e}")))?;
        let lifetime = match req.days {
            0 => legion_pki::CERT_LIFETIME,
            days => std::time::Duration::from_secs(u64::from(days) * 24 * 3600),
        };
        // Any manager with the CA key can sign: nothing is written.
        let ca = &self.issuer()?.ca;
        let id = legion_pki::Identity::new(&req.name, Role::User);
        let cert_pem = ca
            .sign(&req.csr_pem, &id, lifetime)
            .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
        tracing::info!(%id, days = lifetime.as_secs() / 86_400, "issued a user certificate");
        Ok(Response::new(pb::IssueUserResponse {
            cert_pem,
            ca_pem: ca.cert_pem().to_owned(),
        }))
    }
}

impl ApiService {
    fn status(&self) -> Result<StatusResponse, Status> {
        let m = self.store.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let members = membership
            .nodes()
            .map(|(id, node)| pb::Member {
                id: *id,
                addr: node.addr.clone(),
                voter: membership.voter_ids().any(|v| v == *id),
            })
            .collect();
        Ok(StatusResponse {
            node_id: self.id,
            leader_id: m.current_leader,
            state: format!("{:?}", m.state),
            term: m.current_term,
            applied_version: self.store.applied_version().map_err(internal)?,
            members,
            protocol: self.speaking.mine.speaks,
            protocol_floor: crate::upgrade::read(&self.store),
        })
    }
}
