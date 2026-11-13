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
    /// This manager's node name, which `status` gives other managers.
    pub(crate) name: String,
    /// Where it records what it decides, for the audit trail.
    pub(crate) audit: crate::audit::Audit,
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
    /// The CA each node's certificate was of at its last heartbeat here.
    pub(crate) ca_seen: crate::ca::Seen,
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
            .current()?
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
    if let Some(task) = mounted_by(store, cur, false)? {
        return refuse(format!("is mounted by task {task}"));
    }
    Ok(())
}

/// A task that has not ended and mounts the Volume `cur`, if any. With
/// `placed`, only one on a node of the cluster counts: one on no node yet
/// mounts nothing (a lost Volume's waits so), nor does one on a node
/// removed, which is heard no more; so such a Volume may be deleted.
fn mounted_by(store: &Store, cur: &Object, placed: bool) -> Result<Option<String>, Status> {
    use legion_api::task::{TaskSpec, TaskStatus};
    let tasks = store
        .list(Kind::Task, Some(&cur.metadata.namespace))
        .map_err(internal)?;
    let nodes: std::collections::BTreeSet<String> = store
        .list(Kind::Node, None)
        .map_err(internal)?
        .objects
        .into_iter()
        .map(|n| n.metadata.name)
        .collect();
    for t in &tasks.objects {
        let (Ok(spec), Ok(state)) = (
            serde_json::from_value::<TaskSpec>(t.spec.clone()),
            serde_json::from_value::<TaskStatus>(t.status.clone()),
        ) else {
            continue;
        };
        if !state.state.is_terminal()
            && spec.uses_volume(&cur.metadata.name)
            && (!placed || nodes.contains(&spec.node))
        {
            return Ok(Some(t.metadata.name.clone()));
        }
    }
    Ok(None)
}

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

/// Refuses to delete a Volume in use, and returns the version checked, for
/// the delete to be made on: a task that has not ended mounts it, or a
/// node still in the cluster holds the LUN or plugin volume it attached.
/// Deleted then, it would come back unheld, for another node to attach
/// while the first still writes to it. (A ZFS volume's node keeps its
/// dataset whatever its tasks do, and the Volume may go once they have.)
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
    if let Some(task) = mounted_by(store, &cur, true)? {
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
    /// What to audit of a request recorded where it first arrives (its
    /// caller's name is not forwarded): nothing on a manager it was
    /// forwarded to, which the first records.
    pub(crate) fn audit_point<T>(
        &self,
        req: &Request<T>,
        action: &'static str,
        target: legion_api::audit::Target,
    ) -> Option<(
        legion_api::audit::Actor,
        &'static str,
        legion_api::audit::Target,
    )> {
        if req.metadata().contains_key(FORWARDED) {
            return None;
        }
        let actor = crate::audit::actor(req, self.peer_tls.is_some());
        Some((actor, action, target))
    }

    /// Records `result` at `point`, if any, and passes it on.
    pub(crate) fn audit_end<T>(
        &self,
        point: Option<(
            legion_api::audit::Actor,
            &'static str,
            legion_api::audit::Target,
        )>,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        match point {
            Some((actor, action, target)) => self.audited(actor, action, target, result),
            None => result,
        }
    }

    /// Records `result` in the audit trail and passes it on.
    pub(crate) fn audited<T>(
        &self,
        actor: legion_api::audit::Actor,
        action: &str,
        target: legion_api::audit::Target,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        self.audit.record(actor, action, target, &result);
        result
    }

    /// Refuses any caller but a manager (or anyone, without TLS: tests).
    fn only_managers<T>(&self, req: &Request<T>, what: &str) -> Result<(), Status> {
        self.caller(req)?;
        match crate::tls::caller(req, self.peer_tls.is_some())? {
            Some(id) if id.role != Role::Manager => Err(Status::permission_denied(format!(
                "{id} may not {what}: managers only"
            ))),
            _ => Ok(()),
        }
    }

    pub(crate) fn caller<T>(&self, req: &Request<T>) -> Result<Caller, Status> {
        let tls = self.peer_tls.is_some();
        // A removed manager's certificate is refused, and so is what it
        // forwards (PLAN.md phase 7); workers never reach the API.
        if let Some(id) = crate::tls::caller(req, tls)?.filter(|id| id.role == Role::Manager)
            && let Some(issued) = crate::tls::caller_issued(req, tls)?
        {
            crate::removed::check(&self.store, &id.name, issued)?;
        }
        Caller::of(req, tls)
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
        // A manager leaves the Raft membership too, before its Node goes:
        // it no longer votes, and its calls are refused (PLAN.md phase 7).
        if let Err(e) = self.leave_membership(&key.name).await {
            crate::removed::forget(self, &key.name, recorded).await?;
            return Err(e);
        }
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
        } else if let Err(e) = crate::volumes::mark_lost(self, &key.name).await {
            // A node joining under the name marks them before it is let in.
            tracing::warn!(node = %key.name, "marking its Volumes lost: {e}");
        }
        deleted
    }

    /// Takes the manager named `name` out of the Raft membership, if it is
    /// a member: never the last voter.
    pub(crate) async fn leave_membership(&self, name: &str) -> Result<(), Status> {
        use openraft::ChangeMembers;
        let Some(id) = crate::managers::id_of(&self.store, name)? else {
            return Ok(());
        };
        let m = self.store.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let voters: std::collections::BTreeSet<NodeId> = membership.voter_ids().collect();
        let change = if voters.contains(&id) {
            if voters.len() == 1 {
                return Err(Status::failed_precondition(format!(
                    "manager {name} is the cluster's last voter: it cannot be removed"
                )));
            }
            ChangeMembers::RemoveVoters([id].into())
        } else if membership.nodes().any(|(n, _)| *n == id) {
            ChangeMembers::RemoveNodes([id].into())
        } else {
            return Ok(());
        };
        tracing::info!(manager = name, id, "removing a manager from the membership");
        let changed = self.store.raft.change_membership(change, false);
        tokio::time::timeout(crate::COMMIT_TIMEOUT, changed)
            .await
            .map_err(|_| crate::no_quorum())?
            .map_err(|e| Status::unavailable(format!("removing manager {name}: {e}")))?;
        Ok(())
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
        // Audited where it first arrives, which knows the caller, with
        // the answer the leader gave if it went there.
        let target = req
            .get_ref()
            .object
            .as_ref()
            .map(|o| crate::audit::target(&o.kind, &o.namespace, &o.name))
            .unwrap_or_default();
        let audit = self.audit_point(&req, "apply", target);
        let applied = async {
            // A user's rights are checked where the request is served: a
            // follower forwards them, its own copy may lag a new
            // RoleBinding.
            let caller = self.caller(&req)?;
            if let Route::Leader(mut c, _) = self.route(&req)? {
                return c.apply(forwarded_for(req.into_inner(), &caller)?).await;
            }
            // The rights it checks, what it compares with, and the quotas
            // and policies, must be the latest, not what this replica has
            // applied.
            self.consistent().await?;
            if let Some(o) = &req.get_ref().object {
                let kind = parse_kind(&o.kind)?;
                let mut namespace = o.namespace.clone();
                default_namespace(kind, &mut namespace);
                caller.check(&self.store, Verb::Write, kind, &namespace)?;
            }
            Ok(Response::new(self.apply_local(req.into_inner()).await?))
        }
        .await;
        self.audit_end(audit, applied)
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
        // Audited where it first arrives, as an apply is.
        let target = crate::audit::target(key.kind.as_str(), &key.namespace, &key.name);
        let audit = self.audit_point(&req, "delete", target);
        let deleted = async {
            let caller = self.caller(&req)?;
            if let Route::Leader(mut c, _) = self.route(&req)? {
                return c.delete(forwarded_for(req.into_inner(), &caller)?).await;
            }
            self.delete_served(&caller, key, req).await
        }
        .await;
        self.audit_end(audit, deleted)
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
        // A user's rights as the watch opens, raised as they grow: once a
        // role it ran with is lowered or taken, the watch ends saying so,
        // rather than leave out what the user may no longer see, which the
        // client could not tell from nothing happening. Checked as each
        // event goes out and every few seconds.
        let mut rights = caller.roles(&self.store)?;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut closing = self.closing.clone();
        tokio::spawn(async move {
            let mut lowered = |store: &Store| -> Option<Status> {
                let (Caller::User(user), Some(before)) = (&caller, &mut rights) else {
                    return None;
                };
                let now = match caller.roles(store) {
                    Ok(now) => now.unwrap_or_default(),
                    Err(e) => return Some(e),
                };
                if let Some(why) = crate::authz::lowered(user, before, &now) {
                    return Some(why);
                }
                *before = now;
                None
            };
            loop {
                // `Some(None)`: time to check the rights again.
                let next = tokio::select! {
                    next = watcher.next() => Some(Some(next)),
                    _ = closing.wait_for(|c| *c) => None,
                    // Gone, though nothing it may see has happened since.
                    () = tx.closed() => return,
                    () = tokio::time::sleep(crate::authz::REVOCATION_CHECK) => Some(None),
                };
                if let Some(why) = lowered(&store) {
                    let _ = tx.send(Err(why)).await;
                    return;
                }
                let next = match next {
                    Some(Some(next)) => Some(next),
                    Some(None) => continue,
                    None => None,
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

impl ApiService {
    /// A delete this manager, the leader, serves: see `Objects::delete`.
    async fn delete_served(
        &self,
        caller: &Caller,
        key: ObjectKey,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
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
}

#[tonic::async_trait]
impl Cluster for ApiService {
    async fn init(&self, req: Request<InitRequest>) -> Result<Response<StatusResponse>, Status> {
        let audit = self.audit_point(&req, "init", Default::default());
        let result = async {
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
        .await;
        self.audit_end(audit, result)
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
        let audit = self.audit_point(
            &req,
            "join-token",
            crate::audit::target("JoinToken", "", &req.get_ref().role),
        );
        let result = async {
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
        .await;
        self.audit_end(audit, result)
    }

    async fn rotate_ca(
        &self,
        req: Request<pb::RotateCaRequest>,
    ) -> Result<Response<pb::RotateCaResponse>, Status> {
        let start = req.get_ref().start;
        let audit = self.audit_point(
            &req,
            if start { "ca-rotate" } else { "ca-status" },
            Default::default(),
        );
        let result = async {
            self.caller(&req)?.require_all("rotate the cluster CA")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.rotate_ca(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            if start {
                crate::ca::start(self).await?;
            }
            Ok(Response::new(crate::ca::status(self).await?))
        }
        .await;
        // Asking only reads: not a change to audit.
        if start {
            self.audit_end(audit, result)
        } else {
            result
        }
    }

    async fn change_role(
        &self,
        req: Request<pb::ChangeRoleRequest>,
    ) -> Result<Response<pb::ChangeRoleResponse>, Status> {
        let audit = self.audit_point(
            &req,
            "change-role",
            crate::audit::target("Node", "", &req.get_ref().node),
        );
        let result = async {
            self.caller(&req)?.require_all("change a node's role")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.change_role(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            let req = req.into_inner();
            crate::roles::request(self, &req.node, &req.role).await?;
            Ok(Response::new(pb::ChangeRoleResponse {}))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn demote(
        &self,
        req: Request<pb::DemoteRequest>,
    ) -> Result<Response<pb::DemoteResponse>, Status> {
        // Its own demotion, by the manager's certificate: only the leader
        // answers, as one forwarding would call with its own.
        let id = match crate::tls::caller(&req, self.peer_tls.is_some())? {
            Some(id) if id.role == Role::Manager => id,
            _ => {
                return Err(Status::permission_denied(
                    "only a manager asks to be demoted",
                ));
            }
        };
        let m = self.store.raft.metrics().borrow().clone();
        if m.current_leader != Some(self.id) {
            return Err(Status::unavailable("not the leader; ask the leader"));
        }
        self.consistent().await?;
        if let Some(issued) = crate::tls::caller_issued(&req, self.peer_tls.is_some())? {
            crate::removed::check(&self.store, &id.name, issued)?;
        }
        let r = crate::roles::demote(self, &id.name, req.into_inner()).await?;
        Ok(Response::new(r))
    }

    async fn autolock(
        &self,
        req: Request<pb::AutolockRequest>,
    ) -> Result<Response<pb::AutolockResponse>, Status> {
        let audit = self.audit_point(&req, "autolock", Default::default());
        let result = async {
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
        .await;
        self.audit_end(audit, result)
    }

    async fn reach(
        &self,
        req: Request<pb::ReachRequest>,
    ) -> Result<Response<pb::ReachResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.into_inner().namespace;
        if namespace.is_empty() {
            return Err(Status::invalid_argument("which namespace?"));
        }
        check_namespace(&namespace)?;
        caller.check(&self.store, Verb::Read, Kind::Service, &namespace)?;
        // Built as the dispatcher builds what the nodes' guards take.
        let all = |kind| -> Result<BTreeMap<ObjectKey, Object>, Status> {
            Ok(self
                .store
                .list(kind, None)
                .map_err(internal)?
                .objects
                .into_iter()
                .map(|o| (o.key(), o))
                .collect())
        };
        let policies =
            crate::dispatcher::policies(&all(Kind::Namespace)?, &all(Kind::NetworkPolicy)?);
        let endpoints = crate::dispatcher::endpoints(
            &all(Kind::Service)?,
            &all(Kind::Task)?,
            &all(Kind::Node)?,
        );
        let mut services: Vec<String> = endpoints
            .iter()
            .filter(|e| e.namespace == namespace && !e.job)
            .map(|e| e.name.clone())
            .collect();
        services.sort();
        let edges = legion_api::policy::reach(&policies, &endpoints, &namespace)
            .into_iter()
            .map(|r| pb::ReachEdge {
                from: r.from.unwrap_or_default(),
                to: r.to,
                all_ports: r.all_ports,
                ports: r
                    .ports
                    .into_iter()
                    .map(|(protocol, port)| pb::ReachPort {
                        // As the API spells it: `tcp`, `udp`.
                        protocol: serde_json::to_value(protocol)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .unwrap_or_default(),
                        port: port.into(),
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(pb::ReachResponse { services, edges }))
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

    async fn audit(
        &self,
        req: Request<pb::AuditRequest>,
    ) -> Result<Response<pb::AuditResponse>, Status> {
        self.caller(&req)?.require_all("read the audit trail")?;
        // Any manager answers, once caught up with the leader.
        self.consistent().await?;
        let req = req.into_inner();
        let filter: legion_api::audit::Filter = if req.filter_json.is_empty() {
            Default::default()
        } else {
            serde_json::from_slice(&req.filter_json)
                .map_err(|e| Status::invalid_argument(format!("the filter: {e}")))?
        };
        let limit = if req.limit == 0 {
            1000
        } else {
            req.limit as usize
        };
        let (events, truncated) = crate::audit::list(&self.store, &filter, limit)?;
        Ok(Response::new(pb::AuditResponse {
            events_json: events
                .iter()
                .map(|e| serde_json::to_vec(e).expect("events serialize"))
                .collect(),
            truncated,
        }))
    }

    async fn record_audit(
        &self,
        req: Request<pb::RecordAuditRequest>,
    ) -> Result<Response<pb::RecordAuditResponse>, Status> {
        // From another manager only: nobody else adds to the trail.
        let tls = self.peer_tls.is_some();
        if let Some(id) = crate::tls::caller(&req, tls)?
            && id.role != Role::Manager
        {
            return Err(Status::permission_denied(format!(
                "{id} may not add to the audit trail"
            )));
        }
        self.caller(&req)?;
        if self.leader_addr(&req)?.is_some() {
            return Err(Status::unavailable("not the leader; hand the events to it"));
        }
        let events = req
            .into_inner()
            .events_json
            .iter()
            .map(|e| serde_json::from_slice(e))
            .collect::<Result<Vec<legion_api::audit::AuditEvent>, _>>()
            .map_err(|e| Status::invalid_argument(format!("an audit event: {e}")))?;
        if !events.is_empty() {
            crate::audit::write(self, &events).await?;
        }
        Ok(Response::new(pb::RecordAuditResponse {}))
    }

    async fn me(&self, req: Request<pb::MeRequest>) -> Result<Response<pb::MeResponse>, Status> {
        let caller = self.caller(&req)?;
        let id = crate::tls::caller(&req, self.peer_tls.is_some())?;
        let (name, role) = match &id {
            Some(id) => (id.name.clone(), id.role.as_str().to_owned()),
            None => (String::new(), "admin".to_owned()),
        };
        // As of every change committed before the question, so a role just
        // given shows at once, whichever manager is asked.
        self.consistent().await?;
        let roles = caller.roles(&self.store)?;
        Ok(Response::new(pb::MeResponse {
            name,
            role,
            all: roles.is_none(),
            namespaces: roles
                .unwrap_or_default()
                .into_iter()
                .map(|(ns, r)| (ns, r.to_string()))
                .collect(),
        }))
    }

    async fn console_code(
        &self,
        req: Request<pb::ConsoleCodeRequest>,
    ) -> Result<Response<pb::ConsoleCodeResponse>, Status> {
        let tls = self.peer_tls.is_some();
        self.caller(&req)?;
        let id = crate::tls::caller(&req, tls)?
            .ok_or_else(|| Status::failed_precondition("the console needs TLS"))?;
        let audit = self.audit_point(
            &req,
            "console-code",
            crate::audit::target("User", "", &id.name),
        );
        let result = async {
            // Who asked: a person with their own certificate, or one a
            // manager forwards for, as it says.
            let (name, role, until_ms) = match id.role {
                Role::Admin | Role::User => {
                    let until = req
                        .peer_certs()
                        .and_then(|c| c.first().cloned())
                        .and_then(|der| legion_pki::validity(&legion_pki::cert_pem(&der)).ok())
                        .map(|(to, _)| crate::console::ms(to))
                        .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
                    (id.name.clone(), id.role.as_str().to_owned(), until)
                }
                Role::Manager if !req.get_ref().for_name.is_empty() => {
                    let r = req.get_ref();
                    (r.for_name.clone(), r.for_role.clone(), r.for_until_ms)
                }
                _ => {
                    return Err(Status::permission_denied(format!(
                        "{id} may not sign in to the console"
                    )));
                }
            };
            if !matches!(role.as_str(), "admin" | "user") {
                return Err(Status::invalid_argument("the role is `admin` or `user`"));
            }
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c
                    .console_code(forwarded(pb::ConsoleCodeRequest {
                        for_name: name,
                        for_role: role,
                        for_until_ms: until_ms,
                    }))
                    .await;
            }
            self.consistent().await?;
            let (code, expires_ms) = crate::console::new_code(self, &name, &role, until_ms).await?;
            Ok(Response::new(pb::ConsoleCodeResponse { code, expires_ms }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn console_session(
        &self,
        req: Request<pb::ConsoleSessionRequest>,
    ) -> Result<Response<pb::ConsoleSessionResponse>, Status> {
        self.only_managers(&req, "start console sessions")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.console_session(forwarded(req.into_inner())).await;
        }
        self.consistent().await?;
        let (session, signed) = crate::console::redeem(self, &req.get_ref().code).await?;
        tracing::info!(name = %signed.name, role = %signed.role, "a console session began");
        Ok(Response::new(pb::ConsoleSessionResponse {
            session,
            name: signed.name,
            role: signed.role,
            expires_ms: signed.expires_ms,
        }))
    }

    async fn console_end(
        &self,
        req: Request<pb::ConsoleEndRequest>,
    ) -> Result<Response<pb::ConsoleEndResponse>, Status> {
        self.only_managers(&req, "end console sessions")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.console_end(forwarded(req.into_inner())).await;
        }
        crate::console::end(self, &req.get_ref().session).await?;
        Ok(Response::new(pb::ConsoleEndResponse {}))
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
        let audit = self.audit_point(
            &req,
            "issue-user",
            crate::audit::target("User", "", &req.get_ref().name),
        );
        let result = async {
            self.caller(&req)?.require_all("issue user certificates")?;
            let req = req.into_inner();
            legion_api::validate_name(&req.name)
                .map_err(|e| Status::invalid_argument(format!("user name: {e}")))?;
            let lifetime = match req.days {
                0 => legion_pki::CERT_LIFETIME,
                days => std::time::Duration::from_secs(u64::from(days) * 24 * 3600),
            };
            // Any manager with the CA key can sign: nothing is written.
            let ca = self.issuer()?.current()?;
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
        .await;
        self.audit_end(audit, result)
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
            name: self.name.clone(),
            ca_issuer: crate::ca::own_issuer(self),
        })
    }
}
