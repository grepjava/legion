//! The public API: `Objects` and `Cluster` gRPC services over the store.
//!
//! Any manager accepts any request. Writes and consistent reads are served
//! by the leader, so a follower forwards them once (marking the request so it
//! is never forwarded twice). Watches are served from the local replica.

use std::collections::BTreeMap;

use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
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
}

enum Route {
    Local,
    Leader(ObjectsClient<Channel>),
}

impl ApiService {
    /// Where a leader-only request should run.
    fn route<T>(&self, req: &Request<T>) -> Result<Route, Status> {
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
                    ObjectsClient::new(connect_lazy(&addr))
                        .max_decoding_message_size(MAX_MESSAGE)
                        .max_encoding_message_size(MAX_MESSAGE),
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

    async fn consistent(&self) -> Result<(), Status> {
        self.store
            .raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|e| Status::unavailable(e.to_string()))
    }

    async fn write(&self, cmd: Command) -> Result<Outcome, Status> {
        match self.store.raft.client_write(cmd).await {
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
        legion_api::admit(&mut desired).map_err(invalid)?;
        let key = desired.key();

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
                None => (
                    desired.clone(),
                    Precondition::MustNotExist,
                    ApplyResult::Created,
                ),
            };
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

fn forwarded<T>(inner: T) -> Request<T> {
    let mut req = Request::new(inner);
    req.metadata_mut()
        .insert(FORWARDED, tonic::metadata::MetadataValue::from_static("1"));
    req
}

#[tonic::async_trait]
impl Objects for ApiService {
    async fn apply(&self, req: Request<ApplyRequest>) -> Result<Response<ApplyResponse>, Status> {
        match self.route(&req)? {
            Route::Leader(mut c) => c.apply(forwarded(req.into_inner())).await,
            Route::Local => Ok(Response::new(self.apply_local(req.into_inner()).await?)),
        }
    }

    async fn get(&self, req: Request<ObjectRef>) -> Result<Response<pb::Object>, Status> {
        if let Route::Leader(mut c) = self.route(&req)? {
            return c.get(forwarded(req.into_inner())).await;
        }
        let key = parse_ref(Some(req.into_inner()))?;
        self.consistent().await?;
        match self.store.get(&key).map_err(internal)? {
            Some(o) => Ok(Response::new((&o).into())),
            None => Err(not_found(&key)),
        }
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        if let Route::Leader(mut c) = self.route(&req)? {
            return c.list(forwarded(req.into_inner())).await;
        }
        let req = req.into_inner();
        let kind = parse_kind(&req.kind)?;
        let namespace = (!req.namespace.is_empty()).then_some(req.namespace.as_str());
        self.consistent().await?;
        let listing = self.store.list(kind, namespace).map_err(internal)?;
        let selector: BTreeMap<_, _> = req.match_labels.into_iter().collect();
        let objects = listing
            .objects
            .iter()
            .filter(|o| {
                selector
                    .iter()
                    .all(|(k, v)| o.metadata.labels.get(k) == Some(v))
            })
            .map(pb::Object::from)
            .collect();
        Ok(Response::new(ListResponse {
            objects,
            version: listing.version,
        }))
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        if let Route::Leader(mut c) = self.route(&req)? {
            return c.delete(forwarded(req.into_inner())).await;
        }
        let req = req.into_inner();
        let key = parse_ref(req.r#ref)?;
        let precondition = req
            .expected_version
            .map_or(Precondition::None, Precondition::Version);
        match self
            .write(Command::Delete {
                key: key.clone(),
                precondition,
            })
            .await?
        {
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
        let req = req.into_inner();
        let kind = if req.kind.is_empty() {
            None
        } else {
            Some(parse_kind(&req.kind)?)
        };
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
                };
                let Some(next) = next else {
                    let _ = tx
                        .send(Err(Status::unavailable("manager shutting down")))
                        .await;
                    return;
                };
                let item = match next {
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
                // The client went away.
                if tx.send(item).await.is_err() || last {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl Cluster for ApiService {
    async fn init(&self, _req: Request<InitRequest>) -> Result<Response<StatusResponse>, Status> {
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
        })
    }
}
