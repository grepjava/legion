//! A Volume's data backed up and restored (DESIGN.md §4.3), apart from the
//! control state's backup, which holds none of it. The leader serves both,
//! a follower forwarding the caller's request: a backup is a snapshot the
//! Volume's node takes as its tasks run (crash-consistent), streamed to the
//! user after a header saying what it is; a restore streams it to a node
//! without the data, and only once all of it is there makes the Volume,
//! on that node, so no task uses it before.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use legion_api::volume::{VolumeBackupHeader, VolumeDriver, VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::volume_restore_chunk::Part;
use legion_proto::v1::volumes_client::VolumesClient;
use legion_proto::v1::volumes_server::Volumes;
use legion_proto::v1::{
    BackupChunk, InputClosed, SessionError, SessionFrame, SessionOffer, SessionOpen,
    VolumeBackupRequest, VolumeExport, VolumeImport, VolumeRestoreChunk, VolumeRestoreResponse,
    VolumeRestoreStart,
};
use legion_store::{Command, Outcome, Precondition};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::api::{ApiService, Route, check_namespace, forwarded_for};
use crate::authz::Verb;
use crate::raft_net::connect_lazy;
use crate::session::Attachment;

/// How long a client has to send its `start`.
const START_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct VolumesService {
    pub(crate) api: ApiService,
}

type Chunks = ReceiverStream<Result<BackupChunk, Status>>;

impl VolumesService {
    /// The leader, for a follower to forward to.
    fn leader<T>(
        &self,
        req: &Request<T>,
    ) -> Result<Option<VolumesClient<tonic::transport::Channel>>, Status> {
        Ok(match self.api.route(req)? {
            Route::Local => None,
            Route::Leader(_, addr) => Some(VolumesClient::new(connect_lazy(
                &addr,
                self.api.peer_tls.as_ref(),
            ))),
        })
    }

    /// Offers `node`'s agent a session for the Volume.
    async fn attach(
        &self,
        node: &str,
        namespace: &str,
        action: Action,
    ) -> Result<Attachment, Status> {
        let offer = SessionOffer {
            id: legion_pki::random_secret(),
            open: Some(SessionOpen {
                namespace: namespace.to_owned(),
                action: Some(action),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut closing = self.api.closing.clone();
        tokio::select! {
            a = self.api.sessions.offer(node, offer) => a,
            _ = closing.wait_for(|c| *c) => Err(Status::unavailable("the manager is shutting down")),
        }
    }

    /// A Volume whose data a backup or restore may take: one of ZFS's.
    fn volume(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(Object, VolumeSpec, VolumeStatus)>, Status> {
        let Some(o) = self.api.store.get(key).map_err(internal)? else {
            return Ok(None);
        };
        let spec: VolumeSpec = legion_api::parse(&o.spec).map_err(internal)?;
        let status: VolumeStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        Ok(Some((o, spec, status)))
    }
}

#[tonic::async_trait]
impl Volumes for VolumesService {
    type BackupStream = Chunks;

    async fn backup(&self, req: Request<VolumeBackupRequest>) -> Result<Response<Chunks>, Status> {
        let audit = self.api.audit_point(
            &req,
            "volume-backup",
            crate::audit::target("Volume", &req.get_ref().namespace, &req.get_ref().name),
        );
        let result = async {
            let caller = self.api.caller(&req)?;
            if let Some(mut leader) = self.leader(&req)? {
                let mut chunks = leader
                    .backup(forwarded_for(req.into_inner(), &caller)?)
                    .await?
                    .into_inner();
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    while let Some(chunk) = chunks.next().await {
                        let last = chunk.is_err();
                        if tx.send(chunk).await.is_err() || last {
                            return;
                        }
                    }
                });
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
            let VolumeBackupRequest { namespace, name } = req.into_inner();
            let namespace = if namespace.is_empty() {
                "default".to_owned()
            } else {
                namespace
            };
            check_namespace(&namespace)?;
            self.api.consistent().await?;
            // Its data may be anything its tasks keep: as much as exec gives.
            caller.check(&self.api.store, Verb::Write, Kind::Volume, &namespace)?;
            let key = ObjectKey::new(Kind::Volume, &namespace, &name);
            let (o, spec, status) = self.volume(&key)?.ok_or_else(|| {
                Status::not_found(format!("no volume {name} in namespace {namespace}"))
            })?;
            if spec.driver != VolumeDriver::Zfs {
                return Err(Status::failed_precondition(format!(
                    "volume {name} is {}: its data is backed up where it is kept",
                    spec.driver.as_str()
                )));
            }
            if status.node.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "volume {name} has no data yet: no task has used it"
                )));
            }
            let header = VolumeBackupHeader {
                format: VolumeBackupHeader::FORMAT,
                namespace: namespace.clone(),
                name: name.clone(),
                labels: o.metadata.labels.clone(),
                spec,
                node: status.node.clone(),
                taken: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs()),
                consistency: VolumeBackupHeader::CRASH_CONSISTENT.to_owned(),
            };
            let header = serde_json::to_vec(&header).map_err(internal)?;
            let Attachment { mut from_agent, .. } = self
                .attach(
                    &status.node,
                    &namespace,
                    Action::VolumeExport(VolumeExport {
                        volume: name.clone(),
                    }),
                )
                .await?;
            tracing::info!(volume = %key, node = %status.node, "backing up a volume");
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                if tx.send(Ok(BackupChunk { data: header })).await.is_err() {
                    return;
                }
                let end = loop {
                    let data = match from_agent.next().await {
                        Some(Ok(SessionFrame {
                            frame: Some(Frame::Stdout(data)),
                        })) => data,
                        Some(Ok(SessionFrame {
                            frame: Some(Frame::ExitCode(0)),
                        })) => return,
                        Some(Ok(SessionFrame {
                            frame: Some(Frame::Error(e)),
                        })) => {
                            break Status::unavailable(format!("the node failed: {}", e.message));
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => break e,
                        None => break Status::unavailable("the node's stream ended early"),
                    };
                    if tx.send(Ok(BackupChunk { data })).await.is_err() {
                        return;
                    }
                };
                let _ = tx.send(Err(end)).await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }
        .await;
        self.api.audit_end(audit, result)
    }

    async fn restore(
        &self,
        req: Request<Streaming<VolumeRestoreChunk>>,
    ) -> Result<Response<VolumeRestoreResponse>, Status> {
        let audit = self.api.audit_point(
            &req,
            "volume-restore",
            crate::audit::target("Volume", "", ""),
        );
        let result = async {
        let caller = self.api.caller(&req)?;
        let leader = self.leader(&req)?;
        let mut from_user = req.into_inner();
        let start = match tokio::time::timeout(START_TIMEOUT, from_user.next()).await {
            Ok(Some(Ok(VolumeRestoreChunk {
                part: Some(Part::Start(s)),
            }))) => s,
            Ok(Some(Err(e))) => return Err(e),
            _ => return Err(Status::invalid_argument("a restore starts with `start`")),
        };
        if let Some(mut leader) = leader {
            let first = VolumeRestoreChunk {
                part: Some(Part::Start(start)),
            };
            let rest = from_user.map_while(Result::ok);
            let all = tokio_stream::once(first).chain(rest);
            return leader.restore(forwarded_for(all, &caller)?).await;
        }
        let VolumeRestoreStart {
            namespace,
            name,
            node,
            header_json,
            replace,
        } = start;
        let header: VolumeBackupHeader = serde_json::from_str(&header_json)
            .map_err(|e| Status::invalid_argument(format!("the backup's header: {e}")))?;
        if header.format != VolumeBackupHeader::FORMAT {
            return Err(Status::invalid_argument(format!(
                "the backup is of format {}; this build reads {}",
                header.format,
                VolumeBackupHeader::FORMAT
            )));
        }
        let namespace = if namespace.is_empty() {
            header.namespace.clone()
        } else {
            namespace
        };
        let name = if name.is_empty() {
            header.name.clone()
        } else {
            name
        };
        check_namespace(&namespace)?;
        self.api.consistent().await?;
        caller.check(&self.api.store, Verb::Write, Kind::Volume, &namespace)?;
        let key = ObjectKey::new(Kind::Volume, &namespace, &name);
        // Restored into, the Volume lost with its node: its version then,
        // which the write after the data is in must still find.
        let into = match (self.volume(&key)?, replace) {
            (None, false) => None,
            (None, true) => {
                return Err(Status::not_found(format!(
                    "volume {name} is not in namespace {namespace}: restore it without --replace"
                )));
            }
            (Some((_, _, status)), false) => {
                let hint = if status.lost.is_empty() {
                    String::new()
                } else {
                    format!(
                        "; its data was lost with node {}: --replace restores into it",
                        status.lost
                    )
                };
                return Err(Status::already_exists(format!(
                    "volume {name} exists in namespace {namespace}: delete it, or restore under another name{hint}"
                )));
            }
            (Some((o, _, status)), true) if !status.lost.is_empty() => Some(o),
            (Some((_, _, status)), true) => {
                return Err(Status::failed_precondition(format!(
                    "volume {name}'s data is on node {}: --replace restores only into a Volume lost with its node",
                    if status.node.is_empty() { "none yet" } else { &status.node }
                )));
            }
        };
        let ready = self
            .api
            .store
            .get(&ObjectKey::new(Kind::Node, "", &node))
            .map_err(internal)?
            .and_then(|n| serde_json::from_value::<legion_api::node::NodeStatus>(n.status).ok())
            .is_some_and(|s| s.state == legion_api::node::NodeState::Ready);
        if !ready {
            return Err(Status::failed_precondition(format!(
                "node {node} is not ready"
            )));
        }

        let Attachment {
            mut from_agent,
            to_agent,
        } = self
            .attach(
                &node,
                &namespace,
                Action::VolumeImport(VolumeImport {
                    volume: name.clone(),
                }),
            )
            .await?;
        tracing::info!(volume = %key, node, "restoring a volume");
        let send = |f| SessionFrame { frame: Some(f) };
        let fed = async {
            while let Some(chunk) = from_user.next().await {
                match chunk {
                    Ok(VolumeRestoreChunk {
                        part: Some(Part::Data(data)),
                    }) => {
                        if to_agent.send(Ok(send(Frame::Input(data)))).await.is_err() {
                            return Err(Status::unavailable("the node stopped taking the data"));
                        }
                    }
                    Ok(_) => return Err(Status::invalid_argument("only data follows `start`")),
                    Err(e) => return Err(e),
                }
            }
            let _ = to_agent
                .send(Ok(send(Frame::InputClosed(InputClosed {}))))
                .await;
            Ok(())
        };
        if let Err(e) = fed.await {
            let _ = to_agent
                .send(Ok(send(Frame::Error(SessionError {
                    code: e.code() as i32,
                    message: e.message().to_owned(),
                }))))
                .await;
            return Err(e);
        }
        loop {
            match from_agent.next().await {
                Some(Ok(SessionFrame {
                    frame: Some(Frame::ExitCode(0)),
                })) => break,
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Error(e)),
                })) => {
                    return Err(Status::failed_precondition(format!(
                        "node {node}: {}",
                        e.message
                    )));
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(Status::unavailable(format!(
                        "node {node}'s stream ended early"
                    )));
                }
            }
        }

        // The data is there: the Volume now, on that node. Asked for no
        // node of its own, it stays where it was restored. Restored into
        // a lost one, that one is kept as it is (its labels name its
        // ordinal), on its new node.
        let (object, precondition) = match into {
            Some(mut o) => {
                let mut spec: VolumeSpec = legion_api::parse(&o.spec).map_err(internal)?;
                spec.node = None;
                o.spec = serde_json::to_value(&spec).map_err(internal)?;
                o.status = serde_json::to_value(VolumeStatus {
                    node: node.clone(),
                    ..Default::default()
                })
                .map_err(internal)?;
                let version = o.metadata.version;
                (o, Precondition::Version(version))
            }
            None => {
                let mut spec = header.spec;
                spec.node = None;
                let mut object = Object::new(
                    Kind::Volume,
                    &namespace,
                    &name,
                    serde_json::to_value(&spec).map_err(internal)?,
                );
                object.metadata.labels = header.labels;
                object.status = serde_json::to_value(VolumeStatus {
                    node: node.clone(),
                    ..Default::default()
                })
                .map_err(internal)?;
                (object, Precondition::MustNotExist)
            }
        };
        match self
            .api
            .write(Command::Put {
                object,
                precondition,
            })
            .await?
        {
            Outcome::Written(_) => {
                tracing::info!(volume = %key, node, replace, "volume restored");
                Ok(Response::new(VolumeRestoreResponse {}))
            }
            // Made meanwhile by someone else. The data restored stays: that
            // Volume has its dataset's name, and a task of its may already
            // use it on that node.
            _ => Err(Status::already_exists(format!(
                "volume {name} was made in namespace {namespace} as it was restored; the data restored on {node} is under its name"
            ))),
        }
    }
        .await;
        self.api.audit_end(audit, result)
    }
}

/// Marks the Volumes whose data was on `node` lost (`status.lost`), as
/// the node is removed or a node joins under its name: no task uses them
/// again until they are restored into or deleted. Local data only: what
/// is kept apart from nodes (iSCSI, NFS, plugins) is not on the node.
pub(crate) async fn mark_lost(api: &ApiService, node: &str) -> Result<(), Status> {
    for _ in 0..5 {
        let mut conflicted = false;
        let volumes = api
            .store
            .list(Kind::Volume, None)
            .map_err(internal)?
            .objects;
        for mut o in volumes {
            let spec: VolumeSpec = match legion_api::parse(&o.spec) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut status: VolumeStatus =
                serde_json::from_value(o.status.clone()).unwrap_or_default();
            if spec.access() != legion_api::volume::Access::Local || !status.lost.is_empty() {
                continue;
            }
            let held = status.node == node
                || (status.node.is_empty() && spec.node.as_deref() == Some(node));
            // A full copy made elsewhere carries on: the move's planner takes
            // it for the Volume once the node is gone.
            let copied = status.migration.as_ref().is_some_and(|m| {
                m.to != node
                    && matches!(
                        m.phase,
                        legion_api::volume::MigrationPhase::Copied
                            | legion_api::volume::MigrationPhase::Finishing
                    )
            });
            let held = held && !copied;
            if !held {
                // A copy it moved away from is gone with the node too.
                if status.leftovers.iter().any(|n| n == node) {
                    status.leftovers.retain(|n| n != node);
                } else {
                    continue;
                }
            } else {
                status.lost = node.to_owned();
                status.node.clear();
                status.migration = None;
                status.leftovers.retain(|n| n != node);
            }
            o.status = serde_json::to_value(&status).map_err(internal)?;
            let version = o.metadata.version;
            let key = o.key();
            match api
                .write(Command::Put {
                    object: o,
                    precondition: Precondition::Version(version),
                })
                .await?
            {
                Outcome::Written(_) if held => {
                    tracing::warn!(volume = %key, node, "volume lost with its node")
                }
                Outcome::Written(_) => {}
                Outcome::Conflict { .. } => conflicted = true,
                other => return Err(Status::internal(format!("unexpected {other:?}"))),
            }
        }
        if !conflicted {
            return Ok(());
        }
    }
    Err(Status::aborted(format!(
        "the Volumes of node {node} kept changing; retry"
    )))
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
