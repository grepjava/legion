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

    async fn restore(
        &self,
        req: Request<Streaming<VolumeRestoreChunk>>,
    ) -> Result<Response<VolumeRestoreResponse>, Status> {
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
        if self.volume(&key)?.is_some() {
            return Err(Status::already_exists(format!(
                "volume {name} exists in namespace {namespace}: delete it, or restore under another name"
            )));
        }
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
        // node of its own, it stays where it was restored.
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
        match self
            .api
            .write(Command::Put {
                object,
                precondition: Precondition::MustNotExist,
            })
            .await?
        {
            Outcome::Written(_) => {
                tracing::info!(volume = %key, node, "volume restored");
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
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
