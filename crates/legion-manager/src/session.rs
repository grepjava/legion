//! Sessions with tasks' containers (DESIGN.md §4.2): `logs`, `exec` and
//! `port-forward`. Agents keep a `Sessions` stream open with every manager;
//! a user opens a session at any of them, which checks their rights,
//! offers it down the stream of the task's node, and joins the user's
//! stream to the one the agent opens with `Attach`. Nothing is forwarded
//! between managers, and nodes need no port of their own.
//!
//! Logs take `view` in the task's namespace; exec and port-forward, which
//! act inside the pod, take `edit`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::task::{TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::pods_server::Pods;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{InputClosed, SessionFrame, SessionOffer, SessionOpen};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::api::{ApiService, check_namespace};
use crate::authz::Verb;

/// How long an agent has to attach to a session offered to it.
const ATTACH_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a client has to send its `open` frame.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) type Frames = ReceiverStream<Result<SessionFrame, Status>>;

/// The agents connected to this manager and the sessions offered to them.
#[derive(Default)]
pub(crate) struct Hub {
    inner: Mutex<HubState>,
}

#[derive(Default)]
struct HubState {
    /// Each node's `Sessions` streams, newest last; a reconnecting agent
    /// may briefly have two.
    agents: HashMap<String, Vec<mpsc::Sender<Result<SessionOffer, Status>>>>,
    /// Offered sessions waiting for their agent to attach, by id.
    pending: HashMap<String, Pending>,
}

struct Pending {
    node: String,
    attached: oneshot::Sender<Attachment>,
}

/// An agent's end of a session.
pub(crate) struct Attachment {
    pub(crate) from_agent: Streaming<SessionFrame>,
    pub(crate) to_agent: mpsc::Sender<Result<SessionFrame, Status>>,
}

impl Hub {
    /// A new `Sessions` stream for `node`.
    pub(crate) fn follow(&self, node: &str) -> ReceiverStream<Result<SessionOffer, Status>> {
        let (tx, rx) = mpsc::channel(16);
        let mut s = self.inner.lock().unwrap();
        let streams = s.agents.entry(node.to_owned()).or_default();
        streams.retain(|t| !t.is_closed());
        streams.push(tx);
        ReceiverStream::new(rx)
    }

    /// Offers a session to `node`'s agent; resolves once it attaches.
    pub(crate) async fn offer(
        self: &Arc<Self>,
        node: &str,
        offer: SessionOffer,
    ) -> Result<Attachment, Status> {
        let id = offer.id.clone();
        let (attached, wait) = oneshot::channel();
        let agent = {
            let mut s = self.inner.lock().unwrap();
            let agent = s.agents.get_mut(node).and_then(|streams| {
                streams.retain(|t| !t.is_closed());
                streams.last().cloned()
            });
            if agent.is_some() {
                s.pending.insert(
                    id.clone(),
                    Pending {
                        node: node.to_owned(),
                        attached,
                    },
                );
            }
            agent
        };
        let agent = agent.ok_or_else(|| {
            Status::unavailable(format!(
                "the agent of node {node} is not connected to this manager"
            ))
        })?;
        let forget = |hub: &Hub| hub.inner.lock().unwrap().pending.remove(&id);
        if agent.send(Ok(offer)).await.is_err() {
            forget(self);
            return Err(Status::unavailable(format!(
                "the agent of node {node} went away"
            )));
        }
        match tokio::time::timeout(ATTACH_TIMEOUT, wait).await {
            Ok(Ok(a)) => Ok(a),
            _ => {
                forget(self);
                Err(Status::unavailable(format!(
                    "the agent of node {node} did not take the session"
                )))
            }
        }
    }

    /// An agent attaching to session `id`; `node` is the node it acts for,
    /// or `None` for a manager or without TLS, which may attach to any.
    pub(crate) fn attach(
        &self,
        node: Option<&str>,
        id: &str,
        from_agent: Streaming<SessionFrame>,
    ) -> Result<Frames, Status> {
        let mut s = self.inner.lock().unwrap();
        let unknown = || Status::not_found(format!("no session {id} is waiting"));
        match s.pending.get(id) {
            Some(p) if node.is_none_or(|n| n == p.node) => {}
            _ => return Err(unknown()),
        }
        let pending = s.pending.remove(id).expect("checked");
        drop(s);
        let (to_agent, rx) = mpsc::channel(16);
        pending
            .attached
            .send(Attachment {
                from_agent,
                to_agent,
            })
            .map_err(|_| unknown())?;
        Ok(ReceiverStream::new(rx))
    }
}

#[derive(Clone)]
pub(crate) struct PodsService {
    pub(crate) api: ApiService,
}

#[tonic::async_trait]
impl Pods for PodsService {
    type SessionStream = Frames;

    async fn session(
        &self,
        req: Request<Streaming<SessionFrame>>,
    ) -> Result<Response<Frames>, Status> {
        let caller = self.api.caller(&req)?;
        let mut from_user = req.into_inner();
        let open = match tokio::time::timeout(OPEN_TIMEOUT, from_user.next()).await {
            Ok(Some(Ok(SessionFrame {
                frame: Some(Frame::Open(open)),
            }))) => open,
            Ok(Some(Err(status))) => return Err(status),
            _ => return Err(Status::invalid_argument("a session starts with `open`")),
        };
        let mut open = open;
        if open.namespace.is_empty() {
            open.namespace = "default".into();
        }
        check_namespace(&open.namespace)?;
        let verb = match open.action {
            Some(Action::Logs(_)) => Verb::Read,
            Some(Action::Exec(_) | Action::Port(_)) => Verb::Write,
            Some(Action::VolumeSend(_) | Action::VolumeReceive(_) | Action::VolumeDrop(_)) => {
                return Err(Status::invalid_argument(
                    "volumes move between nodes only when the managers move them",
                ));
            }
            None => return Err(Status::invalid_argument("the session needs an action")),
        };
        caller.check(&self.api.store, verb, Kind::Task, &open.namespace)?;
        let task = self.task(&open, verb == Verb::Read)?;
        let spec: TaskSpec = legion_api::parse(&task.spec).map_err(internal)?;
        let status: TaskStatus = serde_json::from_value(task.status.clone()).unwrap_or_default();
        open.task = task.metadata.name.clone();
        open.container = container(&spec, &open.container, &open.task)?;
        if spec.node.is_empty() || status.state == TaskState::Pending {
            return Err(Status::failed_precondition(format!(
                "task {} is not on a node yet",
                open.task
            )));
        }
        let interactive = !matches!(open.action, Some(Action::Logs(_)));
        if interactive && status.state != TaskState::Running {
            return Err(Status::failed_precondition(format!(
                "task {} is {}, not running",
                open.task,
                serde_json::to_value(status.state)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default()
            )));
        }
        let offer = SessionOffer {
            id: legion_pki::random_secret(),
            open: Some(open),
            task_uid: task.metadata.uid.clone(),
            pod_ip: status.ip.unwrap_or_default(),
        };
        let attachment = self.api.sessions.offer(&spec.node, offer).await?;
        let (to_user, rx) = mpsc::channel(16);
        tokio::spawn(relay(
            from_user,
            to_user,
            attachment,
            self.api.closing.clone(),
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl PodsService {
    /// The task `open` names, or a running task of the Service, or else the
    /// Job, it names. With `finished`, a Service or Job with none running
    /// gives the task that last ended on a node, whose output the node
    /// keeps.
    fn task(&self, open: &SessionOpen, finished: bool) -> Result<Object, Status> {
        let store = &self.api.store;
        let key = ObjectKey::new(Kind::Task, &open.namespace, &open.task);
        if let Some(t) = store.get(&key).map_err(internal)? {
            return Ok(t);
        }
        let mut tasks: Vec<(Object, bool, TaskState, bool)> = store
            .list(Kind::Task, Some(&open.namespace))
            .map_err(internal)?
            .objects
            .into_iter()
            .filter_map(|t| {
                let spec = legion_api::parse::<TaskSpec>(&t.spec).ok()?;
                (spec.service == open.task).then_some(())?;
                let state = serde_json::from_value::<TaskStatus>(t.status.clone())
                    .unwrap_or_default()
                    .state;
                let on_node = !spec.node.is_empty();
                Some((t, spec.job, state, on_node))
            })
            .collect();
        let exists = |kind| {
            let key = ObjectKey::new(kind, &open.namespace, &open.task);
            store.get(&key).map(|o| o.is_some()).map_err(internal)
        };
        let job = if exists(Kind::Service)? {
            false
        } else if exists(Kind::Job)? {
            true
        } else if let Some((_, job, ..)) = tasks.first() {
            *job
        } else {
            return Err(Status::not_found(format!(
                "no task, service or job {} in namespace {}",
                open.task, open.namespace
            )));
        };
        tasks.retain(|(_, j, ..)| *j == job);
        let running = tasks
            .iter()
            .filter(|(_, _, s, _)| *s == TaskState::Running)
            .min_by(|a, b| a.0.metadata.name.cmp(&b.0.metadata.name));
        // Rejected tasks never ran.
        let ended = || {
            tasks
                .iter()
                .filter(|(_, _, s, on_node)| {
                    *on_node && s.is_terminal() && *s != TaskState::Rejected
                })
                .max_by_key(|(t, ..)| t.metadata.version)
        };
        let found = running.or_else(|| ended().filter(|_| finished));
        found.map(|(t, ..)| t.clone()).ok_or_else(|| {
            let owner = if job { "job" } else { "service" };
            Status::failed_precondition(format!("{owner} {} has no running task", open.task))
        })
    }
}

/// The container `asked` for, which may be left out when the pod has only
/// one.
fn container(spec: &TaskSpec, asked: &str, task: &str) -> Result<String, Status> {
    let pod = &spec.pod;
    let names = || {
        pod.init_containers
            .iter()
            .chain(&pod.containers)
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    if asked.is_empty() {
        return match pod.containers.as_slice() {
            [only] => Ok(only.name.clone()),
            _ => Err(Status::invalid_argument(format!(
                "task {task} has containers {}: name one",
                names()
            ))),
        };
    }
    if pod
        .init_containers
        .iter()
        .chain(&pod.containers)
        .any(|c| c.name == asked)
    {
        Ok(asked.to_owned())
    } else {
        Err(Status::not_found(format!(
            "task {task} has no container {asked}; it has {}",
            names()
        )))
    }
}

/// Carries input from the user to the agent and output back, until either
/// side ends or the manager shuts down. Dropping the agent's streams ends
/// the session there.
async fn relay(
    mut from_user: Streaming<SessionFrame>,
    to_user: mpsc::Sender<Result<SessionFrame, Status>>,
    agent: Attachment,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let Attachment {
        mut from_agent,
        to_agent,
    } = agent;
    let mut user_sending = true;
    let mut shutting_down = false;
    loop {
        tokio::select! {
            frame = from_user.next(), if user_sending => match frame {
                Some(Ok(SessionFrame { frame: Some(f @ (Frame::Input(_) | Frame::InputClosed(_) | Frame::Resize(_))) })) => {
                    if to_agent.send(Ok(SessionFrame { frame: Some(f) })).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {
                    let _ = to_user
                        .send(Err(Status::invalid_argument(
                            "after `open`, a client sends only input and resizes",
                        )))
                        .await;
                    break;
                }
                // The user is gone.
                Some(Err(_)) => break,
                // The user has no more to send, which closes input.
                None => {
                    user_sending = false;
                    let closed = SessionFrame { frame: Some(Frame::InputClosed(InputClosed {})) };
                    if to_agent.send(Ok(closed)).await.is_err() {
                        break;
                    }
                }
            },
            frame = from_agent.next() => match frame {
                Some(Ok(SessionFrame { frame: Some(Frame::Error(e)) })) => {
                    let _ = to_user
                        .send(Err(Status::new(tonic::Code::from_i32(e.code), e.message)))
                        .await;
                    break;
                }
                Some(Ok(f @ SessionFrame { frame: Some(Frame::ExitCode(_)) })) => {
                    let _ = to_user.send(Ok(f)).await;
                    break;
                }
                Some(Ok(f @ SessionFrame { frame: Some(Frame::Stdout(_) | Frame::Stderr(_)) })) => {
                    if to_user.send(Ok(f)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(status)) => {
                    let _ = to_user
                        .send(Err(Status::unavailable(format!(
                            "lost the agent: {}",
                            status.message()
                        ))))
                        .await;
                    break;
                }
                None => break,
            },
            _ = to_user.closed() => break,
            () = shutdown(&mut closing) => {
                shutting_down = true;
                break;
            }
        }
    }
    if shutting_down {
        let _ = to_user
            .send(Err(Status::unavailable("manager shutting down")))
            .await;
    }
}

/// Resolves once the manager starts shutting down. Unlike `wait_for`, its
/// output holds no lock guard, so `select!` loops awaiting in other arms
/// stay `Send`.
pub(crate) async fn shutdown(closing: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = closing.wait_for(|c| *c).await;
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_names() {
        let spec: TaskSpec = serde_json::from_value(serde_json::json!({
            "service": "web", "serviceUid": "u", "serviceGeneration": 1,
            "pod": {
                "initContainers": [{ "name": "setup", "image": "i" }],
                "containers": [{ "name": "app", "image": "i" }],
            },
        }))
        .unwrap();
        assert_eq!(container(&spec, "", "t").unwrap(), "app");
        assert_eq!(container(&spec, "setup", "t").unwrap(), "setup");
        let err = container(&spec, "db", "t").unwrap_err();
        assert_eq!(
            err.message(),
            "task t has no container db; it has setup, app"
        );

        let mut two = spec.clone();
        two.pod.containers.push(two.pod.containers[0].clone());
        two.pod.containers[1].name = "sidecar".into();
        let err = container(&two, "", "t").unwrap_err();
        assert_eq!(
            err.message(),
            "task t has containers setup, app, sidecar: name one"
        );
    }
}
