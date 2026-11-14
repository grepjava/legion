//! Builds (CONSOLE.md phase 5), run by the leader. A Pending Build goes to
//! a ready builder node (labelled `legion.builder=true`, not draining) with
//! no build running, one at a time each; the leader offers that node's
//! agent a session with the `build` action, as it does a Volume's export,
//! keeps the output as the Build's log, and sets its status from how the
//! session ends. Once pushed, a Build naming a Service sets that
//! container's image to the reference pinned to the digest pushed; the
//! rollout is the Service's own.
//!
//! A Build runs once. One found Running by a leader that did not start it
//! (the leader changed, or this one restarted) failed: its session ended
//! with the old leader.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use legion_api::build::{BUILDER_LABEL, BuildPhase, BuildSpec, BuildStatus, kept_log};
use legion_api::data::DataSpec;
use legion_api::node::{Availability, NodeSpec, NodeState, NodeStatus};
use legion_api::service::ServiceSpec;
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{BuildOptions, SessionFrame, SessionOffer, SessionOpen};
use legion_store::{Command, Outcome, Precondition};
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

use crate::api::ApiService;

/// How much of a build's log the leader keeps while it runs, the end of it.
const LOG_HELD: usize = 4 << 20;
/// How long a finished build's log stays in memory beside its status's.
const LOG_LINGER: Duration = Duration::from_secs(600);
/// A pass at least this often, for nodes coming and going.
const PERIOD: Duration = Duration::from_secs(5);
const DEBOUNCE: Duration = Duration::from_millis(100);

/// The logs of the builds this leader runs, followed as they are written.
#[derive(Default)]
pub(crate) struct Logs {
    inner: Mutex<HashMap<ObjectKey, Log>>,
}

struct Log {
    /// The end of the output.
    text: Vec<u8>,
    /// How many bytes were written in all, of which `text` is the end.
    written: watch::Sender<u64>,
    done: bool,
}

/// Where a reader of a log is: bytes read so far.
pub(crate) struct Reader {
    pub(crate) key: ObjectKey,
    pub(crate) at: u64,
    pub(crate) changed: watch::Receiver<u64>,
}

impl Logs {
    fn start(&self, key: &ObjectKey) {
        let (written, _) = watch::channel(0);
        self.inner.lock().unwrap().insert(
            key.clone(),
            Log {
                text: Vec::new(),
                written,
                done: false,
            },
        );
    }

    fn append(&self, key: &ObjectKey, data: &[u8]) {
        let mut all = self.inner.lock().unwrap();
        if let Some(log) = all.get_mut(key) {
            log.text.extend_from_slice(data);
            if log.text.len() > LOG_HELD {
                let cut = log.text.len() - LOG_HELD;
                log.text.drain(..cut);
            }
            log.written.send_modify(|w| *w += data.len() as u64);
        }
    }

    fn end(&self, key: &ObjectKey) {
        let mut all = self.inner.lock().unwrap();
        if let Some(log) = all.get_mut(key) {
            log.done = true;
            log.written.send_modify(|_| {});
        }
    }

    fn forget(&self, key: &ObjectKey) {
        self.inner.lock().unwrap().remove(key);
    }

    fn text(&self, key: &ObjectKey) -> Option<String> {
        let all = self.inner.lock().unwrap();
        all.get(key)
            .map(|l| String::from_utf8_lossy(&l.text).into_owned())
    }

    /// A reader from the start of what is held, if this leader holds the
    /// build's log.
    pub(crate) fn reader(&self, key: &ObjectKey) -> Option<Reader> {
        let all = self.inner.lock().unwrap();
        let log = all.get(key)?;
        let written = *log.written.borrow();
        Some(Reader {
            key: key.clone(),
            at: written - log.text.len() as u64,
            changed: log.written.subscribe(),
        })
    }

    /// What was written since `r` last read, and whether the build ended.
    pub(crate) fn read(&self, r: &mut Reader) -> Option<(Vec<u8>, bool)> {
        let all = self.inner.lock().unwrap();
        let log = all.get(&r.key)?;
        let written = *log.written.borrow();
        let first = written - log.text.len() as u64;
        let from = r.at.max(first);
        let data = log.text[(from - first) as usize..].to_vec();
        r.at = written;
        Some((data, log.done))
    }
}

/// The sessions of the builds running, outside any one term of leading:
/// stopped, and waited for, when the manager closes, as they hold the
/// store.
type Sessions = Arc<Mutex<JoinSet<()>>>;

/// Runs the builds while this manager leads.
pub(crate) async fn run(api: ApiService, plain_http: Vec<String>, closing: watch::Receiver<bool>) {
    let store = api.store.clone();
    let id = api.id;
    let sessions: Sessions = Default::default();
    crate::controller::while_leading(&store, id, closing, "builds", || {
        lead(api.clone(), plain_http.clone(), sessions.clone())
    })
    .await;
    drop(api);
    let mut left = std::mem::take(&mut *sessions.lock().unwrap());
    left.shutdown().await;
}

async fn lead(api: ApiService, plain_http: Vec<String>, sessions: Sessions) {
    // A term of leading before this one may have left builds running:
    // their Builds are failed below, as this term did not start them.
    sessions.lock().unwrap().abort_all();
    // The builds this leader runs, each until its session ends.
    let running: Arc<Mutex<BTreeMap<ObjectKey, String>>> = Default::default();
    let ended = Arc::new(Notify::new());
    loop {
        let from = match api.store.applied_version() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("reading the store: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if let Err(e) = pass(&api, &plain_http, &running, &ended, &sessions).await {
            tracing::warn!("builds: {e}");
        }
        while sessions.lock().unwrap().try_join_next().is_some() {}
        let watched = (
            api.store.watch(from, Some(Kind::Build)),
            api.store.watch(from, Some(Kind::Node)),
        );
        let (Ok(mut builds), Ok(mut nodes)) = watched else {
            tokio::time::sleep(DEBOUNCE).await;
            continue;
        };
        tokio::select! {
            _ = builds.next() => {}
            _ = nodes.next() => {}
            _ = ended.notified() => {}
            _ = tokio::time::sleep(PERIOD) => {}
        }
        tokio::time::sleep(DEBOUNCE).await;
    }
}

async fn pass(
    api: &ApiService,
    plain_http: &[String],
    running: &Arc<Mutex<BTreeMap<ObjectKey, String>>>,
    ended: &Arc<Notify>,
    sessions: &Sessions,
) -> Result<(), String> {
    let builds = api
        .store
        .list(Kind::Build, None)
        .map_err(|e| e.to_string())?
        .objects;
    let nodes = api
        .store
        .list(Kind::Node, None)
        .map_err(|e| e.to_string())?
        .objects;
    let mut builders: Vec<String> = nodes
        .iter()
        .filter(|n| n.metadata.labels.get(BUILDER_LABEL).map(String::as_str) == Some("true"))
        .filter(|n| {
            let spec: NodeSpec = legion_api::parse(&n.spec).unwrap_or_default();
            let status: NodeStatus = serde_json::from_value(n.status.clone()).unwrap_or_default();
            status.state == NodeState::Ready && spec.availability != Availability::Drain
        })
        .map(|n| n.metadata.name.clone())
        .collect();
    builders.sort();
    let any_builder = nodes
        .iter()
        .any(|n| n.metadata.labels.get(BUILDER_LABEL).map(String::as_str) == Some("true"));
    for b in &builds {
        let key = ObjectKey::new(Kind::Build, &b.metadata.namespace, &b.metadata.name);
        let status: BuildStatus = serde_json::from_value(b.status.clone()).unwrap_or_default();
        match status.phase {
            BuildPhase::Running if !running.lock().unwrap().contains_key(&key) => {
                set_status(api, &key, |s| {
                    finish(
                        s,
                        BuildPhase::Failed,
                        "it was running when the leader changed; apply it again as a new Build",
                    )
                })
                .await?;
            }
            BuildPhase::Pending => {
                let busy: Vec<String> = running.lock().unwrap().values().cloned().collect();
                let Some(node) = builders.iter().find(|n| !busy.contains(n)).cloned() else {
                    let why = if any_builder {
                        "waiting for a builder node: every one is busy, or not ready"
                    } else {
                        "waiting for a builder node: none is labelled legion.builder=true"
                    };
                    if status.message != why {
                        set_status(api, &key, |s| s.message = why.to_owned()).await?;
                    }
                    continue;
                };
                let spec: BuildSpec = match legion_api::parse(&b.spec) {
                    Ok(s) => s,
                    Err(e) => {
                        set_status(api, &key, |s| {
                            finish(s, BuildPhase::Failed, &format!("its spec: {e}"))
                        })
                        .await?;
                        continue;
                    }
                };
                let opts = match options(api, &key, &spec, plain_http) {
                    Ok(o) => o,
                    Err(why) => {
                        set_status(api, &key, |s| finish(s, BuildPhase::Failed, &why)).await?;
                        continue;
                    }
                };
                set_status(api, &key, |s| {
                    s.phase = BuildPhase::Running;
                    s.node = node.clone();
                    s.message = String::new();
                    s.started_ms = Some(crate::console::now_ms());
                })
                .await?;
                running.lock().unwrap().insert(key.clone(), node.clone());
                api.builds.start(&key);
                tracing::info!(build = %key, %node, "building");
                let (api, running, ended) = (api.clone(), running.clone(), ended.clone());
                sessions.lock().unwrap().spawn(async move {
                    let outcome = build(&api, &key, &node, opts).await;
                    let log = api.builds.text(&key).unwrap_or_default();
                    api.builds.end(&key);
                    let written = match outcome {
                        Ok((digest, pinned)) => {
                            let rolled = match &spec.rollout {
                                Some(r) => rollout(&api, &key, r, &pinned).await,
                                None => Ok(()),
                            };
                            set_status(&api, &key, |s| {
                                s.digest = digest.clone();
                                s.pinned = pinned.clone();
                                s.log = kept_log(&log);
                                match &rolled {
                                    Ok(()) => finish(s, BuildPhase::Succeeded, ""),
                                    Err(e) => finish(
                                        s,
                                        BuildPhase::Failed,
                                        &format!("pushed, but not rolled out: {e}"),
                                    ),
                                }
                            })
                            .await
                        }
                        Err(why) => {
                            set_status(&api, &key, |s| {
                                s.log = kept_log(&log);
                                finish(s, BuildPhase::Failed, &why)
                            })
                            .await
                        }
                    };
                    if let Err(e) = written {
                        tracing::warn!(build = %key, "recording the build's end: {e}");
                    }
                    running.lock().unwrap().remove(&key);
                    ended.notify_one();
                    // The log lingers; the store is let go.
                    let logs = api.builds.clone();
                    drop(api);
                    tokio::time::sleep(LOG_LINGER).await;
                    logs.forget(&key);
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn finish(s: &mut BuildStatus, phase: BuildPhase, why: &str) {
    s.phase = phase;
    s.message = why.to_owned();
    s.finished_ms = Some(crate::console::now_ms());
}

/// What the node is asked: the source, the image, the login.
fn options(
    api: &ApiService,
    key: &ObjectKey,
    spec: &BuildSpec,
    plain_http: &[String],
) -> Result<BuildOptions, String> {
    let auth = match &spec.push_secret {
        Some(name) => {
            let secret = api
                .store
                .get(&ObjectKey::new(Kind::Secret, &key.namespace, name))
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("no Secret {name} in namespace {}", key.namespace))?;
            let data: DataSpec = legion_api::parse(&secret.spec).map_err(|e| e.to_string())?;
            data.bytes()
                .map_err(|e| format!("Secret {name}: {}", e.message))?
        }
        None => Vec::new(),
    };
    let git = spec.source.git.clone();
    Ok(BuildOptions {
        git_url: git.as_ref().map(|g| g.url.clone()).unwrap_or_default(),
        git_ref: git
            .as_ref()
            .and_then(|g| g.r#ref.clone())
            .unwrap_or_default(),
        git_path: git.and_then(|g| g.path).unwrap_or_default(),
        containerfile: spec.source.containerfile.clone().unwrap_or_default(),
        image: spec.image.clone(),
        auth,
        plain_http: plain_http.to_vec(),
    })
}

/// Runs the build on `node`: the digest pushed and the pinned reference,
/// or why it failed.
async fn build(
    api: &ApiService,
    key: &ObjectKey,
    node: &str,
    opts: BuildOptions,
) -> Result<(String, String), String> {
    let offer = SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: key.namespace.clone(),
            action: Some(Action::Build(opts)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let attached = api
        .sessions
        .offer(node, offer)
        .await
        .map_err(|e| format!("node {node} did not take the build: {}", e.message()))?;
    // Held until the build ends: the agent stops it when this end closes.
    let _to_agent = attached.to_agent;
    let mut from_agent = attached.from_agent;
    let mut built = None;
    loop {
        match from_agent.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Stdout(data) | Frame::Stderr(data)),
            })) => api.builds.append(key, &data),
            Some(Ok(SessionFrame {
                frame: Some(Frame::Built(b)),
            })) => built = Some((b.digest, b.pinned)),
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => {
                return built.ok_or_else(|| "the node said nothing of what it pushed".to_owned());
            }
            Some(Ok(SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => return Err(e.message),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(format!("node {node}: {}", e.message())),
            None => return Err(format!("node {node}'s stream ended before the build did")),
        }
    }
}

/// Sets the Service's container image to `pinned`.
async fn rollout(
    api: &ApiService,
    build: &ObjectKey,
    r: &legion_api::build::BuildRollout,
    pinned: &str,
) -> Result<(), String> {
    let key = ObjectKey::new(Kind::Service, &build.namespace, &r.service);
    for _ in 0..3 {
        let mut o = api
            .store
            .get(&key)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no Service {} in namespace {}", r.service, build.namespace))?;
        let mut spec: ServiceSpec = legion_api::parse(&o.spec).map_err(|e| e.to_string())?;
        let containers = &mut spec.pod.containers;
        let c = match &r.container {
            Some(name) => containers
                .iter_mut()
                .find(|c| &c.name == name)
                .ok_or_else(|| format!("Service {} has no container {name}", r.service))?,
            None if containers.len() == 1 => &mut containers[0],
            None => {
                return Err(format!(
                    "Service {} has several containers: the Build must name one",
                    r.service
                ));
            }
        };
        c.image = pinned.to_owned();
        o.spec = serde_json::to_value(&spec).map_err(|e| e.to_string())?;
        let version = o.metadata.version;
        legion_api::admit::admit(&mut o).map_err(|e| e.to_string())?;
        match api
            .write(Command::Put {
                object: o,
                precondition: Precondition::Version(version),
            })
            .await
            .map_err(|e| e.message().to_owned())?
        {
            Outcome::Written(_) => {
                tracing::info!(build = %build, service = %key, image = %pinned, "rolled out");
                return Ok(());
            }
            Outcome::Conflict { .. } => continue,
            other => return Err(format!("{other:?}")),
        }
    }
    Err("the Service kept changing".to_owned())
}

/// Changes the Build's status with `change`, as stored now.
async fn set_status(
    api: &ApiService,
    key: &ObjectKey,
    change: impl Fn(&mut BuildStatus),
) -> Result<(), String> {
    for _ in 0..3 {
        let Some(mut o): Option<Object> = api.store.get(key).map_err(|e| e.to_string())? else {
            return Ok(());
        };
        let mut status: BuildStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        let before = status.clone();
        change(&mut status);
        if status == before {
            return Ok(());
        }
        o.status = serde_json::to_value(&status).expect("serializes");
        let version = o.metadata.version;
        match api
            .write(Command::Put {
                object: o,
                precondition: Precondition::Version(version),
            })
            .await
            .map_err(|e| e.message().to_owned())?
        {
            Outcome::Written(_) => return Ok(()),
            Outcome::Conflict { .. } => continue,
            other => return Err(format!("{key}: {other:?}")),
        }
    }
    Err(format!("{key} kept changing"))
}

/// A user's view of a build's log: what this leader holds, else what the
/// status kept.
pub(crate) fn status_log(o: &Object) -> String {
    let s: BuildStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
    s.log
}
