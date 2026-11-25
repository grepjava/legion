//! The migrator (DESIGN.md §4.3): while this manager leads, it moves the
//! data of Volumes the controller has started migrating. For a migration
//! `copying` it has the old node send a full copy and the new node receive
//! it, then marks it `copied`; for one `finishing` (its tasks stopped), the
//! changes since, then it gives the Volume to the new node and has the old
//! one drop its copy. Both agents attach to this manager's sessions (see
//! [`crate::session`]), which joins them, so nodes need no port of their
//! own. A failed attempt is noted in the migration and tried again, after
//! longer each time it fails again: a node out of space would otherwise
//! be sent the full copy every few seconds.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::volume::{Migration, MigrationPhase, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{
    InputClosed, ResumeToken, SessionError, SessionFrame, SessionOffer, SessionOpen, VolumeReceive,
    VolumeSend,
};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};

use crate::session::Hub;

/// How often the Volumes are looked at.
const PERIOD: Duration = Duration::from_secs(1);
/// How long after a failed attempt the next one starts: then twice as
/// long after each failure in a row, up to [`RETRY_MAX`].
const RETRY: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(300);

/// How long after the `count`th failure in a row the next attempt starts.
pub(crate) fn backoff(count: u32) -> Duration {
    RETRY
        .saturating_mul(2u32.saturating_pow(count.saturating_sub(1)))
        .min(RETRY_MAX)
}

/// A migration's failures in a row, of one phase to one node.
struct Failures {
    to: String,
    phase: MigrationPhase,
    count: u32,
    last: Instant,
    /// The nodes of the move not ready at the last failure: once one is
    /// back, the next attempt starts at once, the wait being for it.
    away: Vec<String>,
    /// Attempts in a row that went on with a full copy cut short and got
    /// next to nowhere: at `PartialCopy::TRIES`, the next starts again.
    stalled: u32,
}

impl Failures {
    fn of(&self, m: &Migration) -> bool {
        self.to == m.to && self.phase == m.phase
    }

    /// Whether the next attempt waits still, with `ready` the nodes ready
    /// now.
    fn waiting(&self, ready: &HashSet<String>) -> bool {
        self.last.elapsed() < backoff(self.count) && !self.away.iter().any(|n| ready.contains(n))
    }
}

/// The nodes ready now; none if they can't be read.
fn ready_nodes(store: &Store) -> HashSet<String> {
    let Ok(list) = store.list(Kind::Node, None) else {
        return HashSet::new();
    };
    list.objects
        .into_iter()
        .filter(|o| o.status["state"] == "ready")
        .map(|o| o.metadata.name)
        .collect()
}
/// How long a transfer may go without a frame.
const IDLE: Duration = Duration::from_secs(120);

/// Runs the migrator whenever this manager leads, until `closing`.
pub(crate) async fn run(
    store: Store,
    hub: Arc<Hub>,
    activity: Arc<crate::activity::State>,
    id: NodeId,
    mut closing: watch::Receiver<bool>,
) {
    let mut metrics = store.raft.metrics();
    loop {
        loop {
            if *closing.borrow() {
                return;
            }
            if metrics.borrow().current_leader == Some(id) {
                break;
            }
            tokio::select! {
                changed = metrics.changed() => if changed.is_err() { return },
                _ = closing.changed() => {}
            }
        }
        // Transfers stop with the leadership; the next leader starts over.
        // Replicas are copied, scheduled snapshots taken, and nodes' exports
        // kept, beside the moves, through the same sessions.
        tokio::select! {
            () = async {
                tokio::join!(
                    lead(&store, &hub, &activity),
                    crate::replicate::lead(&store, &hub, &activity),
                    crate::snapshots::lead(&store, &hub),
                    crate::backups::lead(&store, &hub, &activity),
                    crate::archive::lead(&store, &hub),
                    crate::exports::lead(&store, &hub),
                );
            } => {}
            () = async {
                while metrics.borrow().current_leader == Some(id) {
                    if metrics.changed().await.is_err() {
                        return;
                    }
                }
            } => {}
            _ = closing.wait_for(|c| *c) => return,
        }
    }
}

async fn lead(store: &Store, hub: &Arc<Hub>, activity: &Arc<crate::activity::State>) {
    // Each Volume's task, with what it moves and, until called off, the
    // word to stop. What a migration called off left on its new node is
    // among the Volume's leftovers, written by the controller.
    let mut running: HashMap<ObjectKey, (Migration, Option<watch::Sender<bool>>)> = HashMap::new();
    // Each transfer's Volume, migration, node it moves from, and outcome.
    let mut tasks: JoinSet<(ObjectKey, Migration, String, Result<(), Copied>)> = JoinSet::new();
    let mut failed: HashMap<ObjectKey, Failures> = HashMap::new();
    // Old copies being dropped, and when a drop last failed.
    let mut drops: JoinSet<(ObjectKey, String, Result<(), String>)> = JoinSet::new();
    let mut dropping: HashSet<(ObjectKey, String)> = HashSet::new();
    let mut drop_failed: HashMap<(ObjectKey, String), Instant> = HashMap::new();
    loop {
        while let Some(done) = tasks.try_join_next() {
            let Ok((key, m, from, result)) = done else {
                continue;
            };
            running.remove(&key);
            match result {
                Err(Copied { why: e, stalled }) => {
                    let f = failed.entry(key.clone()).or_insert(Failures {
                        to: m.to.clone(),
                        phase: m.phase,
                        count: 0,
                        last: Instant::now(),
                        away: Vec::new(),
                        stalled: 0,
                    });
                    if !f.of(&m) {
                        (f.to, f.phase, f.count, f.stalled) = (m.to.clone(), m.phase, 0, 0);
                    }
                    f.count += 1;
                    f.stalled = if stalled { f.stalled + 1 } else { 0 };
                    f.last = Instant::now();
                    let ready = ready_nodes(store);
                    f.away = [from, m.to.clone()]
                        .into_iter()
                        .filter(|n| !ready.contains(n))
                        .collect();
                    let next = backoff(f.count);
                    tracing::warn!(volume = %key, to = %m.to, attempt = f.count, "moving the volume: {e}");
                    let why = format!("{e} (attempt {}; the next in {}s)", f.count, next.as_secs());
                    note(store, &key, &m, &why).await;
                }
                Ok(()) => {
                    failed.remove(&key);
                }
            }
        }
        // Nothing is called off on a failed read.
        let volumes = match store.list(Kind::Volume, None) {
            Ok(l) => l.objects,
            Err(e) => {
                tracing::warn!("reading volumes: {e}");
                tokio::time::sleep(PERIOD).await;
                continue;
            }
        };
        let statuses: HashMap<ObjectKey, VolumeStatus> = volumes
            .iter()
            .map(|v| {
                let status = serde_json::from_value(v.status.clone()).unwrap_or_default();
                (v.key(), status)
            })
            .collect();
        for (key, (m, cancel)) in &mut running {
            if cancel.is_some() && called_off(m, statuses.get(key)) {
                tracing::info!(volume = %key, to = %m.to, "migration called off");
                let _ = cancel.take().expect("checked").send(true);
            }
        }
        // Old copies of Volumes that moved, dropped until they are.
        while let Some(done) = drops.try_join_next() {
            let Ok((key, node, result)) = done else {
                continue;
            };
            dropping.remove(&(key.clone(), node.clone()));
            match result {
                Ok(()) => forget(store, &key, &node).await,
                Err(e) => {
                    tracing::warn!(volume = %key, node, "dropping the old copy: {e}");
                    drop_failed.insert((key, node), Instant::now());
                }
            }
        }
        let nodes: Option<HashSet<String>> = store
            .list(Kind::Node, None)
            .map(|l| l.objects.into_iter().map(|o| o.metadata.name).collect())
            .map_err(|e| tracing::warn!("reading nodes: {e}"))
            .ok();
        // No drop while the Volume's data moves: it could be where the
        // data goes.
        for (key, status) in statuses.iter().filter(|(_, s)| !s.leftovers.is_empty()) {
            let Some(nodes) = &nodes else { break };
            if running.contains_key(key) {
                continue;
            }
            let (to_drop, to_forget) = leftovers(status, nodes);
            for node in to_forget {
                forget(store, key, &node).await;
            }
            for node in to_drop {
                let at = (key.clone(), node.clone());
                if dropping.contains(&at)
                    || drop_failed.get(&at).is_some_and(|t| t.elapsed() < RETRY)
                {
                    continue;
                }
                let (hub, k) = (hub.clone(), key.clone());
                drops.spawn(async move {
                    let result = drop_copy(&hub, &k.namespace, &k.name, &node).await;
                    (k, node, result)
                });
                dropping.insert(at);
            }
        }
        // A move waiting on a node that is back goes at once.
        let ready = ready_nodes(store);
        for v in volumes {
            let key = v.key();
            let status = statuses[&key].clone();
            let Some(m) = status.migration.clone() else {
                continue;
            };
            if m.phase == MigrationPhase::Copied
                || running.contains_key(&key)
                || failed
                    .get(&key)
                    .is_some_and(|f| f.of(&m) && f.waiting(&ready))
                || moves_through_a_drop(&key, &status.node, &m, &dropping)
            {
                continue;
            }
            let (store, hub, activity) = (store.clone(), hub.clone(), activity.clone());
            let (k, started, from) = (key.clone(), m.clone(), status.node.clone());
            let stalled = failed
                .get(&key)
                .filter(|f| f.of(&m))
                .map_or(0, |f| f.stalled);
            let (cancel, mut cancelled) = watch::channel(false);
            tasks.spawn(async move {
                let result = tokio::select! {
                    r = advance(&store, &hub, &activity, &v, &status.node, &started, stalled) => r,
                    // What the new node received is a leftover now.
                    _ = cancelled.changed() => Ok(()),
                };
                (k, started, from, result)
            });
            running.insert(key, (m, Some(cancel)));
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Whether a migration would send or receive a Volume on a node still
/// dropping an old copy of it: the drop could take what arrives, so the
/// migration waits for it.
fn moves_through_a_drop(
    key: &ObjectKey,
    from: &str,
    m: &Migration,
    dropping: &HashSet<(ObjectKey, String)>,
) -> bool {
    [from, m.to.as_str()]
        .iter()
        .any(|node| dropping.contains(&(key.clone(), (*node).to_owned())))
}

/// A moved Volume's leftover copies to drop, and those to forget without
/// a drop: on a node the Volume is on or moving to again, where the copy
/// is its data or is replaced, and on a node removed, with its data.
fn leftovers(status: &VolumeStatus, nodes: &HashSet<String>) -> (Vec<String>, Vec<String>) {
    status.leftovers.iter().cloned().partition(|n| {
        *n != status.node
            && status.migration.as_ref().is_none_or(|m| m.to != *n)
            && nodes.contains(n)
    })
}

/// Takes a node off a Volume's leftovers.
async fn forget(store: &Store, key: &ObjectKey, node: &str) {
    let r = update(store, key, |s| {
        let before = s.leftovers.len();
        s.leftovers.retain(|n| n != node);
        s.leftovers.len() != before
    })
    .await;
    if let Err(e) = r {
        tracing::debug!(volume = %key, node, "forgetting an old copy: {e}");
    }
}

/// Whether a migration under way was called off, by the Volume's status
/// now: the controller ended it or sent the Volume elsewhere, or the
/// Volume is gone. One this migrator finished (the Volume is on its new
/// node) is not: its old copy is still to drop.
fn called_off(m: &Migration, now: Option<&VolumeStatus>) -> bool {
    !now.is_some_and(|s| match &s.migration {
        Some(now) => now.to == m.to,
        None => s.node == m.to,
    })
}

/// Moves a migration through its phase: the transfer, then the write.
async fn advance(
    store: &Store,
    hub: &Arc<Hub>,
    activity: &Arc<crate::activity::State>,
    v: &Object,
    from: &str,
    m: &Migration,
    stalled: u32,
) -> Result<(), Copied> {
    let (namespace, volume) = (&v.metadata.namespace, &v.metadata.name);
    let last = m.phase == MigrationPhase::Finishing;
    tracing::info!(volume = %v.key(), from, to = %m.to, last, "moving volume data");
    let key = v.key();
    // Said to be sending, and what it has sent, until written.
    let sending = activity.sending(&key);
    if last {
        transfer(
            hub,
            namespace,
            volume,
            from,
            &m.to,
            true,
            "",
            sending.counter(),
        )
        .await
        .map_err(|why| Copied {
            why,
            stalled: false,
        })?;
    } else {
        full_copy(
            hub,
            namespace,
            volume,
            from,
            &m.to,
            sending.counter(),
            stalled,
        )
        .await?;
    }
    let moved = update(store, &key, |s| {
        if s.migration.as_ref() != Some(m) {
            return false;
        }
        if last {
            let mut leftovers = std::mem::take(&mut s.leftovers);
            leftovers.retain(|n| *n != m.to);
            if !leftovers.iter().any(|n| n == from) {
                leftovers.push(from.to_owned());
            }
            *s = VolumeStatus {
                node: m.to.clone(),
                dataset: String::new(),
                migration: None,
                leftovers,
                lost: String::new(),
                // Its base is on the old node: the next copy is whole.
                replica: s.replica.take(),
                promoted: std::mem::take(&mut s.promoted),
                export: s.export.take(),
                // Schedules go on as they were, on the new node.
                snapshots: std::mem::take(&mut s.snapshots),
                // Its base is on the old node: the next backup is whole.
                backup: s.backup.take(),
            };
        } else {
            s.migration = Some(Migration {
                phase: MigrationPhase::Copied,
                message: String::new(),
                ..m.clone()
            });
        }
        true
    })
    .await?;
    sending.done();
    if moved && last {
        // The old copy is a leftover now, dropped by the migrator's round.
        tracing::info!(volume = %key, from, to = %m.to, "volume moved");
    }
    Ok(())
}

/// The resume token of the copy of a Volume cut short that `node` kept (a
/// replica's, or a move's full copy): empty if it kept none, or if its
/// agent is from before tokens were asked for (the copy then starts
/// again).
pub(crate) async fn resume_token(
    hub: &Arc<Hub>,
    namespace: &str,
    volume: &str,
    node: &str,
) -> Result<String, String> {
    let offer = SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: namespace.to_owned(),
            action: Some(Action::ResumeToken(ResumeToken {
                volume: volume.to_owned(),
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let asking = async {
        let mut a = hub
            .offer(node, offer)
            .await
            .map_err(|e| e.message().to_owned())?;
        let mut out = Vec::new();
        loop {
            match a.from_agent.next().await {
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Stdout(data)),
                })) => out.extend_from_slice(&data),
                Some(Ok(SessionFrame {
                    frame: Some(Frame::ExitCode(0)),
                })) => break,
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Error(e)),
                })) if e.message.contains("no action") => return Ok(String::new()),
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Error(e)),
                })) => return Err(format!("node {node}: {}", e.message)),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(format!("node {node}: {}", e.message())),
                None => return Err(format!("node {node}: its session ended early")),
            }
        }
        String::from_utf8(out).map_err(|_| format!("node {node}: a resume token that is not text"))
    };
    tokio::time::timeout(TOKEN_WAIT, asking)
        .await
        .map_err(|_| format!("node {node}: no resume token in {}s", TOKEN_WAIT.as_secs()))?
}

/// How long a node has to say what it kept of a copy cut short.
const TOKEN_WAIT: Duration = Duration::from_secs(30);

/// A move's full copy cut short, which the new node kept, goes on from
/// there; a token the old node cannot send (its full snapshot taken again
/// since) starts it again at once. Attempts to go on that fail in a row
/// moving next to nothing (an old node from before resuming sends a new
/// stream, which is refused) make the next one start again.
async fn full_copy(
    hub: &Arc<Hub>,
    namespace: &str,
    volume: &str,
    from: &str,
    to: &str,
    sent: &std::sync::atomic::AtomicU64,
    stalled: u32,
) -> Result<(), Copied> {
    let token = match stalled < legion_api::volume::PartialCopy::TRIES {
        true => resume_token(hub, namespace, volume, to)
            .await
            .map_err(|e| Copied::failed(e, false, sent))?,
        false => String::new(),
    };
    if !token.is_empty() {
        tracing::info!(volume, from, to, "going on with the full copy cut short");
        match transfer(hub, namespace, volume, from, to, false, &token, sent).await {
            Ok(()) => return Ok(()),
            Err(e) if e.contains(legion_api::volume::NOT_RESUMABLE) => {
                tracing::info!(volume, from, to, "starting the full copy again: {e}");
                sent.store(0, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => return Err(Copied::failed(e, true, sent)),
        }
    }
    transfer(hub, namespace, volume, from, to, false, "", sent)
        .await
        .map_err(|e| Copied::failed(e, false, sent))
}

/// A full copy that failed: why, and whether it went on with one cut
/// short and got next to nowhere.
pub(crate) struct Copied {
    pub why: String,
    pub stalled: bool,
}

/// A failure after the copy (writing it down): nothing to go on with.
impl From<String> for Copied {
    fn from(why: String) -> Copied {
        Copied {
            why,
            stalled: false,
        }
    }
}

impl Copied {
    fn failed(why: String, resumed: bool, sent: &std::sync::atomic::AtomicU64) -> Copied {
        let moved = sent.load(std::sync::atomic::Ordering::Relaxed);
        Copied {
            why,
            stalled: resumed && moved < legion_api::volume::PartialCopy::PROGRESS,
        }
    }
}

/// Sends a Volume from one node to another: all of it, or with `last`
/// what changed since the full copy; with `token`, the rest of the full
/// copy cut short.
#[allow(clippy::too_many_arguments)]
async fn transfer(
    hub: &Arc<Hub>,
    namespace: &str,
    volume: &str,
    from: &str,
    to: &str,
    last: bool,
    token: &str,
    sent: &std::sync::atomic::AtomicU64,
) -> Result<(), String> {
    let open = |action| SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: namespace.to_owned(),
            action: Some(action),
            ..Default::default()
        }),
        ..Default::default()
    };
    let volume = volume.to_owned();
    let mut receiver = hub
        .offer(
            to,
            open(Action::VolumeReceive(VolumeReceive {
                volume: volume.clone(),
                last,
                resume: !token.is_empty(),
            })),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let mut sender = hub
        .offer(
            from,
            open(Action::VolumeSend(VolumeSend {
                volume,
                last,
                resume_token: token.to_owned(),
            })),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let result = pipe(
        &mut sender.from_agent,
        &mut receiver.from_agent,
        &receiver.to_agent,
        from,
        to,
        Some(sent),
    )
    .await;
    // Not waited on: a receiver that stopped reading goes with the session.
    if let Err(e) = &result {
        let _ = receiver
            .to_agent
            .try_send(Ok(frame(Frame::Error(SessionError {
                code: tonic::Code::Aborted as i32,
                message: e.clone(),
            }))));
    }
    result.map(|_| ())
}

/// What an agent sends in a session.
pub(crate) trait Frames:
    Stream<Item = Result<SessionFrame, tonic::Status>> + Unpin + Send
{
}
impl<S: Stream<Item = Result<SessionFrame, tonic::Status>> + Unpin + Send + ?Sized> Frames for S {}

/// Where an agent's session input goes.
pub(crate) type ToAgent = tokio::sync::mpsc::Sender<Result<SessionFrame, tonic::Status>>;

/// The sender's output as the receiver's input, until the receiver has
/// taken all of it; the bytes sent.
pub(crate) async fn pipe(
    sender: &mut (impl Frames + ?Sized),
    received: &mut (impl Frames + ?Sized),
    receiver: &ToAgent,
    from: &str,
    to: &str,
    counted: Option<&std::sync::atomic::AtomicU64>,
) -> Result<u64, String> {
    let mut sent = 0u64;
    loop {
        tokio::select! {
            f = tokio::time::timeout(IDLE, sender.next()) => {
                match f.map_err(|_| idle(from))? {
                    Some(Ok(SessionFrame { frame: Some(Frame::Stdout(data)) })) => {
                        sent += data.len() as u64;
                        if let Some(c) = counted {
                            c.store(sent, std::sync::atomic::Ordering::Relaxed);
                        }
                        forward(received, receiver, Frame::Input(data), to).await?;
                    }
                    Some(Ok(SessionFrame { frame: Some(Frame::ExitCode(0)) })) => {
                        forward(received, receiver, Frame::InputClosed(InputClosed {}), to).await?;
                        break;
                    }
                    f => other(f, from)?,
                }
            }
            f = received.next() => early(f, to)?,
        }
    }
    finished(received, to).await?;
    Ok(sent)
}

fn idle(node: &str) -> String {
    format!("node {node}: nothing moved for {}s", IDLE.as_secs())
}

/// Sends `f` to the receiver. A receiver that stops taking data holds this
/// up, so it is timed, and the receiver's own frames are heard meanwhile;
/// the send itself is never given up for them, or its data would be lost.
async fn forward(
    received: &mut (impl Frames + ?Sized),
    receiver: &ToAgent,
    f: Frame,
    to: &str,
) -> Result<(), String> {
    let send = tokio::time::timeout(IDLE, receiver.send(Ok(frame(f))));
    tokio::pin!(send);
    loop {
        tokio::select! {
            sent = &mut send => {
                return match sent {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => Err(format!("node {to} went away")),
                    Err(_) => Err(idle(to)),
                };
            }
            f = received.next() => early(f, to)?,
        }
    }
}

/// A frame from the receiver while data still flows: any exit, even 0,
/// comes before it has all of it.
fn early(f: Option<Result<SessionFrame, tonic::Status>>, node: &str) -> Result<(), String> {
    match f {
        Some(Ok(SessionFrame {
            frame: Some(Frame::ExitCode(code)),
        })) => Err(format!(
            "node {node} exited with {code} before it had all the data"
        )),
        f => other(f, node),
    }
}

/// Waits for an agent's exit code.
async fn finished(a: &mut (impl Frames + ?Sized), node: &str) -> Result<(), String> {
    loop {
        let f = tokio::time::timeout(IDLE, a.next())
            .await
            .map_err(|_| format!("node {node} did not finish"))?;
        match f {
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => return Ok(()),
            f => other(f, node)?,
        }
    }
}

/// A frame from `node` that carries no data: an error, a failed exit or
/// the end of its stream stops the transfer.
fn other(f: Option<Result<SessionFrame, tonic::Status>>, node: &str) -> Result<(), String> {
    match f {
        Some(Ok(SessionFrame {
            frame: Some(Frame::Error(e)),
        })) => Err(format!("node {node}: {}", e.message)),
        Some(Ok(SessionFrame {
            frame: Some(Frame::ExitCode(code)),
        })) if code != 0 => Err(format!("node {node}: exited with {code}")),
        Some(Ok(_)) => Ok(()),
        Some(Err(e)) => Err(format!("node {node}: {}", e.message())),
        None => Err(format!("node {node} went away")),
    }
}

/// Has a node remove its copy of a Volume that has moved away.
async fn drop_copy(
    hub: &Arc<Hub>,
    namespace: &str,
    volume: &str,
    node: &str,
) -> Result<(), String> {
    let mut a = hub
        .offer(
            node,
            SessionOffer {
                id: legion_pki::random_secret(),
                open: Some(SessionOpen {
                    namespace: namespace.to_owned(),
                    action: Some(Action::VolumeDrop(volume.to_owned())),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    finished(&mut a.from_agent, node).await
}

/// Notes why an attempt failed in the migration, if it is still the same.
async fn note(store: &Store, key: &ObjectKey, m: &Migration, why: &str) {
    let r = update(store, key, |s| match &mut s.migration {
        Some(now) if now.to == m.to && now.phase == m.phase && now.message != why => {
            now.message = why.to_owned();
            true
        }
        _ => false,
    })
    .await;
    if let Err(e) = r {
        tracing::debug!(volume = %key, "noting a failed migration: {e}");
    }
}

/// Changes a Volume's status with `f`, which says whether it changed
/// anything; tries again if the Volume changes meanwhile. Returns whether
/// it wrote.
pub(crate) async fn update(
    store: &Store,
    key: &ObjectKey,
    f: impl Fn(&mut VolumeStatus) -> bool,
) -> Result<bool, String> {
    for _ in 0..5 {
        let Some(mut o) = store.get(key).map_err(|e| e.to_string())? else {
            return Ok(false);
        };
        let mut status: VolumeStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        if !f(&mut status) {
            return Ok(false);
        }
        o.status = serde_json::to_value(&status).map_err(|e| e.to_string())?;
        let version = o.metadata.version;
        let command = Command::Put {
            object: o,
            precondition: Precondition::Version(version),
        };
        let out = tokio::time::timeout(crate::COMMIT_TIMEOUT, store.client_write(command))
            .await
            .map_err(|_| crate::no_quorum().message().to_owned())?
            .map_err(|e| e.to_string())?;
        match out.data {
            Outcome::Written(_) => return Ok(true),
            Outcome::Conflict { .. } => continue,
            other => return Err(format!("{other:?}")),
        }
    }
    Err(format!("{key} keeps changing"))
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    fn ok(f: Frame) -> Result<SessionFrame, tonic::Status> {
        Ok(frame(f))
    }

    #[test]
    fn attempts_after_failures_wait_longer_each_time() {
        let waits: Vec<u64> = (1..=8).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(waits, [5, 10, 20, 40, 80, 160, 300, 300]);
        assert_eq!(backoff(u32::MAX), RETRY_MAX);
    }

    #[test]
    fn a_move_waiting_on_a_node_goes_once_it_is_back() {
        let f = Failures {
            to: "n2".into(),
            phase: MigrationPhase::Copying,
            count: 6,
            last: Instant::now(),
            away: vec!["n1".into()],
            stalled: 0,
        };
        let ready = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect();
        assert!(f.waiting(&ready(&["n2", "n3"])));
        assert!(!f.waiting(&ready(&["n1", "n2"])));
        // Failed with both nodes there: the wait is the backoff's.
        let f = Failures { away: vec![], ..f };
        assert!(f.waiting(&ready(&["n1", "n2"])));
    }

    #[test]
    fn migrations_called_off() {
        let m = |to: &str, phase| Migration {
            to: to.into(),
            phase,
            message: String::new(),
        };
        let copying = m("n2", MigrationPhase::Copying);
        let status = |node: &str, migration| VolumeStatus {
            node: node.into(),
            dataset: String::new(),
            migration,
            leftovers: Vec::new(),
            lost: String::new(),
            ..Default::default()
        };
        // Still on, or moved on to its next phase.
        assert!(!called_off(
            &copying,
            Some(&status("n1", Some(copying.clone())))
        ));
        let copied = m("n2", MigrationPhase::Copied);
        assert!(!called_off(&copying, Some(&status("n1", Some(copied)))));
        // Done: the Volume is on n2.
        assert!(!called_off(&copying, Some(&status("n2", None))));
        // Ended, sent elsewhere, or gone.
        assert!(called_off(&copying, Some(&status("n1", None))));
        let n3 = m("n3", MigrationPhase::Copying);
        assert!(called_off(&copying, Some(&status("n1", Some(n3)))));
        assert!(called_off(&copying, None));
    }

    #[test]
    fn a_migration_waits_for_drops_where_it_moves() {
        let key = ObjectKey {
            kind: Kind::Volume,
            namespace: "default".into(),
            name: "data".into(),
        };
        let back = Migration {
            to: "a".into(),
            phase: MigrationPhase::Copying,
            message: String::new(),
        };
        let dropping = |node: &str| HashSet::from([(key.clone(), node.to_owned())]);
        // Moved a to b, and a still drops its old copy: b to a waits, or
        // the drop could take what a receives.
        assert!(moves_through_a_drop(&key, "b", &back, &dropping("a")));
        assert!(moves_through_a_drop(&key, "b", &back, &dropping("b")));
        // A drop elsewhere, or of another Volume, holds nothing up.
        assert!(!moves_through_a_drop(&key, "b", &back, &dropping("c")));
        let other = ObjectKey {
            name: "logs".into(),
            ..key.clone()
        };
        let elsewhere = HashSet::from([(other, "a".to_owned())]);
        assert!(!moves_through_a_drop(&key, "b", &back, &elsewhere));
    }

    #[test]
    fn old_copies_are_dropped_where_nothing_else_wants_them() {
        let nodes: HashSet<String> = ["n1", "n2", "n3"].map(String::from).into();
        let status = VolumeStatus {
            node: "n2".into(),
            dataset: String::new(),
            migration: Some(Migration {
                to: "n3".into(),
                phase: MigrationPhase::Copying,
                message: String::new(),
            }),
            leftovers: ["n1", "n2", "n3", "gone"].map(String::from).into(),
            lost: String::new(),
            ..Default::default()
        };
        // n1's copy is dropped; n2 holds the Volume, n3 receives it again,
        // and the removed node took its copy with it.
        let (to_drop, to_forget) = leftovers(&status, &nodes);
        assert_eq!(to_drop, ["n1"]);
        assert_eq!(to_forget, ["n2", "n3", "gone"]);
    }

    #[tokio::test]
    async fn no_data_is_lost_while_the_receiver_talks() {
        let chunks: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; 3]).collect();
        let mut sent: Vec<_> = chunks
            .iter()
            .cloned()
            .map(|c| ok(Frame::Stdout(c)))
            .collect();
        sent.push(ok(Frame::ExitCode(0)));
        let mut sender = tokio_stream::iter(sent);
        // The receiver takes its input slowly, through a full channel, and
        // has something to say about every piece.
        let (input, mut taken) = mpsc::channel::<Result<SessionFrame, tonic::Status>>(1);
        let (says, said) = mpsc::channel(64);
        let receiving = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(Ok(f)) = taken.recv().await {
                let _ = says.send(ok(Frame::Stderr(b"progress".to_vec()))).await;
                tokio::time::sleep(Duration::from_millis(2)).await;
                match f.frame {
                    Some(Frame::Input(data)) => got.push(data),
                    Some(Frame::InputClosed(_)) => {
                        let _ = says.send(ok(Frame::ExitCode(0))).await;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
            }
            got
        });
        let mut said = ReceiverStream::new(said);
        let counted = std::sync::atomic::AtomicU64::new(0);
        let sent = pipe(&mut sender, &mut said, &input, "n1", "n2", Some(&counted))
            .await
            .unwrap();
        let got = receiving.await.unwrap();
        assert_eq!(sent, got.iter().map(|c| c.len() as u64).sum::<u64>());
        assert_eq!(counted.into_inner(), sent, "counted as it went");
        assert_eq!(got, chunks);
    }

    #[tokio::test]
    async fn a_receiver_that_ends_early_fails_the_transfer() {
        let mut sender = tokio_stream::pending::<Result<SessionFrame, tonic::Status>>();
        let (input, _taken) = mpsc::channel(1);
        let mut said = tokio_stream::iter(vec![ok(Frame::ExitCode(0))]);
        let err = pipe(&mut sender, &mut said, &input, "n1", "n2", None)
            .await
            .unwrap_err();
        assert!(err.contains("before it had all the data"), "{err}");
    }
}
