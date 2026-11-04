//! The migrator (DESIGN.md §4.3): while this manager leads, it moves the
//! data of Volumes the controller has started migrating. For a migration
//! `copying` it has the old node send a full copy and the new node receive
//! it, then marks it `copied`; for one `finishing` (its tasks stopped), the
//! changes since, then it gives the Volume to the new node and has the old
//! one drop its copy. Both agents attach to this manager's sessions (see
//! [`crate::session`]), which joins them, so nodes need no port of their
//! own. A failed attempt is noted in the migration and tried again.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::volume::{Migration, MigrationPhase, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{
    InputClosed, SessionError, SessionFrame, SessionOffer, SessionOpen, VolumeReceive, VolumeSend,
};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};

use crate::session::Hub;

/// How often the Volumes are looked at.
const PERIOD: Duration = Duration::from_secs(1);
/// How long after a failed attempt the next one starts.
const RETRY: Duration = Duration::from_secs(5);
/// How long a transfer may go without a frame.
const IDLE: Duration = Duration::from_secs(120);

/// Runs the migrator whenever this manager leads, until `closing`.
pub(crate) async fn run(
    store: Store,
    hub: Arc<Hub>,
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
        tokio::select! {
            () = lead(&store, &hub) => {}
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

async fn lead(store: &Store, hub: &Arc<Hub>) {
    // Each Volume's task, with what it moves and, until called off, the
    // word to stop. What a migration called off left on its new node is
    // among the Volume's leftovers, written by the controller.
    let mut running: HashMap<ObjectKey, (Migration, Option<watch::Sender<bool>>)> = HashMap::new();
    let mut tasks: JoinSet<(ObjectKey, Migration, Result<(), String>)> = JoinSet::new();
    let mut failed: HashMap<ObjectKey, Instant> = HashMap::new();
    // Old copies being dropped, and when a drop last failed.
    let mut drops: JoinSet<(ObjectKey, String, Result<(), String>)> = JoinSet::new();
    let mut dropping: HashSet<(ObjectKey, String)> = HashSet::new();
    let mut drop_failed: HashMap<(ObjectKey, String), Instant> = HashMap::new();
    loop {
        while let Some(done) = tasks.try_join_next() {
            let Ok((key, m, result)) = done else { continue };
            running.remove(&key);
            if let Err(e) = result {
                tracing::warn!(volume = %key, to = %m.to, "moving the volume: {e}");
                failed.insert(key.clone(), Instant::now());
                note(store, &key, &m, &e).await;
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
        for v in volumes {
            let key = v.key();
            let status = statuses[&key].clone();
            let Some(m) = status.migration.clone() else {
                continue;
            };
            if m.phase == MigrationPhase::Copied
                || running.contains_key(&key)
                || failed.get(&key).is_some_and(|t| t.elapsed() < RETRY)
                || moves_through_a_drop(&key, &status.node, &m, &dropping)
            {
                continue;
            }
            let (store, hub) = (store.clone(), hub.clone());
            let (k, started) = (key.clone(), m.clone());
            let (cancel, mut cancelled) = watch::channel(false);
            tasks.spawn(async move {
                let result = tokio::select! {
                    r = advance(&store, &hub, &v, &status.node, &started) => r,
                    // What the new node received is a leftover now.
                    _ = cancelled.changed() => Ok(()),
                };
                (k, started, result)
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
    v: &Object,
    from: &str,
    m: &Migration,
) -> Result<(), String> {
    let (namespace, volume) = (&v.metadata.namespace, &v.metadata.name);
    let last = m.phase == MigrationPhase::Finishing;
    tracing::info!(volume = %v.key(), from, to = %m.to, last, "moving volume data");
    transfer(hub, namespace, volume, from, &m.to, last).await?;
    let key = v.key();
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
    if moved && last {
        // The old copy is a leftover now, dropped by the migrator's round.
        tracing::info!(volume = %key, from, to = %m.to, "volume moved");
    }
    Ok(())
}

/// Sends a Volume from one node to another: all of it, or with `last`
/// what changed since the full copy.
async fn transfer(
    hub: &Arc<Hub>,
    namespace: &str,
    volume: &str,
    from: &str,
    to: &str,
    last: bool,
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
            })),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let mut sender = hub
        .offer(from, open(Action::VolumeSend(VolumeSend { volume, last })))
        .await
        .map_err(|e| e.message().to_owned())?;
    let result = pipe(
        &mut sender.from_agent,
        &mut receiver.from_agent,
        &receiver.to_agent,
        from,
        to,
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
    result
}

/// What an agent sends in a session.
trait Frames: Stream<Item = Result<SessionFrame, tonic::Status>> + Unpin + Send {}
impl<S: Stream<Item = Result<SessionFrame, tonic::Status>> + Unpin + Send + ?Sized> Frames for S {}

/// Where an agent's session input goes.
type ToAgent = tokio::sync::mpsc::Sender<Result<SessionFrame, tonic::Status>>;

/// The sender's output as the receiver's input, until the receiver has
/// taken all of it.
async fn pipe(
    sender: &mut (impl Frames + ?Sized),
    received: &mut (impl Frames + ?Sized),
    receiver: &ToAgent,
    from: &str,
    to: &str,
) -> Result<(), String> {
    loop {
        tokio::select! {
            f = tokio::time::timeout(IDLE, sender.next()) => {
                match f.map_err(|_| idle(from))? {
                    Some(Ok(SessionFrame { frame: Some(Frame::Stdout(data)) })) => {
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
    finished(received, to).await
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
async fn update(
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
        let out = tokio::time::timeout(crate::COMMIT_TIMEOUT, store.raft.client_write(command))
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
        pipe(&mut sender, &mut said, &input, "n1", "n2")
            .await
            .unwrap();
        assert_eq!(receiving.await.unwrap(), chunks);
    }

    #[tokio::test]
    async fn a_receiver_that_ends_early_fails_the_transfer() {
        let mut sender = tokio_stream::pending::<Result<SessionFrame, tonic::Status>>();
        let (input, _taken) = mpsc::channel(1);
        let mut said = tokio_stream::iter(vec![ok(Frame::ExitCode(0))]);
        let err = pipe(&mut sender, &mut said, &input, "n1", "n2")
            .await
            .unwrap_err();
        assert!(err.contains("before it had all the data"), "{err}");
    }
}
