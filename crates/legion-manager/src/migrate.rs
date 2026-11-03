//! The migrator (DESIGN.md §4.3): while this manager leads, it moves the
//! data of Volumes the controller has started migrating. For a migration
//! `copying` it has the old node send a full copy and the new node receive
//! it, then marks it `copied`; for one `finishing` (its tasks stopped), the
//! changes since, then it gives the Volume to the new node and has the old
//! one drop its copy. Both agents attach to this manager's sessions (see
//! [`crate::session`]), which joins them, so nodes need no port of their
//! own. A failed attempt is noted in the migration and tried again.

use std::collections::HashMap;
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
use tokio::task::{AbortHandle, JoinSet};
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
    let mut running: HashMap<ObjectKey, (Migration, AbortHandle)> = HashMap::new();
    let mut tasks: JoinSet<(ObjectKey, Migration, Result<(), String>)> = JoinSet::new();
    let mut failed: HashMap<ObjectKey, Instant> = HashMap::new();
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
        let volumes = match store.list(Kind::Volume, None) {
            Ok(l) => l.objects,
            Err(e) => {
                tracing::warn!("reading volumes: {e}");
                Vec::new()
            }
        };
        let statuses: HashMap<ObjectKey, VolumeStatus> = volumes
            .iter()
            .map(|v| {
                let status = serde_json::from_value(v.status.clone()).unwrap_or_default();
                (v.key(), status)
            })
            .collect();
        running.retain(|key, (m, handle)| {
            let wanted = !called_off(m, statuses.get(key));
            if !wanted {
                tracing::info!(volume = %key, to = %m.to, "migration called off");
                handle.abort();
            }
            wanted
        });
        for v in volumes {
            let key = v.key();
            let status = statuses[&key].clone();
            let Some(m) = status.migration.clone() else {
                continue;
            };
            if m.phase == MigrationPhase::Copied
                || running.contains_key(&key)
                || failed.get(&key).is_some_and(|t| t.elapsed() < RETRY)
            {
                continue;
            }
            let (store, hub) = (store.clone(), hub.clone());
            let (k, started) = (key.clone(), m.clone());
            let handle = tasks.spawn(async move {
                let result = advance(&store, &hub, &v, &status.node, &started).await;
                (k, started, result)
            });
            running.insert(key, (m, handle));
        }
        tokio::time::sleep(PERIOD).await;
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
            *s = VolumeStatus {
                node: m.to.clone(),
                dataset: String::new(),
                migration: None,
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
        tracing::info!(volume = %key, from, to = %m.to, "volume moved");
        if let Err(e) = drop_copy(hub, namespace, volume, from).await {
            tracing::warn!(volume = %key, node = from, "dropping the old copy: {e}");
        }
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
