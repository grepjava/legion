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
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_stream::StreamExt;

use crate::session::{Attachment, Hub};

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
    let mut running: HashMap<ObjectKey, Migration> = HashMap::new();
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
        for v in volumes {
            let key = v.key();
            let status: VolumeStatus = serde_json::from_value(v.status.clone()).unwrap_or_default();
            let Some(m) = status.migration.clone() else {
                continue;
            };
            if m.phase == MigrationPhase::Copied
                || running.contains_key(&key)
                || failed.get(&key).is_some_and(|t| t.elapsed() < RETRY)
            {
                continue;
            }
            running.insert(key.clone(), m.clone());
            let (store, hub) = (store.clone(), hub.clone());
            tasks.spawn(async move {
                let result = advance(&store, &hub, &v, &status.node, &m).await;
                (key, m, result)
            });
        }
        tokio::time::sleep(PERIOD).await;
    }
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
    let result = pipe(&mut sender, &mut receiver, from, to).await;
    if let Err(e) = &result {
        let _ = receiver
            .to_agent
            .send(Ok(frame(Frame::Error(SessionError {
                code: tonic::Code::Aborted as i32,
                message: e.clone(),
            }))))
            .await;
    }
    result
}

/// The sender's output as the receiver's input, until the receiver has
/// taken all of it.
async fn pipe(
    sender: &mut Attachment,
    receiver: &mut Attachment,
    from: &str,
    to: &str,
) -> Result<(), String> {
    let idle = || format!("no data for {}s", IDLE.as_secs());
    loop {
        tokio::select! {
            f = tokio::time::timeout(IDLE, sender.from_agent.next()) => {
                match f.map_err(|_| idle())? {
                    Some(Ok(SessionFrame { frame: Some(Frame::Stdout(data)) })) => {
                        if receiver.to_agent.send(Ok(frame(Frame::Input(data)))).await.is_err() {
                            return Err(format!("node {to} went away"));
                        }
                    }
                    Some(Ok(SessionFrame { frame: Some(Frame::ExitCode(0)) })) => {
                        let _ = receiver.to_agent.send(Ok(frame(Frame::InputClosed(InputClosed {})))).await;
                        break;
                    }
                    Some(Ok(SessionFrame { frame: Some(Frame::Error(e)) })) => {
                        return Err(format!("node {from}: {}", e.message));
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(format!("node {from}: {}", e.message())),
                    None => return Err(format!("node {from} went away")),
                }
            }
            f = receiver.from_agent.next() => match f {
                Some(Ok(SessionFrame { frame: Some(Frame::Error(e)) })) => {
                    return Err(format!("node {to}: {}", e.message));
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(format!("node {to}: {}", e.message())),
                None => return Err(format!("node {to} went away")),
            },
        }
    }
    finished(receiver, to).await
}

/// Waits for an agent's exit code.
async fn finished(a: &mut Attachment, node: &str) -> Result<(), String> {
    loop {
        let f = tokio::time::timeout(IDLE, a.from_agent.next())
            .await
            .map_err(|_| format!("node {node} did not finish"))?;
        match f {
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => return Ok(()),
            Some(Ok(SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => return Err(format!("node {node}: {}", e.message)),
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(format!("node {node}: {}", e.message())),
            None => return Err(format!("node {node} went away")),
        }
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
    finished(&mut a, node).await
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
