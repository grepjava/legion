//! The backup runner (DESIGN.md §4.3, Volume backups): while this manager
//! leads, it has the node holding each ZFS Volume with a `backup` take one
//! once its `every` has passed since the last and send it to its
//! `BackupTarget` (what changed since the last the target holds, else
//! whole), and prune the target to `keep`. Each run is an Activity; the
//! last are kept in the Volume's `status.backup`, so a new leader carries
//! on where the old one was. Nothing runs while the data moves, before any
//! task has used the Volume, or while its node is not ready; a failure is
//! said and retried with the migrator's backoff.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::backup::{BackupRun, BackupSchedule, BackupStatus, BackupTargetSpec};
use legion_api::data::DataSpec;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::volume::{VolumeDriver, VolumeSpec, VolumeStatus};
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{BackupTargetAccess, SessionFrame, SessionOffer, SessionOpen};
use legion_store::Store;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::migrate::{backoff, update};
use crate::session::{Attachment, Hub};

/// How often the Volumes are looked at.
const PERIOD: Duration = Duration::from_secs(10);

/// The target `name` as a node reaches it: its spec and the value of the
/// Secret it names (an SSH key, an S3 secret key), if any.
pub(crate) fn access(store: &Store, name: &str) -> Result<BackupTargetAccess, String> {
    let o = store
        .get(&ObjectKey::new(Kind::BackupTarget, "", name))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no BackupTarget {name}"))?;
    let spec: BackupTargetSpec = legion_api::parse(&o.spec).map_err(|e| e.to_string())?;
    let key = match spec.secret() {
        Some(r) => {
            let secret = store
                .get(&ObjectKey::new(Kind::Secret, &r.namespace, &r.name))
                .map_err(|e| e.to_string())?
                .ok_or_else(|| {
                    format!("BackupTarget {name}: no Secret {}/{}", r.namespace, r.name)
                })?;
            let data: DataSpec = legion_api::parse(&secret.spec).map_err(|e| e.to_string())?;
            data.bytes()
                .map_err(|e| format!("Secret {}/{}: {e}", r.namespace, r.name))?
        }
        None => Vec::new(),
    };
    Ok(BackupTargetAccess {
        name: name.to_owned(),
        spec_json: serde_json::to_string(&spec).map_err(|e| e.to_string())?,
        key,
    })
}

/// Whether `schedule` is due at `now_ms`, given its status; one that
/// failed waits its backoff from the last attempt, `tried_ms`.
pub(crate) fn due(
    schedule: &BackupSchedule,
    status: Option<&BackupStatus>,
    now_ms: i64,
    tried_ms: Option<i64>,
) -> bool {
    let st = status.filter(|s| s.target == schedule.target);
    let since_taken = st
        .and_then(|s| s.taken_ms)
        .is_none_or(|at| now_ms - at >= schedule.every.millis() as i64);
    let failures = st.map_or(0, |s| s.failures);
    let since_tried = failures == 0
        || tried_ms.is_none_or(|at| now_ms - at >= backoff(failures).as_millis() as i64);
    since_taken && since_tried
}

/// The base of the next backup: the last the target holds, when it was
/// taken on the node holding the data now.
pub(crate) fn base(schedule: &BackupSchedule, status: Option<&BackupStatus>, node: &str) -> String {
    status
        .filter(|s| s.target == schedule.target && s.from == node)
        .map(|s| s.last.clone())
        .unwrap_or_default()
}

/// Whether a Volume's backups are late at `now_ms`: the last older than
/// twice its `every`, or none yet and the first failing.
pub(crate) fn late(schedule: &BackupSchedule, status: Option<&BackupStatus>, now_ms: i64) -> bool {
    let Some(st) = status.filter(|s| s.target == schedule.target) else {
        return false;
    };
    match st.taken_ms {
        Some(at) => now_ms - at > 2 * schedule.every.millis() as i64,
        None => st.failures > 0,
    }
}

/// The nodes ready now, by name.
pub(crate) fn ready_nodes(store: &Store) -> Option<HashSet<String>> {
    let nodes = store.list(Kind::Node, None).ok()?.objects;
    Some(
        nodes
            .into_iter()
            .filter(|n| {
                serde_json::from_value::<NodeStatus>(n.status.clone())
                    .is_ok_and(|s| s.state == NodeState::Ready)
            })
            .map(|n| n.metadata.name)
            .collect(),
    )
}

/// What a node said a run did.
#[derive(serde::Deserialize, Default)]
struct Ran {
    bytes: u64,
    whole: bool,
}

type Done = (
    ObjectKey,
    BackupSchedule,
    String,
    String,
    i64,
    Result<Ran, String>,
);

pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>, activity: &Arc<crate::activity::State>) {
    let mut runs: JoinSet<Done> = JoinSet::new();
    let mut running: HashSet<ObjectKey> = HashSet::new();
    let mut tried: HashMap<ObjectKey, i64> = HashMap::new();
    loop {
        while let Some(done) = runs.try_join_next() {
            let Ok((key, schedule, snapshot, node, at_ms, result)) = done else {
                continue;
            };
            running.remove(&key);
            let took_ms = (crate::console::now_ms() - at_ms).max(0) as u64;
            let r = update(store, &key, |s| {
                let mut st = s
                    .backup
                    .take()
                    .filter(|b| b.target == schedule.target)
                    .unwrap_or_else(|| BackupStatus {
                        target: schedule.target.clone(),
                        ..Default::default()
                    });
                match &result {
                    Ok(ran) => {
                        st.last = snapshot.clone();
                        st.from = node.clone();
                        st.taken_ms = Some(at_ms);
                        st.message.clear();
                        st.failures = 0;
                        st.record(BackupRun {
                            at_ms,
                            took_ms,
                            bytes: ran.bytes,
                            whole: ran.whole,
                            error: String::new(),
                        });
                    }
                    Err(e) => {
                        st.failures += 1;
                        st.message = format!(
                            "{e} (attempt {}; the next in {}s)",
                            st.failures,
                            backoff(st.failures).as_secs()
                        );
                        st.record(BackupRun {
                            at_ms,
                            took_ms,
                            error: e.clone(),
                            ..Default::default()
                        });
                    }
                }
                s.backup = Some(st);
                true
            })
            .await;
            if let Err(e) = r {
                tracing::debug!(volume = %key, "recording the backup: {e}");
            }
        }
        let volumes = match store.list(Kind::Volume, None) {
            Ok(l) => l.objects,
            Err(e) => {
                tracing::warn!("reading volumes: {e}");
                tokio::time::sleep(PERIOD).await;
                continue;
            }
        };
        let Some(ready) = ready_nodes(store) else {
            tokio::time::sleep(PERIOD).await;
            continue;
        };
        let now = crate::console::now_ms();
        for v in volumes {
            let key = v.key();
            if running.contains(&key) {
                continue;
            }
            let Ok(spec) = legion_api::parse::<VolumeSpec>(&v.spec) else {
                continue;
            };
            let Some(schedule) = spec.backup.clone() else {
                continue;
            };
            let status: VolumeStatus = serde_json::from_value(v.status.clone()).unwrap_or_default();
            if spec.driver != VolumeDriver::Zfs
                || status.node.is_empty()
                || status.migration.is_some()
                || !status.lost.is_empty()
                || !ready.contains(&status.node)
                || !due(
                    &schedule,
                    status.backup.as_ref(),
                    now,
                    tried.get(&key).copied(),
                )
            {
                continue;
            }
            tried.insert(key.clone(), now);
            let snapshot = schedule.name_at(now);
            let base = base(&schedule, status.backup.as_ref(), &status.node);
            let node = status.node.clone();
            let target = access(store, &schedule.target);
            let op = activity.begin(
                legion_api::activity::OperationKind::Backup,
                crate::audit::target("Volume", &key.namespace, &key.name),
                format!(
                    "backup of Volume {}/{} to {}{}",
                    key.namespace,
                    key.name,
                    schedule.target,
                    if base.is_empty() { ", whole" } else { "" }
                ),
                legion_api::activity::Stage::Sending,
                String::new(),
            );
            let (hub, k, s) = (hub.clone(), key.clone(), schedule.clone());
            running.insert(key);
            runs.spawn(async move {
                let result = match target {
                    Ok(t) => run(&hub, &k, &node, t, &snapshot, &base, s.keep).await,
                    Err(e) => Err(e),
                };
                op.finish(match &result {
                    Ok(r) => Ok(format!(
                        "{} bytes{}",
                        r.bytes,
                        if r.whole { ", whole" } else { "" }
                    )),
                    Err(e) => Err(e.clone()),
                });
                (k, s, snapshot, node, now, result)
            });
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Offers `node` a session with `action` for `key`'s namespace.
pub(crate) async fn open(
    hub: &Arc<Hub>,
    node: &str,
    namespace: &str,
    action: Action,
) -> Result<Attachment, String> {
    let offer = SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: namespace.to_owned(),
            action: Some(action),
            ..Default::default()
        }),
        ..Default::default()
    };
    hub.offer(node, offer)
        .await
        .map_err(|e| e.message().to_owned())
}

/// What a session says until it ends: its output, its snapshots list.
pub(crate) async fn said(a: &mut Attachment, node: &str) -> Result<(Vec<u8>, Vec<String>), String> {
    let (mut out, mut listed) = (Vec::new(), Vec::new());
    loop {
        match a.from_agent.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Stdout(b)),
            })) => out.extend(b),
            Some(Ok(SessionFrame {
                frame: Some(Frame::Snapshots(r)),
            })) => listed = r.snapshots,
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => return Ok((out, listed)),
            Some(Ok(SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => return Err(format!("node {node}: {}", e.message)),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.message().to_owned()),
            None => return Err(format!("node {node}'s stream ended early")),
        }
    }
}

async fn run(
    hub: &Arc<Hub>,
    key: &ObjectKey,
    node: &str,
    target: BackupTargetAccess,
    snapshot: &str,
    base: &str,
    keep: u32,
) -> Result<Ran, String> {
    let mut a = open(
        hub,
        node,
        &key.namespace,
        Action::BackupRun(legion_proto::v1::BackupRun {
            volume: key.name.clone(),
            target: Some(target),
            snapshot: snapshot.to_owned(),
            base: base.to_owned(),
            keep,
        }),
    )
    .await?;
    let (out, _) = said(&mut a, node).await?;
    Ok(serde_json::from_slice(&out).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use legion_api::quantity::Duration as D;

    use super::*;

    const HOUR: i64 = 3_600_000;

    fn hourly() -> BackupSchedule {
        BackupSchedule {
            target: "offsite".into(),
            every: D::from_secs(3600),
            keep: 24,
        }
    }

    fn taken(at: i64, from: &str) -> BackupStatus {
        BackupStatus {
            target: "offsite".into(),
            last: "legion-bk-offsite-x".into(),
            from: from.into(),
            taken_ms: Some(at),
            ..Default::default()
        }
    }

    #[test]
    fn a_backup_is_due_once_its_interval_has_passed() {
        let s = hourly();
        assert!(due(&s, None, 0, None), "none yet: at once");
        assert!(!due(&s, Some(&taken(0, "n1")), HOUR - 1, None));
        assert!(due(&s, Some(&taken(0, "n1")), HOUR, None));
        // Another target's status says nothing of this one's.
        let other = BackupStatus {
            target: "local".into(),
            ..taken(0, "n1")
        };
        assert!(due(&s, Some(&other), 1, None));
    }

    #[test]
    fn a_failed_backup_waits_its_backoff() {
        let s = hourly();
        let failed = BackupStatus {
            failures: 2,
            ..taken(0, "n1")
        };
        let wait = backoff(2).as_millis() as i64;
        assert!(!due(&s, Some(&failed), 2 * HOUR, Some(2 * HOUR - wait + 1)));
        assert!(due(&s, Some(&failed), 2 * HOUR, Some(2 * HOUR - wait)));
    }

    #[test]
    fn the_base_is_the_last_taken_where_the_data_is_now() {
        let s = hourly();
        assert_eq!(base(&s, Some(&taken(0, "n1")), "n1"), "legion-bk-offsite-x");
        assert_eq!(base(&s, Some(&taken(0, "n1")), "n2"), "", "moved: whole");
        assert_eq!(base(&s, None, "n1"), "");
    }

    #[test]
    fn late_is_twice_the_interval_or_a_first_that_keeps_failing() {
        let s = hourly();
        assert!(!late(&s, Some(&taken(0, "n1")), 2 * HOUR));
        assert!(late(&s, Some(&taken(0, "n1")), 2 * HOUR + 1));
        let first_failing = BackupStatus {
            target: "offsite".into(),
            failures: 1,
            ..Default::default()
        };
        assert!(late(&s, Some(&first_failing), 0));
        assert!(!late(&s, None, 100 * HOUR));
    }
}
