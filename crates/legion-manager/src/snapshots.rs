//! The snapshotter (DESIGN.md §4.3, Volume snapshot schedules): while this
//! manager leads, it has the node holding each ZFS Volume with
//! `snapshots` take a schedule's snapshot once its `every` has passed
//! since the last, and prune that schedule's beyond `keep`. When each was
//! last taken is kept in the Volume's status, so a new leader carries on
//! where the old one was. Nothing is taken while the data moves, before
//! any task has used the Volume, or while its node is not ready; a failure
//! is said in the schedule's status and retried with the migrator's
//! backoff.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::node::{NodeState, NodeStatus};
use legion_api::volume::{
    ScheduleStatus, SnapshotSchedule, VolumeDriver, VolumeSpec, VolumeStatus,
};
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{SessionFrame, SessionOffer, SessionOpen, VolumeSnapshotAction};
use legion_store::Store;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

use crate::migrate::{backoff, update};
use crate::session::Hub;

/// How often the Volumes are looked at.
const PERIOD: Duration = Duration::from_secs(10);

/// The schedules of `spec` due at `now_ms`, given `status`: those never
/// taken, and those whose `every` has passed since the last; one that
/// failed waits its backoff from the last attempt, `tried_ms`.
pub(crate) fn due<'a>(
    spec: &'a VolumeSpec,
    status: &VolumeStatus,
    now_ms: i64,
    tried_ms: impl Fn(&SnapshotSchedule) -> Option<i64>,
) -> Vec<&'a SnapshotSchedule> {
    spec.snapshots
        .iter()
        .filter(|s| {
            let st = status.snapshots.iter().find(|st| st.every == s.every);
            let since_taken = st
                .and_then(|st| st.taken_ms)
                .is_none_or(|at| now_ms - at >= s.every.millis() as i64);
            let failures = st.map_or(0, |st| st.failures);
            let since_tried = failures == 0
                || tried_ms(s).is_none_or(|at| now_ms - at >= backoff(failures).as_millis() as i64);
            since_taken && since_tried
        })
        .collect()
}

/// What `status.snapshots` keeps: an entry for each schedule the spec has
/// now, none for those it no longer has.
pub(crate) fn tidy(spec: &VolumeSpec, status: &mut VolumeStatus) -> bool {
    let before = status.snapshots.len();
    status
        .snapshots
        .retain(|st| spec.snapshots.iter().any(|s| s.every == st.every));
    status.snapshots.len() != before
}

/// Whether a schedule late by twice its `every` should be said: taken
/// once at least, and not since.
pub(crate) fn late(schedule: &SnapshotSchedule, st: &ScheduleStatus, now_ms: i64) -> bool {
    st.taken_ms
        .is_some_and(|at| now_ms - at > 2 * schedule.every.millis() as i64)
}

/// The nodes ready now, by name.
fn ready_nodes(store: &Store) -> Option<HashSet<String>> {
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

type Taken = (
    ObjectKey,
    SnapshotSchedule,
    String,
    i64,
    Result<u32, String>,
);

pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>) {
    let mut runs: JoinSet<Taken> = JoinSet::new();
    let mut running: HashSet<(ObjectKey, legion_api::quantity::Duration)> = HashSet::new();
    // When each schedule was last tried, for the backoff after a failure.
    let mut tried: HashMap<(ObjectKey, legion_api::quantity::Duration), i64> = HashMap::new();
    loop {
        while let Some(done) = runs.try_join_next() {
            let Ok((key, schedule, name, at_ms, result)) = done else {
                continue;
            };
            running.remove(&(key.clone(), schedule.every));
            let r = update(store, &key, |s| {
                let i = match s.snapshots.iter().position(|st| st.every == schedule.every) {
                    Some(i) => i,
                    None => {
                        s.snapshots.push(ScheduleStatus {
                            every: schedule.every,
                            ..Default::default()
                        });
                        s.snapshots.len() - 1
                    }
                };
                let st = &mut s.snapshots[i];
                match &result {
                    Ok(kept) => {
                        st.last = name.clone();
                        st.taken_ms = Some(at_ms);
                        st.kept = *kept;
                        st.message.clear();
                        st.failures = 0;
                    }
                    Err(e) => {
                        st.failures += 1;
                        st.message = format!(
                            "{e} (attempt {}; the next in {}s)",
                            st.failures,
                            backoff(st.failures).as_secs()
                        );
                    }
                }
                true
            })
            .await;
            match (&result, r) {
                (Ok(kept), _) => {
                    tracing::debug!(volume = %key, snapshot = %name, kept, "scheduled snapshot taken")
                }
                (Err(e), _) => {
                    tracing::warn!(volume = %key, every = %schedule.every, "scheduled snapshot: {e}")
                }
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
            let Ok(spec) = legion_api::parse::<VolumeSpec>(&v.spec) else {
                continue;
            };
            let status: VolumeStatus = serde_json::from_value(v.status.clone()).unwrap_or_default();
            if status
                .snapshots
                .iter()
                .any(|st| !spec.snapshots.iter().any(|s| s.every == st.every))
            {
                let r = update(store, &key, |s| tidy(&spec, s)).await;
                if let Err(e) = r {
                    tracing::debug!(volume = %key, "tidying the schedules' status: {e}");
                }
            }
            if spec.snapshots.is_empty()
                || spec.driver != VolumeDriver::Zfs
                || status.node.is_empty()
                || status.migration.is_some()
                || !status.lost.is_empty()
                || !ready.contains(&status.node)
            {
                continue;
            }
            let tried_at = |s: &SnapshotSchedule| tried.get(&(key.clone(), s.every)).copied();
            for schedule in due(&spec, &status, now, tried_at) {
                let id = (key.clone(), schedule.every);
                if !running.insert(id.clone()) {
                    continue;
                }
                tried.insert(id, now);
                let name = schedule.name_at(now);
                let (hub, k, node, s) = (
                    hub.clone(),
                    key.clone(),
                    status.node.clone(),
                    schedule.clone(),
                );
                runs.spawn(async move {
                    let result = take(&hub, &k, &node, &s, &name).await;
                    (k, s, name, now, result)
                });
            }
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Has `node` take `name` of `key`'s data and prune its schedule; how
/// many of the schedule's it holds after.
async fn take(
    hub: &Arc<Hub>,
    key: &ObjectKey,
    node: &str,
    schedule: &SnapshotSchedule,
    name: &str,
) -> Result<u32, String> {
    let offer = SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: key.namespace.clone(),
            action: Some(Action::VolumeSnapshot(VolumeSnapshotAction {
                volume: key.name.clone(),
                op: "auto".into(),
                snapshot: name.to_owned(),
                keep: schedule.keep,
                into: String::new(),
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut a = hub
        .offer(node, offer)
        .await
        .map_err(|e| e.message().to_owned())?;
    let prefix = schedule.prefix();
    let mut kept = 0;
    loop {
        match a.from_agent.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Snapshots(r)),
            })) => {
                kept = r
                    .snapshots
                    .iter()
                    .filter(|s| s.starts_with(&prefix))
                    .count() as u32
            }
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => return Ok(kept),
            Some(Ok(SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => return Err(format!("node {node}: {}", e.message)),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.message().to_owned()),
            None => return Err(format!("node {node}'s stream ended early")),
        }
    }
}

#[cfg(test)]
mod tests {
    use legion_api::quantity::Duration as D;

    use super::*;

    const HOUR: i64 = 3_600_000;

    fn spec() -> VolumeSpec {
        VolumeSpec {
            snapshots: vec![
                SnapshotSchedule {
                    every: D::from_secs(3600),
                    keep: 24,
                },
                SnapshotSchedule {
                    every: D::from_secs(86_400),
                    keep: 30,
                },
            ],
            ..Default::default()
        }
    }

    fn taken(every_s: u64, at: i64) -> ScheduleStatus {
        ScheduleStatus {
            every: D::from_secs(every_s),
            taken_ms: Some(at),
            ..Default::default()
        }
    }

    fn everies(d: Vec<&SnapshotSchedule>) -> Vec<u64> {
        d.iter().map(|s| s.every.millis() / 1000).collect()
    }

    #[test]
    fn a_schedule_is_due_once_its_interval_has_passed() {
        let spec = spec();
        let never = |_: &SnapshotSchedule| None;
        // None taken yet: both at once.
        assert_eq!(
            everies(due(&spec, &VolumeStatus::default(), 10 * HOUR, never)),
            [3600, 86_400]
        );
        let status = VolumeStatus {
            snapshots: vec![taken(3600, 10 * HOUR), taken(86_400, 10 * HOUR)],
            ..Default::default()
        };
        assert!(due(&spec, &status, 10 * HOUR + HOUR - 1, never).is_empty());
        assert_eq!(everies(due(&spec, &status, 11 * HOUR, never)), [3600]);
        assert_eq!(
            everies(due(&spec, &status, 34 * HOUR, never)),
            [3600, 86_400]
        );
    }

    #[test]
    fn a_failed_schedule_waits_its_backoff() {
        let spec = spec();
        let status = VolumeStatus {
            snapshots: vec![ScheduleStatus {
                failures: 3,
                ..taken(3600, 0)
            }],
            ..Default::default()
        };
        let wait = backoff(3).as_millis() as i64;
        let tried = |s: &SnapshotSchedule| (s.every == D::from_secs(3600)).then_some(5 * HOUR);
        let hourly = |d: Vec<&SnapshotSchedule>| everies(d).contains(&3600);
        assert!(!hourly(due(&spec, &status, 5 * HOUR + wait - 1, tried)));
        assert!(hourly(due(&spec, &status, 5 * HOUR + wait, tried)));
        // A new leader, which never tried it, tries at once.
        assert!(hourly(due(&spec, &status, 5 * HOUR, |_| None)));
    }

    #[test]
    fn a_schedule_dropped_from_the_spec_drops_its_status() {
        let spec = spec();
        let mut status = VolumeStatus {
            snapshots: vec![taken(3600, 0), taken(600, 0)],
            ..Default::default()
        };
        assert!(tidy(&spec, &mut status));
        assert_eq!(status.snapshots, [taken(3600, 0)]);
        assert!(!tidy(&spec, &mut status));
    }

    #[test]
    fn late_is_twice_the_interval_without_a_snapshot() {
        let hourly = SnapshotSchedule {
            every: D::from_secs(3600),
            keep: 1,
        };
        assert!(!late(&hourly, &taken(3600, 0), 2 * HOUR));
        assert!(late(&hourly, &taken(3600, 0), 2 * HOUR + 1));
        assert!(
            !late(&hourly, &ScheduleStatus::default(), 100 * HOUR),
            "never taken: not yet late"
        );
    }

    #[test]
    fn names_sort_by_when_and_carry_their_schedule() {
        let hourly = SnapshotSchedule {
            every: D::from_secs(3600),
            keep: 1,
        };
        // 2026-11-22 14:30:05 UTC.
        assert_eq!(
            hourly.name_at(1_795_357_805_000),
            "legion-auto-1h-20261122T143005Z"
        );
        assert!(
            hourly
                .name_at(1_795_357_805_000)
                .starts_with(&hourly.prefix())
        );
        let daily = SnapshotSchedule {
            every: D::from_secs(86_400),
            keep: 1,
        };
        assert!(
            !hourly.name_at(0).starts_with(&daily.prefix()),
            "{}",
            daily.prefix()
        );
    }
}
