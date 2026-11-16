//! The replicator (DESIGN.md §6.4): while this manager leads, it keeps each
//! ZFS Volume whose spec asks for a `replica` copied to another node every
//! so often. The node holding the data snapshots it and sends what changed
//! since the replica's last snapshot (all of it the first time, or once
//! the data has moved), and the replica's node receives it, unmounted;
//! both go through this manager's sessions, as a migration's do. A
//! replica's node that changes leaves its old copy among the Volume's
//! leftovers, which the migrator drops. When the node holding the data is
//! removed, the Volume carries on from the replica (`volumes::mark_lost`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use legion_api::node::{Availability, NodeSpec, NodeState, NodeStatus};
use legion_api::volume::{ReplicaStatus, VolumeDriver, VolumeSpec, VolumeStatus};
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{ReplicaReceive, ReplicaSend, SessionOffer, SessionOpen};
use legion_store::Store;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::migrate::{backoff, pipe, update};
use crate::session::Hub;

/// How often the Volumes are looked at.
const PERIOD: Duration = Duration::from_secs(1);

/// What a copy failing with this says: the base is gone, so whole next.
const FULL_NEEDED: &str = "a full copy is needed";

/// A node, as the replicator weighs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub name: String,
    /// Ready and heard from: a copy goes to or from it now.
    pub ready: bool,
    /// May take a replica it does not keep yet: ready, active, and its
    /// host has what Legion requires.
    pub usable: bool,
}

/// What to do about one Volume's replica now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    Nothing,
    /// Write the status so.
    Write(Box<VolumeStatus>),
    /// Copy from the node holding the data to the replica's.
    Copy {
        from: String,
        to: String,
        base: String,
        snapshot: String,
    },
}

/// What a Volume's replica needs, by its spec and status, the nodes, and
/// the time; `waiting` while a failed copy's backoff runs.
pub(crate) fn step(
    spec: &VolumeSpec,
    status: &VolumeStatus,
    nodes: &[Candidate],
    now_ms: i64,
    waiting: bool,
) -> Step {
    let node = |n: &str| nodes.iter().find(|c| c.name == n);
    let Some(want) = spec
        .replica
        .as_ref()
        .filter(|_| spec.driver == VolumeDriver::Zfs)
    else {
        // No longer asked for: its copy goes with the leftovers.
        return match &status.replica {
            Some(r) => Step::Write(Box::new(drop_replica(status, &r.node))),
            None => Step::Nothing,
        };
    };
    let holder = status.node.as_str();
    if holder.is_empty() || !status.lost.is_empty() || status.migration.is_some() {
        return Step::Nothing;
    }
    let current = status.replica.as_ref();
    // Where the replica is kept: the spec's node, or the one it is on, or
    // the first usable one; never where the data is, nor where an old
    // copy is still to drop.
    let free = |n: &str| n != holder && !status.leftovers.iter().any(|l| l == n);
    let target = match &want.node {
        Some(n) => Some(n.clone()).filter(|n| free(n)),
        None => current
            .map(|r| r.node.clone())
            .filter(|n| free(n) && node(n).is_some())
            .or_else(|| {
                nodes
                    .iter()
                    .find(|c| c.usable && free(&c.name))
                    .map(|c| c.name.clone())
            }),
    };
    let Some(target) = target else {
        let why = match &want.node {
            Some(n) if n == holder => format!("{n} holds the data: the replica is kept elsewhere"),
            Some(n) => format!("{n} is still dropping an old copy"),
            None => "no other node may keep it".to_owned(),
        };
        return match current {
            Some(r) if r.message == why => Step::Nothing,
            _ => Step::Write(Box::new(VolumeStatus {
                replica: Some(ReplicaStatus {
                    message: why,
                    ..current.cloned().unwrap_or_default()
                }),
                ..status.clone()
            })),
        };
    };
    match current {
        Some(r) if r.node == target => {}
        // Another node: the old copy goes, a new one starts whole.
        Some(r) => {
            let mut s = drop_replica(status, &r.node);
            s.replica = Some(ReplicaStatus {
                node: target,
                ..Default::default()
            });
            return Step::Write(Box::new(s));
        }
        None => {
            return Step::Write(Box::new(VolumeStatus {
                replica: Some(ReplicaStatus {
                    node: target,
                    ..Default::default()
                }),
                ..status.clone()
            }));
        }
    }
    let r = current.expect("matched");
    let ready = |n: &str| node(n).is_some_and(|c| c.ready);
    let incremental = !r.snapshot.is_empty() && r.from == holder;
    let due = !incremental
        || r.taken_ms
            .is_none_or(|t| now_ms - t >= want.every().as_millis() as i64);
    if !due || waiting || !ready(holder) || !ready(&target) {
        return Step::Nothing;
    }
    Step::Copy {
        from: holder.to_owned(),
        to: target,
        base: if incremental {
            r.snapshot.clone()
        } else {
            String::new()
        },
        snapshot: format!("legion-replica-{now_ms}"),
    }
}

/// `status` without its replica, whose copy on `node` joins the leftovers.
fn drop_replica(status: &VolumeStatus, node: &str) -> VolumeStatus {
    let mut s = status.clone();
    s.replica = None;
    if node != s.node && !node.is_empty() && !s.leftovers.iter().any(|l| l == node) {
        s.leftovers.push(node.to_owned());
    }
    s
}

/// The cluster's nodes, as [`step`] weighs them.
fn candidates(store: &Store) -> Option<Vec<Candidate>> {
    let list = store
        .list(Kind::Node, None)
        .map_err(|e| tracing::warn!("reading nodes: {e}"))
        .ok()?;
    let mut nodes: Vec<Candidate> = list
        .objects
        .iter()
        .map(|o| {
            let status: NodeStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
            let spec: NodeSpec = legion_api::parse(&o.spec).unwrap_or_default();
            let ready = status.state == NodeState::Ready;
            Candidate {
                name: o.metadata.name.clone(),
                ready,
                usable: ready
                    && spec.availability == Availability::Active
                    && status.host_blocked(&spec).is_none(),
            }
        })
        .collect();
    nodes.sort_by(|a, b| a.name.cmp(&b.name));
    Some(nodes)
}

/// Keeps replicas copied while this manager leads; ends with the
/// leadership (the caller's select).
pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>) {
    let mut copies: JoinSet<(ObjectKey, ReplicaStatus, Result<(), String>)> = JoinSet::new();
    let mut running: HashMap<ObjectKey, ()> = HashMap::new();
    // Failures in a row, and when the last was.
    let mut failed: HashMap<ObjectKey, (u32, Instant)> = HashMap::new();
    loop {
        while let Some(done) = copies.try_join_next() {
            let Ok((key, taken, result)) = done else {
                continue;
            };
            running.remove(&key);
            let r = match result {
                Ok(()) => {
                    failed.remove(&key);
                    tracing::debug!(volume = %key, to = %taken.node, "replica copied");
                    update(store, &key, |s| match &mut s.replica {
                        Some(now) if now.node == taken.node && s.node == taken.from => {
                            *now = taken.clone();
                            true
                        }
                        _ => false,
                    })
                    .await
                }
                Err(e) => {
                    let f = failed.entry(key.clone()).or_insert((0, Instant::now()));
                    f.0 += 1;
                    f.1 = Instant::now();
                    tracing::warn!(volume = %key, to = %taken.node, attempt = f.0, "copying the replica: {e}");
                    let why = format!(
                        "{e} (attempt {}; the next in {}s)",
                        f.0,
                        backoff(f.0).as_secs()
                    );
                    let whole = e.contains(FULL_NEEDED);
                    update(store, &key, |s| match &mut s.replica {
                        Some(now) if now.node == taken.node => {
                            now.message = why.clone();
                            if whole {
                                now.snapshot.clear();
                            }
                            true
                        }
                        _ => false,
                    })
                    .await
                }
            };
            if let Err(e) = r {
                tracing::debug!(volume = %key, "recording the replica: {e}");
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
        let Some(nodes) = candidates(store) else {
            tokio::time::sleep(PERIOD).await;
            continue;
        };
        let now = crate::console::now_ms();
        for v in volumes {
            let key = v.key();
            if running.contains_key(&key) {
                continue;
            }
            let Ok(spec) = legion_api::parse::<VolumeSpec>(&v.spec) else {
                continue;
            };
            let status: VolumeStatus = serde_json::from_value(v.status.clone()).unwrap_or_default();
            if spec.replica.is_none() && status.replica.is_none() {
                continue;
            }
            let waiting = failed
                .get(&key)
                .is_some_and(|(n, at)| at.elapsed() < backoff(*n));
            match step(&spec, &status, &nodes, now, waiting) {
                Step::Nothing => {}
                Step::Write(next) => {
                    let r = update(store, &key, |s| {
                        // Only over what it was worked out from.
                        if *s != status {
                            return false;
                        }
                        *s = (*next).clone();
                        true
                    })
                    .await;
                    if let Err(e) = r {
                        tracing::debug!(volume = %key, "the replica's status: {e}");
                    }
                }
                Step::Copy {
                    from,
                    to,
                    base,
                    snapshot,
                } => {
                    let taken = ReplicaStatus {
                        node: to.clone(),
                        snapshot: snapshot.clone(),
                        from: from.clone(),
                        taken_ms: Some(now),
                        message: String::new(),
                    };
                    let (hub, k) = (hub.clone(), key.clone());
                    copies.spawn(async move {
                        let result = copy(&hub, &k, &from, &to, &base, &snapshot).await;
                        (k, taken, result)
                    });
                    running.insert(key, ());
                }
            }
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Copies a Volume from the node holding it to its replica's node.
async fn copy(
    hub: &Arc<Hub>,
    key: &ObjectKey,
    from: &str,
    to: &str,
    base: &str,
    snapshot: &str,
) -> Result<(), String> {
    let open = |action| SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: key.namespace.clone(),
            action: Some(action),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut receiver = hub
        .offer(
            to,
            open(Action::ReplicaReceive(ReplicaReceive {
                volume: key.name.clone(),
                snapshot: snapshot.to_owned(),
                base: base.to_owned(),
            })),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let mut sender = hub
        .offer(
            from,
            open(Action::ReplicaSend(ReplicaSend {
                volume: key.name.clone(),
                snapshot: snapshot.to_owned(),
                base: base.to_owned(),
            })),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    pipe(
        &mut sender.from_agent,
        &mut receiver.from_agent,
        &receiver.to_agent,
        from,
        to,
    )
    .await
}

#[cfg(test)]
mod tests {
    use legion_api::volume::{Migration, MigrationPhase, Replica};

    use super::*;

    fn node(name: &str, ready: bool, usable: bool) -> Candidate {
        Candidate {
            name: name.into(),
            ready,
            usable,
        }
    }

    fn spec(node: Option<&str>) -> VolumeSpec {
        VolumeSpec {
            replica: Some(Replica {
                node: node.map(str::to_owned),
                every_seconds: Some(60),
            }),
            ..Default::default()
        }
    }

    fn on(node: &str) -> VolumeStatus {
        VolumeStatus {
            node: node.into(),
            ..Default::default()
        }
    }

    fn replica(node: &str, snapshot: &str, from: &str, taken_ms: Option<i64>) -> ReplicaStatus {
        ReplicaStatus {
            node: node.into(),
            snapshot: snapshot.into(),
            from: from.into(),
            taken_ms,
            message: String::new(),
        }
    }

    #[test]
    fn a_replica_goes_to_a_node_without_the_data_then_is_copied_whole() {
        let nodes = [
            node("n1", true, true),
            node("n2", true, false),
            node("n3", true, true),
        ];
        // Unplaced: nothing yet.
        assert_eq!(
            step(&spec(None), &VolumeStatus::default(), &nodes, 0, false),
            Step::Nothing
        );
        // The first usable node but the holder; n2 is paused.
        let Step::Write(s) = step(&spec(None), &on("n1"), &nodes, 0, false) else {
            panic!()
        };
        let s = *s;
        assert_eq!(s.replica, Some(replica("n3", "", "", None)));
        assert_eq!(
            step(&spec(None), &s, &nodes, 1000, false),
            Step::Copy {
                from: "n1".into(),
                to: "n3".into(),
                base: String::new(),
                snapshot: "legion-replica-1000".into()
            }
        );
        // The spec's node, paused or not.
        let Step::Write(s) = step(&spec(Some("n2")), &on("n1"), &nodes, 0, false) else {
            panic!()
        };
        let s = *s;
        assert_eq!(s.replica.unwrap().node, "n2");
    }

    #[test]
    fn copies_follow_each_other_every_so_often_from_the_last() {
        let nodes = [node("n1", true, true), node("n2", true, true)];
        let mut s = on("n1");
        s.replica = Some(replica("n2", "legion-replica-1000", "n1", Some(1000)));
        assert_eq!(step(&spec(None), &s, &nodes, 30_000, false), Step::Nothing);
        assert_eq!(
            step(&spec(None), &s, &nodes, 61_000, false),
            Step::Copy {
                from: "n1".into(),
                to: "n2".into(),
                base: "legion-replica-1000".into(),
                snapshot: "legion-replica-61000".into()
            }
        );
        // After a failure, the backoff; with a node away, a wait.
        assert_eq!(step(&spec(None), &s, &nodes, 61_000, true), Step::Nothing);
        let away = [node("n1", true, true), node("n2", false, false)];
        assert_eq!(step(&spec(None), &s, &away, 61_000, false), Step::Nothing);
        // The data moved since: whole, at once.
        s.node = "n3".into();
        let nodes = [
            node("n1", true, true),
            node("n2", true, true),
            node("n3", true, true),
        ];
        let Step::Copy { base, .. } = step(&spec(None), &s, &nodes, 2000, false) else {
            panic!()
        };
        assert_eq!(base, "");
        // Not while it moves, nor once lost.
        s.migration = Some(Migration {
            to: "n1".into(),
            phase: MigrationPhase::Copying,
            message: String::new(),
        });
        assert_eq!(step(&spec(None), &s, &nodes, 99_000, false), Step::Nothing);
    }

    #[test]
    fn a_replica_on_another_node_leaves_its_old_copy_to_drop() {
        let nodes = [
            node("n1", true, true),
            node("n2", true, true),
            node("n3", true, true),
        ];
        let mut s = on("n1");
        s.replica = Some(replica("n2", "legion-replica-1", "n1", Some(1)));
        let Step::Write(next) = step(&spec(Some("n3")), &s, &nodes, 5, false) else {
            panic!()
        };
        let next = *next;
        assert_eq!(next.replica, Some(replica("n3", "", "", None)));
        assert_eq!(next.leftovers, ["n2"]);
        // The data moved to the replica's node: another keeps it.
        let mut moved = on("n2");
        moved.replica = Some(replica("n2", "legion-replica-1", "n1", Some(1)));
        let Step::Write(next) = step(&spec(None), &moved, &nodes, 5, false) else {
            panic!()
        };
        let next = *next;
        assert_eq!(next.replica.unwrap().node, "n1");
        // No longer asked for: dropped.
        let Step::Write(next) = step(&VolumeSpec::default(), &s, &nodes, 5, false) else {
            panic!()
        };
        let next = *next;
        assert_eq!(
            (next.replica, next.leftovers),
            (None, vec!["n2".to_owned()])
        );
    }

    #[test]
    fn with_nowhere_to_keep_it_the_status_says_why_once() {
        let nodes = [node("n1", true, true)];
        let Step::Write(s) = step(&spec(None), &on("n1"), &nodes, 0, false) else {
            panic!()
        };
        let s = *s;
        assert_eq!(
            s.replica.as_ref().unwrap().message,
            "no other node may keep it"
        );
        assert_eq!(step(&spec(None), &s, &nodes, 0, false), Step::Nothing);
        let Step::Write(s) = step(&spec(Some("n1")), &on("n1"), &nodes, 0, false) else {
            panic!()
        };
        let s = *s;
        assert_eq!(
            s.replica.unwrap().message,
            "n1 holds the data: the replica is kept elsewhere"
        );
    }
}
