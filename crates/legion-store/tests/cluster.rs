//! Spike S4: the store under failure (DESIGN.md §10).

mod common;

use std::time::Duration;

use common::*;
use legion_api::{Kind, ObjectKey};
use legion_store::{Command, Event, Outcome, Precondition, WatchError};
use openraft::{BasicNode, SnapshotPolicy};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_loss_and_rejoin() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let first = c.leader(None).await;
    put_many(c.store(first), "a", 50).await;

    // Crash the leader; the other two elect a new one and keep accepting writes.
    c.stop(first).await;
    let second = c.leader(Some(first)).await;
    let last = put_many(c.store(second), "b", 50).await;

    // The old leader restarts from disk and catches up as a follower.
    c.start(first).await;
    c.applied_everywhere(last).await;
    assert_eq!(c.leader(None).await, second);
    c.assert_converged(Kind::Service);
    assert_eq!(
        c.store(first)
            .list(Kind::Service, None)
            .unwrap()
            .objects
            .len(),
        100
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_commit() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let old = c.leader(None).await;
    put_many(c.store(old), "a", 10).await;

    // Cut the leader off. A write sent to it must not commit.
    c.router.isolate(old);
    let write = Command::Put {
        object: service("lost", 1),
        precondition: Precondition::None,
    };
    let stuck = tokio::time::timeout(
        Duration::from_secs(2),
        c.store(old).raft.client_write(write),
    )
    .await;
    assert!(
        !matches!(stuck, Ok(Ok(_))),
        "isolated leader committed a write: {stuck:?}"
    );

    // The majority side elects a leader and makes progress.
    let majority: Vec<_> = (1..=3).filter(|&id| id != old).collect();
    let new = wait_for_leader_among(&c, &majority).await;
    let last = put_many(c.store(new), "b", 10).await;

    // After healing, the old leader discards its uncommitted entry.
    c.router.heal(old);
    c.applied_everywhere(last).await;
    c.assert_converged(Kind::Service);
    let lost = ObjectKey::new(Kind::Service, "default", "lost");
    for id in 1..=3 {
        assert_eq!(c.store(id).get(&lost).unwrap(), None, "node {id}");
    }
}

async fn wait_for_leader_among(c: &Cluster, ids: &[u64]) -> u64 {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        for &id in ids {
            let m = c.store(id).raft.metrics().borrow().clone();
            if m.state == openraft::ServerState::Leader {
                return id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader among {ids:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_cluster_restart() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.leader(None).await;
    let before = put_many(c.store(leader), "a", 30).await;
    c.applied_everywhere(before).await;

    for id in 1..=3 {
        c.stop(id).await;
    }
    for id in 1..=3 {
        c.start(id).await;
    }
    // State is there before any election: it was applied durably.
    for id in 1..=3 {
        assert_eq!(c.store(id).applied_version().unwrap(), before, "node {id}");
    }
    let leader = c.leader(None).await;
    let after = put_many(c.store(leader), "b", 1).await;
    assert!(
        after > before,
        "versions must keep increasing across restarts"
    );
    c.applied_everywhere(after).await;
    c.assert_converged(Kind::Service);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_node_catches_up_by_snapshot() {
    let mut c = Cluster::new(openraft::Config {
        snapshot_policy: SnapshotPolicy::LogsSinceLast(20),
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1,
        ..config()
    });
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.leader(None).await;
    let lagger = (1..=3).find(|&id| id != leader).unwrap();
    c.stop(lagger).await;

    let last = put_many(c.store(leader), "a", 100).await;
    c.store(leader)
        .raft
        .wait(Some(TIMEOUT))
        .metrics(
            |m| m.purged.is_some_and(|p| p.index > 50),
            "leader purged its log",
        )
        .await
        .unwrap();

    // The leader no longer has the entries the lagger misses, so it must send
    // a snapshot. A watcher on the lagger is told to list again.
    c.router.isolate(lagger);
    let lagging = c.start(lagger).await;
    let mut watcher = lagging
        .watch(lagging.applied_version().unwrap(), None)
        .unwrap();
    c.router.heal(lagger);
    c.applied_everywhere(last).await;
    assert!(matches!(
        tokio::time::timeout(TIMEOUT, watcher.next()).await.unwrap(),
        Err(WatchError::Reset(_))
    ));
    assert!(lagging.raft.metrics().borrow().snapshot.is_some());
    c.assert_converged(Kind::Service);
    assert_eq!(
        lagging.list(Kind::Service, None).unwrap().objects.len(),
        100
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_member_joins_from_snapshot() {
    let mut c = Cluster::new(openraft::Config {
        snapshot_policy: SnapshotPolicy::LogsSinceLast(20),
        max_in_snapshot_log_to_keep: 0,
        purge_batch_size: 1,
        ..config()
    });
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.leader(None).await;
    let last = put_many(c.store(leader), "a", 60).await;

    // A fourth manager joins as learner, then becomes a voter.
    c.start(4).await;
    let raft = &c.store(leader).raft;
    raft.add_learner(4, BasicNode::default(), true)
        .await
        .unwrap();
    raft.change_membership([1, 2, 3, 4], false).await.unwrap();
    c.applied_everywhere(last).await;
    c.assert_converged(Kind::Service);
    let voters: Vec<_> = c
        .store(4)
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(voters, vec![1, 2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_versions_and_preconditions() {
    let mut c = Cluster::new(config());
    let store = c.start(1).await;
    c.initialize(&[1]).await;
    c.leader(None).await;

    let Outcome::Written(web) = put(&store, service("web", 1), Precondition::MustNotExist).await
    else {
        panic!("create failed")
    };
    assert_eq!(web.metadata.generation, 1);
    assert_eq!(web.metadata.uid, format!("{:016x}", web.metadata.version));

    // Creating again conflicts.
    assert_eq!(
        put(&store, service("web", 1), Precondition::MustNotExist).await,
        Outcome::Conflict {
            current: Some(web.metadata.version)
        }
    );
    // Writing from a stale version conflicts.
    assert_eq!(
        put(
            &store,
            service("web", 2),
            Precondition::Version(web.metadata.version - 1)
        )
        .await,
        Outcome::Conflict {
            current: Some(web.metadata.version)
        }
    );

    // Same spec: version moves, generation does not. New spec: both move.
    let mut status_only = web.clone();
    status_only.status = serde_json::json!({ "ready": 1 });
    let Outcome::Written(v2) = put(
        &store,
        status_only,
        Precondition::Version(web.metadata.version),
    )
    .await
    else {
        panic!("update failed")
    };
    assert!(v2.metadata.version > web.metadata.version);
    assert_eq!(
        (v2.metadata.generation, v2.metadata.uid.as_str()),
        (1, web.metadata.uid.as_str())
    );
    let Outcome::Written(v3) = put(&store, service("web", 3), Precondition::None).await else {
        panic!("update failed")
    };
    assert_eq!(v3.metadata.generation, 2);

    let key = web.key();
    assert_eq!(
        delete(
            &store,
            key.clone(),
            Precondition::Version(v2.metadata.version)
        )
        .await,
        Outcome::Conflict {
            current: Some(v3.metadata.version)
        }
    );
    assert!(matches!(
        delete(&store, key.clone(), Precondition::None).await,
        Outcome::Deleted { .. }
    ));
    assert_eq!(
        delete(&store, key.clone(), Precondition::None).await,
        Outcome::NotFound
    );
    assert_eq!(store.get(&key).unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_then_watch() {
    let mut c = Cluster::new(config());
    let store = c.start(1).await;
    c.initialize(&[1]).await;
    c.leader(None).await;
    put_many(&store, "a", 3).await;

    let listing = store.list(Kind::Service, Some("default")).unwrap();
    assert_eq!(listing.objects.len(), 3);
    let mut watcher = store.watch(listing.version, Some(Kind::Service)).unwrap();

    let other_ns = legion_api::Object::new(Kind::Service, "other", "x", serde_json::Value::Null);
    put(&store, other_ns, Precondition::None).await;
    let secret = legion_api::Object::new(Kind::Secret, "default", "s", serde_json::Value::Null);
    put(&store, secret, Precondition::None).await;
    delete(
        &store,
        ObjectKey::new(Kind::Service, "default", "a-0"),
        Precondition::None,
    )
    .await;

    // The secret is filtered out; events arrive in version order.
    let ev = watcher.next().await.unwrap();
    assert!(matches!(&ev, Event::Put(o) if o.metadata.name == "x"));
    let ev2 = watcher.next().await.unwrap();
    assert!(matches!(&ev2, Event::Delete(o) if o.metadata.name == "a-0"));
    assert!(ev2.version() > ev.version());

    // A watcher can resume from any version still held in memory.
    let mut resumed = store.watch(ev.version(), None).unwrap();
    assert!(matches!(resumed.next().await.unwrap(), Event::Put(o) if o.kind == Kind::Secret));

    // After a restart the history is empty: older versions must list again.
    let applied = store.applied_version().unwrap();
    // Every Store clone keeps the database open.
    drop(store);
    c.stop(1).await;
    let store = c.start(1).await;
    assert_eq!(
        store.watch(listing.version, None).err(),
        Some(WatchError::Compacted {
            requested: listing.version,
            floor: applied
        })
    );
    assert!(store.watch(applied, None).is_ok());
}

/// Rough write throughput with every log append and apply fsynced.
/// Run with `cargo test --release -- --ignored --nocapture write_throughput`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn write_throughput() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.store(c.leader(None).await).clone();

    let n = 500;
    let start = std::time::Instant::now();
    put_many(&leader, "seq", n).await;
    let sequential = start.elapsed();

    let start = std::time::Instant::now();
    let writes = (0..n).map(|i| {
        let leader = leader.clone();
        tokio::spawn(async move {
            put(&leader, service(&format!("par-{i}"), 1), Precondition::None).await
        })
    });
    for w in writes.collect::<Vec<_>>() {
        w.await.unwrap();
    }
    let concurrent = start.elapsed();
    println!(
        "{n} sequential writes: {:.0}/s ({:.2} ms each); {n} concurrent: {:.0}/s",
        n as f64 / sequential.as_secs_f64(),
        sequential.as_secs_f64() * 1000.0 / n as f64,
        n as f64 / concurrent.as_secs_f64(),
    );
    drop(leader);
    for id in 1..=3 {
        c.stop(id).await;
    }
}
