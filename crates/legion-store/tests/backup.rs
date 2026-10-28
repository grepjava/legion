//! Backing up the store and restoring a manager from the backup, or from its
//! own store, as a cluster of one that others join again.

mod common;

use std::collections::BTreeMap;

use common::*;
use legion_api::Kind;
use legion_store::{Backup, Outcome, Precondition, restore, set_aside};
use openraft::BasicNode;

/// Stops node `id` and replaces its store with `backup`, restored.
async fn restore_node(c: &mut Cluster, id: u64, backup: &Backup) {
    c.stop(id).await;
    let dir = c.dir(id);
    set_aside(&dir, "old").unwrap();
    restore(&dir, id, "n1:7470", backup).unwrap();
    assert!(
        restore(&dir, id, "n1:7470", backup).is_err(),
        "over a store"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_restores_a_cluster_of_one_that_others_join() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.leader(None).await;
    let last = put_many(c.store(leader), "a", 30).await;
    c.applied_everywhere(last).await;
    let backup = c.store(leader).backup().unwrap();
    assert_eq!(backup.version, last);
    assert_eq!(
        backup.managers.keys().copied().collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(backup.objects.len(), 30);
    // Through JSON, as the file holds it.
    let backup: Backup = serde_json::from_slice(&serde_json::to_vec(&backup).unwrap()).unwrap();

    // Written after the backup: lost by restoring it.
    put_many(c.store(leader), "b", 5).await;
    for id in [2, 3] {
        c.stop(id).await;
    }
    restore_node(&mut c, 1, &backup).await;
    let store = c.start(1).await;
    assert_eq!(c.leader(None).await, 1, "the only voter leads");
    let services = store.list(Kind::Service, None).unwrap();
    assert_eq!(services.objects, backup.objects, "versions and uids kept");
    let voters: Vec<u64> = store
        .raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(voters, [1]);
    match put(&store, service("c", 1), Precondition::None).await {
        Outcome::Written(o) => assert!(o.metadata.version > backup.version),
        other => panic!("{other:?}"),
    }
    assert!(store.knows(Some(1)) && !store.knows(Some(2)) && !store.knows(None));

    // A new manager joins from an empty store: the restored snapshot, as
    // the log before it is gone, and then the log.
    let joiner = c.start(4).await;
    assert!(joiner.knows(Some(1)), "joining: knows no cluster yet");
    store
        .raft
        .add_learner(4, BasicNode::default(), true)
        .await
        .unwrap();
    store.raft.change_membership([1, 4], false).await.unwrap();
    let last = put_many(&store, "d", 3).await;
    c.applied_everywhere(last).await;
    c.assert_converged(Kind::Service);
    assert_eq!(
        joiner.list(Kind::Service, None).unwrap().objects.len(),
        30 + 1 + 3
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manager_restarts_the_cluster_from_its_own_store() {
    let mut c = Cluster::new(config());
    for id in 1..=3 {
        c.start(id).await;
    }
    c.initialize(&[1, 2, 3]).await;
    let leader = c.leader(None).await;
    let last = put_many(c.store(leader), "a", 20).await;
    c.applied_everywhere(last).await;
    // Two managers are lost; the third has no quorum.
    for id in [2, 3] {
        c.stop(id).await;
    }
    c.stop(1).await;
    let own = Backup::of_store(&c.dir(1)).unwrap();
    assert!(own.version >= last);
    assert_eq!(own.objects.len(), 20);
    assert!(Backup::of_store(&c.dir(1).join("missing")).is_err());
    c.start(1).await;
    restore_node(&mut c, 1, &own).await;
    let store = c.start(1).await;
    assert_eq!(c.leader(None).await, 1);
    let last = put_many(&store, "b", 1).await;
    assert!(last > own.version);
    assert_eq!(store.list(Kind::Service, None).unwrap().objects.len(), 21);

    // Set aside again, under the same suffix: the first one stays.
    drop(store);
    c.stop(1).await;
    let dir = c.dir(1);
    assert!(Backup::of_store(&dir).is_ok());
    assert_eq!(
        set_aside(&dir, "old").unwrap(),
        Some(dir.join("store.redb.old-1"))
    );
    assert!(dir.join("store.redb.old").exists());
    assert_eq!(
        set_aside(&dir, "old").unwrap(),
        None,
        "nothing left to move"
    );

    // A backup this build cannot read.
    let mut other = own.clone();
    other.format += 1;
    assert!(other.check().is_err());
    let none = Backup {
        version: 0,
        managers: BTreeMap::new(),
        objects: Vec::new(),
        ..own
    };
    assert!(none.check().is_err());
}
