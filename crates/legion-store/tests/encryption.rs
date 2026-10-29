//! Encryption at rest: what the store writes is sealed with its key.

mod common;

use common::*;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Backup, Outcome, Precondition, StoreKey};

fn secret(name: &str, value: &str) -> Object {
    Object::new(
        Kind::Secret,
        "default",
        name,
        serde_json::json!({ "text": value }),
    )
}

/// Whether `needle` is anywhere in the store's file.
fn on_disk(c: &Cluster, needle: &str) -> bool {
    let bytes = std::fs::read(c.dir(1).join("store.redb")).unwrap();
    bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_readable_on_disk_and_only_with_the_key() {
    let key = StoreKey::generate();
    let mut c = Cluster::new(config());
    c.key = Some(key.clone());
    c.start(1).await;
    c.initialize(&[1]).await;
    c.leader(None).await;
    let written = put(
        c.store(1),
        secret("db", "hunter2-in-the-log"),
        Precondition::None,
    )
    .await;
    assert!(matches!(written, Outcome::Written(_)));
    // In the snapshot too.
    c.store(1).raft.trigger().snapshot().await.unwrap();
    c.store(1)
        .raft
        .wait(Some(TIMEOUT))
        .metrics(|m| m.snapshot.is_some(), "a snapshot")
        .await
        .unwrap();
    put_many(c.store(1), "svc", 3).await;
    c.stop(1).await;
    // Names stay readable: they are the tables' keys.
    assert!(!on_disk(&c, "hunter2-in-the-log"));

    // The same key reads it all back, through Raft and offline.
    let store = c.start(1).await;
    let got = store
        .get(&ObjectKey::new(Kind::Secret, "default", "db"))
        .unwrap()
        .unwrap();
    assert_eq!(got.spec["text"], "hunter2-in-the-log");
    drop(store);
    c.stop(1).await;
    let backup = Backup::of_store(&c.dir(1), Some(&key)).unwrap();
    assert_eq!(backup.objects.len(), 4);
    // Another key, or none, reads nothing.
    assert!(Backup::of_store(&c.dir(1), Some(&StoreKey::generate())).is_err());
    assert!(Backup::of_store(&c.dir(1), None).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_store_is_read_and_written_sealed_once_it_has_a_key() {
    let mut c = Cluster::new(config());
    c.start(1).await;
    c.initialize(&[1]).await;
    c.leader(None).await;
    put(c.store(1), secret("old", "from-before"), Precondition::None).await;
    c.stop(1).await;
    assert!(on_disk(&c, "from-before"), "written in the clear");

    let key = StoreKey::generate();
    c.key = Some(key.clone());
    let store = c.start(1).await;
    c.leader(None).await;
    let old = store
        .get(&ObjectKey::new(Kind::Secret, "default", "old"))
        .unwrap()
        .unwrap();
    assert_eq!(old.spec["text"], "from-before");
    put(
        &store,
        secret("new", "sealed-from-now-on"),
        Precondition::None,
    )
    .await;
    drop(store);
    c.stop(1).await;
    assert!(!on_disk(&c, "sealed-from-now-on"));
    assert_eq!(
        Backup::of_store(&c.dir(1), Some(&key))
            .unwrap()
            .objects
            .len(),
        2
    );
}
