//! A replicated database's log archiving (DESIGN.md 6.9,
//! `replication.archive`): the leader has the node holding a member's
//! Volume ship the files its plugin left in the spool (PostgreSQL's
//! finished WAL files) to an S3 target, under `<prefix>/<namespace>.<service>/wal/`,
//! and now and then prune the target to `keep`.
//!
//! Each file goes once, by name: one the target holds already, as every
//! member spools what it has and a replica's are a primary's too, is not
//! sent again. A file leaves the spool only once the target holds it.

use std::path::{Path, PathBuf};

use legion_api::backup::BackupTargetSpec;
use legion_proto::v1::ArchiveRun;
use legion_proto::v1::session_frame::Frame;
use tonic::Status;

/// Where shipped files go, and what it holds.
pub(crate) trait Store: Send + Sync {
    /// The size of `name` there, if it is there.
    fn size_of(&self, name: &str) -> Result<Option<u64>, String>;
    fn put(&self, name: &str, data: Vec<u8>) -> Result<(), String>;
    /// Each file there, with when it was stored (Unix ms).
    fn dated(&self) -> Result<Vec<(String, i64)>, String>;
    fn remove(&self, name: &str) -> Result<(), String>;
}

impl Store for crate::s3::S3 {
    fn size_of(&self, name: &str) -> Result<Option<u64>, String> {
        crate::s3::S3::size_of(self, name)
    }
    fn put(&self, name: &str, data: Vec<u8>) -> Result<(), String> {
        crate::s3::S3::put(self, name, data)
    }
    fn dated(&self) -> Result<Vec<(String, i64)>, String> {
        crate::s3::S3::dated(self)
    }
    fn remove(&self, name: &str) -> Result<(), String> {
        crate::s3::S3::remove(self, name)
    }
}

/// What a run did.
#[derive(Debug, Default, PartialEq, serde::Serialize)]
pub(crate) struct Shipped {
    pub shipped: u32,
    pub bytes: u64,
    /// The newest file shipped, or held already, by name.
    pub last: String,
    /// Files left in the spool.
    pub pending: u32,
    pub pruned: u32,
}

/// Ships `spool`'s finished files to `store`, oldest name first, each
/// removed once held there; with `prune`, removes what `store` has held
/// longer than `keep_ms` before `now_ms`.
pub(crate) fn ship(
    spool: &Path,
    store: &dyn Store,
    prune: bool,
    keep_ms: i64,
    now_ms: i64,
) -> Result<Shipped, String> {
    let mut names: Vec<String> = match std::fs::read_dir(spool) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| !n.ends_with(".part") && !n.starts_with('.'))
            .collect(),
        // None yet: the plugin makes it at the member's next start.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("{}: {e}", spool.display())),
    };
    names.sort();
    let mut done = Shipped::default();
    for (i, name) in names.iter().enumerate() {
        let path = spool.join(name);
        let data = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let size = data.len() as u64;
        let held = store.size_of(name)?;
        if held.is_none() {
            if let Err(e) = store.put(name, data) {
                done.pending = (names.len() - i) as u32;
                return Err(format!("{name}: {e} ({} shipped first)", done.shipped));
            }
            done.shipped += 1;
            done.bytes += size;
        } else if held != Some(size) {
            // Another member's of the same name: a timeline's last file,
            // cut where a replica was promoted. The first stays.
            tracing::info!(file = %name, "the target holds another of this name; kept");
        }
        std::fs::remove_file(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        done.last = name.clone();
    }
    if prune {
        for (name, at) in store.dated()? {
            if at > 0 && at + keep_ms < now_ms {
                store.remove(&name)?;
                done.pruned += 1;
            }
        }
    }
    Ok(done)
}

/// The host path of `rel` in the Volume's dataset `ds`, mounted.
fn host_path(sys: &dyn legion_sys::Sys, ds: &str, rel: &str) -> Result<PathBuf, String> {
    if rel.split('/').any(|c| c == "..") || rel.starts_with('/') {
        return Err(format!("{rel:?}: a path within the Volume"));
    }
    let mount = sys
        .zfs_list(ds)
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|d| d.name == ds)
        .and_then(|d| d.mountpoint)
        .ok_or_else(|| format!("{ds} is not mounted here"))?;
    Ok(Path::new(&mount).join(rel))
}

fn frame(f: Frame) -> legion_proto::v1::SessionFrame {
    legion_proto::v1::SessionFrame { frame: Some(f) }
}

/// A manager's session shipping a member's spool.
pub(crate) async fn run_session(
    v: &crate::transfer::Volumes,
    namespace: &str,
    o: ArchiveRun,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &o.volume)?;
    let t = o
        .target
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("no target"))?;
    let spec: BackupTargetSpec = serde_json::from_str(&t.spec_json)
        .map_err(|e| Status::invalid_argument(format!("target {}: {e}", t.name)))?;
    let Some(s3) = &spec.s3 else {
        return Err(Status::failed_precondition(format!(
            "target {}: a database's log goes to an s3 target",
            t.name
        )));
    };
    if o.of.is_empty() || o.of.contains('/') {
        return Err(Status::invalid_argument("of: <namespace>.<service>"));
    }
    let rt = tokio::runtime::Handle::current();
    let store = crate::s3::S3::new(s3, &t.key, &format!("{}/wal", o.of), rt)
        .map_err(Status::failed_precondition)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let keep_ms = (o.keep_seconds as i64).saturating_mul(1000);
    let shipped = crate::task::blocking(&v.sys, move |sys| {
        let spool = host_path(sys, &ds, &o.path)?;
        ship(&spool, &store, o.prune, keep_ms, now_ms)
    })
    .await
    .map_err(Status::failed_precondition)?;
    let json = serde_json::to_vec(&shipped).map_err(|e| Status::internal(e.to_string()))?;
    let _ = tx.send(frame(Frame::Stdout(json))).await;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// A target in memory: each file's bytes and when it was stored.
    #[derive(Default)]
    struct Held(Mutex<BTreeMap<String, (Vec<u8>, i64)>>, Mutex<bool>);

    impl Store for Held {
        fn size_of(&self, name: &str) -> Result<Option<u64>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(name)
                .map(|(d, _)| d.len() as u64))
        }
        fn put(&self, name: &str, data: Vec<u8>) -> Result<(), String> {
            if *self.1.lock().unwrap() {
                return Err("refused".into());
            }
            self.0.lock().unwrap().insert(name.into(), (data, 1_000));
            Ok(())
        }
        fn dated(&self) -> Result<Vec<(String, i64)>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|(n, (_, at))| (n.clone(), *at))
                .collect())
        }
        fn remove(&self, name: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(name);
            Ok(())
        }
    }

    #[test]
    fn a_spool_is_shipped_once_each_emptied_and_the_target_pruned() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path();
        for (n, d) in [
            ("000000010000000000000002", "bb"),
            ("000000010000000000000001", "a"),
        ] {
            std::fs::write(spool.join(n), d).unwrap();
        }
        std::fs::write(spool.join("000000010000000000000003.part"), "half").unwrap();
        let held = Held::default();
        // One the target has already, as another member shipped it.
        held.0
            .lock()
            .unwrap()
            .insert("000000010000000000000001".into(), (b"a".to_vec(), 500));
        let done = ship(spool, &held, false, 0, 0).unwrap();
        assert_eq!(
            done,
            Shipped {
                shipped: 1,
                bytes: 2,
                last: "000000010000000000000002".into(),
                pending: 0,
                pruned: 0,
            }
        );
        let left: Vec<String> = std::fs::read_dir(spool)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            left,
            ["000000010000000000000003.part"],
            "written yet: stays"
        );

        // The target refusing: the file stays for the next run.
        std::fs::write(spool.join("000000010000000000000004"), "d").unwrap();
        *held.1.lock().unwrap() = true;
        let e = ship(spool, &held, false, 0, 0).unwrap_err();
        assert!(e.contains("refused"), "{e}");
        assert!(spool.join("000000010000000000000004").exists());
        *held.1.lock().unwrap() = false;

        // Pruned: what was stored longer ago than keep.
        let done = ship(spool, &held, true, 600, 1_200).unwrap();
        assert_eq!((done.shipped, done.pruned), (1, 1));
        assert_eq!(
            held.0.lock().unwrap().keys().cloned().collect::<Vec<_>>(),
            ["000000010000000000000002", "000000010000000000000004"]
        );
        // No spool yet: nothing to do.
        let none = ship(&dir.path().join("none"), &held, false, 0, 0).unwrap();
        assert_eq!(none, Shipped::default());
    }

    #[test]
    fn s3_times_are_read_as_unix_milliseconds() {
        assert_eq!(crate::s3::iso_ms("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(
            crate::s3::iso_ms("2026-11-25T04:05:06.123Z"),
            Some(1_795_579_506_123)
        );
        assert_eq!(
            crate::s3::iso_ms("2000-03-01T00:00:00Z"),
            Some(951_868_800_000)
        );
        assert_eq!(crate::s3::iso_ms("nonsense"), None);
    }
}
