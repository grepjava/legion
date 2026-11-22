//! A Volume's backups on the node holding its data (DESIGN.md §4.3,
//! Volume backups): one taken and sent to its `BackupTarget`, what the
//! target holds listed and pruned, and one restored onto a node.
//!
//! A target is a [`Remote`]: a ZFS host over SSH, which receives each
//! backup incrementally after the first, or a directory, which keeps
//! each whole as a file. A whole backup to a ZFS host starts a dataset of
//! its own there (`<dataset>/<namespace>.<volume>.<stamp>`), so the
//! backups it already holds are never written over; what changed since is
//! received into the dataset holding its base.

use std::io::{Read, Write};

use legion_api::backup::{BackupTargetSpec, DirectoryTarget, SshTarget};
use legion_sys::Sys;
use tokio::sync::Mutex;

/// A backup target as the agent reaches it. Backups are named by where
/// they are held: `<dataset>@<snapshot>` on a ZFS host, the file's path
/// in a directory.
pub(crate) trait Remote: Send + Sync {
    /// What it holds of this Volume, oldest first.
    fn list(&self) -> Result<Vec<String>, String>;
    /// Takes `snapshot` from `input`: what changed since `base` (as
    /// `list` names it) into the dataset holding it, or whole into a new
    /// one. Where it is held.
    fn receive(
        &self,
        snapshot: &str,
        base: Option<&str>,
        input: &mut dyn Read,
    ) -> Result<String, String>;
    /// Whether it takes what changed since a base.
    fn incremental(&self) -> bool;
    fn destroy(&self, held: &str) -> Result<(), String>;
    /// Writes `held`, whole, to `out`.
    fn send(&self, held: &str, out: &mut dyn Write) -> Result<(), String>;
}

/// The snapshot a held backup is: `legion-bk-offsite-…` from
/// `tank/b/shop.db.2026…@legion-bk-offsite-…` or `/mnt/b/shop.db/legion-bk-offsite-….zfs`.
pub(crate) fn snapshot_of(held: &str) -> &str {
    let name = held.rsplit(['@', '/', '\\']).next().unwrap_or(held);
    name.strip_suffix(".zfs").unwrap_or(name)
}

/// Counts what passes, for the run's bytes.
struct Counted<W> {
    inner: W,
    bytes: u64,
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(b)?;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// What a run did.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Ran {
    pub bytes: u64,
    pub whole: bool,
    pub held: Vec<String>,
}

/// Takes `ds@snapshot` and sends it to `remote`: what changed since
/// `base` when both the dataset and the target still hold it, else whole;
/// then drops the dataset's older backup snapshots (the new one is the
/// next base) and the target's beyond `keep`, oldest first. A run that
/// fails leaves no new snapshot behind.
pub(crate) fn run(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    remote: &dyn Remote,
    ds: &str,
    snapshot: &str,
    base: &str,
    keep: usize,
) -> Result<Ran, String> {
    let prefix = &snapshot[..snapshot.rfind('-').map_or(0, |i| i + 1)];
    let held = remote.list()?;
    {
        let _guard = lock.blocking_lock();
        sys.zfs_snapshot(&format!("{ds}@{snapshot}"))
            .map_err(|e| e.to_string())?;
    }
    let local = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
    let base_held = held.iter().find(|h| snapshot_of(h) == base);
    let since = (remote.incremental() && !base.is_empty() && local.iter().any(|s| s == base))
        .then_some(base_held)
        .flatten();
    let (mut from, to) = std::io::pipe().map_err(|e| format!("a pipe: {e}"))?;
    let sent = std::thread::scope(|scope| {
        let sender = scope.spawn(move || {
            let mut out = Counted {
                inner: to,
                bytes: 0,
            };
            let r = sys
                .zfs_send(&format!("{ds}@{snapshot}"), since.map(|_| base), &mut out)
                .map_err(|e| e.to_string());
            r.map(|()| out.bytes)
        });
        let taken = remote.receive(snapshot, since.map(String::as_str), &mut from);
        drop(from);
        let bytes = sender
            .join()
            .unwrap_or_else(|_| Err("sending stopped".to_owned()));
        match (bytes, taken) {
            (Ok(b), Ok(_)) => Ok(b),
            // Taken there though not all was sent: what it took goes.
            (Err(e), Ok(held)) => {
                let _ = remote.destroy(&held);
                Err(e)
            }
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    });
    let bytes = match sent {
        Ok(b) => b,
        Err(e) => {
            let _guard = lock.blocking_lock();
            let _ = sys.zfs_destroy(&format!("{ds}@{snapshot}"), false);
            return Err(e);
        }
    };
    // The dataset keeps only the newest: the next run's base.
    {
        let _guard = lock.blocking_lock();
        for s in sys.zfs_snapshots(ds).map_err(|e| e.to_string())? {
            if s.starts_with(prefix) && s != snapshot {
                sys.zfs_destroy(&format!("{ds}@{s}"), false)
                    .map_err(|e| format!("dropping {s} here: {e}"))?;
            }
        }
    }
    // The target keeps the last `keep`, by when they were taken (their
    // names sort so), whichever dataset holds them.
    let mut now = remote.list()?;
    now.sort_by(|a, b| snapshot_of(a).cmp(snapshot_of(b)));
    let over = now.len().saturating_sub(keep.max(1));
    for old in now.drain(..over) {
        remote.destroy(&old)?;
    }
    Ok(Ran {
        bytes,
        whole: since.is_none(),
        held: now,
    })
}

/// Restores `held` from `remote` as the dataset `ds` on this node, which
/// must not have it, and mounts it.
pub(crate) fn restore(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    remote: &dyn Remote,
    ds: &str,
    held: &str,
) -> Result<(), String> {
    crate::transfer::make_room(sys, lock, ds)?;
    let (mut from, to) = std::io::pipe().map_err(|e| format!("a pipe: {e}"))?;
    std::thread::scope(|scope| {
        let sender = scope.spawn(move || {
            let mut to = to;
            remote.send(held, &mut to)
        });
        let taken = crate::transfer::restore(sys, lock, ds, &mut from);
        drop(from);
        let sent = sender
            .join()
            .unwrap_or_else(|_| Err("sending stopped".to_owned()));
        sent.and(taken)
    })?;
    // The backup's snapshot is not kept on the restored data.
    let _guard = lock.blocking_lock();
    let snap = snapshot_of(held);
    if sys
        .zfs_snapshots(ds)
        .map_err(|e| e.to_string())?
        .iter()
        .any(|s| s == snap)
    {
        sys.zfs_destroy(&format!("{ds}@{snap}"), false)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The target `spec` reaches, for the Volume `of` (`<namespace>.<volume>`).
pub(crate) fn remote(
    spec: &BackupTargetSpec,
    key: &[u8],
    of: &str,
) -> Result<Box<dyn Remote>, String> {
    match (&spec.ssh, &spec.directory) {
        (Some(s), _) => Ok(Box::new(Ssh::new(s, key, of)?)),
        (None, Some(d)) => Ok(Box::new(Directory::new(d, of))),
        (None, None) => Err("the target names neither ssh nor a directory".to_owned()),
    }
}

/// A directory on this node: each backup a whole stream in a file.
pub(crate) struct Directory {
    dir: std::path::PathBuf,
}

impl Directory {
    pub(crate) fn new(d: &DirectoryTarget, of: &str) -> Directory {
        Directory {
            dir: std::path::Path::new(&d.path).join(of),
        }
    }
}

impl Remote for Directory {
    fn list(&self) -> Result<Vec<String>, String> {
        let mut found: Vec<String> = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path().to_string_lossy().into_owned())
                .filter(|p| p.ends_with(".zfs"))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(format!("reading {}: {e}", self.dir.display())),
        };
        found.sort_by(|a, b| snapshot_of(a).cmp(snapshot_of(b)));
        Ok(found)
    }
    fn incremental(&self) -> bool {
        false
    }
    fn receive(
        &self,
        snapshot: &str,
        _: Option<&str>,
        input: &mut dyn Read,
    ) -> Result<String, String> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| format!("making {}: {e}", self.dir.display()))?;
        let path = self.dir.join(format!("{snapshot}.zfs"));
        let part = self.dir.join(format!("{snapshot}.zfs.part"));
        let written = (|| {
            let mut f = std::fs::File::create(&part)?;
            std::io::copy(input, &mut f)?;
            f.sync_all()?;
            std::fs::rename(&part, &path)
        })();
        if let Err(e) = written {
            let _ = std::fs::remove_file(&part);
            return Err(format!("writing {}: {e}", path.display()));
        }
        Ok(path.to_string_lossy().into_owned())
    }
    fn destroy(&self, held: &str) -> Result<(), String> {
        std::fs::remove_file(held).map_err(|e| format!("removing {held}: {e}"))
    }
    fn send(&self, held: &str, out: &mut dyn Write) -> Result<(), String> {
        let path = std::path::Path::new(held);
        if !path.starts_with(&self.dir) {
            return Err(format!("{held} is not one of this Volume's backups"));
        }
        let mut f = std::fs::File::open(path).map_err(|e| format!("opening {held}: {e}"))?;
        std::io::copy(&mut f, out)
            .map(drop)
            .map_err(|e| format!("reading {held}: {e}"))
    }
}

/// A ZFS host over SSH, its host key pinned, logged into with the key
/// from the target's Secret. The key and the known host are written to a
/// directory of the run's own, gone when it is.
pub(crate) struct Ssh {
    target: SshTarget,
    /// `<dataset>/<namespace>.<volume>`: the datasets of its chains start so.
    base: String,
    files: tempdir::Dir,
}

mod tempdir {
    /// A directory only this process's user reads, removed when dropped.
    pub(crate) struct Dir(pub std::path::PathBuf);

    impl Dir {
        pub(crate) fn new() -> Result<Dir, String> {
            // Unique to this process and moment; create() refuses one there.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let path =
                std::env::temp_dir().join(format!("legion-backup-{}-{nanos}", std::process::id()));
            #[cfg_attr(not(unix), allow(unused_mut))]
            let mut b = std::fs::DirBuilder::new();
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
            b.create(&path)
                .map_err(|e| format!("making {}: {e}", path.display()))?;
            Ok(Dir(path))
        }

        pub(crate) fn write(&self, name: &str, data: &[u8]) -> Result<std::path::PathBuf, String> {
            let path = self.0.join(name);
            let mut o = std::fs::OpenOptions::new();
            o.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
            let mut f = o
                .open(&path)
                .map_err(|e| format!("writing {}: {e}", path.display()))?;
            std::io::Write::write_all(&mut f, data)
                .map_err(|e| format!("writing {}: {e}", path.display()))?;
            Ok(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

impl Ssh {
    pub(crate) fn new(t: &SshTarget, key: &[u8], of: &str) -> Result<Ssh, String> {
        if key.is_empty() {
            return Err("the target's Secret holds no key".to_owned());
        }
        let files = tempdir::Dir::new()?;
        let mut key = key.to_vec();
        if !key.ends_with(b"\n") {
            key.push(b'\n');
        }
        files.write("key", &key)?;
        let host = if t.port == 22 {
            t.host.clone()
        } else {
            format!("[{}]:{}", t.host, t.port)
        };
        files.write(
            "known_hosts",
            format!("{host} {}\n", t.host_key.trim()).as_bytes(),
        )?;
        Ok(Ssh {
            target: t.clone(),
            base: format!("{}/{of}", t.dataset),
            files,
        })
    }

    /// `ssh … user@host <command>`, nothing asked, no other host believed.
    fn command(&self, remote: &str) -> std::process::Command {
        let mut c = std::process::Command::new("ssh");
        c.arg("-i")
            .arg(self.files.0.join("key"))
            .args(["-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes"])
            .args(["-o", "StrictHostKeyChecking=yes"])
            .arg("-o")
            .arg(format!(
                "UserKnownHostsFile={}",
                self.files.0.join("known_hosts").display()
            ))
            .args(["-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15"])
            .arg("-p")
            .arg(self.target.port.to_string())
            .arg(format!("{}@{}", self.target.user, self.target.host))
            .arg(remote);
        c
    }

    /// Runs `remote` there, its output what it says.
    fn ask(&self, remote: &str) -> Result<String, String> {
        let out = self
            .command(remote)
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|e| format!("ssh: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "{}@{}: {}: {}",
                self.target.user,
                self.target.host,
                remote,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Whether `name` (a dataset or snapshot) is one of this Volume's.
    fn ours(&self, name: &str) -> bool {
        let dataset = name.split('@').next().unwrap_or(name);
        dataset == self.base
            || dataset
                .strip_prefix(&self.base)
                .is_some_and(|r| r.starts_with('.'))
    }
}

impl Remote for Ssh {
    fn list(&self) -> Result<Vec<String>, String> {
        let said = self.ask(&format!(
            "zfs list -H -p -o name -t snapshot -s createtxg -r {} 2>/dev/null || true",
            self.target.dataset
        ))?;
        let mut found: Vec<String> = said
            .lines()
            .map(str::trim)
            .filter(|l| {
                self.ours(l)
                    && snapshot_of(l).starts_with(legion_api::backup::BackupSchedule::PREFIX)
            })
            .map(str::to_owned)
            .collect();
        found.sort_by(|a, b| snapshot_of(a).cmp(snapshot_of(b)));
        Ok(found)
    }
    fn incremental(&self) -> bool {
        true
    }
    fn receive(
        &self,
        snapshot: &str,
        base: Option<&str>,
        input: &mut dyn Read,
    ) -> Result<String, String> {
        // What changed goes where its base is; a whole one starts a
        // dataset of its own, named for when.
        let dataset = match base {
            Some(b) => b.split('@').next().unwrap_or(b).to_owned(),
            None => format!(
                "{}.{}",
                self.base,
                snapshot.rsplit('-').next().unwrap_or(snapshot)
            ),
        };
        let mut child = self
            .command(&format!("zfs receive -u {dataset}"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("ssh: {e}"))?;
        let copied = {
            let mut stdin = child.stdin.take().expect("piped");
            std::io::copy(input, &mut stdin)
        };
        let out = child.wait_with_output().map_err(|e| format!("ssh: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "{}@{}: zfs receive {dataset}: {}",
                self.target.user,
                self.target.host,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        copied.map_err(|e| format!("sending to {}: {e}", self.target.host))?;
        Ok(format!("{dataset}@{snapshot}"))
    }
    fn destroy(&self, held: &str) -> Result<(), String> {
        if !self.ours(held) || !held.contains('@') {
            return Err(format!("{held} is not one of this Volume's backups"));
        }
        self.ask(&format!("zfs destroy {held}"))?;
        // A dataset left with no backups goes too.
        let dataset = held.split('@').next().unwrap_or(held);
        let left = self.ask(&format!("zfs list -H -o name -t snapshot -d 1 {dataset}"))?;
        if left.trim().is_empty() {
            self.ask(&format!("zfs destroy {dataset}"))?;
        }
        Ok(())
    }
    fn send(&self, held: &str, out: &mut dyn Write) -> Result<(), String> {
        if !self.ours(held) || !held.contains('@') {
            return Err(format!("{held} is not one of this Volume's backups"));
        }
        let mut child = self
            .command(&format!("zfs send {held}"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("ssh: {e}"))?;
        let copied = {
            let mut stdout = child.stdout.take().expect("piped");
            std::io::copy(&mut stdout, out)
        };
        let done = child.wait_with_output().map_err(|e| format!("ssh: {e}"))?;
        if !done.status.success() {
            return Err(format!(
                "{}@{}: zfs send {held}: {}",
                self.target.user,
                self.target.host,
                String::from_utf8_lossy(&done.stderr).trim()
            ));
        }
        copied.map(drop).map_err(|e| format!("taking {held}: {e}"))
    }
}

/// The target an action names, reached for the Volume `of`.
fn reach(
    t: Option<&legion_proto::v1::BackupTargetAccess>,
    of: &str,
) -> Result<Box<dyn Remote>, tonic::Status> {
    let t = t.ok_or_else(|| tonic::Status::invalid_argument("no target"))?;
    let spec: BackupTargetSpec = serde_json::from_str(&t.spec_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("target {}: {e}", t.name)))?;
    remote(&spec, &t.key, of).map_err(tonic::Status::failed_precondition)
}

fn frame(f: legion_proto::v1::session_frame::Frame) -> legion_proto::v1::SessionFrame {
    legion_proto::v1::SessionFrame { frame: Some(f) }
}

/// A manager's session running a backup of a Volume on this node.
pub(crate) async fn run_session(
    v: &crate::transfer::Volumes,
    namespace: &str,
    o: legion_proto::v1::BackupRun,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), tonic::Status> {
    use legion_proto::v1::session_frame::Frame;
    let ds = v.dataset(namespace, &o.volume)?;
    let remote = reach(o.target.as_ref(), &format!("{namespace}.{}", o.volume))?;
    if !o
        .snapshot
        .starts_with(legion_api::backup::BackupSchedule::PREFIX)
    {
        return Err(tonic::Status::invalid_argument(
            "a backup's snapshot is named legion-bk-<target>-<time>",
        ));
    }
    let lock = v.lock.clone();
    let ran = crate::task::blocking(&v.sys, move |sys| {
        run(
            sys,
            &lock,
            remote.as_ref(),
            &ds,
            &o.snapshot,
            &o.base,
            o.keep as usize,
        )
    })
    .await
    .map_err(tonic::Status::failed_precondition)?;
    let json = serde_json::to_vec(&ran).map_err(|e| tonic::Status::internal(e.to_string()))?;
    let _ = tx.send(frame(Frame::Stdout(json))).await;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// A manager's session listing what a target holds of a Volume.
pub(crate) async fn list_session(
    v: &crate::transfer::Volumes,
    namespace: &str,
    o: legion_proto::v1::BackupList,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), tonic::Status> {
    use legion_proto::v1::session_frame::Frame;
    let remote = reach(o.target.as_ref(), &format!("{namespace}.{}", o.volume))?;
    let held = crate::task::blocking(&v.sys, move |_| remote.list())
        .await
        .map_err(tonic::Status::failed_precondition)?;
    let _ = tx
        .send(frame(Frame::Snapshots(
            legion_proto::v1::VolumeSnapshotResult { snapshots: held },
        )))
        .await;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// A manager's session restoring a backup onto this node.
pub(crate) async fn restore_session(
    v: &crate::transfer::Volumes,
    namespace: &str,
    o: legion_proto::v1::BackupRestore,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), tonic::Status> {
    use legion_proto::v1::session_frame::Frame;
    let ds = v.dataset(namespace, &o.volume)?;
    let remote = reach(o.target.as_ref(), &o.of)?;
    let lock = v.lock.clone();
    crate::task::blocking(&v.sys, move |sys| {
        restore(sys, &lock, remote.as_ref(), &ds, &o.held)
    })
    .await
    .map_err(tonic::Status::failed_precondition)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;

    use legion_sys::FakeSys;

    use super::*;

    const DS: &str = "tank/legion/volumes/shop/db";

    /// A ZFS host in memory: each held backup's stream and whether it was
    /// whole, by `<dataset>@<snapshot>`.
    #[derive(Default)]
    struct Host(StdMutex<BTreeMap<String, (Vec<u8>, bool)>>);

    impl Remote for Host {
        fn list(&self) -> Result<Vec<String>, String> {
            let mut v: Vec<String> = self.0.lock().unwrap().keys().cloned().collect();
            v.sort_by(|a, b| snapshot_of(a).cmp(snapshot_of(b)));
            Ok(v)
        }
        fn incremental(&self) -> bool {
            true
        }
        fn receive(
            &self,
            snapshot: &str,
            base: Option<&str>,
            input: &mut dyn Read,
        ) -> Result<String, String> {
            let mut data = Vec::new();
            input.read_to_end(&mut data).map_err(|e| e.to_string())?;
            let dataset = match base {
                Some(b) => b.split('@').next().unwrap().to_owned(),
                None => format!("tank/b/shop.db.{}", snapshot.rsplit('-').next().unwrap()),
            };
            let held = format!("{dataset}@{snapshot}");
            self.0
                .lock()
                .unwrap()
                .insert(held.clone(), (data, base.is_none()));
            Ok(held)
        }
        fn destroy(&self, held: &str) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .remove(held)
                .map(drop)
                .ok_or("no such".into())
        }
        fn send(&self, held: &str, out: &mut dyn Write) -> Result<(), String> {
            let data = self
                .0
                .lock()
                .unwrap()
                .get(held)
                .cloned()
                .ok_or("no such")?
                .0;
            out.write_all(&data).map_err(|e| e.to_string())
        }
    }

    fn snap(n: u32) -> String {
        format!("legion-bk-offsite-20261122T{n:02}0000Z")
    }

    #[test]
    fn whole_at_first_then_what_changed_pruned_to_keep() {
        let (sys, lock, host) = (FakeSys::new(), Mutex::new(()), Host::default());
        sys.zfs_create(DS, &[]).unwrap();
        sys.write_dataset(DS, b"one");
        let first = run(&sys, &lock, &host, DS, &snap(1), "", 2).unwrap();
        assert!(first.whole);
        sys.write_dataset(DS, b"two");
        let second = run(&sys, &lock, &host, DS, &snap(2), &snap(1), 2).unwrap();
        assert!(!second.whole, "what changed since the first");
        sys.write_dataset(DS, b"three");
        let third = run(&sys, &lock, &host, DS, &snap(3), &snap(2), 2).unwrap();
        assert_eq!(
            third
                .held
                .iter()
                .map(|h| snapshot_of(h))
                .collect::<Vec<_>>(),
            [snap(2), snap(3)],
            "the oldest pruned"
        );
        assert_eq!(
            sys.zfs_snapshots(DS).unwrap(),
            [snap(3)],
            "only the next base kept here"
        );
    }

    #[test]
    fn a_base_gone_here_makes_the_next_whole_in_a_dataset_of_its_own() {
        let (sys, lock, host) = (FakeSys::new(), Mutex::new(()), Host::default());
        sys.zfs_create(DS, &[]).unwrap();
        run(&sys, &lock, &host, DS, &snap(1), "", 5).unwrap();
        // A rollback took it.
        sys.zfs_destroy(&format!("{DS}@{}", snap(1)), false)
            .unwrap();
        let r = run(&sys, &lock, &host, DS, &snap(2), &snap(1), 5).unwrap();
        assert!(r.whole);
        assert_eq!(r.held.len(), 2, "the earlier chain kept: {:?}", r.held);
    }

    #[test]
    fn a_failed_send_leaves_no_snapshot_and_the_target_as_it_was() {
        let (sys, lock, host) = (FakeSys::new(), Mutex::new(()), Host::default());
        sys.zfs_create(DS, &[]).unwrap();
        run(&sys, &lock, &host, DS, &snap(1), "", 5).unwrap();
        sys.fail_next("zfs send");
        assert!(run(&sys, &lock, &host, DS, &snap(2), &snap(1), 5).is_err());
        assert_eq!(sys.zfs_snapshots(DS).unwrap(), [snap(1)]);
        assert_eq!(host.list().unwrap().len(), 1);
    }

    #[test]
    fn a_backup_restores_onto_a_node_without_the_data() {
        let (sys, lock, host) = (FakeSys::new(), Mutex::new(()), Host::default());
        sys.zfs_create(DS, &[]).unwrap();
        sys.write_dataset(DS, b"rows at one");
        let held = run(&sys, &lock, &host, DS, &snap(1), "", 5).unwrap().held;
        let other = FakeSys::new();
        let into = "tank/legion/volumes/shop/db-back";
        restore(&other, &lock, &host, into, &held[0]).unwrap();
        assert_eq!(other.dataset_contents(into).unwrap(), b"rows at one");
        assert!(other.zfs_snapshots(into).unwrap().is_empty());
        let e = restore(&other, &lock, &host, into, &held[0]).unwrap_err();
        assert!(e.contains("has data for the volume already"), "{e}");
    }

    #[test]
    fn a_directory_keeps_each_backup_whole_as_a_file() {
        let dir = std::env::temp_dir().join(format!("legion-bk-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let d = Directory::new(
            &DirectoryTarget {
                path: dir.to_string_lossy().into_owned(),
            },
            "shop.db",
        );
        let (sys, lock) = (FakeSys::new(), Mutex::new(()));
        sys.zfs_create(DS, &[]).unwrap();
        sys.write_dataset(DS, b"v1");
        run(&sys, &lock, &d, DS, &snap(1), "", 2).unwrap();
        sys.write_dataset(DS, b"v2");
        let r = run(&sys, &lock, &d, DS, &snap(2), &snap(1), 2).unwrap();
        assert!(r.whole, "a directory holds no increments");
        run(&sys, &lock, &d, DS, &snap(3), &snap(2), 2).unwrap();
        let held = d.list().unwrap();
        assert_eq!(
            held.iter().map(|h| snapshot_of(h)).collect::<Vec<_>>(),
            [snap(2), snap(3)]
        );
        let other = FakeSys::new();
        restore(&other, &lock, &d, DS, &held[0]).unwrap();
        assert_eq!(other.dataset_contents(DS).unwrap(), b"v2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn held_backups_are_named_for_their_snapshot() {
        assert_eq!(
            snapshot_of("tank/b/shop.db.20261122T010000Z@legion-bk-o-20261122T010000Z"),
            "legion-bk-o-20261122T010000Z"
        );
        assert_eq!(
            snapshot_of("/mnt/b/shop.db/legion-bk-o-20261122T010000Z.zfs"),
            "legion-bk-o-20261122T010000Z"
        );
    }
}
