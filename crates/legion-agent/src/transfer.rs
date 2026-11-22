//! Moving a Volume's data between nodes (DESIGN.md §4.3). The leading
//! manager offers this node a session to send a Volume, and the node it
//! moves to one to receive it, and joins the two: what one sends as output
//! the other takes as input. The first part is a full copy, sent while the
//! Volume's tasks still run; the last, sent once they have stopped, is what
//! changed since. The node it left then drops its copy. The new node keeps
//! the move's snapshots, so the last part can be sent again until the
//! manager gives it the Volume; the Volume's first use there drops them.

use std::io::{Read, Write};
use std::sync::Arc;

use futures::StreamExt;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{
    ReplicaReceive, ReplicaSend, SessionFrame, VolumeExport, VolumeImport, VolumeReceive,
    VolumeSend, VolumeSnapshotAction, VolumeSnapshotResult,
};
use legion_sys::Sys;
use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};

/// The snapshot of the full copy, and of the last part.
pub const FULL: &str = "legion-move-1";
pub const LAST: &str = "legion-move-2";
/// The start of a backup's snapshot's name; each has its own, so two
/// backups at once leave each other's alone.
pub const BACKUP: &str = "legion-backup-";

/// Bytes per frame.
const CHUNK: usize = 64 << 10;

/// What a transfer needs of the node.
pub(crate) struct Volumes {
    pub sys: Arc<dyn Sys>,
    pub zfs_root: Option<String>,
    /// The node's volume lock, held by tasks making their volumes ready:
    /// held here too while snapshots and datasets change, but not while
    /// data streams, which may take long.
    pub lock: Arc<Mutex<()>>,
    /// How this node exports the iSCSI LUNs it provides.
    pub iscsi: crate::exports::IscsiConfig,
}

impl Volumes {
    pub(crate) fn dataset(&self, namespace: &str, volume: &str) -> Result<String, Status> {
        let root = self.zfs_root.as_deref().ok_or_else(|| {
            Status::failed_precondition("this node has no ZFS dataset for volumes")
        })?;
        Ok(crate::volumes::dataset(root, namespace, volume))
    }
}

/// Snapshots the Volume and sends it: all of it, or with `last` what
/// changed since the full copy.
pub(crate) async fn send(
    v: &Volumes,
    namespace: &str,
    opts: VolumeSend,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    let (out, lock) = (tx.clone(), v.lock.clone());
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        let mut out = FrameWriter::new(out);
        give(sys, &lock, &ds, opts.last, &mut out)?;
        out.flush().map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Receives what the sending node sends, until the input closes. After
/// the last part the dataset is mounted, ready for the Volume's tasks; the
/// move's snapshots stay until then (see [`crate::volumes`]).
pub(crate) async fn receive(
    v: &Volumes,
    namespace: &str,
    opts: VolumeReceive,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let lock = v.lock.clone();
    let work = crate::task::blocking(&v.sys, move |sys| {
        take(sys, &lock, &ds, opts.last, &mut FrameReader::new(rx))
    });
    let (result, ()) = tokio::join!(work, feed(inbound, data, "the sending node"));
    result.map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Input until it closes, into `data`; anything else ends the stream
/// early, which makes what reads it fail. `sender` names who sends it.
pub(crate) async fn feed(
    inbound: &mut Streaming<SessionFrame>,
    data: mpsc::Sender<Result<Vec<u8>, String>>,
    sender: &str,
) {
    loop {
        let next = match inbound.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Input(bytes)),
            })) => Ok(bytes),
            Some(Ok(SessionFrame {
                frame: Some(Frame::InputClosed(_)),
            })) => return,
            Some(Ok(SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => Err(format!("{sender} failed: {}", e.message)),
            Some(Ok(_)) => continue,
            Some(Err(e)) => Err(e.message().to_owned()),
            None => Err("the stream ended early".to_owned()),
        };
        let stop = next.is_err();
        if data.send(next).await.is_err() || stop {
            return;
        }
    }
}

/// Snapshots the Volume as its tasks run and sends all of it, for a
/// backup; the snapshot goes once sent, or once sending fails.
pub(crate) async fn export(
    v: &Volumes,
    namespace: &str,
    opts: VolumeExport,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    let (out, lock) = (tx.clone(), v.lock.clone());
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        let mut out = FrameWriter::new(out);
        backup(sys, &lock, &ds, &mut out)?;
        out.flush().map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Takes a backup's data as the Volume's on this node, which must have
/// none, and mounts it.
pub(crate) async fn import(
    v: &Volumes,
    namespace: &str,
    opts: VolumeImport,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    let (target, lock) = (ds.clone(), v.lock.clone());
    crate::task::blocking(&v.sys, move |sys| make_room(sys, &lock, &target))
        .await
        .map_err(Status::failed_precondition)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let lock = v.lock.clone();
    let work = crate::task::blocking(&v.sys, move |sys| {
        restore(sys, &lock, &ds, &mut FrameReader::new(rx))
    });
    let (result, ()) = tokio::join!(work, feed(inbound, data, "the backup's sender"));
    result.map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Refuses a node that has data for `ds` already, and makes its parent.
pub(crate) fn make_room(sys: &dyn Sys, lock: &Mutex<()>, ds: &str) -> Result<(), String> {
    let _guard = lock.blocking_lock();
    let found = sys.zfs_list(ds).map_err(|e| e.to_string())?;
    if found.iter().any(|d| d.name == ds) {
        return Err(format!("this node has data for the volume already ({ds})"));
    }
    match ds.rsplit_once('/') {
        Some((parent, _)) => sys.zfs_create(parent, &[]).map_err(|e| e.to_string()),
        None => Ok(()),
    }
}

/// Snapshots `ds` under a name of its own, writes all of it to `out` and
/// destroys the snapshot, whatever came of writing it.
fn backup(sys: &dyn Sys, lock: &Mutex<()>, ds: &str, out: &mut dyn Write) -> Result<(), String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let snap = format!("{ds}@{BACKUP}{nanos}");
    {
        let _guard = lock.blocking_lock();
        sys.zfs_snapshot(&snap).map_err(|e| e.to_string())?;
    }
    let sent = sys.zfs_send(&snap, None, out).map_err(|e| e.to_string());
    let _guard = lock.blocking_lock();
    let dropped = sys.zfs_destroy(&snap, false).map_err(|e| e.to_string());
    sent.and(dropped)
}

/// Receives a backup's stream as `ds`, drops the backup's snapshot it
/// brings and mounts it. What a failed receive left goes.
pub(crate) fn restore(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    input: &mut dyn Read,
) -> Result<(), String> {
    if let Err(e) = sys.zfs_receive(ds, input) {
        let _guard = lock.blocking_lock();
        if sys
            .zfs_list(ds)
            .is_ok_and(|found| found.iter().any(|d| d.name == ds))
        {
            let _ = sys.zfs_destroy(ds, true);
        }
        return Err(e.to_string());
    }
    let _guard = lock.blocking_lock();
    for s in sys.zfs_snapshots(ds).map_err(|e| e.to_string())? {
        if s.starts_with(BACKUP) {
            sys.zfs_destroy(&format!("{ds}@{s}"), false)
                .map_err(|e| e.to_string())?;
        }
    }
    sys.zfs_mount(ds).map_err(|e| e.to_string())
}

/// Snapshots dataset `ds` and writes it to `out`: all of it, or with
/// `last` what changed since the full copy.
fn give(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    last: bool,
    out: &mut dyn Write,
) -> Result<(), String> {
    let snap = |n: &str| format!("{ds}@{n}");
    // A snapshot left by an earlier attempt goes first.
    let fresh = |n: &str| {
        let _ = sys.zfs_destroy(&snap(n), false);
        sys.zfs_snapshot(&snap(n)).map_err(|e| e.to_string())
    };
    {
        let _guard = lock.blocking_lock();
        if last {
            fresh(LAST)?;
        } else {
            let _ = sys.zfs_destroy(&snap(LAST), false);
            fresh(FULL)?;
        }
    }
    if last {
        sys.zfs_send(&snap(LAST), Some(FULL), out)
    } else {
        sys.zfs_send(&snap(FULL), None, out)
    }
    .map_err(|e| e.to_string())
}

/// Takes into dataset `ds` what `give` wrote; after the last part, mounts
/// it.
fn take(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    last: bool,
    input: &mut dyn Read,
) -> Result<(), String> {
    let guard = lock.blocking_lock();
    if let Some((parent, _)) = ds.rsplit_once('/') {
        sys.zfs_create(parent, &[]).map_err(|e| e.to_string())?;
    }
    // A copy here from before (an attempt cut short, or the Volume's
    // last stay) is stale: the full copy takes its place.
    if !last
        && sys
            .zfs_list(ds)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|d| d.name == ds)
    {
        sys.zfs_destroy(ds, true).map_err(|e| e.to_string())?;
    }
    // The last part again (the manager did not take the first in): back
    // to the full copy, as it was before, to take it on.
    if last {
        let snaps = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
        if snaps.iter().any(|s| s == LAST) {
            sys.zfs_destroy(&format!("{ds}@{LAST}"), false)
                .map_err(|e| e.to_string())?;
        }
        if snaps.iter().any(|s| s == FULL) {
            sys.zfs_rollback(&format!("{ds}@{FULL}"))
                .map_err(|e| e.to_string())?;
        }
    }
    drop(guard);
    sys.zfs_receive(ds, input).map_err(|e| e.to_string())?;
    if last {
        let _guard = lock.blocking_lock();
        sys.zfs_mount(ds).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The start of a replica's snapshots' names (`legion-replica-<ms>`).
pub const REPLICA: &str = "legion-replica-";

/// Whether `name` is one of a replica's snapshots: [`REPLICA`] and digits,
/// so a name a user gave is never taken for one.
fn replica_snapshot(name: &str) -> bool {
    name.strip_prefix(REPLICA)
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// Snapshots the Volume and sends it to its replica's node: all of it,
/// or what changed since `base`.
pub(crate) async fn replica_send(
    v: &Volumes,
    namespace: &str,
    opts: ReplicaSend,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    if !replica_snapshot(&opts.snapshot) || !(opts.base.is_empty() || replica_snapshot(&opts.base))
    {
        return Err(Status::invalid_argument("not a replica's snapshot"));
    }
    let ds = v.dataset(namespace, &opts.volume)?;
    let (out, lock) = (tx.clone(), v.lock.clone());
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        let mut out = FrameWriter::new(out);
        replica_give(sys, &lock, &ds, &opts.base, &opts.snapshot, &mut out)?;
        out.flush().map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Receives a replica's copy, until the input closes.
pub(crate) async fn replica_receive(
    v: &Volumes,
    namespace: &str,
    opts: ReplicaReceive,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    if !replica_snapshot(&opts.snapshot) || !(opts.base.is_empty() || replica_snapshot(&opts.base))
    {
        return Err(Status::invalid_argument("not a replica's snapshot"));
    }
    let ds = v.dataset(namespace, &opts.volume)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let lock = v.lock.clone();
    let work = crate::task::blocking(&v.sys, move |sys| {
        replica_take(
            sys,
            &lock,
            &ds,
            &opts.base,
            &opts.snapshot,
            &mut FrameReader::new(rx),
        )
    });
    let (result, ()) = tokio::join!(work, feed(inbound, data, "the holding node"));
    result.map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Takes the replica's next snapshot and writes it: whole, or since
/// `base` (empty: none). The holder's other replica snapshots go first:
/// `base` is the last the replica took.
fn replica_give(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    base: &str,
    snapshot: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let snap = |n: &str| format!("{ds}@{n}");
    {
        let _guard = lock.blocking_lock();
        let snaps = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
        if !base.is_empty() && !snaps.iter().any(|s| s == base) {
            return Err(format!(
                "{} is gone here: a full copy is needed",
                snap(base)
            ));
        }
        for s in snaps.iter().filter(|s| replica_snapshot(s) && *s != base) {
            sys.zfs_destroy(&snap(s), false)
                .map_err(|e| e.to_string())?;
        }
        sys.zfs_snapshot(&snap(snapshot))
            .map_err(|e| e.to_string())?;
    }
    let since = (!base.is_empty()).then_some(base);
    sys.zfs_send(&snap(snapshot), since, out)
        .map_err(|e| e.to_string())
}

/// Takes into `ds` what `replica_give` wrote, leaving it unmounted; then
/// only `snapshot` stays of the replica's snapshots. A full copy replaces
/// what is here; an incremental one needs `base`.
fn replica_take(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    base: &str,
    snapshot: &str,
    input: &mut dyn Read,
) -> Result<(), String> {
    {
        let _guard = lock.blocking_lock();
        if let Some((parent, _)) = ds.rsplit_once('/') {
            sys.zfs_create(parent, &[]).map_err(|e| e.to_string())?;
        }
        let here = sys
            .zfs_list(ds)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|d| d.name == ds);
        if base.is_empty() {
            if here {
                sys.zfs_destroy(ds, true).map_err(|e| e.to_string())?;
            }
        } else {
            let snaps = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
            if !snaps.iter().any(|s| s == base) {
                return Err(format!(
                    "no replica at {ds}@{base} here: a full copy is needed"
                ));
            }
        }
    }
    sys.zfs_receive(ds, input).map_err(|e| e.to_string())?;
    let _guard = lock.blocking_lock();
    let snaps = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
    for s in snaps
        .iter()
        .filter(|s| replica_snapshot(s) && *s != snapshot)
    {
        sys.zfs_destroy(&format!("{ds}@{s}"), false)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Destroys this node's copy of a Volume that lives elsewhere now.
pub(crate) async fn drop_copy(
    v: &Volumes,
    namespace: &str,
    volume: &str,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, volume)?;
    let lock = v.lock.clone();
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        let _guard = lock.blocking_lock();
        let found = sys.zfs_list(&ds).map_err(|e| e.to_string())?;
        if found.iter().any(|d| d.name == ds) {
            sys.zfs_destroy(&ds, true).map_err(|e| e.to_string())?;
        }
        Ok(())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Lists, creates, rolls back or deletes ZFS snapshots of a Volume's dataset.
pub(crate) async fn snapshot(
    v: &Volumes,
    namespace: &str,
    opts: VolumeSnapshotAction,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    check_snapshot_op(&opts.op, &opts.snapshot, opts.keep)?;
    let into = if opts.op == "clone" {
        v.dataset(namespace, &opts.into)?
    } else {
        String::new()
    };
    let lock = v.lock.clone();
    let listed = crate::task::blocking(&v.sys, move |sys| {
        if opts.op == "clone" {
            clone_snapshot(sys, &lock, &ds, &opts.snapshot, &into)?;
            return Ok(Vec::new());
        }
        snapshot_op(
            sys,
            &lock,
            &ds,
            &opts.op,
            &opts.snapshot,
            opts.keep as usize,
        )
    })
    .await
    .map_err(Status::failed_precondition)?;
    let _ = tx
        .send(frame(Frame::Snapshots(VolumeSnapshotResult {
            snapshots: listed,
        })))
        .await;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Whether `op` may be done with `name`. `legion-` is Legion's: a person
/// names none so, and of Legion's own only a schedule's are theirs to roll
/// back to or delete; a move's, a replica's or a backup's is another
/// copy's base. `auto` takes only a schedule's, keeping at least one.
fn check_snapshot_op(op: &str, name: &str, keep: u32) -> Result<(), Status> {
    use legion_api::volume::SnapshotSchedule;
    let well_formed = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    let refused = match op {
        "list" => false,
        "create" => !well_formed || name.starts_with(SnapshotSchedule::RESERVED),
        "rollback" | "delete" | "clone" => {
            !well_formed
                || (name.starts_with(SnapshotSchedule::RESERVED)
                    && !name.starts_with(SnapshotSchedule::AUTO))
        }
        "auto" => !well_formed || !name.starts_with(SnapshotSchedule::AUTO) || keep == 0,
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown snapshot op {other:?}"
            )));
        }
    };
    if refused {
        return Err(Status::invalid_argument(
            "snapshot name must be 1..128 characters of [a-zA-Z0-9._-], not starting with legion- (Legion's own)",
        ));
    }
    Ok(())
}

/// Copies `ds@snap` into the new dataset `into` on this node, which must
/// not have it: a copy rather than a ZFS clone, so the snapshot can be
/// pruned and the Volume deleted after. Sent and received at once, through
/// a pipe; the snapshot's name is not kept on the copy.
fn clone_snapshot(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    snap: &str,
    into: &str,
) -> Result<(), String> {
    make_room(sys, lock, into)?;
    let (mut from, to) = std::io::pipe().map_err(|e| format!("a pipe: {e}"))?;
    std::thread::scope(|scope| {
        let sender = scope.spawn(move || {
            let mut to = to;
            sys.zfs_send(&format!("{ds}@{snap}"), None, &mut to)
                .map_err(|e| e.to_string())
        });
        let taken = restore(sys, lock, into, &mut from);
        let sent = sender
            .join()
            .unwrap_or_else(|_| Err("sending stopped".to_owned()));
        sent.and(taken)
    })?;
    let _guard = lock.blocking_lock();
    sys.zfs_destroy(&format!("{into}@{snap}"), false)
        .map_err(|e| format!("dropping {snap} from the copy: {e}"))
}

/// Does `op` on the dataset `ds` and lists its snapshots as shown: a
/// person's and a schedule's. `auto` takes `name` and drops its
/// schedule's oldest beyond `keep`.
fn snapshot_op(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    op: &str,
    name: &str,
    keep: usize,
) -> Result<Vec<String>, String> {
    let _guard = lock.blocking_lock();
    match op {
        "list" => {}
        "create" => sys
            .zfs_snapshot(&format!("{ds}@{name}"))
            .map_err(|e| e.to_string())?,
        "rollback" => {
            sys.zfs_rollback(&format!("{ds}@{name}"))
                .map_err(|e| e.to_string())?;
            sys.zfs_mount(ds).map_err(|e| e.to_string())?;
        }
        "delete" => sys
            .zfs_destroy(&format!("{ds}@{name}"), false)
            .map_err(|e| e.to_string())?,
        "auto" => {
            sys.zfs_snapshot(&format!("{ds}@{name}"))
                .map_err(|e| e.to_string())?;
            // The schedule's own, oldest first (they are listed by when
            // they were taken), beyond `keep` dropped.
            let prefix = schedule_prefix(name);
            let mine: Vec<String> = sys
                .zfs_snapshots(ds)
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter(|s| s.starts_with(prefix))
                .collect();
            for old in &mine[..mine.len().saturating_sub(keep)] {
                sys.zfs_destroy(&format!("{ds}@{old}"), false)
                    .map_err(|e| format!("pruning {old}: {e}"))?;
            }
        }
        other => return Err(format!("unknown snapshot op {other:?}")),
    }
    Ok(sys
        .zfs_snapshots(ds)
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|s| shown(s))
        .collect())
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

/// Whether a snapshot is listed: a person's, or a schedule's; not the
/// bases of moves, replicas and backups.
fn shown(snapshot: &str) -> bool {
    use legion_api::volume::SnapshotSchedule;
    !snapshot.starts_with(SnapshotSchedule::RESERVED)
        || snapshot.starts_with(SnapshotSchedule::AUTO)
}

/// A schedule's snapshots' prefix, from one of their names:
/// `legion-auto-1h-` from `legion-auto-1h-20261122T143005Z`.
fn schedule_prefix(snapshot: &str) -> &str {
    snapshot.rfind('-').map_or(snapshot, |i| &snapshot[..=i])
}

/// Output frames, from a blocking writer.
struct FrameWriter {
    tx: mpsc::Sender<SessionFrame>,
    buf: Vec<u8>,
}

impl FrameWriter {
    fn new(tx: mpsc::Sender<SessionFrame>) -> Self {
        FrameWriter {
            tx,
            buf: Vec::with_capacity(CHUNK),
        }
    }
}

impl Write for FrameWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let n = data.len().min(CHUNK - self.buf.len());
        self.buf.extend_from_slice(&data[..n]);
        if self.buf.len() == CHUNK {
            self.flush()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK));
        self.tx
            .blocking_send(frame(Frame::Stdout(chunk)))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the manager went away")
            })
    }
}

/// Input frames, for a blocking reader: it ends when the input closes and
/// fails if the stream ends otherwise.
struct FrameReader {
    rx: mpsc::Receiver<Result<Vec<u8>, String>>,
    buf: Vec<u8>,
    at: usize,
}

impl FrameReader {
    fn new(rx: mpsc::Receiver<Result<Vec<u8>, String>>) -> Self {
        FrameReader {
            rx,
            buf: Vec::new(),
            at: 0,
        }
    }
}

impl Read for FrameReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.at == self.buf.len() {
            match self.rx.blocking_recv() {
                Some(Ok(bytes)) => {
                    self.buf = bytes;
                    self.at = 0;
                }
                Some(Err(e)) => return Err(std::io::Error::other(e)),
                None => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len() - self.at);
        out[..n].copy_from_slice(&self.buf[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use legion_api::volume::VolumeSpec;
    use legion_sys::FakeSys;

    use super::*;

    const DS: &str = "tank/legion/volumes/shop/data";

    fn sent(sys: &FakeSys, last: bool) -> Vec<u8> {
        let mut out = Vec::new();
        give(sys, &Mutex::new(()), DS, last, &mut out).unwrap();
        out
    }

    fn taken(sys: &FakeSys, last: bool, data: &[u8]) {
        take(sys, &Mutex::new(()), DS, last, &mut &data[..]).unwrap();
    }

    /// The last part may be taken twice, as when the manager did not take
    /// the first in: the new node keeps the move's snapshots until the
    /// Volume is first used there.
    #[test]
    fn the_last_part_may_come_again() {
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        to.zfs_create("tank/legion/volumes", &[]).unwrap();
        from.write_dataset(DS, b"one");
        taken(&to, false, &sent(&from, false));

        from.write_dataset(DS, b"two");
        taken(&to, true, &sent(&from, true));
        from.write_dataset(DS, b"three");
        taken(&to, true, &sent(&from, true));
        assert_eq!(to.dataset_contents(DS).unwrap(), b"three");
        assert_eq!(to.zfs_snapshots(DS).unwrap(), [FULL, LAST]);

        // Used: the move is over, its snapshots go. Not on the node it
        // left while a full copy is all there is (the move under way).
        crate::volumes::zfs(&to, DS, &VolumeSpec::default()).unwrap();
        assert!(to.zfs_snapshots(DS).unwrap().is_empty());
        let other = FakeSys::new();
        other.zfs_create(DS, &[]).unwrap();
        sent(&other, false);
        crate::volumes::zfs(&other, DS, &VolumeSpec::default()).unwrap();
        assert_eq!(other.zfs_snapshots(DS).unwrap(), [FULL]);
    }

    /// A backup is the data at its snapshot, which goes once sent, as does
    /// one whose sending failed; a move's snapshots are left alone.
    #[test]
    fn a_backup_restores_elsewhere_and_leaves_no_snapshot() {
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        from.write_dataset(DS, b"rows 1");
        sent(&from, false);
        let mut out = Vec::new();
        backup(&from, &Mutex::new(()), DS, &mut out).unwrap();
        assert_eq!(from.zfs_snapshots(DS).unwrap(), [FULL]);

        make_room(&to, &Mutex::new(()), DS).unwrap();
        restore(&to, &Mutex::new(()), DS, &mut &out[..]).unwrap();
        assert_eq!(to.dataset_contents(DS).unwrap(), b"rows 1");
        assert!(to.zfs_snapshots(DS).unwrap().is_empty());

        from.fail_next("zfs send");
        assert!(backup(&from, &Mutex::new(()), DS, &mut Vec::new()).is_err());
        assert_eq!(from.zfs_snapshots(DS).unwrap(), [FULL]);
    }

    /// A restore never writes over data a node has: it is refused, and a
    /// receive cut short leaves nothing behind.
    #[test]
    fn a_restore_takes_only_a_node_without_the_data() {
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        from.write_dataset(DS, b"rows 1");
        let mut out = Vec::new();
        backup(&from, &Mutex::new(()), DS, &mut out).unwrap();

        let e = make_room(&from, &Mutex::new(()), DS).unwrap_err();
        assert!(e.contains("has data for the volume already"), "{e}");

        to.fail_next("zfs receive");
        assert!(restore(&to, &Mutex::new(()), DS, &mut &out[..]).is_err());
        assert!(to.zfs_list(DS).unwrap().iter().all(|d| d.name != DS));
        make_room(&to, &Mutex::new(()), DS).unwrap();
    }

    /// A schedule's snapshots: each taken, its oldest beyond `keep`
    /// dropped, never another schedule's or a person's or a replica's.
    #[test]
    fn a_schedule_keeps_its_last_snapshots_and_touches_no_others() {
        let lock = Mutex::new(());
        let sys = FakeSys::new();
        sys.zfs_create(DS, &[]).unwrap();
        for s in [
            "mine",
            "legion-replica-1",
            "legion-auto-24h-20261120T000000Z",
        ] {
            sys.zfs_snapshot(&format!("{DS}@{s}")).unwrap();
        }
        let auto = |n: u32| {
            let name = format!("legion-auto-1h-20261122T{n:02}0000Z");
            snapshot_op(&sys, &lock, DS, "auto", &name, 2).unwrap()
        };
        auto(1);
        auto(2);
        let shown = auto(3);
        assert_eq!(
            shown,
            [
                "mine",
                "legion-auto-24h-20261120T000000Z",
                "legion-auto-1h-20261122T020000Z",
                "legion-auto-1h-20261122T030000Z"
            ],
            "the replica's base is not shown"
        );
        assert!(
            sys.zfs_snapshots(DS)
                .unwrap()
                .contains(&"legion-replica-1".to_owned())
        );
    }

    /// A clone is a copy of the snapshot's data in a new dataset, which
    /// needs nothing of the snapshot after.
    #[test]
    fn a_clone_is_a_copy_of_the_snapshot_alone() {
        let lock = Mutex::new(());
        let sys = FakeSys::new();
        sys.zfs_create(DS, &[]).unwrap();
        sys.write_dataset(DS, b"at noon");
        sys.zfs_snapshot(&format!("{DS}@noon")).unwrap();
        sys.write_dataset(DS, b"later");
        let into = "tank/legion/volumes/shop/data-noon";
        clone_snapshot(&sys, &lock, DS, "noon", into).unwrap();
        assert_eq!(sys.dataset_contents(into).unwrap(), b"at noon");
        assert!(sys.zfs_snapshots(into).unwrap().is_empty());
        sys.zfs_destroy(&format!("{DS}@noon"), false).unwrap();
        assert_eq!(sys.dataset_contents(into).unwrap(), b"at noon");
        let e = clone_snapshot(&sys, &lock, DS, "noon", into).unwrap_err();
        assert!(e.contains("has data for the volume already"), "{e}");
    }

    /// `legion-` is Legion's: a person makes none, and touches only a
    /// schedule's of Legion's own.
    #[test]
    fn legions_own_snapshots_are_guarded() {
        let ok = |op: &str, name: &str| check_snapshot_op(op, name, 1).is_ok();
        assert!(ok("create", "before-upgrade"));
        assert!(!ok("create", "legion-auto-1h-x"));
        assert!(!ok("create", "legion-replica-9"));
        assert!(ok("rollback", "legion-auto-1h-20261122T010000Z"));
        assert!(ok("delete", "legion-auto-1h-20261122T010000Z"));
        for base in [
            "legion-replica-9",
            "legion-move-1",
            "legion-backup-1",
            "legion-bk-offsite-1",
        ] {
            assert!(!ok("rollback", base), "{base}");
            assert!(!ok("delete", base), "{base}");
        }
        assert!(!ok("auto", "mine"));
        assert!(
            check_snapshot_op("auto", "legion-auto-1h-x", 0).is_err(),
            "keeps at least one"
        );
        assert!(check_snapshot_op("unmake", "x", 1).is_err());
    }

    /// A replica's copies: whole at first, then what changed, each taken
    /// unmounted; the snapshots before the last go on both nodes, and a
    /// base gone from either asks for a full copy again.
    #[test]
    fn a_replica_takes_each_copy_since_the_last() {
        let lock = Mutex::new(());
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        from.zfs_snapshot(&format!("{DS}@mine")).unwrap();
        let copy = |base: &str, snap: &str| -> Result<(), String> {
            let mut out = Vec::new();
            replica_give(&from, &lock, DS, base, snap, &mut out)?;
            replica_take(&to, &lock, DS, base, snap, &mut out.as_slice())
        };
        copy("", "legion-replica-1").unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-1"]);
        copy("legion-replica-1", "legion-replica-2").unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-2"]);
        // The holder keeps the last sent until the next, and a user's own.
        assert_eq!(
            from.zfs_snapshots(DS).unwrap(),
            ["mine", "legion-replica-1", "legion-replica-2"]
        );
        copy("legion-replica-2", "legion-replica-3").unwrap();
        assert_eq!(
            from.zfs_snapshots(DS).unwrap(),
            ["mine", "legion-replica-2", "legion-replica-3"]
        );
        // A base gone (a rollback on the holder took it): a full copy.
        let e = copy("legion-replica-9", "legion-replica-4").unwrap_err();
        assert!(e.contains("a full copy is needed"), "{e}");
        let mut out = Vec::new();
        replica_give(
            &from,
            &lock,
            DS,
            "legion-replica-3",
            "legion-replica-5",
            &mut out,
        )
        .unwrap();
        let e = replica_take(
            &FakeSys::new(),
            &lock,
            DS,
            "legion-replica-3",
            "legion-replica-5",
            &mut out.as_slice(),
        )
        .unwrap_err();
        assert!(e.contains("a full copy is needed"), "{e}");
        assert!(replica_snapshot("legion-replica-12") && !replica_snapshot("legion-replica-x"));
        assert!(!replica_snapshot("mine"));
    }
}
