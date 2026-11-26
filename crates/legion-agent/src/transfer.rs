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
use legion_api::volume::{NOT_RESUMABLE, RECEIVE_BUSY};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{
    ReplicaReceive, ReplicaSend, ResumeToken, SessionFrame, SnapshotGuid, VolumeExport,
    VolumeImport, VolumeReceive, VolumeSend, VolumeSnapshotAction, VolumeSnapshotResult,
};
use legion_sys::Sys;
use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};

/// The snapshot of the full copy, and of the last part.
pub const FULL: &str = legion_api::volume::MOVE_FULL;
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
    /// The datasets a copy is being received into, by a session of this
    /// agent's: until the receive ends, what it kept has no resume token
    /// yet, and a new copy must not take its place.
    pub receiving: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

/// A receive into a dataset, under way until dropped.
pub(crate) struct Receiving {
    set: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ds: String,
}

impl Drop for Receiving {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.ds);
    }
}

/// [`RECEIVE_BUSY`], for `ds`.
fn busy(ds: &str) -> Status {
    Status::unavailable(format!(
        "{RECEIVE_BUSY}: a copy is still being taken into {ds}"
    ))
}

impl Volumes {
    /// A receive into `ds` begun; refused while another is: its sender
    /// gone (a leader restarted), it lets go of the dataset only once it
    /// notices. Seen 2026-11-26: a new leader, finding no resume token
    /// while the receive before still ran, sent the copy whole again, and
    /// the receive destroyed the 21 GB kept of it.
    pub(crate) fn receiving(&self, ds: &str) -> Result<Receiving, Status> {
        let mut set = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        if !set.insert(ds.to_owned()) {
            return Err(busy(ds));
        }
        Ok(Receiving {
            set: self.receiving.clone(),
            ds: ds.to_owned(),
        })
    }

    /// Whether a receive into `ds` is under way.
    pub(crate) fn is_receiving(&self, ds: &str) -> bool {
        self.receiving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(ds)
    }

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
        if opts.resume_token.is_empty() || opts.last {
            give(sys, &lock, &ds, opts.last, &mut out)?;
        } else {
            // The full snapshot is the one the copy cut short was of.
            replica_resume(sys, &ds, FULL, &opts.resume_token, &mut out)?;
        }
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
    let _receiving = v.receiving(&ds)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let lock = v.lock.clone();
    let work = crate::task::blocking(&v.sys, move |sys| {
        let mut input = FrameReader::new(rx);
        if opts.resume && !opts.last {
            receive_rest(sys, &ds, &mut input)
        } else {
            take(sys, &lock, &ds, opts.last, &mut input)
        }
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
/// it. A full copy cut short keeps what came (`zfs receive -s`), which the
/// next attempt goes on from or, starting again, destroys.
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
    if last {
        sys.zfs_receive(ds, input)
    } else {
        // Cut short, what came is kept for the next attempt to go on from.
        sys.zfs_receive_resumable(ds, input)
    }
    .map_err(|e| e.to_string())?;
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
/// or what changed since `base`; or goes on with a copy cut short, from
/// the receiving node's resume token.
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
        if opts.resume_token.is_empty() {
            replica_give(sys, &lock, &ds, &opts.base, &opts.snapshot, &mut out)?;
        } else {
            replica_resume(sys, &ds, &opts.snapshot, &opts.resume_token, &mut out)?;
        }
        out.flush().map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// The resume token of a copy of the Volume cut short here (a replica's,
/// or a move's full copy), if one was kept: on `stdout`, or nothing.
pub(crate) async fn resume_token(
    v: &Volumes,
    namespace: &str,
    opts: ResumeToken,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    // A receive under way has written no token yet: none is not "nothing
    // kept" then, which would have the copy sent whole again.
    if v.is_receiving(&ds) {
        return Err(busy(&ds));
    }
    let token = crate::task::blocking(&v.sys, move |sys| -> Result<Option<String>, String> {
        let here = sys
            .zfs_list(&ds)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|d| d.name == ds);
        if !here {
            return Ok(None);
        }
        sys.zfs_resume_token(&ds).map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    if let Some(token) = token {
        let _ = tx.send(frame(Frame::Stdout(token.into_bytes()))).await;
    }
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// The GUID of a replica's or a move's snapshot of the Volume here, on
/// `stdout`, or nothing if it is not here.
pub(crate) async fn snapshot_guid(
    v: &Volumes,
    namespace: &str,
    opts: SnapshotGuid,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    if !(replica_snapshot(&opts.snapshot) || opts.snapshot == FULL) {
        return Err(Status::invalid_argument(
            "not a replica's or a move's snapshot",
        ));
    }
    let snapshot = format!("{}@{}", v.dataset(namespace, &opts.volume)?, opts.snapshot);
    let guid = crate::task::blocking(&v.sys, move |sys| {
        sys.zfs_guid(&snapshot).map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    if let Some(guid) = guid {
        let _ = tx.send(frame(Frame::Stdout(guid.into_bytes()))).await;
    }
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
    let _receiving = v.receiving(&ds)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let lock = v.lock.clone();
    let work = crate::task::blocking(&v.sys, move |sys| {
        let mut input = FrameReader::new(rx);
        if opts.resume {
            replica_resumed(sys, &lock, &ds, &opts.snapshot, &mut input)
        } else {
            replica_take(sys, &lock, &ds, &opts.base, &opts.snapshot, &mut input)
        }
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

/// Goes on with a copy of `ds@snapshot` cut short, from the receiving
/// node's resume token: refused unless the token sends just that.
fn replica_resume(
    sys: &dyn Sys,
    ds: &str,
    snapshot: &str,
    token: &str,
    out: &mut dyn Write,
) -> Result<(), String> {
    let wanted = format!("{ds}@{snapshot}");
    let target = sys
        .zfs_resume_target(token)
        .map_err(|e| format!("{NOT_RESUMABLE}: {e}"))?;
    if target != wanted {
        return Err(format!("{NOT_RESUMABLE}: it was of {target}, not {wanted}"));
    }
    sys.zfs_send_resume(token, out).map_err(|e| e.to_string())
}

/// Takes the rest of a copy cut short into `ds`, unmounted; then only
/// `snapshot` stays of the replica's snapshots.
fn replica_resumed(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    ds: &str,
    snapshot: &str,
    input: &mut dyn Read,
) -> Result<(), String> {
    receive_rest(sys, ds, input)?;
    keep_only(sys, lock, ds, snapshot)
}

/// Takes the rest of a copy cut short into `ds` (a replica's, or a move's
/// full copy). Refused while the attempt before is still being taken in,
/// it says so ([`RECEIVE_BUSY`]): the manager tries again later without
/// counting it against what was kept. Seen 2026-11-25: a leader restart
/// left the receive it fed holding the dataset for over 20 s, three
/// attempts were refused meanwhile, and 19 GB kept of a 36 GB copy was
/// given up.
fn receive_rest(sys: &dyn Sys, ds: &str, input: &mut dyn Read) -> Result<(), String> {
    sys.zfs_receive_resumable(ds, input).map_err(|e| {
        let e = e.to_string();
        if legion_api::volume::receive_busy(&e) {
            format!("{RECEIVE_BUSY}: {e}")
        } else {
            e
        }
    })
}

/// Of the replica's snapshots of `ds`, drops all but `snapshot`.
fn keep_only(sys: &dyn Sys, lock: &Mutex<()>, ds: &str, snapshot: &str) -> Result<(), String> {
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

/// Takes into `ds` what `replica_give` wrote, leaving it unmounted; then
/// only `snapshot` stays of the replica's snapshots. A full copy replaces
/// what is here; an incremental one needs `base`, and gives up what was
/// kept of one cut short. Either keeps what comes of it if it too is cut
/// short (`zfs receive -s`), for the next attempt to go on from.
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
            if here
                && sys
                    .zfs_resume_token(ds)
                    .map_err(|e| e.to_string())?
                    .is_some()
            {
                sys.zfs_receive_abort(ds).map_err(|e| e.to_string())?;
            }
            let snaps = sys.zfs_snapshots(ds).map_err(|e| e.to_string())?;
            if !snaps.iter().any(|s| s == base) {
                return Err(format!(
                    "no replica at {ds}@{base} here: a full copy is needed"
                ));
            }
        }
    }
    sys.zfs_receive_resumable(ds, input)
        .map_err(|e| e.to_string())?;
    keep_only(sys, lock, ds, snapshot)
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

    /// Seen 2026-11-26: a leader restarted while its copy was being taken
    /// in; the new one, finding no resume token yet (the receive still
    /// ran), sent it whole again, which destroyed the 21 GB kept. While a
    /// receive is under way, a second is refused and so is the token,
    /// both as busy, which the manager waits out.
    #[tokio::test]
    async fn a_receive_under_way_holds_off_another_and_the_token() {
        let v = Volumes {
            sys: Arc::new(FakeSys::new()),
            zfs_root: Some("tank/legion".into()),
            lock: Arc::new(Mutex::new(())),
            iscsi: Default::default(),
            receiving: Default::default(),
        };
        let ds = v.dataset("shop", "db").unwrap();
        let first = v.receiving(&ds).unwrap();
        assert!(v.is_receiving(&ds));
        let e = v.receiving(&ds).err().unwrap();
        assert!(legion_api::volume::receive_busy(e.message()), "{e:?}");
        let (tx, _rx) = mpsc::channel(4);
        let e = resume_token(
            &v,
            "shop",
            legion_proto::v1::ResumeToken {
                volume: "db".into(),
            },
            &tx,
        )
        .await
        .unwrap_err();
        assert!(legion_api::volume::receive_busy(e.message()), "{e:?}");
        assert!(v.receiving(&v.dataset("shop", "other").unwrap()).is_ok());
        drop(first);
        assert!(!v.is_receiving(&ds));
        assert!(v.receiving(&ds).is_ok());
    }

    /// A copy that went in just before the leader sending it stopped:
    /// sending it again from the base it was sent from finds that base
    /// gone here (only the newest is kept), but the snapshot is here, the
    /// same as the holder's, which is what the next leader asks first.
    /// One taken again under the same name over changed data is another.
    /// Seen 2026-11-25: a 36 GB replica dropped and copied whole.
    #[test]
    fn a_copy_that_went_in_is_known_by_its_snapshots_guid() {
        let lock = Mutex::new(());
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        from.write_dataset(DS, b"one");
        let copy = |base: &str, snap: &str| -> Result<(), String> {
            let mut out = Vec::new();
            replica_give(&from, &lock, DS, base, snap, &mut out)?;
            replica_take(&to, &lock, DS, base, snap, &mut out.as_slice())
        };
        let guid = |sys: &FakeSys, snap: &str| sys.zfs_guid(&format!("{DS}@{snap}")).unwrap();
        copy("", "legion-replica-1").unwrap();
        from.write_dataset(DS, b"two");
        copy("legion-replica-1", "legion-replica-2").unwrap();
        // The leader stopped before it wrote down replica-2 went in.
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-2"]);
        assert!(guid(&to, "legion-replica-2").is_some());
        assert_eq!(
            guid(&to, "legion-replica-2"),
            guid(&from, "legion-replica-2")
        );
        assert_eq!(guid(&to, "legion-replica-1"), None);
        // Sending it again, as before, needs the whole copy.
        let e = copy("legion-replica-1", "legion-replica-2").unwrap_err();
        assert!(e.contains("a full copy is needed"), "{e}");
        // Taken again over other data, under the same name: not the same.
        from.write_dataset(DS, b"three");
        from.zfs_destroy(&format!("{DS}@legion-replica-2"), false)
            .unwrap();
        from.zfs_snapshot(&format!("{DS}@legion-replica-2"))
            .unwrap();
        assert_ne!(
            guid(&to, "legion-replica-2"),
            guid(&from, "legion-replica-2")
        );
    }

    /// A replica's copy cut short (the link dropped, a daemon restarted):
    /// the replica's node keeps what it took, and the next attempt sends
    /// only the rest, from its resume token, whole or incremental. Seen
    /// 2026-11-23: a 37 GB copy over a slow link started again from
    /// nothing at every restart, so never finished.
    #[test]
    fn a_copy_cut_short_goes_on_from_where_it_stopped() {
        let lock = Mutex::new(());
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        to.zfs_create("tank/legion/volumes/shop", &[]).unwrap();
        from.write_dataset(DS, &[7u8; 1000]);

        // Whole: cut halfway.
        from.fail_next("zfs send");
        let mut out = Vec::new();
        let e = replica_give(&from, &lock, DS, "", "legion-replica-1", &mut out).unwrap_err();
        assert!(e.contains("Broken pipe"), "{e}");
        let e =
            replica_take(&to, &lock, DS, "", "legion-replica-1", &mut out.as_slice()).unwrap_err();
        assert!(e.contains("incomplete stream"), "{e}");
        let token = to.zfs_resume_token(DS).unwrap().expect("what came is kept");
        // Only the rest goes.
        let mut rest = Vec::new();
        replica_resume(&from, DS, "legion-replica-1", &token, &mut rest).unwrap();
        assert!(rest.len() < 600, "{}", rest.len());
        replica_resumed(&to, &lock, DS, "legion-replica-1", &mut rest.as_slice()).unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-1"]);
        assert_eq!(to.dataset_contents(DS).unwrap(), [7u8; 1000]);
        assert_eq!(to.zfs_resume_token(DS).unwrap(), None);

        // Incremental: cut, then gone on with.
        from.write_dataset(DS, &[8u8; 1000]);
        from.fail_next("zfs send");
        let mut out = Vec::new();
        replica_give(
            &from,
            &lock,
            DS,
            "legion-replica-1",
            "legion-replica-2",
            &mut out,
        )
        .unwrap_err();
        replica_take(
            &to,
            &lock,
            DS,
            "legion-replica-1",
            "legion-replica-2",
            &mut out.as_slice(),
        )
        .unwrap_err();
        let token = to.zfs_resume_token(DS).unwrap().expect("kept");
        // A token of another snapshot, or one the holder no longer has, is
        // refused as not resumable: the next copy starts again.
        let e = replica_resume(&from, DS, "legion-replica-9", &token, &mut Vec::new()).unwrap_err();
        assert!(e.contains(NOT_RESUMABLE), "{e}");
        let mut rest = Vec::new();
        replica_resume(&from, DS, "legion-replica-2", &token, &mut rest).unwrap();
        replica_resumed(&to, &lock, DS, "legion-replica-2", &mut rest.as_slice()).unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-2"]);
        assert_eq!(to.dataset_contents(DS).unwrap(), [8u8; 1000]);

        // Cut again, but started over: a new incremental gives up what
        // was kept rather than being refused for it.
        from.write_dataset(DS, &[9u8; 1000]);
        from.fail_next("zfs send");
        let mut out = Vec::new();
        replica_give(
            &from,
            &lock,
            DS,
            "legion-replica-2",
            "legion-replica-3",
            &mut out,
        )
        .unwrap_err();
        replica_take(
            &to,
            &lock,
            DS,
            "legion-replica-2",
            "legion-replica-3",
            &mut out.as_slice(),
        )
        .unwrap_err();
        let token = to.zfs_resume_token(DS).unwrap().expect("kept");
        from.zfs_destroy(&format!("{DS}@legion-replica-3"), false)
            .unwrap();
        let e = replica_resume(&from, DS, "legion-replica-3", &token, &mut Vec::new()).unwrap_err();
        assert!(e.contains(NOT_RESUMABLE), "{e}");
        let mut out = Vec::new();
        replica_give(
            &from,
            &lock,
            DS,
            "legion-replica-2",
            "legion-replica-4",
            &mut out,
        )
        .unwrap();
        replica_take(
            &to,
            &lock,
            DS,
            "legion-replica-2",
            "legion-replica-4",
            &mut out.as_slice(),
        )
        .unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-4"]);
        assert_eq!(to.dataset_contents(DS).unwrap(), [9u8; 1000]);

        // Cut, with nothing kept there: started again under the same name
        // (the copy recorded as under way), which the holder takes anew.
        from.write_dataset(DS, &[10u8; 1000]);
        from.fail_next("zfs send");
        let give = |out: &mut Vec<u8>| {
            replica_give(
                &from,
                &lock,
                DS,
                "legion-replica-4",
                "legion-replica-5",
                out,
            )
        };
        give(&mut Vec::new()).unwrap_err();
        from.write_dataset(DS, &[11u8; 1000]);
        let mut out = Vec::new();
        give(&mut out).unwrap();
        replica_take(
            &to,
            &lock,
            DS,
            "legion-replica-4",
            "legion-replica-5",
            &mut out.as_slice(),
        )
        .unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), ["legion-replica-5"]);
        assert_eq!(to.dataset_contents(DS).unwrap(), [11u8; 1000]);
    }

    /// A move's full copy cut short: the new node keeps what came, the
    /// old one sends only the rest from the token, its full snapshot kept,
    /// and the last part goes onto it as before. A token for any other
    /// snapshot is refused as not resumable.
    /// The rest of a copy cut short, sent while the receive the last
    /// attempt fed still holds the dataset (its sender gone, it has not
    /// noticed yet): refused as such, what was kept left alone, and the
    /// next attempt goes on from it.
    #[test]
    fn the_rest_refused_while_the_last_attempt_is_taken_in_goes_on_later() {
        let lock = Mutex::new(());
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        to.zfs_create("tank/legion/volumes/shop", &[]).unwrap();
        from.write_dataset(DS, &[3u8; 1000]);
        from.fail_next("zfs send");
        let mut out = Vec::new();
        replica_give(&from, &lock, DS, "", "legion-replica-1", &mut out).unwrap_err();
        replica_take(&to, &lock, DS, "", "legion-replica-1", &mut out.as_slice()).unwrap_err();
        let token = to.zfs_resume_token(DS).unwrap().expect("what came is kept");
        let rest = || {
            let mut rest = Vec::new();
            replica_resume(&from, DS, "legion-replica-1", &token, &mut rest).unwrap();
            rest
        };
        to.still_receiving(DS);
        let e = replica_resumed(&to, &lock, DS, "legion-replica-1", &mut rest().as_slice())
            .unwrap_err();
        assert!(e.contains(RECEIVE_BUSY), "{e}");
        assert!(legion_api::volume::receive_busy(&e));
        assert_eq!(to.zfs_resume_token(DS).unwrap(), Some(token.clone()));
        replica_resumed(&to, &lock, DS, "legion-replica-1", &mut rest().as_slice()).unwrap();
        assert_eq!(to.dataset_contents(DS).unwrap(), [3u8; 1000]);
    }

    #[test]
    fn a_moves_full_copy_cut_short_goes_on() {
        let lock = Mutex::new(());
        let (from, to) = (FakeSys::new(), FakeSys::new());
        from.zfs_create(DS, &[]).unwrap();
        to.zfs_create("tank/legion/volumes/shop", &[]).unwrap();
        from.write_dataset(DS, &[1u8; 1000]);
        from.fail_next("zfs send");
        let mut out = Vec::new();
        give(&from, &lock, DS, false, &mut out).unwrap_err();
        let e = take(&to, &lock, DS, false, &mut out.as_slice()).unwrap_err();
        assert!(e.contains("incomplete stream"), "{e}");
        let token = to.zfs_resume_token(DS).unwrap().expect("what came is kept");
        let e = replica_resume(&from, DS, LAST, &token, &mut Vec::new()).unwrap_err();
        assert!(e.contains(NOT_RESUMABLE), "{e}");
        let mut rest = Vec::new();
        replica_resume(&from, DS, FULL, &token, &mut rest).unwrap();
        assert!(rest.len() < 600, "{}", rest.len());
        to.zfs_receive_resumable(DS, &mut rest.as_slice()).unwrap();
        assert_eq!(to.zfs_snapshots(DS).unwrap(), [FULL]);
        // The last part, onto the full copy.
        from.write_dataset(DS, &[2u8; 1000]);
        taken(&to, true, &sent(&from, true));
        assert_eq!(to.dataset_contents(DS).unwrap(), [2u8; 1000]);
        // A new full copy over a part kept starts again, the part gone.
        let other = "tank/legion/volumes/shop/other";
        from.zfs_create(other, &[]).unwrap();
        from.write_dataset(other, &[3u8; 1000]);
        from.fail_next("zfs send");
        let mut out = Vec::new();
        give(&from, &lock, other, false, &mut out).unwrap_err();
        take(&to, &lock, other, false, &mut out.as_slice()).unwrap_err();
        assert!(to.zfs_resume_token(other).unwrap().is_some());
        let mut out = Vec::new();
        give(&from, &lock, other, false, &mut out).unwrap();
        take(&to, &lock, other, false, &mut out.as_slice()).unwrap();
        assert_eq!(to.zfs_resume_token(other).unwrap(), None);
        assert_eq!(to.zfs_snapshots(other).unwrap(), [FULL]);
    }
}
