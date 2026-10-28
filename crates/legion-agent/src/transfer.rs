//! Moving a Volume's data between nodes (DESIGN.md §4.3). The leading
//! manager offers this node a session to send a Volume, and the node it
//! moves to one to receive it, and joins the two: what one sends as output
//! the other takes as input. The first part is a full copy, sent while the
//! Volume's tasks still run; the last, sent once they have stopped, is what
//! changed since. The node it left then drops its copy.

use std::io::{Read, Write};
use std::sync::Arc;

use futures::StreamExt;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{SessionFrame, VolumeReceive, VolumeSend};
use legion_sys::Sys;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

/// The snapshot of the full copy, and of the last part.
pub const FULL: &str = "legion-move-1";
pub const LAST: &str = "legion-move-2";

/// Bytes per frame.
const CHUNK: usize = 64 << 10;

/// What a transfer needs of the node.
pub(crate) struct Volumes {
    pub sys: Arc<dyn Sys>,
    pub zfs_root: Option<String>,
}

impl Volumes {
    fn dataset(&self, namespace: &str, volume: &str) -> Result<String, Status> {
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
    let out = tx.clone();
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        let snap = |n: &str| format!("{ds}@{n}");
        // A snapshot left by an earlier attempt goes first.
        let fresh = |n: &str| {
            let _ = sys.zfs_destroy(&snap(n), false);
            sys.zfs_snapshot(&snap(n)).map_err(|e| e.to_string())
        };
        let mut out = FrameWriter::new(out);
        if opts.last {
            fresh(LAST)?;
            sys.zfs_send(&snap(LAST), Some(FULL), &mut out)
        } else {
            let _ = sys.zfs_destroy(&snap(LAST), false);
            fresh(FULL)?;
            sys.zfs_send(&snap(FULL), None, &mut out)
        }
        .map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())
    })
    .await
    .map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Receives what the sending node sends, until the input closes. After
/// the last part the dataset is mounted, ready for the Volume's tasks, and
/// the migration's snapshots go.
pub(crate) async fn receive(
    v: &Volumes,
    namespace: &str,
    opts: VolumeReceive,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let ds = v.dataset(namespace, &opts.volume)?;
    let (data, rx) = mpsc::channel::<Result<Vec<u8>, String>>(16);
    let work = crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
        if let Some((parent, _)) = ds.rsplit_once('/') {
            sys.zfs_create(parent, &[]).map_err(|e| e.to_string())?;
        }
        // A copy here from before (an attempt cut short, or the Volume's
        // last stay) is stale: the full copy takes its place.
        if !opts.last
            && sys
                .zfs_list(&ds)
                .map_err(|e| e.to_string())?
                .iter()
                .any(|d| d.name == ds)
        {
            sys.zfs_destroy(&ds, true).map_err(|e| e.to_string())?;
        }
        sys.zfs_receive(&ds, &mut FrameReader::new(rx))
            .map_err(|e| e.to_string())?;
        if opts.last {
            for n in [FULL, LAST] {
                let _ = sys.zfs_destroy(&format!("{ds}@{n}"), false);
            }
            sys.zfs_mount(&ds).map_err(|e| e.to_string())?;
        }
        Ok(())
    });
    // Input until it closes; anything else ends the stream early, which
    // makes the receive fail.
    let feed = async move {
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
                })) => Err(format!("the sending node failed: {}", e.message)),
                Some(Ok(_)) => continue,
                Some(Err(e)) => Err(e.message().to_owned()),
                None => Err("the stream ended early".to_owned()),
            };
            let stop = next.is_err();
            if data.send(next).await.is_err() || stop {
                return;
            }
        }
    };
    let (result, ()) = tokio::join!(work, feed);
    result.map_err(Status::internal)?;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
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
    crate::task::blocking(&v.sys, move |sys| -> Result<(), String> {
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

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
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
