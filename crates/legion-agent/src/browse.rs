//! Files in a running container (`legion cp`, the console's Files tab): a
//! directory listed, a file read, a file written, inside the container's
//! root as the host sees it, its Volumes' mounts included.
//!
//! The agent is root on the host and the container's processes may change
//! its files while they are read, so a path is never resolved by the host's
//! lookup: each component is opened from the root's directory with
//! `O_NOFOLLOW`, and a symbolic link anywhere on the way is refused rather
//! than followed out of the root.

use std::path::Path;

use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{FilesOptions, SessionFrame};
use serde::Serialize;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

/// What a listing says of each entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub name: String,
    /// `file`, `dir`, `link` or `other`.
    pub kind: &'static str,
    pub size: u64,
    /// Permission bits, in octal: `644`.
    pub mode: String,
    /// Unix seconds.
    pub modified: i64,
    /// Where a link points, as written.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target: String,
}

/// A path's components below the root: `/a/./b/` is `a`, `b`. `..` is
/// refused, as is a name the file system cannot hold.
pub fn components(path: &str) -> Result<Vec<String>, Status> {
    let mut out = Vec::new();
    for c in path.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                return Err(Status::invalid_argument(format!(
                    "{path}: no `..` in a path"
                )));
            }
            c if c.contains('\0') => {
                return Err(Status::invalid_argument("a path holds no NUL"));
            }
            c => out.push(c.to_owned()),
        }
    }
    Ok(out)
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

/// Serves a `files` session on the container whose root is `root`.
pub(crate) async fn run(
    root: &Path,
    opts: FilesOptions,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let parts = components(&opts.path)?;
    let root = root.to_owned();
    match opts.op.as_str() {
        "list" => {
            let entries = tokio::task::spawn_blocking(move || imp::list(&root, &parts))
                .await
                .map_err(|e| Status::internal(e.to_string()))??;
            let json = serde_json::to_vec(&entries).map_err(|e| Status::internal(e.to_string()))?;
            let _ = tx.send(frame(Frame::Stdout(json))).await;
        }
        "read" => {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || imp::read(&root, &parts, &tx))
                .await
                .map_err(|e| Status::internal(e.to_string()))??;
        }
        "write" => write(root, parts, inbound).await?,
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown files operation {other:?}: expected list, read or write"
            )));
        }
    }
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Takes `input` frames until `input_closed` into a new file beside the
/// one named, which then takes its place; nothing is left if it fails.
async fn write(
    root: std::path::PathBuf,
    parts: Vec<String>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    // Waited on, not blocked on, when the writer falls behind: a blocking
    // send held a worker thread of the agent's runtime while the disk was
    // slow (found by an audit).
    let (data_tx, data_rx) = tokio::sync::mpsc::channel::<Option<Vec<u8>>>(16);
    let writing = tokio::task::spawn_blocking(move || imp::write(&root, &parts, data_rx));
    let fed = async {
        loop {
            match inbound.message().await? {
                Some(SessionFrame {
                    frame: Some(Frame::Input(data)),
                }) => {
                    if data_tx.send(Some(data)).await.is_err() {
                        return Ok(());
                    }
                }
                Some(SessionFrame {
                    frame: Some(Frame::InputClosed(_)),
                }) => {
                    let _ = data_tx.send(None).await;
                    return Ok(());
                }
                Some(_) => continue,
                None => return Err(Status::cancelled("the upload ended before it was closed")),
            }
        }
    }
    .await;
    // Dropped unclosed, the writer removes what it wrote.
    drop(data_tx);
    let written = writing.await.map_err(|e| Status::internal(e.to_string()))?;
    fed.and(written)
}

#[cfg(unix)]
mod imp {
    use std::ffi::{CStr, CString};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::path::Path;

    use legion_proto::v1::SessionFrame;
    use legion_proto::v1::session_frame::Frame;
    use tokio::sync::mpsc;
    use tonic::Status;

    use super::Entry;

    const CHUNK: usize = 64 << 10;

    fn status(what: &str, e: std::io::Error) -> Status {
        match e.raw_os_error() {
            Some(libc::ENOENT) => Status::not_found(format!("{what}: no such file or directory")),
            Some(libc::ELOOP | libc::EMLINK) => Status::failed_precondition(format!(
                "{what}: a symbolic link, which is not followed"
            )),
            Some(libc::ENOTDIR) => Status::failed_precondition(format!("{what}: not a directory")),
            Some(libc::EACCES | libc::EPERM) => Status::permission_denied(format!("{what}: {e}")),
            _ => Status::internal(format!("{what}: {e}")),
        }
    }

    fn c(name: &str) -> CString {
        CString::new(name).expect("components hold no NUL")
    }

    fn open_dir(root: &Path) -> std::io::Result<OwnedFd> {
        let path = CString::new(root.as_os_str().as_encoded_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        // SAFETY: a valid C string; the descriptor returned is owned here.
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: just opened, owned by nothing else.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn openat(
        dir: &OwnedFd,
        name: &str,
        flags: libc::c_int,
        mode: libc::c_uint,
    ) -> std::io::Result<OwnedFd> {
        let name = c(name);
        // SAFETY: a valid descriptor and C string.
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: just opened, owned by nothing else.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// The directory `parts` names under `root`, each step taken without
    /// following a link.
    fn walk(root: &Path, parts: &[String]) -> Result<OwnedFd, Status> {
        let mut dir = open_dir(root).map_err(|e| status("the container's root", e))?;
        let mut at = String::new();
        for p in parts {
            at.push('/');
            at.push_str(p);
            dir = openat(&dir, p, libc::O_RDONLY | libc::O_DIRECTORY, 0)
                .map_err(|e| status(&at, e))?;
        }
        Ok(dir)
    }

    fn stat_at(dir: &OwnedFd, name: &CStr) -> std::io::Result<libc::stat> {
        // SAFETY: zeroed is a valid stat; fstatat fills it.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: valid descriptor, C string and out pointer.
        let r = unsafe {
            libc::fstatat(
                dir.as_raw_fd(),
                name.as_ptr(),
                &mut st,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if r < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(st)
    }

    fn link_at(dir: &OwnedFd, name: &CStr) -> String {
        let mut buf = vec![0u8; 4096];
        // SAFETY: valid descriptor, C string and buffer of its length.
        let n = unsafe {
            libc::readlinkat(
                dir.as_raw_fd(),
                name.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        if n < 0 {
            return String::new();
        }
        buf.truncate(n as usize);
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn kind(mode: libc::mode_t) -> &'static str {
        match mode & libc::S_IFMT {
            libc::S_IFREG => "file",
            libc::S_IFDIR => "dir",
            libc::S_IFLNK => "link",
            _ => "other",
        }
    }

    pub(super) fn list(root: &Path, parts: &[String]) -> Result<Vec<Entry>, Status> {
        let dir = walk(root, parts)?;
        // fdopendir takes a descriptor of its own, closed by closedir;
        // close-on-exec, so a process the agent starts meanwhile does not
        // get a descriptor in the container's tree (found by an audit).
        // SAFETY: a valid descriptor.
        let dup = unsafe { libc::fcntl(dir.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if dup < 0 {
            return Err(status("listing", std::io::Error::last_os_error()));
        }
        // SAFETY: `dup` is a directory descriptor handed over to the stream.
        let stream = unsafe { libc::fdopendir(dup) };
        if stream.is_null() {
            let e = std::io::Error::last_os_error();
            // SAFETY: still ours when fdopendir fails.
            unsafe { libc::close(dup) };
            return Err(status("listing", e));
        }
        let mut out = Vec::new();
        loop {
            // SAFETY: an open stream; the entry is read before the next call.
            let ent = unsafe { libc::readdir(stream) };
            if ent.is_null() {
                break;
            }
            // SAFETY: d_name is a NUL-terminated name within the entry.
            let name = unsafe { CStr::from_ptr((*ent).d_name.as_ptr()) }.to_owned();
            let shown = name.to_string_lossy().into_owned();
            if shown == "." || shown == ".." {
                continue;
            }
            let Ok(st) = stat_at(&dir, &name) else {
                continue;
            };
            let kind = kind(st.st_mode);
            out.push(Entry {
                target: if kind == "link" {
                    link_at(&dir, &name)
                } else {
                    String::new()
                },
                name: shown,
                kind,
                size: st.st_size as u64,
                mode: format!("{:o}", st.st_mode & 0o7777),
                modified: st.st_mtime,
            });
        }
        // SAFETY: the stream opened above, closed once.
        unsafe { libc::closedir(stream) };
        out.sort_by(|a, b| (a.kind != "dir", &a.name).cmp(&(b.kind != "dir", &b.name)));
        Ok(out)
    }

    /// The parent's directory and the last name; none for the root itself.
    fn split(root: &Path, parts: &[String], what: &str) -> Result<(OwnedFd, String), Status> {
        let Some((last, dirs)) = parts.split_last() else {
            return Err(Status::invalid_argument(format!(
                "{what} a file, not the root"
            )));
        };
        Ok((walk(root, dirs)?, last.clone()))
    }

    pub(super) fn read(
        root: &Path,
        parts: &[String],
        tx: &mpsc::Sender<SessionFrame>,
    ) -> Result<(), Status> {
        let (dir, name) = split(root, parts, "read")?;
        let at = format!("/{}", parts.join("/"));
        // Non-blocking, so a FIFO put there does not hold the agent.
        let fd = openat(&dir, &name, libc::O_RDONLY | libc::O_NONBLOCK, 0)
            .map_err(|e| status(&at, e))?;
        let mut file = std::fs::File::from(fd);
        let meta = file.metadata().map_err(|e| status(&at, e))?;
        if !meta.is_file() {
            return Err(Status::failed_precondition(format!("{at}: not a file")));
        }
        let mut buf = vec![0u8; CHUNK];
        loop {
            let n = file.read(&mut buf).map_err(|e| status(&at, e))?;
            if n == 0 {
                return Ok(());
            }
            if tx
                .blocking_send(SessionFrame {
                    frame: Some(Frame::Stdout(buf[..n].to_vec())),
                })
                .is_err()
            {
                return Err(Status::cancelled("the reader went away"));
            }
        }
    }

    pub(super) fn write(
        root: &Path,
        parts: &[String],
        mut data: tokio::sync::mpsc::Receiver<Option<Vec<u8>>>,
    ) -> Result<(), Status> {
        let (dir, name) = split(root, parts, "write")?;
        let at = format!("/{}", parts.join("/"));
        // Unique here; O_EXCL refuses one that is somehow there.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp = format!(".legion-upload-{}-{n}", std::process::id());
        let fd = openat(
            &dir,
            &temp,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o644,
        )
        .map_err(|e| status(&at, e))?;
        let mut file = std::fs::File::from(fd);
        let gone = |e: Status| {
            // SAFETY: valid descriptor and C string; removing our own file.
            unsafe { libc::unlinkat(dir.as_raw_fd(), c(&temp).as_ptr(), 0) };
            e
        };
        loop {
            match data.blocking_recv() {
                Some(Some(chunk)) => file.write_all(&chunk).map_err(|e| gone(status(&at, e)))?,
                Some(None) => break,
                None => return Err(gone(Status::cancelled("the upload ended"))),
            }
        }
        file.sync_all().map_err(|e| gone(status(&at, e)))?;
        // renameat replaces a link itself, never what it points to.
        // SAFETY: valid descriptors and C strings.
        let r = unsafe {
            libc::renameat(
                dir.as_raw_fd(),
                c(&temp).as_ptr(),
                dir.as_raw_fd(),
                c(&name).as_ptr(),
            )
        };
        if r < 0 {
            return Err(gone(status(&at, std::io::Error::last_os_error())));
        }
        Ok(())
    }
}

#[cfg(not(unix))]
mod imp {
    use std::path::Path;

    use legion_proto::v1::SessionFrame;
    use tokio::sync::mpsc;
    use tonic::Status;

    use super::Entry;

    fn unsupported() -> Status {
        Status::unimplemented("files are browsed on FreeBSD")
    }

    pub(super) fn list(_: &Path, _: &[String]) -> Result<Vec<Entry>, Status> {
        Err(unsupported())
    }

    pub(super) fn read(
        _: &Path,
        _: &[String],
        _: &mpsc::Sender<SessionFrame>,
    ) -> Result<(), Status> {
        Err(unsupported())
    }

    pub(super) fn write(
        _: &Path,
        _: &[String],
        _: tokio::sync::mpsc::Receiver<Option<Vec<u8>>>,
    ) -> Result<(), Status> {
        Err(unsupported())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_path_stays_below_the_root() {
        assert_eq!(components("/etc/./rc.conf/").unwrap(), ["etc", "rc.conf"]);
        assert!(components("/").unwrap().is_empty());
        assert!(components("/etc/../../host").is_err());
        assert!(components("a\0b").is_err());
    }

    #[test]
    fn listed_read_and_written_without_following_a_link_out() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("host-secret"), "no").unwrap();
        std::fs::create_dir(root.path().join("etc")).unwrap();
        std::fs::write(root.path().join("etc/rc.conf"), "sshd_enable=YES\n").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("host-secret"),
            root.path().join("etc/secret"),
        )
        .unwrap();

        let top = imp::list(root.path(), &[]).unwrap();
        let names: Vec<_> = top.iter().map(|e| (e.name.as_str(), e.kind)).collect();
        assert_eq!(names, [("etc", "dir"), ("escape", "link")]);
        assert_eq!(top[1].target, outside.path().display().to_string());

        // A link is shown, not followed: as a directory on the way, or as
        // the file read.
        let through = imp::list(root.path(), &components("/escape").unwrap()).unwrap_err();
        assert_eq!(
            through.code(),
            tonic::Code::FailedPrecondition,
            "{through:?}"
        );
        let (tx, mut rx) = mpsc::channel(8);
        let link = imp::read(root.path(), &components("/etc/secret").unwrap(), &tx).unwrap_err();
        assert_eq!(link.code(), tonic::Code::FailedPrecondition, "{link:?}");

        imp::read(root.path(), &components("/etc/rc.conf").unwrap(), &tx).unwrap();
        drop(tx);
        let mut got = Vec::new();
        while let Some(SessionFrame {
            frame: Some(Frame::Stdout(d)),
        }) = rx.blocking_recv()
        {
            got.extend(d);
        }
        assert_eq!(got, b"sshd_enable=YES\n");

        // Written in place of a link, the link goes, not what it points to.
        let (data, rx) = tokio::sync::mpsc::channel(4);
        data.blocking_send(Some(b"kept in".to_vec())).unwrap();
        data.blocking_send(None).unwrap();
        imp::write(root.path(), &components("/etc/secret").unwrap(), rx).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.path().join("etc/secret")).unwrap(),
            "kept in"
        );
        assert_eq!(
            std::fs::read_to_string(outside.path().join("host-secret")).unwrap(),
            "no"
        );

        // An upload cut short leaves nothing behind.
        let (data, rx) = tokio::sync::mpsc::channel(4);
        data.blocking_send(Some(b"half".to_vec())).unwrap();
        drop(data);
        assert!(imp::write(root.path(), &components("/etc/new").unwrap(), rx).is_err());
        let left: Vec<_> = std::fs::read_dir(root.path().join("etc"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !left
                .iter()
                .any(|n| n.starts_with(".legion-upload-") || n == "new"),
            "{left:?}"
        );
        assert!(
            imp::list(root.path(), &components("/nothere").unwrap())
                .is_err_and(|e| e.code() == tonic::Code::NotFound)
        );
    }
}
