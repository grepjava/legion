//! The jail runtime: Legion's container specs run as jails, with no Podman
//! or OCI runtime in between (DESIGN.md §6.5).
//!
//! A pod's infra container (annotation `org.freebsd.jail.vnet=new`) is a
//! `persist` VNET jail on `/var/empty` with no process; the agent gives it
//! its network as before. Every other container is a child jail of it
//! (`org.freebsd.parentJail`) on a ZFS clone of its image, sharing the
//! pod's network, with devfs (ruleset 4) and its nullfs mounts, its
//! process run by a supervisor ([`crate::shim`]). Jails are named by
//! container id.
//!
//! A container with neither annotation is a standalone member: a jail of
//! its own on the host's network (`ip4`/`ip6` inherited), set up as a pod's
//! member is. Legion's agent never makes one; jailer runs images so.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use legion_image::{Credentials, Reference, Store};
use legion_runtime::{
    Actor, Attached, Chunk, ContainerConfig, ContainerInspect, ContainerSpec, ContainerState,
    ContainerSummary, Error, Event, ExecOutput, Login, Output, Result,
};
use legion_sys::Sys;
use tokio::io::AsyncReadExt;
use tokio::sync::{broadcast, mpsc, watch};

use crate::log;
use crate::shim::Terminal;
use crate::state::{self, EXIT, Exit, LOG, RECORD, RUN, Record, Run, SHIM_LOG, STARTING};

use crate::{
    ANNOTATION_CPUSET, ANNOTATION_JAIL, ANNOTATION_MLOCK, ANNOTATION_PARENT, ANNOTATION_SYSV,
    ANNOTATION_VNET, DEVFS_RULESET,
};

mod local;
pub use local::{Step, StepMount, StepNetwork};
/// The pod jail's root: it runs nothing, and its members' roots lie
/// outside it.
const POD_ROOT: &str = "/var/empty";
/// What Linux programs expect beside `/dev` (as the Handbook's Linux
/// jails have them): file system type, where, options.
const LINUX_MOUNTS: [(&str, &str, &str); 4] = [
    ("linprocfs", "/proc", ""),
    ("linsysfs", "/sys", ""),
    ("fdescfs", "/dev/fd", "linrdlnk"),
    ("tmpfs", SHM, ""),
];

/// Where a Linux container's `/dev/shm` is: nothing mounts on a devfs
/// directory, so `/dev/shm` links to this tmpfs.
const SHM: &str = "/.legion-shm";

/// How often state is looked at for exits.
const TICK: Duration = Duration::from_millis(200);
/// How long a removal waits for a stopped container's supervisor to end:
/// past the 5 s it gives the container's output after the end.
const SUPERVISOR_GONE: Duration = Duration::from_secs(6);
/// How often unused images are looked for, and how long one stays after
/// its last container (or its pull, or the agent's start) before it goes.
const PRUNE_EVERY: Duration = Duration::from_secs(600);
const UNUSED_FOR: Duration = Duration::from_secs(3600);

pub struct Config {
    /// Where containers' records, output and status live.
    pub state_dir: PathBuf,
    /// The ZFS dataset members' root file systems are cloned under; `None`
    /// copies them instead (a store without ZFS, see
    /// `legion_image::Store::prepare_rootfs`).
    pub dataset: Option<String>,
    /// The supervisor: a program and the arguments before `supervise` or
    /// `exec` (for Legion, its own binary and `jail-shim`).
    pub shim: Vec<OsString>,
    /// Whether images no container has used for an hour are removed, as
    /// a node's are. Off for a store that is not only this runtime's.
    pub prune_images: bool,
}

pub struct JailRuntime {
    sys: Arc<dyn Sys>,
    images: Arc<Store>,
    dir: PathBuf,
    dataset: Option<String>,
    shim: Vec<OsString>,
    boot: u64,
    events: broadcast::Sender<Event>,
    execs: Execs,
    used: Used,
    /// Names of containers being made, not yet recorded.
    creating: Arc<Mutex<BTreeSet<String>>>,
}

/// Attached execs by id. Each is removed when its caller lets go, or once
/// nothing of it has run for [`EXEC_KEPT`] after its process ended.
type Execs = Arc<Mutex<HashMap<String, Session>>>;

/// How long an ended exec's exit code waits for its caller.
const EXEC_KEPT: Duration = Duration::from_secs(60);

/// Reaps an attached exec's process, then publishes its exit code and
/// forgets the session once its caller has had time to take it, and what
/// the process started has ended: until then, the caller's letting go
/// still has to stop that.
fn reap(
    mut child: tokio::process::Child,
    code: watch::Sender<Option<i32>>,
    mut signals: mpsc::UnboundedReceiver<libc::c_int>,
    execs: Execs,
    exec_id: String,
) {
    tokio::spawn(async move {
        let status = loop {
            tokio::select! {
                s = child.wait() => break s,
                Some(signal) = signals.recv() => {
                    // Its pid is ours until it is reaped, so no one else's.
                    if let Some(pid) = child.id() {
                        // SAFETY: signalling our own unreaped child.
                        unsafe { libc::kill(pid as libc::pid_t, signal) };
                    }
                }
            }
        };
        let got = status.map(exit_code).unwrap_or(255);
        let _ = code.send(Some(got));
        loop {
            tokio::time::sleep(EXEC_KEPT).await;
            let held = execs
                .lock()
                .expect("execs")
                .get(&exec_id)
                .map(|s| (s.group, s.jid));
            match held {
                // Let go already.
                None => return,
                Some((Some(group), jid)) if signal_group(group, jid, "0").await => continue,
                Some(_) => {
                    execs.lock().expect("execs").remove(&exec_id);
                    return;
                }
            }
        }
    });
}

/// A command's exit code as a shell gives it: 128 and the signal for one
/// a signal ended, as the supervisor records a container's.
fn exit_code(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status
        .code()
        .or_else(|| status.signal().map(|s| 128 + s))
        .unwrap_or(255)
}

/// Kills a process group when dropped, unless disarmed: a command run
/// for its output whose caller stopped waiting (a probe timing out) must
/// not keep running, nor what it started.
struct KillGroup(Option<u32>);

impl KillGroup {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for KillGroup {
    fn drop(&mut self) {
        if let Some(pgid) = self.0.and_then(|p| libc::pid_t::try_from(p).ok()) {
            // SAFETY: a plain system call; a group already gone is fine.
            unsafe { libc::killpg(pgid, libc::SIGKILL) };
        }
    }
}

/// How much of each of an exec's outputs is kept. It is held in the agent,
/// outside the jail's limits, for a probe that may print without end.
const EXEC_OUTPUT: usize = 1 << 20;

/// The first `cap` bytes of `from`, reading it to its end: a command must
/// not block on, or die of, output no one keeps.
async fn read_capped(
    from: &mut (impl tokio::io::AsyncRead + Unpin),
    cap: usize,
) -> std::io::Result<Vec<u8>> {
    let mut kept = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let n = from.read(&mut chunk).await?;
        if n == 0 {
            return Ok(kept);
        }
        let room = cap.saturating_sub(kept.len());
        kept.extend_from_slice(&chunk[..n.min(room)]);
    }
}

/// How long a supervisor has to say it started its container.
const SUPERVISOR_ANSWERS: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(30)
};

/// How long a stop waits for a start to be over: more than the supervisor
/// has, as making the jail comes first. A create that a run follows counts
/// too, and a copy of a large image may outlast this: the stop then fails, to
/// be tried again.
const START_ENDS: Duration = if cfg!(test) {
    Duration::from_secs(3)
} else {
    Duration::from_secs(60)
};

/// How long a remove waits for another's to be over: it may wait for a
/// supervisor and destroy a dataset.
const REMOVE_ENDS: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(120)
};

/// Says a start is under way until dropped, for stops from any process.
/// Taken before the last run's files go, and dropped once `run.json` is
/// there, or after what undoes a failed start, so that a stop waiting on it
/// finds which of the two it was.
///
/// [`JailRuntime::create_for_run`] returns one to hold across the start.
/// Drop it before stopping or removing the container yourself: a stop
/// waits for a start held by any process, this one too, and would wait a
/// minute for your own and fail.
#[must_use = "dropped, a stop no longer waits for the start"]
pub struct Starting(PathBuf);

impl Starting {
    fn mark(rt: &JailRuntime, rec: &Record) -> Result<Self> {
        Self::mark_in(rt.container_dir(&rec.id), rt.boot)
    }

    fn mark_in(dir: PathBuf, boot: u64) -> Result<Self> {
        let mark = state::Starting {
            pid: std::process::id(),
            boot,
        };
        state::write(&dir, STARTING, &mark).map_err(internal)?;
        Ok(Self(dir))
    }
}

impl Drop for Starting {
    fn drop(&mut self) {
        let _ = state::remove(&self.0, STARTING);
    }
}

/// Undoes a start that fails or is dropped: the supervisor that has not
/// answered is killed first, with what it started, so it cannot take its
/// command killed with the jail for one started, and record a run; then
/// the jail goes, and the mounts.
struct UndoStart<'a> {
    rt: &'a JailRuntime,
    rec: &'a Record,
    jailed: bool,
    supervisor: Option<tokio::process::Child>,
    armed: bool,
}

impl Drop for UndoStart<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Not yet reaped while it is held here: its id is no one else's.
        if let Some(child) = self.supervisor.take() {
            if let Some(pgid) = child.id().and_then(|p| libc::pid_t::try_from(p).ok()) {
                // SAFETY: a plain system call, on the supervisor's own group.
                unsafe { libc::killpg(pgid, libc::SIGKILL) };
            }
            // Reaped by tokio once it has gone.
            drop(child);
        }
        if self.jailed {
            let _ = self.rt.sys.jail_remove(&self.rec.jail);
            let _ = state::remove(&self.rt.container_dir(&self.rec.id), RUN);
        }
        if !self.rec.is_pod() {
            let _ = self.rt.unmount(self.rec);
        }
    }
}

/// When each image (by canonical name) was last used. Pruning holds it
/// while it removes, so an image a task has just found stays for
/// [`UNUSED_FOR`] while the task makes its containers.
type Used = Arc<tokio::sync::Mutex<BTreeMap<String, tokio::time::Instant>>>;

struct Session {
    code: watch::Receiver<Option<i32>>,
    /// The process group the command leads: its own, or with a terminal
    /// its session's.
    group: Option<u32>,
    /// The container's jail, which the command and all it starts are in.
    jid: u32,
    /// Signals for the command itself, which the shim is until it execs:
    /// on the host until it enters the jail, and with a terminal not yet
    /// leading its group.
    signal: mpsc::UnboundedSender<libc::c_int>,
    /// The terminal's controlling side, for resizing.
    #[cfg(unix)]
    master: Option<Arc<Pty>>,
}

/// How long a command let go of has after its hangup before it is killed.
const EXEC_HANGUP_GRACE: Duration = Duration::from_secs(5);

/// Sends `signal` (`HUP`, `KILL`, or `0` to only look) to the members of
/// process group `group` in jail `jid`; whether there were any. Only that
/// jail's: once a group has emptied, its id may be another's on the host.
async fn signal_group(group: u32, jid: u32, signal: &str) -> bool {
    let (group, jid) = (group.to_string(), jid.to_string());
    let status = if signal == "0" {
        tokio::process::Command::new("pgrep")
            .args(["-q", "-g", &group, "-j", &jid])
            .status()
            .await
    } else {
        tokio::process::Command::new("pkill")
            .args([&format!("-{signal}"), "-g", &group, "-j", &jid])
            .status()
            .await
    };
    status.is_ok_and(|s| s.success())
}

/// A terminal's controlling side, non-blocking and polled by the reactor,
/// shared by the session, its output and its input. Its one descriptor
/// closes when the last of them goes, which hangs the terminal up; a read
/// waiting on a blocking thread, as `tokio::fs::File` does, would hold it
/// open while the command stays silent.
struct Pty(tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>);

impl Pty {
    fn new(fd: std::os::fd::OwnedFd) -> std::io::Result<Arc<Self>> {
        use std::os::fd::AsRawFd;
        // SAFETY: plain fcntl calls on a descriptor we own.
        unsafe {
            let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
            if flags < 0 || libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
        }
        // SAFETY: the OwnedFd is the AsyncFd's own: it stays open, and the
        // same descriptor, until the AsyncFd drops it.
        let fd = unsafe { tokio::io::unix::AsyncFd::register(fd)? };
        Ok(Arc::new(Pty(fd)))
    }

    fn raw(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.0.as_raw_fd()
    }
}

/// A [`Pty`]'s reading end.
struct PtyReader(Arc<Pty>);

impl tokio::io::AsyncRead for PtyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            let mut guard = std::task::ready!(self.0.0.poll_read_ready(cx))?;
            let unfilled = buf.initialize_unfilled();
            let read = guard.try_io(|fd| {
                use std::os::fd::AsRawFd;
                // SAFETY: reading into a buffer of the length given.
                let n = unsafe {
                    libc::read(fd.as_raw_fd(), unfilled.as_mut_ptr().cast(), unfilled.len())
                };
                if n < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });
            match read {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return std::task::Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return std::task::Poll::Ready(Err(e)),
                Err(_would_block) => continue,
            }
        }
    }
}

/// A [`Pty`]'s writing end.
struct PtyWriter(Arc<Pty>);

impl tokio::io::AsyncWrite for PtyWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            let mut guard = std::task::ready!(self.0.0.poll_write_ready(cx))?;
            let written = guard.try_io(|fd| {
                use std::os::fd::AsRawFd;
                // SAFETY: writing from a buffer of the length given.
                let n = unsafe { libc::write(fd.as_raw_fd(), data.as_ptr().cast(), data.len()) };
                if n < 0 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });
            match written {
                Ok(r) => return std::task::Poll::Ready(r),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    /// A terminal has no half-close: its input ends with the session.
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn api(status: u16, message: impl Into<String>) -> Error {
    let message = message.into();
    match status {
        404 => Error::NotFound(message),
        409 => Error::WrongState(message),
        _ => Error::Failed(message),
    }
}

fn internal(e: impl std::fmt::Display) -> Error {
    api(500, e.to_string())
}

fn image_error(e: legion_image::Error) -> Error {
    match e {
        legion_image::Error::NotFound(m) => api(404, m),
        e => api(500, e.to_string()),
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(internal)?
}

/// Where a container is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    Created,
    Running(Run),
    Exited(Exit),
}

impl Status {
    fn name(&self) -> &'static str {
        match self {
            Status::Created => "created",
            Status::Running(_) => "running",
            Status::Exited(_) => "exited",
        }
    }
}

fn new_id() -> String {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b).expect("random bytes");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(unix)]
fn alive(pid: u32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    pid != 0 && unsafe { libc::kill(pid as libc::pid_t, 0) } == 0
}

#[cfg(not(unix))]
fn alive(_: u32) -> bool {
    false
}

/// `cpus` as cpuset(1)'s `-l` takes it: numbers and ranges of them,
/// separated by commas.
fn cpu_list(cpus: &str) -> Result<String> {
    let number = |n: &str| !n.is_empty() && n.len() <= 4 && n.bytes().all(|b| b.is_ascii_digit());
    let ok = cpus.split(',').all(|part| match part.split_once('-') {
        Some((a, b)) => number(a) && number(b),
        None => number(part),
    });
    if !ok {
        return Err(api(
            400,
            format!("{ANNOTATION_CPUSET}={cpus:?}: CPUs as cpuset -l takes them, e.g. 0-3,6"),
        ));
    }
    Ok(cpus.to_owned())
}

/// Confines the new jail `jid` to `cpus` before anything runs in it; a
/// jail that cannot be confined goes again.
fn confine(sys: &dyn Sys, jail: &str, jid: u32, cpus: &str) -> Result<()> {
    if let Err(e) = sys.jail_cpuset(jid, cpus) {
        let _ = sys.jail_remove(jail);
        return Err(internal(format!(
            "confining jail {jail} to CPUs {cpus}: {e}"
        )));
    }
    Ok(())
}

/// `dest` (a path in the container) under `root`, made if missing (a file
/// if `file`) without following links: a link in the image must not send a
/// mount outside its root.
fn target(root: &Path, dest: &str, file: bool) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    // Refused before anything is made for it.
    let parts: Vec<_> = Path::new(dest)
        .components()
        .filter_map(|c| match c {
            Component::RootDir | Component::CurDir => None,
            Component::Normal(name) => Some(Some(name)),
            _ => Some(None),
        })
        .collect::<Option<_>>()
        .ok_or_else(|| api(400, format!("mount target {dest}: no `..` allowed")))?;
    for (i, name) in parts.iter().enumerate() {
        path.push(name);
        let last = i + 1 == parts.len();
        match std::fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(api(400, format!("mount target {dest} goes through a link")));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if last && file {
                    std::fs::File::create(&path).map_err(internal)?;
                } else {
                    std::fs::create_dir(&path).map_err(internal)?;
                }
            }
            Err(e) => return Err(internal(e)),
        }
    }
    Ok(path)
}

impl JailRuntime {
    /// The runtime over `images`. Needs a Tokio runtime: it watches for
    /// containers ending.
    pub fn new(sys: Arc<dyn Sys>, images: Arc<Store>, config: Config) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.state_dir)?;
        // Records hold containers' environments, secrets among them.
        state::private(&config.state_dir)?;
        let (events, _) = broadcast::channel(256);
        let rt = JailRuntime {
            sys,
            images,
            dir: config.state_dir,
            dataset: config.dataset,
            shim: config.shim,
            boot: state::boot_time(),
            events,
            execs: Arc::default(),
            used: Used::default(),
            creating: Arc::default(),
        };
        tokio::spawn(watch_exits(
            rt.sys.clone(),
            rt.dir.clone(),
            rt.boot,
            rt.events.clone(),
        ));
        if config.prune_images {
            tokio::spawn(prune_images(
                rt.images.clone(),
                rt.dir.clone(),
                rt.used.clone(),
            ));
        }
        Ok(rt)
    }

    fn container_dir(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }

    fn record(&self, id: &str) -> Result<Record> {
        if id.is_empty() || id.contains(['/', '\\', '.']) {
            return Err(api(404, format!("no such container {id:?}")));
        }
        load(&self.dir, id)?.ok_or_else(|| api(404, format!("no such container {id}")))
    }

    fn status(&self, rec: &Record) -> Result<Status> {
        let files = Files::read(&self.dir, rec)?;
        let jails = jail_names(&*self.sys)?;
        status(&self.dir, rec, files, self.boot, &jails)
    }

    /// The jail id of a running member, for exec.
    fn running_jid(&self, id: &str) -> Result<(Record, u32)> {
        let rec = self.record(id)?;
        match self.status(&rec)? {
            Status::Running(run) if !rec.is_pod() => Ok((rec, run.jid)),
            _ => Err(api(
                409,
                format!("container {id} is not running: container state improper"),
            )),
        }
    }

    fn shim_command(&self, args: &[&std::ffi::OsStr]) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.shim[0]);
        cmd.args(&self.shim[1..]).args(args);
        cmd
    }
}

/// Every container's record.
fn records(dir: &Path) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).map_err(internal)? {
        let e = e.map_err(internal)?;
        if let Some(r) = state::read::<Record>(&e.path(), RECORD).map_err(internal)? {
            out.push(r);
        }
    }
    out.sort_by_key(|r| r.created);
    Ok(out)
}

fn load(dir: &Path, id: &str) -> Result<Option<Record>> {
    state::read(&dir.join(id), RECORD).map_err(internal)
}

fn jail_names(sys: &dyn Sys) -> Result<std::collections::BTreeSet<String>> {
    Ok(sys
        .jails()
        .map_err(internal)?
        .into_iter()
        .map(|j| j.name)
        .collect())
}

/// What a container's files say of its run, read before the jails are
/// listed: a pod's jail is made before its run is written, so a run read
/// first whose jail the listing lacks is over, not just starting.
struct Files {
    exit: Option<Exit>,
    run: Option<Run>,
}

impl Files {
    fn read(dir: &Path, rec: &Record) -> Result<Files> {
        let cdir = dir.join(&rec.id);
        Ok(Files {
            exit: state::read(&cdir, EXIT).map_err(internal)?,
            run: state::read(&cdir, RUN).map_err(internal)?,
        })
    }
}

/// A container's environment: the image's, then the spec's over it, then
/// `HOSTNAME`, unless the spec sets it. Each name once: the first of a
/// name is what `getenv` finds, the last what a shell keeps.
fn container_env(image: &[String], spec: &BTreeMap<String, String>, hostname: &str) -> Vec<String> {
    let name = |e: &String| e.split('=').next().unwrap_or("").to_owned();
    let mut seen = BTreeSet::new();
    let mut env: Vec<String> = image
        .iter()
        .filter(|e| {
            let k = name(e);
            !spec.contains_key(&k) && k != "HOSTNAME" && seen.insert(k)
        })
        .cloned()
        .collect();
    env.extend(spec.iter().map(|(k, v)| format!("{k}={v}")));
    if !spec.contains_key("HOSTNAME") {
        env.push(format!("HOSTNAME={hostname}"));
    }
    env
}

fn status(
    dir: &Path,
    rec: &Record,
    files: Files,
    boot: u64,
    jails: &std::collections::BTreeSet<String>,
) -> Result<Status> {
    if let Some(exit) = files.exit {
        return Ok(Status::Exited(exit));
    }
    let Some(run) = files.run else {
        return Ok(Status::Created);
    };
    let here = jails.contains(&rec.jail);
    let running = run.boot == boot
        && if rec.is_pod() {
            here
        } else {
            here || alive(run.shim)
        };
    if running {
        return Ok(Status::Running(run));
    }
    // A supervisor writes the end before it exits: it may have since the
    // first look.
    if let Some(exit) = state::read::<Exit>(&dir.join(&rec.id), EXIT).map_err(internal)? {
        return Ok(Status::Exited(exit));
    }
    // A pod jail removed, or a supervisor gone without a word (a reboot).
    Ok(Status::Exited(Exit {
        code: if rec.is_pod() { 0 } else { 255 },
        finished: run.started,
        error: None,
    }))
}

/// Sends `died` when a container that was not over is.
async fn watch_exits(sys: Arc<dyn Sys>, dir: PathBuf, boot: u64, events: broadcast::Sender<Event>) {
    let mut seen: BTreeMap<String, (Status, Option<Run>)> = BTreeMap::new();
    let mut first = true;
    loop {
        let dir2 = dir.clone();
        let sys2 = sys.clone();
        let now =
            tokio::task::spawn_blocking(move || -> Result<Vec<(Record, Status, Option<Run>)>> {
                let mut read = Vec::new();
                for rec in records(&dir2)? {
                    let files = Files::read(&dir2, &rec)?;
                    read.push((rec, files));
                }
                let jails = jail_names(&*sys2)?;
                let mut out = Vec::new();
                for (rec, files) in read {
                    let run = files.run.clone();
                    let s = status(&dir2, &rec, files, boot, &jails)?;
                    out.push((rec, s, run));
                }
                Ok(out)
            })
            .await;
        match now {
            Ok(Ok(now)) => {
                let mut next = BTreeMap::new();
                for (rec, s, run) in now {
                    let now = (s, run);
                    // One not seen before ended since the last look: it
                    // was made, started and ended within one.
                    if let Status::Exited(exit) = &now.0
                        && !first
                        && seen
                            .get(&rec.id)
                            .is_none_or(|before| exited_since(before, &now))
                    {
                        let _ = events.send(died(&rec, exit.code));
                    }
                    next.insert(rec.id.clone(), now);
                }
                seen = next;
                first = false;
            }
            Ok(Err(e)) => tracing::warn!("jail runtime: looking at containers: {e}"),
            Err(e) => tracing::warn!("jail runtime: {e}"),
        }
        tokio::time::sleep(TICK).await;
    }
}

/// Whether a container seen as `before` has exited since: it ran then, or
/// its exit is another run's. A run is told by its jail and processes, not
/// its start, which is to the second: a run and its restart may both end
/// between two looks.
fn exited_since(before: &(Status, Option<Run>), now: &(Status, Option<Run>)) -> bool {
    matches!(now.0, Status::Exited(_))
        && (!matches!(before.0, Status::Exited(_)) || before.1 != now.1)
}

/// Removes images no container or task has used for [`UNUSED_FOR`], then
/// the layers nothing uses.
async fn prune_images(images: Arc<Store>, dir: PathBuf, used: Used) {
    loop {
        prune_pass(&images, &dir, &used, tokio::time::Instant::now()).await;
        tokio::time::sleep(PRUNE_EVERY).await;
    }
}

/// One pass of [`prune_images`], at `now`.
async fn prune_pass(images: &Store, dir: &Path, used: &Used, now: tokio::time::Instant) {
    let in_use: BTreeSet<String> = match records(dir) {
        Ok(r) => r
            .iter()
            .filter_map(|r| Reference::parse(&r.image).ok())
            .map(|r| r.to_string())
            .collect(),
        Err(e) => {
            tracing::warn!("jail runtime: listing containers for pruning: {e}");
            return;
        }
    };
    let mut last_used = used.lock().await;
    let known = images.images().unwrap_or_default();
    let mut removed = BTreeSet::new();
    for image in &known {
        for name in &image.references {
            let since = last_used.entry(name.clone()).or_insert(now);
            if in_use.contains(name) {
                *since = now;
            } else if now.duration_since(*since) >= UNUSED_FOR {
                match images.remove(name).await {
                    Ok(_) => {
                        tracing::info!(image = %name, "removed unused image");
                        removed.insert(name);
                    }
                    Err(e) => tracing::warn!(image = %name, "removing an unused image: {e}"),
                }
            }
        }
    }
    // What went is forgotten with what is not there: an image of that
    // name loaded again is new, not unused for as long.
    let names: BTreeSet<&String> = known
        .iter()
        .flat_map(|i| &i.references)
        .filter(|n| !removed.contains(n))
        .collect();
    last_used.retain(|n, _| names.contains(n));
    drop(last_used);
    // Then the layers and blobs nothing uses, even with no image removed:
    // those a tag moved off, a removal that found a layer busy, or a pull
    // that failed part way left.
    if let Err(e) = images.prune().await {
        tracing::warn!("jail runtime: pruning image layers: {e}");
    }
}

fn mark_used(used: &mut BTreeMap<String, tokio::time::Instant>, reference: &str) {
    if let Ok(r) = Reference::parse(reference) {
        used.insert(r.to_string(), tokio::time::Instant::now());
    }
}

fn died(rec: &Record, code: i32) -> Event {
    let mut attributes = rec.labels.clone();
    attributes.insert("name".to_owned(), rec.name.clone());
    attributes.insert("containerExitCode".to_owned(), code.to_string());
    Event {
        kind: "container".to_owned(),
        action: "died".to_owned(),
        actor: Actor {
            id: rec.id.clone(),
            attributes,
        },
        time_nano: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64),
    }
}

fn labelled(labels: &BTreeMap<String, String>, wanted: &[(String, String)]) -> bool {
    wanted.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// A container name [`JailRuntime::create`] is making, freed when dropped.
struct Creating {
    names: Arc<Mutex<BTreeSet<String>>>,
    name: String,
}

impl Creating {
    fn claim(names: &Arc<Mutex<BTreeSet<String>>>, name: &str) -> Result<Creating> {
        if !names.lock().unwrap().insert(name.to_owned()) {
            return Err(api(409, format!("the container name {name:?} is in use")));
        }
        Ok(Creating {
            names: names.clone(),
            name: name.to_owned(),
        })
    }
}

impl Drop for Creating {
    fn drop(&mut self) {
        self.names.lock().unwrap().remove(&self.name);
    }
}

impl JailRuntime {
    /// As [`legion_runtime::Runtime::create`], for a container the caller
    /// starts at once, with [`legion_runtime::Runtime::start`] or
    /// [`JailRuntime::run_attached`]: most of a
    /// run's time goes here, and a stop from another process would find
    /// nothing running and let the command start after it. Holding what this
    /// returns until the start is over makes that stop wait and stop it, or
    /// fail if it takes too long. A plain create promises no start, and a
    /// stop has nothing to wait for. Drop it before stopping or removing the
    /// container yourself, as [`Starting`] says.
    pub async fn create_for_run(&self, spec: &ContainerSpec) -> Result<(String, Starting)> {
        let (id, starting) = self.make(spec, true).await?;
        Ok((id, starting.expect("marked as asked")))
    }

    /// Waits as a stop does for a start of `id` to be over, for one who looks
    /// at its state to decide: during a start it is not running yet.
    pub async fn start_settled(&self, id: &str) -> Result<()> {
        self.start_over(&self.record(id)?).await
    }

    async fn make(&self, spec: &ContainerSpec, run: bool) -> Result<(String, Option<Starting>)> {
        // Held until the record is written, so that two creates of one name
        // cannot both find it free.
        let name = Creating::claim(&self.creating, &spec.name)?;
        let rec = self.make_record(spec)?;
        let dir = self.container_dir(&rec.id);
        let images = self.images.clone();
        let boot = self.boot;
        // On a blocking thread to the end, even if the caller stops
        // waiting: what it makes is then either recorded or gone, never
        // left where nothing lists it. The record comes first for the same
        // reason, should the process die: a record whose root was never
        // made is a container created and not started, which a remove
        // handles, while a clone with no record would stay for good.
        blocking(move || {
            let _name = name;
            std::fs::create_dir_all(&dir).map_err(internal)?;
            // What a create that fails made goes: a clone, or a copy of
            // the image's files, flagged ones too.
            let undo = |e| {
                if !rec.is_pod() {
                    let _ = images.cleanup_rootfs(&rec.root, rec.dataset.as_deref());
                }
                let _ = std::fs::remove_dir_all(&dir);
                e
            };
            let image = if rec.is_pod() {
                None
            } else {
                let image = images
                    .get(&rec.image)
                    .map_err(image_error)
                    .and_then(|i| {
                        i.ok_or_else(|| api(404, format!("{}: image not known", rec.image)))
                    })
                    .map_err(undo)?;
                Some(image)
            };
            // Before the record: a stop finds the container by that.
            let starting = run
                .then(|| Starting::mark_in(dir.clone(), boot))
                .transpose()
                .map_err(undo)?;
            state::write(&dir, RECORD, &rec)
                .map_err(internal)
                .map_err(undo)?;
            if let Some(image) = image {
                images
                    .prepare_rootfs(&image, &rec.root, rec.dataset.as_deref())
                    .map_err(image_error)
                    .map_err(undo)?;
            }
            Ok((rec.id.clone(), starting))
        })
        .await
    }

    fn make_record(&self, spec: &ContainerSpec) -> Result<Record> {
        let records = records(&self.dir)?;
        if records.iter().any(|r| r.name == spec.name) {
            return Err(api(
                409,
                format!("the container name {:?} is in use", spec.name),
            ));
        }
        let id = new_id();
        let pod = spec.annotations.get(ANNOTATION_VNET).map(String::as_str) == Some("new");
        let parent = spec.annotations.get(ANNOTATION_PARENT).cloned();
        let image = self
            .images
            .get(&spec.image)
            .map_err(image_error)?
            .ok_or_else(|| api(404, format!("{}: image not known", spec.image)))?;
        let linux = image.config.os == "linux";
        if linux && !self.images.runs("linux") {
            return Err(api(
                400,
                format!(
                    "{} is a Linux image, and this node does not run them (legion daemon --linux)",
                    spec.image
                ),
            ));
        }
        let mut params = Vec::new();
        if let Some(p) = spec.annotations.get(ANNOTATION_JAIL) {
            for kv in p.split_whitespace() {
                let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                params.push((k.to_owned(), v.to_owned()));
            }
        }
        if spec.annotations.get(ANNOTATION_MLOCK).map(String::as_str) == Some("true") {
            params.push(("allow.mlock".to_owned(), "true".to_owned()));
        }
        for (annotation, param) in ANNOTATION_SYSV {
            if let Some(v) = spec.annotations.get(annotation) {
                params.push((param.to_owned(), v.clone()));
            }
        }
        let cfg = &image.config.config;
        let argv: Vec<String> = if !spec.entrypoint.is_empty() {
            spec.entrypoint
                .iter()
                .chain(&spec.command)
                .cloned()
                .collect()
        } else if !spec.command.is_empty() {
            cfg.entrypoint
                .iter()
                .chain(&spec.command)
                .cloned()
                .collect()
        } else {
            cfg.entrypoint.iter().chain(&cfg.cmd).cloned().collect()
        };
        let hostname = spec.hostname.clone().unwrap_or_else(|| spec.name.clone());
        let env = container_env(&cfg.env, &spec.env, &hostname);
        // A VM's image is its disk: its command is the VM's supervisor.
        let vm = spec.annotations.contains_key(legion_runtime::ANNOTATION_VM);
        let (jail, root, dataset) = match (&parent, pod) {
            (_, true) => (id.clone(), PathBuf::from(POD_ROOT), None),
            (Some(p), false) => {
                let pod = load(&self.dir, p)?
                    .filter(Record::is_pod)
                    .ok_or_else(|| api(404, format!("no pod {p} to run in")))?;
                if argv.is_empty() && !vm {
                    return Err(api(400, format!("{}: no command to run", spec.name)));
                }
                (
                    format!("{}.{id}", pod.jail),
                    self.container_dir(&id).join("root"),
                    self.dataset.as_ref().map(|d| format!("{d}/{id}")),
                )
            }
            (None, false) => {
                if argv.is_empty() && !vm {
                    return Err(api(400, format!("{}: no command to run", spec.name)));
                }
                (
                    id.clone(),
                    self.container_dir(&id).join("root"),
                    self.dataset.as_ref().map(|d| format!("{d}/{id}")),
                )
            }
        };
        let mut mounts = Vec::new();
        for m in &spec.mounts {
            if m.kind != "nullfs" {
                return Err(api(400, format!("{} mounts are not supported", m.kind)));
            }
            mounts.push(state::Mount {
                source: m.source.clone(),
                destination: m.destination.clone(),
                read_only: m.options.iter().any(|o| o == "ro"),
            });
        }
        Ok(Record {
            id,
            name: spec.name.clone(),
            image: spec.image.clone(),
            created: state::now(),
            labels: spec.labels.clone(),
            annotations: spec.annotations.clone(),
            standalone: !pod && parent.is_none(),
            parent: if pod { None } else { parent },
            jail,
            root,
            dataset,
            argv,
            env,
            user: spec
                .user
                .clone()
                .filter(|u| !u.is_empty())
                .unwrap_or_else(|| cfg.user.clone()),
            work_dir: spec
                .work_dir
                .clone()
                .filter(|w| !w.is_empty())
                .or_else(|| Some(cfg.working_dir.clone()).filter(|w| !w.is_empty()))
                .unwrap_or_else(|| "/".to_owned()),
            hostname,
            mounts,
            params,
            stop_signal: state::signal(&cfg.stop_signal).unwrap_or(15),
            stop_timeout: spec.stop_timeout.unwrap_or(10),
            linux,
            cpuset: match spec.annotations.get(ANNOTATION_CPUSET) {
                Some(_) if pod => {
                    return Err(api(
                        400,
                        format!("{ANNOTATION_CPUSET}: a pod runs nothing to confine"),
                    ));
                }
                Some(c) => Some(cpu_list(c)?),
                None => None,
            },
            vm: None,
        })
        .and_then(|mut rec: Record| {
            // A VM: its supervisor runs bhyve on the host, the disk in its
            // root; it has a pod, whose address it takes.
            let Some(json) = spec.annotations.get(legion_runtime::ANNOTATION_VM) else {
                return Ok(rec);
            };
            let vm: legion_runtime::VmConfig = serde_json::from_str(json)
                .map_err(|e| api(400, format!("{}: {e}", legion_runtime::ANNOTATION_VM)))?;
            if rec.parent.is_none() {
                return Err(api(400, format!("{}: a VM runs in a pod", spec.name)));
            }
            rec.argv = crate::vm::argv();
            rec.env = crate::vm::env(&rec.id, &vm, &rec.root, &self.container_dir(&rec.id));
            rec.vm = Some(vm);
            Ok(rec)
        })
    }

    /// Mounts what a member needs, those not mounted yet.
    fn mount(&self, rec: &Record) -> Result<()> {
        let sys = &*self.sys;
        if let Some(ds) = &rec.dataset {
            sys.zfs_mount(ds).map_err(internal)?;
        }
        let mounted = sys.mounts(&rec.root.to_string_lossy()).map_err(internal)?;
        let is_mounted = |p: &Path| mounted.iter().any(|m| Path::new(m) == p);
        let dev = target(&rec.root, "/dev", false)?;
        if !is_mounted(&dev) {
            sys.mount_devfs(&dev.to_string_lossy(), DEVFS_RULESET)
                .map_err(internal)?;
        }
        if rec.linux {
            for (fstype, at, options) in LINUX_MOUNTS {
                let at = target(&rec.root, at, false)?;
                if is_mounted(&at) {
                    continue;
                }
                let at = at.to_string_lossy();
                match fstype {
                    "tmpfs" => sys.mount_tmpfs(&at, None, 0o1777),
                    _ => sys.mount_pseudo(fstype, &at, options),
                }
                .map_err(internal)?;
            }
            // Made with the devfs, so only a new one lacks it.
            if !is_mounted(&dev) {
                sys.devfs_link(&dev.to_string_lossy(), "shm", SHM)
                    .map_err(internal)?;
            }
        }
        for m in &rec.mounts {
            let file = std::fs::metadata(&m.source)
                .map_err(|e| internal(format!("{}: {e}", m.source)))?
                .is_file();
            let at = target(&rec.root, &m.destination, file)?;
            if !is_mounted(&at) {
                sys.mount_nullfs(&m.source, &at.to_string_lossy(), m.read_only)
                    .map_err(internal)?;
            }
        }
        Ok(())
    }

    fn unmount(&self, rec: &Record) -> Result<()> {
        let mut mounted = self
            .sys
            .mounts(&rec.root.to_string_lossy())
            .map_err(internal)?;
        // Deepest first; the root itself is the dataset's.
        mounted.sort_by_key(|m| std::cmp::Reverse(m.len()));
        for m in mounted {
            if Path::new(&m) != rec.root
                && let Err(e) = self.sys.unmount(&m)
            {
                // Unless an attached run ending, in another process, got
                // there first.
                let left = self.sys.mounts(&m).map_err(internal)?;
                if left.contains(&m) {
                    return Err(internal(e));
                }
            }
        }
        Ok(())
    }

    fn jail_params(&self, rec: &Record) -> Vec<(String, String)> {
        let mut p: Vec<(String, String)> = vec![
            ("name".into(), rec.jail.clone()),
            ("path".into(), rec.root.to_string_lossy().into_owned()),
            ("host.hostname".into(), rec.hostname.clone()),
            ("persist".into(), String::new()),
            ("enforce_statfs".into(), "2".into()),
        ];
        if rec.is_pod() {
            p.push(("vnet".into(), String::new()));
        } else {
            p.extend([
                ("ip4".into(), "inherit".into()),
                ("ip6".into(), "inherit".into()),
                ("devfs_ruleset".into(), DEVFS_RULESET.to_string()),
            ]);
        }
        for (k, v) in &rec.params {
            match p.iter_mut().find(|(n, _)| n == k) {
                Some(slot) => slot.1 = v.clone(),
                None => p.push((k.clone(), v.clone())),
            }
        }
        p
    }

    fn create_jail(&self, rec: &Record) -> Result<u32> {
        let params = self.jail_params(rec);
        let refs: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let jid = self
            .sys
            .jail_create(&refs)
            .map_err(|e| internal(format!("creating jail {}: {e}", rec.jail)))?;
        if let Some(cpus) = &rec.cpuset {
            confine(&*self.sys, &rec.jail, jid, cpus)?;
        }
        Ok(jid)
    }

    /// Starts the member's supervisor in its new jail and waits for its
    /// answer.
    /// Starts `rec`'s supervisor for jail `jid` and waits for its answer.
    /// The supervisor is in `slot` while it is to be killed should this
    /// fail: while it has not answered, here and if this is dropped.
    async fn supervise(
        &self,
        rec: &Record,
        jid: u32,
        slot: &mut Option<tokio::process::Child>,
    ) -> Result<()> {
        use tokio::io::AsyncBufReadExt;
        let dir = self.container_dir(&rec.id);
        let shim_log = std::fs::File::create(dir.join(SHIM_LOG)).map_err(internal)?;
        let mut cmd = self.shim_command(&[
            "supervise".as_ref(),
            dir.as_os_str(),
            jid.to_string().as_ref(),
        ]);
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(shim_log);
        #[cfg(unix)]
        cmd.process_group(0);
        let child = slot.insert(
            cmd.spawn()
                .map_err(|e| internal(format!("starting the supervisor: {e}")))?,
        );
        let stdout = child.stdout.take().expect("piped");
        let mut line = String::new();
        let read = tokio::time::timeout(
            SUPERVISOR_ANSWERS,
            tokio::io::BufReader::new(stdout).read_line(&mut line),
        )
        .await;
        if read.is_err() {
            // Killed by the caller's undoing, before the jail goes: its
            // command killed then must not pass for one started.
            return Err(api(
                500,
                format!("starting {}: the supervisor did not answer", rec.name),
            ));
        }
        let mut child = slot.take().expect("put there");
        // Reaped when it ends, while this agent runs; init reaps it after.
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        let line = line.trim();
        if let Some(pid) = line.strip_prefix("ok ") {
            tracing::debug!(container = %rec.name, pid, "started");
            return Ok(());
        }
        let why = match line.strip_prefix("err ") {
            Some(why) => why.to_owned(),
            None => {
                let log = std::fs::read_to_string(dir.join(SHIM_LOG)).unwrap_or_default();
                format!("the supervisor ended: {}", log.trim())
            }
        };
        Err(api(500, format!("starting {}: {why}", rec.name)))
    }

    /// Has `rec` removed by one remover at a time, of any process: two would
    /// trip over each other, a destroy finding its dataset busy or gone. The
    /// lock on its directory goes with the file, or with a process that dies
    /// holding it. `None` if another has removed it.
    async fn remover(&self, rec: &Record) -> Result<Option<std::fs::File>> {
        let dir = self.container_dir(&rec.id);
        let deadline = tokio::time::Instant::now() + REMOVE_ENDS;
        loop {
            let held = match std::fs::File::open(&dir) {
                Ok(held) => held,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(internal(e)),
            };
            match held.try_lock() {
                Ok(()) => return Ok(Some(held)),
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(e)) => return Err(internal(e)),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(api(409, format!("container {} is being removed", rec.id)));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// What every run starts with: it is marked as starting, and what is left
    /// of the last one goes.
    fn begin_run(&self, rec: &Record) -> Result<Starting> {
        let starting = Starting::mark(self, rec)?;
        let dir = self.container_dir(&rec.id);
        // The run before the end: one looking between the two sees the
        // container ended, not ended at once with the made-up 255.
        state::remove(&dir, RUN).map_err(internal)?;
        state::remove(&dir, EXIT).map_err(internal)?;
        // A jail left from a run whose supervisor died.
        if jail_names(&*self.sys)?.contains(&rec.jail) {
            let _ = self.sys.jail_remove(&rec.jail);
        }
        Ok(starting)
    }

    /// Returns once no start of `rec` is under way, or it has its run. A
    /// start that takes too long is an error: the stop would do nothing, and
    /// the command start anyway.
    async fn start_over(&self, rec: &Record) -> Result<()> {
        let dir = self.container_dir(&rec.id);
        let deadline = tokio::time::Instant::now() + START_ENDS;
        loop {
            let mark = state::read::<state::Starting>(&dir, STARTING).map_err(internal)?;
            if mark.is_none_or(|m| m.boot != self.boot || !alive(m.pid))
                || matches!(self.status(rec)?, Status::Running(_))
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(api(409, format!("container {} is still starting", rec.id)));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_over(&self, rec: &Record, within: Duration) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if !matches!(self.status(rec)?, Status::Running(_)) {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn inspect_record(&self, rec: &Record) -> Result<ContainerInspect> {
        let s = self.status(rec)?;
        let (pid, jid, exit_code, started, finished) = match &s {
            Status::Running(r) => (r.pid, r.jid, 0, r.started, 0),
            Status::Exited(e) => (0, 0, e.code, 0, e.finished),
            Status::Created => (0, 0, 0, 0, 0),
        };
        Ok(ContainerInspect {
            id: rec.id.clone(),
            name: rec.name.clone(),
            state: ContainerState {
                status: s.name().to_owned(),
                running: matches!(s, Status::Running(_)),
                pid: pid.into(),
                jid,
                exit_code,
                oom_killed: false,
                started_at: started.to_string(),
                finished_at: finished.to_string(),
            },
            config: ContainerConfig {
                labels: rec.labels.clone(),
                annotations: rec.annotations.clone(),
            },
        })
    }
}

impl legion_runtime::Runtime for JailRuntime {
    async fn image_usable(&self, reference: &str, user: &str) -> Result<bool> {
        let mut used = self.used.lock().await;
        let usable = self
            .images
            .usable(reference, user)
            .map_err(image_error)?
            .is_some();
        if usable {
            mark_used(&mut used, reference);
        }
        Ok(usable)
    }

    async fn image_digest(&self, reference: &str) -> Result<Option<String>> {
        Ok(self
            .images
            .get(reference)
            .map_err(image_error)?
            .map(|i| i.digest))
    }

    async fn pull(&self, reference: &str, user: &str, logins: &[Login]) -> Result<()> {
        let pull_error = |message: String| Error::Pull {
            image: reference.to_owned(),
            message,
        };
        let registry = Reference::parse(reference)
            .map_err(|e| pull_error(e.to_string()))?
            .registry;
        let creds = Credentials::for_registry(logins, &registry);
        // Before and after: an image pulled again must not go meanwhile.
        mark_used(&mut *self.used.lock().await, reference);
        let pulled = self.images.pull(reference, creds.as_ref(), user).await;
        mark_used(&mut *self.used.lock().await, reference);
        pulled.map(drop).map_err(|e| match e {
            legion_image::Error::Platform(message) => Error::Platform {
                image: reference.to_owned(),
                message,
            },
            e => Error::Pull {
                image: reference.to_owned(),
                message: e.to_string(),
            },
        })
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        Ok(self.make(spec, false).await?.0)
    }

    async fn start(&self, id: &str) -> Result<()> {
        let rec = self.record(id)?;
        if matches!(self.status(&rec)?, Status::Running(_)) {
            return Ok(());
        }
        // Refused before anything is cleared: the last run's exit stays.
        if let Some(parent) = &rec.parent {
            let pod = self.record(parent)?;
            if !matches!(self.status(&pod)?, Status::Running(_)) {
                return Err(api(409, format!("pod {parent} is not running")));
            }
        }
        let dir = self.container_dir(id);
        // Before `undo`, so dropped after it.
        let _starting = self.begin_run(&rec)?;
        // A start that fails, or is dropped, leaves nothing mounted, so the
        // next one mounts afresh (a Linux member's `/dev/shm` is made with
        // its devfs).
        let mut undo = UndoStart {
            rt: self,
            rec: &rec,
            jailed: false,
            supervisor: None,
            armed: true,
        };
        if !rec.is_pod() {
            self.mount(&rec)?;
        }
        let jid = self.create_jail(&rec)?;
        undo.jailed = true;
        if rec.is_pod() {
            let run = Run {
                shim: 0,
                pid: 0,
                jid,
                started: state::now(),
                boot: self.boot,
            };
            state::write(&dir, RUN, &run).map_err(internal)?;
        } else {
            self.supervise(&rec, jid, &mut undo.supervisor).await?;
        }
        undo.armed = false;
        Ok(())
    }

    async fn stop(&self, id: &str, timeout: Duration) -> Result<()> {
        let rec = self.record(id)?;
        self.start_over(&rec).await?;
        let Status::Running(run) = self.status(&rec)? else {
            return Ok(());
        };
        if rec.is_pod() {
            return match self.sys.jail_remove(&rec.jail) {
                Ok(()) | Err(legion_sys::Error::NotFound(_)) => Ok(()),
                Err(e) => Err(internal(e)),
            };
        }
        #[cfg(unix)]
        if alive(run.shim) {
            // The supervisor sends the container's stop signal; an attached
            // run's command, its own supervisor, is sent it.
            let signal = if run.shim == run.pid {
                rec.stop_signal
            } else {
                libc::SIGTERM
            };
            // SAFETY: a signal to a process of ours.
            unsafe { libc::kill(run.shim as libc::pid_t, signal) };
        }
        if self.wait_over(&rec, timeout).await? {
            return Ok(());
        }
        match self.sys.jail_remove(&rec.jail) {
            Ok(()) | Err(legion_sys::Error::NotFound(_)) => {}
            Err(e) => return Err(internal(e)),
        }
        // Unless the supervisor recorded the end, it is gone (it may have
        // been before the stop, its exit then only the made-up 255): record
        // the end for it.
        self.wait_over(&rec, Duration::from_secs(10)).await?;
        if state::read::<Exit>(&self.container_dir(id), EXIT)
            .map_err(internal)?
            .is_none()
        {
            state::write_exit(
                &self.container_dir(id),
                &Exit {
                    code: 137,
                    finished: state::now(),
                    error: None,
                },
            )
            .map_err(internal)?;
        }
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<()> {
        let rec = self.record(id)?;
        // A create nobody waits for any more may be recorded and still
        // making the root, which would stay if removed from under it.
        while self.creating.lock().unwrap().contains(&rec.name) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let Some(_remover) = self.remover(&rec).await? else {
            return Ok(());
        };
        self.stop(id, Duration::ZERO).await?;
        if rec.is_pod() {
            // Members first; then the pod's jail if it outlived them.
            match self.sys.jail_remove(&rec.jail) {
                Ok(()) | Err(legion_sys::Error::NotFound(_)) => {}
                Err(e) => return Err(internal(e)),
            }
        } else {
            // Its supervisor may outlive the stop a little, its last output
            // and exit.json still to write: the directory goes once it has.
            if let Some(run) = Files::read(&self.dir, &rec)?.run
                && run.boot == self.boot
            {
                let deadline = tokio::time::Instant::now() + SUPERVISOR_GONE;
                while alive(run.shim) && tokio::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            // A VM outliving its script (killed, say) holds its disk: it
            // goes first, its bhyve and its virtual machine.
            if rec.vm.is_some() {
                let name = crate::vm::name(&rec.id);
                let _ = tokio::process::Command::new("pkill")
                    .args(["-KILL", "-f", &format!("^bhyve: {name}$")])
                    .status()
                    .await;
                let _ = tokio::process::Command::new("bhyvectl")
                    .args([&format!("--vm={name}"), "--destroy"])
                    .status()
                    .await;
            }
            self.unmount(&rec)?;
            // A jail just removed may be dying a while yet, its TCP
            // connections in TIME_WAIT holding it and so its root, which
            // its dataset's destroy would find busy. Nothing runs there
            // now: its root is unmounted by force first.
            if rec.dataset.is_some() {
                if jail_names(&*self.sys)?.contains(&rec.jail) {
                    let _ = self.sys.jail_remove(&rec.jail);
                }
                let _ = self.sys.unmount(&rec.root.to_string_lossy());
            }
            // Its clone, or its copy (whose files may have flags).
            self.images
                .cleanup_rootfs(&rec.root, rec.dataset.as_deref())
                .map_err(|e| internal(format!("removing {}: {e}", rec.root.display())))?;
        }
        std::fs::remove_dir_all(self.container_dir(id)).map_err(internal)?;
        Ok(())
    }

    async fn inspect(&self, id: &str) -> Result<ContainerInspect> {
        let rec = self.record(id)?;
        self.inspect_record(&rec)
    }

    async fn wait(&self, id: &str) -> Result<i32> {
        loop {
            // Each time: one removed meanwhile, its files gone with it, would
            // look created for ever.
            let rec = self.record(id)?;
            match self.status(&rec)? {
                Status::Exited(e) => return Ok(e.code),
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    async fn exec(&self, id: &str, cmd: &[String]) -> Result<ExecOutput> {
        let (_, jid) = self.running_jid(id)?;
        let dir = self.container_dir(id);
        let mut args: Vec<OsString> = vec![
            "exec".into(),
            dir.into_os_string(),
            jid.to_string().into(),
            Terminal::None.arg().into(),
        ];
        args.extend(cmd.iter().map(OsString::from));
        let refs: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        let mut command = self.shim_command(&refs);
        // In a group of its own, which goes if this future is dropped
        // first; kill_on_drop has tokio reap the command then.
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(internal)?;
        let group = KillGroup(child.id());
        // The output to its end before the command is reaped: until then
        // its id, and its group's, are no one else's, even if what it
        // started holds the output open after it ends.
        let (mut stdout, mut stderr) = (child.stdout.take(), child.stderr.take());
        let (out, err) = tokio::try_join!(
            async {
                match &mut stdout {
                    Some(s) => read_capped(s, EXEC_OUTPUT).await,
                    None => Ok(Vec::new()),
                }
            },
            async {
                match &mut stderr {
                    Some(s) => read_capped(s, EXEC_OUTPUT).await,
                    None => Ok(Vec::new()),
                }
            },
        )
        .map_err(internal)?;
        let status = child.wait().await;
        // Reaped now, so its id may be another's soon.
        group.disarm();
        Ok(ExecOutput {
            exit_code: exit_code(status.map_err(internal)?),
            stdout: out,
            stderr: err,
        })
    }

    async fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> Result<Attached> {
        let (_, jid) = self.running_jid(id)?;
        let dir = self.container_dir(id);
        let mut args: Vec<OsString> = vec![
            "exec".into(),
            dir.into_os_string(),
            jid.to_string().into(),
            if tty { Terminal::Own } else { Terminal::None }
                .arg()
                .into(),
        ];
        args.extend(cmd.iter().map(OsString::from));
        let refs: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        let mut command = self.shim_command(&refs);
        let exec_id = new_id();
        let (code_tx, code_rx) = watch::channel(None);
        let (signal, signals) = mpsc::unbounded_channel();
        if tty {
            #[cfg(unix)]
            {
                let (master, slave) = pty().map_err(internal)?;
                let stdio = |fd: &std::os::fd::OwnedFd| -> Result<std::process::Stdio> {
                    Ok(fd.try_clone().map_err(internal)?.into())
                };
                command
                    .stdin(stdio(&slave)?)
                    .stdout(stdio(&slave)?)
                    .stderr(stdio(&slave)?);
                // The shim makes it a session's leader, and so its group's.
                let child = command.spawn().map_err(internal)?;
                drop(slave);
                let group = child.id();
                reap(child, code_tx, signals, self.execs.clone(), exec_id.clone());
                // One descriptor for all three, so dropping them closes it.
                let master = Pty::new(master).map_err(internal)?;
                let reader = PtyReader(master.clone());
                let writer = PtyWriter(master.clone());
                self.execs.lock().expect("execs").insert(
                    exec_id.clone(),
                    Session {
                        code: code_rx,
                        group,
                        jid,
                        signal,
                        master: Some(master),
                    },
                );
                // The terminal reports EIO once the process has closed it.
                let output = tokio_util_read(reader)
                    .map(|r| {
                        r.map(|data| Chunk {
                            stream: Output::Stdout,
                            data,
                            at: None,
                        })
                        .map_err(Error::Io)
                    })
                    .boxed();
                return Ok(Attached {
                    id: exec_id,
                    output,
                    stdin: stdin.then(|| {
                        Box::pin(writer) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>
                    }),
                });
            }
            #[cfg(not(unix))]
            return Err(api(400, "terminals need a Unix host"));
        }
        command
            .stdin(if stdin {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0);
        let mut child = command.spawn().map_err(internal)?;
        let out = child.stdout.take().expect("piped");
        let err = child.stderr.take().expect("piped");
        let input = child.stdin.take();
        let group = child.id();
        reap(child, code_tx, signals, self.execs.clone(), exec_id.clone());
        self.execs.lock().expect("execs").insert(
            exec_id.clone(),
            Session {
                code: code_rx,
                group,
                jid,
                signal,
                #[cfg(unix)]
                master: None,
            },
        );
        let tag = |s: Output| {
            move |r: std::io::Result<Bytes>| {
                r.map(|data| Chunk {
                    stream: s,
                    data,
                    at: None,
                })
                .map_err(Error::Io)
            }
        };
        let output = futures::stream::select(
            tokio_util_read(out).map(tag(Output::Stdout)),
            tokio_util_read(err).map(tag(Output::Stderr)),
        )
        .boxed();
        Ok(Attached {
            id: exec_id,
            output,
            stdin: input
                .map(|i| Box::pin(i) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>),
        })
    }

    async fn exec_resize(&self, exec_id: &str, width: u16, height: u16) -> Result<()> {
        #[cfg(unix)]
        {
            let master = self
                .execs
                .lock()
                .expect("execs")
                .get(exec_id)
                .ok_or_else(|| api(404, format!("no exec session {exec_id}")))?
                .master
                .clone();
            if let Some(m) = master {
                let ws = libc::winsize {
                    ws_row: height,
                    ws_col: width,
                    ws_xpixel: 0,
                    ws_ypixel: 0,
                };
                // SAFETY: setting the size of a terminal we hold.
                unsafe { libc::ioctl(m.raw(), libc::TIOCSWINSZ, &ws) };
            }
        }
        let _ = (exec_id, width, height);
        Ok(())
    }

    async fn exec_exit_code(&self, exec_id: &str) -> Result<i32> {
        let mut code = self
            .execs
            .lock()
            .expect("execs")
            .get(exec_id)
            .ok_or_else(|| api(404, format!("no exec session {exec_id}")))?
            .code
            .clone();
        let got = tokio::time::timeout(Duration::from_secs(30), code.wait_for(Option::is_some))
            .await
            .map_err(|_| api(500, "the exec session did not end"))?
            .map_err(internal)?
            .expect("some");
        // Kept: what the command started may still run, and letting go of
        // the session stops it.
        Ok(got)
    }

    /// Its command and what it started, if still running, are hung up as
    /// a terminal would be, and killed if any outlast
    /// [`EXEC_HANGUP_GRACE`]: no one is left to read them. That is the
    /// command's process group in the container's jail, which the command
    /// itself may have left already, and the command itself, which may not
    /// be in the jail or its group yet. Dropping the session drops a
    /// terminal's last descriptor.
    async fn exec_release(&self, exec_id: &str) {
        let Some(session) = self.execs.lock().expect("execs").remove(exec_id) else {
            return;
        };
        let (Some(group), jid) = (session.group, session.jid) else {
            return;
        };
        let (signal, mut code) = (session.signal.clone(), session.code.clone());
        drop(session);
        let _ = signal.send(libc::SIGHUP);
        let grouped = signal_group(group, jid, "HUP").await;
        if !grouped && code.borrow().is_some() {
            return;
        }
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + EXEC_HANGUP_GRACE;
            while tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if code.borrow_and_update().is_some() && !signal_group(group, jid, "0").await {
                    return;
                }
            }
            let _ = signal.send(libc::SIGKILL);
            signal_group(group, jid, "KILL").await;
        });
    }

    async fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<BoxStream<'static, Result<Chunk>>> {
        let rec = self.record(id)?;
        let path = self.container_dir(id).join(LOG);
        // Where the first read ended, in the file it read: a line written
        // meanwhile, or a rotation, is then followed, not lost.
        let (first, offset, inode) = loop {
            let inode = log::inode(&path);
            let (first, offset) = log::read_all_at(&path, tail).map_err(internal)?;
            if log::inode(&path) == inode {
                break (first, offset, inode);
            }
        };
        let initial = futures::stream::iter(first.into_iter().map(Ok));
        if !follow {
            return Ok(initial.boxed());
        }
        let sys = self.sys.clone();
        let dir = self.dir.clone();
        let boot = self.boot;
        let start = (path, offset, inode, false);
        let more = futures::stream::unfold(start, move |(path, offset, inode, last)| {
            let (sys, dir, rec) = (sys.clone(), dir.clone(), rec.clone());
            async move {
                if last {
                    return None;
                }
                loop {
                    // jls runs off the async workers: this looks every tick.
                    let (sys, dir, rec) = (sys.clone(), dir.clone(), rec.clone());
                    let over = blocking(move || {
                        Files::read(&dir, &rec)
                            .and_then(|f| status(&dir, &rec, f, boot, &jail_names(&*sys)?))
                    })
                    .await
                    .map_or(true, |s| matches!(s, Status::Exited(_)));
                    // The log may have been rotated since: read_since
                    // finishes the old file before the new.
                    let (chunks, at, inode) = match log::read_since(&path, offset, inode) {
                        Ok(r) => r,
                        Err(e) => {
                            return Some((vec![Err(Error::Io(e))], (path, offset, inode, true)));
                        }
                    };
                    if !chunks.is_empty() || over {
                        let chunks = chunks.into_iter().map(Ok).collect();
                        return Some((chunks, (path, at, inode, over)));
                    }
                    tokio::time::sleep(TICK).await;
                }
            }
        })
        .flat_map(futures::stream::iter);
        Ok(initial.chain(more).boxed())
    }

    async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<ContainerSummary>> {
        let wanted: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let mut read = Vec::new();
        for rec in records(&self.dir)? {
            if labelled(&rec.labels, &wanted) {
                let files = Files::read(&self.dir, &rec)?;
                read.push((rec, files));
            }
        }
        let jails = jail_names(&*self.sys)?;
        let mut out = Vec::new();
        for (rec, files) in read {
            let s = status(&self.dir, &rec, files, self.boot, &jails)?;
            let (pid, exit_code) = match &s {
                Status::Running(r) => (r.pid, 0),
                Status::Exited(e) => (0, e.code),
                Status::Created => (0, 0),
            };
            out.push(ContainerSummary {
                id: rec.id.clone(),
                names: vec![rec.name.clone()],
                image: rec.image.clone(),
                labels: rec.labels.clone(),
                state: s.name().to_owned(),
                exit_code,
                exited: matches!(s, Status::Exited(_)),
                pid: pid.into(),
            });
        }
        Ok(out)
    }

    async fn events(&self, labels: &[(&str, &str)]) -> Result<BoxStream<'static, Result<Event>>> {
        let wanted: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let rx = self.events.subscribe();
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(e) => return Some((Ok(e), rx)),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("jail runtime: {n} events dropped");
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .filter(move |e| {
            let keep = match e {
                Ok(e) => labelled(&e.actor.attributes, &wanted),
                Err(_) => true,
            };
            futures::future::ready(keep)
        });
        Ok(stream.boxed())
    }
}

/// What a reader yields, in pieces, until it ends (or fails: a terminal
/// whose process has gone reports EIO, which is its end too).
fn tokio_util_read<R>(reader: R) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    futures::stream::unfold(Some(reader), |reader| async move {
        let mut reader = reader?;
        let mut buf = vec![0u8; 16 << 10];
        match reader.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok(Bytes::from(buf)), Some(reader)))
            }
            Err(e) if e.raw_os_error() == Some(libc_eio()) => None,
            Err(e) => Some((Err(e), None)),
        }
    })
}

#[cfg(unix)]
fn libc_eio() -> i32 {
    libc::EIO
}

#[cfg(not(unix))]
fn libc_eio() -> i32 {
    5
}

/// A new pseudo-terminal: its controlling side and the process's side.
#[cfg(unix)]
fn pty() -> std::io::Result<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    use std::os::fd::FromRawFd;
    // SAFETY: posix_openpt and friends on a descriptor we then own.
    unsafe {
        let m = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if m < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let master = std::os::fd::OwnedFd::from_raw_fd(m);
        if libc::grantpt(m) != 0 || libc::unlockpt(m) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let name = libc::ptsname(m);
        if name.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let s = libc::open(name, libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if s < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((master, std::os::fd::OwnedFd::from_raw_fd(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_execs_output_is_kept_up_to_its_cap_and_read_to_its_end() {
        let all = vec![7u8; 100_000];
        let mut from = all.as_slice();
        assert_eq!(read_capped(&mut from, 10_000).await.unwrap().len(), 10_000);
        assert!(from.is_empty(), "the rest was not read");
        let mut from = &all[..5];
        assert_eq!(read_capped(&mut from, 10_000).await.unwrap().len(), 5);
    }

    #[test]
    fn exit_codes_are_a_shells() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::ExitStatus;
        // Raw wait statuses: an exit code in the second byte, a signal in
        // the first.
        assert_eq!(exit_code(ExitStatus::from_raw(3 << 8)), 3);
        assert_eq!(exit_code(ExitStatus::from_raw(libc::SIGTERM)), 143);
        assert_eq!(exit_code(ExitStatus::from_raw(libc::SIGKILL)), 137);
    }

    #[test]
    fn each_name_is_in_the_environment_once() {
        let image: Vec<String> = ["PATH=/bin", "HOSTNAME=builder", "A=1", "A=2", "B=image"]
            .map(String::from)
            .to_vec();
        let spec = BTreeMap::from([("B".to_owned(), "spec".to_owned())]);
        assert_eq!(
            container_env(&image, &spec, "web-1"),
            ["PATH=/bin", "A=1", "B=spec", "HOSTNAME=web-1"]
        );
        // One the spec sets is kept as it set it.
        let spec = BTreeMap::from([("HOSTNAME".to_owned(), "mine".to_owned())]);
        assert_eq!(
            container_env(&image, &spec, "web-1"),
            ["PATH=/bin", "A=1", "B=image", "HOSTNAME=mine"]
        );
    }

    #[test]
    fn exits_are_told_apart_by_their_run() {
        let run = |jid| {
            Some(Run {
                shim: 10 + jid,
                pid: 20 + jid,
                jid,
                started: 1000,
                boot: 1,
            })
        };
        let exited = |code| {
            Status::Exited(Exit {
                code,
                finished: 1000,
                error: None,
            })
        };
        let running = |jid| (Status::Running(run(jid).unwrap()), run(jid));
        assert!(exited_since(&running(1), &(exited(1), run(1))));
        assert!(!exited_since(&(exited(1), run(1)), &(exited(1), run(1))));
        // Restarted and ended again within the second, between two looks.
        assert!(exited_since(&(exited(1), run(1)), &(exited(1), run(2))));
        assert!(!exited_since(&running(1), &running(2)));
        assert!(exited_since(&(Status::Created, None), &(exited(1), run(1))));
    }

    #[tokio::test]
    async fn a_start_that_fails_leaves_nothing_mounted() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let images = Store::open_directory(
            sys.clone(),
            &t.path().join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        let rt = JailRuntime::new(
            sys.clone(),
            Arc::new(images),
            Config {
                state_dir: t.path().join("state"),
                dataset: None,
                shim: vec![t.path().join("no-shim").into()],
                prune_images: false,
            },
        )
        .unwrap();
        let root = t.path().join("state/c1/root");
        std::fs::create_dir_all(&root).unwrap();
        let source = t.path().join("data");
        let rec = Record {
            id: "c1".into(),
            name: "c1".into(),
            image: String::new(),
            created: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: "c1".into(),
            root: root.clone(),
            dataset: None,
            argv: vec!["/bin/true".into()],
            env: Vec::new(),
            user: String::new(),
            work_dir: "/".into(),
            hostname: "c1".into(),
            mounts: vec![state::Mount {
                source: source.to_string_lossy().into_owned(),
                destination: "/data".into(),
                read_only: false,
            }],
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: true,
            cpuset: None,
            vm: None,
        };
        state::write(&t.path().join("state/c1"), RECORD, &rec).unwrap();
        let mounted = || sys.mounts(&root.to_string_lossy()).unwrap();

        // The volume's source is missing: the devfs and Linux file systems
        // mounted before it go again.
        assert!(rt.start("c1").await.is_err());
        assert_eq!(mounted(), Vec::<String>::new());
        // So does an attached run's.
        assert!(rt.run_attached("c1", false, false).await.is_err());
        assert_eq!(mounted(), Vec::<String>::new());
        // `/proc` is a link, refused after the devfs is mounted and before
        // `/dev/shm` is made in it: that devfs goes too, so the next mount
        // makes a new one, with `/dev/shm`.
        std::fs::create_dir(&source).unwrap();
        std::fs::remove_dir(root.join("proc")).unwrap();
        std::os::unix::fs::symlink("/proc", root.join("proc")).unwrap();
        assert!(rt.start("c1").await.is_err());
        assert_eq!(mounted(), Vec::<String>::new());
        std::fs::remove_file(root.join("proc")).unwrap();
        rt.mount(&rec).unwrap();
        let shm = root.join("dev/shm").to_string_lossy().into_owned();
        assert!(
            sys.devfs_links().contains_key(&shm),
            "{:?}",
            sys.devfs_links()
        );
        // The supervisor is missing: a start that gets that far fails, and
        // unmounts all as well.
        assert!(rt.start("c1").await.is_err());
        assert_eq!(mounted(), Vec::<String>::new());
    }

    /// A runtime whose supervisor is `script`, and a stopped container c1.
    fn with_shim(t: &Path, sys: Arc<legion_sys::FakeSys>, script: &str) -> (JailRuntime, Record) {
        let images = Store::open_directory(
            sys.clone(),
            &t.join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        let shim = t.join("shim");
        std::fs::write(&shim, script).unwrap();
        let rt = JailRuntime::new(
            sys,
            Arc::new(images),
            Config {
                state_dir: t.join("state"),
                dataset: None,
                shim: vec!["/bin/sh".into(), shim.into()],
                prune_images: false,
            },
        )
        .unwrap();
        let root = t.join("state/c1/root");
        std::fs::create_dir_all(&root).unwrap();
        let rec = Record {
            id: "c1".into(),
            name: "c1".into(),
            image: String::new(),
            created: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: "c1".into(),
            root,
            dataset: None,
            argv: vec!["/bin/true".into()],
            env: Vec::new(),
            user: String::new(),
            work_dir: "/".into(),
            hostname: "c1".into(),
            mounts: Vec::new(),
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: false,
            cpuset: None,
            vm: None,
        };
        state::write(&t.join("state/c1"), RECORD, &rec).unwrap();
        (rt, rec)
    }

    /// A stop from another process finds no run while a start is under way.
    /// Reporting that as stopped would leave the command running.
    #[tokio::test]
    async fn a_stop_during_a_start_stops_what_it_starts() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let boot = state::boot_time();
        let script = format!(
            r#"dir=$2
trap 'printf "{{\"code\":143,\"finished\":1}}" > "$dir/exit.json"; exit 0' TERM
: > "$dir/supervising"
sleep 0.5
printf '{{"shim":%d,"pid":%d,"jid":%s,"started":1,"boot":{boot}}}' $$ $$ "$3" > "$dir/run.json"
echo "ok $$"
sleep 5 &
wait
"#
        );
        let (rt, rec) = with_shim(t.path(), sys, &script);
        let dir = rt.container_dir("c1");
        let stop = async {
            while !dir.join("supervising").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(!dir.join(RUN).exists(), "too late to show anything");
            rt.stop("c1", Duration::from_secs(5)).await
        };
        let (started, stopped) = tokio::join!(rt.start("c1"), stop);
        started.unwrap();
        stopped.unwrap();
        let status = rt.status(&rec).unwrap();
        assert!(matches!(status, Status::Exited(_)), "{status:?}");
        assert!(!dir.join(STARTING).exists());
    }

    /// A failed start has nothing to stop, and a stop waiting on it says so.
    #[tokio::test]
    async fn a_stop_during_a_start_that_fails_has_nothing_to_do() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let script = ": > \"$2/supervising\"\nsleep 0.3\necho 'err no such user'\n";
        let (rt, rec) = with_shim(t.path(), sys.clone(), script);
        let dir = rt.container_dir("c1");
        let stop = async {
            while !dir.join("supervising").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            rt.stop("c1", Duration::from_secs(5)).await
        };
        let (started, stopped) = tokio::join!(rt.start("c1"), stop);
        assert!(started.is_err());
        stopped.unwrap();
        assert!(!dir.join(STARTING).exists());
        assert!(!jail_names(&*sys).unwrap().contains(&rec.jail));
    }

    /// Only a start that may yet end is waited for, and not for ever.
    #[tokio::test]
    async fn a_stop_waits_only_for_a_live_start() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, _) = with_shim(t.path(), sys, "exit 1\n");
        let dir = rt.container_dir("c1");
        let mark = |pid, boot| {
            state::write(&dir, STARTING, &state::Starting { pid, boot }).unwrap();
        };
        // Left by a process gone, and by one from before a reboot.
        let mut gone = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = gone.id();
        gone.wait().unwrap();
        mark(pid, rt.boot);
        rt.stop("c1", Duration::ZERO).await.unwrap();
        mark(std::process::id(), rt.boot + 1);
        rt.stop("c1", Duration::ZERO).await.unwrap();
        rt.start_settled("c1").await.unwrap();
        // One that never ends.
        mark(std::process::id(), rt.boot);
        let err = rt.stop("c1", Duration::ZERO).await.unwrap_err();
        assert!(err.to_string().contains("still starting"), "{err}");
        let err = rt.start_settled("c1").await.unwrap_err();
        assert!(err.to_string().contains("still starting"), "{err}");
    }

    /// A wait ends with what it waits for, not with a container that looks
    /// created for ever once its files have gone.
    #[tokio::test]
    async fn a_wait_ends_when_the_container_is_removed() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, _) = with_shim(t.path(), sys, "exit 1\n");
        let (waited, removed) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), rt.wait("c1")),
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                rt.remove("c1").await
            }
        );
        removed.unwrap();
        let err = waited.expect("still waiting").unwrap_err();
        assert!(err.to_string().contains("no such container"), "{err}");
    }

    /// A remove waits its turn, and then finds nothing left to do.
    #[tokio::test]
    async fn a_remove_waits_for_another_and_is_done_with_it() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, _) = with_shim(t.path(), sys, "exit 1\n");
        let dir = rt.container_dir("c1");
        let other = std::fs::File::open(&dir).unwrap();
        other.lock().unwrap();
        let (removed, ()) = tokio::join!(rt.remove("c1"), async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(dir.join(RECORD).exists(), "removed alongside the other");
            std::fs::remove_dir_all(&dir).unwrap();
            drop(other);
        });
        removed.unwrap();
    }

    /// One that stays is reported, and a remover gone is no reason to wait.
    #[tokio::test]
    async fn a_remove_that_never_ends_is_not_waited_for_ever() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, _) = with_shim(t.path(), sys, "exit 1\n");
        let dir = rt.container_dir("c1");
        let other = std::fs::File::open(&dir).unwrap();
        other.lock().unwrap();
        let err = rt.remove("c1").await.unwrap_err();
        assert!(err.to_string().contains("being removed"), "{err}");
        assert!(dir.join(RECORD).exists());
        drop(other);
        rt.remove("c1").await.unwrap();
        assert!(!dir.exists());
    }

    /// A mount another process took down first is no failure; one that stays
    /// is.
    #[tokio::test]
    async fn a_mount_gone_meanwhile_is_not_a_failed_unmount() {
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, rec) = with_shim(t.path(), sys.clone(), "exit 1\n");
        let inside = rec.root.join("dev").to_string_lossy().into_owned();
        sys.mount_devfs(&inside, 4).unwrap();
        let other = sys.clone();
        let path = inside.clone();
        sys.before_unmount(move || other.unmount(&path).unwrap());
        rt.unmount(&rec).unwrap();
        assert_eq!(sys.mounts(&inside).unwrap(), Vec::<String>::new());
    }

    /// Removed by another process as it ends: an attached run's end is not
    /// an error, with no one left to record it for.
    #[tokio::test]
    async fn an_attached_run_removed_as_it_ends_still_ends() {
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let script =
            "while [ -e \"$2/starting.json\" ]; do sleep 0.02; done\nrm -rf \"$2\"\nexit 3\n";
        let (rt, _) = with_shim(t.path(), sys, script);
        assert_eq!(rt.run_attached("c1", false, false).await.unwrap(), 3);
    }

    /// An attached run's supervisor is its command, so `run.json` is written
    /// after it starts: marked until then, and not for the whole run.
    #[tokio::test]
    async fn an_attached_run_is_starting_until_it_has_its_run() {
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let script = r#"dir=$2
i=0
while [ -e "$dir/starting.json" ]; do
    i=$((i + 1))
    [ "$i" -lt 100 ] || exit 8
    sleep 0.02
done
[ -e "$dir/run.json" ] || exit 9
exit 0
"#;
        let (rt, _) = with_shim(t.path(), sys, script);
        assert_eq!(rt.run_attached("c1", false, false).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_root_a_dying_jail_holds_is_removed_all_the_same() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        sys.zfs_create("tank", &[]).unwrap();
        let images = Store::open(
            sys.clone(),
            "tank/images",
            &t.path().join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        let rt = JailRuntime::new(
            sys.clone(),
            Arc::new(images),
            Config {
                state_dir: t.path().join("state"),
                dataset: Some("tank/jails".into()),
                shim: vec![t.path().join("no-shim").into()],
                prune_images: false,
            },
        )
        .unwrap();
        let root = t.path().join("state/c2/root");
        std::fs::create_dir_all(&root).unwrap();
        let at = root.to_string_lossy().into_owned();
        sys.zfs_create("tank/jails", &[("mountpoint", "none")])
            .unwrap();
        sys.zfs_create("tank/jails/c2", &[("mountpoint", &at)])
            .unwrap();
        let rec = Record {
            id: "c2".into(),
            name: "c2".into(),
            image: String::new(),
            created: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: "c2".into(),
            root: root.clone(),
            dataset: Some("tank/jails/c2".into()),
            argv: vec!["/bin/true".into()],
            env: Vec::new(),
            user: String::new(),
            work_dir: "/".into(),
            hostname: "c2".into(),
            mounts: Vec::new(),
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: false,
            cpuset: None,
            vm: None,
        };
        state::write(&t.path().join("state/c2"), RECORD, &rec).unwrap();
        // Its jail ended, but is dying yet, holding its root.
        sys.hold(&at);
        rt.remove("c2").await.unwrap();
        assert!(sys.zfs_list("tank/jails/c2").unwrap().is_empty());
        assert!(!t.path().join("state/c2").exists());
    }

    #[tokio::test]
    async fn a_container_made_and_ended_between_looks_is_told_of() {
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (tx, mut events) = broadcast::channel(8);
        let watching = tokio::spawn(watch_exits(sys, t.path().to_owned(), 1, tx));
        tokio::time::sleep(TICK * 2).await;
        let dir = t.path().join("c3");
        let rec = Record {
            id: "c3".into(),
            name: "c3".into(),
            image: String::new(),
            created: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: "c3".into(),
            root: dir.join("root"),
            dataset: None,
            argv: vec!["/bin/true".into()],
            env: Vec::new(),
            user: String::new(),
            work_dir: "/".into(),
            hostname: "c3".into(),
            mounts: Vec::new(),
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: false,
            cpuset: None,
            vm: None,
        };
        std::fs::create_dir_all(&dir).unwrap();
        let run = Run {
            shim: 0,
            pid: 0,
            jid: 1,
            started: 1,
            boot: 1,
        };
        state::write(&dir, RUN, &run).unwrap();
        let exit = Exit {
            code: 7,
            finished: 1,
            error: None,
        };
        state::write(&dir, EXIT, &exit).unwrap();
        // Its record last: no look finds it before it ended.
        state::write(&dir, RECORD, &rec).unwrap();
        let e = tokio::time::timeout(TICK * 10, events.recv())
            .await
            .expect("no died event")
            .unwrap();
        assert_eq!((e.action.as_str(), e.actor.id.as_str()), ("died", "c3"));
        assert_eq!(e.actor.attributes["containerExitCode"], "7");
        watching.abort();
    }

    #[test]
    fn a_name_is_made_once_at_a_time() {
        let names = Arc::default();
        let first = Creating::claim(&names, "web").unwrap();
        assert!(Creating::claim(&names, "web").is_err());
        let _other = Creating::claim(&names, "db").unwrap();
        drop(first);
        Creating::claim(&names, "web").unwrap();
    }

    #[test]
    fn cpu_lists_as_cpuset_takes_them() {
        for ok in ["0", "0-3", "0-3,6", "1,3,5-7"] {
            assert_eq!(cpu_list(ok).unwrap(), ok);
        }
        for bad in ["", "all", "0-", "-1", "1,,2", "0;reboot", "1 2", "12345"] {
            assert!(cpu_list(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    #[cfg(unix)]
    fn mount_targets_stay_in_the_root() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let p = target(&root, "/etc/legion/secret", true).unwrap();
        assert!(p.is_file());
        assert!(root.join("etc/legion").is_dir());
        assert!(target(&root, "/data", false).unwrap().is_dir());
        std::os::unix::fs::symlink(t.path(), root.join("escape")).unwrap();
        assert!(target(&root, "/escape/x", false).is_err());
        assert!(!t.path().join("x").exists());
        assert!(target(&root, "/a/../../b", false).is_err());
    }

    /// A record of a container `id` rooted under `state`, which runs
    /// nothing real.
    fn plain_record(state: &Path, id: &str) -> Record {
        Record {
            id: id.into(),
            name: id.into(),
            image: String::new(),
            created: 0,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: id.into(),
            root: state.join(id).join("root"),
            dataset: None,
            argv: vec!["/bin/true".into()],
            env: Vec::new(),
            user: String::new(),
            work_dir: "/".into(),
            hostname: id.into(),
            mounts: Vec::new(),
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: false,
            cpuset: None,
            vm: None,
        }
    }

    #[tokio::test]
    async fn a_supervisor_that_does_not_answer_is_killed_before_its_jail_goes() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let images = Store::open_directory(
            sys.clone(),
            &t.path().join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        // A supervisor that never answers, and would go on, later, as one
        // whose command was killed before its exec takes it to have
        // started: it writes `late`.
        let shim: Vec<OsString> = vec![
            "/bin/sh".into(),
            "-c".into(),
            r#"echo $$ > "$2/sup.pid"; sleep 4; echo late > "$2/late"; sleep 600"#.into(),
            "sh".into(),
        ];
        let state_dir = t.path().join("state");
        let rt = JailRuntime::new(
            sys.clone(),
            Arc::new(images),
            Config {
                state_dir: state_dir.clone(),
                dataset: None,
                shim,
                prune_images: false,
            },
        )
        .unwrap();
        let mounted = |rec: &Record| sys.mounts(&rec.root.to_string_lossy()).unwrap();
        let tidy = |dir: &Path| {
            if let Ok(pid) = std::fs::read_to_string(dir.join("sup.pid"))
                && let Ok(pid) = pid.trim().parse::<libc::pid_t>()
            {
                // SAFETY: a plain system call, on the test's own process.
                unsafe { libc::killpg(pid, libc::SIGKILL) };
            }
        };
        for (id, dropped) in [("timed-out", false), ("dropped", true)] {
            let rec = plain_record(&state_dir, id);
            std::fs::create_dir_all(&rec.root).unwrap();
            let dir = state_dir.join(id);
            state::write(&dir, RECORD, &rec).unwrap();
            if dropped {
                // The caller stops waiting before the supervisor answers.
                assert!(
                    tokio::time::timeout(Duration::from_millis(500), rt.start(id))
                        .await
                        .is_err()
                );
            } else {
                let e = rt.start(id).await.unwrap_err();
                assert!(e.to_string().contains("did not answer"), "{e}");
            }
            assert!(dir.join("sup.pid").exists(), "{id}: the supervisor ran");
            assert_eq!(mounted(&rec), Vec::<String>::new(), "{id}");
            assert!(!jail_names(&*sys).unwrap().contains(id), "{id}");
            tokio::time::sleep(Duration::from_secs(5)).await;
            let late = dir.join("late").exists();
            tidy(&dir);
            assert!(!late, "{id}: the supervisor outlived the start");
            assert!(state::read::<Run>(&dir, RUN).unwrap().is_none(), "{id}");
        }
    }

    /// An OCI layout of one plain-tar layer holding `file`, named `name`.
    fn layout(at: &Path, file: &str, name: &str) {
        layout_running(at, file, name, &["/bin/sh"]);
    }

    /// [`layout`], its image running `cmd` (none, as a VM's disk image).
    fn layout_running(at: &Path, file: &str, name: &str, cmd: &[&str]) {
        use sha2::Digest as _;
        let digest = |b: &[u8]| format!("sha256:{:x}", sha2::Sha256::digest(b));
        let blobs = at.join("blobs/sha256");
        std::fs::create_dir_all(&blobs).unwrap();
        let put = |data: &[u8]| {
            let d = digest(data);
            std::fs::write(blobs.join(&d[7..]), data).unwrap();
            d
        };
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_ustar();
        h.set_mode(0o644);
        h.set_size(file.len() as u64);
        b.append_data(&mut h, file, file.as_bytes()).unwrap();
        let layer = b.into_inner().unwrap();
        let config = serde_json::to_vec(&serde_json::json!({"os": "freebsd",
            "config": {"Cmd": cmd}, "rootfs": {"diff_ids": [digest(&layer)]}}))
        .unwrap();
        let config = put(&config);
        let layer = put(&layer);
        let manifest = serde_json::to_vec(&serde_json::json!({"schemaVersion": 2,
            "config": {"digest": config}, "layers": [{"digest": layer}]}))
        .unwrap();
        let manifest = put(&manifest);
        std::fs::write(
            at.join("index.json"),
            serde_json::to_vec(&serde_json::json!({"schemaVersion": 2,
                "manifests": [{"digest": manifest,
                    "annotations": {"org.opencontainers.image.ref.name": name}}]}))
            .unwrap(),
        )
        .unwrap();
    }

    /// A runtime keeping images and containers in the fake's ZFS, with one
    /// image.
    async fn on_zfs(t: &Path, sys: &Arc<legion_sys::FakeSys>) -> (JailRuntime, String) {
        let images = Store::open(
            sys.clone(),
            "tank/images",
            &t.join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        let name = Reference::parse("localhost/app:1").unwrap().to_string();
        let at = t.join("layout");
        layout(&at, "etc/one", &name);
        images.import(&at, &name).await.unwrap();
        let rt = JailRuntime::new(
            sys.clone(),
            Arc::new(images),
            Config {
                state_dir: t.join("state"),
                dataset: Some("tank/jails".into()),
                shim: vec![t.join("no-shim").into()],
                prune_images: false,
            },
        )
        .unwrap();
        (rt, name)
    }

    /// A VM's image is only its disk, with no command: its container runs
    /// the VM's supervisor; any other container without one is refused.
    #[tokio::test]
    async fn a_vm_runs_from_an_image_without_a_command() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, _) = on_zfs(t.path(), &sys).await;
        let disk = Reference::parse("localhost/disk:1").unwrap().to_string();
        let at = t.path().join("disk-layout");
        layout_running(&at, "disk.img", &disk, &[]);
        rt.images.import(&at, &disk).await.unwrap();
        let pod = rt
            .create(&ContainerSpec {
                name: "pod".into(),
                image: disk.clone(),
                annotations: [(ANNOTATION_VNET.to_owned(), "new".to_owned())].into(),
                ..ContainerSpec::default()
            })
            .await
            .unwrap();
        let member = |name: &str, vm: bool| {
            let mut annotations: BTreeMap<String, String> =
                [(ANNOTATION_PARENT.to_owned(), pod.clone())].into();
            if vm {
                let config = legion_runtime::VmConfig {
                    cpus: 1,
                    memory: 256 << 20,
                    uefi: false,
                    tap: "tap0".into(),
                    ip: "10.200.0.2/24".into(),
                    gateway: "10.200.0.1".into(),
                    ip6: None,
                    gateway6: None,
                    mtu: 1500,
                    dns: "10.200.0.1".into(),
                    hostname: "guest".into(),
                };
                annotations.insert(
                    legion_runtime::ANNOTATION_VM.to_owned(),
                    serde_json::to_string(&config).unwrap(),
                );
            }
            ContainerSpec {
                name: name.into(),
                image: disk.clone(),
                annotations,
                ..ContainerSpec::default()
            }
        };
        let err = rt.create(&member("plain", false)).await.unwrap_err();
        assert!(err.to_string().contains("no command to run"), "{err}");
        let id = rt.create(&member("guest", true)).await.unwrap();
        let rec = load(&t.path().join("state"), &id).unwrap().unwrap();
        assert_eq!(rec.argv, crate::vm::argv());
        assert!(rec.vm.is_some());
    }

    #[tokio::test]
    async fn a_process_killed_while_a_root_is_made_leaves_a_record_of_it() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, image) = on_zfs(t.path(), &sys).await;
        let state_dir = t.path().join("state");
        let (tx, seen) = std::sync::mpsc::channel();
        let looked_in = state_dir.clone();
        sys.before_clone(move || {
            let found = records(&looked_in).unwrap();
            tx.send(found.iter().map(|r| r.name.clone()).collect::<Vec<_>>())
                .unwrap();
        });
        let spec = ContainerSpec {
            name: "c1".into(),
            image,
            ..ContainerSpec::default()
        };
        let id = rt.create(&spec).await.unwrap();
        assert_eq!(seen.recv().unwrap(), ["c1"], "nothing would list the clone");
        // And one whose root was never made can be removed.
        let rec = load(&state_dir, &id).unwrap().unwrap();
        sys.zfs_destroy(rec.dataset.as_deref().unwrap(), true)
            .unwrap();
        rt.remove(&id).await.unwrap();
        assert!(!state_dir.join(&id).exists());
    }

    /// A create that a run follows is marked from before anyone can find it,
    /// and only that one: a stop has no start to wait for after a plain one.
    #[tokio::test]
    async fn a_create_for_a_run_is_starting_from_when_it_can_be_found() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, image) = on_zfs(t.path(), &sys).await;
        let state_dir = t.path().join("state");
        let marked = |sys: &legion_sys::FakeSys| {
            let (tx, seen) = std::sync::mpsc::channel();
            let dir = state_dir.clone();
            sys.before_clone(move || {
                let found = records(&dir).unwrap().pop().unwrap();
                tx.send(dir.join(found.id).join(STARTING).exists()).unwrap();
            });
            seen
        };
        let spec = |name: &str| ContainerSpec {
            name: name.into(),
            image: image.clone(),
            ..ContainerSpec::default()
        };

        let seen = marked(&sys);
        let (id, held) = rt.create_for_run(&spec("run")).await.unwrap();
        assert!(seen.recv().unwrap(), "unmarked while the root was made");
        assert!(state_dir.join(&id).join(STARTING).exists());
        drop(held);
        assert!(!state_dir.join(&id).join(STARTING).exists());
        rt.stop(&id, Duration::ZERO).await.unwrap();
        rt.remove(&id).await.unwrap();

        let seen = marked(&sys);
        let id = rt.create(&spec("plain")).await.unwrap();
        assert!(!seen.recv().unwrap());
        rt.stop(&id, Duration::ZERO).await.unwrap();
    }

    /// A stop that lands between the create and the start, as most do, stops
    /// what starts: it would otherwise report it stopped, and it would run.
    #[tokio::test]
    async fn a_stop_after_a_create_for_a_run_stops_the_run() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, image) = on_zfs(t.path(), &sys).await;
        let rt = Arc::new(rt);
        // A pod: it starts with no supervisor.
        let spec = ContainerSpec {
            name: "p1".into(),
            image,
            annotations: BTreeMap::from([(crate::ANNOTATION_VNET.to_owned(), "new".to_owned())]),
            ..ContainerSpec::default()
        };
        let (id, held) = rt.create_for_run(&spec).await.unwrap();
        let rec = rt.record(&id).unwrap();
        assert!(rec.is_pod());
        let stopping = tokio::spawn({
            let (rt, id) = (rt.clone(), id.clone());
            async move { rt.stop(&id, Duration::from_secs(1)).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!stopping.is_finished(), "reported stopped before the start");
        rt.start(&id).await.unwrap();
        drop(held);
        stopping.await.unwrap().unwrap();
        let status = rt.status(&rec).unwrap();
        assert!(!matches!(status, Status::Running(_)), "{status:?}");
        assert!(!jail_names(&*sys).unwrap().contains(&rec.jail));
    }

    #[tokio::test]
    async fn a_remove_waits_for_a_create_still_making_the_root() {
        use legion_runtime::Runtime as _;
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let (rt, image) = on_zfs(t.path(), &sys).await;
        let spec = ContainerSpec {
            name: "c1".into(),
            image,
            ..ContainerSpec::default()
        };
        let id = rt.create(&spec).await.unwrap();
        // As while a create is at work: recorded, its name still claimed.
        let claim = Creating::claim(&rt.creating, "c1").unwrap();
        let rt = Arc::new(rt);
        let removing = tokio::spawn({
            let (rt, id) = (rt.clone(), id.clone());
            async move { rt.remove(&id).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!removing.is_finished(), "removed from under the create");
        assert!(t.path().join("state").join(&id).exists());
        drop(claim);
        removing.await.unwrap().unwrap();
        assert!(!t.path().join("state").join(&id).exists());
    }

    #[tokio::test]
    async fn pruning_forgets_what_it_removed_and_takes_layers_left_unused() {
        let t = tempfile::tempdir().unwrap();
        let sys = Arc::new(legion_sys::FakeSys::new());
        let images = Store::open_directory(
            sys.clone(),
            &t.path().join("images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        let state_dir = t.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let used = Used::default();
        let name = Reference::parse("localhost/app:1").unwrap().to_string();
        let (one, two) = (t.path().join("one"), t.path().join("two"));
        layout(&one, "etc/one", &name);
        layout(&two, "etc/two", &name);
        let first = images.import(&one, &name).await.unwrap();

        // The tag moves to another image: the first one's layer is used by
        // nothing, and goes with the next pass, though no image is removed.
        let t0 = tokio::time::Instant::now();
        images.import(&two, &name).await.unwrap();
        prune_pass(&images, &state_dir, &used, t0).await;
        assert!(images.get(&name).unwrap().is_some());
        let left = images.prune().await.unwrap();
        assert!(
            !left.iter().any(|l| first.layers.contains(l)),
            "{left:?} was left for a later prune"
        );

        // Unused for long enough: removed, and forgotten, so the same name
        // loaded again is newly used, not removed at the next pass.
        prune_pass(&images, &state_dir, &used, t0 + UNUSED_FOR).await;
        assert!(images.get(&name).unwrap().is_none());
        assert!(!used.lock().await.contains_key(&name));
        images.import(&one, &name).await.unwrap();
        prune_pass(
            &images,
            &state_dir,
            &used,
            t0 + UNUSED_FOR + Duration::from_secs(1),
        )
        .await;
        assert!(images.get(&name).unwrap().is_some());
    }
}
