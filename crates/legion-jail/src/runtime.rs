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

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use legion_agent::runtime::{
    Actor, Attached, Chunk, ContainerConfig, ContainerInspect, ContainerSpec, ContainerState,
    ContainerSummary, Error, Event, ExecOutput, Login, Output, Result,
};
use legion_image::{Credentials, Reference, Store};
use legion_sys::Sys;
use tokio::sync::{broadcast, watch};

use crate::log;
use crate::state::{self, EXIT, Exit, LOG, RECORD, RUN, Record, Run, SHIM_LOG};

const ANNOTATION_VNET: &str = "org.freebsd.jail.vnet";
const ANNOTATION_PARENT: &str = "org.freebsd.parentJail";
const ANNOTATION_JAIL: &str = "legion.jail";
const ANNOTATION_MLOCK: &str = "org.freebsd.jail.allow.mlock";
const ANNOTATION_SYSV: [(&str, &str); 3] = [
    ("org.freebsd.jail.sysvmsg", "sysvmsg"),
    ("org.freebsd.jail.sysvsem", "sysvsem"),
    ("org.freebsd.jail.sysvshm", "sysvshm"),
];
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

/// devfs(8)'s ruleset for jails.
const DEVFS_RULESET: u32 = 4;
/// How often state is looked at for exits.
const TICK: Duration = Duration::from_millis(200);
/// How often unused images are looked for, and how long one stays after
/// its last container (or its pull, or the agent's start) before it goes.
const PRUNE_EVERY: Duration = Duration::from_secs(600);
const UNUSED_FOR: Duration = Duration::from_secs(3600);

pub struct Config {
    /// Where containers' records, output and status live.
    pub state_dir: PathBuf,
    /// The ZFS dataset members' root file systems are cloned under.
    pub dataset: String,
    /// The supervisor: a program and the arguments before `supervise` or
    /// `exec` (for Legion, its own binary and `jail-shim`).
    pub shim: Vec<OsString>,
}

pub struct JailRuntime {
    sys: Arc<dyn Sys>,
    images: Arc<Store>,
    dir: PathBuf,
    dataset: String,
    shim: Vec<OsString>,
    boot: u64,
    events: broadcast::Sender<Event>,
    execs: Execs,
    used: Used,
}

/// Attached execs by id. Each is removed when its exit code is taken, when
/// its caller lets go, or [`EXEC_KEPT`] after its process ended.
type Execs = Arc<Mutex<HashMap<String, Session>>>;

/// How long an ended exec's exit code waits for its caller.
const EXEC_KEPT: Duration = Duration::from_secs(60);

/// Reaps an attached exec's process, then publishes its exit code and
/// forgets the session once its caller has had time to take it.
fn reap(
    mut child: tokio::process::Child,
    code: watch::Sender<Option<i32>>,
    execs: Execs,
    exec_id: String,
) {
    tokio::spawn(async move {
        let got = child
            .wait()
            .await
            .ok()
            .and_then(|s| s.code())
            .unwrap_or(255);
        let _ = code.send(Some(got));
        tokio::time::sleep(EXEC_KEPT).await;
        execs.lock().expect("execs").remove(&exec_id);
    });
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

/// `dest` (a path in the container) under `root`, made if missing (a file
/// if `file`) without following links: a link in the image must not send a
/// mount outside its root.
fn target(root: &Path, dest: &str, file: bool) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    let parts: Vec<_> = Path::new(dest)
        .components()
        .filter(|c| !matches!(c, Component::RootDir | Component::CurDir))
        .collect();
    for (i, c) in parts.iter().enumerate() {
        let Component::Normal(name) = c else {
            return Err(api(400, format!("mount target {dest}: no `..` allowed")));
        };
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
        };
        tokio::spawn(watch_exits(
            rt.sys.clone(),
            rt.dir.clone(),
            rt.boot,
            rt.events.clone(),
        ));
        tokio::spawn(prune_images(
            rt.images.clone(),
            rt.dir.clone(),
            rt.used.clone(),
        ));
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
        let jails = jail_names(&*self.sys)?;
        status(&self.dir, rec, self.boot, &jails)
    }

    /// The jail id of a running member, for exec.
    fn running_jid(&self, id: &str) -> Result<(Record, u32)> {
        let rec = self.record(id)?;
        match self.status(&rec)? {
            Status::Running(run) if rec.parent.is_some() => Ok((rec, run.jid)),
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

fn status(
    dir: &Path,
    rec: &Record,
    boot: u64,
    jails: &std::collections::BTreeSet<String>,
) -> Result<Status> {
    let cdir = dir.join(&rec.id);
    if let Some(exit) = state::read::<Exit>(&cdir, EXIT).map_err(internal)? {
        return Ok(Status::Exited(exit));
    }
    let Some(run) = state::read::<Run>(&cdir, RUN).map_err(internal)? else {
        return Ok(Status::Created);
    };
    let here = jails.contains(&rec.jail);
    let running = run.boot == boot
        && if rec.parent.is_none() {
            here
        } else {
            here || alive(run.shim)
        };
    if running {
        return Ok(Status::Running(run));
    }
    // A pod jail removed, or a supervisor gone without a word (a reboot).
    Ok(Status::Exited(Exit {
        code: if rec.parent.is_none() { 0 } else { 255 },
        finished: run.started,
        error: None,
    }))
}

/// Sends `died` when a container that was not over is.
async fn watch_exits(sys: Arc<dyn Sys>, dir: PathBuf, boot: u64, events: broadcast::Sender<Event>) {
    let mut seen: BTreeMap<String, (Status, Option<u64>)> = BTreeMap::new();
    let mut first = true;
    loop {
        let dir2 = dir.clone();
        let sys2 = sys.clone();
        let now =
            tokio::task::spawn_blocking(move || -> Result<Vec<(Record, Status, Option<u64>)>> {
                let jails = jail_names(&*sys2)?;
                let mut out = Vec::new();
                for rec in records(&dir2)? {
                    let s = status(&dir2, &rec, boot, &jails)?;
                    let started = state::read::<Run>(&dir2.join(&rec.id), RUN)
                        .ok()
                        .flatten()
                        .map(|r| r.started);
                    out.push((rec, s, started));
                }
                Ok(out)
            })
            .await;
        match now {
            Ok(Ok(now)) => {
                let mut next = BTreeMap::new();
                for (rec, s, started) in now {
                    if let Status::Exited(exit) = &s {
                        let changed = seen.get(&rec.id).is_none_or(|(old, at)| {
                            !matches!(old, Status::Exited(_)) || *at != started
                        });
                        if !first && changed && seen.contains_key(&rec.id) {
                            let _ = events.send(died(&rec, exit.code));
                        }
                    }
                    next.insert(rec.id.clone(), (s, started));
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

/// Removes images no container or task has used for [`UNUSED_FOR`], then
/// the layers nothing uses.
async fn prune_images(images: Arc<Store>, dir: PathBuf, used: Used) {
    loop {
        let now = tokio::time::Instant::now();
        let in_use: std::collections::BTreeSet<String> = match records(&dir) {
            Ok(r) => r
                .iter()
                .filter_map(|r| Reference::parse(&r.image).ok())
                .map(|r| r.to_string())
                .collect(),
            Err(e) => {
                tracing::warn!("jail runtime: listing containers for pruning: {e}");
                tokio::time::sleep(PRUNE_EVERY).await;
                continue;
            }
        };
        let mut last_used = used.lock().await;
        let known = images.images().unwrap_or_default();
        for image in &known {
            for name in &image.references {
                let since = last_used.entry(name.clone()).or_insert(now);
                if in_use.contains(name) {
                    *since = now;
                } else if now.duration_since(*since) >= UNUSED_FOR {
                    match images.remove(name).await {
                        Ok(_) => tracing::info!(image = %name, "removed unused image"),
                        Err(e) => tracing::warn!(image = %name, "removing an unused image: {e}"),
                    }
                }
            }
        }
        let names: std::collections::BTreeSet<&String> =
            known.iter().flat_map(|i| &i.references).collect();
        last_used.retain(|n, _| names.contains(n));
        drop(last_used);
        tokio::time::sleep(PRUNE_EVERY).await;
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

impl JailRuntime {
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
        let mut env: Vec<String> = cfg
            .env
            .iter()
            .filter(|e| {
                let k = e.split('=').next().unwrap_or("");
                !spec.env.contains_key(k)
            })
            .cloned()
            .collect();
        env.extend(spec.env.iter().map(|(k, v)| format!("{k}={v}")));
        let hostname = spec.hostname.clone().unwrap_or_else(|| spec.name.clone());
        env.push(format!("HOSTNAME={hostname}"));
        let (jail, root, dataset) = match (&parent, pod) {
            (_, true) => (id.clone(), PathBuf::from(POD_ROOT), None),
            (Some(p), false) => {
                let pod = load(&self.dir, p)?
                    .filter(|r| r.parent.is_none())
                    .ok_or_else(|| api(404, format!("no pod {p} to run in")))?;
                if argv.is_empty() {
                    return Err(api(400, format!("{}: no command to run", spec.name)));
                }
                (
                    format!("{}.{id}", pod.jail),
                    self.container_dir(&id).join("root"),
                    Some(format!("{}/{id}", self.dataset)),
                )
            }
            (None, false) => {
                return Err(api(
                    400,
                    "the jail runtime runs containers in pods: no parent jail given",
                ));
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
            if Path::new(&m) != rec.root {
                self.sys.unmount(&m).map_err(internal)?;
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
        if rec.parent.is_none() {
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
        self.sys
            .jail_create(&refs)
            .map_err(|e| internal(format!("creating jail {}: {e}", rec.jail)))
    }

    /// Starts the member's supervisor in its new jail and waits for its
    /// answer.
    async fn supervise(&self, rec: &Record, jid: u32) -> Result<()> {
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
        let mut child = cmd
            .spawn()
            .map_err(|e| internal(format!("starting the supervisor: {e}")))?;
        let stdout = child.stdout.take().expect("piped");
        let mut line = String::new();
        let read = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::io::BufReader::new(stdout).read_line(&mut line),
        )
        .await;
        // Reaped when it ends, while this agent runs; init reaps it after.
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        let line = line.trim();
        if let Some(pid) = line.strip_prefix("ok ") {
            tracing::debug!(container = %rec.name, pid, "started");
            return Ok(());
        }
        let why = match (read, line.strip_prefix("err ")) {
            (_, Some(why)) => why.to_owned(),
            (Err(_), _) => "the supervisor did not answer".to_owned(),
            _ => {
                let log = std::fs::read_to_string(dir.join(SHIM_LOG)).unwrap_or_default();
                format!("the supervisor ended: {}", log.trim())
            }
        };
        Err(api(500, format!("starting {}: {why}", rec.name)))
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
        let (pid, exit_code, started, finished) = match &s {
            Status::Running(r) => (r.pid, 0, r.started, 0),
            Status::Exited(e) => (0, e.code, 0, e.finished),
            Status::Created => (0, 0, 0, 0),
        };
        Ok(ContainerInspect {
            id: rec.id.clone(),
            name: rec.name.clone(),
            state: ContainerState {
                status: s.name().to_owned(),
                running: matches!(s, Status::Running(_)),
                pid: pid.into(),
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

impl legion_agent::Runtime for JailRuntime {
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

    async fn pull(&self, reference: &str, user: &str, logins: &[Login]) -> Result<()> {
        let pull_error = |message: String| Error::Pull {
            image: reference.to_owned(),
            message,
        };
        let registry = Reference::parse(reference)
            .map_err(|e| pull_error(e.to_string()))?
            .registry;
        let creds = logins
            .iter()
            .find(|l| legion_image::registry_of(&l.registry) == registry)
            .map(|l| Credentials {
                username: l.username.clone(),
                password: l.password.clone(),
            });
        // Before and after: an image pulled again must not go meanwhile.
        mark_used(&mut *self.used.lock().await, reference);
        let pulled = self.images.pull(reference, creds.as_ref(), user).await;
        mark_used(&mut *self.used.lock().await, reference);
        pulled.map(drop).map_err(|e| Error::Pull {
            image: reference.to_owned(),
            message: e.to_string(),
        })
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        let rec = self.make_record(spec)?;
        let dir = self.container_dir(&rec.id);
        std::fs::create_dir_all(&dir).map_err(internal)?;
        if let Some(ds) = rec.dataset.clone() {
            let image = self
                .images
                .get(&rec.image)
                .map_err(image_error)?
                .ok_or_else(|| api(404, format!("{}: image not known", rec.image)))?;
            let snapshot = self
                .images
                .snapshot(&image)
                .ok_or_else(|| api(400, format!("{}: an image without layers", rec.image)))?;
            let sys = self.sys.clone();
            let root = rec.root.clone();
            let parent = self.dataset.clone();
            let made = blocking(move || {
                sys.zfs_create(&parent, &[("mountpoint", "none")])
                    .map_err(internal)?;
                std::fs::create_dir_all(&root).map_err(internal)?;
                sys.zfs_clone(&snapshot, &ds, &[("mountpoint", &root.to_string_lossy())])
                    .map_err(internal)
            })
            .await;
            if let Err(e) = made {
                let _ = std::fs::remove_dir_all(&dir);
                return Err(e);
            }
        }
        state::write(&dir, RECORD, &rec).map_err(internal)?;
        Ok(rec.id)
    }

    async fn start(&self, id: &str) -> Result<()> {
        let rec = self.record(id)?;
        if matches!(self.status(&rec)?, Status::Running(_)) {
            return Ok(());
        }
        let dir = self.container_dir(id);
        state::remove(&dir, EXIT).map_err(internal)?;
        state::remove(&dir, RUN).map_err(internal)?;
        if let Some(parent) = &rec.parent {
            let pod = self.record(parent)?;
            if !matches!(self.status(&pod)?, Status::Running(_)) {
                return Err(api(409, format!("pod {parent} is not running")));
            }
        }
        // A jail left from a run whose supervisor died.
        if jail_names(&*self.sys)?.contains(&rec.jail) {
            let _ = self.sys.jail_remove(&rec.jail);
        }
        if rec.parent.is_some() {
            self.mount(&rec)?;
        }
        let jid = self.create_jail(&rec)?;
        if rec.parent.is_none() {
            let run = Run {
                shim: 0,
                pid: 0,
                jid,
                started: state::now(),
                boot: self.boot,
            };
            return state::write(&dir, RUN, &run).map_err(internal);
        }
        if let Err(e) = self.supervise(&rec, jid).await {
            let _ = self.sys.jail_remove(&rec.jail);
            return Err(e);
        }
        Ok(())
    }

    async fn stop(&self, id: &str, timeout: Duration) -> Result<()> {
        let rec = self.record(id)?;
        let Status::Running(run) = self.status(&rec)? else {
            return Ok(());
        };
        if rec.parent.is_none() {
            return match self.sys.jail_remove(&rec.jail) {
                Ok(()) | Err(legion_sys::Error::NotFound(_)) => Ok(()),
                Err(e) => Err(internal(e)),
            };
        }
        #[cfg(unix)]
        if alive(run.shim) {
            // SAFETY: the supervisor sends the container's stop signal.
            unsafe { libc::kill(run.shim as libc::pid_t, libc::SIGTERM) };
        }
        if self.wait_over(&rec, timeout).await? {
            return Ok(());
        }
        match self.sys.jail_remove(&rec.jail) {
            Ok(()) | Err(legion_sys::Error::NotFound(_)) => {}
            Err(e) => return Err(internal(e)),
        }
        if !self.wait_over(&rec, Duration::from_secs(10)).await? {
            // The supervisor is gone: record the end for it.
            state::write(
                &self.container_dir(id),
                EXIT,
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
        self.stop(id, Duration::ZERO).await?;
        if rec.parent.is_none() {
            // Members first; then the pod's jail if it outlived them.
            match self.sys.jail_remove(&rec.jail) {
                Ok(()) | Err(legion_sys::Error::NotFound(_)) => {}
                Err(e) => return Err(internal(e)),
            }
        } else {
            self.unmount(&rec)?;
            if let Some(ds) = &rec.dataset
                && !self.sys.zfs_list(ds).map_err(internal)?.is_empty()
            {
                self.sys
                    .zfs_destroy(ds, true)
                    .map_err(|e| internal(format!("destroying {ds}: {e}")))?;
            }
        }
        std::fs::remove_dir_all(self.container_dir(id)).map_err(internal)?;
        Ok(())
    }

    async fn inspect(&self, id: &str) -> Result<ContainerInspect> {
        let rec = self.record(id)?;
        self.inspect_record(&rec)
    }

    async fn wait(&self, id: &str) -> Result<i32> {
        let rec = self.record(id)?;
        loop {
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
            "0".into(),
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
        let child = command.spawn().map_err(internal)?;
        let group = KillGroup(child.id());
        let out = child.wait_with_output().await;
        // Reaped now, so its id may be another's soon.
        group.disarm();
        let out = out.map_err(internal)?;
        Ok(ExecOutput {
            exit_code: out.status.code().unwrap_or(255),
            stdout: out.stdout,
            stderr: out.stderr,
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
            if tty { "1" } else { "0" }.into(),
        ];
        args.extend(cmd.iter().map(OsString::from));
        let refs: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        let mut command = self.shim_command(&refs);
        let exec_id = new_id();
        let (code_tx, code_rx) = watch::channel(None);
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
                reap(child, code_tx, self.execs.clone(), exec_id.clone());
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
                        master: Some(master),
                    },
                );
                // The terminal reports EIO once the process has closed it.
                let output = tokio_util_read(reader)
                    .map(|r| {
                        r.map(|data| Chunk {
                            stream: Output::Stdout,
                            data,
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
        reap(child, code_tx, self.execs.clone(), exec_id.clone());
        self.execs.lock().expect("execs").insert(
            exec_id.clone(),
            Session {
                code: code_rx,
                group,
                jid,
                #[cfg(unix)]
                master: None,
            },
        );
        let tag = |s: Output| {
            move |r: std::io::Result<Bytes>| {
                r.map(|data| Chunk { stream: s, data }).map_err(Error::Io)
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
        self.execs.lock().expect("execs").remove(exec_id);
        Ok(got)
    }

    /// Its command and what it started, if still running, are hung up as
    /// a terminal would be, and killed if any outlast
    /// [`EXEC_HANGUP_GRACE`]: no one is left to read them. That is the
    /// command's process group in the container's jail, which the command
    /// itself may have left already. Dropping the session drops a
    /// terminal's last descriptor.
    async fn exec_release(&self, exec_id: &str) {
        let Some(session) = self.execs.lock().expect("execs").remove(exec_id) else {
            return;
        };
        let (Some(group), jid) = (session.group, session.jid) else {
            return;
        };
        drop(session);
        if !signal_group(group, jid, "HUP").await {
            return;
        }
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + EXEC_HANGUP_GRACE;
            while tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if !signal_group(group, jid, "0").await {
                    return;
                }
            }
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
        let first = log::read_all(&path, tail).map_err(internal)?;
        let offset = std::fs::metadata(&path).map_or(0, |m| m.len());
        let initial = futures::stream::iter(first.into_iter().map(Ok));
        if !follow {
            return Ok(initial.boxed());
        }
        let sys = self.sys.clone();
        let dir = self.dir.clone();
        let boot = self.boot;
        let more = futures::stream::unfold((path, offset, false), move |(path, offset, last)| {
            let (sys, dir, rec) = (sys.clone(), dir.clone(), rec.clone());
            async move {
                if last {
                    return None;
                }
                loop {
                    let over = jail_names(&*sys)
                        .and_then(|j| status(&dir, &rec, boot, &j))
                        .map_or(true, |s| matches!(s, Status::Exited(_)));
                    let len = std::fs::metadata(&path).map_or(0, |m| m.len());
                    let from = if len < offset { 0 } else { offset };
                    let (chunks, at) = match log::read_from(&path, from) {
                        Ok(r) => r,
                        Err(e) => return Some((vec![Err(Error::Io(e))], (path, from, true))),
                    };
                    if !chunks.is_empty() || over {
                        let chunks = chunks.into_iter().map(Ok).collect();
                        return Some((chunks, (path, at, over)));
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
        let jails = jail_names(&*self.sys)?;
        let mut out = Vec::new();
        for rec in records(&self.dir)? {
            if !labelled(&rec.labels, &wanted) {
                continue;
            }
            let s = status(&self.dir, &rec, self.boot, &jails)?;
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
        use tokio::io::AsyncReadExt;
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
}
