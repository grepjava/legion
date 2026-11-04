//! What a tool on one host calls beyond [`legion_runtime::Runtime`]
//! (jailer, which builds and tries images): containers found by name,
//! a member's process run in the foreground on the caller's terminal, an
//! exec likewise, and a build's `RUN` steps.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use super::{
    JailRuntime, KillGroup, Status, api, confine, cpu_list, exit_code, internal, jail_names,
    new_id, records, target,
};
use crate::DEVFS_RULESET;
use crate::shim::Terminal;
use crate::state::{self, EXIT, Exit, RECORD, RUN, Record, Run};
use legion_runtime::Result;

/// One `RUN` step of a build: a command in a jail on a directory, which
/// is the build's file system as it stands.
pub struct Step<'a> {
    /// The jail's name (and its host name).
    pub name: &'a str,
    /// A plain directory, the step's root.
    pub root: &'a Path,
    pub argv: &'a [String],
    /// `NAME=value`.
    pub env: &'a [String],
    /// `user[:group]`, resolved in the root's `/etc/passwd` and `/etc/group`;
    /// empty for root.
    pub user: &'a str,
    /// Made if missing; empty for `/`.
    pub work_dir: &'a str,
    pub mounts: &'a [StepMount],
    pub network: StepNetwork,
    /// More jail parameters, which win over the step's own.
    pub params: &'a [(String, String)],
    /// The CPUs the step may run on (as `crate::ANNOTATION_CPUSET`), set
    /// on its jail before the step runs.
    pub cpuset: Option<&'a str>,
}

/// A mount for one step. What the step made to mount it on goes after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepMount {
    /// A host file or directory, by nullfs.
    Nullfs {
        source: PathBuf,
        target: String,
        read_only: bool,
    },
    Tmpfs {
        target: String,
        size: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepNetwork {
    /// The host's addresses (`ip4`/`ip6` inherited), as `pkg` needs.
    Host,
    /// A VNET jail of its own: only `lo0`, down.
    None,
}

/// A mount point, and the first directory or file made for it.
type Made = (PathBuf, Option<PathBuf>);

impl JailRuntime {
    /// A container's id from its id, its name, or the start of one id.
    pub fn find(&self, id_or_name: &str) -> Result<String> {
        let all = records(&self.dir)?;
        if let Some(r) = all
            .iter()
            .find(|r| r.id == id_or_name || r.name == id_or_name)
        {
            return Ok(r.id.clone());
        }
        let mut like = all
            .iter()
            .filter(|r| !id_or_name.is_empty() && r.id.starts_with(id_or_name));
        match (like.next(), like.next()) {
            (Some(r), None) => Ok(r.id.clone()),
            (Some(_), Some(_)) => Err(api(409, format!("{id_or_name} names several containers"))),
            _ => Err(api(404, format!("no such container {id_or_name}"))),
        }
    }

    /// Runs a member's process with this process's output (and input, if
    /// `stdin`; on its terminal, if `tty`) instead of under a supervisor,
    /// until it ends; its jail goes then. Returns its exit code, which is
    /// also recorded, as a supervised run's is.
    pub async fn run_attached(&self, id: &str, stdin: bool, tty: bool) -> Result<i32> {
        let rec = self.record(id)?;
        if rec.is_pod() {
            return Err(api(400, format!("{id} is a pod, which runs nothing")));
        }
        if matches!(self.status(&rec)?, Status::Running(_)) {
            return Err(api(409, format!("container {id} is running")));
        }
        if let Some(parent) = &rec.parent {
            let pod = self.record(parent)?;
            if !matches!(self.status(&pod)?, Status::Running(_)) {
                return Err(api(409, format!("pod {parent} is not running")));
            }
        }
        let dir = self.container_dir(id);
        // The run before the end, as `start` clears them.
        state::remove(&dir, RUN).map_err(internal)?;
        state::remove(&dir, EXIT).map_err(internal)?;
        if jail_names(&*self.sys)?.contains(&rec.jail) {
            let _ = self.sys.jail_remove(&rec.jail);
        }
        // What a failed start mounted goes, as `start` leaves it.
        let jid = match self.mount(&rec).and_then(|()| self.create_jail(&rec)) {
            Ok(j) => j,
            Err(e) => {
                let _ = self.unmount(&rec);
                return Err(e);
            }
        };
        let mut undo = UndoAttached {
            rt: self,
            rec: &rec,
            done: false,
        };
        let mut cmd = self.shim_exec(&rec, jid, &rec.argv, stdin, tty);
        cmd.kill_on_drop(true);
        let status = match cmd.spawn() {
            Ok(mut child) => {
                // The supervisor's exec becomes the command, which is so
                // its own supervisor: a stop sends it the stop signal, not
                // this process a SIGTERM.
                let pid = child.id().unwrap_or_default();
                let run = Run {
                    shim: pid,
                    pid,
                    jid,
                    started: state::now(),
                    boot: self.boot,
                };
                state::write(&dir, RUN, &run).map_err(internal)?;
                child.wait().await
            }
            Err(e) => Err(e),
        };
        let (code, error) = match &status {
            Ok(s) => (exit_code(*s), None),
            Err(e) => (127, Some(e.to_string())),
        };
        undo.finish(code, error)?;
        status.map_err(|e| internal(format!("running {}: {e}", rec.name)))?;
        Ok(code)
    }

    /// Runs `cmd` in a running member with this process's output (and
    /// input, if `stdin`; on its terminal, if `tty`), until it ends.
    pub async fn exec_inherit(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> Result<i32> {
        let (rec, jid) = self.running_jid(id)?;
        let mut command = self.shim_exec(&rec, jid, cmd, stdin, tty);
        // Gone with its caller, as an attached run's command is. Unless it
        // reads the caller's terminal (where only the foreground group
        // may), in a group of its own, so what it started goes too.
        command.kill_on_drop(true);
        let own_group = !stdin && !tty;
        if own_group {
            command.process_group(0);
        }
        let mut child = command.spawn().map_err(internal)?;
        let group = KillGroup(own_group.then(|| child.id()).flatten());
        let status = child.wait().await;
        // Reaped now, so its group's id may be another's soon.
        group.disarm();
        Ok(exit_code(status.map_err(internal)?))
    }

    /// The supervisor's `exec` of `argv` in `rec`'s jail, on this
    /// process's output.
    fn shim_exec(
        &self,
        rec: &Record,
        jid: u32,
        argv: &[String],
        stdin: bool,
        tty: bool,
    ) -> tokio::process::Command {
        self.shim_exec_in(&self.container_dir(&rec.id), jid, argv, stdin, tty)
    }

    fn shim_exec_in(
        &self,
        dir: &Path,
        jid: u32,
        argv: &[String],
        stdin: bool,
        tty: bool,
    ) -> tokio::process::Command {
        let mut args: Vec<OsString> = vec![
            "exec".into(),
            dir.as_os_str().to_owned(),
            jid.to_string().into(),
            // The caller's terminal, not one of its own: a new session
            // could not have it as its controlling terminal.
            if tty {
                Terminal::Caller
            } else {
                Terminal::None
            }
            .arg()
            .into(),
        ];
        args.extend(argv.iter().map(OsString::from));
        let refs: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        let mut cmd = self.shim_command(&refs);
        cmd.stdin(if stdin || tty {
            std::process::Stdio::inherit()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
        cmd
    }

    /// Runs a build step and returns its exit code. Its output is this
    /// process's; its input is empty. Unlike a container's, its jail may
    /// change file flags (`pkg` sets them) and has no securelevel. When
    /// the image has no `/etc/resolv.conf` (or an empty one), the host's
    /// is there while the step runs. The jail and the step's mounts go
    /// afterwards, and so do the mount points and the resolver file it
    /// made, so none of them lands in the layer.
    /// So does all that when the future is dropped before the step ends.
    pub async fn run_step(&self, step: &Step<'_>) -> Result<i32> {
        let id = new_id();
        let dir = self.dir.join(".steps").join(&id);
        std::fs::create_dir_all(&dir).map_err(internal)?;
        let mut undo = UndoStep {
            rt: self,
            name: step.name,
            dir,
            made: Vec::new(),
            resolver: None,
        };
        self.step_in(step, &mut undo).await
    }

    async fn step_in(&self, step: &Step<'_>, undo: &mut UndoStep<'_>) -> Result<i32> {
        let (dir, made) = (undo.dir.clone(), &mut undo.made);
        let dir = dir.as_path();
        let work_dir = if step.work_dir.is_empty() {
            "/"
        } else {
            step.work_dir
        };
        // As the jail will see it: links resolve inside the root (FreeBSD's
        // /home is one), and what is missing is made.
        work_dir_in(step.root, work_dir)?;
        undo.resolver = (step.network == StepNetwork::Host)
            .then(|| Resolver::lend(step.root))
            .transpose()?
            .flatten();
        // The supervisor reads the user, environment and directory here.
        let rec = Record {
            id: new_id(),
            name: step.name.to_owned(),
            image: String::new(),
            created: state::now(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            parent: None,
            standalone: true,
            jail: step.name.to_owned(),
            root: step.root.to_path_buf(),
            dataset: None,
            argv: step.argv.to_vec(),
            env: step.env.to_vec(),
            user: step.user.to_owned(),
            work_dir: work_dir.to_owned(),
            hostname: step.name.to_owned(),
            mounts: Vec::new(),
            params: Vec::new(),
            stop_signal: 15,
            stop_timeout: 10,
            linux: false,
            cpuset: None,
        };
        let cpus = step.cpuset.map(cpu_list).transpose()?;
        state::write(dir, RECORD, &rec).map_err(internal)?;
        let dev = mount_point(step.root, "/dev", false, made)?;
        self.sys
            .mount_devfs(&dev.to_string_lossy(), DEVFS_RULESET)
            .map_err(internal)?;
        for m in step.mounts {
            match m {
                StepMount::Nullfs {
                    source,
                    target,
                    read_only,
                } => {
                    let at = mount_point(step.root, target, source.is_file(), made)?;
                    self.sys
                        .mount_nullfs(&source.to_string_lossy(), &at.to_string_lossy(), *read_only)
                        .map_err(internal)?;
                }
                StepMount::Tmpfs { target, size } => {
                    let at = mount_point(step.root, target, false, made)?;
                    self.sys
                        .mount_tmpfs(&at.to_string_lossy(), *size, 0o1777)
                        .map_err(internal)?;
                }
            }
        }
        let params = step_params(step);
        let refs: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let jid = self
            .sys
            .jail_create(&refs)
            .map_err(|e| internal(format!("creating jail {}: {e}", step.name)))?;
        if let Some(cpus) = &cpus {
            confine(&*self.sys, step.name, jid, cpus)?;
        }
        let mut cmd = self.shim_exec_in(dir, jid, step.argv, false, false);
        cmd.kill_on_drop(true);
        let status = cmd.status().await.map_err(internal)?;
        Ok(exit_code(status))
    }
}

/// Ends an attached run: its jail and mounts go, then its exit is
/// recorded. Dropped before [`UndoAttached::finish`], as when the run's
/// future is, it does the same, recording the run as cancelled.
struct UndoAttached<'a> {
    rt: &'a JailRuntime,
    rec: &'a Record,
    done: bool,
}

impl UndoAttached<'_> {
    fn finish(&mut self, code: i32, error: Option<String>) -> Result<()> {
        self.done = true;
        let _ = self.rt.sys.jail_remove(&self.rec.jail);
        let _ = self.rt.unmount(self.rec);
        let exit = Exit {
            code,
            finished: state::now(),
            error,
        };
        state::write(&self.rt.container_dir(&self.rec.id), EXIT, &exit).map_err(internal)
    }
}

impl Drop for UndoAttached<'_> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.finish(255, Some("cancelled".into()));
        }
    }
}

/// Undoes a step when dropped, whether it ended or its future was dropped:
/// its jail goes, then its mounts and what was made for them, then the
/// lent resolver file (nothing in the jail reads it any more), then its
/// record.
struct UndoStep<'a> {
    rt: &'a JailRuntime,
    name: &'a str,
    dir: PathBuf,
    made: Vec<Made>,
    resolver: Option<Resolver>,
}

impl Drop for UndoStep<'_> {
    fn drop(&mut self) {
        let _ = self.rt.sys.jail_remove(self.name);
        for (at, first) in self.made.iter().rev() {
            let _ = self.rt.sys.unmount(&at.to_string_lossy());
            if let Some(first) = first {
                remove_made(at, first);
            }
        }
        drop(self.resolver.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A step's jail: as a member's, but allowed to change file flags, with
/// no securelevel, and on the host's network or a VNET of its own.
fn step_params(step: &Step<'_>) -> Vec<(String, String)> {
    let mut p: Vec<(String, String)> = vec![
        ("name".into(), step.name.to_owned()),
        ("path".into(), step.root.to_string_lossy().into_owned()),
        ("host.hostname".into(), step.name.to_owned()),
        ("persist".into(), String::new()),
        ("enforce_statfs".into(), "2".into()),
        ("devfs_ruleset".into(), DEVFS_RULESET.to_string()),
        ("allow.chflags".into(), "true".into()),
    ];
    match step.network {
        StepNetwork::Host => p.extend([
            ("ip4".into(), "inherit".into()),
            ("ip6".into(), "inherit".into()),
        ]),
        StepNetwork::None => p.push(("vnet".into(), String::new())),
    }
    for (k, v) in step.params {
        match p.iter_mut().find(|(n, _)| n == k) {
            Some(slot) => slot.1 = v.clone(),
            None => p.push((k.clone(), v.clone())),
        }
    }
    p
}

/// `dest` under `root` (see [`target`]), noting it and the first
/// directory or file made on the way to it.
fn mount_point(root: &Path, dest: &str, file: bool, made: &mut Vec<Made>) -> Result<PathBuf> {
    let first = first_missing(root, dest);
    let at = target(root, dest, file).inspect_err(|_| {
        // What it made before failing; all of it under `first` is new.
        if let Some(first) = &first {
            let _ = std::fs::remove_dir_all(first);
        }
    })?;
    made.push((at.clone(), first));
    Ok(at)
}

/// The first missing path on the way from `root` to `dest` inside it.
fn first_missing(root: &Path, dest: &str) -> Option<PathBuf> {
    let mut path = root.to_path_buf();
    for c in Path::new(dest).components() {
        if let Component::Normal(name) = c {
            path.push(name);
            if std::fs::symlink_metadata(&path).is_err() {
                return Some(path);
            }
        }
    }
    None
}

/// Removes `at` and its parents up to `first`, while each is an empty
/// directory or file: what was made for a mount, unless the step wrote
/// there.
fn remove_made(at: &Path, first: &Path) {
    let mut cur = at.to_path_buf();
    loop {
        let removed = match std::fs::symlink_metadata(&cur) {
            Ok(m) if m.is_dir() => std::fs::remove_dir(&cur).is_ok(),
            Ok(m) if m.is_file() && m.len() == 0 => std::fs::remove_file(&cur).is_ok(),
            _ => false,
        };
        if !removed || cur == first {
            return;
        }
        match cur.parent() {
            Some(p) => cur = p.to_path_buf(),
            None => return,
        }
    }
}

/// The host's resolver file lent to a root that has none (or an empty
/// one) while a step runs; the root's own comes back when dropped.
struct Resolver {
    root: PathBuf,
    path: PathBuf,
    /// The first directory or file made for it, if any was.
    made: Option<PathBuf>,
    /// The root's file was there, empty.
    was_empty: bool,
    /// What was lent: a file that no longer holds it is the step's own.
    lent: Vec<u8>,
}

impl Resolver {
    fn lend(root: &Path) -> Result<Option<Resolver>> {
        const HOST: &str = "/etc/resolv.conf";
        let path = root.join("etc/resolv.conf");
        let was = std::fs::symlink_metadata(&path).ok();
        // The root's own, a link, or no file at all (a FIFO would hold
        // the write up): left alone.
        if was
            .as_ref()
            .is_some_and(|m| !m.file_type().is_file() || m.len() > 0)
        {
            return Ok(None);
        }
        // Nor lent through a linked `/etc` (`/etc -> usr/etc`): it might
        // lead out of the root, and the step runs without it.
        if std::fs::symlink_metadata(root.join("etc")).is_ok_and(|m| m.file_type().is_symlink()) {
            return Ok(None);
        }
        let Ok(host) = std::fs::read(HOST) else {
            return Ok(None);
        };
        let made = first_missing(root, HOST);
        let path = target(root, HOST, true)?;
        std::fs::write(&path, &host).map_err(internal)?;
        Ok(Some(Resolver {
            root: root.to_path_buf(),
            path,
            made,
            was_empty: was.is_some(),
            lent: host,
        }))
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        // The step may have made `/etc` or the file a link, maybe to the
        // host's own: then the root's is left as the step made it.
        if !unlinked(&self.root, &self.path) {
            return;
        }
        // One the step wrote is the step's.
        if std::fs::read(&self.path).ok().as_ref() != Some(&self.lent) {
            return;
        }
        let _ = std::fs::remove_file(&self.path);
        if self.was_empty {
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path);
            return;
        }
        // And `/etc`, if it was made for it.
        if let (Some(first), Some(parent)) = (&self.made, self.path.parent())
            && *first != self.path
        {
            remove_made(parent, first);
        }
    }
}

/// A step's working directory `dir` under `root`, resolved as a process
/// in the jail would: links are followed inside the root (an absolute one
/// starts again from it, `..` stops at it); directories missing are made.
fn work_dir_in(root: &Path, dir: &str) -> Result<PathBuf> {
    // Hops through links before giving up, as the kernel does.
    const MAX_LINKS: usize = 40;
    let mut rest: Vec<OsString> = Vec::new();
    let push = |rest: &mut Vec<OsString>, p: &Path| {
        // Kept reversed: the next component is last.
        let mut parts: Vec<OsString> = p
            .components()
            .filter_map(|c| match c {
                Component::Normal(n) => Some(n.to_owned()),
                Component::ParentDir => Some(OsString::from("..")),
                _ => None,
            })
            .collect();
        parts.reverse();
        rest.extend(parts);
    };
    push(&mut rest, Path::new(dir));
    let mut at: Vec<OsString> = Vec::new();
    let mut links = 0;
    while let Some(name) = rest.pop() {
        if name == ".." {
            at.pop();
            continue;
        }
        let path: PathBuf = std::iter::once(root.as_os_str().to_owned())
            .chain(at.iter().cloned())
            .chain([name.clone()])
            .collect();
        match std::fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => {
                links += 1;
                if links > MAX_LINKS {
                    return Err(api(400, format!("working directory {dir}: too many links")));
                }
                let to = std::fs::read_link(&path).map_err(internal)?;
                if to.is_absolute() {
                    at.clear();
                }
                push(&mut rest, &to);
            }
            Ok(m) if m.is_dir() => at.push(name),
            Ok(_) => {
                return Err(api(
                    400,
                    format!("working directory {dir} is not a directory"),
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&path).map_err(internal)?;
                at.push(name);
            }
            Err(e) => return Err(internal(e)),
        }
    }
    Ok(std::iter::once(root.as_os_str().to_owned())
        .chain(at)
        .collect())
}

/// Whether `path`, under `root`, is a file reached through no link.
fn unlinked(root: &Path, path: &Path) -> bool {
    let Ok(rest) = path.strip_prefix(root) else {
        return false;
    };
    let mut cur = root.to_path_buf();
    let mut parts = rest.components().peekable();
    while let Some(part) = parts.next() {
        cur.push(part);
        match std::fs::symlink_metadata(&cur) {
            Ok(m) if parts.peek().is_none() => return m.is_file(),
            Ok(m) if m.is_dir() => {}
            _ => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step's working directory through a link (FreeBSD's /home) is
    /// where the jail will find it, links resolved inside the root.
    #[cfg(unix)]
    #[test]
    fn working_directories_follow_links_inside_the_root() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        std::fs::create_dir_all(root.join("usr/home")).unwrap();
        std::os::unix::fs::symlink("usr/home", root.join("home")).unwrap();
        std::os::unix::fs::symlink("/usr/home", root.join("abs")).unwrap();
        std::os::unix::fs::symlink("../../..", root.join("usr/home/up")).unwrap();
        std::os::unix::fs::symlink("loop", root.join("loop")).unwrap();

        let made = work_dir_in(root, "/home/app").unwrap();
        assert_eq!(made, root.join("usr/home/app"));
        assert!(made.is_dir());
        assert_eq!(work_dir_in(root, "/abs/app").unwrap(), made);
        // `..` stops at the root, through links too.
        assert_eq!(work_dir_in(root, "/../home/app").unwrap(), made);
        assert_eq!(work_dir_in(root, "/home/up/etc").unwrap(), root.join("etc"));
        assert!(work_dir_in(root, "/loop").is_err());
        std::fs::write(root.join("file"), "").unwrap();
        assert!(work_dir_in(root, "/file").is_err());
    }

    #[test]
    fn a_step_jail_is_a_member_that_may_change_flags() {
        let root = Path::new("/b/root");
        let step = Step {
            name: "jailer_build_1",
            root,
            argv: &[],
            env: &[],
            user: "",
            work_dir: "",
            mounts: &[],
            network: StepNetwork::Host,
            params: &[
                ("allow.chflags".into(), "false".into()),
                ("x".into(), "1".into()),
            ],
            cpuset: None,
        };
        let p = step_params(&step);
        let get = |k: &str| p.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("ip4"), Some("inherit"));
        assert_eq!(get("vnet"), None);
        assert_eq!(get("devfs_ruleset"), Some("4"));
        assert_eq!(get("securelevel"), None);
        // The caller's parameters win.
        assert_eq!(get("allow.chflags"), Some("false"));
        assert_eq!(get("x"), Some("1"));
        let p = step_params(&Step {
            network: StepNetwork::None,
            ..step
        });
        assert!(p.iter().any(|(k, _)| k == "vnet"));
        assert!(!p.iter().any(|(k, _)| k == "ip4"));
    }

    #[test]
    fn what_a_step_made_for_its_mounts_goes() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        std::fs::create_dir(root.join("etc")).unwrap();
        let mut made = Vec::new();
        let a = mount_point(root, "/run/secrets/key", true, &mut made).unwrap();
        let b = mount_point(root, "/etc/x", false, &mut made).unwrap();
        assert!(a.is_file() && b.is_dir());
        // The step wrote beside one of them: that stays.
        std::fs::write(root.join("run/kept"), b"x").unwrap();
        for (at, first) in made.iter().rev() {
            remove_made(at, first.as_ref().unwrap());
        }
        assert!(!root.join("run/secrets").exists());
        assert!(root.join("run/kept").exists());
        assert!(!root.join("etc/x").exists() && root.join("etc").is_dir());
    }

    #[test]
    fn a_refused_mount_point_leaves_nothing_made() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        let mut made = Vec::new();
        assert!(mount_point(root, "/a/b/../../../x", false, &mut made).is_err());
        assert!(made.is_empty());
        assert!(!root.join("a").exists());
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
    }

    #[test]
    fn the_resolver_is_lent_and_taken_back() {
        let Ok(host) = std::fs::read("/etc/resolv.conf") else {
            return;
        };
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        // No /etc at all: both go afterwards.
        let r = Resolver::lend(root).unwrap().unwrap();
        assert_eq!(std::fs::read(root.join("etc/resolv.conf")).unwrap(), host);
        drop(r);
        assert!(!root.join("etc").exists());
        // An empty one stays empty.
        std::fs::create_dir(root.join("etc")).unwrap();
        std::fs::write(root.join("etc/resolv.conf"), b"").unwrap();
        drop(Resolver::lend(root).unwrap().unwrap());
        assert_eq!(std::fs::read(root.join("etc/resolv.conf")).unwrap(), b"");
        // The image's own is left alone.
        std::fs::write(root.join("etc/resolv.conf"), b"nameserver 10.0.0.1\n").unwrap();
        assert!(Resolver::lend(root).unwrap().is_none());
        // One the step writes is the step's, whether there was none or an
        // empty one.
        for was_empty in [false, true] {
            std::fs::remove_file(root.join("etc/resolv.conf")).unwrap();
            if was_empty {
                std::fs::write(root.join("etc/resolv.conf"), b"").unwrap();
            }
            let r = Resolver::lend(root).unwrap().unwrap();
            std::fs::write(root.join("etc/resolv.conf"), b"nameserver 10.9.9.9\n").unwrap();
            drop(r);
            assert_eq!(
                std::fs::read(root.join("etc/resolv.conf")).unwrap(),
                b"nameserver 10.9.9.9\n"
            );
        }
        // Nothing lent to a FIFO (the write would wait for a reader) or
        // through a linked `/etc`.
        std::fs::remove_file(root.join("etc/resolv.conf")).unwrap();
        let fifo =
            std::ffi::CString::new(root.join("etc/resolv.conf").as_os_str().as_encoded_bytes())
                .unwrap();
        // SAFETY: a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(Resolver::lend(root).unwrap().is_none());
        std::fs::remove_file(root.join("etc/resolv.conf")).unwrap();
        std::fs::create_dir_all(root.join("usr/etc")).unwrap();
        std::fs::remove_dir(root.join("etc")).unwrap();
        std::os::unix::fs::symlink("usr/etc", root.join("etc")).unwrap();
        assert!(Resolver::lend(root).unwrap().is_none());
        assert!(!root.join("usr/etc/resolv.conf").exists());
    }

    /// A step that makes `/etc` or the file a link does not have what is
    /// behind the link emptied or removed when the resolver is taken back.
    #[test]
    fn taking_the_resolver_back_follows_no_link() {
        let t = tempfile::tempdir().unwrap();
        let host = tempfile::tempdir().unwrap();
        let theirs = host.path().join("resolv.conf");
        for was_empty in [true, false] {
            for link_etc in [true, false] {
                let root = t.path().join(format!("{was_empty}-{link_etc}"));
                std::fs::create_dir_all(root.join("etc")).unwrap();
                std::fs::write(&theirs, b"host's\n").unwrap();
                let r = Resolver {
                    root: root.clone(),
                    path: root.join("etc/resolv.conf"),
                    made: None,
                    was_empty,
                    lent: b"lent\n".to_vec(),
                };
                if link_etc {
                    std::fs::remove_dir_all(root.join("etc")).unwrap();
                    std::os::unix::fs::symlink(host.path(), root.join("etc")).unwrap();
                } else {
                    std::os::unix::fs::symlink(&theirs, root.join("etc/resolv.conf")).unwrap();
                }
                drop(r);
                assert_eq!(std::fs::read(&theirs).unwrap(), b"host's\n");
            }
        }
    }
}
