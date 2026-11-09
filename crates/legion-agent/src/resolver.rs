//! The node resolver as a process of its own (`legion node-resolver`),
//! which outlives the daemon as pods do: pods' name lookups go on while
//! the daemon restarts or is upgraded. The agent keeps the zone file it
//! answers from, and makes sure one of this build's is running: a resolver
//! of another build, or none, is replaced, the new one binding the address
//! beside the old before the old is stopped.

use std::ffi::OsString;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Duration;

/// How the agent runs the resolver.
#[derive(Debug, Clone)]
pub struct ExternalResolver {
    /// The command, before its `--listen`, `--zone` and `--pid`: this
    /// build's `legion node-resolver`.
    pub command: Vec<OsString>,
    /// Where its zone file, pid file and log go.
    pub dir: PathBuf,
}

impl ExternalResolver {
    pub fn zone_file(&self) -> PathBuf {
        self.dir.join("zone.json")
    }

    #[cfg(unix)]
    fn pid_file(&self) -> PathBuf {
        self.dir.join("resolver.pid")
    }
}

/// What the pid file says: the resolver's pid, its version and address.
#[cfg(unix)]
fn running(pid_file: &Path) -> Option<(u32, String, String)> {
    let line = std::fs::read_to_string(pid_file).ok()?;
    let mut words = line.split_whitespace();
    let pid = words.next()?.parse().ok()?;
    Some((pid, words.next()?.to_owned(), words.next()?.to_owned()))
}

/// Whether `pid` is a node resolver still.
#[cfg(unix)]
fn is_resolver(pid: u32) -> bool {
    std::process::Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
        .is_ok_and(|o| {
            o.status.success() && String::from_utf8_lossy(&o.stdout).contains("node-resolver")
        })
}

/// Makes sure a resolver of `version` serves `listen`, starting one if
/// none does and stopping one of another build or address once the new
/// one is up. A zone file is there first: an empty one if none.
#[cfg(unix)]
pub fn ensure(r: &ExternalResolver, listen: SocketAddr, version: &str) -> Result<(), String> {
    use std::os::unix::process::CommandExt;

    std::fs::create_dir_all(&r.dir).map_err(|e| format!("{}: {e}", r.dir.display()))?;
    let zone = r.zone_file();
    if !zone.exists() {
        legion_net::dns::write_zone_file(&zone, &[])
            .map_err(|e| format!("{}: {e}", zone.display()))?;
    }
    let pid_file = r.pid_file();
    let old = running(&pid_file).filter(|(pid, ..)| is_resolver(*pid));
    if let Some((_, v, l)) = &old
        && v == version
        && *l == listen.to_string()
    {
        return Ok(());
    }
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(r.dir.join("resolver.log"))
        .map_err(|e| format!("the resolver's log: {e}"))?;
    let (program, args) = r.command.split_first().ok_or("no resolver command")?;
    let child = std::process::Command::new(program)
        .args(args)
        .arg("--listen")
        .arg(listen.to_string())
        .arg("--zone")
        .arg(&zone)
        .arg("--pid")
        .arg(&pid_file)
        // Its own group: what stops the daemon leaves it running.
        .process_group(0)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone().map_err(|e| e.to_string())?)
        .stderr(log)
        .spawn()
        .map_err(|e| format!("starting the node resolver: {e}"))?;
    let pid = child.id();
    // Not waited for: it outlives this process.
    drop(child);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while running(&pid_file).is_none_or(|(p, ..)| p != pid) {
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "the node resolver (pid {pid}) did not start: see {}",
                r.dir.join("resolver.log").display()
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if let Some((old, ..)) = old
        && old != pid
    {
        // SAFETY: a plain signal to a process checked to be a resolver.
        unsafe {
            libc::kill(old as libc::pid_t, libc::SIGTERM);
        }
    }
    tracing::info!(pid, %listen, "node resolver started");
    Ok(())
}

#[cfg(not(unix))]
pub fn ensure(_: &ExternalResolver, _: SocketAddr, _: &str) -> Result<(), String> {
    Err("the node resolver runs on FreeBSD".into())
}
