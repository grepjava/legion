//! What the runtime keeps of a container, one directory each under its
//! state directory: `record.json` (made at create), `run.json` (written by
//! the supervisor once the process runs), `exit.json` (written by the
//! supervisor when it ends) and `log`. They outlive the agent, which finds
//! its containers again from them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub name: String,
    pub image: String,
    /// Unix seconds.
    pub created: u64,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    /// The pod (infra container) a member runs in; `None` for a pod or a
    /// standalone member.
    pub parent: Option<String>,
    /// A member with no pod: a jail of its own on the host's network.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub standalone: bool,
    /// The jail's name: the id, or `<pod>.<id>` for a pod's member.
    pub jail: String,
    pub root: PathBuf,
    /// The member's root file system, a clone of its image.
    pub dataset: Option<String>,
    pub argv: Vec<String>,
    /// `NAME=value`.
    pub env: Vec<String>,
    /// `user[:group]`, by name or number, resolved in the jail.
    pub user: String,
    pub work_dir: String,
    pub hostname: String,
    pub mounts: Vec<Mount>,
    /// Jail parameters beyond the runtime's own.
    pub params: Vec<(String, String)>,
    pub stop_signal: i32,
    /// Seconds.
    pub stop_timeout: u64,
    /// A Linux image's, run through the Linux ABI.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub linux: bool,
    /// The CPUs its jail is confined to (`crate::ANNOTATION_CPUSET`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpuset: Option<String>,
}

impl Record {
    /// A pod's jail, which runs nothing; every other record is a member.
    pub fn is_pod(&self) -> bool {
        self.parent.is_none() && !self.standalone
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    pub source: String,
    /// In the container.
    pub destination: String,
    pub read_only: bool,
}

/// A started process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// The supervisor; for an attached run, which has none, its command,
    /// as `pid` is.
    pub shim: u32,
    pub pid: u32,
    pub jid: u32,
    /// Unix seconds.
    pub started: u64,
    /// When the host booted ([`boot_time`]): a run from before a reboot
    /// is over, whatever its files say.
    pub boot: u64,
}

/// How it ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exit {
    pub code: i32,
    /// Unix seconds.
    pub finished: u64,
    /// Why it could not start, if it did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub const RECORD: &str = "record.json";
pub const RUN: &str = "run.json";
pub const EXIT: &str = "exit.json";
pub const LOG: &str = "log";
/// The supervisor's own complaints.
pub const SHIM_LOG: &str = "shim.log";

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub fn read<T: for<'de> Deserialize<'de>>(dir: &Path, name: &str) -> std::io::Result<Option<T>> {
    match std::fs::read(dir.join(name)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Writes `name` in `dir` whole or not at all. Each write has a file of
/// its own to rename: the supervisor and the runtime may both write a
/// container's end.
pub fn write<T: Serialize>(dir: &Path, name: &str, value: &T) -> std::io::Result<()> {
    static WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.{}-{n}.tmp", std::process::id()));
    let mut file = std::fs::OpenOptions::new();
    file.write(true).create_new(true);
    // Whatever the umask: a record holds its container's environment.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut file, 0o600);
    std::io::Write::write_all(
        &mut file.open(&tmp)?,
        &serde_json::to_vec(value).map_err(std::io::Error::other)?,
    )?;
    std::fs::rename(tmp, dir.join(name))
}

/// Makes `dir` its owner's alone, as a umask or an earlier version may have
/// left it open.
pub fn private(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(dir)?.permissions().mode() & 0o077 != 0 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

pub fn remove(dir: &Path, name: &str) -> std::io::Result<()> {
    match std::fs::remove_file(dir.join(name)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// When the host booted, in Unix seconds (0 where unknown).
#[cfg(target_os = "freebsd")]
pub fn boot_time() -> u64 {
    let mut tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut len = std::mem::size_of::<libc::timeval>();
    // SAFETY: kern.boottime is a struct timeval, written into tv.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.boottime".as_ptr(),
            (&raw mut tv).cast(),
            &mut len,
            std::ptr::null(),
            0,
        )
    };
    if rc == 0 { tv.tv_sec as u64 } else { 0 }
}

#[cfg(not(target_os = "freebsd"))]
pub fn boot_time() -> u64 {
    0
}

/// A signal by name (`SIGQUIT`, `QUIT`) or number, as images give their
/// stop signal. Numbers name a signal only from 1 to FreeBSD's last, 128:
/// 0 would only check the process is there.
pub fn signal(s: &str) -> Option<i32> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<i32>() {
        return (1..=128).contains(&n).then_some(n);
    }
    let name = s.strip_prefix("SIG").unwrap_or(s);
    // FreeBSD's numbers (signal(3)).
    Some(match name {
        "HUP" => 1,
        "INT" => 2,
        "QUIT" => 3,
        "KILL" => 9,
        "USR1" => 30,
        "USR2" => 31,
        "TERM" => 15,
        "WINCH" => 28,
        "PWR" => 32,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_signals() {
        assert_eq!(signal("SIGQUIT"), Some(3));
        assert_eq!(signal("TERM"), Some(15));
        assert_eq!(signal("9"), Some(9));
        assert_eq!(signal("0"), None);
        assert_eq!(signal("-1"), None);
        assert_eq!(signal("99999"), None);
        assert_eq!(signal(""), None);
        assert_eq!(signal("SIGNOPE"), None);
    }

    #[cfg(unix)]
    #[test]
    fn records_and_their_directory_are_their_owners_alone() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let t = tempfile::tempdir().unwrap();
        std::fs::set_permissions(t.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        private(t.path()).unwrap();
        assert_eq!(mode(t.path()), 0o700);
        write(t.path(), "r.json", &1).unwrap();
        assert_eq!(mode(&t.path().join("r.json")), 0o600);
    }

    #[test]
    fn writers_of_one_file_do_not_trip_each_other() {
        let t = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            for code in 0..8 {
                let dir = t.path();
                scope.spawn(move || {
                    for _ in 0..50 {
                        let e = Exit {
                            code,
                            finished: 1,
                            error: None,
                        };
                        write(dir, EXIT, &e).unwrap();
                    }
                });
            }
        });
        assert!(read::<Exit>(t.path(), EXIT).unwrap().is_some());
        assert_eq!(std::fs::read_dir(t.path()).unwrap().count(), 1);
    }

    #[test]
    fn files_whole_or_not_at_all() {
        let t = tempfile::tempdir().unwrap();
        assert_eq!(read::<Exit>(t.path(), EXIT).unwrap(), None);
        let e = Exit {
            code: 3,
            finished: 1,
            error: None,
        };
        write(t.path(), EXIT, &e).unwrap();
        assert_eq!(read::<Exit>(t.path(), EXIT).unwrap(), Some(e));
        remove(t.path(), EXIT).unwrap();
        remove(t.path(), EXIT).unwrap();
        assert!(read::<Exit>(t.path(), EXIT).unwrap().is_none());
    }
}
