//! Self-fencing (docs/PARTITIONS.md): a pod that must not run twice
//! (`onPartition: stop`) stops on its node once the managers have not
//! answered the node for as long as they take to replace it, even when the
//! node's daemon is gone.
//!
//! The agent renews a lease each time the managers answer a heartbeat: the
//! moment, on the system's monotonic clock, past which they will have
//! marked the node down. Each such container's supervisor
//! (`legion jail-shim`), which outlives the daemon, reads it every second
//! and stops the container once that moment has passed. The monotonic
//! clock is the same for every process and does not move when the time of
//! day is set.

use std::path::Path;

/// The lease a container's supervisor follows: its file's path. Set on the
/// containers of pods with `onPartition: stop`.
pub const ANNOTATION_FENCE: &str = "legion.fence";

/// Milliseconds on the system's monotonic clock (since boot on FreeBSD),
/// the same in every process.
#[cfg(unix)]
pub fn monotonic_ms() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: t is a valid timespec to write to.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) };
    (t.tv_sec as u64) * 1000 + (t.tv_nsec as u64) / 1_000_000
}

/// Elsewhere (a development build), this process's own clock: a lease is
/// then good only within one process, which is all tests there need.
#[cfg(not(unix))]
pub fn monotonic_ms() -> u64 {
    use std::sync::OnceLock;
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

/// Renews the lease at `path`: answered until `until_ms` on the monotonic
/// clock. Written aside and renamed, so a reader never sees half of it.
pub fn renew(path: &Path, until_ms: u64) -> std::io::Result<()> {
    let aside = path.with_extension("new");
    std::fs::write(&aside, format!("{until_ms}\n"))?;
    std::fs::rename(&aside, path)
}

/// Until when the lease at `path` holds (monotonic milliseconds), if it can
/// be read.
pub fn until(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Whether the lease at `path` ran out before `now_ms`. Not when there is
/// none to read: a container is not stopped for want of a file, only for a
/// lease the managers stopped renewing.
pub fn expired(path: &Path, now_ms: u64) -> bool {
    until(path).is_some_and(|until| now_ms > until)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_holds_until_its_moment_and_none_never_runs_out() {
        let dir = tempfile::tempdir().unwrap();
        let lease = dir.path().join("lease");
        assert!(!expired(&lease, u64::MAX), "no lease, nothing stops");
        renew(&lease, 1_000).unwrap();
        assert_eq!(until(&lease), Some(1_000));
        assert!(!expired(&lease, 1_000));
        assert!(expired(&lease, 1_001));
        // Renewed, it holds again; nothing is left aside.
        renew(&lease, 5_000).unwrap();
        assert!(!expired(&lease, 1_001));
        assert!(!lease.with_extension("new").exists());
        // Unreadable is as none.
        std::fs::write(&lease, "garbage").unwrap();
        assert!(!expired(&lease, u64::MAX));
    }

    #[test]
    fn the_monotonic_clock_moves_on() {
        let a = monotonic_ms();
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(monotonic_ms() >= a + 15);
    }
}
