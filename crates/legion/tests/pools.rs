//! A node's ZFS pools as the cluster sees them (CONSOLE.md, phase 8),
//! through the real daemon and CLI on FreeBSD as root, so ignored by
//! default. A mirror of two files is made a pool; one side taken offline,
//! `pool-degraded` is raised on the node within 30 s, and cleared once it
//! is back; scrubs scheduled on the node, its agent scrubs the pool.
//!
//! ```sh
//! sudo LEGION_POOLS_LAB=/var/tmp/lpools LEGION_POOLS_ZFS_ROOT=lexit/legion \
//!     target/debug/deps/pools-<hash> --ignored --nocapture
//! ```
//!
//! The lab directory is wiped first; remove it afterwards with
//! `chflags -R 0` and `rm -rf`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

// For the pf teardown its other lab tests share.
mod common;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const SERVER: &str = "127.0.0.1:7482";
/// The pool the test makes, and takes apart at its end.
const POOL: &str = "lpools";

struct Daemon(Option<Child>);

impl Daemon {
    /// Interrupts it, as a service stop does; whether it ended well.
    fn stop(&mut self) -> Option<std::process::ExitStatus> {
        let mut child = self.0.take()?;
        let _ = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Some(status);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                return None;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The pool, destroyed however the test ends.
struct Made;

impl Drop for Made {
    fn drop(&mut self) {
        let _ = Command::new("zpool").args(["destroy", "-f", POOL]).status();
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn sh(cmd: &str) -> Output {
    Command::new("/bin/sh").args(["-c", cmd]).output().unwrap()
}

fn sh_ok(cmd: &str) {
    let out = sh(cmd);
    assert!(out.status.success(), "{cmd}: {}", text(&out.stderr));
}

fn legion(lab: &Path, args: &[&str]) -> Output {
    Command::new(LEGION)
        .args(["--server", SERVER])
        .arg("--identity")
        .arg(lab.join("data/admin.pem"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

fn ok(lab: &Path, args: &[&str]) -> String {
    let out = legion(lab, args);
    assert!(
        out.status.success(),
        "legion {args:?}: {}",
        text(&out.stderr)
    );
    text(&out.stdout)
}

/// Waits up to `secs` for `f`; how long it took.
fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) -> Duration {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    start.elapsed()
}

/// The test's pool as the node's status has it.
fn pool(lab: &Path, node: &str) -> Value {
    let out = legion(lab, &["get", "node", node, "-o", "json"]);
    let n: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    n["status"]["pools"]
        .as_array()
        .and_then(|p| p.iter().find(|p| p["name"] == POOL).cloned())
        .unwrap_or(Value::Null)
}

/// The alerts of `kind` on `node` naming the test's pool.
fn alerted(lab: &Path, kind: &str, node: &str) -> Vec<Value> {
    let out = legion(lab, &["alerts", "-o", "json"]);
    let all: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    all.as_array()
        .into_iter()
        .flatten()
        .filter(|a| {
            a["kind"] == kind
                && a["object"] == format!("Node/{node}")
                && a["message"].as_str().is_some_and(|m| m.contains(POOL))
        })
        .cloned()
        .collect()
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD; see the module docs"]
fn a_pool_losing_a_device_is_degraded_on_its_node_within_30_s() {
    let lab = PathBuf::from(env("LEGION_POOLS_LAB"));
    let zfs_root = env("LEGION_POOLS_ZFS_ROOT");
    assert!(
        !sh(&format!("zpool list {POOL}")).status.success(),
        "a pool {POOL} is left from an earlier run"
    );
    if lab.exists() {
        sh(&format!("chflags -R 0 {0} && rm -rf {0}", lab.display()));
    }
    std::fs::create_dir_all(&lab).unwrap();
    let (a, b) = (lab.join("side-a"), lab.join("side-b"));
    sh_ok(&format!(
        "truncate -s 128m {a} {b} && zpool create -m none {POOL} mirror {a} {b}",
        a = a.display(),
        b = b.display()
    ));
    let _made = Made;

    let log = std::fs::File::create(lab.join("daemon.log")).unwrap();
    let child = Command::new(LEGION)
        .args(["daemon", "--listen", SERVER, "--zfs-root", &zfs_root])
        .arg("--data-dir")
        .arg(lab.join("data"))
        .arg("--run-dir")
        .arg(lab.join("run"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let mut daemon = Daemon(Some(child));
    wait("the daemon", 60, || {
        legion(&lab, &["status"]).status.success()
    });
    ok(&lab, &["init"]);
    let nodes: Value = serde_json::from_str(&ok(&lab, &["list", "nodes", "-o", "json"])).unwrap();
    let node = match &nodes {
        Value::Array(v) => v[0]["metadata"]["name"].as_str().unwrap().to_owned(),
        one => one["metadata"]["name"].as_str().unwrap().to_owned(),
    };

    // Read with its host: healthy, nothing raised.
    wait("the pool in the node's status", 60, || {
        pool(&lab, &node)["state"] == "ONLINE"
    });
    let n: Value = serde_json::from_str(&ok(&lab, &["get", "node", &node, "-o", "json"])).unwrap();
    let host = &n["status"]["host"];
    assert!(
        host["release"]
            .as_str()
            .is_some_and(|r| r.contains("RELEASE")
                || r.contains("STABLE")
                || r.contains("CURRENT")),
        "the host's release: {host}"
    );
    assert!(
        host["bootedMs"].as_i64().is_some_and(|t| t > 0),
        "when it booted: {host}"
    );
    assert!(
        ok(&lab, &["node", "pools", &node]).contains(POOL),
        "legion node pools lists it"
    );
    assert_eq!(alerted(&lab, "pool-degraded", &node), Vec::<Value>::new());

    // One side offline: degraded, said on the node, within 30 s.
    sh_ok(&format!("zpool offline {POOL} {}", b.display()));
    let took = wait("pool-degraded", 30, || {
        !alerted(&lab, "pool-degraded", &node).is_empty()
    });
    eprintln!(
        "pool-degraded raised {} ms after the side went offline",
        took.as_millis()
    );
    let raised = &alerted(&lab, "pool-degraded", &node)[0];
    assert_eq!(raised["severity"], "warning", "{raised}");
    assert!(
        raised["message"].as_str().unwrap().contains("DEGRADED"),
        "{raised}"
    );
    assert_eq!(pool(&lab, &node)["state"], "DEGRADED");

    // Back: the alert goes.
    sh_ok(&format!("zpool online {POOL} {}", b.display()));
    wait("pool-degraded to clear", 60, || {
        alerted(&lab, "pool-degraded", &node).is_empty()
    });

    // Scrubs scheduled: the pool, never scrubbed, is scrubbed by the agent.
    ok(&lab, &["node", "scrub", &node, "--every", "7"]);
    wait("a scrub of the pool", 60, || {
        pool(&lab, &node)["scan"]["function"] == "scrub"
    });
    ok(&lab, &["node", "scrub", &node, "--never"]);

    let status = daemon.stop().expect("the daemon stops within a minute");
    assert!(
        status.success(),
        "the daemon ended with {status}; see {}",
        lab.join("daemon.log").display()
    );
    let tidy = format!(
        "for i in legion0 wg0; do ifconfig $i destroy; done\n{}",
        common::PF_TIDY
    );
    let _ = Command::new("sh").args(["-c", &tidy]).output();
}
