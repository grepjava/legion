//! The daemon dies under running pods, on the real kernel, and the next one
//! takes them over: the same jails, processes and addresses, nothing lost
//! from their output, and nothing left behind. The agent's own tests cover
//! the decisions, with fakes; this covers what fakes can't: that a
//! supervisor outlives SIGKILL, and what the jails, mounts, interfaces and
//! pf rules are afterwards.
//!
//! Ignored by default. Safe on a shared host: the daemon runs in a VNET jail
//! of its own, `ladopt`, with its own bridge and pf, and its pods are that
//! jail's children, so nothing reaches the host's interfaces or rules, and
//! nothing is published. It has no route out, so the image is pulled from
//! the host first, into a dataset then handed to the jail.
//!
//! A jail can't do two things a node does. It can't set a devfs ruleset, so
//! it has ruleset 4 as its own, which the kernel puts on every devfs mounted
//! in it, and the daemon finds a `devfs` that leaves that step out. And it
//! can't set rctl limits, so the pods have none: limits on adopted pods are
//! left to the agent's tests and to `exit`.
//!
//! ```sh
//! kldload -n if_bridge if_epair if_wg pf
//! sudo LEGION_ADOPT_LAB=/var/tmp/ladopt LEGION_ADOPT_ZFS_ROOT=pool/ladopt \
//!     target/debug/deps/adopt-<hash> --ignored --nocapture
//! ```
//!
//! The lab, the dataset and the jail are removed at the end.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const JAIL: &str = "ladopt";
const SERVER: &str = "127.0.0.1:7471";
const IMAGE: &str = "docker.io/freebsd/freebsd-runtime:15.1";

/// As /etc/pf.conf must on a node: hooks for Legion's anchors.
const PF_CONF: &str =
    "nat-anchor \"legion/*\"\nrdr-anchor \"legion/*\"\npass all\nanchor \"legion/*\"\n";

/// Found first by the daemon: the kernel refuses these two in a jail, where
/// the jail's own ruleset is applied anyway.
const DEVFS: &str = r#"#!/bin/sh
case "$*" in
*" ruleset "* | *" rule applyset") exit 0 ;;
esac
exec /sbin/devfs "$@"
"#;

/// Counts on stdout, so a gap in its output shows; ready unless `/tmp/down`
/// exists.
fn service(replicas: u32) -> String {
    format!(
        r#"apiVersion: legion/v1
kind: Service
metadata: {{ name: beat, namespace: shop }}
spec:
  replicas: {replicas}
  pod:
    restartPolicy: {{ condition: any, delay: 0s }}
    containers:
      - name: beat
        image: {IMAGE}
        command: [/bin/sh, -c, 'n=0; while :; do n=$((n+1)); echo "beat $n"; sleep 1; done']
        probes:
          readiness:
            exec: {{ command: [/bin/sh, -c, 'test ! -e /tmp/down'] }}
            period: 1s
            failureThreshold: 1
"#
    )
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn sh(cmd: &str) -> Output {
    Command::new("/bin/sh").args(["-c", cmd]).output().unwrap()
}

fn ok(cmd: &str) -> String {
    let out = sh(cmd);
    assert!(out.status.success(), "{cmd}: {}", text(&out.stderr));
    text(&out.stdout)
}

/// In the lab's jail.
fn inside(cmd: &str) -> String {
    ok(&format!("jexec {JAIL} /bin/sh -c '{cmd}'"))
}

fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// What must stay the same for a pod to have been adopted, not replaced.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Pod {
    task: String,
    ip: String,
    jid: u64,
    container: String,
    /// The container's first process.
    pid: String,
}

struct Lab {
    dir: PathBuf,
    zfs_root: String,
    daemon: Option<Child>,
    /// Where the running daemon's part of the log starts.
    log_from: usize,
}

impl Lab {
    fn new(dir: PathBuf, zfs_root: String) -> Lab {
        assert!(
            !sh(&format!("jls -j {JAIL} jid")).status.success(),
            "the jail {JAIL} is there already: remove what an earlier run left"
        );
        // From here on, dropping it removes what it finds.
        let lab = Lab {
            dir,
            zfs_root,
            daemon: None,
            log_from: 0,
        };
        std::fs::create_dir_all(lab.dir.join("data")).unwrap();
        ok(&format!(
            "zfs create -p -o mountpoint=none {}",
            lab.zfs_root
        ));
        lab.pull();
        ok(&format!("zfs set jailed=on {}", lab.zfs_root));
        ok(&format!(
            "jail -c name={JAIL} path=/ vnet persist children.max=64 enforce_statfs=1              devfs_ruleset=4 \
             allow.raw_sockets allow.chflags allow.mount allow.mount.devfs \
             allow.mount.nullfs allow.mount.tmpfs allow.mount.zfs"
        ));
        ok(&format!("zfs jail {JAIL} {}", lab.zfs_root));
        inside("ifconfig lo0 inet 127.0.0.1/8 up");
        let bin = lab.dir.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("devfs"), DEVFS).unwrap();
        ok(&format!("chmod +x {}/devfs", bin.display()));
        let conf = lab.dir.join("pf.conf");
        std::fs::write(&conf, PF_CONF).unwrap();
        inside(&format!("pfctl -q -e; pfctl -q -f {}", conf.display()));
        lab
    }

    /// Into the store the daemon opens, while the host still has the network
    /// and the dataset.
    fn pull(&self) {
        let sys = Arc::new(legion_sys::Shell::new());
        let store = legion_image::Store::open(
            sys,
            &format!("{}/images", self.zfs_root),
            &self.dir.join("data/images"),
            legion_image::Client::new().unwrap(),
        )
        .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.pull(IMAGE, None, "shop"))
            .unwrap();
    }

    fn start(&mut self) {
        self.log_from = self.log().len();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("daemon.log"))
            .unwrap();
        let path = format!(
            "{}/bin:{}",
            self.dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let child = Command::new("jexec")
            .env("PATH", path)
            .arg(JAIL)
            .arg(LEGION)
            .args(["daemon", "--listen", SERVER, "--node-name", "adopt"])
            .args(["--advertise", SERVER, "--zfs-root", &self.zfs_root])
            .arg("--data-dir")
            .arg(self.dir.join("data"))
            .arg("--run-dir")
            .arg(self.dir.join("run"))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        wait("the daemon", 60, || {
            self.legion(&["status"]).status.success()
        });
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("daemon.log")).unwrap_or_default()
    }

    /// Lines of the running daemon's log with `what` in them. A task's
    /// status can't say what this daemon has done: until its agent reports,
    /// the store holds what the one before reported.
    fn logged(&self, what: &str) -> usize {
        self.log()[self.log_from..]
            .lines()
            .filter(|l| l.contains(what))
            .count()
    }

    /// This daemon has taken over `n` pods, and they are ready.
    fn adopted(&self, n: usize) -> bool {
        self.logged("adopted running pod") == n && self.ready(n as u64)
    }

    /// The jail a task's status gives.
    fn jid(&self, task: &str) -> Option<u64> {
        self.tasks()
            .iter()
            .find(|t| t["metadata"]["name"] == task)?["status"]["jid"]
            .as_u64()
    }

    /// As a crash would: no chance to clean up.
    fn kill(&mut self) {
        let mut child = self.daemon.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// As the service would on a restart.
    fn stop(&mut self) {
        let mut child = self.daemon.take().unwrap();
        ok(&format!("kill -TERM {}", child.id()));
        let deadline = Instant::now() + Duration::from_secs(60);
        while child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the daemon did not stop");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn legion(&self, args: &[&str]) -> Output {
        Command::new("jexec")
            .arg(JAIL)
            .arg(LEGION)
            .args(["--server", SERVER, "--identity"])
            .arg(self.dir.join("data/admin.pem"))
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn cli(&self, args: &[&str]) -> String {
        let out = self.legion(args);
        assert!(
            out.status.success(),
            "legion {args:?}: {}",
            text(&out.stderr)
        );
        text(&out.stdout)
    }

    fn apply(&self, replicas: u32) {
        let file = self.dir.join("beat.yaml");
        std::fs::write(&file, service(replicas)).unwrap();
        self.cli(&["apply", "-f", file.to_str().unwrap()]);
    }

    fn list(&self, kind: &str) -> Vec<Value> {
        let out = self.legion(&["list", kind, "-n", "shop", "-o", "json"]);
        if !out.status.success() {
            return Vec::new();
        }
        match serde_json::from_slice(&out.stdout) {
            Ok(Value::Array(v)) => v,
            Ok(one @ Value::Object(_)) => vec![one],
            _ => Vec::new(),
        }
    }

    /// All `n` tasks of the service ready, as the agent now reports them.
    fn ready(&self, n: u64) -> bool {
        let services = self.list("services");
        services.len() == 1
            && services[0]["status"]["desired"] == n
            && services[0]["status"]["ready"] == n
            && services[0]["status"]["observedGeneration"] == services[0]["metadata"]["generation"]
    }

    /// The containers' records: name to (id, jail).
    fn records(&self) -> BTreeMap<String, (String, String)> {
        let Ok(dirs) = std::fs::read_dir(self.dir.join("data/jails")) else {
            return BTreeMap::new();
        };
        dirs.filter_map(|d| {
            let path = d.ok()?.path().join("record.json");
            let r: Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
            let s = |k: &str| r[k].as_str().unwrap_or_default().to_owned();
            Some((s("name"), (s("id"), s("jail"))))
        })
        .collect()
    }

    fn tasks(&self) -> Vec<Value> {
        self.list("tasks")
            .into_iter()
            .filter(|t| t["spec"]["desiredState"] == "running")
            .collect()
    }

    fn restarts(&self, task: &str) -> Option<u64> {
        self.tasks()
            .iter()
            .find(|t| t["metadata"]["name"] == task)?["status"]["restarts"]
            .as_u64()
    }

    fn member_jid(&self, task: &str) -> String {
        let (_, jail) = &self.records()[&format!("shop_{task}_beat")];
        inside(&format!("jls -j {jail} jid")).trim().to_owned()
    }

    fn pods(&self) -> Vec<Pod> {
        let records = self.records();
        let mut pods: Vec<Pod> = self
            .tasks()
            .iter()
            .map(|t| {
                let task = t["metadata"]["name"].as_str().unwrap().to_owned();
                let (container, _) = records[&format!("shop_{task}_beat")].clone();
                let jid = self.member_jid(&task);
                let pid = ok(&format!("pgrep -o -j {jid} sh")).trim().to_owned();
                Pod {
                    ip: t["status"]["ip"].as_str().unwrap().to_owned(),
                    jid: t["status"]["jid"].as_u64().unwrap(),
                    task,
                    container,
                    pid,
                }
            })
            .collect();
        pods.sort();
        pods
    }

    /// The numbers a task's container has printed, in order.
    fn beats(&self, task: &str) -> Vec<u64> {
        self.cli(&["logs", task, "-n", "shop"])
            .lines()
            .filter_map(|l| l.rsplit_once("beat ")?.1.trim().parse().ok())
            .collect()
    }

    /// The pf rules of a pod's network, as the agent loaded them.
    fn rules(&self) -> String {
        inside(
            "pfctl -a legion/svc -sn 2>/dev/null; pfctl -a legion/svc -sr 2>/dev/null; \
             pfctl -a legion/policy -sr 2>/dev/null",
        )
    }

    fn reachable(&self, ip: &str) -> bool {
        sh(&format!("jexec {JAIL} ping -c 1 -t 2 {ip}"))
            .status
            .success()
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let log = self.log();
            let tail: Vec<&str> = log.lines().rev().take(40).collect();
            let tail: Vec<&str> = tail.into_iter().rev().collect();
            eprintln!("--- daemon.log\n{}", tail.join("\n"));
        }
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // With the jail go its children, and the interfaces they made.
        let _ = sh(&format!("jail -r {JAIL}"));
        let dir = self.dir.display();
        // Last mounted first: one on top keeps what it stacks on busy.
        let _ = sh(&format!(
            "mount -p | awk '$2 ~ \"^{dir}/\" {{ print $2 }}' | tail -r | xargs -n 1 umount -f"
        ));
        let _ = sh(&format!("zfs destroy -rf {}", self.zfs_root));
        let _ = sh(&format!("chflags -R 0 {dir}; rm -rf {dir}"));
    }
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD; set LEGION_ADOPT_LAB and LEGION_ADOPT_ZFS_ROOT"]
fn pods_outlive_the_daemon() {
    let (Some(dir), Some(zfs_root)) = (
        std::env::var_os("LEGION_ADOPT_LAB"),
        std::env::var("LEGION_ADOPT_ZFS_ROOT").ok(),
    ) else {
        legion_sys::live::skip("LEGION_ADOPT_LAB or LEGION_ADOPT_ZFS_ROOT not set");
        return;
    };
    let mut lab = Lab::new(dir.into(), zfs_root);
    lab.start();
    lab.cli(&["init"]);
    lab.apply(2);
    wait("the pods", 180, || lab.ready(2));
    let pods = lab.pods();
    assert_eq!(pods.len(), 2);
    assert_ne!(pods[0].ip, pods[1].ip);
    wait("the first output", 30, || {
        !lab.beats(&pods[0].task).is_empty()
    });
    let rules = lab.rules();

    eprintln!("adopt: killed under running pods");
    // Killed: the pods go on without it, and their output is kept.
    let before = lab.beats(&pods[0].task).last().copied().unwrap();
    lab.kill();
    assert!(
        lab.reachable(&pods[0].ip),
        "a pod's network outlives the daemon"
    );
    std::thread::sleep(Duration::from_secs(6));
    assert_eq!(lab.rules(), rules, "pf rules outlive the daemon");
    lab.start();
    wait("the pods adopted", 120, || lab.adopted(2));
    assert_eq!(lab.pods(), pods, "the same jails, processes and addresses");
    assert_eq!(lab.restarts(&pods[0].task), Some(0));
    let beats = lab.beats(&pods[0].task);
    let expected: Vec<u64> = (1..=beats.len() as u64).collect();
    assert_eq!(beats, expected, "output with no gap");
    assert!(
        *beats.last().unwrap() >= before + 5,
        "counted on while the daemon was down: {before}, then {beats:?}"
    );
    for pod in &pods {
        assert!(lab.reachable(&pod.ip), "{} after adoption", pod.task);
    }
    wait("the same pf rules", 30, || lab.rules() == rules);

    eprintln!("adopt: exec and probes after adoption");
    // Adopted pods are watched and reached as if never left: exec goes
    // through the supervisor, and probes run again.
    let task = &pods[0].task;
    let exec = |cmd: &str| lab.cli(&["exec", task, "-n", "shop", "--", "/bin/sh", "-c", cmd]);
    exec(": > /tmp/down");
    wait("the probe to fail", 30, || !lab.ready(2));
    exec("rm /tmp/down");
    wait("the probe to pass", 30, || lab.ready(2));
    assert_eq!(
        lab.pods(),
        pods,
        "a failing readiness probe restarts nothing"
    );

    eprintln!("adopt: stopped as for an upgrade");
    // Stopped as for an upgrade, it is no different.
    lab.stop();
    lab.start();
    wait("the pods adopted after a stop", 120, || lab.adopted(2));
    assert_eq!(lab.pods(), pods);

    eprintln!("adopt: a container dies while it is down");
    // A container dies with no daemon to see it: it is restarted in its pod,
    // the other pod untouched.
    lab.kill();
    ok(&format!("pkill -KILL -j {}", lab.member_jid(&pods[0].task)));
    lab.start();
    wait("the missed exit restarted", 120, || {
        lab.adopted(2) && lab.restarts(&pods[0].task) == Some(1)
    });
    let now = lab.pods();
    assert_eq!(now[1], pods[1], "the other pod untouched");
    assert_eq!(
        (&now[0].task, &now[0].ip, now[0].jid),
        (&pods[0].task, &pods[0].ip, pods[0].jid),
        "restarted in its pod"
    );
    assert_ne!(now[0].pid, pods[0].pid, "a new process");
    let pods = now;

    eprintln!("adopt: a pod goes while it is down");
    // A whole pod goes with no daemon to see it: replaced, and what it left
    // removed.
    lab.kill();
    let records = lab.records();
    let name = format!("shop_{}_beat", pods[1].task);
    inside(&format!("jail -r {}", records[&name].1));
    let infra = format!("shop_{}", pods[1].task);
    inside(&format!("jail -r {}", records[&infra].1));
    lab.start();
    wait("the lost pod replaced", 120, || {
        lab.logged("removing an incomplete pod") == 1
            && lab.jid(&pods[1].task).is_some_and(|jid| jid != pods[1].jid)
            && lab.ready(2)
            && lab.records().len() == 4
    });
    assert_eq!(lab.logged("adopted running pod"), 1, "the other pod");
    let now = lab.pods();
    assert!(now.contains(&pods[0]), "the other pod untouched: {now:?}");
    assert!(!now.contains(&pods[1]), "a new pod: {now:?}");
    let mut pods = now;

    // Killed while pods start, at different points: the ones running stay,
    // and each that was on its way ends up one whole pod.
    for delay in [0, 300, 1000] {
        eprintln!("adopt: killed {delay} ms into a start");
        let known = lab.records().len();
        lab.apply(4);
        wait("a pod to start", 60, || lab.records().len() > known);
        std::thread::sleep(Duration::from_millis(delay));
        lab.kill();
        lab.start();
        wait("the pods after a start cut short", 180, || lab.ready(4));
        let now = lab.pods();
        assert_eq!(now.len(), 4);
        for pod in &pods {
            assert!(now.contains(pod), "{pod:?} went: {now:?}");
        }
        wait("only what the pods use", 60, || lab.records().len() == 8);
        let jails = inside("jls name");
        assert_eq!(jails.lines().count(), 8, "{jails}");
        lab.apply(2);
        wait("two pods again", 120, || {
            lab.ready(2) && lab.records().len() == 4
        });
        pods = lab.pods();
    }

    eprintln!("adopt: deleted");
    // Deleted after all that, nothing of them stays.
    lab.cli(&["delete", "service", "beat", "-n", "shop"]);
    wait("the pods removed", 120, || {
        lab.records().is_empty() && lab.tasks().is_empty()
    });
    assert_eq!(inside("jls name").trim(), "", "no jails left");
    let interfaces = inside("ifconfig -l");
    assert!(!interfaces.contains("epair"), "{interfaces}");
    let mounts = ok(&format!(
        "mount -p | awk '$2 ~ \"^{}/data/jails\"'",
        lab.dir.display()
    ));
    assert_eq!(mounts, "", "no mounts left");
    let clones = ok(&format!("zfs list -H -o name -r {}/jails", lab.zfs_root));
    assert_eq!(clones.lines().count(), 1, "no clones left: {clones}");
    lab.stop();
    eprintln!("adopt: all done");
}
