//! A pod on network storage (DESIGN.md §6.7), through the real daemon and
//! CLI on FreeBSD as root, so ignored by default. A service writes a line
//! to an NFS volume and to an iSCSI volume each time it starts; scaled to
//! nothing, the node lets go of the LUN; scaled up again, both lines are
//! still there.
//!
//! It needs an NFSv3 export on this host and an iSCSI target with UFS on
//! LUN 0 (iscsid running), which it only mounts:
//!
//! ```sh
//! sudo LEGION_STORAGE_LAB=/var/tmp/lstorage LEGION_STORAGE_ZFS_ROOT=lexit/legion \
//!     LEGION_STORAGE_NFS=127.0.0.1:/var/tmp/lvol/export \
//!     LEGION_STORAGE_ISCSI="127.0.0.1 iqn.2026-10.lan.legion:lvol" \
//!     target/debug/deps/storage-<hash> --ignored --nocapture
//! ```
//!
//! The lab directory is wiped first; remove it afterwards with
//! `chflags -R 0` and `rm -rf`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const SERVER: &str = "127.0.0.1:7481";
const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";

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

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn sh(cmd: &str) -> String {
    text(
        &Command::new("/bin/sh")
            .args(["-c", cmd])
            .output()
            .unwrap()
            .stdout,
    )
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

fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn get(lab: &Path, kind: &str, name: &str) -> Value {
    let out = legion(lab, &["get", kind, name, "-n", "shop", "-o", "json"]);
    serde_json::from_slice(&out.stdout).unwrap_or(Value::Null)
}

fn running(lab: &Path) -> usize {
    let out = legion(lab, &["list", "tasks", "-n", "shop", "-o", "json"]);
    let tasks: Value = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    let all = match tasks {
        Value::Array(v) => v,
        one @ Value::Object(_) => vec![one],
        _ => Vec::new(),
    };
    all.iter()
        .filter(|t| t["status"]["state"] == "running" && t["spec"]["desiredState"] == "running")
        .count()
}

/// The lines of this run (the LUN keeps earlier runs' too).
fn lines(path: &Path, run: &str) -> usize {
    std::fs::read_to_string(path).map_or(0, |s| s.lines().filter(|l| *l == run).count())
}

fn app(nfs: &str, portal: &str, target: &str, replicas: u32, run: &str) -> String {
    let (server, path) = nfs.split_once(':').expect("server:/path");
    format!(
        r#"apiVersion: legion/v1
kind: Volume
metadata: {{ name: files, namespace: shop }}
spec: {{ driver: nfs, nfs: {{ server: "{server}", path: "{path}", options: [nfsv3] }} }}
---
apiVersion: legion/v1
kind: Volume
metadata: {{ name: lun, namespace: shop }}
spec: {{ driver: iscsi, iscsi: {{ portal: "{portal}", target: "{target}" }} }}
---
apiVersion: legion/v1
kind: Service
metadata: {{ name: writer, namespace: shop }}
spec:
  replicas: {replicas}
  pod:
    containers:
      - name: writer
        image: {IMAGE}
        command: [/bin/sh, -c, "echo {run} >> /files/legion-storage; echo {run} >> /lun/legion-storage; exec sleep 100000"]
        volumeMounts:
          - {{ name: files, path: /files }}
          - {{ name: lun, path: /lun }}
    volumes:
      - {{ name: files, volume: {{ name: files }} }}
      - {{ name: lun, volume: {{ name: lun }} }}
"#
    )
}

#[test]
#[ignore = "needs root, ZFS, an NFS export and an iSCSI target on FreeBSD; see the module docs"]
fn pod_on_network_storage() {
    let lab = PathBuf::from(env("LEGION_STORAGE_LAB"));
    let zfs_root = env("LEGION_STORAGE_ZFS_ROOT");
    let nfs = env("LEGION_STORAGE_NFS");
    let iscsi = env("LEGION_STORAGE_ISCSI");
    let (portal, target) = iscsi.split_once(' ').expect("portal target");
    assert!(
        !sh("jls path")
            .lines()
            .any(|p| p.starts_with(&*lab.to_string_lossy())),
        "jails from an earlier run still run under {}",
        lab.display()
    );
    if lab.exists() {
        sh(&format!("chflags -R 0 {0} && rm -rf {0}", lab.display()));
    }
    std::fs::create_dir_all(&lab).unwrap();
    // The export is on this host: its lines are read where it is.
    let exported = Path::new(nfs.split_once(':').unwrap().1).join("legion-storage");
    let _ = std::fs::remove_file(&exported);
    let lun_mount = lab.join("run/volumes/iscsi/shop/lun");
    let on_lun = lun_mount.join("legion-storage");
    let run = format!("started-{}", std::process::id());

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
    let apply = |replicas: u32| {
        let file = lab.join("app.yaml");
        std::fs::write(&file, app(&nfs, portal, target, replicas, &run)).unwrap();
        ok(&lab, &["apply", "-f", file.to_str().unwrap()]);
    };
    let held_by = || {
        get(&lab, "volume", "lun")["status"]["node"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    let logged_in = || sh("iscsictl -L").contains(target);

    // The first run pulls the image.
    apply(1);
    wait("the writer to run", 300, || running(&lab) == 1);
    wait("its first lines", 30, || {
        lines(&exported, &run) == 1 && lines(&on_lun, &run) == 1
    });
    assert!(!held_by().is_empty(), "the LUN is the node's while used");
    assert_eq!(
        get(&lab, "volume", "files")["status"]["node"],
        Value::Null,
        "an NFS volume is no node's"
    );
    assert!(logged_in());

    // Scaled to nothing: the node unmounts, logs out and lets go.
    apply(0);
    wait("the node to let go of the LUN", 120, || {
        running(&lab) == 0 && held_by().is_empty() && !logged_in()
    });
    assert!(!sh("mount -p").contains(&*lun_mount.to_string_lossy()));

    // Back: a new pod finds what the first wrote, on both.
    apply(1);
    wait("the writer to run again", 120, || running(&lab) == 1);
    wait("the second lines", 30, || {
        lines(&exported, &run) == 2 && lines(&on_lun, &run) == 2
    });

    ok(&lab, &["delete", "service", "writer", "-n", "shop"]);
    wait("the writer to go", 120, || {
        running(&lab) == 0 && held_by().is_empty() && !logged_in()
    });
    let _ = std::fs::remove_file(&exported);
    // A clean stop: the store closes in time.
    let status = daemon.stop().expect("the daemon stops within a minute");
    assert!(
        status.success(),
        "the daemon ended with {status}; see {}",
        lab.join("daemon.log").display()
    );
}
