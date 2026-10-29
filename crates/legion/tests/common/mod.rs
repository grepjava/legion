//! The lab the multi-node exit tests share: this host's daemon and two
//! machines reached over ssh, set from the environment.
//!
//! - `LEGION_CLUSTER_LAB`: this host's lab directory, wiped first.
//! - `LEGION_CLUSTER_HOST`: the address the others reach this host at.
//! - `LEGION_CLUSTER_VMS`: the two machines, as `name=[user@]addr`, logging
//!   in as root unless a user is given, who must have sudo.
//! - `LEGION_CLUSTER_KEY`: the ssh key to log in with.
//!
//! [`Lab::local`] is this host alone, reached on loopback, from
//! `LEGION_CLUSTER_LAB` only.
//!
//! Every node gets the throwaway pool [`POOL`] for images, containers
//! and volumes. The test's own `legion` is copied to the machines. Remove the lab
//! directory afterwards with `chflags -R 0` and `rm -rf`.

#![allow(dead_code)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

pub const LEGION: &str = env!("CARGO_BIN_EXE_legion");
pub const PORT: u16 = 7470;
/// Where the test's `legion` goes on the other nodes.
pub const REMOTE: &str = "/usr/local/bin/legion-lab";

pub struct Vm {
    pub name: String,
    /// Who logs in, root if empty; another user runs commands through sudo.
    pub user: String,
    pub addr: String,
}

impl Vm {
    fn login(&self) -> String {
        let user = if self.user.is_empty() {
            "root"
        } else {
            &self.user
        };
        format!("{user}@{}", self.addr)
    }
}

/// How a machine joins.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Manager,
    Worker,
}

/// A throwaway pool, `llab`, backed by a file in `/var/tmp`.
pub const POOL: &str = "kldload -n zfs
    zpool destroy -f llab 2>/dev/null || true
    rm -f /var/tmp/llab.img
    truncate -s 2g /var/tmp/llab.img
    zpool create -m /var/tmp/llab llab /var/tmp/llab.img
";
pub const NO_POOL: &str = "zpool destroy -f llab
    rm -f /var/tmp/llab.img
";
/// The daemons' dataset in [`POOL`].
pub const ZFS_ROOT: &str = "llab/legion";

pub struct Lab {
    pub dir: PathBuf,
    pub host: String,
    pub vms: Vec<Vm>,
    pub key: String,
    pub daemon: Option<Child>,
    /// Run as root on each VM before its daemon starts, after what an
    /// earlier run left is removed.
    pub setup: String,
    /// More flags for each VM's daemon.
    pub flags: String,
    /// Run as root on every node once the lab's daemons have stopped and
    /// their pods are gone.
    pub teardown: String,
}

impl Lab {
    /// The lab the environment describes, with what an earlier run left in
    /// this host's lab directory removed.
    pub fn from_env() -> Lab {
        let var = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("set {k}"));
        let dir = PathBuf::from(var("LEGION_CLUSTER_LAB"));
        let vms: Vec<Vm> = var("LEGION_CLUSTER_VMS")
            .split_whitespace()
            .map(|v| {
                let (name, login) = v.split_once('=').expect("name=[user@]addr");
                let (user, addr) = match login.split_once('@') {
                    Some((u, a)) => (u.into(), a),
                    None => (String::new(), login),
                };
                Vm {
                    name: name.into(),
                    user,
                    addr: addr.into(),
                }
            })
            .collect();
        assert_eq!(vms.len(), 2, "two VMs");
        Lab::new(
            dir,
            var("LEGION_CLUSTER_HOST"),
            vms,
            var("LEGION_CLUSTER_KEY"),
        )
    }

    /// This host alone, as the manager `n1` on loopback.
    pub fn local() -> Lab {
        let dir = std::env::var("LEGION_CLUSTER_LAB").expect("set LEGION_CLUSTER_LAB");
        Lab::new(dir.into(), "127.0.0.1".into(), Vec::new(), String::new())
    }

    fn new(dir: PathBuf, host: String, vms: Vec<Vm>, key: String) -> Lab {
        if dir.exists() {
            let d = dir.display();
            let script = clean(&format!("{d}/data"), &format!("{d}/run"));
            Command::new("/bin/sh")
                .args(["-c", &format!("{script}chflags -R 0 {d}; rm -rf {d}")])
                .status()
                .unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        // Every node gets the pool, its daemon `--zfs-root` in it, and the
        // pool goes when the lab does.
        Lab {
            dir,
            host,
            vms,
            key,
            daemon: None,
            setup: POOL.to_owned(),
            flags: format!("--zfs-root {ZFS_ROOT}"),
            teardown: NO_POOL.to_owned(),
        }
    }

    pub fn server(&self) -> String {
        format!("{}:{PORT}", self.host)
    }

    pub fn admin(&self) -> PathBuf {
        self.dir.join("data/admin.pem")
    }

    /// `legion` as the cluster admin.
    pub fn legion(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        self.legion_as(&self.admin(), args, env)
    }

    /// `legion` with the credentials in `identity`.
    pub fn legion_as(&self, identity: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
        Command::new(LEGION)
            .args(["--server", &self.server()])
            .arg("--identity")
            .arg(identity)
            .args(args)
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    pub fn ok(&self, args: &[&str]) -> String {
        self.ok_as(&self.admin(), args)
    }

    pub fn ok_as(&self, identity: &Path, args: &[&str]) -> String {
        let out = self.legion_as(identity, args, &[]);
        assert!(
            out.status.success(),
            "legion {args:?}: {}",
            text(&out.stderr)
        );
        text(&out.stdout)
    }

    /// `ssh` or `scp` with the lab's options.
    fn remote(&self, program: &str) -> Command {
        let mut c = Command::new(program);
        c.args(["-i", &self.key])
            .args(["-o", "StrictHostKeyChecking=no"])
            .args(["-o", "UserKnownHostsFile=/dev/null"])
            .args(["-o", "LogLevel=ERROR"])
            .args(["-o", "ConnectTimeout=5"])
            .args(["-o", "ServerAliveInterval=5"]);
        c
    }

    /// Runs the script `cmd` as root on a VM.
    pub fn ssh(&self, vm: &Vm, cmd: &str) -> Output {
        let shell = if vm.user.is_empty() {
            "sh -s"
        } else {
            "sudo sh -s"
        };
        let mut child = self
            .remote("ssh")
            .arg(vm.login())
            .arg(shell)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(cmd.as_bytes()).unwrap();
        drop(stdin);
        child.wait_with_output().unwrap()
    }

    /// Copies a file to a VM, as the user that logs in.
    pub fn copy(&self, vm: &Vm, from: &str, to: &str) {
        let status = self
            .remote("scp")
            .arg("-q")
            .arg(from)
            .arg(format!("{}:{to}", vm.login()))
            .status()
            .unwrap();
        assert!(status.success(), "copying {from} to {}", vm.name);
    }

    /// Copies the test's `legion` to a VM, stripped: it is far smaller.
    fn install(&self, vm: &Vm) {
        let stripped = self.dir.join("legion");
        if !stripped.exists() {
            let status = Command::new("strip")
                .arg("-o")
                .arg(&stripped)
                .arg(LEGION)
                .status()
                .unwrap();
            assert!(status.success(), "stripping legion");
        }
        self.copy(vm, stripped.to_str().unwrap(), "/tmp/legion-lab");
        let out = self.ssh(
            vm,
            &format!("install -m 755 /tmp/legion-lab {REMOTE} && rm /tmp/legion-lab"),
        );
        assert!(out.status.success(), "{}", text(&out.stderr));
    }

    /// Starts this host's daemon as the manager `n1`, with `extra` flags.
    pub fn start_host(&mut self, extra: &[&str]) {
        if !self.setup.is_empty() {
            let status = Command::new("/bin/sh")
                .args(["-c", &self.setup])
                .status()
                .unwrap();
            assert!(status.success(), "setting this host up");
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("daemon.log"))
            .unwrap();
        let child = Command::new(LEGION)
            .args(["daemon", "--node-name", "n1"])
            .args(["--listen", &self.server(), "--advertise", &self.server()])
            .arg("--data-dir")
            .arg(self.dir.join("data"))
            .arg("--run-dir")
            .arg(self.dir.join("run"))
            .args(self.flags.split_whitespace())
            .args(extra)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        wait("the host's daemon", 60, || {
            self.legion(&["status"], &[]).status.success()
        });
    }

    /// Joins VM `i` as `n<i+2>` with `token` and starts its daemon, after
    /// removing what an earlier run left there. A manager listens on the
    /// API port; a worker only advertises where WireGuard reaches it.
    pub fn join(&self, i: usize, token: &str, role: Role) {
        let vm = &self.vms[i];
        self.install(vm);
        let name = format!("n{}", i + 2);
        let addr = format!("{}:{PORT}", vm.addr);
        let (join, daemon) = match role {
            Role::Manager => (
                format!("--advertise {addr}"),
                format!("--listen {addr} --advertise {addr}"),
            ),
            Role::Worker => (String::new(), format!("--advertise {}", vm.addr)),
        };
        let cmd = format!(
            "{STOP}
             {clean}
             set -e
             {setup}
             {REMOTE} join {token} --manager {server} --data-dir /var/db/legion-lab \
                 {join} --node-name {name}
             daemon -o /var/log/legion-lab.log {REMOTE} daemon --node-name {name} \
                 --data-dir /var/db/legion-lab --run-dir /var/run/legion-lab \
                 {daemon} {flags} </dev/null >/dev/null 2>&1
",
            clean = clean(VM_DATA, VM_RUN),
            server = self.server(),
            setup = self.setup,
            flags = self.flags,
        );
        let out = self.ssh(vm, &cmd);
        assert!(
            out.status.success(),
            "joining {}: {}{}",
            vm.name,
            text(&out.stdout),
            text(&out.stderr)
        );
    }

    /// Objects of a kind in a namespace, as the admin sees them, as JSON.
    pub fn list(&self, kind: &str, namespace: &str) -> Vec<Value> {
        let out = self.legion(&["list", kind, "-n", namespace, "-o", "json"], &[]);
        if !out.status.success() {
            return Vec::new();
        }
        match serde_json::from_slice(&out.stdout) {
            Ok(Value::Array(v)) => v,
            Ok(one @ Value::Object(_)) => vec![one],
            _ => Vec::new(),
        }
    }

    pub fn node_state(&self, name: &str) -> String {
        self.list("nodes", "default")
            .iter()
            .find(|n| n["metadata"]["name"] == name)
            .and_then(|n| n["status"]["state"].as_str().map(str::to_owned))
            .unwrap_or_default()
    }

    /// The nodes running a service's tasks in a namespace.
    pub fn placed(&self, namespace: &str, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .list("tasks", namespace)
            .into_iter()
            .filter(|t| {
                t["spec"]["service"] == service
                    && t["status"]["state"] == "running"
                    && t["spec"]["desiredState"] == "running"
            })
            .map(|t| t["spec"]["node"].as_str().unwrap().to_owned())
            .collect();
        v.sort();
        v
    }
}

/// A script removing what a stopped daemon with these directories left:
/// its pods (by their records' ids, as pod jails are on /var/empty),
/// interfaces, load balancing and state.
pub fn clean(data: &str, run: &str) -> String {
    format!(
        "for r in {data}/jails/*/record.json; do
             [ -f \"$r\" ] || continue
             id=$(basename $(dirname \"$r\"))
             jls -j $id jid >/dev/null 2>&1 && jail -r $id
             # rctl keeps the rules of jails that are gone.
             rctl | cut -d: -f2 | grep \"^$id\\.\" | sort -u |
                 while read -r j; do rctl -r \"jail:$j\"; done
         done
         i=0; while jls -d -h path | grep -q '^{data}/' && [ $i -lt 90 ]; do
             sleep 1; i=$((i + 1))
         done
         for i in $(ifconfig -l); do
             case $i in lp*|legion0|wg0) ifconfig $i destroy ;; esac
         done
         pfctl -a legion/svc -F all >/dev/null 2>&1
         pfctl -a legion/policy -F all >/dev/null 2>&1
         # Dying jails may hold mounts for a while: try again until none
         # are left, for up to 30 s.
         i=0; while mount -p | awk '{{print $2}}' | grep -qE '^({data}|{run})/' && [ $i -lt 30 ]; do
             mount -p | awk '{{print $2}}' | grep -E '^({data}|{run})/' | sort -r |
                 while read -r m; do umount -f \"$m\" 2>/dev/null; done
             mount -p | awk '{{print $2}}' | grep -qE '^({data}|{run})/' && sleep 1
             i=$((i + 1))
         done
         chflags -R 0 {data} {run} 2>/dev/null
         rm -rf {data} {run}
"
    )
}

/// A VM's lab directories.
pub const VM_DATA: &str = "/var/db/legion-lab";
pub const VM_RUN: &str = "/var/run/legion-lab";

/// Stops a VM's daemon, waiting up to 30 s.
pub const STOP: &str = "pkill -INT -f 'legion-lab daemon'
    i=0; while pgrep -qf 'legion-lab daemon' && [ $i -lt 30 ]; do sleep 1; i=$((i + 1)); done
    pkill -KILL -f 'legion-lab daemon'
";

impl Drop for Lab {
    fn drop(&mut self) {
        let teardown = |data: &str, run: &str| {
            if self.teardown.is_empty() {
                String::new()
            } else {
                format!("{}{}", clean(data, run), self.teardown)
            }
        };
        for vm in &self.vms {
            let _ = self.ssh(vm, &format!("{STOP}{}", teardown(VM_DATA, VM_RUN)));
        }
        if let Some(mut child) = self.daemon.take() {
            let _ = Command::new("kill")
                .args(["-INT", &child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(30);
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(200));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        if !self.teardown.is_empty() {
            let d = self.dir.display();
            let script = teardown(&format!("{d}/data"), &format!("{d}/run"));
            let _ = Command::new("/bin/sh").args(["-c", &script]).status();
        }
    }
}

pub fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

pub fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(500));
    }
}
