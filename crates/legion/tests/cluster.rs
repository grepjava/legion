//! The phase 2 exit test (PHASE.md): a compose app deploys on three nodes,
//! updates with zero downtime, and survives losing any one node. It drives
//! real daemons, so it is ignored by default and run by hand on a FreeBSD
//! host as root: this host is node n1, and two machines reachable over ssh
//! are n2 and n3, each with Podman and ocijail. The test copies its own
//! `legion` to them. All three are managers.
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lcluster \
//!     LEGION_CLUSTER_HOST=192.168.253.254 \
//!     LEGION_CLUSTER_VMS="lvm2=192.168.253.2 lvm3=192.168.253.3" \
//!     LEGION_CLUSTER_KEY=/var/tmp/lvm/id_lab \
//!     target/debug/deps/cluster-<hash> --ignored --nocapture
//! ```
//!
//! Each of `LEGION_CLUSTER_VMS` is `name=[user@]addr`, logging in as root
//! unless a user is given, who must have sudo. The second must be a bhyve
//! guest of this host called `name`: losing a node powers it off with
//! `bhyvectl --destroy`, and it is left off.
//!
//! The app is `tests/cluster/compose.yaml`: web (three replicas, published
//! on port 30080) asks api (three replicas) for its name through api's
//! VIP. The lab directory is wiped first; remove it afterwards with
//! `chflags -R 0` and `rm -rf`.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const PORT: u16 = 7470;
const PUBLISHED: u16 = 30080;
/// Where the test's `legion` goes on the other nodes.
const REMOTE: &str = "/usr/local/bin/legion-lab";

struct Vm {
    name: String,
    /// Who logs in, root if empty; another user runs commands through sudo.
    user: String,
    addr: String,
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

struct Lab {
    dir: PathBuf,
    host: String,
    vms: Vec<Vm>,
    key: String,
    daemon: Option<Child>,
}

impl Lab {
    fn server(&self) -> String {
        format!("{}:{PORT}", self.host)
    }

    fn legion(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        Command::new(LEGION)
            .args(["--server", &self.server()])
            .arg("--identity")
            .arg(self.dir.join("data/admin.pem"))
            .args(args)
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.legion(args, &[]);
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
    fn ssh(&self, vm: &Vm, cmd: &str) -> Output {
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
        let status = self
            .remote("scp")
            .arg("-q")
            .arg(&stripped)
            .arg(format!("{}:/tmp/legion-lab", vm.login()))
            .status()
            .unwrap();
        assert!(status.success(), "copying legion to {}", vm.name);
        let out = self.ssh(
            vm,
            &format!("install -m 755 /tmp/legion-lab {REMOTE} && rm /tmp/legion-lab"),
        );
        assert!(out.status.success(), "{}", text(&out.stderr));
    }

    fn start_host(&mut self) {
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
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        wait("the host's daemon", 60, || {
            self.legion(&["status"], &[]).status.success()
        });
    }

    /// Joins VM `i` as a manager named `n<i+2>` and starts its daemon, after
    /// removing what an earlier run left there.
    fn join(&self, i: usize, token: &str) {
        let vm = &self.vms[i];
        self.install(vm);
        let name = format!("n{}", i + 2);
        let addr = format!("{}:{PORT}", vm.addr);
        let cmd = format!(
            "{STOP}
             {clean}
             set -e
             {REMOTE} join {token} --manager {server} --data-dir /var/db/legion-lab \
                 --advertise {addr} --node-name {name}
             daemon -o /var/log/legion-lab.log {REMOTE} daemon --node-name {name} \
                 --data-dir /var/db/legion-lab --run-dir /var/run/legion-lab \
                 --listen {addr} --advertise {addr} </dev/null >/dev/null 2>&1
",
            clean = clean("/var/db/legion-lab", "/var/run/legion-lab"),
            server = self.server(),
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

    /// [`get`] of `addr`, sent from a VM.
    fn get_from(&self, vm: &Vm, addr: &str) -> Result<String, String> {
        let out = self.ssh(
            vm,
            &format!("printf 'GET / HTTP/1.0\\r\\n\\r\\n' | nc -N -w 5 {addr} {PUBLISHED}"),
        );
        body(&text(&out.stdout)).map_err(|e| format!("from {}: {e}", vm.name))
    }

    /// Objects of a kind in `shop`, as JSON.
    fn list(&self, kind: &str) -> Vec<Value> {
        let namespace = if kind == "nodes" { "default" } else { "shop" };
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

    fn deploy(&self, version: &str) {
        let compose = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cluster/compose.yaml");
        let out = self.legion(
            &["stack", "deploy", "shop", "-c", compose.to_str().unwrap()],
            &[("WEB_VERSION", version)],
        );
        assert!(out.status.success(), "deploy: {}", text(&out.stderr));
    }

    /// Both services have all their tasks ready and none mid-update.
    fn app_ready(&self) -> bool {
        let services = self.list("services");
        services.len() == 2
            && services.iter().all(|s| {
                let st = &s["status"];
                st["desired"] == 3
                    && st["ready"] == 3
                    && st["running"] == 3
                    && st["observedGeneration"] == s["metadata"]["generation"]
                    && st["update"]["state"]
                        .as_str()
                        .is_none_or(|u| u == "completed")
            })
    }

    /// The nodes running a service's tasks.
    fn placed(&self, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .list("tasks")
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

    fn node_state(&self, name: &str) -> String {
        self.list("nodes")
            .iter()
            .find(|n| n["metadata"]["name"] == name)
            .and_then(|n| n["status"]["state"].as_str().map(str::to_owned))
            .unwrap_or_default()
    }
}

/// A script removing what a stopped daemon with these directories left:
/// its pods, Podman service, interfaces, load balancing and state.
fn clean(data: &str, run: &str) -> String {
    format!(
        "pkill -f 'podman --root {data}/podman'
         podman --root {data}/podman --runroot {run}/podman --tmpdir {run}/podman-tmp \
             rm -af -t 0 >/dev/null 2>&1
         for i in $(ifconfig -l); do
             case $i in lp*|legion0|wg0) ifconfig $i destroy ;; esac
         done
         pfctl -a legion/svc -F all >/dev/null 2>&1
         mount -p | awk '{{print $2}}' | grep -E '^({data}|{run})/' | sort -r |
             while read -r m; do umount -f \"$m\"; done
         chflags -R 0 {data} {run} 2>/dev/null
         rm -rf {data} {run}
"
    )
}

/// Stops a VM's daemon, waiting up to 30 s.
const STOP: &str = "pkill -INT -f 'legion-lab daemon'
    i=0; while pgrep -qf 'legion-lab daemon' && [ $i -lt 30 ]; do sleep 1; i=$((i + 1)); done
    pkill -KILL -f 'legion-lab daemon'
";

impl Drop for Lab {
    fn drop(&mut self) {
        for vm in &self.vms {
            let _ = self.ssh(vm, STOP);
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
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// One HTTP/1.0 GET of `/` on a node's published port: the body of a 200.
fn get(addr: &str) -> Result<String, String> {
    let addr: SocketAddr = format!("{addr}:{PUBLISHED}").parse().unwrap();
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect: {e}"))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    write!(s, "GET / HTTP/1.0\r\n\r\n").map_err(|e| format!("write: {e}"))?;
    let mut resp = String::new();
    s.read_to_string(&mut resp)
        .map_err(|e| format!("read: {e}"))?;
    body(&resp)
}

/// The body of a 200 response.
fn body(resp: &str) -> Result<String, String> {
    match resp.split_once("\r\n\r\n") {
        Some((head, body)) if head.contains(" 200 ") => Ok(body.trim().to_owned()),
        _ => Err(format!("response {resp:?}")),
    }
}

/// When each request was answered, the node asked, and the body or error.
type Results = Vec<(Instant, String, Result<String, String>)>;

/// Requests sent round-robin to some nodes' published port until stopped.
struct Load {
    stop: Arc<AtomicBool>,
    results: Arc<Mutex<Results>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Load {
    fn start(addrs: Vec<String>) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let results = Arc::new(Mutex::new(Vec::new()));
        let thread = {
            let (stop, results) = (stop.clone(), results.clone());
            std::thread::spawn(move || {
                for addr in addrs.iter().cycle() {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let r = get(addr);
                    results
                        .lock()
                        .unwrap()
                        .push((Instant::now(), addr.clone(), r));
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        };
        Load {
            stop,
            results,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> Results {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
        std::mem::take(&mut *self.results.lock().unwrap())
    }
}

#[test]
#[ignore = "needs root, bhyve guests and Podman on FreeBSD; see the module docs"]
fn app_updates_and_survives_losing_a_node() {
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
    if dir.exists() {
        let d = dir.display();
        let script = clean(&format!("{d}/data"), &format!("{d}/run"));
        Command::new("/bin/sh")
            .args(["-c", &format!("{script}chflags -R 0 {d}; rm -rf {d}")])
            .status()
            .unwrap();
    }
    std::fs::create_dir_all(&dir).unwrap();
    let mut lab = Lab {
        dir,
        host: var("LEGION_CLUSTER_HOST"),
        vms,
        key: var("LEGION_CLUSTER_KEY"),
        daemon: None,
    };
    let step = |what: &str| eprintln!("--- {what}");

    step("three managers");
    lab.start_host();
    lab.ok(&["init"]);
    let token = lab.ok(&["join-token", "manager"]).trim().to_owned();
    for i in 0..2 {
        lab.join(i, &token);
    }
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });

    step("deploy");
    lab.deploy("1");
    wait("the app", 600, || lab.app_ready());
    assert_eq!(
        lab.placed("web"),
        ["n1", "n2", "n3"],
        "spread over the nodes"
    );
    assert_eq!(
        lab.placed("api"),
        ["n1", "n2", "n3"],
        "spread over the nodes"
    );
    // Every node answers on the published port, whichever task serves it,
    // and web reaches api across the mesh. This host cannot reach its own
    // published ports (DESIGN.md §5.4), so a VM asks it; the load goes to
    // the VMs.
    let addrs: Vec<String> = lab.vms.iter().map(|v| v.addr.clone()).collect();
    let mut webs = BTreeSet::new();
    let mut apis = BTreeSet::new();
    for _ in 0..10 {
        let host = lab.get_from(&lab.vms[1], &lab.host);
        let bodies = addrs.iter().map(|a| (a, get(a)));
        for (a, body) in std::iter::once((&lab.host, host)).chain(bodies) {
            let body = body.unwrap_or_else(|e| panic!("{a}: {e}"));
            let words: Vec<&str> = body.split_whitespace().collect();
            assert_eq!(&words[..2], ["web", "1"], "{body}");
            webs.insert(words[2].to_owned());
            apis.insert(words[4].to_owned());
        }
    }
    eprintln!("web tasks answering: {webs:?}; api tasks: {apis:?}");
    assert_eq!(webs.len(), 3, "every web task serves: {webs:?}");
    assert_eq!(apis.len(), 3, "every api task serves: {apis:?}");

    step("update with zero downtime");
    let load = Load::start(addrs.clone());
    std::thread::sleep(Duration::from_secs(2));
    lab.deploy("2");
    wait("the update", 300, || {
        lab.list("services").iter().any(|s| {
            s["metadata"]["name"] == "web" && s["status"]["update"]["state"] == "completed"
        }) && lab.app_ready()
    });
    std::thread::sleep(Duration::from_secs(2));
    let results = load.finish();
    let failures: Vec<_> = results.iter().filter(|r| r.2.is_err()).collect();
    let versions: BTreeSet<&str> = results
        .iter()
        .filter_map(|r| r.2.as_ref().ok())
        .filter_map(|b| b.split_whitespace().nth(1))
        .collect();
    eprintln!(
        "{} requests during the update, {} failed: {:?}",
        results.len(),
        failures.len(),
        failures.iter().take(5).collect::<Vec<_>>()
    );
    assert!(failures.is_empty(), "requests failed during the update");
    assert_eq!(versions, BTreeSet::from(["1", "2"]));
    for a in &addrs {
        assert!(get(a).unwrap().starts_with("web 2 "));
    }
    let host = lab.get_from(&lab.vms[1], &lab.host);
    assert!(host.unwrap().starts_with("web 2 "));

    step("lose a node");
    let lost = &lab.vms[1];
    let survivors: Vec<String> = addrs.iter().filter(|a| **a != lost.addr).cloned().collect();
    let load = Load::start(survivors.clone());
    std::thread::sleep(Duration::from_secs(2));
    let gone = Instant::now();
    let out = Command::new("bhyvectl")
        .args(["--destroy", &format!("--vm={}", lost.name)])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", text(&out.stderr));
    wait("n3 down", 60, || lab.node_state("n3") == "down");
    wait("the app back on n1 and n2", 300, || {
        lab.app_ready()
            && !lab.placed("web").contains(&"n3".to_owned())
            && !lab.placed("api").contains(&"n3".to_owned())
    });
    let recovered = Instant::now();
    std::thread::sleep(Duration::from_secs(10));
    let results = load.finish();
    let failed: Vec<_> = results.iter().filter(|r| r.2.is_err()).collect();
    let last_failure = failed.last().map(|r| r.0.duration_since(gone));
    eprintln!(
        "{} requests while losing n3, {} failed, the last {:?} after it went; back after {:?}",
        results.len(),
        failed.len(),
        last_failure,
        recovered.duration_since(gone),
    );
    assert!(
        results
            .iter()
            .filter(|r| r.0 > recovered)
            .all(|r| r.2.is_ok()),
        "requests fail after recovery"
    );
    // The two managers left still write.
    lab.deploy("3");
    wait("an update on two nodes", 300, || {
        lab.app_ready()
            && survivors
                .iter()
                .all(|a| get(a).is_ok_and(|b| b.starts_with("web 3 ")))
    });
    step("done");
}
