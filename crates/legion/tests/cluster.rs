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

mod common;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Lab, Role, Vm, text, wait};

const PUBLISHED: u16 = 30080;

impl Lab {
    /// [`get`] of `addr`, sent from a VM.
    fn get_from(&self, vm: &Vm, addr: &str) -> Result<String, String> {
        let out = self.ssh(
            vm,
            &format!("printf 'GET / HTTP/1.0\\r\\n\\r\\n' | nc -N -w 5 {addr} {PUBLISHED}"),
        );
        body(&text(&out.stdout)).map_err(|e| format!("from {}: {e}", vm.name))
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
        let services = self.list("services", "shop");
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
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("three managers");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let token = lab.ok(&["join-token", "manager"]).trim().to_owned();
    for i in 0..2 {
        lab.join(i, &token, Role::Manager);
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
        lab.placed("shop", "web"),
        ["n1", "n2", "n3"],
        "spread over the nodes"
    );
    assert_eq!(
        lab.placed("shop", "api"),
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
        lab.list("services", "shop").iter().any(|s| {
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
            && !lab.placed("shop", "web").contains(&"n3".to_owned())
            && !lab.placed("shop", "api").contains(&"n3".to_owned())
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
