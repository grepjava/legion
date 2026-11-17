//! The phase 2 exit test (PHASE.md): a compose app deploys on three nodes,
//! updates with zero downtime, and survives losing any one node. It drives
//! real daemons, so it is ignored by default and run by hand on a FreeBSD
//! host as root: this host is node n1, and two machines reachable over ssh
//! are n2 and n3 (each gets a throwaway ZFS pool). The test copies its own
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
//! unless a user is given, who must have sudo. Losing a node stops the
//! second at once: with `LEGION_CLUSTER_LOSE=reboot` it restarts without
//! syncing, and comes back with no daemon for the tests after this one;
//! otherwise it must be a bhyve guest of this host called `name`, which
//! `bhyvectl --destroy` powers off and leaves off.
//!
//! The app is `tests/cluster/compose.yaml`: web (three replicas, published
//! on port 30080) asks api (three replicas) for its name through api's
//! VIP. The lab directory is wiped first; remove it afterwards with
//! `chflags -R 0` and `rm -rf`.

mod common;

use std::collections::BTreeSet;
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
        self.http_get_from(vm, addr, PUBLISHED)
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
    common::http_get(addr, PUBLISHED)
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
#[ignore = "needs root, bhyve guests and ZFS on FreeBSD; see the module docs"]
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
    if !failures.is_empty() {
        lab.daemon_logs(&["WARN", "ERROR", "shop/web", "shop/api", "endpoint"], 60);
    }
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
    if std::env::var("LEGION_CLUSTER_LOSE").as_deref() == Ok("reboot") {
        // The connection goes with the machine, so its status says nothing.
        lab.ssh(lost, "reboot -q");
    } else {
        let out = Command::new("bhyvectl")
            .args(["--destroy", &format!("--vm={}", lost.name)])
            .output()
            .unwrap();
        assert!(out.status.success(), "{}", text(&out.stderr));
    }
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

/// The protocol `node`'s agent says it speaks: 1 from a build before the
/// handshake. It tells the previous release from this build where their
/// versions are the same, as they are until the next release is cut.
fn speaks(lab: &Lab, node: &str) -> u64 {
    lab.list("nodes", "")
        .iter()
        .find(|n| n["metadata"]["name"] == node)
        .and_then(|n| n["status"]["protocol"]["speaks"].as_u64())
        .unwrap_or(1)
}

/// The managers' protocol floor, as the manager at `server` reads it: 1
/// from a build before the handshake.
fn floor_at(lab: &Lab, server: &str) -> u64 {
    let out = Command::new(common::LEGION)
        .args(["--server", server, "--identity"])
        .arg(lab.admin())
        .arg("status")
        .output()
        .unwrap();
    text(&out.stdout)
        .lines()
        .find_map(|l| l.split_once("the managers' floor: "))
        .and_then(|(_, f)| f.trim_end_matches(')').parse().ok())
        .unwrap_or(1)
}

/// The app's running pods on `node`: each task's name, jail and address.
/// A restarted daemon adopts them, and they stay the same; replaced, they
/// don't, though the app may well answer throughout.
fn pods_on(lab: &Lab, node: &str) -> BTreeSet<(String, u64, String)> {
    lab.list("tasks", "shop")
        .iter()
        .filter(|t| {
            t["spec"]["node"] == node
                && t["spec"]["desiredState"] == "running"
                && t["status"]["state"] == "running"
        })
        .map(|t| {
            (
                t["metadata"]["name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                t["status"]["jid"].as_u64().unwrap_or_default(),
                t["status"]["ip"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

/// The pods `name` ran before its restart, as [`pods_on`] gave them, are
/// the ones it runs now.
fn kept(lab: &Lab, name: &str, before: &BTreeSet<(String, u64, String)>) {
    assert!(!before.is_empty(), "{name} runs some of the app");
    assert_eq!(
        &pods_on(lab, name),
        before,
        "{name}'s pods adopted, not replaced"
    );
}

/// The upgrade drill (PLAN.md, phase 6): three managers on the previous
/// release (`LEGION_PREVIOUS`, its `legion`) run the app, and each node in
/// turn is restarted on this build, as an operator upgrades. Each node's
/// pods are adopted by the new daemon, not replaced. The managers' floor
/// stays where the previous release left it until the last manager is
/// upgraded, then rises. A node's pods may fail to look names up while
/// that node restarts, and only then, if the previous release's node
/// resolver lives in its daemon; restarted again on this build, whose
/// resolver outlives the daemon, the app answers throughout, and the pods
/// are adopted again. Then the upgraded cluster updates the app.
#[test]
#[ignore = "needs root, the lab's VMs and ZFS on FreeBSD; see the module docs"]
fn the_cluster_upgrades_one_node_at_a_time() {
    let previous = std::env::var("LEGION_PREVIOUS").expect("set LEGION_PREVIOUS");
    assert!(Path::new(&previous).is_file(), "{previous} is no file");
    let mut lab = Lab::from_env();
    lab.binary = previous;
    let step = |what: &str| eprintln!("--- {what}");

    step("three managers on the previous release");
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
    let this = legion_api::protocol::SPEAKS as u64;
    for n in ["n1", "n2", "n3"] {
        assert!(speaks(&lab, n) < this, "{n} runs the previous release");
    }
    lab.deploy("1");
    wait("the app", 600, || lab.app_ready());
    let old = floor_at(&lab, &lab.server());
    assert!(
        old < this,
        "the previous release's floor {old}, this build's {this}"
    );

    step("upgrade n3, then n2, then n1");
    let addrs: Vec<String> = lab.vms.iter().map(|v| v.addr.clone()).collect();
    let load = Load::start(addrs.clone());
    std::thread::sleep(Duration::from_secs(2));
    lab.binary = common::LEGION.to_owned();
    // Each node's restart, from its daemon's stop to its being ready.
    let mut restarts = Vec::new();
    for i in [1, 0] {
        let name = format!("n{}", i + 2);
        let before = pods_on(&lab, &name);
        let from = Instant::now();
        lab.restart_vm(i, Role::Manager);
        wait(&format!("{name} on this build"), 120, || {
            lab.node_state(&name) == "ready" && speaks(&lab, &name) == this
        });
        restarts.push((from, Instant::now()));
        wait("the app", 300, || lab.app_ready());
        kept(&lab, &name, &before);
        let at = format!("{}:{}", lab.vms[i].addr, common::PORT);
        std::thread::sleep(Duration::from_secs(5));
        assert_eq!(floor_at(&lab, &at), old, "the floor with n1 not upgraded");
    }
    let before = pods_on(&lab, "n1");
    let from = Instant::now();
    lab.restart_host(&[]);
    wait("n1 on this build", 120, || {
        lab.node_state("n1") == "ready" && speaks(&lab, "n1") == this
    });
    restarts.push((from, Instant::now()));
    wait("the floor raised", 60, || {
        floor_at(&lab, &lab.server()) == this
    });
    wait("the app", 300, || lab.app_ready());
    kept(&lab, "n1", &before);
    std::thread::sleep(Duration::from_secs(2));
    let results = load.finish();
    let failures: Vec<_> = results.iter().filter(|r| r.2.is_err()).collect();
    eprintln!(
        "{} requests during the upgrade, {} failed: {:?}",
        results.len(),
        failures.len(),
        failures.iter().take(5).collect::<Vec<_>>()
    );
    // A request is sent up to 5 s before its answer is recorded.
    let slack = Duration::from_secs(10);
    let outside: Vec<_> = failures
        .iter()
        .filter(|r| {
            !restarts
                .iter()
                .any(|(from, to)| r.0 >= *from && r.0 <= *to + slack)
        })
        .collect();
    assert!(
        outside.is_empty(),
        "requests failed outside a node's restart: {:?}",
        outside.iter().take(5).collect::<Vec<_>>()
    );

    step("restart every node again on this build: no request fails");
    let load = Load::start(addrs.clone());
    std::thread::sleep(Duration::from_secs(2));
    for i in [1, 0] {
        let name = format!("n{}", i + 2);
        let before = pods_on(&lab, &name);
        lab.restart_vm(i, Role::Manager);
        wait(&format!("{name} back"), 120, || {
            lab.node_state(&name) == "ready"
        });
        wait("the app", 300, || lab.app_ready());
        kept(&lab, &name, &before);
    }
    let before = pods_on(&lab, "n1");
    lab.restart_host(&[]);
    wait("n1 back", 120, || lab.node_state("n1") == "ready");
    wait("the app", 300, || lab.app_ready());
    kept(&lab, "n1", &before);
    std::thread::sleep(Duration::from_secs(2));
    let results = load.finish();
    let failures: Vec<_> = results.iter().filter(|r| r.2.is_err()).collect();
    eprintln!(
        "{} requests through the restarts, {} failed: {:?}",
        results.len(),
        failures.len(),
        failures.iter().take(5).collect::<Vec<_>>()
    );
    assert!(failures.is_empty(), "requests failed through the restarts");

    step("the upgraded cluster updates the app");
    lab.deploy("2");
    wait("the update", 300, || {
        lab.list("services", "shop").iter().any(|s| {
            s["metadata"]["name"] == "web" && s["status"]["update"]["state"] == "completed"
        }) && lab.app_ready()
    });
    for a in &addrs {
        assert!(get(a).unwrap().starts_with("web 2 "));
    }
    step("done");
}
