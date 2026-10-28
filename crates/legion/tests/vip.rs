//! The managers' floating address (DESIGN.md §3.8) with real CARP: three
//! managers, each in a VNET jail of its own on a throwaway bridge, share
//! one address. The leader holds it, the API answers on it, and it moves
//! to the next leader when the leader stops, cleanly or killed.
//!
//! Ignored by default; run as root on a FreeBSD host, where it creates the
//! jails `lvip1` to `lvip3`, the bridge `lvipbr0` and their epairs, and
//! `/var/tmp/lvip`, all removed at the end:
//!
//! ```sh
//! sudo target/debug/deps/vip-<hash> --ignored --nocapture
//! ```

use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const DIR: &str = "/var/tmp/lvip";
const VIP: &str = "10.78.0.100";

fn sh(cmd: &str) -> Output {
    Command::new("/bin/sh").args(["-c", cmd]).output().unwrap()
}

fn ok(cmd: &str) -> String {
    let out = sh(cmd);
    assert!(
        out.status.success(),
        "{cmd}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        if Instant::now() >= deadline {
            // The daemons' logs go with the lab; show them first.
            for n in 1..=3 {
                let log = std::fs::read_to_string(format!("{DIR}/n{n}.log")).unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(15).collect();
                eprintln!(
                    "--- n{n}.log
{}",
                    tail.into_iter().rev().collect::<Vec<_>>().join(
                        "
"
                    )
                );
            }
            panic!("timed out waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

struct Lab {
    /// The epair end inside each jail, and its daemon.
    nodes: Vec<(String, Option<Child>)>,
    carp_loaded: bool,
}

impl Lab {
    fn new() -> Lab {
        let carp_loaded = sh("kldstat -q -m carp").status.success();
        ok("kldload -n carp if_bridge if_epair");
        let _ = std::fs::remove_dir_all(DIR);
        std::fs::create_dir_all(DIR).unwrap();
        std::fs::write(format!("{DIR}/vip-pass"), "lab-secret\n").unwrap();
        ok("ifconfig bridge create name lvipbr0 up");
        let mut lab = Lab {
            nodes: Vec::new(),
            carp_loaded,
        };
        for n in 1..=3 {
            let a = ok("ifconfig epair create").trim().to_owned();
            let b = format!("{}b", a.trim_end_matches('a'));
            // Renamed, so they do not collide with epairs the jails make.
            let (ha, jb) = (format!("lvip{n}a"), format!("lvip{n}b"));
            ok(&format!(
                "ifconfig {a} name {ha} up && ifconfig {b} name {jb}"
            ));
            ok(&format!("ifconfig lvipbr0 addm {ha}"));
            ok(&format!("jail -c name=lvip{n} path=/ vnet persist"));
            ok(&format!("ifconfig {jb} vnet lvip{n}"));
            ok(&format!(
                "jexec lvip{n} ifconfig lo0 inet 127.0.0.1/8 up && jexec lvip{n} ifconfig {jb} inet 10.78.0.{n}/24 up"
            ));
            lab.nodes.push((jb, None));
        }
        lab
    }

    fn data(n: usize) -> String {
        format!("{DIR}/n{n}")
    }

    fn start(&mut self, n: usize) {
        let iface = self.nodes[n - 1].0.clone();
        let log = std::fs::File::create(format!("{DIR}/n{n}.log")).unwrap();
        let child = Command::new("jexec")
            .arg(format!("lvip{n}"))
            .arg(LEGION)
            .args(["daemon", "--no-agent", "--listen", "0.0.0.0:7470"])
            .args(["--advertise", &format!("10.78.0.{n}:7470")])
            .args(["--data-dir", &Lab::data(n)])
            // The jails have no hostname to name the node after.
            .args(["--node-name", &format!("m{n}")])
            .args(["--vip", VIP, "--vip-interface", &iface])
            .args(["--vip-vhid", "42"])
            .args(["--vip-password-file", &format!("{DIR}/vip-pass")])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.nodes[n - 1].1 = Some(child);
    }

    /// `legion` inside jail `n`, against `server`, with n's credentials.
    fn legion(&self, n: usize, server: &str, args: &[&str]) -> Output {
        Command::new("jexec")
            .arg(format!("lvip{n}"))
            .arg(LEGION)
            .args(["--server", server, "--identity"])
            .arg(format!("{}/admin.pem", Lab::data(n)))
            .args(args)
            .output()
            .unwrap()
    }

    fn up(&self, n: usize) -> bool {
        self.legion(n, "127.0.0.1:7470", &["status"])
            .status
            .success()
    }

    /// Which jails hold the address, as CARP masters.
    fn masters(&self) -> Vec<usize> {
        (1..=3)
            .filter(|n| {
                let shown = sh(&format!("jexec lvip{n} ifconfig {}", self.nodes[n - 1].0));
                String::from_utf8_lossy(&shown.stdout).contains("carp: MASTER vhid 42")
            })
            .collect()
    }

    /// The leader's node id, as the API on the address reports it.
    fn status_via_vip(&self, from: usize) -> Option<String> {
        let out = self.legion(from, &format!("{VIP}:7470"), &["status"]);
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn node_id(n: usize) -> String {
        std::fs::read_to_string(format!("{}/node-id", Lab::data(n)))
            .unwrap()
            .trim()
            .to_owned()
    }

    /// The jail whose manager leads, by its own account.
    fn leader(&self) -> Option<usize> {
        (1..=3).find(|&n| {
            self.nodes[n - 1].1.is_some() && {
                let out = self.legion(n, "127.0.0.1:7470", &["status"]);
                let text = String::from_utf8_lossy(&out.stdout).into_owned();
                text.lines()
                    .any(|l| l.contains(&Lab::node_id(n)) && l.contains("leader"))
            }
        })
    }

    fn stop(&mut self, n: usize, signal: &str) {
        if let Some(mut child) = self.nodes[n - 1].1.take() {
            ok(&format!("kill -{signal} {}", child.id()));
            let _ = child.wait();
        }
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        for n in 1..=3 {
            self.stop(n, "KILL");
        }
        for n in 1..=3 {
            let jb = &self
                .nodes
                .get(n - 1)
                .map(|(i, _)| i.clone())
                .unwrap_or_default();
            let _ = sh(&format!("jexec lvip{n} ifconfig {jb} destroy"));
            let _ = sh(&format!("jail -r lvip{n}"));
            let _ = sh(&format!("ifconfig lvip{n}a destroy"));
        }
        let _ = sh("ifconfig lvipbr0 destroy");
        if !self.carp_loaded {
            let _ = sh("kldunload carp");
        }
        let _ = sh(&format!("chflags -R 0 {DIR}; rm -rf {DIR}"));
    }
}

#[test]
#[ignore = "needs root on FreeBSD; makes jails, a bridge and epairs (see the module docs)"]
fn the_floating_address_follows_the_leader() {
    let mut lab = Lab::new();
    let step = |what: &str| eprintln!("--- {what}");

    step("three managers sharing one address");
    lab.start(1);
    wait("the first manager", 30, || lab.up(1));
    let init = lab.legion(1, "127.0.0.1:7470", &["init"]);
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let token = lab.legion(1, "127.0.0.1:7470", &["join-token", "manager"]);
    let token = String::from_utf8_lossy(&token.stdout).trim().to_owned();
    for n in [2, 3] {
        let out = Command::new("jexec")
            .arg(format!("lvip{n}"))
            .arg(LEGION)
            .args(["join", &token, "--manager", "10.78.0.1:7470"])
            .args(["--advertise", &format!("10.78.0.{n}:7470")])
            .args(["--node-name", &format!("m{n}"), "--data-dir", &Lab::data(n)])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        lab.start(n);
        wait(&format!("manager {n}"), 30, || lab.up(n));
    }
    wait("three voters", 60, || {
        lab.status_via_vip(3)
            .is_some_and(|s| s.matches("voter").count() == 3 && !s.contains("learner"))
    });
    wait("the leader holding the address alone", 30, || {
        lab.leader().is_some_and(|l| lab.masters() == [l])
    });
    let first = lab.leader().unwrap();
    eprintln!("leader and holder: lvip{first}");

    step("stopped cleanly, the leader hands the address on");
    lab.stop(first, "TERM");
    wait("a new leader holding the address", 60, || {
        lab.leader()
            .is_some_and(|l| l != first && lab.masters() == [l])
    });
    let second = lab.leader().unwrap();
    let other = (1..=3).find(|n| *n != first && *n != second).unwrap();
    assert!(lab.status_via_vip(other).is_some(), "the API answers on it");
    eprintln!("now lvip{second}");

    step("killed, the leader loses the address to CARP and the next leader");
    lab.start(first);
    wait("the first manager back", 30, || lab.up(first));
    lab.stop(second, "KILL");
    wait("another leader holding the address", 60, || {
        lab.leader()
            .is_some_and(|l| l != second && lab.masters() == [l])
    });
    let third = lab.leader().unwrap();
    assert!(lab.status_via_vip(third).is_some());
    eprintln!("now lvip{third}");
    step("done");
}
