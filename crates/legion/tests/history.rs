//! What tasks used, kept 30 days (CONSOLE.md, phase 12), through real
//! daemons on three machines, so ignored by default: this host is the
//! manager n1, two VMs the managers n2 and n3 (see `tests/cluster.rs` for
//! the environment). A week of samples is put in n1's file while it is the
//! only manager, as a week of its running would leave it; the managers
//! that join copy it within a few minutes; the leader's daemon is killed,
//! and the next leader answers `legion usage --range 7d` with the week,
//! and goes on keeping a sample a minute in its own file.

mod common;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{Lab, PORT, Role, text, wait};
use serde_json::Value;

const MIN: i64 = 60_000;
const DAY: i64 = 24 * 60 * MIN;
/// Where a VM's daemon keeps its data (`Lab::vm_daemon`).
const VM_DATA: &str = "/var/db/legion-lab";

/// The node whose manager leads, as the manager at `server` says.
fn leader(lab: &Lab, server: &str) -> Option<String> {
    let out = lab.legion_at(server, &lab.admin(), &["status"]);
    let said = text(&out.stdout);
    let id = said
        .lines()
        .find_map(|l| l.strip_prefix("leader:"))?
        .trim()
        .to_owned();
    let addr = said.lines().find_map(|l| {
        let mut w = l.split_whitespace();
        (w.next() == Some(id.as_str())).then(|| w.next().unwrap_or_default().to_owned())
    })?;
    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
    if host == lab.host {
        return Some("n1".into());
    }
    let i = lab.vms.iter().position(|v| v.addr == host)?;
    Some(format!("n{}", i + 2))
}

/// The lines of node `n`'s history file.
fn kept(lab: &Lab, n: &str) -> Vec<Value> {
    let raw = if n == "n1" {
        std::fs::read_to_string(lab.dir.join("data/usage-history.jsonl")).unwrap_or_default()
    } else {
        let out = lab.ssh(
            &lab.vms[common::vm(n)],
            &format!("cat {VM_DATA}/usage-history.jsonl 2>/dev/null || true"),
        );
        text(&out.stdout)
    };
    raw.lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[test]
#[ignore = "needs root, ZFS and two more FreeBSD machines; see tests/cluster.rs"]
fn a_weeks_usage_outlives_a_change_of_leader() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");
    let servers: Vec<String> = std::iter::once(lab.server())
        .chain(lab.vms.iter().map(|v| format!("{}:{PORT}", v.addr)))
        .collect();

    step("n1 alone, a week of samples in its file, as a week of its running leaves it");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let now = now_ms();
    let first = (now - 7 * DAY).div_euclid(15 * MIN) * 15 * MIN + 15 * MIN;
    let mut seed = String::new();
    let mut at = first;
    while at < now - 30 * MIN {
        seed.push_str(
            &serde_json::json!({
                "k": "t/default/seed-1-1", "n": "n9", "s": "seed", "t": at,
                "v": [5.0, 64.0 * 1024.0 * 1024.0],
            })
            .to_string(),
        );
        seed.push('\n');
        at += 15 * MIN;
    }
    let week = seed.lines().count();
    // Stopped first: the daemon writes the file again as it compacts it.
    let child = lab.daemon.as_mut().expect("the host's daemon");
    let _ = std::process::Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the host's daemon stops");
        std::thread::sleep(Duration::from_millis(200));
    }
    lab.daemon = None;
    let path = lab.dir.join("data/usage-history.jsonl");
    let mut was = std::fs::read_to_string(&path).unwrap_or_default();
    was.push_str(&seed);
    std::fs::write(&path, was).unwrap();
    lab.restart_host(&[]);

    step("two managers join and copy the week, on the minute");
    let manager = lab.ok(&["join-token", "manager"]).trim().to_owned();
    lab.join(0, &manager, Role::Manager);
    lab.join(1, &manager, Role::Manager);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });
    let has_week = |n: &str| {
        kept(&lab, n)
            .iter()
            .filter(|e| e["k"] == "t/default/seed-1-1")
            .count()
            >= week
    };
    let copied = Instant::now();
    wait("n2 and n3 have the week", 300, || {
        has_week("n2") && has_week("n3")
    });
    eprintln!("copied in {} s", copied.elapsed().as_secs());
    assert!(has_week("n1"), "n1 keeps it");

    step("the leader's daemon killed; the next answers for the week");
    let lead = leader(&lab, &lab.server()).expect("a leader");
    let killed = now_ms();
    if lead == "n1" {
        let mut child = lab.daemon.take().expect("the host's daemon");
        child.kill().unwrap();
        let _ = child.wait();
    } else {
        let out = lab.ssh(
            &lab.vms[common::vm(&lead)],
            "pkill -KILL -f 'legion-lab daemon'",
        );
        assert!(out.status.success(), "{}", text(&out.stderr));
    }
    let other = servers
        .iter()
        .zip(["n1", "n2", "n3"])
        .find(|(_, n)| *n != lead)
        .map(|(s, _)| s.clone())
        .unwrap();
    let mut next = None;
    wait("a new leader", 120, || {
        next = leader(&lab, &other).filter(|n| *n != lead);
        next.is_some()
    });
    let next = next.unwrap();
    eprintln!("{lead} led; {next} leads now");
    let mut usage = Value::Null;
    wait("the week from the next leader", 120, || {
        let out = lab.legion_at(
            &other,
            &lab.admin(),
            &["usage", "--range", "7d", "-o", "json"],
        );
        usage = serde_json::from_str(&text(&out.stdout)).unwrap_or(Value::Null);
        usage
            .as_array()
            .is_some_and(|ts| ts.iter().any(|t| t["task"] == "seed-1-1"))
    });
    let seed = usage
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task"] == "seed-1-1")
        .unwrap();
    assert_eq!(seed["service"], "seed", "{seed}");
    assert_eq!(seed["node"], "n9", "{seed}");
    let samples = seed["samples"].as_array().unwrap();
    let span =
        samples.last().unwrap()["atMs"].as_i64().unwrap() - samples[0]["atMs"].as_i64().unwrap();
    assert!(
        span > 6 * DAY + 22 * 60 * MIN,
        "a week: {span} ms in {} samples",
        samples.len()
    );
    eprintln!(
        "{} samples over {:.1} days",
        samples.len(),
        span as f64 / DAY as f64
    );

    step("the next leader goes on keeping a sample a minute in its file");
    wait(
        "a sample from after the change in the new leader's file",
        180,
        || {
            kept(&lab, &next).iter().any(|e| {
                e["k"].as_str().is_some_and(|k| k.starts_with("n/"))
                    && e["t"].as_i64() > Some(killed)
            })
        },
    );

    step("the old leader back, for the teardown");
    if lead == "n1" {
        lab.restart_host(&[]);
    } else {
        lab.restart_vm(common::vm(&lead), Role::Manager);
    }
}
