//! A witness manager (docs/HA.md) through real daemons on three machines,
//! so ignored by default: this host is the manager n1, the first VM the
//! manager n2, the second VM the witness n3, which joins with `--witness`
//! and runs no agent (see `tests/cluster.rs` for the environment). Two
//! managers and a witness are three voters: the leader's daemon killed,
//! the other full manager leads, never the witness; writes go on through
//! the witness; the lost node's tasks move to the node left; and the old
//! leader comes back as a follower.

mod common;

use std::time::Instant;

use common::{Lab, PORT, Role, text, wait};

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";

fn app(replicas: u32) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Service
metadata: {{ name: sleeper, namespace: default }}
spec:
  replicas: {replicas}
  pod:
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sleep, "86400"]
"#
    )
}

/// The leader as the manager at `server` sees it, by node name.
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

/// The running tasks of `sleeper`, by node, as the manager at `server`
/// lists them.
fn running(lab: &Lab, server: &str) -> Vec<String> {
    let out = lab.legion_at(
        server,
        &lab.admin(),
        &["list", "tasks", "-n", "default", "-o", "json"],
    );
    let tasks: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap_or_default();
    let mut nodes: Vec<String> = tasks
        .iter()
        .filter(|t| t["spec"]["service"] == "sleeper" && t["status"]["state"] == "running")
        .map(|t| t["spec"]["node"].as_str().unwrap_or_default().to_owned())
        .collect();
    nodes.sort();
    nodes
}

#[test]
#[ignore = "needs root, ZFS and two more FreeBSD machines; see tests/cluster.rs"]
fn a_witness_keeps_the_quorum_and_a_full_manager_takes_over() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");
    let server = |n: &str, lab: &Lab| match n {
        "n1" => lab.server(),
        _ => format!("{}:{PORT}", lab.vms[common::vm(n)].addr),
    };

    step("n1 and n2 full managers, n3 a witness");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let token = lab.ok(&["join-token", "manager"]).trim().to_owned();
    lab.join(0, &token, Role::Manager);
    lab.join(1, &token, Role::Witness);
    let witness = server("n3", &lab);
    wait("n1 and n2 ready", 120, || {
        ["n1", "n2"].iter().all(|n| lab.node_state(n) == "ready")
    });
    wait("three voters, n3 a witness", 120, || {
        let s = lab.ok(&["status"]);
        s.lines().filter(|l| l.contains(" voter")).count() == 3
            && s.lines().any(|l| {
                l.contains(&format!("{}:", lab.vms[1].addr))
                    && l.trim_end().ends_with("voter, witness")
            })
    });
    assert!(
        lab.list("nodes", "")
            .iter()
            .all(|n| n["metadata"]["name"] != "n3"),
        "a witness runs no agent, so has no Node"
    );
    let ha = text(&lab.legion(&["ha"], &[]).stdout);
    assert!(
        ha.starts_with("3 voting managers (1 witness), 2 nodes running tasks"),
        "{ha}"
    );
    assert!(
        !ha.contains("RISK     cluster:"),
        "two managers and a witness lose no quorum with one machine: {ha}"
    );

    step("a task on each node that runs tasks");
    let file = lab.dir.join("sleeper.yaml");
    std::fs::write(&file, app(2)).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
    wait("a task on n1 and on n2", 600, || {
        running(&lab, &lab.server()) == ["n1", "n2"]
    });

    step("the leader's daemon killed: the other full manager leads, never the witness");
    let lead = leader(&lab, &lab.server()).expect("a leader");
    assert_ne!(lead, "n3", "a witness never leads");
    let other = if lead == "n1" { "n2" } else { "n1" };
    let killed = Instant::now();
    if lead == "n1" {
        let mut child = lab.daemon.take().expect("the host's daemon");
        child.kill().unwrap();
        let _ = child.wait();
    } else {
        let out = lab.ssh(&lab.vms[0], "pkill -KILL -f 'legion-lab daemon'");
        assert!(out.status.success(), "{}", text(&out.stderr));
    }
    let other_server = server(other, &lab);
    wait("the other full manager leads", 120, || {
        leader(&lab, &other_server).as_deref() == Some(other)
    });
    eprintln!(
        "{other} leads {} ms after {lead}'s daemon was killed",
        killed.elapsed().as_millis()
    );
    assert_eq!(
        leader(&lab, &witness).as_deref(),
        Some(other),
        "the witness follows it"
    );

    step("writes go on through the witness");
    std::fs::write(&file, app(3)).unwrap();
    let out = lab.legion_at(
        &witness,
        &lab.admin(),
        &["apply", "-f", file.to_str().unwrap()],
    );
    assert!(out.status.success(), "{}", text(&out.stderr));

    step("the lost node's task moves to the node left");
    wait("three tasks, all on the node left", 600, || {
        running(&lab, &other_server) == [other, other, other]
    });

    step("the old leader back, a follower");
    if lead == "n1" {
        lab.restart_host(&[]);
    } else {
        lab.restart_vm(0, Role::Manager);
    }
    wait("it is ready again", 180, || {
        let out = lab.legion_at(
            &other_server,
            &lab.admin(),
            &["list", "nodes", "-o", "json"],
        );
        let nodes: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap_or_default();
        nodes
            .iter()
            .any(|n| n["metadata"]["name"] == lead && n["status"]["state"] == "ready")
    });
    let lead_after = leader(&lab, &witness).expect("a leader");
    assert_ne!(lead_after, "n3", "a witness never leads");
}
