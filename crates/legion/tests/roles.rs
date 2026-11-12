//! Changing nodes' roles in place on three nodes (DESIGN.md §3.7, roles):
//! a worker promoted to a manager, then a manager demoted to a worker,
//! each node's tasks running on throughout, the cluster serving.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root: this host is the manager n1, the
//! first VM the worker n2, the second the manager n3.

mod common;

use common::{Lab, text, wait};

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";

/// Three tasks, one per node as the scheduler spreads them.
fn app() -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Service
metadata: {{ name: sleeper, namespace: default }}
spec:
  replicas: 3
  pod:
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sleep, "86400"]
"#
    )
}

/// The running tasks of `sleeper`, by name, with their nodes.
fn running(lab: &Lab) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = lab
        .list("tasks", "default")
        .into_iter()
        .filter(|t| t["spec"]["service"] == "sleeper" && t["status"]["state"] == "running")
        .map(|t| {
            (
                t["metadata"]["name"].as_str().unwrap().to_owned(),
                t["spec"]["node"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    v.sort();
    v
}

/// A node's role and state, as its Node says.
fn node(lab: &Lab, name: &str) -> (String, String) {
    lab.list("nodes", "")
        .into_iter()
        .find(|n| n["metadata"]["name"] == name)
        .map(|n| {
            (
                n["spec"]["role"].as_str().unwrap_or_default().to_owned(),
                n["status"]["state"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .unwrap_or_default()
}

/// How many Raft members vote, as `legion status` lists them.
fn voters(lab: &Lab) -> usize {
    lab.ok(&["status"])
        .lines()
        .filter(|l| l.trim_end().ends_with("voter"))
        .count()
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD and two more nodes; see the module docs"]
fn a_worker_and_a_manager_change_roles_in_place() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("three nodes, two of them managers, a task on each");
    lab.three_nodes(&[]);
    let file = lab.dir.join("sleeper.yaml");
    std::fs::write(&file, app()).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
    wait("a task on every node", 600, || running(&lab).len() == 3);
    let tasks = running(&lab);
    assert_eq!(voters(&lab), 2);
    assert_eq!(node(&lab, "n2").0, "worker");

    step("a promotion asked of what is a manager already is refused");
    let out = lab.legion(&["node", "promote", "n3"], &[]);
    assert!(
        !out.status.success() && text(&out.stderr).contains("already"),
        "{}",
        text(&out.stderr)
    );

    step("the worker n2 promoted: a manager and a voter, its task running on");
    let started = std::time::Instant::now();
    lab.ok(&["node", "promote", "n2"]);
    wait("n2 a manager, ready", 300, || {
        node(&lab, "n2") == ("manager".into(), "ready".into())
    });
    wait("three voters", 300, || voters(&lab) == 3);
    eprintln!("promoted in {:.0} s", started.elapsed().as_secs_f64());
    assert_eq!(running(&lab), tasks, "the tasks ran on");

    step("the manager n3 demoted: a worker, out of the membership, its task running on");
    let started = std::time::Instant::now();
    lab.ok(&["node", "demote", "n3"]);
    wait("n3 a worker, ready", 300, || {
        node(&lab, "n3") == ("worker".into(), "ready".into())
    });
    wait("two voters", 300, || voters(&lab) == 2);
    eprintln!("demoted in {:.0} s", started.elapsed().as_secs_f64());
    assert_eq!(running(&lab), tasks, "the tasks ran on");

    step("the cluster serves, its new manager among those deciding");
    let scaled = app().replace("replicas: 3", "replicas: 4");
    std::fs::write(&file, scaled).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
    wait("a fourth task", 300, || running(&lab).len() == 4);
}
