//! CA rotation on three nodes (DESIGN.md §9): `legion ca rotate` on a
//! running cluster, with tasks on every node. Every manager and node moves
//! to the new CA by itself, the rotation ends, the tasks run on untouched,
//! and what the old CA issued is refused from then on.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root: this host is the manager n1, the
//! first VM the worker n2, the second the manager n3.

mod common;

use common::{Lab, Role, VM_DATA, text, wait};

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

/// `legion ca status`: the CA that issues, and each holder's line.
fn status(lab: &Lab) -> (String, String) {
    let out = lab.ok(&["ca", "status"]);
    let digest = out
        .lines()
        .find_map(|l| l.strip_prefix("CA: "))
        .unwrap_or_default()
        .trim()
        .to_owned();
    (digest, out)
}

/// The rotation has ended, and every manager and node is on the CA.
fn settled(out: &str) -> bool {
    out.contains("no rotation under way")
        && out
            .lines()
            .skip_while(|l| !l.starts_with("NAME"))
            .skip(1)
            .filter(|l| !l.trim().is_empty())
            .all(|l| l.trim_end().ends_with("current"))
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

#[test]
#[ignore = "needs root, ZFS on FreeBSD and two more nodes; see the module docs"]
fn the_ca_rotates_with_every_node_on_it() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("three nodes, a task on each, a user and the admin's credentials from before");
    lab.three_nodes(&[]);
    let file = lab.dir.join("sleeper.yaml");
    std::fs::write(&file, app()).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
    wait("a task on every node", 600, || {
        let r = running(&lab);
        let mut nodes: Vec<&str> = r.iter().map(|(_, n)| n.as_str()).collect();
        nodes.sort();
        nodes == ["n1", "n2", "n3"]
    });
    let tasks = running(&lab);
    let before_admin = lab.dir.join("admin-before.pem");
    std::fs::copy(lab.admin(), &before_admin).unwrap();
    let user = lab.dir.join("viewer-before.pem");
    lab.ok(&[
        "user",
        "create",
        "viewer",
        "--role",
        "view",
        "-n",
        "default",
        "-o",
        user.to_str().unwrap(),
    ]);
    lab.ok_as(&user, &["list", "services", "-n", "default"]);
    let (old, out) = status(&lab);
    assert!(settled(&out), "{out}");
    let old_token = lab.ok(&["join-token", "worker"]).trim().to_owned();
    assert!(old_token.contains(&old), "{old_token}");

    step("the CA rotated");
    let started = std::time::Instant::now();
    let out = lab.ok(&["ca", "rotate"]);
    eprintln!("{out}");
    let new = out
        .lines()
        .find_map(|l| l.strip_prefix("CA: "))
        .unwrap()
        .trim()
        .to_owned();
    assert_ne!(new, old);
    // A second rotation while one runs is refused.
    let again = lab.legion(&["ca", "rotate"], &[]);
    assert!(
        !again.status.success() && text(&again.stderr).contains("under way"),
        "{}",
        text(&again.stderr)
    );
    // What was issued before still works while the rotation runs.
    lab.ok_as(&before_admin, &["list", "nodes"]);
    let mut last = String::new();
    let ended = (0..60).any(|_| {
        let (digest, out) = status(&lab);
        last = out;
        if digest == new && settled(&last) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
        false
    });
    if !ended {
        lab.daemon_logs(&["CA", "renew", "certificate"], 40);
    }
    assert!(ended, "the rotation did not end in 5 minutes:\n{last}");
    eprintln!("ended in {:.0} s\n{last}", started.elapsed().as_secs_f64());

    step("the tasks ran on untouched");
    assert_eq!(running(&lab), tasks);

    step("what the old CA issued is refused; the new admin bundle works");
    // Each manager drops the old CA when it next reads the record, within
    // its period (5 s).
    for (who, identity) in [
        ("the admin's old bundle", &before_admin),
        ("the user", &user),
    ] {
        wait(&format!("{who} refused"), 30, || {
            !lab.legion_as(identity, &["list", "nodes"], &[]).status.success()
        });
    }
    lab.ok(&["list", "nodes"]);
    let fresh = lab.dir.join("viewer-after.pem");
    lab.ok(&[
        "user",
        "create",
        "viewer2",
        "--role",
        "view",
        "-n",
        "default",
        "-o",
        fresh.to_str().unwrap(),
    ]);
    lab.ok_as(&fresh, &["list", "services", "-n", "default"]);

    step("join tokens name the new CA; one from before is refused");
    let token = lab.ok(&["join-token", "worker"]).trim().to_owned();
    assert!(token.contains(&new), "{token}");
    assert_ne!(token, old_token);

    step("each node holds the new CA alone");
    for node in ["n2", "n3"] {
        let out = lab.on(node, &format!("cat {VM_DATA}/pki/ca.crt"));
        let pem = text(&out.stdout);
        let certs = legion_pki::split_certs(&pem);
        assert_eq!(certs.len(), 1, "{node}: {pem}");
        assert_eq!(legion_pki::digest(&certs[0]).unwrap(), new, "{node}");
    }

    step("a worker restarted comes back on the new CA");
    lab.restart_vm(0, Role::Worker);
    wait("n2 ready again", 180, || lab.node_state("n2") == "ready");
    let (_, out) = status(&lab);
    assert!(settled(&out), "{out}");
}
