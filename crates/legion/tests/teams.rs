//! The phase 3 exit test (PHASE.md): two teams share a cluster with
//! isolation enforced. Each team has its own user and namespace, with a
//! quota, and deploys on the same three nodes; neither can read or change
//! the other's objects, reach the other's tasks, or go over its quota.
//!
//! It drives real daemons, so it is ignored by default and run by hand on
//! a FreeBSD host as root, with the lab of `tests/common` (see there): this
//! host is the manager n1, the first machine joins as the worker n2 and
//! the second as the manager n3. The managers belong near each other: with
//! two, Raft needs both for every write and read, so the far node is the
//! worker.
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lteams \
//!     LEGION_CLUSTER_HOST=192.168.253.254 \
//!     LEGION_CLUSTER_VMS="vultr=grepjava@192.168.254.2 lvm3=192.168.253.3" \
//!     LEGION_CLUSTER_KEY=/var/tmp/lvm/id_lab \
//!     target/debug/deps/teams-<hash> --ignored --nocapture
//! ```
//!
//! Each team runs `web` (three replicas, one per node): inetd answering on
//! port 8080 with the team's name and the task's.

mod common;

use std::path::PathBuf;

use common::{Lab, Role, text, wait};
use serde_json::Value;

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";
const METRICS: &str = "127.0.0.1:7472";

/// A team: its user's credentials and its namespace.
struct Team {
    user: &'static str,
    namespace: &'static str,
    identity: PathBuf,
}

impl Team {
    fn new(lab: &Lab, user: &'static str, namespace: &'static str) -> Team {
        Team {
            user,
            namespace,
            identity: lab.dir.join(format!("{user}.pem")),
        }
    }
}

/// The team's app: the configs inetd needs, a secret and `web`, published
/// on `published` of every node.
fn app(namespace: &str, published: u16) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Config
metadata: {{ name: services, namespace: {namespace} }}
spec:
  text: "legion-name 8080/tcp\n"
---
apiVersion: legion/v1
kind: Config
metadata: {{ name: inetd, namespace: {namespace} }}
spec:
  text: "legion-name stream tcp nowait root /bin/sh sh /etc/legion/name.sh\n"
---
apiVersion: legion/v1
kind: Config
metadata: {{ name: name-sh, namespace: {namespace} }}
spec:
  text: "echo \"$TEAM $(hostname)\"\n"
---
apiVersion: legion/v1
kind: Secret
metadata: {{ name: token, namespace: {namespace} }}
spec:
  text: "{namespace}-only"
---
apiVersion: legion/v1
kind: Service
metadata: {{ name: web, namespace: {namespace} }}
spec:
  replicas: 3
  pod:
    containers:
      - name: web
        image: {IMAGE}
        command: [/usr/sbin/inetd, -d, /etc/inetd.conf]
        env: [{{ name: TEAM, value: {namespace} }}]
        ports: [{{ containerPort: 8080 }}]
        configs:
          - {{ name: services, target: /etc/services }}
          - {{ name: inetd, target: /etc/inetd.conf }}
          - {{ name: name-sh, target: /etc/legion/name.sh }}
        probes:
          readiness: {{ tcp: {{ port: 8080 }}, period: 2s }}
        resources: {{ limits: {{ memory: 64Mi, maxproc: 64 }} }}
  endpoint:
    ports: [{{ port: 8080, publish: ingress, publishedPort: {published} }}]
"#
    )
}

/// A Service of one task in `namespace`, with `extra` added to its
/// container and `endpoint` as its spec's endpoint.
fn one(namespace: &str, name: &str, extra: &str, endpoint: &str) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Service
metadata: {{ name: {name}, namespace: {namespace} }}
spec:
  replicas: 1
  pod:
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sleep, "3600"]
        {extra}
  {endpoint}
"#
    )
}

impl Lab {
    /// Applies `yaml` as the team's user.
    fn apply_as(&self, team: &Team, yaml: &str) -> std::process::Output {
        let file = self.dir.join(format!("{}-apply.yaml", team.user));
        std::fs::write(&file, yaml).unwrap();
        self.legion_as(
            &team.identity,
            &["apply", "-f", file.to_str().unwrap()],
            &[],
        )
    }

    /// What `legion list` gives the team's user, as JSON.
    fn list_as(&self, team: &Team, kind: &str, scope: &[&str]) -> Vec<Value> {
        let mut args = vec!["list", kind, "-o", "json"];
        args.extend(scope);
        let out = self.ok_as(&team.identity, &args);
        match serde_json::from_str(&out) {
            Ok(Value::Array(v)) => v,
            Ok(one @ Value::Object(_)) => vec![one],
            _ => Vec::new(),
        }
    }

    /// `web` in a namespace has three ready tasks, one per node.
    fn web_ready(&self, namespace: &str) -> bool {
        self.list("services", namespace)
            .iter()
            .any(|s| s["metadata"]["name"] == "web" && s["status"]["ready"] == 3)
            && self.placed(namespace, "web") == ["n1", "n2", "n3"]
    }

    /// Running tasks of a service: (name, address).
    fn tasks(&self, namespace: &str, service: &str) -> Vec<(String, String)> {
        self.list("tasks", namespace)
            .into_iter()
            .filter(|t| t["spec"]["service"] == service && t["status"]["state"] == "running")
            .map(|t| {
                (
                    t["metadata"]["name"].as_str().unwrap().to_owned(),
                    t["status"]["ip"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    }

    fn vip(&self, namespace: &str, service: &str) -> String {
        self.list("services", namespace)
            .iter()
            .find(|s| s["metadata"]["name"] == service)
            .and_then(|s| s["status"]["virtualIp"].as_str())
            .unwrap()
            .to_owned()
    }

    /// Which of `addrs` a task reaches on port 8080, asked from inside the
    /// task as its team's user: `addr open` or `addr closed` per line.
    fn reach_from(&self, team: &Team, task: &str, addrs: &[String]) -> Vec<String> {
        let script = format!(
            "for a in {}; do if nc -z -w 3 $a 8080 2>/dev/null; then echo $a open; \
             else echo $a closed; fi; done",
            addrs.join(" ")
        );
        let out = self.ok_as(
            &team.identity,
            &[
                "exec",
                task,
                "-n",
                team.namespace,
                "--",
                "/bin/sh",
                "-c",
                &script,
            ],
        );
        out.lines().map(str::to_owned).collect()
    }
}

/// `legion` failed, with this in its error.
fn refused(out: std::process::Output, what: &str) {
    let err = text(&out.stderr);
    assert!(
        !out.status.success() && err.contains(what),
        "expected a refusal mentioning {what:?}; got {:?}: {err}{}",
        out.status,
        text(&out.stdout)
    );
}

/// One line from a node's published port.
fn line_from(lab: &Lab, vm: usize, host: &str, port: u16) -> String {
    let out = lab.ssh(
        &lab.vms[vm],
        &format!("nc -N -w 5 {host} {port} </dev/null"),
    );
    text(&out.stdout).trim().to_owned()
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD and two more nodes; see the module docs"]
fn two_teams_share_a_cluster() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("two managers and a worker");
    lab.start_host(&["--metrics-listen", METRICS]);
    lab.ok(&["init"]);
    let manager = lab.ok(&["join-token", "manager"]).trim().to_owned();
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(0, &worker, Role::Worker);
    lab.join(1, &manager, Role::Manager);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });

    step("the cluster admin sets up two teams");
    let alpha = Team::new(&lab, "alice", "alpha");
    let beta = Team::new(&lab, "bob", "beta");
    for t in [&alpha, &beta] {
        let ns = lab.dir.join(format!("{}.yaml", t.namespace));
        std::fs::write(
            &ns,
            format!(
                "apiVersion: legion/v1\nkind: Namespace\nmetadata: {{ name: {} }}\n\
                 spec: {{ quota: {{ tasks: 5 }} }}\n",
                t.namespace
            ),
        )
        .unwrap();
        lab.ok(&["apply", "-f", ns.to_str().unwrap()]);
        lab.ok(&[
            "user",
            "create",
            t.user,
            "--role",
            "edit",
            "-n",
            t.namespace,
            "-o",
            t.identity.to_str().unwrap(),
        ]);
    }

    step("each team deploys as its own user");
    for (t, published) in [(&alpha, 30090), (&beta, 30091)] {
        let out = lab.apply_as(t, &app(t.namespace, published));
        assert!(out.status.success(), "{}", text(&out.stderr));
    }
    wait("both teams' web", 600, || {
        lab.web_ready("alpha") && lab.web_ready("beta")
    });

    step("neither reads or changes the other's objects");
    for (me, other) in [(&alpha, &beta), (&beta, &alpha)] {
        let id = &me.identity;
        let theirs = other.namespace;
        // Their own: all there.
        let own = lab.list_as(me, "services", &["-n", me.namespace]);
        assert_eq!(own.len(), 1, "{} sees its web", me.user);
        let secret = lab.ok_as(id, &["get", "secret", "token", "-n", me.namespace]);
        assert!(secret.contains("namespace"), "{secret}");
        // Across all namespaces, only their own.
        for kind in ["services", "tasks", "secrets", "configs"] {
            for o in lab.list_as(me, kind, &["-A"]) {
                assert_eq!(o["metadata"]["namespace"], me.namespace, "{kind}: {o}");
            }
        }
        let no_role = format!("no role in namespace {theirs}");
        for args in [
            vec!["list", "services", "-n", theirs],
            vec!["get", "service", "web", "-n", theirs],
            vec!["get", "secret", "token", "-n", theirs],
            vec!["delete", "service", "web", "-n", theirs],
            vec!["logs", "web", "-n", theirs],
            vec!["exec", "web", "-n", theirs, "--", "/bin/echo", "in"],
        ] {
            refused(lab.legion_as(id, &args, &[]), &no_role);
        }
        refused(lab.apply_as(me, &one(theirs, "intruder", "", "")), &no_role);
        // Nor the namespaces themselves: not even their own quota.
        refused(
            lab.apply_as(
                me,
                &format!(
                    "apiVersion: legion/v1\nkind: Namespace\nmetadata: {{ name: {} }}\n\
                     spec: {{ quota: {{ tasks: 50 }} }}\n",
                    me.namespace
                ),
            ),
            "Namespace",
        );
    }

    step("neither reaches the other's tasks, on any node");
    let alpha_tasks = lab.tasks("alpha", "web");
    let beta_tasks = lab.tasks("beta", "web");
    assert_eq!((alpha_tasks.len(), beta_tasks.len()), (3, 3));
    for (me, mine, theirs, other) in [
        (&alpha, &alpha_tasks, &beta_tasks, "beta"),
        (&beta, &beta_tasks, &alpha_tasks, "alpha"),
    ] {
        let own: Vec<String> = mine
            .iter()
            .map(|t| t.1.clone())
            .chain([lab.vip(me.namespace, "web")])
            .collect();
        let foreign: Vec<String> = theirs
            .iter()
            .map(|t| t.1.clone())
            .chain([lab.vip(other, "web")])
            .collect();
        let addrs: Vec<String> = own.iter().chain(&foreign).cloned().collect();
        for (task, _) in mine {
            let seen = lab.reach_from(me, task, &addrs);
            let want: Vec<String> = own
                .iter()
                .map(|a| format!("{a} open"))
                .chain(foreign.iter().map(|a| format!("{a} closed")))
                .collect();
            assert_eq!(seen, want, "from {}/{task}", me.namespace);
        }
    }

    step("published ports stay with the team that has them");
    // n3 asks the worker n2 for alpha's port and beta's.
    let n2 = lab.vms[0].addr.clone();
    assert!(line_from(&lab, 1, &n2, 30090).starts_with("alpha "));
    assert!(line_from(&lab, 1, &n2, 30091).starts_with("beta "));
    let grab = one(
        "beta",
        "grab",
        "ports: [{ containerPort: 8080 }]",
        "endpoint: { ports: [{ port: 8080, publish: ingress, publishedPort: 30090 }] }",
    );
    let out = lab.apply_as(&beta, &grab);
    assert!(out.status.success(), "{}", text(&out.stderr));
    wait("grab's conflict reported", 60, || {
        lab.list("services", "beta").iter().any(|s| {
            s["metadata"]["name"] == "grab"
                && s["status"]["conflicts"]
                    .as_array()
                    .is_some_and(|c| !c.is_empty())
        })
    });
    for _ in 0..5 {
        assert!(line_from(&lab, 1, &n2, 30090).starts_with("alpha "));
    }

    step("neither goes over its quota");
    // web takes three of alpha's five tasks: six replicas are refused.
    let six = app("alpha", 30090).replace("replicas: 3", "replicas: 6");
    refused(lab.apply_as(&alpha, &six), "quota");
    // A Job's tasks past the quota wait.
    let job = format!(
        "apiVersion: legion/v1\nkind: Job\nmetadata: {{ name: batch, namespace: alpha }}\n\
         spec:\n  completions: 3\n  parallelism: 3\n  pod:\n    containers:\n      \
         - {{ name: b, image: {IMAGE}, command: [/bin/sleep, \"3600\"] }}\n"
    );
    let out = lab.apply_as(&alpha, &job);
    assert!(out.status.success(), "{}", text(&out.stderr));
    wait("two of the Job's tasks placed, one held", 120, || {
        let tasks: Vec<Value> = lab
            .list("tasks", "alpha")
            .into_iter()
            .filter(|t| t["spec"]["service"] == "batch")
            .collect();
        let held = tasks.iter().filter(|t| {
            t["status"]["state"] == "pending"
                && t["status"]["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("quota"))
        });
        let placed = tasks.iter().filter(|t| t["status"]["state"] == "running");
        tasks.len() == 3 && held.count() == 1 && placed.count() == 2
    });
    // beta is not held back by alpha's use: grab runs as its fourth task.
    wait("grab running", 120, || {
        lab.placed("beta", "grab").len() == 1
    });
    // The leader's metrics show each quota beside its use.
    let (code, metrics) = metrics();
    assert_eq!(code, 200);
    for line in [
        "legion_quota_limit{namespace=\"alpha\",resource=\"tasks\"} 5",
        "legion_quota_used{namespace=\"alpha\",resource=\"tasks\"} 5",
        "legion_quota_used{namespace=\"beta\",resource=\"tasks\"} 4",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in:\n{metrics}");
    }
    // Nor past the jail defaults its namespace allows.
    refused(
        lab.apply_as(
            &alpha,
            &one("alpha", "pinger", "security: { allow: [rawSockets] }", ""),
        ),
        "does not allow",
    );

    step("clean up as the teams");
    for t in [&alpha, &beta] {
        let services = lab.list_as(t, "services", &["-n", t.namespace]);
        for s in services {
            let name = s["metadata"]["name"].as_str().unwrap().to_owned();
            lab.ok_as(
                &t.identity,
                &["delete", "service", &name, "-n", t.namespace],
            );
        }
    }
    lab.ok_as(&alpha.identity, &["delete", "job", "batch", "-n", "alpha"]);
    wait("every task gone", 180, || {
        ["alpha", "beta"].iter().all(|ns| {
            lab.list("tasks", ns)
                .iter()
                .all(|t| t["status"]["state"] != "running")
        })
    });
    step("done");
}

/// The host's metrics: status and body.
fn metrics() -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(METRICS).unwrap();
    write!(s, "GET /metrics HTTP/1.0\r\n\r\n").unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).unwrap();
    let code = resp.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = resp.split_once("\r\n\r\n").unwrap().1.to_owned();
    (code, body)
}
