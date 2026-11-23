//! High availability (docs/HA.md): what in a cluster would stop working, or
//! lose data, if one machine were lost. Worked out by any manager from its
//! replica and Raft's membership (`Cluster.Availability`); `legion ha`
//! prints it and the console's Availability page shows it.
//!
//! Each finding is a single point of failure, a `risk` (losing one machine
//! stops something, or loses data) or a `warning` (it carries on, but
//! slowly, by hand, or only for clients that do the right thing), with how
//! to put it right.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::node::{Availability, NodeSpec, NodeState, NodeStatus};
use crate::service::{Publish, ServiceMode, ServiceSpec};
use crate::task::{TaskSpec, TaskState, TaskStatus};
use crate::volume::{VolumeDriver, VolumeSpec};
use crate::{Object, parse};

/// A manager, as Raft's membership and the managers' own word have it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manager {
    pub name: String,
    /// It votes: it counts towards a quorum.
    pub voter: bool,
    /// A witness (`legion join --witness`): it votes, runs no tasks and
    /// never leads.
    #[serde(default)]
    pub witness: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Losing one machine stops something, or loses data.
    Risk,
    /// It carries on, but slowly, by hand, or only for clients that do the
    /// right thing.
    Warning,
}

/// A single point of failure, and how to put it right.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub severity: Severity,
    /// A stable word a tool can act on: `managers` (too few to lose one),
    /// `leaders` (only one manager may lead), `nodes` (one node runs every
    /// task), `one-replica`, `one-node` (a service's tasks all on one
    /// node), `published` (clients may reach it through one node only),
    /// `no-replica` (a Volume's data on one node), `manual-failover`.
    pub reason: String,
    /// What it is about, as `Kind namespace/name`; empty for the cluster.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub object: String,
    /// The object's namespace, for who may read it; empty for the cluster.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    pub message: String,
    pub fix: String,
}

/// What `Cluster.Availability` answers.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    /// Risks first, the cluster's before its objects'.
    #[serde(default)]
    pub findings: Vec<Finding>,
    /// The managers, when the caller may read nodes.
    #[serde(default)]
    pub managers: Vec<Manager>,
    /// Nodes ready and active that may run tasks, when the caller may read
    /// nodes.
    #[serde(default)]
    pub nodes: u32,
    /// Findings left out, in namespaces the caller may not read.
    #[serde(default)]
    pub hidden: u32,
}

/// What a cluster is made of, as the check reads it.
pub struct Cluster<'a> {
    pub managers: &'a [Manager],
    pub nodes: &'a [Object],
    pub services: &'a [Object],
    pub tasks: &'a [Object],
    pub volumes: &'a [Object],
}

/// The namespace Legion keeps its own objects in, which the check leaves
/// alone.
const SYSTEM: &str = "legion-system";

/// The nodes that may run tasks now: ready, active, and lacking nothing
/// the host needs.
pub fn task_nodes(nodes: &[Object]) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| {
            let (Ok(status), Ok(spec)) = (
                serde_json::from_value::<NodeStatus>(n.status.clone()),
                parse::<NodeSpec>(&n.spec),
            ) else {
                return false;
            };
            status.state == NodeState::Ready
                && spec.availability == Availability::Active
                && status.host_blocked(&spec).is_none()
        })
        .map(|n| n.metadata.name.clone())
        .collect()
}

/// Every single point of failure in `c`, risks first.
pub fn check(c: &Cluster) -> Vec<Finding> {
    let mut out = Vec::new();
    let cluster = |severity, reason: &str, message: String, fix: &str| Finding {
        severity,
        reason: reason.into(),
        object: String::new(),
        namespace: String::new(),
        message,
        fix: fix.into(),
    };

    // The managers: a quorum survives losing a minority.
    let voters = c.managers.iter().filter(|m| m.voter).count();
    let leaders = c.managers.iter().filter(|m| m.voter && !m.witness).count();
    match voters {
        0 => {}
        1 => out.push(cluster(
            Severity::Risk,
            "managers",
            "One manager: if it is lost, nothing is placed, moved or changed until it is back \
             (running tasks carry on)"
                .into(),
            "Add two managers, or a manager and a witness (legion join --witness), for three",
        )),
        2 => out.push(cluster(
            Severity::Risk,
            "managers",
            "Two managers: losing either leaves no quorum, as one of two is not a majority".into(),
            "Add a third: a manager, or a witness on any small machine (legion join --witness)",
        )),
        n if n % 2 == 0 => out.push(cluster(
            Severity::Warning,
            "managers",
            format!(
                "{n} managers survive losing {} of them, as {} would",
                n / 2 - 1,
                n - 1
            ),
            "Add or remove one: an odd number makes the most of them",
        )),
        _ => {}
    }
    if voters >= 3 && leaders == 1 {
        out.push(cluster(
            Severity::Risk,
            "leaders",
            "Only one manager may lead, the others being witnesses: if it is lost, no leader is \
             chosen"
                .into(),
            "Join another full manager (legion join with a manager token, no --witness)",
        ));
    }

    // The nodes that run tasks.
    let runners = task_nodes(c.nodes);
    if runners.len() < 2 && !c.nodes.is_empty() {
        out.push(cluster(
            Severity::Risk,
            "nodes",
            match runners.first() {
                Some(n) => format!("Only node {n} runs tasks: if it is lost, every workload stops"),
                None => "No node is ready to run tasks".into(),
            },
            "Join another worker, or make a drained or paused node active",
        ));
    }

    // Each service.
    let mut running: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for t in c.tasks {
        let (Ok(spec), Ok(status)) = (
            parse::<TaskSpec>(&t.spec),
            serde_json::from_value::<TaskStatus>(t.status.clone()),
        ) else {
            continue;
        };
        if !spec.job && status.state == TaskState::Running && !spec.node.is_empty() {
            running
                .entry((t.metadata.namespace.clone(), spec.service))
                .or_default()
                .push(spec.node);
        }
    }
    for s in c.services {
        let (ns, name) = (&s.metadata.namespace, &s.metadata.name);
        if ns == SYSTEM {
            continue;
        }
        let Ok(spec) = parse::<ServiceSpec>(&s.spec) else {
            continue;
        };
        let wanted = match &spec.autoscale {
            Some(a) => a.min,
            None => spec.replicas.unwrap_or(1),
        };
        if spec.stopped || spec.paused || wanted == 0 || spec.mode == ServiceMode::Global {
            continue;
        }
        let object = format!("Service {ns}/{name}");
        let finding = |severity, reason: &str, message: String, fix: String| Finding {
            severity,
            reason: reason.into(),
            object: object.clone(),
            namespace: ns.clone(),
            message,
            fix,
        };
        // The Volumes its pod mounts as they are (not a stateful claim,
        // each replica's own): a second replica would share their data.
        let shared: Vec<&str> = spec
            .pod
            .volumes
            .iter()
            .filter_map(|v| v.volume.as_ref().map(|s| s.name.as_str()))
            .collect();
        if wanted == 1 && !shared.is_empty() {
            out.push(finding(
                Severity::Risk,
                "one-replica",
                format!(
                    "One replica, its data on Volume {}: if its node is lost, it is down until \
                     the Volume is back or taken over by its replica",
                    shared.join(", ")
                ),
                "Keep it at 1: a second replica would share the Volume's data, which a \
                 database can't survive. Protect the Volume (a replica failing over on its own), \
                 and for no loss at all run the application's own replication, a second Service \
                 with a Volume of its own"
                    .into(),
            ));
        } else if wanted == 1 {
            out.push(finding(
                Severity::Risk,
                "one-replica",
                "One replica: if its node is lost, it is down until it starts on another \
                 (16 s at least, longer if the image must be pulled or its data is on that node)"
                    .into(),
                if spec.mode == ServiceMode::Stateful {
                    "A database: run a replica of it beside it with its own replication, the two \
                     on different nodes"
                        .into()
                } else {
                    "replicas: 2 or more, if it can run more than one copy".into()
                },
            ));
        } else if runners.len() >= 2 {
            let on: BTreeSet<&String> = running
                .get(&(ns.clone(), name.clone()))
                .into_iter()
                .flatten()
                .collect();
            if on.len() == 1 {
                let node = on.into_iter().next().unwrap();
                out.push(finding(
                    Severity::Warning,
                    "one-node",
                    format!("All its tasks run on node {node}: if it is lost, so are they"),
                    format!(
                        "placement.antiAffinity: [{{service: {name}}}] keeps them on different \
                         nodes"
                    ),
                ));
            }
        }
        if let Some(endpoint) = &spec.endpoint
            && endpoint.load_balancer.is_none()
            && runners.len() >= 2
            && let Some(port) = endpoint
                .ports
                .iter()
                .find(|p| p.publish != Publish::default() || p.published_port.is_some())
        {
            let published = port.published_port.unwrap_or(port.port);
            out.push(finding(
                Severity::Warning,
                "published",
                format!(
                    "Published on port {published} of every node: clients that use one node's \
                     address lose it with that node"
                ),
                "A load balancer address (endpoint.loadBalancer), which CARP moves to another \
                 node, or every node's address in its DNS name"
                    .into(),
            ));
        }
    }

    // Each Volume whose data is on one node.
    for v in c.volumes {
        let (ns, name) = (&v.metadata.namespace, &v.metadata.name);
        let Ok(spec) = parse::<VolumeSpec>(&v.spec) else {
            continue;
        };
        if spec.driver != VolumeDriver::Zfs || ns == SYSTEM {
            continue;
        }
        let node = v.status["node"]
            .as_str()
            .map(str::to_owned)
            .or(spec.node.clone())
            .unwrap_or_default();
        let on = if node.is_empty() {
            "one node".to_owned()
        } else {
            format!("node {node}")
        };
        let finding = |severity, reason: &str, message: String, fix: &str| Finding {
            severity,
            reason: reason.into(),
            object: format!("Volume {ns}/{name}"),
            namespace: ns.clone(),
            message,
            fix: fix.into(),
        };
        match &spec.replica {
            None if spec.backup.is_some() => out.push(finding(
                Severity::Warning,
                "no-replica",
                format!(
                    "Its data is on {on}, backed up but with no replica: if the node is lost, it \
                     is restored by hand, losing what changed since the last backup"
                ),
                "A replica on another node (replica: {everySeconds: 30, failover: automatic})",
            )),
            None => out.push(finding(
                Severity::Risk,
                "no-replica",
                format!(
                    "Its data is on {on} alone: if the node is lost, so is the data, and what uses \
                     it waits"
                ),
                "A replica on another node (replica: {everySeconds: 30, failover: automatic}), \
                 or backups to a target (backup:)",
            )),
            Some(r) if r.failover.is_manual() => out.push(finding(
                Severity::Warning,
                "manual-failover",
                "Its replica takes over only by hand (legion volume promote) if its node is lost"
                    .into(),
                "failover: automatic, its tasks onPartition: stop",
            )),
            Some(_) => {}
        }
    }

    // Risks first; the cluster's before its objects'; then by object.
    out.sort_by(|a, b| {
        (a.severity, !a.object.is_empty(), &a.object).cmp(&(
            b.severity,
            !b.object.is_empty(),
            &b.object,
        ))
    });
    out
}

/// A report as `legion ha` prints it.
pub fn render(r: &Report) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    if !r.managers.is_empty() {
        let voters = r.managers.iter().filter(|m| m.voter).count();
        let witnesses = r.managers.iter().filter(|m| m.witness).count();
        let _ = write!(
            out,
            "{voters} voting manager{}",
            if voters == 1 { "" } else { "s" }
        );
        if witnesses > 0 {
            let _ = write!(
                out,
                " ({witnesses} witness{})",
                if witnesses == 1 { "" } else { "es" }
            );
        }
        let _ = writeln!(
            out,
            ", {} node{} running tasks",
            r.nodes,
            if r.nodes == 1 { "" } else { "s" }
        );
    }
    if r.findings.is_empty() {
        let _ = writeln!(out, "No single point of failure found.");
    }
    for f in &r.findings {
        let what = if f.object.is_empty() {
            "cluster"
        } else {
            &f.object
        };
        let severity = match f.severity {
            Severity::Risk => "RISK",
            Severity::Warning => "WARNING",
        };
        let _ = writeln!(out, "{severity:<8} {what}: {}", f.message);
        let _ = writeln!(out, "         fix: {}", f.fix);
    }
    if r.hidden > 0 {
        let _ = writeln!(out, "{} more in namespaces you may not read", r.hidden);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kind;

    fn obj(
        kind: Kind,
        ns: &str,
        name: &str,
        spec: serde_json::Value,
        status: serde_json::Value,
    ) -> Object {
        let mut o = Object::new(kind, ns, name, spec);
        o.status = status;
        o
    }

    fn node(name: &str, state: &str) -> Object {
        obj(
            Kind::Node,
            "",
            name,
            serde_json::json!({}),
            serde_json::json!({ "state": state }),
        )
    }

    fn manager(name: &str, witness: bool) -> Manager {
        Manager {
            name: name.into(),
            voter: true,
            witness,
        }
    }

    fn reasons(f: &[Finding]) -> Vec<(&str, &str)> {
        f.iter()
            .map(|f| (f.reason.as_str(), f.object.as_str()))
            .collect()
    }

    #[test]
    fn managers_are_counted_for_a_quorum_and_a_leader() {
        let check_managers = |m: &[Manager]| {
            check(&Cluster {
                managers: m,
                nodes: &[node("n1", "ready"), node("n2", "ready")],
                services: &[],
                tasks: &[],
                volumes: &[],
            })
        };
        let one = check_managers(&[manager("m1", false)]);
        assert_eq!(reasons(&one), [("managers", "")]);
        assert_eq!(one[0].severity, Severity::Risk);
        assert_eq!(
            reasons(&check_managers(&[
                manager("m1", false),
                manager("m2", false)
            ])),
            [("managers", "")]
        );
        // Two managers and a witness: a quorum, two that may lead.
        assert!(
            check_managers(&[
                manager("m1", false),
                manager("m2", false),
                manager("w", true)
            ])
            .is_empty()
        );
        // One manager and two witnesses: a quorum, but one leader.
        assert_eq!(
            reasons(&check_managers(&[
                manager("m1", false),
                manager("w1", true),
                manager("w2", true)
            ])),
            [("leaders", "")]
        );
        let four = check_managers(&[
            manager("m1", false),
            manager("m2", false),
            manager("m3", false),
            manager("m4", false),
        ]);
        assert_eq!(four[0].severity, Severity::Warning);
        assert!(
            four[0].message.contains("survive losing 1"),
            "{}",
            four[0].message
        );
    }

    #[test]
    fn services_and_volumes_on_one_node_are_found() {
        let managers = [
            manager("m1", false),
            manager("m2", false),
            manager("m3", false),
        ];
        let svc = |name: &str, spec: serde_json::Value| {
            obj(Kind::Service, "shop", name, spec, serde_json::json!({}))
        };
        let pod = serde_json::json!({ "containers": [{ "name": "c", "image": "web:1" }] });
        let with_data = serde_json::json!({
            "containers": [{ "name": "c", "image": "pg:17" }],
            "volumes": [{ "name": "data", "volume": { "name": "alone" } }],
        });
        let services = [
            svc("db", serde_json::json!({ "replicas": 1, "pod": with_data })),
            svc("one", serde_json::json!({ "replicas": 1, "pod": pod })),
            svc("two", serde_json::json!({ "replicas": 2, "pod": pod })),
            svc("spread", serde_json::json!({ "replicas": 2, "pod": pod })),
            svc(
                "off",
                serde_json::json!({ "replicas": 1, "stopped": true, "pod": pod }),
            ),
            svc("every", serde_json::json!({ "mode": "global", "pod": pod })),
            svc(
                "front",
                serde_json::json!({ "replicas": 2, "pod": pod, "endpoint": {
                    "ports": [{ "port": 80, "publishedPort": 8080 }],
                } }),
            ),
            svc(
                "held",
                serde_json::json!({ "replicas": 2, "pod": pod, "endpoint": {
                    "ports": [{ "port": 80, "publishedPort": 8081 }],
                    "loadBalancer": { "address": "192.0.2.10", "vhid": 1 },
                } }),
            ),
        ];
        let task = |service: &str, n: u32, node: &str| {
            obj(
                Kind::Task,
                "shop",
                &format!("{service}-{n}-1"),
                serde_json::json!({
                    "service": service, "serviceUid": "u", "serviceGeneration": 1,
                    "node": node, "pod": pod,
                }),
                serde_json::json!({ "state": "running" }),
            )
        };
        let tasks = [
            task("two", 1, "n1"),
            task("two", 2, "n1"),
            task("spread", 1, "n1"),
            task("spread", 2, "n2"),
            task("front", 1, "n1"),
            task("front", 2, "n2"),
            task("held", 1, "n1"),
            task("held", 2, "n2"),
        ];
        let vol = |name: &str, spec: serde_json::Value| {
            obj(
                Kind::Volume,
                "shop",
                name,
                spec,
                serde_json::json!({ "node": "n1" }),
            )
        };
        let volumes = [
            vol("alone", serde_json::json!({ "size": "1Gi" })),
            vol(
                "backed",
                serde_json::json!({ "size": "1Gi", "backup": { "target": "t", "every": "1d", "keep": 7 } }),
            ),
            vol(
                "by-hand",
                serde_json::json!({ "size": "1Gi", "replica": {} }),
            ),
            vol(
                "safe",
                serde_json::json!({ "size": "1Gi", "replica": { "failover": "automatic" } }),
            ),
            vol(
                "shared",
                serde_json::json!({ "driver": "nfs", "nfs": { "server": "s", "path": "/p" } }),
            ),
        ];
        let found = check(&Cluster {
            managers: &managers,
            nodes: &[node("n1", "ready"), node("n2", "ready")],
            services: &services,
            tasks: &tasks,
            volumes: &volumes,
        });
        assert_eq!(
            reasons(&found),
            [
                ("one-replica", "Service shop/db"),
                ("one-replica", "Service shop/one"),
                ("no-replica", "Volume shop/alone"),
                ("published", "Service shop/front"),
                ("one-node", "Service shop/two"),
                ("no-replica", "Volume shop/backed"),
                ("manual-failover", "Volume shop/by-hand"),
            ]
        );
        // Its data on a Volume: told to keep one, never to add replicas.
        assert!(found[0].fix.starts_with("Keep it at 1"), "{}", found[0].fix);
        assert!(
            found[0].message.contains("Volume alone"),
            "{}",
            found[0].message
        );
        assert!(found[1].fix.starts_with("replicas: 2"), "{}", found[1].fix);
        assert!(
            found[3].message.contains("port 8080"),
            "{}",
            found[3].message
        );
        assert!(found[4].message.contains("node n1"), "{}", found[4].message);
        assert!(
            found[4].fix.contains("antiAffinity: [{service: two}]"),
            "{}",
            found[4].fix
        );
    }

    #[test]
    fn one_node_running_tasks_is_a_risk_and_says_so() {
        let found = check(&Cluster {
            managers: &[
                manager("m1", false),
                manager("m2", false),
                manager("m3", false),
            ],
            nodes: &[node("n1", "ready"), node("n2", "down")],
            services: &[],
            tasks: &[],
            volumes: &[],
        });
        assert_eq!(reasons(&found), [("nodes", "")]);
        assert!(found[0].message.contains("node n1"), "{}", found[0].message);
        let r = Report {
            findings: found,
            managers: vec![
                manager("m1", false),
                manager("m2", false),
                manager("w", true),
            ],
            nodes: 1,
            hidden: 2,
        };
        let text = render(&r);
        assert!(
            text.starts_with("3 voting managers (1 witness), 1 node running tasks\n"),
            "{text}"
        );
        assert!(
            text.contains("RISK     cluster: Only node n1 runs tasks"),
            "{text}"
        );
        assert!(
            text.contains("2 more in namespaces you may not read"),
            "{text}"
        );
    }
}
