//! Alerts: what an operator acts on, worked out by a manager from the
//! cluster's objects as its replica holds them, the restarts it has seen
//! and its own view of Raft (`Cluster.WatchAlerts`, `legion alerts`, the
//! console's alerts). Nothing here is stored: each manager works its
//! alerts out the same way, so any of them answers, a follower cut off from
//! the leader too, which is when the quorum alert matters.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::node::NodeState;

/// Restarts within [`CRASH_LOOP_WINDOW`] that make a task crash-looping.
pub const CRASH_LOOP_RESTARTS: u32 = 3;
/// How far back restarts count, and how long a task stays crash-looping
/// after its last.
pub const CRASH_LOOP_WINDOW: Duration = Duration::from_secs(300);
/// A CA expiring sooner than this is a warning; sooner than
/// [`CA_CRITICAL`], critical.
pub const CA_WARNING: Duration = Duration::from_secs(30 * 24 * 3600);
pub const CA_CRITICAL: Duration = Duration::from_secs(7 * 24 * 3600);
/// A leader that has not heard from a quorum for longer than this.
pub const QUORUM_SILENCE: Duration = Duration::from_secs(10);
/// ZFS volume utilization percentage (`used / (used + avail)`) that raises a
/// warning alert.
pub const VOLUME_NEARLY_FULL_PERCENT: u64 = 90;
/// A pool this full raises a warning; [`POOL_FULL_CRITICAL`], critical.
pub const POOL_FULL_WARNING: u64 = 80;
pub const POOL_FULL_CRITICAL: u64 = 90;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Critical,
    Warning,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::Warning => "warning",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AlertKind {
    NoLeader,
    NoQuorum,
    NodeDown,
    NodeSuspect,
    CrashLoop,
    VolumeLost,
    VolumeNearlyFull,
    ReplicaStale,
    CaExpiring,
    PoolDegraded,
    PoolErrors,
    PoolNearlyFull,
    ScrubOverdue,
    VersionSkew,
}

impl AlertKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertKind::NoLeader => "no-leader",
            AlertKind::NoQuorum => "no-quorum",
            AlertKind::NodeDown => "node-down",
            AlertKind::NodeSuspect => "node-suspect",
            AlertKind::CrashLoop => "crash-loop",
            AlertKind::VolumeLost => "volume-lost",
            AlertKind::VolumeNearlyFull => "volume-nearly-full",
            AlertKind::ReplicaStale => "replica-stale",
            AlertKind::CaExpiring => "ca-expiring",
            AlertKind::PoolDegraded => "pool-degraded",
            AlertKind::PoolErrors => "pool-errors",
            AlertKind::PoolNearlyFull => "pool-nearly-full",
            AlertKind::ScrubOverdue => "scrub-overdue",
            AlertKind::VersionSkew => "version-skew",
        }
    }
}

/// One thing to act on. `object` names what it is about as `Kind/name`
/// (in `namespace` when the kind has one), empty for the cluster itself:
/// the way to the cause.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alert {
    pub kind: AlertKind,
    pub severity: Severity,
    pub namespace: String,
    pub object: String,
    pub message: String,
}

impl Alert {
    /// What makes it the same alert from one look to the next.
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.kind.as_str(), self.namespace, self.object)
    }
}

/// A task as alerts look at it.
pub struct TaskInput<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    /// Its restarts seen within [`CRASH_LOOP_WINDOW`].
    pub recent_restarts: u32,
    /// Why it is as it is, from its status.
    pub message: &'a str,
}

/// A Volume as alerts look at it: `lost` names the node it was lost with.
#[derive(Default)]
pub struct VolumeInput<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub lost: &'a str,
    pub used_bytes: u64,
    pub avail_bytes: u64,
    /// Its replica, when the last copy there is stale
    /// (`ReplicaStatus::stale_at`).
    pub stale_replica: Option<StaleReplica>,
}

/// A Volume's replica whose last copy is too old.
#[derive(Default)]
pub struct StaleReplica {
    pub node: String,
    /// Since the last copy was taken.
    pub age: Duration,
    /// How often it is meant to be copied.
    pub every: Duration,
    /// Why the last attempt failed, if it did.
    pub why: String,
}

/// A node's pool as alerts look at it.
pub struct PoolInput<'a> {
    pub node: &'a str,
    pub pool: &'a crate::node::Pool,
    /// Its errors taken as seen (`NodeSpec::pool_errors_seen`).
    pub errors_seen: u64,
    /// How long it may go unscrubbed (`NodeSpec::scrub_due`).
    pub scrub_due: Duration,
    /// Whether the node schedules scrubs: one never scrubbed is overdue
    /// only then, a pool's age being unknown.
    pub scheduled: bool,
}

/// What a manager knows that alerts are worked out from.
#[derive(Default)]
pub struct Inputs<'a> {
    pub nodes: Vec<(&'a str, NodeState)>,
    pub tasks: Vec<TaskInput<'a>>,
    pub volumes: Vec<VolumeInput<'a>>,
    /// How long the cluster's CA has left, if this manager holds it.
    pub ca_left: Option<Duration>,
    /// Whether this manager knows of a leader.
    pub leader: bool,
    /// As leader: since it last heard from a quorum.
    pub quorum_silence: Option<Duration>,
    /// The nodes' pools, from nodes that are not down.
    pub pools: Vec<PoolInput<'a>>,
    /// Each node's build (`NodeStatus::version`), of the nodes not down.
    pub versions: Vec<(&'a str, &'a str)>,
    /// Now, in milliseconds since the epoch.
    pub now_ms: i64,
}

/// The alerts `i` raises, most severe first, then by kind and object.
pub fn alerts(i: &Inputs) -> Vec<Alert> {
    let mut out = Vec::new();
    let cluster = |kind, severity, message: String| Alert {
        kind,
        severity,
        namespace: String::new(),
        object: String::new(),
        message,
    };
    if !i.leader {
        out.push(cluster(
            AlertKind::NoLeader,
            Severity::Critical,
            "no manager leads: the cluster takes no changes until a majority of managers \
             reach each other"
                .into(),
        ));
    }
    if let Some(s) = i.quorum_silence.filter(|s| *s > QUORUM_SILENCE) {
        out.push(cluster(
            AlertKind::NoQuorum,
            Severity::Critical,
            format!(
                "the leader has not heard from a majority of managers for {} s",
                s.as_secs()
            ),
        ));
    }
    for (name, state) in &i.nodes {
        let (kind, severity, message) = match state {
            NodeState::Down => (
                AlertKind::NodeDown,
                Severity::Critical,
                format!("node {name} is down: its tasks are being placed elsewhere"),
            ),
            NodeState::Suspect => (
                AlertKind::NodeSuspect,
                Severity::Warning,
                format!("node {name} has missed its heartbeats: it gets no new tasks"),
            ),
            _ => continue,
        };
        out.push(Alert {
            kind,
            severity,
            namespace: String::new(),
            object: format!("Node/{name}"),
            message,
        });
    }
    for t in &i.tasks {
        if t.recent_restarts < CRASH_LOOP_RESTARTS {
            continue;
        }
        let why = if t.message.is_empty() {
            String::new()
        } else {
            format!(": {}", t.message)
        };
        out.push(Alert {
            kind: AlertKind::CrashLoop,
            severity: Severity::Critical,
            namespace: t.namespace.to_owned(),
            object: format!("Task/{}", t.name),
            message: format!(
                "task {} restarted {} times in {} min{why}",
                t.name,
                t.recent_restarts,
                CRASH_LOOP_WINDOW.as_secs() / 60
            ),
        });
    }
    for v in &i.volumes {
        if !v.lost.is_empty() {
            out.push(Alert {
                kind: AlertKind::VolumeLost,
                severity: Severity::Critical,
                namespace: v.namespace.to_owned(),
                object: format!("Volume/{}", v.name),
                message: format!(
                    "Volume {} was lost with node {}: restore a backup into it on another node",
                    v.name, v.lost
                ),
            });
            continue;
        }
        if let Some(r) = &v.stale_replica {
            let why = if r.why.is_empty() {
                String::new()
            } else {
                format!(": {}", r.why)
            };
            out.push(Alert {
                kind: AlertKind::ReplicaStale,
                severity: Severity::Warning,
                namespace: v.namespace.to_owned(),
                object: format!("Volume/{}", v.name),
                message: format!(
                    "Volume {}'s replica on {} was last copied {} ago, meant to be every {}{why}",
                    v.name,
                    r.node,
                    crate::clock::span(r.age),
                    crate::clock::span(r.every),
                ),
            });
        }
        let cap = v.used_bytes.saturating_add(v.avail_bytes);
        if cap > 0
            && (u128::from(v.used_bytes) * 100)
                >= (u128::from(cap) * u128::from(VOLUME_NEARLY_FULL_PERCENT))
        {
            let pct = (u128::from(v.used_bytes) * 100) / u128::from(cap);
            out.push(Alert {
                kind: AlertKind::VolumeNearlyFull,
                severity: Severity::Warning,
                namespace: v.namespace.to_owned(),
                object: format!("Volume/{}", v.name),
                message: format!(
                    "Volume {} is {pct}% full ({} MiB free): raise its quota or prune data",
                    v.name,
                    v.avail_bytes >> 20
                ),
            });
        }
    }
    for p in &i.pools {
        let (node, pool) = (p.node, p.pool);
        let object = format!("Node/{node}");
        let on_node = |kind, severity, message: String| Alert {
            kind,
            severity,
            namespace: String::new(),
            object: object.clone(),
            message,
        };
        if !pool.healthy() {
            // Still serving but short of redundancy, or not serving at all.
            let severity = if pool.state == "DEGRADED" {
                Severity::Warning
            } else {
                Severity::Critical
            };
            out.push(on_node(
                AlertKind::PoolDegraded,
                severity,
                format!(
                    "pool {} on {node} is {}: see `zpool status {}` there, and bring back or \
                     replace the device it lacks",
                    pool.name, pool.state, pool.name
                ),
            ));
        }
        if pool.errors() > p.errors_seen {
            out.push(on_node(
                AlertKind::PoolErrors,
                Severity::Warning,
                format!(
                    "pool {} on {node} has had {} read, {} write and {} checksum errors{}: see \
                     `zpool status -v {}` there; once dealt with, take them as seen \
                     (legion node errors-seen {node})",
                    pool.name,
                    pool.read_errors,
                    pool.write_errors,
                    pool.checksum_errors,
                    if p.errors_seen > 0 {
                        format!(", {} more than taken as seen", pool.errors() - p.errors_seen)
                    } else {
                        String::new()
                    },
                    pool.name
                ),
            ));
        }
        let pct = pool.percent_used();
        if pct >= POOL_FULL_WARNING {
            let severity = if pct >= POOL_FULL_CRITICAL {
                Severity::Critical
            } else {
                Severity::Warning
            };
            out.push(on_node(
                AlertKind::PoolNearlyFull,
                severity,
                format!(
                    "pool {} on {node} is {pct}% full ({} GiB free): ZFS slows as a pool fills; \
                     prune snapshots and images (legion disk) or move Volumes off",
                    pool.name,
                    pool.size_bytes.saturating_sub(pool.alloc_bytes) >> 30
                ),
            ));
        }
        if !pool.scanning() {
            let due = p.scrub_due.as_millis() as i64;
            let late = match pool.scrubbed_ms() {
                Some(at) => (i.now_ms - at > due).then(|| {
                    format!(
                        "last scrubbed {} ago",
                        crate::clock::span(Duration::from_millis((i.now_ms - at).max(0) as u64))
                    )
                }),
                None => p.scheduled.then(|| "never scrubbed".to_owned()),
            };
            if let Some(late) = late {
                out.push(on_node(
                    AlertKind::ScrubOverdue,
                    Severity::Warning,
                    format!(
                        "pool {} on {node} was {late}, meant to be within {}: a scrub finds bad \
                         blocks while the copies to repair them are there (zpool scrub {})",
                        pool.name,
                        crate::clock::span(p.scrub_due),
                        pool.name
                    ),
                ));
            }
        }
    }
    // Builds told apart by their version: those of the nodes up, unless
    // one says none (an agent from before it did).
    let mut builds: Vec<&str> = i
        .versions
        .iter()
        .map(|(_, v)| *v)
        .filter(|v| !v.is_empty())
        .collect();
    builds.sort_unstable();
    builds.dedup();
    if builds.len() > 1 {
        let mut by: Vec<String> = builds
            .iter()
            .map(|b| {
                let mut on: Vec<&str> = i
                    .versions
                    .iter()
                    .filter(|(_, v)| v == b)
                    .map(|(n, _)| *n)
                    .collect();
                on.sort_unstable();
                format!("{b} on {}", on.join(", "))
            })
            .collect();
        by.sort();
        out.push(cluster(
            AlertKind::VersionSkew,
            Severity::Warning,
            format!(
                "the nodes run different builds ({}): finish the upgrade, as mixed builds are \
                 meant only while it runs",
                by.join("; ")
            ),
        ));
    }
    if let Some(left) = i.ca_left.filter(|l| *l < CA_WARNING) {
        let severity = if left < CA_CRITICAL {
            Severity::Critical
        } else {
            Severity::Warning
        };
        out.push(cluster(
            AlertKind::CaExpiring,
            severity,
            format!(
                "the cluster's CA expires in {} days: rotate it (legion ca rotate)",
                left.as_secs() / 86400
            ),
        ));
    }
    out.sort_by(|a, b| {
        (a.severity, a.kind, &a.namespace, &a.object).cmp(&(
            b.severity,
            b.kind,
            &b.namespace,
            &b.object,
        ))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_healthy_cluster_raises_nothing() {
        let i = Inputs {
            nodes: vec![("n1", NodeState::Ready)],
            tasks: vec![TaskInput {
                namespace: "a",
                name: "web-1",
                recent_restarts: CRASH_LOOP_RESTARTS - 1,
                message: "",
            }],
            volumes: vec![VolumeInput {
                namespace: "a",
                name: "data",
                lost: "",
                used_bytes: 500 << 20,
                avail_bytes: 500 << 20,
                stale_replica: None,
            }],
            ca_left: Some(CA_WARNING * 2),
            leader: true,
            quorum_silence: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        assert_eq!(alerts(&i), []);
    }

    #[test]
    fn each_problem_raises_its_alert_most_severe_first() {
        let i = Inputs {
            nodes: vec![
                ("n1", NodeState::Ready),
                ("n2", NodeState::Suspect),
                ("n3", NodeState::Down),
            ],
            tasks: vec![TaskInput {
                namespace: "a",
                name: "web-1",
                recent_restarts: CRASH_LOOP_RESTARTS,
                message: "container c exited with 1; restarting in 1s",
            }],
            volumes: vec![
                VolumeInput {
                    namespace: "a",
                    name: "data",
                    lost: "n3",
                    ..Default::default()
                },
                VolumeInput {
                    namespace: "a",
                    name: "logs",
                    lost: "",
                    used_bytes: 950 << 20,
                    avail_bytes: 50 << 20,
                    stale_replica: None,
                },
            ],
            ca_left: Some(CA_CRITICAL * 2),
            leader: true,
            quorum_silence: Some(QUORUM_SILENCE * 2),
            ..Default::default()
        };
        let got: Vec<_> = alerts(&i)
            .iter()
            .map(|a| (a.severity.as_str(), a.kind.as_str(), a.object.clone()))
            .collect();
        assert_eq!(
            got,
            [
                ("critical", "no-quorum", String::new()),
                ("critical", "node-down", "Node/n3".into()),
                ("critical", "crash-loop", "Task/web-1".into()),
                ("critical", "volume-lost", "Volume/data".into()),
                ("warning", "node-suspect", "Node/n2".into()),
                ("warning", "volume-nearly-full", "Volume/logs".into()),
                ("warning", "ca-expiring", String::new()),
            ]
        );
        let crash = &alerts(&i)[2];
        assert_eq!(crash.namespace, "a");
        assert!(
            crash.message.contains("restarted 3 times"),
            "{}",
            crash.message
        );
        assert!(crash.message.contains("exited with 1"), "{}", crash.message);
    }

    fn pool(state: &str, alloc_pct: u64) -> crate::node::Pool {
        crate::node::Pool {
            name: "tank".into(),
            state: state.into(),
            size_bytes: 100 << 30,
            alloc_bytes: alloc_pct << 30,
            ..Default::default()
        }
    }

    fn with_pool(p: &crate::node::Pool, seen: u64, scheduled: bool, now_ms: i64) -> Vec<Alert> {
        alerts(&Inputs {
            leader: true,
            pools: vec![PoolInput {
                node: "n1",
                pool: p,
                errors_seen: seen,
                scrub_due: Duration::from_secs(35 * 86_400),
                scheduled,
            }],
            now_ms,
            ..Default::default()
        })
    }

    #[test]
    fn a_pool_short_of_a_device_is_degraded_on_its_node() {
        let a = with_pool(&pool("DEGRADED", 10), 0, false, 0);
        assert_eq!(a.len(), 1, "{a:?}");
        assert_eq!(a[0].kind, AlertKind::PoolDegraded);
        assert_eq!(a[0].severity, Severity::Warning);
        assert_eq!(a[0].object, "Node/n1");
        assert!(a[0].message.contains("tank on n1 is DEGRADED"), "{}", a[0].message);
        let a = with_pool(&pool("SUSPENDED", 10), 0, false, 0);
        assert_eq!(a[0].severity, Severity::Critical);
        assert_eq!(with_pool(&pool("ONLINE", 10), 0, false, 0), []);
    }

    #[test]
    fn pool_errors_count_past_those_taken_as_seen() {
        let mut p = pool("ONLINE", 10);
        p.checksum_errors = 3;
        let a = with_pool(&p, 0, false, 0);
        assert_eq!(a[0].kind, AlertKind::PoolErrors);
        assert!(a[0].message.contains("3 checksum errors"), "{}", a[0].message);
        assert_eq!(with_pool(&p, 3, false, 0), [], "taken as seen");
        p.read_errors = 1;
        let a = with_pool(&p, 3, false, 0);
        assert!(a[0].message.contains("1 more than taken as seen"), "{}", a[0].message);
    }

    #[test]
    fn a_filling_pool_warns_at_80_and_is_critical_at_90() {
        assert_eq!(with_pool(&pool("ONLINE", 79), 0, false, 0), []);
        let a = with_pool(&pool("ONLINE", 80), 0, false, 0);
        assert_eq!((a[0].kind, a[0].severity), (AlertKind::PoolNearlyFull, Severity::Warning));
        let a = with_pool(&pool("ONLINE", 90), 0, false, 0);
        assert_eq!(a[0].severity, Severity::Critical);
        assert!(a[0].message.contains("10 GiB free"), "{}", a[0].message);
    }

    #[test]
    fn a_scrub_is_overdue_past_its_interval_unless_one_runs() {
        let day = 86_400_000i64;
        let mut p = pool("ONLINE", 10);
        p.scan = Some(crate::node::PoolScan {
            function: "scrub".into(),
            state: "finished".into(),
            end_ms: Some(0),
            ..Default::default()
        });
        assert_eq!(with_pool(&p, 0, false, 35 * day), []);
        let a = with_pool(&p, 0, false, 36 * day);
        assert_eq!(a[0].kind, AlertKind::ScrubOverdue);
        assert!(a[0].message.contains("last scrubbed 36d ago"), "{}", a[0].message);
        p.scan.as_mut().unwrap().state = "scanning".into();
        assert_eq!(with_pool(&p, 0, false, 36 * day), [], "one runs now");
        // Never scrubbed: overdue only where scrubs are scheduled.
        p.scan = None;
        assert_eq!(with_pool(&p, 0, false, 36 * day), []);
        let a = with_pool(&p, 0, true, 36 * day);
        assert!(a[0].message.contains("never scrubbed"), "{}", a[0].message);
    }

    #[test]
    fn nodes_on_different_builds_are_a_skew() {
        let i = Inputs {
            leader: true,
            versions: vec![("n1", "0.2.0 (abc)"), ("n2", "0.2.0 (def)"), ("n3", "0.2.0 (abc)"), ("n4", "")],
            ..Default::default()
        };
        let a = alerts(&i);
        assert_eq!(a.len(), 1, "{a:?}");
        assert_eq!(a[0].kind, AlertKind::VersionSkew);
        assert!(
            a[0].message.contains("0.2.0 (abc) on n1, n3; 0.2.0 (def) on n2"),
            "{}",
            a[0].message
        );
        let same = Inputs {
            leader: true,
            versions: vec![("n1", "0.2.0 (abc)"), ("n2", "0.2.0 (abc)")],
            ..Default::default()
        };
        assert_eq!(alerts(&same), []);
    }

    #[test]
    fn a_manager_with_no_leader_says_so() {
        let got = alerts(&Inputs::default());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, AlertKind::NoLeader);
    }

    #[test]
    fn a_stale_replica_says_how_old_its_copy_is() {
        let i = Inputs {
            leader: true,
            volumes: vec![VolumeInput {
                namespace: "a",
                name: "pg-db-0",
                stale_replica: Some(StaleReplica {
                    node: "n2".into(),
                    age: Duration::from_secs(16 * 60),
                    every: Duration::from_secs(300),
                    why: "node n2: refused (attempt 3; the next in 40s)".into(),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let got = alerts(&i);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, AlertKind::ReplicaStale);
        assert_eq!(got[0].severity, Severity::Warning);
        assert_eq!(got[0].object, "Volume/pg-db-0");
        assert_eq!(
            got[0].message,
            "Volume pg-db-0's replica on n2 was last copied 16m ago, meant to be every 5m: \
             node n2: refused (attempt 3; the next in 40s)"
        );
    }
}
