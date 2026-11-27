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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Critical,
    #[default]
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
    SnapshotLate,
    BackupLate,
    CaExpiring,
    PoolDegraded,
    PoolErrors,
    PoolNearlyFull,
    ScrubOverdue,
    VersionSkew,
    DatabaseDegraded,
    DatabaseSplit,
    DatabaseStalled,
    ArchiveLate,
    ManagerSilent,
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
            AlertKind::SnapshotLate => "snapshot-late",
            AlertKind::BackupLate => "backup-late",
            AlertKind::CaExpiring => "ca-expiring",
            AlertKind::PoolDegraded => "pool-degraded",
            AlertKind::PoolErrors => "pool-errors",
            AlertKind::PoolNearlyFull => "pool-nearly-full",
            AlertKind::ScrubOverdue => "scrub-overdue",
            AlertKind::VersionSkew => "version-skew",
            AlertKind::DatabaseDegraded => "database-degraded",
            AlertKind::DatabaseSplit => "database-split",
            AlertKind::DatabaseStalled => "database-stalled",
            AlertKind::ArchiveLate => "archive-late",
            AlertKind::ManagerSilent => "manager-silent",
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
    /// Its snapshot schedules late by twice their interval.
    pub late_snapshots: Vec<LateSnapshots>,
    /// Its backups, when late (`backup-late`).
    pub late_backup: Option<LateBackup>,
}

/// A Volume's backups that are late: the last too old, or the first
/// failing.
#[derive(Default)]
pub struct LateBackup {
    pub target: String,
    /// Since the last was taken; none yet if `None`.
    pub age: Option<Duration>,
    pub every: Duration,
    pub why: String,
}

/// A Volume's snapshot schedule whose last snapshot is too old.
#[derive(Default)]
pub struct LateSnapshots {
    /// Since the last was taken.
    pub age: Duration,
    /// How often one is meant to be taken.
    pub every: Duration,
    /// Why the last attempt failed, if it did.
    pub why: String,
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

/// A replicated database (a Service's `status.replication`) as alerts
/// look at it.
pub struct DatabaseInput<'a> {
    pub namespace: &'a str,
    /// The Service.
    pub name: &'a str,
    pub phase: crate::replication::Phase,
    pub message: &'a str,
    /// How long a change of primary has been under way, if one is.
    pub change_for: Option<Duration>,
    /// With `replication.archive`: its runs failed in a row, and why the
    /// last did.
    pub archive_failures: u32,
    pub archive_message: &'a str,
}

/// Archive runs failing in a row past this raise `archive-late`: a run is
/// tried every 15 s, then after a backoff.
pub const ARCHIVE_FAILURES: u32 = 3;

/// A change of primary under way longer than this raises
/// `database-stalled`: one that goes well takes seconds to a minute.
pub const DATABASE_STALLED: Duration = Duration::from_secs(180);

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

/// A manager the leader has not heard from for [`MANAGER_SILENCE`] or more.
pub struct SilentManager {
    pub name: String,
    /// Since when, in milliseconds since the epoch.
    pub since_ms: i64,
    /// It votes: the quorum counts it.
    pub voter: bool,
}

/// How long a manager goes unheard by the leader before it is said to be
/// silent: well past a restart or a heartbeat lost.
pub const MANAGER_SILENCE: Duration = Duration::from_secs(30);

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
    /// The replicated databases.
    pub databases: Vec<DatabaseInput<'a>>,
    /// The managers the leader has not heard from lately, as it recorded
    /// them; and how many managers vote.
    pub silent_managers: Vec<SilentManager>,
    pub voters: usize,
    /// Now, in milliseconds since the epoch.
    pub now_ms: i64,
}

/// Each manager the leader has not heard from, and what the cluster can
/// still lose: one that votes and no longer answers leaves no other manager
/// to spare in a cluster of three, which used to raise nothing as long as
/// the other two answered (found 2026-11-27, a witness's VPS destroyed).
fn silent_managers(i: &Inputs) -> Vec<Alert> {
    let quorum = i.voters / 2 + 1;
    let silent_voters = i.silent_managers.iter().filter(|m| m.voter).count();
    let answering = i.voters.saturating_sub(silent_voters);
    i.silent_managers
        .iter()
        .map(|m| {
            let mins = (i.now_ms - m.since_ms).max(0) / 60_000;
            let ago = if mins < 1 {
                "under a minute".to_owned()
            } else if mins < 120 {
                format!("{mins} min")
            } else {
                format!("{} h", mins / 60)
            };
            let (severity, then) = if !m.voter {
                (Severity::Warning, "it does not vote, so the quorum does not count it".to_owned())
            } else if answering < quorum {
                (Severity::Critical, "a majority of managers no longer answers".to_owned())
            } else if answering == quorum {
                (
                    Severity::Critical,
                    "the cluster survives losing no other manager".to_owned(),
                )
            } else {
                let spare = answering - quorum;
                (
                    Severity::Warning,
                    format!(
                        "the cluster survives losing {spare} more manager{}",
                        if spare == 1 { "" } else { "s" }
                    ),
                )
            };
            Alert {
                kind: AlertKind::ManagerSilent,
                severity,
                namespace: String::new(),
                object: format!("Manager/{}", m.name),
                message: format!(
                    "manager {} has not answered the leader for {ago}: {then}; if it is gone                      for good, join another and remove it (legion delete node {})",
                    m.name, m.name
                ),
            }
        })
        .collect()
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
    out.extend(silent_managers(i));
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
        for l in &v.late_snapshots {
            let why = if l.why.is_empty() {
                String::new()
            } else {
                format!(": {}", l.why)
            };
            out.push(Alert {
                kind: AlertKind::SnapshotLate,
                severity: Severity::Warning,
                namespace: v.namespace.to_owned(),
                object: format!("Volume/{}", v.name),
                message: format!(
                    "Volume {}'s last scheduled snapshot was taken {} ago, meant to be every {}{why}",
                    v.name,
                    crate::clock::span(l.age),
                    crate::clock::span(l.every),
                ),
            });
        }
        if let Some(b) = &v.late_backup {
            let why = if b.why.is_empty() {
                String::new()
            } else {
                format!(": {}", b.why)
            };
            let when = match b.age {
                Some(age) => format!(
                    "was last backed up to {} {} ago",
                    b.target,
                    crate::clock::span(age)
                ),
                None => format!("has not been backed up to {} yet", b.target),
            };
            out.push(Alert {
                kind: AlertKind::BackupLate,
                severity: Severity::Warning,
                namespace: v.namespace.to_owned(),
                object: format!("Volume/{}", v.name),
                message: format!(
                    "Volume {} {when}, meant to be every {}{why}",
                    v.name,
                    crate::clock::span(b.every),
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
                        format!(
                            ", {} more than taken as seen",
                            pool.errors() - p.errors_seen
                        )
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
    for d in &i.databases {
        use crate::replication::Phase;
        let alert = |kind, severity, message| Alert {
            kind,
            severity,
            namespace: d.namespace.to_owned(),
            object: format!("Service/{}", d.name),
            message,
        };
        match (d.phase, d.change_for) {
            (Phase::Split, _) => out.push(alert(
                AlertKind::DatabaseSplit,
                Severity::Critical,
                format!("Service {}'s database is split: {}", d.name, d.message),
            )),
            (Phase::FailingOver, Some(f)) if f >= DATABASE_STALLED => out.push(alert(
                AlertKind::DatabaseStalled,
                Severity::Critical,
                format!(
                    "Service {}'s change of primary has been under way for {}, with no primary \
                     taking writes meanwhile: {}",
                    d.name,
                    crate::clock::span(f),
                    d.message
                ),
            )),
            (Phase::Degraded, _) => out.push(alert(
                AlertKind::DatabaseDegraded,
                Severity::Warning,
                format!("Service {}'s database is degraded: {}", d.name, d.message),
            )),
            _ => {}
        }
        if d.archive_failures >= ARCHIVE_FAILURES {
            out.push(alert(
                AlertKind::ArchiveLate,
                Severity::Warning,
                format!(
                    "Service {}'s log has not been archived for the last {} runs: {}",
                    d.name, d.archive_failures, d.archive_message
                ),
            ));
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

/// Spec of an `AlertReceiver` (cluster-scoped): where the leader sends the
/// alerts it chooses, each once as it is raised and once as it clears
/// (CONSOLE.md, phase 9). Exactly one of `webhook`, `ntfy` and `smtp`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceiverSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<Webhook>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ntfy: Option<Ntfy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp: Option<Smtp>,
    /// The least severe alert sent: `warning` (every alert, the default)
    /// or `critical`.
    #[serde(default, skip_serializing_if = "is_warning")]
    pub severity: Severity,
    /// The namespaces whose alerts are sent; none, every one and the
    /// cluster's own. The cluster's own (a node down, a pool degraded) are
    /// named by the empty string.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// The kinds of alert sent (`node-down`, `pool-degraded`…); none, all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<String>,
}

fn is_warning(s: &Severity) -> bool {
    *s == Severity::Warning
}

/// A Secret named from another object: its value, whole.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretRef {
    pub namespace: String,
    pub name: String,
}

/// An HTTP POST of each alert as JSON (see `ReceiverSpec`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Webhook {
    /// `https://…`, or `http://…` on a network that is trusted.
    pub url: String,
    /// Sent as `Authorization: Bearer <value>` when given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretRef>,
}

/// A message to an ntfy topic (<https://ntfy.sh>), its priority by severity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ntfy {
    /// The topic's URL: `https://ntfy.sh/my-topic`, or a server of one's own.
    pub url: String,
    /// An access token, sent as `Authorization: Bearer`, for a topic that
    /// asks for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretRef>,
}

/// A mail through an SMTP server.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Smtp {
    /// `host:port`: 587 with `starttls`, 465 with `tls`.
    pub server: String,
    #[serde(default, skip_serializing_if = "SmtpTls::is_default")]
    pub tls: SmtpTls,
    pub from: String,
    pub to: Vec<String>,
    /// Who logs in (`AUTH PLAIN`), its password the Secret's value; no
    /// login without one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretRef>,
}

/// How a mail server is spoken to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SmtpTls {
    /// Plain, then TLS before anything is sent (`STARTTLS`).
    #[default]
    Starttls,
    /// TLS from the start.
    Tls,
    /// None: a relay on a network that is trusted.
    None,
}

impl SmtpTls {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl ReceiverSpec {
    /// How it sends: `webhook`, `ntfy` or `smtp`.
    pub fn via(&self) -> &'static str {
        if self.webhook.is_some() {
            "webhook"
        } else if self.ntfy.is_some() {
            "ntfy"
        } else {
            "smtp"
        }
    }

    /// Whether `a` is one it sends.
    pub fn wants(&self, a: &Alert) -> bool {
        a.severity <= self.severity
            && (self.namespaces.is_empty() || self.namespaces.contains(&a.namespace))
            && (self.kinds.is_empty() || self.kinds.iter().any(|k| k == a.kind.as_str()))
    }

    /// The Secret it reads, if any.
    pub fn secret(&self) -> Option<&SecretRef> {
        self.webhook
            .as_ref()
            .and_then(|w| w.secret.as_ref())
            .or_else(|| self.ntfy.as_ref().and_then(|n| n.secret.as_ref()))
            .or_else(|| self.smtp.as_ref().and_then(|s| s.secret.as_ref()))
    }
}

/// The kinds of alert there are, as receivers name them.
pub const KINDS: [AlertKind; 21] = [
    AlertKind::NoLeader,
    AlertKind::NoQuorum,
    AlertKind::NodeDown,
    AlertKind::NodeSuspect,
    AlertKind::CrashLoop,
    AlertKind::VolumeLost,
    AlertKind::VolumeNearlyFull,
    AlertKind::ReplicaStale,
    AlertKind::SnapshotLate,
    AlertKind::BackupLate,
    AlertKind::CaExpiring,
    AlertKind::PoolDegraded,
    AlertKind::PoolErrors,
    AlertKind::PoolNearlyFull,
    AlertKind::ScrubOverdue,
    AlertKind::VersionSkew,
    AlertKind::DatabaseDegraded,
    AlertKind::DatabaseSplit,
    AlertKind::DatabaseStalled,
    AlertKind::ArchiveLate,
    AlertKind::ManagerSilent,
];

fn http_url(url: &str) -> bool {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
}

impl crate::admit::Spec for ReceiverSpec {
    fn admit(&mut self) -> Result<(), crate::admit::Invalid> {
        use crate::admit::{Invalid, field, index};
        let ways = [
            self.webhook.is_some(),
            self.ntfy.is_some(),
            self.smtp.is_some(),
        ];
        if ways.iter().filter(|w| **w).count() != 1 {
            return Err(Invalid::new(
                "",
                "exactly one of webhook, ntfy and smtp says where the alerts go",
            ));
        }
        if let Some(w) = &self.webhook
            && !http_url(&w.url)
        {
            return Err(Invalid::new(
                field("webhook", "url"),
                "an http:// or https:// URL",
            ));
        }
        if let Some(n) = &self.ntfy
            && !http_url(&n.url)
        {
            return Err(Invalid::new(
                field("ntfy", "url"),
                "the topic's URL: https://ntfy.sh/my-topic",
            ));
        }
        if let Some(s) = &self.smtp {
            let port = s.server.rsplit_once(':').and_then(|(h, p)| {
                (!h.is_empty()).then_some(())?;
                p.parse::<u16>().ok()
            });
            if port.is_none() {
                return Err(Invalid::new(
                    field("smtp", "server"),
                    "host:port, e.g. mail.example.com:587",
                ));
            }
            // Nothing that could end a line of the mail's header or of the
            // envelope's command, and start another.
            let address = |a: &str| {
                a.contains('@')
                    && !a.contains(|c: char| {
                        c.is_whitespace() || c.is_control() || c == '<' || c == '>'
                    })
            };
            if !address(&s.from) {
                return Err(Invalid::new(field("smtp", "from"), "an address"));
            }
            if s.to.is_empty() {
                return Err(Invalid::new(field("smtp", "to"), "at least one address"));
            }
            for (i, to) in s.to.iter().enumerate() {
                if !address(to) {
                    return Err(Invalid::new(index("smtp", "to", i), "an address"));
                }
            }
            if s.username.is_empty() != s.secret.is_none() {
                return Err(Invalid::new(
                    field("smtp", "username"),
                    "a login needs both a username and a secret holding its password",
                ));
            }
        }
        for (i, k) in self.kinds.iter().enumerate() {
            if !KINDS.iter().any(|a| a.as_str() == k) {
                return Err(Invalid::new(
                    index("", "kinds", i),
                    format!(
                        "an alert's kind: {}",
                        KINDS
                            .iter()
                            .map(|a| a.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        }
        self.kinds.sort();
        self.kinds.dedup();
        self.namespaces.sort();
        self.namespaces.dedup();
        Ok(())
    }
}

/// What a receiver was last sent, and how its sending goes; written by the
/// leader. `sent` is what lets a new leader carry on without sending again
/// what was sent, nor missing what clears.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiverStatus {
    /// The alerts it was told are raised and has not been told cleared,
    /// by [`Alert::key`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sent: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_ms: Option<i64>,
    /// Why the last attempt failed, until one succeeds.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A manager the leader no longer hears: critical while the cluster
    /// has no other to spare, a warning while it has; a learner's silence
    /// touches no quorum.
    #[test]
    fn a_silent_manager_says_what_the_cluster_can_still_lose() {
        let silent = |name: &str, voter| SilentManager {
            name: name.into(),
            since_ms: 0,
            voter,
        };
        let raised = |voters, silent_managers| {
            alerts(&Inputs {
                leader: true,
                voters,
                silent_managers,
                now_ms: 5 * 60_000,
                ..Inputs::default()
            })
        };
        // Three voters, one silent: none to spare.
        let a = raised(3, vec![silent("vultr", true)]);
        assert_eq!(a.len(), 1, "{a:?}");
        assert_eq!(
            (a[0].kind, a[0].severity),
            (AlertKind::ManagerSilent, Severity::Critical)
        );
        assert_eq!(a[0].object, "Manager/vultr");
        assert!(a[0].message.contains("for 5 min"), "{}", a[0].message);
        assert!(
            a[0].message.contains("losing no other manager"),
            "{}",
            a[0].message
        );
        assert!(
            a[0].message.contains("legion delete node vultr"),
            "{}",
            a[0].message
        );
        // Five voters, one silent: one more may go.
        let a = raised(5, vec![silent("m5", true)]);
        assert_eq!(a[0].severity, Severity::Warning);
        assert!(
            a[0].message.contains("losing 1 more manager;"),
            "{}",
            a[0].message
        );
        // A learner.
        let a = raised(3, vec![silent("new", false)]);
        assert_eq!(a[0].severity, Severity::Warning);
        assert!(a[0].message.contains("does not vote"), "{}", a[0].message);
        // None silent: nothing.
        assert!(raised(3, Vec::new()).is_empty());
    }

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
                late_snapshots: Vec::new(),
                late_backup: None,
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
                    late_snapshots: Vec::new(),
                    late_backup: None,
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
        assert!(
            a[0].message.contains("tank on n1 is DEGRADED"),
            "{}",
            a[0].message
        );
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
        assert!(
            a[0].message.contains("3 checksum errors"),
            "{}",
            a[0].message
        );
        assert_eq!(with_pool(&p, 3, false, 0), [], "taken as seen");
        p.read_errors = 1;
        let a = with_pool(&p, 3, false, 0);
        assert!(
            a[0].message.contains("1 more than taken as seen"),
            "{}",
            a[0].message
        );
    }

    #[test]
    fn a_filling_pool_warns_at_80_and_is_critical_at_90() {
        assert_eq!(with_pool(&pool("ONLINE", 79), 0, false, 0), []);
        let a = with_pool(&pool("ONLINE", 80), 0, false, 0);
        assert_eq!(
            (a[0].kind, a[0].severity),
            (AlertKind::PoolNearlyFull, Severity::Warning)
        );
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
        assert!(
            a[0].message.contains("last scrubbed 36d ago"),
            "{}",
            a[0].message
        );
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
            versions: vec![
                ("n1", "0.2.0 (abc)"),
                ("n2", "0.2.0 (def)"),
                ("n3", "0.2.0 (abc)"),
                ("n4", ""),
            ],
            ..Default::default()
        };
        let a = alerts(&i);
        assert_eq!(a.len(), 1, "{a:?}");
        assert_eq!(a[0].kind, AlertKind::VersionSkew);
        assert!(
            a[0].message
                .contains("0.2.0 (abc) on n1, n3; 0.2.0 (def) on n2"),
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

    fn receiver(yaml: &str) -> Result<ReceiverSpec, String> {
        use crate::admit::Spec;
        let mut r: ReceiverSpec = serde_yaml_ng::from_str(yaml).map_err(|e| e.to_string())?;
        r.admit().map_err(|e| e.to_string())?;
        Ok(r)
    }

    #[test]
    fn a_receiver_names_one_way_and_valid_kinds() {
        assert!(receiver("webhook: {url: https://hooks.example/legion}").is_ok());
        assert!(receiver("ntfy: {url: https://ntfy.sh/ops}").is_ok());
        let e = receiver("{}").unwrap_err();
        assert!(e.contains("exactly one"), "{e}");
        let e = receiver(
            "webhook: {url: https://a.example}
ntfy: {url: https://ntfy.sh/x}",
        )
        .unwrap_err();
        assert!(e.contains("exactly one"), "{e}");
        let e = receiver("webhook: {url: ftp://a.example}").unwrap_err();
        assert!(e.contains("http"), "{e}");
        let e = receiver(
            "webhook: {url: https://a.example}
kinds: [node-gone]",
        )
        .unwrap_err();
        assert!(e.contains("node-down"), "lists the kinds there are: {e}");
        let e = receiver("smtp: {server: mail.example, from: a@example.com, to: [b@example.com]}")
            .unwrap_err();
        assert!(e.contains("host:port"), "{e}");
        let e = receiver(
            "smtp: {server: 'mail.example:587', from: a@example.com, to: [b@example.com], username: a}",
        )
        .unwrap_err();
        assert!(e.contains("both a username and a secret"), "{e}");
        let e = receiver(
            "smtp: {server: 'mail.example:587', from: a@example.com, to: [\"b@example.com\\r\\nBcc: c@example.com\"]}",
        )
        .unwrap_err();
        assert!(e.contains("to[0]"), "a header can't be slipped in: {e}");
    }

    #[test]
    fn a_receiver_wants_what_its_severity_namespaces_and_kinds_say() {
        let alert = |kind, severity, namespace: &str| Alert {
            kind,
            severity,
            namespace: namespace.into(),
            object: String::new(),
            message: String::new(),
        };
        let all = receiver("webhook: {url: https://a.example}").unwrap();
        assert!(all.wants(&alert(AlertKind::NodeDown, Severity::Critical, "")));
        assert!(all.wants(&alert(AlertKind::CrashLoop, Severity::Warning, "shop")));
        let critical = receiver(
            "webhook: {url: https://a.example}
severity: critical",
        )
        .unwrap();
        assert!(critical.wants(&alert(AlertKind::NodeDown, Severity::Critical, "")));
        assert!(!critical.wants(&alert(AlertKind::CrashLoop, Severity::Warning, "shop")));
        let shop = receiver(
            "webhook: {url: https://a.example}
namespaces: [shop]
kinds: [crash-loop]",
        )
        .unwrap();
        assert!(shop.wants(&alert(AlertKind::CrashLoop, Severity::Warning, "shop")));
        assert!(!shop.wants(&alert(AlertKind::CrashLoop, Severity::Warning, "blog")));
        assert!(!shop.wants(&alert(AlertKind::NodeDown, Severity::Critical, "")));
        let cluster = receiver(
            "webhook: {url: https://a.example}
namespaces: ['']",
        )
        .unwrap();
        assert!(cluster.wants(&alert(AlertKind::NodeDown, Severity::Critical, "")));
        assert!(!cluster.wants(&alert(AlertKind::CrashLoop, Severity::Warning, "shop")));
    }

    #[test]
    fn every_kind_of_alert_can_be_named() {
        // A kind added to AlertKind but not to KINDS couldn't be chosen.
        let named: std::collections::BTreeSet<_> = KINDS.iter().map(|k| k.as_str()).collect();
        assert_eq!(named.len(), KINDS.len());
        let last = AlertKind::ArchiveLate;
        assert!(KINDS.contains(&last));
    }

    #[test]
    fn a_manager_with_no_leader_says_so() {
        let got = alerts(&Inputs::default());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, AlertKind::NoLeader);
    }

    #[test]
    fn a_late_snapshot_schedule_says_how_late() {
        let i = Inputs {
            leader: true,
            volumes: vec![VolumeInput {
                namespace: "a",
                name: "pg-db-0",
                late_snapshots: vec![LateSnapshots {
                    age: Duration::from_secs(3 * 3600),
                    every: Duration::from_secs(3600),
                    why: "node n2: out of space (attempt 4; the next after 2026-11-25T12:05:54Z)"
                        .into(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let got = alerts(&i);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind.as_str(), "snapshot-late");
        assert_eq!(
            got[0].message,
            "Volume pg-db-0's last scheduled snapshot was taken 3h ago, meant to be every 1h: node n2: out of space (attempt 4; the next after 2026-11-25T12:05:54Z)"
        );
    }

    #[test]
    fn late_backups_say_when_the_last_was_or_that_none_was() {
        let i = Inputs {
            leader: true,
            volumes: vec![
                VolumeInput {
                    namespace: "a",
                    name: "db",
                    late_backup: Some(LateBackup {
                        target: "offsite".into(),
                        age: Some(Duration::from_secs(3 * 3600)),
                        every: Duration::from_secs(3600),
                        why: String::new(),
                    }),
                    ..Default::default()
                },
                VolumeInput {
                    namespace: "a",
                    name: "logs",
                    late_backup: Some(LateBackup {
                        target: "offsite".into(),
                        age: None,
                        every: Duration::from_secs(3600),
                        why: "offsite: Permission denied (attempt 1; the next after 2026-11-25T12:05:54Z)".into(),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let got = alerts(&i);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|a| a.kind.as_str() == "backup-late"));
        assert_eq!(
            got[0].message,
            "Volume db was last backed up to offsite 3h ago, meant to be every 1h"
        );
        assert_eq!(
            got[1].message,
            "Volume logs has not been backed up to offsite yet, meant to be every 1h: offsite: Permission denied (attempt 1; the next after 2026-11-25T12:05:54Z)"
        );
    }

    #[test]
    fn a_replicated_database_raises_degraded_split_and_stalled() {
        use crate::replication::Phase;
        let db = |name, phase, change_for| DatabaseInput {
            namespace: "data",
            name,
            phase,
            message: "why",
            change_for,
            archive_failures: 0,
            archive_message: "",
        };
        let i = Inputs {
            leader: true,
            databases: vec![
                db("a", Phase::Healthy, None),
                db("b", Phase::Degraded, None),
                db("c", Phase::Split, None),
                db("d", Phase::FailingOver, Some(Duration::from_secs(30))),
                db("e", Phase::FailingOver, Some(Duration::from_secs(300))),
                db("f", Phase::Forming, None),
            ],
            ..Default::default()
        };
        let got = alerts(&i);
        let kinds: Vec<(AlertKind, Severity, &str)> = got
            .iter()
            .map(|a| (a.kind, a.severity, a.object.as_str()))
            .collect();
        assert_eq!(
            kinds,
            [
                (AlertKind::DatabaseSplit, Severity::Critical, "Service/c"),
                (AlertKind::DatabaseStalled, Severity::Critical, "Service/e"),
                (AlertKind::DatabaseDegraded, Severity::Warning, "Service/b"),
            ]
        );
        assert_eq!(
            got[1].message,
            "Service e's change of primary has been under way for 5m, with no primary taking \
             writes meanwhile: why"
        );
    }

    #[test]
    fn a_log_not_archived_for_three_runs_raises_archive_late() {
        let db = |failures| Inputs {
            leader: true,
            databases: vec![DatabaseInput {
                namespace: "data",
                name: "pg",
                phase: crate::replication::Phase::Healthy,
                message: "",
                change_for: None,
                archive_failures: failures,
                archive_message: "pg-0: 403 Forbidden",
            }],
            ..Default::default()
        };
        assert!(alerts(&db(2)).is_empty());
        let got = alerts(&db(3));
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0].kind, got[0].severity),
            (AlertKind::ArchiveLate, Severity::Warning)
        );
        assert_eq!(
            got[0].message,
            "Service pg's log has not been archived for the last 3 runs: pg-0: 403 Forbidden"
        );
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
                    why: "node n2: refused (attempt 3; the next after 2026-11-25T12:05:54Z)".into(),
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
             node n2: refused (attempt 3; the next after 2026-11-25T12:05:54Z)"
        );
    }
}
