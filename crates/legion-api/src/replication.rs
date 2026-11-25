//! Database replication (DESIGN.md §6.9): a stateful Service's members
//! replicate a database among themselves, Legion naming the primary and a
//! plugin doing the database's part. [`Replication`] is the Service's
//! `replication`, [`ReplicationStatus`] what the leader records of it, and
//! [`spi`] the plugin contract (docs/REPLICATION.md, SPI v1).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, check_absolute, field};
use crate::quantity::{Bytes, Duration};
use crate::volume::Failover;

/// Where node-provided plugins are found, and mounted in the container.
pub const PLUGIN_DIR: &str = "/usr/local/libexec/legion/replication";
pub const PLUGIN_MOUNT: &str = "/.legion/replication";

/// The Service's `replication`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Replication {
    /// A plugin of the node's plugin directory, mounted read-only into the
    /// container at [`PLUGIN_MOUNT`]`/<name>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
    /// Or an executable of the image, with its arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// The container whose jail runs the plugin; unset, the pod's only one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// Passed to the plugin as given.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub options: serde_json::Map<String, serde_json::Value>,
    /// Who promotes a replica when the primary is lost: the leader on its
    /// own (the default), or the operator.
    #[serde(default = "automatic")]
    pub failover: Failover,
    /// How long a primary whose node is up may be unready before it counts
    /// as lost.
    #[serde(default = "default_failover_after")]
    pub failover_after: Duration,
    /// Automatic failover passes over a replica further behind than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_lag: Option<Bytes>,
    /// Replicas that must confirm each commit.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub synchronous: u32,
    /// A `<service>-read` address and name over the ready replicas.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read: bool,
    /// What becomes of a member whose data cannot follow the primary.
    #[serde(default, skip_serializing_if = "Reclone::is_deny")]
    pub reclone: Reclone,
    /// The operator's latest request for a new primary (`legion
    /// replication switchover` and `failover`), acted on once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<ChangeRequest>,
    /// The database's log shipped to a BackupTarget as it is written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<Archive>,
}

/// Log archiving (`replication.archive`): every member's plugin keeps each
/// finished piece of the database's log (PostgreSQL's WAL segments) in a
/// spool it names (`spool` in its `status`), and the leader has the
/// member's node ship them to the target, under
/// `<prefix>/<namespace>.<service>/wal/`, dropping those older than
/// `keep`. With the Volume backups to the same target, it is what a
/// restore to a point in time replays.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Archive {
    /// A BackupTarget, `s3` or `directory`.
    pub target: String,
    #[serde(default = "default_keep")]
    pub keep: Duration,
}

fn default_keep() -> Duration {
    Duration::from_secs(7 * 86400)
}

/// An operator's request for a new primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeRequest {
    /// `switchover`: planned, the primary running; `failover`: the primary
    /// lost, under `failover: manual`.
    pub kind: ChangeKind,
    /// The member to promote; unset, the replica furthest ahead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Pass over the lag and synchronous checks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force: bool,
    /// When it was asked, Unix milliseconds: a new time is a new request.
    pub at: i64,
}

fn automatic() -> Failover {
    Failover::Automatic
}

fn default_failover_after() -> Duration {
    Duration::from_secs(30)
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// See [`Replication::reclone`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reclone {
    /// It waits for the operator.
    #[default]
    Deny,
    /// Its data directory is emptied and copied anew from the primary.
    Allow,
}

impl Reclone {
    pub fn is_deny(&self) -> bool {
        *self == Reclone::Deny
    }
}

/// A plugin's name: a file of the plugin directory.
fn check_plugin(path: &str, name: &str) -> Result<(), Invalid> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(Invalid::new(
            path,
            format!("{name:?}: a file name of letters, digits, '-', '_' and '.'"),
        ))
    }
}

impl Replication {
    /// Admission of what can be told from the spec alone; what the plugin
    /// supports is checked by the agent (`describe`).
    pub(crate) fn admit(
        &self,
        path: &str,
        replicas: u32,
        containers: &[&str],
    ) -> Result<(), Invalid> {
        match (&self.plugin, self.command.is_empty()) {
            (Some(p), true) => check_plugin(&field(path, "plugin"), p)?,
            (None, false) => check_absolute(&field(path, "command"), &self.command[0])?,
            _ => {
                return Err(Invalid::new(
                    path,
                    "set exactly one of `plugin` and `command`",
                ));
            }
        }
        match &self.container {
            Some(c) if !containers.contains(&c.as_str()) => {
                return Err(Invalid::new(
                    field(path, "container"),
                    format!("{c:?}: no such container in the pod"),
                ));
            }
            None if containers.len() != 1 => {
                return Err(Invalid::new(
                    field(path, "container"),
                    "name the container running the database: the pod has several",
                ));
            }
            _ => {}
        }
        if self.failover_after.millis() < 5000 {
            return Err(Invalid::new(
                field(path, "failoverAfter"),
                "at least 5s: a primary is checked every 5s",
            ));
        }
        if let Some(a) = &self.archive {
            let path = field(path, "archive");
            if a.target.is_empty() {
                return Err(Invalid::new(field(&path, "target"), "name a BackupTarget"));
            }
            if a.keep.millis() < 3_600_000 {
                return Err(Invalid::new(
                    field(&path, "keep"),
                    "at least 1h: a restore replays the log since a backup",
                ));
            }
        }
        if self.synchronous > 0 && self.synchronous >= replicas {
            return Err(Invalid::new(
                field(path, "synchronous"),
                format!(
                    "{}: below the replica count ({replicas}), which counts the primary",
                    self.synchronous
                ),
            ));
        }
        Ok(())
    }

    /// The container running the database and the plugin.
    pub fn container<'a>(&'a self, containers: &[&'a str]) -> Option<&'a str> {
        self.container.as_deref().or(match containers {
            [only] => Some(*only),
            _ => None,
        })
    }

    /// The plugin's command line, as run in the container's jail.
    pub fn argv(&self, operation: &str) -> Vec<String> {
        let mut argv = match &self.plugin {
            Some(p) => vec![format!("{PLUGIN_MOUNT}/{p}")],
            None => self.command.clone(),
        };
        argv.push(operation.to_owned());
        argv
    }
}

/// What the leader records of a replicated Service (`status.replication`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationStatus {
    /// Goes up by one each time a new primary is named.
    pub epoch: u64,
    /// The member named primary (`<service>-<ordinal>`), once there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,
    pub phase: Phase,
    /// Why, when it is not healthy.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Each member's last `status`, by member.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub members: BTreeMap<String, MemberStatus>,
    /// With `synchronous`, the replicas the primary waits for: those healthy
    /// and streaming, as last seen while the primary was. A failover
    /// promotes only one of them; none, and the primary runs on without
    /// waiting, rather than stop taking writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub synchronous: Vec<String>,
    /// The furthest position seen of a primary: what automatic failover
    /// measures lag against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_position: Option<spi::Position>,
    /// A failover or switchover under way, recorded before anything is
    /// asked of a member, so a new leader carries on with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change: Option<Change>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_change: Option<LastChange>,
    /// The operator's request last acted on (its `at`), and what came of
    /// it when it was refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handled: Option<i64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub refused: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// The first primary is not up yet.
    #[default]
    Forming,
    Healthy,
    /// A replica is missing, unhealthy or behind.
    Degraded,
    FailingOver,
    /// A member Legion calls a replica says it is primary.
    Split,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Forming => "forming",
            Phase::Healthy => "healthy",
            Phase::Degraded => "degraded",
            Phase::FailingOver => "failing-over",
            Phase::Split => "split",
        }
    }
}

/// A member's last `status`, with when it was taken and what Legion made
/// of it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberStatus {
    /// The member's task when it reported.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub task: String,
    /// Unix milliseconds.
    pub at: i64,
    #[serde(flatten)]
    pub status: spi::Status,
    /// Its data cannot follow the primary (`diverged`) and waits for the
    /// operator, or for `reclone: allow`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub diverged: bool,
    /// The last call that failed, and why.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// A change of primary under way.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    pub kind: ChangeKind,
    /// The primary being replaced, if there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Chosen once the old primary is fenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Why.
    pub reason: String,
    /// Unix milliseconds.
    pub started: i64,
    /// The operator passed over the lag and synchronous checks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub force: bool,
    /// The old primary's tasks, by uid, as the change began: each must
    /// have stopped, or be surely stopped by its node cut off, before
    /// another member is promoted. Tasks made since are told there is no
    /// primary, so they wait.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fence: Vec<String>,
    /// The member the operator asked for, if one was named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChangeKind {
    /// The primary was lost.
    Failover,
    /// Planned: the primary stops cleanly once the target has caught up.
    Switchover,
}

/// Where a replicated database's log archiving stands
/// (`status.archive`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveStatus {
    pub target: String,
    /// The newest piece of the log the target holds, by name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last: String,
    /// When a member's spool was last emptied to the target, Unix ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shipped_ms: Option<i64>,
    /// When the target was last pruned to `keep`, Unix ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruned_ms: Option<i64>,
    /// Pieces waiting in the members' spools after the last runs.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pending: u32,
    /// Runs failed in a row, and why the last did.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// The last completed change of primary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastChange {
    pub kind: ChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    pub to: String,
    pub reason: String,
    /// Where the new primary's history starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<spi::Position>,
    /// Unix milliseconds.
    pub at: i64,
}

/// What a member's task is told (its spec's `replication`): the Service's
/// settings, and its role and the topology as the leader last named them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskReplication {
    pub config: Replication,
    pub epoch: u64,
    pub role: spi::Role,
    #[serde(default)]
    pub topology: spi::Topology,
}

impl TaskReplication {
    /// The request for `operation` from member `me`.
    pub fn request(
        &self,
        operation: &str,
        namespace: &str,
        service: &str,
        me: spi::Member,
        now_ms: i64,
    ) -> spi::Request {
        spi::Request {
            spi_version: spi::VERSION.into(),
            operation: operation.into(),
            deadline: now_ms + spi::deadline(operation).as_millis() as i64,
            service: spi::ServiceRef {
                namespace: namespace.into(),
                name: service.into(),
            },
            epoch: self.epoch,
            me,
            topology: self.topology.clone(),
            options: self.config.options.clone(),
            role: (operation == "prepare").then_some(self.role),
            reclone: false,
            probe: None,
            archive: self.config.archive.is_some(),
        }
    }
}

/// What an agent reports of a member, with each heartbeat.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberReport {
    /// The task, `<namespace>/<name>`.
    pub task: String,
    pub member: String,
    /// The epoch whose topology the member last applied (`configure`).
    #[serde(default)]
    pub applied: u64,
    /// The last `status`, when one answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<spi::Status>,
    /// The last call that failed, and its code.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    /// Unix milliseconds of the last `status`.
    #[serde(default)]
    pub at: i64,
}

/// The plugin contract, SPI v1 (docs/REPLICATION.md).
pub mod spi {
    use serde::{Deserialize, Serialize};

    pub const VERSION: &str = "1";

    /// How long each operation may take.
    pub fn deadline(operation: &str) -> std::time::Duration {
        std::time::Duration::from_secs(match operation {
            "status" => 4,
            "describe" => 10,
            "configure" | "promote" => 60,
            "probe" => 30,
            // A first copy of a large database takes what it takes.
            _ => 24 * 3600,
        })
    }

    /// Every request.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Request {
        pub spi_version: String,
        pub operation: String,
        /// Unix milliseconds.
        pub deadline: i64,
        pub service: ServiceRef,
        pub epoch: u64,
        #[serde(rename = "self")]
        pub me: Member,
        pub topology: Topology,
        #[serde(default)]
        pub options: serde_json::Map<String, serde_json::Value>,
        /// `prepare` only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub role: Option<Role>,
        /// `prepare` of a replica only, after it answered `diverged` and
        /// the Service allows it (`reclone: allow`): discard the data and
        /// copy the primary's anew.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub reclone: bool,
        /// `probe` only (capability `probe`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub probe: Option<Probe>,
        /// Keep each finished piece of the log in a spool for Legion to
        /// ship (capability `archive`), and say where in `status`.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub archive: bool,
    }

    /// What `probe` does, for the conformance check
    /// (`legion replication check`): on the primary, write a marker
    /// `{"write": KEY}` (committed as any write is, so synchronous
    /// replicas confirm it); on any member, say whether one is there
    /// `{"read": KEY}`. Keys are letters, digits, `-` and `_`, up to 64.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum Probe {
        Write(String),
        Read(String),
    }

    impl Probe {
        pub fn key(&self) -> &str {
            match self {
                Probe::Write(k) | Probe::Read(k) => k,
            }
        }
    }

    /// `probe`'s response.
    #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
    pub struct Probed {
        pub found: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ServiceRef {
        pub namespace: String,
        pub name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Member {
        pub member: String,
        #[serde(default)]
        pub ordinal: u32,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub address: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub dns: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Topology {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub primary: Option<Member>,
        #[serde(default)]
        pub members: Vec<Member>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub synchronous: Option<Synchronous>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Synchronous {
        pub count: u32,
        pub members: Vec<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum Role {
        Primary,
        Replica,
        #[default]
        Unknown,
    }

    impl Role {
        pub fn as_str(self) -> &'static str {
            match self {
                Role::Primary => "primary",
                Role::Replica => "replica",
                Role::Unknown => "unknown",
            }
        }
    }

    /// How far a member's data goes: ordered by term, then offset.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct Position {
        pub term: u64,
        pub offset: u64,
    }

    /// On the wire the offset is a decimal string, as JSON numbers past
    /// 2^53 are not safe everywhere.
    #[derive(Serialize, Deserialize)]
    struct Wire {
        term: u64,
        offset: Offset,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Offset {
        Text(String),
        Number(u64),
    }

    impl Serialize for Position {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            Wire {
                term: self.term,
                offset: Offset::Text(self.offset.to_string()),
            }
            .serialize(s)
        }
    }

    impl<'de> Deserialize<'de> for Position {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let w = Wire::deserialize(d)?;
            let offset = match w.offset {
                Offset::Number(n) => n,
                Offset::Text(t) => t.parse().map_err(|_| {
                    serde::de::Error::custom(format!(
                        "offset {t:?}: an unsigned integer in decimal"
                    ))
                })?,
            };
            Ok(Position {
                term: w.term,
                offset,
            })
        }
    }

    impl Position {
        /// How far `self` is behind `ahead`, in offset units, when on the
        /// same term; a lower term is behind by an unknown amount.
        pub fn behind(&self, ahead: &Position) -> Option<u64> {
            if self >= ahead {
                return Some(0);
            }
            (self.term == ahead.term).then(|| ahead.offset - self.offset)
        }
    }

    impl std::fmt::Display for Position {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}/{}", self.term, self.offset)
        }
    }

    /// `describe`'s response.
    #[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Describe {
        pub spi_versions: Vec<String>,
        #[serde(default)]
        pub database: String,
        #[serde(default)]
        pub capabilities: Capabilities,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Capabilities {
        #[serde(default)]
        pub synchronous: bool,
        #[serde(default)]
        pub readable_replicas: bool,
        #[serde(default)]
        pub rewind: bool,
        #[serde(default)]
        pub seed_from_snapshot: bool,
        /// Answers `probe`, so the conformance check can see data travel.
        #[serde(default)]
        pub probe: bool,
        /// Keeps its log for Legion to ship, when asked (`archive`).
        #[serde(default)]
        pub archive: bool,
    }

    impl Describe {
        /// Why the plugin cannot serve these settings, if it cannot.
        pub fn refuses(&self, r: &super::Replication) -> Option<String> {
            if !self.spi_versions.iter().any(|v| v == VERSION) {
                return Some(format!(
                    "the plugin speaks SPI {:?}, not {VERSION}",
                    self.spi_versions
                ));
            }
            if r.synchronous > 0 && !self.capabilities.synchronous {
                return Some("`synchronous` is set; the plugin has no synchronous replicas".into());
            }
            if r.read && !self.capabilities.readable_replicas {
                return Some("`read` is set; the plugin's replicas serve no reads".into());
            }
            if r.archive.is_some() && !self.capabilities.archive {
                return Some("`archive` is set; the plugin keeps no log for Legion to ship".into());
            }
            None
        }
    }

    /// `status`'s response.
    #[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Status {
        pub role: Role,
        #[serde(default)]
        pub healthy: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub position: Option<Position>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub upstream: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub streaming: bool,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub synchronous: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub lag: Option<Lag>,
        #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
        pub detail: serde_json::Map<String, serde_json::Value>,
        /// Asked to `archive`: the directory, as the container sees it,
        /// holding the finished pieces of the log for Legion to ship and
        /// remove. Files ending `.part` are being written.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub spool: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Lag {
        #[serde(default, skip_serializing_if = "Option::is_none", with = "decimal")]
        pub bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub seconds: Option<f64>,
    }

    /// An optional u64 written as a decimal string, read from either.
    mod decimal {
        use serde::{Deserialize, Deserializer, Serializer};

        pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
            match v {
                Some(n) => s.serialize_str(&n.to_string()),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Either {
                Text(String),
                Number(u64),
            }
            Ok(match Option::<Either>::deserialize(d)? {
                None => None,
                Some(Either::Number(n)) => Some(n),
                Some(Either::Text(t)) => Some(t.parse().map_err(serde::de::Error::custom)?),
            })
        }
    }

    /// A failure's `error`, on standard output with a non-zero exit.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Error {
        pub code: String,
        #[serde(default)]
        pub message: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Failure {
        pub error: Error,
    }

    /// The codes Legion acts on.
    pub const RETRY: &str = "retry";
    pub const RESTART: &str = "restart";
    pub const DIVERGED: &str = "diverged";
}

#[cfg(test)]
mod tests {
    use super::spi::*;
    use super::*;

    fn plugin() -> Replication {
        serde_json::from_value(serde_json::json!({ "plugin": "postgres" })).unwrap()
    }

    #[test]
    fn a_replication_takes_its_defaults_and_is_admitted_with_one_plugin() {
        let r = plugin();
        assert_eq!(r.failover, Failover::Automatic);
        assert_eq!(r.failover_after, Duration::from_secs(30));
        assert_eq!(r.reclone, Reclone::Deny);
        r.admit("replication", 3, &["postgres"]).unwrap();
        assert_eq!(r.container(&["postgres"]), Some("postgres"));
        assert_eq!(
            r.argv("status"),
            ["/.legion/replication/postgres", "status"]
        );
        let c = Replication {
            plugin: None,
            command: vec!["/usr/local/bin/repl".into(), "-v".into()],
            ..plugin()
        };
        assert_eq!(c.argv("promote"), ["/usr/local/bin/repl", "-v", "promote"]);
    }

    #[test]
    fn a_replication_is_refused_what_it_cannot_do() {
        let both = Replication {
            command: vec!["/bin/x".into()],
            ..plugin()
        };
        let neither = Replication {
            plugin: None,
            ..plugin()
        };
        let relative = Replication {
            plugin: None,
            command: vec!["x".into()],
            ..plugin()
        };
        let path = Replication {
            plugin: Some("../x".into()),
            ..plugin()
        };
        let quick = Replication {
            failover_after: Duration::from_secs(1),
            ..plugin()
        };
        let sync = Replication {
            synchronous: 3,
            ..plugin()
        };
        let named = Replication {
            container: Some("db".into()),
            ..plugin()
        };
        for (r, containers, at) in [
            (&both, &["pg"][..], "replication"),
            (&neither, &["pg"][..], "replication"),
            (&relative, &["pg"][..], "replication.command"),
            (&path, &["pg"][..], "replication.plugin"),
            (&quick, &["pg"][..], "replication.failoverAfter"),
            (&sync, &["pg"][..], "replication.synchronous"),
            (&named, &["pg"][..], "replication.container"),
            (&plugin(), &["pg", "exporter"][..], "replication.container"),
        ] {
            let e = r.admit("replication", 3, containers).unwrap_err();
            assert_eq!(e.path, at, "{e}");
        }
        Replication {
            synchronous: 2,
            ..plugin()
        }
        .admit("replication", 3, &["pg"])
        .unwrap();
    }

    #[test]
    fn positions_order_by_term_then_offset_and_travel_as_decimal_text() {
        let p = |term, offset| Position { term, offset };
        assert!(p(2, 1) > p(1, u64::MAX));
        assert!(p(1, 9) > p(1, 8));
        assert_eq!(p(1, 8).behind(&p(1, 20)), Some(12));
        assert_eq!(p(1, 30).behind(&p(1, 20)), Some(0));
        assert_eq!(p(1, 8).behind(&p(2, 0)), None);
        let big = p(7, u64::MAX);
        let text = serde_json::to_string(&big).unwrap();
        assert_eq!(text, r#"{"term":7,"offset":"18446744073709551615"}"#);
        assert_eq!(serde_json::from_str::<Position>(&text).unwrap(), big);
        let n: Position = serde_json::from_str(r#"{"term":1,"offset":42}"#).unwrap();
        assert_eq!(n, p(1, 42));
        assert!(serde_json::from_str::<Position>(r#"{"term":1,"offset":"-1"}"#).is_err());
    }

    #[test]
    fn a_status_as_the_spec_shows_it_is_read_and_unknown_fields_ignored() {
        let s: Status = serde_json::from_value(serde_json::json!({
            "role": "replica", "healthy": true,
            "position": { "term": 7, "offset": "50331744" },
            "upstream": "pg-0", "streaming": true, "synchronous": true,
            "lag": { "bytes": "8192", "seconds": 0.4 },
            "detail": { "slot": "legion_pg_1" },
            "somethingNew": 1
        }))
        .unwrap();
        assert_eq!(s.role, Role::Replica);
        assert_eq!(
            s.position,
            Some(Position {
                term: 7,
                offset: 50331744
            })
        );
        assert_eq!(s.lag.as_ref().unwrap().bytes, Some(8192));
        let m = MemberStatus {
            task: "t".into(),
            at: 1,
            status: s.clone(),
            ..Default::default()
        };
        let back: MemberStatus = serde_json::from_value(serde_json::to_value(&m).unwrap()).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn a_plugin_is_refused_settings_it_lacks_capabilities_for() {
        let d = Describe {
            spi_versions: vec!["1".into()],
            database: "x".into(),
            capabilities: Capabilities::default(),
        };
        assert_eq!(d.refuses(&plugin()), None);
        let sync = Replication {
            synchronous: 1,
            ..plugin()
        };
        assert!(d.refuses(&sync).unwrap().contains("synchronous"));
        let read = Replication {
            read: true,
            ..plugin()
        };
        assert!(d.refuses(&read).unwrap().contains("read"));
        let v2 = Describe {
            spi_versions: vec!["2".into()],
            ..d
        };
        assert!(v2.refuses(&plugin()).unwrap().contains("SPI"));
    }

    #[test]
    fn a_request_is_written_as_the_spec_shows() {
        let me = Member {
            member: "pg-1".into(),
            ordinal: 1,
            address: "10.200.1.17".into(),
            dns: "pg-1.pg.data.legion".into(),
        };
        let r = Request {
            spi_version: VERSION.into(),
            operation: "prepare".into(),
            deadline: 5,
            service: ServiceRef {
                namespace: "data".into(),
                name: "pg".into(),
            },
            epoch: 4,
            me: me.clone(),
            topology: Topology {
                primary: None,
                members: vec![me],
                synchronous: Some(Synchronous {
                    count: 1,
                    members: vec!["pg-1".into()],
                }),
            },
            options: Default::default(),
            role: Some(Role::Replica),
            reclone: false,
            probe: None,
            archive: false,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["spiVersion"], "1");
        assert_eq!(v["self"]["member"], "pg-1");
        assert_eq!(v["role"], "replica");
        assert!(v["topology"].get("primary").is_none());
        assert_eq!(serde_json::from_value::<Request>(v).unwrap(), r);
    }
}
