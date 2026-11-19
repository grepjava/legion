//! `Node`: one per machine running `legion daemon`. The agent creates it and
//! writes its status; operators change the spec (drain, pause). Node labels
//! are the object's labels.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, field, index};
use crate::quantity::{Bytes, Cpu};
use crate::service::TaintEffect;

/// Heartbeat periods a node may be silent for before the leader marks it
/// down and its tasks are orphaned. An agent cut off from the managers that
/// long stops its pods that must not run twice (`onPartition: stop`).
pub const DOWN_AFTER_HEARTBEATS: u32 = 8;

/// Label (`true`) marking the nodes that serve HTTPS routes. While no node
/// has it, any node may read routes' certificates; once one has it, only
/// nodes with it may.
pub const LABEL_INGRESS: &str = "legion.ingress";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    #[serde(default)]
    pub availability: Availability,
    #[serde(default)]
    pub role: Role,
    /// Keep tasks off the node unless their service tolerates them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub taints: Vec<Taint>,
    /// Ids of `legion doctor`'s advised checks this node needs met to
    /// take new tasks, as its required ones (`forwarding`, `racct`…). By
    /// default an advised check found lacking only warns (`racct` also
    /// keeps pods with limits off, as `platform.noLimits` says).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub require: Vec<String>,
    /// Days between scrubs of the node's pools, started by its agent; none
    /// unless set. A pool unscrubbed for longer raises `scrub-overdue`.
    #[serde(
        default,
        rename = "scrubEveryDays",
        skip_serializing_if = "Option::is_none"
    )]
    pub scrub_every_days: Option<u32>,
    /// Read, write and checksum errors per pool taken as seen: only more
    /// than these raise `pool-errors` (`legion node errors-seen`).
    #[serde(
        default,
        rename = "poolErrorsSeen",
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub pool_errors_seen: std::collections::BTreeMap<String, u64>,
}

impl NodeSpec {
    /// The days between scrubs that a pool is held to: the node's own, or
    /// [`SCRUB_DUE_DAYS`] when it sets none.
    pub fn scrub_due(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            u64::from(self.scrub_every_days.unwrap_or(SCRUB_DUE_DAYS)) * 86_400,
        )
    }
}

/// Days a pool may go unscrubbed on a node that schedules none.
pub const SCRUB_DUE_DAYS: u32 = 35;

impl Spec for NodeSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        for (i, id) in self.require.iter().enumerate() {
            let ok = !id.is_empty()
                && id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c));
            if !ok {
                return Err(Invalid::new(
                    index("", "require", i),
                    "a check's id, as legion doctor --json gives it (forwarding, module.pf…)",
                ));
            }
        }
        self.require.sort();
        self.require.dedup();
        if self.scrub_every_days == Some(0) {
            return Err(Invalid::new(
                field("", "scrubEveryDays"),
                "at least 1, or unset for no scrubs",
            ));
        }
        for (i, t) in self.taints.iter().enumerate() {
            if t.key.is_empty() {
                return Err(Invalid::new(
                    field(&index("", "taints", i), "key"),
                    "must not be empty",
                ));
            }
        }
        Ok(())
    }
}

/// `NoSchedule` keeps new tasks off, `PreferNoSchedule` makes the node a
/// last choice, `NoExecute` also moves running tasks away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Taint {
    pub key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    pub effect: TaintEffect,
}

impl Taint {
    /// Whether one of `tolerations` covers this taint.
    pub fn tolerated_by(&self, tolerations: &[crate::service::Toleration]) -> bool {
        tolerations.iter().any(|t| {
            t.key == self.key
                && (t.value.is_empty() || t.value == self.value)
                && t.effect.is_none_or(|e| e == self.effect)
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Availability {
    /// Takes new tasks.
    #[default]
    Active,
    /// Keeps its tasks but takes no new ones.
    Pause,
    /// Its tasks move elsewhere.
    Drain,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Manager,
    #[default]
    Worker,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Manager => "manager",
            Role::Worker => "worker",
        }
    }
}

/// Written by the node's agent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    pub state: NodeState,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    /// Address managers and other nodes reach the agent at.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub addr: String,
    #[serde(default)]
    pub platform: Platform,
    /// What tasks may request in total.
    #[serde(default)]
    pub allocatable: Capacity,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    /// Which build of that version: the first 12 hex digits of the SHA-256
    /// of the daemon's executable, told apart where the version is not
    /// (`version-skew`). Empty from builds before.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub build: String,
    /// What the agent speaks (`legion_api::protocol`); unset from builds
    /// before the handshake, which speak protocol 1.
    #[serde(default, skip_serializing_if = "crate::protocol::Protocol::is_unset")]
    pub protocol: crate::protocol::Protocol,
    /// The agent runs a task's images by the digests they were pinned to
    /// (`TaskSpec::images`). Until every node does, tasks aren't pinned.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub image_pins: bool,
    /// This node's part of the cluster pod network, e.g. `10.200.3.0/24`.
    /// Assigned once, when the node first registers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,
    /// How other nodes reach this one over the WireGuard mesh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wireguard: Option<WireGuard>,
    /// Where its drain stands, written by the leader while the node's
    /// availability is `drain`; registration keeps it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain: Option<DrainStatus>,
    /// What `legion doctor` finds the host lacks, as the agent last
    /// checked: its findings not met. Empty from agents that don't say.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lacks: Vec<HostLack>,
    /// The host, as its agent last said: unset from agents that don't.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostFacts>,
    /// Its ZFS pools, as its agent last read them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pools: Vec<Pool>,
}

/// What an agent says of its host with a heartbeat (`host_json`): the
/// leader writes it into the Node's status when it changed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostFacts>,
    #[serde(default)]
    pub pools: Vec<Pool>,
}

/// What a node's host is: written when it changes (a reboot, an upgrade).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostFacts {
    /// FreeBSD's release, its userland's (`freebsd-version -u`).
    pub release: String,
    /// The running kernel's (`freebsd-version -k`).
    pub kernel: String,
    /// How it booted: `UEFI` or `BIOS`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub boot: String,
    /// When it booted, in milliseconds since the epoch.
    #[serde(default)]
    pub booted_ms: i64,
}

/// A ZFS pool of a node, as its agent last read it (`zpool status`). Its
/// space moves in steps of a hundredth of its size, so the Node is
/// written when the pool fills by a percent, not with every write.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    pub name: String,
    /// `ONLINE`, `DEGRADED`, `FAULTED`, `OFFLINE`, `UNAVAIL`, `REMOVED`,
    /// `SUSPENDED`.
    pub state: String,
    pub size_bytes: u64,
    pub alloc_bytes: u64,
    /// Over all its devices, since they were last cleared.
    #[serde(default)]
    pub read_errors: u64,
    #[serde(default)]
    pub write_errors: u64,
    #[serde(default)]
    pub checksum_errors: u64,
    /// Its last or current scrub or resilver.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<PoolScan>,
}

impl Pool {
    pub fn errors(&self) -> u64 {
        self.read_errors + self.write_errors + self.checksum_errors
    }

    /// How full, in whole percent: rounded, as the agent reads the space in
    /// steps of a hundredth of the pool, each a little under a percent.
    pub fn percent_used(&self) -> u64 {
        if self.size_bytes == 0 {
            0
        } else {
            let size = u128::from(self.size_bytes);
            ((u128::from(self.alloc_bytes) * 100 + size / 2) / size) as u64
        }
    }

    /// When its last scrub ended, if one did.
    pub fn scrubbed_ms(&self) -> Option<i64> {
        self.scan
            .as_ref()
            .filter(|s| s.function == "scrub" && s.state == "finished")
            .and_then(|s| s.end_ms)
    }

    /// Whether a scrub or resilver runs now.
    pub fn scanning(&self) -> bool {
        self.scan.as_ref().is_some_and(|s| s.state == "scanning")
    }

    /// Healthy: nothing missing or failing.
    pub fn healthy(&self) -> bool {
        self.state == "ONLINE"
    }
}

/// A pool's scrub or resilver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolScan {
    /// `scrub` or `resilver`.
    pub function: String,
    /// `scanning`, `finished` or `canceled`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<i64>,
    /// What it found and could not repair.
    #[serde(default)]
    pub errors: u64,
}

/// A check of the node's host (`legion doctor`) found not met.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLack {
    /// The check's id: `module.pf`, `pf.anchors`, `zfs.root`, `racct`…
    pub id: String,
    /// `required` (the node takes no new tasks) or `advised`.
    pub need: String,
    /// `unmet`, `unsupported`, or `unknown` (it could not be read, which
    /// says nothing against the host, and blocks nothing).
    pub state: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

impl HostLack {
    /// Found lacking, not merely unread.
    pub fn found(&self) -> bool {
        matches!(self.state.as_str(), "unmet" | "unsupported")
    }
}

impl NodeStatus {
    /// Why the node's host takes no new work (P1.1): a required check
    /// found lacking, or one the node's spec requires (`spec.require`).
    /// What runs there stays: the operator drains it to move it.
    pub fn host_blocked(&self, spec: &NodeSpec) -> Option<String> {
        self.blocking(spec)
            .next()
            .map(|l| format!("the host lacks {}: {}", l.id, l.detail))
    }

    /// Each of [`NodeStatus::lacks`] that keeps new work off.
    pub fn blocking<'a>(&'a self, spec: &'a NodeSpec) -> impl Iterator<Item = &'a HostLack> {
        self.lacks
            .iter()
            .filter(|l| l.found() && (l.need == "required" || spec.require.contains(&l.id)))
    }
}

/// Where a node's drain stands, as the leader last saw it: what still
/// holds the node, each with a stable reason a tool can act on. Unknown
/// never counts as released: a node that can't be reached, or a task
/// orphaned there and not yet seen to stop, blocks the drain.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainStatus {
    pub phase: DrainPhase,
    /// Why it is blocked: `unreachable` (the node isn't ready, so what it
    /// runs is unknown) or `unconfirmed` (a task orphaned there).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// What still holds the node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remaining: Vec<DrainItem>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DrainPhase {
    /// Its tasks are moving off, and its data with them.
    #[default]
    Draining,
    /// It can't go on by itself: see the reason.
    Blocked,
    /// Nothing runs on it and none of its data is moving: it may be
    /// stopped.
    Completed,
}

/// A task or Volume a drain waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainItem {
    /// `Task` or `Volume`.
    pub kind: String,
    pub namespace: String,
    pub name: String,
    /// For a task still told to run: `budget` (its service's disruption
    /// budget holds it), `data` (its data is copied first),
    /// `replacement` (its replacement starts first), or `running` (not yet
    /// planned). Told to stop: `stopping`, or `unconfirmed` (orphaned, not
    /// yet seen to stop). For a Volume: `copying` or `finishing`, as its
    /// data moves off, or `waiting` for its move to start (again, after
    /// one called off as the node it went to stopped being ready).
    pub reason: String,
    /// Why the last attempt failed, for a Volume whose move is tried
    /// again (a transfer cut short, or a node not there to take part).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// A node's end of the WireGuard mesh.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireGuard {
    /// Base64, as `wg` prints it.
    pub public_key: String,
    /// `host:port` other nodes send to. Without one, they wait for this
    /// node to reach them first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Behind a NAT no other node gets through: two such nodes reach each
    /// other through a relay (`--mesh-nat`, DESIGN.md §5.3).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nat: bool,
    /// Relays between nodes behind NAT (`--mesh-relay`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub relay: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    #[default]
    Unknown,
    Ready,
    /// Silent for a few heartbeats: its tasks get no new connections and it
    /// no new tasks, but they are not replaced yet. Ready again when heard
    /// from, down if it stays silent.
    Suspect,
    Down,
}

impl NodeState {
    pub const ALL: [NodeState; 4] = [
        NodeState::Unknown,
        NodeState::Ready,
        NodeState::Suspect,
        NodeState::Down,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Unknown => "unknown",
            NodeState::Ready => "ready",
            NodeState::Suspect => "suspect",
            NodeState::Down => "down",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Platform {
    /// `freebsd`.
    pub os: String,
    /// OCI architecture name: `amd64`, `arm64`.
    pub arch: String,
    /// Whether the node runs Linux images too, through the Linux ABI.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub linux: bool,
    /// Whether containers' limits cannot hold here: rctl needs
    /// `kern.racct.enable=1`, a loader tunable. Pods with limits are not
    /// placed on such a node. (The negative, so that nodes reporting
    /// nothing, as older agents do, still take them.)
    #[serde(
        default,
        rename = "noLimits",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub no_limits: bool,
}

impl Platform {
    /// What decides which images run: the OS, the architecture, and
    /// whether Linux ones do too.
    pub fn runs(&self) -> (&str, &str, bool) {
        (&self.os, &self.arch, self.linux)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub cpu: Cpu,
    pub memory: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lack(id: &str, need: &str, state: &str) -> HostLack {
        HostLack {
            id: id.to_owned(),
            need: need.to_owned(),
            state: state.to_owned(),
            detail: format!("{id} is wanting"),
            remedy: None,
        }
    }

    fn status(lacks: Vec<HostLack>) -> NodeStatus {
        NodeStatus {
            platform: Platform {
                os: "freebsd".into(),
                arch: "amd64".into(),
                linux: true,
                no_limits: false,
            },
            lacks,
            ..NodeStatus::default()
        }
    }

    #[test]
    fn a_required_check_found_lacking_blocks_new_work() {
        let spec = NodeSpec::default();
        assert_eq!(status(vec![]).host_blocked(&spec), None);
        let s = status(vec![lack("pf.anchors", "required", "unmet")]);
        assert_eq!(
            s.host_blocked(&spec).as_deref(),
            Some("the host lacks pf.anchors: pf.anchors is wanting")
        );
        let s = status(vec![lack("module.pf", "required", "unsupported")]);
        assert!(s.host_blocked(&spec).is_some());
        // Unread says nothing against the host.
        let s = status(vec![lack("pf.anchors", "required", "unknown")]);
        assert_eq!(s.host_blocked(&spec), None);
    }

    #[test]
    fn an_advised_check_blocks_only_where_the_spec_requires_it() {
        let s = status(vec![lack("forwarding", "advised", "unmet")]);
        assert_eq!(s.host_blocked(&NodeSpec::default()), None);
        let mut spec: NodeSpec = serde_json::from_value(serde_json::json!({
            "require": ["forwarding", "forwarding"]
        }))
        .unwrap();
        spec.admit().unwrap();
        assert_eq!(spec.require, ["forwarding"]);
        assert!(s.host_blocked(&spec).is_some());
        for bad in ["", "Forwarding", "pf anchors"] {
            let mut spec = NodeSpec {
                require: vec![bad.to_owned()],
                ..NodeSpec::default()
            };
            assert!(spec.admit().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_status_without_lacks_reads_as_before() {
        let s: NodeStatus = serde_json::from_value(serde_json::json!({"state": "ready"})).unwrap();
        assert!(s.lacks.is_empty());
        let v = serde_json::to_value(&s).unwrap();
        assert!(v.get("lacks").is_none());
    }
}
