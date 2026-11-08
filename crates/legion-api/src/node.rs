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
}

impl Spec for NodeSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
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
    /// For a task: `running` (not yet told to stop: waiting its turn under
    /// its service's disruption budget, or for its data to be copied),
    /// `stopping`, or `unconfirmed` (orphaned, not yet seen to stop). For a
    /// Volume: `copying` or `finishing`, as its data moves off.
    pub reason: String,
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
