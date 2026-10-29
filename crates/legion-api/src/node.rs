//! `Node`: one per machine running `legion daemon`. The agent creates it and
//! writes its status; operators change the spec (drain, pause). Node labels
//! are the object's labels.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, field, index};
use crate::quantity::{Bytes, Cpu};
use crate::service::TaintEffect;

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
    /// This node's part of the cluster pod network, e.g. `10.200.3.0/24`.
    /// Assigned once, when the node first registers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_cidr: Option<String>,
    /// How other nodes reach this one over the WireGuard mesh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wireguard: Option<WireGuard>,
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
    Down,
}

impl NodeState {
    pub const ALL: [NodeState; 3] = [NodeState::Unknown, NodeState::Ready, NodeState::Down];

    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Unknown => "unknown",
            NodeState::Ready => "ready",
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub cpu: Cpu,
    pub memory: Bytes,
}
