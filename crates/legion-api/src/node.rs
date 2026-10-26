//! `Node`: one per machine running `legion daemon`. The agent creates it and
//! writes its status; operators change the spec (drain, pause). Node labels
//! are the object's labels.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::quantity::{Bytes, Cpu};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    #[serde(default)]
    pub availability: Availability,
    #[serde(default)]
    pub role: Role,
}

impl Spec for NodeSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        Ok(())
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
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeState {
    #[default]
    Unknown,
    Ready,
    Down,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Platform {
    /// `freebsd`.
    pub os: String,
    /// OCI architecture name: `amd64`, `arm64`.
    pub arch: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub cpu: Cpu,
    pub memory: Bytes,
}
