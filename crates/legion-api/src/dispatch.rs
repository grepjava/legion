//! What managers and agents exchange over the dispatcher (DESIGN.md §3.4):
//! the tasks assigned to a node, every service's endpoints, the nodes of
//! the WireGuard mesh, network policy, and the outcome of claiming a
//! volume. These travel as JSON inside the dispatcher's messages.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use crate::ObjectKey;
use crate::policy::{Isolation, NetworkPolicySpec};
use crate::service::Protocol;
use crate::task::{TaskSpec, TaskStatus};
use crate::volume::VolumeSpec;

/// A task assigned to a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub key: ObjectKey,
    /// Identifies this run; a task recreated under the same name gets a new
    /// uid.
    pub uid: String,
    pub spec: TaskSpec,
    pub status: TaskStatus,
}

/// Every task assigned to a node, by uid.
pub type Assignments = BTreeMap<String, Assignment>;

/// A service's addresses, for DNS and load balancing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceEndpoints {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// The Service's labels, which network policies select by.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub vip: Option<Ipv4Addr>,
    pub ports: Vec<EndpointPort>,
    /// Addresses of the service's ready tasks, in order.
    pub ready: Vec<Ipv4Addr>,
    /// Addresses of its running tasks, ready or not, in order.
    pub alive: Vec<Ipv4Addr>,
    /// A stateful service's ready tasks, by ordinal.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ordinals: BTreeMap<u32, Ipv4Addr>,
}

/// A service port, resolved against the pod.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointPort {
    pub protocol: Protocol,
    pub port: u16,
    /// The tasks' port.
    pub target: u16,
    /// Where it is published, if it is.
    pub published: Option<u16>,
}

/// Every service in the cluster, by namespace and name.
pub type Endpoints = Vec<ServiceEndpoints>;

/// What may reach which tasks, cluster-wide (DESIGN.md §5.6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policies {
    /// Namespaces whose isolation is not the default.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub isolation: BTreeMap<String, Isolation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<NamespacedPolicy>,
}

impl Policies {
    pub fn isolation(&self, namespace: &str) -> Isolation {
        self.isolation.get(namespace).copied().unwrap_or_default()
    }
}

/// A `NetworkPolicy`, with where it applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacedPolicy {
    pub namespace: String,
    pub name: String,
    pub spec: NetworkPolicySpec,
}

/// A node of the WireGuard mesh (DESIGN.md §5.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub node: String,
    pub public_key: String,
    /// `host:port`, if the node gave one.
    pub endpoint: Option<String>,
    /// The node's pod subnet, e.g. `10.200.3.0/24`.
    pub pod_cidr: String,
}

/// Every node with a pod subnet and a WireGuard key, by name.
pub type Peers = Vec<Peer>;

/// The outcome of claiming a volume for a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeClaim {
    /// The volume is the node's now, or was already.
    Claimed(VolumeSpec),
    Missing,
    /// Another node holds the volume's data.
    Elsewhere(String),
}

/// What a running task's containers use together, as its node last read
/// it from rctl.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskUsage {
    /// In percent of one CPU.
    pub cpu: f64,
    pub memory: u64,
}
