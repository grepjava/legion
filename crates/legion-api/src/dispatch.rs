//! What managers and agents exchange over the dispatcher (DESIGN.md §3.4):
//! the tasks assigned to a node, every service's endpoints, and the outcome
//! of claiming a volume. These travel as JSON inside the dispatcher's
//! messages.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use crate::ObjectKey;
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
    pub vip: Option<Ipv4Addr>,
    pub ports: Vec<EndpointPort>,
    /// Addresses of the service's ready tasks, in order.
    pub ready: Vec<Ipv4Addr>,
    /// Addresses of its running tasks, ready or not, in order.
    pub alive: Vec<Ipv4Addr>,
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

/// The outcome of claiming a volume for a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeClaim {
    /// The volume is the node's now, or was already.
    Claimed(VolumeSpec),
    Missing,
    /// Another node holds the volume's data.
    Elsewhere(String),
}
