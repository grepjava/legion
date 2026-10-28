//! `Volume`: a named ZFS dataset that outlives the tasks mounting it
//! (DESIGN.md §6.4).

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::quantity::Bytes;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSpec {
    #[serde(default)]
    pub driver: VolumeDriver,
    /// ZFS quota; none by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeDriver {
    #[default]
    Zfs,
}

impl Spec for VolumeSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        if self.size == Some(Bytes(0)) {
            return Err(Invalid::new("size", "must be above zero"));
        }
        Ok(())
    }
}

/// Written by the agent of the node holding the dataset, and by the
/// managers moving it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeStatus {
    /// The node holding the dataset; empty until first used.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dataset: String,
    /// The data moving to another node (DESIGN.md §4.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<Migration>,
}

/// A Volume's data moving to another node: copied whole while its tasks
/// run, then, with them stopped, what changed since; then the Volume is the
/// other node's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Migration {
    pub to: String,
    pub phase: MigrationPhase,
    /// Why the last attempt at the phase failed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationPhase {
    /// Sending a full copy while the tasks using it run.
    Copying,
    /// The copy is on the new node; the tasks using it may stop.
    Copied,
    /// The tasks have stopped: sending what changed since the copy.
    Finishing,
}
