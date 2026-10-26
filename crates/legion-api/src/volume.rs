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

/// Written by the agent of the node holding the dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeStatus {
    /// The node holding the dataset; empty until first used.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dataset: String,
}
