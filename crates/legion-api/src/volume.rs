//! `Volume`: storage that outlives the tasks mounting it (DESIGN.md §6.4,
//! §6.7): a ZFS dataset on one node, an NFS export, an iSCSI LUN, or what a
//! node's volume plugin provides.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::quantity::Bytes;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSpec {
    #[serde(default)]
    pub driver: VolumeDriver,
    /// ZFS quota; none by default. ZFS only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<Bytes>,
    /// The node to keep the data on. Unset, the first task to use the
    /// Volume decides; set to another node, the data moves there, as off
    /// a draining node, its tasks with it (DESIGN.md §4.3). ZFS only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The export, for `driver: nfs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NfsSource>,
    /// The LUN, for `driver: iscsi`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iscsi: Option<IscsiSource>,
    /// The plugin and what it is told, for `driver: plugin`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<PluginSource>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeDriver {
    #[default]
    Zfs,
    Nfs,
    Iscsi,
    Plugin,
}

impl VolumeDriver {
    pub fn as_str(self) -> &'static str {
        match self {
            VolumeDriver::Zfs => "zfs",
            VolumeDriver::Nfs => "nfs",
            VolumeDriver::Iscsi => "iscsi",
            VolumeDriver::Plugin => "plugin",
        }
    }
}

/// How the tasks of a cluster may share a Volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// The data is on one node, and stays there unless moved (ZFS).
    Local,
    /// One node at a time, any number of its tasks; the node lets go once
    /// none of them uses it (iSCSI, a plugin's unshared volumes).
    Single,
    /// Any number of tasks on any nodes (NFS, a plugin's shared volumes).
    Shared,
}

/// An NFS export, mounted on each node using it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NfsSource {
    /// Host name or address of the server.
    pub server: String,
    /// The exported directory.
    pub path: String,
    /// `mount_nfs` options (`nfsv4`, `soft`, ...); `nfsv4` if none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// An iSCSI LUN with a UFS file system, which a node logs in to while its
/// tasks use it. A LUN without a file system gets a new one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IscsiSource {
    /// `host[:port]` of the target.
    pub portal: String,
    /// The target's name (`iqn.…`).
    pub target: String,
    #[serde(default)]
    pub lun: u32,
}

/// A volume a node's plugin provides: the executable `name` in the node's
/// plugin directory mounts and unmounts it (DESIGN.md §6.7).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginSource {
    pub name: String,
    /// Given to the plugin as they are.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
    /// Whether tasks on several nodes may use it at once; otherwise one
    /// node at a time.
    #[serde(default)]
    pub shared: bool,
}

impl VolumeSpec {
    pub fn access(&self) -> Access {
        match self.driver {
            VolumeDriver::Zfs => Access::Local,
            VolumeDriver::Iscsi => Access::Single,
            VolumeDriver::Nfs => Access::Shared,
            VolumeDriver::Plugin => match &self.plugin {
                Some(p) if p.shared => Access::Shared,
                _ => Access::Single,
            },
        }
    }
}

impl Spec for VolumeSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        if self.size == Some(Bytes(0)) {
            return Err(Invalid::new("size", "must be above zero"));
        }
        if let Some(node) = &self.node {
            crate::validate_name(node).map_err(|e| Invalid::new("node", e.to_string()))?;
        }
        let driver = self.driver;
        let not_zfs = |field: &str| {
            Invalid::new(
                field,
                format!("is for ZFS volumes, not {}", driver.as_str()),
            )
        };
        if driver != VolumeDriver::Zfs {
            if self.size.is_some() {
                return Err(not_zfs("size"));
            }
            if self.node.is_some() {
                return Err(not_zfs("node"));
            }
        }
        let sections = [
            ("nfs", self.nfs.is_some(), VolumeDriver::Nfs),
            ("iscsi", self.iscsi.is_some(), VolumeDriver::Iscsi),
            ("plugin", self.plugin.is_some(), VolumeDriver::Plugin),
        ];
        for (field, given, wants) in sections {
            if given != (driver == wants) {
                let why = if given {
                    format!("is for driver {field}, not {}", driver.as_str())
                } else {
                    format!("is needed with driver {field}")
                };
                return Err(Invalid::new(field, why));
            }
        }
        if let Some(n) = &self.nfs {
            if n.server.is_empty() || n.server.contains([':', '/', ' ']) && !is_ipv6(&n.server) {
                return Err(Invalid::new("nfs.server", "a host name or address"));
            }
            if !n.path.starts_with('/') {
                return Err(Invalid::new("nfs.path", "must be absolute"));
            }
            if let Some(o) = n.options.iter().find(|o| !option_word(o)) {
                return Err(Invalid::new("nfs.options", format!("bad option {o:?}")));
            }
        }
        if let Some(i) = &self.iscsi {
            if i.portal.is_empty() || i.portal.contains([' ', '/']) {
                return Err(Invalid::new("iscsi.portal", "host or host:port"));
            }
            if !(i.target.starts_with("iqn.")
                || i.target.starts_with("eui.")
                || i.target.starts_with("naa."))
                || i.target.contains(' ')
            {
                return Err(Invalid::new(
                    "iscsi.target",
                    "an iSCSI name (iqn., eui. or naa.)",
                ));
            }
        }
        if let Some(p) = &self.plugin {
            crate::validate_name(&p.name)
                .map_err(|e| Invalid::new("plugin.name", e.to_string()))?;
        }
        Ok(())
    }
}

fn is_ipv6(s: &str) -> bool {
    s.parse::<std::net::Ipv6Addr>().is_ok()
}

/// A `mount -o` word: no separators or spaces that would change the
/// command.
fn option_word(o: &str) -> bool {
    !o.is_empty()
        && o.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '=' | '_' | '-' | '.' | '/'))
}

/// Written by the agent of the node holding the dataset, and by the
/// managers moving it. For a volume used one node at a time, the node
/// using it, until it lets go.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn admit(yaml: &str) -> Result<VolumeSpec, String> {
        let mut s: VolumeSpec = serde_yaml_ng::from_str(yaml).map_err(|e| e.to_string())?;
        s.admit().map_err(|e| e.to_string())?;
        Ok(s)
    }

    #[test]
    fn drivers() {
        let zfs = admit("size: 1Gi").unwrap();
        assert_eq!(zfs.access(), Access::Local);
        let nfs = admit("driver: nfs\nnfs: { server: files.lan, path: /export/shop }").unwrap();
        assert_eq!(nfs.access(), Access::Shared);
        let iscsi = admit(
            "driver: iscsi\niscsi: { portal: '10.0.0.5:3260', target: 'iqn.2026-10.lan:shop', lun: 1 }",
        )
        .unwrap();
        assert_eq!(
            (iscsi.access(), iscsi.iscsi.unwrap().lun),
            (Access::Single, 1)
        );
        let p = admit("driver: plugin\nplugin: { name: ceph, options: { pool: rbd } }").unwrap();
        assert_eq!(p.access(), Access::Single);
        let p = admit("driver: plugin\nplugin: { name: s3fs, shared: true }").unwrap();
        assert_eq!(p.access(), Access::Shared);
        assert!(admit("driver: nfs\nnfs: { server: 'fd00::5', path: /x }").is_ok());
    }

    #[test]
    fn refused() {
        for (yaml, why) in [
            ("driver: nfs", "nfs: is needed"),
            (
                "nfs: { server: a, path: /x }",
                "nfs: is for driver nfs, not zfs",
            ),
            (
                "driver: nfs\nsize: 1Gi\nnfs: { server: a, path: /x }",
                "size: is for ZFS",
            ),
            (
                "driver: iscsi\nnode: n1\niscsi: { portal: a, target: iqn.x }",
                "node: is for ZFS",
            ),
            (
                "driver: nfs\nnfs: { server: a, path: x }",
                "must be absolute",
            ),
            (
                "driver: nfs\nnfs: { server: 'a b', path: /x }",
                "nfs.server",
            ),
            (
                "driver: nfs\nnfs: { server: a, path: /x, options: ['ro,suid'] }",
                "bad option",
            ),
            (
                "driver: iscsi\niscsi: { portal: a, target: x }",
                "iscsi.target",
            ),
            (
                "driver: plugin\nplugin: { name: 'Bad/Name' }",
                "plugin.name",
            ),
        ] {
            let e = admit(yaml).unwrap_err();
            assert!(e.contains(why), "{yaml}: {e}");
        }
    }
}
