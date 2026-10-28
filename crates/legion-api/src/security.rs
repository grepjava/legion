//! Jail profiles (DESIGN.md §9). Every container's jail is locked down: no
//! raw sockets, no changing file flags, no mlock, no SysV IPC, no mounting,
//! `enforce_statfs=2` and `securelevel=2`. A container's `security` loosens
//! that, and only as far as its namespace's `security.allow` lets it, which
//! only the cluster admin sets.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, field};
use crate::service::PodSpec;

pub const SECURELEVEL: i8 = 2;
pub const ENFORCE_STATFS: u8 = 2;

/// What a container's jail may be allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Allow {
    /// Raw sockets, as `ping` and `traceroute` use.
    RawSockets,
    /// Changing file flags (`chflags`), below securelevel 1.
    Chflags,
    /// Locking memory (`mlock`).
    Mlock,
    /// SysV message queues, semaphores and shared memory, shared by the
    /// pod's containers that allow it.
    Sysvipc,
}

/// A way a container may loosen its jail, as a namespace allows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Loosening {
    RawSockets,
    Chflags,
    Mlock,
    Sysvipc,
    /// A `securelevel` below 2.
    Securelevel,
    /// `enforceStatfs: 1`.
    EnforceStatfs,
}

impl From<Allow> for Loosening {
    fn from(a: Allow) -> Self {
        match a {
            Allow::RawSockets => Loosening::RawSockets,
            Allow::Chflags => Loosening::Chflags,
            Allow::Mlock => Loosening::Mlock,
            Allow::Sysvipc => Loosening::Sysvipc,
        }
    }
}

impl Loosening {
    pub fn as_str(self) -> &'static str {
        match self {
            Loosening::RawSockets => "rawSockets",
            Loosening::Chflags => "chflags",
            Loosening::Mlock => "mlock",
            Loosening::Sysvipc => "sysvipc",
            Loosening::Securelevel => "securelevel",
            Loosening::EnforceStatfs => "enforceStatfs",
        }
    }
}

/// A container's jail profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Security {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub allow: BTreeSet<Allow>,
    /// The jail's `securelevel`, -1 to 3.
    #[serde(default = "securelevel")]
    pub securelevel: i8,
    /// Which mounts the jail sees: 2 its root only, 1 those below it.
    #[serde(default = "enforce_statfs")]
    pub enforce_statfs: u8,
}

fn securelevel() -> i8 {
    SECURELEVEL
}

fn enforce_statfs() -> u8 {
    ENFORCE_STATFS
}

impl Default for Security {
    fn default() -> Self {
        Security {
            allow: BTreeSet::new(),
            securelevel: SECURELEVEL,
            enforce_statfs: ENFORCE_STATFS,
        }
    }
}

impl Security {
    pub fn is_default(&self) -> bool {
        *self == Security::default()
    }

    pub(crate) fn admit(&self, path: &str) -> Result<(), Invalid> {
        if !(-1..=3).contains(&self.securelevel) {
            return Err(Invalid::new(field(path, "securelevel"), "must be -1 to 3"));
        }
        // A pod's containers are children of its infra jail, which has 1;
        // a child may not see more than its parent.
        if !(1..=2).contains(&self.enforce_statfs) {
            return Err(Invalid::new(field(path, "enforceStatfs"), "must be 1 or 2"));
        }
        Ok(())
    }

    /// How this profile loosens the defaults.
    pub fn loosenings(&self) -> BTreeSet<Loosening> {
        let mut l: BTreeSet<Loosening> = self.allow.iter().map(|&a| a.into()).collect();
        if self.securelevel < SECURELEVEL {
            l.insert(Loosening::Securelevel);
        }
        if self.enforce_statfs < ENFORCE_STATFS {
            l.insert(Loosening::EnforceStatfs);
        }
        l
    }
}

/// How the pod's containers, init containers included, loosen their jails.
pub fn pod_loosenings(pod: &PodSpec) -> BTreeSet<Loosening> {
    pod.containers()
        .flat_map(|c| c.security.loosenings())
        .collect()
}

/// A namespace's part: the loosenings its containers may use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSecurity {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub allow: BTreeSet<Loosening>,
}

impl NamespaceSecurity {
    pub fn is_empty(&self) -> bool {
        self.allow.is_empty()
    }

    /// What `pod` uses that this does not allow.
    pub fn refused(&self, pod: &PodSpec) -> Vec<Loosening> {
        pod_loosenings(pod)
            .into_iter()
            .filter(|l| !self.allow.contains(l))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(yaml: &str) -> PodSpec {
        let mut p: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        p.admit("pod").unwrap();
        p
    }

    #[test]
    fn defaults_loosen_nothing() {
        let p = pod("containers: [{ name: a, image: x }]");
        assert!(p.containers[0].security.is_default());
        assert!(pod_loosenings(&p).is_empty());
        assert!(NamespaceSecurity::default().refused(&p).is_empty());
    }

    #[test]
    fn loosenings_of_a_pod() {
        let p = pod(
            "initContainers: [{ name: i, image: x, security: { securelevel: 0 } }]\n\
             containers:\n\
             - { name: a, image: x, security: { allow: [rawSockets, sysvipc] } }\n\
             - { name: b, image: x, security: { enforceStatfs: 1, securelevel: 3 } }",
        );
        assert_eq!(
            pod_loosenings(&p).into_iter().collect::<Vec<_>>(),
            [
                Loosening::RawSockets,
                Loosening::Sysvipc,
                Loosening::Securelevel,
                Loosening::EnforceStatfs
            ]
        );
        let ns: NamespaceSecurity =
            serde_yaml_ng::from_str("allow: [rawSockets, securelevel]").unwrap();
        assert_eq!(
            ns.refused(&p),
            [Loosening::Sysvipc, Loosening::EnforceStatfs]
        );
    }

    #[test]
    fn bounds() {
        for (yaml, path) in [
            (
                "{ securelevel: 4 }",
                "pod.containers[0].security.securelevel",
            ),
            (
                "{ securelevel: -2 }",
                "pod.containers[0].security.securelevel",
            ),
            (
                "{ enforceStatfs: 0 }",
                "pod.containers[0].security.enforceStatfs",
            ),
        ] {
            let mut p: PodSpec = serde_yaml_ng::from_str(&format!(
                "containers: [{{ name: a, image: x, security: {yaml} }}]"
            ))
            .unwrap();
            assert_eq!(p.admit("pod").unwrap_err().path, path, "{yaml}");
        }
        let unknown: Result<Security, _> = serde_yaml_ng::from_str("allow: [mount]");
        assert!(unknown.is_err());
    }
}
