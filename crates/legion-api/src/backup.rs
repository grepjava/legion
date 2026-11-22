//! Where Volumes are backed up (DESIGN.md §4.3, Volume backups): a
//! cluster-scoped `BackupTarget`, a ZFS host reached over SSH or a
//! directory on the node holding the data (an NFS mount, a second disk),
//! named by a Volume's `backup: { target, every, keep }`.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::quantity::Duration;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupTargetSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh: Option<SshTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<DirectoryTarget>,
}

/// A ZFS host reached over SSH: each Volume's backups are received into
/// `<dataset>/<namespace>.<volume>`, incrementally after the first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SshTarget {
    pub host: String,
    #[serde(default = "ssh_port")]
    pub port: u16,
    /// Who logs in; they run `zfs receive`, `zfs list` and `zfs destroy`
    /// under `dataset` (root, or a user `zfs allow` lets).
    pub user: String,
    pub dataset: String,
    /// The Secret whose value is the private key (`legion secret create
    /// backup-key --from-file id_ed25519`).
    pub key_secret: SecretRef,
    /// The host's public key, as `ssh-keyscan` prints it without the
    /// host name (`ssh-ed25519 AAAA…`): no other host is believed.
    pub host_key: String,
}

fn ssh_port() -> u16 {
    22
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    pub namespace: String,
    pub name: String,
}

/// A directory on the node holding each Volume's data, mounted there by
/// the operator (NFS, a second disk): each backup a file, whole, in the
/// format `legion volume backup` writes, under `<path>/<namespace>.<volume>/`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryTarget {
    pub path: String,
}

impl Spec for BackupTargetSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        match (&self.ssh, &self.directory) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(Invalid::new("", "give one of ssh and directory"));
            }
            (Some(s), None) => {
                let plain = |v: &str| {
                    !v.is_empty()
                        && v.bytes().all(|b| {
                            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':')
                        })
                };
                if !plain(&s.host) {
                    return Err(Invalid::new("ssh.host", "a host name or address"));
                }
                if !plain(&s.user) || s.user.contains(':') {
                    return Err(Invalid::new("ssh.user", "a user name"));
                }
                if s.port == 0 {
                    return Err(Invalid::new("ssh.port", "1 to 65535"));
                }
                let dataset_ok = !s.dataset.is_empty()
                    && !s.dataset.starts_with('/')
                    && !s.dataset.ends_with('/')
                    && s.dataset.bytes().all(|b| {
                        b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'-' | b'_' | b':')
                    });
                if !dataset_ok {
                    return Err(Invalid::new(
                        "ssh.dataset",
                        "a ZFS dataset, as tank/backups",
                    ));
                }
                crate::validate_name(&s.key_secret.namespace)
                    .map_err(|e| Invalid::new("ssh.keySecret.namespace", e.to_string()))?;
                crate::validate_name(&s.key_secret.name)
                    .map_err(|e| Invalid::new("ssh.keySecret.name", e.to_string()))?;
                let words: Vec<_> = s.host_key.split_whitespace().collect();
                if words.len() < 2
                    || !words[0].starts_with("ssh-") && !words[0].starts_with("ecdsa-")
                {
                    return Err(Invalid::new(
                        "ssh.hostKey",
                        "the host's public key, as `ssh-keyscan HOST` prints it after the host: ssh-ed25519 AAAA…",
                    ));
                }
            }
            (None, Some(d)) => {
                if !d.path.starts_with('/') || d.path.contains("..") || d.path.len() < 2 {
                    return Err(Invalid::new(
                        "directory.path",
                        "an absolute path, without ..",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// A Volume's backups: to `target`, every `every`, the last `keep` kept
/// there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupSchedule {
    pub target: String,
    pub every: Duration,
    pub keep: u32,
}

impl BackupSchedule {
    pub const MIN_EVERY: Duration = Duration::from_secs(60);
    pub const MOST_KEPT: u32 = 1000;
    /// What its snapshots start with: `legion-bk-`.
    pub const PREFIX: &str = "legion-bk-";

    /// The snapshot (and backup) taken at `ms`: `legion-bk-offsite-20261122T143005Z`.
    pub fn name_at(&self, ms: i64) -> String {
        format!(
            "{}{}-{}",
            Self::PREFIX,
            self.target,
            crate::volume::stamp(ms)
        )
    }

    /// Those of this schedule's target: `legion-bk-offsite-`.
    pub fn prefix(&self) -> String {
        format!("{}{}-", Self::PREFIX, self.target)
    }

    pub fn check(&self, field: &str) -> Result<(), Invalid> {
        crate::validate_name(&self.target)
            .map_err(|e| Invalid::new(format!("{field}.target"), e.to_string()))?;
        if self.every < Self::MIN_EVERY {
            return Err(Invalid::new(format!("{field}.every"), "at least 1m"));
        }
        if self.keep == 0 || self.keep > Self::MOST_KEPT {
            return Err(Invalid::new(
                format!("{field}.keep"),
                format!("1 to {}", Self::MOST_KEPT),
            ));
        }
        Ok(())
    }
}

/// A Volume's backups as the leader last ran them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    pub target: String,
    /// The last backup the target holds, the next one's base, and the
    /// node it was taken on (a move makes the next one whole).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taken_ms: Option<i64>,
    /// Why the last run failed, and how many have since one worked.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    /// The last runs, newest last, at most [`BackupStatus::KEPT`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runs: Vec<BackupRun>,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl BackupStatus {
    pub const KEPT: usize = 10;

    pub fn record(&mut self, run: BackupRun) {
        self.runs.push(run);
        let over = self.runs.len().saturating_sub(Self::KEPT);
        self.runs.drain(..over);
    }
}

/// One backup run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRun {
    pub at_ms: i64,
    pub took_ms: u64,
    pub bytes: u64,
    /// Whole, rather than what changed since the last.
    #[serde(default)]
    pub whole: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ssh() -> SshTarget {
        SshTarget {
            host: "backup.example".into(),
            port: 22,
            user: "legion".into(),
            dataset: "tank/backups".into(),
            key_secret: SecretRef {
                namespace: "legion-ops".into(),
                name: "backup-key".into(),
            },
            host_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample".into(),
        }
    }

    #[test]
    fn a_target_is_one_of_ssh_or_a_directory() {
        let mut both = BackupTargetSpec {
            ssh: Some(ssh()),
            directory: Some(DirectoryTarget {
                path: "/backups".into(),
            }),
        };
        assert!(both.admit().is_err());
        assert!(BackupTargetSpec::default().admit().is_err());
        assert!(
            BackupTargetSpec {
                ssh: Some(ssh()),
                directory: None
            }
            .admit()
            .is_ok()
        );
        let dir = |p: &str| BackupTargetSpec {
            ssh: None,
            directory: Some(DirectoryTarget { path: p.into() }),
        };
        assert!(dir("/mnt/backups").admit().is_ok());
        assert!(dir("backups").admit().is_err());
        assert!(dir("/mnt/../etc").admit().is_err());
    }

    #[test]
    fn nothing_that_reaches_a_shell_is_let_through() {
        for (field, bad) in [
            ("host", "backup.example; rm -rf /"),
            ("user", "root@x"),
            ("dataset", "tank/b $(id)"),
            ("dataset", "/tank"),
        ] {
            let mut s = ssh();
            match field {
                "host" => s.host = bad.into(),
                "user" => s.user = bad.into(),
                _ => s.dataset = bad.into(),
            }
            let e = BackupTargetSpec {
                ssh: Some(s),
                directory: None,
            }
            .admit();
            assert!(e.is_err(), "{field} {bad:?}");
        }
        let mut s = ssh();
        s.host_key = "AAAA".into();
        assert!(
            BackupTargetSpec {
                ssh: Some(s),
                directory: None
            }
            .admit()
            .is_err()
        );
    }

    #[test]
    fn a_schedule_names_its_backups_after_its_target() {
        let b = BackupSchedule {
            target: "offsite".into(),
            every: Duration::from_secs(3600),
            keep: 7,
        };
        assert!(b.check("backup").is_ok());
        assert_eq!(
            b.name_at(1_795_357_805_000),
            "legion-bk-offsite-20261122T143005Z"
        );
        assert!(b.name_at(0).starts_with(&b.prefix()));
        let quick = BackupSchedule {
            every: Duration::from_secs(30),
            ..b.clone()
        };
        assert!(quick.check("backup").is_err());
    }

    #[test]
    fn the_last_runs_are_kept() {
        let mut s = BackupStatus::default();
        for i in 0..15 {
            s.record(BackupRun {
                at_ms: i,
                ..Default::default()
            });
        }
        assert_eq!(s.runs.len(), BackupStatus::KEPT);
        assert_eq!(s.runs[0].at_ms, 5);
    }
}
