//! `Volume`: storage that outlives the tasks mounting it (DESIGN.md §6.4,
//! §6.7): a ZFS dataset on one node, an NFS export, an iSCSI LUN, or what a
//! node's volume plugin provides.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::quantity::{Bytes, Duration};

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
    /// A copy kept on another node, sent every so often (ZFS only): when
    /// the node holding the data is removed, the Volume carries on from
    /// the last copy there, instead of being lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica: Option<Replica>,
    /// Snapshots taken on a schedule and pruned to how many are kept
    /// (ZFS only; DESIGN.md §4.3, Volume snapshot schedules).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<SnapshotSchedule>,
    /// Backups to a `BackupTarget` on a schedule (ZFS only; DESIGN.md
    /// §4.3, Volume backups).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<crate::backup::BackupSchedule>,
}

/// A snapshot every `every`, the last `keep` of them kept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotSchedule {
    pub every: Duration,
    pub keep: u32,
}

impl SnapshotSchedule {
    /// The fewest minutes apart, and the most schedules and kept snapshots.
    pub const MIN_EVERY: Duration = Duration::from_secs(60);
    pub const MOST: usize = 8;
    pub const MOST_KEPT: u32 = 1000;
    /// What every snapshot Legion takes or keeps starts with: a user's
    /// may not.
    pub const RESERVED: &str = "legion-";
    /// What a schedule's snapshots start with.
    pub const AUTO: &str = "legion-auto-";

    /// The schedule's own snapshots' prefix: `legion-auto-1h-`.
    pub fn prefix(&self) -> String {
        format!("{}{}-", Self::AUTO, self.every)
    }

    /// The name of the snapshot taken at `ms` since the epoch:
    /// `legion-auto-1h-20261122T143005Z`.
    pub fn name_at(&self, ms: i64) -> String {
        format!("{}{}", self.prefix(), stamp(ms))
    }
}

/// `20261122T143005Z`, UTC, from milliseconds since the epoch.
pub fn stamp(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rest) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// `2026-11-25T04:05:06.123Z` (or without the fraction, or the `Z`) in
/// Unix milliseconds, read as UTC.
pub fn iso_ms(t: &str) -> Option<i64> {
    let (date, time) = t.trim().trim_end_matches('Z').split_once('T')?;
    let mut d = date.split('-').map(|x| x.parse::<i64>());
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut h = hms.split(':').map(|x| x.parse::<i64>());
    let (hh, mm, ss) = (h.next()?.ok()?, h.next()?.ok()?, h.next()?.ok()?);
    if d.next().is_some()
        || h.next().is_some()
        || !(1..=12).contains(&m)
        || !(1..=31).contains(&day)
        || hh > 23
        || mm > 59
        || ss > 60
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let ms: i64 = format!("{frac:0<3}")[..3].parse().ok()?;
    // Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + hh) * 60 + mm) * 60_000 + ss * 1000 + ms)
}

/// The time a snapshot's or backup's name ends with ([`stamp`]), in Unix
/// milliseconds: `legion-bk-offsite-20261122T143005Z` (`.zfs` after it,
/// as a target holds it, is passed over).
pub fn stamp_ms(name: &str) -> Option<i64> {
    let name = name.trim_end_matches(".zfs");
    let s = name.get(name.len().checked_sub(16)?..)?;
    let b = s.as_bytes();
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !(digits(0..8) && b[8] == b'T' && digits(9..15) && b[15] == b'Z') {
        return None;
    }
    iso_ms(&format!(
        "{}-{}-{}T{}:{}:{}Z",
        &s[0..4],
        &s[4..6],
        &s[6..8],
        &s[9..11],
        &s[11..13],
        &s[13..15]
    ))
}

/// Checks schedules given at `field`: each at least a minute apart, none
/// twice, keeping 1 to 1000, at most 8.
pub fn check_schedules(schedules: &[SnapshotSchedule], field: &str) -> Result<(), Invalid> {
    if schedules.len() > SnapshotSchedule::MOST {
        return Err(Invalid::new(
            field,
            format!("at most {} schedules", SnapshotSchedule::MOST),
        ));
    }
    for (i, s) in schedules.iter().enumerate() {
        if s.every < SnapshotSchedule::MIN_EVERY {
            return Err(Invalid::new(format!("{field}[{i}].every"), "at least 1m"));
        }
        if s.keep == 0 || s.keep > SnapshotSchedule::MOST_KEPT {
            return Err(Invalid::new(
                format!("{field}[{i}].keep"),
                format!("1 to {}", SnapshotSchedule::MOST_KEPT),
            ));
        }
        if schedules[..i].iter().any(|o| o.every == s.every) {
            return Err(Invalid::new(
                format!("{field}[{i}].every"),
                format!("{} is given twice: one schedule an interval", s.every),
            ));
        }
    }
    Ok(())
}

/// Where a ZFS Volume's replica is kept, and how often it is brought up to
/// date.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Replica {
    /// The node keeping it; unset, the leader picks one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Seconds between copies: what may be lost with the node holding the
    /// data. [`Replica::DEFAULT_EVERY`] when unset, at least
    /// [`Replica::MIN_EVERY`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_seconds: Option<u32>,
    /// Who hands the Volume to the replica when the node holding the data
    /// is gone: `manual` (the default), the operator, by removing the node;
    /// `automatic`, the leader, once the node has been down long enough
    /// that its agent has stopped the tasks using it. Automatic needs every
    /// task using the Volume to be one that must not run twice
    /// (`onPartition: stop`), so nothing writes to the old copy as the
    /// replica takes over (docs/PARTITIONS.md).
    #[serde(default, skip_serializing_if = "Failover::is_manual")]
    pub failover: Failover,
}

/// Who hands a Volume to its replica: see [`Replica::failover`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Failover {
    #[default]
    Manual,
    Automatic,
}

impl Failover {
    pub fn is_manual(&self) -> bool {
        *self == Failover::Manual
    }
}

impl Replica {
    pub const DEFAULT_EVERY: u32 = 300;
    pub const MIN_EVERY: u32 = 10;

    pub fn every(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.every_seconds.unwrap_or(Self::DEFAULT_EVERY).into())
    }
}

/// A ZFS Volume's replica, as the leader last brought it up to date.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    pub node: String,
    /// Its last snapshot (`legion-replica-<ms>`), the next copy's base;
    /// empty until the first copy is taken.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub snapshot: String,
    /// The node it was copied from: the next copy is whole if the data
    /// has moved since.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,
    /// When the last copy was taken, in milliseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taken_ms: Option<i64>,
    /// Why the last attempt failed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// What the copy kept there amounts to, as the leader last weighed it
    /// ([`ReplicaStatus::settled`]); a copy under way does not change it.
    #[serde(default, skip_serializing_if = "ReplicaState::is_pending")]
    pub state: ReplicaState,
    /// When the copy under way began, in milliseconds since the epoch;
    /// unset while none is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copying_ms: Option<i64>,
    /// The last copies, newest last, at most [`ReplicaStatus::KEPT`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copies: Vec<ReplicaCopy>,
    /// Why the Volume would not fail over on its own if its node went down
    /// now; empty when it would, or when its failover is manual.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<String>,
    /// A copy cut short (the link dropped, a daemon restarted) of which the
    /// replica's node kept what it took: the next attempt goes on from
    /// there rather than starting again, as long as the data is still on
    /// `from`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial: Option<PartialCopy>,
}

/// What a replica's copy that cannot be resumed says, the agent sending
/// it to the manager: the next starts again.
pub const NOT_RESUMABLE: &str = "the copy cut short cannot be resumed";

/// What a copy cut short says when the receiving node is still taking in
/// the attempt before it (cut off as its sender stopped, it lets go of the
/// dataset only once it notices, seconds later): nothing is wrong with
/// what it kept, so the attempt is no sign the copy cannot go on.
pub const RECEIVE_BUSY: &str = "the attempt before is still being taken in here";

/// Whether a copy's error is [`RECEIVE_BUSY`]; also as an agent from
/// before it says so, in ZFS's words (EBUSY on a resume: the dataset still
/// held by a receive).
pub fn receive_busy(error: &str) -> bool {
    error.contains(RECEIVE_BUSY) || error.contains("contains partially-complete state")
}

/// The snapshot of a move's full copy, on both nodes.
pub const MOVE_FULL: &str = "legion-move-1";

/// A replica's copy cut short, to go on with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialCopy {
    /// The snapshot it sends.
    pub snapshot: String,
    /// The snapshot it sends what changed since; empty for a whole copy.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base: String,
    /// The node it comes from.
    pub from: String,
    /// Attempts to go on with it that failed in a row, moving next to
    /// nothing: after [`PartialCopy::TRIES`], it is given up and the next
    /// copy starts again.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub tries: u32,
}

impl PartialCopy {
    /// Failed attempts to go on, without getting anywhere, before starting
    /// again.
    pub const TRIES: u32 = 3;
    /// What an attempt must move to count as getting somewhere.
    pub const PROGRESS: u64 = 16 << 20;
}

/// One copy to a Volume's replica, or an attempt at one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCopy {
    /// When it began, in milliseconds since the epoch.
    pub at_ms: i64,
    /// How long it took.
    pub took_ms: u64,
    /// What was sent: the whole Volume, or what changed since the last.
    #[serde(default)]
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub whole: bool,
    /// Why it failed; empty when it did not.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

/// What a Volume's replica amounts to (see [`ReplicaStatus::settled`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaState {
    /// No copy yet.
    #[default]
    Pending,
    /// A copy at most [`ReplicaStatus::STALE_AFTER`] intervals old.
    Ready,
    /// The last copy older than that, whether or not the attempts since
    /// failed (the message says why they did): what a copy is worth comes
    /// first.
    Stale,
    /// The last attempt failed, or there is nowhere to keep it, while the
    /// last copy, if any, is not yet stale.
    Failed,
}

impl ReplicaState {
    pub fn is_pending(&self) -> bool {
        *self == ReplicaState::Pending
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ReplicaState::Pending => "pending",
            ReplicaState::Ready => "ready",
            ReplicaState::Stale => "stale",
            ReplicaState::Failed => "failed",
        }
    }
}

impl ReplicaStatus {
    /// Copies kept in [`ReplicaStatus::copies`].
    pub const KEPT: usize = 10;
    /// Intervals after which the last copy is stale.
    pub const STALE_AFTER: u32 = 3;

    /// Whether the last copy is older than [`Self::STALE_AFTER`] intervals
    /// of `every` at `now_ms`; never before the first.
    pub fn stale_at(&self, every: std::time::Duration, now_ms: i64) -> bool {
        let limit = every.as_millis() as i64 * i64::from(Self::STALE_AFTER);
        !self.snapshot.is_empty() && self.taken_ms.is_none_or(|t| now_ms - t > limit)
    }

    /// What the replica amounts to at `now_ms`: stale once the last copy
    /// is too old, as the replica-stale alert says, whatever the attempts
    /// since; else failed while the last attempt did; else pending until
    /// the first copy, ready after it.
    pub fn settled(&self, every: std::time::Duration, now_ms: i64) -> ReplicaState {
        if self.stale_at(every, now_ms) {
            ReplicaState::Stale
        } else if !self.message.is_empty() {
            ReplicaState::Failed
        } else if self.snapshot.is_empty() {
            ReplicaState::Pending
        } else {
            ReplicaState::Ready
        }
    }

    /// `copy` added to the copies kept, the oldest going past
    /// [`Self::KEPT`].
    pub fn record(&mut self, copy: ReplicaCopy) {
        self.copies.push(copy);
        let over = self.copies.len().saturating_sub(Self::KEPT);
        self.copies.drain(..over);
    }
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
    /// Host name or address of the server; empty with `node`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server: String,
    /// The exported directory; empty with `node`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// A node of the cluster that provides the export itself, instead: a
    /// ZFS dataset of its own, shared with the cluster's nodes, with the
    /// Volume's `size` as its quota.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// `mount_nfs` options (`nfsv4`, `soft`, ...); `nfsv4` if none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// An iSCSI LUN with a UFS file system, which a node logs in to while its
/// tasks use it. A LUN without a file system gets a new one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IscsiSource {
    /// `host[:port]` of the target; empty with `node`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub portal: String,
    /// The target's name (`iqn.…`); empty with `node`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(default)]
    pub lun: u32,
    /// A node of the cluster that provides the LUN itself, instead: a ZFS
    /// volume of the Volume's `size`, formatted UFS once, a target of the
    /// node's ctld.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

/// Where a provisioned NFS or iSCSI Volume is exported, as its node said:
/// what the nodes using it mount.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Export {
    /// The node providing it.
    pub node: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub server: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub portal: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    /// Why it isn't exported (yet), when making it failed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

impl Export {
    /// Whether it may be mounted: made, and saying where.
    pub fn ready(&self) -> bool {
        self.message.is_empty() && (!self.path.is_empty() || !self.target.is_empty())
    }
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
    /// The node providing it, for a provisioned NFS or iSCSI Volume.
    pub fn provider(&self) -> Option<&str> {
        match self.driver {
            VolumeDriver::Nfs => self.nfs.as_ref()?.node.as_deref(),
            VolumeDriver::Iscsi => self.iscsi.as_ref()?.node.as_deref(),
            _ => None,
        }
    }

    /// This spec with its provisioned source filled in from `export`, as
    /// the nodes using it mount it; `None` while it isn't ready. Others as
    /// they are.
    pub fn resolved(&self, export: Option<&Export>) -> Option<VolumeSpec> {
        let Some(node) = self.provider() else {
            return Some(self.clone());
        };
        let e = export.filter(|e| e.node == node && e.ready())?;
        let mut spec = self.clone();
        if let Some(n) = &mut spec.nfs {
            n.server.clone_from(&e.server);
            n.path.clone_from(&e.path);
        }
        if let Some(i) = &mut spec.iscsi {
            i.portal.clone_from(&e.portal);
            i.target.clone_from(&e.target);
            i.lun = 0;
        }
        Some(spec)
    }

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

    /// Whether `other` names the same storage: the driver and its source.
    /// A ZFS volume's size and node may change under its tasks; these not.
    pub fn same_storage(&self, other: &VolumeSpec) -> bool {
        self.driver == other.driver
            && self.nfs == other.nfs
            && self.iscsi == other.iscsi
            && self.plugin == other.plugin
    }

    /// What a Volume used by one node at a time stands for, alike for every
    /// Volume naming the same storage: an iSCSI target's LUN, whichever
    /// portal reaches it (target names are case-insensitive), or a plugin's
    /// volume with its options. `None` for storage several nodes share, and
    /// for ZFS, which is each Volume's own dataset.
    pub fn exclusive_storage(&self) -> Option<String> {
        // A LUN a node provides is its Volume's own: no other names it.
        if self.access() != Access::Single || self.provider().is_some() {
            return None;
        }
        match self.driver {
            VolumeDriver::Iscsi => self
                .iscsi
                .as_ref()
                .map(|i| format!("iscsi {} {}", i.target.trim().to_ascii_lowercase(), i.lun)),
            VolumeDriver::Plugin => self.plugin.as_ref().map(|p| {
                let options: Vec<String> = p
                    .options
                    .iter()
                    .map(|(k, v)| format!("{k:?}={v:?}"))
                    .collect();
                format!("plugin {:?} {}", p.name, options.join(" "))
            }),
            _ => None,
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
        if let Some(r) = &self.replica {
            if let Some(node) = &r.node {
                crate::validate_name(node)
                    .map_err(|e| Invalid::new("replica.node", e.to_string()))?;
                if self.node.as_ref() == Some(node) {
                    return Err(Invalid::new(
                        "replica.node",
                        "is the node holding the data: the replica is kept elsewhere",
                    ));
                }
            }
            if r.every_seconds.is_some_and(|s| s < Replica::MIN_EVERY) {
                return Err(Invalid::new(
                    "replica.everySeconds",
                    format!("at least {}", Replica::MIN_EVERY),
                ));
            }
        }
        check_schedules(&self.snapshots, "snapshots")?;
        if let Some(b) = &self.backup {
            b.check("backup")?;
        }
        if driver != VolumeDriver::Zfs {
            if self.replica.is_some() {
                return Err(not_zfs("replica"));
            }
            if !self.snapshots.is_empty() {
                return Err(not_zfs("snapshots"));
            }
            if self.backup.is_some() {
                return Err(not_zfs("backup"));
            }
            if self.size.is_some() && self.provider().is_none() {
                return Err(Invalid::new(
                    "size",
                    format!(
                        "is for ZFS volumes and those a node provides, not {}",
                        driver.as_str()
                    ),
                ));
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
        if let Some(n) = &self.nfs
            && let Some(node) = &n.node
        {
            crate::validate_name(node).map_err(|e| Invalid::new("nfs.node", e.to_string()))?;
            if !n.server.is_empty() || !n.path.is_empty() {
                return Err(Invalid::new(
                    "nfs.node",
                    "the node provides the export: no server or path with it",
                ));
            }
            if let Some(o) = n.options.iter().find(|o| !option_word(o)) {
                return Err(Invalid::new("nfs.options", format!("bad option {o:?}")));
            }
        } else if let Some(n) = &self.nfs {
            if n.server.is_empty()
                || n.server.starts_with('-')
                || !plain(&n.server)
                || n.server.contains([':', '/']) && !is_ipv6(&n.server)
            {
                return Err(Invalid::new("nfs.server", "a host name or address"));
            }
            if !n.path.starts_with('/') {
                return Err(Invalid::new("nfs.path", "must be absolute"));
            }
            if !plain(&n.path) {
                return Err(Invalid::new("nfs.path", "no spaces or control characters"));
            }
            if let Some(o) = n.options.iter().find(|o| !option_word(o)) {
                return Err(Invalid::new("nfs.options", format!("bad option {o:?}")));
            }
        }
        if let Some(i) = &self.iscsi
            && let Some(node) = &i.node
        {
            crate::validate_name(node).map_err(|e| Invalid::new("iscsi.node", e.to_string()))?;
            if !i.portal.is_empty() || !i.target.is_empty() || i.lun != 0 {
                return Err(Invalid::new(
                    "iscsi.node",
                    "the node provides the LUN: no portal, target or lun with it",
                ));
            }
            if self.size.is_none() {
                return Err(Invalid::new("size", "is needed for a LUN a node provides"));
            }
        } else if let Some(i) = &self.iscsi {
            if i.portal.is_empty()
                || i.portal.starts_with('-')
                || !plain(&i.portal)
                || i.portal.contains('/')
            {
                return Err(Invalid::new("iscsi.portal", "host or host:port"));
            }
            if !(i.target.starts_with("iqn.")
                || i.target.starts_with("eui.")
                || i.target.starts_with("naa."))
                || !plain(&i.target)
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

/// No whitespace or control characters: it goes on a command line, and
/// comes back in `mount`'s whitespace-separated output.
fn plain(s: &str) -> bool {
    !s.chars().any(|c| c.is_whitespace() || c.is_control())
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
    /// Nodes still holding a copy of the data it moved away from: the
    /// migrator has each drop it, and forgets the node once it has.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub leftovers: Vec<String>,
    /// The node its data was on, removed from the cluster: the data went
    /// with it. No task uses the Volume again until it is restored
    /// (`legion volume restore --replace`) or deleted to start empty; a new
    /// node joining under that name is not where the data is.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lost: String,
    /// The replica, while the spec asks for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica: Option<ReplicaStatus>,
    /// The Volume carries on from its replica, its node holding the data
    /// removed: from which node, and as of when.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub promoted: String,
    /// Where a provisioned NFS or iSCSI Volume is exported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export: Option<Export>,
    /// Each snapshot schedule, as the leader last took its snapshot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<ScheduleStatus>,
    /// Its backups, as the leader last ran them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<crate::backup::BackupStatus>,
}

/// A snapshot schedule as the leader last kept it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleStatus {
    /// The schedule's `every`, which tells them apart.
    pub every: Duration,
    /// The last snapshot it took, and when, in milliseconds since the epoch.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taken_ms: Option<i64>,
    /// How many of its snapshots the node held after the last pruning.
    #[serde(default)]
    pub kept: u32,
    /// Why the last attempt failed, and how many have failed since one
    /// worked (for the backoff).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
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

/// What a Volume's backup says of itself, before its data: what to make
/// the Volume again from, and what the data is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeBackupHeader {
    /// [`VolumeBackupHeader::FORMAT`].
    pub format: u32,
    pub namespace: String,
    pub name: String,
    /// The Volume's labels: a stateful service's own its Volumes by them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub spec: VolumeSpec,
    /// The node the data was on.
    pub node: String,
    /// When the snapshot was taken, in seconds since the Unix epoch.
    pub taken: u64,
    /// [`VolumeBackupHeader::CRASH_CONSISTENT`]: the snapshot is the
    /// dataset at one instant, taken while its tasks ran, as after a power
    /// cut. An application whose files must agree with each other (a
    /// database) recovers as from a crash.
    pub consistency: String,
}

impl VolumeBackupHeader {
    pub const FORMAT: u32 = 1;
    pub const CRASH_CONSISTENT: &str = "crash-consistent";

    /// The line a backup file starts with, from the header as a manager
    /// sends it: compact JSON, which has no line break, then one. The data
    /// follows it in the file.
    pub fn line(sent: &[u8]) -> Result<(VolumeBackupHeader, Vec<u8>), String> {
        let header: VolumeBackupHeader =
            serde_json::from_slice(sent).map_err(|e| format!("the backup's header: {e}"))?;
        let mut line = serde_json::to_vec(&header).expect("a header serialises");
        line.push(b'\n');
        Ok((header, line))
    }

    /// The header a backup file's first line holds.
    pub fn from_line(line: &str) -> Result<VolumeBackupHeader, String> {
        serde_json::from_str(line.trim_end())
            .map_err(|_| "not a volume backup: it does not start with a backup's header".to_owned())
    }
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
    fn storage_one_node_holds_is_known_however_it_is_reached() {
        let lun = |portal: &str, target: &str, lun: u32| {
            admit(&format!(
                "driver: iscsi\niscsi: {{ portal: '{portal}', target: '{target}', lun: {lun} }}"
            ))
            .unwrap()
            .exclusive_storage()
        };
        let a = lun("10.0.0.5:3260", "iqn.2026-10.lan:shop", 1);
        assert!(a.is_some());
        assert_eq!(
            a,
            lun("san.lan", "iqn.2026-10.LAN:Shop", 1),
            "another portal"
        );
        assert_ne!(
            a,
            lun("10.0.0.5:3260", "iqn.2026-10.lan:shop", 2),
            "another LUN"
        );
        let ceph = |pool: &str| {
            admit(&format!(
                "driver: plugin\nplugin: {{ name: ceph, options: {{ pool: {pool} }} }}"
            ))
            .unwrap()
            .exclusive_storage()
        };
        assert_eq!(ceph("rbd"), ceph("rbd"));
        assert_ne!(ceph("rbd"), ceph("other"));
        for shared in [
            "size: 1Gi",
            "driver: nfs\nnfs: { server: files.lan, path: /export/shop }",
            "driver: plugin\nplugin: { name: s3fs, shared: true }",
        ] {
            assert_eq!(admit(shared).unwrap().exclusive_storage(), None, "{shared}");
        }
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
            // Nothing a command line or `mount`'s output would misread.
            ("driver: nfs\nnfs: { server: '-a', path: /x }", "nfs.server"),
            (
                "driver: nfs\nnfs: { server: \"a\\tb\", path: /x }",
                "nfs.server",
            ),
            (
                "driver: nfs\nnfs: { server: a, path: \"/x\\ny\" }",
                "nfs.path",
            ),
            ("driver: nfs\nnfs: { server: a, path: '/x y' }", "nfs.path"),
            (
                "driver: iscsi\niscsi: { portal: '-d', target: iqn.x }",
                "iscsi.portal",
            ),
            (
                "driver: iscsi\niscsi: { portal: \"a\\tb\", target: iqn.x }",
                "iscsi.portal",
            ),
            (
                "driver: iscsi\niscsi: { portal: a, target: \"iqn.x\\ty\" }",
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

    #[test]
    fn a_node_provides_nfs_and_iscsi_volumes_with_their_size() {
        let nfs = admit("driver: nfs\nsize: 10Gi\nnfs: { node: store1 }").unwrap();
        assert_eq!(
            (nfs.provider(), nfs.access()),
            (Some("store1"), Access::Shared)
        );
        let lun = admit("driver: iscsi\nsize: 4Gi\niscsi: { node: store1 }").unwrap();
        assert_eq!(
            (lun.provider(), lun.access()),
            (Some("store1"), Access::Single)
        );
        // Its own LUN: no other Volume can name it.
        assert_eq!(lun.exclusive_storage(), None);
        assert!(admit("driver: nfs\nnfs: { node: store1 }").is_ok());
        for (bad, field) in [
            ("driver: iscsi\niscsi: { node: store1 }", "size"),
            (
                "driver: nfs\nnfs: { node: store1, server: a, path: /x }",
                "nfs.node",
            ),
            (
                "driver: iscsi\nsize: 1Gi\niscsi: { node: store1, lun: 2 }",
                "iscsi.node",
            ),
            ("driver: nfs\nnfs: { node: 'Store 1' }", "nfs.node"),
            (
                "driver: nfs\nsize: 1Gi\nnfs: { server: a, path: /x }",
                "size",
            ),
        ] {
            let e = admit(bad).unwrap_err();
            assert!(e.starts_with(&format!("spec.{field}:")), "{bad}: {e}");
        }
    }

    #[test]
    fn a_provided_volume_is_mounted_from_where_its_node_said() {
        let nfs = admit("driver: nfs\nnfs: { node: store1, options: [nfsv3] }").unwrap();
        assert_eq!(nfs.resolved(None), None);
        let mut e = Export {
            node: "store1".into(),
            server: "10.0.0.7".into(),
            path: "/tank/legion/exports/shop/files".into(),
            ..Default::default()
        };
        let r = nfs.resolved(Some(&e)).unwrap();
        let n = r.nfs.unwrap();
        assert_eq!(
            (n.server.as_str(), n.path.as_str(), n.options.len()),
            ("10.0.0.7", "/tank/legion/exports/shop/files", 1)
        );
        // Said by another node, or failed: not yet.
        e.node = "store2".into();
        assert_eq!(nfs.resolved(Some(&e)), None);
        e.node = "store1".into();
        e.message = "out of space".into();
        assert_eq!(nfs.resolved(Some(&e)), None);
        // Others as they are.
        let plain = admit("size: 1Gi").unwrap();
        assert_eq!(plain.resolved(None), Some(plain.clone()));
    }

    #[test]
    fn a_replica_is_pending_ready_stale_or_failed() {
        let every = std::time::Duration::from_secs(10);
        let mut r = ReplicaStatus::default();
        assert_eq!(r.settled(every, 0), ReplicaState::Pending);
        assert!(
            !r.stale_at(every, i64::MAX),
            "never stale before the first copy"
        );
        r.snapshot = "legion-replica-1000".into();
        r.taken_ms = Some(1000);
        assert_eq!(r.settled(every, 31_000), ReplicaState::Ready);
        assert_eq!(r.settled(every, 31_001), ReplicaState::Stale);
        r.message = "node n2: refused".into();
        assert_eq!(r.settled(every, 2000), ReplicaState::Failed);
        // Too old, it is stale though its attempts fail: as the alert says.
        assert_eq!(r.settled(every, 31_001), ReplicaState::Stale);
        // Failing before any copy: failed, never stale.
        r.snapshot.clear();
        r.taken_ms = None;
        assert_eq!(r.settled(every, i64::MAX), ReplicaState::Failed);
    }
}
