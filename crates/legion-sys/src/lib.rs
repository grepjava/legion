//! FreeBSD system operations the agent needs (DESIGN.md §3.6): jails,
//! interfaces and routes, WireGuard, rctl, pf anchors and tables, ZFS
//! datasets and tmpfs mounts.
//!
//! [`Sys`] is the interface. [`Shell`] implements it by running `jls`,
//! `jail`, `ifconfig`, `route`, `wg`, `rctl`, `pfctl`, `zfs` and `mount`, with the forms
//! checked on FreeBSD 15.1; native ioctl versions can replace single methods
//! later. [`FakeSys`] keeps the same state in memory for tests of the layers
//! above.
//!
//! Methods block; async callers run them on the blocking pool.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

mod fake;
mod memory;
mod shell;

pub use fake::{FakeSys, FakeWireGuard};
pub use memory::JailMemory;
pub use shell::{CommandOutput, Host, Runner, Shell};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{command}: {message}")]
    Failed { command: String, message: String },
    #[error("running {command}: {source}")]
    Spawn {
        command: String,
        source: std::io::Error,
    },
    #[error("{command}: unexpected output: {message}")]
    Parse { command: String, message: String },
    #[error("{0} not found")]
    NotFound(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The system operations, grouped by tool. Interface methods take an
/// optional jail: `None` is the agent's own (host) network stack, `Some`
/// a VNET jail's, reached with `ifconfig -j` / `route -j`.
pub trait Sys: Send + Sync {
    /// Loads a kernel module unless it is loaded already (`kldload -n`).
    fn kld_load(&self, module: &str) -> Result<()>;

    // Jails.

    /// Every jail visible to the caller, children included.
    fn jails(&self) -> Result<Vec<Jail>>;
    /// Sets parameters on an existing jail (`jail -m`).
    fn jail_set(&self, jail: &str, params: &[(&str, &str)]) -> Result<()>;
    /// Creates a jail with these parameters (`name` among them; a child's
    /// is `<parent>.<child>`); an empty value sets a flag (`persist`,
    /// `vnet`). Returns its jid.
    fn jail_create(&self, params: &[(&str, &str)]) -> Result<u32>;
    /// Removes a jail and its children, killing their processes.
    /// [`Error::NotFound`] if there is no such jail.
    fn jail_remove(&self, jail: &str) -> Result<()>;
    /// Reads a jail's parameters (`jls -j <jail> -n`); booleans read
    /// `true` or `false`. [`Error::NotFound`] if the jail is gone.
    fn jail_get(&self, jail: &str, params: &[&str]) -> Result<BTreeMap<String, String>>;

    // Interfaces and routes.

    fn interfaces(&self, jail: Option<&str>) -> Result<Vec<String>>;
    /// Creates a bridge named `name` in the host stack.
    fn bridge_create(&self, name: &str) -> Result<()>;
    /// Creates an epair in the host stack.
    fn epair_create(&self) -> Result<Epair>;
    fn interface_destroy(&self, name: &str) -> Result<()>;
    /// Renames a host interface (at most 15 characters).
    fn interface_rename(&self, name: &str, new_name: &str) -> Result<()>;
    fn bridge_add_member(&self, bridge: &str, member: &str) -> Result<()>;
    /// A host interface's Ethernet address (`ifconfig`'s `ether`).
    fn interface_ether(&self, name: &str) -> Result<String>;
    /// Gives a host interface another Ethernet address.
    fn interface_set_ether(&self, name: &str, ether: &str) -> Result<()>;
    /// Moves a host interface into a VNET jail. It returns to the host when
    /// the jail is removed.
    fn interface_move(&self, name: &str, jail: &str) -> Result<()>;
    fn interface_configure(&self, jail: Option<&str>, name: &str, config: &IfConfig) -> Result<()>;
    /// Routes `dest` through `gateway`, which is of the same family.
    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: IpAddr) -> Result<()>;
    fn route_delete(&self, jail: Option<&str>, dest: Destination) -> Result<()>;
    /// Routes `dest` out of `interface` with no gateway, replacing any
    /// route to `dest` already there.
    fn route_to_interface(
        &self,
        jail: Option<&str>,
        dest: Destination,
        interface: &str,
    ) -> Result<()>;

    // CARP (host only).

    /// Puts `carp.addr` on `interface` as CARP virtual host `carp.vhid`
    /// (loading `carp`), or, when it is there, sets its skew; `state`
    /// forces the virtual host master or backup.
    fn carp_set(&self, interface: &str, carp: &Carp, state: Option<CarpState>) -> Result<()>;
    /// The state of virtual host `vhid` on `interface`; `None` if absent.
    fn carp_state(&self, interface: &str, vhid: u8) -> Result<Option<CarpState>>;
    /// Removes the address, and with it the virtual host; one not there
    /// is fine.
    fn carp_remove(&self, interface: &str, addr: Ipv4Addr) -> Result<()>;

    // WireGuard (host only). Keys are base64, as `wg` prints them.

    /// A new private key.
    fn wg_genkey(&self) -> Result<String>;
    /// The public key of a private one.
    fn wg_pubkey(&self, private_key: &str) -> Result<String>;
    /// Creates the WireGuard interface `name`.
    fn wg_create(&self, name: &str) -> Result<()>;
    /// Sets the interface's private key and the UDP port it listens on.
    fn wg_configure(&self, name: &str, private_key: &str, port: u16) -> Result<()>;
    fn wg_peers(&self, name: &str) -> Result<Vec<WgPeer>>;
    /// Adds a peer or updates it: its allowed IPs and keepalive are
    /// replaced, its endpoint only when one is given (otherwise WireGuard
    /// keeps the one it learned).
    fn wg_set_peer(&self, name: &str, peer: &WgPeer) -> Result<()>;
    /// Removes a peer; one not there is fine.
    fn wg_remove_peer(&self, name: &str, public_key: &str) -> Result<()>;

    // rctl (host only).

    fn rctl_add(&self, rule: &RctlRule) -> Result<()>;
    /// Removes every rule for `jail`; none is not an error.
    fn rctl_remove_jail(&self, jail: &str) -> Result<()>;
    /// Current usage by resource (`memoryuse`, `pcpu`, `maxproc`, ...).
    fn rctl_usage(&self, jail: &str) -> Result<BTreeMap<String, u64>>;
    /// The memory of each of these jails' processes, every page counted
    /// once (see [`JailMemory`]); a jail with none running has none.
    fn jail_memory(&self, jails: &[String]) -> Result<BTreeMap<String, JailMemory>>;
    /// Kills a process (`SIGKILL`); one already gone is fine.
    fn kill(&self, pid: u32) -> Result<()>;

    /// The interface of the default route, if there is one.
    fn default_interface(&self) -> Result<Option<String>>;
    /// Sets a kernel variable (`sysctl name=value`).
    fn sysctl_set(&self, name: &str, value: &str) -> Result<()>;

    // pf.

    /// The main ruleset's translation and filter rules, as `pfctl -sn` and
    /// `-sr` print them: enough to check that Legion's anchors are called.
    fn pf_main_rules(&self) -> Result<String>;
    /// Every pf state.
    fn pf_states(&self) -> Result<Vec<PfState>>;
    /// Kills one state by its [`PfState::id`]; one already gone is fine.
    fn pf_kill_state(&self, id: &str) -> Result<()>;
    /// Replaces the rules of `anchor` (e.g. `legion/svc`).
    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()>;
    /// Removes the rules and tables of `anchor`.
    fn pf_flush_anchor(&self, anchor: &str) -> Result<()>;
    /// Sets a table's contents (of either family, or both), creating it if
    /// needed. Existing states are untouched, so connections to removed
    /// addresses drain.
    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[IpAddr]) -> Result<()>;
    /// A table's contents; empty if it does not exist.
    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<IpAddr>>;

    // ZFS.

    /// `root` and its descendants; empty if `root` does not exist.
    fn zfs_list(&self, root: &str) -> Result<Vec<Dataset>>;
    /// Creates a dataset and any missing parents. An existing dataset is
    /// left as it is (`props` are not applied to it).
    fn zfs_create(&self, name: &str, props: &[(&str, &str)]) -> Result<()>;
    fn zfs_set(&self, name: &str, prop: &str, value: &str) -> Result<()>;
    fn zfs_snapshot(&self, snapshot: &str) -> Result<()>;
    /// The names (after the `@`) of a dataset's snapshots, oldest first;
    /// empty if the dataset does not exist.
    fn zfs_snapshots(&self, dataset: &str) -> Result<Vec<String>>;
    /// Rolls a dataset back to `snapshot` (`<dataset>@<name>`), destroying
    /// the snapshots taken after it (`zfs rollback -r`).
    fn zfs_rollback(&self, snapshot: &str) -> Result<()>;
    /// Makes `dataset` a writable clone of `snapshot` (`<dataset>@<name>`),
    /// with `props` set on it.
    fn zfs_clone(&self, snapshot: &str, dataset: &str, props: &[(&str, &str)]) -> Result<()>;
    /// Destroys a dataset or, if `name` has an `@`, a snapshot. A snapshot
    /// that clones were made from cannot be destroyed, nor its dataset.
    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()>;
    /// Writes a `zfs send` stream of `snapshot` (`<dataset>@<name>`) to
    /// `out`: everything, or only what changed since the dataset's snapshot
    /// `since` (the name after the `@`).
    fn zfs_send(
        &self,
        snapshot: &str,
        since: Option<&str>,
        out: &mut dyn std::io::Write,
    ) -> Result<()>;
    /// Receives a `zfs send` stream from `input` into `dataset`, whose
    /// parent must exist, and leaves it unmounted (`zfs receive -F -u`): an
    /// incremental stream rolls the dataset back to the stream's starting
    /// snapshot first; a full one replaces it only if it has no snapshots.
    fn zfs_receive(&self, dataset: &str, input: &mut dyn std::io::Read) -> Result<()>;
    /// Mounts a dataset at its mountpoint unless it is mounted.
    fn zfs_mount(&self, dataset: &str) -> Result<()>;

    // Mounts.

    /// Mounts a memory-backed file system on the existing directory `path`,
    /// of at most `size` bytes (by default, as much as memory and swap
    /// allow), with `mode` as its root's permissions.
    fn mount_tmpfs(&self, path: &str, size: Option<u64>, mode: u32) -> Result<()>;
    /// Mounts `source` (a directory or a file) at `target` with nullfs.
    fn mount_nullfs(&self, source: &str, target: &str, read_only: bool) -> Result<()>;
    /// Mounts a devfs at `target` showing what devfs ruleset `ruleset`
    /// allows.
    fn mount_devfs(&self, target: &str, ruleset: u32) -> Result<()>;
    /// Makes `name` in the devfs mounted at `dev` a symbolic link to
    /// `target`, shown whatever the mount's ruleset hides. (devfs makes
    /// directories but nothing mounts on them, and links are how it shows
    /// other file systems.)
    fn devfs_link(&self, dev: &str, name: &str, target: &str) -> Result<()>;
    /// Mounts a file system that needs no source (`linprocfs`, `linsysfs`,
    /// `fdescfs`) at `target`, with `options` if not empty.
    fn mount_pseudo(&self, fstype: &str, target: &str, options: &str) -> Result<()>;
    /// Mounts `source` (`server:/path` for `nfs`, a device for `ufs`) at
    /// the existing directory `target`, with `options` (comma-separated)
    /// if not empty.
    fn mount_fs(&self, fstype: &str, source: &str, target: &str, options: &str) -> Result<()>;
    /// The file system on a device (`ufs`, `zfs`, `msdosfs`, ...), as
    /// fstyp(8) knows it; `None` if it finds none.
    fn fs_type(&self, device: &str) -> Result<Option<String>>;
    /// Checks a UFS file system as boot does before mounting it (`fsck -p`):
    /// a clean one at once, an unclean one (left by a crash) repaired where
    /// that is safe; an error if it needs a person.
    fn fsck_preen(&self, device: &str) -> Result<()>;

    // iSCSI initiator (host only; iscsid(8) must run).

    /// The node's iSCSI sessions.
    fn iscsi_sessions(&self) -> Result<Vec<IscsiSession>>;
    /// Logs in to `target` at `portal` (`host[:port]`); the session
    /// connects in the background.
    fn iscsi_attach(&self, portal: &str, target: &str) -> Result<()>;
    /// Logs out of `target`, removing its devices; no session is fine.
    fn iscsi_detach(&self, target: &str) -> Result<()>;

    // Programs.

    /// Runs `program` with `args` and `input` on its standard input, as
    /// volume plugins are; its standard output if it succeeds.
    /// [`Error::NotFound`] if there is no such program.
    fn run_program(&self, program: &str, args: &[&str], input: &str) -> Result<String>;
    /// Unmounts `path`, forcibly: callers unmount only what nothing uses.
    fn unmount(&self, path: &str) -> Result<()>;
    /// Mount points at or below `prefix`.
    fn mounts(&self, prefix: &str) -> Result<Vec<String>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jail {
    pub jid: u32,
    /// Full name; a child jail's is `<parent>.<child>`.
    pub name: String,
    /// `new` for a jail with its own network stack, `inherit` otherwise.
    pub vnet: String,
    pub children_max: u32,
    pub children_cur: u32,
    /// Parent's jid; 0 at the top level.
    pub parent: u32,
}

/// A pf state, as far as Legion cares: which addresses it involves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PfState {
    /// `<id>/<creatorid>`, for [`Sys::pf_kill_state`].
    pub id: String,
    /// Every address on the state's line: both ends and the translated
    /// one (the VIP of a load-balanced connection, for example).
    pub addrs: Vec<IpAddr>,
}

/// An iSCSI session of this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IscsiSession {
    pub target: String,
    pub portal: String,
    /// `Connected`, `Disconnected`, ...
    pub state: String,
    /// Why it is not connected, if it says.
    pub failure: String,
    /// Its LUNs' devices (`da0`), by LUN.
    pub luns: BTreeMap<u32, String>,
}

impl IscsiSession {
    pub fn connected(&self) -> bool {
        self.state == "Connected"
    }

    /// The device of a LUN, once it has one: while CAM probes it, iscsictl
    /// shows a placeholder (`probe0`) that is no device.
    pub fn device(&self, lun: u32) -> Option<&str> {
        self.luns
            .get(&lun)
            .map(String::as_str)
            .filter(|d| !d.starts_with("probe"))
    }
}

/// A WireGuard peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WgPeer {
    pub public_key: String,
    /// `host:port`.
    pub endpoint: Option<String>,
    /// The addresses it may send from and is sent to, in order.
    pub allowed_ips: Vec<IpNet>,
    /// Seconds between keepalives.
    pub keepalive: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Epair {
    /// Stays on the host, joins the bridge.
    pub a: String,
    /// Goes into the pod's jail.
    pub b: String,
}

/// A CARP virtual host: an address that one of the hosts sharing `vhid`
/// and `password` on a link holds at a time, the one advertising with the
/// lowest `advskew` when they disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Carp {
    pub vhid: u8,
    pub addr: Ipv4Addr,
    pub password: String,
    pub advskew: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarpState {
    Init,
    Backup,
    Master,
}

impl CarpState {
    pub fn as_str(self) -> &'static str {
        match self {
            CarpState::Init => "init",
            CarpState::Backup => "backup",
            CarpState::Master => "master",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IfConfig {
    pub addr: Option<Ipv4Net>,
    /// Added beside the interface's other IPv6 addresses (its link-local
    /// one), usable at once: duplicate address detection is turned off.
    pub addr6: Option<Ipv6Net>,
    pub mtu: Option<u32>,
    pub up: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Destination {
    Default,
    Net(Ipv4Net),
    /// The IPv6 default route.
    Default6,
    Net6(Ipv6Net),
}

impl fmt::Display for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Destination::Default => f.write_str("default"),
            Destination::Net(n) => n.fmt(f),
            Destination::Default6 => f.write_str("default (IPv6)"),
            Destination::Net6(n) => n.fmt(f),
        }
    }
}

/// An IPv4 address with a prefix length: `10.200.3.17/24`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv4Net {
    pub addr: Ipv4Addr,
    pub prefix: u8,
}

impl Ipv4Net {
    pub fn new(addr: Ipv4Addr, prefix: u8) -> Self {
        assert!(prefix <= 32, "prefix {prefix} is longer than 32");
        Ipv4Net { addr, prefix }
    }

    /// The network address (host bits cleared).
    pub fn network(&self) -> Ipv4Addr {
        let mask = u32::MAX
            .checked_shl(32 - u32::from(self.prefix))
            .unwrap_or(0);
        Ipv4Addr::from(u32::from(self.addr) & mask)
    }

    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        Ipv4Net::new(ip, self.prefix).network() == self.network()
    }
}

impl fmt::Display for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl FromStr for Ipv4Net {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("invalid IPv4 network {s:?}: expected a.b.c.d/len");
        let (addr, prefix) = s.split_once('/').ok_or_else(bad)?;
        let addr = addr.parse().map_err(|_| bad())?;
        let prefix = prefix.parse().ok().filter(|&p| p <= 32).ok_or_else(bad)?;
        Ok(Ipv4Net { addr, prefix })
    }
}

/// An IPv6 address with a prefix length: `fd00:10:200:3::11/64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ipv6Net {
    pub addr: Ipv6Addr,
    pub prefix: u8,
}

impl Ipv6Net {
    pub fn new(addr: Ipv6Addr, prefix: u8) -> Self {
        assert!(prefix <= 128, "prefix {prefix} is longer than 128");
        Ipv6Net { addr, prefix }
    }

    /// The network address (host bits cleared).
    pub fn network(&self) -> Ipv6Addr {
        let mask = u128::MAX
            .checked_shl(128 - u32::from(self.prefix))
            .unwrap_or(0);
        Ipv6Addr::from(u128::from(self.addr) & mask)
    }

    pub fn contains(&self, ip: Ipv6Addr) -> bool {
        Ipv6Net::new(ip, self.prefix).network() == self.network()
    }
}

impl fmt::Display for Ipv6Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl FromStr for Ipv6Net {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("invalid IPv6 network {s:?}: expected addr/len");
        let (addr, prefix) = s.split_once('/').ok_or_else(bad)?;
        let addr = addr.parse().map_err(|_| bad())?;
        let prefix = prefix.parse().ok().filter(|&p| p <= 128).ok_or_else(bad)?;
        Ok(Ipv6Net { addr, prefix })
    }
}

/// A network of either family, where both may appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IpNet {
    V4(Ipv4Net),
    V6(Ipv6Net),
}

impl From<Ipv4Net> for IpNet {
    fn from(n: Ipv4Net) -> Self {
        IpNet::V4(n)
    }
}

impl From<Ipv6Net> for IpNet {
    fn from(n: Ipv6Net) -> Self {
        IpNet::V6(n)
    }
}

impl fmt::Display for IpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IpNet::V4(n) => n.fmt(f),
            IpNet::V6(n) => n.fmt(f),
        }
    }
}

impl FromStr for IpNet {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        if s.contains(':') {
            s.parse().map(IpNet::V6)
        } else {
            s.parse().map(IpNet::V4)
        }
    }
}

/// An rctl rule on a jail, with the resource's action: the limits Legion
/// sets are hard ones (DESIGN.md §6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RctlRule {
    pub jail: String,
    pub resource: Resource,
    pub amount: u64,
}

impl fmt::Display for RctlRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "jail:{}:{}:{}={}",
            self.jail,
            self.resource.as_str(),
            self.resource.action(),
            self.amount
        )?;
        if self.resource == Resource::MemoryUse {
            f.write_str("/process")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Resource {
    /// Bytes of resident memory, for each process of the jail on its own:
    /// one going past it is killed (rctl reads resident memory about once
    /// a second, as the agent does). rctl sums the jail's processes
    /// counting shared pages once for each, so the jail's total is the
    /// agent's to keep ([`Sys::jail_memory`]); no process can use more
    /// than its jail, so this never kills one the total would spare.
    /// (`deny` would page the process out instead, which rctl(8) warns
    /// can thrash.)
    MemoryUse,
    /// Percent of one CPU; enforced by pausing the process.
    Pcpu,
    MaxProc,
    OpenFiles,
}

impl Resource {
    pub fn as_str(self) -> &'static str {
        match self {
            Resource::MemoryUse => "memoryuse",
            Resource::Pcpu => "pcpu",
            Resource::MaxProc => "maxproc",
            Resource::OpenFiles => "openfiles",
        }
    }

    /// The rctl action enforcing the limit.
    pub fn action(self) -> &'static str {
        match self {
            Resource::MemoryUse => "sigkill",
            _ => "deny",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    pub name: String,
    /// `None` when not mounted (`-` or `none`).
    pub mountpoint: Option<String>,
    /// Bytes; `None` without a quota.
    pub quota: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_net() {
        let n: Ipv4Net = "10.200.3.17/24".parse().unwrap();
        assert_eq!(n.network(), Ipv4Addr::new(10, 200, 3, 0));
        assert!(n.contains(Ipv4Addr::new(10, 200, 3, 1)));
        assert!(!n.contains(Ipv4Addr::new(10, 200, 4, 1)));
        assert_eq!(n.to_string(), "10.200.3.17/24");
        assert_eq!(
            "0.0.0.0/0".parse::<Ipv4Net>().unwrap().network(),
            Ipv4Addr::UNSPECIFIED
        );
        assert!("10.0.0.1/33".parse::<Ipv4Net>().is_err());
        assert!("10.0.0.1".parse::<Ipv4Net>().is_err());
    }

    #[test]
    fn ipv6_net() {
        let n: Ipv6Net = "fd00:10:200:3::11/64".parse().unwrap();
        assert_eq!(n.network(), "fd00:10:200:3::".parse::<Ipv6Addr>().unwrap());
        assert!(n.contains("fd00:10:200:3::1".parse().unwrap()));
        assert!(!n.contains("fd00:10:200:4::1".parse().unwrap()));
        assert_eq!(n.to_string(), "fd00:10:200:3::11/64");
        assert_eq!(
            "::/0".parse::<Ipv6Net>().unwrap().network(),
            Ipv6Addr::UNSPECIFIED
        );
        assert!("fd00::1/129".parse::<Ipv6Net>().is_err());
        let both: Vec<IpNet> = ["10.200.2.0/24", "fd00:10:200:2::/64"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        assert!(matches!(both[..], [IpNet::V4(_), IpNet::V6(_)]));
        assert_eq!(both[1].to_string(), "fd00:10:200:2::/64");
    }

    #[test]
    fn rctl_rule_text() {
        let r = RctlRule {
            jail: "abc.def".into(),
            resource: Resource::MemoryUse,
            amount: 512 << 20,
        };
        assert_eq!(
            r.to_string(),
            "jail:abc.def:memoryuse:sigkill=536870912/process"
        );
    }
}
