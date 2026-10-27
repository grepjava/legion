//! FreeBSD system operations the agent needs (DESIGN.md §3.6): jails,
//! interfaces and routes, rctl, pf anchors and tables, ZFS datasets and
//! tmpfs mounts.
//!
//! [`Sys`] is the interface. [`Shell`] implements it by running `jls`,
//! `jail`, `ifconfig`, `route`, `rctl`, `pfctl`, `zfs` and `mount`, with the forms
//! checked on FreeBSD 15.1; native ioctl versions can replace single methods
//! later. [`FakeSys`] keeps the same state in memory for tests of the layers
//! above.
//!
//! Methods block; async callers run them on the blocking pool.

use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv4Addr;
use std::str::FromStr;

mod fake;
mod shell;

pub use fake::FakeSys;
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
    /// Moves a host interface into a VNET jail. It returns to the host when
    /// the jail is removed.
    fn interface_move(&self, name: &str, jail: &str) -> Result<()>;
    fn interface_configure(&self, jail: Option<&str>, name: &str, config: &IfConfig) -> Result<()>;
    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: Ipv4Addr) -> Result<()>;
    fn route_delete(&self, jail: Option<&str>, dest: Destination) -> Result<()>;

    // rctl (host only).

    fn rctl_add(&self, rule: &RctlRule) -> Result<()>;
    /// Removes every rule for `jail`; none is not an error.
    fn rctl_remove_jail(&self, jail: &str) -> Result<()>;
    /// Current usage by resource (`memoryuse`, `pcpu`, `maxproc`, ...).
    fn rctl_usage(&self, jail: &str) -> Result<BTreeMap<String, u64>>;

    // pf.

    /// Replaces the rules of `anchor` (e.g. `legion/svc`).
    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()>;
    /// Removes the rules and tables of `anchor`.
    fn pf_flush_anchor(&self, anchor: &str) -> Result<()>;
    /// Sets a table's contents, creating it if needed. Existing states are
    /// untouched, so connections to removed addresses drain.
    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[Ipv4Addr]) -> Result<()>;
    /// A table's contents; empty if it does not exist.
    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<Ipv4Addr>>;

    // ZFS.

    /// `root` and its descendants; empty if `root` does not exist.
    fn zfs_list(&self, root: &str) -> Result<Vec<Dataset>>;
    /// Creates a dataset and any missing parents. An existing dataset is
    /// left as it is (`props` are not applied to it).
    fn zfs_create(&self, name: &str, props: &[(&str, &str)]) -> Result<()>;
    fn zfs_set(&self, name: &str, prop: &str, value: &str) -> Result<()>;
    fn zfs_snapshot(&self, snapshot: &str) -> Result<()>;
    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()>;

    // Mounts.

    /// Mounts a memory-backed file system on the existing directory `path`,
    /// of at most `size` bytes (by default, as much as memory and swap
    /// allow), with `mode` as its root's permissions.
    fn mount_tmpfs(&self, path: &str, size: Option<u64>, mode: u32) -> Result<()>;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Epair {
    /// Stays on the host, joins the bridge.
    pub a: String,
    /// Goes into the pod's jail.
    pub b: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IfConfig {
    pub addr: Option<Ipv4Net>,
    pub mtu: Option<u32>,
    pub up: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Destination {
    Default,
    Net(Ipv4Net),
}

impl fmt::Display for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Destination::Default => f.write_str("default"),
            Destination::Net(n) => n.fmt(f),
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

/// An rctl rule on a jail, always with the `deny` action: the limits Legion
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
            "jail:{}:{}:deny={}",
            self.jail,
            self.resource.as_str(),
            self.amount
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Resource {
    /// Bytes of virtual memory; enforced by refusing allocations.
    VMemoryUse,
    /// Percent of one CPU; enforced by pausing the process.
    Pcpu,
    MaxProc,
    OpenFiles,
}

impl Resource {
    pub fn as_str(self) -> &'static str {
        match self {
            Resource::VMemoryUse => "vmemoryuse",
            Resource::Pcpu => "pcpu",
            Resource::MaxProc => "maxproc",
            Resource::OpenFiles => "openfiles",
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
    fn rctl_rule_text() {
        let r = RctlRule {
            jail: "abc.def".into(),
            resource: Resource::VMemoryUse,
            amount: 512 << 20,
        };
        assert_eq!(r.to_string(), "jail:abc.def:vmemoryuse:deny=536870912");
    }
}
