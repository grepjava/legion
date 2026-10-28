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
use std::net::Ipv4Addr;
use std::str::FromStr;

mod fake;
mod shell;

pub use fake::{FakeSys, FakeWireGuard};
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
    /// Moves a host interface into a VNET jail. It returns to the host when
    /// the jail is removed.
    fn interface_move(&self, name: &str, jail: &str) -> Result<()>;
    fn interface_configure(&self, jail: Option<&str>, name: &str, config: &IfConfig) -> Result<()>;
    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: Ipv4Addr) -> Result<()>;
    fn route_delete(&self, jail: Option<&str>, dest: Destination) -> Result<()>;
    /// Routes `dest` out of `interface` with no gateway, replacing any
    /// route to `dest` already there.
    fn route_to_interface(
        &self,
        jail: Option<&str>,
        dest: Destination,
        interface: &str,
    ) -> Result<()>;

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

/// A pf state, as far as Legion cares: which addresses it involves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PfState {
    /// `<id>/<creatorid>`, for [`Sys::pf_kill_state`].
    pub id: String,
    /// Every address on the state's line: both ends and the translated
    /// one (the VIP of a load-balanced connection, for example).
    pub addrs: Vec<Ipv4Addr>,
}

/// A WireGuard peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WgPeer {
    pub public_key: String,
    /// `host:port`.
    pub endpoint: Option<String>,
    /// The addresses it may send from and is sent to, in order.
    pub allowed_ips: Vec<Ipv4Net>,
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
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Resource {
    /// Bytes of resident memory; a process taking the jail past it is
    /// killed, as Linux's OOM killer does in a container. (`deny` would
    /// page the jail out instead, which rctl(8) warns can thrash.)
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
    fn rctl_rule_text() {
        let r = RctlRule {
            jail: "abc.def".into(),
            resource: Resource::MemoryUse,
            amount: 512 << 20,
        };
        assert_eq!(r.to_string(), "jail:abc.def:memoryuse:sigkill=536870912");
    }
}
