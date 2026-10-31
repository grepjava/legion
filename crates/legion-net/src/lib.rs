//! Node-local pod networking (DESIGN.md §5.3).
//!
//! Each node owns a subnet of the cluster's pod network and holds a bridge,
//! `legion0`, with the subnet's first address as every pod's gateway. A pod
//! gets an epair: the host end joins the bridge, the other end moves into
//! the pod's infra jail and carries the pod's address and default route.
//! The host end is renamed after the task (`lp<id>`), so the agent finds it
//! again after a restart, and destroying it removes the pair.
//!
//! Pods on other nodes are reached over the WireGuard mesh ([`mesh`]);
//! services, published ports and NAT to the outside are pf rules ([`lb`]).
//!
//! The network is dual-stack (DESIGN.md §5.7), with nothing allocated for
//! IPv6: every address Legion gives out is IPv4, and each has an IPv6 twin
//! ([`v6`]) that the bridge, pods, mesh, pf and DNS use beside it.

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, Ipv6Addr};

use legion_sys::{Destination, IfConfig, Ipv4Net, Ipv6Net, Sys};

pub mod dns;
pub mod lb;
pub mod mesh;
pub mod policy;

pub use legion_sys::Error;

/// The pod network of the whole cluster; each node gets one /24 of it.
pub const CLUSTER_POD_CIDR: Ipv4Net = Ipv4Net {
    addr: Ipv4Addr::new(10, 200, 0, 0),
    prefix: 16,
};
pub const NODE_PREFIX: u8 = 24;
/// The IPv6 twin of [`CLUSTER_POD_CIDR`]; each node gets one /64 of it.
pub const CLUSTER_POD_CIDR6: Ipv6Net = Ipv6Net {
    addr: Ipv6Addr::new(0xfd00, 0x10, 0x200, 0, 0, 0, 0, 0),
    prefix: 48,
};
pub const NODE_PREFIX6: u8 = 64;
/// The VIP range, as `legion_api::service::SERVICE_NET` (which the agent
/// checks), and its IPv6 twin.
pub const SERVICE_CIDR: Ipv4Net = Ipv4Net {
    addr: Ipv4Addr::new(10, 201, 0, 0),
    prefix: 16,
};
pub const SERVICE_CIDR6: Ipv6Net = Ipv6Net {
    addr: Ipv6Addr::new(0xfd00, 0x10, 0x201, 0, 0, 0, 0, 0),
    prefix: 96,
};
pub const BRIDGE: &str = "legion0";
/// What Legion's pods' host interfaces are named with (`lp` and the end of
/// the pod's id).
pub const HOST_PREFIX: &str = "lp";
/// The WireGuard MTU, which pods use from the start so nothing changes when
/// traffic starts crossing nodes (DESIGN.md §5.3).
pub const MTU: u32 = 1420;

/// Node subnet number `n` (from 0) of the cluster pod network.
pub fn node_subnet(n: u32) -> Option<Ipv4Net> {
    let count = 1u32 << (NODE_PREFIX - CLUSTER_POD_CIDR.prefix);
    (n < count).then(|| {
        let base = u32::from(CLUSTER_POD_CIDR.network());
        Ipv4Net::new(
            Ipv4Addr::from(base + (n << (32 - NODE_PREFIX))),
            NODE_PREFIX,
        )
    })
}

/// The lowest node subnet not in `taken`.
pub fn free_node_subnet(taken: &BTreeSet<Ipv4Net>) -> Option<Ipv4Net> {
    (0..).map_while(node_subnet).find(|s| !taken.contains(s))
}

/// The IPv6 twin of a pod's, node's or service's address; `None` outside
/// those ranges. The last two octets become the low groups, each written
/// as hex digits, the way the ranges' first two read:
///
/// | IPv4         | IPv6               |
/// |--------------|--------------------|
/// | `10.200.n.h` | `fd00:10:200:n::h` |
/// | `10.201.a.b` | `fd00:10:201::a:b` |
/// | `100.64.n.1` | `fd00:100:64::n:1` |
///
/// so pod `10.200.3.17` is `fd00:10:200:3::11`, in its node's /64.
pub fn v6(ip: Ipv4Addr) -> Option<Ipv6Addr> {
    let [_, _, c, d] = ip.octets();
    let (c, d) = (u16::from(c), u16::from(d));
    if CLUSTER_POD_CIDR.contains(ip) {
        let [a, b, e, ..] = CLUSTER_POD_CIDR6.addr.segments();
        Some(Ipv6Addr::new(a, b, e, c, 0, 0, 0, d))
    } else if SERVICE_CIDR.contains(ip) {
        let [a, b, e, ..] = SERVICE_CIDR6.addr.segments();
        Some(Ipv6Addr::new(a, b, e, 0, 0, 0, c, d))
    } else if mesh::UNDERLAY.contains(ip) {
        let [a, b, e, ..] = mesh::UNDERLAY6.addr.segments();
        Some(Ipv6Addr::new(a, b, e, 0, 0, 0, c, d))
    } else {
        None
    }
}

/// The /64 of node subnet `subnet`.
pub fn node_subnet6(subnet: Ipv4Net) -> Option<Ipv6Net> {
    let net = v6(subnet.network()).filter(|_| subnet.prefix == NODE_PREFIX)?;
    Some(Ipv6Net::new(net, NODE_PREFIX6))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodNetwork {
    /// This node's subnet, e.g. `10.200.3.0/24`.
    pub subnet: Ipv4Net,
    /// The bridge pods join: [`BRIDGE`], unless [`Self::named`] otherwise.
    pub bridge: String,
    /// What the names of pods' host interfaces start with: [`HOST_PREFIX`],
    /// unless named otherwise.
    pub prefix: String,
    /// Where its IPv6 addresses come from: the cluster's range, unless
    /// [`Self::with_v6`] or [`Self::without_v6`] say otherwise.
    pub v6: PodV6,
    /// The bridge's and pods' MTU: [`MTU`], for the WireGuard mesh, unless
    /// [`Self::with_mtu`] says otherwise.
    pub mtu: u32,
}

/// Where a pod network's IPv6 addresses come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodV6 {
    /// Legion's: [`v6`]'s, in the cluster's range.
    Cluster,
    /// A /64 of a tool's own: a pod's address there ends in the last two
    /// octets of its IPv4 address (`10.88.1.5` is `<net>::1:5`).
    Within(Ipv6Net),
    /// None: IPv4 only.
    Off,
}

/// The longest interface name FreeBSD takes.
const IFNAMSIZ: usize = 15;

/// The address selection policies (prefix, precedence, label) of RFC 6724
/// preferring IPv4, as FreeBSD's rc installs them on a host
/// (`ip6addrctl_policy="ipv4_prefer"`, which `AUTO` picks there). A new
/// VNET has none, and without them a pod ranks its ULA source above IPv4:
/// it tries an outside name's IPv6 address first, which it cannot reach
/// (no NAT66), and waits for that to time out before IPv4.
const ADDR_POLICIES: [(&str, u32, u32); 9] = [
    ("::1/128", 50, 0),
    ("::/0", 40, 1),
    ("::ffff:0.0.0.0/96", 100, 4),
    ("2002::/16", 30, 2),
    ("2001::/32", 5, 5),
    ("fc00::/7", 3, 13),
    ("::/96", 1, 3),
    ("fec0::/10", 1, 11),
    ("3ffe::/16", 1, 12),
];

impl PodNetwork {
    pub fn new(subnet: Ipv4Net) -> Self {
        PodNetwork {
            subnet: Ipv4Net::new(subnet.network(), subnet.prefix),
            bridge: BRIDGE.to_owned(),
            prefix: HOST_PREFIX.to_owned(),
            v6: PodV6::Cluster,
            mtu: MTU,
        }
    }

    /// The same network with its IPv6 addresses in the /64 that `net`
    /// starts (its first 64 bits), not the cluster's range, which a tool
    /// beside Legion must not take. `None` for a prefix longer than 64.
    pub fn with_v6(self, net: Ipv6Net) -> Option<Self> {
        if net.prefix > NODE_PREFIX6 {
            return None;
        }
        let [a, b, c, d, ..] = net.addr.segments();
        let net = Ipv6Net::new(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0), NODE_PREFIX6);
        Some(PodNetwork {
            v6: PodV6::Within(net),
            ..self
        })
    }

    /// The same network without IPv6.
    pub fn without_v6(self) -> Self {
        PodNetwork {
            v6: PodV6::Off,
            ..self
        }
    }

    /// The same network with MTU `mtu` (1500 where there is no WireGuard
    /// mesh to fit in).
    pub fn with_mtu(self, mtu: u32) -> Self {
        PodNetwork { mtu, ..self }
    }

    /// The same network on a bridge of another name, its pods' host
    /// interfaces named `prefix` and the end of their ids: for a tool
    /// beside Legion on the host (jailer's `jailer0`). `None` for a name
    /// FreeBSD would not take, or a prefix leaving fewer than eight
    /// characters of an id.
    pub fn named(self, bridge: &str, prefix: &str) -> Option<Self> {
        let name = |n: &str, max: usize| {
            !n.is_empty()
                && n.len() <= max
                && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                && n.as_bytes()[0].is_ascii_alphabetic()
        };
        (name(bridge, IFNAMSIZ) && name(prefix, IFNAMSIZ - 8)).then(|| PodNetwork {
            bridge: bridge.to_owned(),
            prefix: prefix.to_owned(),
            ..self
        })
    }

    /// The host end's name for the pod `id`, as [`host_interface`] names it
    /// with this network's prefix.
    pub fn host_interface(&self, id: &str) -> String {
        named_interface(&self.prefix, id)
    }

    /// The bridge's address, every pod's default route.
    pub fn gateway(&self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.subnet.network()) + 1)
    }

    /// The bridge's IPv6 address.
    pub fn gateway6(&self) -> Ipv6Addr {
        self.twin(self.gateway())
    }

    /// This node's /64.
    pub fn subnet6(&self) -> Ipv6Net {
        match self.v6 {
            PodV6::Within(net) => net,
            PodV6::Cluster | PodV6::Off => {
                Ipv6Net::new(self.twin(self.subnet.network()), NODE_PREFIX6)
            }
        }
    }

    /// A pod's IPv6 address, [`v6`]'s; a subnet outside the cluster network
    /// (some tests use one) gets the same from its last two octets. In a
    /// network [`Self::with_v6`], its address in that /64 (see [`PodV6`]).
    pub fn twin(&self, ip: Ipv4Addr) -> Ipv6Addr {
        let [.., c, d] = ip.octets();
        if let PodV6::Within(net) = self.v6 {
            let [a, b, e, f, ..] = net.addr.segments();
            return Ipv6Addr::new(a, b, e, f, 0, 0, c.into(), d.into());
        }
        let [a, b, e, ..] = CLUSTER_POD_CIDR6.addr.segments();
        Ipv6Addr::new(a, b, e, c.into(), 0, 0, 0, d.into())
    }

    /// `ip`'s IPv6 twin as an interface address, unless IPv6 is off.
    fn addr6(&self, ip: Ipv4Addr) -> Option<Ipv6Net> {
        (self.v6 != PodV6::Off).then(|| Ipv6Net::new(self.twin(ip), NODE_PREFIX6))
    }

    fn broadcast(&self) -> Ipv4Addr {
        let host_bits = 32 - u32::from(self.subnet.prefix);
        Ipv4Addr::from(u32::from(self.subnet.network()) | ((1u32 << host_bits) - 1))
    }

    /// Creates the bridge if it is missing and gives it the gateway address.
    pub fn ensure_bridge(&self, sys: &dyn Sys) -> Result<(), Error> {
        let bridge = self.bridge.as_str();
        if !sys.interfaces(None)?.iter().any(|i| i == bridge) {
            sys.bridge_create(bridge)?;
            tracing::info!(bridge, "created the pod bridge");
        }
        sys.interface_configure(
            None,
            bridge,
            &IfConfig {
                addr: Some(Ipv4Net::new(self.gateway(), self.subnet.prefix)),
                addr6: self.addr6(self.gateway()),
                mtu: Some(self.mtu),
                up: true,
            },
        )
    }

    /// The lowest free pod address: not the network, gateway or broadcast
    /// address, and not in `used`.
    pub fn allocate(&self, used: &BTreeSet<Ipv4Addr>) -> Option<Ipv4Addr> {
        let first = u32::from(self.gateway()) + 1;
        let last = u32::from(self.broadcast()) - 1;
        (first..=last)
            .map(Ipv4Addr::from)
            .find(|ip| !used.contains(ip))
    }

    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        self.subnet.contains(ip) && ip != self.gateway() && ip != self.broadcast()
    }

    /// Gives the pod in `jail` the address `ip`: an epair whose host end
    /// (named [`Self::host_interface`]`(id)`) joins the bridge and whose other end
    /// moves into the jail. Does nothing if the host end exists already.
    pub fn attach(&self, sys: &dyn Sys, id: &str, jail: &str, ip: Ipv4Addr) -> Result<(), Error> {
        let host = self.host_interface(id);
        if sys.interfaces(None)?.contains(&host) {
            return Ok(());
        }
        let pair = sys.epair_create()?;
        let result = (|| {
            sys.interface_rename(&pair.a, &host)?;
            self.join(sys, &host)?;
            sys.interface_move(&pair.b, jail)?;
            // A new VNET's loopback has only ::1; rc gives the host its
            // 127.0.0.1, so a pod needs it set too.
            sys.interface_configure(
                Some(jail),
                "lo0",
                &IfConfig {
                    addr: Some(Ipv4Net::new(Ipv4Addr::LOCALHOST, 8)),
                    up: true,
                    ..Default::default()
                },
            )?;
            sys.interface_configure(
                Some(jail),
                &pair.b,
                &IfConfig {
                    addr: Some(Ipv4Net::new(ip, self.subnet.prefix)),
                    addr6: self.addr6(ip),
                    mtu: Some(self.mtu),
                    up: true,
                },
            )?;
            sys.route_add(Some(jail), Destination::Default, self.gateway().into())?;
            if self.v6 == PodV6::Off {
                return Ok(());
            }
            let policies: Vec<(Ipv6Net, u32, u32)> = ADDR_POLICIES
                .iter()
                .map(|(p, prec, label)| (p.parse().expect("a prefix"), *prec, *label))
                .collect();
            sys.addr_policy_add(jail, &policies)?;
            sys.route_add(Some(jail), Destination::Default6, self.gateway6().into())
        })();
        if let Err(e) = result {
            // Either end name works until the rename; try both.
            for name in [&host, &pair.a] {
                if sys.interface_destroy(name).is_ok() {
                    break;
                }
            }
            return Err(e);
        }
        Ok(())
    }

    /// Puts a pod's host end (an epair's, a guest's tap) on the bridge,
    /// with the bridge's MTU and Ethernet address. With an address of its
    /// own, the pod's ARP for the gateway could be answered with it: the
    /// bridge hands a broadcast up on the member it came in on as well as
    /// on itself, and ARP answers with the address of the interface it
    /// heard on, twice, the last answer winning. Frames to the member's
    /// address are then taken in on the member, where the VIPs' rules,
    /// which are on the bridge, never see them, and the pod's VIPs fail
    /// until the host's next ARP request gives it the bridge's address
    /// again. Neighbor discovery answers on the bridge only.
    pub fn join(&self, sys: &dyn Sys, name: &str) -> Result<(), Error> {
        sys.interface_set_ether(name, &sys.interface_ether(&self.bridge)?)?;
        sys.interface_configure(
            None,
            name,
            &IfConfig {
                mtu: Some(self.mtu),
                up: true,
                ..Default::default()
            },
        )?;
        sys.bridge_add_member(&self.bridge, name)
    }

    /// Removes the pod's epair, wherever its jail end is.
    pub fn detach(&self, sys: &dyn Sys, id: &str) -> Result<(), Error> {
        let host = self.host_interface(id);
        if sys.interfaces(None)?.contains(&host) {
            sys.interface_destroy(&host)?;
        }
        Ok(())
    }
}

/// Host-side interface name for a pod: [`HOST_PREFIX`] and the last 12
/// characters of its id (a task uid, hex), within the 15-character limit.
pub fn host_interface(id: &str) -> String {
    named_interface(HOST_PREFIX, id)
}

/// `prefix` and the last 12 characters of `id`, or as many as the name has
/// room for.
fn named_interface(prefix: &str, id: &str) -> String {
    let room = IFNAMSIZ.saturating_sub(prefix.len()).min(12);
    let tail = &id[id.len().saturating_sub(room)..];
    format!("{prefix}{tail}")
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn subnets() {
        assert_eq!(node_subnet(0).unwrap().to_string(), "10.200.0.0/24");
        assert_eq!(node_subnet(3).unwrap().to_string(), "10.200.3.0/24");
        assert_eq!(node_subnet(255).unwrap().to_string(), "10.200.255.0/24");
        assert!(node_subnet(256).is_none());
        let taken: BTreeSet<_> = [node_subnet(0).unwrap(), node_subnet(1).unwrap()].into();
        assert_eq!(free_node_subnet(&taken), node_subnet(2));
    }

    #[test]
    fn twins() {
        let twin = |s: &str| v6(ip(s)).map(|a| a.to_string());
        assert_eq!(twin("10.200.3.17").as_deref(), Some("fd00:10:200:3::11"));
        assert_eq!(
            twin("10.200.255.254").as_deref(),
            Some("fd00:10:200:ff::fe")
        );
        assert_eq!(twin("10.201.0.7").as_deref(), Some("fd00:10:201::7"));
        assert_eq!(twin("10.201.1.2").as_deref(), Some("fd00:10:201::1:2"));
        assert_eq!(twin("100.64.3.1").as_deref(), Some("fd00:100:64::3:1"));
        assert_eq!(twin("192.0.2.1"), None);
        for s in ["10.200.3.17", "10.201.1.2", "100.64.3.1"] {
            let t = v6(ip(s)).unwrap();
            let ranges = [CLUSTER_POD_CIDR6, SERVICE_CIDR6, mesh::UNDERLAY6];
            assert_eq!(ranges.iter().filter(|r| r.contains(t)).count(), 1, "{s}");
        }
        let node = node_subnet6("10.200.3.0/24".parse().unwrap()).unwrap();
        assert_eq!(node.to_string(), "fd00:10:200:3::/64");
        assert!(CLUSTER_POD_CIDR6.contains(node.addr));
        assert_eq!(node_subnet6("10.200.0.0/16".parse().unwrap()), None);
        // A test subnet outside the cluster: twins as node 0's.
        let other = PodNetwork::new("10.77.0.0/24".parse().unwrap());
        assert_eq!(other.gateway6().to_string(), "fd00:10:200::1");
    }

    #[test]
    fn addresses() {
        let net = PodNetwork::new("10.200.3.77/24".parse().unwrap());
        assert_eq!(net.subnet.to_string(), "10.200.3.0/24");
        assert_eq!(net.gateway(), ip("10.200.3.1"));
        let used: BTreeSet<_> = [ip("10.200.3.2"), ip("10.200.3.4")].into();
        assert_eq!(net.allocate(&used), Some(ip("10.200.3.3")));
        assert_eq!(
            net.gateway6(),
            "fd00:10:200:3::1".parse::<Ipv6Addr>().unwrap()
        );
        assert_eq!(net.subnet6().to_string(), "fd00:10:200:3::/64");
        assert_eq!(net.twin(ip("10.200.3.17")).to_string(), "fd00:10:200:3::11");
        let full: BTreeSet<_> = (2..=254).map(|n| Ipv4Addr::new(10, 200, 3, n)).collect();
        assert_eq!(net.allocate(&full), None);
        assert!(net.contains(ip("10.200.3.9")));
        assert!(!net.contains(ip("10.200.3.255")) && !net.contains(ip("10.200.4.9")));
        assert_eq!(host_interface("00000000000004d2"), "lp0000000004d2");
    }

    #[test]
    fn attach_and_detach() {
        let sys = FakeSys::new();
        let net = PodNetwork::new("10.200.3.0/24".parse().unwrap());
        net.ensure_bridge(&sys).unwrap();
        net.ensure_bridge(&sys).unwrap(); // again: fine
        let bridge = sys.interface(BRIDGE).unwrap();
        assert_eq!(bridge.config.addr.unwrap().to_string(), "10.200.3.1/24");
        assert_eq!(
            bridge.config.addr6.unwrap().to_string(),
            "fd00:10:200:3::1/64"
        );

        sys.add_jail("infra1", true);
        net.attach(&sys, "00000000000000aa", "infra1", ip("10.200.3.2"))
            .unwrap();
        net.attach(&sys, "00000000000000aa", "infra1", ip("10.200.3.2"))
            .unwrap(); // adopted
        let host = host_interface("00000000000000aa");
        assert_eq!(
            sys.interface(BRIDGE).unwrap().members,
            std::slice::from_ref(&host)
        );
        // The bridge's own address: ARP's answers name the bridge.
        assert_eq!(sys.interface(&host).unwrap().ether, bridge.ether);
        assert!(bridge.ether.is_some());
        let inside = sys.interfaces(Some("infra1")).unwrap();
        assert_eq!(inside, ["lo0", "epair0b"]);
        let b = sys.interface("epair0b").unwrap();
        assert_eq!(b.config.addr.unwrap().to_string(), "10.200.3.2/24");
        assert_eq!(b.config.addr6.unwrap().to_string(), "fd00:10:200:3::2/64");
        let routes = sys.routes(Some("infra1"));
        assert_eq!(routes[&Destination::Default], net.gateway());
        assert_eq!(routes[&Destination::Default6], net.gateway6());
        // The host's address selection: IPv4 before a ULA source's IPv6.
        let policies = sys.addr_policies("infra1");
        assert_eq!(policies.len(), 9);
        let ranked = |p: &str| {
            policies
                .iter()
                .find(|(n, ..)| n.to_string() == p)
                .map(|(_, prec, label)| (*prec, *label))
        };
        assert_eq!(ranked("::ffff:0.0.0.0/96"), Some((100, 4)));
        assert_eq!(ranked("fc00::/7"), Some((3, 13)));

        // A jail without VNET cannot take the interface; nothing is left.
        sys.add_jail("plain", false);
        assert!(
            net.attach(&sys, "00000000000000bb", "plain", ip("10.200.3.3"))
                .is_err()
        );
        assert!(sys.interface(&host_interface("00000000000000bb")).is_none());
        assert!(sys.interface("epair1a").is_none() && sys.interface("epair1b").is_none());

        net.detach(&sys, "00000000000000aa").unwrap();
        net.detach(&sys, "00000000000000aa").unwrap(); // gone: fine
        assert!(sys.interface("epair0b").is_none());
        assert!(sys.interface(BRIDGE).unwrap().members.is_empty());
    }

    #[test]
    fn a_network_of_other_names() {
        let sys = FakeSys::new();
        let net = PodNetwork::new("10.88.0.0/16".parse().unwrap())
            .named("jailer0", "jp")
            .unwrap();
        net.ensure_bridge(&sys).unwrap();
        assert!(sys.interface(BRIDGE).is_none(), "not Legion's");
        let bridge = sys.interface("jailer0").unwrap();
        assert_eq!(bridge.config.addr.unwrap().to_string(), "10.88.0.1/16");
        sys.add_jail("j1", true);
        let id = "0123456789abcdef0123";
        net.attach(&sys, id, "j1", ip("10.88.0.2")).unwrap();
        let host = net.host_interface(id);
        // The end of its id, as Legion's pods' are named.
        assert_eq!(host, "jp89abcdef0123");
        assert_eq!(
            sys.interface("jailer0").unwrap().members,
            std::slice::from_ref(&host)
        );
        net.detach(&sys, id).unwrap();
        assert!(sys.interface(&host).is_none());
        // Legion's own names, unchanged.
        let legion = PodNetwork::new("10.200.3.0/24".parse().unwrap());
        assert_eq!(
            (legion.bridge.as_str(), legion.prefix.as_str()),
            (BRIDGE, "lp")
        );
        assert_eq!(legion.host_interface(id), host_interface(id));
        // Names FreeBSD would not take.
        let named = |b: &str, p: &str| legion.clone().named(b, p).is_some();
        assert!(!named("", "jp"));
        assert!(!named("a-very-long-bridge", "jp"));
        assert!(!named("jailer0", "prefix_too_long"));
        assert!(!named("jailer 0", "jp"));
        assert!(!named("0bridge", "jp"));
    }

    #[test]
    fn a_named_network_keeps_off_legions_v6() {
        let jailer = || {
            PodNetwork::new("10.88.0.0/16".parse().unwrap())
                .named("jailer0", "jp")
                .unwrap()
        };
        let legion = PodNetwork::new("10.88.0.0/16".parse().unwrap());
        // Its own /64: the bridge and a pod there, nothing of Legion's.
        let sys = FakeSys::new();
        let net = jailer()
            .with_v6("fd00:88::/48".parse().unwrap())
            .unwrap()
            .with_mtu(1500);
        net.ensure_bridge(&sys).unwrap();
        let bridge = sys.interface("jailer0").unwrap();
        assert_eq!(bridge.config.addr6.unwrap().to_string(), "fd00:88::1/64");
        assert_ne!(net.gateway6(), legion.gateway6());
        assert_eq!(bridge.config.mtu, Some(1500));
        assert_eq!(net.subnet6().to_string(), "fd00:88::/64");
        sys.add_jail("j1", true);
        net.attach(&sys, "00000000000000aa", "j1", ip("10.88.1.5"))
            .unwrap();
        let b = sys.interface("epair0b").unwrap();
        assert_eq!(b.config.addr6.unwrap().to_string(), "fd00:88::1:5/64");
        assert_eq!(b.config.mtu, Some(1500));
        let routes = sys.routes(Some("j1"));
        assert_eq!(routes[&Destination::Default6], net.gateway6());
        assert!(jailer().with_v6("fd00:88::/96".parse().unwrap()).is_none());

        // Without IPv6: none on the bridge or the pod, and no route.
        let sys = FakeSys::new();
        let net = jailer().without_v6();
        net.ensure_bridge(&sys).unwrap();
        assert_eq!(sys.interface("jailer0").unwrap().config.addr6, None);
        sys.add_jail("j2", true);
        net.attach(&sys, "00000000000000bb", "j2", ip("10.88.0.2"))
            .unwrap();
        assert_eq!(sys.interface("epair0b").unwrap().config.addr6, None);
        assert!(!sys.routes(Some("j2")).contains_key(&Destination::Default6));
        assert!(sys.addr_policies("j2").is_empty());

        // Legion's own, unchanged.
        assert_eq!((legion.v6, legion.mtu), (PodV6::Cluster, MTU));
    }
}
