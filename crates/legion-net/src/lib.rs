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

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use legion_sys::{Destination, IfConfig, Ipv4Net, Sys};

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
pub const BRIDGE: &str = "legion0";
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodNetwork {
    /// This node's subnet, e.g. `10.200.3.0/24`.
    pub subnet: Ipv4Net,
}

impl PodNetwork {
    pub fn new(subnet: Ipv4Net) -> Self {
        PodNetwork {
            subnet: Ipv4Net::new(subnet.network(), subnet.prefix),
        }
    }

    /// The bridge's address, every pod's default route.
    pub fn gateway(&self) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.subnet.network()) + 1)
    }

    fn broadcast(&self) -> Ipv4Addr {
        let host_bits = 32 - u32::from(self.subnet.prefix);
        Ipv4Addr::from(u32::from(self.subnet.network()) | ((1u32 << host_bits) - 1))
    }

    /// Creates the bridge if it is missing and gives it the gateway address.
    pub fn ensure_bridge(&self, sys: &dyn Sys) -> Result<(), Error> {
        if !sys.interfaces(None)?.iter().any(|i| i == BRIDGE) {
            sys.bridge_create(BRIDGE)?;
            tracing::info!(bridge = BRIDGE, "created the pod bridge");
        }
        sys.interface_configure(
            None,
            BRIDGE,
            &IfConfig {
                addr: Some(Ipv4Net::new(self.gateway(), self.subnet.prefix)),
                mtu: Some(MTU),
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
    /// (named [`host_interface`]`(id)`) joins the bridge and whose other end
    /// moves into the jail. Does nothing if the host end exists already.
    pub fn attach(&self, sys: &dyn Sys, id: &str, jail: &str, ip: Ipv4Addr) -> Result<(), Error> {
        let host = host_interface(id);
        if sys.interfaces(None)?.contains(&host) {
            return Ok(());
        }
        let pair = sys.epair_create()?;
        let result = (|| {
            sys.interface_rename(&pair.a, &host)?;
            sys.interface_configure(
                None,
                &host,
                &IfConfig {
                    mtu: Some(MTU),
                    up: true,
                    ..Default::default()
                },
            )?;
            sys.bridge_add_member(BRIDGE, &host)?;
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
                    mtu: Some(MTU),
                    up: true,
                },
            )?;
            sys.route_add(Some(jail), Destination::Default, self.gateway())
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

    /// Removes the pod's epair, wherever its jail end is.
    pub fn detach(&self, sys: &dyn Sys, id: &str) -> Result<(), Error> {
        let host = host_interface(id);
        if sys.interfaces(None)?.contains(&host) {
            sys.interface_destroy(&host)?;
        }
        Ok(())
    }
}

/// Host-side interface name for a pod: `lp` and the last 12 characters of
/// its id (a task uid, hex), within the 15-character limit.
pub fn host_interface(id: &str) -> String {
    let tail = &id[id.len().saturating_sub(12)..];
    format!("lp{tail}")
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
    fn addresses() {
        let net = PodNetwork::new("10.200.3.77/24".parse().unwrap());
        assert_eq!(net.subnet.to_string(), "10.200.3.0/24");
        assert_eq!(net.gateway(), ip("10.200.3.1"));
        let used: BTreeSet<_> = [ip("10.200.3.2"), ip("10.200.3.4")].into();
        assert_eq!(net.allocate(&used), Some(ip("10.200.3.3")));
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
        let inside = sys.interfaces(Some("infra1")).unwrap();
        assert_eq!(inside, ["lo0", "epair0b"]);
        let b = sys.interface("epair0b").unwrap();
        assert_eq!(b.config.addr.unwrap().to_string(), "10.200.3.2/24");
        assert_eq!(
            sys.routes(Some("infra1"))[&Destination::Default],
            net.gateway()
        );

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
}
