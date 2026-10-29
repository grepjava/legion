//! The WireGuard mesh between nodes (DESIGN.md §5.3).
//!
//! Every node has a `wg0` with an address in the underlay network, derived
//! from its pod subnet (`10.200.<n>.0/24` has `100.64.<n>.1`), and every
//! other node as a peer whose allowed IPs are that node's underlay address
//! and pod subnet. One route sends the whole cluster pod network into
//! `wg0`; this node's own subnet is on the bridge, a longer match, and
//! WireGuard picks the peer for each packet by its allowed IPs. So routes
//! never change as nodes come and go: only peers do. IPv6 is the same,
//! with the twins of the addresses and subnets ([`crate::v6`]).

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};

use legion_sys::{Destination, IfConfig, IpNet, Ipv4Net, Ipv6Net, Sys, WgPeer};

use crate::{CLUSTER_POD_CIDR, CLUSTER_POD_CIDR6, Error, MTU, NODE_PREFIX, node_subnet6, v6};

pub const INTERFACE: &str = "wg0";
/// The UDP port every node listens on.
pub const PORT: u16 = 51820;
/// One address per node, on `wg0`.
pub const UNDERLAY: Ipv4Net = Ipv4Net {
    addr: Ipv4Addr::new(100, 64, 0, 0),
    prefix: 16,
};
/// The IPv6 twin of [`UNDERLAY`].
pub const UNDERLAY6: Ipv6Net = Ipv6Net {
    addr: Ipv6Addr::new(0xfd00, 0x100, 0x64, 0, 0, 0, 0, 0),
    prefix: 96,
};
/// Keeps NAT mappings open, so a node behind NAT stays reachable.
const KEEPALIVE: u16 = 25;

/// The underlay address of the node holding pod subnet `subnet`.
pub fn underlay_addr(subnet: Ipv4Net) -> Option<Ipv4Addr> {
    let base = u32::from(CLUSTER_POD_CIDR.network());
    let net = u32::from(subnet.network());
    if subnet.prefix != NODE_PREFIX || !CLUSTER_POD_CIDR.contains(subnet.network()) {
        return None;
    }
    let n = (net - base) >> (32 - NODE_PREFIX);
    Some(Ipv4Addr::from(u32::from(UNDERLAY.network()) + (n << 8) + 1))
}

/// [`underlay_addr`]'s IPv6 twin.
pub fn underlay_addr6(subnet: Ipv4Net) -> Option<Ipv6Addr> {
    underlay_addr(subnet).and_then(v6)
}

/// Another node, as the mesh needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub public_key: String,
    /// `host:port`; without one, the peer must reach this node first.
    pub endpoint: Option<String>,
    pub subnet: Ipv4Net,
}

/// This node's end of the mesh.
#[derive(Debug, Clone)]
pub struct Mesh {
    pub subnet: Ipv4Net,
    pub private_key: String,
    pub port: u16,
}

impl Mesh {
    /// Creates `wg0` if it is missing, and sets its key, port, address and
    /// the route into it. Peers are left as they are.
    pub fn ensure(&self, sys: &dyn Sys) -> Result<(), Error> {
        let (addr, addr6) = underlay_addr(self.subnet)
            .zip(underlay_addr6(self.subnet))
            .ok_or_else(|| Error::Parse {
                command: "mesh".into(),
                message: format!("{} is not a node subnet of {CLUSTER_POD_CIDR}", self.subnet),
            })?;
        if !sys.interfaces(None)?.iter().any(|i| i == INTERFACE) {
            sys.wg_create(INTERFACE)?;
            tracing::info!(interface = INTERFACE, "created the WireGuard interface");
        }
        sys.wg_configure(INTERFACE, &self.private_key, self.port)?;
        sys.interface_configure(
            None,
            INTERFACE,
            &IfConfig {
                addr: Some(Ipv4Net::new(addr, UNDERLAY.prefix)),
                addr6: Some(Ipv6Net::new(addr6, UNDERLAY6.prefix)),
                mtu: Some(MTU),
                up: true,
            },
        )?;
        sys.route_to_interface(
            None,
            Destination::Net(Ipv4Net::new(
                CLUSTER_POD_CIDR.network(),
                CLUSTER_POD_CIDR.prefix,
            )),
            INTERFACE,
        )?;
        sys.route_to_interface(None, Destination::Net6(CLUSTER_POD_CIDR6), INTERFACE)
    }

    /// Makes `wg0`'s peers exactly `peers`, leaving out this node's own
    /// subnet, and touching only peers that changed.
    pub fn apply(&self, sys: &dyn Sys, peers: &[Peer]) -> Result<(), Error> {
        let own = self.subnet.network();
        let wanted: BTreeMap<&str, WgPeer> = peers
            .iter()
            .filter(|p| p.subnet.network() != own)
            .filter_map(|p| {
                let subnet = Ipv4Net::new(p.subnet.network(), p.subnet.prefix);
                let mut allowed_ips: Vec<IpNet> = vec![
                    subnet.into(),
                    Ipv4Net::new(underlay_addr(subnet)?, 32).into(),
                    node_subnet6(subnet)?.into(),
                    Ipv6Net::new(underlay_addr6(subnet)?, 128).into(),
                ];
                allowed_ips.sort();
                Some((
                    p.public_key.as_str(),
                    WgPeer {
                        public_key: p.public_key.clone(),
                        endpoint: p.endpoint.clone(),
                        allowed_ips,
                        keepalive: Some(KEEPALIVE),
                    },
                ))
            })
            .collect();
        let current = sys.wg_peers(INTERFACE)?;
        for p in &current {
            if !wanted.contains_key(p.public_key.as_str()) {
                sys.wg_remove_peer(INTERFACE, &p.public_key)?;
                tracing::info!(peer = %p.public_key, "removed a WireGuard peer");
            }
        }
        for (key, want) in &wanted {
            let have = current.iter().find(|p| p.public_key == *key);
            // An endpoint WireGuard learned stands unless we know one.
            let same = have.is_some_and(|h| {
                h.allowed_ips == want.allowed_ips
                    && h.keepalive == want.keepalive
                    && (want.endpoint.is_none() || h.endpoint == want.endpoint)
            });
            if !same {
                sys.wg_set_peer(INTERFACE, want)?;
                tracing::info!(peer = %key, endpoint = ?want.endpoint, "set a WireGuard peer");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    fn net(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    #[test]
    fn underlay_addresses() {
        assert_eq!(
            underlay_addr(net("10.200.0.0/24")),
            Some(Ipv4Addr::new(100, 64, 0, 1))
        );
        assert_eq!(
            underlay_addr(net("10.200.3.0/24")),
            Some(Ipv4Addr::new(100, 64, 3, 1))
        );
        assert_eq!(
            underlay_addr(net("10.200.255.0/24")),
            Some(Ipv4Addr::new(100, 64, 255, 1))
        );
        assert_eq!(underlay_addr(net("10.201.0.0/24")), None);
        assert_eq!(underlay_addr(net("10.200.0.0/16")), None);
        assert_eq!(
            underlay_addr6(net("10.200.3.0/24")).unwrap().to_string(),
            "fd00:100:64::3:1"
        );
    }

    #[test]
    fn converges_peers() {
        let sys = FakeSys::new();
        let mesh = Mesh {
            subnet: net("10.200.1.0/24"),
            private_key: "key1".into(),
            port: PORT,
        };
        mesh.ensure(&sys).unwrap();
        mesh.ensure(&sys).unwrap(); // again: fine
        let wg = sys.wireguard(INTERFACE).unwrap();
        assert_eq!(
            (wg.private_key.as_deref(), wg.port),
            (Some("key1"), Some(PORT))
        );
        let i = sys.interface(INTERFACE).unwrap();
        assert_eq!(i.config.addr, Some(net("100.64.1.1/16")));
        assert_eq!(i.config.addr6, Some("fd00:100:64::1:1/96".parse().unwrap()));
        assert_eq!(i.config.mtu, Some(MTU));
        let routes = sys.interface_routes(None);
        assert_eq!(routes[&Destination::Net(net("10.200.0.0/16"))], INTERFACE);
        assert_eq!(
            routes[&Destination::Net6("fd00:10:200::/48".parse().unwrap())],
            INTERFACE
        );

        let peer = |key: &str, endpoint: Option<&str>, subnet: &str| Peer {
            public_key: key.into(),
            endpoint: endpoint.map(str::to_owned),
            subnet: net(subnet),
        };
        let me = peer("pub:key1", Some("192.0.2.1:51820"), "10.200.1.0/24");
        let two = peer("pub:key2", Some("192.0.2.2:51820"), "10.200.2.0/24");
        let three = peer("pub:key3", None, "10.200.3.0/24");
        mesh.apply(&sys, &[me.clone(), two.clone(), three.clone()])
            .unwrap();
        let peers = sys.wireguard(INTERFACE).unwrap().peers;
        assert_eq!(peers.keys().collect::<Vec<_>>(), ["pub:key2", "pub:key3"]);
        assert_eq!(
            peers["pub:key2"],
            WgPeer {
                public_key: "pub:key2".into(),
                endpoint: Some("192.0.2.2:51820".into()),
                allowed_ips: [
                    "10.200.2.0/24",
                    "100.64.2.1/32",
                    "fd00:10:200:2::/64",
                    "fd00:100:64::2:1/128"
                ]
                .iter()
                .map(|n| n.parse().unwrap())
                .collect(),
                keepalive: Some(KEEPALIVE),
            }
        );

        // Three reached us first: the endpoint WireGuard learned stays.
        let learned = WgPeer {
            endpoint: Some("198.51.100.3:4000".into()),
            ..peers["pub:key3"].clone()
        };
        sys.add_wg_peer(INTERFACE, learned.clone());
        // Two moved; a stranger is removed.
        sys.add_wg_peer(
            INTERFACE,
            WgPeer {
                public_key: "stranger".into(),
                endpoint: None,
                allowed_ips: vec![net("10.200.9.0/24").into()],
                keepalive: None,
            },
        );
        let moved = peer("pub:key2", Some("192.0.2.22:51820"), "10.200.2.0/24");
        mesh.apply(&sys, &[me, moved, three]).unwrap();
        let peers = sys.wireguard(INTERFACE).unwrap().peers;
        assert_eq!(peers.keys().collect::<Vec<_>>(), ["pub:key2", "pub:key3"]);
        assert_eq!(
            peers["pub:key2"].endpoint.as_deref(),
            Some("192.0.2.22:51820")
        );
        assert_eq!(peers["pub:key3"], learned);

        mesh.apply(&sys, &[]).unwrap();
        assert!(sys.wireguard(INTERFACE).unwrap().peers.is_empty());
    }
}
