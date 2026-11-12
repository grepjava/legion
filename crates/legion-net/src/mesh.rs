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
    /// Behind a NAT no other node gets through.
    pub nat: bool,
    /// Relays between nodes behind NAT.
    pub relay: bool,
}

/// This node's end of the mesh.
#[derive(Debug, Clone)]
pub struct Mesh {
    pub subnet: Ipv4Net,
    pub private_key: String,
    pub port: u16,
    /// Behind a NAT no other node gets through: traffic to other such
    /// nodes goes through a relay, as neither can reach the other.
    pub nat: bool,
}

/// The relay this node's traffic to `to` goes through, if both are behind
/// NAT: of the relays not behind NAT themselves, the one with the lowest
/// subnet, so every node picks the same while the list is the same.
pub fn relay_for<'a>(nat: bool, to: &Peer, peers: &'a [Peer], own: Ipv4Net) -> Option<&'a Peer> {
    if !(nat && to.nat) {
        return None;
    }
    peers
        .iter()
        .filter(|p| p.relay && !p.nat && p.subnet.network() != own.network())
        .min_by_key(|p| u32::from(p.subnet.network()))
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
    /// subnet, and touching only peers that changed. `before` is the list
    /// applied last (empty at first): a peer's advertised endpoint is set
    /// when the peer is new to `wg0` or advertises another than in
    /// `before`, and otherwise the one WireGuard has stands. That may be
    /// one it learned from the peer's packets, the only one that reaches a
    /// node behind NAT, which advertises an address of its own network.
    pub fn apply(&self, sys: &dyn Sys, peers: &[Peer], before: &[Peer]) -> Result<(), Error> {
        let own = self.subnet.network();
        let addresses = |p: &Peer| -> Option<Vec<IpNet>> {
            let subnet = Ipv4Net::new(p.subnet.network(), p.subnet.prefix);
            Some(vec![
                subnet.into(),
                Ipv4Net::new(underlay_addr(subnet)?, 32).into(),
                node_subnet6(subnet)?.into(),
                Ipv6Net::new(underlay_addr6(subnet)?, 128).into(),
            ])
        };
        let mut wanted: BTreeMap<&str, WgPeer> = BTreeMap::new();
        // Peers neither side of which can reach the other: their addresses
        // go to the relay, which forwards them on.
        let mut relayed: Vec<(&str, Vec<IpNet>)> = Vec::new();
        for p in peers.iter().filter(|p| p.subnet.network() != own) {
            let Some(allowed_ips) = addresses(p) else {
                continue;
            };
            if let Some(r) = relay_for(self.nat, p, peers, self.subnet) {
                relayed.push((r.public_key.as_str(), allowed_ips));
                continue;
            }
            wanted.insert(
                p.public_key.as_str(),
                WgPeer {
                    public_key: p.public_key.clone(),
                    endpoint: p.endpoint.clone(),
                    allowed_ips,
                    keepalive: Some(KEEPALIVE),
                },
            );
        }
        for (relay, allowed_ips) in relayed {
            if let Some(r) = wanted.get_mut(relay) {
                r.allowed_ips.extend(allowed_ips);
            }
        }
        for p in wanted.values_mut() {
            p.allowed_ips.sort();
            p.allowed_ips.dedup();
        }
        let current = sys.wg_peers(INTERFACE)?;
        for p in &current {
            if !wanted.contains_key(p.public_key.as_str()) {
                sys.wg_remove_peer(INTERFACE, &p.public_key)?;
                tracing::info!(peer = %p.public_key, "removed a WireGuard peer");
            }
        }
        for (key, want) in &wanted {
            let have = current.iter().find(|p| p.public_key == *key);
            let advertised = |list: &[Peer]| {
                list.iter()
                    .find(|p| p.public_key == *key)
                    .map(|p| p.endpoint.clone())
            };
            let moved = match advertised(before) {
                Some(was) => was != want.endpoint,
                None => false,
            };
            let mut want = want.clone();
            if have.is_some_and(|h| h.endpoint.is_some()) && !moved {
                // Unset, WireGuard keeps the endpoint it has.
                want.endpoint = None;
            }
            let same = have.is_some_and(|h| {
                h.allowed_ips == want.allowed_ips
                    && h.keepalive == want.keepalive
                    && (want.endpoint.is_none() || h.endpoint == want.endpoint)
            });
            if !same {
                sys.wg_set_peer(INTERFACE, &want)?;
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
            nat: false,
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
            nat: false,
            relay: false,
        };
        let me = peer("pub:key1", Some("192.0.2.1:51820"), "10.200.1.0/24");
        let two = peer("pub:key2", Some("192.0.2.2:51820"), "10.200.2.0/24");
        let three = peer("pub:key3", None, "10.200.3.0/24");
        let first = [me.clone(), two.clone(), three.clone()];
        mesh.apply(&sys, &first, &[]).unwrap();
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
        let second = [me.clone(), moved.clone(), three.clone()];
        mesh.apply(&sys, &second, &first).unwrap();
        let peers = sys.wireguard(INTERFACE).unwrap().peers;
        assert_eq!(peers.keys().collect::<Vec<_>>(), ["pub:key2", "pub:key3"]);
        assert_eq!(
            peers["pub:key2"].endpoint.as_deref(),
            Some("192.0.2.22:51820")
        );
        assert_eq!(peers["pub:key3"], learned);

        // Four is behind NAT: it advertises an address of its own network,
        // set while it is new, and reaches us from its NAT's.
        let four = peer("pub:key4", Some("192.168.1.4:51820"), "10.200.4.0/24");
        let third = [me.clone(), moved.clone(), three.clone(), four.clone()];
        mesh.apply(&sys, &third, &second).unwrap();
        let peers = sys.wireguard(INTERFACE).unwrap().peers;
        assert_eq!(
            peers["pub:key4"].endpoint.as_deref(),
            Some("192.168.1.4:51820")
        );
        let nat = WgPeer {
            endpoint: Some("203.0.113.9:61000".into()),
            ..peers["pub:key4"].clone()
        };
        sys.add_wg_peer(INTERFACE, nat.clone());
        // The list changes elsewhere, and after a restart (nothing applied
        // before): the endpoint learned stays.
        let five = peer("pub:key5", Some("192.0.2.5:51820"), "10.200.5.0/24");
        let fourth = [me.clone(), moved, three, four, five];
        mesh.apply(&sys, &fourth, &third).unwrap();
        mesh.apply(&sys, &fourth, &[]).unwrap();
        let peers = sys.wireguard(INTERFACE).unwrap().peers;
        assert_eq!(peers["pub:key4"], nat);
        assert_eq!(peers["pub:key3"], learned);
        // Until it advertises another.
        let mut fifth = fourth.clone();
        fifth[3].endpoint = Some("192.168.7.4:51820".into());
        mesh.apply(&sys, &fifth, &fourth).unwrap();
        assert_eq!(
            sys.wireguard(INTERFACE).unwrap().peers["pub:key4"]
                .endpoint
                .as_deref(),
            Some("192.168.7.4:51820")
        );

        mesh.apply(&sys, &[], &fifth).unwrap();
        assert!(sys.wireguard(INTERFACE).unwrap().peers.is_empty());
    }

    /// Two nodes behind NAT reach each other through a relay: each has the
    /// other's addresses on the relay's peer, and none of its own; a node
    /// not behind NAT keeps every peer direct.
    #[test]
    fn nodes_behind_nat_go_through_a_relay() {
        let peer = |key: &str, subnet: &str, nat: bool, relay: bool| Peer {
            public_key: key.into(),
            endpoint: Some(format!("192.0.2.{}:51820", &key[3..])),
            subnet: net(subnet),
            nat,
            relay,
        };
        let peers = [
            peer("pub1", "10.200.1.0/24", false, true),
            peer("pub2", "10.200.2.0/24", true, false),
            peer("pub3", "10.200.3.0/24", true, false),
            peer("pub4", "10.200.4.0/24", false, false),
        ];
        let mesh = |n: u8, nat: bool| Mesh {
            subnet: net(&format!("10.200.{n}.0/24")),
            private_key: format!("key{n}"),
            port: PORT,
            nat,
        };
        let peers_of = |m: &Mesh| {
            let sys = FakeSys::new();
            m.ensure(&sys).unwrap();
            m.apply(&sys, &peers, &[]).unwrap();
            let mut got: Vec<(String, Vec<String>)> = sys
                .wg_peers(INTERFACE)
                .unwrap()
                .into_iter()
                .map(|p| {
                    let mut ips: Vec<String> = p
                        .allowed_ips
                        .iter()
                        .map(ToString::to_string)
                        .filter(|a| a.ends_with("/24"))
                        .collect();
                    ips.sort();
                    (p.public_key, ips)
                })
                .collect();
            got.sort();
            got
        };
        // Node 2 behind NAT: node 3, behind NAT too, through the relay.
        assert_eq!(
            peers_of(&mesh(2, true)),
            [
                (
                    "pub1".into(),
                    vec!["10.200.1.0/24".into(), "10.200.3.0/24".into()]
                ),
                ("pub4".into(), vec!["10.200.4.0/24".into()]),
            ]
        );
        // The relay and a node not behind NAT: every peer direct.
        for n in [1, 4] {
            assert_eq!(peers_of(&mesh(n, false)).len(), 3, "node {n}");
        }
        // No relay at all: direct, for what it is worth.
        let no_relay: Vec<Peer> = peers
            .iter()
            .cloned()
            .map(|p| Peer { relay: false, ..p })
            .collect();
        assert_eq!(
            relay_for(true, &no_relay[2], &no_relay, net("10.200.2.0/24")),
            None
        );
    }
}
