//! Network policy with pf (DESIGN.md §5.6), in the anchor `legion/policy`:
//! what may reach each pod on this node, and which VIPs it may use.
//!
//! Rules sit on the pod's host end (`lp…`): `out on` it is traffic to the
//! pod, `in on` it is traffic from the pod. pf sees traffic there only as
//! the bridge forwards it between its members, from pods on this node; a
//! packet the host itself sends onto the bridge, routed in from another
//! node or from the host, goes out the member unfiltered. Traffic from
//! other nodes' pods is therefore checked where it comes in, on the mesh
//! (`in on wg0`), against the same tables; the state made there carries
//! the connection the rest of its way. A pod may send only from its own
//! address, so it cannot pass for a node or for another namespace's task,
//! and whatever it starts is kept as state, so replies come back through
//! its own refusals.
//!
//! What a pod sends is checked as it leaves it (`in on` its host end): it
//! must come from the pod's address, a VIP only on the ports it may use,
//! and with egress limited, only to what that allows, and DNS to the
//! node's resolver; replies to what reached it pass as their connection's
//! state. What the host sends the pod (probes, VIP traffic the hairpin
//! rule NATed to the gateway, published ports' translated traffic) makes
//! no state on the host end, as pf does not see it there: for a pod with
//! egress limited, it is passed out on the bridge keeping a floating
//! state, which the pod's replies match. Other nodes' traffic already
//! floats from the mesh.
//!
//! What reaches a pod is checked here, on its node, by source: the nodes
//! (probes, other hosts, published ports reached through another node),
//! traffic a published port's or a VIP's rule translated here, and the
//! tables of tasks the agent allows. Traffic through a VIP to a task on
//! this same node is NATed to the gateway by the hairpin rule (lb.rs), so
//! its source is lost by then: VIPs are checked at the client pod instead,
//! while the destination is still the VIP.
//!
//! States on the host ends are bound to the interface (`if-bound`): a
//! floating one made there would match the same packet again on the
//! bridge, before the VIP's translation, and break it. Those on the mesh
//! float, so they also pass the packet out to the pod, and its replies.
//!
//! IPv6 gets the same rules with the twins of the addresses
//! ([`crate::v6`]); the tables hold both. Neighbor discovery, which IPv6
//! does over ICMPv6 where IPv4 has ARP below pf, passes on the host ends.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr};

use legion_sys::{Ipv4Net, Sys};

use crate::lb::{Proto, TAG, dual};
use crate::mesh::{INTERFACE as MESH, UNDERLAY, UNDERLAY6};
use crate::{BRIDGE, CLUSTER_POD_CIDR, CLUSTER_POD_CIDR6, SERVICE_CIDR6, v6};

pub const ANCHOR: &str = "legion/policy";

/// Sources that may reach a pod: a table, on some ports (all if empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allow {
    pub table: String,
    pub ports: Vec<(Proto, u16)>,
}

/// Where a pod may send, on some ports (all if empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Egress {
    pub to: Target,
    pub ports: Vec<(Proto, u16)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Any,
    /// A table of tasks, as [`Allow::table`].
    Table(String),
    /// IPv4 addresses (pods reach the outside over IPv4 only).
    Net(Ipv4Net),
}

/// A pod on this node, with what may reach it and what it may reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guard {
    /// Its host end.
    pub interface: String,
    pub ip: Ipv4Addr,
    /// Any task may reach it (its namespace is open).
    pub open: bool,
    /// Who else may, beyond the nodes and translated traffic.
    pub allowed: Vec<Allow>,
    /// The VIP ports it may use; other VIPs are refused at the pod.
    pub vips: Vec<(Ipv4Addr, Proto, u16)>,
    /// Where else it may send, if that is limited; besides these, only
    /// DNS to its gateway.
    pub egress: Option<Vec<Egress>>,
}

/// One address family's addresses for a guard's rules.
struct Family {
    name: &'static str,
    ip: IpAddr,
    /// This node's address on the bridge, the pods' resolver.
    gateway: IpAddr,
    /// The nodes: this one's gateway and the mesh range.
    nodes: String,
    /// The VIP range.
    vips: String,
    pods: String,
    /// The VIPs the pod may use.
    allowed_vips: Vec<(IpAddr, Proto, u16)>,
}

/// The anchor's rules. `gateway` is this node's address on the bridge;
/// `services` is the VIP range.
pub fn rules(guards: &[Guard], gateway: Ipv4Addr, services: Ipv4Net) -> String {
    let mut r = String::new();
    for g in guards {
        let inet = Family {
            name: "inet",
            ip: g.ip.into(),
            gateway: gateway.into(),
            nodes: format!(
                "{{ {gateway}, {}/{} }}",
                UNDERLAY.network(),
                UNDERLAY.prefix
            ),
            vips: format!("{}/{}", services.network(), services.prefix),
            pods: format!("{}/{}", CLUSTER_POD_CIDR.network(), CLUSTER_POD_CIDR.prefix),
            allowed_vips: g.vips.iter().map(|&(v, p, n)| (v.into(), p, n)).collect(),
        };
        guard_rules(&mut r, g, &inet);
        let lp = &g.interface;
        let (Some(ip6), Some(gateway6)) = (v6(g.ip), v6(gateway)) else {
            // No twin: link-local IPv6 would go around all this.
            let _ = writeln!(r, "block drop quick on {lp} inet6 all");
            continue;
        };
        let _ = writeln!(
            r,
            "pass quick on {lp} inet6 proto icmp6 icmp6-type {{ neighbrsol, neighbradv }} no state"
        );
        let inet6 = Family {
            name: "inet6",
            ip: ip6.into(),
            gateway: gateway6.into(),
            nodes: format!("{{ {gateway6}, {UNDERLAY6} }}"),
            vips: SERVICE_CIDR6.to_string(),
            pods: CLUSTER_POD_CIDR6.to_string(),
            allowed_vips: g
                .vips
                .iter()
                .filter_map(|&(v, p, n)| Some((v6(v)?.into(), p, n)))
                .collect(),
        };
        guard_rules(&mut r, g, &inet6);
    }
    r
}

/// A guard's rules for one family.
fn guard_rules(r: &mut String, g: &Guard, f: &Family) {
    let (lp, ip, af) = (&g.interface, f.ip, f.name);
    let state = "keep state (if-bound)";
    // From the pod.
    let _ = writeln!(r, "block drop in quick on {lp} {af} from ! {ip}");
    for (vip, proto, port) in &f.allowed_vips {
        let _ = writeln!(
            r,
            "pass in quick on {lp} {af} proto {} from {ip} to {vip} port {port} {state}",
            proto.as_str()
        );
    }
    let _ = writeln!(r, "block drop in quick on {lp} {af} to {}", f.vips);
    match &g.egress {
        None => {
            let _ = writeln!(r, "pass in quick on {lp} {af} from {ip} {state}");
        }
        Some(egress) => {
            let gateway = f.gateway;
            let _ = writeln!(
                r,
                "pass in quick on {lp} {af} proto {{ udp, tcp }} from {ip} to {gateway} port 53 {state}"
            );
            for e in egress {
                let to = match &e.to {
                    Target::Any => "any".to_owned(),
                    Target::Table(t) => format!("<{t}>"),
                    Target::Net(_) if af != "inet" => continue,
                    Target::Net(n) => format!("{}/{}", n.network(), n.prefix),
                };
                if e.ports.is_empty() {
                    let _ = writeln!(r, "pass in quick on {lp} {af} from {ip} to {to} {state}");
                }
                for (proto, port) in &e.ports {
                    let _ = writeln!(
                        r,
                        "pass in quick on {lp} {af} proto {} from {ip} to {to} port {port} {state}",
                        proto.as_str()
                    );
                }
            }
            let _ = writeln!(r, "block drop in quick on {lp} {af} all");
            // Replies to what the host sends it, which passes the host end
            // unseen: a floating state where pf does see it.
            let _ = writeln!(
                r,
                "pass out quick on {BRIDGE} {af} from {gateway} to {ip} keep state"
            );
            let _ = writeln!(
                r,
                "pass out quick on {BRIDGE} {af} to {ip} tagged {TAG} keep state"
            );
        }
    }
    // Its state there is bound to its host end; what it starts toward
    // other nodes needs one on the mesh as well, which its replies
    // match before the refusals below.
    let _ = writeln!(r, "pass out quick on {MESH} {af} from {ip} keep state");
    // To the pod.
    if g.open {
        let _ = writeln!(r, "pass out quick on {lp} {af} {state}");
        return;
    }
    let _ = writeln!(r, "pass out quick on {lp} {af} tagged {TAG} {state}");
    let _ = writeln!(r, "pass out quick on {lp} {af} from {} {state}", f.nodes);
    for a in &g.allowed {
        let from = format!("<{}>", a.table);
        if a.ports.is_empty() {
            let _ = writeln!(r, "pass out quick on {lp} {af} from {from} {state}");
        }
        for (proto, port) in &a.ports {
            let _ = writeln!(
                r,
                "pass out quick on {lp} {af} proto {} from {from} to {ip} port {port} {state}",
                proto.as_str()
            );
        }
    }
    let _ = writeln!(r, "block drop out quick on {lp} {af} all");
    // From other nodes' pods, which the host routes in from the mesh.
    for a in &g.allowed {
        let from = format!("<{}>", a.table);
        if a.ports.is_empty() {
            let _ = writeln!(
                r,
                "pass in quick on {MESH} {af} from {from} to {ip} keep state"
            );
        }
        for (proto, port) in &a.ports {
            let _ = writeln!(
                r,
                "pass in quick on {MESH} {af} proto {} from {from} to {ip} port {port} keep state",
                proto.as_str()
            );
        }
    }
    let _ = writeln!(
        r,
        "block drop in quick on {MESH} {af} from {} to {ip}",
        f.pods
    );
}

/// Keeps the anchor in line, doing only what changed.
#[derive(Debug, Default)]
pub struct Policy {
    loaded: Option<String>,
    tables: BTreeMap<String, Vec<IpAddr>>,
}

impl Policy {
    /// Loads `guards`' rules and sets `tables`, the source tables they name
    /// (with the addresses' twins).
    pub fn apply(
        &mut self,
        sys: &dyn Sys,
        guards: &[Guard],
        tables: &BTreeMap<String, Vec<Ipv4Addr>>,
        gateway: Ipv4Addr,
        services: Ipv4Net,
    ) -> legion_sys::Result<()> {
        // Tables first: rules naming one that is not there yet would see
        // it empty and refuse what they should let through.
        for (name, addrs) in tables {
            let addrs = dual(addrs);
            if self.tables.get(name) != Some(&addrs) {
                sys.pf_table_replace(ANCHOR, name, &addrs)?;
                self.tables.insert(name.clone(), addrs);
            }
        }
        let rules = rules(guards, gateway, services);
        if self.loaded.as_ref() != Some(&rules) {
            sys.pf_load_anchor(ANCHOR, &rules)?;
            self.loaded = Some(rules);
            // Loading rules drops the anchor's tables the rules do not
            // name; set every table again next time.
            self.tables.clear();
            for (name, addrs) in tables {
                let addrs = dual(addrs);
                sys.pf_table_replace(ANCHOR, name, &addrs)?;
                self.tables.insert(name.clone(), addrs);
            }
        }
        self.tables.retain(|name, _| tables.contains_key(name));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn guards() -> Vec<Guard> {
        vec![
            Guard {
                interface: "lp000000000007".into(),
                ip: ip("10.200.0.7"),
                open: false,
                allowed: vec![
                    Allow {
                        table: "n0123".into(),
                        ports: vec![],
                    },
                    Allow {
                        table: "p4567".into(),
                        ports: vec![(Proto::Tcp, 5432)],
                    },
                ],
                vips: vec![(ip("10.201.0.3"), Proto::Tcp, 80)],
                egress: None,
            },
            Guard {
                interface: "lp000000000008".into(),
                ip: ip("10.200.0.8"),
                open: true,
                allowed: vec![],
                vips: vec![],
                egress: None,
            },
        ]
    }

    #[test]
    fn renders_egress() {
        let services = Ipv4Net::new(ip("10.201.0.0"), 16);
        let g = Guard {
            interface: "lp000000000009".into(),
            ip: ip("10.200.0.9"),
            open: true,
            allowed: vec![],
            vips: vec![],
            egress: Some(vec![
                Egress {
                    to: Target::Table("tdb".into()),
                    ports: vec![(Proto::Tcp, 5432)],
                },
                Egress {
                    to: Target::Net(Ipv4Net::new(ip("192.0.2.0"), 24)),
                    ports: vec![],
                },
            ]),
        };
        let r = rules(&[g], ip("10.200.0.1"), services);
        let from_pod: Vec<&str> = r
            .lines()
            .filter(|l| l.contains("in quick on lp000000000009"))
            .collect();
        assert_eq!(
            from_pod,
            [
                "block drop in quick on lp000000000009 inet from ! 10.200.0.9",
                "block drop in quick on lp000000000009 inet to 10.201.0.0/16",
                "pass in quick on lp000000000009 inet proto { udp, tcp } from 10.200.0.9 to 10.200.0.1 port 53 keep state (if-bound)",
                "pass in quick on lp000000000009 inet proto tcp from 10.200.0.9 to <tdb> port 5432 keep state (if-bound)",
                "pass in quick on lp000000000009 inet from 10.200.0.9 to 192.0.2.0/24 keep state (if-bound)",
                "block drop in quick on lp000000000009 inet all",
                "block drop in quick on lp000000000009 inet6 from ! fd00:10:200::9",
                "block drop in quick on lp000000000009 inet6 to fd00:10:201::/96",
                "pass in quick on lp000000000009 inet6 proto { udp, tcp } from fd00:10:200::9 to fd00:10:200::1 port 53 keep state (if-bound)",
                "pass in quick on lp000000000009 inet6 proto tcp from fd00:10:200::9 to <tdb> port 5432 keep state (if-bound)",
                "block drop in quick on lp000000000009 inet6 all",
            ]
        );
        // What the host sends it keeps a state its replies match.
        for rule in [
            "pass out quick on legion0 inet from 10.200.0.1 to 10.200.0.9 keep state",
            "pass out quick on legion0 inet to 10.200.0.9 tagged LSVC keep state",
            "pass out quick on legion0 inet6 from fd00:10:200::1 to fd00:10:200::9 keep state",
        ] {
            assert!(r.contains(rule), "{rule}");
        }
        // Not for a pod whose egress is free: its own rule passes replies.
        assert!(!rules(&guards(), ip("10.200.0.1"), services).contains("on legion0"));
    }

    #[test]
    fn renders_rules() {
        let services = Ipv4Net::new(ip("10.201.0.0"), 16);
        assert_eq!(
            rules(&guards(), ip("10.200.0.1"), services),
            "block drop in quick on lp000000000007 inet from ! 10.200.0.7\n\
             pass in quick on lp000000000007 inet proto tcp from 10.200.0.7 to 10.201.0.3 port 80 keep state (if-bound)\n\
             block drop in quick on lp000000000007 inet to 10.201.0.0/16\n\
             pass in quick on lp000000000007 inet from 10.200.0.7 keep state (if-bound)\n\
             pass out quick on wg0 inet from 10.200.0.7 keep state\n\
             pass out quick on lp000000000007 inet tagged LSVC keep state (if-bound)\n\
             pass out quick on lp000000000007 inet from { 10.200.0.1, 100.64.0.0/16 } keep state (if-bound)\n\
             pass out quick on lp000000000007 inet from <n0123> keep state (if-bound)\n\
             pass out quick on lp000000000007 inet proto tcp from <p4567> to 10.200.0.7 port 5432 keep state (if-bound)\n\
             block drop out quick on lp000000000007 inet all\n\
             pass in quick on wg0 inet from <n0123> to 10.200.0.7 keep state\n\
             pass in quick on wg0 inet proto tcp from <p4567> to 10.200.0.7 port 5432 keep state\n\
             block drop in quick on wg0 inet from 10.200.0.0/16 to 10.200.0.7\n\
             pass quick on lp000000000007 inet6 proto icmp6 icmp6-type { neighbrsol, neighbradv } no state\n\
             block drop in quick on lp000000000007 inet6 from ! fd00:10:200::7\n\
             pass in quick on lp000000000007 inet6 proto tcp from fd00:10:200::7 to fd00:10:201::3 port 80 keep state (if-bound)\n\
             block drop in quick on lp000000000007 inet6 to fd00:10:201::/96\n\
             pass in quick on lp000000000007 inet6 from fd00:10:200::7 keep state (if-bound)\n\
             pass out quick on wg0 inet6 from fd00:10:200::7 keep state\n\
             pass out quick on lp000000000007 inet6 tagged LSVC keep state (if-bound)\n\
             pass out quick on lp000000000007 inet6 from { fd00:10:200::1, fd00:100:64::/96 } keep state (if-bound)\n\
             pass out quick on lp000000000007 inet6 from <n0123> keep state (if-bound)\n\
             pass out quick on lp000000000007 inet6 proto tcp from <p4567> to fd00:10:200::7 port 5432 keep state (if-bound)\n\
             block drop out quick on lp000000000007 inet6 all\n\
             pass in quick on wg0 inet6 from <n0123> to fd00:10:200::7 keep state\n\
             pass in quick on wg0 inet6 proto tcp from <p4567> to fd00:10:200::7 port 5432 keep state\n\
             block drop in quick on wg0 inet6 from fd00:10:200::/48 to fd00:10:200::7\n\
             block drop in quick on lp000000000008 inet from ! 10.200.0.8\n\
             block drop in quick on lp000000000008 inet to 10.201.0.0/16\n\
             pass in quick on lp000000000008 inet from 10.200.0.8 keep state (if-bound)\n\
             pass out quick on wg0 inet from 10.200.0.8 keep state\n\
             pass out quick on lp000000000008 inet keep state (if-bound)\n\
             pass quick on lp000000000008 inet6 proto icmp6 icmp6-type { neighbrsol, neighbradv } no state\n\
             block drop in quick on lp000000000008 inet6 from ! fd00:10:200::8\n\
             block drop in quick on lp000000000008 inet6 to fd00:10:201::/96\n\
             pass in quick on lp000000000008 inet6 from fd00:10:200::8 keep state (if-bound)\n\
             pass out quick on wg0 inet6 from fd00:10:200::8 keep state\n\
             pass out quick on lp000000000008 inet6 keep state (if-bound)\n"
        );
    }

    #[test]
    fn applies_only_changes() {
        let sys = FakeSys::new();
        let services = Ipv4Net::new(ip("10.201.0.0"), 16);
        let gateway = ip("10.200.0.1");
        let mut p = Policy::default();
        let mut tables = BTreeMap::from([
            ("n0123".to_owned(), vec![ip("10.200.1.5"), ip("10.200.0.7")]),
            ("p4567".to_owned(), vec![]),
        ]);
        p.apply(&sys, &guards(), &tables, gateway, services)
            .unwrap();
        assert!(sys.anchor(ANCHOR).unwrap().contains("lp000000000007"));
        assert_eq!(
            sys.pf_table_show(ANCHOR, "n0123").unwrap(),
            dual(&[ip("10.200.0.7"), ip("10.200.1.5")])
        );
        // A table changes; the rules stay.
        tables.insert("p4567".into(), vec![ip("10.200.2.9")]);
        p.apply(&sys, &guards(), &tables, gateway, services)
            .unwrap();
        assert_eq!(
            sys.pf_table_show(ANCHOR, "p4567").unwrap(),
            dual(&[ip("10.200.2.9")])
        );
        // No pods: an empty anchor.
        p.apply(&sys, &[], &BTreeMap::new(), gateway, services)
            .unwrap();
        assert_eq!(sys.anchor(ANCHOR).as_deref(), Some(""));
    }
}
