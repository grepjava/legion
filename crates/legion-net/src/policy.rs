//! Network policy with pf (DESIGN.md §5.6), in the anchor `legion/policy`:
//! what may reach each pod on this node, and which VIPs it may use.
//!
//! Rules sit on the pod's host end (`lp…`), which pf sees bridged traffic
//! on as well as routed: `out on` it is traffic to the pod, `in on` it is
//! traffic from the pod. A pod may send only from its own address, so it
//! cannot pass for a node or for another namespace's task, and whatever it
//! starts is kept as state, so replies come back through its own refusals.
//!
//! What reaches a pod is checked here, on its node, by source: the nodes
//! (probes, other hosts, published ports reached through another node),
//! traffic a published port's or a VIP's rule translated here, and the
//! tables of tasks the agent allows. Traffic through a VIP to a task on
//! this same node is NATed to the gateway by the hairpin rule (lb.rs), so
//! its source is lost by then: VIPs are checked at the client pod instead,
//! while the destination is still the VIP.
//!
//! States are bound to the interface (`if-bound`): a floating one made
//! here would match the same packet again on the bridge, before the VIP's
//! translation, and break it.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::Ipv4Addr;

use legion_sys::{Ipv4Net, Sys};

use crate::lb::{Proto, TAG};
use crate::mesh::UNDERLAY;

pub const ANCHOR: &str = "legion/policy";

/// Sources that may reach a pod: a table, on some ports (all if empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allow {
    pub table: String,
    pub ports: Vec<(Proto, u16)>,
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
}

/// The anchor's rules. `gateway` is this node's address on the bridge;
/// `services` is the VIP range.
pub fn rules(guards: &[Guard], gateway: Ipv4Addr, services: Ipv4Net) -> String {
    let nodes = format!(
        "{{ {gateway}, {}/{} }}",
        UNDERLAY.network(),
        UNDERLAY.prefix
    );
    let vips = format!("{}/{}", services.network(), services.prefix);
    let state = "keep state (if-bound)";
    let mut r = String::new();
    for g in guards {
        let (lp, ip) = (&g.interface, g.ip);
        // Pods speak IPv4 only; link-local IPv6 would go around all this.
        let _ = writeln!(r, "block drop quick on {lp} inet6 all");
        // From the pod.
        let _ = writeln!(r, "block drop in quick on {lp} inet from ! {ip}");
        for (vip, proto, port) in &g.vips {
            let _ = writeln!(
                r,
                "pass in quick on {lp} inet proto {} from {ip} to {vip} port {port} {state}",
                proto.as_str()
            );
        }
        let _ = writeln!(r, "block drop in quick on {lp} inet to {vips}");
        let _ = writeln!(r, "pass in quick on {lp} inet from {ip} {state}");
        // To the pod.
        if g.open {
            let _ = writeln!(r, "pass out quick on {lp} inet {state}");
            continue;
        }
        let _ = writeln!(r, "pass out quick on {lp} inet tagged {TAG} {state}");
        let _ = writeln!(r, "pass out quick on {lp} inet from {nodes} {state}");
        for a in &g.allowed {
            let from = format!("<{}>", a.table);
            if a.ports.is_empty() {
                let _ = writeln!(r, "pass out quick on {lp} inet from {from} {state}");
            }
            for (proto, port) in &a.ports {
                let _ = writeln!(
                    r,
                    "pass out quick on {lp} inet proto {} from {from} to {ip} port {port} {state}",
                    proto.as_str()
                );
            }
        }
        let _ = writeln!(r, "block drop out quick on {lp} inet all");
    }
    r
}

/// Keeps the anchor in line, doing only what changed.
#[derive(Debug, Default)]
pub struct Policy {
    loaded: Option<String>,
    tables: BTreeMap<String, Vec<Ipv4Addr>>,
}

impl Policy {
    /// Loads `guards`' rules and sets `tables`, the source tables they name.
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
            let mut addrs = addrs.clone();
            addrs.sort();
            addrs.dedup();
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
                let mut addrs = addrs.clone();
                addrs.sort();
                addrs.dedup();
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
            },
            Guard {
                interface: "lp000000000008".into(),
                ip: ip("10.200.0.8"),
                open: true,
                allowed: vec![],
                vips: vec![],
            },
        ]
    }

    #[test]
    fn renders_rules() {
        let services = Ipv4Net::new(ip("10.201.0.0"), 16);
        assert_eq!(
            rules(&guards(), ip("10.200.0.1"), services),
            "block drop quick on lp000000000007 inet6 all\n\
             block drop in quick on lp000000000007 inet from ! 10.200.0.7\n\
             pass in quick on lp000000000007 inet proto tcp from 10.200.0.7 to 10.201.0.3 port 80 keep state (if-bound)\n\
             block drop in quick on lp000000000007 inet to 10.201.0.0/16\n\
             pass in quick on lp000000000007 inet from 10.200.0.7 keep state (if-bound)\n\
             pass out quick on lp000000000007 inet tagged LSVC keep state (if-bound)\n\
             pass out quick on lp000000000007 inet from { 10.200.0.1, 100.64.0.0/16 } keep state (if-bound)\n\
             pass out quick on lp000000000007 inet from <n0123> keep state (if-bound)\n\
             pass out quick on lp000000000007 inet proto tcp from <p4567> to 10.200.0.7 port 5432 keep state (if-bound)\n\
             block drop out quick on lp000000000007 inet all\n\
             block drop quick on lp000000000008 inet6 all\n\
             block drop in quick on lp000000000008 inet from ! 10.200.0.8\n\
             block drop in quick on lp000000000008 inet to 10.201.0.0/16\n\
             pass in quick on lp000000000008 inet from 10.200.0.8 keep state (if-bound)\n\
             pass out quick on lp000000000008 inet keep state (if-bound)\n"
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
            [ip("10.200.0.7"), ip("10.200.1.5")]
        );
        // A table changes; the rules stay.
        tables.insert("p4567".into(), vec![ip("10.200.2.9")]);
        p.apply(&sys, &guards(), &tables, gateway, services)
            .unwrap();
        assert_eq!(
            sys.pf_table_show(ANCHOR, "p4567").unwrap(),
            [ip("10.200.2.9")]
        );
        // No pods: an empty anchor.
        p.apply(&sys, &[], &BTreeMap::new(), gateway, services)
            .unwrap();
        assert_eq!(sys.anchor(ANCHOR).as_deref(), Some(""));
    }
}
