//! Service load balancing with pf (DESIGN.md §5.4), in the anchor
//! `legion/svc`. The host's pf.conf must call Legion's anchors:
//!
//! ```text
//! nat-anchor "legion/*"
//! rdr-anchor "legion/*"
//! anchor "legion/*"
//! ```
//!
//! Each service with a VIP gets a table of its ready tasks and an `rdr`
//! per port, round-robin over the table. Traffic between pods on this node
//! that went through a VIP is NATed to the gateway (the hairpin rule), or
//! the backend would answer the client directly and the client would drop
//! the reply. Pods reach the outside through NAT on the egress interface,
//! where published ports also lead into service tables. Traffic
//! from outside the pod network that leaves for another node's pod (a
//! published port's, to a remote task) is NATed to this node's mesh
//! address, so the reply comes back through here to be translated.
//!
//! Rules change only with services and ports; readiness changes only touch
//! tables, so connections to a task that stops being ready finish (soft
//! drain). Connections to a task that is gone are killed, so clients fail
//! fast instead of waiting on a dead backend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::net::Ipv4Addr;

use legion_sys::{Ipv4Net, Sys};

use crate::{BRIDGE, CLUSTER_POD_CIDR};

pub const ANCHOR: &str = "legion/svc";
/// Marks translated service traffic for the hairpin NAT.
const TAG: &str = "LSVC";

/// The lines a host's pf.conf needs for Legion's anchors.
pub const HOOKS: [&str; 3] = [
    "nat-anchor \"legion/*\"",
    "rdr-anchor \"legion/*\"",
    "anchor \"legion/*\"",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    fn as_str(self) -> &'static str {
        match self {
            Proto::Tcp => "tcp",
            Proto::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Port {
    pub proto: Proto,
    /// On the VIP.
    pub port: u16,
    /// On the tasks.
    pub target: u16,
    /// On every address of the node, if published.
    pub published: Option<u16>,
}

/// A service as load balancing sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    /// Stable and unique: names the service's table.
    pub uid: String,
    pub vip: Option<Ipv4Addr>,
    pub ports: Vec<Port>,
    /// Tasks that take new connections.
    pub ready: Vec<Ipv4Addr>,
    /// Tasks that run, ready or not: their connections are left alone.
    pub alive: Vec<Ipv4Addr>,
}

impl Service {
    fn table(&self) -> String {
        // pf table names are at most 31 characters; uids are 16.
        format!("s{}", self.uid)
    }

    fn balanced(&self) -> bool {
        !self.ports.is_empty()
            && (self.vip.is_some() || self.ports.iter().any(|p| p.published.is_some()))
    }
}

/// Where this node's traffic leaves it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Exits<'a> {
    /// The interface of the default route.
    pub egress: Option<&'a str>,
    /// The mesh interface to other nodes.
    pub mesh: Option<&'a str>,
    /// Where published ports are reached: the node's own interfaces.
    pub published: &'a [String],
}

/// Whether published ports should be reached through `interface`: every
/// interface of the node but loopback, pf's, the pod bridge, pods' links
/// and the mesh, as Swarm publishes on every address.
pub fn publishes_on(interface: &str) -> bool {
    !(interface.starts_with("lo")
        || interface.starts_with("pflog")
        || interface.starts_with("pfsync")
        || interface == BRIDGE
        || interface.starts_with("lp")
        || interface == crate::mesh::INTERFACE)
}

/// The anchor's rules for this node.
pub fn rules(services: &[Service], subnet: Ipv4Net, exits: Exits) -> String {
    let egress = exits.egress;
    let local = format!("{}/{}", subnet.network(), subnet.prefix);
    let cluster = format!("{}/{}", CLUSTER_POD_CIDR.network(), CLUSTER_POD_CIDR.prefix);
    let mut r = String::new();
    let _ = writeln!(
        r,
        "nat pass on {BRIDGE} inet from {local} to {local} tagged {TAG} -> ({BRIDGE})"
    );
    if let Some(ext) = egress {
        let _ = writeln!(
            r,
            "nat pass on {ext} inet from {local} to ! {cluster} -> ({ext})"
        );
    }
    if let Some(mesh) = exits.mesh {
        let _ = writeln!(
            r,
            "nat pass on {mesh} inet from ! {cluster} to {cluster} -> ({mesh})"
        );
    }
    for s in services.iter().filter(|s| s.balanced()) {
        let table = s.table();
        for p in &s.ports {
            let proto = p.proto.as_str();
            let to = format!("<{table}> port {} round-robin", p.target);
            if let Some(vip) = s.vip {
                let _ = writeln!(
                    r,
                    "rdr pass on {BRIDGE} inet proto {proto} to {vip} port {} tag {TAG} -> {to}",
                    p.port
                );
            }
            if let Some(published) = p.published {
                for i in exits.published {
                    let _ = writeln!(
                        r,
                        "rdr pass on {i} inet proto {proto} to ({i}) port {published} tag {TAG} -> {to}"
                    );
                }
            }
        }
    }
    r
}

/// Keeps the anchor in line with the services, doing only what changed.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    loaded: Option<String>,
    tables: BTreeMap<String, Vec<Ipv4Addr>>,
    /// Tasks of balanced services last time, ready or draining: when one
    /// goes away, so do its connections.
    backends: BTreeSet<Ipv4Addr>,
}

impl LoadBalancer {
    pub fn apply(
        &mut self,
        sys: &dyn Sys,
        services: &[Service],
        subnet: Ipv4Net,
        exits: Exits,
    ) -> legion_sys::Result<()> {
        let rules = rules(services, subnet, exits);
        if self.loaded.as_ref() != Some(&rules) {
            sys.pf_load_anchor(ANCHOR, &rules)?;
            self.loaded = Some(rules);
            // Contents are set below for every table the rules use.
            self.tables.clear();
        }

        let mut tables = BTreeMap::new();
        for s in services.iter().filter(|s| s.balanced()) {
            let mut ready = s.ready.clone();
            ready.sort();
            ready.dedup();
            let table = s.table();
            if self.tables.get(&table) != Some(&ready) {
                sys.pf_table_replace(ANCHOR, &table, &ready)?;
            }
            tables.insert(table, ready);
        }

        // Backends that no longer run anywhere.
        let backends: BTreeSet<Ipv4Addr> = services
            .iter()
            .filter(|s| s.balanced())
            .flat_map(|s| s.alive.iter().chain(&s.ready))
            .copied()
            .collect();
        let gone: BTreeSet<Ipv4Addr> = self.backends.difference(&backends).copied().collect();
        self.tables = tables;
        if !gone.is_empty() {
            for state in sys.pf_states()? {
                if state.addrs.iter().any(|a| gone.contains(a)) {
                    sys.pf_kill_state(&state.id)?;
                }
            }
        }
        // Only now: if killing failed, the next pass finds them gone again.
        self.backends = backends;
        Ok(())
    }
}

/// The hooks the host's main ruleset lacks.
pub fn missing_hooks(main_rules: &str) -> Vec<&'static str> {
    let called = |kind: &str| {
        main_rules.lines().any(|l| {
            let l = l.trim();
            l.starts_with(kind) && l.contains("\"legion/*\"")
        })
    };
    let mut missing = Vec::new();
    if !called("nat-anchor") {
        missing.push(HOOKS[0]);
    }
    if !called("rdr-anchor") {
        missing.push(HOOKS[1]);
    }
    if !called("anchor") {
        missing.push(HOOKS[2]);
    }
    missing
}

#[cfg(test)]
mod tests {
    use legion_sys::{FakeSys, PfState};

    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn web(ready: &[&str], alive: &[&str]) -> Service {
        Service {
            uid: "0000000000000007".into(),
            vip: Some(ip("10.201.0.1")),
            ports: vec![
                Port {
                    proto: Proto::Tcp,
                    port: 80,
                    target: 8080,
                    published: Some(30000),
                },
                Port {
                    proto: Proto::Udp,
                    port: 53,
                    target: 5353,
                    published: None,
                },
            ],
            ready: ready.iter().map(|s| ip(s)).collect(),
            alive: alive.iter().map(|s| ip(s)).collect(),
        }
    }

    #[test]
    fn renders_rules() {
        let subnet = "10.200.0.0/24".parse().unwrap();
        let mut dnsrr = web(&[], &[]);
        dnsrr.uid = "0000000000000008".into();
        dnsrr.vip = None;
        dnsrr.ports.iter_mut().for_each(|p| p.published = None);
        let published = ["re0".to_owned(), "wlan0".to_owned()];
        let exits = Exits {
            egress: Some("wlan0"),
            mesh: Some("wg0"),
            published: &published,
        };
        assert_eq!(
            rules(&[web(&[], &[]), dnsrr], subnet, exits),
            "nat pass on legion0 inet from 10.200.0.0/24 to 10.200.0.0/24 tagged LSVC -> (legion0)\n\
             nat pass on wlan0 inet from 10.200.0.0/24 to ! 10.200.0.0/16 -> (wlan0)\n\
             nat pass on wg0 inet from ! 10.200.0.0/16 to 10.200.0.0/16 -> (wg0)\n\
             rdr pass on legion0 inet proto tcp to 10.201.0.1 port 80 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on re0 inet proto tcp to (re0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on wlan0 inet proto tcp to (wlan0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on legion0 inet proto udp to 10.201.0.1 port 53 tag LSVC -> <s0000000000000007> port 5353 round-robin\n"
        );
        // No route out and no mesh: no outbound NAT, nothing published.
        let r = rules(&[web(&[], &[])], subnet, Exits::default());
        assert!(
            !r.contains("wlan0") && !r.contains("wg0") && r.contains("10.201.0.1 port 80"),
            "{r}"
        );
    }

    #[test]
    fn published_on_the_nodes_own_interfaces() {
        let all = [
            "re0",
            "wlan0",
            "lo0",
            "pflog0",
            "legion0",
            "lp0000000004d2",
            "wg0",
            "lvbr0",
        ];
        let on: Vec<&str> = all.into_iter().filter(|i| publishes_on(i)).collect();
        assert_eq!(on, ["re0", "wlan0", "lvbr0"]);
    }

    #[test]
    fn hooks() {
        let host = "nat on wlan0 inet from <cni-nat> to any -> (wlan0) round-robin\n\
                    nat-anchor \"cni-rdr/*\" all\nrdr-anchor \"cni-rdr/*\" all\n";
        assert_eq!(missing_hooks(host), HOOKS);
        let hooked = format!(
            "{host}nat-anchor \"legion/*\" all\nrdr-anchor \"legion/*\" all\nanchor \"legion/*\" all\n"
        );
        assert!(missing_hooks(&hooked).is_empty());
    }

    #[test]
    fn failed_cleanup_is_retried() {
        let sys = FakeSys::new();
        let subnet = "10.200.0.0/24".parse().unwrap();
        let mut lb = LoadBalancer::default();
        let both = [web(&["10.200.0.2", "10.200.0.3"], &[])];
        let one = [web(&["10.200.0.3"], &[])];
        lb.apply(&sys, &both, subnet, Exits::default()).unwrap();
        sys.add_state(PfState {
            id: "a".into(),
            addrs: vec![ip("10.200.0.2"), ip("10.201.0.1")],
        });
        // .2 goes, but its connections cannot be listed, then not killed.
        for command in ["pfctl -ss", "pfctl -k"] {
            sys.fail_next(command);
            let r = lb.apply(&sys, &one, subnet, Exits::default());
            assert!(r.is_err(), "{command} did not fail");
            assert_eq!(sys.states().len(), 1, "after {command} failed");
        }
        // The next pass still knows .2 went.
        lb.apply(&sys, &one, subnet, Exits::default()).unwrap();
        assert!(sys.states().is_empty());
    }

    #[test]
    fn applies_only_changes() {
        let sys = FakeSys::new();
        let subnet = "10.200.0.0/24".parse().unwrap();
        let mut lb = LoadBalancer::default();
        let table = "s0000000000000007";
        let state = |id: &str, backend: &str| PfState {
            id: id.into(),
            addrs: vec![ip(backend), ip("10.201.0.1"), ip("10.200.0.9")],
        };

        lb.apply(
            &sys,
            &[web(&["10.200.0.3", "10.200.0.2"], &[])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert!(sys.anchor(ANCHOR).unwrap().contains("10.201.0.1 port 80"));
        assert_eq!(
            sys.pf_table_show(ANCHOR, table).unwrap(),
            [ip("10.200.0.2"), ip("10.200.0.3")]
        );

        // .3 stops being ready but runs: out of the table, its
        // connections kept.
        sys.add_state(state("a", "10.200.0.3"));
        sys.add_state(state("b", "10.200.0.2"));
        lb.apply(
            &sys,
            &[web(&["10.200.0.2"], &["10.200.0.3"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert_eq!(
            sys.pf_table_show(ANCHOR, table).unwrap(),
            [ip("10.200.0.2")]
        );
        assert_eq!(sys.states().len(), 2);

        // .2 is gone: its connections are killed; .3 is ready again.
        lb.apply(
            &sys,
            &[web(&["10.200.0.3"], &["10.200.0.3"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert_eq!(
            sys.pf_table_show(ANCHOR, table).unwrap(),
            [ip("10.200.0.3")]
        );
        assert_eq!(
            sys.states()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );

        // Draining, then gone: killed as well.
        sys.add_state(state("c", "10.200.0.4"));
        lb.apply(
            &sys,
            &[web(&["10.200.0.3"], &["10.200.0.3", "10.200.0.4"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert_eq!(sys.states().len(), 2);
        lb.apply(
            &sys,
            &[web(&["10.200.0.3"], &["10.200.0.3"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert_eq!(
            sys.states()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );

        // Nothing changed: no reload (a reload would refill the table).
        sys.pf_table_replace(ANCHOR, table, &[]).unwrap();
        lb.apply(
            &sys,
            &[web(&["10.200.0.3"], &["10.200.0.3"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert!(sys.pf_table_show(ANCHOR, table).unwrap().is_empty());
    }
}
