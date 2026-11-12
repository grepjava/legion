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
//!
//! Every rule but the NAT to the outside has an IPv6 twin, with the twins
//! of the addresses ([`crate::v6`]); a service's table holds its tasks'
//! addresses of both families, and pf takes those of the packet's.
//! Pods reach the outside over IPv4 only.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr};

use legion_sys::{Ipv4Net, Sys};

use crate::{BRIDGE, CLUSTER_POD_CIDR, CLUSTER_POD_CIDR6, PodNetwork, mesh, v6};

pub const ANCHOR: &str = "legion/svc";
/// Marks translated service traffic for the hairpin NAT.
pub(crate) const TAG: &str = "LSVC";

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
    pub fn as_str(self) -> &'static str {
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
    /// Its address on the nodes' network, if it has one: its ports answer
    /// there, on the node holding it.
    pub address: Option<Ipv4Addr>,
    /// That address as this node's CARP virtual host, if this node is one
    /// that may hold it.
    pub carp: Option<legion_sys::Carp>,
}

impl Service {
    fn table(&self) -> String {
        // pf table names are at most 31 characters; uids are 16.
        format!("s{}", self.uid)
    }

    fn balanced(&self) -> bool {
        !self.ports.is_empty()
            && (self.vip.is_some()
                || self.address.is_some()
                || self.ports.iter().any(|p| p.published.is_some()))
    }
}

/// Addresses and their IPv6 twins, sorted.
pub(crate) fn dual(addrs: &[Ipv4Addr]) -> Vec<IpAddr> {
    let mut all: Vec<IpAddr> = addrs
        .iter()
        .flat_map(|&ip| [Some(IpAddr::V4(ip)), v6(ip).map(IpAddr::V6)])
        .flatten()
        .collect();
    all.sort();
    all.dedup();
    all
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
    /// Where load balancer addresses are held; the egress interface if
    /// unset.
    pub balancer: Option<&'a str>,
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
    let net = PodNetwork::new(subnet);
    let local = format!("{}/{}", subnet.network(), subnet.prefix);
    let cluster = format!("{}/{}", CLUSTER_POD_CIDR.network(), CLUSTER_POD_CIDR.prefix);
    let (local6, cluster6) = (net.subnet6(), CLUSTER_POD_CIDR6);
    let mut r = String::new();
    let _ = writeln!(
        r,
        "nat pass on {BRIDGE} inet from {local} to {local} tagged {TAG} -> ({BRIDGE})"
    );
    // Not `({BRIDGE})`: for IPv6 that holds the link-local address too.
    let _ = writeln!(
        r,
        "nat pass on {BRIDGE} inet6 from {local6} to {local6} tagged {TAG} -> {}",
        net.gateway6()
    );
    if let Some(ext) = egress {
        let _ = writeln!(
            r,
            "nat pass on {ext} inet from {local} to ! {cluster} -> ({ext})"
        );
    }
    if let Some(wg) = exits.mesh {
        let _ = writeln!(
            r,
            "nat pass on {wg} inet from ! {cluster} to {cluster} -> ({wg})"
        );
        if let Some(own) = mesh::underlay_addr6(subnet) {
            let _ = writeln!(
                r,
                "nat pass on {wg} inet6 from ! {cluster6} to {cluster6} -> {own}"
            );
        }
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
                if let Some(vip6) = v6(vip) {
                    let _ = writeln!(
                        r,
                        "rdr pass on {BRIDGE} inet6 proto {proto} to {vip6} port {} tag {TAG} -> {to}",
                        p.port
                    );
                }
            }
            if let (Some(addr), Some(ext)) = (s.address, exits.balancer.or(egress)) {
                let _ = writeln!(
                    r,
                    "rdr pass on {ext} inet proto {proto} to {addr} port {} tag {TAG} -> {to}",
                    p.port
                );
            }
            if let Some(published) = p.published {
                for i in exits.published {
                    for family in ["inet", "inet6"] {
                        let _ = writeln!(
                            r,
                            "rdr pass on {i} {family} proto {proto} to ({i}) port {published} tag {TAG} -> {to}"
                        );
                    }
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
    tables: BTreeMap<String, Vec<IpAddr>>,
    /// Tasks of balanced services last time, ready or draining, with their
    /// twins: when one goes away, so do its connections.
    backends: BTreeSet<IpAddr>,
    /// Load balancer addresses this node holds as CARP virtual hosts, and
    /// on which interface.
    carps: BTreeMap<Ipv4Addr, (String, legion_sys::Carp)>,
}

impl LoadBalancer {
    pub fn apply(
        &mut self,
        sys: &dyn Sys,
        services: &[Service],
        subnet: Ipv4Net,
        exits: Exits,
    ) -> legion_sys::Result<()> {
        let mut tables = BTreeMap::new();
        for s in services.iter().filter(|s| s.balanced()) {
            tables.insert(s.table(), dual(&s.ready));
        }
        let rules = rules(services, subnet, exits);
        if self.loaded.as_ref() != Some(&rules) {
            // The rules with every table and its addresses, in one load:
            // pf takes it whole, so no rule sends connections to a table not
            // filled yet. Setting each table on its own cost a pfctl run per
            // service, 4 s at a thousand (docs/MEASURE.md).
            self.loaded = None;
            self.tables.clear();
            let mut text = String::new();
            for (table, ready) in &tables {
                // An empty list is no pf syntax: such a table is emptied
                // after the load.
                if ready.is_empty() {
                    let _ = writeln!(text, "table <{table}> persist");
                } else {
                    let addrs: Vec<String> = ready.iter().map(IpAddr::to_string).collect();
                    let _ = writeln!(text, "table <{table}> persist {{ {} }}", addrs.join(" "));
                }
            }
            text.push_str(&rules);
            sys.pf_load_anchor(ANCHOR, &text)?;
            self.loaded = Some(rules);
            for (table, ready) in &tables {
                if ready.is_empty() {
                    sys.pf_table_replace(ANCHOR, table, ready)?;
                }
                self.tables.insert(table.clone(), ready.clone());
            }
        } else {
            for (table, ready) in &tables {
                if self.tables.get(table) != Some(ready) {
                    sys.pf_table_replace(ANCHOR, table, ready)?;
                    self.tables.insert(table.clone(), ready.clone());
                }
            }
        }

        // Load balancer addresses: the ones this node may hold as CARP
        // virtual hosts on its egress interface, and the others given up.
        let wanted: BTreeMap<Ipv4Addr, (String, legion_sys::Carp)> =
            match exits.balancer.or(exits.egress) {
                Some(ext) => services
                    .iter()
                    .filter_map(|s| s.carp.clone())
                    .map(|c| (c.addr, (ext.to_owned(), c)))
                    .collect(),
                None => BTreeMap::new(),
            };
        for (addr, (iface, _)) in &self.carps.clone() {
            if wanted.get(addr).is_none_or(|(i, _)| i != iface) {
                sys.carp_remove(iface, *addr)?;
                self.carps.remove(addr);
            }
        }
        for (addr, (iface, carp)) in wanted {
            if self.carps.get(&addr) != Some(&(iface.clone(), carp.clone())) {
                sys.carp_set(&iface, &carp, None)?;
                self.carps.insert(addr, (iface, carp));
            }
        }

        // Backends that no longer run anywhere. A task of any service,
        // balanced or not, keeps its connections: one that stopped being
        // balanced still runs, and an address freed is soon another
        // task's. Those stay tracked, to go once they stop running.
        let live: BTreeSet<IpAddr> = services
            .iter()
            .flat_map(|s| dual(&s.alive).into_iter().chain(dual(&s.ready)))
            .collect();
        let backends: BTreeSet<IpAddr> = services
            .iter()
            .filter(|s| s.balanced())
            .flat_map(|s| dual(&s.alive).into_iter().chain(dual(&s.ready)))
            .chain(self.backends.intersection(&live).copied())
            .collect();
        let gone: BTreeSet<IpAddr> = self.backends.difference(&backends).copied().collect();
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

    /// Forgets what the anchor holds, after a pass failed, so the next
    /// sets it all again; the backends stay, so their connections are
    /// still killed once they go.
    pub fn forget(&mut self) {
        self.loaded = None;
        self.tables.clear();
    }
}

pub use legion_sys::check::missing_hooks_of;

/// The hooks the host's main ruleset lacks.
pub fn missing_hooks(main_rules: &str) -> Vec<&'static str> {
    missing_hooks_of(main_rules, "legion/*", HOOKS)
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
            address: None,
            carp: None,
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
            balancer: None,
        };
        assert_eq!(
            rules(&[web(&[], &[]), dnsrr], subnet, exits),
            "nat pass on legion0 inet from 10.200.0.0/24 to 10.200.0.0/24 tagged LSVC -> (legion0)\n\
             nat pass on legion0 inet6 from fd00:10:200::/64 to fd00:10:200::/64 tagged LSVC -> fd00:10:200::1\n\
             nat pass on wlan0 inet from 10.200.0.0/24 to ! 10.200.0.0/16 -> (wlan0)\n\
             nat pass on wg0 inet from ! 10.200.0.0/16 to 10.200.0.0/16 -> (wg0)\n\
             nat pass on wg0 inet6 from ! fd00:10:200::/48 to fd00:10:200::/48 -> fd00:100:64::1\n\
             rdr pass on legion0 inet proto tcp to 10.201.0.1 port 80 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on legion0 inet6 proto tcp to fd00:10:201::1 port 80 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on re0 inet proto tcp to (re0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on re0 inet6 proto tcp to (re0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on wlan0 inet proto tcp to (wlan0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on wlan0 inet6 proto tcp to (wlan0) port 30000 tag LSVC -> <s0000000000000007> port 8080 round-robin\n\
             rdr pass on legion0 inet proto udp to 10.201.0.1 port 53 tag LSVC -> <s0000000000000007> port 5353 round-robin\n\
             rdr pass on legion0 inet6 proto udp to fd00:10:201::1 port 53 tag LSVC -> <s0000000000000007> port 5353 round-robin\n"
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
    fn tables_come_in_one_load_with_the_rules() {
        let sys = FakeSys::new();
        let subnet = "10.200.0.0/24".parse().unwrap();
        let mut lb = LoadBalancer::default();
        // A load that fails leaves neither, and the next pass loads both.
        sys.break_anchor(ANCHOR, true);
        let r = lb.apply(&sys, &[web(&["10.200.0.2"], &[])], subnet, Exits::default());
        assert!(r.is_err());
        assert!(sys.anchor(ANCHOR).is_none());
        assert!(
            sys.pf_table_show(ANCHOR, "s0000000000000007")
                .unwrap()
                .is_empty()
        );
        sys.break_anchor(ANCHOR, false);
        lb.apply(&sys, &[web(&["10.200.0.2"], &[])], subnet, Exits::default())
            .unwrap();
        let loaded = sys.anchor(ANCHOR).unwrap();
        let table = loaded
            .find("table <s0000000000000007> persist { 10.200.0.2 ")
            .expect(&loaded);
        assert!(table < loaded.find("rdr ").unwrap(), "{loaded}");
        assert_eq!(
            sys.pf_table_show(ANCHOR, "s0000000000000007")
                .unwrap()
                .len(),
            2
        );
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
            addrs: dual(&[ip("10.200.0.2"), ip("10.201.0.1")]),
        });
        // .2 goes, but its connections cannot be listed, then not killed.
        for command in ["pfctl -ss", "pfctl -k"] {
            sys.fail_next(command);
            let r = lb.apply(&sys, &one, subnet, Exits::default());
            assert!(r.is_err(), "{command} did not fail");
            assert_eq!(sys.states().len(), 1, "after {command} failed");
            // As the agent does after a failed pass.
            lb.forget();
        }
        // The next pass still knows .2 went.
        lb.apply(&sys, &one, subnet, Exits::default()).unwrap();
        assert!(sys.states().is_empty());
    }

    #[test]
    fn connections_of_tasks_still_running_unbalanced_are_kept() {
        let sys = FakeSys::new();
        let subnet = "10.200.0.0/24".parse().unwrap();
        let mut lb = LoadBalancer::default();
        let state = |id: &str, addr: &str| PfState {
            id: id.into(),
            addrs: dual(&[ip(addr), ip("10.200.0.9")]),
        };
        let unbalanced = |uid: &str, alive: &[&str]| Service {
            uid: uid.into(),
            vip: None,
            ports: Vec::new(),
            ready: alive.iter().map(|a| ip(a)).collect(),
            alive: alive.iter().map(|a| ip(a)).collect(),
            address: None,
            carp: None,
        };
        lb.apply(
            &sys,
            &[web(&["10.200.0.2", "10.200.0.3"], &[])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        sys.add_state(state("a", "10.200.0.2"));
        sys.add_state(state("b", "10.200.0.3"));

        // The service turns headless; its tasks run on. And .3's address,
        // freed, goes to a job's task.
        let now = [
            unbalanced("0000000000000007", &["10.200.0.2"]),
            unbalanced("0000000000000008", &["10.200.0.3"]),
        ];
        lb.apply(&sys, &now, subnet, Exits::default()).unwrap();
        assert_eq!(sys.states().len(), 2);

        // Once they stop running, their connections go.
        lb.apply(
            &sys,
            &[unbalanced("0000000000000008", &["10.200.0.3"])],
            subnet,
            Exits::default(),
        )
        .unwrap();
        assert_eq!(
            sys.states()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["b"]
        );
        lb.apply(&sys, &[], subnet, Exits::default()).unwrap();
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
            addrs: vec![
                ip(backend).into(),
                ip("10.201.0.1").into(),
                ip("10.200.0.9").into(),
            ],
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
            dual(&[ip("10.200.0.2"), ip("10.200.0.3")])
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
            dual(&[ip("10.200.0.2")])
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
            dual(&[ip("10.200.0.3")])
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

    /// A load balancer address: its ports answer on it on the egress
    /// interface, and a node that may hold it puts it there as a CARP
    /// virtual host, taking it off once it may no more.
    #[test]
    fn a_load_balancer_address_is_held_with_carp() {
        let sys = FakeSys::new();
        let subnet = "10.200.0.0/24".parse().unwrap();
        let exits = Exits {
            egress: Some("vtnet0"),
            ..Exits::default()
        };
        let carp = legion_sys::Carp {
            vhid: 7,
            addr: ip("192.168.1.50"),
            password: "secret".into(),
            advskew: 20,
        };
        let with = |carp: Option<legion_sys::Carp>| Service {
            vip: None,
            address: Some(ip("192.168.1.50")),
            carp,
            ..web(&["10.200.0.2"], &[])
        };
        let mut lb = LoadBalancer::default();
        lb.apply(&sys, &[with(Some(carp.clone()))], subnet, exits)
            .unwrap();
        let rules = sys.anchor(ANCHOR).unwrap();
        assert!(
            rules.contains("rdr pass on vtnet0 inet proto tcp to 192.168.1.50 port 80"),
            "{rules}"
        );
        let (held, _) = sys.carp("vtnet0", 7).expect("held");
        assert_eq!(held, carp);
        // Again: nothing to do.
        lb.apply(&sys, &[with(Some(carp.clone()))], subnet, exits)
            .unwrap();
        // No more among those that may hold it: given up, the rules kept.
        lb.apply(&sys, &[with(None)], subnet, exits).unwrap();
        assert!(sys.carp("vtnet0", 7).is_none());
        assert!(
            sys.anchor(ANCHOR)
                .unwrap()
                .contains("to 192.168.1.50 port 80")
        );
    }
}
