//! Network policy on this node (DESIGN.md §5.6): which tasks may reach the
//! pods here, and which VIPs they may use, from the namespaces' isolation
//! and the policies. legion-net turns this into pf rules.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use legion_api::dispatch::{Endpoints, Policies, ServiceEndpoints};
use legion_api::policy::{Isolation, PolicyPeer, selects};
use legion_api::service::Protocol;
use legion_net::lb::Proto;
use legion_net::policy::{Allow, Guard};

/// A pod running on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPod {
    pub interface: String,
    pub ip: Ipv4Addr,
    pub namespace: String,
    pub service: String,
}

/// Each local pod's guard, and the source tables they name.
pub fn guards(
    pods: &[LocalPod],
    endpoints: &Endpoints,
    policies: &Policies,
) -> (Vec<Guard>, BTreeMap<String, Vec<Ipv4Addr>>) {
    let no_labels = BTreeMap::new();
    let labels = |namespace: &str, service: &str| {
        endpoints
            .iter()
            .find(|e| e.namespace == namespace && e.name == service)
            .map_or(&no_labels, |e| &e.labels)
    };
    let mut tables: BTreeMap<String, Vec<Ipv4Addr>> = BTreeMap::new();
    let mut guards = Vec::new();
    for pod in pods {
        let ns = pod.namespace.as_str();
        let mine = labels(ns, &pod.service);
        let isolation = policies.isolation(ns);
        let mut allowed = Vec::new();
        if isolation == Isolation::Namespace {
            let peer = PolicyPeer {
                namespace: Some(ns.to_owned()),
                services: BTreeMap::new(),
            };
            allowed.push(Allow {
                table: table(&mut tables, endpoints, ns, &[peer]),
                ports: vec![],
            });
        }
        for p in policies
            .policies
            .iter()
            .filter(|p| p.namespace == ns && selects(&p.spec.services, mine))
        {
            for rule in &p.spec.ingress {
                allowed.push(Allow {
                    table: table(&mut tables, endpoints, ns, &rule.from),
                    ports: rule
                        .ports
                        .iter()
                        .map(|port| (proto(port.protocol), port.port))
                        .collect(),
                });
            }
        }
        let vips = endpoints
            .iter()
            .filter_map(|e| Some((e, e.vip?)))
            .flat_map(|(e, vip)| {
                e.ports
                    .iter()
                    .filter(|port| may_reach(policies, pod, mine, e, port.protocol, port.target))
                    .map(move |port| (vip, proto(port.protocol), port.port))
            })
            .collect();
        guards.push(Guard {
            interface: pod.interface.clone(),
            ip: pod.ip,
            open: isolation == Isolation::Open,
            allowed,
            vips,
        });
    }
    (guards, tables)
}

/// Whether `from` may reach `to`'s tasks on `port`: the same decision the
/// destination's guard makes, for checking VIPs at the client.
fn may_reach(
    policies: &Policies,
    from: &LocalPod,
    from_labels: &BTreeMap<String, String>,
    to: &ServiceEndpoints,
    protocol: Protocol,
    port: u16,
) -> bool {
    let ns = to.namespace.as_str();
    match policies.isolation(ns) {
        Isolation::Open => return true,
        Isolation::Namespace if from.namespace == ns => return true,
        _ => {}
    }
    policies
        .policies
        .iter()
        .filter(|p| p.namespace == ns && selects(&p.spec.services, &to.labels))
        .flat_map(|p| &p.spec.ingress)
        .any(|rule| {
            let peer_ok = rule.from.is_empty()
                || rule.from.iter().any(|peer| {
                    peer.namespace.as_deref().unwrap_or(ns) == from.namespace
                        && selects(&peer.services, from_labels)
                });
            let port_ok = rule.ports.is_empty()
                || rule
                    .ports
                    .iter()
                    .any(|p| p.port == port && p.protocol == protocol);
            peer_ok && port_ok
        })
}

/// The table of the running tasks `peers` name (all of the cluster's when
/// empty), relative to namespace `ns`, added to `tables`; its name.
fn table(
    tables: &mut BTreeMap<String, Vec<Ipv4Addr>>,
    endpoints: &Endpoints,
    ns: &str,
    peers: &[PolicyPeer],
) -> String {
    let resolved: BTreeSet<(String, Vec<(String, String)>)> = peers
        .iter()
        .map(|p| {
            let namespace = p.namespace.clone().unwrap_or_else(|| ns.to_owned());
            (namespace, p.services.clone().into_iter().collect())
        })
        .collect();
    // pf table names are at most 31 characters.
    let name = format!("t{:016x}", fnv(&format!("{resolved:?}")));
    if !tables.contains_key(&name) {
        let addrs = endpoints
            .iter()
            .filter(|e| {
                resolved.is_empty()
                    || resolved.iter().any(|(namespace, services)| {
                        e.namespace == *namespace
                            && services.iter().all(|(k, v)| e.labels.get(k) == Some(v))
                    })
            })
            .flat_map(|e| e.alive.iter().copied())
            .collect();
        tables.insert(name.clone(), addrs);
    }
    name
}

fn proto(p: Protocol) -> Proto {
    match p {
        Protocol::Tcp => Proto::Tcp,
        Protocol::Udp => Proto::Udp,
    }
}

/// FNV-1a: stable across runs and builds, unlike std's hasher.
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use legion_api::dispatch::{EndpointPort, NamespacedPolicy};
    use legion_api::policy::{IngressRule, NetworkPolicySpec, PolicyPort};

    use super::*;

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn svc(ns: &str, name: &str, app: &str, vip: &str, alive: &[&str]) -> ServiceEndpoints {
        ServiceEndpoints {
            namespace: ns.into(),
            name: name.into(),
            uid: format!("{ns}-{name}"),
            labels: BTreeMap::from([("app".into(), app.into())]),
            vip: Some(ip(vip)),
            ports: vec![EndpointPort {
                protocol: Protocol::Tcp,
                port: 80,
                target: 8080,
                published: None,
            }],
            ready: alive.iter().map(|a| ip(a)).collect(),
            alive: alive.iter().map(|a| ip(a)).collect(),
            ordinals: BTreeMap::new(),
        }
    }

    fn pod(ns: &str, service: &str, addr: &str) -> LocalPod {
        LocalPod {
            interface: format!("lp-{service}"),
            ip: ip(addr),
            namespace: ns.into(),
            service: service.into(),
        }
    }

    fn endpoints() -> Endpoints {
        vec![
            svc("a", "web", "web", "10.201.0.1", &["10.200.0.2"]),
            svc("a", "db", "db", "10.201.0.2", &["10.200.0.3"]),
            svc("b", "web", "web", "10.201.0.3", &["10.200.1.2"]),
        ]
    }

    /// The addresses of the tables a guard allows from, ports aside.
    fn sources(g: &Guard, tables: &BTreeMap<String, Vec<Ipv4Addr>>) -> BTreeSet<Ipv4Addr> {
        g.allowed
            .iter()
            .flat_map(|a| tables[&a.table].iter().copied())
            .collect()
    }

    #[test]
    fn namespaces_keep_to_themselves() {
        let pods = [pod("a", "db", "10.200.0.3")];
        let (guards, tables) = guards(&pods, &endpoints(), &Policies::default());
        let g = &guards[0];
        assert!(!g.open);
        assert_eq!(
            sources(g, &tables),
            BTreeSet::from([ip("10.200.0.2"), ip("10.200.0.3")])
        );
        // Its own namespace's VIPs only.
        let vips: Vec<Ipv4Addr> = g.vips.iter().map(|v| v.0).collect();
        assert_eq!(vips, [ip("10.201.0.1"), ip("10.201.0.2")]);
        assert_eq!(g.vips[0].2, 80, "the VIP's port, not the task's");
    }

    #[test]
    fn open_and_locked() {
        let policies = Policies {
            isolation: BTreeMap::from([
                ("a".into(), Isolation::Locked),
                ("b".into(), Isolation::Open),
            ]),
            ..Default::default()
        };
        let pods = [pod("a", "db", "10.200.0.3"), pod("b", "web", "10.200.1.2")];
        let (guards, tables) = guards(&pods, &endpoints(), &policies);
        // Locked: not even its own namespace; and its VIPs are closed to
        // its own tasks too. Open: b's VIP is everyone's.
        assert!(sources(&guards[0], &tables).is_empty());
        assert!(!guards[0].open);
        let vips: Vec<Ipv4Addr> = guards[0].vips.iter().map(|v| v.0).collect();
        assert_eq!(vips, [ip("10.201.0.3")]);
        assert!(guards[1].open);
    }

    #[test]
    fn policies_let_more_in() {
        // b's web may reach a's db on 8080 only.
        let policies = Policies {
            policies: vec![NamespacedPolicy {
                namespace: "a".into(),
                name: "db-from-b".into(),
                spec: NetworkPolicySpec {
                    services: BTreeMap::from([("app".into(), "db".into())]),
                    ingress: vec![IngressRule {
                        from: vec![PolicyPeer {
                            namespace: Some("b".into()),
                            services: BTreeMap::from([("app".into(), "web".into())]),
                        }],
                        ports: vec![PolicyPort {
                            port: 8080,
                            protocol: Protocol::Tcp,
                        }],
                    }],
                },
            }],
            ..Default::default()
        };
        let pods = [
            pod("a", "db", "10.200.0.3"),
            pod("a", "web", "10.200.0.2"),
            pod("b", "web", "10.200.1.2"),
        ];
        let (guards, tables) = guards(&pods, &endpoints(), &policies);
        let db = &guards[0];
        let from_b = db.allowed.iter().find(|a| !a.ports.is_empty()).unwrap();
        assert_eq!(tables[&from_b.table], [ip("10.200.1.2")]);
        assert_eq!(from_b.ports, [(Proto::Tcp, 8080)]);
        // a's web is not selected: nothing from b.
        assert_eq!(guards[1].allowed.len(), 1);
        // b's web may use a's db VIP, not a's web VIP.
        let vips: Vec<Ipv4Addr> = guards[2].vips.iter().map(|v| v.0).collect();
        assert_eq!(vips, [ip("10.201.0.2"), ip("10.201.0.3")]);
    }
}
