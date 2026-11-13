//! What may reach a namespace's tasks (DESIGN.md §5.6): the namespace's
//! isolation, and `NetworkPolicy` objects allowing more; and what tasks
//! may reach, when a policy's `egress` says.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, field, index};
use crate::dispatch::{Policies, ServiceEndpoints};
use crate::service::Protocol;

/// Which tasks may reach a namespace's tasks without a policy saying so.
/// The nodes themselves (probes, published ports) always may.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Isolation {
    /// Tasks of the same namespace.
    #[default]
    Namespace,
    /// Every task in the cluster.
    Open,
    /// None: only what policies allow.
    Locked,
}

/// Spec of a `NetworkPolicy`: lets traffic reach the namespace's tasks of
/// the services it selects, and with `egress` limits what they send.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NetworkPolicySpec {
    /// Services of the policy's namespace whose labels include all of
    /// these; empty: all of them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, String>,
    /// Traffic that may reach their tasks, in addition to what the
    /// namespace's isolation allows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ingress: Vec<IngressRule>,
    /// Given, the selected tasks may send only what one of the egress
    /// rules of the policies selecting them allows, and DNS to their
    /// node's resolver; empty, nothing more. Not given, this policy does
    /// not limit what they send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<Vec<EgressRule>>,
}

/// The tasks may send to some destinations, on some ports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressRule {
    /// Where; empty: anywhere.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<EgressPeer>,
    /// The destination's ports (a task's, not a VIP's); empty: all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PolicyPort>,
}

/// A destination: tasks by namespace and service labels, as
/// [`PolicyPeer`], or IPv4 addresses (`cidr`), such as the outside's.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressPeer {
    /// The policy's own when not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Services whose labels include all of these; empty: all of them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, String>,
    /// `a.b.c.d/n`, instead of tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cidr: Option<String>,
}

impl EgressPeer {
    /// The tasks it names, unless it is addresses.
    pub fn tasks(&self) -> Option<PolicyPeer> {
        self.cidr.is_none().then(|| PolicyPeer {
            namespace: self.namespace.clone(),
            services: self.services.clone(),
        })
    }
}

/// `a.b.c.d/n`: the address and the prefix length.
pub fn parse_cidr(s: &str) -> Option<(std::net::Ipv4Addr, u8)> {
    let (addr, prefix) = s.split_once('/')?;
    let prefix: u8 = prefix.parse().ok()?;
    (prefix <= 32).then_some((addr.parse().ok()?, prefix))
}

/// Tasks of some services may reach some ports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngressRule {
    /// Whose tasks; empty: every task in the cluster.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<PolicyPeer>,
    /// The tasks' ports (not a VIP's); empty: all of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PolicyPort>,
}

/// Tasks by namespace and service labels.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PolicyPeer {
    /// The policy's own when not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Services whose labels include all of these; empty: all of them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PolicyPort {
    pub port: u16,
    #[serde(default)]
    pub protocol: Protocol,
}

impl Spec for NetworkPolicySpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        for (i, rule) in self.ingress.iter().enumerate() {
            let path = index("", "ingress", i);
            for (j, peer) in rule.from.iter().enumerate() {
                if let Some(ns) = &peer.namespace {
                    crate::validate_name(ns).map_err(|e| {
                        Invalid::new(field(&index(&path, "from", j), "namespace"), e.to_string())
                    })?;
                }
            }
            ports_valid(&path, &rule.ports)?;
        }
        for (i, rule) in self.egress.iter().flatten().enumerate() {
            let path = index("", "egress", i);
            for (j, peer) in rule.to.iter().enumerate() {
                let at = index(&path, "to", j);
                if let Some(ns) = &peer.namespace {
                    crate::validate_name(ns)
                        .map_err(|e| Invalid::new(field(&at, "namespace"), e.to_string()))?;
                }
                if let Some(cidr) = &peer.cidr {
                    if parse_cidr(cidr).is_none() {
                        return Err(Invalid::new(
                            field(&at, "cidr"),
                            "an IPv4 network, a.b.c.d/n",
                        ));
                    }
                    if peer.namespace.is_some() || !peer.services.is_empty() {
                        return Err(Invalid::new(
                            field(&at, "cidr"),
                            "addresses or tasks (namespace, services), not both",
                        ));
                    }
                }
            }
            ports_valid(&path, &rule.ports)?;
        }
        Ok(())
    }
}

fn ports_valid(path: &str, ports: &[PolicyPort]) -> Result<(), Invalid> {
    for (j, port) in ports.iter().enumerate() {
        if port.port == 0 {
            return Err(Invalid::new(
                field(&index(path, "ports", j), "port"),
                "a port is 1 to 65535",
            ));
        }
    }
    Ok(())
}

/// Whether `labels` include every pair of `selector`.
pub fn selects(selector: &BTreeMap<String, String>, labels: &BTreeMap<String, String>) -> bool {
    selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

/// Whether tasks of namespace `from_ns`, of a service labelled
/// `from_labels`, may reach `to`'s tasks on `port`: the decision the
/// destination's guard makes, by its namespace's isolation and the
/// policies selecting it.
pub fn may_reach(
    policies: &Policies,
    from_ns: &str,
    from_labels: &BTreeMap<String, String>,
    to: &ServiceEndpoints,
    protocol: Protocol,
    port: u16,
) -> bool {
    let ns = to.namespace.as_str();
    match policies.isolation(ns) {
        Isolation::Open => return true,
        Isolation::Namespace if from_ns == ns => return true,
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
                    peer.namespace.as_deref().unwrap_or(ns) == from_ns
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

/// The egress rules of the policies selecting a service of namespace `ns`
/// labelled `labels`, if any of them limits what its tasks send.
pub fn egress_rules<'a>(
    policies: &'a Policies,
    ns: &str,
    labels: &BTreeMap<String, String>,
) -> Option<Vec<&'a EgressRule>> {
    let mut limited = false;
    let mut rules = Vec::new();
    for p in policies
        .policies
        .iter()
        .filter(|p| p.namespace == ns && selects(&p.spec.services, labels))
    {
        if let Some(egress) = &p.spec.egress {
            limited = true;
            rules.extend(egress);
        }
    }
    limited.then_some(rules)
}

/// Whether tasks of namespace `from_ns` limited to `rules` (not limited
/// without) may send to `to`'s tasks on `port`, as through its VIP.
pub fn may_send(
    rules: Option<&[&EgressRule]>,
    from_ns: &str,
    to: &ServiceEndpoints,
    protocol: Protocol,
    port: u16,
) -> bool {
    let Some(rules) = rules else {
        return true;
    };
    rules.iter().any(|rule| {
        let to_ok = rule.to.is_empty()
            || rule.to.iter().any(|peer| match peer.tasks() {
                Some(t) => {
                    t.namespace.as_deref().unwrap_or(from_ns) == to.namespace
                        && selects(&t.services, &to.labels)
                }
                None => peer
                    .cidr
                    .as_deref()
                    .and_then(parse_cidr)
                    .zip(to.vip)
                    .is_some_and(|((net, prefix), vip)| in_net(vip, net, prefix)),
            });
        let port_ok = rule.ports.is_empty()
            || rule
                .ports
                .iter()
                .any(|p| p.port == port && p.protocol == protocol);
        to_ok && port_ok
    })
}

/// What one source may reach of one destination: every port of its tasks,
/// or some of the ports the destination names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reach {
    /// A service of the namespace; `None` for the tasks of every other
    /// namespace.
    pub from: Option<String>,
    pub to: String,
    pub all_ports: bool,
    /// The destination's ports (its tasks'), when not all.
    pub ports: Vec<(Protocol, u16)>,
}

/// What may reach what among namespace `ns`'s services, as the nodes'
/// guards decide it: each service of `ns`, and the tasks of other
/// namespaces, against each service of `ns`, through both the
/// destination's ingress and the source's egress. Jobs are left out: their
/// tasks carry no service's labels and name no ports.
pub fn reach(policies: &Policies, endpoints: &[ServiceEndpoints], ns: &str) -> Vec<Reach> {
    let services: Vec<&ServiceEndpoints> = endpoints
        .iter()
        .filter(|e| e.namespace == ns && !e.job)
        .collect();
    let none = BTreeMap::new();
    // Other namespaces' tasks: labelled by nothing this namespace's rules
    // name (a rule naming one other namespace allows only that one), and
    // limited in what they send by their own policies, not these.
    let mut sources: Vec<(Option<&str>, &str, &BTreeMap<String, String>)> = vec![(None, "", &none)];
    sources.extend(
        services
            .iter()
            .map(|s| (Some(s.name.as_str()), ns, &s.labels)),
    );
    let mut out = Vec::new();
    for (from, from_ns, labels) in sources {
        let egress = from.and_then(|_| egress_rules(policies, ns, labels));
        let allowed = |to: &ServiceEndpoints, protocol: Protocol, port: u16| {
            may_reach(policies, from_ns, labels, to, protocol, port)
                && may_send(egress.as_deref(), from_ns, to, protocol, port)
        };
        for to in &services {
            // No rule names port 0: allowed there is allowed on every port.
            let all_ports = allowed(to, Protocol::Tcp, 0) && allowed(to, Protocol::Udp, 0);
            let ports: Vec<(Protocol, u16)> = if all_ports {
                Vec::new()
            } else {
                to.ports
                    .iter()
                    .filter(|p| allowed(to, p.protocol, p.target))
                    .map(|p| (p.protocol, p.target))
                    .collect()
            };
            if all_ports || !ports.is_empty() {
                out.push(Reach {
                    from: from.map(str::to_owned),
                    to: to.name.clone(),
                    all_ports,
                    ports,
                });
            }
        }
    }
    out
}

/// Whether `addr` is in `net/prefix`.
fn in_net(addr: std::net::Ipv4Addr, net: std::net::Ipv4Addr, prefix: u8) -> bool {
    let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
    u32::from(addr) & mask == u32::from(net) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_reaches_what_in_a_namespace() {
        use crate::dispatch::{EndpointPort, NamespacedPolicy};
        let svc = |name: &str, port: u16| ServiceEndpoints {
            namespace: "shop".into(),
            name: name.into(),
            uid: format!("u-{name}"),
            labels: [("app".to_owned(), name.to_owned())].into(),
            vip: None,
            ports: vec![EndpointPort {
                protocol: Protocol::Tcp,
                port,
                target: port,
                published: None,
            }],
            ready: vec![],
            alive: vec![],
            ordinals: BTreeMap::new(),
            job: false,
            load_balancer: None,
        };
        let policy = |name: &str, spec: serde_json::Value| NamespacedPolicy {
            namespace: "shop".into(),
            name: name.into(),
            spec: serde_json::from_value(spec).unwrap(),
        };
        let mut elsewhere = svc("api", 8080);
        elsewhere.namespace = "bank".into();
        let endpoints = [svc("web", 80), svc("db", 5432), elsewhere];
        let policies = Policies {
            isolation: BTreeMap::new(),
            policies: vec![
                // web's port 80 to every task; db sends nothing.
                policy(
                    "public-web",
                    serde_json::json!({ "services": { "app": "web" }, "ingress": [{ "ports": [{ "port": 80 }] }] }),
                ),
                policy(
                    "db-quiet",
                    serde_json::json!({ "services": { "app": "db" }, "egress": [] }),
                ),
            ],
        };
        let got: Vec<_> = reach(&policies, &endpoints, "shop")
            .into_iter()
            .map(|r| (r.from, r.to, r.all_ports, r.ports))
            .collect();
        let s = |v: &str| Some(v.to_owned());
        assert_eq!(
            got,
            [
                (None, "web".to_owned(), false, vec![(Protocol::Tcp, 80)]),
                (s("web"), "web".to_owned(), true, vec![]),
                (s("web"), "db".to_owned(), true, vec![]),
            ]
        );

        // Open, the namespace takes every task; db still sends nothing.
        let open = Policies {
            isolation: [("shop".to_owned(), Isolation::Open)].into(),
            ..policies
        };
        let from: Vec<_> = reach(&open, &endpoints, "shop")
            .into_iter()
            .filter(|r| r.all_ports)
            .map(|r| (r.from, r.to))
            .collect();
        assert_eq!(
            from,
            [
                (None, "web".to_owned()),
                (None, "db".to_owned()),
                (s("web"), "web".to_owned()),
                (s("web"), "db".to_owned()),
            ]
        );
    }

    #[test]
    fn an_address_is_in_a_network_by_its_prefix() {
        let ip = |s: &str| s.parse::<std::net::Ipv4Addr>().unwrap();
        assert!(in_net(ip("10.1.2.3"), ip("10.1.0.0"), 16));
        assert!(!in_net(ip("10.2.2.3"), ip("10.1.0.0"), 16));
        assert!(
            in_net(ip("192.0.2.9"), ip("0.0.0.0"), 0),
            "/0 is everything"
        );
        assert!(in_net(ip("192.0.2.9"), ip("192.0.2.9"), 32));
        assert!(!in_net(ip("192.0.2.8"), ip("192.0.2.9"), 32));
        assert!(in_net(ip("10.0.0.255"), ip("10.0.0.128"), 25));
        assert!(!in_net(ip("10.0.0.127"), ip("10.0.0.128"), 25));
    }

    #[test]
    fn policies() {
        let mut p: NetworkPolicySpec = serde_json::from_value(serde_json::json!({
            "services": { "app": "db" },
            "ingress": [{ "from": [{ "namespace": "shop", "services": { "app": "api" } }],
                          "ports": [{ "port": 5432 }] }],
        }))
        .unwrap();
        p.admit().unwrap();
        assert_eq!(p.ingress[0].ports[0].protocol, Protocol::Tcp);
        p.ingress[0].ports[0].port = 0;
        assert_eq!(p.admit().unwrap_err().path, "ingress[0].ports[0].port");
        p.ingress[0].ports.clear();
        p.ingress[0].from[0].namespace = Some("Shop!".into());
        assert_eq!(p.admit().unwrap_err().path, "ingress[0].from[0].namespace");
    }

    #[test]
    fn egress() {
        let mut p: NetworkPolicySpec = serde_json::from_value(serde_json::json!({
            "egress": [
                { "to": [{ "services": { "app": "db" } }], "ports": [{ "port": 5432 }] },
                { "to": [{ "cidr": "192.0.2.0/24" }], "ports": [{ "port": 443 }] },
            ],
        }))
        .unwrap();
        p.admit().unwrap();
        let rules = p.egress.as_ref().unwrap();
        assert!(rules[0].to[0].tasks().is_some());
        assert!(rules[1].to[0].tasks().is_none());
        // Nothing allowed, but given: everything blocked.
        let none: NetworkPolicySpec = serde_json::from_str(r#"{"egress": []}"#).unwrap();
        assert_eq!(none.egress, Some(vec![]));
        let not: NetworkPolicySpec = serde_json::from_str("{}").unwrap();
        assert_eq!(not.egress, None);

        p.egress.as_mut().unwrap()[1].to[0].cidr = Some("192.0.2.0/33".into());
        assert_eq!(p.admit().unwrap_err().path, "egress[1].to[0].cidr");
        p.egress.as_mut().unwrap()[1].to[0] = EgressPeer {
            namespace: Some("shop".into()),
            cidr: Some("192.0.2.0/24".into()),
            ..Default::default()
        };
        assert_eq!(p.admit().unwrap_err().path, "egress[1].to[0].cidr");
        p.egress.as_mut().unwrap()[1].to.clear();
        p.egress.as_mut().unwrap()[1].ports[0].port = 0;
        assert_eq!(p.admit().unwrap_err().path, "egress[1].ports[0].port");
        assert_eq!(
            parse_cidr("0.0.0.0/0"),
            Some((std::net::Ipv4Addr::UNSPECIFIED, 0))
        );
        assert_eq!(parse_cidr("10.0.0.1"), None);
    }

    #[test]
    fn selectors() {
        let labels = BTreeMap::from([("app".into(), "db".into()), ("tier".into(), "x".into())]);
        assert!(selects(&BTreeMap::new(), &labels));
        assert!(selects(
            &BTreeMap::from([("app".into(), "db".into())]),
            &labels
        ));
        assert!(!selects(
            &BTreeMap::from([("app".into(), "web".into())]),
            &labels
        ));
    }
}
