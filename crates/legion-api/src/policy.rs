//! What may reach a namespace's tasks (DESIGN.md §5.6): the namespace's
//! isolation, and `NetworkPolicy` objects allowing more; and what tasks
//! may reach, when a policy's `egress` says.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, field, index};
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

#[cfg(test)]
mod tests {
    use super::*;

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
