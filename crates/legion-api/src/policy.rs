//! What may reach a namespace's tasks (DESIGN.md §5.6): the namespace's
//! isolation, and `NetworkPolicy` objects allowing more.

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
/// the services it selects.
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
            for (j, port) in rule.ports.iter().enumerate() {
                if port.port == 0 {
                    return Err(Invalid::new(
                        field(&index(&path, "ports", j), "port"),
                        "a port is 1 to 65535",
                    ));
                }
            }
        }
        Ok(())
    }
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
