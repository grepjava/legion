//! Legion object model (DESIGN.md §7): the envelope every object shares, the
//! typed spec and status of each kind, and admission, which validates and
//! normalises a spec before it is stored.
//!
//! The envelope carries spec and status as JSON so the store and the API
//! stay independent of kinds; [`admit`] and [`parse`] convert to the types.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub mod admit;
pub mod clock;
pub mod compose;
pub mod cron;
pub mod data;
pub mod dispatch;
pub mod job;
pub mod metrics;
pub mod node;
pub mod policy;
pub mod quantity;
pub mod quota;
pub mod rbac;
pub mod route;
pub mod security;
pub mod service;
pub mod task;
pub mod volume;

pub use admit::{Invalid, Spec, admit, parse};

pub const API_VERSION: &str = "legion/v1";

/// Namespace used for cluster-scoped kinds (`Namespace`, `Node`).
pub const CLUSTER_SCOPE: &str = "";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Kind {
    Namespace,
    Node,
    Service,
    Job,
    CronJob,
    Task,
    Secret,
    Config,
    Volume,
    NetworkPolicy,
    Route,
    RoleBinding,
}

impl Kind {
    pub const ALL: [Kind; 12] = [
        Kind::Namespace,
        Kind::Node,
        Kind::Service,
        Kind::Job,
        Kind::CronJob,
        Kind::Task,
        Kind::Secret,
        Kind::Config,
        Kind::Volume,
        Kind::NetworkPolicy,
        Kind::Route,
        Kind::RoleBinding,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Namespace => "Namespace",
            Kind::Node => "Node",
            Kind::Service => "Service",
            Kind::Job => "Job",
            Kind::CronJob => "CronJob",
            Kind::Task => "Task",
            Kind::Secret => "Secret",
            Kind::Config => "Config",
            Kind::Volume => "Volume",
            Kind::NetworkPolicy => "NetworkPolicy",
            Kind::Route => "Route",
            Kind::RoleBinding => "RoleBinding",
        }
    }

    /// Cluster-scoped kinds live outside any namespace.
    pub fn is_cluster_scoped(self) -> bool {
        matches!(self, Kind::Namespace | Kind::Node)
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Kind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| Error::UnknownKind(s.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    // Set by the server.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
    /// Raft index of the last write.
    #[serde(default)]
    pub version: u64,
    /// Bumps on spec changes only.
    #[serde(default)]
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    pub api_version: String,
    pub kind: Kind,
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: serde_json::Value,
    #[serde(default)]
    pub status: serde_json::Value,
}

impl Object {
    pub fn new(kind: Kind, namespace: &str, name: &str, spec: serde_json::Value) -> Self {
        Object {
            api_version: API_VERSION.to_owned(),
            kind,
            metadata: ObjectMeta {
                name: name.to_owned(),
                namespace: namespace.to_owned(),
                ..Default::default()
            },
            spec,
            status: serde_json::Value::Null,
        }
    }

    pub fn key(&self) -> ObjectKey {
        ObjectKey {
            kind: self.kind,
            namespace: self.metadata.namespace.clone(),
            name: self.metadata.name.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.api_version != API_VERSION {
            return Err(Error::ApiVersion(self.api_version.clone()));
        }
        self.key().validate()
    }
}

/// Identifies an object. Its string form `Kind/namespace/name` is also the
/// storage key, so a prefix scan by kind or by kind and namespace is a range scan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectKey {
    pub kind: Kind,
    pub namespace: String,
    pub name: String,
}

impl ObjectKey {
    pub fn new(kind: Kind, namespace: &str, name: &str) -> Self {
        ObjectKey {
            kind,
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        validate_name(&self.name)?;
        if self.kind.is_cluster_scoped() {
            if !self.namespace.is_empty() {
                return Err(Error::Scope(self.kind));
            }
        } else {
            validate_name(&self.namespace)?;
        }
        Ok(())
    }

    /// Prefix shared by every key of `kind`, optionally within one namespace.
    pub fn prefix(kind: Kind, namespace: Option<&str>) -> String {
        match namespace {
            Some(ns) => format!("{kind}/{ns}/"),
            None => format!("{kind}/"),
        }
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.kind, self.namespace, self.name)
    }
}

impl FromStr for ObjectKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let mut parts = s.splitn(3, '/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(kind), Some(ns), Some(name)) => Ok(ObjectKey::new(kind.parse()?, ns, name)),
            _ => Err(Error::BadKey(s.to_owned())),
        }
    }
}

/// DNS-label rules: names end up in DNS (section 5.5) and jail names.
pub fn validate_name(name: &str) -> Result<(), Error> {
    let ok = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(Error::BadName(name.to_owned()))
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("unknown kind {0:?}")]
    UnknownKind(String),
    #[error("unsupported apiVersion {0:?}, expected {API_VERSION}")]
    ApiVersion(String),
    #[error("invalid name {0:?}: lowercase letters, digits and '-', at most 63 characters")]
    BadName(String),
    #[error("{0} is cluster-scoped and takes no namespace")]
    Scope(Kind),
    #[error("malformed object key {0:?}")]
    BadKey(String),
    #[error("invalid {kind}: {invalid}")]
    Spec { kind: Kind, invalid: Invalid },
    #[error("{0} objects are managed by Legion")]
    Managed(Kind),
    #[error("{0} is not supported yet")]
    Unsupported(Kind),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trip() {
        let key = ObjectKey::new(Kind::Service, "shop", "web");
        assert_eq!(key.to_string(), "Service/shop/web");
        assert_eq!("Service/shop/web".parse::<ObjectKey>().unwrap(), key);
        assert_eq!(
            "Node//n1".parse::<ObjectKey>().unwrap(),
            ObjectKey::new(Kind::Node, "", "n1")
        );
    }

    #[test]
    fn names() {
        assert!(validate_name("web-1").is_ok());
        assert!(validate_name("Web").is_err());
        assert!(validate_name("-web").is_err());
        assert!(validate_name("").is_err());
        assert!(validate_name(&"a".repeat(64)).is_err());
    }

    #[test]
    fn scope() {
        assert!(ObjectKey::new(Kind::Node, "", "n1").validate().is_ok());
        assert_eq!(
            ObjectKey::new(Kind::Node, "shop", "n1").validate(),
            Err(Error::Scope(Kind::Node))
        );
        assert!(ObjectKey::new(Kind::Service, "", "web").validate().is_err());
    }

    #[test]
    fn envelope_yaml_shape() {
        let json = r#"{"apiVersion":"legion/v1","kind":"Service",
            "metadata":{"name":"web","namespace":"shop","labels":{"app":"web"}},
            "spec":{"replicas":3}}"#;
        let obj: Object = serde_json::from_str(json).unwrap();
        obj.validate().unwrap();
        assert_eq!(obj.metadata.labels["app"], "web");
        assert_eq!(obj.metadata.version, 0);
    }
}
