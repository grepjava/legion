//! Admission: every object written through the API is parsed into its typed
//! spec, validated and normalised here. The stored spec is the normalised one,
//! with every default written out, so controllers never apply defaults
//! themselves and two manifests that mean the same thing store the same spec.

use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::data::DataSpec;
use crate::node::NodeSpec;
use crate::service::ServiceSpec;
use crate::volume::VolumeSpec;
use crate::{Error, Kind, Object};

/// A typed spec.
pub trait Spec: Serialize + DeserializeOwned {
    /// Checks the spec and fills in defaults. Paths in errors are relative to
    /// the spec (`pod.containers[0].image`).
    fn admit(&mut self) -> Result<(), Invalid>;
}

/// Why a spec was rejected, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid {
    /// Dotted path from the spec root; empty for the spec itself.
    pub path: String,
    pub message: String,
}

impl Invalid {
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Invalid {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            write!(f, "spec: {}", self.message)
        } else {
            write!(f, "spec.{}: {}", self.path, self.message)
        }
    }
}

/// Spec of a `Namespace`: nothing yet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSpec {}

impl Spec for NamespaceSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        Ok(())
    }
}

/// Validates `obj` and replaces its spec with the normalised form.
pub fn admit(obj: &mut Object) -> Result<(), Error> {
    obj.validate()?;
    let spec = std::mem::take(&mut obj.spec);
    let normalised = match obj.kind {
        Kind::Namespace => normalise::<NamespaceSpec>(spec),
        Kind::Node => normalise::<NodeSpec>(spec),
        Kind::Service => normalise::<ServiceSpec>(spec),
        Kind::Secret | Kind::Config => normalise::<DataSpec>(spec),
        Kind::Volume => normalise::<VolumeSpec>(spec),
        Kind::Task => return Err(Error::Managed(obj.kind)),
        Kind::Job | Kind::CronJob | Kind::NetworkPolicy | Kind::Route | Kind::RoleBinding => {
            return Err(Error::Unsupported(obj.kind));
        }
    };
    obj.spec = normalised.map_err(|invalid| Error::Spec {
        kind: obj.kind,
        invalid,
    })?;
    Ok(())
}

/// Parses `value` as `T`, as the stored spec of an object that passed
/// admission. Controllers use this to read specs.
pub fn parse<T: Spec>(value: &serde_json::Value) -> Result<T, Invalid> {
    let value = match value {
        serde_json::Value::Null => &serde_json::Value::Object(Default::default()),
        v => v,
    };
    serde_path_to_error::deserialize(value).map_err(|e| {
        let path = e.path().to_string();
        let path = if path == "." { String::new() } else { path };
        Invalid::new(path, e.into_inner().to_string())
    })
}

fn normalise<T: Spec>(value: serde_json::Value) -> Result<serde_json::Value, Invalid> {
    let mut spec: T = parse(&value)?;
    spec.admit()?;
    serde_json::to_value(&spec).map_err(|e| Invalid::new("", e.to_string()))
}

/// Builds child paths for error messages.
pub(crate) fn field(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}.{name}")
    }
}

pub(crate) fn index(parent: &str, name: &str, i: usize) -> String {
    format!("{}[{i}]", field(parent, name))
}

/// `name` must be a DNS label; used for names that refer to other objects or
/// end up in DNS and jail names.
pub(crate) fn check_name(path: &str, name: &str) -> Result<(), Invalid> {
    crate::validate_name(name).map_err(|e| Invalid::new(path, e.to_string()))
}

pub(crate) fn check_absolute(path: &str, p: &str) -> Result<(), Invalid> {
    if p.starts_with('/') && !p.split('/').any(|c| c == "..") {
        Ok(())
    } else {
        Err(Invalid::new(
            path,
            format!("{p:?} must be an absolute path without `..`"),
        ))
    }
}
