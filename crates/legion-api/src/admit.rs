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

/// Spec of a `Namespace`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSpec {
    /// Which tasks may reach this namespace's tasks without a policy.
    #[serde(default)]
    pub isolation: crate::policy::Isolation,
    /// Limits on what the namespace's tasks and volumes may take.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<crate::quota::Quota>,
    /// How far the namespace's containers may loosen their jails.
    #[serde(
        default,
        skip_serializing_if = "crate::security::NamespaceSecurity::is_empty"
    )]
    pub security: crate::security::NamespaceSecurity,
}

impl Spec for NamespaceSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        Ok(())
    }
}

/// Validates `obj` and replaces its spec with the normalised form.
/// The most an object's labels and spec take, as JSON. Every manager
/// holds every object, and watches keep thousands of changes in memory;
/// a Secret's largest value fits, encoded.
pub const MAX_OBJECT: usize = 1 << 20;

pub fn admit(obj: &mut Object) -> Result<(), Error> {
    obj.validate()?;
    let labels: usize = obj
        .metadata
        .labels
        .iter()
        .map(|(k, v)| k.len() + v.len() + 6)
        .sum();
    let too_big = |obj: &Object| {
        let size = labels + serde_json::to_vec(&obj.spec).map_or(0, |v| v.len());
        (size > MAX_OBJECT).then(|| Error::Spec {
            kind: obj.kind,
            invalid: Invalid::new(
                "",
                format!("{size} bytes of labels and spec is above the {MAX_OBJECT}-byte limit"),
            ),
        })
    };
    if let Some(e) = too_big(obj) {
        return Err(e);
    }
    let spec = std::mem::take(&mut obj.spec);
    let normalised = match obj.kind {
        Kind::Namespace => normalise::<NamespaceSpec>(spec),
        Kind::Node => normalise::<NodeSpec>(spec),
        Kind::Service => normalise::<ServiceSpec>(spec).and_then(|v| {
            let s: ServiceSpec = parse(&v)?;
            s.check_names(&obj.metadata.name)?;
            Ok(v)
        }),
        Kind::Secret | Kind::Config => normalise::<DataSpec>(spec),
        Kind::Volume => normalise::<VolumeSpec>(spec),
        Kind::Task => return Err(Error::Managed(obj.kind)),
        Kind::RoleBinding => normalise::<crate::rbac::RoleBindingSpec>(spec),
        Kind::NetworkPolicy => normalise::<crate::policy::NetworkPolicySpec>(spec),
        Kind::Job => normalise::<crate::job::JobSpec>(job_defaults(spec, &["pod"])),
        Kind::CronJob => {
            if obj.metadata.name.len() > crate::job::CRON_JOB_NAME_MAX {
                return Err(Error::Spec {
                    kind: obj.kind,
                    invalid: Invalid::new(
                        "",
                        format!(
                            "a CronJob's name is at most {} characters: its Jobs add the time of each run",
                            crate::job::CRON_JOB_NAME_MAX
                        ),
                    ),
                });
            }
            normalise::<crate::job::CronJobSpec>(job_defaults(spec, &["job", "pod"]))
        }
        Kind::Route => normalise::<crate::route::RouteSpec>(spec),
    };
    obj.spec = normalised.map_err(|invalid| Error::Spec {
        kind: obj.kind,
        invalid,
    })?;
    // Again with the defaults filled in, as it is stored: what is stored
    // is admitted again on restore.
    match too_big(obj) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// A Job's pod restarts nothing in place unless asked: its restart
/// condition defaults to `none` rather than a Service's `any`, which a Job
/// may not have. `pod` is the path to the pod in `spec`.
fn job_defaults(mut spec: serde_json::Value, pod: &[&str]) -> serde_json::Value {
    let mut at = &mut spec;
    for key in pod {
        match at.get_mut(*key) {
            Some(v) => at = v,
            None => return spec,
        }
    }
    if let Some(pod) = at.as_object_mut() {
        let policy = pod
            .entry("restartPolicy")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(policy) = policy.as_object_mut() {
            policy.entry("condition").or_insert_with(|| "none".into());
        }
    }
    spec
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

/// An absolute path in its one plain spelling, so two that name the same
/// place compare equal: no `..`, `.` or empty parts, no trailing `/`.
pub(crate) fn check_absolute(path: &str, p: &str) -> Result<(), Invalid> {
    let plain = p == "/"
        || p.strip_prefix('/')
            .is_some_and(|rest| rest.split('/').all(|c| !matches!(c, "" | "." | "..")));
    if plain {
        Ok(())
    } else {
        Err(Invalid::new(
            path,
            format!("{p:?} must be an absolute path without `..`, `.`, `//` or a trailing `/`"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_have_a_size_limit() {
        let secret = |data: String, labels: usize| {
            let mut o = Object::new(
                Kind::Secret,
                "shop",
                "key",
                serde_json::json!({ "text": data }),
            );
            for n in 0..labels {
                o.metadata.labels.insert(format!("l{n}"), "v".repeat(60));
            }
            o
        };
        // The largest Secret fits.
        admit(&mut secret("x".repeat(crate::data::MAX_DATA), 0)).unwrap();
        // Labels count.
        let why = admit(&mut secret(String::new(), 20_000))
            .unwrap_err()
            .to_string();
        assert!(why.contains("above the 1048576-byte limit"), "{why}");
    }

    #[test]
    fn absolute_paths_have_one_spelling() {
        for good in ["/", "/data", "/etc/app.conf", "/a..b/.c"] {
            check_absolute("p", good).unwrap();
        }
        for bad in [
            "", "data", "/data/", "//data", "/a//b", "/a/./b", "/.", "/a/..", "/a/../b",
        ] {
            assert!(check_absolute("p", bad).is_err(), "{bad:?}");
        }
    }
}
