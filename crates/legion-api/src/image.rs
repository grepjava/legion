//! Image identity (PLAN.md, phase 3): which image a reference named when a
//! Service or Job was deployed. The leader resolves each reference once, for
//! a generation of the pod, and its tasks run that digest wherever they land.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// How a container's image is found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PullPolicy {
    /// The leader asks the registry what the reference names at deployment,
    /// and every node runs that. A reference by digest, or a local one, is
    /// already what it names.
    #[default]
    Resolve,
    /// What the node holds by that name: no registry is asked, and nodes may
    /// differ.
    Never,
}

impl PullPolicy {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// A pod's images, as the leader resolved them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageStatus {
    /// The pod they are for, as a hash of it: a change elsewhere in the
    /// spec, such as the replica count, keeps them, as nothing was deployed.
    pub pod: String,
    /// Each reference as written, and the same image by digest. A reference
    /// left as written (by digest already, local, or `never`) isn't here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resolved: BTreeMap<String, String>,
    /// Why they aren't all resolved yet; empty once they are.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

impl ImageStatus {
    /// Whether tasks of the pod hashed as `pod` may be made with these.
    pub fn ready_for(&self, pod: &str) -> bool {
        self.pod == pod && self.message.is_empty()
    }
}

/// What a task runs for `image` as written: the digest it was pinned to, if
/// it was.
pub fn pinned<'a>(images: &'a BTreeMap<String, String>, image: &'a str) -> &'a str {
    images.get(image).map_or(image, String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_runs_as_pinned_or_as_written() {
        let images = BTreeMap::from([(
            "nginx:1".to_owned(),
            "docker.io/library/nginx@sha256:aa".to_owned(),
        )]);
        assert_eq!(
            pinned(&images, "nginx:1"),
            "docker.io/library/nginx@sha256:aa"
        );
        assert_eq!(pinned(&images, "localhost/app"), "localhost/app");
        assert_eq!(pinned(&BTreeMap::new(), "nginx:1"), "nginx:1");
    }

    #[test]
    fn images_are_ready_for_their_own_pod_once_resolved() {
        let mut s = ImageStatus {
            pod: "p1".into(),
            ..ImageStatus::default()
        };
        assert!(s.ready_for("p1"));
        assert!(!s.ready_for("p2"));
        s.message = "registry unreachable".into();
        assert!(!s.ready_for("p1"));
    }

    #[test]
    fn pull_policies_are_lowercase_and_resolve_by_default() {
        assert_eq!(
            serde_json::to_string(&PullPolicy::Never).unwrap(),
            r#""never""#
        );
        let p: PullPolicy = serde_json::from_str(r#""resolve""#).unwrap();
        assert!(p.is_default());
        assert!(serde_json::from_str::<PullPolicy>(r#""always""#).is_err());
    }
}
