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
    /// Each reference as written that an `ImagePolicy` covers, and the
    /// signature found good on what it runs (phase 7).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub verified: BTreeMap<String, Verification>,
    /// What a policy in `warn` mode found wrong; the pod runs regardless.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub warning: String,
}

/// A good signature on an image: the digest it signs and the key's
/// fingerprint (`sha256:` of its DER SubjectPublicKeyInfo).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verification {
    pub digest: String,
    pub key: String,
}

/// Spec of an `ImagePolicy` (cluster-scoped): images whose repository
/// matches one of `images` run only with a good signature, made as cosign
/// makes them, by one of `keys`. Every policy an image matches must be met.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImagePolicySpec {
    /// Repository patterns: `registry[:port]/path`, where `*` matches within
    /// one segment and `**` any number of whole segments, none included.
    pub images: Vec<String>,
    /// P-256 public keys, PKIX PEM (`PUBLIC KEY`).
    pub keys: Vec<String>,
    #[serde(default, skip_serializing_if = "PolicyMode::is_default")]
    pub mode: PolicyMode,
}

/// What a policy does with an image it finds no good signature on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyMode {
    /// Its pod gets no tasks until one is found.
    #[default]
    Enforce,
    /// Its pod runs, and the owner's status says what was found.
    Warn,
}

impl PolicyMode {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl crate::admit::Spec for ImagePolicySpec {
    fn admit(&mut self) -> Result<(), crate::admit::Invalid> {
        use crate::admit::{Invalid, index};
        if self.images.is_empty() {
            return Err(Invalid::new(
                "images",
                "a policy names at least one pattern",
            ));
        }
        for (i, p) in self.images.iter().enumerate() {
            pattern_valid(p).map_err(|e| Invalid::new(index("", "images", i), e))?;
        }
        if self.keys.is_empty() {
            return Err(Invalid::new("keys", "a policy names at least one key"));
        }
        for (i, k) in self.keys.iter().enumerate() {
            if !k.trim_start().starts_with("-----BEGIN PUBLIC KEY-----") {
                return Err(Invalid::new(
                    index("", "keys", i),
                    "not a PEM public key (-----BEGIN PUBLIC KEY-----)",
                ));
            }
        }
        Ok(())
    }
}

/// Whether `pattern` is one: a registry host first, then path segments of
/// a repository's characters or wildcards, and no tag or digest.
fn pattern_valid(pattern: &str) -> Result<(), String> {
    let mut segments = pattern.split('/');
    let host = segments.next().unwrap_or_default();
    if !(host.contains(['.', ':']) || host == "localhost") {
        return Err(format!(
            "{pattern:?} does not start with a registry (a host such as ghcr.io or registry.lab:5000)"
        ));
    }
    let host_ok = host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-.:".contains(&b));
    if !host_ok {
        return Err(format!("{pattern:?}: bad registry {host:?}"));
    }
    let mut any = false;
    for s in segments {
        any = true;
        let ok = s == "**"
            || (!s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-*".contains(&b))
                && !s.contains("**"));
        if !ok {
            return Err(format!(
                "{pattern:?}: bad segment {s:?} (a repository's lowercase name, `*` or `**`; no tag or digest)"
            ));
        }
    }
    if !any {
        return Err(format!(
            "{pattern:?} names no repository: add `/**` for all of the registry"
        ));
    }
    Ok(())
}

/// Whether the repository `name` (`registry/path`, as an image reference
/// normalises it) matches `pattern`.
pub fn matches(pattern: &str, name: &str) -> bool {
    fn segs(p: &[&str], n: &[&str]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(&"**"), _) => segs(&p[1..], n) || (!n.is_empty() && segs(p, &n[1..])),
            (Some(ps), Some(ns)) => one(ps.as_bytes(), ns.as_bytes()) && segs(&p[1..], &n[1..]),
            _ => false,
        }
    }
    // One segment: `*` matches any run of characters within it.
    fn one(p: &[u8], n: &[u8]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => one(&p[1..], n) || (!n.is_empty() && one(p, &n[1..])),
            (Some(a), Some(b)) => a == b && one(&p[1..], &n[1..]),
            _ => false,
        }
    }
    let p: Vec<&str> = pattern.split('/').collect();
    let n: Vec<&str> = name.split('/').collect();
    // The registry is matched whole, as written.
    p.len() > 1 && n.len() > 1 && p[0] == n[0] && segs(&p[1..], &n[1..])
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
    fn patterns_match_within_a_segment_or_across_whole_ones() {
        let yes = [
            ("ghcr.io/me/app", "ghcr.io/me/app"),
            ("ghcr.io/me/*", "ghcr.io/me/app"),
            ("ghcr.io/me/app-*", "ghcr.io/me/app-web"),
            ("ghcr.io/**", "ghcr.io/me/team/app"),
            ("ghcr.io/me/**", "ghcr.io/me/app"),
            ("ghcr.io/**/app", "ghcr.io/app"),
            ("ghcr.io/**/app", "ghcr.io/a/b/app"),
            ("registry.lab:5000/**", "registry.lab:5000/handoff/keep"),
            ("docker.io/library/*", "docker.io/library/nginx"),
        ];
        for (p, n) in yes {
            assert!(matches(p, n), "{p} should match {n}");
        }
        let no = [
            ("ghcr.io/me/*", "ghcr.io/me/team/app"),
            ("ghcr.io/me/app", "ghcr.io/me/app2"),
            ("ghcr.io/me/**", "ghcr.io/you/app"),
            ("ghcr.io/**", "quay.io/me/app"),
            ("registry.lab:5000/**", "registry.lab:5001/handoff/keep"),
            ("registry.lab/**", "registry.lab:5000/handoff/keep"),
            ("*.io/**", "ghcr.io/me/app"),
        ];
        for (p, n) in no {
            assert!(!matches(p, n), "{p} should not match {n}");
        }
    }

    #[test]
    fn a_policy_needs_registries_in_its_patterns_and_pem_keys() {
        use crate::admit::Spec;
        let key = "-----BEGIN PUBLIC KEY-----\nMFk...\n-----END PUBLIC KEY-----\n";
        let policy = |images: &[&str], keys: &[&str]| ImagePolicySpec {
            images: images.iter().map(|s| s.to_string()).collect(),
            keys: keys.iter().map(|s| s.to_string()).collect(),
            mode: PolicyMode::Enforce,
        };
        assert!(
            policy(&["ghcr.io/me/*", "localhost/**"], &[key])
                .admit()
                .is_ok()
        );
        for (bad, why) in [
            ("me/app", "registry"),
            ("ghcr.io", "names no repository"),
            ("ghcr.io/me/app:1", "bad segment"),
            ("ghcr.io/me/app@sha256:aa", "bad segment"),
            ("ghcr.io/Me/app", "bad segment"),
            ("ghcr.io/me//app", "bad segment"),
            ("ghcr.io/a**b", "bad segment"),
        ] {
            let e = policy(&[bad], &[key]).admit().unwrap_err();
            assert_eq!(e.path, "images[0]");
            assert!(e.message.contains(why), "{bad}: {}", e.message);
        }
        assert_eq!(policy(&[], &[key]).admit().unwrap_err().path, "images");
        assert_eq!(
            policy(&["ghcr.io/**"], &[]).admit().unwrap_err().path,
            "keys"
        );
        let e = policy(&["ghcr.io/**"], &["ssh-ed25519 AAAA"])
            .admit()
            .unwrap_err();
        assert_eq!(e.path, "keys[0]");
        let warn: ImagePolicySpec =
            serde_json::from_str(r#"{"images":["ghcr.io/**"],"keys":[],"mode":"warn"}"#).unwrap();
        assert_eq!(warn.mode, PolicyMode::Warn);
        assert!(
            serde_json::from_str::<ImagePolicySpec>(r#"{"images":[],"keys":[],"mode":"audit"}"#)
                .is_err()
        );
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
