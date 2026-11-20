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
    /// Each reference as written whose tag names another image now than
    /// the one its tasks run, and the digest it names: the tag moved since
    /// the rollout (CONSOLE.md, phase 12). A restart runs it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub moved: BTreeMap<String, String>,
}

/// A good signature on an image: the digest it signs and the key's
/// fingerprint (`sha256:` of its DER SubjectPublicKeyInfo).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verification {
    pub digest: String,
    pub key: String,
    /// The predicate types of the attestations its policies required,
    /// each found signed by one of their keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attestations: Vec<String>,
}

/// In-toto predicate types of the attestations cosign makes.
pub const SPDX: &str = "https://spdx.dev/Document";
pub const CYCLONEDX: &str = "https://cyclonedx.org/bom";
pub const SLSA_PROVENANCE_02: &str = "https://slsa.dev/provenance/v0.2";
pub const SLSA_PROVENANCE_1: &str = "https://slsa.dev/provenance/v1";
pub const VULN: &str = "https://cosign.sigstore.dev/attestation/vuln/v1";

/// The predicate type `name` stands for: cosign's `--type` names
/// (`spdx`, `spdxjson`, `cyclonedx`, `slsaprovenance`, `slsaprovenance02`,
/// `slsaprovenance1`, `vuln`) or an `https://` URI itself.
pub fn predicate_type(name: &str) -> Option<&str> {
    Some(match name {
        "spdx" | "spdxjson" => SPDX,
        "cyclonedx" => CYCLONEDX,
        "slsaprovenance" | "slsaprovenance02" => SLSA_PROVENANCE_02,
        "slsaprovenance1" => SLSA_PROVENANCE_1,
        "vuln" => VULN,
        uri if uri.starts_with("https://") && uri.len() > "https://".len() => uri,
        _ => return None,
    })
}

/// Spec of an `ImagePolicy` (cluster-scoped): images whose repository
/// matches one of `images` run only with a good signature, made as cosign
/// makes them, by one of `keys`, and with an attestation of each of
/// `attestations` signed by one of them. Every policy an image matches
/// must be met.
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
    /// Predicate types each image must also carry an attestation of,
    /// signed by one of `keys` (`cosign attest --key`): `spdx`,
    /// `cyclonedx`, `slsaprovenance1`… or a URI; kept as URIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attestations: Vec<String>,
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
        for (i, a) in self.attestations.iter_mut().enumerate() {
            let Some(t) = predicate_type(a) else {
                return Err(Invalid::new(
                    index("", "attestations", i),
                    "spdx, cyclonedx, slsaprovenance, slsaprovenance1, vuln or an https:// URI",
                ));
            };
            *a = t.to_owned();
        }
        self.attestations.sort();
        self.attestations.dedup();
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

/// The native recipe (`examples/<name>`, a template in the console) that
/// runs from FreeBSD's packages what the Linux image `image` runs, by the
/// image's name: `docker.io/library/postgres:17` is `postgres`.
pub fn native_recipe(image: &str) -> Option<&'static str> {
    let name = image.split('@').next().unwrap_or(image);
    let name = name.rsplit('/').next().unwrap_or(name);
    let name = name.split(':').next().unwrap_or(name);
    Some(match name {
        "postgres" | "postgresql" => "postgres",
        "redis" | "valkey" => "redis",
        "nginx" => "static-site",
        "node" => "node",
        "python" => "python",
        _ => return None,
    })
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
    fn a_linux_image_names_its_native_recipe() {
        for (image, recipe) in [
            ("postgres:17-alpine", Some("postgres")),
            ("docker.io/library/postgres@sha256:00", Some("postgres")),
            ("ghcr.io/x/redis:7", Some("redis")),
            ("valkey/valkey:8", Some("redis")),
            ("nginx:1.27-alpine", Some("static-site")),
            ("node:22-alpine", Some("node")),
            ("python:3.12-slim", Some("python")),
            ("localhost:5000/team/python", Some("python")),
            ("alpine:3.20", None),
            ("ubuntu:24.04", None),
        ] {
            assert_eq!(native_recipe(image), recipe, "{image}");
        }
    }

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
            attestations: Vec::new(),
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
    fn a_policy_names_the_attestations_it_requires_as_uris() {
        use crate::admit::Spec;
        let key = "-----BEGIN PUBLIC KEY-----\nMFk...\n-----END PUBLIC KEY-----\n";
        let mut p = ImagePolicySpec {
            images: vec!["ghcr.io/**".into()],
            keys: vec![key.into()],
            mode: PolicyMode::Enforce,
            attestations: vec![
                "spdxjson".into(),
                "slsaprovenance1".into(),
                SPDX.into(),
                "https://example.com/attest/v1".into(),
            ],
        };
        p.admit().unwrap();
        assert_eq!(
            p.attestations,
            ["https://example.com/attest/v1", SLSA_PROVENANCE_1, SPDX]
        );
        for bad in ["sbom", "http://example.com/x", "https://"] {
            p.attestations = vec![bad.into()];
            let e = p.admit().unwrap_err();
            assert_eq!(e.path, "attestations[0]", "{bad}");
        }
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
