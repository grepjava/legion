//! Attestations as cosign makes them with a key pair (`cosign attest`, and
//! jailer's `attest`, which Legion's builds run): an in-toto statement
//! naming an image by digest, with a predicate (an SBOM, SLSA provenance),
//! signed in a DSSE envelope with ECDSA P-256, kept as a layer of the
//! manifest tagged `sha256-<hex>.att` in the image's repository, beside
//! the signatures of [`crate::cosign`]. Here they are read and checked;
//! jailer makes them.
//!
//! Not covered, as for signatures: cosign v3's bundles (OCI 1.1
//! referrers), the transparency log and keyless signing.

use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::Deserialize;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

use crate::cosign::{PublicKey, VerifyError};
use crate::{Client, Credentials, Error, Reference};

/// A layer's media type: a DSSE envelope.
pub const ENVELOPE_TYPE: &str = "application/vnd.dsse.envelope.v1+json";
/// What a DSSE envelope's payload is: an in-toto statement.
pub const PAYLOAD_TYPE: &str = "application/vnd.in-toto+json";
/// The layer annotation naming the statement's predicate type.
pub const PREDICATE_ANNOTATION: &str = "predicateType";
/// The statement written: in-toto's v1. v0.1, which older cosign writes,
/// is read too.
pub const STATEMENT_TYPE: &str = "https://in-toto.io/Statement/v1";
const STATEMENT_TYPES: [&str; 2] = [STATEMENT_TYPE, "https://in-toto.io/Statement/v0.1"];

pub use legion_api::image::{
    CYCLONEDX, SLSA_PROVENANCE_1, SLSA_PROVENANCE_02, SPDX, VULN, predicate_type,
};

/// The most an attestation layer may hold; SBOMs of large images run to a
/// few megabytes.
pub const MAX_LAYER: u64 = 32 << 20;

/// Whether `t` is a kind of SBOM, which `legion image sbom` shows.
pub fn is_sbom(t: &str) -> bool {
    t == SPDX || t == CYCLONEDX
}

/// Whether `t` is SLSA provenance, of any version.
pub fn is_provenance(t: &str) -> bool {
    t.starts_with("https://slsa.dev/provenance/")
}

/// The reference attestations of `digest` in `r`'s repository are kept
/// under: `<repository>:sha256-<hex>.att`.
pub fn attestation_reference(r: &Reference, digest: &str) -> Reference {
    Reference {
        tag: Some(format!("{}.att", digest.replacen(':', "-", 1))),
        digest: None,
        ..r.clone()
    }
}

/// DSSE's pre-authentication encoding of `body`: what is signed.
pub fn pae(payload_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "DSSEv1 {} {} {} ",
        payload_type.len(),
        payload_type,
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope {
    payload_type: String,
    /// Base64 of the statement.
    payload: String,
    signatures: Vec<EnvelopeSignature>,
}

#[derive(Debug, Clone, Deserialize)]
struct EnvelopeSignature {
    /// Base64 (standard) of the DER signature over [`pae`].
    sig: String,
}

/// What an attestation states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub predicate_type: String,
    pub predicate: Value,
}

/// Reads an envelope's statement, without checking who signed it.
pub fn read(envelope: &[u8]) -> Result<Statement, String> {
    let env: Envelope =
        serde_json::from_slice(envelope).map_err(|_| "not a DSSE envelope".to_owned())?;
    if env.payload_type != PAYLOAD_TYPE {
        return Err(format!(
            "its payload is {}, not an in-toto statement",
            env.payload_type
        ));
    }
    let body = B64
        .decode(env.payload.trim())
        .map_err(|_| "its payload is not base64".to_owned())?;
    let doc: Value =
        serde_json::from_slice(&body).map_err(|_| "its statement is not JSON".to_owned())?;
    if !STATEMENT_TYPES.contains(&doc["_type"].as_str().unwrap_or_default()) {
        return Err(format!("not an in-toto statement (_type {})", doc["_type"]));
    }
    Ok(Statement {
        predicate_type: doc["predicateType"].as_str().unwrap_or_default().to_owned(),
        predicate: doc["predicate"].clone(),
    })
}

/// Whether `envelope` holds a statement about the image `digest`, signed
/// by one of `keys`. Returns the index of the key that verified and what
/// it states, or says which check failed.
pub fn check(
    envelope: &[u8],
    keys: &[PublicKey],
    digest: &str,
) -> Result<(usize, Statement), String> {
    let env: Envelope =
        serde_json::from_slice(envelope).map_err(|_| "not a DSSE envelope".to_owned())?;
    let body = B64
        .decode(env.payload.trim())
        .map_err(|_| "its payload is not base64".to_owned())?;
    let signed = pae(&env.payload_type, &body);
    let Some(index) = keys
        .iter()
        .position(|k| env.signatures.iter().any(|s| k.verifies(&signed, &s.sig)))
    else {
        return Err("made by none of the keys given".to_owned());
    };
    let statement = read(envelope)?;
    let doc: Value = serde_json::from_slice(&body).map_err(|_| "not JSON".to_owned())?;
    let (algorithm, hex) = digest.split_once(':').unwrap_or(("sha256", digest));
    let about = doc["subject"]
        .as_array()
        .is_some_and(|s| s.iter().any(|s| s["digest"][algorithm] == hex));
    if !about {
        return Err(format!("it is about another image, not {digest}"));
    }
    Ok((index, statement))
}

/// An attestation found for an image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub predicate_type: String,
    pub predicate: Value,
    /// The fingerprint of the key of those given that signed it.
    pub verified: Option<String>,
    /// Why it isn't verified, when keys were given.
    pub why: Option<String>,
}

/// The attestations kept for the image `digest` in `r`'s repository, each
/// checked against `keys` (none given: none verified). Layers that are not
/// envelopes are passed over; an envelope that can't be read says why.
pub async fn fetch(
    client: &Client,
    r: &Reference,
    digest: &str,
    keys: &[PublicKey],
    creds: Option<&Credentials>,
) -> Result<Vec<Found>, VerifyError> {
    let fetch = |e: Error| VerifyError::CannotFetch(e.to_string());
    let at = attestation_reference(r, digest);
    let layers = match client.document(&at, at.target(), creds).await {
        Ok((_, doc)) => doc.into_manifest().map_err(fetch)?.layers,
        Err(Error::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(fetch(e)),
    };
    let tmp = tempfile::tempdir().map_err(|e| VerifyError::CannotFetch(e.to_string()))?;
    let mut found = Vec::new();
    for (i, layer) in layers.iter().enumerate() {
        if layer.media_type != ENVELOPE_TYPE {
            continue;
        }
        if layer.size > MAX_LAYER {
            found.push(Found {
                predicate_type: layer
                    .annotations
                    .get(PREDICATE_ANNOTATION)
                    .cloned()
                    .unwrap_or_default(),
                predicate: Value::Null,
                verified: None,
                why: Some(format!("{} bytes, past {MAX_LAYER}", layer.size)),
            });
            continue;
        }
        let path: PathBuf = tmp.path().join(i.to_string());
        client
            .blob(&at, &layer.digest, layer.size, &path, creds)
            .await
            .map_err(fetch)?;
        let bytes = std::fs::read(&path).map_err(|e| VerifyError::CannotFetch(e.to_string()))?;
        let read = read(&bytes);
        let checked = (!keys.is_empty()).then(|| check(&bytes, keys, digest));
        let (verified, why) = match checked {
            Some(Ok((k, _))) => (Some(keys[k].fingerprint().to_owned()), None),
            Some(Err(why)) => (None, Some(why)),
            None => (None, read.as_ref().err().cloned()),
        };
        let (predicate_type, predicate) = match read {
            Ok(s) => (s.predicate_type, s.predicate),
            Err(_) => (
                layer
                    .annotations
                    .get(PREDICATE_ANNOTATION)
                    .cloned()
                    .unwrap_or_default(),
                Value::Null,
            ),
        };
        found.push(Found {
            predicate_type,
            predicate,
            verified,
            why,
        });
    }
    Ok(found)
}

/// Of `required` predicate types, the first `found` has none verified for,
/// saying why the last of that type was not.
pub fn missing<'a>(found: &[Found], required: &'a [String]) -> Option<(&'a str, String)> {
    required.iter().find_map(|t| {
        let of: Vec<&Found> = found.iter().filter(|f| &f.predicate_type == t).collect();
        if of.iter().any(|f| f.verified.is_some()) {
            return None;
        }
        let why = of
            .iter()
            .rev()
            .find_map(|f| f.why.clone())
            .unwrap_or_else(|| "none is kept".to_owned());
        Some((t.as_str(), why))
    })
}

/// What an SBOM attestation lists: each package's name and version, from
/// SPDX's `packages` or CycloneDX's `components`; the image itself left out.
pub fn sbom_packages(predicate_type: &str, predicate: &Value) -> Vec<(String, String)> {
    let (list, version) = match predicate_type {
        SPDX => (&predicate["packages"], "versionInfo"),
        CYCLONEDX => (&predicate["components"], "version"),
        _ => return Vec::new(),
    };
    list.as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["SPDXID"] != "SPDXRef-Image" && p["primaryPackagePurpose"] != "CONTAINER")
        .filter_map(|p| {
            Some((
                p["name"].as_str()?.to_owned(),
                p[version].as_str().unwrap_or_default().to_owned(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cosign::{PrivateKey, sign};

    const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// An attestation of `predicate` about `digest`, as jailer makes one.
    fn envelope(digest: &str, predicate_type: &str, predicate: Value, key: &PrivateKey) -> Vec<u8> {
        let statement = serde_json::to_vec(&json!({
            "_type": STATEMENT_TYPE,
            "subject": [{
                "name": "registry.lan/shop/web",
                "digest": {"sha256": digest.trim_start_matches("sha256:")},
            }],
            "predicateType": predicate_type,
            "predicate": predicate,
        }))
        .unwrap();
        serde_json::to_vec(&json!({
            "payloadType": PAYLOAD_TYPE,
            "payload": B64.encode(&statement),
            "signatures": [{"keyid": "", "sig": sign(&pae(PAYLOAD_TYPE, &statement), key)}],
        }))
        .unwrap()
    }

    #[test]
    fn names_stand_for_predicate_types_as_cosigns_do() {
        assert_eq!(predicate_type("spdxjson"), Some(SPDX));
        assert_eq!(predicate_type("slsaprovenance"), Some(SLSA_PROVENANCE_02));
        assert_eq!(predicate_type("slsaprovenance1"), Some(SLSA_PROVENANCE_1));
        assert_eq!(
            predicate_type("https://example.com/x/v1"),
            Some("https://example.com/x/v1")
        );
        assert_eq!(predicate_type("sbom"), None);
        assert!(is_provenance(SLSA_PROVENANCE_1) && is_sbom(CYCLONEDX));
    }

    #[test]
    fn dsse_encodes_what_it_signs_with_lengths() {
        assert_eq!(pae("a/b", b"hello"), b"DSSEv1 3 a/b 5 hello");
    }

    #[test]
    fn an_envelope_checks_with_its_key_for_its_image_only() {
        let key = PrivateKey::generate();
        let other = PrivateKey::generate();
        let env = envelope(DIGEST, SPDX, json!({"packages": []}), &key);
        let (k, st) = check(&env, &[other.public(), key.public()], DIGEST).unwrap();
        assert_eq!(k, 1);
        assert_eq!(st.predicate_type, SPDX);
        assert_eq!(
            check(&env, &[other.public()], DIGEST).unwrap_err(),
            "made by none of the keys given"
        );
        let elsewhere = DIGEST.replace('a', "b");
        assert!(
            check(&env, &[key.public()], &elsewhere)
                .unwrap_err()
                .contains("another image")
        );
        // A statement changed after signing is no longer the key's.
        let mut tampered: Value = serde_json::from_slice(&env).unwrap();
        let changed = envelope(&elsewhere, SPDX, json!({}), &other);
        let changed: Value = serde_json::from_slice(&changed).unwrap();
        tampered["payload"] = changed["payload"].clone();
        let tampered = serde_json::to_vec(&tampered).unwrap();
        assert!(check(&tampered, &[key.public()], &elsewhere).is_err());
        // Read without keys, as `legion image inspect` lists them.
        assert_eq!(read(&env).unwrap().predicate_type, SPDX);
        assert!(read(b"{}").is_err());
    }

    #[test]
    fn what_is_required_and_not_verified_is_named() {
        let f = |t: &str, ok: bool| Found {
            predicate_type: t.to_owned(),
            predicate: Value::Null,
            verified: ok.then(|| "sha256:k".to_owned()),
            why: (!ok).then(|| "made by none of the keys given".to_owned()),
        };
        let required = [SPDX.to_owned(), SLSA_PROVENANCE_1.to_owned()];
        assert_eq!(
            missing(&[f(SPDX, true), f(SLSA_PROVENANCE_1, true)], &required),
            None
        );
        assert_eq!(
            missing(&[f(SPDX, true)], &required),
            Some((SLSA_PROVENANCE_1, "none is kept".to_owned()))
        );
        assert_eq!(
            missing(
                &[f(SPDX, false), f(SPDX, true), f(SLSA_PROVENANCE_1, false)],
                &required
            ),
            Some((
                SLSA_PROVENANCE_1,
                "made by none of the keys given".to_owned()
            ))
        );
    }

    #[test]
    fn an_sbom_lists_the_images_packages() {
        // As jailer writes one: the image first, then its packages.
        let spdx = json!({
            "spdxVersion": "SPDX-2.3",
            "packages": [
                {"SPDXID": "SPDXRef-Image", "name": "web", "versionInfo": DIGEST},
                {"SPDXID": "SPDXRef-Package-0", "name": "nginx", "versionInfo": "1.26.2"},
                {"SPDXID": "SPDXRef-Package-1", "name": "pcre2", "versionInfo": "10.45"},
            ],
        });
        let pkgs = [
            ("nginx".to_owned(), "1.26.2".to_owned()),
            ("pcre2".to_owned(), "10.45".to_owned()),
        ];
        assert_eq!(sbom_packages(SPDX, &spdx), pkgs);
        let cdx = json!({"components": [{"name": "a", "version": "1"}]});
        assert_eq!(
            sbom_packages(CYCLONEDX, &cdx),
            [("a".to_owned(), "1".to_owned())]
        );
        assert!(sbom_packages(SLSA_PROVENANCE_1, &spdx).is_empty());
    }
}
