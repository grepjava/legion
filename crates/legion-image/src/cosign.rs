//! Image signatures as cosign makes them with a key pair (PLAN.md, phase 7):
//! a "simple signing" payload naming an image's repository and the digest
//! of its manifest or index, signed with ECDSA P-256, kept as a layer of
//! the manifest tagged `sha256-<hex>.sig` in the image's repository, its
//! signature (base64 of the DER encoding) in an annotation. One crate makes
//! and checks them: jailer signs with it and Legion verifies with it.
//!
//! Not covered: cosign v3's bundles (OCI 1.1 referrers), the transparency
//! log, keyless signing and cosign's encrypted private keys.

use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::pkcs8::LineEnding;
use p256::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use serde_json::json;
use sha2::Digest;

use crate::{Client, Credentials, Error, Reference};

pub const PAYLOAD_TYPE: &str = "application/vnd.dev.cosign.simplesigning.v1+json";
pub const SIGNATURE_ANNOTATION: &str = "dev.cosignproject.cosign/signature";
pub const SIGNATURE_TYPE: &str = "cosign container image signature";

/// A key that did not parse, saying why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError(pub String);

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for KeyError {}

/// A P-256 public key, read from PKIX PEM (`PUBLIC KEY`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicKey {
    key: VerifyingKey,
    fingerprint: String,
}

impl PublicKey {
    pub fn from_pem(pem: &str) -> Result<PublicKey, KeyError> {
        let public = p256::PublicKey::from_public_key_pem(pem.trim())
            .map_err(|e| KeyError(format!("not a P-256 public key: {e}")))?;
        Ok(PublicKey::from(public))
    }

    /// `sha256:<hex>` of the key's DER SubjectPublicKeyInfo.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn to_pem(&self) -> String {
        p256::PublicKey::from(&self.key)
            .to_public_key_pem(LineEnding::LF)
            .expect("a P-256 key writes as PEM")
    }
}

impl From<p256::PublicKey> for PublicKey {
    fn from(public: p256::PublicKey) -> PublicKey {
        let der = public
            .to_public_key_der()
            .expect("a P-256 key writes as DER");
        let hex: String = sha2::Sha256::digest(der.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        PublicKey {
            key: VerifyingKey::from(&public),
            fingerprint: format!("sha256:{hex}"),
        }
    }
}

/// A P-256 private key: read from PKCS#8 (`PRIVATE KEY`) or SEC1
/// (`EC PRIVATE KEY`) PEM, unencrypted; written as PKCS#8.
#[derive(Clone)]
pub struct PrivateKey {
    secret: p256::SecretKey,
}

impl std::fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PrivateKey({})", self.public().fingerprint())
    }
}

impl PrivateKey {
    pub fn from_pem(pem: &str) -> Result<PrivateKey, KeyError> {
        let pem = pem.trim();
        let secret = p256::SecretKey::from_pkcs8_pem(pem)
            .or_else(|_| p256::SecretKey::from_sec1_pem(pem))
            .map_err(|e| KeyError(format!("not an unencrypted P-256 private key: {e}")))?;
        Ok(PrivateKey { secret })
    }

    pub fn generate() -> PrivateKey {
        loop {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes).expect("random bytes");
            if let Ok(secret) = p256::SecretKey::from_slice(&bytes) {
                return PrivateKey { secret };
            }
        }
    }

    pub fn to_pem(&self) -> String {
        self.secret
            .to_pkcs8_pem(LineEnding::LF)
            .expect("a P-256 key writes as PEM")
            .to_string()
    }

    pub fn public(&self) -> PublicKey {
        PublicKey::from(self.secret.public_key())
    }
}

/// What is signed: `repository`'s name (its registry and repository) and
/// the digest of the manifest or index signed.
pub fn payload(repository: &Reference, digest: &str) -> Vec<u8> {
    let doc = json!({
        "critical": {
            "identity": {
                "docker-reference": format!("{}/{}", repository.registry, repository.repository),
            },
            "image": {"docker-manifest-digest": digest},
            "type": SIGNATURE_TYPE,
        },
        "optional": null,
    });
    serde_json::to_vec(&doc).expect("JSON")
}

/// The reference the signatures of `digest` in `r`'s repository are kept
/// under: `<repository>:sha256-<hex>.sig`.
pub fn signature_reference(r: &Reference, digest: &str) -> Reference {
    Reference {
        tag: Some(format!("{}.sig", digest.replacen(':', "-", 1))),
        digest: None,
        ..r.clone()
    }
}

/// `payload` signed with `key`: base64 (standard) of the DER signature.
pub fn sign(payload: &[u8], key: &PrivateKey) -> String {
    let signature: Signature = SigningKey::from(&key.secret).sign(payload);
    B64.encode(signature.to_der().as_bytes())
}

/// Docker Hub's names: `index.docker.io` is `docker.io`.
fn same_registry(a: &str, b: &str) -> bool {
    let hub = |r: &str| {
        if r == "index.docker.io" {
            "docker.io"
        } else {
            r
        }
        .to_owned()
    };
    hub(a) == hub(b)
}

/// Whether `payload` (its exact bytes) and `signature` are a signature, by
/// one of `keys`, of the image `digest` in `repository`. Returns the index of
/// the key that verified, or says which check failed.
pub fn check(
    payload: &[u8],
    signature: &str,
    keys: &[PublicKey],
    digest: &str,
    repository: &Reference,
) -> Result<usize, String> {
    let der = B64
        .decode(signature.trim())
        .map_err(|_| "the signature is not base64".to_owned())?;
    let signature =
        Signature::from_der(&der).map_err(|_| "the signature is not a DER ECDSA signature")?;
    let Some(index) = keys
        .iter()
        .position(|k| k.key.verify(payload, &signature).is_ok())
    else {
        return Err("no key of the policy made it".to_owned());
    };
    let signed: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "the payload is not JSON".to_owned())?;
    let critical = &signed["critical"];
    if critical["type"] != SIGNATURE_TYPE {
        return Err(format!(
            "the payload is not an image signature (type {})",
            critical["type"]
        ));
    }
    if critical["image"]["docker-manifest-digest"] != digest {
        return Err(format!(
            "it signs {}, not {digest}",
            critical["image"]["docker-manifest-digest"]
        ));
    }
    let named = critical["identity"]["docker-reference"]
        .as_str()
        .unwrap_or_default();
    let same = Reference::parse(named).is_ok_and(|n| {
        same_registry(&n.registry, &repository.registry) && n.repository == repository.repository
    });
    if !same {
        return Err(format!(
            "it signs the image in {named:?}, not {}/{}",
            repository.registry, repository.repository
        ));
    }
    Ok(index)
}

/// A good signature, and by which key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub fingerprint: String,
}

/// Why an image's signature was not found good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// No signatures are kept for it.
    Unsigned,
    /// They could not be read: the registry did not answer, refused, or
    /// sent something else.
    CannotFetch(String),
    /// Signatures are there, none good: why the last one was not.
    Bad(String),
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyError::Unsigned => f.write_str("unsigned"),
            VerifyError::CannotFetch(e) => write!(f, "cannot check the signature: {e}"),
            VerifyError::Bad(e) => write!(f, "bad signature: {e}"),
        }
    }
}

impl std::error::Error for VerifyError {}

/// Checks that the image `digest` in `r`'s repository carries a good
/// signature by one of `keys`. `r` is not resolved: the digest is the one
/// to be run. Every signature layer is tried; one without a well-formed
/// signature annotation is passed over.
pub async fn verify(
    client: &Client,
    r: &Reference,
    digest: &str,
    keys: &[PublicKey],
    creds: Option<&Credentials>,
) -> Result<Verified, VerifyError> {
    let fetch = |e: Error| VerifyError::CannotFetch(e.to_string());
    let at = signature_reference(r, digest);
    let layers = match client.document(&at, at.target(), creds).await {
        Ok((_, doc)) => doc.into_manifest().map_err(fetch)?.layers,
        Err(Error::NotFound(_)) => return Err(VerifyError::Unsigned),
        Err(e) => return Err(fetch(e)),
    };
    let tmp = tempfile::tempdir().map_err(|e| VerifyError::CannotFetch(e.to_string()))?;
    let mut last = "no layer of it holds a signature".to_owned();
    for (i, layer) in layers.iter().enumerate() {
        let Some(signature) = layer.annotations.get(SIGNATURE_ANNOTATION) else {
            continue;
        };
        let well_formed = B64
            .decode(signature.trim())
            .ok()
            .is_some_and(|der| Signature::from_der(&der).is_ok());
        if !well_formed {
            continue;
        }
        let path: PathBuf = tmp.path().join(i.to_string());
        client
            .blob(&at, &layer.digest, layer.size, &path, creds)
            .await
            .map_err(fetch)?;
        let payload = std::fs::read(&path).map_err(|e| VerifyError::CannotFetch(e.to_string()))?;
        match check(&payload, signature, keys, digest, r) {
            Ok(k) => {
                return Ok(Verified {
                    fingerprint: keys[k].fingerprint().to_owned(),
                });
            }
            Err(why) => last = why,
        }
    }
    Err(VerifyError::Bad(last))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn keys_are_read_and_written_as_pem_with_a_stable_fingerprint() {
        let private = PrivateKey::generate();
        let pem = private.to_pem();
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----"), "{pem}");
        let again = PrivateKey::from_pem(&pem).unwrap();
        assert_eq!(again.public(), private.public());
        let public = private.public().to_pem();
        assert!(public.starts_with("-----BEGIN PUBLIC KEY-----"), "{public}");
        let read = PublicKey::from_pem(&public).unwrap();
        assert_eq!(read.fingerprint(), private.public().fingerprint());
        assert!(read.fingerprint().starts_with("sha256:"));
        assert_eq!(read.fingerprint().len(), "sha256:".len() + 64);
        assert_ne!(
            PrivateKey::generate().public().fingerprint(),
            read.fingerprint()
        );
        // Each kind of key is refused where the other is wanted.
        assert!(PublicKey::from_pem(&pem).is_err());
        assert!(PrivateKey::from_pem(&public).is_err());
        assert!(PublicKey::from_pem("not a key").is_err());
    }

    #[test]
    fn a_sec1_private_key_is_read() {
        let private = PrivateKey::generate();
        let sec1 = private.secret.to_sec1_pem(LineEnding::LF).unwrap();
        assert!(sec1.starts_with("-----BEGIN EC PRIVATE KEY-----"));
        assert_eq!(
            PrivateKey::from_pem(&sec1).unwrap().public(),
            private.public()
        );
    }

    #[test]
    fn a_signature_checks_against_its_key_digest_and_repository() {
        let r = Reference::parse("ghcr.io/me/app:1").unwrap();
        let key = PrivateKey::generate();
        let other = PrivateKey::generate();
        let payload = payload(&r, DIGEST);
        let text = String::from_utf8(payload.clone()).unwrap();
        assert!(
            text.contains(r#""docker-reference":"ghcr.io/me/app""#),
            "{text}"
        );
        let sig = sign(&payload, &key);
        let keys = [other.public(), key.public()];
        assert_eq!(check(&payload, &sig, &keys, DIGEST, &r), Ok(1));

        let err = |p: &[u8], s: &str, keys: &[PublicKey], d: &str, r: &Reference| {
            check(p, s, keys, d, r).unwrap_err()
        };
        assert!(err(&payload, &sig, &[other.public()], DIGEST, &r).contains("no key"));
        assert!(err(&payload, "!!", &keys, DIGEST, &r).contains("base64"));
        assert!(err(&payload, "aGVsbG8=", &keys, DIGEST, &r).contains("DER"));
        let other_digest = DIGEST.replace('a', "b");
        assert!(err(&payload, &sig, &keys, &other_digest, &r).contains("it signs"));
        let mirror = Reference::parse("mirror.example/me/app:1").unwrap();
        assert!(err(&payload, &sig, &keys, DIGEST, &mirror).contains("ghcr.io/me/app"));
        // The bytes signed, not the JSON they hold: spaces added break it.
        let spaced = text.replace(':', ": ").into_bytes();
        assert!(err(&spaced, &sig, &keys, DIGEST, &r).contains("no key"));
        // Something else the key signed is not an image signature.
        let note = br#"{"critical":{"type":"a note"}}"#;
        let signed_note = sign(note, &key);
        assert!(err(note, &signed_note, &keys, DIGEST, &r).contains("not an image signature"));
        assert_eq!(
            signature_reference(&r, DIGEST).to_string(),
            format!("ghcr.io/me/app:{}.sig", DIGEST.replacen(':', "-", 1))
        );
    }

    #[test]
    fn docker_hub_is_one_registry_however_it_is_named() {
        let key = PrivateKey::generate();
        let r = Reference::parse("nginx:1").unwrap();
        // As cosign writes a Hub image's name.
        let payload = serde_json::to_vec(&json!({
            "critical": {
                "identity": {"docker-reference": "index.docker.io/library/nginx"},
                "image": {"docker-manifest-digest": DIGEST},
                "type": SIGNATURE_TYPE,
            },
            "optional": null,
        }))
        .unwrap();
        let sig = sign(&payload, &key);
        assert_eq!(check(&payload, &sig, &[key.public()], DIGEST, &r), Ok(0));
    }
}
