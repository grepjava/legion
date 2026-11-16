//! Attestations about an image, as cosign makes them with a key pair: an
//! in-toto statement in a DSSE envelope signed with ECDSA P-256, pushed as a
//! layer of the manifest tagged `sha256-<hex>.att` in the image's repository.
//! `cosign verify-attestation --key` checks them (with
//! `--insecure-ignore-tlog`, as no transparency log entry is made), and so
//! do `legion local verify --type` and Legion's image policies: reading and
//! checking them is `legion_image::attest`'s, so both sides make and read the
//! same bytes. Making and pushing them is the builder's. Provenance is
//! SLSA's, from a release record, and an SBOM SPDX's, from the image's pkg(8)
//! database.

use std::collections::BTreeMap;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use legion_image::attest::{
    ENVELOPE_TYPE, PAYLOAD_TYPE, PREDICATE_ANNOTATION, STATEMENT_TYPE, attestation_reference,
    fetch, pae,
};
use legion_image::cosign;
use serde_json::{Value, json};

use crate::record::Record;
use crate::registry::{Client, Credentials};
use crate::sign::{PrivateKey, PublicKey, SIGNATURE_ANNOTATION, add_layer};
use crate::{Error, Reference};

pub use legion_image::attest::{SLSA_PROVENANCE_1 as PROVENANCE, SPDX as SBOM};

const BUILD_TYPE: &str = "https://github.com/grepjava/legion/blob/main/docs/RELEASE-RECORD.md";
const BUILDER: &str = "https://github.com/grepjava/legion";

/// The tag a manifest's attestations are kept under.
pub fn attestation_ref(r: &Reference, digest: &str) -> Reference {
    attestation_reference(r, digest)
}

fn hex_of(digest: &str) -> &str {
    digest.strip_prefix("sha256:").unwrap_or(digest)
}

/// `predicate` about the manifest `digest` of `r`.
fn statement(r: &Reference, digest: &str, predicate_type: &str, predicate: &Value) -> Value {
    json!({
        "_type": STATEMENT_TYPE,
        "subject": [{
            "name": format!("{}/{}", r.registry, r.repository),
            "digest": {"sha256": hex_of(digest)},
        }],
        "predicateType": predicate_type,
        "predicate": predicate,
    })
}

/// A signed envelope of `statement`.
fn envelope(statement: &Value, key: &PrivateKey) -> Result<Vec<u8>, Error> {
    let payload = serde_json::to_vec(statement)?;
    let signature = cosign::sign(&pae(PAYLOAD_TYPE, &payload), key);
    Ok(serde_json::to_vec(&json!({
        "payloadType": PAYLOAD_TYPE,
        "payload": B64.encode(&payload),
        "signatures": [{"keyid": "", "sig": signature}],
    }))?)
}

/// Attests `predicate` about the manifest `digest` in `r`'s repository,
/// adding to the attestations already there.
pub async fn attest(
    client: &Client,
    r: &Reference,
    digest: &str,
    predicate_type: &str,
    predicate: &Value,
    key: &PrivateKey,
    creds: Option<&Credentials>,
) -> Result<(), Error> {
    let body = envelope(&statement(r, digest, predicate_type, predicate), key)?;
    let annotations = BTreeMap::from([
        (SIGNATURE_ANNOTATION.to_owned(), String::new()),
        (PREDICATE_ANNOTATION.to_owned(), predicate_type.to_owned()),
    ]);
    let at = attestation_ref(r, digest);
    add_layer(client, r, &at, ENVELOPE_TYPE, &body, annotations, creds).await
}

/// The manifest digest of `r`, and the predicate of `predicate_type` about
/// it that `key` signed: what an image policy requiring it would accept.
pub async fn verify(
    client: &Client,
    r: &Reference,
    predicate_type: &str,
    key: &PublicKey,
    creds: Option<&Credentials>,
) -> Result<(String, Value), Error> {
    let (digest, _) = client.document(r, r.target(), creds).await?;
    let found = fetch(client, r, &digest, std::slice::from_ref(key), creds)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;
    found
        .into_iter()
        .find(|f| f.predicate_type == predicate_type && f.verified.is_some())
        .map(|f| (digest.clone(), f.predicate))
        .ok_or_else(|| {
            Error::NotFound(format!(
                "{r} ({digest}) has no such attestation by this key"
            ))
        })
}

/// SLSA provenance from `record`: what was built from what, by digest. Build
/// arguments are left out: they may hold what is not to be published.
pub fn provenance(record: &Record) -> Result<Value, Error> {
    let source = record.source.as_ref().ok_or_else(|| {
        Error::Runtime("the record has no build: give the one `build --record` wrote".to_owned())
    })?;
    let dependencies: Vec<Value> = record
        .bases
        .iter()
        .map(|b| {
            json!({
                "name": b.reference,
                "digest": {"sha256": hex_of(&b.manifest)},
            })
        })
        .collect();
    Ok(json!({
        "buildDefinition": {
            "buildType": BUILD_TYPE,
            "externalParameters": {
                "containerfile": source.containerfile,
                "context": source.context,
            },
            "resolvedDependencies": dependencies,
        },
        "runDetails": {
            "builder": {"id": BUILDER, "version": {"legion": env!("CARGO_PKG_VERSION")}},
        },
    }))
}

/// A package pkg(8) has registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Package {
    pub name: String,
    pub version: String,
    pub origin: String,
}

/// Rows of name, version and origin, split by tabs.
fn parse_packages(text: &str) -> Vec<Package> {
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            Some(Package {
                name: fields.next().filter(|n| !n.is_empty())?.to_owned(),
                version: fields.next()?.to_owned(),
                origin: fields.next().unwrap_or("").to_owned(),
            })
        })
        .collect()
}

/// The packages registered in the root filesystem at `root`, or `None` if it
/// has no pkg(8) database to tell. `pkg query` wants the host's schema, and
/// refuses an image's older one: this asks for columns all of them have, of
/// a root that is a copy, as `pkg shell` may write to what it opens.
pub fn packages(root: &Path) -> Result<Option<Vec<Package>>, Error> {
    if !root.join("var/db/pkg/local.sqlite").exists() {
        return Ok(None);
    }
    let dbdir = root.join("var/db/pkg");
    let out = std::process::Command::new("pkg")
        .arg("-o")
        .arg(format!("PKG_DBDIR={}", dbdir.display()))
        .args(["shell", "-separator", "\t"])
        .arg("select name, version, origin from packages order by name")
        .output()
        .map_err(|e| Error::Runtime(format!("running pkg(8) to list packages: {e}")))?;
    if !out.status.success() {
        return Err(Error::Runtime(format!(
            "pkg(8) could not list the image's packages: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(Some(parse_packages(&String::from_utf8_lossy(&out.stdout))))
}

/// What an inventory leaves out: said in the document, so that it does not
/// read as complete.
pub fn coverage(packages: Option<&[Package]>) -> String {
    match packages {
        Some(_) => "Packages registered with pkg(8) only: files put in the image any other \
                    way are not listed."
            .to_owned(),
        None => "NOT AN INVENTORY: the image has no pkg(8) database, so what is in it is unknown."
            .to_owned(),
    }
}

/// An SPDX document of the image `name`, whose manifest is `digest`.
pub fn sbom(name: &str, digest: &str, created: &str, packages: Option<&[Package]>) -> Value {
    let image = "SPDXRef-Image";
    let mut listed = vec![json!({
        "SPDXID": image,
        "name": name,
        "downloadLocation": "NOASSERTION",
        "filesAnalyzed": false,
        "checksums": [{"algorithm": "SHA256", "checksumValue": hex_of(digest)}],
        "primaryPackagePurpose": "CONTAINER",
    })];
    let mut relationships = vec![json!({
        "spdxElementId": "SPDXRef-DOCUMENT",
        "relationshipType": "DESCRIBES",
        "relatedSpdxElement": image,
    })];
    for (i, p) in packages.unwrap_or_default().iter().enumerate() {
        let id = format!("SPDXRef-Package-{i}");
        listed.push(json!({
            "SPDXID": id,
            "name": p.name,
            "versionInfo": p.version,
            "sourceInfo": format!("FreeBSD ports origin {}", p.origin),
            "downloadLocation": "NOASSERTION",
            "filesAnalyzed": false,
        }));
        relationships.push(json!({
            "spdxElementId": image,
            "relationshipType": "CONTAINS",
            "relatedSpdxElement": id,
        }));
    }
    json!({
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": name,
        "documentNamespace": format!("{BUILDER}/spdx/{}", hex_of(digest)),
        "creationInfo": {
            "created": created,
            "creators": [format!("Tool: legion-{}", env!("CARGO_PKG_VERSION"))],
            "comment": coverage(packages),
        },
        "packages": listed,
        "relationships": relationships,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Base, Source};
    use crate::sign::PrivateKey;

    #[test]
    fn an_envelope_is_what_legion_image_checks() {
        let private = PrivateKey::generate();
        let other = PrivateKey::generate().public();
        let r = Reference::parse("ghcr.io/me/app:1").unwrap();
        let said = statement(&r, "sha256:abc", PROVENANCE, &json!({"a": 1}));
        let sealed = envelope(&said, &private).unwrap();
        let key = private.public();
        let (i, got) =
            legion_image::attest::check(&sealed, std::slice::from_ref(&key), "sha256:abc").unwrap();
        assert_eq!(i, 0);
        assert_eq!(got.predicate_type, PROVENANCE);
        assert_eq!(got.predicate, json!({"a": 1}));
        assert!(legion_image::attest::check(&sealed, &[other], "sha256:abc").is_err());
        assert!(
            legion_image::attest::check(&sealed, std::slice::from_ref(&key), "sha256:def").is_err()
        );

        // Changing what is said breaks the signature.
        let mut changed: Value = serde_json::from_slice(&sealed).unwrap();
        changed["payload"] = json!(B64.encode(b"{}"));
        let changed = serde_json::to_vec(&changed).unwrap();
        assert!(legion_image::attest::check(&changed, &[key], "sha256:abc").is_err());
    }

    #[test]
    fn provenance_comes_from_a_build_and_names_its_inputs() {
        assert!(provenance(&Record::default()).is_err());
        let record = Record {
            source: Some(Source {
                containerfile: "sha256:c".to_owned(),
                context: "sha256:x".to_owned(),
            }),
            bases: vec![Base {
                reference: "freebsd:15".to_owned(),
                config: "sha256:bc".to_owned(),
                manifest: "sha256:bm".to_owned(),
            }],
            ..Record::default()
        };
        let p = provenance(&record).unwrap();
        let definition = &p["buildDefinition"];
        assert_eq!(definition["externalParameters"]["context"], "sha256:x");
        let dependency = &definition["resolvedDependencies"][0];
        assert_eq!(dependency["name"], "freebsd:15");
        assert_eq!(dependency["digest"]["sha256"], "bm");
    }

    #[test]
    fn packages_are_read_from_rows() {
        let got =
            parse_packages("curl\t8.1\tftp/curl\nbad\n\tno name\t\nzstd\t1.5\tarchivers/zstd\n");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].origin, "ftp/curl");
    }

    #[test]
    fn an_sbom_says_what_it_does_not_cover() {
        let curl = [Package {
            name: "curl".to_owned(),
            version: "8.1".to_owned(),
            origin: "ftp/curl".to_owned(),
        }];
        let doc = sbom("app", "sha256:abc", "2026-11-06T00:00:00Z", Some(&curl));
        assert_eq!(doc["spdxVersion"], "SPDX-2.3");
        assert_eq!(doc["packages"].as_array().unwrap().len(), 2);
        assert_eq!(doc["packages"][1]["versionInfo"], "8.1");
        let comment = doc["creationInfo"]["comment"].as_str().unwrap();
        assert!(comment.contains("not listed"), "{comment}");

        let unknown = sbom("app", "sha256:abc", "2026-11-06T00:00:00Z", None);
        let comment = unknown["creationInfo"]["comment"].as_str().unwrap();
        assert!(comment.contains("NOT AN INVENTORY"), "{comment}");
        assert_eq!(unknown["packages"].as_array().unwrap().len(), 1);
    }
}
