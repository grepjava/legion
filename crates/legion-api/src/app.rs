//! Ready-to-deploy applications (TODO.MD item 5): the daemonless catalog
//! (`https://catalog.daemonless.io/v1/daemonless`, FreeBSD OCI images,
//! BSD-2-Clause) read as it is published, nothing of it bundled. An app's
//! manifest is a compose file with an `x-fjord` part saying what its
//! variables are; [`render`] fills them in and makes the compose file a
//! Legion stack is deployed from (`legion app deploy`, the console's Apps).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Where the catalog is read from unless told otherwise.
pub const CATALOG: &str = "https://catalog.daemonless.io/v1/daemonless";

/// The catalog's `catalog.json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    #[serde(default)]
    pub catalog_name: String,
    #[serde(default)]
    pub apps: Vec<CatalogApp>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogApp {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub description: String,
    /// Its main image, without a tag.
    #[serde(default)]
    pub image: String,
    /// Relative to the catalog.
    #[serde(default)]
    pub manifest_url: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub web_url: String,
    #[serde(default)]
    pub icon: String,
    #[serde(default)]
    pub variants: Vec<Variant>,
}

/// A build of an app's image: its tag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Variant {
    pub id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub default: bool,
}

/// An app's manifest: the compose file, and what its variables are.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Manifest {
    pub name: String,
    pub description: String,
    pub variables: Vec<AppVariable>,
    pub variants: Vec<Variant>,
    /// The compose file, as written.
    #[serde(skip)]
    pub compose: String,
}

/// A variable an app's compose file reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppVariable {
    pub name: String,
    #[serde(default)]
    pub label: String,
    /// `port`, `zfs_dataset`, `path`, `string`, `secret`, `bool`,
    /// `image_tag`.
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default, deserialize_with = "scalar")]
    pub default: Option<String>,
}

fn scalar<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let v: Option<serde_json::Value> = Option::deserialize(d)?;
    Ok(v.and_then(|v| match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }))
}

impl AppVariable {
    /// Whether its value names a Volume: the app's data, kept apart.
    pub fn is_storage(&self) -> bool {
        matches!(self.kind.as_str(), "zfs_dataset" | "path")
    }
}

/// Reads an app's manifest.
pub fn manifest(yaml: &str) -> Result<Manifest, String> {
    #[derive(Deserialize, Default)]
    struct Info {
        #[serde(default)]
        name: String,
        #[serde(default)]
        description: String,
    }
    #[derive(Deserialize, Default)]
    struct Fjord {
        #[serde(default)]
        info: Info,
        #[serde(default)]
        variables: Vec<AppVariable>,
        #[serde(default)]
        variants: Vec<Variant>,
    }
    #[derive(Deserialize)]
    struct Doc {
        #[serde(default, rename = "x-fjord")]
        fjord: Option<Fjord>,
    }
    let doc: Doc =
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("reading the manifest: {e}"))?;
    let f = doc.fjord.unwrap_or_default();
    Ok(Manifest {
        name: f.info.name,
        description: f.info.description,
        variables: f.variables,
        variants: f.variants,
        compose: yaml.to_owned(),
    })
}

/// The compose file and the variables to deploy `m` with, given the
/// answers in `values` (the rest take their defaults, an empty one staying
/// empty: the app's own default then): each storage variable's value is a
/// Volume of the stack, declared so, named after the variable when it has
/// no default (`HTML_PATH` is `html-path`); a secret not given is made up
/// by `secret`; `variant` is the main image's tag (`image` without one).
pub fn render(
    m: &Manifest,
    image: &str,
    variant: Option<&str>,
    values: &BTreeMap<String, String>,
    secret: &dyn Fn() -> String,
) -> Result<(String, BTreeMap<String, String>), String> {
    let mut env = BTreeMap::new();
    for v in &m.variables {
        let default = v.default.as_deref().map(unquote).filter(|d| !d.is_empty());
        let value = values.get(&v.name).cloned().or(default).unwrap_or_else(|| {
            if v.kind == "secret" {
                secret()
            } else if v.is_storage() {
                v.name.to_lowercase().replace('_', "-")
            } else {
                String::new()
            }
        });
        env.insert(v.name.clone(), value);
    }
    let unknown: Vec<_> = values
        .keys()
        .filter(|k| !m.variables.iter().any(|v| &v.name == *k))
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "{} is no variable of {}",
            unknown
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            m.name
        ));
    }
    let mut doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&m.compose).map_err(|e| format!("reading the manifest: {e}"))?;
    let map = doc
        .as_mapping_mut()
        .ok_or("the manifest is not a compose file")?;
    map.remove("x-fjord");
    // Each storage variable's value is a Volume, declared as compose wants.
    let volumes = map
        .entry("volumes".into())
        .or_insert_with(|| serde_yaml_ng::Value::Mapping(Default::default()));
    if let Some(vols) = volumes.as_mapping_mut() {
        for v in m.variables.iter().filter(|v| v.is_storage()) {
            let name = &env[&v.name];
            crate::validate_name(name)
                .map_err(|e| format!("{}: {name:?} names a Volume: {e}", v.name))?;
            vols.entry(name.as_str().into())
                .or_insert_with(|| serde_yaml_ng::Value::Mapping(Default::default()));
        }
        if vols.is_empty() {
            map.remove("volumes");
        }
    }
    // The variant is the main image's tag.
    if let (Some(tag), Some(services)) = (
        variant,
        map.get_mut("services").and_then(|s| s.as_mapping_mut()),
    ) {
        for (_, svc) in services.iter_mut() {
            if let Some(img) = svc.get_mut("image")
                && let Some(s) = img.as_str()
                && !image.is_empty()
                && s.split(':').next() == Some(image)
            {
                *img = format!("{image}:{tag}").into();
            }
        }
    }
    let compose = serde_yaml_ng::to_string(&doc).map_err(|e| e.to_string())?;
    Ok((compose, env))
}

/// A default as an `.env` file holds it, quoted or not: `"@every 1h"` is
/// `@every 1h`, as the catalog's own tools read it.
fn unquote(v: &str) -> String {
    let v = v.trim();
    for q in ['"', '\''] {
        if v.len() >= 2 && v.starts_with(q) && v.ends_with(q) {
            return v[1..v.len() - 1].to_owned();
        }
    }
    v.to_owned()
}

/// A made-up secret: 24 characters from the system's randomness.
pub fn random_secret() -> String {
    use std::io::Read;
    const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut b = [0u8; 24];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .is_err()
    {
        // No /dev/urandom (not a Unix): the clock and the process, mixed.
        let mut x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ (u64::from(std::process::id()) << 32);
        for byte in &mut b {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *byte = x as u8;
        }
    }
    b.iter()
        .map(|c| ALPHABET[*c as usize % ALPHABET.len()] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const VW: &str = r#"
name: vaultwarden
services:
  vaultwarden:
    image: ghcr.io/daemonless/vaultwarden:latest
    container_name: vaultwarden
    environment:
      - PUID=${PUID}
      - ADMIN_TOKEN=${ADMIN_TOKEN}
    volumes:
      - "${CONFIG_DATA}:/config"
    ports:
      - "${WEB_PORT}:80"
x-fjord:
  version: "0.1"
  info:
    id: vaultwarden
    name: Vaultwarden
    description: A password manager.
  variables:
    - name: WEB_PORT
      label: Web UI
      type: port
      default: "80"
    - name: CONFIG_DATA
      type: zfs_dataset
      default: config
    - name: PUID
      type: string
      default: 1000
    - name: ADMIN_TOKEN
      type: secret
  variants:
    - id: latest
      label: Latest
    - id: pkg
      label: FreeBSD quarterly packages
      default: true
"#;

    #[test]
    fn a_manifest_says_its_variables_and_variants() {
        let m = manifest(VW).unwrap();
        assert_eq!(m.name, "Vaultwarden");
        assert_eq!(m.variables.len(), 4);
        assert_eq!(
            m.variables[2].default.as_deref(),
            Some("1000"),
            "a number read as text"
        );
        assert!(m.variables[1].is_storage());
        assert_eq!(
            m.variants[1],
            Variant {
                id: "pkg".into(),
                label: "FreeBSD quarterly packages".into(),
                default: true
            }
        );
    }

    #[test]
    fn rendered_it_deploys_as_a_stack_with_its_data_on_a_volume() {
        let m = manifest(VW).unwrap();
        let values = BTreeMap::from([("WEB_PORT".to_owned(), "18080".to_owned())]);
        let (compose, env) = render(
            &m,
            "ghcr.io/daemonless/vaultwarden",
            Some("pkg"),
            &values,
            &|| "s3cret".into(),
        )
        .unwrap();
        assert_eq!(env["WEB_PORT"], "18080");
        assert_eq!(env["CONFIG_DATA"], "config");
        assert_eq!(
            env["ADMIN_TOKEN"], "s3cret",
            "a secret not given is made up"
        );
        assert!(
            compose.contains("ghcr.io/daemonless/vaultwarden:pkg"),
            "{compose}"
        );
        assert!(!compose.contains("x-fjord"), "{compose}");
        let get = |k: &str| env.get(k).cloned();
        let stack = crate::compose::convert(
            &compose,
            "vw",
            "vw",
            &crate::compose::Inputs {
                env: &get,
                read: &|p| Err(format!("{p}: no files")),
            },
        )
        .unwrap();
        let kinds: Vec<_> = stack
            .objects
            .iter()
            .map(|o| (o.kind, o.metadata.name.as_str()))
            .collect();
        assert_eq!(
            kinds,
            [
                (crate::Kind::Volume, "config"),
                (crate::Kind::Service, "vaultwarden")
            ]
        );
    }

    #[test]
    fn unknown_or_missing_variables_are_said() {
        let mut m = manifest(VW).unwrap();
        let typo = BTreeMap::from([("WEBPORT".to_owned(), "1".to_owned())]);
        let e = render(&m, "", None, &typo, &|| "x".into()).unwrap_err();
        assert!(e.contains("WEBPORT is no variable"), "{e}");
        m.variables.push(AppVariable {
            name: "DB_TYPE".into(),
            kind: "string".into(),
            ..Default::default()
        });
        m.variables.push(AppVariable {
            name: "HTML_PATH".into(),
            kind: "path".into(),
            ..Default::default()
        });
        let (compose, env) = render(&m, "", None, &BTreeMap::new(), &|| "x".into()).unwrap();
        assert_eq!(env["DB_TYPE"], "", "left to the app");
        assert_eq!(env["HTML_PATH"], "html-path", "a Volume named after it");
        assert!(compose.contains("html-path"), "{compose}");
        m.variables.truncate(4);
        let bad = BTreeMap::from([("CONFIG_DATA".to_owned(), "../etc".to_owned())]);
        m.variables.pop();
        assert!(
            render(&m, "", None, &bad, &|| "x".into()).is_err(),
            "a Volume's name is a name"
        );
    }

    #[test]
    fn a_default_is_read_as_an_env_file_reads_it() {
        assert_eq!(unquote("\"@every 1h\""), "@every 1h");
        assert_eq!(unquote("'x y'"), "x y");
        assert_eq!(unquote("plain"), "plain");
        assert_eq!(unquote("\""), "\"");
    }

    #[test]
    fn a_made_up_secret_is_long_and_varies() {
        let (a, b) = (random_secret(), random_secret());
        assert_eq!(a.len(), 24);
        assert_ne!(a, b);
    }

    #[test]
    fn the_catalog_reads_as_published() {
        let c: Catalog = serde_json::from_str(
            r#"{"catalog_name":"Daemonless Apps","generated":"x","apps":[{"id":"actual","name":"Actual Budget","category":"Productivity","class":"service","image":"ghcr.io/daemonless/actual","manifest_url":"manifests/actual.yaml","version":"v26.11.0","architectures":["amd64"],"variants":[{"id":"latest","label":"Latest","default":true,"image":"ghcr.io/daemonless/actual:latest","version":"v26.11.0"}]}]}"#,
        )
        .unwrap();
        assert_eq!(c.apps[0].manifest_url, "manifests/actual.yaml");
        assert!(c.apps[0].variants[0].default);
    }
}
