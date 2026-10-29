//! The OCI (and Docker v2) documents an image is made of.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::Error;

pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
pub const DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
pub const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// What a client accepts when asking for a manifest.
pub const MANIFEST_TYPES: [&str; 4] = [OCI_INDEX, DOCKER_LIST, OCI_MANIFEST, DOCKER_MANIFEST];

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    #[serde(default)]
    pub media_type: String,
    pub digest: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub platform: Option<Platform>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
}

/// An index or a manifest, told apart by their contents: some registries
/// leave `mediaType` out.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Document {
    #[serde(default)]
    pub media_type: String,
    #[serde(default)]
    pub manifests: Option<Vec<Descriptor>>,
    #[serde(default)]
    pub config: Option<Descriptor>,
    #[serde(default)]
    pub layers: Vec<Descriptor>,
    #[serde(default)]
    pub schema_version: u32,
}

pub struct Manifest {
    pub config: Descriptor,
    pub layers: Vec<Descriptor>,
}

impl Document {
    pub fn parse(bytes: &[u8]) -> Result<Document, Error> {
        let d: Document = serde_json::from_slice(bytes)
            .map_err(|e| Error::Unsupported(format!("not an image manifest: {e}")))?;
        if d.schema_version == 1 {
            return Err(Error::Unsupported(
                "Docker schema 1 manifests are not supported".to_owned(),
            ));
        }
        Ok(d)
    }

    pub fn into_manifest(self) -> Result<Manifest, Error> {
        let config = self
            .config
            .ok_or_else(|| Error::Unsupported("a manifest without a config".to_owned()))?;
        Ok(Manifest {
            config,
            layers: self.layers,
        })
    }
}

/// This node's platform in OCI's words.
pub fn platform() -> (&'static str, &'static str) {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64",
        "riscv64" => "riscv64",
        other => other,
    };
    ("freebsd", arch)
}

/// The entry of an index for this node's platform.
pub fn select(manifests: &[Descriptor]) -> Result<&Descriptor, Error> {
    let (os, arch) = platform();
    manifests
        .iter()
        .find(|m| {
            m.platform
                .as_ref()
                .is_some_and(|p| p.os == os && p.architecture == arch)
        })
        .ok_or_else(|| {
            let offered: Vec<String> = manifests
                .iter()
                .filter_map(|m| m.platform.as_ref())
                .map(|p| format!("{}/{}", p.os, p.architecture))
                .collect();
            Error::Unsupported(format!(
                "no image for {os}/{arch} (offered: {})",
                if offered.is_empty() {
                    "none".to_owned()
                } else {
                    offered.join(", ")
                }
            ))
        })
}

/// An image's configuration file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub architecture: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub os: String,
    #[serde(default)]
    pub config: Config,
    #[serde(default)]
    pub rootfs: RootFs,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootFs {
    #[serde(default)]
    pub diff_ids: Vec<String>,
}

/// How to run the image. FreeBSD's own images write these keys in lower
/// case (`cmd`), so both spellings are read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    #[serde(
        rename = "Entrypoint",
        alias = "entrypoint",
        default,
        deserialize_with = "nullable"
    )]
    pub entrypoint: Vec<String>,
    #[serde(rename = "Cmd", alias = "cmd", default, deserialize_with = "nullable")]
    pub cmd: Vec<String>,
    #[serde(rename = "Env", alias = "env", default, deserialize_with = "nullable")]
    pub env: Vec<String>,
    #[serde(rename = "User", alias = "user", default)]
    pub user: String,
    #[serde(
        rename = "WorkingDir",
        alias = "workingdir",
        alias = "working_dir",
        default
    )]
    pub working_dir: String,
    #[serde(rename = "StopSignal", alias = "stopsignal", default)]
    pub stop_signal: String,
    #[serde(
        rename = "Labels",
        alias = "labels",
        default,
        deserialize_with = "nullable"
    )]
    pub labels: BTreeMap<String, String>,
}

fn nullable<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configs_in_either_case() {
        let freebsd: ImageConfig = serde_json::from_str(
            r#"{"architecture":"amd64","os":"freebsd","config":{"cmd":["/bin/sh"]},
                "rootfs":{"type":"layers","diff_ids":["sha256:aa"]}}"#,
        )
        .unwrap();
        assert_eq!(freebsd.config.cmd, ["/bin/sh"]);
        let docker: ImageConfig = serde_json::from_str(
            r#"{"config":{"Entrypoint":null,"Cmd":["x"],"Env":["A=1"],"User":"www",
                "WorkingDir":"/srv","Labels":null}}"#,
        )
        .unwrap();
        assert_eq!(docker.config.cmd, ["x"]);
        assert!(docker.config.entrypoint.is_empty());
        assert_eq!(docker.config.user, "www");
        assert_eq!(docker.config.working_dir, "/srv");
        let none: ImageConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(none, ImageConfig::default());
    }

    #[test]
    fn selects_this_platform() {
        let (os, arch) = platform();
        let list: Vec<Descriptor> = serde_json::from_value(serde_json::json!([
            {"digest": "sha256:l", "platform": {"os": "linux", "architecture": arch}},
            {"digest": "sha256:f", "platform": {"os": os, "architecture": arch}},
        ]))
        .unwrap();
        assert_eq!(select(&list).unwrap().digest, "sha256:f");
        let e = select(&list[..1]).unwrap_err().to_string();
        assert!(e.contains(&format!("linux/{arch}")), "{e}");
    }
}
