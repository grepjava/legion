//! Request and response bodies, limited to the fields Legion uses. Names
//! follow libpod's JSON.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

/// What `containers/create` takes: a subset of libpod's SpecGenerator.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    /// Replaces the image's entrypoint.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entrypoint: Vec<String>,
    /// Replaces the image's command.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// OCI annotations; ocijail reads `org.freebsd.jail.*` here.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    pub netns: Namespace,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Seconds between the stop signal and SIGKILL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_timeout: Option<u64>,
}

/// A namespace mode. Legion always uses `none` for the network: the pod's
/// VNET comes from the `org.freebsd.jail.vnet` annotation instead
/// (DESIGN.md §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Namespace {
    pub nsmode: String,
}

impl Default for Namespace {
    fn default() -> Self {
        Namespace {
            nsmode: "none".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Mount {
    pub destination: String,
    /// `nullfs` for host directories on FreeBSD.
    #[serde(rename = "type")]
    pub kind: String,
    pub source: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

impl Mount {
    pub fn nullfs(source: &str, destination: &str, read_only: bool) -> Self {
        Mount {
            destination: destination.to_owned(),
            kind: "nullfs".to_owned(),
            source: source.to_owned(),
            options: if read_only {
                vec!["ro".to_owned()]
            } else {
                Vec::new()
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Created {
    pub id: String,
}

/// One row of `containers/json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerSummary {
    pub id: String,
    #[serde(default, deserialize_with = "nullable")]
    pub names: Vec<String>,
    #[serde(default)]
    pub image: String,
    #[serde(default, deserialize_with = "nullable")]
    pub labels: BTreeMap<String, String>,
    /// `created`, `running`, `exited`, ...
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub exit_code: i32,
    #[serde(default)]
    pub exited: bool,
    #[serde(default)]
    pub pid: i64,
}

impl ContainerSummary {
    pub fn name(&self) -> &str {
        self.names.first().map_or("", String::as_str)
    }
}

/// The parts of `containers/{id}/json` Legion reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerInspect {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub state: ContainerState,
    #[serde(default)]
    pub config: ContainerConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerState {
    pub status: String,
    #[serde(default)]
    pub running: bool,
    #[serde(default)]
    pub pid: i64,
    #[serde(default)]
    pub exit_code: i32,
    #[serde(default, rename = "OOMKilled")]
    pub oom_killed: bool,
    /// RFC 3339; the zero time when never started or finished.
    #[serde(default)]
    pub started_at: String,
    #[serde(default)]
    pub finished_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerConfig {
    #[serde(default, deserialize_with = "nullable")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "nullable")]
    pub annotations: BTreeMap<String, String>,
}

/// One line of the `events` stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Event {
    /// `container`, `image`, ...
    #[serde(rename = "Type", default)]
    pub kind: String,
    /// `create`, `start`, `died`, `remove`, ...
    #[serde(rename = "Action", default)]
    pub action: String,
    #[serde(rename = "Actor", default)]
    pub actor: Actor,
    #[serde(rename = "timeNano", default)]
    pub time_nano: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Actor {
    #[serde(rename = "ID", default)]
    pub id: String,
    /// Includes the container's labels, its `name`, and on `died` its
    /// `containerExitCode`.
    #[serde(rename = "Attributes", default, deserialize_with = "nullable")]
    pub attributes: BTreeMap<String, String>,
}

impl Event {
    pub fn exit_code(&self) -> Option<i32> {
        self.actor.attributes.get("containerExitCode")?.parse().ok()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ExecCreate<'a> {
    pub cmd: &'a [String],
    pub attach_stdin: bool,
    pub attach_stdout: bool,
    pub attach_stderr: bool,
    pub tty: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ExecInspect {
    #[serde(default)]
    pub exit_code: i32,
    #[serde(default)]
    pub running: bool,
}

/// Output of a finished exec.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Credentials for a registry pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegistryAuth {
    pub username: String,
    pub password: String,
}

/// One line of the `images/pull` progress stream.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct PullProgress {
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
}

/// The body of an error response.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ErrorBody {
    #[serde(default)]
    pub message: String,
}

/// libpod writes `null` for empty maps and lists.
fn nullable<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}
