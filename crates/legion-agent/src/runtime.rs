//! The container runtime calls the agent makes, and what they take and
//! give: `legion_jail::JailRuntime` in production,
//! [`crate::fake::FakeRuntime`] in tests.

use std::future::Future;
use std::time::Duration;

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::stream::BoxStream;

pub trait Runtime: Send + Sync + 'static {
    /// Whether the node has the image `reference` names and `user` (a
    /// namespace) may use it without asking its registry: it was pulled
    /// without credentials, loaded onto the node, or pulled for `user`.
    fn image_usable(
        &self,
        reference: &str,
        user: &str,
    ) -> impl Future<Output = Result<bool>> + Send;
    /// Pulls `reference` for `user`, with the login in `logins` for its
    /// registry if there is one.
    fn pull(
        &self,
        reference: &str,
        user: &str,
        logins: &[Login],
    ) -> impl Future<Output = Result<()>> + Send;
    fn create(&self, spec: &ContainerSpec) -> impl Future<Output = Result<String>> + Send;
    fn start(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn stop(&self, id: &str, timeout: Duration) -> impl Future<Output = Result<()>> + Send;
    fn remove(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn inspect(&self, id: &str) -> impl Future<Output = Result<ContainerInspect>> + Send;
    fn wait(&self, id: &str) -> impl Future<Output = Result<i32>> + Send;
    /// Runs a command in a running container and collects its output.
    fn exec(&self, id: &str, cmd: &[String]) -> impl Future<Output = Result<ExecOutput>> + Send;
    /// Starts a command in a running container with its output streamed
    /// and, with `stdin`, its input from the caller.
    fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> impl Future<Output = Result<Attached>> + Send;
    fn exec_resize(
        &self,
        exec_id: &str,
        width: u16,
        height: u16,
    ) -> impl Future<Output = Result<()>> + Send;
    /// The exit code of an attached exec whose output has ended.
    fn exec_exit_code(&self, exec_id: &str) -> impl Future<Output = Result<i32>> + Send;
    /// Lets go of an attached exec, however its caller ended: its exit code
    /// is no longer wanted, and a terminal's last descriptor closes, which
    /// hangs the terminal up. One already gone is fine.
    fn exec_release(&self, exec_id: &str) -> impl Future<Output = ()> + Send;
    /// A container's output; with `follow`, until it stops.
    fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Chunk>>>> + Send;
    fn list(
        &self,
        labels: &[(&str, &str)],
    ) -> impl Future<Output = Result<Vec<ContainerSummary>>> + Send;
    /// Container events from now on, for containers carrying every label.
    fn events(
        &self,
        labels: &[(&str, &str)],
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Event>>>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No such container, image or exec session.
    #[error("{0}")]
    NotFound(String),
    /// The container is in the wrong state for the call, e.g. exec in a
    /// stopped one.
    #[error("{0}")]
    WrongState(String),
    #[error("{0}")]
    Failed(String),
    #[error("pulling {image}: {message}")]
    Pull { image: String, message: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound(_))
    }

    pub fn is_wrong_state(&self) -> bool {
        matches!(self, Error::WrongState(_))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A registry account, from a pod's `imagePullSecrets`.
#[derive(Clone, PartialEq, Eq)]
pub struct Login {
    /// As a Docker config's `auths` names it: `ghcr.io`,
    /// `https://index.docker.io/v1/`.
    pub registry: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("registry", &self.registry)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// A container to create.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    /// Replaces the image's entrypoint (and then its command).
    pub entrypoint: Vec<String>,
    /// Replaces the image's command.
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub work_dir: Option<String>,
    pub user: Option<String>,
    pub hostname: Option<String>,
    pub labels: BTreeMap<String, String>,
    /// How to make its jail (see [`crate::pod`]).
    pub annotations: BTreeMap<String, String>,
    pub mounts: Vec<Mount>,
    /// Seconds between the stop signal and killing it.
    pub stop_timeout: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    pub destination: String,
    /// `nullfs`.
    pub kind: String,
    pub source: String,
    /// `ro` for read-only.
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerSummary {
    pub id: String,
    pub names: Vec<String>,
    pub image: String,
    pub labels: BTreeMap<String, String>,
    /// `created`, `running` or `exited`.
    pub state: String,
    pub exit_code: i32,
    pub exited: bool,
    pub pid: i64,
}

impl ContainerSummary {
    pub fn name(&self) -> &str {
        self.names.first().map_or("", String::as_str)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerInspect {
    pub id: String,
    pub name: String,
    pub state: ContainerState,
    pub config: ContainerConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerState {
    pub status: String,
    pub running: bool,
    pub pid: i64,
    /// Its jail's id while it runs; 0 otherwise, or where there is none.
    pub jid: u32,
    pub exit_code: i32,
    pub oom_killed: bool,
    pub started_at: String,
    pub finished_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerConfig {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// Something that happened to a container.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Event {
    /// `container`.
    pub kind: String,
    /// `died`, ...
    pub action: String,
    pub actor: Actor,
    pub time_nano: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Actor {
    pub id: String,
    /// The container's labels, its `name`, and on `died` its
    /// `containerExitCode`.
    pub attributes: BTreeMap<String, String>,
}

impl Event {
    pub fn exit_code(&self) -> Option<i32> {
        self.actor.attributes.get("containerExitCode")?.parse().ok()
    }
}

/// Output of a finished exec.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecOutput {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Which stream a piece of output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Stdout,
    Stderr,
}

/// A piece of container output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub stream: Output,
    pub data: Bytes,
}

/// A process started by [`Runtime::exec_attach`].
pub struct Attached {
    /// The exec session, for [`Runtime::exec_resize`] and
    /// [`Runtime::exec_exit_code`].
    pub id: String,
    /// What the process writes; ends when it exits.
    pub output: BoxStream<'static, Result<Chunk>>,
    /// Its stdin, when asked for; shutting it down closes stdin.
    pub stdin: Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>>,
}

impl std::fmt::Debug for Attached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attached")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
