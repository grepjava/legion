//! Typed client for the libpod REST API (DESIGN.md §6), covering what the
//! agent needs: images, containers, exec, logs and events. Also starts and
//! supervises the Legion-owned `podman system service` ([`service`]).
//!
//! Each request opens its own connection. Over the Unix socket this costs
//! little, and it lets long-lived streams (events, followed logs) run next
//! to ordinary calls without a pool.

use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use futures::Stream;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod service;
mod stream;
mod types;

pub use stream::{Chunk, Output};
pub use types::*;

/// API version in request paths; the oldest libpod Legion is tested with is 5.x.
const API: &str = "/v5.0.0/libpod";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// libpod answered with an error status.
    #[error("podman: {message} (HTTP {status})")]
    Api { status: u16, message: String },
    #[error("connecting to podman at {endpoint}: {source}")]
    Connect {
        endpoint: String,
        source: std::io::Error,
    },
    #[error("podman HTTP: {0}")]
    Http(#[from] hyper::Error),
    #[error("podman stream: {0}")]
    Io(#[from] std::io::Error),
    #[error("decoding podman response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("podman protocol: {0}")]
    Protocol(String),
    #[error("pulling {image}: {message}")]
    Pull { image: String, message: String },
}

impl Error {
    /// The container, image or exec session does not exist.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::Api { status: 404, .. })
    }

    /// The container is in the wrong state for the request, e.g. exec in a
    /// stopped container. libpod reports some of these as 409 and others as
    /// 500 "container state improper".
    pub fn is_wrong_state(&self) -> bool {
        match self {
            Error::Api { status: 409, .. } => true,
            Error::Api {
                status: 500,
                message,
            } => message.contains("container state improper"),
            _ => false,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A process started by [`Podman::exec_attach`].
pub struct Attached {
    /// The exec session, for [`Podman::exec_resize`] and
    /// [`Podman::exec_exit_code`].
    pub id: String,
    /// What the process writes; ends when it exits.
    pub output: futures::stream::BoxStream<'static, Result<Chunk>>,
    /// Its stdin, when asked for; shutting it down closes stdin.
    pub stdin: Option<std::pin::Pin<Box<dyn AsyncWrite + Send>>>,
}

impl std::fmt::Debug for Attached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attached")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Where the service listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Unix(PathBuf),
    /// `host:port`, for `podman system service tcp://...` and tests.
    Tcp(String),
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Unix(p) => write!(f, "unix://{}", p.display()),
            Endpoint::Tcp(a) => write!(f, "tcp://{a}"),
        }
    }
}

trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

#[derive(Debug, Clone)]
pub struct Podman {
    endpoint: Endpoint,
}

impl Podman {
    pub fn new(endpoint: Endpoint) -> Self {
        Podman { endpoint }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The libpod version, from the `_ping` response headers.
    pub async fn ping(&self) -> Result<String> {
        let resp = self.call(Method::GET, "/_ping", None, None).await?;
        let resp = check(resp).await?;
        Ok(resp
            .headers()
            .get("Libpod-Api-Version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned())
    }

    pub async fn image_exists(&self, reference: &str) -> Result<bool> {
        let path = format!("/images/{}/exists", segment(reference));
        let resp = self.call(Method::GET, &path, None, None).await?;
        match resp.status() {
            StatusCode::NOT_FOUND => Ok(false),
            _ => check(resp).await.map(|_| true),
        }
    }

    /// Pulls `reference` and returns the image id.
    pub async fn pull(&self, reference: &str, auth: Option<&RegistryAuth>) -> Result<String> {
        use futures::StreamExt;
        let path = format!("/images/pull?{}", query(&[("reference", reference)]));
        let auth = auth
            .map(|a| -> Result<String> {
                Ok(base64::engine::general_purpose::URL_SAFE.encode(serde_json::to_vec(a)?))
            })
            .transpose()?;
        let resp = self
            .call(Method::POST, &path, None, auth.as_deref())
            .await?;
        let resp = check(resp).await?;
        let mut lines = Box::pin(stream::json_lines::<PullProgress, _>(Box::pin(
            stream::chunks(resp.into_body()),
        )));
        let mut id = None;
        while let Some(line) = lines.next().await {
            let line = line?;
            if let Some(message) = line.error {
                return Err(Error::Pull {
                    image: reference.to_owned(),
                    message,
                });
            }
            id = line.id.or(id);
        }
        id.ok_or_else(|| Error::Pull {
            image: reference.to_owned(),
            message: "the pull ended without an image id".to_owned(),
        })
    }

    /// Creates a container and returns its id.
    pub async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        let created: Created = self
            .json(Method::POST, "/containers/create", Some(spec))
            .await?;
        Ok(created.id)
    }

    /// Starts a container; starting a running one is not an error.
    pub async fn start(&self, id: &str) -> Result<()> {
        self.no_content(Method::POST, &format!("/containers/{}/start", segment(id)))
            .await
    }

    /// Stops a container, killing it after `timeout`; stopping a stopped one
    /// is not an error.
    pub async fn stop(&self, id: &str, timeout: Duration) -> Result<()> {
        let path = format!(
            "/containers/{}/stop?{}",
            segment(id),
            query(&[("timeout", &timeout.as_secs().to_string())])
        );
        self.no_content(Method::POST, &path).await
    }

    /// Sends `signal` (a name such as `SIGKILL`, or a number).
    pub async fn kill(&self, id: &str, signal: &str) -> Result<()> {
        let path = format!(
            "/containers/{}/kill?{}",
            segment(id),
            query(&[("signal", signal)])
        );
        self.no_content(Method::POST, &path).await
    }

    /// Removes a container, stopping it first if needed.
    pub async fn remove(&self, id: &str) -> Result<()> {
        let path = format!("/containers/{}?force=true", segment(id));
        let resp = self.call(Method::DELETE, &path, None, None).await?;
        check(resp).await.map(drop)
    }

    pub async fn inspect(&self, id: &str) -> Result<ContainerInspect> {
        let path = format!("/containers/{}/json", segment(id));
        self.json::<(), _>(Method::GET, &path, None).await
    }

    /// All containers, running or not, carrying every one of `labels`.
    pub async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<ContainerSummary>> {
        let path = format!("/containers/json?all=true&{}", label_filter(labels));
        self.json::<(), _>(Method::GET, &path, None).await
    }

    /// Waits for a container to stop and returns its exit code. Returns at
    /// once for a container that is not running.
    pub async fn wait(&self, id: &str) -> Result<i32> {
        let path = format!("/containers/{}/wait", segment(id));
        let resp = self.call(Method::POST, &path, None, None).await?;
        let body = stream::collect(check(resp).await?.into_body()).await?;
        let text = String::from_utf8_lossy(&body);
        text.trim()
            .parse()
            .map_err(|_| Error::Protocol(format!("unexpected wait result {text:?}")))
    }

    /// Runs `cmd` in a running container and collects its output.
    pub async fn exec(&self, id: &str, cmd: &[String]) -> Result<ExecOutput> {
        use futures::StreamExt;
        let created: Created = self
            .json(
                Method::POST,
                &format!("/containers/{}/exec", segment(id)),
                Some(&ExecCreate {
                    cmd,
                    attach_stdin: false,
                    attach_stdout: true,
                    attach_stderr: true,
                    tty: false,
                }),
            )
            .await?;
        let start = serde_json::json!({ "Detach": false, "Tty": false });
        let resp = self
            .call(
                Method::POST,
                &format!("/exec/{}/start", segment(&created.id)),
                Some(serde_json::to_vec(&start)?),
                None,
            )
            .await?;
        let resp = check(resp).await?;
        let mut out = ExecOutput::default();
        let mut chunks = Box::pin(stream::demux(Box::pin(stream::chunks(resp.into_body()))));
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            match chunk.stream {
                Output::Stdout => out.stdout.extend_from_slice(&chunk.data),
                Output::Stderr => out.stderr.extend_from_slice(&chunk.data),
            }
        }
        out.exit_code = self.exec_exit_code(&created.id).await?;
        Ok(out)
    }

    /// Starts `cmd` in a running container with its output streamed and,
    /// with `stdin`, its input taken from the caller. Under `tty` the
    /// process gets a terminal, and its output comes unframed as stdout.
    pub async fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> Result<Attached> {
        use futures::StreamExt;
        let created: Created = self
            .json(
                Method::POST,
                &format!("/containers/{}/exec", segment(id)),
                Some(&ExecCreate {
                    cmd,
                    attach_stdin: stdin,
                    attach_stdout: true,
                    attach_stderr: true,
                    tty,
                }),
            )
            .await?;
        // libpod hijacks the connection for the process's streams; asking
        // for an upgrade gets a 101 that hyper hands the connection over on.
        let io = self.connect().await?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.with_upgrades().await {
                tracing::debug!("podman exec connection: {e}");
            }
        });
        let start = serde_json::json!({ "Detach": false, "Tty": tty });
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{API}/exec/{}/start", segment(&created.id)))
            .header(http::header::HOST, "podman")
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::CONNECTION, "Upgrade")
            .header(http::header::UPGRADE, "tcp")
            .body(Full::new(Bytes::from(serde_json::to_vec(&start)?)))
            .map_err(|e| Error::Protocol(e.to_string()))?;
        let resp = sender.send_request(req).await?;
        if resp.status() != StatusCode::SWITCHING_PROTOCOLS {
            let status = check(resp).await?.status();
            return Err(Error::Protocol(format!(
                "exec start answered HTTP {status} instead of taking over the connection"
            )));
        }
        let upgraded = hyper::upgrade::on(resp).await?;
        let (read, write) = tokio::io::split(TokioIo::new(upgraded));
        let raw = Box::pin(stream::read(read));
        let output = if tty {
            raw.map(|b| {
                b.map(|data| Chunk {
                    stream: Output::Stdout,
                    data,
                })
            })
            .boxed()
        } else {
            stream::demux(raw).boxed()
        };
        Ok(Attached {
            id: created.id,
            output,
            stdin: stdin.then(|| Box::pin(write) as std::pin::Pin<Box<dyn AsyncWrite + Send>>),
        })
    }

    /// Sets the terminal size of an exec session started with a tty.
    pub async fn exec_resize(&self, exec_id: &str, width: u16, height: u16) -> Result<()> {
        let path = format!(
            "/exec/{}/resize?{}",
            segment(exec_id),
            query(&[("h", &height.to_string()), ("w", &width.to_string())])
        );
        self.no_content(Method::POST, &path).await
    }

    /// The exit code of an exec session whose output has ended.
    pub async fn exec_exit_code(&self, exec_id: &str) -> Result<i32> {
        // The output ends when the process does, but libpod may record the
        // exit code a moment later.
        for _ in 0..50 {
            let inspect: ExecInspect = self
                .json::<(), _>(
                    Method::GET,
                    &format!("/exec/{}/json", segment(exec_id)),
                    None,
                )
                .await?;
            if !inspect.running {
                return Ok(inspect.exit_code);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(Error::Protocol(
            "exec output ended but the process is still running".into(),
        ))
    }

    /// A container's output. With `follow`, the stream stays open until the
    /// container stops.
    pub async fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<impl Stream<Item = Result<Chunk>> + Send + use<>> {
        let tail = tail.map_or("all".to_owned(), |n| n.to_string());
        let path = format!(
            "/containers/{}/logs?{}",
            segment(id),
            query(&[
                ("stdout", "true"),
                ("stderr", "true"),
                ("follow", if follow { "true" } else { "false" }),
                ("tail", &tail),
            ])
        );
        let resp = check(self.call(Method::GET, &path, None, None).await?).await?;
        Ok(stream::demux(Box::pin(stream::chunks(resp.into_body()))))
    }

    /// Container events from now on, for containers carrying every one of
    /// `labels`.
    pub async fn events(
        &self,
        labels: &[(&str, &str)],
    ) -> Result<impl Stream<Item = Result<Event>> + Send + use<>> {
        let mut filters: Vec<(&str, Vec<String>)> = vec![("type", vec!["container".into()])];
        if !labels.is_empty() {
            filters.push((
                "label",
                labels.iter().map(|(k, v)| format!("{k}={v}")).collect(),
            ));
        }
        let filters: serde_json::Map<_, _> = filters
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.into()))
            .collect();
        let path = format!(
            "/events?{}",
            query(&[
                ("stream", "true"),
                ("filters", &serde_json::Value::Object(filters).to_string())
            ])
        );
        let resp = check(self.call(Method::GET, &path, None, None).await?).await?;
        Ok(stream::json_lines(Box::pin(stream::chunks(
            resp.into_body(),
        ))))
    }

    async fn no_content(&self, method: Method, path: &str) -> Result<()> {
        let resp = self.call(method, path, None, None).await?;
        check(resp).await.map(drop)
    }

    async fn json<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let body = body.map(serde_json::to_vec).transpose()?;
        let resp = self.call(method, path, body, None).await?;
        let body = stream::collect(check(resp).await?.into_body()).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        registry_auth: Option<&str>,
    ) -> Result<Response<Incoming>> {
        let io = self.connect().await?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("podman connection: {e}");
            }
        });
        let mut req = Request::builder()
            .method(method)
            .uri(format!("{API}{path}"))
            .header(http::header::HOST, "podman");
        if body.is_some() {
            req = req.header(http::header::CONTENT_TYPE, "application/json");
        }
        if let Some(auth) = registry_auth {
            req = req.header("X-Registry-Auth", auth);
        }
        let req = req
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|e| Error::Protocol(e.to_string()))?;
        Ok(sender.send_request(req).await?)
    }

    async fn connect(&self) -> Result<Box<dyn Io>> {
        let err = |source| Error::Connect {
            endpoint: self.endpoint.to_string(),
            source,
        };
        match &self.endpoint {
            Endpoint::Tcp(addr) => Ok(Box::new(
                tokio::net::TcpStream::connect(addr).await.map_err(err)?,
            )),
            #[cfg(unix)]
            Endpoint::Unix(path) => Ok(Box::new(
                tokio::net::UnixStream::connect(path).await.map_err(err)?,
            )),
            #[cfg(not(unix))]
            Endpoint::Unix(_) => Err(err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix sockets need a Unix host",
            ))),
        }
    }
}

/// Passes 2xx and 304 (already started / stopped) through; turns anything
/// else into [`Error::Api`] with libpod's message.
async fn check(resp: Response<Incoming>) -> Result<Response<Incoming>> {
    let status = resp.status();
    if status.is_success() || status == StatusCode::NOT_MODIFIED {
        return Ok(resp);
    }
    let body = stream::collect(resp.into_body()).await?;
    let message = serde_json::from_slice::<ErrorBody>(&body)
        .ok()
        .map(|e| e.message)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| String::from_utf8_lossy(&body).trim().to_owned());
    Err(Error::Api {
        status: status.as_u16(),
        message,
    })
}

fn query(pairs: &[(&str, &str)]) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

/// A name, id or image reference as a path segment. libpod routes image
/// references with their `/`, `:` and `@` unescaped; anything else unusual
/// is percent-encoded.
fn segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~/:@".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn label_filter(labels: &[(&str, &str)]) -> String {
    let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    query(&[(
        "filters",
        &serde_json::json!({ "label": labels }).to_string(),
    )])
}
