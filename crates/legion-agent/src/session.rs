//! Sessions users open with this node's tasks (DESIGN.md §4.2): `logs`,
//! `exec` and `port-forward`. A manager offers each one over the agent's
//! `Sessions` stream with it, after checking the user's rights; the agent
//! attaches to it at that manager and runs it against Podman or the pod.
//!
//! The manager's end of an attached session closing means the user is
//! gone: whatever the session runs is dropped. Exec and port-forward carry
//! input in one direction and output in the other independently, so a
//! process that stops reading never stalls its output.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use legion_podman::{Chunk, Output};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{ExecOptions, LogsOptions, SessionError, SessionFrame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::control::Offer;
use crate::pod::{LABEL_CONTAINER, LABEL_NODE, LABEL_ROLE, LABEL_TASK};
use crate::runtime::Runtime;

/// How long connecting to a pod for port-forward may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a finished session waits for the manager to take its last
/// frames.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs every session offered, until the offers end.
pub(crate) async fn serve<R: Runtime>(
    runtime: Arc<R>,
    node: String,
    kept: Option<PathBuf>,
    mut offers: mpsc::Receiver<Offer>,
) {
    while let Some(offer) = offers.recv().await {
        tokio::spawn(run(runtime.clone(), node.clone(), kept.clone(), offer));
    }
}

async fn run<R: Runtime>(runtime: Arc<R>, node: String, kept: Option<PathBuf>, offer: Offer) {
    let id = offer.offer.id.clone();
    let (tx, rx) = mpsc::channel(16);
    let _ = tx.send(frame(Frame::Attach(id.clone()))).await;
    let mut inbound = match offer.attach(rx).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(session = %id, "attaching to a session: {e}");
            return;
        }
    };
    let o = offer.offer;
    let open = o.open.unwrap_or_default();
    let result = match open.action {
        Some(Action::Logs(opts)) => {
            match container(runtime.as_ref(), &node, &o.task_uid, &open.container).await {
                Ok(c) => logs(runtime.as_ref(), &c, opts, &tx, &mut inbound).await,
                Err(e) if e.code() == tonic::Code::NotFound => {
                    match saved(kept, &o.task_uid, &open.container, opts.tail).await {
                        Some(lines) => send_all(lines, &tx).await,
                        None => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        }
        Some(Action::Exec(opts)) => {
            match container(runtime.as_ref(), &node, &o.task_uid, &open.container).await {
                Ok(c) => exec(runtime, &c, opts, &tx, &mut inbound).await,
                Err(e) => Err(e),
            }
        }
        Some(Action::Port(port)) => forward(&o.pod_ip, port, &tx, &mut inbound).await,
        None => Err(Status::invalid_argument("the session has no action")),
    };
    if let Err(e) = result {
        tracing::debug!(session = %id, task = %open.task, "session failed: {}", e.message());
        let _ = tx
            .send(frame(Frame::Error(SessionError {
                code: e.code() as i32,
                message: e.message().to_owned(),
            })))
            .await;
    }
    // Dropping the manager's stream would reset the call and lose frames
    // not yet sent: end ours, and let the manager end its.
    drop(tx);
    let drained = async { while let Some(Ok(_)) = inbound.next().await {} };
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, drained).await;
}

/// The Podman id of the task's container `name`.
async fn container<R: Runtime>(
    runtime: &R,
    node: &str,
    uid: &str,
    name: &str,
) -> Result<String, Status> {
    let found = runtime
        .list(&[
            (LABEL_NODE, node),
            (LABEL_TASK, uid),
            (LABEL_CONTAINER, name),
        ])
        .await
        .map_err(podman)?;
    found
        .into_iter()
        .find(|c| c.labels.get(LABEL_ROLE).map(String::as_str) != Some("infra"))
        .map(|c| c.id)
        .ok_or_else(|| {
            Status::not_found(format!(
                "container {name} has not been created on this node"
            ))
        })
}

async fn logs<R: Runtime>(
    runtime: &R,
    id: &str,
    opts: LogsOptions,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let tail = opts.tail.map(|n| n as usize);
    let mut output = runtime.logs(id, opts.follow, tail).await.map_err(podman)?;
    loop {
        tokio::select! {
            chunk = output.next() => match chunk {
                Some(Ok(c)) => if tx.send(output_frame(c)).await.is_err() {
                    return Ok(());
                },
                Some(Err(e)) => return Err(podman(e)),
                None => return Ok(()),
            },
            f = inbound.next() => if !matches!(f, Some(Ok(_))) {
                return Ok(());
            },
        }
    }
}

/// The output kept of a container whose pod is gone.
async fn saved(
    root: Option<PathBuf>,
    uid: &str,
    container: &str,
    tail: Option<u32>,
) -> Option<Vec<Chunk>> {
    let (uid, container) = (uid.to_owned(), container.to_owned());
    let tail = tail.map(|n| n as usize);
    tokio::task::spawn_blocking(move || crate::output::read(&root?, &uid, &container, tail))
        .await
        .ok()?
}

async fn send_all(lines: Vec<Chunk>, tx: &mpsc::Sender<SessionFrame>) -> Result<(), Status> {
    for c in lines {
        if tx.send(output_frame(c)).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn exec<R: Runtime>(
    runtime: Arc<R>,
    id: &str,
    opts: ExecOptions,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    if opts.command.is_empty() {
        return Err(Status::invalid_argument("exec needs a command"));
    }
    let attached = runtime
        .exec_attach(id, &opts.command, opts.stdin, opts.tty)
        .await
        .map_err(podman)?;
    let exec_id = attached.id.clone();
    if let (true, Some(size)) = (opts.tty, opts.size) {
        resize(runtime.as_ref(), &exec_id, size.width, size.height).await;
    }
    // Output, then the exit code, in a task of its own.
    let mut output = attached.output;
    let out = {
        let tx = tx.clone();
        let runtime = runtime.clone();
        let exec_id = exec_id.clone();
        tokio::spawn(async move {
            while let Some(chunk) = output.next().await {
                if tx.send(output_frame(chunk.map_err(podman)?)).await.is_err() {
                    return Ok(());
                }
            }
            let code = runtime.exec_exit_code(&exec_id).await.map_err(podman)?;
            let _ = tx.send(frame(Frame::ExitCode(code))).await;
            Ok(())
        })
    };
    // Input to the process: `None` closes its stdin.
    let (input, mut input_rx) = mpsc::channel::<Option<Bytes>>(16);
    if let Some(mut stdin) = attached.stdin {
        tokio::spawn(async move {
            while let Some(data) = input_rx.recv().await {
                let Some(data) = data else {
                    let _ = stdin.shutdown().await;
                    return;
                };
                if stdin.write_all(&data).await.is_err() {
                    return;
                }
            }
        });
    }
    let mut out = std::pin::pin!(out);
    loop {
        tokio::select! {
            done = &mut out => return done.unwrap_or(Ok(())),
            f = inbound.next() => match f {
                Some(Ok(SessionFrame { frame: Some(f) })) => match f {
                    Frame::Input(data) => {
                        let _ = input.send(Some(data.into())).await;
                    }
                    Frame::InputClosed(_) => {
                        let _ = input.send(None).await;
                    }
                    Frame::Resize(size) if opts.tty => {
                        resize(runtime.as_ref(), &exec_id, size.width, size.height).await;
                    }
                    _ => {}
                },
                Some(Ok(_)) => {}
                // The user is gone; the process loses its streams.
                _ => {
                    out.abort();
                    return Ok(());
                }
            },
        }
    }
}

async fn resize<R: Runtime>(runtime: &R, exec_id: &str, width: u32, height: u32) {
    let clamp = |n: u32| u16::try_from(n).unwrap_or(u16::MAX);
    if let Err(e) = runtime
        .exec_resize(exec_id, clamp(width), clamp(height))
        .await
    {
        tracing::debug!("resizing exec {exec_id}: {e}");
    }
}

/// One forwarded connection to `port` on the pod.
async fn forward(
    pod_ip: &str,
    port: u32,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let ip = pod_ip
        .parse::<std::net::IpAddr>()
        .map_err(|_| Status::failed_precondition("the task has no address yet"))?;
    let port = u16::try_from(port)
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| Status::invalid_argument(format!("port {port} is out of range")))?;
    let addr = SocketAddr::new(ip, port);
    let stream =
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(Status::unavailable(format!("connecting to {addr}: {e}"))),
            Err(_) => {
                return Err(Status::unavailable(format!(
                    "connecting to {addr}: timed out"
                )));
            }
        };
    let (mut read, mut write) = stream.into_split();
    let from_pod = {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match read.read(&mut buf).await {
                    Ok(0) => return Ok(()),
                    Ok(n) => {
                        let data = Frame::Stdout(buf[..n].to_vec());
                        if tx.send(frame(data)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => return Err(Status::unavailable(format!("reading from {addr}: {e}"))),
                }
            }
        })
    };
    let mut from_pod = std::pin::pin!(from_pod);
    let mut sending = true;
    loop {
        tokio::select! {
            done = &mut from_pod => return done.unwrap_or(Ok(())),
            f = inbound.next() => match f {
                Some(Ok(SessionFrame { frame: Some(Frame::Input(data)) })) if sending => {
                    if write.write_all(&data).await.is_err() {
                        sending = false;
                    }
                }
                Some(Ok(SessionFrame { frame: Some(Frame::InputClosed(_)) })) => {
                    let _ = write.shutdown().await;
                    sending = false;
                }
                Some(Ok(_)) => {}
                _ => {
                    from_pod.abort();
                    return Ok(());
                }
            },
        }
    }
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

fn output_frame(c: Chunk) -> SessionFrame {
    frame(match c.stream {
        Output::Stdout => Frame::Stdout(c.data.to_vec()),
        Output::Stderr => Frame::Stderr(c.data.to_vec()),
    })
}

fn podman(e: legion_podman::Error) -> Status {
    if e.is_not_found() {
        Status::not_found(e.to_string())
    } else if e.is_wrong_state() {
        Status::failed_precondition(format!("the container is not running ({e})"))
    } else {
        Status::unavailable(e.to_string())
    }
}
