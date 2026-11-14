//! Builds (CONSOLE.md phase 5). A node the operator labels a builder runs
//! jailer on the host when a manager asks, over a session as a Volume's
//! export is: it fetches the source, builds and pushes, and sends jailer's
//! output as it comes, then what it pushed. Its `RUN` steps run in
//! jailer's build jails; pods never make jails.
//!
//! The manager's end of the session closing ends the build: jailer is
//! stopped and the build's directory removed.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use futures::StreamExt;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{BuildOptions, BuildResult, SessionFrame};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

/// The longest a build may take, fetching and pushing included.
const BUILD_LIMIT: Duration = Duration::from_secs(2 * 3600);

/// How this node builds.
#[derive(Debug, Clone)]
pub struct Builder {
    /// jailer and the arguments put before each of its commands
    /// (`--state-dir`, say); `jailer` on the PATH by default.
    pub jailer: Vec<String>,
    /// Where each build's source and files are kept while it runs.
    pub work: PathBuf,
}

impl Builder {
    pub fn new(work: impl Into<PathBuf>) -> Builder {
        Builder {
            jailer: vec!["jailer".to_owned()],
            work: work.into(),
        }
    }
}

/// What the build's record says was pushed (jailer's
/// docs/RELEASE-RECORD.md, schema 1).
#[derive(Deserialize)]
struct Record {
    #[serde(default)]
    published: Vec<Published>,
}

#[derive(Deserialize)]
struct Published {
    digest: String,
    pinned: String,
}

pub(crate) async fn run(
    b: &Builder,
    id: &str,
    opts: BuildOptions,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    if b.jailer.is_empty() {
        return Err(Status::failed_precondition(
            "this node builds nothing: its agent was given no jailer",
        ));
    }
    let short: String = id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(16)
        .collect();
    let dir = b.work.join(format!("build-{short}"));
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| Status::internal(format!("{}: {e}", dir.display())))?;
    let result = tokio::time::timeout(BUILD_LIMIT, build(b, &dir, &short, opts, tx, inbound))
        .await
        .unwrap_or_else(|_| {
            Err(Status::deadline_exceeded(format!(
                "the build took longer than {} h",
                BUILD_LIMIT.as_secs() / 3600
            )))
        });
    let _ = tokio::fs::remove_dir_all(&dir).await;
    result
}

async fn build(
    b: &Builder,
    dir: &Path,
    short: &str,
    opts: BuildOptions,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let io = |what: &str, e: std::io::Error| Status::internal(format!("{what}: {e}"));
    // The context: the repository's directory, or the Containerfile alone.
    let context = if opts.git_url.is_empty() {
        let ctx = dir.join("context");
        tokio::fs::create_dir_all(&ctx)
            .await
            .map_err(|e| io("the build's context", e))?;
        tokio::fs::write(ctx.join("Containerfile"), &opts.containerfile)
            .await
            .map_err(|e| io("the Containerfile", e))?;
        ctx
    } else {
        let src = dir.join("source");
        let mut args = vec!["clone".to_owned(), "--depth".into(), "1".into()];
        if !opts.git_ref.is_empty() {
            args.extend(["--branch".to_owned(), opts.git_ref.clone()]);
        }
        args.extend([
            "--".to_owned(),
            opts.git_url.clone(),
            src.display().to_string(),
        ]);
        step("git clone", "git", &args, &[], tx, inbound).await?;
        if opts.git_path.is_empty() {
            src
        } else {
            src.join(&opts.git_path)
        }
    };
    let mut env = Vec::new();
    if !opts.auth.is_empty() {
        let auth = dir.join("auth.json");
        write_private(&auth, &opts.auth)
            .await
            .map_err(|e| io("the registry's login", e))?;
        env.push(("REGISTRY_AUTH_FILE".to_owned(), auth.display().to_string()));
    }
    let record = dir.join("record.json");
    let tag = format!("legion-build/{short}:1");
    let (program, before) = b.jailer.split_first().expect("checked");
    let mut common: Vec<String> = before.to_vec();
    for p in &opts.plain_http {
        common.extend(["--plain-http".to_owned(), p.clone()]);
    }
    let with = |more: &[&str]| -> Vec<String> {
        let mut a = common.clone();
        a.extend(more.iter().map(|s| s.to_string()));
        a
    };
    let record_arg = record.display().to_string();
    let context_arg = context.display().to_string();
    step(
        "jailer build",
        program,
        &with(&["build", "-t", &tag, "--record", &record_arg, &context_arg]),
        &env,
        tx,
        inbound,
    )
    .await?;
    step(
        "jailer push",
        program,
        &with(&["push", "--record", &record_arg, &tag, &opts.image]),
        &env,
        tx,
        inbound,
    )
    .await?;
    let r: Record = serde_json::from_slice(
        &tokio::fs::read(&record)
            .await
            .map_err(|e| io("jailer's record", e))?,
    )
    .map_err(|e| Status::internal(format!("jailer's record: {e}")))?;
    let pushed = r
        .published
        .last()
        .ok_or_else(|| Status::internal("jailer's record names nothing pushed"))?;
    let _ = tx
        .send(frame(Frame::Built(BuildResult {
            digest: pushed.digest.clone(),
            pinned: pushed.pinned.clone(),
        })))
        .await;
    let _ = tx.send(frame(Frame::ExitCode(0))).await;
    Ok(())
}

/// Writes `data` readable by root alone.
async fn write_private(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    std::io::Write::write_all(&mut f, data)
}

/// Runs `program` with `args`, its output sent as it comes; fails if it
/// does, or if the manager's end closes (it is stopped then).
async fn step(
    what: &str,
    program: &str,
    args: &[String],
    env: &[(String, String)],
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    let shown = std::iter::once(program.to_owned())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    let _ = tx
        .send(frame(Frame::Stdout(format!("$ {shown}\n").into_bytes())))
        .await;
    let mut child = Command::new(program)
        .args(args)
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| Status::failed_precondition(format!("running {program} on this node: {e}")))?;
    let out = pass(child.stdout.take().expect("piped"), tx.clone(), false);
    let err = pass(child.stderr.take().expect("piped"), tx.clone(), true);
    let ended = async {
        loop {
            match inbound.next().await {
                Some(Ok(_)) => continue,
                _ => return,
            }
        }
    };
    let status = tokio::select! {
        s = async {
            let (s, (), ()) = tokio::join!(child.wait(), out, err);
            s
        } => s.map_err(|e| Status::internal(format!("{program}: {e}")))?,
        () = ended => {
            let _ = child.kill().await;
            return Err(Status::cancelled("the manager's end closed: the build is stopped"));
        }
    };
    if status.success() {
        Ok(())
    } else {
        Err(Status::aborted(format!("{what} failed ({status})")))
    }
}

/// Sends what `from` writes as output frames, until it ends.
async fn pass(
    mut from: impl tokio::io::AsyncRead + Unpin,
    tx: mpsc::Sender<SessionFrame>,
    stderr: bool,
) {
    let mut buf = vec![0; 16 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                let data = buf[..n].to_vec();
                let f = if stderr {
                    Frame::Stderr(data)
                } else {
                    Frame::Stdout(data)
                };
                if tx.send(frame(f)).await.is_err() {
                    return;
                }
            }
        }
    }
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}
