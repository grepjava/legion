//! Builds (CONSOLE.md phase 5). A node the operator labels a builder runs
//! `legion local build` on the host when a manager asks, over a session as
//! a Volume's export is: it fetches the source, builds and pushes, and
//! sends the builder's output as it comes, then what it pushed. Its `RUN`
//! steps run in the builder's own jails; pods never make jails.
//!
//! The manager's end of the session closing ends the build: the builder is
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
    /// The builder program and the arguments put before each of its
    /// commands (`local --state-dir ...`, say); `legion local` by default.
    pub command: Vec<String>,
    /// Where each build's source and files are kept while it runs.
    pub work: PathBuf,
    /// pkg, which audits images' packages, and the arguments put before
    /// its commands; `pkg` on the PATH by default.
    pub pkg: Vec<String>,
}

/// The builder with the arguments put before each of its commands, reading
/// nothing.
pub(crate) fn builder(b: &Builder) -> Command {
    let (program, before) = b.command.split_first().expect("checked");
    let mut c = Command::new(program);
    c.args(before).stdin(std::process::Stdio::null());
    c
}

impl Builder {
    /// The ZFS dataset the builder keeps its images and build cache under,
    /// as its arguments name it (`--zfs-root`); none when they do not.
    pub fn zfs_root(&self) -> Option<String> {
        let mut args = self.command.iter().skip(1);
        while let Some(a) = args.next() {
            if a == "--zfs-root" {
                return args.next().cloned();
            }
            if let Some(v) = a.strip_prefix("--zfs-root=") {
                return Some(v.to_owned());
            }
        }
        None
    }

    pub fn new(work: impl Into<PathBuf>) -> Builder {
        Builder {
            command: vec!["legion".to_owned(), "local".to_owned()],
            work: work.into(),
            pkg: vec!["pkg".to_owned()],
        }
    }
}

/// What the build's record says was pushed (docs/RELEASE-RECORD.md,
/// schema 1).
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
    if b.command.is_empty() {
        return Err(Status::failed_precondition(
            "this node builds nothing: its agent was given no builder",
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
    forget(b, &tag(&short), result.is_ok()).await;
    result
}

/// The name a build gives its image in the builder's store.
fn tag(short: &str) -> String {
    format!("legion-build/{short}:1")
}

/// The build's image forgotten in the builder's store, pushed or not: the
/// registry has it, and `legion local rmi` destroys the layers nothing else
/// uses. The base images stay, for the next build, and the builder bounds its build
/// cache itself. A build that failed before tagging has nothing to forget.
async fn forget(b: &Builder, tag: &str, built: bool) {
    match builder(b).args(["rmi", tag]).output().await {
        Ok(o) if o.status.success() => {}
        Ok(o) if !built => tracing::debug!(
            "legion local rmi {tag} after the failed build: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Ok(o) => tracing::warn!(
            "legion local rmi {tag}: {}; it stays in the builder's store",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Err(e) => tracing::warn!("legion local rmi {tag}: {e}"),
    }
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
    let key = dir.join("sign.key");
    let signing = !opts.sign_key.is_empty();
    if signing {
        write_private(&key, &opts.sign_key)
            .await
            .map_err(|e| io("the signing key", e))?;
    }
    let key_arg = key.display().to_string();
    let record = dir.join("record.json");
    let tag = tag(short);
    let (program, before) = b.command.split_first().expect("checked");
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
        "legion local build",
        program,
        &with(&["build", "-t", &tag, "--record", &record_arg, &context_arg]),
        &env,
        tx,
        inbound,
    )
    .await?;
    let mut push = vec!["push", "--record", &record_arg];
    if signing {
        push.extend(["--sign-key", &key_arg]);
    }
    push.extend([tag.as_str(), &opts.image]);
    step(
        "legion local push",
        program,
        &with(&push),
        &env,
        tx,
        inbound,
    )
    .await?;
    let r: Record = serde_json::from_slice(
        &tokio::fs::read(&record)
            .await
            .map_err(|e| io("the build's record", e))?,
    )
    .map_err(|e| Status::internal(format!("the build's record: {e}")))?;
    let pushed = r
        .published
        .last()
        .ok_or_else(|| Status::internal("the build's record names nothing pushed"))?;
    if signing {
        // Provenance from the record, and the SBOM of the image built.
        for (kind, more) in [("provenance", None), ("sbom", Some(tag.as_str()))] {
            let mut args = vec!["attest", "--key", &key_arg, "--type", kind];
            args.extend(["--record", &record_arg]);
            if let Some(image) = more {
                args.extend(["--image", image]);
            }
            args.push(&pushed.pinned);
            step(
                &format!("legion local attest {kind}"),
                program,
                &with(&args),
                &env,
                tx,
                inbound,
            )
            .await?;
        }
    }
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
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut f = options.open(path)?;
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn the_builders_zfs_root_is_read_from_its_arguments() {
        let with = |args: &[&str]| Builder {
            command: args.iter().map(|a| (*a).to_owned()).collect(),
            work: PathBuf::new(),
            pkg: Vec::new(),
        };
        let root = |b: Builder| b.zfs_root();
        assert_eq!(root(with(&["legion", "local"])), None);
        assert_eq!(
            root(with(&[
                "/usr/local/bin/legion",
                "local",
                "--zfs-root",
                "zroot/legion-local"
            ])),
            Some("zroot/legion-local".to_owned())
        );
        assert_eq!(
            root(with(&[
                "legion",
                "local",
                "--state-dir",
                "/x",
                "--zfs-root=tank/j"
            ])),
            Some("tank/j".to_owned())
        );
        // The first word is the program, not one of its arguments.
        assert_eq!(root(with(&["--zfs-root", "legion"])), None);
    }

    /// A stand-in builder writing its arguments down, failing as told.
    fn stand_in(dir: &Path, exit: i32) -> Builder {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("legion");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho \"$@\" >> {}/args\nexit {exit}\n",
                dir.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        Builder {
            command: vec![
                script.display().to_string(),
                "local".into(),
                "--state-dir".into(),
                "/var/db/legion-local".into(),
            ],
            work: dir.to_owned(),
            pkg: vec!["pkg".to_owned()],
        }
    }

    #[tokio::test]
    async fn the_built_image_is_forgotten_with_the_builder_arguments_given() {
        let dir = tempfile::tempdir().unwrap();
        forget(&stand_in(dir.path(), 0), &tag("abc123"), true).await;
        let args = std::fs::read_to_string(dir.path().join("args")).unwrap();
        assert_eq!(
            args.trim(),
            "local --state-dir /var/db/legion-local rmi legion-build/abc123:1"
        );
        // Nothing to forget after a failed build: said, and no harm done.
        forget(&stand_in(dir.path(), 1), &tag("def456"), false).await;
        let args = std::fs::read_to_string(dir.path().join("args")).unwrap();
        assert_eq!(args.lines().count(), 2, "{args}");
    }
}
