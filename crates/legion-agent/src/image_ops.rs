//! Node-local OCI image operations (`list`, `pull`, `push`, `remove`,
//! `prune`, `export`, `load`), served over a manager-initiated session.

use std::path::PathBuf;

use futures::StreamExt;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{CachedNodeImage, ImageNodeAction, ImageNodeResult, SessionFrame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::Builder;
use crate::runtime::{CachedImage, Runtime};

const CHUNK: usize = 64 << 10;

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(builder: &Builder, id: &str) -> Result<TempDir, Status> {
    let short: String = id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(16)
        .collect();
    let dir = builder.work.join(format!("image-{short}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)
        .map_err(|e| Status::internal(format!("creating {}: {e}", dir.display())))?;
    Ok(TempDir(dir))
}

fn to_pb(i: CachedImage) -> CachedNodeImage {
    CachedNodeImage {
        reference: i.reference,
        digest: i.digest,
        os: i.os,
        arch: i.arch,
        layers: i.layers,
    }
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

fn runtime_err(e: crate::runtime::Error) -> Status {
    if e.is_not_found() {
        Status::not_found(e.to_string())
    } else {
        Status::failed_precondition(e.to_string())
    }
}

/// FreeBSD's VuXML database as `pkg audit` keeps it, fetched again when
/// older than [`VUXML_AGE`].
const VUXML: &str = "/var/db/pkg/vuln.xml";
const VUXML_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// What `pkg audit` says of `packages`, as JSON (`--raw=json-compact`).
async fn audit(builder: &Builder, packages: &[(String, String)]) -> Result<String, Status> {
    let (program, before) = builder
        .pkg
        .split_first()
        .ok_or_else(|| Status::failed_precondition("no pkg to audit with"))?;
    let stale = std::fs::metadata(VUXML)
        .and_then(|m| m.modified())
        .map_or(true, |at| at.elapsed().map_or(true, |age| age > VUXML_AGE));
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(before).arg("audit");
    if stale {
        cmd.arg("-F");
    }
    cmd.arg("--raw=json-compact")
        .args(packages.iter().map(|(n, v)| format!("{n}-{v}")))
        .stdin(std::process::Stdio::null());
    let out = cmd
        .output()
        .await
        .map_err(|e| Status::failed_precondition(format!("pkg audit: {e}")))?;
    // 1: something is vulnerable, which is what the JSON says.
    match out.status.code() {
        Some(0 | 1) => Ok(audit_json(&String::from_utf8_lossy(&out.stdout)).to_owned()),
        _ => Err(Status::failed_precondition(format!(
            "pkg audit: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))),
    }
}

/// The JSON in `pkg audit --raw`'s output: with `-F` it first says how the
/// fetch of the vulnerability database went ("vulnxml file up-to-date"),
/// on standard output too. Empty when there is none.
fn audit_json(stdout: &str) -> &str {
    stdout
        .lines()
        .find(|l| l.trim_start().starts_with('{'))
        .unwrap_or("")
}

fn parse_auth(auth: &[u8]) -> Result<Vec<crate::runtime::Login>, Status> {
    if auth.is_empty() {
        return Ok(Vec::new());
    }
    legion_api::login::docker_config(auth)
        .map_err(|e| Status::invalid_argument(format!("registry credentials: {e}")))
}

pub(crate) async fn run<R: Runtime>(
    runtime: &R,
    builder: &Builder,
    id: &str,
    namespace: &str,
    opts: ImageNodeAction,
    tx: &mpsc::Sender<SessionFrame>,
    inbound: &mut Streaming<SessionFrame>,
) -> Result<(), Status> {
    match opts.op.as_str() {
        "list" | "" => {
            let images = runtime
                .cached_images()
                .await
                .map_err(runtime_err)?
                .into_iter()
                .map(to_pb)
                .collect();
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images,
                    removed: 0,
                    digest: String::new(),
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        // jailer's store: layers and blobs no image or build cache entry
        // uses, as `jailer prune` removes them.
        "prune-builds" => {
            let out = crate::build::jailer(builder)
                .arg("prune")
                .output()
                .await
                .map_err(|e| Status::failed_precondition(format!("jailer prune: {e}")))?;
            if !out.status.success() {
                return Err(Status::failed_precondition(format!(
                    "jailer prune: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                )));
            }
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult::default())))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "scan" => {
            if opts.image.trim().is_empty() {
                return Err(Status::invalid_argument("scan needs an image reference"));
            }
            let packages = runtime
                .image_packages(&opts.image)
                .await
                .map_err(runtime_err)?
                .ok_or_else(|| {
                    Status::not_found(format!("{}: not in this node's image store", opts.image))
                })?;
            let audit = if packages.is_empty() {
                String::new()
            } else {
                audit(builder, &packages).await?
            };
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    audit,
                    packages: packages
                        .into_iter()
                        .map(|(n, v)| format!("{n}\t{v}"))
                        .collect(),
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "inspect" => {
            if opts.image.trim().is_empty() {
                return Err(Status::invalid_argument("inspect needs an image reference"));
            }
            let details = runtime
                .image_details(&opts.image)
                .await
                .map_err(runtime_err)?
                .ok_or_else(|| {
                    Status::not_found(format!("{}: not in this node's image store", opts.image))
                })?;
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    config: details.config,
                    layer_sizes: details.layer_sizes,
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "pull" => {
            if opts.image.trim().is_empty() {
                return Err(Status::invalid_argument("pull needs an image reference"));
            }
            let logins = parse_auth(&opts.auth)?;
            let user = if namespace.is_empty() {
                "default"
            } else {
                namespace
            };
            runtime
                .pull(&opts.image, user, &logins)
                .await
                .map_err(runtime_err)?;
            let digest = runtime
                .image_digest(&opts.image)
                .await
                .map_err(runtime_err)?
                .unwrap_or_default();
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images: Vec::new(),
                    removed: 0,
                    digest,
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "push" => {
            if opts.image.trim().is_empty() || opts.target.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "push needs both source image and target reference",
                ));
            }
            let logins = parse_auth(&opts.auth)?;
            let digest = runtime
                .push_image(&opts.image, &opts.target, &logins)
                .await
                .map_err(runtime_err)?;
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images: Vec::new(),
                    removed: 0,
                    digest,
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "remove" => {
            if opts.image.trim().is_empty() {
                return Err(Status::invalid_argument("remove needs an image reference"));
            }
            let removed = u64::from(
                runtime
                    .remove_image(&opts.image)
                    .await
                    .map_err(runtime_err)?,
            );
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images: Vec::new(),
                    removed,
                    digest: String::new(),
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "prune" => {
            let removed = runtime.prune_images().await.map_err(runtime_err)?;
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images: Vec::new(),
                    removed,
                    digest: String::new(),
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "export" => {
            if opts.image.trim().is_empty() {
                return Err(Status::invalid_argument("export needs an image reference"));
            }
            let dir = temp_dir(builder, id)?;
            let archive = dir.0.join("export.tar");
            let tag = (!opts.target.trim().is_empty()).then_some(opts.target.as_str());
            runtime
                .export_image(&opts.image, tag, &archive)
                .await
                .map_err(runtime_err)?;
            let mut file = tokio::fs::File::open(&archive)
                .await
                .map_err(|e| Status::internal(format!("opening exported archive: {e}")))?;
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = file
                    .read(&mut buf)
                    .await
                    .map_err(|e| Status::internal(format!("reading exported archive: {e}")))?;
                if n == 0 {
                    break;
                }
                if tx
                    .send(frame(Frame::Stdout(buf[..n].to_vec())))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        "load" => {
            let dir = temp_dir(builder, id)?;
            let archive = dir.0.join("load.tar");
            let mut file = tokio::fs::File::create(&archive)
                .await
                .map_err(|e| Status::internal(format!("creating archive file: {e}")))?;
            let mut closed = false;
            while let Some(msg) = inbound.next().await {
                match msg? {
                    SessionFrame {
                        frame: Some(Frame::Input(data)),
                    } => {
                        file.write_all(&data)
                            .await
                            .map_err(|e| Status::internal(format!("writing archive: {e}")))?;
                    }
                    SessionFrame {
                        frame: Some(Frame::InputClosed(_)),
                    } => {
                        closed = true;
                        break;
                    }
                    SessionFrame {
                        frame: Some(Frame::Error(e)),
                    } => {
                        return Err(Status::aborted(e.message));
                    }
                    _ => {}
                }
            }
            if !closed {
                return Err(Status::aborted(
                    "the archive upload ended before completing",
                ));
            }
            file.flush()
                .await
                .map_err(|e| Status::internal(format!("flushing archive: {e}")))?;
            drop(file);
            let name = (!opts.image.trim().is_empty()).then_some(opts.image.as_str());
            let loaded = runtime
                .load_image(&archive, name)
                .await
                .map_err(runtime_err)?;
            let digest = loaded.digest.clone();
            let _ = tx
                .send(frame(Frame::Images(ImageNodeResult {
                    images: vec![to_pb(loaded)],
                    removed: 0,
                    digest,
                    ..Default::default()
                })))
                .await;
            let _ = tx.send(frame(Frame::ExitCode(0))).await;
            Ok(())
        }
        other => Err(Status::invalid_argument(format!(
            "unknown image operation {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_audits_json_is_found_after_what_the_fetch_said() {
        // pkg 2.8's `pkg audit -F --raw=json-compact`, as on a lab node.
        let json = r#"{"pkg_count":1,"packages":{"pcre2":{"version":"10.45","issue_count":0,"issues":[]}}}"#;
        assert_eq!(audit_json(&format!("vulnxml file up-to-date\n{json}\n")), json);
        assert_eq!(audit_json(&format!("{json}\n")), json);
        assert_eq!(audit_json("vulnxml file up-to-date\n"), "");
    }
}
