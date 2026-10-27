//! The Legion-owned Podman instance (DESIGN.md §6.1): `podman system
//! service` with its own root, runroot **and** tmpdir, so it never shares
//! state with the operator's containers, restarted if it exits.
//!
//! Killing the service does not touch containers: each runs under its own
//! conmon, and the service is only the API in front of Podman's database.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::io::AsyncBufReadExt;
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{Endpoint, Podman};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceConfig {
    pub program: PathBuf,
    /// Images and container storage; persistent.
    pub root: PathBuf,
    /// Runtime state; may be on tmpfs.
    pub runroot: PathBuf,
    /// Podman's temp directory, which holds the marker it uses to detect a
    /// reboot. Sharing it with another Podman store corrupts that store's
    /// view of its containers.
    pub tmpdir: PathBuf,
    pub socket: PathBuf,
    /// OCI runtime, by name or path; `ocijail` unless overridden.
    pub runtime: Option<String>,
    pub storage_driver: Option<String>,
    /// `--storage-opt` values, e.g. `zfs.fsname=zroot/legion/podman`.
    pub storage_opts: Vec<String>,
}

impl ServiceConfig {
    /// Legion's layout: storage under `state_dir`, everything else under
    /// `run_dir`.
    pub fn new(state_dir: &Path, run_dir: &Path) -> Self {
        ServiceConfig {
            program: "podman".into(),
            root: state_dir.join("podman"),
            runroot: run_dir.join("podman"),
            tmpdir: run_dir.join("podman-tmp"),
            socket: run_dir.join("podman.sock"),
            runtime: Some("ocijail".to_owned()),
            storage_driver: None,
            storage_opts: Vec::new(),
        }
    }

    pub fn endpoint(&self) -> Endpoint {
        Endpoint::Unix(self.socket.clone())
    }

    pub fn args(&self) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec![
            "--root".into(),
            self.root.clone().into(),
            "--runroot".into(),
            self.runroot.clone().into(),
            "--tmpdir".into(),
            self.tmpdir.clone().into(),
        ];
        if let Some(runtime) = &self.runtime {
            args.extend(["--runtime".into(), runtime.into()]);
        }
        if let Some(driver) = &self.storage_driver {
            args.extend(["--storage-driver".into(), driver.into()]);
        }
        for opt in &self.storage_opts {
            args.extend(["--storage-opt".into(), opt.into()]);
        }
        let mut socket = OsString::from("unix://");
        socket.push(&self.socket);
        args.extend([
            "system".into(),
            "service".into(),
            "--time".into(),
            "0".into(),
            socket,
        ]);
        args
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("starting {program}: {source}")]
    Spawn {
        program: String,
        source: std::io::Error,
    },
    #[error("podman service exited during startup ({0})")]
    Exited(ExitStatus),
    #[error("podman service not ready after {0:?}: {1}")]
    NotReady(Duration, crate::Error),
}

/// A running service, restarted with backoff whenever it exits. Dropping it
/// kills the service.
pub struct PodmanService {
    client: Podman,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

/// Backoff between restarts, reset once the service has run this long.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const STABLE: Duration = Duration::from_secs(60);

impl PodmanService {
    /// Starts the service and waits until it answers.
    pub async fn start(
        config: ServiceConfig,
        ready_timeout: Duration,
    ) -> Result<Self, ServiceError> {
        let client = Podman::new(config.endpoint());
        let mut child = spawn(&config)?;
        let deadline = Instant::now() + ready_timeout;
        loop {
            let err = match client.ping().await {
                Ok(version) => {
                    tracing::info!(%version, socket = %config.socket.display(), "podman service ready");
                    break;
                }
                Err(e) => e,
            };
            if let Some(status) = child.try_wait().map_err(|source| ServiceError::Spawn {
                program: config.program.display().to_string(),
                source,
            })? {
                return Err(ServiceError::Exited(status));
            }
            if Instant::now() >= deadline {
                return Err(ServiceError::NotReady(ready_timeout, err));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(supervise(config, child, stopped));
        Ok(PodmanService {
            client,
            stop,
            task: Some(task),
        })
    }

    pub fn client(&self) -> &Podman {
        &self.client
    }

    /// Stops the service and waits for it to exit.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for PodmanService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Stops the service recorded in `pidfile` if it is still running and
/// serving `socket`: the pid alone could be another process's by now.
fn stop_stale(pidfile: &std::path::Path, socket: &std::path::Path) {
    let Some(pid) = std::fs::read_to_string(pidfile)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    let pid = pid.to_string();
    let serving = || {
        std::process::Command::new("ps")
            .args(["-o", "command=", "-p", &pid])
            .output()
            .is_ok_and(|o| {
                String::from_utf8_lossy(&o.stdout).contains(&socket.display().to_string())
            })
    };
    if !serving() {
        return;
    }
    tracing::info!(pid, "stopping the podman service a previous run left");
    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    for _ in 0..50 {
        if !serving() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = std::process::Command::new("kill")
        .args(["-KILL", &pid])
        .status();
}

fn spawn(config: &ServiceConfig) -> Result<Child, ServiceError> {
    let err = |source| ServiceError::Spawn {
        program: config.program.display().to_string(),
        source,
    };
    for dir in [&config.root, &config.runroot, &config.tmpdir] {
        std::fs::create_dir_all(dir).map_err(err)?;
    }
    if let Some(parent) = config.socket.parent() {
        std::fs::create_dir_all(parent).map_err(err)?;
    }
    // A service left by a killed daemon would keep running beside the new
    // one, and its socket would make the new one fail to bind.
    let pidfile = config.socket.with_extension("pid");
    stop_stale(&pidfile, &config.socket);
    let _ = std::fs::remove_file(&config.socket);
    let mut child = Command::new(&config.program)
        .args(config.args())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(err)?;
    if let Some(pid) = child.id() {
        let _ = std::fs::write(
            &pidfile,
            format!(
                "{pid}
"
            ),
        );
    }
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(target: "podman", "{line}");
            }
        });
    }
    Ok(child)
}

async fn supervise(config: ServiceConfig, mut child: Child, mut stop: watch::Receiver<bool>) {
    let mut backoff = BACKOFF_MIN;
    let mut started = Instant::now();
    loop {
        let status = tokio::select! {
            status = child.wait() => status,
            _ = stopped(&mut stop) => {
                let _ = child.kill().await;
                return;
            }
        };
        if started.elapsed() >= STABLE {
            backoff = BACKOFF_MIN;
        }
        tracing::warn!(?status, "podman service exited; restarting in {backoff:?}");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stopped(&mut stop) => return,
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
        match spawn(&config) {
            Ok(c) => {
                child = c;
                started = Instant::now();
            }
            Err(e) => tracing::error!("{e}"),
        }
    }
}

/// Resolves once a stop is requested (or the service handle is gone).
async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_keep_all_three_paths_separate() {
        let c = ServiceConfig::new(Path::new("/var/db/legion"), Path::new("/var/run/legion"));
        let args: Vec<String> = c
            .args()
            .iter()
            .map(|a| a.to_string_lossy().replace('\\', "/"))
            .collect();
        assert_eq!(
            args,
            [
                "--root",
                "/var/db/legion/podman",
                "--runroot",
                "/var/run/legion/podman",
                "--tmpdir",
                "/var/run/legion/podman-tmp",
                "--runtime",
                "ocijail",
                "system",
                "service",
                "--time",
                "0",
                "unix:///var/run/legion/podman.sock",
            ]
        );
    }
}
