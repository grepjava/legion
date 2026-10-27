//! Against a real Podman on FreeBSD, started by [`PodmanService`] in a
//! scratch directory. Needs root, Podman and ocijail, so it is ignored by
//! default:
//!
//! ```sh
//! sudo LEGION_PODMAN_LAB=/var/tmp/legion-podman-test \
//!     target/debug/deps/live-<hash> --ignored --nocapture
//! ```
//!
//! `LEGION_OCIJAIL` names the OCI runtime to use instead of `ocijail` from
//! `PATH`, e.g. a development build of ocijail.
//!
//! The lab directory gets its own root, runroot and tmpdir, so the host's
//! own Podman containers are not affected. Remove it afterwards with
//! `chflags -R 0` and `rm -rf`: FreeBSD base images contain immutable files.

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use legion_podman::service::{PodmanService, ServiceConfig};
use legion_podman::{ContainerSpec, Output};

const IMAGE: &str = "docker.io/freebsd/freebsd-runtime:15.1";
const LABEL: &str = "legion.test";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs root and Podman; set LEGION_PODMAN_LAB"]
async fn pod_lifecycle() {
    let lab = PathBuf::from(std::env::var("LEGION_PODMAN_LAB").expect("set LEGION_PODMAN_LAB"));
    let run = std::process::id().to_string();
    let mut config = ServiceConfig::new(&lab.join("state"), &lab.join("run"));
    if let Ok(runtime) = std::env::var("LEGION_OCIJAIL") {
        config.runtime = Some(runtime);
    }
    let service = PodmanService::start(config, Duration::from_secs(30))
        .await
        .expect("podman service");
    let podman = service.client().clone();

    if !podman.image_exists(IMAGE).await.unwrap() {
        let id = podman.pull(IMAGE, None).await.unwrap();
        assert!(!id.is_empty());
    }
    assert!(podman.image_exists(IMAGE).await.unwrap());

    // The pod pattern from DESIGN.md §6.2: an infra container with a new
    // VNET, and a container that runs as its child jail.
    let mut infra = ContainerSpec {
        name: format!("test-{run}-infra"),
        image: IMAGE.into(),
        command: vec!["/bin/sleep".into(), "3600".into()],
        stop_timeout: Some(1),
        ..Default::default()
    };
    infra.labels.insert(LABEL.into(), run.clone());
    infra
        .annotations
        .insert("org.freebsd.jail.vnet".into(), "new".into());
    let infra_id = podman.create(&infra).await.unwrap();
    podman.start(&infra_id).await.unwrap();
    podman.start(&infra_id).await.unwrap(); // already running: fine

    let mut events = Box::pin(podman.events(&[(LABEL, &run)]).await.unwrap());

    let mut app = ContainerSpec {
        name: format!("test-{run}-app"),
        image: IMAGE.into(),
        entrypoint: vec!["/bin/sh".into(), "-c".into()],
        command: vec!["echo hello from $GREETING; echo oops >&2; sleep 1; exit 7".into()],
        ..Default::default()
    };
    app.env.insert("GREETING".into(), "legion".into());
    app.labels.insert(LABEL.into(), run.clone());
    app.annotations
        .insert("org.freebsd.parentJail".into(), infra_id.clone());
    let app_id = podman.create(&app).await.unwrap();
    podman.start(&app_id).await.unwrap();
    assert_eq!(podman.wait(&app_id).await.unwrap(), 7);

    let logs: Vec<_> = podman
        .logs(&app_id, false, None)
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await;
    let text = |o| {
        logs.iter()
            .filter(|c| c.stream == o)
            .map(|c| String::from_utf8_lossy(&c.data).into_owned())
            .collect::<String>()
    };
    assert_eq!(text(Output::Stdout), "hello from legion\n");
    assert_eq!(text(Output::Stderr), "oops\n");

    // The app's start and death arrive as events, with its exit code.
    let died = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let ev = events.next().await.unwrap().unwrap();
            if ev.actor.id == app_id && ev.action == "died" {
                return ev;
            }
        }
    })
    .await
    .expect("died event");
    assert_eq!(died.exit_code(), Some(7));

    let listed = podman.list(&[(LABEL, &run)]).await.unwrap();
    assert_eq!(listed.len(), 2, "{listed:?}");
    let infra_state = podman.inspect(&infra_id).await.unwrap();
    assert!(infra_state.state.running);
    assert_eq!(
        infra_state.config.annotations["org.freebsd.jail.vnet"],
        "new"
    );

    let out = podman
        .exec(
            &infra_id,
            &["/bin/sh", "-c", "echo out; echo err >&2; exit 3"].map(String::from),
        )
        .await
        .unwrap();
    assert_eq!(
        (out.exit_code, &out.stdout[..], &out.stderr[..]),
        (3, &b"out\n"[..], &b"err\n"[..])
    );

    podman
        .stop(&infra_id, Duration::from_secs(1))
        .await
        .unwrap();
    podman
        .stop(&infra_id, Duration::from_secs(1))
        .await
        .unwrap(); // already stopped: fine
    assert!(!podman.inspect(&infra_id).await.unwrap().state.running);
    let err = podman
        .exec(&infra_id, &["/bin/true".into()])
        .await
        .unwrap_err();
    assert!(err.is_wrong_state(), "{err}");

    for id in [&app_id, &infra_id] {
        podman.remove(id).await.unwrap();
    }
    assert!(podman.remove(&app_id).await.unwrap_err().is_not_found());
    assert!(podman.list(&[(LABEL, &run)]).await.unwrap().is_empty());

    // The supervisor restarts a service that dies.
    let socket = lab.join("run").join("podman.sock");
    let killed = std::process::Command::new("pkill")
        .args(["-9", "-f", &format!("system service.*{}", socket.display())])
        .status()
        .unwrap();
    assert!(killed.success());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let back = tokio::time::timeout(Duration::from_secs(20), async {
        while podman.ping().await.is_err() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(back.is_ok(), "service did not come back");

    service.shutdown().await;
    assert!(
        podman.ping().await.is_err(),
        "service still answering after shutdown"
    );
}

/// A service a killed daemon left running is stopped when the next one
/// starts on the same socket, instead of running beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs root and Podman; set LEGION_PODMAN_LAB"]
async fn a_left_service_is_stopped() {
    let lab = PathBuf::from(std::env::var("LEGION_PODMAN_LAB").expect("set LEGION_PODMAN_LAB"));
    let config = || ServiceConfig::new(&lab.join("state"), &lab.join("run-left"));
    let socket = config().socket.display().to_string();
    let running = || {
        let out = std::process::Command::new("pgrep")
            .args(["-f", &socket])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).lines().count()
    };
    let first = PodmanService::start(config(), Duration::from_secs(30))
        .await
        .expect("podman service");
    // As a SIGKILL of the daemon: nothing stops the service.
    std::mem::forget(first);
    assert_eq!(running(), 1);
    let second = PodmanService::start(config(), Duration::from_secs(30))
        .await
        .expect("podman service");
    assert_eq!(running(), 1, "only the new service runs");
    second.client().ping().await.unwrap();
    second.shutdown().await;
    assert_eq!(running(), 0);
}
