//! The agent with real Podman, ocijail, jails and rctl on FreeBSD, against a
//! single-manager store. Needs root, so it is ignored by default:
//!
//! ```sh
//! sudo LEGION_AGENT_LAB=/var/tmp/legion-agent-test \
//!     target/debug/deps/live-<hash> --ignored --nocapture
//! ```
//!
//! Everything lives under the lab directory, with Podman on its own root,
//! runroot and tmpdir. `LEGION_OCIJAIL` picks the OCI runtime. Remove the lab
//! afterwards with `chflags -R 0` and `rm -rf`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::Arc;
use std::time::Duration;

use legion_agent::{Agent, AgentConfig, StoreControl};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object, ObjectKey, Spec};
use legion_manager::{Manager, ManagerConfig, default_raft_config};
use legion_podman::Podman;
use legion_podman::service::{PodmanService, ServiceConfig};
use legion_store::{Command, Outcome, Precondition, Store};
use openraft::BasicNode;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const IMAGE: &str = "docker.io/freebsd/freebsd-runtime:15.1";

fn sh(cmd: &str) -> String {
    let out = Process::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

async fn eventually(what: &str, secs: u64, mut f: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !f().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn start_agent(store: &Store, podman: &Podman) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let mut config = AgentConfig::new("lab");
    config.infra_image = IMAGE.into();
    config.resync = Duration::from_secs(2);
    let agent = Agent::new(
        config,
        Arc::new(podman.clone()),
        Arc::new(legion_sys::Shell::new()),
        Arc::new(StoreControl::new(store.clone())),
    );
    let (tx, rx) = oneshot::channel();
    let status = NodeStatus {
        state: NodeState::Ready,
        ..Default::default()
    };
    (
        tx,
        tokio::spawn(agent.run(status, async move {
            let _ = rx.await;
        })),
    )
}

fn status(store: &Store, key: &ObjectKey) -> TaskStatus {
    let o = store.get(key).unwrap().unwrap();
    serde_json::from_value(o.status).unwrap_or_default()
}

async fn put(store: &Store, object: Object, precondition: Precondition) {
    let out = store
        .raft
        .client_write(Command::Put {
            object,
            precondition,
        })
        .await
        .unwrap();
    assert!(matches!(out.data, Outcome::Written(_)), "{:?}", out.data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs root, Podman and ocijail; set LEGION_AGENT_LAB"]
async fn agent_runs_a_real_pod() {
    let lab = PathBuf::from(std::env::var("LEGION_AGENT_LAB").expect("set LEGION_AGENT_LAB"));
    let mut config = ServiceConfig::new(&lab.join("state"), &lab.join("run"));
    if let Ok(runtime) = std::env::var("LEGION_OCIJAIL") {
        config.runtime = Some(runtime);
    }
    let service = PodmanService::start(config, Duration::from_secs(30))
        .await
        .unwrap();
    let podman = service.client().clone();

    // A fresh store each run; Podman's images are kept.
    let _ = std::fs::remove_dir_all(lab.join("store"));
    std::fs::create_dir_all(lab.join("store")).unwrap();
    let manager = Manager::open(ManagerConfig {
        data_dir: lab.join("store"),
        advertise: "127.0.0.1:1".into(),
        raft: default_raft_config(),
    })
    .await
    .unwrap();
    let store = manager.store().clone();
    let id = manager.id();
    let members: BTreeMap<_, _> = [(
        id,
        BasicNode {
            addr: "127.0.0.1:1".into(),
        },
    )]
    .into();
    store.raft.initialize(members).await.unwrap();
    eventually("leader", 10, async || {
        store.raft.metrics().borrow().current_leader == Some(id)
    })
    .await;

    let (stop, agent) = start_agent(&store, &podman);

    let pod: serde_json::Value = serde_json::json!({
        "containers": [{
            "name": "app",
            "image": IMAGE,
            "command": ["/bin/sh", "-c", "echo started; exec sleep 3600"],
            "env": [{ "name": "GREETING", "value": "hello" }],
            "resources": { "limits": { "memory": "256Mi", "maxproc": 32 } },
        }],
        "restartPolicy": { "condition": "any", "delay": "1s" },
        "stopGracePeriod": "2s",
    });
    let mut spec: TaskSpec = legion_api::parse(&serde_json::json!({
        "service": "web", "serviceUid": "1", "serviceGeneration": 1, "slot": 1,
        "node": "lab", "pod": pod,
    }))
    .unwrap();
    spec.admit().unwrap();
    let task = Object::new(
        Kind::Task,
        "lab",
        "web-1",
        serde_json::to_value(&spec).unwrap(),
    );
    let key = task.key();
    put(&store, task, Precondition::MustNotExist).await;

    eventually("running", 120, async || {
        status(&store, &key).state == TaskState::Running
    })
    .await;
    let infra = podman.inspect("lab_web-1").await.unwrap();
    let app = podman.inspect("lab_web-1_app").await.unwrap();
    assert!(infra.state.running && app.state.running);

    // The pod has its address on the node's bridge, reachable from the host.
    let ip = status(&store, &key).ip.expect("pod address");
    assert!(ip.starts_with("10.200.0."), "{ip}");
    let ping = sh(&format!("ping -c1 -t2 {ip} >/dev/null && echo reachable"));
    assert_eq!(ping.trim(), "reachable", "host to pod {ip}");
    let route = sh(&format!("route -j {} -n get default", infra.id));
    assert!(route.contains("gateway: 10.200.0.1"), "{route}");
    let uid = status_uid(&store, &key);
    let host_end = legion_net::host_interface(&uid);
    assert!(sh("ifconfig -l").contains(&host_end));

    // ocijail names the app's jail <infra id>.<app id>, as the limits assume.
    let child = format!("{}.{}", infra.id, app.id);
    let jails = sh("jls -n name vnet");
    assert!(
        jails.contains(&format!("name={} vnet=new", infra.id)),
        "{jails}"
    );
    assert!(jails.contains(&format!("name={child} ")), "{jails}");
    let rules = sh(&format!("rctl jail:{child}"));
    assert!(rules.contains("vmemoryuse:deny=268435456"), "{rules}");
    assert!(rules.contains("maxproc:deny=32"), "{rules}");
    // The limit holds inside the container.
    let forks = podman
        .exec(
            &app.id,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "i=0; while [ $i -lt 40 ]; do sleep 30 & i=$((i+1)); done; kill $(jobs -p)".into(),
            ],
        )
        .await
        .unwrap();
    let stderr = String::from_utf8_lossy(&forks.stderr);
    assert!(
        stderr.contains("fork"),
        "maxproc 32 should refuse 40 sleeps; stderr: {stderr}"
    );
    let env = podman
        .exec(
            &app.id,
            &["/bin/sh".into(), "-c".into(), "echo $GREETING".into()],
        )
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&env.stdout), "hello\n");

    // A killed container is restarted under the policy.
    podman.kill(&app.id, "SIGKILL").await.unwrap();
    eventually("restart", 30, async || {
        status(&store, &key).restarts == 1
            && podman.inspect(&app.id).await.is_ok_and(|c| c.state.running)
    })
    .await;

    // A new agent adopts the running pod.
    stop.send(()).unwrap();
    agent.await.unwrap();
    let (stop, agent) = start_agent(&store, &podman);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let again = podman.inspect("lab_web-1_app").await.unwrap();
    assert_eq!(again.id, app.id, "the pod was adopted, not recreated");
    assert!(again.state.running);
    assert_eq!(status(&store, &key).state, TaskState::Running);

    // Shutting the task down removes containers, jails and limits.
    let mut o = store.get(&key).unwrap().unwrap();
    o.spec["desiredState"] = serde_json::to_value(DesiredState::Shutdown).unwrap();
    let v = o.metadata.version;
    put(&store, o, Precondition::Version(v)).await;
    eventually("shutdown", 60, async || {
        status(&store, &key).state == TaskState::Shutdown
    })
    .await;
    assert!(
        podman
            .list(&[("legion.task.id", &status_uid(&store, &key))])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!sh("jls -n name").contains(&infra.id));
    assert!(sh(&format!("rctl jail:{child}")).trim().is_empty());
    assert!(
        !sh("ifconfig -l").contains(&host_end),
        "pod interface removed"
    );

    stop.send(()).unwrap();
    agent.await.unwrap();
    drop(manager);
    service.shutdown().await;
    // The bridge outlives tasks; remove it with the lab.
    let _ = Process::new("ifconfig")
        .args([legion_net::BRIDGE, "destroy"])
        .status();
}

fn status_uid(store: &Store, key: &ObjectKey) -> String {
    store.get(key).unwrap().unwrap().metadata.uid
}
