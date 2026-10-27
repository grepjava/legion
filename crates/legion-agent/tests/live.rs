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
//!
//! With `LEGION_AGENT_ZFS_ROOT=<dataset>` the pod also mounts a ZFS volume,
//! created under that dataset and left there. A throwaway pool will do:
//!
//! ```sh
//! kldload -n zfs; truncate -s 256m /var/tmp/lvol.img
//! zpool create -m /var/tmp/lvol lvol /var/tmp/lvol.img   # LEGION_AGENT_ZFS_ROOT=lvol/legion
//! zpool destroy lvol; rm /var/tmp/lvol.img
//! ```

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
    let lab = PathBuf::from(std::env::var("LEGION_AGENT_LAB").expect("set LEGION_AGENT_LAB"));
    let mut config = AgentConfig::new("lab", lab.join("tasks"));
    config.zfs_root = std::env::var("LEGION_AGENT_ZFS_ROOT").ok();
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

    let zfs_root = std::env::var("LEGION_AGENT_ZFS_ROOT").ok();
    let mut pod: serde_json::Value = serde_json::json!({
        "containers": [{
            "name": "app",
            "image": IMAGE,
            "command": ["/bin/sh", "-c", ": > /tmp/dead; echo started; exec sleep 3600"],
            // The image has no utilities to spare: only sh builtins.
            "probes": {
                "readiness": {
                    "exec": { "command": ["/bin/sh", "-c", "test -e /tmp/ready"] },
                    "period": "1s", "failureThreshold": 1,
                },
                "liveness": {
                    "exec": { "command": ["/bin/sh", "-c", "test ! -s /tmp/dead"] },
                    "period": "1s", "failureThreshold": 2,
                },
            },
            "env": [{ "name": "GREETING", "secret": "greeting" }],
            "secrets": [{ "name": "greeting" }],
            "configs": [{ "name": "app-conf", "target": "/etc/app.conf" }],
            "resources": { "limits": { "memory": "256Mi", "maxproc": 32 } },
        }],
        "restartPolicy": { "condition": "any", "delay": "1s" },
        "stopGracePeriod": "2s",
    });
    let mut volume_mounts = vec![serde_json::json!({ "name": "scratch", "path": "/scratch" })];
    pod["volumes"] = serde_json::json!([{ "name": "scratch", "tmpfs": { "size": "16Mi" } }]);
    if zfs_root.is_some() {
        volume_mounts.push(serde_json::json!({ "name": "data", "path": "/data" }));
        pod["volumes"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({ "name": "data", "volume": { "name": "data" } }));
        let v = Object::new(
            Kind::Volume,
            "lab",
            "data",
            serde_json::json!({ "size": "64Mi" }),
        );
        put(&store, v, Precondition::MustNotExist).await;
    }
    pod["containers"][0]["volumeMounts"] = volume_mounts.into();
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
    for (kind, name, text) in [
        (Kind::Secret, "greeting", "hello"),
        (Kind::Config, "app-conf", "level = debug"),
    ] {
        let o = Object::new(kind, "lab", name, serde_json::json!({ "text": text }));
        put(&store, o, Precondition::MustNotExist).await;
    }
    put(&store, task, Precondition::MustNotExist).await;

    eventually("running", 120, async || {
        status(&store, &key).state == TaskState::Running
    })
    .await;
    let infra = podman.inspect("lab_web-1").await.unwrap();
    let app = podman.inspect("lab_web-1_app").await.unwrap();
    assert!(infra.state.running && app.state.running);
    assert!(
        !status(&store, &key).ready,
        "readiness probe not passed yet"
    );

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

    // Secrets and configs are read-only files from the task's tmpfs.
    let files = podman
        .exec(
            &app.id,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "read s < /run/secrets/greeting; read c < /etc/app.conf; echo \"$s|$c\"; \
                 echo x > /run/secrets/greeting || echo read-only"
                    .into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&files.stdout),
        "hello|level = debug\nread-only\n",
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let uid = status_uid(&store, &key);
    let tasks_dir = lab.join("tasks");
    let task_files = tasks_dir.join(&uid);
    let mounts = sh("mount -p");
    let tmpfs = mounts.lines().any(|l| {
        let f: Vec<&str> = l.split_whitespace().collect();
        f.get(1) == task_files.to_str().as_ref() && f.get(2) == Some(&"tmpfs")
    });
    assert!(tmpfs, "{mounts}");

    // Readiness follows its probe.
    let sh_in = async |cmd: &str| {
        podman
            .exec(&app.id, &["/bin/sh".into(), "-c".into(), cmd.into()])
            .await
            .unwrap()
    };

    // Volumes are writable; the ZFS one keeps its data beyond the pod.
    let out = sh_in("echo kept > /scratch/f && read x < /scratch/f && echo $x").await;
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "kept\n",
        "tmpfs volume"
    );
    let data_dir = zfs_root.as_ref().map(|root| {
        let ds = format!("{root}/volumes/lab/data");
        let list = sh(&format!("zfs get -H -p -o value quota,mountpoint {ds}"));
        let mut it = list.lines();
        assert_eq!(it.next(), Some("67108864"), "quota of {ds}: {list}");
        PathBuf::from(it.next().expect("mountpoint"))
    });
    if data_dir.is_some() {
        let out = sh_in("echo kept > /data/f && echo ok").await;
        assert_eq!(String::from_utf8_lossy(&out.stdout), "ok\n", "zfs volume");
    }
    sh_in(": > /tmp/ready").await;
    eventually("ready", 15, async || status(&store, &key).ready).await;

    // A killed container is restarted under the policy.
    podman.kill(&app.id, "SIGKILL").await.unwrap();
    eventually("restart", 30, async || {
        status(&store, &key).restarts == 1
            && podman.inspect(&app.id).await.is_ok_and(|c| c.state.running)
    })
    .await;

    // A failing liveness probe restarts the container too; the command
    // clears the marker, so it comes back healthy and, its ready file
    // still there, ready.
    eventually("ready after the restart", 15, async || {
        status(&store, &key).ready
    })
    .await;
    sh_in("echo dead > /tmp/dead").await;
    eventually("liveness restart", 30, async || {
        status(&store, &key).restarts == 2
    })
    .await;
    eventually("healthy and ready", 30, async || {
        let st = status(&store, &key);
        st.ready && st.state == TaskState::Running
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
    eventually("adopted pod ready", 15, async || status(&store, &key).ready).await;
    assert_eq!(status(&store, &key).restarts, 2);

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
    assert!(
        !sh("mount -p").contains(tasks_dir.to_str().unwrap()),
        "task tmpfs unmounted"
    );
    assert!(!task_files.exists());
    if let Some(dir) = &data_dir {
        assert_eq!(
            std::fs::read_to_string(dir.join("f")).unwrap(),
            "kept\n",
            "the volume's data outlives the task"
        );
    }

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
