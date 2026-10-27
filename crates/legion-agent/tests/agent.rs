//! The agent against a real single manager, through its dispatcher, with
//! Podman and the system faked in memory.

use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object, ObjectKey, Spec};
use legion_manager::{Manager, ManagerConfig};
use legion_store::{Command, Outcome, Precondition, Store};
use legion_sys::{FakeSys, Sys};
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const NODE: &str = "n1";
const INFRA: &str = "docker.io/freebsd/freebsd-runtime:15.1";

struct Harness {
    _dir: TempDir,
    /// The manager's address.
    addr: String,
    _server: JoinHandle<()>,
    store: Store,
    sys: Arc<FakeSys>,
    rt: Arc<FakeRuntime>,
    agent: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    /// Whether the agent joins the WireGuard mesh.
    mesh: bool,
}

impl Harness {
    async fn new() -> Self {
        Self::start(false).await
    }

    async fn start(mesh: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // The tests write tasks without services, which the controllers
        // would remove.
        let manager = Manager::open(ManagerConfig {
            controllers: false,
            ..ManagerConfig::new(dir.path(), &addr)
        })
        .await
        .unwrap();
        let store = manager.store().clone();
        let id = manager.id();
        store
            .raft
            .initialize(
                [(id, BasicNode { addr: addr.clone() })]
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        eventually("leader", || {
            store.raft.metrics().borrow().current_leader == Some(id)
        })
        .await;
        let server = tokio::spawn(async move {
            manager
                .serve(listener, std::future::pending())
                .await
                .unwrap();
        });
        let sys = Arc::new(FakeSys::new());
        let rt = Arc::new(FakeRuntime::new(sys.clone()));
        let mut h = Harness {
            _dir: dir,
            addr,
            _server: server,
            store,
            sys,
            rt,
            agent: None,
            mesh,
        };
        h.start_agent();
        h
    }

    fn start_agent(&mut self) {
        let mut config = AgentConfig::new(NODE, self.files_dir());
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.zfs_root = Some("tank/legion".into());
        if self.mesh {
            config.wireguard = Some(WireGuardConfig {
                key_file: self.key_file(),
                port: 51820,
            });
        }
        let agent = Agent::new(
            config,
            self.rt.clone(),
            self.sys.clone() as Arc<dyn Sys>,
            Arc::new(GrpcControl::new(NODE, vec![self.addr.clone()], None).unwrap()),
        );
        let (tx, rx) = oneshot::channel();
        let status = NodeStatus {
            state: NodeState::Ready,
            hostname: "n1.example".into(),
            addr: "192.0.2.1".into(),
            ..Default::default()
        };
        let handle = tokio::spawn(agent.run(status, async move {
            let _ = rx.await;
        }));
        self.agent = Some((tx, handle));
    }

    fn files_dir(&self) -> std::path::PathBuf {
        self._dir.path().join("tasks")
    }

    fn key_file(&self) -> std::path::PathBuf {
        self._dir.path().join("wg").join("wireguard.key")
    }

    async fn stop_agent(&mut self) {
        let (tx, handle) = self.agent.take().unwrap();
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("agent stops")
            .unwrap();
    }

    async fn write(&self, object: Object, precondition: Precondition) -> Object {
        match self
            .store
            .raft
            .client_write(Command::Put {
                object,
                precondition,
            })
            .await
            .unwrap()
            .data
        {
            Outcome::Written(o) => o,
            other => panic!("{other:?}"),
        }
    }

    /// Creates a task for this node running `pod` (YAML).
    async fn task(&self, name: &str, pod: &str) -> ObjectKey {
        let pod: serde_json::Value = serde_yaml_ng::from_str(pod).unwrap();
        let mut spec: TaskSpec = legion_api::parse(&serde_json::json!({
            "service": "web",
            "serviceUid": "0000000000000001",
            "serviceGeneration": 1,
            "slot": 1,
            "node": NODE,
            "pod": pod,
        }))
        .unwrap();
        spec.admit().unwrap();
        let object = Object::new(
            Kind::Task,
            "shop",
            name,
            serde_json::to_value(&spec).unwrap(),
        );
        self.write(object, Precondition::MustNotExist).await.key()
    }

    async fn shut_down(&self, key: &ObjectKey) {
        let mut o = self.store.get(key).unwrap().unwrap();
        o.spec["desiredState"] = serde_json::to_value(DesiredState::Shutdown).unwrap();
        let v = o.metadata.version;
        self.write(o, Precondition::Version(v)).await;
    }

    async fn delete(&self, key: &ObjectKey) {
        self.store
            .raft
            .client_write(Command::Delete {
                key: key.clone(),
                precondition: Precondition::None,
            })
            .await
            .unwrap();
    }

    fn status(&self, key: &ObjectKey) -> TaskStatus {
        let o = self.store.get(key).unwrap().expect("task exists");
        if o.status.is_null() {
            TaskStatus::default()
        } else {
            serde_json::from_value(o.status).unwrap()
        }
    }

    async fn wait_state(&self, key: &ObjectKey, state: TaskState) -> TaskStatus {
        eventually(&format!("{key} {}", state.as_str()), || {
            self.status(key).state == state
        })
        .await;
        self.status(key)
    }

    /// Names of the containers that are running.
    fn running(&self) -> Vec<String> {
        self.rt
            .containers()
            .into_iter()
            .filter(|(_, running, _)| *running)
            .map(|(n, _, _)| n)
            .collect()
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const WEB: &str = r#"
initContainers: [{ name: migrate, image: "web:1", command: ["/app/migrate"] }]
containers:
  - name: web
    image: "web:1"
    env: [{ name: MODE, value: prod }]
    resources: { limits: { cpu: 500m, memory: 256Mi, maxproc: 64 } }
  - name: shipper
    image: "shipper:2"
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_a_pod() {
    let h = Harness::new().await;
    // Images are pulled as needed.
    h.rt.add_image("web:1");

    let key = h.task("web-1", WEB).await;
    let status = h.wait_state(&key, TaskState::Running).await;
    assert!(status.ready, "no readiness probe: ready once running");
    assert_eq!(
        h.running(),
        ["shop_web-1", "shop_web-1_shipper", "shop_web-1_web"]
    );

    // The node registered itself and loaded the modules it needs.
    let node = h
        .store
        .get(&ObjectKey::new(Kind::Node, "", NODE))
        .unwrap()
        .unwrap();
    assert_eq!(node.status["state"], "ready");
    assert_eq!(node.spec["availability"], "active");
    assert_eq!(
        h.sys.modules().into_iter().collect::<Vec<_>>(),
        ["if_bridge", "if_epair", "if_wg", "pf"]
    );

    // The pod is an infra jail with a VNET and the containers as its
    // children; limits are on the web container's own jail.
    let infra = h.rt.id_of("shop_web-1").unwrap();
    let web = h.rt.id_of("shop_web-1_web").unwrap();
    let jails = h.sys.jails().unwrap();
    let infra_jail = jails.iter().find(|j| j.name == infra).unwrap();
    assert_eq!(infra_jail.vnet, "new");
    assert_eq!(infra_jail.children_max, 3);
    assert!(jails.iter().any(|j| j.name == format!("{infra}.{web}")));
    let rules: Vec<String> = h.sys.rctl_rules().iter().map(ToString::to_string).collect();
    assert_eq!(
        rules,
        [
            format!("jail:{infra}.{web}:memoryuse:sigkill=268435456"),
            format!("jail:{infra}.{web}:pcpu:deny=50"),
            format!("jail:{infra}.{web}:maxproc:deny=64"),
        ]
    );

    // The node got the first pod subnet; the pod its first address, on an
    // epair from the bridge into the infra jail, with a default route.
    assert_eq!(node.status["podCidr"], "10.200.0.0/24");
    assert_eq!(status.ip.as_deref(), Some("10.200.0.2"));
    let uid = h.store.get(&key).unwrap().unwrap().metadata.uid;
    let host_end = legion_net::host_interface(&uid);
    assert_eq!(
        h.sys.interface(legion_net::BRIDGE).unwrap().members,
        std::slice::from_ref(&host_end)
    );
    let inside = h.sys.interfaces(Some(&infra)).unwrap();
    assert_eq!(inside.len(), 2, "lo0 and the pod's end: {inside:?}");
    let routes = h.sys.routes(Some(&infra));
    assert_eq!(
        routes[&legion_sys::Destination::Default].to_string(),
        "10.200.0.1"
    );

    // Shutting the task down removes the pod, its network and its limits.
    h.shut_down(&key).await;
    h.wait_state(&key, TaskState::Shutdown).await;
    assert!(h.rt.containers().is_empty());
    assert!(h.sys.rctl_rules().is_empty());
    assert!(h.sys.jails().unwrap().is_empty());
    assert!(h.sys.interface(&host_end).is_none());
    assert_eq!(h.sys.interfaces(None).unwrap(), ["lo0", legion_net::BRIDGE]);

    // The next pod reuses the freed address.
    let next = h.task("web-2", WEB).await;
    let status = h.wait_state(&next, TaskState::Running).await;
    assert_eq!(status.ip.as_deref(), Some("10.200.0.2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_policy() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task(
            "app-1",
            r#"
containers: [{ name: app, image: "app:1" }]
restartPolicy: { condition: on-failure, maxAttempts: 2, delay: 0s }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;

    for attempt in 1..=2 {
        h.rt.exit("shop_app-1_app", 1);
        eventually("restart", || {
            h.rt.containers().iter().any(|(n, running, starts)| {
                n == "shop_app-1_app" && *running && *starts == attempt + 1
            })
        })
        .await;
    }
    eventually("restarts counted", || h.status(&key).restarts == 2).await;

    // Out of attempts: the task fails and the pod is removed.
    h.rt.exit("shop_app-1_app", 3);
    let status = h.wait_state(&key, TaskState::Failed).await;
    assert_eq!(status.exit_code, Some(3));
    assert!(
        status.message.contains("app exited with 3"),
        "{}",
        status.message
    );
    eventually("pod removed", || h.rt.containers().is_empty()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_to_completion() {
    let h = Harness::new().await;
    h.rt.add_image("job:1");
    let key = h
        .task(
            "job-1",
            r#"
containers: [{ name: a, image: "job:1" }, { name: b, image: "job:1" }]
restartPolicy: { condition: on-failure }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    h.rt.exit("shop_job-1_a", 0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.status(&key).state, TaskState::Running, "b still runs");
    h.rt.exit("shop_job-1_b", 0);
    let status = h.wait_state(&key, TaskState::Complete).await;
    assert_eq!(status.exit_code, Some(0));
    eventually("pod removed", || h.rt.containers().is_empty()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn infra_death_fails_the_task() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task("app-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    h.wait_state(&key, TaskState::Running).await;
    h.rt.exit("shop_app-1", 137);
    let status = h.wait_state(&key, TaskState::Failed).await;
    assert!(status.message.contains("infra"), "{}", status.message);
    eventually("pod removed", || h.rt.containers().is_empty()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_a_pod_without_vnet() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    h.rt.ignore_vnet();
    let key = h
        .task("app-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    let status = h.wait_state(&key, TaskState::Rejected).await;
    assert!(
        status.message.contains("vnet=inherit"),
        "{}",
        status.message
    );
    eventually("nothing left", || h.rt.containers().is_empty()).await;
    assert_eq!(h.rt.created(), 1, "no app container was created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_failures() {
    let h = Harness::new().await;
    h.rt.fail_pulls_of("private:1");
    let pull = h
        .task("p-1", r#"containers: [{ name: app, image: "private:1" }]"#)
        .await;
    let status = h.wait_state(&pull, TaskState::Failed).await;
    assert!(
        status.message.contains("pulling private:1"),
        "{}",
        status.message
    );

    h.rt.add_image("web:1");
    h.rt.set_init_exit_code(2);
    let init = h.task("i-1", WEB).await;
    let status = h.wait_state(&init, TaskState::Failed).await;
    assert!(
        status
            .message
            .contains("init container migrate exited with 2"),
        "{}",
        status.message
    );

    let https = h
        .task(
            "h-1",
            r#"
containers:
  - name: app
    image: "web:1"
    ports: [{ name: https, containerPort: 443 }]
    probes: { readiness: { http: { port: https, scheme: https } } }
"#,
        )
        .await;
    let status = h.wait_state(&https, TaskState::Rejected).await;
    assert!(
        status.message.contains("not supported yet"),
        "{}",
        status.message
    );
    eventually("nothing left", || h.rt.containers().is_empty()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pods_survive_an_agent_restart() {
    let mut h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task(
            "app-1",
            r#"
containers: [{ name: app, image: "app:1" }]
restartPolicy: { condition: any, delay: 0s }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    let created = h.rt.created();

    h.stop_agent().await;
    assert_eq!(
        h.running(),
        ["shop_app-1", "shop_app-1_app"],
        "pod keeps running"
    );

    // A container dies while no agent is watching.
    h.rt.exit("shop_app-1_app", 1);
    h.start_agent();
    // The new agent adopts the pod rather than recreating it, and applies
    // the restart policy to the exit it missed.
    eventually("restarted", || h.running().len() == 2).await;
    assert_eq!(h.rt.created(), created, "no container recreated");
    eventually("restart counted", || h.status(&key).restarts == 1).await;
    assert_eq!(h.status(&key).state, TaskState::Running);
    // The adopted pod keeps its address, and a new pod does not take it.
    assert_eq!(h.status(&key).ip.as_deref(), Some("10.200.0.2"));
    let other = h
        .task("other-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    let st = h.wait_state(&other, TaskState::Running).await;
    assert_eq!(st.ip.as_deref(), Some("10.200.0.3"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removes_pods_of_deleted_tasks() {
    let mut h = Harness::new().await;
    h.rt.add_image("app:1");
    let a = h
        .task("a-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    let b = h
        .task("b-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    h.wait_state(&a, TaskState::Running).await;
    h.wait_state(&b, TaskState::Running).await;

    h.delete(&a).await;
    eventually("a removed", || h.running() == ["shop_b-1", "shop_b-1_app"]).await;

    // Deleted while the agent is down: removed as an orphan on restart.
    h.stop_agent().await;
    h.delete(&b).await;
    h.start_agent();
    eventually("b removed", || h.rt.containers().is_empty()).await;
}

const PROBED: &str = r#"
restartPolicy: { condition: any, delay: 0s }
containers:
  - name: app
    image: "app:1"
    probes:
      readiness: { exec: { command: [/ready] }, period: 20ms, failureThreshold: 1 }
      liveness: { exec: { command: [/alive] }, period: 20ms, failureThreshold: 2 }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readiness_decides_ready() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    // The same fake answer serves both probes: failing readiness alone is
    // not possible here, so liveness tolerates it for a while.
    let key = h
        .task(
            "app-1",
            &PROBED.replace("failureThreshold: 2", "failureThreshold: 1000"),
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    eventually("ready", || h.status(&key).ready).await;

    h.rt.set_exec("shop_app-1_app", 1);
    eventually("not ready", || !h.status(&key).ready).await;
    assert_eq!(h.status(&key).state, TaskState::Running);
    assert_eq!(h.status(&key).restarts, 0);

    h.rt.set_exec("shop_app-1_app", 0);
    eventually("ready again", || h.status(&key).ready).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liveness_restarts_the_container() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h.task("app-1", PROBED).await;
    h.wait_state(&key, TaskState::Running).await;
    eventually("ready", || h.status(&key).ready).await;

    h.rt.set_exec("shop_app-1_app", 1);
    eventually("restarted", || h.status(&key).restarts >= 1).await;
    let st = h.status(&key);
    assert!(
        st.message.contains("liveness probe failed") || st.message.is_empty(),
        "{}",
        st.message
    );
    h.rt.set_exec("shop_app-1_app", 0);
    eventually("ready after the restart", || {
        let st = h.status(&key);
        st.ready && st.state == TaskState::Running
    })
    .await;
    let restarts = h.status(&key).restarts;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.status(&key).restarts,
        restarts,
        "healthy now: no restarts"
    );
    let (_, running, starts) =
        h.rt.containers()
            .into_iter()
            .find(|(n, ..)| n == "shop_app-1_app")
            .unwrap();
    assert!(running && starts >= 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_startup_probe_fails_the_task() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    h.rt.set_exec("shop_app-1_app", 1);
    let key = h
        .task(
            "app-1",
            r#"
restartPolicy: { condition: none }
containers:
  - name: app
    image: "app:1"
    probes:
      startup: { exec: { command: [/started] }, period: 20ms, failureThreshold: 3 }
      readiness: { exec: { command: [/ready] }, period: 20ms }
"#,
        )
        .await;
    let st = h.wait_state(&key, TaskState::Failed).await;
    assert!(!st.ready);
    assert!(
        st.message
            .contains("startup probe failed: \"/started\" exited with 1: unhealthy"),
        "{}",
        st.message
    );
    assert!(h.rt.containers().is_empty(), "pod removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secrets_and_configs() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    for (kind, name, spec) in [
        (
            Kind::Secret,
            "db-password",
            serde_json::json!({ "text": "s3cret" }),
        ),
        (
            Kind::Config,
            "app-conf",
            serde_json::json!({ "text": "level = debug\n" }),
        ),
    ] {
        h.write(
            Object::new(kind, "shop", name, spec),
            Precondition::MustNotExist,
        )
        .await;
    }
    let key = h
        .task(
            "app-1",
            r#"
containers:
  - name: app
    image: "app:1"
    secrets: [{ name: db-password }]
    configs: [{ name: app-conf, target: /etc/app.conf }]
    env: [{ name: DB_PASSWORD, secret: db-password }, { name: MODE, value: prod }]
  - name: sidecar
    image: "app:1"
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    let uid = h.store.get(&key).unwrap().unwrap().metadata.uid;
    let dir = h.files_dir().join(&uid);

    // The task's files live on a tmpfs of its own...
    let mounts = h.sys.tmpfs_mounts();
    assert_eq!(
        mounts.keys().collect::<Vec<_>>(),
        [dir.to_str().unwrap()],
        "{mounts:?}"
    );
    let secret = dir.join("secrets").join("db-password");
    let config = dir.join("configs").join("app-conf");
    assert_eq!(std::fs::read_to_string(&secret).unwrap(), "s3cret");
    assert_eq!(std::fs::read_to_string(&config).unwrap(), "level = debug\n");

    // ...mounted read-only into the container that asked for them only.
    let spec = h.rt.spec("shop_app-1_app").unwrap();
    let mounts: Vec<_> = spec
        .mounts
        .iter()
        .filter(|m| m.destination != "/etc/resolv.conf")
        .map(|m| {
            (
                m.source.as_str(),
                m.destination.as_str(),
                m.kind.as_str(),
                m.options.clone(),
            )
        })
        .collect();
    assert_eq!(
        mounts,
        [
            (
                secret.to_str().unwrap(),
                "/run/secrets/db-password",
                "nullfs",
                vec!["ro".to_owned()]
            ),
            (
                config.to_str().unwrap(),
                "/etc/app.conf",
                "nullfs",
                vec!["ro".to_owned()]
            ),
        ]
    );
    assert_eq!(spec.env["DB_PASSWORD"], "s3cret");
    assert_eq!(spec.env["MODE"], "prod");
    // Every container gets the pod's resolv.conf, from the same tmpfs.
    let sidecar = h.rt.spec("shop_app-1_sidecar").unwrap();
    assert!(sidecar.env.is_empty());
    let resolv = dir.join("resolv.conf");
    assert_eq!(
        sidecar
            .mounts
            .iter()
            .map(|m| (m.source.as_str(), m.destination.as_str()))
            .collect::<Vec<_>>(),
        [(resolv.to_str().unwrap(), "/etc/resolv.conf")]
    );
    assert_eq!(
        std::fs::read_to_string(&resolv).unwrap(),
        "search shop.legion legion\nnameserver 10.200.0.1\noptions ndots:2\n"
    );

    // Shutting down removes the files with the pod.
    h.shut_down(&key).await;
    h.wait_state(&key, TaskState::Shutdown).await;
    assert!(h.sys.tmpfs_mounts().is_empty());
    assert!(!dir.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_secret_fails_the_task() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task(
            "app-1",
            r#"containers: [{ name: app, image: "app:1", env: [{ name: TOKEN, secret: nope }] }]"#,
        )
        .await;
    let st = h.wait_state(&key, TaskState::Failed).await;
    assert_eq!(st.message, "secret nope not found");
    assert!(h.rt.containers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removes_files_of_unknown_tasks() {
    let h = Harness::new().await;
    let stale = h.files_dir().join("0123456789abcdef");
    std::fs::create_dir_all(stale.join("secrets")).unwrap();
    std::fs::write(stale.join("secrets").join("old"), "x").unwrap();
    eventually("stale files removed", || !stale.exists()).await;
}

const VOLUMES: &str = r#"
containers:
  - name: db
    image: "app:1"
    volumeMounts:
      - { name: data, path: /var/db/app }
      - { name: scratch, path: /var/tmp/scratch }
  - name: backup
    image: "app:1"
    volumeMounts: [{ name: data, path: /backup/data, readOnly: true }]
volumes:
  - { name: data, volume: { name: data } }
  - { name: scratch, tmpfs: { size: 64Mi } }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volumes() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let volume = ObjectKey::new(Kind::Volume, "shop", "data");
    h.write(
        Object::new(
            Kind::Volume,
            "shop",
            "data",
            serde_json::json!({ "size": "1Gi" }),
        ),
        Precondition::MustNotExist,
    )
    .await;
    let key = h.task("db-1", VOLUMES).await;
    h.wait_state(&key, TaskState::Running).await;
    let uid = h.store.get(&key).unwrap().unwrap().metadata.uid;

    // The volume is a dataset with its quota, bound to this node.
    let ds = "tank/legion/volumes/shop/data";
    let datasets = h.sys.zfs_list(ds).unwrap();
    assert_eq!(datasets.len(), 1, "{datasets:?}");
    assert_eq!(datasets[0].quota, Some(1 << 30));
    let status = h.store.get(&volume).unwrap().unwrap().status;
    assert_eq!(status["node"], NODE);
    assert_eq!(status["dataset"], ds);

    // Each container mounts what it asks for, read-only where asked.
    let scratch = h.files_dir().join(&uid).join("volumes").join("scratch");
    let mounts = |name: &str| -> Vec<(String, String, bool)> {
        h.rt.spec(name)
            .unwrap()
            .mounts
            .iter()
            .filter(|m| m.destination != "/etc/resolv.conf")
            .map(|m| {
                (
                    m.source.clone(),
                    m.destination.clone(),
                    m.options.contains(&"ro".to_owned()),
                )
            })
            .collect()
    };
    assert_eq!(
        mounts("shop_db-1_db"),
        [
            (
                "/tank/legion/volumes/shop/data".to_owned(),
                "/var/db/app".to_owned(),
                false
            ),
            (
                scratch.to_str().unwrap().to_owned(),
                "/var/tmp/scratch".to_owned(),
                false
            ),
        ]
    );
    assert_eq!(
        mounts("shop_db-1_backup"),
        [(
            "/tank/legion/volumes/shop/data".to_owned(),
            "/backup/data".to_owned(),
            true
        )]
    );
    let tmpfs = h.sys.tmpfs_mounts();
    assert_eq!(
        tmpfs.get(scratch.to_str().unwrap()),
        Some(&Some(64 << 20)),
        "{tmpfs:?}"
    );

    // The tmpfs goes with the pod; the dataset stays for the next task.
    h.shut_down(&key).await;
    h.wait_state(&key, TaskState::Shutdown).await;
    assert!(h.sys.tmpfs_mounts().is_empty());
    assert_eq!(h.sys.zfs_list(ds).unwrap().len(), 1);
    let next = h.task("db-2", VOLUMES).await;
    h.wait_state(&next, TaskState::Running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volume_problems() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    // Missing: fails, so the controller tries again later.
    let key = h.task("db-1", VOLUMES).await;
    let st = h.wait_state(&key, TaskState::Failed).await;
    assert_eq!(st.message, "volume data not found");

    // Held by another node: rejected.
    let mut v = Object::new(Kind::Volume, "shop", "data", serde_json::json!({}));
    v.status = serde_json::json!({ "node": "n2", "dataset": "tank/legion/volumes/shop/data" });
    h.write(v, Precondition::MustNotExist).await;
    let key = h.task("db-2", VOLUMES).await;
    let st = h.wait_state(&key, TaskState::Rejected).await;
    assert_eq!(st.message, "volume data lives on node n2");
    assert!(h.sys.zfs_list("tank").unwrap().is_empty());
    assert!(h.rt.containers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joins_the_mesh() {
    let mut h = Harness::start(true).await;
    let node_status = |h: &Harness, name: &str| {
        h.store
            .get(&ObjectKey::new(Kind::Node, "", name))
            .unwrap()
            .map(|n| n.status)
    };
    eventually("the node registers with its key", || {
        node_status(&h, NODE).is_some_and(|s| s.get("wireguard").is_some())
    })
    .await;

    // The key is made once and kept; the status carries its public half
    // and where other nodes send to.
    let key = std::fs::read_to_string(h.key_file()).unwrap();
    let key = key.trim();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(h.key_file())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let status = node_status(&h, NODE).unwrap();
    assert_eq!(
        status["wireguard"],
        serde_json::json!({ "publicKey": format!("pub:{key}"), "endpoint": "192.0.2.1:51820" })
    );
    eventually("wg0 is set up", || {
        h.sys
            .wireguard(legion_net::mesh::INTERFACE)
            .is_some_and(|wg| wg.private_key.as_deref() == Some(key))
    })
    .await;
    let wg0 = h.sys.interface(legion_net::mesh::INTERFACE).unwrap();
    assert_eq!(wg0.config.addr.unwrap().to_string(), "100.64.0.1/16");
    assert_eq!(
        h.sys.interface_routes(None)
            [&legion_sys::Destination::Net("10.200.0.0/16".parse().unwrap())],
        legion_net::mesh::INTERFACE
    );

    // Another node appears: it becomes a peer; this one does not.
    let mut other = Object::new(
        Kind::Node,
        "",
        "n2",
        serde_json::to_value(legion_api::node::NodeSpec::default()).unwrap(),
    );
    other.status = serde_json::json!({
        "state": "ready",
        "podCidr": "10.200.5.0/24",
        "wireguard": { "publicKey": "pub:other", "endpoint": "192.0.2.2:51820" },
    });
    let other = h.write(other, Precondition::MustNotExist).await;
    let peers = |h: &Harness| {
        h.sys
            .wireguard(legion_net::mesh::INTERFACE)
            .unwrap()
            .peers
            .into_values()
            .collect::<Vec<_>>()
    };
    eventually("n2 is a peer", || peers(&h).len() == 1).await;
    let peer = &peers(&h)[0];
    assert_eq!(peer.public_key, "pub:other");
    assert_eq!(peer.endpoint.as_deref(), Some("192.0.2.2:51820"));
    assert_eq!(
        peer.allowed_ips
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["10.200.5.0/24", "100.64.5.1/32"]
    );

    // A restarted agent keeps its key and its peers.
    h.stop_agent().await;
    h.start_agent();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(std::fs::read_to_string(h.key_file()).unwrap().trim(), key);
    assert_eq!(peers(&h).len(), 1);
    assert_eq!(
        node_status(&h, NODE).unwrap()["wireguard"]["publicKey"],
        format!("pub:{key}")
    );

    // The node is removed: so is the peer.
    h.delete(&other.key()).await;
    eventually("n2 is no longer a peer", || peers(&h).is_empty()).await;
    h.stop_agent().await;
}
