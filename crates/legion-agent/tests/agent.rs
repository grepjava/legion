//! The agent against a real single manager, through its dispatcher, with
//! the runtime and the system faked in memory.

use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::runtime::Output;
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object, ObjectKey, Spec};
use legion_manager::{Manager, ManagerConfig};
use legion_proto::v1 as pb;
use legion_proto::v1::pods_client::PodsClient;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
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
    metrics: Option<legion_agent::metrics::AgentMetrics<FakeRuntime>>,
    /// The agent's drain before stopping a ready task; none unless a test
    /// asks.
    drain: Duration,
    /// Whether the agent joins the WireGuard mesh.
    mesh: bool,
}

impl Harness {
    async fn new() -> Self {
        Self::start(false).await
    }

    async fn start(mesh: bool) -> Self {
        // RUST_LOG shows what the agent and manager do, for a failing test.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
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
            metrics: None,
            drain: Duration::ZERO,
            mesh,
        };
        h.start_agent();
        h
    }

    fn start_agent(&mut self) {
        let mut config = AgentConfig::new(NODE, self.files_dir());
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.drain = self.drain;
        config.zfs_root = Some("tank/legion".into());
        config.logs_dir = Some(self.logs_dir());
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
        self.metrics = Some(agent.metrics());
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

    fn logs_dir(&self) -> std::path::PathBuf {
        self._dir.path().join("logs")
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
        self.task_of(name, pod, false).await
    }

    /// A task of a Service, or with `job` of a Job.
    async fn task_of(&self, name: &str, pod: &str, job: bool) -> ObjectKey {
        self.task_in("shop", name, pod, job).await
    }

    async fn task_in(&self, namespace: &str, name: &str, pod: &str, job: bool) -> ObjectKey {
        let pod: serde_json::Value = serde_yaml_ng::from_str(pod).unwrap();
        let mut spec: TaskSpec = legion_api::parse(&serde_json::json!({
            "service": "web",
            "serviceUid": "0000000000000001",
            "serviceGeneration": 1,
            "job": job,
            "slot": 1,
            "node": NODE,
            "pod": pod,
        }))
        .unwrap();
        spec.admit().unwrap();
        let mut object = Object::new(
            Kind::Task,
            namespace,
            name,
            serde_json::to_value(&spec).unwrap(),
        );
        // As the scheduler leaves it.
        object.status = serde_json::json!({ "state": "assigned" });
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

    /// Waits until no container is left, naming those that are if not.
    async fn nothing_left(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !self.rt.containers().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "containers left: {:?}",
                self.rt.containers()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    h.nothing_left().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_tasks_drain_before_they_stop() {
    let mut h = Harness::new().await;
    h.stop_agent().await;
    h.drain = Duration::from_millis(1500);
    h.start_agent();
    h.rt.add_image("app:1");
    let pod = r#"containers: [{ name: app, image: "app:1" }]"#;
    let web = h.task("web-1", pod).await;
    let job = h.task_of("job-1", pod, true).await;
    h.wait_state(&web, TaskState::Running).await;
    h.wait_state(&job, TaskState::Running).await;
    eventually("web ready", || h.status(&web).ready).await;

    // Other nodes may still send the service's task connections for a
    // moment: it runs on. A Job's task has nothing to drain.
    h.shut_down(&web).await;
    h.shut_down(&job).await;
    h.wait_state(&job, TaskState::Shutdown).await;
    assert!(
        h.running().iter().any(|n| n == "shop_web-1_app"),
        "web drains"
    );
    h.wait_state(&web, TaskState::Shutdown).await;
    eventually("web's pod removed", || {
        !h.running().iter().any(|n| n.starts_with("shop_web-1"))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_events_do_not_restart_a_new_run() {
    let h = Harness::new().await;
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
    let starts = || {
        h.rt.containers()
            .into_iter()
            .find(|(n, ..)| n == "shop_app-1_app")
            .map_or(0, |(_, _, starts)| starts)
    };

    // The exit's event comes only after a resync has seen the exit and
    // restarted the container: it is about the run before.
    h.rt.hold_events(true);
    h.rt.exit("shop_app-1_app", 1);
    eventually("restarted by a resync", || starts() == 2).await;
    h.rt.hold_events(false);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(starts(), 2, "the late event restarted the new run");
    assert_eq!(h.status(&key).restarts, 1);
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
    h.nothing_left().await;
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
    h.nothing_left().await;
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
    h.nothing_left().await;
    assert_eq!(h.rt.created(), 1, "no app container was created");
}

/// The profile of a pod member's jail, as `k=v` in name order.
fn jail_profile(h: &Harness, pod: &str, container: &str) -> Vec<String> {
    let jail = legion_agent::pod::child_jail(
        &h.rt.id_of(pod).unwrap(),
        &h.rt.id_of(&format!("{pod}_{container}")).unwrap(),
    );
    let params = [
        "allow.raw_sockets",
        "allow.chflags",
        "enforce_statfs",
        "securelevel",
    ];
    h.sys
        .jail_get(&jail, &params)
        .unwrap()
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jails_are_locked_down() {
    let mut h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task(
            "app-1",
            r#"
containers:
  - { name: app, image: "app:1" }
  - name: ping
    image: "app:1"
    security: { allow: [rawSockets, mlock, sysvipc], securelevel: 0 }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    let locked = [
        "allow.chflags=false",
        "allow.raw_sockets=false",
        "enforce_statfs=2",
        "securelevel=2",
    ];
    assert_eq!(jail_profile(&h, "shop_app-1", "app"), locked);
    assert_eq!(
        jail_profile(&h, "shop_app-1", "ping"),
        [
            "allow.chflags=false",
            "allow.raw_sockets=true",
            "enforce_statfs=2",
            "securelevel=0"
        ]
    );
    // What a child jail may have only if its parent has it is asked of
    // the runtime for both, and only for the container that wants it.
    let infra = h.rt.spec("shop_app-1").unwrap().annotations;
    assert_eq!(infra["org.freebsd.jail.allow.mlock"], "true");
    assert_eq!(infra["org.freebsd.jail.sysvshm"], "new");
    let ping = h.rt.spec("shop_app-1_ping").unwrap().annotations;
    assert_eq!(ping["org.freebsd.jail.sysvshm"], "inherit");
    let app = h.rt.spec("shop_app-1_app").unwrap().annotations;
    assert!(!app.contains_key("org.freebsd.jail.allow.mlock"));

    // A pod adopted from an agent older than the profile is tightened in
    // place rather than restarted.
    h.stop_agent().await;
    let jail = legion_agent::pod::child_jail(
        &h.rt.id_of("shop_app-1").unwrap(),
        &h.rt.id_of("shop_app-1_app").unwrap(),
    );
    h.sys
        .jail_set(
            &jail,
            &[("allow.raw_sockets", "true"), ("securelevel", "-1")],
        )
        .unwrap();
    let created = h.rt.created();
    h.start_agent();
    eventually("tightened", || {
        jail_profile(&h, "shop_app-1", "app") == locked
    })
    .await;
    assert_eq!(h.status(&key).state, TaskState::Running);
    assert_eq!(h.rt.created(), created);

    // Without the hook, a new pod is refused.
    h.rt.skip_jail_hook();
    let bare = h
        .task("bare-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    let status = h.wait_state(&bare, TaskState::Rejected).await;
    assert!(
        status.message.contains("jail hook did not run"),
        "{}",
        status.message
    );
    eventually("refused pod removed", || {
        !h.running().iter().any(|n| n.starts_with("shop_bare-1"))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_containers_run_first_and_can_be_stopped() {
    let h = Harness::new().await;
    h.rt.add_image("web:1");
    h.rt.hold_init(true);
    let key = h.task("web-1", WEB).await;
    // The message comes as the agent turns to the container, which may
    // not have started yet.
    eventually("the init container", || {
        h.status(&key).message == "init container migrate (1/1)"
            && h.running() == ["shop_web-1", "shop_web-1_migrate"]
    })
    .await;
    assert_eq!(h.status(&key).state, TaskState::Starting);

    // An init container that never ends does not hold up a stop.
    h.shut_down(&key).await;
    h.wait_state(&key, TaskState::Shutdown).await;
    assert!(h.rt.containers().is_empty());
    assert!(h.sys.jails().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn containers_report_their_usage() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task(
            "app-1",
            r#"
containers:
  - { name: app, image: "app:1" }
  - { name: side, image: "app:1" }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    let jail = |c: &str| {
        legion_agent::pod::child_jail(
            &h.rt.id_of("shop_app-1").unwrap(),
            &h.rt.id_of(&format!("shop_app-1_{c}")).unwrap(),
        )
    };
    h.sys.set_usage(
        &jail("app"),
        &[("cputime", 12), ("memoryuse", 4096), ("maxproc", 3)],
    );
    h.sys.set_usage(&jail("side"), &[("memoryuse", 1024)]);
    // The infra jail's usage, which counts its containers', is left out.
    h.sys
        .set_usage(&h.rt.id_of("shop_app-1").unwrap(), &[("memoryuse", 5120)]);
    let text = h.metrics.as_ref().unwrap().collect().await.finish();
    for line in [
        "legion_agent_runtime_up 1",
        "legion_agent_pods 1",
        "legion_agent_containers 2",
        "legion_agent_usage_unread 0",
        "legion_container_cpu_seconds_total{namespace=\"shop\",task=\"app-1\",container=\"app\"} 12",
        "legion_container_memory_bytes{namespace=\"shop\",task=\"app-1\",container=\"app\"} 4096",
        "legion_container_memory_bytes{namespace=\"shop\",task=\"app-1\",container=\"side\"} 1024",
        "legion_container_processes{namespace=\"shop\",task=\"app-1\",container=\"app\"} 3",
    ] {
        assert!(text.contains(&format!("\n{line}\n")), "{line} in:\n{text}");
    }
    assert!(text.contains("# TYPE legion_container_cpu_seconds_total counter\n"));
    assert!(!text.contains("5120"), "{text}");
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
    // The pod goes with the failure, so its last words are kept.
    h.rt.log("shop_i-1_migrate", Output::Stdout, "migrating\n\n");
    h.rt.log("shop_i-1_migrate", Output::Stderr, "no such table: users\n");
    let init = h.task("i-1", WEB).await;
    let status = h.wait_state(&init, TaskState::Failed).await;
    assert_eq!(
        status.message,
        "init container migrate exited with 2: migrating; no such table: users"
    );
    h.rt.set_init_exit_code(0);

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
    h.nothing_left().await;
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
async fn private_images() {
    let h = Harness::new().await;
    let image = "ghcr.io/team/app:1";
    h.rt.private_image(image, "bot", "pw");
    for ns in ["shop", "other"] {
        let login =
            serde_json::json!({ "auths": { "ghcr.io": { "username": "bot", "password": "pw" } } });
        h.write(
            Object::new(
                Kind::Secret,
                ns,
                "ghcr",
                serde_json::json!({ "text": login.to_string() }),
            ),
            Precondition::MustNotExist,
        )
        .await;
    }
    let with = format!(
        r#"{{ imagePullSecrets: [ghcr], containers: [{{ name: app, image: "{image}" }}] }}"#
    );
    let without = format!(r#"{{ containers: [{{ name: app, image: "{image}" }}] }}"#);

    let key = h.task("p-1", &without).await;
    let status = h.wait_state(&key, TaskState::Failed).await;
    assert!(status.message.contains("denied"), "{}", status.message);
    let key = h.task("p-2", &with).await;
    h.wait_state(&key, TaskState::Running).await;
    // The login stays out of the task's files.
    let uid = h.store.get(&key).unwrap().unwrap().metadata.uid;
    assert!(!h.files_dir().join(&uid).join("secrets").exists());

    // Another namespace without the login cannot use the image the node
    // has; with its own, it can.
    let key = h.task_in("other", "p-3", &without, false).await;
    let status = h.wait_state(&key, TaskState::Failed).await;
    assert!(status.message.contains("denied"), "{}", status.message);
    let key = h.task_in("other", "p-4", &with, false).await;
    h.wait_state(&key, TaskState::Running).await;

    let missing =
        r#"{ imagePullSecrets: [nope], containers: [{ name: app, image: "ghcr.io/team/app:2" }] }"#;
    let key = h.task("p-5", missing).await;
    let status = h.wait_state(&key, TaskState::Failed).await;
    assert!(
        status.message.contains("secret nope not found"),
        "{}",
        status.message
    );
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
async fn stateful_tasks_mount_their_own_volumes() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    // The controller makes `<claim>-<service>-<ordinal>`.
    let volume = ObjectKey::new(Kind::Volume, "shop", "data-web-1");
    h.write(
        Object::new(
            Kind::Volume,
            "shop",
            "data-web-1",
            serde_json::json!({ "size": "1Gi" }),
        ),
        Precondition::MustNotExist,
    )
    .await;
    let pod = "containers: [{ name: db, image: \"app:1\", volumeMounts: [{ name: data, path: /var/db }] }]\nvolumes: [{ name: data, claim: { size: 1Gi } }]";
    let pod: serde_json::Value = serde_yaml_ng::from_str(pod).unwrap();
    let mut spec: TaskSpec = legion_api::parse(&serde_json::json!({
        "service": "web",
        "serviceUid": "0000000000000001",
        "serviceGeneration": 1,
        "slot": 1,
        "stateful": true,
        "node": NODE,
        "pod": pod,
    }))
    .unwrap();
    spec.admit().unwrap();
    let mut object = Object::new(
        Kind::Task,
        "shop",
        "web-1-1",
        serde_json::to_value(&spec).unwrap(),
    );
    object.status = serde_json::json!({ "state": "assigned" });
    let key = h.write(object, Precondition::MustNotExist).await.key();
    h.wait_state(&key, TaskState::Running).await;

    let ds = "tank/legion/volumes/shop/data-web-1";
    assert_eq!(h.sys.zfs_list(ds).unwrap()[0].quota, Some(1 << 30));
    assert_eq!(h.store.get(&volume).unwrap().unwrap().status["node"], NODE);
    let spec = h.rt.spec("shop_web-1-1_db").unwrap();
    assert!(
        spec.mounts
            .iter()
            .any(|m| m.source == format!("/{ds}") && m.destination == "/var/db"),
        "{:?}",
        spec.mounts
    );
    // Its pod is named for its ordinal.
    assert_eq!(spec.hostname.as_deref(), Some("web-1"));
    let infra = h.rt.spec("shop_web-1-1").unwrap();
    assert_eq!(infra.hostname.as_deref(), Some("web-1"));
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
    assert_eq!(st.message, "volume data is held by node n2");
    assert!(h.sys.zfs_list("tank").unwrap().is_empty());
    assert!(h.rt.containers().is_empty());
}

const NETWORK_VOLUMES: &str = r#"
containers:
  - name: app
    image: "app:1"
    volumeMounts:
      - { name: files, path: /srv/files }
      - { name: block, path: /var/db }
      - { name: objects, path: /srv/objects }
volumes:
  - { name: files, volume: { name: files } }
  - { name: block, volume: { name: block } }
  - { name: objects, volume: { name: objects } }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_volumes() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let iqn = "iqn.2026-10.lan.legion:block";
    h.sys.add_iscsi_target(iqn, &[(0, "da0", Some("ufs"))]);
    // As the last node to use it left it, going down.
    h.sys.make_unclean("/dev/da0");
    let plugin = format!("{}/store", legion_agent::DEFAULT_PLUGIN_DIR);
    h.sys.add_plugin(&plugin);
    for (name, spec) in [
        (
            "files",
            serde_json::json!({ "driver": "nfs", "nfs": { "server": "files.lan", "path": "/export/shop", "options": ["nfsv3", "soft"] } }),
        ),
        (
            "block",
            serde_json::json!({ "driver": "iscsi", "iscsi": { "portal": "192.0.2.9", "target": iqn } }),
        ),
        (
            "objects",
            serde_json::json!({ "driver": "plugin", "plugin": { "name": "store", "options": { "bucket": "shop" } } }),
        ),
    ] {
        h.write(
            Object::new(Kind::Volume, "shop", name, spec),
            Precondition::MustNotExist,
        )
        .await;
    }
    let volume_node = |name: &str| {
        let v = h
            .store
            .get(&ObjectKey::new(Kind::Volume, "shop", name))
            .unwrap()
            .unwrap();
        v.status["node"].as_str().unwrap_or_default().to_owned()
    };

    let one = h.task("app-1", NETWORK_VOLUMES).await;
    eventually("app-1 running", || {
        let st = h.status(&one);
        assert!(!st.state.is_terminal(), "{st:?}");
        st.state == TaskState::Running
    })
    .await;
    let uid = |key: &ObjectKey| h.store.get(key).unwrap().unwrap().metadata.uid;
    let task_volume = |key: &ObjectKey, name: &str| {
        h.files_dir()
            .join(uid(key))
            .join("volumes")
            .join(name)
            .to_str()
            .unwrap()
            .to_owned()
    };
    // NFS, in the task's directory; bound to no node.
    assert_eq!(
        h.sys.mount_source(&task_volume(&one, "files")).as_deref(),
        Some("nfs files.lan:/export/shop")
    );
    assert_eq!(volume_node("files"), "");
    // iSCSI: logged in, mounted once for the node, bound to it.
    let block = h
        ._dir
        .path()
        .join("volumes")
        .join("iscsi")
        .join("shop")
        .join("block");
    let block = block.to_str().unwrap();
    assert_eq!(h.sys.iscsi_targets(), [iqn]);
    assert_eq!(h.sys.mount_source(block).as_deref(), Some("ufs /dev/da0"));
    assert_eq!(volume_node("block"), NODE);
    // The plugin mounted its volume, unshared, so bound as well.
    let objects = task_volume(&one, "objects");
    assert!(
        h.sys
            .programs_run()
            .contains(&format!("{plugin} mount {objects}")),
        "{:?}",
        h.sys.programs_run()
    );
    assert_eq!(volume_node("objects"), NODE);
    let spec = h.rt.spec("shop_app-1_app").unwrap();
    for (source, dest) in [
        (task_volume(&one, "files"), "/srv/files"),
        (block.to_owned(), "/var/db"),
        (objects.clone(), "/srv/objects"),
    ] {
        assert!(
            spec.mounts
                .iter()
                .any(|m| m.source == source && m.destination == dest),
            "{source} at {dest}: {:?}",
            spec.mounts
        );
    }

    // A second task here shares the iSCSI mount.
    let two = h.task("app-2", NETWORK_VOLUMES).await;
    h.wait_state(&two, TaskState::Running).await;
    assert_eq!(h.sys.iscsi_targets(), [iqn]);
    // The first goes: the second still uses the LUN and the plugin's
    // volume, so the node keeps them.
    h.delete(&one).await;
    eventually("app-1's pod is gone", || {
        h.sys.mount_source(&task_volume(&two, "files")).is_some()
            && h.rt.spec("shop_app-1_app").is_none()
            && h.sys
                .programs_run()
                .contains(&format!("{plugin} unmount {objects}"))
    })
    .await;
    assert_eq!(h.sys.iscsi_targets(), [iqn]);
    assert!(h.sys.mount_source(block).is_some());
    assert_eq!(
        (volume_node("block"), volume_node("objects")),
        (NODE.to_owned(), NODE.to_owned())
    );
    // The last goes: unmounted, logged out, let go.
    h.delete(&two).await;
    eventually("the node lets go", || {
        h.sys.iscsi_targets().is_empty()
            && volume_node("block").is_empty()
            && volume_node("objects").is_empty()
    })
    .await;
    assert!(h.sys.mount_source(block).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn network_volume_problems() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let pod = |volume: &str| {
        format!(
            "containers: [{{ name: app, image: \"app:1\", volumeMounts: [{{ name: v, path: /v }}] }}]\n\
             volumes: [{{ name: v, volume: {{ name: {volume} }} }}]"
        )
    };
    let iscsi = |target: &str| serde_json::json!({ "driver": "iscsi", "iscsi": { "portal": "192.0.2.9", "target": target } });
    // A LUN with no file system is refused, not formatted; the node lets
    // go of it.
    h.sys
        .add_iscsi_target("iqn.2026-10.lan.legion:blank", &[(0, "da1", None)]);
    h.write(
        Object::new(
            Kind::Volume,
            "shop",
            "blank",
            iscsi("iqn.2026-10.lan.legion:blank"),
        ),
        Precondition::MustNotExist,
    )
    .await;
    let key = h.task("a-1", &pod("blank")).await;
    let st = h.wait_state(&key, TaskState::Failed).await;
    assert!(
        st.message.contains("holds no file system"),
        "{}",
        st.message
    );
    eventually("logged out of the blank LUN", || {
        h.sys.iscsi_targets().is_empty()
    })
    .await;
    // A LUN another node holds: rejected.
    let mut v = Object::new(
        Kind::Volume,
        "shop",
        "held",
        iscsi("iqn.2026-10.lan.legion:held"),
    );
    v.status = serde_json::json!({ "node": "n2" });
    h.write(v, Precondition::MustNotExist).await;
    let key = h.task("a-2", &pod("held")).await;
    let st = h.wait_state(&key, TaskState::Rejected).await;
    assert_eq!(st.message, "volume held is held by node n2");
    // A plugin the node does not have.
    h.write(
        Object::new(
            Kind::Volume,
            "shop",
            "odd",
            serde_json::json!({ "driver": "plugin", "plugin": { "name": "nope", "shared": true } }),
        ),
        Precondition::MustNotExist,
    )
    .await;
    let key = h.task("a-3", &pod("odd")).await;
    let st = h.wait_state(&key, TaskState::Failed).await;
    assert!(st.message.contains("no volume plugin"), "{}", st.message);
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
        [
            "10.200.5.0/24",
            "100.64.5.1/32",
            "fd00:10:200:5::/64",
            "fd00:100:64::5:1/128"
        ]
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

/// A session opened at the manager, as the CLI opens one.
struct Session {
    tx: tokio::sync::mpsc::Sender<pb::SessionFrame>,
    rx: tonic::Streaming<pb::SessionFrame>,
}

impl Session {
    /// Opens a session, waiting for the agent to be reachable through the
    /// manager.
    async fn open(h: &Harness, open: pb::SessionOpen) -> Result<Session, tonic::Status> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            tx.send(frame(Frame::Open(open.clone()))).await.unwrap();
            let mut client = PodsClient::connect(format!("http://{}", h.addr))
                .await
                .unwrap();
            match client
                .session(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
            {
                Ok(resp) => {
                    return Ok(Session {
                        tx,
                        rx: resp.into_inner(),
                    });
                }
                Err(s)
                    if s.code() == tonic::Code::Unavailable
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(s) => return Err(s),
            }
        }
    }

    async fn send(&self, f: Frame) {
        self.tx.send(frame(f)).await.unwrap();
    }

    async fn next(&mut self) -> Option<Result<Frame, tonic::Status>> {
        tokio::time::timeout(Duration::from_secs(10), self.rx.message())
            .await
            .expect("a frame in time")
            .transpose()
            .map(|r| r.map(|f| f.frame.unwrap()))
    }

    /// Every frame to the end, and the error the session ended with.
    async fn rest(&mut self) -> (Vec<Frame>, Option<(tonic::Code, String)>) {
        let mut frames = Vec::new();
        loop {
            match self.next().await {
                Some(Ok(f)) => frames.push(f),
                Some(Err(s)) => return (frames, Some((s.code(), s.message().to_owned()))),
                None => return (frames, None),
            }
        }
    }
}

fn frame(f: Frame) -> pb::SessionFrame {
    pb::SessionFrame { frame: Some(f) }
}

fn open(task: &str, action: Action) -> pb::SessionOpen {
    pb::SessionOpen {
        namespace: "shop".into(),
        task: task.into(),
        container: String::new(),
        action: Some(action),
    }
}

fn exec(command: &[&str], stdin: bool, tty: bool) -> Action {
    Action::Exec(pb::ExecOptions {
        command: command.iter().map(|s| s.to_string()).collect(),
        stdin,
        tty,
        size: tty.then_some(pb::TerminalSize {
            width: 80,
            height: 24,
        }),
    })
}

fn logs(follow: bool, tail: Option<u32>) -> Action {
    Action::Logs(pb::LogsOptions { follow, tail })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_reach_the_pod() {
    let h = Harness::new().await;
    h.rt.add_image("app:1");
    let key = h
        .task("web-1", r#"containers: [{ name: app, image: "app:1" }]"#)
        .await;
    h.wait_state(&key, TaskState::Running).await;
    const APP: &str = "shop_web-1_app";
    h.rt.log(APP, Output::Stdout, "one\n");
    h.rt.log(APP, Output::Stderr, "two\n");

    // Logs, of the task or its service's running task.
    for task in ["web-1", "web"] {
        let mut s = Session::open(&h, open(task, logs(false, None)))
            .await
            .unwrap();
        assert_eq!(
            s.rest().await,
            (
                vec![
                    Frame::Stdout(b"one\n".to_vec()),
                    Frame::Stderr(b"two\n".to_vec())
                ],
                None
            )
        );
    }
    let mut s = Session::open(&h, open("web-1", logs(false, Some(1))))
        .await
        .unwrap();
    assert_eq!(
        s.rest().await,
        (vec![Frame::Stderr(b"two\n".to_vec())], None)
    );

    // Exec with stdin: `cat` echoes until its input closes.
    let mut s = Session::open(&h, open("web-1", exec(&["cat"], true, false)))
        .await
        .unwrap();
    s.send(Frame::Input(b"abc".to_vec())).await;
    assert_eq!(
        s.next().await.unwrap().unwrap(),
        Frame::Stdout(b"abc".to_vec())
    );
    s.send(Frame::InputClosed(pb::InputClosed {})).await;
    assert_eq!(s.rest().await, (vec![Frame::ExitCode(0)], None));

    // Under a terminal of the size asked for; the exit code comes last.
    h.rt.set_exec(APP, 3);
    let mut s = Session::open(&h, open("web-1", exec(&["ls", "-l"], false, true)))
        .await
        .unwrap();
    assert_eq!(
        s.rest().await,
        (
            vec![
                Frame::Stdout(b"ran ls -l\n24 80\n".to_vec()),
                Frame::Stderr(b"failed\n".to_vec()),
                Frame::ExitCode(3),
            ],
            None
        )
    );

    // A user who goes away takes the process's streams with them.
    let s = Session::open(&h, open("web-1", exec(&["cat"], true, false)))
        .await
        .unwrap();
    eventually("cat runs", || h.rt.attached() == 1).await;
    drop(s);
    eventually("cat ends", || h.rt.attached() == 0).await;

    // What cannot be opened says why.
    let refused = |r: Result<Session, tonic::Status>| {
        let s = r.err().expect("refused");
        (s.code(), s.message().to_owned())
    };
    let mut bad = open("web-1", logs(false, None));
    bad.container = "db".into();
    assert_eq!(
        refused(Session::open(&h, bad).await),
        (
            tonic::Code::NotFound,
            "task web-1 has no container db; it has app".into()
        )
    );
    assert_eq!(
        refused(Session::open(&h, open("nope", logs(false, None))).await),
        (
            tonic::Code::NotFound,
            "no task, service or job nope in namespace shop".into()
        )
    );
    // Errors from the agent end the stream.
    let mut s = Session::open(&h, open("web-1", exec(&[], false, false)))
        .await
        .unwrap();
    assert_eq!(
        s.rest().await,
        (
            vec![],
            Some((tonic::Code::InvalidArgument, "exec needs a command".into()))
        )
    );

    // Port-forward: one connection per session, to the pod's address
    // (here a local echo server standing in for the pod).
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = echo.local_addr().unwrap().port();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut c, _) = echo.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            let n = c.read(&mut buf).await.unwrap();
            if n == 0 {
                return;
            }
            c.write_all(&buf[..n]).await.unwrap();
        }
    });
    let mut task = h.store.get(&key).unwrap().unwrap();
    task.status["ip"] = "127.0.0.1".into();
    let v = task.metadata.version;
    h.write(task, Precondition::Version(v)).await;
    let mut s = Session::open(&h, open("web-1", Action::Port(port.into())))
        .await
        .unwrap();
    s.send(Frame::Input(b"ping".to_vec())).await;
    assert_eq!(
        s.next().await.unwrap().unwrap(),
        Frame::Stdout(b"ping".to_vec())
    );
    s.send(Frame::InputClosed(pb::InputClosed {})).await;
    assert_eq!(s.rest().await, (vec![], None));

    // Followed logs go on until the container stops. The session is open
    // before the agent follows, so they are followed from the start: a
    // line written in between comes either way.
    let mut s = Session::open(&h, open("web-1", logs(true, None)))
        .await
        .unwrap();
    h.rt.log(APP, Output::Stdout, "three\n");
    for want in [
        Frame::Stdout(b"one\n".to_vec()),
        Frame::Stderr(b"two\n".to_vec()),
        Frame::Stdout(b"three\n".to_vec()),
    ] {
        assert_eq!(s.next().await.unwrap().unwrap(), want);
    }
    h.rt.exit(APP, 0);
    assert_eq!(s.rest().await, (vec![], None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_tasks_keep_their_output() {
    let h = Harness::new().await;
    h.rt.add_image("job:1");
    let key = h
        .task(
            "web-1",
            r#"
initContainers: [{ name: setup, image: "job:1" }]
containers: [{ name: app, image: "job:1" }]
restartPolicy: { condition: none }
"#,
        )
        .await;
    h.wait_state(&key, TaskState::Running).await;
    h.rt.log("shop_web-1_app", Output::Stdout, "done 1\ndone 2\n");
    h.rt.log("shop_web-1_app", Output::Stderr, "warned\n");
    h.rt.exit("shop_web-1_app", 0);
    h.wait_state(&key, TaskState::Complete).await;
    h.nothing_left().await;

    // By the task's name, or its owner's when nothing of it runs.
    for task in ["web-1", "web"] {
        let mut s = Session::open(&h, open(task, logs(false, None)))
            .await
            .unwrap();
        assert_eq!(
            s.rest().await,
            (
                vec![
                    Frame::Stdout(b"done 1\n".to_vec()),
                    Frame::Stdout(b"done 2\n".to_vec()),
                    Frame::Stderr(b"warned\n".to_vec()),
                ],
                None
            )
        );
    }
    let mut s = Session::open(&h, open("web-1", logs(true, Some(1))))
        .await
        .unwrap();
    assert_eq!(
        s.rest().await,
        (vec![Frame::Stderr(b"warned\n".to_vec())], None)
    );
    let mut setup = open("web-1", logs(false, None));
    setup.container = "setup".into();
    let mut s = Session::open(&h, setup).await.unwrap();
    assert_eq!(s.rest().await, (vec![], None));
    // Exec needs the pod.
    let refused = Session::open(&h, open("web-1", exec(&["ls"], false, false)))
        .await
        .err()
        .expect("refused");
    assert_eq!(refused.message(), "task web-1 is complete, not running");

    // The output goes with the task.
    let kept = h
        .logs_dir()
        .join(&h.store.get(&key).unwrap().unwrap().metadata.uid);
    assert!(kept.join("app").exists());
    h.delete(&key).await;
    eventually("output removed", || !kept.exists()).await;
}
