//! The agent against a real single-manager store, with Podman and the
//! system faked in memory.

use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::{Agent, AgentConfig, StoreControl};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object, ObjectKey, Spec};
use legion_manager::{Manager, ManagerConfig, default_raft_config};
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
    _manager: Manager,
    store: Store,
    sys: Arc<FakeSys>,
    rt: Arc<FakeRuntime>,
    agent: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let manager = Manager::open(ManagerConfig {
            data_dir: dir.path().to_owned(),
            advertise: "127.0.0.1:1".into(),
            raft: default_raft_config(),
        })
        .await
        .unwrap();
        let store = manager.store().clone();
        let id = manager.id();
        store
            .raft
            .initialize(
                [(
                    id,
                    BasicNode {
                        addr: "127.0.0.1:1".into(),
                    },
                )]
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>(),
            )
            .await
            .unwrap();
        eventually("leader", || {
            store.raft.metrics().borrow().current_leader == Some(id)
        })
        .await;
        let sys = Arc::new(FakeSys::new());
        let rt = Arc::new(FakeRuntime::new(sys.clone()));
        let mut h = Harness {
            _dir: dir,
            _manager: manager,
            store,
            sys,
            rt,
            agent: None,
        };
        h.start_agent();
        h
    }

    fn start_agent(&mut self) {
        let mut config = AgentConfig::new(NODE);
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        let agent = Agent::new(
            config,
            self.rt.clone(),
            self.sys.clone() as Arc<dyn Sys>,
            Arc::new(StoreControl::new(self.store.clone())),
        );
        let (tx, rx) = oneshot::channel();
        let status = NodeStatus {
            state: NodeState::Ready,
            hostname: "n1.example".into(),
            ..Default::default()
        };
        let handle = tokio::spawn(agent.run(status, async move {
            let _ = rx.await;
        }));
        self.agent = Some((tx, handle));
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
        ["if_bridge", "if_epair", "pf"]
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
            format!("jail:{infra}.{web}:vmemoryuse:deny=268435456"),
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

    let vol = h
        .task(
            "v-1",
            r#"
containers: [{ name: app, image: "web:1", volumeMounts: [{ name: data, path: /data }] }]
volumes: [{ name: data, tmpfs: {} }]
"#,
        )
        .await;
    let status = h.wait_state(&vol, TaskState::Rejected).await;
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
