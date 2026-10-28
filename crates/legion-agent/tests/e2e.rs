//! From `apply` to running pods in one process: a served manager (API and
//! controllers) and an agent with Podman and the system faked.

use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::{Agent, AgentConfig, EndpointPort, GrpcControl, ServiceEndpoints};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::{Kind, Object};
use legion_manager::{Manager, ManagerConfig};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{ApplyRequest, DeleteRequest, InitRequest, ListRequest, ObjectRef};
use legion_sys::{FakeSys, Sys};
use tokio::sync::oneshot;
use tonic::transport::Channel;

const INFRA: &str = "docker.io/freebsd/freebsd-runtime:15.1";

struct Cluster {
    api: ObjectsClient<Channel>,
    rt: Arc<FakeRuntime>,
    sys: Arc<FakeSys>,
    /// What the node resolver and load balancing see.
    endpoints: tokio::sync::watch::Receiver<legion_agent::Endpoints>,
    stop: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let manager = Manager::open(ManagerConfig::new(dir.path(), &addr))
            .await
            .unwrap();

        let sys = Arc::new(FakeSys::new());
        sys.set_default_interface("re0");
        let rt = Arc::new(FakeRuntime::new(sys.clone()));
        rt.add_image(INFRA);
        let mut config = AgentConfig::new("n1", dir.path().join("tasks"));
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        let control = GrpcControl::new("n1", vec![addr.clone()], None).unwrap();
        let endpoints = legion_agent::Control::endpoints(&control);
        let agent = Agent::new(
            config,
            rt.clone(),
            sys.clone() as Arc<dyn Sys>,
            Arc::new(control),
        );
        let (agent_stop, agent_stopped) = oneshot::channel::<()>();
        let agent = tokio::spawn(agent.run(
            NodeStatus {
                state: NodeState::Ready,
                ..Default::default()
            },
            async move {
                let _ = agent_stopped.await;
            },
        ));

        // As the daemon does: the agent stops before the store closes.
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            manager
                .serve(listener, async move {
                    let _ = stopped.await;
                    let _ = agent_stop.send(());
                    let _ = agent.await;
                })
                .await
                .unwrap();
        });

        let url = format!("http://{addr}");
        let channel = loop {
            if let Ok(c) = Channel::from_shared(url.clone()).unwrap().connect().await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ClusterClient::new(channel.clone())
            .init(InitRequest {})
            .await
            .unwrap();
        Cluster {
            api: ObjectsClient::new(channel),
            rt,
            sys,
            endpoints,
            stop: Some(stop),
            server,
            _dir: dir,
        }
    }

    async fn apply(&self, yaml: &str) {
        let o: Object = serde_yaml_ng::from_str(yaml).unwrap();
        self.api
            .clone()
            .apply(ApplyRequest {
                object: Some((&o).into()),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    async fn list(&self, kind: Kind) -> Vec<Object> {
        let resp = self
            .api
            .clone()
            .list(ListRequest {
                kind: kind.to_string(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        resp.objects
            .into_iter()
            .map(|o| Object::try_from(o).unwrap())
            .collect()
    }

    /// `name state` of every task, sorted.
    async fn tasks(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .list(Kind::Task)
            .await
            .iter()
            .map(|t| {
                format!(
                    "{} {}",
                    t.metadata.name,
                    t.status["state"].as_str().unwrap_or("?")
                )
            })
            .collect();
        v.sort();
        v
    }

    /// Names of running containers, sorted.
    fn running(&self) -> Vec<String> {
        self.rt
            .containers()
            .into_iter()
            .filter(|(_, running, _)| *running)
            .map(|(n, _, _)| n)
            .collect()
    }

    async fn stop(mut self) {
        // Followers of the store end with their last receiver, and the
        // store closes only once they have.
        drop(self.endpoints);
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(15), self.server)
            .await
            .expect("server stops")
            .unwrap();
    }
}

async fn eventually<F: AsyncFnMut() -> bool>(what: &str, mut f: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !f().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn service(replicas: u32, image: &str) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Service
metadata: {{ name: web, namespace: shop }}
spec:
  replicas: {replicas}
  pod:
    containers: [{{ name: web, image: "{image}", ports: [{{ name: http, containerPort: 8080 }}] }}]
    restartPolicy: {{ condition: on-failure, maxAttempts: 1, delay: 0s }}
  endpoint:
    ports: [{{ port: 80, targetPort: http, publish: ingress }}]
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_to_running_pods() {
    let c = Cluster::start().await;
    c.rt.add_image("web:1");
    c.rt.add_image("web:2");

    c.apply(&service(2, "web:1")).await;
    eventually("two running tasks", async || {
        c.tasks().await == ["web-1-1 running", "web-2-1 running"]
    })
    .await;
    assert_eq!(
        c.running(),
        [
            "shop_web-1-1",
            "shop_web-1-1_web",
            "shop_web-2-1",
            "shop_web-2-1_web"
        ]
    );
    eventually("service status", async || {
        let s = &c.list(Kind::Service).await[0];
        s.status["running"] == 2 && s.status["ready"] == 2 && s.status["desired"] == 2
    })
    .await;
    // Both tasks are the service's endpoints, behind its VIP.
    let uid = c.list(Kind::Service).await[0].metadata.uid.clone();
    let both: Vec<std::net::Ipv4Addr> = vec![[10, 200, 0, 2].into(), [10, 200, 0, 3].into()];
    eventually("endpoints", async || {
        *c.endpoints.borrow()
            == [ServiceEndpoints {
                namespace: "shop".into(),
                name: "web".into(),
                uid: uid.clone(),
                labels: Default::default(),
                vip: Some([10, 201, 0, 1].into()),
                ports: vec![EndpointPort {
                    protocol: legion_api::service::Protocol::Tcp,
                    port: 80,
                    target: 8080,
                    published: Some(30000),
                }],
                ready: both.clone(),
                alive: both.clone(),
            }]
    })
    .await;
    // pf balances the VIP and the published port over the ready tasks.
    let table = format!("s{uid}");
    eventually("the service's pf table", async || {
        c.sys.pf_table_show(legion_net::lb::ANCHOR, &table).unwrap() == both
    })
    .await;
    let rules = c.sys.anchor(legion_net::lb::ANCHOR).unwrap();
    assert!(
        rules.contains(&format!(
            "rdr pass on legion0 inet proto tcp to 10.201.0.1 port 80 tag LSVC -> <{table}> port 8080"
        )),
        "{rules}"
    );
    assert!(rules.contains("to (re0) port 30000"), "{rules}");
    assert!(
        rules.contains("nat pass on re0 inet from 10.200.0.0/24"),
        "{rules}"
    );
    assert_eq!(c.sys.sysctl("net.inet.ip.forwarding").as_deref(), Some("1"));
    // pf sees the pod bridge's members, for network policy.
    for name in [
        "net.link.bridge.pfil_member",
        "net.link.bridge.pfil_local_phys",
    ] {
        assert_eq!(c.sys.sysctl(name).as_deref(), Some("1"), "{name}");
    }
    // The node registered itself and the scheduler used it.
    let nodes = c.list(Kind::Node).await;
    assert_eq!(nodes[0].metadata.name, "n1");
    let task = &c.list(Kind::Task).await[0];
    assert_eq!(task.spec["node"], "n1");
    assert_eq!(task.metadata.labels["legion.service"], "web");

    // A new image rolls through, one slot at a time.
    c.apply(&service(2, "web:2")).await;
    eventually("updated", async || {
        c.running()
            == [
                "shop_web-1-2",
                "shop_web-1-2_web",
                "shop_web-2-2",
                "shop_web-2-2_web",
            ]
    })
    .await;
    eventually("old tasks shut down", async || {
        c.tasks().await
            == [
                "web-1-1 shutdown",
                "web-1-2 running",
                "web-2-1 shutdown",
                "web-2-2 running",
            ]
    })
    .await;

    // Scaling down removes a slot with its history.
    c.apply(&service(1, "web:2")).await;
    eventually("scaled down", async || {
        c.tasks().await == ["web-1-1 shutdown", "web-1-2 running"]
    })
    .await;
    assert_eq!(c.running(), ["shop_web-1-2", "shop_web-1-2_web"]);
    eventually("one endpoint", async || {
        c.endpoints.borrow()[0].ready.len() == 1
    })
    .await;

    // A container that keeps failing exhausts the restart policy; the task
    // fails and the controller replaces it.
    c.rt.exit("shop_web-1-2_web", 1);
    eventually("restarted once", async || {
        c.rt.containers()
            .iter()
            .any(|(n, running, starts)| n == "shop_web-1-2_web" && *running && *starts == 2)
    })
    .await;
    c.rt.exit("shop_web-1-2_web", 1);
    eventually("replaced", async || {
        c.tasks().await.contains(&"web-1-3 running".to_owned())
    })
    .await;
    assert!(c.tasks().await.contains(&"web-1-2 failed".to_owned()));

    // Deleting the service removes its tasks and pods.
    c.api
        .clone()
        .delete(DeleteRequest {
            r#ref: Some(ObjectRef {
                kind: "Service".into(),
                namespace: "shop".into(),
                name: "web".into(),
            }),
            expected_version: None,
        })
        .await
        .unwrap();
    eventually("all gone", async || {
        c.tasks().await.is_empty() && c.running().is_empty()
    })
    .await;

    c.stop().await;
}

const JOB: &str = r#"
apiVersion: legion/v1
kind: Job
metadata: { name: backup, namespace: shop }
spec:
  completions: 2
  pod:
    containers: [{ name: dump, image: "tools:1" }]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_runs_to_completion() {
    let c = Cluster::start().await;
    c.rt.add_image("tools:1");
    c.apply(JOB).await;

    // One at a time: the second starts when the first completes.
    eventually("the first task", async || {
        c.tasks().await == ["backup-1-1 running"]
    })
    .await;
    c.rt.exit("shop_backup-1-1_dump", 0);
    eventually("the second task", async || {
        c.tasks().await == ["backup-1-1 complete", "backup-2-1 running"]
    })
    .await;
    // A failure is retried as a new task; the Job still completes.
    c.rt.exit("shop_backup-2-1_dump", 1);
    eventually("the retry", async || {
        c.tasks().await.contains(&"backup-2-2 running".to_owned())
    })
    .await;
    c.rt.exit("shop_backup-2-2_dump", 0);
    eventually("the job complete", async || {
        let j = &c.list(Kind::Job).await[0];
        j.status["state"] == "complete"
    })
    .await;
    let j = &c.list(Kind::Job).await[0];
    assert_eq!(
        (
            &j.status["succeeded"],
            &j.status["failed"],
            &j.status["active"]
        ),
        (&2.into(), &1.into(), &0.into())
    );
    eventually("pods gone", async || c.running().is_empty()).await;

    // Deleting the Job removes its tasks.
    c.api
        .clone()
        .delete(DeleteRequest {
            r#ref: Some(ObjectRef {
                kind: "Job".into(),
                namespace: "shop".into(),
                name: "backup".into(),
            }),
            expected_version: None,
        })
        .await
        .unwrap();
    eventually("tasks gone", async || c.tasks().await.is_empty()).await;

    c.stop().await;
}
