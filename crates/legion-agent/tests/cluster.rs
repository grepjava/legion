//! Three nodes in one process: a served manager and three agents, each
//! with its own faked Podman and system, all through the dispatcher. Tasks
//! spread over the nodes, every node balances every service, and a node
//! that goes silent loses its tasks to the others.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig};
use legion_api::node::{NodeState, NodeStatus};
use legion_api::{Kind, Object};
use legion_manager::{Manager, ManagerConfig};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{ApplyRequest, InitRequest, ListRequest};
use legion_sys::{FakeSys, Sys};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

const INFRA: &str = "docker.io/freebsd/freebsd-runtime:15.1";
const HEARTBEAT: Duration = Duration::from_millis(200);

struct Node {
    name: String,
    rt: Arc<FakeRuntime>,
    sys: Arc<FakeSys>,
    agent: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

struct Cluster {
    addr: String,
    api: ObjectsClient<Channel>,
    nodes: Vec<Node>,
    stop: Option<oneshot::Sender<()>>,
    server: JoinHandle<()>,
    dir: tempfile::TempDir,
}

impl Cluster {
    async fn start(nodes: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut config = ManagerConfig::new(dir.path().join("manager"), &addr);
        config.heartbeat = HEARTBEAT;
        let manager = Manager::open(config).await.unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            manager
                .serve(listener, async move {
                    let _ = stopped.await;
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

        let mut c = Cluster {
            addr,
            api: ObjectsClient::new(channel),
            nodes: vec![],
            stop: Some(stop),
            server,
            dir,
        };
        for n in 1..=nodes {
            let sys = Arc::new(FakeSys::new());
            sys.set_default_interface("re0");
            let rt = Arc::new(FakeRuntime::new(sys.clone()));
            rt.add_image(INFRA);
            rt.add_image("web:1");
            c.nodes.push(Node {
                name: format!("n{n}"),
                rt,
                sys,
                agent: None,
            });
            c.start_agent(n - 1);
            // Registered in order, so node n has the pod subnet 10.200.n-1.
            c.eventually_node_ready(&format!("n{n}")).await;
        }
        c
    }

    fn start_agent(&mut self, i: usize) {
        let node = &self.nodes[i];
        let dir = self.dir.path().join(&node.name);
        // Each fake system would make the same first key.
        std::fs::create_dir_all(&dir).unwrap();
        let key_file = dir.join("wireguard.key");
        if !key_file.exists() {
            std::fs::write(&key_file, format!("key-{}\n", node.name)).unwrap();
        }
        let mut config = AgentConfig::new(&node.name, dir.join("tasks"));
        config.drain = Duration::ZERO;
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.wireguard = Some(WireGuardConfig {
            key_file,
            port: 51820,
        });
        let control = GrpcControl::new(&node.name, vec![self.addr.clone()], None).unwrap();
        let agent = Agent::new(
            config,
            node.rt.clone(),
            node.sys.clone() as Arc<dyn Sys>,
            Arc::new(control),
        );
        let (stop, stopped) = oneshot::channel::<()>();
        let status = NodeStatus {
            state: NodeState::Ready,
            addr: format!("192.0.2.{}", i + 1),
            ..Default::default()
        };
        let handle = tokio::spawn(agent.run(status, async move {
            let _ = stopped.await;
        }));
        self.nodes[i].agent = Some((stop, handle));
    }

    /// Stops a node's agent: its pods keep running, but it no longer
    /// answers, as a node cut off from the managers.
    async fn stop_agent(&mut self, i: usize) {
        if let Some((stop, handle)) = self.nodes[i].agent.take() {
            let _ = stop.send(());
            tokio::time::timeout(Duration::from_secs(15), handle)
                .await
                .expect("agent stops")
                .unwrap();
        }
    }

    async fn eventually_node_ready(&self, name: &str) {
        eventually(&format!("{name} ready"), async || {
            self.list(Kind::Node)
                .await
                .iter()
                .any(|n| n.metadata.name == name && n.status["state"] == "ready")
        })
        .await;
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

    /// The nodes running web tasks, sorted.
    async fn placed(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .list(Kind::Task)
            .await
            .iter()
            .filter(|t| t.status["state"] == "running")
            .map(|t| t.spec["node"].as_str().unwrap().to_owned())
            .collect();
        v.sort();
        v
    }

    /// The running tasks' addresses.
    async fn addresses(&self) -> BTreeSet<Ipv4Addr> {
        self.list(Kind::Task)
            .await
            .iter()
            .filter(|t| t.status["state"] == "running")
            .map(|t| t.status["ip"].as_str().unwrap().parse().unwrap())
            .collect()
    }

    /// What node `i`'s pf balances the service over.
    fn table(&self, i: usize, uid: &str) -> BTreeSet<Ipv4Addr> {
        self.nodes[i]
            .sys
            .pf_table_show(legion_net::lb::ANCHOR, &format!("s{uid}"))
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    fn pods(&self, i: usize) -> Vec<String> {
        self.nodes[i]
            .rt
            .containers()
            .into_iter()
            .filter(|(_, running, _)| *running)
            .map(|(n, _, _)| n)
            .filter(|n| !n.ends_with("_web"))
            .collect()
    }

    async fn stop(mut self) {
        for i in 0..self.nodes.len() {
            self.stop_agent(i).await;
        }
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

const WEB: &str = r#"
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  replicas: 3
  pod:
    containers: [{ name: web, image: "web:1", ports: [{ name: http, containerPort: 8080 }] }]
  endpoint:
    ports: [{ port: 80, targetPort: http, publish: ingress, publishedPort: 30080 }]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tasks_survive_losing_a_node() {
    let mut c = Cluster::start(3).await;
    c.apply(WEB).await;
    eventually("one task per node", async || {
        c.placed().await == ["n1", "n2", "n3"]
    })
    .await;
    let uid = c.list(Kind::Service).await[0].metadata.uid.clone();

    // Every node balances the VIP and the published port over every task,
    // and reaches the others over the mesh.
    let all = c.addresses().await;
    let subnets: BTreeSet<[u8; 3]> = all
        .iter()
        .map(|ip| {
            let o = ip.octets();
            [o[0], o[1], o[2]]
        })
        .collect();
    assert_eq!(subnets.len(), 3, "{all:?}");
    for i in 0..3 {
        eventually("the node balances over all tasks", async || {
            c.table(i, &uid) == all
        })
        .await;
        let rules = c.nodes[i].sys.anchor(legion_net::lb::ANCHOR).unwrap();
        assert!(rules.contains("to (re0) port 30080"), "{rules}");
        // Its pod takes traffic from its namespace's tasks, all three.
        eventually("the node guards its pod", async || {
            let Some(rules) = c.nodes[i].sys.anchor(legion_net::policy::ANCHOR) else {
                return false;
            };
            let Some(table) = rules
                .split(['<', '>'])
                .nth(1)
                .filter(|_| rules.contains("block drop out quick on lp"))
            else {
                return false;
            };
            let addrs: BTreeSet<Ipv4Addr> = c.nodes[i]
                .sys
                .pf_table_show(legion_net::policy::ANCHOR, table)
                .unwrap_or_default()
                .into_iter()
                .collect();
            addrs == all
        })
        .await;
        eventually("the node has the others as peers", async || {
            c.nodes[i]
                .sys
                .wireguard(legion_net::mesh::INTERFACE)
                .is_some_and(|wg| wg.peers.len() == 2)
        })
        .await;
    }

    // n2 goes silent: once it is down its task is orphaned, a new one runs
    // elsewhere, and the others stop sending to the lost one.
    let lost: BTreeSet<Ipv4Addr> = c
        .list(Kind::Task)
        .await
        .iter()
        .filter(|t| t.spec["node"] == "n2")
        .map(|t| t.status["ip"].as_str().unwrap().parse().unwrap())
        .collect();
    c.stop_agent(1).await;
    eventually("n2 down", async || {
        c.list(Kind::Node)
            .await
            .iter()
            .any(|n| n.metadata.name == "n2" && n.status["state"] == "down")
    })
    .await;
    eventually("three tasks on n1 and n3", async || {
        let placed = c.placed().await;
        placed.len() == 3 && !placed.contains(&"n2".to_owned())
    })
    .await;
    let tasks = c.list(Kind::Task).await;
    let orphan = tasks.iter().find(|t| t.spec["node"] == "n2").unwrap();
    assert_eq!(orphan.status["state"], "orphaned");
    assert_eq!(orphan.spec["desiredState"], "shutdown");
    let now = c.addresses().await;
    assert!(now.is_disjoint(&lost));
    for i in [0, 2] {
        eventually("the node balances over the tasks left", async || {
            c.table(i, &uid) == now
        })
        .await;
    }
    // Its pod is still there: nothing could tell it to stop.
    assert_eq!(c.pods(1).len(), 1);

    // n2 comes back: it removes the lost task's pod, and is a peer again,
    // but the tasks stay where they are.
    c.start_agent(1);
    c.eventually_node_ready("n2").await;
    eventually("the lost pod removed", async || c.pods(1).is_empty()).await;
    eventually("n2 balances over the tasks", async || {
        c.table(1, &uid) == now
    })
    .await;
    let placed = c.placed().await;
    assert_eq!(placed.len(), 3);
    assert!(!placed.contains(&"n2".to_owned()), "{placed:?}");

    // A NoExecute taint moves n1's task away.
    c.apply(
        r#"
apiVersion: legion/v1
kind: Node
metadata: { name: n1 }
spec:
  taints: [{ key: maintenance, effect: NoExecute }]
"#,
    )
    .await;
    eventually("nothing on n1", async || {
        let placed = c.placed().await;
        placed.len() == 3 && !placed.contains(&"n1".to_owned())
    })
    .await;
    eventually("n1's pods gone", async || c.pods(0).is_empty()).await;

    c.stop().await;
}
