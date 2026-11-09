//! Three nodes in one process: a served manager and three agents, each
//! with its own faked runtime and system, all through the dispatcher. Tasks
//! spread over the nodes, every node balances every service, a node that
//! goes silent loses its tasks to the others, and a stateful task's data
//! moves with it off a draining node.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use legion_agent::fake::FakeRuntime;
use legion_agent::{Agent, AgentConfig, GrpcControl, WireGuardConfig, WireGuardKey};
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

/// A table entry's IPv4 address (tables hold the IPv6 twins as well).
fn v4(a: IpAddr) -> Option<Ipv4Addr> {
    match a {
        IpAddr::V4(a) => Some(a),
        IpAddr::V6(_) => None,
    }
}

struct Node {
    name: String,
    rt: Arc<FakeRuntime>,
    sys: Arc<FakeSys>,
    agent: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    /// The node's way to the manager.
    link: Link,
    /// What its agent says it speaks: this build's unless a test poses as
    /// another.
    protocol: legion_api::protocol::Protocol,
}

/// A TCP proxy between an agent and the manager that a test can cut, for a
/// node cut off while its agent keeps running. Cut, it is silent, as a
/// split network is: connections stay open and nothing passes, new ones
/// included. Mended, those connections end, and new ones pass.
struct Link {
    addr: String,
    cut: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Link {
    async fn to(target: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let cut = Arc::new(tokio::sync::watch::Sender::new(false));
        let (target, watch) = (target.to_owned(), cut.clone());
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let (target, mut cut) = (target.clone(), watch.subscribe());
                tokio::spawn(async move {
                    if *cut.borrow_and_update() {
                        let _ = cut.wait_for(|c| !*c).await;
                        return;
                    }
                    let Ok(outbound) = tokio::net::TcpStream::connect(&target).await else {
                        return;
                    };
                    let (a, b) = (inbound.into_split(), outbound.into_split());
                    let (up, down) = (cut.clone(), cut.clone());
                    tokio::select! {
                        () = pass(a.0, b.1, up) => {}
                        () = pass(b.0, a.1, down) => {}
                        _ = async {
                            let _ = cut.wait_for(|c| *c).await;
                            // Held, silent, until mended.
                            let _ = cut.wait_for(|c| !*c).await;
                        } => {}
                    }
                });
            }
        });
        Link { addr, cut }
    }

    fn cut(&self) {
        self.cut.send_replace(true);
    }

    fn mend(&self) {
        self.cut.send_replace(false);
    }
}

/// Copies what `from` reads to `to` until either ends. Each read is looked
/// at against the cut before it passes, so nothing written once the cut is
/// made gets through: a cut racing the copy would otherwise let some by.
async fn pass(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut cut: tokio::sync::watch::Receiver<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 16 << 10];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if *cut.borrow_and_update() {
            // Dropped, and the link silent until mended.
            let _ = cut.wait_for(|c| !*c).await;
            return;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

struct Cluster {
    addr: String,
    api: ObjectsClient<Channel>,
    nodes: Vec<Node>,
    stop: Option<oneshot::Sender<()>>,
    server: JoinHandle<()>,
    dir: tempfile::TempDir,
    heartbeat: Duration,
}

/// Serves a manager on `addr` with its data in `dir`.
async fn serve_manager(
    dir: &std::path::Path,
    addr: &str,
    heartbeat: Duration,
) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let mut config = ManagerConfig::new(dir.join("manager"), addr);
    config.heartbeat = heartbeat;
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
    (stop, server)
}

impl Cluster {
    async fn start(nodes: usize) -> Self {
        Self::start_with(nodes, HEARTBEAT).await
    }

    async fn start_with(nodes: usize, heartbeat: Duration) -> Self {
        let dir = tempfile::tempdir().unwrap();
        // A free port, for the manager to have again once restarted.
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        let (stop, server) = serve_manager(dir.path(), &addr, heartbeat).await;
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
            heartbeat,
        };
        for n in 1..=nodes {
            let i = c.add_node(legion_api::protocol::Protocol::current()).await;
            c.start_agent(i);
            // Registered in order, so node n has the pod subnet 10.200.n-1.
            c.eventually_node_ready(&format!("n{n}")).await;
        }
        c
    }

    /// Adds a node, its agent not started, whose agent says it speaks
    /// `protocol`. Returns its index.
    async fn add_node(&mut self, protocol: legion_api::protocol::Protocol) -> usize {
        let sys = Arc::new(FakeSys::new());
        sys.set_default_interface("re0");
        let rt = Arc::new(FakeRuntime::new(sys.clone()));
        rt.add_image(INFRA);
        rt.add_image("web:1");
        let link = Link::to(&self.addr).await;
        self.nodes.push(Node {
            name: format!("n{}", self.nodes.len() + 1),
            rt,
            sys,
            agent: None,
            link,
            protocol,
        });
        self.nodes.len() - 1
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
        config.zfs_root = Some("tank/legion".into());
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.usage_period = Duration::from_millis(100);
        config.wireguard = Some(WireGuardConfig {
            key: WireGuardKey::File(key_file),
            port: 51820,
        });
        let control = GrpcControl::new(&node.name, vec![node.link.addr.clone()], None).unwrap();
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
            protocol: node.protocol,
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

    /// Has the manager start shutting down: its controllers and migrator
    /// stop at once, as a manager's do that is killed.
    fn stop_manager(&mut self) {
        self.stop.take().unwrap().send(()).unwrap();
    }

    /// Waits for the manager [`Self::stop_manager`] stopped to be gone,
    /// then starts it again on its data, answering once it does.
    async fn restart_manager(&mut self) {
        let server = std::mem::replace(&mut self.server, tokio::spawn(async {}));
        tokio::time::timeout(Duration::from_secs(15), server)
            .await
            .expect("manager stops")
            .unwrap();
        let (stop, server) = serve_manager(self.dir.path(), &self.addr, self.heartbeat).await;
        (self.stop, self.server) = (Some(stop), server);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while self
            .api
            .clone()
            .list(ListRequest {
                kind: Kind::Node.to_string(),
                ..Default::default()
            })
            .await
            .is_err()
        {
            assert!(tokio::time::Instant::now() < deadline, "manager back");
            tokio::time::sleep(Duration::from_millis(50)).await;
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

    /// Sets a node's availability (`active`, `pause`, `drain`).
    async fn availability(&self, name: &str, availability: &str) {
        let mut node = self
            .list(Kind::Node)
            .await
            .into_iter()
            .find(|n| n.metadata.name == name)
            .unwrap();
        node.spec["availability"] = availability.into();
        self.api
            .clone()
            .apply(ApplyRequest {
                object: Some((&node).into()),
                ..Default::default()
            })
            .await
            .unwrap();
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

    /// Sets a label on Node `name`, as `legion node label` does.
    async fn label_node(&self, name: &str, key: &str, value: &str) {
        let mut node = self
            .list(Kind::Node)
            .await
            .into_iter()
            .find(|n| n.metadata.name == name)
            .unwrap();
        node.metadata.labels.insert(key.into(), value.into());
        self.api
            .clone()
            .apply(ApplyRequest {
                object: Some((&node).into()),
                expected_version: Some(node.metadata.version),
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
            .filter_map(v4)
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

/// Waits up to 60 s: a deadline for a hang, not a measure of speed. On the
/// lab's two-CPU VMs, two of these three-node clusters at once took over
/// 20 s to place their first tasks.
async fn eventually<F: AsyncFnMut() -> bool>(what: &str, mut f: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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

/// A node cut off with its agent running (docs/PARTITIONS.md): its task is
/// orphaned and replaced while the old pod runs on, as Legion fails open,
/// and once the node is back its agent stops the pod and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_cut_off_runs_on_until_it_is_back() {
    let c = Cluster::start(3).await;
    c.apply(WEB).await;
    eventually("one task per node", async || {
        c.placed().await == ["n1", "n2", "n3"]
    })
    .await;
    let old = c
        .list(Kind::Task)
        .await
        .into_iter()
        .find(|t| t.spec["node"] == "n2")
        .unwrap()
        .metadata
        .name;

    c.nodes[1].link.cut();
    eventually("n2's task orphaned and replaced", async || {
        let placed = c.placed().await;
        let tasks = c.list(Kind::Task).await;
        placed.len() == 3
            && !placed.contains(&"n2".to_owned())
            && tasks
                .iter()
                .any(|t| t.metadata.name == old && t.status["state"] == "orphaned")
    })
    .await;
    // Running twice: nothing could tell the old pod to stop.
    assert_eq!(c.pods(1).len(), 1);
    tokio::time::sleep(HEARTBEAT * 10).await;
    assert_eq!(c.pods(1).len(), 1);

    c.nodes[1].link.mend();
    c.eventually_node_ready("n2").await;
    eventually("the old pod stopped", async || c.pods(1).is_empty()).await;
    eventually("the old task reported stopped", async || {
        c.list(Kind::Task)
            .await
            .iter()
            .any(|t| t.metadata.name == old && t.status["state"] == "shutdown")
    })
    .await;
    let placed = c.placed().await;
    assert_eq!(placed.len(), 3);
    assert!(!placed.contains(&"n2".to_owned()), "{placed:?}");
    c.stop().await;
}

/// A node cut off with its agent running, whose pod must not run twice
/// (`onPartition: stop`): the agent stops it before the managers place its
/// replacement, and nothing starts it again once the node is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_that_must_not_run_twice_stops_before_it_is_replaced() {
    let c = Cluster::start(3).await;
    c.apply(&WEB.replace(
        "  pod:
",
        "  pod:
    onPartition: stop
    stopGracePeriod: 1s
",
    ))
    .await;
    eventually("one task per node", async || {
        c.placed().await == ["n1", "n2", "n3"]
    })
    .await;

    c.nodes[1].link.cut();
    let start = tokio::time::Instant::now();
    let (mut stopped, mut replaced) = (None, None);
    while replaced.is_none() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "not replaced: {:?}",
            c.list(Kind::Task).await
        );
        if stopped.is_none() && c.pods(1).is_empty() {
            stopped = Some(start.elapsed());
        }
        let elsewhere = c
            .list(Kind::Task)
            .await
            .iter()
            .filter(|t| {
                let node = t.spec["node"].as_str().unwrap_or_default();
                !node.is_empty() && node != "n2" && t.status["state"] != "pending"
            })
            .count();
        if elsewhere == 3 {
            replaced = Some(start.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stopped = stopped.expect("the cut-off pod was never stopped");
    assert!(
        stopped < replaced.unwrap(),
        "stopped {stopped:?}, replaced {replaced:?}"
    );

    c.nodes[1].link.mend();
    c.eventually_node_ready("n2").await;
    eventually("three running, none on n2", async || {
        let placed = c.placed().await;
        placed.len() == 3 && !placed.contains(&"n2".to_owned())
    })
    .await;
    tokio::time::sleep(HEARTBEAT * 10).await;
    assert!(c.pods(1).is_empty());
    c.stop().await;
}

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
                .filter_map(v4)
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
    // And says the orphan stopped, so what waits for it can go on.
    let orphaned = orphan.metadata.name.clone();
    eventually("the orphan reported stopped", async || {
        c.list(Kind::Task).await.iter().any(|t| {
            t.metadata.name == orphaned
                && t.status["state"] == "shutdown"
                && t.status["message"] == "not running on its node once back"
        })
    })
    .await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_node_loses_its_connections_before_its_tasks() {
    // Suspect after three missed heartbeats, down after eight: 3 s and 8 s.
    let mut c = Cluster::start_with(3, Duration::from_secs(1)).await;
    c.apply(WEB).await;
    eventually("one task per node", async || {
        c.placed().await == ["n1", "n2", "n3"]
    })
    .await;
    let uid = c.list(Kind::Service).await[0].metadata.uid.clone();
    let all = c.addresses().await;
    for i in 0..3 {
        eventually("the node balances over all tasks", async || {
            c.table(i, &uid) == all
        })
        .await;
    }
    let task_on = |tasks: &[Object], node: &str| {
        tasks
            .iter()
            .find(|t| t.spec["node"] == node)
            .map(|t| (t.metadata.name.clone(), t.status["state"].clone()))
    };
    let (n2_task, _) = task_on(&c.list(Kind::Task).await, "n2").unwrap();
    let n2_ip: Ipv4Addr = c
        .list(Kind::Task)
        .await
        .iter()
        .find(|t| t.spec["node"] == "n2")
        .map(|t| t.status["ip"].as_str().unwrap().parse().unwrap())
        .unwrap();

    // n2 falls silent: suspect, and the others stop sending to its task,
    // which still runs and is not replaced.
    c.stop_agent(1).await;
    eventually("n2 suspect", async || {
        c.list(Kind::Node)
            .await
            .iter()
            .any(|n| n.metadata.name == "n2" && n.status["state"] == "suspect")
    })
    .await;
    let rest: BTreeSet<Ipv4Addr> = all.iter().copied().filter(|ip| *ip != n2_ip).collect();
    for i in [0, 2] {
        eventually("the node stops sending to n2's task", async || {
            c.table(i, &uid) == rest
        })
        .await;
    }
    let tasks = c.list(Kind::Task).await;
    assert_eq!(
        task_on(&tasks, "n2"),
        Some((n2_task.clone(), "running".into())),
        "not replaced while suspect"
    );
    assert_eq!(tasks.len(), 3, "nothing started elsewhere");

    // Heard from again before it is down: ready, the same task, sent to.
    c.start_agent(1);
    c.eventually_node_ready("n2").await;
    for i in 0..3 {
        eventually("the node sends to n2's task again", async || {
            c.table(i, &uid) == all
        })
        .await;
    }
    let tasks = c.list(Kind::Task).await;
    assert_eq!(tasks.len(), 3);
    assert_eq!(task_on(&tasks, "n2"), Some((n2_task, "running".into())));
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pod_runs_nothing_before_it_is_guarded() {
    let c = Cluster::start(1).await;
    // pf takes no policy: the pod is attached, but nothing in it runs.
    c.nodes[0]
        .sys
        .break_anchor(legion_net::policy::ANCHOR, true);
    c.apply(&WEB.replace("replicas: 3", "replicas: 1")).await;
    let task = || async {
        c.list(Kind::Task)
            .await
            .into_iter()
            .find(|t| t.metadata.namespace == "shop")
    };
    eventually("the task attached", async || {
        task().await.is_some_and(|t| t.status["ip"].is_string())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let t = task().await.unwrap();
    assert_eq!(t.status["state"], "starting", "{:?}", t.status);
    let name = format!("shop_{}_web", t.metadata.name);
    assert_eq!(c.nodes[0].rt.policy_at_start(&name), None);

    // Once pf takes it, the pod starts guarded at its address.
    c.nodes[0]
        .sys
        .break_anchor(legion_net::policy::ANCHOR, false);
    eventually("the task running", async || {
        task().await.is_some_and(|t| t.status["state"] == "running")
    })
    .await;
    let rules = c.nodes[0].rt.policy_at_start(&name).unwrap();
    let lp = legion_net::host_interface(&t.metadata.uid);
    for rule in [
        format!("block drop out quick on {lp} inet all"),
        format!(
            "block drop in quick on {lp} inet from ! {}",
            t.status["ip"].as_str().unwrap()
        ),
    ] {
        assert!(rules.contains(&rule), "{rule} not in:\n{rules}");
    }
    c.stop().await;
}

const DB: &str = r#"
apiVersion: legion/v1
kind: Service
metadata: { name: db, namespace: shop }
spec:
  mode: stateful
  replicas: 1
  maxUnavailable: MAX
  pod:
    containers: [{ name: db, image: "web:1", volumeMounts: [{ name: data, path: /var/db }] }]
    volumes: [{ name: data, claim: { size: 1Gi } }]
"#;

/// A Volume's backup, restored onto another node under another name, is
/// the data as it was; a restore never takes a name in use or a node that
/// has the data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_backup_restores_onto_another_node() {
    use legion_api::volume::{VolumeBackupHeader, VolumeStatus};
    use legion_proto::v1::volume_restore_chunk::Part;
    use legion_proto::v1::volumes_client::VolumesClient;
    use legion_proto::v1::{VolumeBackupRequest, VolumeRestoreChunk, VolumeRestoreStart};

    let c = Cluster::start(3).await;
    c.apply(&DB.replace("MAX", "1")).await;
    let status = async |c: &Cluster, name: &str| {
        c.list(Kind::Volume)
            .await
            .into_iter()
            .find(|v| v.metadata.name == name)
            .map(|v| serde_json::from_value::<VolumeStatus>(v.status).unwrap_or_default())
    };
    eventually("the volume on a node", async || {
        status(&c, "data-db-0")
            .await
            .is_some_and(|s| !s.node.is_empty())
    })
    .await;
    let from = status(&c, "data-db-0").await.unwrap().node;
    let i = c.nodes.iter().position(|n| n.name == from).unwrap();
    let ds = |name: &str| format!("tank/legion/volumes/shop/{name}");
    eventually("db running", async || {
        c.list(Kind::Task)
            .await
            .iter()
            .any(|t| t.status["state"] == "running")
    })
    .await;
    c.nodes[i].sys.write_dataset(&ds("data-db-0"), b"rows 1");

    let channel = Channel::from_shared(format!("http://{}", c.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut volumes = VolumesClient::new(channel);
    let mut chunks = volumes
        .backup(VolumeBackupRequest {
            namespace: "shop".into(),
            name: "data-db-0".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let header = chunks.message().await.unwrap().unwrap().data;
    let parsed: VolumeBackupHeader = serde_json::from_slice(&header).unwrap();
    assert_eq!(parsed.node, from);
    assert_eq!(parsed.consistency, "crash-consistent");
    let mut data = Vec::new();
    while let Some(chunk) = chunks.message().await.unwrap() {
        data.extend_from_slice(&chunk.data);
    }
    // Written after the backup: not in it.
    c.nodes[i].sys.write_dataset(&ds("data-db-0"), b"rows 2");

    let restore = |name: &str, node: &str| {
        let start = VolumeRestoreStart {
            namespace: String::new(),
            name: name.to_owned(),
            node: node.to_owned(),
            header_json: String::from_utf8(header.clone()).unwrap(),
        };
        let parts: Vec<VolumeRestoreChunk> = std::iter::once(Part::Start(start))
            .chain(data.chunks(1000).map(|d| Part::Data(d.to_vec())))
            .map(|p| VolumeRestoreChunk { part: Some(p) })
            .collect();
        let mut volumes = volumes.clone();
        async move { volumes.restore(tokio_stream::iter(parts)).await }
    };
    // Not over the Volume it came from, nor onto a node with that data.
    let e = restore("", &from).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");
    let e = restore("data-copy", &from).await;
    assert!(e.is_ok(), "{e:?}");
    let to = c
        .nodes
        .iter()
        .find(|n| n.name != from)
        .unwrap()
        .name
        .clone();
    let e = restore("data-copy", &to).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");
    // Data of that name on the node, of no Volume: left alone.
    let j = c.nodes.iter().position(|n| n.name == to).unwrap();
    c.nodes[j].sys.zfs_create(&ds("data-stray"), &[]).unwrap();
    c.nodes[j].sys.write_dataset(&ds("data-stray"), b"stray");
    let e = restore("data-stray", &to).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
    assert!(
        e.message().contains("has data for the volume already"),
        "{e:?}"
    );
    assert_eq!(
        c.nodes[j]
            .sys
            .dataset_contents(&ds("data-stray"))
            .as_deref(),
        Some(&b"stray"[..])
    );
    assert!(status(&c, "data-stray").await.is_none());

    restore("data-old", &to).await.unwrap();
    assert_eq!(
        c.nodes[j].sys.dataset_contents(&ds("data-old")).as_deref(),
        Some(&b"rows 1"[..])
    );
    assert_eq!(status(&c, "data-old").await.unwrap().node, to);
    // The Volume it came from is as it was, data and all.
    assert_eq!(
        c.nodes[i].sys.dataset_contents(&ds("data-db-0")).as_deref(),
        Some(&b"rows 2"[..])
    );
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stateful_data_moves_off_a_draining_node() {
    let c = Cluster::start(3).await;
    // No task may stop yet: the test writes between the copy and the stop.
    c.apply(&DB.replace("MAX", "0")).await;
    let task = async |c: &Cluster| {
        c.list(Kind::Task)
            .await
            .into_iter()
            .find(|t| t.status["state"] == "running")
    };
    eventually("db running", async || task(&c).await.is_some()).await;
    let t = task(&c).await.unwrap();
    let from = t.spec["node"].as_str().unwrap().to_owned();
    let index = |name: &str| c.nodes.iter().position(|n| n.name == name).unwrap();
    let ds = "tank/legion/volumes/shop/data-db-0";
    c.nodes[index(&from)].sys.write_dataset(ds, b"rows 1");

    // Drained, the node sends a full copy while the task runs on.
    c.availability(&from, "drain").await;
    let volume = async |c: &Cluster| {
        c.list(Kind::Volume)
            .await
            .into_iter()
            .find(|v| v.metadata.name == "data-db-0")
            .unwrap()
    };
    eventually("copied", async || {
        volume(&c).await.status["migration"]["phase"] == "copied"
    })
    .await;
    let to = volume(&c).await.status["migration"]["to"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(to, from);
    assert_eq!(
        c.nodes[index(&to)].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 1"[..])
    );
    assert_eq!(task(&c).await.unwrap().spec["node"], from.as_str());

    // What the task writes meanwhile follows once it stops.
    c.nodes[index(&from)].sys.write_dataset(ds, b"rows 2");
    c.apply(&DB.replace("MAX", "1")).await;
    eventually("db running elsewhere", async || {
        task(&c)
            .await
            .is_some_and(|t| t.spec["node"] == to.as_str())
    })
    .await;
    let v = volume(&c).await;
    assert_eq!(v.status["node"], to.as_str());
    assert!(v.status.get("migration").is_none(), "{}", v.status);
    let target = &c.nodes[index(&to)].sys;
    assert_eq!(target.dataset_contents(ds).as_deref(), Some(&b"rows 2"[..]));
    // Mounted for the task, without the migration's snapshots.
    assert!(target.zfs_list(ds).unwrap()[0].mountpoint.is_some());
    assert!(target.snapshots().is_empty(), "{:?}", target.snapshots());
    // The old node drops its copy.
    eventually("old copy dropped", async || {
        c.nodes[index(&from)].sys.zfs_list(ds).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

/// A node whose build the manager cannot work with stays out, and its
/// agent keeps asking, for when one of them is upgraded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_of_a_build_the_manager_cannot_work_with_stays_out() {
    use legion_api::protocol::Protocol;
    let mut c = Cluster::start(1).await;
    c.apply(&WEB.replace("replicas: 3", "replicas: 2")).await;
    let ahead = Protocol {
        speaks: legion_api::protocol::SPEAKS + 2,
        oldest: legion_api::protocol::SPEAKS + 1,
    };
    let i = c.add_node(ahead).await;
    c.start_agent(i);
    tokio::time::sleep(HEARTBEAT * 15).await;
    assert!(
        !c.list(Kind::Node)
            .await
            .iter()
            .any(|n| n.metadata.name == "n2"),
        "n2 recorded"
    );
    assert_eq!(c.placed().await, ["n1", "n1"]);
    assert!(c.nodes[i].rt.containers().is_empty());
    // Still asking, not ended.
    assert!(!c.nodes[i].agent.as_ref().unwrap().1.is_finished());

    // One protocol ahead, it joins and takes work.
    c.stop_agent(i).await;
    c.nodes[i].protocol = Protocol {
        speaks: legion_api::protocol::SPEAKS + 1,
        oldest: legion_api::protocol::OLDEST,
    };
    c.start_agent(i);
    c.eventually_node_ready("n2").await;
    c.stop().await;
}

/// A node's drain status, as the leader last wrote it.
async fn drain_of(c: &Cluster, node: &str) -> serde_json::Value {
    c.list(Kind::Node)
        .await
        .into_iter()
        .find(|n| n.metadata.name == node)
        .map(|n| n.status["drain"].clone())
        .unwrap_or_default()
}

/// The drain's item for the Volume, if it is one.
fn volume_item<'a>(drain: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    drain["remaining"]
        .as_array()?
        .iter()
        .find(|i| i["kind"] == "Volume" && i["name"] == name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_transfer_is_tried_again_and_the_drain_waits_for_it() {
    let c = Cluster::start(3).await;
    c.apply(&DB.replace("MAX", "0")).await;
    let task = async |c: &Cluster| {
        c.list(Kind::Task)
            .await
            .into_iter()
            .find(|t| t.status["state"] == "running")
    };
    eventually("db running", async || task(&c).await.is_some()).await;
    let from = task(&c).await.unwrap().spec["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let index = |name: &str| c.nodes.iter().position(|n| n.name == name).unwrap();
    let ds = "tank/legion/volumes/shop/data-db-0";
    c.nodes[index(&from)].sys.write_dataset(ds, b"rows 1");
    let volume = async |c: &Cluster| {
        c.list(Kind::Volume)
            .await
            .into_iter()
            .find(|v| v.metadata.name == "data-db-0")
            .unwrap()
    };

    // The full copy is cut short: the drain says why, and goes on.
    c.nodes[index(&from)].sys.fail_next("zfs send");
    c.availability(&from, "drain").await;
    eventually("the cut copy in the drain status", async || {
        let d = drain_of(&c, &from).await;
        d["phase"] == "draining"
            && volume_item(&d, "data-db-0").is_some_and(|i| {
                i["reason"] == "copying" && i["message"].as_str().is_some_and(|m| !m.is_empty())
            })
    })
    .await;
    eventually("copied on the next attempt", async || {
        volume(&c).await.status["migration"]["phase"] == "copied"
    })
    .await;
    let to = volume(&c).await.status["migration"]["to"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        c.nodes[index(&to)].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 1"[..])
    );
    let d = drain_of(&c, &from).await;
    assert_eq!(d["phase"], "draining", "{d}");
    assert!(volume_item(&d, "data-db-0").is_some(), "{d}");

    // The last part is lost on its way in: tried again too, and the task
    // starts on its new node only with all of its data there.
    c.nodes[index(&from)].sys.write_dataset(ds, b"rows 2");
    c.nodes[index(&to)].sys.fail_next("zfs receive");
    c.apply(&DB.replace("MAX", "1")).await;
    eventually("the lost last part in the drain status", async || {
        volume_item(&drain_of(&c, &from).await, "data-db-0").is_some_and(|i| {
            i["reason"] == "finishing" && i["message"].as_str().is_some_and(|m| !m.is_empty())
        })
    })
    .await;
    eventually("the drain completed", async || {
        drain_of(&c, &from).await["phase"] == "completed"
    })
    .await;
    let d = drain_of(&c, &from).await;
    assert!(d.get("remaining").is_none(), "{d}");
    let v = volume(&c).await;
    assert_eq!(v.status["node"], to.as_str());
    assert!(v.status.get("migration").is_none(), "{}", v.status);
    assert_eq!(
        c.nodes[index(&to)].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 2"[..])
    );
    eventually("db running on its new node", async || {
        task(&c)
            .await
            .is_some_and(|t| t.spec["node"] == to.as_str())
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_whose_source_is_cut_off_mid_copy_waits_for_it() {
    let c = Cluster::start(3).await;
    c.apply(&DB.replace("MAX", "1")).await;
    let running = async |c: &Cluster| {
        c.list(Kind::Task)
            .await
            .into_iter()
            .filter(|t| t.status["state"] == "running")
            .collect::<Vec<_>>()
    };
    eventually("db running", async || !running(&c).await.is_empty()).await;
    let from = running(&c).await[0].spec["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let i = c.nodes.iter().position(|n| n.name == from).unwrap();
    let ds = "tank/legion/volumes/shop/data-db-0";
    c.nodes[i].sys.write_dataset(ds, b"rows 1");

    // The node is cut off as it starts sending.
    let cut = c.nodes[i].link.cut.clone();
    c.nodes[i].sys.before_send(move || {
        cut.send_replace(true);
    });
    c.availability(&from, "drain").await;
    eventually("the drain blocked on the unreachable node", async || {
        let d = drain_of(&c, &from).await;
        d["phase"] == "blocked" && d["reason"] == "unreachable"
    })
    .await;
    // Its data is still there, and nothing of the database runs elsewhere
    // or takes the copy as the Volume.
    tokio::time::sleep(HEARTBEAT * 10).await;
    let v = c
        .list(Kind::Volume)
        .await
        .into_iter()
        .find(|v| v.metadata.name == "data-db-0")
        .unwrap();
    assert_eq!(v.status["node"], from.as_str(), "{}", v.status);
    assert_eq!(v.status["migration"]["phase"], "copying", "{}", v.status);
    let elsewhere: Vec<_> = running(&c)
        .await
        .into_iter()
        .filter(|t| t.spec["node"] != from.as_str())
        .collect();
    assert!(elsewhere.is_empty(), "{elsewhere:?}");
    let dbs = |c: &Cluster| {
        c.nodes[i]
            .rt
            .containers()
            .into_iter()
            .filter(|(n, running, _)| *running && n.ends_with("_db"))
            .count()
    };
    // Running on, as a pod cut off does unless it must not run twice.
    assert_eq!(dbs(&c), 1);

    // Back, it sends again; the drain then ends as one never cut off does.
    c.nodes[i].link.mend();
    c.eventually_node_ready(&from).await;
    eventually("the drain completed", async || {
        drain_of(&c, &from).await["phase"] == "completed"
    })
    .await;
    let v = c
        .list(Kind::Volume)
        .await
        .into_iter()
        .find(|v| v.metadata.name == "data-db-0")
        .unwrap();
    let to = v.status["node"].as_str().unwrap().to_owned();
    assert_ne!(to, from);
    let j = c.nodes.iter().position(|n| n.name == to).unwrap();
    assert_eq!(
        c.nodes[j].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 1"[..])
    );
    assert_eq!(dbs(&c), 0);
    eventually("db running on its new node", async || {
        running(&c)
            .await
            .iter()
            .any(|t| t.spec["node"] == to.as_str())
    })
    .await;
    c.stop().await;
}

/// The Volume of the database the migration tests move.
const DS: &str = "tank/legion/volumes/shop/data-db-0";

/// Starts the database, no task of it allowed to stop yet, and writes
/// `rows 1` to its Volume; returns the index of its node.
async fn db_with_rows(c: &Cluster) -> usize {
    c.apply(&DB.replace("MAX", "0")).await;
    eventually("db running", async || db_task(c).await.is_some()).await;
    let node = db_task(c).await.unwrap().spec["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let i = c.nodes.iter().position(|n| n.name == node).unwrap();
    c.nodes[i].sys.write_dataset(DS, b"rows 1");
    i
}

/// The database's running task.
async fn db_task(c: &Cluster) -> Option<Object> {
    c.list(Kind::Task)
        .await
        .into_iter()
        .find(|t| t.status["state"] == "running")
}

async fn db_volume(c: &Cluster) -> Object {
    c.list(Kind::Volume)
        .await
        .into_iter()
        .find(|v| v.metadata.name == "data-db-0")
        .unwrap()
}

/// Waits for the full copy and returns the index of the node it went to.
async fn copied(c: &Cluster) -> usize {
    eventually("copied", async || {
        db_volume(c).await.status["migration"]["phase"] == "copied"
    })
    .await;
    let v = db_volume(c).await;
    let to = v.status["migration"]["to"].as_str().unwrap();
    c.nodes.iter().position(|n| n.name == to).unwrap()
}

/// Waits for the database to run on node `i` with its Volume, moved
/// there, holding `rows`.
async fn moved_to(c: &Cluster, i: usize, rows: &[u8]) {
    let name = c.nodes[i].name.clone();
    eventually(&format!("db running on {name}"), async || {
        db_task(c)
            .await
            .is_some_and(|t| t.spec["node"] == name.as_str())
    })
    .await;
    let v = db_volume(c).await;
    assert_eq!(v.status["node"], name.as_str(), "{}", v.status);
    assert!(v.status.get("migration").is_none(), "{}", v.status);
    let sys = &c.nodes[i].sys;
    assert_eq!(sys.dataset_contents(DS).as_deref(), Some(rows));
    assert!(sys.snapshots().is_empty(), "{:?}", sys.snapshots());
}

/// A full disk where a Volume goes holds its move, which says so, until
/// there is room. Nothing is lost: a receive out of space leaves no part
/// copy, and the data stays whole where it was, its task running on until
/// the full copy is in. Out of space for the last part, the task has
/// stopped: it waits, down, for room, or for the full node to be drained,
/// which sends the Volume elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_disk_holds_a_move_until_there_is_room() {
    let c = Cluster::start(3).await;
    let from = db_with_rows(&c).await;
    let name = c.nodes[from].name.clone();
    let others: Vec<usize> = (0..3).filter(|&j| j != from).collect();
    for &j in &others {
        c.nodes[j].sys.set_free_space(Some(0));
    }
    let says = async |reason: &str| {
        volume_item(&drain_of(&c, &name).await, "data-db-0").is_some_and(|i| {
            i["reason"] == reason
                && i["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("out of space"))
        })
    };

    c.availability(&name, "drain").await;
    eventually("the full disk in the drain status", async || {
        says("copying").await
    })
    .await;
    assert_eq!(db_volume(&c).await.status["node"], name.as_str());
    assert_eq!(
        db_task(&c).await.unwrap().spec["node"],
        name.as_str(),
        "the task runs on"
    );
    for &j in &others {
        assert!(
            c.nodes[j].sys.zfs_list(DS).unwrap().is_empty(),
            "no part copy"
        );
    }

    for &j in &others {
        c.nodes[j].sys.set_free_space(None);
    }
    let to = copied(&c).await;
    c.nodes[from].sys.write_dataset(DS, b"rows 2");
    c.nodes[to].sys.set_free_space(Some(0));
    c.apply(&DB.replace("MAX", "1")).await;
    eventually(
        "the full disk for the last part in the drain status",
        async || says("finishing").await,
    )
    .await;
    assert!(db_task(&c).await.is_none(), "stopped, and started nowhere");
    assert_eq!(db_volume(&c).await.status["node"], name.as_str());
    assert_eq!(
        c.nodes[from].sys.dataset_contents(DS).as_deref(),
        Some(&b"rows 2"[..])
    );

    // The full node drained too, the Volume goes to the third, all of it
    // sent again from the node it leaves; the full node keeps nothing.
    let third = (0..3).find(|&j| j != from && j != to).unwrap();
    c.availability(&c.nodes[to].name, "drain").await;
    moved_to(&c, third, b"rows 2").await;
    eventually("the full node's copy dropped", async || {
        c.nodes[to].sys.zfs_list(DS).unwrap().is_empty()
    })
    .await;
    eventually("the drain completed", async || {
        drain_of(&c, &name).await["phase"] == "completed"
    })
    .await;
    c.stop().await;
}

/// The node a Volume leaves, cut off once its task has stopped and before
/// it sends the last part, has the only copy of the latest data: the move
/// waits for it, and the task starts nowhere else meanwhile, not even
/// where the full copy is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_whose_old_node_is_cut_off_as_it_finishes_waits_for_it() {
    let c = Cluster::start(3).await;
    let from = db_with_rows(&c).await;
    let name = c.nodes[from].name.clone();
    c.availability(&name, "drain").await;
    let to = copied(&c).await;

    c.nodes[from].sys.write_dataset(DS, b"rows 2");
    let cut = c.nodes[from].link.cut.clone();
    c.nodes[from].sys.before_send(move || {
        cut.send_replace(true);
    });
    c.apply(&DB.replace("MAX", "1")).await;
    eventually("the drain blocked on the unreachable node", async || {
        let d = drain_of(&c, &name).await;
        d["phase"] == "blocked" && d["reason"] == "unreachable"
    })
    .await;
    tokio::time::sleep(HEARTBEAT * 10).await;
    let v = db_volume(&c).await;
    assert_eq!(v.status["node"], name.as_str(), "{}", v.status);
    assert_eq!(v.status["migration"]["phase"], "finishing", "{}", v.status);
    assert!(
        db_task(&c).await.is_none(),
        "started with the full copy only"
    );
    assert_eq!(
        c.nodes[to].sys.dataset_contents(DS).as_deref(),
        Some(&b"rows 1"[..])
    );

    c.nodes[from].link.mend();
    c.eventually_node_ready(&name).await;
    moved_to(&c, to, b"rows 2").await;
    eventually("the old copy dropped", async || {
        c.nodes[from].sys.zfs_list(DS).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

/// The node a Volume goes to, gone silent as the last part is sent, is no
/// place for it any more: the move is called off and another starts, to
/// a node that is there, from the old node, which has all of the data.
/// The silent node's copy, stale, goes once it is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_whose_new_node_goes_silent_as_it_finishes_goes_elsewhere() {
    let c = Cluster::start(3).await;
    let from = db_with_rows(&c).await;
    c.availability(&c.nodes[from].name, "drain").await;
    let to = copied(&c).await;
    let third = (0..3).find(|&j| j != from && j != to).unwrap();

    c.nodes[from].sys.write_dataset(DS, b"rows 2");
    let cut = c.nodes[to].link.cut.clone();
    c.nodes[from].sys.before_send(move || {
        cut.send_replace(true);
    });
    c.apply(&DB.replace("MAX", "1")).await;
    moved_to(&c, third, b"rows 2").await;

    c.nodes[to].link.mend();
    eventually("the stale copy dropped", async || {
        c.nodes[to].sys.zfs_list(DS).unwrap().is_empty()
    })
    .await;
    eventually("the old copy dropped", async || {
        c.nodes[from].sys.zfs_list(DS).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

/// A manager lost once the last part is in, before it gives the Volume to
/// its new node: the next has the part sent again, which the new node
/// takes on the full copy as before, and the move ends as one never cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manager_lost_as_the_last_part_lands_has_it_sent_again() {
    let mut c = Cluster::start(3).await;
    let from = db_with_rows(&c).await;
    c.availability(&c.nodes[from].name, "drain").await;
    let to = copied(&c).await;

    c.nodes[from].sys.write_dataset(DS, b"rows 2");
    let (landed, has_landed) = std::sync::mpsc::channel();
    let (go, may_go) = std::sync::mpsc::channel::<()>();
    c.nodes[to].sys.after_receive(move || {
        let _ = landed.send(());
        let _ = may_go.recv();
    });
    c.apply(&DB.replace("MAX", "1")).await;
    tokio::task::spawn_blocking(move || has_landed.recv_timeout(Duration::from_secs(60)))
        .await
        .unwrap()
        .expect("the last part lands");
    // All of it is there; the manager goes before it hears so.
    assert_eq!(
        c.nodes[to].sys.dataset_contents(DS).as_deref(),
        Some(&b"rows 2"[..])
    );
    c.stop_manager();
    tokio::time::sleep(Duration::from_secs(1)).await;
    go.send(()).unwrap();
    c.restart_manager().await;
    let v = db_volume(&c).await;
    assert_eq!(v.status["migration"]["phase"], "finishing", "{}", v.status);

    moved_to(&c, to, b"rows 2").await;
    eventually("the old copy dropped", async || {
        c.nodes[from].sys.zfs_list(DS).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stateful_data_moves_back_over_a_stale_copy() {
    let c = Cluster::start(2).await;
    c.apply(&DB.replace("MAX", "1")).await;
    let on = async |c: &Cluster, node: &str| {
        c.list(Kind::Task)
            .await
            .iter()
            .any(|t| t.status["state"] == "running" && t.spec["node"] == node)
    };
    eventually("db running", async || {
        on(&c, "n1").await || on(&c, "n2").await
    })
    .await;
    let (a, b) = if on(&c, "n1").await { (0, 1) } else { (1, 0) };
    let (na, nb) = (c.nodes[a].name.clone(), c.nodes[b].name.clone());
    let ds = "tank/legion/volumes/shop/data-db-0";
    c.nodes[a].sys.write_dataset(ds, b"rows 1");

    // Its old copy cannot be dropped, and stays.
    c.nodes[a].sys.fail_next("zfs destroy -r");
    c.availability(&na, "drain").await;
    eventually("moved", async || on(&c, &nb).await).await;
    assert_eq!(
        c.nodes[b].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 1"[..])
    );
    assert!(!c.nodes[a].sys.zfs_list(ds).unwrap().is_empty());

    // Moving back, the full copy takes the stale one's place.
    c.nodes[b].sys.write_dataset(ds, b"rows 2");
    c.availability(&na, "active").await;
    c.availability(&nb, "drain").await;
    eventually("moved back", async || on(&c, &na).await).await;
    assert_eq!(
        c.nodes[a].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 2"[..])
    );
    eventually("copy dropped", async || {
        c.nodes[b].sys.zfs_list(ds).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_returns_stateful_data_to_the_update_snapshot() {
    let c = Cluster::start(1).await;
    let spec = DB.replace("MAX", "1").replace(
        "    volumes:",
        "    restartPolicy: { condition: none }\n    volumes:",
    ) + "  update: { snapshot: true, failureAction: rollback }\n";
    let running = async |c: &Cluster, name: &str| {
        c.list(Kind::Task)
            .await
            .iter()
            .any(|t| t.metadata.name == name && t.status["state"] == "running")
    };
    c.apply(&spec).await;
    eventually("db-0-1 running", async || running(&c, "db-0-1").await).await;
    let sys = &c.nodes[0].sys;
    let ds = "tank/legion/volumes/shop/data-db-0";
    sys.write_dataset(ds, b"rows 1");

    // The update's task snapshots the Volume before it runs.
    c.apply(&spec.replace("web:1", "web:2")).await;
    eventually("db-0-2 running", async || running(&c, "db-0-2").await).await;
    assert_eq!(
        sys.snapshots().into_iter().collect::<Vec<_>>(),
        [format!("{ds}@legion-update-2")]
    );
    // It changes the data, as a migration to a new schema might, and fails.
    sys.write_dataset(ds, b"rows 1, new schema");
    c.nodes[0].rt.exit("shop_db-0-2_db", 1);

    // The rollback's task finds the data as it was before the update.
    eventually("db-0-3 running", async || running(&c, "db-0-3").await).await;
    assert_eq!(sys.dataset_contents(ds).as_deref(), Some(&b"rows 1"[..]));
    assert!(sys.snapshots().is_empty(), "{:?}", sys.snapshots());
    let t = c
        .list(Kind::Task)
        .await
        .into_iter()
        .find(|t| t.metadata.name == "db-0-3")
        .unwrap();
    assert_eq!(t.spec["pod"]["containers"][0]["image"], "web:1");
    c.stop().await;
}

impl Cluster {
    /// Every app container of a pod, on every node, uses `pcpu` percent
    /// of a CPU from now on, as rctl would report.
    async fn load(&self, pcpu: u64) {
        use legion_agent::Runtime;
        use legion_agent::pod::{LABEL_ROLE, LABEL_TASK, child_jail};
        for node in &self.nodes {
            let found = node.rt.list(&[]).await.unwrap();
            for c in &found {
                if c.labels.get(LABEL_ROLE).map(String::as_str) != Some("app") {
                    continue;
                }
                let infra = found.iter().find(|i| {
                    i.labels.get(LABEL_ROLE).map(String::as_str) == Some("infra")
                        && i.labels.get(LABEL_TASK) == c.labels.get(LABEL_TASK)
                });
                if let Some(infra) = infra {
                    node.sys.set_usage(
                        &child_jail(&infra.id, &c.id),
                        &[("pcpu", pcpu), ("memoryuse", 1 << 20)],
                    );
                }
            }
        }
    }

    async fn running(&self) -> usize {
        self.list(Kind::Task)
            .await
            .iter()
            .filter(|t| t.status["state"] == "running" && t.spec["desiredState"] == "running")
            .count()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_service_grows_from_what_its_nodes_report() {
    let c = Cluster::start(2).await;
    c.apply(
        r#"
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  autoscale: { min: 1, max: 3, cpu: 50, downDelay: 1s }
  pod:
    containers:
      - { name: web, image: "web:1", resources: { requests: { cpu: 500m } } }
"#,
    )
    .await;
    eventually("one task running", async || c.running().await == 1).await;

    // A whole CPU against half a CPU requested: four times the target.
    c.load(100).await;
    eventually("three tasks running", async || {
        c.load(100).await;
        c.running().await == 3
    })
    .await;
    let status = &c
        .list(Kind::Service)
        .await
        .into_iter()
        .find(|s| s.metadata.name == "web")
        .unwrap()
        .status;
    assert_eq!(status["autoscale"]["replicas"], 3, "{status}");

    // Idle: back to one once the delay has passed.
    eventually("one task again", async || {
        c.load(0).await;
        c.running().await == 1
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_moves_to_the_node_it_is_asked_to_be_on() {
    let c = Cluster::start(2).await;
    c.apply(&DB.replace("MAX", "1")).await;
    let task = async |c: &Cluster| {
        c.list(Kind::Task)
            .await
            .into_iter()
            .find(|t| t.status["state"] == "running")
    };
    eventually("db running", async || task(&c).await.is_some()).await;
    let from = task(&c).await.unwrap().spec["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let to = if from == "n1" { "n2" } else { "n1" };
    let index = |name: &str| c.nodes.iter().position(|n| n.name == name).unwrap();
    let ds = "tank/legion/volumes/shop/data-db-0";
    c.nodes[index(&from)].sys.write_dataset(ds, b"rows 1");

    // As `legion volume move` does: the Volume's spec names the node.
    let mut v = c
        .list(Kind::Volume)
        .await
        .into_iter()
        .find(|v| v.metadata.name == "data-db-0")
        .unwrap();
    v.spec["node"] = to.into();
    c.api
        .clone()
        .apply(ApplyRequest {
            object: Some((&v).into()),
            ..Default::default()
        })
        .await
        .unwrap();
    eventually("db running on the other node", async || {
        task(&c).await.is_some_and(|t| t.spec["node"] == to)
    })
    .await;
    assert_eq!(
        c.nodes[index(to)].sys.dataset_contents(ds).as_deref(),
        Some(&b"rows 1"[..])
    );
    eventually("old copy dropped", async || {
        c.nodes[index(&from)].sys.zfs_list(ds).unwrap().is_empty()
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_follow_the_routes() {
    use legion_agent::Control;
    let c = Cluster::start(1).await;
    let control = GrpcControl::new("n1", vec![c.addr.clone()], None).unwrap();
    let mut routes = control.routes();
    let route = r#"
apiVersion: legion/v1
kind: Route
metadata: { name: www, namespace: shop }
spec: { host: shop.example.org, path: /cart/, service: web }
"#;
    c.apply(route).await;
    eventually("the route on the node", async || {
        let r = routes.borrow_and_update().clone();
        r.len() == 1 && r[0].namespace == "shop" && r[0].spec.path == "/cart"
    })
    .await;
    c.apply(&route.replace("service: web", "service: api"))
        .await;
    eventually("the change on the node", async || {
        routes
            .borrow()
            .first()
            .is_some_and(|r| r.spec.service == "api")
    })
    .await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_node_reads_the_certificates_of_routes_but_no_other_secret() {
    use legion_agent::Control;
    let c = Cluster::start(1).await;
    let key = rcgen::generate_simple_self_signed(vec!["shop.example.org".to_owned()]).unwrap();
    let pem = format!("{}{}", key.cert.pem(), key.signing_key.serialize_pem());
    let quoted = serde_json::to_string(&pem).unwrap();
    for (name, text) in [("www-cert", quoted.as_str()), ("db-password", "hunter2")] {
        c.apply(&format!(
            "apiVersion: legion/v1\nkind: Secret\nmetadata: {{ name: {name}, namespace: shop }}\nspec: {{ text: {text} }}\n"
        ))
        .await;
    }
    c.apply(
        "apiVersion: legion/v1\nkind: Route\nmetadata: { name: www, namespace: shop }\n\
         spec: { host: shop.example.org, service: web, tls: { secret: www-cert } }\n",
    )
    .await;
    let control = GrpcControl::new("n1", vec![c.addr.clone()], None).unwrap();
    let cert = control
        .data("shop", Kind::Secret, "www-cert")
        .await
        .unwrap();
    assert_eq!(cert.as_deref(), Some(pem.as_bytes()));
    assert!(
        control
            .data("shop", Kind::Secret, "db-password")
            .await
            .is_err(),
        "no task here uses it"
    );
    // A route naming another Secret does not hand it out.
    c.apply(
        "apiVersion: legion/v1\nkind: Route\nmetadata: { name: leak, namespace: shop }\n\
         spec: { host: leak.example.org, service: web, tls: { secret: db-password } }\n",
    )
    .await;
    let err = control
        .data("shop", Kind::Secret, "db-password")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not a PEM certificate and key"),
        "{err}"
    );
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn once_nodes_are_marked_for_ingress_only_they_read_certificates() {
    use legion_agent::Control;
    let c = Cluster::start(2).await;
    let key = rcgen::generate_simple_self_signed(vec!["shop.example.org".to_owned()]).unwrap();
    let pem = format!("{}{}", key.cert.pem(), key.signing_key.serialize_pem());
    c.apply(&format!(
        "apiVersion: legion/v1\nkind: Secret\nmetadata: {{ name: www-cert, namespace: shop }}\nspec: {{ text: {} }}\n",
        serde_json::to_string(&pem).unwrap()
    ))
    .await;
    c.apply(
        "apiVersion: legion/v1\nkind: Route\nmetadata: { name: www, namespace: shop }\n\
         spec: { host: shop.example.org, service: web, tls: { secret: www-cert } }\n",
    )
    .await;
    let read = async |node: &str| {
        GrpcControl::new(node, vec![c.addr.clone()], None)
            .unwrap()
            .data("shop", Kind::Secret, "www-cert")
            .await
    };
    // None marked: any node.
    assert!(read("n1").await.is_ok());
    assert!(read("n2").await.is_ok());
    // n1 marked: only n1.
    c.label_node("n1", legion_api::node::LABEL_INGRESS, "true")
        .await;
    assert_eq!(read("n1").await.unwrap().as_deref(), Some(pem.as_bytes()));
    let err = read("n2").await.unwrap_err();
    assert!(
        err.to_string().contains("not marked to serve HTTPS routes"),
        "{err}"
    );
    c.stop().await;
}
