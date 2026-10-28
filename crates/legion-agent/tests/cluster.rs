//! Three nodes in one process: a served manager and three agents, each
//! with its own faked Podman and system, all through the dispatcher. Tasks
//! spread over the nodes, every node balances every service, a node that
//! goes silent loses its tasks to the others, and a stateful task's data
//! moves with it off a draining node.

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
        config.zfs_root = Some("tank/legion".into());
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.usage_period = Duration::from_millis(100);
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
