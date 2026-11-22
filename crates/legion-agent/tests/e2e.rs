//! From `apply` to running pods in one process: a served manager (API and
//! controllers) and an agent with the runtime and the system faked.

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
    channel: Channel,
    rt: Arc<FakeRuntime>,
    sys: Arc<FakeSys>,
    /// What the node resolver and load balancing see.
    endpoints: tokio::sync::watch::Receiver<legion_agent::Endpoints>,
    stop: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

/// A builder that only writes its arguments down, a line a run, in
/// `dir/builder-args`.
fn stand_in_builder(dir: &std::path::Path) -> String {
    let script = dir.join("legion");
    std::fs::write(
        &script,
        format!("#!/bin/sh\necho \"$@\" >> {}/builder-args\n", dir.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    script.display().to_string()
}

/// A pkg whose `audit` says nginx 1.27.0 is vulnerable, as pkg 2.8 says
/// it (`--raw=json-compact`), and exits 1 as it does then.
fn stand_in_pkg(dir: &std::path::Path) -> String {
    let script = dir.join("pkg");
    let json = r#"{"pkg_count":2,"packages":{"nginx":{"version":"1.27.0","issue_count":1,"issues":[{"Affected versions":["< 1.30.1,3"],"description":"www/nginx -- Remote Code Execution/DoS","cve":["CVE-2026-42926"],"url":"https://vuxml.FreeBSD.org/freebsd/3414ac89.html"}]},"pcre2":{"version":"10.45","issue_count":0,"issues":[]}}}"#;
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho \"$@\" >> {}/pkg-args\n# As -F says first, on standard output.\necho 'vulnxml file up-to-date'\necho '{json}'\nexit 1\n",
            dir.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    script.display().to_string()
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
        // What legion doctor requires of a host: else the node takes no
        // new tasks.
        sys.set_main_rules(&format!("{}\n", legion_net::lb::HOOKS.join("\n")));
        sys.zfs_create("tank/legion", &[]).unwrap();
        let rt = Arc::new(FakeRuntime::new(sys.clone()));
        rt.add_image(INFRA);
        let mut config = AgentConfig::new("n1", dir.path().join("tasks"));
        config.drain = Duration::ZERO;
        config.zfs_root = Some("tank/legion".into());
        config.infra_image = INFRA.into();
        config.resync = Duration::from_millis(100);
        config.builder.command = vec![stand_in_builder(dir.path())];
        config.builder.pkg = vec![stand_in_pkg(dir.path())];
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
            api: ObjectsClient::new(channel.clone()),
            channel,
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
                ordinals: Default::default(),
                job: false,
                load_balancer: None,
            }]
    })
    .await;
    // pf balances the VIP and the published port over the ready tasks.
    let table = format!("s{uid}");
    eventually("the service's pf table", async || {
        // With the tasks' IPv6 twins.
        let twins =
            ["fd00:10:200::2", "fd00:10:200::3"].map(|a| a.parse::<std::net::IpAddr>().unwrap());
        let mut want: Vec<std::net::IpAddr> = both.iter().map(|&a| a.into()).collect();
        want.extend(twins);
        c.sys.pf_table_show(legion_net::lb::ANCHOR, &table).unwrap() == want
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

/// `legion logs-search`: a service's tasks' lines holding the text, from
/// every task, in time order, and only those in the range asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_services_logs_are_searched_across_its_tasks() {
    use legion_manager::logsearch::{Search, search};
    use legion_runtime::Output;
    let c = Cluster::start().await;
    c.rt.add_image("web:1");
    c.apply(&service(2, "web:1")).await;
    eventually("two tasks", async || {
        c.tasks().await == ["web-1-1 running", "web-2-1 running"]
    })
    .await;
    let ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let start = ms() - 1000;
    c.rt.log("shop_web-1-1_web", Output::Stdout, "GET /a 200\n");
    c.rt.log("shop_web-1-1_web", Output::Stderr, "Error: disk full\n");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let split = ms();
    tokio::time::sleep(Duration::from_millis(20)).await;
    c.rt.log(
        "shop_web-2-1_web",
        Output::Stderr,
        "error: connection reset\n",
    );
    c.rt.log("shop_web-2-1_web", Output::Stdout, "GET /b 200\n");

    let find = |since_ms, until_ms, text: &str| Search {
        namespace: "shop".into(),
        service: "web".into(),
        since_ms: Some(since_ms),
        until_ms,
        text: text.into(),
        limit: 0,
    };
    let lines = |r: &legion_manager::logsearch::Results| -> Vec<(String, bool, String)> {
        assert!(r.unread.is_empty(), "{:?}", r.unread);
        r.lines
            .iter()
            .map(|l| (l.task.clone(), l.stderr, l.text.clone()))
            .collect()
    };
    let r = search(c.channel.clone(), &find(start, None, "ERROR"))
        .await
        .unwrap();
    assert_eq!(
        lines(&r),
        [
            ("web-1-1".to_owned(), true, "Error: disk full".to_owned()),
            (
                "web-2-1".to_owned(),
                true,
                "error: connection reset".to_owned()
            ),
        ]
    );
    assert!(r.lines[0].at_ms <= r.lines[1].at_ms);
    // Before the split, only the first task had written.
    let r = search(c.channel.clone(), &find(start, Some(split), ""))
        .await
        .unwrap();
    assert_eq!(
        lines(&r),
        [
            ("web-1-1".to_owned(), false, "GET /a 200".to_owned()),
            ("web-1-1".to_owned(), true, "Error: disk full".to_owned()),
        ]
    );
    // The newest when more match than the limit.
    let r = search(
        c.channel.clone(),
        &Search {
            limit: 1,
            ..find(start, None, "")
        },
    )
    .await
    .unwrap();
    assert!(r.truncated);
    assert_eq!(
        lines(&r),
        [("web-2-1".to_owned(), false, "GET /b 200".to_owned())]
    );
    c.stop().await;
}

/// ZFS volume snapshots (`create`, `list`, `rollback`, `delete`) and node
/// image management (`list`, `export`, `load`, `remove`, `prune`) work
/// end-to-end through the manager API and agent sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volume_snapshots_and_node_image_operations_work_end_to_end() {
    use legion_proto::v1::load_image_chunk::Part;
    use legion_proto::v1::volumes_client::VolumesClient;
    use legion_proto::v1::{
        ExportImageRequest, LoadImageChunk, LoadImageStart, NodeImagesRequest,
        VolumeSnapshotRequest,
    };

    let c = Cluster::start().await;
    c.rt.add_image("db:1");
    c.apply(
        r#"
apiVersion: legion/v1
kind: Namespace
metadata: { name: shop }
"#,
    )
    .await;
    c.apply(
        r#"
apiVersion: legion/v1
kind: Volume
metadata: { name: pgdata, namespace: shop }
spec: { driver: zfs }
"#,
    )
    .await;
    c.apply(
        r#"
apiVersion: legion/v1
kind: Service
metadata: { name: db, namespace: shop }
spec:
  replicas: 1
  pod:
    containers:
      - name: db
        image: "db:1"
        volumeMounts: [{ name: data, path: /var/db/postgres }]
    volumes:
      - name: data
        volume: { name: pgdata }
"#,
    )
    .await;
    eventually("db running", async || c.tasks().await == ["db-1-1 running"]).await;

    // 1. ZFS Volume Snapshots: create, list, rollback, delete.
    let mut vols = VolumesClient::new(c.channel.clone());
    let created = vols
        .snapshot(VolumeSnapshotRequest {
            namespace: "shop".into(),
            name: "pgdata".into(),
            op: "create".into(),
            snapshot: "pre-upgrade".into(),
            into: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        created.snapshots.contains(&"pre-upgrade".to_owned()),
        "{created:?}"
    );

    let listed = vols
        .snapshot(VolumeSnapshotRequest {
            namespace: "shop".into(),
            name: "pgdata".into(),
            op: "list".into(),
            snapshot: String::new(),
            into: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.snapshots, ["pre-upgrade"]);

    let rolled = vols
        .snapshot(VolumeSnapshotRequest {
            namespace: "shop".into(),
            name: "pgdata".into(),
            op: "rollback".into(),
            snapshot: "pre-upgrade".into(),
            into: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rolled.snapshots, ["pre-upgrade"]);

    let deleted = vols
        .snapshot(VolumeSnapshotRequest {
            namespace: "shop".into(),
            name: "pgdata".into(),
            op: "delete".into(),
            snapshot: "pre-upgrade".into(),
            into: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.snapshots.is_empty(), "{deleted:?}");

    // 2. Node Images: list, export .tar archive, load under a new tag, remove, prune.
    let mut cluster = ClusterClient::new(c.channel.clone());
    let inv = cluster
        .node_images(NodeImagesRequest {
            op: "list".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let db = inv
        .images
        .iter()
        .find(|i| i.node == "n1" && i.reference == "db:1")
        .unwrap_or_else(|| panic!("{inv:?}"));
    assert_eq!(db.used_by, ["Service shop/db"], "{db:?}");

    // Its details, from the node's copy: how it was built and runs.
    let d = cluster
        .inspect_registry_image(legion_proto::v1::InspectRegistryImageRequest {
            image: "db:1".into(),
            node: "n1".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let steps: Vec<_> = d
        .history
        .iter()
        .map(|s| (s.created_by.as_str(), s.size))
        .collect();
    assert_eq!(
        steps,
        [("FROM freebsd-runtime", 4096), ("CMD [\"serve\"]", 0)],
        "{d:?}"
    );
    assert_eq!(d.run.as_ref().map(|r| r.user.as_str()), Some("www"));
    assert_eq!(
        (d.size, d.used_by.as_slice()),
        (4096, &["Service shop/db".to_owned()][..])
    );
    let missing = cluster
        .inspect_registry_image(legion_proto::v1::InspectRegistryImageRequest {
            image: "nothere:1".into(),
            node: "n1".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound, "{missing:?}");

    let mut stream = cluster
        .export_image(ExportImageRequest {
            node: "n1".into(),
            image: "db:1".into(),
            tag: "exported:1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut archive = Vec::new();
    while let Some(chunk) = stream.message().await.unwrap() {
        archive.extend_from_slice(&chunk.data);
    }
    assert!(!archive.is_empty());

    let loaded = cluster
        .load_image(tokio_stream::iter(vec![
            LoadImageChunk {
                part: Some(Part::Start(LoadImageStart {
                    node: "n1".into(),
                    reference: "reloaded:2".into(),
                })),
            },
            LoadImageChunk {
                part: Some(Part::Data(archive)),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert!(
        loaded.images.iter().any(|i| i.reference == "reloaded:2"),
        "{loaded:?}"
    );

    let inv2 = cluster
        .node_images(NodeImagesRequest {
            node: "n1".into(),
            op: "list".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        inv2.images.iter().any(|i| i.reference == "reloaded:2"),
        "{inv2:?}"
    );

    let removed = cluster
        .node_images(NodeImagesRequest {
            node: "n1".into(),
            op: "remove".into(),
            image: "reloaded:2".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(removed.removed >= 1, "{removed:?}");

    let inv3 = cluster
        .node_images(NodeImagesRequest {
            node: "n1".into(),
            op: "list".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        !inv3.images.iter().any(|i| i.reference == "reloaded:2"),
        "{inv3:?}"
    );

    let _pruned = cluster
        .node_images(NodeImagesRequest {
            node: "n1".into(),
            op: "prune".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    // `legion local`'s build store: on a node named, `legion local prune` there; on every
    // builder, none of which there is.
    cluster
        .node_images(NodeImagesRequest {
            node: "n1".into(),
            op: "prune-builds".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let ran = std::fs::read_to_string(c._dir.path().join("builder-args")).unwrap();
    assert_eq!(ran.trim(), "prune");
    let none = cluster
        .node_images(NodeImagesRequest {
            op: "prune-builds".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(none.code(), tonic::Code::FailedPrecondition, "{none:?}");
    assert!(none.message().contains("legion.builder=true"), "{none:?}");

    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_nodes_are_diagnosed_and_a_support_bundle_keeps_secrets_out() {
    use legion_proto::v1::{DiagnoseRequest, SupportBundleRequest};

    let c = Cluster::start().await;
    c.rt.add_image("web:1");
    c.apply("apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: shop }\n")
        .await;
    c.apply(
        "apiVersion: legion/v1\nkind: Secret\nmetadata: { name: db, namespace: shop }\nspec: { text: s3cr3t-in-a-secret }\n",
    )
    .await;
    c.apply(
        r#"
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  replicas: 1
  pod:
    containers:
      - name: web
        image: "web:1"
        env:
          - { name: API_TOKEN, value: s3cr3t-in-env }
          - { name: DB, secret: db }
"#,
    )
    .await;
    eventually("web running", async || {
        c.tasks().await == ["web-1-1 running"]
    })
    .await;

    let mut cluster = ClusterClient::new(c.channel.clone());
    let nodes = cluster
        .diagnose(DiagnoseRequest::default())
        .await
        .unwrap()
        .into_inner()
        .nodes;
    assert_eq!(nodes.len(), 1, "{nodes:?}");
    let n1 = &nodes[0];
    assert_eq!((n1.node.as_str(), n1.error.as_str()), ("n1", ""), "{n1:?}");
    // What `legion doctor` checks, each with its state and need.
    let pf = n1.findings.iter().find(|f| f.id == "module.pf");
    let pf = pf.unwrap_or_else(|| panic!("{n1:?}"));
    assert_eq!(pf.need, "required");
    assert!(!pf.state.is_empty());
    let missing = cluster
        .diagnose(DiagnoseRequest { node: "n9".into() })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let bundle = cluster
        .support_bundle(SupportBundleRequest {})
        .await
        .unwrap()
        .into_inner()
        .json;
    let text = String::from_utf8(bundle).unwrap();
    assert!(!text.contains("s3cr3t-in-a-secret"), "a Secret's text");
    assert!(!text.contains("s3cr3t-in-env"), "an env value");
    let b: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(b["objects"].get("Secret").is_none(), "Secrets are left out");
    let web = b["objects"]["Service"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["metadata"]["name"] == "web")
        .unwrap();
    // The environment's names stay, to say how it runs.
    assert_eq!(
        web["spec"]["pod"]["containers"][0]["env"][0]["name"],
        "API_TOKEN"
    );
    assert!(
        b["objects"]["Task"]
            .as_array()
            .is_some_and(|t| !t.is_empty())
    );
    assert_eq!(b["diagnosis"][0]["node"], "n1");
    assert!(
        b["events"].as_array().is_some_and(|e| !e.is_empty()),
        "{}",
        b["events"]
    );

    c.stop().await;
}

/// A `files` session's output, or its error, through the manager.
async fn files_session(
    c: &Cluster,
    task: &str,
    op: &str,
    path: &str,
    input: Option<&[u8]>,
) -> Result<Vec<u8>, tonic::Status> {
    use legion_proto::v1::pods_client::PodsClient;
    use legion_proto::v1::session_frame::Frame;
    use legion_proto::v1::session_open::Action;
    use legion_proto::v1::{FilesOptions, InputClosed, SessionFrame, SessionOpen};
    let frame = |f| SessionFrame { frame: Some(f) };
    let mut frames = vec![frame(Frame::Open(SessionOpen {
        namespace: "shop".into(),
        task: task.into(),
        action: Some(Action::Files(FilesOptions {
            op: op.into(),
            path: path.into(),
        })),
        ..Default::default()
    }))];
    if let Some(data) = input {
        frames.push(frame(Frame::Input(data.to_vec())));
        frames.push(frame(Frame::InputClosed(InputClosed {})));
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    for f in frames {
        tx.send(f).await.unwrap();
    }
    let mut out = PodsClient::new(c.channel.clone())
        .session(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    let mut got = Vec::new();
    while let Some(f) = out.message().await? {
        match f.frame {
            Some(Frame::Stdout(d)) => got.extend(d),
            Some(Frame::ExitCode(_)) => break,
            Some(Frame::Error(e)) => {
                return Err(tonic::Status::new(tonic::Code::from_i32(e.code), e.message));
            }
            _ => {}
        }
    }
    drop(tx);
    Ok(got)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tasks_files_are_listed_read_and_written() {
    let c = Cluster::start().await;
    let roots = tempfile::tempdir().unwrap();
    c.rt.roots_in(roots.path());
    c.rt.add_image("web:1");
    c.apply("apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: shop }\n")
        .await;
    c.apply(
        "apiVersion: legion/v1\nkind: Service\nmetadata: { name: web, namespace: shop }\n\
         spec: { replicas: 1, pod: { containers: [{ name: web, image: \"web:1\" }] } }\n",
    )
    .await;
    eventually("web running", async || {
        c.tasks().await == ["web-1-1 running"]
    })
    .await;

    // Written, then listed and read back, through the manager.
    let wrote = files_session(&c, "web-1-1", "write", "/index.html", Some(b"<h1>hi</h1>")).await;
    assert_eq!(wrote.unwrap(), b"");
    let listed = files_session(&c, "web-1-1", "list", "/", None)
        .await
        .unwrap();
    let entries: serde_json::Value = serde_json::from_slice(&listed).unwrap();
    let index = entries
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "index.html")
        .unwrap_or_else(|| panic!("{entries}"));
    assert_eq!(
        (index["kind"].as_str(), index["size"].as_u64()),
        (Some("file"), Some(11))
    );
    let read = files_session(&c, "web-1-1", "read", "/index.html", None).await;
    assert_eq!(read.unwrap(), b"<h1>hi</h1>");

    // What is not there, and a path out of the root, are refused.
    let missing = files_session(&c, "web-1-1", "read", "/nothere", None).await;
    assert_eq!(missing.unwrap_err().code(), tonic::Code::NotFound);
    let out = files_session(&c, "web-1-1", "list", "/../..", None).await;
    assert_eq!(out.unwrap_err().code(), tonic::Code::InvalidArgument);

    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_image_is_scanned_and_what_runs_it_named() {
    use legion_proto::v1::ScanImageRequest;

    let c = Cluster::start().await;
    c.rt.add_image("web:1");
    c.apply("apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: shop }\n")
        .await;
    c.apply(
        "apiVersion: legion/v1\nkind: Service\nmetadata: { name: web, namespace: shop }\n\
         spec: { replicas: 1, pod: { containers: [{ name: web, image: \"web:1\" }] } }\n",
    )
    .await;
    eventually("web running", async || {
        c.tasks().await == ["web-1-1 running"]
    })
    .await;

    let mut cluster = ClusterClient::new(c.channel.clone());
    let r = cluster
        .scan_image(ScanImageRequest {
            node: "n1".into(),
            image: "web:1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let packages: Vec<_> = r
        .packages
        .iter()
        .map(|p| format!("{}-{}", p.name, p.version))
        .collect();
    assert_eq!(packages, ["nginx-1.27.0", "pcre2-10.45"]);
    assert_eq!(r.vulnerable.len(), 1, "{r:?}");
    assert_eq!(r.vulnerable[0].name, "nginx");
    assert_eq!(r.vulnerable[0].issues[0].cves, ["CVE-2026-42926"]);
    assert_eq!(r.used_by, ["Service shop/web"]);
    // pkg was asked of the image's packages, by name and version.
    let asked = std::fs::read_to_string(c._dir.path().join("pkg-args")).unwrap();
    assert!(asked.contains("audit"), "{asked}");
    assert!(
        asked.contains("--raw=json-compact nginx-1.27.0 pcre2-10.45"),
        "{asked}"
    );

    let missing = cluster
        .scan_image(ScanImageRequest {
            node: "n1".into(),
            image: "nothere:1".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    c.stop().await;
}
