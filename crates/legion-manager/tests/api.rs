//! Managers over real gRPC on localhost.

use std::time::Duration;

use legion_manager::{Manager, ManagerConfig, default_raft_config};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    ApplyRequest, ApplyResult, DeleteRequest, InitRequest, ListRequest, Object, ObjectRef,
    StatusRequest, WatchRequest, watch_event,
};
use legion_store::Store;
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::Code;
use tonic::transport::Channel;

struct Running {
    addr: String,
    store: Store,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    _dir: TempDir,
}

impl Running {
    async fn start() -> Running {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let manager = Manager::open(ManagerConfig {
            data_dir: dir.path().to_owned(),
            advertise: addr.clone(),
            raft: openraft::Config {
                heartbeat_interval: 50,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..default_raft_config()
            },
        })
        .await
        .unwrap();
        let store = manager.store().clone();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            manager
                .serve(listener, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Running {
            addr,
            store,
            stop: Some(stop),
            task,
            _dir: dir,
        }
    }

    async fn objects(&self) -> ObjectsClient<Channel> {
        ObjectsClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap()
    }

    async fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap()
    }

    async fn stop(mut self) {
        drop(self.store);
        self.stop.take().unwrap().send(()).unwrap();
        self.task.await.unwrap();
    }
}

fn service(name: &str, replicas: u64) -> Object {
    Object {
        kind: "Service".into(),
        name: name.into(),
        spec_json: format!(
            r#"{{"replicas":{replicas},"pod":{{"containers":[{{"name":"web","image":"web:1"}}]}}}}"#
        )
        .into_bytes(),
        ..Default::default()
    }
}

fn apply(object: Object) -> ApplyRequest {
    ApplyRequest {
        object: Some(object),
        ..Default::default()
    }
}

/// Retries while the cluster settles (leader election, forwarding target).
async fn eventually<T, F, Fut>(mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match f().await {
            Ok(v) => return v,
            Err(s) if s.code() == Code::Unavailable && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(s) => panic!("{s}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_manager_crud_and_watch() {
    let m = Running::start().await;
    let mut api = m.objects().await;

    // Nothing works before init.
    let err = api.apply(apply(service("web", 1))).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    let status = m
        .cluster()
        .await
        .init(InitRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.state, "Leader");
    assert_eq!(status.members.len(), 1);
    let again = m.cluster().await.init(InitRequest {}).await.unwrap_err();
    assert_eq!(again.code(), Code::AlreadyExists);

    let created = api
        .apply(apply(service("web", 1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.result(), ApplyResult::Created);
    let web = created.object.unwrap();
    assert_eq!((web.namespace.as_str(), web.generation), ("default", 1));

    let same = api
        .apply(apply(service("web", 1)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(same.result(), ApplyResult::Unchanged);

    // Invalid specs are rejected with the field at fault.
    let mut bad = service("web", 1);
    bad.spec_json = br#"{"pod":{"containers":[{"name":"web"}]}}"#.to_vec();
    let err = api.apply(apply(bad)).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message()
            .contains("spec.pod.containers[0]: missing field `image`"),
        "{}",
        err.message()
    );

    // A dry run reports the change without making it.
    let dry = api
        .apply(ApplyRequest {
            object: Some(service("web", 2)),
            dry_run: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dry.result(), ApplyResult::Updated);
    let dry_spec: serde_json::Value =
        serde_json::from_slice(&dry.object.unwrap().spec_json).unwrap();
    assert_eq!(dry_spec["replicas"], 2);

    let listing = api
        .list(ListRequest {
            kind: "Service".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listing.objects.len(), 1);
    let mut events = api
        .watch(WatchRequest {
            kind: "Service".into(),
            from_version: listing.version,
        })
        .await
        .unwrap()
        .into_inner();

    let updated = api
        .apply(apply(service("web", 3)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.result(), ApplyResult::Updated);
    assert_eq!(updated.object.as_ref().unwrap().generation, 2);

    // A stale expected version is rejected.
    let stale = api
        .apply(ApplyRequest {
            object: Some(service("web", 4)),
            expected_version: Some(web.version),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted);

    let web_ref = ObjectRef {
        kind: "Service".into(),
        namespace: String::new(),
        name: "web".into(),
    };
    let got = api.get(web_ref.clone()).await.unwrap().into_inner();
    let spec: serde_json::Value = serde_json::from_slice(&got.spec_json).unwrap();
    assert_eq!(spec["replicas"], 3);
    // Stored with defaults filled in.
    assert_eq!(spec["pod"]["restartPolicy"]["condition"], "any");
    api.delete(DeleteRequest {
        r#ref: Some(web_ref.clone()),
        expected_version: None,
    })
    .await
    .unwrap();
    assert_eq!(api.get(web_ref).await.unwrap_err().code(), Code::NotFound);

    let ev = events.message().await.unwrap().unwrap();
    assert_eq!(ev.r#type(), watch_event::Type::Put);
    assert_eq!(ev.object.unwrap().generation, 2);
    let ev = events.message().await.unwrap().unwrap();
    assert_eq!(ev.r#type(), watch_event::Type::Delete);

    // Bad input.
    let task = Object {
        kind: "Task".into(),
        name: "t".into(),
        ..Default::default()
    };
    assert_eq!(
        api.apply(apply(task)).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        api.apply(apply(service("Bad_Name", 1)))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_managers_forward_and_fail_over() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    managers[0]
        .cluster()
        .await
        .init(InitRequest {})
        .await
        .unwrap();
    let leader_raft = managers[0].store.raft.clone();
    for m in &managers[1..] {
        let id = m
            .cluster()
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .node_id;
        leader_raft
            .add_learner(
                id,
                BasicNode {
                    addr: m.addr.clone(),
                },
                true,
            )
            .await
            .unwrap();
    }
    let ids: Vec<u64> = leader_raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .nodes()
        .map(|(id, _)| *id)
        .collect();
    leader_raft.change_membership(ids, false).await.unwrap();

    // Writes through a follower are forwarded to the leader. On a slow host
    // (the emulated CI runner) leadership can still move here, which the API
    // reports as retryable.
    let follower = managers[2].objects().await;
    for n in 0..20 {
        eventually(|| {
            let mut follower = follower.clone();
            async move { follower.apply(apply(service(&format!("s{n}"), 1))).await }
        })
        .await;
    }
    let status = managers[2]
        .cluster()
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.members.iter().filter(|m| m.voter).count(), 3);

    // Stop the leader; the survivors elect a new one and keep serving.
    let old = managers.remove(0);
    drop(leader_raft);
    old.stop().await;
    let api = managers[0].objects().await;
    eventually(|| {
        let mut api = api.clone();
        async move { api.apply(apply(service("after", 1))).await }
    })
    .await;
    let listing = eventually(|| {
        let mut api = api.clone();
        async move {
            api.list(ListRequest {
                kind: "Service".into(),
                ..Default::default()
            })
            .await
        }
    })
    .await
    .into_inner();
    assert_eq!(listing.objects.len(), 21);
    eventually(|| {
        let mut api = api.clone();
        async move {
            api.get(ObjectRef {
                kind: "Service".into(),
                namespace: "default".into(),
                name: "s19".into(),
            })
            .await
        }
    })
    .await;

    for m in managers {
        m.stop().await;
    }
}
