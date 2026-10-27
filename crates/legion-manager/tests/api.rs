//! Managers over real gRPC on localhost.

use std::time::Duration;

use legion_api::dispatch::{Assignment, Peer, ServiceEndpoints};
use legion_manager::{Manager, ManagerConfig, Tls, client_config, default_raft_config};
use legion_pki::{CERT_LIFETIME, Ca, Identity, Role};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::dispatcher_client::DispatcherClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    ApplyRequest, ApplyResult, AssignmentsRequest, DataRequest, DeleteRequest, EndpointsRequest,
    HeartbeatRequest, InitRequest, JoinTokenRequest, ListRequest, Object, ObjectRef, PeersRequest,
    RegisterRequest, ReportRequest, StatusRequest, WatchRequest, watch_event,
};
use legion_store::Store;
use openraft::BasicNode;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::Code;
use tonic::transport::{Channel, ClientTlsConfig};

/// Short, so node-down detection is quick to test.
const HEARTBEAT: Duration = Duration::from_millis(200);

struct Running {
    addr: String,
    /// Admin credentials, when the manager runs with TLS.
    tls: Option<ClientTlsConfig>,
    store: Store,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    _dir: TempDir,
}

impl Running {
    async fn start() -> Running {
        Running::start_with(None).await
    }

    /// With `ca`, the manager has a certificate from it and requires one.
    async fn start_with(ca: Option<&Ca>) -> Running {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let tls = ca.map(|ca| {
            let node = ca
                .issue(
                    &Identity::new(&format!("m{addr}"), Role::Manager),
                    CERT_LIFETIME,
                )
                .unwrap();
            Tls {
                ca_pem: ca.cert_pem().into(),
                cert_pem: node.cert_pem,
                key_pem: node.key_pem,
                ca_key_pem: Some(ca.key_pem().into()),
            }
        });
        let admin = ca.map(|ca| creds(ca, "admin", Role::Admin));
        Running::serve(dir, listener, tls, admin).await
    }

    /// A fresh manager that joins the cluster at `via` with `token`.
    async fn join(via: &str, token: &str, name: &str, admin: ClientTlsConfig) -> Running {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let id = legion_manager::node_id(dir.path()).unwrap();
        let joined = legion_manager::join(via, token, name, id, &addr)
            .await
            .unwrap();
        assert_eq!(joined.role, Role::Manager);
        Running::serve(dir, listener, Some(joined.tls), Some(admin)).await
    }

    async fn serve(
        dir: TempDir,
        listener: TcpListener,
        tls: Option<Tls>,
        admin: Option<ClientTlsConfig>,
    ) -> Running {
        let addr = listener.local_addr().unwrap().to_string();
        let manager = Manager::open(ManagerConfig {
            raft: openraft::Config {
                heartbeat_interval: 50,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..default_raft_config()
            },
            tls,
            heartbeat: HEARTBEAT,
            ..ManagerConfig::new(dir.path(), &addr)
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
            tls: admin,
            store,
            stop: Some(stop),
            task,
            _dir: dir,
        }
    }

    async fn channel(
        &self,
        tls: Option<&ClientTlsConfig>,
    ) -> Result<Channel, tonic::transport::Error> {
        match tls {
            Some(tls) => {
                Channel::from_shared(format!("https://{}", self.addr))
                    .unwrap()
                    .tls_config(tls.clone())?
                    .connect()
                    .await
            }
            None => {
                Channel::from_shared(format!("http://{}", self.addr))
                    .unwrap()
                    .connect()
                    .await
            }
        }
    }

    async fn objects(&self) -> ObjectsClient<Channel> {
        ObjectsClient::new(self.channel(self.tls.as_ref()).await.unwrap())
    }

    async fn cluster(&self) -> ClusterClient<Channel> {
        ClusterClient::new(self.channel(self.tls.as_ref()).await.unwrap())
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
    // The Service controller may write the status in between: same
    // generation, then the delete.
    loop {
        let ev = events.message().await.unwrap().unwrap();
        if ev.r#type() == watch_event::Type::Delete {
            break;
        }
        assert_eq!(ev.r#type(), watch_event::Type::Put);
        assert_eq!(ev.object.unwrap().generation, 2, "a status write");
    }

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

/// Client credentials from `ca`.
fn creds(ca: &Ca, name: &str, role: Role) -> ClientTlsConfig {
    let issued = ca.issue(&Identity::new(name, role), CERT_LIFETIME).unwrap();
    client_config(ca.cert_pem(), &issued.cert_pem, &issued.key_pem)
}

/// Makes `managers[0]` the first member and the others voters.
async fn form_cluster(managers: &[Running]) {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_managers_forward_and_fail_over() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    form_cluster(&managers).await;
    let leader_raft = managers[0].store.raft.clone();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls() {
    let ca = Ca::generate().unwrap();
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start_with(Some(&ca)).await);
    }
    // Raft between managers and forwarding to the leader run over mTLS.
    form_cluster(&managers).await;
    let follower = managers[2].objects().await;
    eventually(|| {
        let mut follower = follower.clone();
        async move { follower.apply(apply(service("web", 1))).await }
    })
    .await;

    let m = &managers[1];
    let list = || ListRequest {
        kind: "Service".into(),
        ..Default::default()
    };
    // No certificate, a plaintext client, or one from another CA: no
    // connection at all.
    let no_cert = ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca.cert_pem()))
        .domain_name(legion_pki::SERVER_NAME);
    let refused = async |tls: Option<&ClientTlsConfig>| match m.channel(tls).await {
        Err(_) => true,
        Ok(ch) => ObjectsClient::new(ch).list(list()).await.is_err(),
    };
    assert!(refused(Some(&no_cert)).await, "no client certificate");
    assert!(refused(None).await, "plaintext");
    let other = Ca::generate().unwrap();
    assert!(
        refused(Some(&creds(&other, "admin", Role::Admin))).await,
        "another cluster's CA"
    );

    // A worker may not use the API; an admin may not speak Raft.
    let worker = m
        .channel(Some(&creds(&ca, "w1", Role::Worker)))
        .await
        .unwrap();
    let err = ObjectsClient::new(worker).list(list()).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    let admin = m.channel(m.tls.as_ref()).await.unwrap();
    let err = legion_proto::v1::raft_client::RaftClient::new(admin)
        .vote(legion_proto::v1::RaftPayload::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    // The admin lists what was written through the follower.
    let listed = m.objects().await.list(list()).await.unwrap().into_inner();
    assert_eq!(listed.objects.len(), 1);

    for m in managers {
        m.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_join_with_tokens() {
    let ca = Ca::generate().unwrap();
    let first = Running::start_with(Some(&ca)).await;
    first.cluster().await.init(InitRequest {}).await.unwrap();
    let token = |role: &str, rotate: bool| {
        let first = &first;
        let role = role.to_owned();
        async move {
            first
                .cluster()
                .await
                .join_token(JoinTokenRequest { role, rotate })
                .await
                .unwrap()
                .into_inner()
                .token
        }
    };
    let manager_token = token("manager", false).await;
    assert_eq!(token("manager", false).await, manager_token, "stable");
    assert!(manager_token.starts_with(&format!("LGN-1-{}-", ca.digest())));

    // Two managers join and become voters once they have caught up.
    let admin = creds(&ca, "admin", Role::Admin);
    let second = Running::join(&first.addr, &manager_token, "m2", admin.clone()).await;
    let third = Running::join(&first.addr, &manager_token, "m3", admin).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let status = third
            .cluster()
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner();
        if status.members.iter().filter(|m| m.voter).count() == 3 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{status:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Writes through a joined manager reach the leader.
    let follower = third.objects().await;
    eventually(|| {
        let mut follower = follower.clone();
        async move { follower.apply(apply(service("web", 1))).await }
    })
    .await;

    // A worker gets a worker certificate and no CA key.
    let worker_token = token("worker", false).await;
    assert_ne!(worker_token, manager_token);
    let worker = legion_manager::join(&second.addr, &worker_token, "w1", 0, "")
        .await
        .unwrap();
    assert_eq!(worker.role, Role::Worker);
    assert!(worker.tls.ca_key_pem.is_none());
    let bundle = legion_pki::Bundle {
        cert_pem: worker.tls.cert_pem.clone(),
        key_pem: worker.tls.key_pem.clone(),
        ca_pem: worker.tls.ca_pem.clone(),
    };
    assert_eq!(
        bundle.identity().unwrap(),
        Identity::new("w1", Role::Worker)
    );
    // A manager must say where other managers reach it.
    let err = legion_manager::join(&first.addr, &manager_token, "m4", 9, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("advertised address"), "{err}");

    // Bad, rotated and foreign tokens are refused.
    let digest = ca.digest();
    let err = legion_manager::join(&first.addr, &format!("LGN-1-{digest}-nope"), "w2", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid or rotated"), "{err}");
    let rotated = token("worker", true).await;
    assert_ne!(rotated, worker_token);
    let err = legion_manager::join(&first.addr, &worker_token, "w2", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid or rotated"), "{err}");
    legion_manager::join(&first.addr, &rotated, "w2", 0, "")
        .await
        .unwrap();
    let other = Ca::generate().unwrap();
    let foreign = format!("LGN-1-{}-x", other.digest());
    let err = legion_manager::join(&first.addr, &foreign, "w3", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("CA digest mismatch"), "{err}");

    // The tokens themselves stay out of the API.
    let secrets = first
        .objects()
        .await
        .list(ListRequest {
            kind: "Secret".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(secrets.objects.is_empty());
    let err = first
        .objects()
        .await
        .list(ListRequest {
            kind: "Secret".into(),
            namespace: legion_manager::SYSTEM_NAMESPACE.into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    for m in [first, second, third] {
        m.stop().await;
    }
}

/// Applies an object given as JSON, retrying while the cluster settles.
async fn apply_json(m: &Running, kind: &str, name: &str, spec: serde_json::Value) {
    let objects = m.objects().await;
    let object = Object {
        kind: kind.into(),
        name: name.into(),
        spec_json: spec.to_string().into_bytes(),
        ..Default::default()
    };
    eventually(|| {
        let mut objects = objects.clone();
        let object = object.clone();
        async move { objects.apply(apply(object)).await }
    })
    .await;
}

/// The node's state as the leader's store has it.
fn node_state(m: &Running, name: &str) -> Option<String> {
    let key = legion_api::ObjectKey::new(legion_api::Kind::Node, "", name);
    let node = m.store.get(&key).unwrap()?;
    node.status["state"].as_str().map(str::to_owned)
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_registering_at_once_get_their_own_subnets() {
    let ca = Ca::generate().unwrap();
    let first = Running::start_with(Some(&ca)).await;
    first.cluster().await.init(InitRequest {}).await.unwrap();
    async fn register(d: DispatcherClient<Channel>) -> String {
        eventually(|| {
            let mut d = d.clone();
            async move {
                d.register(RegisterRequest {
                    node: String::new(),
                    status_json: br#"{"state":"ready"}"#.to_vec(),
                })
                .await
            }
        })
        .await
        .into_inner()
        .pod_cidr
    }
    let mut clients = Vec::new();
    for n in 0..8 {
        let w = creds(&ca, &format!("w{n}"), Role::Worker);
        clients.push(DispatcherClient::new(
            first.channel(Some(&w)).await.unwrap(),
        ));
    }
    let mut all = tokio::task::JoinSet::new();
    for (n, d) in clients.iter().cloned().enumerate() {
        all.spawn(async move { (n, register(d).await) });
    }
    let mut subnets = std::collections::BTreeMap::new();
    while let Some(r) = all.join_next().await {
        let (n, cidr) = r.unwrap();
        subnets.insert(n, cidr);
    }
    let distinct: std::collections::BTreeSet<&String> = subnets.values().collect();
    assert_eq!(distinct.len(), 8, "{subnets:?}");
    // Registering again keeps the subnet.
    assert_eq!(register(clients[3].clone()).await, subnets[&3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_follow_their_work_through_any_manager() {
    let ca = Ca::generate().unwrap();
    let first = Running::start_with(Some(&ca)).await;
    first.cluster().await.init(InitRequest {}).await.unwrap();
    let token = first
        .cluster()
        .await
        .join_token(JoinTokenRequest {
            role: "manager".into(),
            rotate: false,
        })
        .await
        .unwrap()
        .into_inner()
        .token;
    let admin = creds(&ca, "admin", Role::Admin);
    // The worker calls a follower, which forwards writes to the leader.
    let follower = Running::join(&first.addr, &token, "m2", admin.clone()).await;
    until("the follower knows the leader", || {
        follower
            .store
            .raft
            .metrics()
            .borrow()
            .current_leader
            .is_some()
    })
    .await;
    let w1 = creds(&ca, "w1", Role::Worker);
    let d1 = DispatcherClient::new(follower.channel(Some(&w1)).await.unwrap());
    let ready =
        br#"{"state":"ready","wireguard":{"publicKey":"pub1","endpoint":"192.0.2.1:51820"}}"#
            .to_vec();

    let registered = eventually(|| {
        let mut d = d1.clone();
        let status_json = ready.clone();
        async move {
            d.register(RegisterRequest {
                node: String::new(),
                status_json,
            })
            .await
        }
    })
    .await
    .into_inner();
    assert!(registered.pod_cidr.starts_with("10.200."), "{registered:?}");
    assert_eq!(node_state(&first, "w1").as_deref(), Some("ready"));

    // A worker acts only for itself, and admins do not dispatch.
    let err = d1
        .clone()
        .register(RegisterRequest {
            node: "w2".into(),
            status_json: ready.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    let err = DispatcherClient::new(first.channel(Some(&admin)).await.unwrap())
        .heartbeat(HeartbeatRequest { node: "w1".into() })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    // Heartbeats keep the node up while the test runs.
    let beating = tokio::spawn({
        let d = d1.clone();
        async move {
            loop {
                let _ = d.clone().heartbeat(HeartbeatRequest::default()).await;
                tokio::time::sleep(HEARTBEAT / 2).await;
            }
        }
    });

    // A service using one of two secrets lands on the only node.
    apply_json(
        &first,
        "Secret",
        "token",
        serde_json::json!({ "text": "s3cret" }),
    )
    .await;
    apply_json(
        &first,
        "Secret",
        "other",
        serde_json::json!({ "text": "no" }),
    )
    .await;
    apply_json(
        &first,
        "Service",
        "web",
        serde_json::json!({
            "replicas": 1,
            "pod": { "containers": [{
                "name": "web", "image": "web:1", "secrets": [{ "name": "token" }],
            }] },
        }),
    )
    .await;
    let mut stream = d1
        .clone()
        .assignments(AssignmentsRequest::default())
        .await
        .unwrap()
        .into_inner();
    let task = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let update = stream.message().await.unwrap().expect("stream open");
            let set: Vec<Assignment> = serde_json::from_slice(&update.assignments_json).unwrap();
            if let Some(a) = set.into_iter().next() {
                break a;
            }
        }
    })
    .await
    .expect("the task reaches the worker");
    assert_eq!(task.spec.node, "w1");

    // The worker reads the secrets its tasks use, and no others.
    let data = |name: &str| {
        let mut d = d1.clone();
        let req = DataRequest {
            node: String::new(),
            namespace: "default".into(),
            kind: "Secret".into(),
            name: name.into(),
        };
        async move { d.data(req).await }
    };
    let value = data("token").await.unwrap().into_inner().value;
    assert_eq!(value.as_deref(), Some(&b"s3cret"[..]));
    assert_eq!(
        data("other").await.unwrap_err().code(),
        Code::PermissionDenied
    );

    // It reports on its own task through the follower; another node cannot.
    let report = |status: &str| ReportRequest {
        node: String::new(),
        namespace: task.key.namespace.clone(),
        name: task.key.name.clone(),
        uid: task.uid.clone(),
        status_json: status.as_bytes().to_vec(),
    };
    d1.clone()
        .report(report(r#"{"state":"running","ip":"10.200.0.9"}"#))
        .await
        .unwrap();
    let key = task.key.clone();
    until("the report reaches the leader", || {
        first.store.get(&key).unwrap().unwrap().status["state"] == "running"
    })
    .await;
    let w2 = creds(&ca, "w2", Role::Worker);
    let err = DispatcherClient::new(first.channel(Some(&w2)).await.unwrap())
        .report(report(r#"{"state":"failed"}"#))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    // So do the nodes of the mesh, with what the worker registered.
    let mut peers = d1
        .clone()
        .peers(PeersRequest::default())
        .await
        .unwrap()
        .into_inner();
    let update = tokio::time::timeout(Duration::from_secs(10), peers.message())
        .await
        .expect("the peers arrive")
        .unwrap()
        .expect("stream open");
    let list: Vec<Peer> = serde_json::from_slice(&update.peers_json).unwrap();
    assert_eq!(
        list,
        [Peer {
            node: "w1".into(),
            public_key: "pub1".into(),
            endpoint: Some("192.0.2.1:51820".into()),
            pod_cidr: registered.pod_cidr.clone(),
        }]
    );

    // Endpoints come from the follower's replica.
    let mut endpoints = d1
        .clone()
        .endpoints(EndpointsRequest::default())
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let update = endpoints.message().await.unwrap().expect("stream open");
            let set: Vec<ServiceEndpoints> =
                serde_json::from_slice(&update.endpoints_json).unwrap();
            if set.iter().any(|s| {
                s.name == "web" && s.alive == ["10.200.0.9".parse::<std::net::Ipv4Addr>().unwrap()]
            }) {
                break;
            }
        }
    })
    .await
    .expect("the task's address reaches the endpoints");

    // Silent for three periods, the node is down; heard from, it is up.
    beating.abort();
    until("w1 down", || {
        node_state(&first, "w1").as_deref() == Some("down")
    })
    .await;
    let beat = d1
        .clone()
        .heartbeat(HeartbeatRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(beat.period_ms, HEARTBEAT.as_millis() as u32);
    assert_eq!(beat.managers.len(), 2, "{beat:?}");
    until("w1 up", || {
        node_state(&first, "w1").as_deref() == Some("ready")
    })
    .await;

    drop((stream, endpoints, peers));
    for m in [first, follower] {
        m.stop().await;
    }
}
