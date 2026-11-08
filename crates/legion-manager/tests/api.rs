//! Managers over real gRPC on localhost.

use std::time::Duration;

use legion_api::dispatch::{Assignment, Peer, ServiceEndpoints};
use legion_manager::{
    Credentials, Manager, ManagerConfig, Tls, client_config, default_raft_config,
};
use legion_pki::{CERT_LIFETIME, Ca, Identity, Role};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::dispatcher_client::DispatcherClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::pods_client::PodsClient;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{
    ApplyRequest, ApplyResult, AssignmentsRequest, ClaimVolumeRequest, DataRequest, DeleteRequest,
    EndpointsRequest, HeartbeatRequest, InitRequest, JoinTokenRequest, ListRequest, Object,
    ObjectRef, PeersRequest, RegisterRequest, RenewRequest, ReportRequest, StatusRequest,
    WatchRequest, watch_event,
};
use legion_proto::v1::{ExecOptions, LogsOptions, SessionFrame, SessionOpen};
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
const STEADY: Duration = Duration::from_secs(600);

struct Running {
    addr: String,
    /// Admin credentials, when the manager runs with TLS.
    tls: Option<ClientTlsConfig>,
    /// The manager's own, which it serves with.
    credentials: Option<std::sync::Arc<Credentials>>,
    store: Store,
    metrics: legion_manager::metrics::ManagerMetrics,
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

    /// A fresh manager that joins at `via` as this build, then serves as
    /// one speaking `protocol`: one upgraded since it joined.
    async fn join_then_serve_as(
        via: &str,
        token: &str,
        name: &str,
        admin: ClientTlsConfig,
        protocol: legion_api::protocol::Protocol,
    ) -> Running {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let id = legion_manager::node_id(dir.path()).unwrap();
        let joined = legion_manager::join(via, token, name, id, &addr)
            .await
            .unwrap();
        let config = ManagerConfig {
            raft: Running::quick_raft(),
            tls: Some(joined.tls),
            heartbeat: HEARTBEAT,
            registry_loopback: true,
            protocol,
            ..ManagerConfig::new(dir.path(), &addr)
        };
        Running::serve_config(dir, listener, config, Some(admin)).await
    }

    /// With nodes that stay up however long the host stalls, for tests that
    /// are not about nodes going down: on [`HEARTBEAT`], one busy moment
    /// orphans their tasks.
    async fn start_steady() -> Running {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft = Running::quick_raft();
        Running::serve_timed(dir, listener, None, None, raft, STEADY).await
    }

    /// With the Raft timing of a real cluster rather than the tests' quick
    /// one, for calls that take longer than its heartbeat.
    async fn start_real_timing() -> Running {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Running::serve_with(dir, listener, None, None, default_raft_config()).await
    }

    async fn serve(
        dir: TempDir,
        listener: TcpListener,
        tls: Option<Tls>,
        admin: Option<ClientTlsConfig>,
    ) -> Running {
        Running::serve_with(dir, listener, tls, admin, Running::quick_raft()).await
    }

    fn quick_raft() -> openraft::Config {
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..default_raft_config()
        }
    }

    async fn serve_with(
        dir: TempDir,
        listener: TcpListener,
        tls: Option<Tls>,
        admin: Option<ClientTlsConfig>,
        raft: openraft::Config,
    ) -> Running {
        Running::serve_timed(dir, listener, tls, admin, raft, HEARTBEAT).await
    }

    async fn serve_timed(
        dir: TempDir,
        listener: TcpListener,
        tls: Option<Tls>,
        admin: Option<ClientTlsConfig>,
        raft: openraft::Config,
        heartbeat: Duration,
    ) -> Running {
        let addr = listener.local_addr().unwrap().to_string();
        let config = ManagerConfig {
            raft,
            tls,
            heartbeat,
            registry_loopback: true,
            ..ManagerConfig::new(dir.path(), &addr)
        };
        Running::serve_config(dir, listener, config, admin).await
    }

    /// Serves `config` (its address is the listener's).
    async fn serve_config(
        dir: TempDir,
        listener: TcpListener,
        config: ManagerConfig,
        admin: Option<ClientTlsConfig>,
    ) -> Running {
        let addr = listener.local_addr().unwrap().to_string();
        let manager = Manager::open(config).await.unwrap();
        let store = manager.store().clone();
        let metrics = manager.metrics();
        let credentials = manager.credentials();
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
            credentials,
            store,
            metrics,
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

    async fn stop(self) {
        self.stop_keeping().await;
    }

    /// Stops the manager and hands back its data directory.
    async fn stop_keeping(mut self) -> TempDir {
        drop(self.store);
        self.stop.take().unwrap().send(()).unwrap();
        self.task.await.unwrap();
        self._dir
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

    // Status is the cluster's: what a caller sends with an object is dropped.
    let mut forged = service("forged", 1);
    forged.status_json = br#"{"update":{"state":"completed"}}"#.to_vec();
    api.apply(apply(forged)).await.unwrap();
    let key = legion_api::ObjectKey::new(legion_api::Kind::Service, "default", "forged");
    // The Service controller may write a status of its own meanwhile; the
    // forged update is never part of it.
    let status = m.store.get(&key).unwrap().unwrap().status;
    assert!(status.get("update").is_none(), "{status}");
    api.delete(DeleteRequest {
        r#ref: Some(ObjectRef {
            kind: "Service".into(),
            namespace: "default".into(),
            name: "forged".into(),
        }),
        expected_version: None,
    })
    .await
    .unwrap();

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

/// Makes `managers[0]` the first member and the others voters. Leadership
/// can move on a busy host as the cluster forms, so each change goes to
/// whichever manager takes it.
async fn form_cluster(managers: &[Running]) {
    managers[0]
        .cluster()
        .await
        .init(InitRequest {})
        .await
        .unwrap();
    let mut ids = Vec::new();
    for m in managers {
        let id = m
            .cluster()
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .node_id;
        ids.push(id);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for (m, id) in managers.iter().zip(&ids).skip(1) {
        let node = BasicNode {
            addr: m.addr.clone(),
        };
        'added: loop {
            for leader in managers {
                match leader.store.raft.add_learner(*id, node.clone(), true).await {
                    Ok(_) => break 'added,
                    Err(e) => assert!(tokio::time::Instant::now() < deadline, "{e}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    'changed: loop {
        for leader in managers {
            match leader
                .store
                .raft
                .change_membership(ids.clone(), false)
                .await
            {
                Ok(_) => break 'changed,
                Err(e) => assert!(tokio::time::Instant::now() < deadline, "{e}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Every manager knows the cluster before the test talks to it: one
    // that has not yet heard the membership says it is in none.
    for m in managers {
        m.store
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |x| {
                    x.current_leader.is_some()
                        && x.membership_config.membership().voter_ids().count() == ids.len()
                },
                "know the leader and every voter",
            )
            .await
            .unwrap();
    }
}

/// A log of objects near the size limit, more of them than one message
/// holds: a manager that joins later still catches up, in smaller batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_manager_catches_up_on_a_log_of_large_objects() {
    let first = Running::start_real_timing().await;
    form_cluster(std::slice::from_ref(&first)).await;
    let api = first.objects().await;
    let text = "x".repeat(500_000);
    let mut writes = tokio::task::JoinSet::new();
    // A few at a time: it's the log that is under test, and 70 MB at once on
    // a busy host outlasts the commit timeout.
    let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
    for n in 0..140 {
        let config = Object {
            kind: "Config".into(),
            name: format!("big{n}"),
            spec_json: serde_json::to_vec(&serde_json::json!({ "text": text })).unwrap(),
            ..Default::default()
        };
        let api = api.clone();
        let limit = limit.clone();
        writes.spawn(async move {
            let _turn = limit.acquire_owned().await.unwrap();
            eventually(|| {
                let mut api = api.clone();
                let config = config.clone();
                async move { api.apply(apply(config)).await }
            })
            .await
        });
    }
    while let Some(written) = writes.join_next().await {
        written.unwrap();
    }
    let second = Running::start_real_timing().await;
    let id = second
        .cluster()
        .await
        .status(StatusRequest {})
        .await
        .unwrap()
        .into_inner()
        .node_id;
    let leader = first.store.raft.clone();
    tokio::time::timeout(
        Duration::from_secs(60),
        leader.add_learner(
            id,
            BasicNode {
                addr: second.addr.clone(),
            },
            true,
        ),
    )
    .await
    .expect("the learner catches up")
    .unwrap();
    let applied = leader.metrics().borrow().last_applied;
    let mut caught_up = second.store.raft.metrics();
    tokio::time::timeout(
        // Some 70 MB to apply: generous for a small, slow test host.
        Duration::from_secs(180),
        caught_up.wait_for(|m| m.last_applied >= applied),
    )
    .await
    .expect("the learner applies the log")
    .unwrap();
    let key = legion_api::ObjectKey::new(legion_api::Kind::Config, "default", "big139");
    assert!(second.store.get(&key).unwrap().is_some());
    drop(leader);
    drop(caught_up);
    second.stop().await;
    first.stop().await;
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

/// Whom a certificate names.
fn named(cert_pem: &str) -> Identity {
    legion_pki::Bundle {
        cert_pem: cert_pem.to_owned(),
        key_pem: String::new(),
        ca_pem: String::new(),
    }
    .identity()
    .unwrap()
}

/// The certificate the server at `addr` presents to a new connection.
async fn served(addr: &str, ca: &Ca) -> Vec<u8> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ca.cert_pem().as_bytes()).unwrap())
        .unwrap();
    let config = tokio_rustls::rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect(ServerName::try_from(legion_pki::SERVER_NAME).unwrap(), tcp)
        .await
        .unwrap();
    tls.get_ref().1.peer_certificates().unwrap()[0].to_vec()
}

fn der(cert_pem: &str) -> Vec<u8> {
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    CertificateDer::from_pem_slice(cert_pem.as_bytes())
        .unwrap()
        .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn certificates_renew_without_restart() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let endpoint =
        || tonic::transport::Endpoint::from_shared(format!("http://{}", m.addr)).unwrap();

    // A worker, calling as nodes do, gets a certificate for a new key
    // that names it as before, and calls with it from then on.
    let issued = ca
        .issue(&Identity::new("w1", Role::Worker), CERT_LIFETIME)
        .unwrap();
    let worker = Credentials::new(ca.cert_pem(), &issued.cert_pem, &issued.key_pem).unwrap();
    let renew = |channel: Channel| async move {
        let (key_pem, csr_pem) = legion_pki::request().unwrap();
        DispatcherClient::new(channel)
            .renew(RenewRequest { csr_pem })
            .await
            .map(|r| (r.into_inner().cert_pem, key_pem))
    };
    let (cert_pem, key_pem) = renew(worker.channel(endpoint())).await.unwrap();
    assert_ne!(cert_pem, issued.cert_pem);
    assert_eq!(named(&cert_pem), Identity::new("w1", Role::Worker));
    worker.renew(&cert_pem, &key_pem).unwrap();
    renew(worker.channel(endpoint())).await.unwrap();

    // Others are refused: managers issue their own, and admins and users
    // are not nodes.
    for (name, role) in [("m9", Role::Manager), ("admin", Role::Admin)] {
        let other = m.channel(Some(&creds(&ca, name, role))).await.unwrap();
        let err = renew(other).await.unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied, "{role:?}: {err}");
    }
    let err = DispatcherClient::new(worker.channel(endpoint()))
        .renew(RenewRequest {
            csr_pem: "not a request".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // The manager's own certificate, renewed while it serves: the next
    // connection is shown the new one, and the API goes on.
    let credentials = m.credentials.clone().unwrap();
    let (current, _) = credentials.cert_and_key();
    assert_eq!(served(&m.addr, &ca).await, der(&current));
    let next = ca.issue(&named(&current), CERT_LIFETIME).unwrap();
    credentials.renew(&next.cert_pem, &next.key_pem).unwrap();
    assert_eq!(served(&m.addr, &ca).await, der(&next.cert_pem));
    let listed = m
        .objects()
        .await
        .list(ListRequest {
            kind: "Service".into(),
            ..Default::default()
        })
        .await;
    assert!(listed.is_ok(), "{listed:?}");

    m.stop().await;
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
    // A token certifies no node already in the cluster, under its name.
    let w5 = DispatcherClient::new(
        first
            .channel(Some(&creds(&ca, "w5", Role::Worker)))
            .await
            .unwrap(),
    );
    w5.clone()
        .register(RegisterRequest {
            node: String::new(),
            status_json: br#"{"state":"ready"}"#.to_vec(),
            role: String::new(),
        })
        .await
        .unwrap();
    let err = legion_manager::join(&first.addr, &rotated, "w5", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("w5 is in the cluster"), "{err}");
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

async fn until(what: &str, f: impl FnMut() -> bool) {
    until_within(what, Duration::from_secs(10), f).await
}

/// [`until`] with a longer wait: a joined manager's promotion to voter
/// takes longer while the other tests load the host.
async fn until_within(what: &str, wait: Duration, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + wait;
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
                    ..Default::default()
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
    // So does a node deleted and joining again, with a new certificate:
    // the network is not full.
    first
        .objects()
        .await
        .delete(DeleteRequest {
            r#ref: Some(get("Node", "", "w3")),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let w3 = creds(&ca, "w3", Role::Worker);
    let rejoined = DispatcherClient::new(first.channel(Some(&w3)).await.unwrap());
    assert_eq!(register(rejoined).await, subnets[&3]);
    assert_eq!(node_state(&first, "w3").as_deref(), Some("ready"));
}

/// The node's role as the leader's store has it.
fn node_role(m: &Running, name: &str) -> Option<String> {
    let key = legion_api::ObjectKey::new(legion_api::Kind::Node, "", name);
    let node = m.store.get(&key).unwrap()?;
    node.spec["role"].as_str().map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_register_with_their_certificates_role() {
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
    let follower = Running::join(&first.addr, &token, "m2", admin).await;
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
    async fn register(d: DispatcherClient<Channel>, role: &str) {
        eventually(|| {
            let mut d = d.clone();
            let role = role.to_owned();
            async move {
                d.register(RegisterRequest {
                    node: String::new(),
                    status_json: br#"{"state":"ready"}"#.to_vec(),
                    role,
                })
                .await
            }
        })
        .await;
    }
    let through = |m: &'static str, name: &str, role| {
        let creds = creds(&ca, name, role);
        let m = if m == "leader" { &first } else { &follower };
        async move { DispatcherClient::new(m.channel(Some(&creds)).await.unwrap()) }
    };

    // A manager, to the leader and through a follower, which forwards.
    register(through("leader", "m1", Role::Manager).await, "").await;
    assert_eq!(node_role(&first, "m1").as_deref(), Some("manager"));
    let m2 = through("follower", "m2", Role::Manager).await;
    register(m2.clone(), "").await;
    assert_eq!(node_role(&first, "m2").as_deref(), Some("manager"));
    // A worker, whatever it claims.
    register(through("follower", "w1", Role::Worker).await, "manager").await;
    assert_eq!(node_role(&first, "w1").as_deref(), Some("worker"));

    // A role edited away comes back when the node registers again.
    apply_json(
        &first,
        "Node",
        "m2",
        serde_json::json!({ "role": "worker" }),
    )
    .await;
    assert_eq!(node_role(&first, "m2").as_deref(), Some("worker"));
    register(m2, "").await;
    assert_eq!(node_role(&first, "m2").as_deref(), Some("manager"));
}

/// A name joined twice before its node registers is certified once: the
/// earlier certificate is refused from the second join on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_name_joined_twice_keeps_only_the_newest_certificate() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let token = m
        .cluster()
        .await
        .join_token(JoinTokenRequest {
            role: "worker".into(),
            rotate: false,
        })
        .await
        .unwrap()
        .into_inner()
        .token;
    let first = legion_manager::join(&m.addr, &token, "w7", 0, "")
        .await
        .unwrap();
    let second = legion_manager::join(&m.addr, &token, "w7", 0, "")
        .await
        .unwrap();
    let dispatcher = |tls: &Tls| {
        let creds = client_config(&tls.ca_pem, &tls.cert_pem, &tls.key_pem);
        let m = &m;
        async move { DispatcherClient::new(m.channel(Some(&creds)).await.unwrap()) }
    };
    let register = |mut d: DispatcherClient<Channel>| async move {
        d.register(RegisterRequest {
            node: String::new(),
            status_json: br#"{"state":"ready"}"#.to_vec(),
            role: String::new(),
        })
        .await
    };
    let err = register(dispatcher(&first.tls).await).await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("joined again"), "{err}");
    register(dispatcher(&second.tls).await).await.unwrap();
    assert_eq!(node_role(&m, "w7").as_deref(), Some("worker"));
    // Registered, the name takes no more joins, and its node stays in.
    let err = legion_manager::join(&m.addr, &token, "w7", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("w7 is in the cluster"), "{err}");
    register(dispatcher(&second.tls).await).await.unwrap();
    m.stop().await;
}

/// A join of a name that fails certifies nothing, and refuses nothing:
/// the certificate an earlier join gave the name stays good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_join_leaves_the_names_certificate_good() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let token = |role: &str| {
        let m = &m;
        let role = role.to_owned();
        async move {
            m.cluster()
                .await
                .join_token(JoinTokenRequest {
                    role,
                    rotate: false,
                })
                .await
                .unwrap()
                .into_inner()
                .token
        }
    };
    let joined = legion_manager::join(&m.addr, &token("worker").await, "w8", 0, "")
        .await
        .unwrap();
    // A manager joins with its Raft id and address: without, it is
    // refused, and the name's record is left as it was.
    let err = legion_manager::join(&m.addr, &token("manager").await, "w8", 0, "")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("node id"), "{err}");
    let creds = client_config(
        &joined.tls.ca_pem,
        &joined.tls.cert_pem,
        &joined.tls.key_pem,
    );
    DispatcherClient::new(m.channel(Some(&creds)).await.unwrap())
        .register(RegisterRequest {
            node: String::new(),
            status_json: br#"{"state":"ready"}"#.to_vec(),
            role: String::new(),
        })
        .await
        .unwrap();
    m.stop().await;
}

/// A record of the node whose write went through but whose reply was lost
/// counts as written: a join goes on and its certificate is good, and a
/// removal removes the node, rather than leave it refused while its Node
/// stays (PLAN.md, phase 4: a supersede whose write outcome is unknown).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_whose_reply_was_lost_counts_as_written() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let token = m
        .cluster()
        .await
        .join_token(JoinTokenRequest {
            role: "worker".into(),
            rotate: false,
        })
        .await
        .unwrap()
        .into_inner()
        .token;
    legion_manager::hooks::lose_reply("removed-write", "w10");
    let joined = legion_manager::join(&m.addr, &token, "w10", 0, "")
        .await
        .unwrap();
    let creds = client_config(
        &joined.tls.ca_pem,
        &joined.tls.cert_pem,
        &joined.tls.key_pem,
    );
    let register = || {
        let creds = creds.clone();
        let m = &m;
        async move {
            DispatcherClient::new(m.channel(Some(&creds)).await.unwrap())
                .register(RegisterRequest {
                    node: String::new(),
                    status_json: br#"{"state":"ready"}"#.to_vec(),
                    role: String::new(),
                })
                .await
        }
    };
    register().await.unwrap();
    assert_eq!(node_role(&m, "w10").as_deref(), Some("worker"));

    legion_manager::hooks::lose_reply("removed-write", "w10");
    m.objects().await.delete(delete_node("w10")).await.unwrap();
    assert_eq!(node_role(&m, "w10"), None);
    let err = register().await.unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    m.stop().await;
}

/// The Delete request for Node `name`.
fn delete_node(name: &str) -> DeleteRequest {
    DeleteRequest {
        r#ref: Some(ObjectRef {
            kind: "Node".into(),
            namespace: String::new(),
            name: name.into(),
        }),
        expected_version: None,
    }
}

/// A second registration of a worker, checked before the worker is removed
/// and making its Node after, does not bring it back: refused, and the Node
/// it made goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registration_checked_before_a_removal_does_not_bring_the_node_back() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let w = DispatcherClient::new(
        m.channel(Some(&creds(&ca, "w9", Role::Worker)))
            .await
            .unwrap(),
    );
    let register = |mut d: DispatcherClient<Channel>| async move {
        d.register(RegisterRequest {
            node: String::new(),
            status_json: br#"{"state":"ready"}"#.to_vec(),
            role: String::new(),
        })
        .await
    };
    let mut paused = legion_manager::hooks::arm("register-create", "w9");
    let late = tokio::spawn(register(w.clone()));
    paused.reached().await;
    // The other registration goes through, and then the node is removed.
    register(w.clone()).await.unwrap();
    assert_eq!(node_state(&m, "w9").as_deref(), Some("ready"));
    m.objects().await.delete(delete_node("w9")).await.unwrap();
    paused.release();
    let err = late.await.unwrap().unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    let key = legion_api::ObjectKey::new(legion_api::Kind::Node, "", "w9");
    assert!(m.store.get(&key).unwrap().is_none(), "the node came back");
    m.stop().await;
}

/// A renewal checked before its node is removed, and signed after, gives
/// no certificate: one signed the second after the removal would pass it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renewal_signed_after_its_node_is_removed_is_not_handed_out() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let mut w = DispatcherClient::new(
        m.channel(Some(&creds(&ca, "w10", Role::Worker)))
            .await
            .unwrap(),
    );
    w.register(RegisterRequest {
        node: String::new(),
        status_json: br#"{"state":"ready"}"#.to_vec(),
        role: String::new(),
    })
    .await
    .unwrap();
    let mut paused = legion_manager::hooks::arm("renew-checked", "w10");
    let (_, csr_pem) = legion_pki::request().unwrap();
    let renewing = tokio::spawn({
        let mut w = w.clone();
        async move { w.renew(RenewRequest { csr_pem }).await }
    });
    paused.reached().await;
    m.objects().await.delete(delete_node("w10")).await.unwrap();
    // Signed in a later second than the removal.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    paused.release();
    let err = renewing.await.unwrap().unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert!(err.message().contains("while renewing"), "{err}");
    m.stop().await;
}

/// A session with a task stopped before it was placed, asked for by the
/// task's own name, is refused at once: its node never had it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_with_a_task_never_placed_is_refused() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    apply_json(
        &m,
        "Service",
        "web",
        serde_json::json!({
            "replicas": 1,
            "pod": { "containers": [{ "name": "app", "image": "app:1" }] },
        }),
    )
    .await;
    let key = legion_api::ObjectKey::new(legion_api::Kind::Service, "default", "web");
    let web = m.store.get(&key).unwrap().unwrap();
    let mut task = legion_api::Object::new(
        legion_api::Kind::Task,
        "default",
        "web-1-9",
        serde_json::json!({
            "service": "web",
            "serviceUid": web.metadata.uid,
            "serviceGeneration": web.metadata.generation,
            "slot": 1,
            "node": "n1",
            "desiredState": "shutdown",
            "pod": { "containers": [{ "name": "app", "image": "app:1" }] },
        }),
    );
    task.status = serde_json::json!({
        "state": "shutdown",
        "message": legion_manager::controller::plan::NEVER_PLACED,
    });
    m.store
        .raft
        .client_write(legion_store::Command::Put {
            object: task,
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    let open = SessionFrame {
        frame: Some(Frame::Open(SessionOpen {
            namespace: "default".into(),
            task: "web-1-9".into(),
            container: String::new(),
            action: Some(Action::Logs(LogsOptions::default())),
        })),
    };
    let mut pods = PodsClient::new(m.channel(None).await.unwrap());
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        pods.session(tokio_stream::iter([open])),
    )
    .await
    .expect("refused at once, not offered to the node")
    .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains("never ran"), "{refused}");
    m.stop().await;
}

/// Deleting a worker's Node takes the worker out: its certificate is
/// refused, renewals included, until it joins again for a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_worker_stays_out_until_it_joins_again() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let worker = |creds: ClientTlsConfig| {
        let m = &m;
        async move { DispatcherClient::new(m.channel(Some(&creds)).await.unwrap()) }
    };
    let heartbeat = |mut d: DispatcherClient<Channel>| async move {
        d.heartbeat(HeartbeatRequest {
            node: String::new(),
            usage: Vec::new(),
        })
        .await
    };
    let old_issued = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let old = worker(creds(&ca, "w1", Role::Worker)).await;
    eventually(|| {
        let mut d = old.clone();
        async move {
            d.register(RegisterRequest {
                node: String::new(),
                status_json: br#"{"state":"ready"}"#.to_vec(),
                role: String::new(),
            })
            .await
        }
    })
    .await;
    heartbeat(old.clone()).await.unwrap();

    let mut admin = m.objects().await;
    let w1 = || {
        Some(ObjectRef {
            kind: "Node".into(),
            namespace: String::new(),
            name: "w1".into(),
        })
    };
    // A delete refused for a stale version removes nothing.
    let stale = admin
        .delete(DeleteRequest {
            r#ref: w1(),
            expected_version: Some(u64::MAX),
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted, "{stale}");
    heartbeat(old.clone()).await.unwrap();
    // A stream open before the removal ends with it.
    let mut open = old
        .clone()
        .endpoints(EndpointsRequest {
            node: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    admin
        .delete(DeleteRequest {
            r#ref: w1(),
            expected_version: None,
        })
        .await
        .unwrap();
    let refused = |r: Result<_, tonic::Status>| match r {
        Err(e) if e.code() == Code::PermissionDenied => e.message().to_owned(),
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("allowed"),
    };
    let ended = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match open.message().await {
                Ok(Some(_)) => continue,
                other => return other,
            }
        }
    })
    .await
    .expect("the stream ends");
    assert_eq!(ended.unwrap_err().code(), Code::PermissionDenied);
    let why = refused(heartbeat(old.clone()).await.map(|_| ()));
    assert!(why.contains("node w1 was removed"), "{why}");
    let (_, csr_pem) = legion_pki::request().unwrap();
    refused(
        old.clone()
            .renew(RenewRequest { csr_pem })
            .await
            .map(|_| ()),
    );

    // A manager forwarding for it is refused too: the leader checks the
    // certificate it names against what it knows, whatever the forwarding
    // manager's replica held.
    let manager = worker(creds(&ca, "m9", Role::Manager)).await;
    let forwarded_for = |issued: u64| {
        let mut req = tonic::Request::new(HeartbeatRequest {
            node: "w1".into(),
            usage: Vec::new(),
        });
        let md = req.metadata_mut();
        md.insert("legion-forwarded-worker", "w1".parse().unwrap());
        md.insert("legion-forwarded-issued", issued.into());
        req
    };
    let why = refused(
        manager
            .clone()
            .heartbeat(forwarded_for(old_issued))
            .await
            .map(|_| ()),
    );
    assert!(why.contains("node w1 was removed"), "{why}");

    // Issued after the removal, as joining again gives it.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let new = worker(creds(&ca, "w1", Role::Worker)).await;
    heartbeat(new).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    manager.clone().heartbeat(forwarded_for(now)).await.unwrap();

    // A worker a manager forwards for registers as a worker, even named
    // as that manager is and asking for more.
    let mut req = tonic::Request::new(RegisterRequest {
        node: "m9".into(),
        status_json: br#"{"state":"ready"}"#.to_vec(),
        role: "manager".into(),
    });
    let md = req.metadata_mut();
    md.insert("legion-forwarded-worker", "m9".parse().unwrap());
    md.insert("legion-forwarded-issued", now.into());
    manager.clone().register(req).await.unwrap();
    assert_eq!(node_role(&m, "m9").as_deref(), Some("worker"));
    m.stop().await;
}

/// The managers' protocol floor, as recorded: 1 until raised.
fn floor(m: &Running) -> u32 {
    let key = legion_api::ObjectKey::new(
        legion_api::Kind::Config,
        legion_manager::SYSTEM_NAMESPACE,
        "protocol",
    );
    m.store
        .get(&key)
        .unwrap()
        .and_then(|o| o.spec["text"].as_str().and_then(|t| t.parse().ok()))
        .unwrap_or(1)
}

/// The floor rises to what every manager speaks, and only then: a manager
/// still on protocol 1 holds it there. Risen, a manager below it may no
/// longer join, as the log may hold what it cannot apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_floor_rises_once_every_manager_speaks_it() {
    use legion_api::protocol::{Protocol, SPEAKS};
    const { assert!(SPEAKS >= 2) };
    let ca = Ca::generate().unwrap();
    let token_of = async |m: &Running| {
        m.cluster()
            .await
            .join_token(JoinTokenRequest {
                role: "manager".into(),
                rotate: false,
            })
            .await
            .unwrap()
            .into_inner()
            .token
    };
    let one = Protocol {
        speaks: 1,
        oldest: 1,
    };
    let admin = creds(&ca, "admin", Role::Admin);

    // One manager on protocol 1 holds the floor at 1.
    let first = Running::start_with(Some(&ca)).await;
    first.cluster().await.init(InitRequest {}).await.unwrap();
    let token = token_of(&first).await;
    let old = Running::join_then_serve_as(&first.addr, &token, "m2", admin.clone(), one).await;
    let voters = || {
        let m = first.store.raft.metrics().borrow().clone();
        m.membership_config.membership().voter_ids().count()
    };
    until_within("both voters", Duration::from_secs(30), || voters() == 2).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(floor(&first), 1);
    old.stop().await;
    first.stop().await;

    // All on this build, it rises to what they speak.
    let first = Running::start_with(Some(&ca)).await;
    first.cluster().await.init(InitRequest {}).await.unwrap();
    let token = token_of(&first).await;
    let second = Running::join(&first.addr, &token, "m2", admin).await;
    until("the floor raised", || floor(&first) == SPEAKS).await;
    until("the floor everywhere", || floor(&second) == SPEAKS).await;

    // Then a manager on protocol 1 is refused its join.
    let dir = tempfile::tempdir().unwrap();
    let id = legion_manager::node_id(dir.path()).unwrap();
    let err = legion_manager::join_as(&first.addr, &token, "m3", id, "127.0.0.1:1", one)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!(
            "node m3 speaks protocol 1, older than {SPEAKS}, the oldest this one works with: upgrade it first"
        )),
        "{err}"
    );
    second.stop().await;
    first.stop().await;
}

/// A manager upgraded past what the others work with takes no part in the
/// cluster: each refuses the other's Raft calls, so it neither catches up
/// nor votes, and the others go on without it. One protocol ahead, it
/// takes part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manager_upgraded_too_far_takes_no_part() {
    use legion_api::protocol::{OLDEST, Protocol, SPEAKS};
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
    let ahead = Protocol {
        speaks: SPEAKS + 2,
        oldest: SPEAKS + 1,
    };
    let far = Running::join_then_serve_as(&first.addr, &token, "m2", admin.clone(), ahead).await;
    let next = Protocol {
        speaks: SPEAKS + 1,
        oldest: OLDEST,
    };
    let near = Running::join_then_serve_as(&first.addr, &token, "m3", admin, next).await;
    let voter = |id: u64| {
        let m = first.store.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .voter_ids()
            .any(|v| v == id)
    };
    let (far_id, near_id) = (
        far.store.raft.metrics().borrow().id,
        near.store.raft.metrics().borrow().id,
    );
    until_within("the near manager a voter", Duration::from_secs(30), || {
        voter(near_id)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!voter(far_id));
    let applied = far.store.raft.metrics().borrow().last_applied;
    assert!(applied.is_none(), "{applied:?}");
    // The two that work together still write.
    let api = near.objects().await;
    eventually(|| {
        let mut api = api.clone();
        async move { api.apply(apply(service("after", 1))).await }
    })
    .await;
    for m in [far, near, first] {
        m.stop().await;
    }
}

/// A manager or worker joins only if its build and the cluster's work
/// together, and is told which goes first; refused, it is no member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_the_cluster_cannot_work_with_is_refused_its_join() {
    use legion_api::protocol::{OLDEST, Protocol, SPEAKS};
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
    let members = async || {
        first
            .cluster()
            .await
            .status(StatusRequest {})
            .await
            .unwrap()
            .into_inner()
            .members
            .len()
    };
    let dir = tempfile::tempdir().unwrap();
    let id = legion_manager::node_id(dir.path()).unwrap();
    let ahead = Protocol {
        speaks: SPEAKS + 2,
        oldest: SPEAKS + 1,
    };
    let err = legion_manager::join_as(&first.addr, &token, "m2", id, "127.0.0.1:1", ahead)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!(
            "node m2 works with protocol {} and newer, and this one speaks {SPEAKS}: upgrade this one first",
            SPEAKS + 1
        )),
        "{err}"
    );
    assert_eq!(members().await, 1);

    // One protocol ahead, it joins.
    let next = Protocol {
        speaks: SPEAKS + 1,
        oldest: OLDEST,
    };
    legion_manager::join_as(&first.addr, &token, "m2", id, "127.0.0.1:1", next)
        .await
        .unwrap();
    first.stop().await;
}

/// A node registers only if its build and the manager's work together
/// (`legion_api::protocol`): refused, it is told which goes first, and
/// nothing of it is recorded; taken, it hears what the manager speaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_of_a_build_the_manager_cannot_work_with_is_refused() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let d = DispatcherClient::new(m.channel(None).await.unwrap());
    let register = |node: &str, protocol: &str| {
        let mut d = d.clone();
        let req = RegisterRequest {
            node: node.into(),
            status_json: format!(r#"{{"state":"ready"{protocol}}}"#).into_bytes(),
            ..Default::default()
        };
        async move { d.register(req).await }
    };

    use legion_api::protocol::{OLDEST, SPEAKS};
    let said = |speaks: u32, oldest: u32| {
        format!(r#","protocol":{{"speaks":{speaks},"oldest":{oldest}}}"#)
    };
    let err = register("new", &said(SPEAKS + 2, SPEAKS + 1))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented, "{err}");
    assert_eq!(
        err.message(),
        format!(
            "node new works with protocol {} and newer, and this one speaks {SPEAKS}: upgrade this one first",
            SPEAKS + 1
        )
    );
    assert_eq!(node_state(&m, "new"), None);

    // One protocol ahead, or from before the handshake, it is taken.
    for (node, protocol) in [
        ("next", said(SPEAKS + 1, OLDEST)),
        ("before", String::new()),
    ] {
        let answer = eventually(|| register(node, &protocol)).await.into_inner();
        assert_eq!(
            (answer.protocol, answer.oldest_protocol),
            (legion_api::protocol::SPEAKS, legion_api::protocol::OLDEST)
        );
        assert_eq!(node_state(&m, node).as_deref(), Some("ready"));
    }
    m.stop().await;
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
                ..Default::default()
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
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    let err = DispatcherClient::new(first.channel(Some(&admin)).await.unwrap())
        .heartbeat(HeartbeatRequest {
            node: "w1".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    // A renewal through the follower is signed by the leader, for the
    // worker.
    let (_, csr_pem) = legion_pki::request().unwrap();
    let cert_pem = d1
        .clone()
        .renew(RenewRequest { csr_pem })
        .await
        .unwrap()
        .into_inner()
        .cert_pem;
    let renewed = legion_pki::Bundle {
        cert_pem,
        key_pem: String::new(),
        ca_pem: String::new(),
    }
    .identity()
    .unwrap();
    assert_eq!(renewed, Identity::new("w1", Role::Worker));

    // Releasing a volume no task of the node uses tells it nothing of it.
    apply_json(&first, "Volume", "vol", serde_json::json!({})).await;
    let released = d1
        .clone()
        .claim_volume(ClaimVolumeRequest {
            node: String::new(),
            namespace: "default".into(),
            name: "vol".into(),
            dataset: String::new(),
            release: true,
        })
        .await
        .unwrap()
        .into_inner();
    let released: legion_api::dispatch::VolumeClaim =
        serde_json::from_slice(&released.claim_json).unwrap();
    assert!(
        !matches!(released, legion_api::dispatch::VolumeClaim::Claimed(_)),
        "{released:?}"
    );

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
    // Its task lost with it, the node may no longer read the task's
    // secrets, from either manager.
    let orphaned = |m: &Running| {
        m.store
            .get(&key)
            .unwrap()
            .is_some_and(|t| t.status["state"] == "orphaned")
    };
    until("the task is orphaned on both managers", || {
        orphaned(&first) && orphaned(&follower)
    })
    .await;
    assert_eq!(
        data("token").await.unwrap_err().code(),
        Code::PermissionDenied
    );
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

fn object(kind: &str, namespace: &str, name: &str, spec: serde_json::Value) -> ApplyRequest {
    apply(Object {
        kind: kind.into(),
        namespace: namespace.into(),
        name: name.into(),
        spec_json: spec.to_string().into_bytes(),
        ..Default::default()
    })
}

fn binding(namespace: &str, user: &str, role: &str) -> ApplyRequest {
    object(
        "RoleBinding",
        namespace,
        user,
        serde_json::json!({ "user": user, "role": role }),
    )
}

/// The message of a refusal.
fn denied<T>(r: Result<T, tonic::Status>) -> String {
    match r {
        Err(e) if e.code() == Code::PermissionDenied => e.message().to_owned(),
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("allowed"),
    }
}

fn get(kind: &str, namespace: &str, name: &str) -> ObjectRef {
    ObjectRef {
        kind: kind.into(),
        namespace: namespace.into(),
        name: name.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn usage_shows_what_the_caller_may_read() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let admin = m.objects().await;
    eventually(|| {
        let mut admin = admin.clone();
        async move { admin.apply(binding("a", "alice", "view")).await }
    })
    .await;
    let w1 = creds(&ca, "w1", Role::Worker);
    let d1 = DispatcherClient::new(m.channel(Some(&w1)).await.unwrap());
    // Usage is believed only of a node's own tasks: w1 runs web in a and b.
    eventually(|| {
        let mut d = d1.clone();
        async move {
            d.register(RegisterRequest {
                node: String::new(),
                status_json: br#"{"state":"ready"}"#.to_vec(),
                ..Default::default()
            })
            .await
        }
    })
    .await;
    for namespace in ["a", "b"] {
        let object = Object {
            namespace: namespace.into(),
            ..service("web", 1)
        };
        eventually(|| {
            let mut admin = admin.clone();
            let object = object.clone();
            async move { admin.apply(apply(object)).await }
        })
        .await;
    }
    until("web runs on w1 in both namespaces", || {
        ["a", "b"].iter().all(|ns| {
            let key = legion_api::ObjectKey::new(legion_api::Kind::Task, ns, "web-1-1");
            m.store
                .get(&key)
                .unwrap()
                .is_some_and(|t| t.spec["node"] == "w1")
        })
    })
    .await;
    let task = |namespace: &str, cpu_percent| legion_proto::v1::TaskUsage {
        namespace: namespace.into(),
        task: "web-1-1".into(),
        cpu_percent,
        memory_bytes: 1 << 20,
    };
    eventually(|| {
        let mut d = d1.clone();
        let usage = vec![task("a", 25.0), task("b", 50.0)];
        async move {
            d.heartbeat(HeartbeatRequest {
                node: String::new(),
                usage,
            })
            .await
        }
    })
    .await;
    let usage = |mut c: ClusterClient<Channel>, namespace: &str| {
        let req = legion_proto::v1::UsageRequest {
            namespace: namespace.into(),
        };
        async move { c.usage(req).await }
    };
    let seen = |u: legion_proto::v1::UsageResponse| -> Vec<(String, String, f64)> {
        u.nodes
            .into_iter()
            .flat_map(|n| {
                let node = n.node;
                n.tasks
                    .into_iter()
                    .map(move |t| (node.clone(), t.namespace, t.cpu_percent))
            })
            .collect()
    };
    let row = |ns: &str, cpu| ("w1".to_owned(), ns.to_owned(), cpu);

    // The admin sees every task, by node; one namespace when asked.
    let all = usage(m.cluster().await, "").await.unwrap().into_inner();
    assert_eq!(seen(all), [row("a", 25.0), row("b", 50.0)]);
    let b = usage(m.cluster().await, "b").await.unwrap().into_inner();
    assert_eq!(seen(b), [row("b", 50.0)]);
    // A user, only the namespaces they may read in.
    let alice = ClusterClient::new(
        m.channel(Some(&creds(&ca, "alice", Role::User)))
            .await
            .unwrap(),
    );
    let mine = usage(alice.clone(), "").await.unwrap().into_inner();
    assert_eq!(seen(mine), [row("a", 25.0)]);
    denied(usage(alice, "b").await);
}

/// A manager forwards a user's request with the user's name, and the leader
/// checks the user's rights again rather than granting the manager's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_checks_users_a_manager_forwards() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let admin = m.objects().await;
    eventually(|| {
        let mut admin = admin.clone();
        async move { admin.apply(binding("a", "alice", "view")).await }
    })
    .await;
    let manager = ObjectsClient::new(
        m.channel(Some(&creds(&ca, "m2", Role::Manager)))
            .await
            .unwrap(),
    );
    let as_alice = |kind: &str, namespace: &str| {
        let mut req = tonic::Request::new(get(kind, namespace, "web"));
        req.metadata_mut()
            .insert("legion-forwarded", "1".parse().unwrap());
        req.metadata_mut()
            .insert("legion-forwarded-for", "alice".parse().unwrap());
        req
    };
    // Alice may not read in b, nor read Secrets in a; the manager may.
    let why = denied(manager.clone().get(as_alice("Service", "b")).await);
    assert!(
        why.contains("user alice has no role in namespace b"),
        "{why}"
    );
    denied(manager.clone().get(as_alice("Secret", "a")).await);
    let code = |r: Result<tonic::Response<Object>, tonic::Status>| r.err().map(|e| e.code());
    assert_eq!(
        code(manager.clone().get(as_alice("Service", "a")).await),
        Some(Code::NotFound)
    );
    assert_eq!(
        code(manager.clone().get(get("Secret", "b", "web")).await),
        Some(Code::NotFound)
    );
    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn users_act_within_their_role_bindings() {
    let ca = Ca::generate().unwrap();
    let m = Running::start_with(Some(&ca)).await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let mut admin = m.objects().await;
    let web = |ns: &str| {
        let mut s = service("web", 1);
        s.namespace = ns.into();
        apply(s)
    };
    for req in [
        web("a"),
        web("b"),
        object(
            "Secret",
            "a",
            "key",
            serde_json::json!({ "text": "s3cret" }),
        ),
        binding("a", "alice", "view"),
        binding("a", "bob", "edit"),
        binding("a", "carol", "admin"),
    ] {
        eventually(|| {
            let mut admin = admin.clone();
            let req = req.clone();
            async move { admin.apply(req).await }
        })
        .await;
    }
    let user = |name: &str| {
        let creds = creds(&ca, name, Role::User);
        let m = &m;
        async move {
            let channel = m.channel(Some(&creds)).await.unwrap();
            (
                ObjectsClient::new(channel.clone()),
                ClusterClient::new(channel),
            )
        }
    };

    // view reads its namespace, but not Secrets, and changes nothing.
    let (mut alice, mut alice_cluster) = user("alice").await;
    alice.get(get("Service", "a", "web")).await.unwrap();
    let why = denied(alice.get(get("Service", "b", "web")).await);
    assert!(why.contains("no role in namespace b"), "{why}");
    let why = denied(alice.get(get("Secret", "a", "key")).await);
    assert!(why.contains("takes edit"), "{why}");
    denied(alice.apply(web("a")).await);
    // Across namespaces, only what it may read; nothing cluster-scoped.
    let all = alice
        .list(ListRequest {
            kind: "Service".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let seen: Vec<&str> = all.objects.iter().map(|o| o.namespace.as_str()).collect();
    assert_eq!(seen, ["a"]);
    denied(
        alice
            .list(ListRequest {
                kind: "Node".into(),
                ..Default::default()
            })
            .await,
    );
    denied(alice_cluster.init(InitRequest {}).await);
    denied(
        alice_cluster
            .join_token(JoinTokenRequest {
                role: "worker".into(),
                rotate: false,
            })
            .await,
    );
    alice_cluster.status(StatusRequest {}).await.unwrap();

    // A watch of every namespace shows only its own.
    let from = all.version;
    let mut events = alice
        .watch(WatchRequest {
            kind: "Service".into(),
            from_version: from,
        })
        .await
        .unwrap()
        .into_inner();
    let mut other = service("other", 1);
    other.namespace = "b".into();
    admin.apply(apply(other)).await.unwrap();
    let mut mine = service("mine", 1);
    mine.namespace = "a".into();
    admin.apply(apply(mine)).await.unwrap();
    // The controller writes statuses meanwhile: read until "mine".
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), events.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let o = event.object.unwrap();
        assert_eq!(o.namespace, "a", "{} from another namespace", o.name);
        if o.name == "mine" {
            break;
        }
    }

    // edit reads Secrets and changes objects, but not RoleBindings, and
    // only in its namespace.
    let (mut bob, _) = user("bob").await;
    bob.get(get("Secret", "a", "key")).await.unwrap();
    bob.apply(web("a")).await.unwrap();
    let why = denied(bob.apply(binding("a", "bob", "admin")).await);
    assert!(why.contains("takes admin"), "{why}");
    denied(bob.apply(web("b")).await);
    denied(
        bob.delete(DeleteRequest {
            r#ref: Some(get("Service", "b", "web")),
            ..Default::default()
        })
        .await,
    );

    // Sessions with tasks: logs take view, exec and port-forward edit. The
    // service has no running task (no node), which is as far as a
    // permitted session gets here.
    let session = |name: &str, namespace: &str, action: Action| {
        let creds = creds(&ca, name, Role::User);
        let m = &m;
        let namespace = namespace.to_owned();
        async move {
            let channel = m.channel(Some(&creds)).await.unwrap();
            let open = SessionFrame {
                frame: Some(Frame::Open(SessionOpen {
                    namespace,
                    task: "web".into(),
                    container: String::new(),
                    action: Some(action),
                })),
            };
            PodsClient::new(channel)
                .session(tokio_stream::iter([open]))
                .await
                .map(drop)
        }
    };
    let logs = || Action::Logs(LogsOptions::default());
    let exec = || {
        Action::Exec(ExecOptions {
            command: vec!["sh".into()],
            ..Default::default()
        })
    };
    let refused = |r: Result<(), tonic::Status>| {
        let s = r.unwrap_err();
        (s.code(), s.message().to_owned())
    };
    let no_task = (
        Code::FailedPrecondition,
        "service web has no running task".to_owned(),
    );
    assert_eq!(refused(session("alice", "a", logs()).await), no_task);
    let why = denied(session("alice", "a", exec()).await);
    assert!(why.contains("to change Tasks takes edit"), "{why}");
    denied(session("alice", "a", Action::Port(80)).await);
    denied(session("alice", "b", logs()).await);
    assert_eq!(refused(session("bob", "a", exec()).await), no_task);
    assert_eq!(
        refused(session("bob", "a", Action::Port(80)).await),
        no_task
    );
    denied(session("bob", "b", exec()).await);

    // admin manages the namespace's bindings.
    let (mut carol, _) = user("carol").await;
    carol.apply(binding("a", "dave", "view")).await.unwrap();
    denied(carol.apply(binding("b", "dave", "view")).await);

    // Rights live in the bindings: deleting one takes them away at once.
    admin
        .delete(DeleteRequest {
            r#ref: Some(get("RoleBinding", "a", "alice")),
            ..Default::default()
        })
        .await
        .unwrap();
    denied(alice.get(get("Service", "a", "web")).await);

    // Only the cluster admin certifies users.
    let (key_pem, csr) = legion_pki::request().unwrap();
    let issue = |name: &str| legion_proto::v1::IssueUserRequest {
        name: name.into(),
        csr_pem: csr.clone(),
        days: 7,
    };
    denied(alice_cluster.issue_user(issue("eve")).await);
    let issued = m
        .cluster()
        .await
        .issue_user(issue("eve"))
        .await
        .unwrap()
        .into_inner();
    let bundle = legion_pki::Bundle {
        cert_pem: issued.cert_pem,
        key_pem,
        ca_pem: issued.ca_pem,
    };
    assert_eq!(bundle.identity().unwrap(), Identity::new("eve", Role::User));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_quotas_refuse_what_would_not_fit() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let api = m.objects().await;
    let try_apply = |req: ApplyRequest| {
        let api = api.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match api.clone().apply(req.clone()).await {
                    Err(s)
                        if s.code() == Code::Unavailable
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    r => return r.map(|_| ()),
                }
            }
        }
    };
    let refused = |r: Result<(), tonic::Status>| match r {
        Err(e) if e.code() == Code::FailedPrecondition => e.message().to_owned(),
        other => panic!("{other:?}"),
    };
    let quota =
        |q: serde_json::Value| object("Namespace", "", "team", serde_json::json!({ "quota": q }));
    let svc = |name: &str, replicas: u32, cpu: Option<&str>| {
        let resources = match cpu {
            Some(c) => serde_json::json!({ "requests": { "cpu": c } }),
            None => serde_json::json!({}),
        };
        object(
            "Service",
            "team",
            name,
            serde_json::json!({ "replicas": replicas, "pod": { "containers": [
                { "name": "c", "image": "c:1", "resources": resources }
            ] } }),
        )
    };
    let volume = |name: &str, size: Option<&str>| {
        let spec = match size {
            Some(s) => serde_json::json!({ "size": s }),
            None => serde_json::json!({}),
        };
        object("Volume", "team", name, spec)
    };

    try_apply(quota(
        serde_json::json!({ "cpu": "1", "tasks": 3, "volumes": 1, "storage": "10Gi" }),
    ))
    .await
    .unwrap();
    // Under a CPU quota, a pod must say what it needs.
    let why = refused(try_apply(svc("web", 2, None)).await);
    assert!(why.contains("must request cpu"), "{why}");
    try_apply(svc("web", 2, Some("400m"))).await.unwrap();
    assert_eq!(
        refused(try_apply(svc("api", 2, Some("300m"))).await),
        "over the quota of namespace team: cpu 1400m of 1, tasks 4 of 3"
    );
    // Room made, it fits.
    try_apply(svc("web", 1, Some("400m"))).await.unwrap();
    try_apply(svc("api", 2, Some("300m"))).await.unwrap();
    // So must a Job's and a CronJob's, whose tasks the scheduler holds to
    // the quota.
    let job = |cpu: Option<&str>| {
        let resources = match cpu {
            Some(c) => serde_json::json!({ "requests": { "cpu": c } }),
            None => serde_json::json!({}),
        };
        serde_json::json!({ "pod": { "containers": [
            { "name": "c", "image": "c:1", "resources": resources }
        ] } })
    };
    let why = refused(try_apply(object("Job", "team", "once", job(None))).await);
    assert!(why.contains("must request cpu"), "{why}");
    try_apply(object("Job", "team", "once", job(Some("100m"))))
        .await
        .unwrap();
    let cron = |cpu| serde_json::json!({ "schedule": "0 3 * * *", "job": job(cpu) });
    let why = refused(try_apply(object("CronJob", "team", "nightly", cron(None))).await);
    assert!(why.contains("must request cpu"), "{why}");
    try_apply(object("CronJob", "team", "nightly", cron(Some("100m"))))
        .await
        .unwrap();

    // Volumes: counted, and sized under a storage quota.
    let why = refused(try_apply(volume("data", None)).await);
    assert!(why.contains("needs a size"), "{why}");
    try_apply(volume("data", Some("10Gi"))).await.unwrap();
    assert_eq!(
        refused(try_apply(volume("logs", Some("1Gi"))).await),
        "over the quota of namespace team: volumes 2 of 1, storage 11Gi of 10Gi"
    );
    // A volume of another driver takes no storage, so has no size, and is
    // counted all the same.
    let nfs = object(
        "Volume",
        "team",
        "shared",
        serde_json::json!({ "driver": "nfs", "nfs": { "server": "files", "path": "/export" } }),
    );
    assert_eq!(
        refused(try_apply(nfs).await),
        "over the quota of namespace team: volumes 2 of 1"
    );

    // Under a lowered quota, shrinking still goes through; growing does not.
    try_apply(quota(serde_json::json!({ "tasks": 1 })))
        .await
        .unwrap();
    try_apply(svc("api", 1, Some("300m"))).await.unwrap();
    refused(try_apply(svc("api", 2, Some("300m"))).await);

    // Other namespaces are not limited.
    let mut free = svc("api", 50, None);
    free.object.as_mut().unwrap().namespace = "other".into();
    try_apply(free).await.unwrap();

    // A stateful service's claims count as the Volumes they make, made or
    // not, and once only.
    try_apply(object(
        "Namespace",
        "",
        "store",
        serde_json::json!({ "quota": { "volumes": 2, "storage": "3Gi" } }),
    ))
    .await
    .unwrap();
    let db = |replicas: u32, size: Option<&str>| {
        let claim = match size {
            Some(s) => serde_json::json!({ "size": s }),
            None => serde_json::json!({}),
        };
        object(
            "Service",
            "store",
            "db",
            serde_json::json!({ "mode": "stateful", "replicas": replicas, "pod": {
                "containers": [{ "name": "c", "image": "c:1",
                                 "volumeMounts": [{ "name": "data", "path": "/data" }] }],
                "volumes": [{ "name": "data", "claim": claim }],
            } }),
        )
    };
    let why = refused(try_apply(db(2, None)).await);
    assert!(why.contains("claim data needs a size"), "{why}");
    assert_eq!(
        refused(try_apply(db(3, Some("1Gi"))).await),
        "over the quota of namespace store: volumes 3 of 2"
    );
    try_apply(db(2, Some("1Gi"))).await.unwrap();
    let mut extra = volume("extra", Some("1Gi"));
    extra.object.as_mut().unwrap().namespace = "store".into();
    assert_eq!(
        refused(try_apply(extra.clone()).await),
        "over the quota of namespace store: volumes 3 of 2"
    );
    // Made by the controller, they still count once.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let list = api
            .clone()
            .list(ListRequest {
                kind: "Volume".into(),
                namespace: "store".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        if list.objects.len() == 2 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "volumes not made");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    try_apply(db(2, Some("1Gi"))).await.unwrap();
    refused(try_apply(extra).await);
    // Growing the claim grows the Volumes, so it counts at its new size.
    assert_eq!(
        refused(try_apply(db(2, Some("2Gi"))).await),
        "over the quota of namespace store: storage 4Gi of 3Gi"
    );
    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespaces_decide_how_far_jails_loosen() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let api = m.objects().await;
    let try_apply = |req: ApplyRequest| {
        let mut api = api.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match api.apply(req.clone()).await {
                    Err(s)
                        if s.code() == Code::Unavailable
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    r => return r.map(|_| ()),
                }
            }
        }
    };
    let pod = |security: serde_json::Value| {
        serde_json::json!({ "containers": [
            { "name": "c", "image": "c:1", "security": security }
        ] })
    };
    let svc = |security| {
        object(
            "Service",
            "team",
            "ping",
            serde_json::json!({ "pod": pod(security) }),
        )
    };
    let job = |security| {
        object(
            "Job",
            "team",
            "fsck",
            serde_json::json!({ "pod": pod(security) }),
        )
    };
    let cron = |security| {
        object(
            "CronJob",
            "team",
            "nightly",
            serde_json::json!({ "schedule": "0 3 * * *", "job": { "pod": pod(security) } }),
        )
    };
    let raw = serde_json::json!({ "allow": ["rawSockets"] });
    let low = serde_json::json!({ "securelevel": -1, "enforceStatfs": 1 });

    // Without a word from the namespace, nothing is loosened.
    try_apply(svc(serde_json::json!({}))).await.unwrap();
    assert_eq!(
        denied(try_apply(svc(raw.clone())).await),
        "namespace team does not allow its containers' jails rawSockets \
         (the cluster admin sets spec.security.allow on the Namespace)"
    );
    try_apply(object("Namespace", "", "team", serde_json::json!({})))
        .await
        .unwrap();
    denied(try_apply(svc(raw.clone())).await);

    try_apply(object(
        "Namespace",
        "",
        "team",
        serde_json::json!({ "security": { "allow": ["rawSockets", "securelevel"] } }),
    ))
    .await
    .unwrap();
    try_apply(svc(raw.clone())).await.unwrap();
    let why = denied(try_apply(job(low.clone())).await);
    assert!(why.contains("jails enforceStatfs ("), "{why}");
    let why = denied(try_apply(cron(low)).await);
    assert!(why.contains("jails enforceStatfs ("), "{why}");
    try_apply(cron(serde_json::json!({ "securelevel": 0 })))
        .await
        .unwrap();
    m.stop().await;
}

#[tokio::test]
async fn the_leader_reports_the_cluster() {
    let m = Running::start().await;
    let before = m.metrics.collect().finish();
    assert!(before.contains("\nlegion_raft_leader 0\n"), "{before}");
    assert!(!before.contains("legion_tasks"), "{before}");

    m.cluster().await.init(InitRequest {}).await.unwrap();
    let api = m.objects().await;
    let apply = |req: ApplyRequest| {
        let mut api = api.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match api.apply(req.clone()).await {
                    Err(s)
                        if s.code() == Code::Unavailable
                            && tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    r => return r.unwrap(),
                }
            }
        }
    };
    apply(object(
        "Namespace",
        "",
        "team",
        serde_json::json!({ "quota": { "tasks": 5, "volumes": 2 } }),
    ))
    .await;
    apply(object(
        "Service",
        "team",
        "web",
        serde_json::json!({ "replicas": 2, "pod": { "containers": [{ "name": "c", "image": "c:1" }] } }),
    ))
    .await;
    apply(object("Volume", "team", "data", serde_json::json!({}))).await;

    // The controllers make the tasks; with no node, they wait.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let text = loop {
        let text = m.metrics.collect().finish();
        if text.contains("legion_tasks{namespace=\"team\",state=\"pending\"} 2\n") {
            break text;
        }
        assert!(tokio::time::Instant::now() < deadline, "{text}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for line in [
        "legion_raft_leader 1",
        "legion_raft_voters 1",
        "legion_objects{kind=\"Namespace\",namespace=\"\"} 1",
        "legion_objects{kind=\"Service\",namespace=\"team\"} 1",
        "legion_objects{kind=\"Task\",namespace=\"team\"} 2",
        "legion_tasks{namespace=\"team\",state=\"running\"} 0",
        "legion_task_restarts{namespace=\"team\",task=\"web-1-1\",service=\"web\",node=\"\"} 0",
        "legion_service_replicas{namespace=\"team\",service=\"web\"} 2",
        "legion_service_ready_tasks{namespace=\"team\",service=\"web\"} 0",
        "legion_nodes{state=\"ready\"} 0",
        "legion_quota_limit{namespace=\"team\",resource=\"tasks\"} 5",
        "legion_quota_limit{namespace=\"team\",resource=\"volumes\"} 2",
        "legion_quota_used{namespace=\"team\",resource=\"tasks\"} 0",
        "legion_quota_used{namespace=\"team\",resource=\"volumes\"} 1",
    ] {
        assert!(text.contains(&format!("\n{line}\n")), "{line} in:\n{text}");
    }
    assert!(!text.contains("resource=\"cpu\""), "{text}");

    // A scraper holding on does not keep the store open; what it reads
    // then is the requests the manager answered.
    let metrics = m.metrics.clone();
    m.stop().await;
    let text = metrics.collect().finish();
    assert!(
        text.starts_with("# HELP legion_api_requests_total"),
        "{text}"
    );
    assert!(!text.contains("legion_raft"), "{text}");
}

#[tokio::test]
async fn requests_are_counted_by_method_and_code_and_timed() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let mut api = m.objects().await;
    let missing = ObjectRef {
        kind: "Service".into(),
        namespace: String::new(),
        name: "none".into(),
    };
    for _ in 0..2 {
        assert_eq!(
            api.get(missing.clone()).await.unwrap_err().code(),
            Code::NotFound
        );
    }
    // A stream the client drops ends cancelled.
    let events = api
        .watch(WatchRequest {
            kind: "Service".into(),
            from_version: 0,
        })
        .await
        .unwrap()
        .into_inner();
    drop(events);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let text = loop {
        let text = m.metrics.collect().finish();
        let watch = "legion_api_requests_total{service=\"legion.v1.Objects\",\
                     method=\"Watch\",code=\"Cancelled\"} 1\n";
        if text.contains(watch) {
            break text;
        }
        assert!(tokio::time::Instant::now() < deadline, "{text}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for line in [
        "legion_api_requests_total{service=\"legion.v1.Cluster\",method=\"Init\",code=\"Ok\"} 1",
        "legion_api_requests_total{service=\"legion.v1.Objects\",method=\"Get\",code=\"NotFound\"} 2",
        "legion_api_request_duration_seconds_count{service=\"legion.v1.Objects\",method=\"Get\"} 2",
        "legion_api_request_duration_seconds_bucket{service=\"legion.v1.Objects\",method=\"Get\",le=\"+Inf\"} 2",
        "legion_api_request_duration_seconds_count{service=\"legion.v1.Objects\",method=\"Watch\"} 1",
    ] {
        assert!(text.contains(&format!("\n{line}\n")), "{line} in:\n{text}");
    }
    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_manager_leads_alone_and_ignores_the_old_cluster() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    form_cluster(&managers).await;
    let follower = managers[2].objects().await;
    for n in 0..10 {
        eventually(|| {
            let mut follower = follower.clone();
            async move { follower.apply(apply(service(&format!("s{n}"), 1))).await }
        })
        .await;
    }

    // Through a follower, which forwards to the leader.
    let third = &managers[2];
    let mut chunks = eventually(|| async move {
        third
            .cluster()
            .await
            .backup(legion_proto::v1::BackupRequest {})
            .await
    })
    .await
    .into_inner();
    let mut json = Vec::new();
    while let Some(chunk) = chunks.message().await.unwrap() {
        json.extend(chunk.data);
    }
    let backup: legion_store::Backup = serde_json::from_slice(&json).unwrap();
    let services = |objects: &[legion_api::Object]| {
        objects
            .iter()
            .filter(|o| o.kind == legion_api::Kind::Service)
            .count()
    };
    assert_eq!(services(&backup.objects), 10);
    assert_eq!(backup.managers.len(), 3);

    // Restored at the same address while the other two carry on: they
    // must not pull it back into the old cluster with what they wrote.
    eventually(|| {
        let mut follower = follower.clone();
        async move { follower.apply(apply(service("lost", 1))).await }
    })
    .await;
    let first = managers.remove(0);
    let addr = first.addr.clone();
    let dir = first.stop_keeping().await;
    let id = legion_manager::node_id(dir.path()).unwrap();
    legion_store::set_aside(dir.path(), "old").unwrap();
    legion_store::restore(dir.path(), id, &addr, &backup, None).unwrap();
    let listener = loop {
        match TcpListener::bind(&addr).await {
            Ok(l) => break l,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    let restored = Running::serve(dir, listener, None, None).await;
    let raft = restored.store.raft.clone();
    raft.wait(Some(Duration::from_secs(10)))
        .current_leader(id, "lead the restored cluster")
        .await
        .unwrap();
    // The old cluster elects at ever higher terms and keeps writing.
    for _ in 0..3 {
        for m in &managers {
            let _ = m.store.raft.trigger().elect().await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        eventually(|| {
            let mut follower = follower.clone();
            async move { follower.apply(apply(service("lost", 2))).await }
        })
        .await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let api = restored.objects().await;
    eventually(|| {
        let mut api = api.clone();
        async move { api.apply(apply(service("after", 1))).await }
    })
    .await;
    let m = raft.metrics().borrow().clone();
    assert_eq!(m.current_leader, Some(id));
    assert_eq!(
        m.membership_config.membership().nodes().count(),
        1,
        "{:?}",
        m.membership_config
    );
    let listed = restored
        .store
        .list(legion_api::Kind::Service, None)
        .unwrap()
        .objects;
    let names: Vec<_> = listed.iter().map(|o| o.metadata.name.as_str()).collect();
    assert_eq!(names.len(), 11, "{names:?}");
    assert!(names.contains(&"after") && !names.contains(&"lost"));

    restored.stop().await;
    for m in managers {
        m.stop().await;
    }
}

/// Deleting a task that has not ended stops it: it stays until its agent
/// reports it gone, and its slot gets no other task before then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_running_task_stops_it_first() {
    let m = Running::start_steady().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let d = DispatcherClient::new(m.channel(None).await.unwrap());
    let register = || {
        let mut d = d.clone();
        async move {
            d.register(RegisterRequest {
                node: "w1".into(),
                status_json: br#"{"state":"ready"}"#.to_vec(),
                ..Default::default()
            })
            .await
        }
    };
    eventually(register).await;
    apply_json(
        &m,
        "Service",
        "web",
        serde_json::json!({
            "replicas": 1,
            "pod": { "containers": [{ "name": "web", "image": "web:1" }] },
        }),
    )
    .await;
    let tasks = |m: &Running| {
        m.store
            .list(legion_api::Kind::Task, Some("default"))
            .unwrap()
            .objects
    };
    until("the task is assigned", || {
        tasks(&m).iter().any(|t| t.spec["node"] == "w1")
    })
    .await;
    let task = tasks(&m).remove(0);
    let report = |state: &str| ReportRequest {
        node: "w1".into(),
        namespace: "default".into(),
        name: task.metadata.name.clone(),
        uid: task.metadata.uid.clone(),
        status_json: format!(r#"{{"state":"{state}"}}"#).into_bytes(),
    };
    d.clone().report(report("running")).await.unwrap();
    let key = task.key();
    until("it runs", || {
        m.store.get(&key).unwrap().unwrap().status["state"] == "running"
    })
    .await;

    let objects = m.objects().await;
    let delete = || {
        let mut objects = objects.clone();
        let r#ref = Some(get("Task", "default", &task.metadata.name));
        async move {
            objects
                .delete(DeleteRequest {
                    r#ref,
                    ..Default::default()
                })
                .await
        }
    };
    delete().await.unwrap();
    let stopping = m.store.get(&key).unwrap().expect("still there");
    assert_eq!(stopping.spec["desiredState"], "shutdown");
    // Deleting it again changes nothing.
    delete().await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    let now = tasks(&m);
    assert_eq!(now.len(), 1, "no replacement while it stops: {now:?}");

    d.clone().report(report("shutdown")).await.unwrap();
    until("a replacement once it has stopped", || tasks(&m).len() == 2).await;
    // Ended, it goes when deleted.
    delete().await.unwrap();
    assert!(m.store.get(&key).unwrap().is_none());
    m.stop().await;
}

/// A task its node rejected never had a pod: no session opens on it, even
/// named directly.
/// A follower authorizes a session on what the leader has committed: it
/// sees a Service written just before, and cut off from the leader it
/// refuses sessions rather than going by what its replica last held (a
/// RoleBinding revoked since, say).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_opens_sessions_only_as_the_leader_has_it() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    form_cluster(&managers).await;
    let open = |task: &str| SessionFrame {
        frame: Some(Frame::Open(SessionOpen {
            namespace: "default".into(),
            task: task.into(),
            container: String::new(),
            action: Some(Action::Logs(LogsOptions::default())),
        })),
    };
    async fn session(m: &Running, open: SessionFrame) -> tonic::Status {
        PodsClient::new(m.channel(None).await.unwrap())
            .session(tokio_stream::iter([open]))
            .await
            .map(drop)
            .unwrap_err()
    }
    let leader = managers[0].objects().await;
    for n in 0..5 {
        let name = format!("web{n}");
        eventually(|| {
            let mut leader = leader.clone();
            let name = name.clone();
            async move { leader.apply(apply(service(&name, 1))).await }
        })
        .await;
        // At once on the follower: the Service is there (no task runs on
        // a node, there being none), never unknown.
        let refused = session(&managers[2], open(&name)).await;
        assert_eq!(refused.code(), Code::FailedPrecondition, "{refused}");
    }

    // The leader and the other follower gone, the last one is cut off.
    let follower = managers.pop().unwrap();
    for m in managers {
        m.stop().await;
    }
    let refused = session(&follower, open("web0")).await;
    assert_eq!(refused.code(), Code::Unavailable, "{refused}");
    follower.stop().await;
}

/// A watch checks rights against the follower's store as each event goes
/// out: one cut off from the leader may lack a revocation, so serves none
/// of its history, just as it reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_cut_off_from_the_leader_opens_no_watch() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    form_cluster(&managers).await;
    let leader = managers[0].objects().await;
    eventually(|| {
        let mut leader = leader.clone();
        async move { leader.apply(apply(service("web", 1))).await }
    })
    .await;
    async fn watch(m: &Running) -> Result<(), tonic::Status> {
        m.objects()
            .await
            .watch(WatchRequest {
                kind: "Service".into(),
                from_version: 0,
            })
            .await
            .map(drop)
    }
    // Caught up, it watches.
    eventually(|| watch(&managers[2])).await;

    let follower = managers.pop().unwrap();
    for m in managers {
        m.stop().await;
    }
    let refused = watch(&follower).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused}");
    follower.stop().await;
}

/// A leader cut off from the other managers may not know yet that another
/// leads, which counts its nodes' silence: it stops answering heartbeats,
/// so their agents know they are cut off (docs/PARTITIONS.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_cut_off_from_a_majority_answers_no_heartbeats() {
    let mut managers = Vec::new();
    for _ in 0..3 {
        managers.push(Running::start().await);
    }
    form_cluster(&managers).await;
    let heartbeat = |m: &Running| {
        let m = m.addr.clone();
        async move {
            let channel = tonic::transport::Channel::from_shared(format!("http://{m}"))
                .unwrap()
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            DispatcherClient::new(channel)
                .heartbeat(HeartbeatRequest {
                    node: "w1".into(),
                    usage: Vec::new(),
                })
                .await
                .map(drop)
        }
    };
    let register = DispatcherClient::new(managers[0].channel(None).await.unwrap());
    eventually(|| {
        let mut d = register.clone();
        async move {
            d.register(RegisterRequest {
                node: "w1".into(),
                status_json: br#"{"state":"ready"}"#.to_vec(),
                ..Default::default()
            })
            .await
        }
    })
    .await;
    eventually(|| heartbeat(&managers[0])).await;

    let leader = managers.remove(0);
    for m in managers {
        m.stop().await;
    }
    // Within the shortest election timeout (1.5 s here), and a check.
    let start = std::time::Instant::now();
    let refused = loop {
        match heartbeat(&leader).await {
            Err(e) => break e,
            Ok(()) => {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(4),
                    "still answering"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };
    assert_eq!(refused.code(), Code::Unavailable, "{refused}");
    assert!(refused.message().contains("majority"), "{refused}");
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_task_has_no_sessions() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let d = DispatcherClient::new(m.channel(None).await.unwrap());
    let register = || {
        let mut d = d.clone();
        async move {
            d.register(RegisterRequest {
                node: "w1".into(),
                status_json: br#"{"state":"ready"}"#.to_vec(),
                ..Default::default()
            })
            .await
        }
    };
    eventually(register).await;
    apply_json(
        &m,
        "Service",
        "web",
        serde_json::json!({
            "replicas": 1,
            "pod": { "containers": [{ "name": "web", "image": "web:1" }] },
        }),
    )
    .await;
    let assigned = || {
        m.store
            .list(legion_api::Kind::Task, Some("default"))
            .unwrap()
            .objects
            .into_iter()
            .find(|t| t.spec["node"] == "w1")
    };
    until("the task is assigned", || assigned().is_some()).await;
    let task = assigned().unwrap();
    d.clone()
        .report(ReportRequest {
            node: "w1".into(),
            namespace: "default".into(),
            name: task.metadata.name.clone(),
            uid: task.metadata.uid.clone(),
            status_json: br#"{"state":"rejected","message":"no such image"}"#.to_vec(),
        })
        .await
        .unwrap();
    let key = task.key();
    until("it is rejected", || {
        m.store.get(&key).unwrap().unwrap().status["state"] == "rejected"
    })
    .await;

    let open = SessionFrame {
        frame: Some(Frame::Open(SessionOpen {
            namespace: "default".into(),
            task: task.metadata.name.clone(),
            container: String::new(),
            action: Some(Action::Logs(LogsOptions::default())),
        })),
    };
    let refused = PodsClient::new(m.channel(None).await.unwrap())
        .session(tokio_stream::iter([open]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains("was rejected: no such image"),
        "{}",
        refused.message()
    );
    m.stop().await;
}

/// A Volume in use keeps its storage: an agent would not see the change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_in_use_keeps_its_storage() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let lun = |target: &str| {
        serde_json::json!({
            "driver": "iscsi",
            "iscsi": { "portal": "192.0.2.5", "target": target, "lun": 0 },
        })
    };
    let objects = m.objects().await;
    let try_apply = |spec: serde_json::Value| {
        let objects = objects.clone();
        async move {
            objects
                .clone()
                .apply(object("Volume", "default", "lun", spec))
                .await
        }
    };
    apply_json(&m, "Volume", "lun", lun("iqn.2026-11.test:a")).await;
    // Unused, it may name another LUN.
    try_apply(lun("iqn.2026-11.test:b")).await.unwrap();

    // Held by a node: no. One the cluster knows, or the controller frees the
    // Volume, as a removed node holds nothing.
    m.store
        .raft
        .client_write(legion_store::Command::Put {
            object: legion_api::Object::new(
                legion_api::Kind::Node,
                "",
                "w1",
                serde_json::json!({}),
            ),
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    let key = legion_api::ObjectKey::new(legion_api::Kind::Volume, "default", "lun");
    let mut held = m.store.get(&key).unwrap().unwrap();
    held.status = serde_json::json!({ "node": "w1" });
    m.store
        .raft
        .client_write(legion_store::Command::Put {
            object: held.clone(),
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    let err = try_apply(lun("iqn.2026-11.test:c")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("held by node w1"), "{err}");

    // Mounted by a task that has not ended: no either.
    held.status = serde_json::Value::Null;
    m.store
        .raft
        .client_write(legion_store::Command::Put {
            object: held,
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    apply_json(
        &m,
        "Service",
        "db",
        serde_json::json!({
            "replicas": 1,
            "pod": {
                "containers": [{
                    "name": "db", "image": "db:1",
                    "volumeMounts": [{ "name": "data", "path": "/data" }],
                }],
                "volumes": [{ "name": "data", "volume": { "name": "lun" } }],
            },
        }),
    )
    .await;
    until("its task", || {
        !m.store
            .list(legion_api::Kind::Task, Some("default"))
            .unwrap()
            .objects
            .is_empty()
    })
    .await;
    let err = try_apply(lun("iqn.2026-11.test:c")).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("mounted by task"), "{err}");
    m.stop().await;
}

/// A registry on plain HTTP whose one repository, `shop/app`, serves the
/// manifest in `served` for any tag, to `bot:hunter2` only.
fn registry(served: std::sync::Arc<std::sync::Mutex<String>>) -> String {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let name = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut read = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            if read.read_line(&mut first).is_err() {
                continue;
            }
            let mut auth = None;
            loop {
                let mut line = String::new();
                if read.read_line(&mut line).is_err() || line.trim().is_empty() {
                    break;
                }
                if let Some((k, v)) = line.split_once(':')
                    && k.eq_ignore_ascii_case("authorization")
                {
                    auth = Some(v.trim().to_owned());
                }
            }
            // "bot:hunter2"
            let reply = if auth.as_deref() != Some("Basic Ym90Omh1bnRlcjI=") {
                "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"r\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned()
            } else if first.starts_with("GET /v2/shop/app/manifests/") {
                let body = served.lock().unwrap().clone();
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.oci.image.manifest.v1+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned()
            };
            let _ = stream.write_all(reply.as_bytes());
            let _ = stream.flush();
        }
    });
    name
}

/// A manifest that differs by `v`, and its digest.
fn manifest(v: &str) -> (String, String) {
    let body = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", "c".repeat(64)),
            "size": 2,
        },
        "layers": [],
        "annotations": { "v": v },
    })
    .to_string();
    let digest = legion_image::sha256(body.as_bytes());
    (body, digest)
}

/// Each task made, as the env its pod has and the pin it runs.
type Made = [(String, String)];

/// What a deployment runs is the digest its tag named then, on every task:
/// scaling keeps it after the tag moves, an update takes the tag's new
/// digest, and a rollback the old one again. The login stays out of all of
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_runs_what_its_tags_named_then() {
    use legion_api::{Kind, Object as Stored};
    let (one, d1) = manifest("1");
    let (two, d2) = manifest("2");
    let served = std::sync::Arc::new(std::sync::Mutex::new(one));
    let reg = registry(served.clone());
    let image = format!("{reg}/shop/app:1");
    let pinned = |d: &str| format!("{reg}/shop/app@{d}");

    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let mut node = Stored::new(Kind::Node, "", "w1", serde_json::json!({}));
    node.status = serde_json::json!({ "state": "ready", "imagePins": true });
    m.store
        .raft
        .client_write(legion_store::Command::Put {
            object: node,
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    let login =
        format!(r#"{{"auths": {{"{reg}": {{"username": "bot", "password": "hunter2"}}}}}}"#);
    apply_json(&m, "Namespace", "shop", serde_json::json!({})).await;
    let secret = Object {
        kind: "Secret".into(),
        namespace: "shop".into(),
        name: "login".into(),
        spec_json: serde_json::json!({ "text": login })
            .to_string()
            .into_bytes(),
        ..Default::default()
    };
    let objects = m.objects().await;
    objects.clone().apply(apply(secret)).await.unwrap();
    let web = |replicas: u32, env: &str| {
        let mut container = serde_json::json!({ "name": "a", "image": image });
        if !env.is_empty() {
            container["env"] = serde_json::json!([{ "name": "E", "value": env }]);
        }
        Object {
            kind: "Service".into(),
            namespace: "shop".into(),
            name: "web".into(),
            spec_json: serde_json::json!({
                "replicas": replicas,
                "update": { "order": "start-first" },
                "pod": { "imagePullSecrets": ["login"], "containers": [container] },
            })
            .to_string()
            .into_bytes(),
            ..Default::default()
        }
    };
    // The pins of every task made, by the env their pod has: a pin is fixed
    // when a task is made, and with no agent the node goes down and they
    // are shut down.
    let tasks = || -> Vec<(String, String)> {
        let mut v: Vec<_> = m
            .store
            .list(Kind::Task, None)
            .unwrap()
            .objects
            .into_iter()
            .map(|t| {
                let env = t.spec["pod"]["containers"][0]["env"][0]["value"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let pin = t.spec["images"][&image]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                (env, pin)
            })
            .collect();
        v.sort();
        v
    };
    let until = |what: &str, want: &dyn Fn(&Made) -> bool| {
        let what = what.to_owned();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let now = tasks();
            if want(&now) {
                return now;
            }
            assert!(std::time::Instant::now() < deadline, "{what}: {now:?}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    objects.clone().apply(apply(web(2, ""))).await.unwrap();
    let all = |env: &'static str, pin: String, n: usize| {
        move |t: &[(String, String)]| {
            let made: Vec<_> = t.iter().filter(|(e, _)| e == env).collect();
            made.len() >= n && made.iter().all(|(_, p)| *p == pin)
        }
    };
    tokio::task::block_in_place(|| until("deployed", &all("", pinned(&d1), 2)));

    // The tag moves; scaling deploys nothing.
    *served.lock().unwrap() = two;
    objects.clone().apply(apply(web(3, ""))).await.unwrap();
    tokio::task::block_in_place(|| until("scaled", &all("", pinned(&d1), 3)));

    // A new pod runs what the tag names now.
    objects.clone().apply(apply(web(3, "x"))).await.unwrap();
    let new = pinned(&d2);
    tokio::task::block_in_place(|| {
        until("updated", &|t| t.iter().any(|(e, p)| e == "x" && *p == new))
    });

    // Rolled back: the old pod's digest again, though the tag names the new.
    // (With no agents its old tasks are current again, and none is made.)
    let key = legion_api::ObjectKey::new(Kind::Service, "shop", "web");
    let resolved = || {
        m.store.get(&key).unwrap().unwrap().status["images"]["resolved"][&image]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    assert_eq!(resolved(), new);
    objects.clone().apply(apply(web(3, ""))).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while resolved() != pinned(&d1) {
        assert!(
            std::time::Instant::now() < deadline,
            "rolled back: {}",
            resolved()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let status = m.store.get(&key).unwrap().unwrap().status.to_string();
    assert!(!status.contains("hunter2"), "{status}");
    for t in m.store.list(Kind::Task, None).unwrap().objects {
        assert!(!t.spec.to_string().contains("hunter2"));
    }
    m.stop().await;
}

/// A Volume that changes while it is deleted is checked again: a change that
/// doesn't matter is no reason to fail, and a node's claim still refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_changed_during_its_delete_is_checked_again() {
    use legion_api::{Kind, Object as Stored, ObjectKey};
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let lun = |target: &str| {
        serde_json::json!({
            "driver": "iscsi",
            "iscsi": { "portal": "192.0.2.5", "target": target, "lun": 0 },
        })
    };
    let put = |object: Stored| {
        let raft = m.store.raft.clone();
        async move {
            raft.client_write(legion_store::Command::Put {
                object,
                precondition: legion_store::Precondition::None,
            })
            .await
            .unwrap();
        }
    };
    let status = |name: &str, status: serde_json::Value| {
        let key = ObjectKey::new(Kind::Volume, "default", name);
        let mut v = m.store.get(&key).unwrap().unwrap();
        v.status = status;
        v
    };
    let objects = m.objects().await;
    let delete = |name: &str| {
        let mut objects = objects.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            objects
                .delete(DeleteRequest {
                    r#ref: Some(ObjectRef {
                        kind: "Volume".into(),
                        namespace: "default".into(),
                        name,
                    }),
                    expected_version: None,
                })
                .await
        })
    };
    put(Stored::new(Kind::Node, "", "w1", serde_json::json!({}))).await;

    // A change that holds nothing: the delete goes through.
    apply_json(&m, "Volume", "free", lun("iqn.2026-11.test:a")).await;
    let mut paused = legion_manager::hooks::arm("volume-delete", "free");
    let deleting = delete("free");
    paused.reached().await;
    put(status("free", serde_json::json!({}))).await;
    paused.release();
    deleting.await.unwrap().unwrap();

    // A node's claim: refused for that, and the Volume stays.
    apply_json(&m, "Volume", "claimed", lun("iqn.2026-11.test:b")).await;
    let mut paused = legion_manager::hooks::arm("volume-delete", "claimed");
    let deleting = delete("claimed");
    paused.reached().await;
    put(status("claimed", serde_json::json!({ "node": "w1" }))).await;
    paused.release();
    let err = deleting.await.unwrap().unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("held by node w1"), "{err}");
    let key = ObjectKey::new(Kind::Volume, "default", "claimed");
    assert!(m.store.get(&key).unwrap().is_some());
    m.stop().await;
}

/// A Volume in use is not deleted: it would come back unheld, for another
/// node to attach while the first still writes to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_volume_in_use_is_not_deleted() {
    use legion_api::{Kind, Object as Stored, ObjectKey};
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let objects = m.objects().await;
    let delete = |name: &str| {
        let mut objects = objects.clone();
        let name = name.to_owned();
        async move {
            objects
                .delete(DeleteRequest {
                    r#ref: Some(ObjectRef {
                        kind: "Volume".into(),
                        namespace: "default".into(),
                        name,
                    }),
                    expected_version: None,
                })
                .await
        }
    };
    let put = |object: Stored| {
        let raft = m.store.raft.clone();
        async move {
            raft.client_write(legion_store::Command::Put {
                object,
                precondition: legion_store::Precondition::None,
            })
            .await
            .unwrap();
        }
    };
    let hold = |name: &str, node: &str| {
        let key = ObjectKey::new(Kind::Volume, "default", name);
        let mut v = m.store.get(&key).unwrap().unwrap();
        v.status = serde_json::json!({ "node": node });
        v
    };
    let lun = serde_json::json!({
        "driver": "iscsi",
        "iscsi": { "portal": "192.0.2.5", "target": "iqn.2026-11.test:a", "lun": 0 },
    });

    // A LUN held by a node of the cluster stays.
    put(Stored::new(Kind::Node, "", "w1", serde_json::json!({}))).await;
    apply_json(&m, "Volume", "lun", lun.clone()).await;
    put(hold("lun", "w1")).await;
    let err = delete("lun").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("held by node w1"), "{err}");
    let key = ObjectKey::new(Kind::Volume, "default", "lun");
    assert!(m.store.get(&key).unwrap().is_some());

    // Once that node is removed, it may go.
    m.store
        .raft
        .client_write(legion_store::Command::Delete {
            key: ObjectKey::new(Kind::Node, "", "w1"),
            precondition: legion_store::Precondition::None,
        })
        .await
        .unwrap();
    delete("lun").await.unwrap();

    // A ZFS volume keeps its node, and goes once nothing mounts it.
    apply_json(&m, "Volume", "data", serde_json::json!({})).await;
    put(Stored::new(Kind::Node, "", "w2", serde_json::json!({}))).await;
    put(hold("data", "w2")).await;
    delete("data").await.unwrap();

    // Mounted by a task that has not ended, no volume goes.
    apply_json(&m, "Volume", "lun", lun).await;
    apply_json(
        &m,
        "Service",
        "db",
        serde_json::json!({
            "replicas": 1,
            "pod": {
                "containers": [{
                    "name": "db", "image": "db:1",
                    "volumeMounts": [{ "name": "data", "path": "/data" }],
                }],
                "volumes": [{ "name": "data", "volume": { "name": "lun" } }],
            },
        }),
    )
    .await;
    until("its task", || {
        !m.store
            .list(Kind::Task, Some("default"))
            .unwrap()
            .objects
            .is_empty()
    })
    .await;
    let err = delete("lun").await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    assert!(err.message().contains("mounted by task"), "{err}");
    m.stop().await;
}

/// Two Volumes naming one LUN, in two namespaces, are never held by two
/// nodes at once: each would mount the same UFS. A second one is refused,
/// and one already there (from before the check) waits for the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lun_two_volumes_name_is_held_by_one_node() {
    use legion_api::dispatch::VolumeClaim;
    use legion_api::{Kind, Object as Stored, ObjectKey};
    let m = Running::start_steady().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let d = DispatcherClient::new(m.channel(None).await.unwrap());
    for node in ["w1", "w2"] {
        let d = d.clone();
        eventually(move || {
            let mut d = d.clone();
            async move {
                d.register(RegisterRequest {
                    node: node.into(),
                    status_json: br#"{"state":"ready"}"#.to_vec(),
                    ..Default::default()
                })
                .await
            }
        })
        .await;
    }
    let put = |object: Stored| {
        let raft = m.store.raft.clone();
        async move {
            raft.client_write(legion_store::Command::Put {
                object,
                precondition: legion_store::Precondition::None,
            })
            .await
            .unwrap();
        }
    };
    let lun = |portal: &str, target: &str| {
        serde_json::json!({
            "driver": "iscsi",
            "iscsi": { "portal": portal, "target": target, "lun": 0 },
        })
    };
    let objects = m.objects().await;
    objects
        .clone()
        .apply(object("Namespace", "", "other", serde_json::json!({})))
        .await
        .unwrap();
    apply_json(&m, "Volume", "a", lun("192.0.2.5", "iqn.2026-11.test:shop")).await;

    // Another Volume for the same LUN, by another portal and in another
    // namespace, is refused, and says nothing of the first.
    let err = objects
        .clone()
        .apply(object(
            "Volume",
            "other",
            "b",
            lun("san.test:3260", "iqn.2026-11.TEST:Shop"),
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err}");
    assert!(!err.message().contains("default/a"), "{err}");

    // One there already, as before the check, and a task on each node
    // using one of the two.
    put(Stored::new(
        Kind::Volume,
        "other",
        "b",
        lun("san.test:3260", "iqn.2026-11.TEST:Shop"),
    ))
    .await;
    for (namespace, task, node, volume) in
        [("default", "ta", "w1", "a"), ("other", "tb", "w2", "b")]
    {
        let mut t = Stored::new(
            Kind::Task,
            namespace,
            task,
            serde_json::json!({
                "service": task,
                "serviceUid": "0000000000000000",
                "serviceGeneration": 1,
                "slot": 1,
                "node": node,
                "pod": {
                    "containers": [{
                        "name": "db", "image": "db:1",
                        "volumeMounts": [{ "name": "data", "path": "/data" }],
                    }],
                    "volumes": [{ "name": "data", "volume": { "name": volume } }],
                },
            }),
        );
        t.status = serde_json::json!({ "state": "preparing" });
        put(t).await;
    }
    let claim = |node: &str, namespace: &str, name: &str, release: bool| {
        let mut d = d.clone();
        let (node, namespace, name) = (node.to_owned(), namespace.to_owned(), name.to_owned());
        async move {
            let r = d
                .claim_volume(ClaimVolumeRequest {
                    node,
                    namespace,
                    name,
                    dataset: String::new(),
                    release,
                })
                .await
                .unwrap()
                .into_inner();
            serde_json::from_slice::<VolumeClaim>(&r.claim_json).unwrap()
        }
    };
    let holder = |namespace: &str, name: &str| {
        m.store
            .get(&ObjectKey::new(Kind::Volume, namespace, name))
            .unwrap()
            .unwrap()
            .status
            .get("node")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_owned()
    };

    assert!(matches!(
        claim("w1", "default", "a", false).await,
        VolumeClaim::Claimed(_)
    ));
    let second = claim("w2", "other", "b", false).await;
    assert!(
        matches!(&second, VolumeClaim::Elsewhere(n) if n == "w1"),
        "{second:?}"
    );
    assert_eq!(holder("other", "b"), "", "the refused claim is undone");
    // The holder claims again, as after an agent restart: still its.
    assert!(matches!(
        claim("w1", "default", "a", false).await,
        VolumeClaim::Claimed(_)
    ));

    // Let go of, the LUN may be the other's.
    claim("w1", "default", "a", true).await;
    assert!(matches!(
        claim("w2", "other", "b", false).await,
        VolumeClaim::Claimed(_)
    ));
    let first = claim("w1", "default", "a", false).await;
    assert!(
        matches!(&first, VolumeClaim::Elsewhere(n) if n == "w2"),
        "{first:?}"
    );
    assert_eq!(holder("default", "a"), "");

    m.stop().await;
}

/// Two nodes claiming two Volumes of one LUN at once: one holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aliases_claimed_at_once_are_held_once() {
    use legion_api::dispatch::VolumeClaim;
    use legion_api::{Kind, Object as Stored};
    let m = Running::start_steady().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let d = DispatcherClient::new(m.channel(None).await.unwrap());
    let nodes: Vec<String> = (1..=4).map(|n| format!("w{n}")).collect();
    for node in &nodes {
        let d = d.clone();
        let node = node.clone();
        eventually(move || {
            let mut d = d.clone();
            let node = node.clone();
            async move {
                d.register(RegisterRequest {
                    node,
                    status_json: br#"{"state":"ready"}"#.to_vec(),
                    ..Default::default()
                })
                .await
            }
        })
        .await;
    }
    for (i, node) in nodes.iter().enumerate() {
        let volume = format!("v{i}");
        for object in [
            Stored::new(
                Kind::Volume,
                "default",
                &volume,
                serde_json::json!({
                    "driver": "iscsi",
                    "iscsi": { "portal": "192.0.2.5", "target": "iqn.2026-11.test:one", "lun": 3 },
                }),
            ),
            Stored {
                status: serde_json::json!({ "state": "preparing" }),
                ..Stored::new(
                    Kind::Task,
                    "default",
                    &format!("t{i}"),
                    serde_json::json!({
                            "service": format!("t{i}"),
                            "serviceUid": "0000000000000000",
                    "serviceGeneration": 1,
                            "slot": 1,
                            "node": node,
                            "pod": {
                                "containers": [{ "name": "db", "image": "db:1" }],
                                "volumes": [{ "name": "data", "volume": { "name": volume } }],
                            },
                        }),
                )
            },
        ] {
            m.store
                .raft
                .client_write(legion_store::Command::Put {
                    object,
                    precondition: legion_store::Precondition::None,
                })
                .await
                .unwrap();
        }
    }
    for _ in 0..5 {
        let mut claims = tokio::task::JoinSet::new();
        for (i, node) in nodes.iter().enumerate() {
            let mut d = d.clone();
            let node = node.clone();
            claims.spawn(async move {
                let r = d
                    .claim_volume(ClaimVolumeRequest {
                        node,
                        namespace: "default".into(),
                        name: format!("v{i}"),
                        dataset: String::new(),
                        release: false,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                (
                    i,
                    serde_json::from_slice::<VolumeClaim>(&r.claim_json).unwrap(),
                )
            });
        }
        let got = claims.join_all().await;
        let held: Vec<usize> = got
            .iter()
            .filter(|(_, c)| matches!(c, VolumeClaim::Claimed(_)))
            .map(|(i, _)| *i)
            .collect();
        assert_eq!(held.len(), 1, "{got:?}");
        // Let the holder go for the next round.
        let i = held[0];
        d.clone()
            .claim_volume(ClaimVolumeRequest {
                node: nodes[i].clone(),
                namespace: "default".into(),
                name: format!("v{i}"),
                dataset: String::new(),
                release: true,
            })
            .await
            .unwrap();
    }
    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_is_routed_by_one_namespace() {
    let m = Running::start().await;
    m.cluster().await.init(InitRequest {}).await.unwrap();
    let mut objects = m.objects().await;
    let route = |namespace: &str, name: &str, host: &str| {
        object(
            "Route",
            namespace,
            name,
            serde_json::json!({ "host": host, "service": "web" }),
        )
    };
    objects
        .apply(route("shop", "www", "shop.example.org"))
        .await
        .unwrap();
    // Its own namespace may route it again, on another path too.
    objects
        .apply(object(
            "Route",
            "shop",
            "api",
            serde_json::json!({ "host": "shop.example.org", "path": "/api", "service": "api" }),
        ))
        .await
        .unwrap();
    // Another may not: not the host, nor a `*.` host over it, nor a host
    // under one of theirs.
    for host in ["shop.example.org", "*.example.org"] {
        let err = objects.apply(route("evil", "www", host)).await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
        assert!(err.message().contains("namespace shop"), "{err}");
    }
    objects
        .apply(route("shop", "any", "*.example.net"))
        .await
        .unwrap();
    let err = objects
        .apply(route("evil", "www", "www.example.net"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    objects
        .apply(route("evil", "www", "evil.example.org"))
        .await
        .unwrap();
    m.stop().await;
}
