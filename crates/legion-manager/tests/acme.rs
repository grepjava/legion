//! Route certificates from an ACME authority: a manager against a small
//! authority of the test's own, which validates HTTP-01 challenges by what
//! the routes' status hands the ingress proxies.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use legion_api::route::{RouteSpec, RouteStatus};
use legion_api::{Kind, ObjectKey};
use legion_manager::{Manager, ManagerConfig, default_raft_config};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{ApplyRequest, DeleteRequest, InitRequest, Object, ObjectRef};
use legion_store::Store;
use rcgen::{CertificateParams, Issuer, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
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
            raft: openraft::Config {
                heartbeat_interval: 50,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..default_raft_config()
            },
            // The authority the tests run is on the loopback.
            acme_loopback: true,
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
        let m = Running {
            addr,
            store,
            stop: Some(stop),
            task,
            _dir: dir,
        };
        let channel = m.channel().await;
        ClusterClient::new(channel)
            .init(InitRequest {})
            .await
            .unwrap();
        m
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    async fn apply_route(&self, name: &str, spec: Value) {
        let object = Object {
            kind: "Route".into(),
            name: name.into(),
            spec_json: spec.to_string().into_bytes(),
            ..Default::default()
        };
        ObjectsClient::new(self.channel().await)
            .apply(ApplyRequest {
                object: Some(object),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    fn secret(&self, namespace: &str, name: &str) -> Option<String> {
        let o = self
            .store
            .get(&ObjectKey::new(Kind::Secret, namespace, name))
            .unwrap()?;
        let spec: legion_api::data::DataSpec = serde_json::from_value(o.spec).unwrap();
        Some(String::from_utf8(spec.bytes().unwrap()).unwrap())
    }

    fn status(&self, route: &str) -> RouteStatus {
        let o = self
            .store
            .get(&ObjectKey::new(Kind::Route, "default", route))
            .unwrap()
            .unwrap();
        serde_json::from_value(o.status).unwrap_or_default()
    }

    async fn stop(mut self) {
        drop(self.store);
        self.stop.take().unwrap().send(()).unwrap();
        self.task.await.unwrap();
    }
}

/// What the proxies would answer for `token` on `host`: a route of the
/// host's challenge, as the dispatcher hands it to them.
fn served(store: &Store, host: &str, token: &str) -> Option<String> {
    store
        .list(Kind::Route, None)
        .unwrap()
        .objects
        .into_iter()
        .filter(|o| legion_api::parse::<RouteSpec>(&o.spec).is_ok_and(|s| s.host == host))
        .find_map(|o| {
            let status: RouteStatus = serde_json::from_value(o.status).ok()?;
            status.acme?.challenges.get(token).cloned()
        })
}

type Answers = Box<dyn Fn(&str, &str) -> Option<String> + Send + Sync>;

/// A small ACME authority (RFC 8555): it trusts what it is sent without
/// checking signatures, checks each HTTP-01 answer with `answers` when
/// told the challenge is ready, and signs with a CA of its own.
struct Authority {
    base: String,
    issuer: Issuer<'static, KeyPair>,
    ca_pem: String,
    answers: Answers,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    nonce: u64,
    accounts: u32,
    thumbprint: String,
    /// How long the certificates it issues last.
    lifetime: Duration,
    orders: Vec<FakeOrder>,
}

/// Each authorization's: validated, or failed with why; none yet.
type Checked = Vec<Option<Result<(), String>>>;

struct FakeOrder {
    hosts: Vec<String>,
    tokens: Vec<String>,
    checked: Checked,
    cert: Option<String>,
}

impl Authority {
    async fn start(answers: Answers) -> Arc<Authority> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let ca = legion_pki::Ca::generate().unwrap();
        let issuer =
            Issuer::from_ca_cert_pem(ca.cert_pem(), KeyPair::from_pem(ca.key_pem()).unwrap())
                .unwrap();
        let authority = Arc::new(Authority {
            base,
            issuer,
            ca_pem: ca.cert_pem().to_owned(),
            answers,
            state: Mutex::new(State {
                lifetime: Duration::from_secs(90 * 86_400),
                ..Default::default()
            }),
        });
        let serving = authority.clone();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                let authority = serving.clone();
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(move |req| {
                        let authority = authority.clone();
                        async move { Ok::<_, Infallible>(authority.handle(req).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(s), svc)
                        .await;
                });
            }
        });
        authority
    }

    fn directory(&self) -> String {
        format!("{}/dir", self.base)
    }

    fn set_lifetime(&self, lifetime: Duration) {
        self.state.lock().unwrap().lifetime = lifetime;
    }

    fn orders(&self) -> Vec<(Vec<String>, Checked)> {
        let state = self.state.lock().unwrap();
        state
            .orders
            .iter()
            .map(|o| (o.hosts.clone(), o.checked.clone()))
            .collect()
    }

    fn accounts(&self) -> u32 {
        self.state.lock().unwrap().accounts
    }

    /// How many certificates it has issued.
    fn issued(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.orders.iter().filter(|o| o.cert.is_some()).count()
    }

    async fn handle(&self, req: Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
        let path = req.uri().path().to_owned();
        let body = req.into_body().collect().await.unwrap().to_bytes();
        let mut state = self.state.lock().unwrap();
        state.nonce += 1;
        let nonce = format!("nonce-{}", state.nonce);
        let base = &self.base;
        let (status, location, value) = if path == "/dir" {
            (
                StatusCode::OK,
                None,
                json!({
                    "newNonce": format!("{base}/nonce"),
                    "newAccount": format!("{base}/account"),
                    "newOrder": format!("{base}/order"),
                }),
            )
        } else if path == "/nonce" {
            (StatusCode::OK, None, Value::Null)
        } else {
            let jws: Value = serde_json::from_slice(&body).unwrap();
            let decode = |field: &str| -> Value {
                let raw = URL_SAFE_NO_PAD
                    .decode(jws[field].as_str().unwrap())
                    .unwrap();
                match raw.is_empty() {
                    true => Value::Null,
                    false => serde_json::from_slice(&raw).unwrap(),
                }
            };
            let (protected, payload) = (decode("protected"), decode("payload"));
            let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
            match parts.as_slice() {
                ["account"] => {
                    let jwk = &protected["jwk"];
                    let canonical = format!(
                        r#"{{"crv":"{}","kty":"{}","x":"{}","y":"{}"}}"#,
                        jwk["crv"].as_str().unwrap(),
                        jwk["kty"].as_str().unwrap(),
                        jwk["x"].as_str().unwrap(),
                        jwk["y"].as_str().unwrap(),
                    );
                    state.thumbprint = URL_SAFE_NO_PAD.encode(Sha256::digest(canonical));
                    state.accounts += 1;
                    let id = format!("{base}/acct/{}", state.accounts);
                    (StatusCode::CREATED, Some(id), json!({"status": "valid"}))
                }
                ["order"] => {
                    let hosts: Vec<String> = payload["identifiers"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|i| i["value"].as_str().unwrap().to_owned())
                        .collect();
                    let n = state.orders.len();
                    state.orders.push(FakeOrder {
                        tokens: (0..hosts.len()).map(|i| format!("token-{n}-{i}")).collect(),
                        checked: vec![None; hosts.len()],
                        hosts,
                        cert: None,
                    });
                    let url = format!("{base}/order/{n}");
                    (StatusCode::CREATED, Some(url), self.order(&state, n))
                }
                ["order", n] => (StatusCode::OK, None, self.order(&state, n.parse().unwrap())),
                ["authz", n, i] => {
                    let (n, i) = (n.parse().unwrap(), i.parse().unwrap());
                    (StatusCode::OK, None, self.authorization(&state, n, i))
                }
                ["chall", n, i] => {
                    let (n, i): (usize, usize) = (n.parse().unwrap(), i.parse().unwrap());
                    let order = &state.orders[n];
                    let (host, token) = (order.hosts[i].clone(), order.tokens[i].clone());
                    let expected = format!("{token}.{}", state.thumbprint);
                    let checked = match (self.answers)(&host, &token) {
                        Some(a) if a == expected => Ok(()),
                        Some(a) => Err(format!("{host} answered {a:?}")),
                        None => Err(format!("{host} did not answer")),
                    };
                    state.orders[n].checked[i] = Some(checked);
                    // Validation is reported later, as authorities do.
                    let mut c = self.challenge(&state, n, i);
                    c["status"] = json!("processing");
                    c.as_object_mut().unwrap().remove("error");
                    (StatusCode::OK, None, c)
                }
                ["finalize", n] => {
                    let n: usize = n.parse().unwrap();
                    let der = URL_SAFE_NO_PAD
                        .decode(payload["csr"].as_str().unwrap())
                        .unwrap();
                    let csr =
                        rcgen::CertificateSigningRequestParams::from_der(&der.into()).unwrap();
                    let mut params = CertificateParams::new(state.orders[n].hosts.clone()).unwrap();
                    let now = SystemTime::now();
                    params.not_before = now.into();
                    params.not_after = (now + state.lifetime).into();
                    let cert = params.signed_by(&csr.public_key, &self.issuer).unwrap();
                    state.orders[n].cert = Some(format!("{}{}", cert.pem(), self.ca_pem));
                    (StatusCode::OK, None, self.order(&state, n))
                }
                ["cert", n] => {
                    let n: usize = n.parse().unwrap();
                    let pem = state.orders[n].cert.clone().unwrap();
                    return Response::builder()
                        .header("replay-nonce", nonce)
                        .header("content-type", "application/pem-certificate-chain")
                        .body(Full::new(Bytes::from(pem)))
                        .unwrap();
                }
                other => panic!("unexpected request {other:?}"),
            }
        };
        let mut r = Response::builder()
            .status(status)
            .header("replay-nonce", nonce)
            .header("content-type", "application/json");
        if let Some(location) = location {
            r = r.header("location", location);
        }
        let body = match value {
            Value::Null => Bytes::new(),
            v => Bytes::from(v.to_string()),
        };
        r.body(Full::new(body)).unwrap()
    }

    fn order(&self, state: &State, n: usize) -> Value {
        let o = &state.orders[n];
        let failed = o.checked.iter().any(|c| matches!(c, Some(Err(_))));
        let valid = o.checked.iter().all(|c| matches!(c, Some(Ok(()))));
        let status = match (failed, valid, o.cert.is_some()) {
            (true, _, _) => "invalid",
            (_, true, true) => "valid",
            (_, true, false) => "ready",
            _ => "pending",
        };
        let base = &self.base;
        let mut order = json!({
            "status": status,
            "identifiers": o.hosts.iter().map(|h| json!({"type": "dns", "value": h})).collect::<Vec<_>>(),
            "authorizations": (0..o.hosts.len()).map(|i| format!("{base}/authz/{n}/{i}")).collect::<Vec<_>>(),
            "finalize": format!("{base}/finalize/{n}"),
        });
        if o.cert.is_some() {
            order["certificate"] = json!(format!("{base}/cert/{n}"));
        }
        order
    }

    fn challenge(&self, state: &State, n: usize, i: usize) -> Value {
        let o = &state.orders[n];
        let mut c = json!({
            "type": "http-01",
            "url": format!("{}/chall/{n}/{i}", self.base),
            "token": o.tokens[i],
            "status": match &o.checked[i] {
                None => "pending",
                Some(Ok(())) => "valid",
                Some(Err(_)) => "invalid",
            },
        });
        if let Some(Err(why)) = &o.checked[i] {
            c["error"] = json!({
                "type": "urn:ietf:params:acme:error:incorrectResponse",
                "detail": why,
            });
        }
        c
    }

    fn authorization(&self, state: &State, n: usize, i: usize) -> Value {
        let o = &state.orders[n];
        let challenge = self.challenge(state, n, i);
        json!({
            "identifier": {"type": "dns", "value": o.hosts[i]},
            "status": challenge["status"],
            "challenges": [challenge],
        })
    }
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn route(host: &str, path: &str, directory: &str) -> Value {
    json!({
        "host": host,
        "path": path,
        "service": "web",
        "tls": {
            "secret": "shop-cert",
            "acme": {"directory": directory, "email": "ops@example.org"},
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_get_their_certificates_and_renew_them() {
    let store: Arc<Mutex<Option<Store>>> = Default::default();
    let looked = store.clone();
    let authority = Authority::start(Box::new(move |host, token| {
        served(looked.lock().unwrap().as_ref()?, host, token)
    }))
    .await;
    let m = Running::start().await;
    *store.lock().unwrap() = Some(m.store.clone());
    let dir = authority.directory();

    // Two routes naming one Secret share a certificate for both hosts.
    m.apply_route("www", route("shop.example.org", "/", &dir))
        .await;
    m.apply_route("api", route("api.example.org", "/api", &dir))
        .await;
    until("a certificate for both hosts", || {
        m.secret("default", "shop-cert").is_some_and(|pem| {
            legion_pki::dns_names(&pem).unwrap() == ["api.example.org", "shop.example.org"]
        })
    })
    .await;
    let pem = m.secret("default", "shop-cert").unwrap();
    assert!(pem.contains("-----BEGIN PRIVATE KEY-----"), "{pem}");
    let orders = authority.orders();
    let both = vec!["api.example.org".to_owned(), "shop.example.org".to_owned()];
    // The first route alone may have been ordered for before the second
    // was applied.
    let last = orders.last().unwrap();
    assert_eq!(last, &(both.clone(), vec![Some(Ok(())), Some(Ok(()))]));
    until("statuses saying when it expires", || {
        ["www", "api"].iter().all(|r| {
            m.status(r).acme.is_some_and(|a| {
                a.not_after.is_some() && a.challenges.is_empty() && a.error.is_none()
            })
        })
    })
    .await;
    let account = m
        .store
        .list(Kind::Secret, Some("legion-system"))
        .unwrap()
        .objects
        .into_iter()
        .filter(|o| o.metadata.name.starts_with("acme-account-"))
        .count();
    assert_eq!(account, 1);

    // A host the certificate lacks has it ordered again; one past two
    // thirds of its life is renewed.
    let before = authority.orders().len();
    authority.set_lifetime(Duration::from_secs(3));
    m.apply_route("blog", route("blog.example.org", "/", &dir))
        .await;
    until("a new order, then a renewal", || {
        authority.orders().len() >= before + 2
    })
    .await;
    authority.set_lifetime(Duration::from_secs(90 * 86_400));
    until("a lasting certificate for the three", || {
        m.secret("default", "shop-cert").is_some_and(|pem| {
            legion_pki::dns_names(&pem).unwrap().len() == 3
                && legion_pki::validity(&pem).unwrap().1 > Duration::from_secs(86_400)
        })
    })
    .await;
    for (hosts, checked) in &authority.orders()[before..] {
        assert_eq!(hosts.len(), 3);
        assert!(checked.iter().all(|c| c == &Some(Ok(()))), "{checked:?}");
    }
    assert_eq!(authority.accounts(), 1, "the account is kept");
    store.lock().unwrap().take();
    m.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_orders_are_tried_again_later() {
    // Nothing answers the challenges.
    let authority = Authority::start(Box::new(|_, _| None)).await;
    let m = Running::start().await;
    m.apply_route(
        "www",
        route("shop.example.org", "/", &authority.directory()),
    )
    .await;
    until("a failure in the status", || {
        m.status("www").acme.is_some_and(|a| a.failures == 1)
    })
    .await;
    let acme = m.status("www").acme.unwrap();
    let error = acme.error.unwrap();
    assert!(
        error.starts_with("the order is Invalid: shop.example.org: ")
            && error.contains("shop.example.org did not answer"),
        "{error}"
    );
    assert!(acme.challenges.is_empty());
    let retry = legion_api::clock::parse(&acme.retry_at.unwrap()).unwrap();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!((now + 590..=now + 600).contains(&retry), "{retry} {now}");
    assert!(m.secret("default", "shop-cert").is_none());

    // A Secret holding something else is left alone, and nothing ordered.
    let secret = Object {
        kind: "Secret".into(),
        name: "db-password".into(),
        spec_json: br#"{"text": "hunter2"}"#.to_vec(),
        ..Default::default()
    };
    ObjectsClient::new(m.channel().await)
        .apply(ApplyRequest {
            object: Some(secret),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut db = route("db.example.org", "/", &authority.directory());
    db["tls"]["secret"] = json!("db-password");
    m.apply_route("db", db).await;
    until("the Secret refused", || {
        m.status("db").acme.and_then(|a| a.error)
            == Some(
                "Secret default/db-password holds no certificate, so it is not written over".into(),
            )
    })
    .await;
    assert_eq!(m.secret("default", "db-password").unwrap(), "hunter2");

    // Not tried again before then.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(authority.orders().len(), 1);
    m.stop().await;
}

/// Held while a challenge is checked: says so, then waits to be let go.
type Hold = Arc<Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>>;

/// Arms `hold` for the next challenge checked: returns when it is being
/// checked, and what lets it go.
fn arm(hold: &Hold) -> (impl Future<Output = ()>, std::sync::mpsc::Sender<()>) {
    let (asked_tx, asked) = std::sync::mpsc::channel();
    let (go, go_rx) = std::sync::mpsc::channel();
    *hold.lock().unwrap() = Some((asked_tx, go_rx));
    let asked = async move {
        tokio::task::spawn_blocking(move || asked.recv_timeout(Duration::from_secs(20)))
            .await
            .unwrap()
            .expect("a challenge checked");
    };
    (asked, go)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_changes_during_an_order_is_not_written_over() {
    let store: Arc<Mutex<Option<Store>>> = Default::default();
    let hold: Hold = Default::default();
    let (looked, held) = (store.clone(), hold.clone());
    let authority = Authority::start(Box::new(move |host, token| {
        let answer = served(looked.lock().unwrap().as_ref()?, host, token);
        if let Some((asked, go)) = held.lock().unwrap().take() {
            asked.send(()).unwrap();
            let _ = go.recv_timeout(Duration::from_secs(20));
        }
        answer
    }))
    .await;
    let m = Running::start().await;
    *store.lock().unwrap() = Some(m.store.clone());
    let dir = authority.directory();
    let objects = ObjectsClient::new(m.channel().await);

    // A Secret written while the certificate is ordered stays as written.
    let (asked, go) = arm(&hold);
    m.apply_route("www", route("shop.example.org", "/", &dir))
        .await;
    asked.await;
    objects
        .clone()
        .apply(ApplyRequest {
            object: Some(Object {
                kind: "Secret".into(),
                name: "shop-cert".into(),
                spec_json: br#"{"text": "hunter2"}"#.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    go.send(()).unwrap();
    until("the certificate issued", || authority.issued() == 1).await;
    until("the Secret left alone", || {
        m.status("www")
            .acme
            .and_then(|a| a.error)
            .is_some_and(|e| e.contains("so it is not written over"))
    })
    .await;
    assert_eq!(m.secret("default", "shop-cert").unwrap(), "hunter2");

    // Nor is one written for a route gone while it was ordered.
    let (asked, go) = arm(&hold);
    let mut blog = route("blog.example.org", "/", &dir);
    blog["tls"]["secret"] = json!("blog-cert");
    m.apply_route("blog", blog).await;
    asked.await;
    objects
        .clone()
        .delete(DeleteRequest {
            r#ref: Some(ObjectRef {
                kind: "Route".into(),
                namespace: "default".into(),
                name: "blog".into(),
            }),
            expected_version: None,
        })
        .await
        .unwrap();
    go.send(()).unwrap();
    until("the certificate issued", || authority.issued() == 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(m.secret("default", "blog-cert").is_none());

    store.lock().unwrap().take();
    m.stop().await;
}
