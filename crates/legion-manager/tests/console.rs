//! The web console's gateway (DESIGN.md §3.9), over real HTTPS: signing in
//! with a code from `legion console login`, calls made as the signed-in
//! user and checked by the API as the CLI's are, and signing out.

use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use legion_manager::{Console, Manager, ManagerConfig, Tls, client_config};
use legion_pki::{CERT_LIFETIME, Ca, Identity, Role};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{ApplyRequest, ConsoleCodeRequest, InitRequest, Object};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::ServerName;
use tonic::transport::{Channel, ClientTlsConfig};

struct Response {
    status: u16,
    cookie: Option<String>,
    body: serde_json::Value,
    text: String,
}

/// One HTTPS request to the console at `addr`, trusting the cluster CA,
/// over HTTP/1.1.
async fn https(
    ca: &Ca,
    addr: &str,
    method: &str,
    path: &str,
    cookie: Option<&str>,
    origin: Option<&str>,
    body: Option<serde_json::Value>,
) -> Response {
    request(ca, addr, false, method, path, cookie, origin, body, &[]).await
}

/// One HTTPS request, over HTTP/2 when `h2` (as browsers speak to the
/// console): the host is then the URI's authority, with no `Host` header.
#[allow(clippy::too_many_arguments)]
async fn request(
    ca: &Ca,
    addr: &str,
    h2: bool,
    method: &str,
    path: &str,
    cookie: Option<&str>,
    origin: Option<&str>,
    body: Option<serde_json::Value>,
    headers: &[(&str, &str)],
) -> Response {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots
        .add(der(ca.cert_pem()).into())
        .expect("the CA certificate");
    let mut config = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![if h2 {
        b"h2".to_vec()
    } else {
        b"http/1.1".to_vec()
    }];
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from(legion_pki::SERVER_NAME).unwrap(), tcp)
        .await
        .unwrap();
    assert_eq!(
        tls.get_ref().1.alpn_protocol(),
        Some(if h2 { &b"h2"[..] } else { &b"http/1.1"[..] }),
        "the console speaks what was asked"
    );
    let io = hyper_util::rt::TokioIo::new(tls);
    let mut req = hyper::Request::builder().method(method);
    if h2 {
        req = req.uri(format!("https://{addr}{path}"));
    } else {
        req = req.uri(path).header("host", addr);
    }
    if let Some(c) = cookie {
        req = req.header("cookie", c);
    }
    if let Some(o) = origin {
        req = req.header("origin", o);
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Full::new(Bytes::from(b.to_string()))
        }
        None => Full::new(Bytes::new()),
    };
    let req = req.body(body).unwrap();
    let res = if h2 {
        let (mut send, conn) =
            hyper::client::conn::http2::handshake(hyper_util::rt::TokioExecutor::new(), io)
                .await
                .unwrap();
        tokio::spawn(conn);
        send.send_request(req).await.unwrap()
    } else {
        let (mut send, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(conn);
        send.send_request(req).await.unwrap()
    };
    let status = res.status().as_u16();
    let cookie = res
        .headers()
        .get("set-cookie")
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    Response {
        status,
        cookie,
        body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        text,
    }
}

fn der(pem: &str) -> Vec<u8> {
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    CertificateDer::from_pem_slice(pem.as_bytes())
        .unwrap()
        .to_vec()
}

fn creds(ca: &Ca, name: &str, role: Role) -> ClientTlsConfig {
    let issued = ca.issue(&Identity::new(name, role), CERT_LIFETIME).unwrap();
    client_config(ca.cert_pem(), &issued.cert_pem, &issued.key_pem)
}

async fn channel(addr: &str, tls: &ClientTlsConfig) -> Channel {
    Channel::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls.clone())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

fn object(kind: &str, namespace: &str, name: &str, spec: serde_json::Value) -> ApplyRequest {
    ApplyRequest {
        object: Some(Object {
            kind: kind.into(),
            namespace: namespace.into(),
            name: name.into(),
            spec_json: spec.to_string().into_bytes(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A manager with the console, initialised, and the admin's channel to it.
struct Fixture {
    ca: Ca,
    addr: String,
    console_addr: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    admin: Channel,
    _dir: tempfile::TempDir,
}

async fn start() -> Fixture {
    let ca = Ca::generate().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let console_addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let node = ca
        .issue(&Identity::new("m1", Role::Manager), CERT_LIFETIME)
        .unwrap();
    let config = ManagerConfig {
        tls: Some(Tls {
            ca_pem: ca.cert_pem().into(),
            cert_pem: node.cert_pem,
            key_pem: node.key_pem,
            ca_key_pem: Some(ca.key_pem().into()),
        }),
        name: "m1".into(),
        registry_loopback: true,
        console: Some(Console {
            listen: console_addr,
            cert_pem: None,
            key_pem: None,
        }),
        ..ManagerConfig::new(dir.path(), &addr)
    };
    let manager = Manager::open(config).await.unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        manager
            .serve(listener, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let admin = channel(&addr, &creds(&ca, "admin", Role::Admin)).await;
    ClusterClient::new(admin.clone())
        .init(InitRequest {})
        .await
        .unwrap();
    Fixture {
        ca,
        addr,
        console_addr,
        stop,
        task,
        admin,
        _dir: dir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_calls_the_api_as_its_user() {
    let Fixture {
        ca,
        addr,
        console_addr,
        stop,
        task,
        admin,
        _dir,
    } = start().await;
    let mut objects = ObjectsClient::new(admin);
    for (kind, ns, name, spec) in [
        ("Namespace", "", "shop", serde_json::json!({})),
        ("Namespace", "", "bank", serde_json::json!({})),
        (
            "RoleBinding",
            "shop",
            "alice",
            serde_json::json!({ "user": "alice", "role": "view" }),
        ),
        (
            "Config",
            "shop",
            "menu",
            serde_json::json!({ "text": "soup" }),
        ),
        (
            "Config",
            "bank",
            "vault",
            serde_json::json!({ "text": "gold" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let console = console_addr.to_string();
    let call = |method: &'static str, path: &'static str, cookie: Option<String>| {
        let (ca, console) = (&ca, &console);
        async move { https(ca, console, method, path, cookie.as_deref(), None, None).await }
    };

    // Not signed in: refused, and the pages say they were not built.
    assert_eq!(call("GET", "/api/me", None).await.status, 401);
    let page = call("GET", "/", None).await;
    if !legion_console::built() {
        assert_eq!(page.status, 503);
        assert!(page.text.contains("no console pages"), "{}", page.text);
    } else {
        // The app's page for any of its paths; its files by name.
        assert_eq!(page.status, 200);
        assert!(page.text.contains("<!doctype html>"), "{}", page.text);
        let deep = call("GET", "/k/Task/shop/web-1", None).await;
        assert_eq!((deep.status, deep.text.as_str()), (200, page.text.as_str()));
        assert_eq!(call("GET", "/favicon.svg", None).await.status, 200);
    }

    // Alice's code, as `legion console login` asks for it, signs her in once.
    let alice = channel(&addr, &creds(&ca, "alice", Role::User)).await;
    let code = ClusterClient::new(alice)
        .console_code(ConsoleCodeRequest::default())
        .await
        .unwrap()
        .into_inner()
        .code;
    let login = |code: &str, h2: bool, origin: Option<String>| {
        let (ca, console) = (&ca, &console);
        let body = serde_json::json!({ "code": code });
        async move {
            request(
                ca,
                console,
                h2,
                "POST",
                "/api/login",
                None,
                origin.as_deref(),
                Some(body),
                &[],
            )
            .await
        }
    };
    for h2 in [false, true] {
        let other = login(&code, h2, Some("https://evil.example".into())).await;
        assert_eq!(other.status, 403, "another site's page: {}", other.text);
        assert!(other.text.contains("cross-origin"), "{}", other.text);
    }
    // As a browser signs in: over HTTP/2, from the console's own page.
    let signed = login(&code, true, Some(format!("https://{console}"))).await;
    assert_eq!(signed.status, 200, "{}", signed.text);
    assert_eq!(signed.body["name"], "alice");
    let set = signed.cookie.expect("a session cookie");
    for attr in ["HttpOnly", "Secure", "SameSite=Strict", "Path=/"] {
        assert!(set.contains(attr), "{set}");
    }
    let session = set.split(';').next().unwrap().to_owned();
    assert_eq!(
        login(&code, false, None).await.status,
        403,
        "a code works once"
    );

    // As alice: who she is, what she may read, and nothing more.
    let me = call("GET", "/api/me", Some(session.clone())).await;
    assert_eq!(me.status, 200, "{}", me.text);
    assert_eq!(me.body["name"], "alice");
    assert_eq!(me.body["all"], false);
    assert_eq!(me.body["namespaces"]["shop"], "view");
    let mine = call(
        "GET",
        "/api/objects/Config?namespace=shop",
        Some(session.clone()),
    )
    .await;
    assert_eq!(mine.status, 200, "{}", mine.text);
    let names: Vec<_> = mine.body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["metadata"]["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["menu"]);
    let menu = call(
        "GET",
        "/api/objects/Config/shop/menu",
        Some(session.clone()),
    )
    .await;
    // Kept as base64, as the API stores a Config.
    assert_eq!(menu.body["spec"]["data"], "c291cA==", "{}", menu.text);
    let vault = call(
        "GET",
        "/api/objects/Config/bank/vault",
        Some(session.clone()),
    )
    .await;
    assert_eq!(vault.status, 403, "{}", vault.text);
    assert!(vault.body["error"].as_str().unwrap().contains("alice"));
    let nodes = call("GET", "/api/objects/Node", Some(session.clone())).await;
    assert_eq!(nodes.status, 403, "cluster-scoped: {}", nodes.text);
    // What tasks use now: hers, never another namespace's.
    let used = call("GET", "/api/usage?namespace=shop", Some(session.clone())).await;
    assert_eq!(used.status, 200, "{}", used.text);
    assert!(used.body["nodes"].is_array(), "{}", used.text);
    let theirs = call("GET", "/api/usage?namespace=bank", Some(session.clone())).await;
    assert_eq!(theirs.status, 403, "{}", theirs.text);
    let kinds = call("GET", "/api/kinds", Some(session.clone())).await;
    assert!(kinds.body.as_array().unwrap().len() >= 13);
    let headers_page = call("GET", "/api/nothing", Some(session.clone())).await;
    assert_eq!(headers_page.status, 404);

    // Signed out: the session is gone, here and in the store.
    let out = https(
        &ca,
        &console,
        "POST",
        "/api/logout",
        Some(&session),
        None,
        None,
    )
    .await;
    assert_eq!(out.status, 204);
    assert!(out.cookie.unwrap().contains("Max-Age=0"));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(call("GET", "/api/me", Some(session)).await.status, 401);

    let _ = stop.send(());
    task.await.unwrap();
}

/// Signs `user` in to the console as `legion console login` would: the
/// session's cookie and its token for writes.
async fn sign_in(f: &Fixture, user: &str) -> (String, String) {
    sign_in_as(f, user, Role::User).await
}

async fn sign_in_as(f: &Fixture, user: &str, role: Role) -> (String, String) {
    let console = f.console_addr.to_string();
    let code = ClusterClient::new(channel(&f.addr, &creds(&f.ca, user, role)).await)
        .console_code(ConsoleCodeRequest::default())
        .await
        .unwrap()
        .into_inner()
        .code;
    let signed = https(
        &f.ca,
        &console,
        "POST",
        "/api/login",
        None,
        None,
        Some(serde_json::json!({ "code": code })),
    )
    .await;
    assert_eq!(signed.status, 200, "{}", signed.text);
    let session = signed.cookie.unwrap().split(';').next().unwrap().to_owned();
    let me = https(
        &f.ca,
        &console,
        "GET",
        "/api/me",
        Some(&session),
        None,
        None,
    )
    .await;
    let token = me.body["csrf"].as_str().expect("a token").to_owned();
    (session, token)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_writes_as_its_user() {
    let f = start().await;
    let mut objects = ObjectsClient::new(f.admin.clone());
    for (kind, ns, name, spec) in [
        ("Namespace", "", "shop", serde_json::json!({})),
        (
            "RoleBinding",
            "shop",
            "alice",
            serde_json::json!({ "user": "alice", "role": "edit" }),
        ),
        (
            "RoleBinding",
            "shop",
            "bob",
            serde_json::json!({ "user": "bob", "role": "view" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let console = f.console_addr.to_string();
    let origin = format!("https://{console}");
    let (alice, token) = sign_in(&f, "alice").await;
    let write = |method: &'static str, path: String, body: Option<serde_json::Value>| {
        let (ca, console, origin, alice, token) = (&f.ca, &console, &origin, &alice, &token);
        async move {
            request(
                ca,
                console,
                true,
                method,
                &path,
                Some(alice),
                Some(origin),
                body,
                &[("x-legion-csrf", token)],
            )
            .await
        }
    };
    let menu = "apiVersion: legion/v1\nkind: Config\nmetadata:\n  name: menu\n  namespace: shop\nspec:\n  text: soup\n";

    // Without the session's token, as another site's form would send it.
    let forged = https(
        &f.ca,
        &console,
        "POST",
        "/api/apply",
        Some(&alice),
        None,
        Some(serde_json::json!({ "yaml": menu })),
    )
    .await;
    assert_eq!(forged.status, 403, "{}", forged.text);
    assert!(forged.text.contains("console's page"), "{}", forged.text);

    // A dry run shows what would be stored, and stores nothing.
    let dry = write(
        "POST",
        "/api/apply".into(),
        Some(serde_json::json!({ "yaml": menu, "dryRun": true })),
    )
    .await;
    assert_eq!(dry.status, 200, "{}", dry.text);
    assert_eq!(dry.body["results"][0]["result"], "created", "{}", dry.text);
    let diff = dry.body["results"][0]["diff"].as_str().unwrap();
    assert!(diff.contains("+kind: Config"), "{diff}");
    let none = write("GET", "/api/objects/Config/shop/menu".into(), None).await;
    assert_eq!(none.status, 404, "{}", none.text);

    // Applied: as `legion apply` would, by alice.
    let applied = write(
        "POST",
        "/api/apply".into(),
        Some(serde_json::json!({ "yaml": menu })),
    )
    .await;
    assert_eq!(applied.status, 200, "{}", applied.text);
    assert_eq!(applied.body["results"][0]["result"], "created");
    let stew = menu.replace("soup", "stew");
    let changed = write(
        "POST",
        "/api/apply".into(),
        Some(serde_json::json!({ "yaml": stew, "dryRun": true })),
    )
    .await;
    assert_eq!(
        changed.body["results"][0]["result"], "updated",
        "{}",
        changed.text
    );
    let diff = changed.body["results"][0]["diff"].as_str().unwrap();
    assert!(diff.contains("\n-") && diff.contains("\n+"), "{diff}");

    // Its manifest, to edit; an edit over a version that moved on is refused.
    let manifest = write("GET", "/api/manifest/Config/shop/menu".into(), None).await;
    assert_eq!(manifest.status, 200, "{}", manifest.text);
    assert!(
        manifest.body["yaml"]
            .as_str()
            .unwrap()
            .contains("name: menu")
    );
    let version = manifest.body["version"].as_u64().unwrap();
    let stale = write(
        "POST",
        "/api/apply".into(),
        Some(serde_json::json!({ "yaml": stew, "expectedVersion": version - 1 })),
    )
    .await;
    assert_eq!(stale.status, 409, "{}", stale.text);

    // Labels, and a Service scaled as `legion top` scales it.
    let labelled = write(
        "POST",
        "/api/labels".into(),
        Some(serde_json::json!({
            "kind": "Config", "namespace": "shop", "name": "menu", "changes": { "tier": "front" },
        })),
    )
    .await;
    assert_eq!(labelled.status, 200, "{}", labelled.text);
    assert_eq!(labelled.body["changed"], true);
    let got = write("GET", "/api/objects/Config/shop/menu".into(), None).await;
    assert_eq!(
        got.body["metadata"]["labels"]["tier"], "front",
        "{}",
        got.text
    );
    let web = serde_json::json!({
        "apiVersion": "legion/v1", "kind": "Service",
        "metadata": { "name": "web", "namespace": "shop" },
        "spec": { "replicas": 1, "pod": { "containers": [{ "name": "web", "image": "web:1" }] } },
    });
    let created = write(
        "POST",
        "/api/apply".into(),
        Some(serde_json::json!({ "object": web })),
    )
    .await;
    assert_eq!(created.status, 200, "{}", created.text);
    let scale = serde_json::json!({
        "kind": "Service", "namespace": "shop", "name": "web", "field": "replicas", "value": 3,
    });
    let scaled = write("POST", "/api/set".into(), Some(scale.clone())).await;
    assert_eq!(scaled.status, 200, "{}", scaled.text);
    assert_eq!(scaled.body["changed"], true);
    let got = write("GET", "/api/objects/Service/shop/web".into(), None).await;
    assert_eq!(got.body["spec"]["replicas"], 3, "{}", got.text);
    let back = write(
        "POST",
        "/api/rollback".into(),
        Some(serde_json::json!({ "namespace": "shop", "service": "web" })),
    )
    .await;
    assert_eq!(back.status, 409, "{}", back.text);
    assert!(
        back.text.contains("no update to roll back"),
        "{}",
        back.text
    );

    // Bob may only look; and alice's token is not his. (A change, since
    // asking for what is already there writes nothing.)
    let (bob, bob_token) = sign_in(&f, "bob").await;
    let mut scale_down = scale.clone();
    scale_down["value"] = 0.into();
    for (sent, why) in [
        (&bob_token, "a view user"),
        (&token, "another session's token"),
    ] {
        let refused = request(
            &f.ca,
            &console,
            true,
            "POST",
            "/api/set",
            Some(&bob),
            Some(&origin),
            Some(scale_down.clone()),
            &[("x-legion-csrf", sent)],
        )
        .await;
        assert_eq!(refused.status, 403, "{why}: {}", refused.text);
    }
    let got = write("GET", "/api/objects/Service/shop/web".into(), None).await;
    assert_eq!(got.body["spec"]["replicas"], 3, "unchanged: {}", got.text);

    // Deleted; and every write is in the audit log as alice's.
    let deleted = write("DELETE", "/api/objects/Config/shop/menu".into(), None).await;
    assert_eq!(deleted.status, 200, "{}", deleted.text);
    let gone = write("GET", "/api/objects/Config/shop/menu".into(), None).await;
    assert_eq!(gone.status, 404, "{}", gone.text);
    let filter = serde_json::json!({ "actor": "alice" }).to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let events = ClusterClient::new(f.admin.clone())
            .audit(legion_proto::v1::AuditRequest {
                filter_json: filter.clone().into_bytes(),
                limit: 100,
            })
            .await
            .unwrap()
            .into_inner()
            .events_json;
        let actions: Vec<String> = events
            .iter()
            .map(|e| serde_json::from_slice::<serde_json::Value>(e).unwrap())
            .map(|e| format!("{} {}", e["action"].as_str().unwrap(), e["result"]))
            .collect();
        if actions.iter().filter(|a| a.starts_with("apply")).count() >= 4
            && actions.iter().any(|a| a.starts_with("delete"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "alice's writes in the audit log: {actions:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = f.stop.send(());
    f.task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_runs_the_cluster_for_an_admin() {
    let f = start().await;
    let mut objects = ObjectsClient::new(f.admin.clone());
    for (kind, ns, name, spec) in [
        ("Namespace", "", "shop", serde_json::json!({})),
        ("Namespace", "", "bank", serde_json::json!({})),
        (
            "RoleBinding",
            "shop",
            "alice",
            serde_json::json!({ "user": "alice", "role": "admin" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let console = f.console_addr.to_string();
    let origin = format!("https://{console}");
    let (root, root_token) = sign_in_as(&f, "root", Role::Admin).await;
    let (alice, alice_token) = sign_in(&f, "alice").await;
    let as_user = |who: &str, token: &str, method: &'static str, path: &str, body| {
        let (ca, console, origin) = (&f.ca, &console, &origin);
        let (who, token, path) = (who.to_owned(), token.to_owned(), path.to_owned());
        async move {
            request(
                ca,
                console,
                true,
                method,
                &path,
                Some(&who),
                Some(origin),
                body,
                &[("x-legion-csrf", &token)],
            )
            .await
        }
    };
    let admin = |method, path: &str, body| as_user(&root, &root_token, method, path, body);

    // The CA, and who holds a certificate of it.
    let ca = admin("GET", "/api/ca", None).await;
    assert_eq!(ca.status, 200, "{}", ca.text);
    assert!(
        !ca.body["digest"].as_str().unwrap().is_empty(),
        "{}",
        ca.text
    );
    assert!(
        ca.body["holders"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["name"] == "m1"),
        "{}",
        ca.text
    );
    let token = admin(
        "POST",
        "/api/join-token",
        Some(serde_json::json!({ "role": "worker" })),
    )
    .await;
    assert_eq!(token.status, 200, "{}", token.text);
    assert!(!token.body["token"].as_str().unwrap().is_empty());
    let lock = admin(
        "POST",
        "/api/autolock",
        Some(serde_json::json!({ "action": "show" })),
    )
    .await;
    assert_eq!(
        (lock.status, &lock.body["key"]),
        (200, &serde_json::json!("")),
        "{}",
        lock.text
    );

    // A user, with their binding: credentials shown once.
    let carol = admin(
        "POST",
        "/api/users",
        Some(serde_json::json!({ "name": "carol", "role": "view", "namespace": "shop" })),
    )
    .await;
    assert_eq!(carol.status, 200, "{}", carol.text);
    let pem = carol.body["pem"].as_str().unwrap();
    assert!(pem.contains("BEGIN CERTIFICATE") && pem.contains("PRIVATE KEY"));
    assert_eq!(carol.body["binding"], "created");
    let bound = admin("GET", "/api/objects/RoleBinding/shop/carol", None).await;
    assert_eq!(bound.body["spec"]["role"], "view", "{}", bound.text);
    let half = admin(
        "POST",
        "/api/users",
        Some(serde_json::json!({ "name": "dave", "role": "view" })),
    )
    .await;
    assert_eq!(half.status, 400, "{}", half.text);

    // The control state, as `legion restore` takes it.
    let backup = admin("GET", "/api/backup", None).await;
    assert_eq!(backup.status, 200, "{}", backup.text);
    assert!(
        backup.body["objects"].is_array(),
        "a backup: {}",
        &backup.text[..backup.text.len().min(200)]
    );

    // The audit log, filtered as `legion audit` filters it.
    // Events are recorded a moment after what they record.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let log = loop {
        let log = admin("GET", "/api/audit?action=issue-user&limit=10", None).await;
        assert_eq!(log.status, 200, "{}", log.text);
        if !log.body["events"].as_array().unwrap().is_empty()
            || tokio::time::Instant::now() > deadline
        {
            break log;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let events = log.body["events"].as_array().unwrap();
    assert!(
        !events.is_empty() && events.iter().all(|e| e["action"] == "issue-user"),
        "{}",
        log.text
    );
    let wrong = admin("GET", "/api/audit?result=maybe", None).await;
    assert_eq!(wrong.status, 400, "{}", wrong.text);

    // What reaches what in a namespace, as the nodes enforce it: by
    // default its own tasks, every port, and no other namespace's.
    for name in ["web", "db"] {
        let spec = serde_json::json!({
            "replicas": 1, "pod": { "containers": [{ "name": "c", "image": "web:1" }] },
        });
        objects
            .apply(object("Service", "shop", name, spec))
            .await
            .unwrap();
    }
    let reach = as_user(
        &alice,
        &alice_token,
        "GET",
        "/api/reach?namespace=shop",
        None,
    )
    .await;
    assert_eq!(reach.status, 200, "{}", reach.text);
    assert_eq!(reach.body["services"], serde_json::json!(["db", "web"]));
    let edges: Vec<(String, String, bool)> = reach.body["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["from"].as_str().unwrap().to_owned(),
                e["to"].as_str().unwrap().to_owned(),
                e["all_ports"].as_bool().unwrap(),
            )
        })
        .collect();
    for (from, to) in [("web", "db"), ("db", "web"), ("web", "web"), ("db", "db")] {
        assert!(
            edges.contains(&(from.into(), to.into(), true)),
            "{from} to {to}: {edges:?}"
        );
    }
    assert!(
        edges.iter().all(|e| !e.0.is_empty()),
        "others reach nothing: {edges:?}"
    );
    let theirs = as_user(
        &alice,
        &alice_token,
        "GET",
        "/api/reach?namespace=bank",
        None,
    )
    .await;
    assert_eq!(theirs.status, 403, "{}", theirs.text);

    // An admin of one namespace runs none of the cluster.
    for (method, path, body) in [
        (
            "POST",
            "/api/join-token",
            Some(serde_json::json!({ "role": "worker" })),
        ),
        ("POST", "/api/ca", None),
        ("GET", "/api/backup", None),
        (
            "POST",
            "/api/users",
            Some(serde_json::json!({ "name": "eve" })),
        ),
    ] {
        let refused = as_user(&alice, &alice_token, method, path, body).await;
        assert_eq!(refused.status, 403, "{method} {path}: {}", refused.text);
    }

    let _ = f.stop.send(());
    f.task.await.unwrap();
}
