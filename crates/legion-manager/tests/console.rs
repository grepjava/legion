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
    /// Every cookie set, and where a redirect goes.
    cookies: Vec<String>,
    location: Option<String>,
    /// The page origin allowed to read the answer across origins.
    allow_origin: Option<String>,
    /// The answer's `Content-Security-Policy`.
    policy: Option<String>,
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
    let cookies = res
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    let location = res
        .headers()
        .get("location")
        .map(|v| v.to_str().unwrap().to_owned());
    let allow_origin = res
        .headers()
        .get("access-control-allow-origin")
        .map(|v| v.to_str().unwrap().to_owned());
    let policy = res
        .headers()
        .get("content-security-policy")
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    Response {
        status,
        cookie,
        cookies,
        location,
        allow_origin,
        policy,
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

/// Compose import and templates (CONSOLE.md phase 5): a compose file as the
/// objects `legion stack deploy` would apply, nothing written, with its
/// variables and the namespace first when missing; the examples and the
/// user's saved templates to start from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_converts_compose_files_and_lists_templates() {
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
            "Config",
            "shop",
            "plain",
            serde_json::json!({ "text": "services: {}" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    objects
        .apply(ApplyRequest {
            object: Some(Object {
                kind: "Config".into(),
                namespace: "shop".into(),
                name: "mine".into(),
                labels: [("legion.template".to_owned(), "compose".to_owned())].into(),
                spec_json: serde_json::json!({
                    "text": "# My shop.\n# More.\n\nservices: { a: { image: a } }\n"
                })
                .to_string()
                .into_bytes(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let console = f.console_addr.to_string();
    let origin = format!("https://{console}");
    let (alice, token) = sign_in(&f, "alice").await;
    let (root, root_token) = sign_in_as(&f, "root", Role::Admin).await;
    let post_as = |who: &str, body: serde_json::Value| {
        let (session, token) = if who == "root" {
            (&root, &root_token)
        } else {
            (&alice, &token)
        };
        let (ca, console, origin) = (&f.ca, &console, &origin);
        async move {
            request(
                ca,
                console,
                true,
                "POST",
                "/api/compose",
                Some(session),
                Some(origin),
                Some(body),
                &[("x-legion-csrf", token)],
            )
            .await
        }
    };
    let post = |body: serde_json::Value| post_as("alice", body);
    let file = "services:\n  web:\n    image: \"web:${TAG:-1}\"\n    ports: [\"8080\"]\n";

    let r = post(serde_json::json!({
        "compose": file, "stack": "site", "namespace": "shop", "env": { "TAG": "2" },
    }))
    .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let made = &r.body["objects"];
    assert_eq!(made.as_array().unwrap().len(), 1, "{made}");
    assert_eq!(made[0]["kind"], "Service");
    assert_eq!(made[0]["metadata"]["namespace"], "shop");
    assert_eq!(made[0]["metadata"]["labels"]["legion.stack"], "site");
    assert_eq!(made[0]["spec"]["pod"]["containers"][0]["image"], "web:2");
    assert_eq!(
        r.body["variables"],
        serde_json::json!([{ "name": "TAG", "default": "1", "required": false }])
    );
    // Nothing was written.
    let none = objects
        .get(legion_proto::v1::ObjectRef {
            kind: "Service".into(),
            namespace: "shop".into(),
            name: "web".into(),
        })
        .await;
    assert_eq!(none.unwrap_err().code(), tonic::Code::NotFound);

    // Into a namespace there is not yet: it comes first, for the admin who
    // may make it. A user who may not look at namespaces is not told, as
    // with `legion stack deploy`: theirs is there.
    let r = post(serde_json::json!({ "compose": file, "stack": "site2", "env": {} })).await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["objects"][0]["kind"], "Service");
    let r = post_as(
        "root",
        serde_json::json!({ "compose": file, "stack": "site2", "env": {} }),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["objects"][0]["kind"], "Namespace");
    assert_eq!(r.body["objects"][0]["metadata"]["name"], "site2");
    assert_eq!(r.body["objects"][1]["metadata"]["namespace"], "site2");
    assert_eq!(
        r.body["objects"][1]["spec"]["pod"]["containers"][0]["image"],
        "web:1"
    );

    // A file it would read, or one that is wrong, says why.
    let r = post(serde_json::json!({
        "compose": "services: { a: { image: a } }\nconfigs: { c: { file: ./c.conf } }\n",
        "stack": "site", "namespace": "shop", "env": {},
    }))
    .await;
    assert_eq!(r.status, 400, "{}", r.text);
    assert!(r.text.contains("reads no files"), "{}", r.text);

    let r = https(
        &f.ca,
        &console,
        "GET",
        "/api/templates",
        Some(&alice),
        None,
        None,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let listed: Vec<(String, String, String)> = r.body["templates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                t["namespace"].as_str().unwrap().to_owned(),
                t["name"].as_str().unwrap().to_owned(),
                t["about"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(listed.len(), 3, "{listed:?}");
    assert_eq!(listed[0].0, "");
    assert_eq!(listed[0].1, "counter");
    assert!(listed[0].2.starts_with("A hit counter"), "{listed:?}");
    assert_eq!(listed[1].1, "static-site");
    assert_eq!(
        listed[2],
        ("shop".into(), "mine".into(), "My shop. More.".into())
    );
    // Each example converts as it is.
    for t in r.body["templates"].as_array().unwrap() {
        let c = post(serde_json::json!({
            "compose": t["compose"], "stack": t["name"], "namespace": "shop", "env": {},
        }))
        .await;
        assert_eq!(c.status, 200, "{}: {}", t["name"], c.text);
    }

    let _ = f.stop.send(());
    f.task.await.unwrap();
}

/// The console's settings over the gateway (CONSOLE.md phase 6): before
/// signing in only the banner, which the sign-in page shows; all of them
/// after; set by the admin alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_serves_its_settings() {
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
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let console = f.console_addr.to_string();
    let origin = format!("https://{console}");
    let get = |cookie: Option<String>| {
        let (ca, console) = (&f.ca, &console);
        async move {
            https(
                ca,
                console,
                "GET",
                "/api/console-settings",
                cookie.as_deref(),
                None,
                None,
            )
            .await
        }
    };
    let r = get(None).await;
    assert_eq!(
        (r.status, r.body.clone()),
        (200, serde_json::json!({})),
        "{}",
        r.text
    );

    let wanted = serde_json::json!({
        "banner": { "text": "Production", "level": "danger" },
        "links": [{ "label": "Runbook", "url": "https://wiki.example/run" }],
        "defaultNamespace": "shop",
        "session": "8h",
    });
    let post = |who: (String, String), body: serde_json::Value| {
        let (ca, console, origin) = (&f.ca, &console, &origin);
        async move {
            request(
                ca,
                console,
                true,
                "POST",
                "/api/console-settings",
                Some(&who.0),
                Some(origin),
                Some(body),
                &[("x-legion-csrf", &who.1)],
            )
            .await
        }
    };
    let root = sign_in_as(&f, "root", Role::Admin).await;
    let alice = sign_in(&f, "alice").await;
    let r = post(root.clone(), wanted.clone()).await;
    assert_eq!(
        (r.status, r.body.clone()),
        (200, wanted.clone()),
        "{}",
        r.text
    );
    let r = post(alice.clone(), serde_json::json!({})).await;
    assert_eq!(r.status, 403, "{}", r.text);
    let r = post(root.clone(), serde_json::json!({ "session": "1m" })).await;
    assert_eq!(r.status, 400, "{}", r.text);

    // Signed out: the banner alone. Signed in: everything.
    let r = get(None).await;
    assert_eq!(
        r.body,
        serde_json::json!({ "banner": { "text": "Production", "level": "danger" } })
    );
    let r = get(Some("legion_session=stale".into())).await;
    assert!(r.body.get("links").is_none(), "{}", r.text);
    let r = get(Some(alice.0.clone())).await;
    assert_eq!(r.body, wanted, "{}", r.text);

    let _ = f.stop.send(());
    f.task.await.unwrap();
}

/// Several clusters (CONSOLE.md phase 6): each console keeps its own
/// sessions, and tells another's page who the user is only if its settings
/// list that console, without the token for writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_consoles_of_listed_clusters_see_who_is_signed_in() {
    use legion_proto::v1::SetConsoleSettingsRequest;
    let (east, west) = (start().await, start().await);
    let (east_url, west_url) = (
        format!("https://{}", east.console_addr),
        format!("https://{}", west.console_addr),
    );
    for (f, name, other, url) in [
        (&east, "east", "west", &west_url),
        (&west, "west", "east", &east_url),
    ] {
        ClusterClient::new(f.admin.clone())
            .set_console_settings(SetConsoleSettingsRequest {
                settings_json: serde_json::json!({
                    "name": name,
                    "clusters": [{ "name": other, "url": url }],
                })
                .to_string()
                .into_bytes(),
            })
            .await
            .unwrap();
    }
    let mut objects = ObjectsClient::new(west.admin.clone());
    for (kind, ns, name, spec) in [
        ("Namespace", "", "shop", serde_json::json!({})),
        (
            "RoleBinding",
            "shop",
            "alice",
            serde_json::json!({ "user": "alice", "role": "view" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let alice = sign_in(&west, "alice").await;
    let west_console = west.console_addr.to_string();
    let me = |cookie: Option<String>, origin: Option<String>| {
        let (ca, console) = (&west.ca, &west_console);
        async move {
            https(
                ca,
                console,
                "GET",
                "/api/me",
                cookie.as_deref(),
                origin.as_deref(),
                None,
            )
            .await
        }
    };

    // West's own page: the token for writes, no one else allowed to read.
    let r = me(Some(alice.0.clone()), Some(west_url.clone())).await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.body["cluster"], "west");
    assert!(r.body["csrf"].is_string());
    assert_eq!(r.allow_origin, None);

    // East's Clusters page: who alice is in west, and nothing to write with.
    let r = me(Some(alice.0.clone()), Some(east_url.clone())).await;
    assert_eq!(r.status, 200, "{}", r.text);
    assert_eq!(r.allow_origin.as_deref(), Some(east_url.as_str()));
    assert_eq!(r.body["name"], "alice");
    assert_eq!(r.body["namespaces"], serde_json::json!({ "shop": "view" }));
    assert!(r.body.get("csrf").is_none(), "{}", r.text);
    // Not signed in there: east's page may read that too.
    let r = me(None, Some(east_url.clone())).await;
    assert_eq!(
        (r.status, r.allow_origin.as_deref()),
        (401, Some(east_url.as_str()))
    );

    // A console west does not list learns nothing.
    for origin in ["https://evil.example", "null", "http://127.0.0.1"] {
        let r = me(Some(alice.0.clone()), Some(origin.into())).await;
        assert_eq!((r.status, r.allow_origin), (403, None), "{origin}");
    }
    // And west's writes stay its own page's, listed or not.
    let r = request(
        &west.ca,
        &west_console,
        false,
        "POST",
        "/api/apply",
        Some(&alice.0),
        Some(&east_url),
        Some(serde_json::json!({})),
        &[("x-legion-csrf", &alice.1)],
    )
    .await;
    assert_eq!(r.status, 403, "{}", r.text);

    for f in [east, west] {
        let _ = f.stop.send(());
        f.task.await.unwrap();
    }
}

/// Plugins (CONSOLE.md phase 6): a Config labelled as one is a page the
/// console lists in the namespaces the user reads, served under a policy
/// that fences it in. The console's own pages may reach the clusters the
/// settings list, and no other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_serves_plugins_fenced_in() {
    use legion_proto::v1::SetConsoleSettingsRequest;
    let f = start().await;
    let page = "<!doctype html><title>Status</title><p>hello</p>";
    let mut objects = ObjectsClient::new(f.admin.clone());
    for ns in ["shop", "other"] {
        objects
            .apply(object("Namespace", "", ns, serde_json::json!({})))
            .await
            .unwrap();
    }
    objects
        .apply(object(
            "RoleBinding",
            "shop",
            "alice",
            serde_json::json!({ "user": "alice", "role": "view" }),
        ))
        .await
        .unwrap();
    for (ns, name, labelled) in [
        ("shop", "status", true),
        ("shop", "plain", false),
        ("other", "hidden", true),
    ] {
        let mut req = object("Config", ns, name, serde_json::json!({ "text": page }));
        if labelled {
            req.object.as_mut().unwrap().labels =
                [("legion.console-plugin".to_owned(), "page".to_owned())].into();
        }
        objects.apply(req).await.unwrap();
    }
    let alice = sign_in(&f, "alice").await;
    let console = f.console_addr.to_string();
    let get = |path: &str| {
        let (ca, console, cookie) = (&f.ca, &console, alice.0.clone());
        let path = path.to_owned();
        async move { https(ca, console, "GET", &path, Some(&cookie), None, None).await }
    };

    // Listed where she reads: shop's, not other's, not an unlabelled Config.
    let r = get("/api/plugins").await;
    assert_eq!(
        r.body["plugins"],
        serde_json::json!([{ "namespace": "shop", "name": "status" }]),
        "{}",
        r.text
    );

    // The page, in a sandbox of its own that reaches nothing.
    let r = get("/api/plugin/shop/status").await;
    assert_eq!((r.status, r.text.as_str()), (200, page));
    let policy = r.policy.unwrap();
    for part in [
        "sandbox allow-scripts;",
        "default-src 'none'",
        "connect-src 'none'",
        "frame-ancestors 'self'",
    ] {
        assert!(policy.contains(part), "{policy}");
    }
    assert!(!policy.contains("allow-same-origin"), "{policy}");
    assert_eq!(get("/api/plugin/shop/plain").await.status, 404);
    assert_eq!(get("/api/plugin/other/hidden").await.status, 403);

    // The example applies as it is, and is a plugin.
    let root = sign_in_as(&f, "root", Role::Admin).await;
    let r = request(
        &f.ca,
        &console,
        false,
        "POST",
        "/api/apply",
        Some(&root.0),
        Some(&format!("https://{console}")),
        Some(serde_json::json!({
            "yaml": include_str!("../../../examples/console-plugin/plugin.yaml"),
        })),
        &[("x-legion-csrf", &root.1)],
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text);
    let r = https(
        &f.ca,
        &console,
        "GET",
        "/api/plugin/default/services-at-a-glance",
        Some(&root.0),
        None,
        None,
    )
    .await;
    assert!(r.text.contains("Services at a glance"), "{}", r.text);

    // The console's pages: their own origin, and the listed clusters'.
    let r = get("/").await;
    let policy = r.policy.unwrap();
    assert!(policy.contains("connect-src 'self';"), "{policy}");
    assert!(policy.contains("frame-ancestors 'none'"), "{policy}");
    ClusterClient::new(f.admin.clone())
        .set_console_settings(SetConsoleSettingsRequest {
            settings_json: serde_json::json!({
                "clusters": [{ "name": "west", "url": "https://west.example:7480/k" }],
            })
            .to_string()
            .into_bytes(),
        })
        .await
        .unwrap();
    let policy = get("/").await.policy.unwrap();
    assert!(
        policy.contains("connect-src 'self' https://west.example:7480;"),
        "{policy}"
    );
    // Not on the API's answers, which are not pages.
    let policy = get("/api/plugins").await.policy.unwrap();
    assert!(policy.contains("connect-src 'self';"), "{policy}");

    let _ = f.stop.send(());
    f.task.await.unwrap();
}

/// Single sign-on (CONSOLE.md phase 6), against a provider this test plays:
/// discovery, its key, a token endpoint that checks the code, the client
/// and PKCE. A user signs in through it, gets what their group's binding
/// gives, and loses it when the binding goes; a forged or replayed answer
/// signs no one in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_signs_in_with_an_identity_provider() {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
    use legion_proto::v1::SetConsoleOidcRequest;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
    use sha2::{Digest, Sha256};

    // The provider: one ES256 key, one good code, used once.
    #[derive(Default)]
    struct Idp {
        challenge: String,
        nonce: String,
        used: bool,
    }
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let pair = Arc::new(
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap(),
    );
    let point = pair.public_key().as_ref().to_vec();
    let idp = Arc::new(std::sync::Mutex::new(Idp::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let f = start().await;
    let console = f.console_addr.to_string();
    let callback = format!("https://{console}/api/oidc/callback");
    {
        use axum::routing::{get, post};
        let discovery = serde_json::json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "jwks_uri": format!("{issuer}/jwks"),
        });
        let jwks = serde_json::json!({ "keys": [{
            "kty": "EC", "crv": "P-256", "kid": "k1", "use": "sig",
            "x": B64.encode(&point[1..33]), "y": B64.encode(&point[33..]),
        }]});
        let (issuer, idp, pair, callback) =
            (issuer.clone(), idp.clone(), pair.clone(), callback.clone());
        let token = move |body: String| {
            let (issuer, idp, pair, callback) =
                (issuer.clone(), idp.clone(), pair.clone(), callback.clone());
            async move {
                let form: std::collections::HashMap<String, String> =
                    reqwest::Url::parse(&format!("http://x/?{body}"))
                        .unwrap()
                        .query_pairs()
                        .into_owned()
                        .collect();
                let mut idp = idp.lock().unwrap();
                let verifier = form.get("code_verifier").cloned().unwrap_or_default();
                let good = form.get("grant_type").map(String::as_str) == Some("authorization_code")
                    && form.get("code").map(String::as_str) == Some("good-code")
                    && form.get("client_id").map(String::as_str) == Some("legion")
                    && form.get("client_secret").map(String::as_str) == Some("s3cret")
                    && form.get("redirect_uri") == Some(&callback)
                    && B64.encode(Sha256::digest(verifier.as_bytes())) == idp.challenge
                    && !idp.used;
                if !good {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        axum::Json(serde_json::json!({ "error": "invalid_grant" })),
                    );
                }
                idp.used = true;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let header = serde_json::json!({ "alg": "ES256", "kid": "k1" });
                let claims = serde_json::json!({
                    "iss": issuer, "aud": "legion", "exp": now + 300, "iat": now,
                    "nonce": idp.nonce, "email": "alice@example.com",
                    "email_verified": true, "groups": ["shop-team", "other"],
                });
                let signed = format!(
                    "{}.{}",
                    B64.encode(header.to_string()),
                    B64.encode(claims.to_string())
                );
                let sig = pair.sign(&SystemRandom::new(), signed.as_bytes()).unwrap();
                let id_token = format!("{signed}.{}", B64.encode(sig.as_ref()));
                (
                    axum::http::StatusCode::OK,
                    axum::Json(serde_json::json!({ "id_token": id_token, "token_type": "Bearer" })),
                )
            }
        };
        let app = axum::Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || async move { axum::Json(discovery) }),
            )
            .route("/jwks", get(move || async move { axum::Json(jwks) }))
            .route("/token", post(token));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    }

    // The admin turns single sign-on on and binds a group.
    let mut cluster = ClusterClient::new(f.admin.clone());
    let set = cluster
        .set_console_oidc(SetConsoleOidcRequest {
            oidc_json: serde_json::json!({
                "issuer": issuer, "clientId": "legion",
                "consoleUrl": format!("https://{console}"),
            })
            .to_string()
            .into_bytes(),
            client_secret: "s3cret".into(),
            remove: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(set.has_secret);
    let mut objects = ObjectsClient::new(f.admin.clone());
    for (kind, ns, name, spec) in [
        ("Namespace", "", "shop", serde_json::json!({})),
        (
            "RoleBinding",
            "shop",
            "shop-team",
            serde_json::json!({ "group": "shop-team", "role": "edit" }),
        ),
    ] {
        objects.apply(object(kind, ns, name, spec)).await.unwrap();
    }
    let get_as = |path: String, cookie: Option<String>| {
        let (ca, console) = (&f.ca, &console);
        async move { https(ca, console, "GET", &path, cookie.as_deref(), None, None).await }
    };
    let r = get_as("/api/console-settings".into(), None).await;
    assert!(r.body["sso"]["label"].is_string(), "{}", r.text);

    // To the provider, with a flow only this browser holds.
    let r = get_as("/api/oidc/start".into(), None).await;
    assert_eq!(r.status, 303, "{}", r.text);
    let to = reqwest::Url::parse(r.location.as_deref().unwrap()).unwrap();
    assert!(
        to.as_str().starts_with(&format!("{issuer}/authorize?")),
        "{to}"
    );
    let q: std::collections::HashMap<String, String> = to.query_pairs().into_owned().collect();
    assert_eq!(q["redirect_uri"], callback);
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(q["scope"], "openid email profile");
    let flow = r
        .cookies
        .iter()
        .find(|c| c.starts_with("legion_oidc="))
        .expect("the flow's cookie")
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    {
        let mut idp = idp.lock().unwrap();
        idp.challenge = q["code_challenge"].clone();
        idp.nonce = q["nonce"].clone();
    }
    let back = |state: String, cookie: Option<String>| {
        get_as(
            format!("/api/oidc/callback?code=good-code&state={state}"),
            cookie,
        )
    };
    let failed = |r: &Response| {
        r.status == 303
            && r.location
                .as_deref()
                .is_some_and(|l| l.starts_with("/login#error="))
    };

    // Not this browser's flow, or not its state: no one signed in, and the
    // code not spent.
    let r = back(q["state"].clone(), None).await;
    assert!(failed(&r), "{:?}", r.location);
    let r = back("forged".into(), Some(flow.clone())).await;
    assert!(failed(&r), "{:?}", r.location);
    assert!(!idp.lock().unwrap().used);

    // The provider's answer: signed in as oidc:alice@example.com, with what
    // shop-team may do.
    let r = back(q["state"].clone(), Some(flow.clone())).await;
    assert_eq!(
        (r.status, r.location.as_deref()),
        (303, Some("/")),
        "{:?}",
        r.cookies
    );
    let session = r
        .cookies
        .iter()
        .find(|c| c.starts_with("legion_session="))
        .expect("a session")
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let me = get_as("/api/me".into(), Some(session.clone())).await;
    assert_eq!(me.body["name"], "oidc:alice@example.com", "{}", me.text);
    assert_eq!(me.body["namespaces"], serde_json::json!({ "shop": "edit" }));

    // The code once only.
    let r = back(q["state"].clone(), Some(flow)).await;
    assert!(failed(&r), "{:?}", r.location);
    assert!(r.location.unwrap().contains("refused"));

    // Rights stay in the bindings: the group's taken, hers are gone.
    objects
        .delete(legion_proto::v1::DeleteRequest {
            r#ref: Some(legion_proto::v1::ObjectRef {
                kind: "RoleBinding".into(),
                namespace: "shop".into(),
                name: "shop-team".into(),
            }),
            expected_version: None,
        })
        .await
        .unwrap();
    let me = get_as("/api/me".into(), Some(session)).await;
    assert_eq!(me.body["namespaces"], serde_json::json!({}), "{}", me.text);

    // Off: the start page says so.
    cluster
        .set_console_oidc(SetConsoleOidcRequest {
            remove: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let r = get_as("/api/oidc/start".into(), None).await;
    assert!(failed(&r), "{:?}", r.location);

    let _ = f.stop.send(());
    f.task.await.unwrap();
}
