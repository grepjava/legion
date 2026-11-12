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
    request(ca, addr, false, method, path, cookie, origin, body).await
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_calls_the_api_as_its_user() {
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
