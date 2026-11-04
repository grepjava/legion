//! The ingress proxy (DESIGN.md §5.8): HTTP on every node that enables it,
//! each request sent to a ready task of the service its route names, by
//! the task's address and port. Routes match the `Host` header and the
//! longest path prefix, an exact host before a wildcard; tasks take turns.
//! The backend sees the client in `X-Forwarded-For`. Upgrades (WebSocket)
//! pass through. Clients speak HTTP/1.1 or HTTP/2 (over TLS by ALPN, in
//! the clear by its preface); backends HTTP/1.1, over connections kept
//! open for the next request.
//!
//! HTTPS serves the hosts of routes with `tls`, each with the certificate
//! in its Secret, chosen by SNI; the Secrets are read again when routes
//! change and every minute, so renewed certificates take over. While the
//! leader orders a route's certificate from an ACME authority, the proxy
//! answers its HTTP-01 challenges for the route's host.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{HOST, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use legion_api::dispatch::{Endpoints, Routes};
use legion_api::service::Protocol;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;

type Body = BoxBody<Bytes, hyper::Error>;

/// How long a client has for its TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a backend has to accept the connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a connection to a backend is kept open for another request.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Where an ACME authority asks for a challenge's key authorization.
const CHALLENGE_PATH: &str = "/.well-known/acme-challenge/";

/// How often the routes' certificates are read again.
const CERTIFICATES_PERIOD: Duration = Duration::from_secs(60);

struct Proxy {
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
    turn: AtomicUsize,
    https: bool,
    /// To the backends, keeping connections open between requests.
    client: Client<HttpConnector, Incoming>,
}

/// Serves HTTP on `listener` until the task is dropped.
pub async fn serve(
    listener: TcpListener,
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
) {
    run(listener, routes, endpoints, None).await
}

/// Serves HTTPS on `listener`, with the certificates `certificates` holds.
pub async fn serve_tls(
    listener: TcpListener,
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
    certificates: Arc<Certificates>,
) {
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(certificates);
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(config));
    run(listener, routes, endpoints, Some(acceptor)).await
}

async fn run(
    listener: TcpListener,
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
    tls: Option<TlsAcceptor>,
) {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
    connector.set_nodelay(true);
    let client = Client::builder(TokioExecutor::new())
        .pool_idle_timeout(IDLE_TIMEOUT)
        .build(connector);
    let proxy = Arc::new(Proxy {
        routes,
        endpoints,
        turn: AtomicUsize::new(0),
        https: tls.is_some(),
        client,
    });
    loop {
        let (stream, client) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("ingress: accepting: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (proxy, tls) = (proxy.clone(), tls.clone());
        tokio::spawn(async move {
            match tls {
                None => connection(stream, proxy, client).await,
                // A client that never finishes its handshake is let go.
                Some(acceptor) => {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(stream)) => connection(stream, proxy, client).await,
                        Ok(Err(e)) => tracing::debug!(%client, "ingress TLS handshake: {e}"),
                        Err(_) => tracing::debug!(%client, "ingress TLS handshake timed out"),
                    }
                }
            }
        });
    }
}

async fn connection<IO>(io: IO, proxy: Arc<Proxy>, client: SocketAddr)
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |req| {
        let proxy = proxy.clone();
        async move { Ok::<_, Infallible>(proxy.handle(req, client).await) }
    });
    let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection_with_upgrades(TokioIo::new(io), service);
    if let Err(e) = conn.await {
        tracing::debug!(%client, "ingress connection: {e}");
    }
}

/// The certificates of routes with `tls`, by host as the routes give it
/// (`*.example.org` included), chosen by the client's SNI.
#[derive(Debug, Default)]
pub struct Certificates {
    by_host: RwLock<BTreeMap<String, Arc<CertifiedKey>>>,
}

impl Certificates {
    /// Parses `pem`, a certificate chain and its private key, for `host`.
    pub fn set(&self, host: &str, pem: &[u8]) -> Result<(), String> {
        let certs = CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("certificates: {e}"))?;
        if certs.is_empty() {
            return Err("no certificate in the PEM".into());
        }
        let key = PrivateKeyDer::from_pem_slice(pem).map_err(|e| format!("private key: {e}"))?;
        let key = tokio_rustls::rustls::crypto::ring::sign::any_supported_type(&key)
            .map_err(|e| format!("private key: {e}"))?;
        let certified = CertifiedKey::new(certs, key);
        certified
            .keys_match()
            .map_err(|e| format!("the key is not the certificate's: {e}"))?;
        self.by_host
            .write()
            .unwrap()
            .insert(host.to_owned(), Arc::new(certified));
        Ok(())
    }

    /// Drops the certificates of hosts not in `hosts`.
    pub fn retain(&self, hosts: &BTreeSet<String>) {
        self.by_host
            .write()
            .unwrap()
            .retain(|h, _| hosts.contains(h));
    }

    fn get(&self, sni: &str) -> Option<Arc<CertifiedKey>> {
        let by_host = self.by_host.read().unwrap();
        let sni = sni.to_ascii_lowercase();
        by_host.get(&sni).cloned().or_else(|| {
            let (_, domain) = sni.split_once('.')?;
            by_host.get(&format!("*.{domain}")).cloned()
        })
    }
}

impl ResolvesServerCert for Certificates {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.get(hello.server_name()?)
    }
}

/// Keeps `certificates` holding those of the routes' Secrets, read with
/// `read` (namespace, name) when routes change and every
/// [`CERTIFICATES_PERIOD`]. One that cannot be read keeps what was read
/// before.
pub async fn follow_certificates<F, Fut>(
    mut routes: watch::Receiver<Routes>,
    certificates: Arc<Certificates>,
    read: F,
) where
    F: Fn(String, String) -> Fut,
    Fut: std::future::Future<Output = Result<Option<Vec<u8>>, String>>,
{
    loop {
        let wanted: Vec<(String, String, String)> = routes
            .borrow_and_update()
            .iter()
            .filter_map(|r| {
                let tls = r.spec.tls.as_ref()?;
                Some((r.spec.host.clone(), r.namespace.clone(), tls.secret.clone()))
            })
            .collect();
        certificates.retain(&wanted.iter().map(|(h, _, _)| h.clone()).collect());
        for (host, namespace, secret) in wanted {
            let outcome = match read(namespace.clone(), secret.clone()).await {
                Ok(Some(pem)) => certificates.set(&host, &pem),
                Ok(None) => Err("no such Secret".into()),
                Err(e) => Err(e),
            };
            if let Err(e) = outcome {
                let secret = format!("{namespace}/{secret}");
                tracing::warn!(%host, %secret, "route certificate: {e}");
            }
        }
        tokio::select! {
            changed = routes.changed() => if changed.is_err() { return },
            _ = tokio::time::sleep(CERTIFICATES_PERIOD) => {}
        }
    }
}

fn reply(status: StatusCode, message: &str) -> Response<Body> {
    let mut r = Response::new(
        Full::new(Bytes::from(format!("{message}\n")))
            .map_err(|never| match never {})
            .boxed(),
    );
    *r.status_mut() = status;
    r
}

impl Proxy {
    /// The key authorization for challenge `token` of a route of `host`.
    fn challenge(&self, host: &str, token: &str) -> Option<String> {
        let routes = self.routes.borrow();
        routes
            .iter()
            .filter(|r| r.spec.host == host)
            .find_map(|r| r.challenges.get(token).cloned())
    }

    /// The backend for a request: a ready task's address and port.
    fn backend(&self, host: &str, path: &str) -> Result<SocketAddr, (StatusCode, String)> {
        let routes = self.routes.borrow();
        let route = routes
            .iter()
            .filter_map(|r| Some((r.spec.matches(host, path)?, r)))
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, r)| r)
            .ok_or((StatusCode::NOT_FOUND, format!("no route for {host}{path}")))?;
        let endpoints = self.endpoints.borrow();
        let service = endpoints
            .iter()
            .find(|s| !s.job && s.namespace == route.namespace && s.name == route.spec.service)
            .ok_or_else(|| {
                let what = format!("service {}/{}", route.namespace, route.spec.service);
                (StatusCode::SERVICE_UNAVAILABLE, format!("no {what}"))
            })?;
        let tcp: Vec<_> = service
            .ports
            .iter()
            .filter(|p| p.protocol == Protocol::Tcp)
            .collect();
        let port = match route.spec.port {
            Some(want) => tcp.iter().find(|p| p.port == want),
            None if tcp.len() == 1 => tcp.first(),
            None => None,
        }
        .ok_or_else(|| {
            let why = format!(
                "route {}/{}: service {} has no such port",
                route.namespace, route.name, route.spec.service
            );
            (StatusCode::BAD_GATEWAY, why)
        })?;
        if service.ready.is_empty() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("no ready task of {}", route.spec.service),
            ));
        }
        let n = self.turn.fetch_add(1, Ordering::Relaxed) % service.ready.len();
        let ip: Ipv4Addr = service.ready[n];
        Ok(SocketAddr::from((ip, port.target)))
    }

    async fn handle(&self, mut req: Request<Incoming>, client: SocketAddr) -> Response<Body> {
        let host = req
            .headers()
            .get(HOST)
            .and_then(|h| h.to_str().ok())
            .filter(|h| !h.is_empty())
            .or_else(|| req.uri().host())
            .unwrap_or_default();
        let host = host
            .rsplit_once(':')
            .filter(|(_, port)| port.bytes().all(|b| b.is_ascii_digit()))
            .map_or(host, |(name, _)| name)
            .to_ascii_lowercase();
        if let Some(token) = req.uri().path().strip_prefix(CHALLENGE_PATH)
            && let Some(answer) = self.challenge(&host, token)
        {
            let mut r = Response::new(
                Full::new(Bytes::from(answer))
                    .map_err(|n| match n {})
                    .boxed(),
            );
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            return r;
        }
        let backend = match self.backend(&host, req.uri().path()) {
            Ok(b) => b,
            Err((status, why)) => return reply(status, &why),
        };

        // Only HTTP/1.1 upgrades: HTTP/2 has none (nor a 101 to answer).
        let wants_upgrade = req.version() == hyper::Version::HTTP_11
            && req.headers().contains_key(hyper::header::UPGRADE);
        let headers = req.headers_mut();
        hop_by_hop(headers, wants_upgrade);
        // Earlier proxies' addresses, on as many header lines as they came.
        let forwarded: Vec<&str> = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        let forwarded = match forwarded.is_empty() {
            true => client.ip().to_string(),
            false => format!("{}, {}", forwarded.join(", "), client.ip()),
        };
        if let Ok(v) = HeaderValue::from_str(&forwarded) {
            headers.insert("x-forwarded-for", v);
        }
        if let Ok(v) = HeaderValue::from_str(&host) {
            headers.insert("x-forwarded-host", v);
        }
        let proto = if self.https { "https" } else { "http" };
        headers.insert("x-forwarded-proto", HeaderValue::from_static(proto));
        // An HTTP/2 request names its host in its URI alone; the backend
        // speaks HTTP/1.1 and reads it from `Host`.
        if !headers.contains_key(HOST)
            && let Some(authority) = req.uri().authority()
            && let Ok(v) = HeaderValue::from_str(authority.as_str())
        {
            req.headers_mut().insert(HOST, v);
        }
        *req.version_mut() = hyper::Version::HTTP_11;
        let pq = req.uri().path_and_query().map_or("/", |pq| pq.as_str());
        *req.uri_mut() = match format!("http://{backend}{pq}").parse() {
            Ok(uri) => uri,
            Err(e) => return reply(StatusCode::BAD_REQUEST, &format!("{pq}: {e}")),
        };

        let upgrade = wants_upgrade.then(|| hyper::upgrade::on(&mut req));
        let mut resp = match self.client.request(req).await {
            Ok(r) => r,
            Err(e) if timed_out(&e) => {
                return reply(
                    StatusCode::GATEWAY_TIMEOUT,
                    &format!("{backend}: timed out"),
                );
            }
            Err(e) => {
                let why = std::iter::successors(Some(&e as &dyn std::error::Error), |e| e.source())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(": ");
                return reply(StatusCode::BAD_GATEWAY, &format!("{backend}: {why}"));
            }
        };
        if resp.status() == StatusCode::SWITCHING_PROTOCOLS
            && let Some(client_side) = upgrade
        {
            let backend_side = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                let (Ok(c), Ok(b)) = (client_side.await, backend_side.await) else {
                    return;
                };
                let (mut c, mut b) = (TokioIo::new(c), TokioIo::new(b));
                let _ = tokio::io::copy_bidirectional(&mut c, &mut b).await;
            });
        } else {
            hop_by_hop(resp.headers_mut(), false);
        }
        resp.map(BodyExt::boxed)
    }
}

/// Whether a backend did not accept the connection in time.
fn timed_out(e: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(e), |e| e.source()).any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::TimedOut)
    })
}

/// Drops the headers meant for one connection alone (RFC 9110 §7.6.1):
/// those `Connection` names, and the usual ones. The client's would reach
/// the backend otherwise, and could have it drop the `X-Forwarded-*` the
/// proxy adds; the backend's would reach the client. An upgrade keeps what
/// it needs.
fn hop_by_hop(headers: &mut hyper::HeaderMap, upgrade: bool) {
    use hyper::header::{CONNECTION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE};
    let named: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|n| n.trim().to_ascii_lowercase())
        .filter(|n| !n.is_empty() && !(upgrade && n == "upgrade"))
        .collect();
    for name in &named {
        headers.remove(name.as_str());
    }
    for name in ["keep-alive", "proxy-connection"] {
        headers.remove(name);
    }
    for name in [TE, TRAILER, TRANSFER_ENCODING] {
        headers.remove(name);
    }
    if !upgrade {
        headers.remove(CONNECTION);
        headers.remove(UPGRADE);
    } else {
        headers.insert(CONNECTION, HeaderValue::from_static("upgrade"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_api::dispatch::{EndpointPort, NamespacedRoute, ServiceEndpoints};
    use legion_api::route::RouteSpec;
    use std::sync::atomic::AtomicU32;
    use tokio::net::TcpStream;

    /// A backend answering with its name, and the headers it was sent.
    async fn backend(name: &'static str) -> u16 {
        counted_backend(name).await.0
    }

    /// [`backend`], with the connections made to it so far.
    async fn counted_backend(name: &'static str) -> (u16, Arc<AtomicU32>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicU32::new(0));
        let counted = connections.clone();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let svc =
                        hyper::service::service_fn(move |req: Request<Incoming>| async move {
                            let h = |k: &str| {
                                req.headers()
                                    .get(k)
                                    .map_or("-".to_owned(), |v| v.to_str().unwrap().to_owned())
                            };
                            let mut body = format!(
                                "{name} {} host={} xff={} xfh={}",
                                req.uri(),
                                h("host"),
                                h("x-forwarded-for"),
                                h("x-forwarded-host")
                            );
                            // Only when there, as they should not be.
                            for k in ["connection", "keep-alive"] {
                                if req.headers().contains_key(k) {
                                    body.push_str(&format!(" {k}={}", h(k)));
                                }
                            }
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(body))))
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(s), svc)
                        .await;
                });
            }
        });
        (port, connections)
    }

    fn route(name: &str, host: &str, path: &str, service: &str) -> NamespacedRoute {
        NamespacedRoute {
            namespace: "shop".into(),
            name: name.into(),
            spec: RouteSpec {
                host: host.into(),
                path: path.into(),
                service: service.into(),
                port: None,
                tls: None,
            },
            challenges: Default::default(),
        }
    }

    fn service(name: &str, target: u16, ready: Vec<Ipv4Addr>) -> ServiceEndpoints {
        ServiceEndpoints {
            namespace: "shop".into(),
            name: name.into(),
            uid: name.into(),
            labels: Default::default(),
            vip: None,
            ports: vec![EndpointPort {
                protocol: Protocol::Tcp,
                port: 80,
                target,
                published: None,
            }],
            ready: ready.clone(),
            alive: ready,
            ordinals: Default::default(),
            job: false,
        }
    }

    async fn get(proxy: SocketAddr, host: &str, path: &str) -> (StatusCode, String) {
        let stream = TcpStream::connect(proxy).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn);
        let req = Request::get(path)
            .header(HOST, host)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).trim().to_owned())
    }

    #[tokio::test]
    async fn every_forwarded_for_line_is_kept() {
        let web = backend("web").await;
        let (_routes_tx, routes) =
            watch::channel(vec![route("www", "shop.example.org", "/", "web")]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("web", web, vec![Ipv4Addr::LOCALHOST])]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        let stream = TcpStream::connect(proxy).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn);
        let req = Request::get("/")
            .header(HOST, "shop.example.org")
            .header("x-forwarded-for", "10.0.0.1, 10.0.0.2")
            .header("x-forwarded-for", "10.0.0.3")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(
            String::from_utf8_lossy(&body).contains("xff=10.0.0.1, 10.0.0.2, 10.0.0.3, 127.0.0.1 "),
            "{body:?}"
        );
    }

    /// What the client means for its connection to the proxy alone does
    /// not reach the backend: above all no header it names to drop the
    /// proxy's own.
    #[tokio::test]
    async fn connection_headers_stay_with_their_connection() {
        let web = backend("web").await;
        let (_routes_tx, routes) =
            watch::channel(vec![route("www", "shop.example.org", "/", "web")]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("web", web, vec![Ipv4Addr::LOCALHOST])]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        let stream = TcpStream::connect(proxy).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn);
        let req = Request::get("/")
            .header(HOST, "shop.example.org")
            .header(
                "connection",
                "keep-alive, X-Forwarded-For, x-forwarded-host",
            )
            .header("keep-alive", "timeout=5")
            .header("x-forwarded-for", "10.9.9.9")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&body);
        // The client's own forwarded-for was named, so it goes; the
        // proxy's stay.
        assert_eq!(
            body,
            "web / host=shop.example.org xff=127.0.0.1 xfh=shop.example.org"
        );
    }

    #[tokio::test]
    async fn requests_go_to_the_routed_service() {
        let (web, api) = (backend("web").await, backend("api").await);
        let local = Ipv4Addr::LOCALHOST;
        let (routes_tx, routes) = watch::channel(vec![
            route("www", "shop.example.org", "/", "web"),
            route("api", "shop.example.org", "/api", "api"),
            route("any", "*.example.org", "/", "api"),
            route("down", "down.example.org", "/", "down"),
        ]);
        let (endpoints_tx, endpoints) = watch::channel(vec![
            service("web", web, vec![local]),
            service("api", api, vec![local]),
            service("down", 1, vec![]),
        ]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        let (status, body) = get(proxy, "Shop.Example.org:8080", "/cart?x=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "web /cart?x=1 host=Shop.Example.org:8080 xff=127.0.0.1 xfh=shop.example.org"
        );
        assert!(
            get(proxy, "shop.example.org", "/api/orders")
                .await
                .1
                .starts_with("api /api/orders")
        );
        assert!(
            get(proxy, "shop.example.org", "/apis")
                .await
                .1
                .starts_with("web ")
        );
        assert!(
            get(proxy, "www.example.org", "/")
                .await
                .1
                .starts_with("api ")
        );
        assert_eq!(
            get(proxy, "example.com", "/").await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(proxy, "down.example.org", "/").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        // Routes and endpoints follow their streams.
        routes_tx.send_modify(|r| r.retain(|r| r.name != "api"));
        assert!(
            get(proxy, "shop.example.org", "/api/orders")
                .await
                .1
                .starts_with("web ")
        );
        endpoints_tx.send_modify(|e| e[0].ready.clear());
        assert_eq!(
            get(proxy, "shop.example.org", "/").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        // A ready task that does not answer.
        endpoints_tx.send_modify(|e| {
            e[0].ready = vec![local];
            e[0].ports[0].target = 1;
        });
        assert_eq!(
            get(proxy, "shop.example.org", "/").await.0,
            StatusCode::BAD_GATEWAY
        );
    }

    #[tokio::test]
    async fn acme_challenges_answered_for_their_host() {
        let web = backend("web").await;
        let mut shop = route("www", "shop.example.org", "/", "web");
        shop.challenges
            .insert("tok-1".into(), "tok-1.thumbprint".into());
        let (_routes_tx, routes) =
            watch::channel(vec![shop, route("other", "other.example.org", "/", "web")]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("web", web, vec![Ipv4Addr::LOCALHOST])]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        let path = "/.well-known/acme-challenge/tok-1";
        assert_eq!(
            get(proxy, "Shop.example.org:80", path).await,
            (StatusCode::OK, "tok-1.thumbprint".to_owned())
        );
        // Other tokens, and the token on another host, go to the backend.
        let (status, body) = get(
            proxy,
            "shop.example.org",
            "/.well-known/acme-challenge/tok-2",
        )
        .await;
        assert_eq!(
            (status, body.split(' ').next()),
            (StatusCode::OK, Some("web"))
        );
        let (status, body) = get(proxy, "other.example.org", path).await;
        assert_eq!(
            (status, body.split(' ').next()),
            (StatusCode::OK, Some("web"))
        );
    }

    #[tokio::test]
    async fn http2_clients_and_backend_connections_kept() {
        let (web, connections) = counted_backend("web").await;
        let (_routes_tx, routes) =
            watch::channel(vec![route("www", "shop.example.org", "/", "web")]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("web", web, vec![Ipv4Addr::LOCALHOST])]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        // HTTP/1.1 requests on connections of their own share one to the
        // backend.
        for _ in 0..3 {
            let (status, body) = get(proxy, "shop.example.org", "/").await;
            assert_eq!(
                (status, body.split(' ').next()),
                (StatusCode::OK, Some("web"))
            );
        }
        assert_eq!(connections.load(Ordering::SeqCst), 1);

        // HTTP/2 in the clear, its host in its URI: the backend gets it as
        // `Host`.
        let stream = TcpStream::connect(proxy).await.unwrap();
        let (mut sender, conn) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(conn);
        let req = Request::get("http://shop.example.org/h2?x=1")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        assert_eq!(
            (resp.status(), resp.version()),
            (StatusCode::OK, hyper::Version::HTTP_2)
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            String::from_utf8_lossy(&body),
            "web /h2?x=1 host=shop.example.org xff=127.0.0.1 xfh=shop.example.org"
        );
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    /// A backend that switches to echoing whatever comes.
    async fn echo_backend() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (s, _) = listener.accept().await.unwrap();
            let svc = hyper::service::service_fn(|mut req: Request<Incoming>| async move {
                tokio::spawn(async move {
                    let up = hyper::upgrade::on(&mut req).await.unwrap();
                    let mut io = TokioIo::new(up);
                    let (mut r, mut w) = tokio::io::split(&mut io);
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
                let resp = Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header("upgrade", "echo")
                    .header("connection", "upgrade")
                    .body(Full::new(Bytes::new()))
                    .unwrap();
                Ok::<_, Infallible>(resp)
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(s), svc)
                .with_upgrades()
                .await;
        });
        port
    }

    #[tokio::test]
    async fn upgrades_pass_through() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let port = echo_backend().await;
        let (_routes_tx, routes) = watch::channel(vec![route("ws", "ws.example.org", "/", "ws")]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("ws", port, vec![Ipv4Addr::LOCALHOST])]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, routes, endpoints));

        let stream = TcpStream::connect(proxy).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(conn.with_upgrades());
        let req = Request::get("/socket")
            .header(HOST, "ws.example.org")
            .header("upgrade", "echo")
            .header("connection", "upgrade")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        let mut io = TokioIo::new(hyper::upgrade::on(resp).await.unwrap());
        io.write_all(b"ping").await.unwrap();
        let mut got = [0u8; 4];
        io.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"ping");
    }

    /// A self-signed certificate for `names`, with its key, in PEM.
    fn certificate(names: &[&str]) -> (String, CertificateDer<'static>) {
        let names: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
        let c = rcgen::generate_simple_self_signed(names).unwrap();
        let pem = format!("{}{}", c.cert.pem(), c.signing_key.serialize_pem());
        (pem, c.cert.der().clone())
    }

    /// The certificate the proxy shows for `sni`, and the response to a GET.
    async fn get_tls(
        proxy: SocketAddr,
        sni: &str,
        trusted: &[CertificateDer<'static>],
    ) -> Result<String, String> {
        get_tls_alpn(proxy, sni, trusted, false).await
    }

    /// [`get_tls`], over HTTP/2 if `h2`, which ALPN agrees.
    async fn get_tls_alpn(
        proxy: SocketAddr,
        sni: &str,
        trusted: &[CertificateDer<'static>],
        h2: bool,
    ) -> Result<String, String> {
        use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
        let mut roots = RootCertStore::empty();
        for c in trusted {
            roots.add(c.clone()).unwrap();
        }
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        if h2 {
            config.alpn_protocols = vec![b"h2".to_vec()];
        }
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = TcpStream::connect(proxy).await.unwrap();
        let name = ServerName::try_from(sni.to_owned()).unwrap();
        let tls = connector
            .connect(name, tcp)
            .await
            .map_err(|e| e.to_string())?;
        if h2 {
            assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
            let (mut sender, conn) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
                    .await
                    .map_err(|e| e.to_string())?;
            tokio::spawn(conn);
            let req = Request::get(format!("https://{sni}/"))
                .body(Full::new(Bytes::new()))
                .unwrap();
            let resp = sender.send_request(req).await.map_err(|e| e.to_string())?;
            assert_eq!(resp.version(), hyper::Version::HTTP_2);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            return Ok(String::from_utf8_lossy(&body).trim().to_owned());
        }
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(conn);
        let req = Request::get("/")
            .header(HOST, sni)
            .header("x-forwarded-proto", "made-up")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.map_err(|e| e.to_string())?;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        Ok(String::from_utf8_lossy(&body).trim().to_owned())
    }

    #[tokio::test]
    async fn https_with_each_route_certificate() {
        let web = backend("web").await;
        let mut shop = route("www", "shop.example.org", "/", "web");
        shop.spec.tls = Some(legion_api::route::RouteTls {
            secret: "shop-cert".into(),
            acme: None,
        });
        let mut any = route("any", "*.example.org", "/", "web");
        any.spec.tls = Some(legion_api::route::RouteTls {
            secret: "wild-cert".into(),
            acme: None,
        });
        let (routes_tx, routes) = watch::channel(vec![shop, any]);
        let (_endpoints_tx, endpoints) =
            watch::channel(vec![service("web", web, vec![Ipv4Addr::LOCALHOST])]);
        let (shop_pem, shop_der) = certificate(&["shop.example.org"]);
        let (wild_pem, wild_der) = certificate(&["*.example.org"]);
        let secrets = Arc::new(std::sync::Mutex::new(BTreeMap::from([
            ("shop-cert".to_owned(), shop_pem.into_bytes()),
            ("wild-cert".to_owned(), wild_pem.into_bytes()),
            ("broken".to_owned(), b"not PEM".to_vec()),
        ])));
        let certificates: Arc<Certificates> = Arc::default();
        let read = {
            let secrets = secrets.clone();
            move |namespace: String, name: String| {
                let found = secrets.lock().unwrap().get(&name).cloned();
                async move {
                    assert_eq!(namespace, "shop");
                    Ok(found)
                }
            }
        };
        tokio::spawn(follow_certificates(
            routes.clone(),
            certificates.clone(),
            read,
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        tokio::spawn(serve_tls(listener, routes, endpoints, certificates.clone()));

        // Each host gets its own certificate, a wildcard the rest.
        let mut got = Err(String::new());
        for _ in 0..50 {
            got = get_tls(proxy, "shop.example.org", std::slice::from_ref(&shop_der)).await;
            if got.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let body = got.unwrap();
        assert!(body.starts_with("web / host=shop.example.org"), "{body}");
        let body = get_tls_alpn(
            proxy,
            "shop.example.org",
            std::slice::from_ref(&shop_der),
            true,
        )
        .await
        .unwrap();
        assert!(body.starts_with("web / host=shop.example.org"), "{body}");
        assert!(
            get_tls(proxy, "www.example.org", std::slice::from_ref(&wild_der))
                .await
                .is_ok()
        );
        assert!(
            get_tls(proxy, "www.example.org", std::slice::from_ref(&shop_der))
                .await
                .is_err(),
            "the wildcard's certificate, not the shop's"
        );
        assert!(
            get_tls(
                proxy,
                "other.example.com",
                &[shop_der.clone(), wild_der.clone()]
            )
            .await
            .is_err(),
            "no certificate for a host without a route"
        );

        // A renewed certificate takes over once read again; a broken one
        // keeps the last good one.
        let (renewed_pem, renewed_der) = certificate(&["shop.example.org"]);
        secrets
            .lock()
            .unwrap()
            .insert("shop-cert".into(), renewed_pem.into_bytes());
        routes_tx.send_modify(|_| {});
        let mut renewed = false;
        for _ in 0..50 {
            if get_tls(
                proxy,
                "shop.example.org",
                std::slice::from_ref(&renewed_der),
            )
            .await
            .is_ok()
            {
                renewed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(renewed);
        routes_tx.send_modify(|r| r[0].spec.tls.as_mut().unwrap().secret = "broken".into());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            get_tls(
                proxy,
                "shop.example.org",
                std::slice::from_ref(&renewed_der)
            )
            .await
            .is_ok()
        );

        // A route gone takes its certificate with it.
        routes_tx.send_modify(|r| r.retain(|r| r.name != "www"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            certificates
                .get("shop.example.org")
                .is_some_and(|c| { c.cert[0] == wild_der }),
            "only the wildcard is left for the shop"
        );
    }
}
