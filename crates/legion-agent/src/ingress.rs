//! The ingress proxy (DESIGN.md §5.8): HTTP on every node that enables it,
//! each request sent to a ready task of the service its route names, by
//! the task's address and port. Routes match the `Host` header and the
//! longest path prefix, an exact host before a wildcard; tasks take turns.
//! The backend sees the client in `X-Forwarded-For`. Upgrades (WebSocket)
//! pass through.

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{HOST, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use legion_api::dispatch::{Endpoints, Routes};
use legion_api::service::Protocol;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

type Body = BoxBody<Bytes, hyper::Error>;

/// How long a backend has to accept the connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

struct Proxy {
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
    turn: AtomicUsize,
}

/// Serves `listener` until the task is dropped.
pub async fn serve(
    listener: TcpListener,
    routes: watch::Receiver<Routes>,
    endpoints: watch::Receiver<Endpoints>,
) {
    let proxy = Arc::new(Proxy {
        routes,
        endpoints,
        turn: AtomicUsize::new(0),
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
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let proxy = proxy.clone();
                async move { Ok::<_, Infallible>(proxy.handle(req, client).await) }
            });
            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades();
            if let Err(e) = conn.await {
                tracing::debug!(%client, "ingress connection: {e}");
            }
        });
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
            .find(|s| s.namespace == route.namespace && s.name == route.spec.service)
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
            .or_else(|| req.uri().host())
            .unwrap_or_default();
        let host = host
            .rsplit_once(':')
            .filter(|(_, port)| port.bytes().all(|b| b.is_ascii_digit()))
            .map_or(host, |(name, _)| name)
            .to_ascii_lowercase();
        let backend = match self.backend(&host, req.uri().path()) {
            Ok(b) => b,
            Err((status, why)) => return reply(status, &why),
        };

        let headers = req.headers_mut();
        let forwarded = match headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            Some(earlier) => format!("{earlier}, {}", client.ip()),
            None => client.ip().to_string(),
        };
        if let Ok(v) = HeaderValue::from_str(&forwarded) {
            headers.insert("x-forwarded-for", v);
        }
        if let Ok(v) = HeaderValue::from_str(&host) {
            headers.insert("x-forwarded-host", v);
        }
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        // Only the path goes on to the backend.
        if let Some(pq) = req.uri().path_and_query().cloned() {
            *req.uri_mut() = pq.as_str().parse().expect("a path and query parse");
        }

        let stream = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(backend)).await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return reply(StatusCode::BAD_GATEWAY, &format!("{backend}: {e}")),
            Err(_) => {
                return reply(
                    StatusCode::GATEWAY_TIMEOUT,
                    &format!("{backend}: timed out"),
                );
            }
        };
        let (mut sender, conn) =
            match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
                Ok(c) => c,
                Err(e) => return reply(StatusCode::BAD_GATEWAY, &format!("{backend}: {e}")),
            };
        tokio::spawn(async move {
            if let Err(e) = conn.with_upgrades().await {
                tracing::debug!(%backend, "ingress backend connection: {e}");
            }
        });

        let upgrade = req
            .headers()
            .contains_key(hyper::header::UPGRADE)
            .then(|| hyper::upgrade::on(&mut req));
        let mut resp = match sender.send_request(req).await {
            Ok(r) => r,
            Err(e) => return reply(StatusCode::BAD_GATEWAY, &format!("{backend}: {e}")),
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
        }
        resp.map(BodyExt::boxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_api::dispatch::{EndpointPort, NamespacedRoute, ServiceEndpoints};
    use legion_api::route::RouteSpec;

    /// A backend answering with its name, and the headers it was sent.
    async fn backend(name: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (s, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let svc =
                        hyper::service::service_fn(move |req: Request<Incoming>| async move {
                            let h = |k: &str| {
                                req.headers()
                                    .get(k)
                                    .map_or("-".to_owned(), |v| v.to_str().unwrap().to_owned())
                            };
                            let body = format!(
                                "{name} {} host={} xff={} xfh={}",
                                req.uri(),
                                h("host"),
                                h("x-forwarded-for"),
                                h("x-forwarded-host")
                            );
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(body))))
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(s), svc)
                        .await;
                });
            }
        });
        port
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
            },
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
}
