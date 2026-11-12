//! The web console's gateway (DESIGN.md §3.9): HTTPS for browsers, serving
//! the console's pages and turning their requests into API calls made as
//! the signed-in user. Each session gets a short-lived certificate for the
//! user's identity, issued here and held in memory only, and its calls go
//! to this manager's gRPC port over mutual TLS with it: the API checks them
//! as it checks the CLI's. The gateway decides nothing itself.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use legion_pki::{Identity, Role};
use legion_proto::v1 as pb;
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tonic::transport::Channel;

use crate::Credentials;
use crate::api::ApiService;
use crate::raft_net::connect_lazy;

/// Where and how a manager serves the console (`--console-listen`).
#[derive(Debug, Clone)]
pub struct Console {
    pub listen: SocketAddr,
    /// A certificate and key for browsers to trust, PEM; the manager's own
    /// when not given.
    pub cert_pem: Option<String>,
    pub key_pem: Option<String>,
}

const COOKIE: &str = "legion_session";
/// How long a session's certificate lasts; renewed before it ends.
const USER_CERT: Duration = Duration::from_secs(3600);
const RENEW_BEFORE: Duration = Duration::from_secs(600);
/// A client that never finishes its TLS handshake is let go.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to wait for a session just made on the leader to reach this
/// manager's replica.
const SESSION_LAG: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Gateway(Arc<Inner>);

struct Inner {
    api: ApiService,
    peer: Arc<Credentials>,
    /// Each session's channel as its user, by session.
    users: Mutex<HashMap<String, User>>,
}

struct User {
    channel: Channel,
    renew_at: Instant,
}

/// Binds `console.listen`, then serves the console there until the
/// manager closes.
pub(crate) async fn bind(console: &Console) -> Result<TcpListener, crate::Error> {
    TcpListener::bind(console.listen)
        .await
        .map_err(|e| format!("the console's address {}: {e}", console.listen).into())
}

pub(crate) async fn run(listener: TcpListener, console: Console, api: ApiService) {
    let Some(peer) = api.peer_tls.clone() else {
        tracing::warn!("the console needs TLS; not serving it");
        return;
    };
    if api.issuer.is_none() {
        tracing::warn!("the console needs the CA key to certify its sessions; not serving it");
        return;
    }
    let acceptor = match tls(&console, &peer) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("the console's certificate: {e}; not serving it");
            return;
        }
    };
    let mut closing = api.closing.clone();
    let gateway = Gateway(Arc::new(Inner {
        api,
        peer,
        users: Default::default(),
    }));
    let router = router(gateway);
    tracing::info!(listen = %console.listen, built = legion_console::built(), "serving the console");
    loop {
        let accepted = tokio::select! {
            a = listener.accept() => a,
            _ = closing.wait_for(|c| *c) => return,
        };
        let (stream, client) = match accepted {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("console: accepting: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (acceptor, router) = (acceptor.clone(), router.clone());
        tokio::spawn(async move {
            let stream =
                match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return tracing::debug!(%client, "console TLS handshake: {e}"),
                    Err(_) => return tracing::debug!(%client, "console TLS handshake timed out"),
                };
            let service = hyper_util::service::TowerToHyperService::new(router);
            let builder =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            if let Err(e) = builder
                .serve_connection_with_upgrades(hyper_util::rt::TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%client, "console connection: {e}");
            }
        });
    }
}

fn tls(console: &Console, peer: &Credentials) -> Result<TlsAcceptor, String> {
    let (cert_pem, key_pem) = match (&console.cert_pem, &console.key_pem) {
        (Some(c), Some(k)) => (c.clone(), k.clone()),
        _ => peer.cert_and_key(),
    };
    let chain = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("reading the certificate: {e}"))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| format!("reading the key: {e}"))?;
    let mut config = ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .map_err(|e| e.to_string())?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn router(gateway: Gateway) -> axum::Router {
    axum::Router::new()
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/status", get(status))
        .route("/api/kinds", get(kinds))
        .route("/api/objects/{kind}", get(list))
        .route("/api/objects/{kind}/{namespace}/{name}", get(object))
        .route("/api/watch/{kind}", get(watch))
        .fallback(page)
        .layer(axum::middleware::map_response(security_headers))
        .with_state(gateway)
}

async fn security_headers(mut res: Response) -> Response {
    let h = res.headers_mut();
    let set = |h: &mut HeaderMap, name: &'static str, value: &'static str| {
        h.insert(name, HeaderValue::from_static(value));
    };
    set(h, "x-content-type-options", "nosniff");
    set(h, "referrer-policy", "no-referrer");
    set(h, "x-frame-options", "DENY");
    set(
        h,
        "content-security-policy",
        "default-src 'self'; connect-src 'self'; img-src 'self' data:; \
         style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; \
         frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
    );
    set(h, "strict-transport-security", "max-age=31536000");
    res
}

/// An API refusal, as the browser sees it.
struct Refused(StatusCode, String);

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl From<tonic::Status> for Refused {
    fn from(s: tonic::Status) -> Refused {
        use tonic::Code;
        let code = match s.code() {
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            Code::PermissionDenied => StatusCode::FORBIDDEN,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::InvalidArgument | Code::OutOfRange => StatusCode::BAD_REQUEST,
            Code::AlreadyExists | Code::Aborted | Code::FailedPrecondition => StatusCode::CONFLICT,
            Code::Unavailable | Code::DeadlineExceeded => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Refused(code, s.message().to_owned())
    }
}

fn unauthorized() -> Refused {
    Refused(StatusCode::UNAUTHORIZED, "not signed in".into())
}

/// The session the browser's cookie names.
fn cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|c| c.trim().split_once('='))
        .find(|(k, _)| *k == COOKIE)
        .map(|(_, v)| v.to_owned())
        .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Refuses a request from another site's page: its `Origin` must be this
/// host. (The cookie is `SameSite=Strict` too; this covers WebSockets.)
/// HTTP/2 has no `Host` header: the host is the URI's authority there.
fn same_origin(headers: &HeaderMap, uri: &Uri) -> Result<(), Refused> {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return Ok(());
    };
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()));
    let origin_host = origin
        .to_str()
        .ok()
        .and_then(|o| o.strip_prefix("https://"));
    match (origin_host, host) {
        (Some(o), Some(h)) if o == h => Ok(()),
        _ => Err(Refused(
            StatusCode::FORBIDDEN,
            "cross-origin request".into(),
        )),
    }
}

impl Gateway {
    fn manager(&self) -> Channel {
        connect_lazy(&self.0.api.advertise, Some(&self.0.peer))
    }

    /// The signed-in user's channel, with a certificate for them made or
    /// renewed as needed.
    async fn user(&self, headers: &HeaderMap) -> Result<(String, Channel), Refused> {
        let session = cookie(headers).ok_or_else(unauthorized)?;
        let deadline = Instant::now() + SESSION_LAG;
        let signed = loop {
            match crate::console::session(&self.0.api.store, &session)? {
                Some(s) => break s,
                None if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                None => {
                    self.0.users.lock().unwrap().remove(&session);
                    return Err(unauthorized());
                }
            }
        };
        let now = Instant::now();
        if let Some(u) = self.0.users.lock().unwrap().get(&session)
            && u.renew_at > now
        {
            return Ok((session, u.channel.clone()));
        }
        let role = Role::parse(&signed.role)
            .filter(|r| matches!(r, Role::Admin | Role::User))
            .ok_or_else(unauthorized)?;
        let left =
            Duration::from_millis((signed.expires_ms - crate::console::now_ms()).max(0) as u64);
        let lifetime = USER_CERT.min(left);
        let ca = self
            .0
            .api
            .issuer
            .as_ref()
            .ok_or_else(|| Refused(StatusCode::SERVICE_UNAVAILABLE, "no CA here".into()))?
            .current()?;
        let issued = ca
            .issue(&Identity::new(&signed.name, role), lifetime)
            .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let creds = Credentials::new(&self.0.peer.ca_pem(), &issued.cert_pem, &issued.key_pem)
            .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let channel = connect_lazy(&self.0.api.advertise, Some(&creds));
        let mut users = self.0.users.lock().unwrap();
        users.retain(|_, u| u.renew_at + RENEW_BEFORE > now);
        users.insert(
            session.clone(),
            User {
                channel: channel.clone(),
                renew_at: now + lifetime.saturating_sub(RENEW_BEFORE),
            },
        );
        Ok((session, channel))
    }
}

#[derive(Deserialize)]
struct LoginBody {
    code: String,
}

async fn login(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<LoginBody>,
) -> Result<Response, Refused> {
    same_origin(&headers, &uri)?;
    let s = ClusterClient::new(g.manager())
        .console_session(pb::ConsoleSessionRequest { code: body.code })
        .await?
        .into_inner();
    let max_age = ((s.expires_ms - crate::console::now_ms()) / 1000).max(0);
    let cookie = format!(
        "{COOKIE}={}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age={max_age}",
        s.session
    );
    let mut res = Json(serde_json::json!({
        "name": s.name, "role": s.role, "expires_ms": s.expires_ms,
    }))
    .into_response();
    res.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).expect("hex and ASCII"),
    );
    Ok(res)
}

async fn logout(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, Refused> {
    same_origin(&headers, &uri)?;
    if let Some(session) = cookie(&headers) {
        g.0.users.lock().unwrap().remove(&session);
        ClusterClient::new(g.manager())
            .console_end(pb::ConsoleEndRequest { session })
            .await?;
    }
    let mut res = StatusCode::NO_CONTENT.into_response();
    res.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "legion_session=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0",
        ),
    );
    Ok(res)
}

async fn me(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let me = ClusterClient::new(channel)
        .me(pb::MeRequest {})
        .await?
        .into_inner();
    Ok(Json(serde_json::json!({
        "name": me.name, "role": me.role, "all": me.all, "namespaces": me.namespaces,
    }))
    .into_response())
}

async fn status(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let s = ClusterClient::new(channel)
        .status(pb::StatusRequest {})
        .await?
        .into_inner();
    let members: Vec<_> = s
        .members
        .iter()
        .map(|m| serde_json::json!({ "id": m.id.to_string(), "addr": m.addr, "voter": m.voter }))
        .collect();
    Ok(Json(serde_json::json!({
        "node": s.node_id.to_string(),
        "name": s.name,
        "leader": s.leader_id.map(|l| l.to_string()),
        "state": s.state,
        "term": s.term,
        "version": s.applied_version,
        "protocol": s.protocol,
        "members": members,
    }))
    .into_response())
}

async fn kinds() -> Response {
    let kinds: Vec<_> = legion_api::Kind::ALL
        .iter()
        .map(|k| serde_json::json!({ "kind": k.as_str(), "clusterScoped": k.is_cluster_scoped() }))
        .collect();
    Json(kinds).into_response()
}

#[derive(Deserialize)]
struct Scope {
    namespace: Option<String>,
}

fn object_json(o: pb::Object) -> Result<serde_json::Value, Refused> {
    let o = legion_api::Object::try_from(o)
        .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(serde_json::to_value(o).expect("serialises"))
}

async fn list(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Path(kind): Path<String>,
    Query(scope): Query<Scope>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ObjectsClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .list(pb::ListRequest {
            kind,
            namespace: scope.namespace.unwrap_or_default(),
            match_labels: Default::default(),
        })
        .await?
        .into_inner();
    let objects = r
        .objects
        .into_iter()
        .map(object_json)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(serde_json::json!({ "objects": objects, "version": r.version })).into_response())
}

async fn object(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Path((kind, namespace, name)): Path<(String, String, String)>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    // `_` for a cluster-scoped object's namespace.
    let namespace = if namespace == "_" {
        String::new()
    } else {
        namespace
    };
    let o = ObjectsClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .get(pb::ObjectRef {
            kind,
            namespace,
            name,
        })
        .await?
        .into_inner();
    Ok(Json(object_json(o)?).into_response())
}

#[derive(Deserialize)]
struct WatchFrom {
    namespace: Option<String>,
    from: Option<u64>,
}

/// The changes to objects of `kind` from version `from` on, as JSON text
/// messages `{type: "put"|"delete", object}`, until either side closes;
/// an error ends it with `{error}`.
async fn watch(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Path(kind): Path<String>,
    Query(from): Query<WatchFrom>,
    ws: WebSocketUpgrade,
) -> Result<Response, Refused> {
    same_origin(&headers, &uri)?;
    let (_, channel) = g.user(&headers).await?;
    let stream = ObjectsClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .watch(pb::WatchRequest {
            kind,
            from_version: from.from.unwrap_or(0),
        })
        .await?
        .into_inner();
    let namespace = from.namespace.filter(|n| !n.is_empty());
    Ok(ws.on_upgrade(move |socket| relay(socket, stream, namespace)))
}

async fn relay(
    mut socket: WebSocket,
    mut stream: tonic::Streaming<pb::WatchEvent>,
    namespace: Option<String>,
) {
    loop {
        let event = tokio::select! {
            e = stream.message() => e,
            m = socket.recv() => match m {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => continue,
            },
        };
        let text = match event {
            Ok(Some(e)) => {
                let Some(object) = e.object else { continue };
                if let Some(ns) = &namespace
                    && &object.namespace != ns
                {
                    continue;
                }
                let kind = if e.r#type == pb::watch_event::Type::Delete as i32 {
                    "delete"
                } else {
                    "put"
                };
                match object_json(object) {
                    Ok(o) => serde_json::json!({ "type": kind, "object": o }),
                    Err(Refused(_, e)) => serde_json::json!({ "error": e }),
                }
            }
            Ok(None) => serde_json::json!({ "error": "the watch ended" }),
            Err(s) => serde_json::json!({ "error": s.message() }),
        };
        let end = text.get("error").is_some();
        if socket
            .send(Message::Text(text.to_string().into()))
            .await
            .is_err()
            || end
        {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    }
}

/// The console's pages: a file of the build, or the app's page for any
/// other path (its router takes it from there).
async fn page(uri: Uri) -> Result<Response, Infallible> {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("api/") {
        return Ok(Refused(StatusCode::NOT_FOUND, "no such API call".into()).into_response());
    }
    if let Some((bytes, media)) = legion_console::asset(path) {
        let cache = if path.starts_with("_app/immutable/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        return Ok((
            [
                (header::CONTENT_TYPE, media),
                (header::CACHE_CONTROL, cache),
            ],
            bytes,
        )
            .into_response());
    }
    match legion_console::asset("index.html") {
        Some((bytes, media)) => Ok((
            [
                (header::CONTENT_TYPE, media),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            bytes,
        )
            .into_response()),
        None => Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            "<!doctype html><title>Legion</title><p>This build of Legion has no console pages: \
             it was built without them (see crates/legion-console).</p>",
        )
            .into_response()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_session_comes_from_its_cookie_only() {
        let session = "a".repeat(64);
        let mut h = HeaderMap::new();
        h.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("other=1; {COOKIE}={session}")).unwrap(),
        );
        assert_eq!(cookie(&h), Some(session));
        let mut bad = HeaderMap::new();
        bad.insert(
            header::COOKIE,
            HeaderValue::from_static("legion_session=../../etc"),
        );
        assert_eq!(cookie(&bad), None);
    }

    #[test]
    fn another_sites_page_is_refused() {
        // HTTP/1.1: a path in the URI, the host in Host.
        let path = Uri::from_static("/api/login");
        let mut h = HeaderMap::new();
        h.insert(
            header::HOST,
            HeaderValue::from_static("console.example:7480"),
        );
        assert!(
            same_origin(&h, &path).is_ok(),
            "no Origin: not a browser's cross-site request"
        );
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example:7480"),
        );
        assert!(same_origin(&h, &path).is_ok());
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(same_origin(&h, &path).is_err());
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://console.example:7480"),
        );
        assert!(
            same_origin(&h, &path).is_err(),
            "plain HTTP is another origin"
        );
    }

    #[test]
    fn over_http2_the_host_is_the_uris_authority() {
        let uri = Uri::from_static("https://console.example:7480/api/login");
        let mut h = HeaderMap::new();
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://console.example:7480"),
        );
        assert!(same_origin(&h, &uri).is_ok());
        h.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(same_origin(&h, &uri).is_err());
        assert!(
            same_origin(&h, &Uri::from_static("/api/login")).is_err(),
            "no host at all"
        );
    }
}
