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
    /// Single sign-on's client (CONSOLE.md phase 6).
    sso: crate::oidc::Provider,
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
        sso: crate::oidc::Provider::new(),
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
        .route("/api/oidc/start", get(oidc_start))
        .route("/api/oidc/callback", get(oidc_callback))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/status", get(status))
        .route("/api/kinds", get(kinds))
        .route("/api/objects/{kind}", get(list))
        .route(
            "/api/objects/{kind}/{namespace}/{name}",
            get(object).delete(delete_object),
        )
        .route("/api/watch/{kind}", get(watch))
        .route("/api/usage", get(usage))
        .route("/api/alerts", get(alerts))
        .route("/api/usage-history", get(usage_history))
        .route("/api/quotas", get(quotas))
        .route("/api/log-search", get(log_search))
        .route("/api/reach", get(reach))
        .route("/api/logs/{namespace}/{task}", get(logs))
        .route("/api/build-logs/{namespace}/{name}", get(build_logs))
        // Writes (CONSOLE.md phases 2 and 3): each from the console's own
        // page with the session's token, as the user.
        .route("/api/manifest/{kind}/{namespace}/{name}", get(manifest))
        .route("/api/apply", post(apply))
        .route("/api/compose", post(compose))
        .route(
            "/api/console-settings",
            get(console_settings).post(set_console_settings),
        )
        .route("/api/templates", get(templates))
        .route("/api/plugins", get(plugins))
        .route("/api/plugin/{namespace}/{name}", get(plugin))
        .route("/api/set", post(set_field))
        .route("/api/labels", post(labels))
        .route("/api/rollback", post(rollback))
        .route("/api/node-role", post(node_role))
        .route("/api/exec/{namespace}/{task}", get(exec))
        // Cluster operations (CONSOLE.md phase 3); the API lets only
        // admins do most of them.
        .route("/api/audit", get(audit))
        .route("/api/join-token", post(join_token))
        .route("/api/ca", get(ca_status).post(ca_rotate))
        .route("/api/autolock", post(autolock))
        .route("/api/users", post(create_user))
        .route("/api/backup", get(store_backup))
        .route("/api/volume-backup/{namespace}/{name}", get(volume_backup))
        .route("/api/volume-restore", post(volume_restore))
        .fallback(page)
        .layer(axum::middleware::map_response_with_state(
            gateway.clone(),
            security_headers,
        ))
        .with_state(gateway)
}

/// The console's pages' policy, the other clusters' consoles that
/// settings list added to where they may connect (the Clusters page).
fn page_policy(peers: &str) -> String {
    format!(
        "default-src 'self'; connect-src 'self'{peers}; img-src 'self' data:; \
         style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; \
         frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    )
}

/// What every answer carries, unless it set its own policy (a plugin's
/// page, CONSOLE.md phase 6).
async fn security_headers(State(g): State<Gateway>, mut res: Response) -> Response {
    let html = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/html"));
    let h = res.headers_mut();
    let set = |h: &mut HeaderMap, name: &'static str, value: &'static str| {
        h.insert(name, HeaderValue::from_static(value));
    };
    set(h, "x-content-type-options", "nosniff");
    set(h, "referrer-policy", "no-referrer");
    set(h, "strict-transport-security", "max-age=31536000");
    if h.contains_key(header::CONTENT_SECURITY_POLICY) {
        return res;
    }
    set(h, "x-frame-options", "DENY");
    let peers: String = match html.then(|| crate::console::settings(&g.0.api.store)) {
        Some(Ok(s)) => s
            .clusters
            .iter()
            .filter_map(|p| p.origin())
            .map(|o| format!(" {o}"))
            .collect(),
        _ => String::new(),
    };
    // The origins were checked when set; should one not make a header
    // value, the policy is the console's alone.
    let policy = HeaderValue::from_str(&page_policy(&peers))
        .or_else(|_| HeaderValue::from_str(&page_policy("")))
        .expect("ASCII");
    h.insert(header::CONTENT_SECURITY_POLICY, policy);
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
    cookie_named(headers, COOKIE)
        .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn cookie_named(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|c| c.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

/// The header a write carries its session's token in.
const CSRF_HEADER: &str = "x-legion-csrf";

/// The token a session's writes carry (DESIGN.md §3.9): derived from the
/// session, which only the console's own pages learn, from `/api/me`.
/// Another site's page can neither read it nor send the cookie.
fn csrf_token(session: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(format!("legion-console-csrf:{session}").as_bytes());
    format!("{hash:x}")[..32].to_owned()
}

/// Whether `sent` is `session`'s token, in time that does not depend on
/// where they differ.
fn csrf_ok(session: &str, sent: Option<&str>) -> bool {
    let want = csrf_token(session);
    let Some(sent) = sent else { return false };
    sent.len() == want.len()
        && sent
            .bytes()
            .zip(want.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            == 0
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
    /// The user's channel for a write: from the console's own page, with
    /// the session's token (in the header; a WebSocket, which cannot set
    /// one, sends it as `csrf` in the query).
    async fn writer(
        &self,
        headers: &HeaderMap,
        uri: &Uri,
        query_token: Option<&str>,
    ) -> Result<Channel, Refused> {
        same_origin(headers, uri)?;
        let (session, channel) = self.user(headers).await?;
        let sent = headers
            .get(CSRF_HEADER)
            .and_then(|v| v.to_str().ok())
            .or(query_token);
        if !csrf_ok(&session, sent) {
            return Err(Refused(
                StatusCode::FORBIDDEN,
                "this request did not come from the console's page".into(),
            ));
        }
        Ok(channel)
    }

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

/// The cookie keeping a single sign-on flow under way, for its callback
/// alone; `Lax`, as the provider sends the browser back from its own site.
const FLOW_COOKIE: &str = "legion_oidc";

fn see_other(to: &str) -> Response {
    let mut res = StatusCode::SEE_OTHER.into_response();
    if let Ok(v) = HeaderValue::from_str(to) {
        res.headers_mut().insert(header::LOCATION, v);
    }
    res
}

/// Back to the sign-in page, saying why single sign-on failed (in the
/// fragment, which no server sees).
fn sso_failed(why: &str) -> Response {
    tracing::warn!("single sign-on failed: {why}");
    let encoded: String = why
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    let mut res = see_other(&format!("/login#error={encoded}"));
    res.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{FLOW_COOKIE}=; Path=/api/oidc; HttpOnly; Secure; SameSite=Lax; Max-Age=0"
        ))
        .expect("ASCII"),
    );
    res
}

/// Single sign-on (CONSOLE.md phase 6): the browser sent to the provider,
/// the flow's state, nonce and PKCE verifier kept in a cookie.
async fn oidc_start(State(g): State<Gateway>) -> Response {
    let r = match crate::console::oidc(&g.0.api.store) {
        Ok(Some(r)) => r,
        Ok(None) => return sso_failed("single sign-on is off"),
        Err(e) => return sso_failed(e.message()),
    };
    match g.0.sso.start(&r.oidc).await {
        Ok((url, flow)) => {
            let mut res = see_other(&url);
            res.headers_mut().append(
                header::SET_COOKIE,
                HeaderValue::from_str(&format!(
                    "{FLOW_COOKIE}={}; Path=/api/oidc; HttpOnly; Secure; SameSite=Lax; Max-Age=600",
                    flow.cookie_value()
                ))
                .expect("hex and ASCII"),
            );
            res
        }
        Err(e) => sso_failed(&e),
    }
}

#[derive(Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// The provider's answer: the flow checked, the code exchanged and the ID
/// token verified, then a session like any other for whom it names.
async fn oidc_callback(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    if let Some(error) = q.error {
        let more = q.error_description.unwrap_or_default();
        return sso_failed(&format!("the provider said {error}: {more}"));
    }
    let Some(flow) =
        cookie_named(&headers, FLOW_COOKIE).and_then(|v| crate::oidc::Flow::from_cookie(&v))
    else {
        return sso_failed("no sign-in was under way in this browser; start again");
    };
    if q.state.as_deref() != Some(flow.state.as_str()) {
        return sso_failed("the provider's answer is not for this sign-in");
    }
    let Some(code) = q.code else {
        return sso_failed("the provider sent no code");
    };
    let r = match crate::console::oidc(&g.0.api.store) {
        Ok(Some(r)) => r,
        Ok(None) => return sso_failed("single sign-on is off"),
        Err(e) => return sso_failed(e.message()),
    };
    let (name, groups) = match g
        .0
        .sso
        .finish(&r.oidc, &r.client_secret, &code, &flow)
        .await
    {
        Ok(who) => who,
        Err(e) => return sso_failed(&e),
    };
    let s = match ClusterClient::new(g.manager())
        .console_oidc_session(pb::ConsoleOidcSessionRequest { name, groups })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => return sso_failed(e.message()),
    };
    let max_age = ((s.expires_ms - crate::console::now_ms()) / 1000).max(0);
    let mut res = see_other("/");
    for c in [
        format!(
            "{COOKIE}={}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age={max_age}",
            s.session
        ),
        format!("{FLOW_COOKIE}=; Path=/api/oidc; HttpOnly; Secure; SameSite=Lax; Max-Age=0"),
    ] {
        res.headers_mut().append(
            header::SET_COOKIE,
            HeaderValue::from_str(&c).expect("hex and ASCII"),
        );
    }
    res
}

/// Who the signed-in user is. Another cluster's console asks too, from its
/// Clusters page (CONSOLE.md phase 6), if this cluster's settings list it:
/// it reads the answer across origins, without the token for writes.
async fn me(State(g): State<Gateway>, headers: HeaderMap, uri: Uri) -> Response {
    let peer = match same_origin(&headers, &uri) {
        Ok(()) => None,
        Err(refused) => {
            let origin = headers.get(header::ORIGIN).and_then(|o| o.to_str().ok());
            match (origin, crate::console::settings(&g.0.api.store)) {
                (Some(o), Ok(s)) if s.lists(o) => Some(o.to_owned()),
                _ => return refused.into_response(),
            }
        }
    };
    let answer = async {
        let (session, channel) = g.user(&headers).await?;
        let me = ClusterClient::new(channel)
            .me(pb::MeRequest {})
            .await?
            .into_inner();
        let cluster = crate::console::settings(&g.0.api.store)?.name;
        let mut body = serde_json::json!({
            "name": me.name, "role": me.role, "all": me.all, "namespaces": me.namespaces,
            "cluster": cluster,
        });
        if peer.is_none() {
            body["csrf"] = csrf_token(&session).into();
        }
        Ok::<_, Refused>(Json(body))
    };
    let mut res = answer.await.into_response();
    let h = res.headers_mut();
    h.insert(header::VARY, HeaderValue::from_static("Origin"));
    if let Some(o) = peer.and_then(|o| HeaderValue::from_str(&o).ok()) {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, o);
        h.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    }
    res
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
struct AlertsQuery {
    namespace: Option<String>,
}

/// `Cluster.WatchAlerts` as the user: JSON text messages `{alerts: [...]}`,
/// the whole list each time it changes, or `{error}` as the last.
async fn alerts(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<AlertsQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, Refused> {
    same_origin(&headers, &uri)?;
    let (_, channel) = g.user(&headers).await?;
    let stream = ClusterClient::new(channel)
        .watch_alerts(pb::AlertsRequest {
            namespace: q.namespace.unwrap_or_default(),
        })
        .await?
        .into_inner();
    Ok(ws.on_upgrade(move |socket| relay_alerts(socket, stream)))
}

async fn relay_alerts(mut socket: WebSocket, mut stream: tonic::Streaming<pb::AlertsResponse>) {
    loop {
        let next = tokio::select! {
            r = stream.message() => r,
            m = socket.recv() => match m {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => continue,
            },
        };
        let text = match next {
            Ok(Some(r)) => serde_json::json!({
                "alerts": r.alerts.iter().map(|a| serde_json::json!({
                    "kind": a.kind,
                    "severity": a.severity,
                    "namespace": a.namespace,
                    "object": a.object,
                    "message": a.message,
                    "since_ms": a.since_ms,
                })).collect::<Vec<_>>()
            }),
            Ok(None) => serde_json::json!({ "error": "the alerts ended" }),
            Err(s) => serde_json::json!({ "error": s.message() }),
        };
        let end = text.get("error").is_some();
        if socket
            .send(Message::Text(text.to_string().into()))
            .await
            .is_err()
            || end
        {
            return;
        }
    }
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

/// What each node's tasks use now, as `legion top` shows it: `{nodes:
/// [{node, tasks: [{namespace, task, cpu_percent, memory_bytes}]}]}`.
async fn usage(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(scope): Query<Scope>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ClusterClient::new(channel)
        .usage(pb::UsageRequest {
            namespace: scope.namespace.unwrap_or_default(),
        })
        .await?
        .into_inner();
    let nodes: Vec<_> = r
        .nodes
        .iter()
        .map(|n| {
            let tasks: Vec<_> = n
                .tasks
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "namespace": t.namespace, "task": t.task,
                        "cpu_percent": t.cpu_percent, "memory_bytes": t.memory_bytes,
                    })
                })
                .collect();
            serde_json::json!({ "node": n.node, "tasks": tasks })
        })
        .collect();
    Ok(Json(serde_json::json!({ "nodes": nodes })).into_response())
}

/// `legion quota`: `{namespaces: [{namespace, resources: [{resource,
/// limit, used}]}]}`, CPU in cores, memory and storage in bytes.
async fn quotas(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(scope): Query<Scope>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ClusterClient::new(channel)
        .quotas(pb::QuotasRequest {
            namespace: scope.namespace.unwrap_or_default(),
        })
        .await?
        .into_inner();
    Ok(Json(serde_json::json!({ "namespaces": quotas_json(&r) })).into_response())
}

fn quotas_json(r: &pb::QuotasResponse) -> Vec<serde_json::Value> {
    r.namespaces
        .iter()
        .map(|n| {
            let resources: Vec<_> = n
                .resources
                .iter()
                .map(|q| {
                    serde_json::json!({ "resource": q.resource, "limit": q.limit, "used": q.used })
                })
                .collect();
            serde_json::json!({ "namespace": n.namespace, "resources": resources })
        })
        .collect()
}

#[derive(Deserialize)]
struct LogSearchQuery {
    namespace: String,
    service: String,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    text: Option<String>,
    limit: Option<usize>,
}

/// `legion logs-search`: `{lines: [{at_ms, task, container, stderr,
/// text}], truncated, unread: [why]}`, oldest first.
async fn log_search(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(q): Query<LogSearchQuery>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = crate::logsearch::search(
        channel,
        &crate::logsearch::Search {
            namespace: q.namespace,
            service: q.service,
            since_ms: q.since_ms,
            until_ms: q.until_ms,
            text: q.text.unwrap_or_default(),
            limit: q.limit.unwrap_or(1000).min(5000),
        },
    )
    .await?;
    let lines: Vec<_> = r
        .lines
        .iter()
        .map(|l| {
            serde_json::json!({
                "at_ms": l.at_ms, "task": l.task, "container": l.container,
                "stderr": l.stderr, "text": l.text,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({
        "lines": lines, "truncated": r.truncated, "unread": r.unread,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct HistoryScope {
    namespace: Option<String>,
    task: Option<String>,
}

/// `Cluster.UsageHistory`: `{step_ms, tasks: [{namespace, task, node,
/// samples: [[at_ms, cpu_percent, memory_bytes], ...]}]}`, oldest first.
async fn usage_history(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(scope): Query<HistoryScope>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ClusterClient::new(channel)
        .usage_history(pb::UsageHistoryRequest {
            namespace: scope.namespace.unwrap_or_default(),
            task: scope.task.unwrap_or_default(),
        })
        .await?
        .into_inner();
    let tasks: Vec<_> = r
        .tasks
        .iter()
        .map(|t| {
            let samples: Vec<_> = t
                .samples
                .iter()
                .map(|s| serde_json::json!([s.at_ms, s.cpu_percent, s.memory_bytes]))
                .collect();
            serde_json::json!({
                "namespace": t.namespace, "task": t.task, "node": t.node,
                "service": t.service, "samples": samples,
                "cpu_request_percent": t.cpu_request_percent,
                "memory_request_bytes": t.memory_request_bytes,
                "cpu_limit_percent": t.cpu_limit_percent,
                "memory_limit_bytes": t.memory_limit_bytes,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "step_ms": r.step_ms, "tasks": tasks })).into_response())
}

/// `legion reach`: what may reach what among a namespace's services,
/// `{services, edges: [{from, to, all_ports, ports: [{protocol, port}]}]}`;
/// `from` empty for other namespaces' tasks.
async fn reach(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(scope): Query<Scope>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ClusterClient::new(channel)
        .reach(pb::ReachRequest {
            namespace: scope.namespace.unwrap_or_default(),
        })
        .await?
        .into_inner();
    let edges: Vec<_> = r
        .edges
        .iter()
        .map(|e| {
            let ports: Vec<_> = e
                .ports
                .iter()
                .map(|p| serde_json::json!({ "protocol": p.protocol, "port": p.port }))
                .collect();
            serde_json::json!({ "from": e.from, "to": e.to, "all_ports": e.all_ports, "ports": ports })
        })
        .collect();
    Ok(Json(serde_json::json!({ "services": r.services, "edges": edges })).into_response())
}

#[derive(Deserialize)]
struct LogsQuery {
    container: Option<String>,
    tail: Option<u32>,
    #[serde(default)]
    follow: bool,
}

/// A task's output, as `legion logs` prints it, as JSON text messages
/// `{out}` or `{err}` (the bytes as text), then `{end: true}` when the
/// container's output ends or `{error}` when the session fails.
async fn logs(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Path((namespace, task)): Path<(String, String)>,
    Query(q): Query<LogsQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, Refused> {
    use pb::session_frame::Frame;
    same_origin(&headers, &uri)?;
    let (_, channel) = g.user(&headers).await?;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let open = pb::SessionOpen {
        namespace,
        task,
        container: q.container.unwrap_or_default(),
        action: Some(pb::session_open::Action::Logs(pb::LogsOptions {
            follow: q.follow,
            tail: q.tail,
            ..Default::default()
        })),
    };
    tx.send(pb::SessionFrame {
        frame: Some(Frame::Open(open)),
    })
    .await
    .expect("the receiver is held");
    let stream = pb::pods_client::PodsClient::new(channel)
        .session(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    // `tx` is kept until the end: closing it would end the session's input.
    Ok(ws.on_upgrade(move |socket| relay_logs(socket, stream, tx)))
}

async fn relay_logs(
    mut socket: WebSocket,
    mut stream: tonic::Streaming<pb::SessionFrame>,
    _input: tokio::sync::mpsc::Sender<pb::SessionFrame>,
) {
    use pb::session_frame::Frame;
    // What each stream has left of a character split between frames.
    let (mut out, mut err) = (Vec::new(), Vec::new());
    loop {
        let frame = tokio::select! {
            f = stream.message() => f,
            m = socket.recv() => match m {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => continue,
            },
        };
        let text = match frame {
            Ok(Some(pb::SessionFrame {
                frame: Some(Frame::Stdout(data)),
            })) => serde_json::json!({ "out": text_of(&mut out, data) }),
            Ok(Some(pb::SessionFrame {
                frame: Some(Frame::Stderr(data)),
            })) => serde_json::json!({ "err": text_of(&mut err, data) }),
            Ok(Some(pb::SessionFrame {
                frame: Some(Frame::Error(e)),
            })) => serde_json::json!({ "error": e.message }),
            Ok(Some(_)) => continue,
            Ok(None) => serde_json::json!({ "end": true }),
            Err(s) => serde_json::json!({ "error": s.message() }),
        };
        let end = text.get("out").is_none() && text.get("err").is_none();
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

#[derive(Deserialize)]
struct BuildLogsQuery {
    #[serde(default)]
    follow: bool,
}

/// A Build's log (CONSOLE.md phase 5), as `legion build-logs` prints it,
/// as the same messages as a task's: `{out}`, then `{end: true}`.
async fn build_logs(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Path((namespace, name)): Path<(String, String)>,
    Query(q): Query<BuildLogsQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, Refused> {
    same_origin(&headers, &uri)?;
    let (_, channel) = g.user(&headers).await?;
    let stream = ClusterClient::new(channel)
        .build_logs(pb::BuildLogsRequest {
            namespace,
            name,
            follow: q.follow,
        })
        .await?
        .into_inner();
    Ok(ws.on_upgrade(move |socket| relay_build_log(socket, stream)))
}

async fn relay_build_log(mut socket: WebSocket, mut stream: tonic::Streaming<pb::BuildLogsChunk>) {
    let mut pending = Vec::new();
    loop {
        let chunk = tokio::select! {
            c = stream.message() => c,
            m = socket.recv() => match m {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => continue,
            },
        };
        let text = match chunk {
            Ok(Some(c)) => serde_json::json!({ "out": text_of(&mut pending, c.data) }),
            Ok(None) => serde_json::json!({ "end": true }),
            Err(s) => serde_json::json!({ "error": s.message() }),
        };
        let end = text.get("out").is_none();
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

/// `data` after what `pending` held, as text, keeping back the start of a
/// character it ends inside of for the next; bytes that are not UTF-8 are
/// replaced.
fn text_of(pending: &mut Vec<u8>, data: Vec<u8>) -> String {
    pending.extend_from_slice(&data);
    let whole = match std::str::from_utf8(pending) {
        Ok(_) => pending.len(),
        // An incomplete character at the end (at most 3 bytes) waits.
        Err(e) if e.error_len().is_none() && pending.len() - e.valid_up_to() <= 3 => {
            e.valid_up_to()
        }
        Err(_) => pending.len(),
    };
    let rest = pending.split_off(whole);
    let text = String::from_utf8_lossy(pending).into_owned();
    *pending = rest;
    text
}

#[derive(Deserialize)]
struct ComposeBody {
    compose: String,
    stack: String,
    /// The stack's name when not given, as `legion stack deploy` does.
    namespace: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
}

/// A compose file as Legion objects, as `legion stack convert` prints them
/// (CONSOLE.md phase 5: compose import, templates), nothing written:
/// `{objects, warnings, variables: [{name, default, required}]}`. The
/// namespace comes first when it is missing, made as `legion stack
/// deploy` makes it. Files a compose file names are not read: in the
/// console, content goes in the file.
async fn compose(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Json(body): Json<ComposeBody>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let namespace = body.namespace.unwrap_or_else(|| body.stack.clone());
    let env = |k: &str| body.env.get(k).cloned();
    let read = |p: &str| -> Result<Vec<u8>, String> {
        Err(format!(
            "{p}: the console reads no files; put the content in the compose file"
        ))
    };
    let stack = legion_api::compose::convert(
        &body.compose,
        &body.stack,
        &namespace,
        &legion_api::compose::Inputs {
            env: &env,
            read: &read,
        },
    )
    .map_err(bad_request)?;
    let missing = match ObjectsClient::new(channel)
        .get(pb::ObjectRef {
            kind: legion_api::Kind::Namespace.as_str().into(),
            namespace: String::new(),
            name: namespace.clone(),
        })
        .await
    {
        Err(s) if s.code() == tonic::Code::NotFound => true,
        // A user with a role there may not look at it: it is there.
        Err(s) if s.code() != tonic::Code::PermissionDenied => return Err(s.into()),
        _ => false,
    };
    let mut docs = Vec::new();
    if missing {
        docs.push(serde_json::json!({
            "apiVersion": "legion/v1", "kind": "Namespace",
            "metadata": { "name": namespace }, "spec": {},
        }));
    }
    for o in &stack.objects {
        docs.push(serde_json::json!({
            "apiVersion": o.api_version,
            "kind": o.kind,
            "metadata": {
                "name": o.metadata.name,
                "namespace": o.metadata.namespace,
                "labels": o.metadata.labels,
            },
            "spec": o.spec,
        }));
    }
    let variables: Vec<_> = stack
        .variables
        .iter()
        .map(
            |v| serde_json::json!({ "name": v.name, "default": v.default, "required": v.required }),
        )
        .collect();
    Ok(Json(serde_json::json!({
        "objects": docs, "warnings": stack.warnings, "variables": variables,
    }))
    .into_response())
}

/// The label marking a Config as a saved template: its text a compose file.
const TEMPLATE_LABEL: &str = "legion.template";

/// The examples in `examples/`, the catalog every console has.
const EXAMPLES: &[(&str, &str)] = &[
    (
        "counter",
        include_str!("../../../examples/counter/compose.yaml"),
    ),
    (
        "static-site",
        include_str!("../../../examples/static-site/compose.yaml"),
    ),
];

/// A compose file's opening comment, as what it is.
fn about(compose: &str) -> String {
    compose
        .lines()
        .take_while(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim())
        .take_while(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The templates to deploy from: the examples, and the Configs labelled
/// `legion.template` in the namespaces the user may read. `{templates:
/// [{name, namespace, about, compose}]}`, an example's namespace empty.
async fn templates(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let mut out: Vec<_> = EXAMPLES
        .iter()
        .map(|(name, compose)| {
            serde_json::json!({
                "name": name, "namespace": "", "about": about(compose), "compose": compose,
            })
        })
        .collect();
    let saved = ObjectsClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .list(pb::ListRequest {
            kind: legion_api::Kind::Config.as_str().into(),
            namespace: String::new(),
            match_labels: [(TEMPLATE_LABEL.to_owned(), "compose".to_owned())].into(),
        })
        .await?
        .into_inner();
    for o in saved.objects {
        let Ok(spec) = serde_json::from_slice::<legion_api::data::DataSpec>(&o.spec_json) else {
            continue;
        };
        // Stored as `data`, whichever the manifest gave.
        let Some(compose) = spec.bytes().ok().and_then(|b| String::from_utf8(b).ok()) else {
            continue;
        };
        out.push(serde_json::json!({
            "name": o.name, "namespace": o.namespace, "about": about(&compose), "compose": compose,
        }));
    }
    Ok(Json(serde_json::json!({ "templates": out })).into_response())
}

/// A Config labelled so is a plugin's page (CONSOLE.md phase 6).
const PLUGIN_LABEL: &str = "legion.console-plugin";

/// A plugin page's policy: a sandbox of its own origin (no cookie, no
/// session, none of the console's storage), reaching no address, framed
/// by the console alone.
const PLUGIN_POLICY: &str = "sandbox allow-scripts; default-src 'none'; \
     script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; font-src data:; \
     connect-src 'none'; form-action 'none'; base-uri 'none'; frame-ancestors 'self'";

/// The plugins in the namespaces the user may read.
async fn plugins(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let found = objects_client(channel)
        .list(pb::ListRequest {
            kind: legion_api::Kind::Config.as_str().into(),
            namespace: String::new(),
            match_labels: [(PLUGIN_LABEL.to_owned(), "page".to_owned())].into(),
        })
        .await?
        .into_inner();
    let mut found: Vec<_> = found
        .objects
        .into_iter()
        .map(|o| (o.namespace, o.name))
        .collect();
    found.sort();
    let out: Vec<_> = found
        .into_iter()
        .map(|(namespace, name)| serde_json::json!({ "namespace": namespace, "name": name }))
        .collect();
    Ok(Json(serde_json::json!({ "plugins": out })).into_response())
}

/// A plugin's page, read as the user, under its own policy.
async fn plugin(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let o = objects_client(channel)
        .get(pb::ObjectRef {
            kind: legion_api::Kind::Config.as_str().into(),
            namespace,
            name,
        })
        .await?
        .into_inner();
    let not_one = || Refused(StatusCode::NOT_FOUND, "no such plugin".into());
    if o.labels.get(PLUGIN_LABEL).map(String::as_str) != Some("page") {
        return Err(not_one());
    }
    let page = serde_json::from_slice::<legion_api::data::DataSpec>(&o.spec_json)
        .ok()
        .and_then(|s| s.bytes().ok())
        .ok_or_else(not_one)?;
    let mut res = page.into_response();
    let h = res.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(PLUGIN_POLICY),
    );
    h.insert("x-frame-options", HeaderValue::from_static("SAMEORIGIN"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(res)
}

fn bad_request(message: impl Into<String>) -> Refused {
    Refused(StatusCode::BAD_REQUEST, message.into())
}

fn objects_client(channel: Channel) -> ObjectsClient<Channel> {
    ObjectsClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .max_encoding_message_size(crate::MAX_MESSAGE)
}

/// The parts of a stored object a manifest sets, as YAML.
fn manifest_of(o: pb::Object) -> Result<String, Refused> {
    let o = legion_api::Object::try_from(o)
        .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(legion_api::manifest::yaml(&o))
}

fn no_namespace(namespace: String) -> String {
    // `_` in a path for a cluster-scoped object's namespace.
    if namespace == "_" {
        String::new()
    } else {
        namespace
    }
}

/// An object as its manifest, to edit and apply again: `{yaml, version}`.
async fn manifest(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Path((kind, namespace, name)): Path<(String, String, String)>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let o = objects_client(channel)
        .get(pb::ObjectRef {
            kind,
            namespace: no_namespace(namespace),
            name,
        })
        .await?
        .into_inner();
    let version = o.version;
    Ok(Json(serde_json::json!({ "yaml": manifest_of(o)?, "version": version })).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApplyBody {
    /// A manifest: one or more YAML documents.
    yaml: Option<String>,
    /// Or one object, as JSON.
    object: Option<serde_json::Value>,
    #[serde(default)]
    dry_run: bool,
    /// Only over this version (one object only): an edit of what was read.
    expected_version: Option<u64>,
}

/// Applies a manifest's objects in order, or one object, as `legion apply`
/// does: `{results: [{key, result, version}]}`. With `dryRun` nothing is
/// written, and each result has the `diff` of the stored object's
/// manifest to what would be stored, as `legion diff` prints it. The first
/// refusal ends it, naming the object and those applied before it.
async fn apply(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<ApplyBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let objects: Vec<legion_api::Object> = match (body.yaml, body.object) {
        (Some(y), None) => legion_api::manifest::parse(&y).map_err(bad_request)?,
        (None, Some(o)) => vec![serde_json::from_value(o).map_err(|e| bad_request(e.to_string()))?],
        _ => return Err(bad_request("send a manifest (yaml) or an object")),
    };
    if objects.is_empty() {
        return Err(bad_request("the manifest has no objects"));
    }
    if body.expected_version.is_some() && objects.len() > 1 {
        return Err(bad_request("an expected version is for one object"));
    }
    let mut client = objects_client(channel);
    let mut results: Vec<serde_json::Value> = Vec::new();
    for o in &objects {
        let key = o.key().to_string();
        let r = match client
            .apply(pb::ApplyRequest {
                object: Some(o.into()),
                expected_version: body.expected_version,
                dry_run: body.dry_run,
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(s) => {
                let Refused(code, message) = Refused::from(s);
                let done: Vec<_> = results.iter().filter_map(|r| r["key"].as_str()).collect();
                let message = if done.is_empty() || body.dry_run {
                    format!("{key}: {message}")
                } else {
                    format!("{key}: {message} (applied before it: {})", done.join(", "))
                };
                return Err(Refused(code, message));
            }
        };
        let result = match r.result() {
            pb::ApplyResult::Created => "created",
            pb::ApplyResult::Updated => "updated",
            pb::ApplyResult::Unchanged => "unchanged",
            pb::ApplyResult::Unspecified => "applied",
        };
        let after = r.object.unwrap_or_default();
        let mut entry =
            serde_json::json!({ "key": key, "result": result, "version": after.version });
        if body.dry_run && result != "unchanged" {
            let before = if result == "created" {
                String::new()
            } else {
                let live = client
                    .get(pb::ObjectRef {
                        kind: after.kind.clone(),
                        namespace: after.namespace.clone(),
                        name: after.name.clone(),
                    })
                    .await?
                    .into_inner();
                manifest_of(live)?
            };
            let after = manifest_of(after)?;
            entry["diff"] = similar::TextDiff::from_lines(&before, &after)
                .unified_diff()
                .context_radius(3)
                .header(&format!("live {key}"), &format!("applied {key}"))
                .to_string()
                .into();
        }
        results.push(entry);
    }
    Ok(Json(serde_json::json!({ "results": results })).into_response())
}

#[derive(Deserialize)]
struct DeleteQuery {
    /// Only at this version.
    version: Option<u64>,
}

async fn delete_object(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Path((kind, namespace, name)): Path<(String, String, String)>,
    Query(q): Query<DeleteQuery>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let r = objects_client(channel)
        .delete(pb::DeleteRequest {
            r#ref: Some(pb::ObjectRef {
                kind,
                namespace: no_namespace(namespace),
                name,
            }),
            expected_version: q.version,
        })
        .await?
        .into_inner();
    Ok(Json(serde_json::json!({ "version": r.version })).into_response())
}

#[derive(Deserialize)]
struct Target {
    kind: String,
    #[serde(default)]
    namespace: String,
    name: String,
}

impl Target {
    fn into_ref(self) -> pb::ObjectRef {
        pb::ObjectRef {
            kind: self.kind,
            namespace: self.namespace,
            name: self.name,
        }
    }
}

#[derive(Deserialize)]
struct SetBody {
    #[serde(flatten)]
    target: Target,
    field: String,
    value: serde_json::Value,
}

/// Sets one field of an object's spec, as `legion top`'s actions do
/// (`actions::set_spec`): a Service's replicas, a CronJob's suspend, a
/// node's availability, a Volume's node. `{key, changed}`.
async fn set_field(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<SetBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    if body.field.is_empty() {
        return Err(bad_request("which field?"));
    }
    let (key, changed) = crate::actions::set_spec(
        &mut objects_client(channel),
        body.target.into_ref(),
        &body.field,
        body.value,
    )
    .await?;
    Ok(Json(serde_json::json!({ "key": key, "changed": changed })).into_response())
}

#[derive(Deserialize)]
struct LabelsBody {
    #[serde(flatten)]
    target: Target,
    /// A value sets the label; `null` removes it.
    changes: std::collections::BTreeMap<String, Option<String>>,
}

async fn labels(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<LabelsBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let changes: Vec<_> = body.changes.into_iter().collect();
    let (key, changed) = crate::actions::set_labels(
        &mut objects_client(channel),
        body.target.into_ref(),
        &changes,
    )
    .await?;
    Ok(Json(serde_json::json!({ "key": key, "changed": changed })).into_response())
}

#[derive(Deserialize)]
struct RollbackBody {
    namespace: String,
    service: String,
}

/// `legion rollback`: a Service back on the spec before its latest update.
async fn rollback(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<RollbackBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let message =
        crate::actions::rollback(&mut objects_client(channel), body.namespace, body.service)
            .await?;
    Ok(Json(serde_json::json!({ "message": message })).into_response())
}

#[derive(Deserialize)]
struct NodeRoleBody {
    node: String,
    /// `manager` or `worker`.
    role: String,
}

/// `legion node promote|demote`: a node becomes a manager or a worker.
async fn node_role(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<NodeRoleBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    ClusterClient::new(channel)
        .change_role(pb::ChangeRoleRequest {
            node: body.node,
            role: body.role,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct ExecQuery {
    container: Option<String>,
    /// The command as a JSON array; a shell when left out.
    command: Option<String>,
    cols: Option<u32>,
    rows: Option<u32>,
    csrf: Option<String>,
}

/// A command in a task's container under a terminal, as `legion exec -it`
/// runs it. Binary messages carry its input and output; a text message
/// `{resize: [cols, rows]}` resizes the terminal and `{eof: true}` closes
/// its input; the end is `{exit: code}` or `{error}`.
async fn exec(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Path((namespace, task)): Path<(String, String)>,
    Query(q): Query<ExecQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, Refused> {
    use pb::session_frame::Frame;
    let channel = g.writer(&headers, &uri, q.csrf.as_deref()).await?;
    let command: Vec<String> = match &q.command {
        Some(c) => serde_json::from_str(c).map_err(|e| bad_request(format!("command: {e}")))?,
        None => vec!["/bin/sh".into()],
    };
    if command.is_empty() {
        return Err(bad_request("an empty command"));
    }
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let open = pb::SessionOpen {
        namespace,
        task,
        container: q.container.unwrap_or_default(),
        action: Some(pb::session_open::Action::Exec(pb::ExecOptions {
            command,
            stdin: true,
            tty: true,
            size: Some(pb::TerminalSize {
                width: q.cols.unwrap_or(80).clamp(1, 1000),
                height: q.rows.unwrap_or(24).clamp(1, 1000),
            }),
        })),
    };
    tx.send(pb::SessionFrame {
        frame: Some(Frame::Open(open)),
    })
    .await
    .expect("the receiver is held");
    let stream = pb::pods_client::PodsClient::new(channel)
        .session(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    Ok(ws.on_upgrade(move |socket| relay_exec(socket, stream, tx)))
}

#[derive(Deserialize)]
struct TerminalMessage {
    resize: Option<(u32, u32)>,
    #[serde(default)]
    eof: bool,
}

async fn relay_exec(
    mut socket: WebSocket,
    mut stream: tonic::Streaming<pb::SessionFrame>,
    input: tokio::sync::mpsc::Sender<pb::SessionFrame>,
) {
    use pb::session_frame::Frame;
    let frame = |f: Frame| pb::SessionFrame { frame: Some(f) };
    loop {
        tokio::select! {
            f = stream.message() => {
                let end = match f {
                    Ok(Some(pb::SessionFrame { frame: Some(Frame::Stdout(d) | Frame::Stderr(d)) })) => {
                        if socket.send(Message::Binary(d.into())).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Ok(Some(pb::SessionFrame { frame: Some(Frame::ExitCode(c)) })) => {
                        serde_json::json!({ "exit": c })
                    }
                    Ok(Some(pb::SessionFrame { frame: Some(Frame::Error(e)) })) => {
                        serde_json::json!({ "error": e.message })
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) => serde_json::json!({ "error": "the session ended without the command's exit code" }),
                    Err(s) => serde_json::json!({ "error": s.message() }),
                };
                let _ = socket.send(Message::Text(end.to_string().into())).await;
                let _ = socket.send(Message::Close(None)).await;
                return;
            }
            m = socket.recv() => {
                let sent = match m {
                    Some(Ok(Message::Binary(d))) => input.send(frame(Frame::Input(d.to_vec()))).await,
                    Some(Ok(Message::Text(t))) => match serde_json::from_str::<TerminalMessage>(&t) {
                        Ok(TerminalMessage { resize: Some((cols, rows)), .. }) => {
                            input.send(frame(Frame::Resize(pb::TerminalSize {
                                width: cols.clamp(1, 1000),
                                height: rows.clamp(1, 1000),
                            }))).await
                        }
                        Ok(TerminalMessage { eof: true, .. }) => {
                            input.send(frame(Frame::InputClosed(pb::InputClosed {}))).await
                        }
                        _ => Ok(()),
                    },
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                    _ => Ok(()),
                };
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

/// `legion audit`'s filters, as a query string carries them.
#[derive(Deserialize)]
struct AuditQuery {
    since_ms: Option<u64>,
    actor: Option<String>,
    action: Option<String>,
    namespace: Option<String>,
    kind: Option<String>,
    /// `ok`, `denied` or `error`.
    result: Option<String>,
    limit: Option<u32>,
}

/// `legion audit`: the newest events that match, `{events, truncated}`.
async fn audit(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Query(q): Query<AuditQuery>,
) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let result = match q.result.as_deref().filter(|r| !r.is_empty()) {
        Some(r) => Some(
            serde_json::from_value(r.into())
                .map_err(|_| bad_request("result: ok, denied or error"))?,
        ),
        None => None,
    };
    let filter = legion_api::audit::Filter {
        since_ms: q.since_ms.unwrap_or(0),
        actor: q.actor.unwrap_or_default(),
        action: q.action.unwrap_or_default(),
        namespace: q.namespace.unwrap_or_default(),
        kind: q.kind.unwrap_or_default(),
        result,
    };
    let r = ClusterClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .audit(pb::AuditRequest {
            filter_json: serde_json::to_vec(&filter).expect("a filter serialises"),
            limit: q.limit.unwrap_or(200).min(5000),
        })
        .await?
        .into_inner();
    let events = r
        .events_json
        .iter()
        .map(|e| serde_json::from_slice(e))
        .collect::<Result<Vec<serde_json::Value>, _>>()
        .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "events": events, "truncated": r.truncated })).into_response())
}

#[derive(Deserialize)]
struct JoinTokenBody {
    /// `manager` or `worker`.
    role: String,
    #[serde(default)]
    rotate: bool,
}

/// `legion join-token`: the token nodes join with, made anew with `rotate`.
async fn join_token(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<JoinTokenBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let token = ClusterClient::new(channel)
        .join_token(pb::JoinTokenRequest {
            role: body.role,
            rotate: body.rotate,
        })
        .await?
        .into_inner()
        .token;
    Ok(Json(serde_json::json!({ "token": token })).into_response())
}

fn ca_json(r: pb::RotateCaResponse) -> Response {
    let holders: Vec<_> = r
        .holders
        .iter()
        .map(|h| serde_json::json!({ "name": h.name, "kind": h.kind, "issuer": h.issuer }))
        .collect();
    Json(serde_json::json!({
        "digest": r.digest, "previous": r.previous_digest, "since_ms": r.since_ms,
        "holders": holders,
    }))
    .into_response()
}

/// `legion ca status`: the CA that issues, and each holder's.
async fn ca_status(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let r = ClusterClient::new(channel)
        .rotate_ca(pb::RotateCaRequest { start: false })
        .await?
        .into_inner();
    Ok(ca_json(r))
}

/// `legion ca rotate`: starts a rotation to a new CA.
async fn ca_rotate(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let r = ClusterClient::new(channel)
        .rotate_ca(pb::RotateCaRequest { start: true })
        .await?
        .into_inner();
    Ok(ca_json(r))
}

/// The console's settings (`legion console settings`): the banner alone
/// before signing in, as the sign-in page shows it; all of them after.
async fn console_settings(
    State(g): State<Gateway>,
    headers: HeaderMap,
) -> Result<Response, Refused> {
    let settings = crate::console::settings(&g.0.api.store)?;
    let signed_in = match cookie(&headers) {
        Some(session) => crate::console::session(&g.0.api.store, &session)?.is_some(),
        None => false,
    };
    let settings = if signed_in {
        settings
    } else {
        settings.public()
    };
    let mut body = serde_json::to_value(&settings).expect("serialises");
    // The sign-in page offers single sign-on when it is on.
    if let Some(r) = crate::console::oidc(&g.0.api.store)? {
        body["sso"] = serde_json::json!({ "label": r.oidc.label });
    }
    Ok(Json(body).into_response())
}

/// `legion console configure`, the cluster admin only: the settings as
/// now set.
async fn set_console_settings(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<legion_api::console::Settings>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    let r = ClusterClient::new(channel)
        .set_console_settings(pb::SetConsoleSettingsRequest {
            settings_json: serde_json::to_vec(&body).expect("serialises"),
        })
        .await?
        .into_inner();
    let set: legion_api::console::Settings =
        serde_json::from_slice(&r.settings_json).map_err(|e| bad_request(e.to_string()))?;
    Ok(Json(set).into_response())
}

#[derive(Deserialize)]
struct AutolockBody {
    /// `enable`, `disable`, `rotate` or `show`.
    action: String,
}

/// `legion autolock`: `{key}`, empty when autolock is off.
async fn autolock(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<AutolockBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    if !["enable", "disable", "rotate", "show"].contains(&body.action.as_str()) {
        return Err(bad_request("enable, disable, rotate or show"));
    }
    let key = ClusterClient::new(channel)
        .autolock(pb::AutolockRequest {
            action: body.action,
        })
        .await?
        .into_inner()
        .key;
    Ok(Json(serde_json::json!({ "key": key })).into_response())
}

#[derive(Deserialize)]
struct UserBody {
    name: String,
    role: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    days: u32,
}

/// `legion user create`: the user's credentials, as the file `--identity`
/// takes (`{pem, binding, days}`), shown once and kept nowhere.
async fn create_user(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<UserBody>,
) -> Result<Response, Refused> {
    let channel = g.writer(&headers, &uri, None).await?;
    fn given(v: &Option<String>) -> Option<&str> {
        v.as_deref().filter(|s| !s.is_empty())
    }
    let binding = match (given(&body.role), given(&body.namespace)) {
        (Some(role), Some(namespace)) => Some((role, namespace)),
        (None, None) => None,
        _ => return Err(bad_request("a role and a namespace go together")),
    };
    let made = crate::actions::create_user(
        &mut objects_client(channel.clone()),
        &mut ClusterClient::new(channel),
        &body.name,
        binding,
        body.days,
        // Handed to the browser in the answer; nothing keeps them here.
        |_| Ok(()),
    )
    .await?;
    let days = legion_pki::validity(&made.bundle.cert_pem)
        .map_or(u64::from(body.days), |(_, life)| {
            (life.as_secs() + 43_200) / 86_400
        });
    let mut res = Json(serde_json::json!({
        "pem": made.bundle.to_pem(), "binding": made.binding, "days": days,
    }))
    .into_response();
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(res)
}

/// A download of what `stream` yields, as `filename`. A stream that fails
/// partway ends the response unfinished, so the browser reports the
/// download failed rather than keeping part of it.
fn download(
    filename: &str,
    stream: impl tokio_stream::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static,
) -> Response {
    let disposition = format!("attachment; filename=\"{}\"", filename.replace('"', ""));
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (header::CONTENT_DISPOSITION, disposition),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

fn chunks(
    s: tonic::Streaming<pb::BackupChunk>,
) -> impl tokio_stream::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    use tokio_stream::StreamExt;
    s.map(|c| {
        c.map(|c| bytes::Bytes::from(c.data))
            .map_err(|s| std::io::Error::other(s.message().to_owned()))
    })
}

/// `legion backup`: the control state, as `legion restore` takes it.
async fn store_backup(State(g): State<Gateway>, headers: HeaderMap) -> Result<Response, Refused> {
    let (_, channel) = g.user(&headers).await?;
    let stream = ClusterClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .backup(pb::BackupRequest {})
        .await?
        .into_inner();
    let name = format!("legion-backup-{}.json", crate::console::now_ms() / 1000);
    Ok(download(&name, chunks(stream)))
}

/// `legion volume backup`: a Volume's data, as `legion volume restore` and
/// the console's restore take it: the header's line, then the data.
async fn volume_backup(
    State(g): State<Gateway>,
    headers: HeaderMap,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Response, Refused> {
    use tokio_stream::StreamExt;
    let (_, channel) = g.user(&headers).await?;
    let mut stream = pb::volumes_client::VolumesClient::new(channel)
        .max_decoding_message_size(crate::MAX_MESSAGE)
        .backup(pb::VolumeBackupRequest { namespace, name })
        .await?
        .into_inner();
    let first = stream
        .message()
        .await?
        .ok_or_else(|| Refused(StatusCode::BAD_GATEWAY, "the manager sent nothing".into()))?;
    let (header, line) = legion_api::volume::VolumeBackupHeader::line(&first.data)
        .map_err(|e| Refused(StatusCode::BAD_GATEWAY, e))?;
    let file = format!(
        "{}-{}-{}.volume",
        header.namespace, header.name, header.taken
    );
    let body = tokio_stream::once(Ok(bytes::Bytes::from(line))).chain(chunks(stream));
    Ok(download(&file, body))
}

#[derive(Deserialize)]
struct RestoreQuery {
    /// The node to restore onto.
    node: String,
    /// The Volume's namespace and name; the backup's when left out.
    namespace: Option<String>,
    name: Option<String>,
    /// Into the Volume of that name, lost with its node.
    #[serde(default)]
    replace: bool,
}

/// `legion volume restore`: a backup file uploaded as the request's body,
/// to a Volume on `node`. Only once all of it is there does the Volume
/// take it; an upload cut short leaves nothing.
async fn volume_restore(
    State(g): State<Gateway>,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<RestoreQuery>,
    body: axum::body::Body,
) -> Result<Response, Refused> {
    use pb::volume_restore_chunk::Part;
    use tokio_stream::StreamExt;
    /// Bytes per message sent, as the CLI sends them.
    const CHUNK: usize = 1 << 20;
    let channel = g.writer(&headers, &uri, None).await?;
    let mut upload = body.into_data_stream();
    // The header's line first.
    let mut first = Vec::new();
    let rest = loop {
        match upload.next().await {
            Some(Ok(b)) => {
                first.extend_from_slice(&b);
                if let Some(end) = first.iter().position(|&c| c == b'\n') {
                    break first.split_off(end + 1);
                }
                if first.len() > CHUNK {
                    return Err(bad_request("not a volume backup: no header line"));
                }
            }
            Some(Err(e)) => return Err(bad_request(format!("reading the upload: {e}"))),
            None => return Err(bad_request("not a volume backup: no header line")),
        }
    };
    let line = String::from_utf8(first)
        .map_err(|_| bad_request("not a volume backup: its header is not text"))?;
    let header = legion_api::volume::VolumeBackupHeader::from_line(&line).map_err(bad_request)?;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let start = pb::VolumeRestoreStart {
        namespace: q.namespace.clone().unwrap_or_default(),
        name: q.name.clone().unwrap_or_default(),
        node: q.node.clone(),
        header_json: line.trim_end().to_owned(),
        replace: q.replace,
    };
    let send = |part| pb::VolumeRestoreChunk { part: Some(part) };
    tx.send(send(Part::Start(start)))
        .await
        .expect("the receiver is held");
    // The upload failing partway ends the stream early: the node then
    // takes nothing.
    let feeding = tokio::spawn(async move {
        let mut bytes = rest.len() as u64;
        if !rest.is_empty() && tx.send(send(Part::Data(rest))).await.is_err() {
            return Err("the manager stopped taking the data".to_owned());
        }
        while let Some(piece) = upload.next().await {
            let piece = piece.map_err(|e| format!("reading the upload: {e}"))?;
            bytes += piece.len() as u64;
            for part in piece.chunks(CHUNK) {
                if tx.send(send(Part::Data(part.to_vec()))).await.is_err() {
                    return Err("the manager stopped taking the data".to_owned());
                }
            }
        }
        Ok(bytes)
    });
    let sent = pb::volumes_client::VolumesClient::new(channel)
        .max_encoding_message_size(crate::MAX_MESSAGE)
        .restore(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await;
    let fed = feeding
        .await
        .map_err(|e| Refused(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    sent?;
    let bytes = fed.map_err(bad_request)?;
    Ok(Json(serde_json::json!({
        "namespace": q.namespace.unwrap_or(header.namespace),
        "name": q.name.unwrap_or(header.name),
        "node": q.node,
        "bytes": bytes,
        "taken": header.taken,
        "consistency": header.consistency,
    }))
    .into_response())
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
    fn a_character_split_between_frames_waits_for_its_end() {
        let mut pending = Vec::new();
        let e = "é".as_bytes(); // two bytes
        assert_eq!(text_of(&mut pending, [b"caf", &e[..1]].concat()), "caf");
        assert_eq!(text_of(&mut pending, [&e[1..], b"!"].concat()), "é!");
        assert!(pending.is_empty());
        assert_eq!(text_of(&mut pending, vec![0xff, b'a']), "\u{fffd}a");
    }

    #[test]
    fn a_write_carries_its_sessions_token() {
        let session = "ab".repeat(32);
        let token = csrf_token(&session);
        assert_eq!(token.len(), 32);
        assert!(csrf_ok(&session, Some(&token)));
        assert!(!csrf_ok(&session, None));
        assert!(!csrf_ok(&session, Some("")));
        assert!(
            !csrf_ok(&"cd".repeat(32), Some(&token)),
            "another session's"
        );
        let mut wrong = token.clone();
        wrong.replace_range(31.., if token.ends_with('0') { "1" } else { "0" });
        assert!(!csrf_ok(&session, Some(&wrong)));
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
