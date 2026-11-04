//! Certificates from an ACME authority for routes with `tls.acme`
//! (DESIGN.md §5.8), kept by the leader. Routes naming one Secret share a
//! certificate for all their hosts. The leader orders one when the Secret
//! holds none for every host, or one past two thirds of its life; puts the
//! authority's HTTP-01 challenges in the routes' status, from which the
//! dispatcher hands them to every ingress proxy to answer; and writes the
//! chain and its new key into the Secret, which the proxies read. A failed
//! order is tried again after ten minutes, then twice as long each time up
//! to a day, as the routes' status shows. Each directory and email has its
//! own account, kept in a Secret of `legion-system`. The manager talks to
//! authorities over HTTPS at public addresses only, as any namespace's
//! route may name one.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::{GaiResolver, Name};
use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, BodyWrapper, ChallengeType, HttpClient,
    Identifier, NewAccount, NewOrder, Order, OrderStatus, RetryPolicy,
};
use legion_api::data::DataSpec;
use legion_api::route::{AcmeStatus, RouteAcme, RouteSpec, RouteStatus};
use legion_api::{Kind, Object, ObjectKey, clock};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::join::SYSTEM_NAMESPACE;

/// Route changes arriving within this window are handled in one pass.
const DEBOUNCE: Duration = Duration::from_millis(50);
/// The longest between passes, so a Secret changed by hand is looked at.
const PERIOD: Duration = Duration::from_secs(3600);
const FIRST_RETRY: Duration = Duration::from_secs(600);
const LAST_RETRY: Duration = Duration::from_secs(86_400);
/// What a Secret the leader may write over holds.
const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----";
/// How long an order may take, validation and issuing included.
const ORDER_TIMEOUT: Duration = Duration::from_secs(180);
/// How long the authority may take to validate, then to issue.
const POLL: RetryPolicy = RetryPolicy::new().timeout(Duration::from_secs(60));

/// Keeps the routes' ACME certificates whenever this manager leads, until
/// `closing` turns true. With `loopback`, authorities on this host's
/// loopback are reached too, over plain HTTP: for tests.
pub async fn run(store: Store, id: NodeId, loopback: bool, closing: watch::Receiver<bool>) {
    crate::controller::while_leading(&store, id, closing, "ACME", || lead(&store, loopback)).await
}

async fn lead(store: &Store, loopback: bool) {
    let mut accounts = Accounts {
        loopback,
        known: BTreeMap::new(),
    };
    loop {
        let from = match store.applied_version() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("reading the store: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let next = pass(store, &mut accounts, SystemTime::now()).await;
        let mut watcher = match store.watch(from, Some(Kind::Route)) {
            Ok(w) => w,
            Err(_) => continue,
        };
        tokio::select! {
            ev = watcher.next() => if ev.is_ok() {
                tokio::time::sleep(DEBOUNCE).await;
            },
            _ = tokio::time::sleep_until(Instant::now() + next.min(PERIOD)) => {}
        }
    }
}

/// A certificate routes ask for: their Secret and hosts.
struct Wanted {
    namespace: String,
    secret: String,
    hosts: BTreeSet<String>,
    /// The first route's.
    acme: RouteAcme,
    routes: Vec<ObjectKey>,
    /// The most failed of its routes'.
    status: AcmeStatus,
}

/// What is to be done about a [`Wanted`].
#[derive(Debug, PartialEq)]
enum Due {
    /// Nothing until `renew_at`: the Secret holds a certificate for every
    /// host, expiring at `not_after`.
    No {
        not_after: SystemTime,
        renew_at: SystemTime,
    },
    /// Order one at this time, after a failure.
    Later(SystemTime),
    Now,
    /// Never: the Secret holds something else, which is not written over.
    Refused(String),
}

/// One pass over the routes; returns how long until the next is due.
async fn pass(store: &Store, accounts: &mut Accounts, now: SystemTime) -> Duration {
    let (wanted, stale) = match wanted(store) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("reading routes: {e}");
            return Duration::from_secs(1);
        }
    };
    // ACME dropped from a route, so is its status: an error or
    // challenges from before no longer apply.
    if let Err(e) = change_status(store, &stale, |s| *s = None).await {
        tracing::warn!("clearing a route's status: {e}");
    }
    let mut next = PERIOD;
    let mut until = |at: SystemTime| {
        next = next.min(at.duration_since(now).unwrap_or_default());
    };
    for w in wanted {
        let due = match due(store, &w, now) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(secret = %format!("{}/{}", w.namespace, w.secret), "{e}");
                continue;
            }
        };
        let status = match due {
            Due::No {
                not_after,
                renew_at,
            } => {
                until(renew_at);
                AcmeStatus {
                    not_after: Some(rfc3339(not_after)),
                    ..Default::default()
                }
            }
            Due::Later(at) => {
                until(at);
                continue;
            }
            Due::Refused(why) => AcmeStatus {
                error: Some(why),
                ..Default::default()
            },
            Due::Now => {
                let ordered = tokio::time::timeout(ORDER_TIMEOUT, order(store, accounts, &w))
                    .await
                    .unwrap_or_else(|_| Err("the order took too long".into()));
                match ordered.and_then(|pem| {
                    let (not_after, lifetime) =
                        legion_pki::validity(&pem).map_err(|e| e.to_string())?;
                    Ok((pem, not_after, not_after - lifetime / 3))
                }) {
                    Ok((pem, not_after, renew_at)) => {
                        match put_secret(store, &w.namespace, &w.secret, pem.as_bytes()).await {
                            Ok(()) => {
                                tracing::info!(
                                    hosts = ?w.hosts,
                                    secret = %format!("{}/{}", w.namespace, w.secret),
                                    "ACME certificate written"
                                );
                                until(renew_at);
                                AcmeStatus {
                                    not_after: Some(rfc3339(not_after)),
                                    ..Default::default()
                                }
                            }
                            Err(e) => failed(&w, &format!("writing the Secret: {e}"), now),
                        }
                    }
                    Err(e) => failed(&w, &e, now),
                }
            }
        };
        if let Some(at) = status.retry_at.as_deref().and_then(clock::parse) {
            until(UNIX_EPOCH + Duration::from_secs(at.max(0) as u64));
        }
        if let Err(e) = set_status(store, &w.routes, |s| *s = status.clone()).await {
            tracing::warn!("writing a route's status: {e}");
        }
    }
    next
}

/// The status after a failed order, with when to try again.
fn failed(w: &Wanted, why: &str, now: SystemTime) -> AcmeStatus {
    tracing::warn!(
        hosts = ?w.hosts,
        secret = %format!("{}/{}", w.namespace, w.secret),
        "ACME certificate: {why}"
    );
    let failures = w.status.failures.saturating_add(1);
    AcmeStatus {
        not_after: w.status.not_after.clone(),
        error: Some(why.to_owned()),
        failures,
        retry_at: Some(rfc3339(now + retry_after(failures))),
        ..Default::default()
    }
}

/// How long to wait after `failures` failures in a row.
fn retry_after(failures: u32) -> Duration {
    FIRST_RETRY
        .saturating_mul(1 << failures.saturating_sub(1).min(16))
        .min(LAST_RETRY)
}

fn rfc3339(t: SystemTime) -> String {
    clock::format(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
}

/// The certificates routes ask for, by Secret, and the routes with an
/// ACME status they no longer ask for one for.
fn wanted(store: &Store) -> Result<(Vec<Wanted>, Vec<ObjectKey>), String> {
    let routes = store
        .list(Kind::Route, None)
        .map_err(|e| e.to_string())?
        .objects;
    Ok(wanted_of(routes))
}

fn wanted_of(routes: Vec<Object>) -> (Vec<Wanted>, Vec<ObjectKey>) {
    let routes: Vec<(Object, RouteSpec)> = routes
        .into_iter()
        .filter_map(|o| {
            let spec = legion_api::parse::<RouteSpec>(&o.spec).ok()?;
            Some((o, spec))
        })
        .collect();
    // A host routes of more than one namespace claim gets no certificate:
    // it would go into a Secret its other namespaces' users may read.
    let contested = legion_api::route::contested(
        routes
            .iter()
            .map(|(o, spec)| (o.metadata.namespace.as_str(), spec.host.as_str())),
    );
    let mut by_secret: BTreeMap<(String, String), Wanted> = BTreeMap::new();
    let mut stale = Vec::new();
    for (o, spec) in routes {
        let status: RouteStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        let acme = spec
            .tls
            .and_then(|t| Some((t.secret, t.acme?)))
            .filter(|_| !contested.contains(&spec.host));
        let Some((secret, acme)) = acme else {
            if status.acme.is_some() {
                stale.push(o.key());
            }
            continue;
        };
        let status = status.acme.unwrap_or_default();
        let namespace = o.metadata.namespace.clone();
        let w = by_secret
            .entry((namespace.clone(), secret.clone()))
            .or_insert_with(|| Wanted {
                namespace,
                secret,
                hosts: BTreeSet::new(),
                acme,
                routes: Vec::new(),
                status: status.clone(),
            });
        // The routes' statuses agree once written; until then, as one
        // added since has none, the most failed holds: its backoff stands.
        if status.failures > w.status.failures {
            w.status = status;
        }
        w.hosts.insert(spec.host);
        w.routes.push(o.key());
    }
    (by_secret.into_values().collect(), stale)
}

fn due(store: &Store, w: &Wanted, now: SystemTime) -> Result<Due, String> {
    let key = ObjectKey::new(Kind::Secret, &w.namespace, &w.secret);
    let data = read_data(store, &key)?;
    if let Some(data) = &data
        && !data
            .windows(CERTIFICATE.len())
            .any(|w| w == CERTIFICATE.as_bytes())
    {
        return Ok(Due::Refused(format!(
            "Secret {}/{} holds no certificate, so it is not written over",
            w.namespace, w.secret
        )));
    }
    if let Some(pem) = data.and_then(|b| String::from_utf8(b).ok())
        && let Ok(names) = legion_pki::dns_names(&pem)
        && w.hosts.iter().all(|h| names.contains(h))
        && let Ok((not_after, lifetime)) = legion_pki::validity(&pem)
    {
        let renew_at = not_after - lifetime / 3;
        if now < renew_at {
            return Ok(Due::No {
                not_after,
                renew_at,
            });
        }
    }
    let retry_at = w.status.retry_at.as_deref().and_then(clock::parse);
    match retry_at.map(|at| UNIX_EPOCH + Duration::from_secs(at.max(0) as u64)) {
        Some(at) if at > now => Ok(Due::Later(at)),
        _ => Ok(Due::Now),
    }
}

/// Orders a certificate for `w`'s hosts: its chain, then its new key, in
/// PEM.
async fn order(store: &Store, accounts: &mut Accounts, w: &Wanted) -> Result<String, String> {
    let account = accounts.get(store, &w.acme).await?;
    let identifiers: Vec<Identifier> = w.hosts.iter().cloned().map(Identifier::Dns).collect();
    let mut order = account
        .new_order(&NewOrder::new(&identifiers))
        .await
        .map_err(|e| format!("ordering: {e}"))?;

    // The challenges are answered once every proxy may know them.
    let mut challenges = BTreeMap::new();
    let mut authorizations = order.authorizations();
    while let Some(a) = authorizations.next().await {
        let mut a = a.map_err(|e| format!("reading an authorization: {e}"))?;
        match a.status {
            AuthorizationStatus::Pending => {}
            AuthorizationStatus::Valid => continue,
            other => return Err(format!("{} is {other:?}", a.identifier())),
        }
        let name = a.identifier().to_string();
        let challenge = a
            .challenge(ChallengeType::Http01)
            .ok_or_else(|| format!("the authority offers no HTTP-01 challenge for {name}"))?;
        challenges.insert(
            challenge.token.clone(),
            challenge.key_authorization().as_str().to_owned(),
        );
    }
    set_status(store, &w.routes, |s| s.challenges = challenges.clone()).await?;

    let mut authorizations = order.authorizations();
    while let Some(a) = authorizations.next().await {
        let mut a = a.map_err(|e| format!("reading an authorization: {e}"))?;
        if a.status != AuthorizationStatus::Pending {
            continue;
        }
        if let Some(mut challenge) = a.challenge(ChallengeType::Http01) {
            challenge
                .set_ready()
                .await
                .map_err(|e| format!("answering a challenge: {e}"))?;
        }
    }
    let status = order
        .poll_ready(&POLL)
        .await
        .map_err(|e| format!("waiting for validation: {e}"))?;
    if status != OrderStatus::Ready {
        return Err(why_not(&mut order, status).await);
    }
    let key_pem = order
        .finalize()
        .await
        .map_err(|e| format!("finalizing: {e}"))?;
    let chain = order
        .poll_certificate(&POLL)
        .await
        .map_err(|e| format!("fetching the certificate: {e}"))?;
    let sep = if chain.ends_with('\n') { "" } else { "\n" };
    Ok(format!("{chain}{sep}{key_pem}"))
}

/// What the authority said of an order that did not become ready.
async fn why_not(order: &mut Order, status: OrderStatus) -> String {
    let mut problems = Vec::new();
    let mut authorizations = order.authorizations();
    while let Some(Ok(mut a)) = authorizations.next().await {
        let Ok(state) = a.refresh().await else {
            continue;
        };
        for c in &state.challenges {
            if let Some(problem) = &c.error {
                problems.push(format!("{}: {problem}", state.identifier()));
            }
        }
    }
    match problems.is_empty() {
        true => format!("the order is {status:?}"),
        false => format!("the order is {status:?}: {}", problems.join("; ")),
    }
}

/// The ACME accounts in use, by directory and email.
struct Accounts {
    /// Whether authorities on the loopback are allowed (tests).
    loopback: bool,
    known: BTreeMap<(String, Option<String>), Account>,
}

impl Accounts {
    /// The account for `acme`: known, kept in the store, or created.
    async fn get(&mut self, store: &Store, acme: &RouteAcme) -> Result<Account, String> {
        let which = (acme.directory.clone(), acme.email.clone());
        if let Some(a) = self.known.get(&which) {
            return Ok(a.clone());
        }
        let key = ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, &account_name(acme));
        let account = match read_data(store, &key)? {
            Some(json) => {
                let credentials: AccountCredentials = serde_json::from_slice(&json)
                    .map_err(|e| format!("the account in {key}: {e}"))?;
                Account::builder_with_http(http(&acme.directory, self.loopback)?)
                    .from_credentials(credentials)
                    .await
                    .map_err(|e| format!("the account in {key}: {e}"))?
            }
            None => {
                let contact = acme.email.as_ref().map(|e| format!("mailto:{e}"));
                let contact: Vec<&str> = contact.iter().map(String::as_str).collect();
                let (account, credentials) =
                    Account::builder_with_http(http(&acme.directory, self.loopback)?)
                        .create(
                            &NewAccount {
                                contact: &contact,
                                terms_of_service_agreed: true,
                                only_return_existing: false,
                            },
                            acme.directory.clone(),
                            None,
                        )
                        .await
                        .map_err(|e| format!("creating an account at {}: {e}", acme.directory))?;
                let json = serde_json::to_vec(&credentials).expect("credentials serialize");
                put_secret(store, SYSTEM_NAMESPACE, &key.name, &json).await?;
                account
            }
        };
        self.known.insert(which, account.clone());
        Ok(account)
    }
}

/// The Secret an account is kept in.
fn account_name(acme: &RouteAcme) -> String {
    let which = format!(
        "{}\n{}",
        acme.directory,
        acme.email.as_deref().unwrap_or("")
    );
    let digest = Sha256::digest(which.as_bytes());
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("acme-account-{hex}")
}

/// What an authority may answer at most, in bytes.
const MAX_ANSWER: usize = 1 << 20;

/// HTTPS to the authority, trusting what this host trusts, at public
/// addresses only: a route names the authority, and its directory names
/// the rest, so neither may send the manager to its own host or networks.
/// With `loopback` (tests), a directory over plain HTTP on the loopback,
/// and nothing but its own origin.
fn http(directory: &str, loopback: bool) -> Result<Box<dyn HttpClient>, String> {
    let plain = match directory.parse::<http::Uri>() {
        Ok(uri) if uri.scheme_str() == Some("http") => {
            if !loopback {
                return Err(format!(
                    "{directory}: this manager reaches ACME authorities over https:// only"
                ));
            }
            uri.authority().cloned()
        }
        _ => None,
    };
    let mut connector = HttpConnector::new_with_resolver(Public {
        gai: GaiResolver::new(),
        loopback,
    });
    connector.enforce_http(false);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_platform_verifier(tokio_rustls::rustls::crypto::ring::default_provider())
        .map_err(|e| format!("TLS to the authority: {e}"))?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(connector);
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, BodyWrapper<bytes::Bytes>>(https);
    Ok(Box::new(Authority {
        client,
        plain,
        loopback,
    }))
}

type HyperClient = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<HttpConnector<Public>>,
    BodyWrapper<bytes::Bytes>,
>;

/// Whether the manager may call an authority at `ip`: an address of the
/// internet, not of this host, its networks or Legion's.
fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_multicast()
                // "This network", and the shared space Legion's mesh uses.
                || a == 0
                || (a == 100 && (64..128).contains(&b))
                || a >= 240)
        }
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => public(IpAddr::V4(v4)),
            None => {
                let first = v6.segments()[0];
                !(v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    // Unique local, Legion's own included, and link-local.
                    || (first & 0xfe00) == 0xfc00
                    || (first & 0xffc0) == 0xfe80)
            }
        },
    }
}

/// Name lookups for [`Authority`] keeping only public addresses (and the
/// loopback's, in tests).
#[derive(Clone)]
struct Public {
    gai: GaiResolver,
    loopback: bool,
}

impl tower_service::Service<Name> for Public {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = std::io::Error;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.gai.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let loopback = self.loopback;
        let found = self.gai.call(name.clone());
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = found
                .await?
                .filter(|a| public(a.ip()) || (loopback && a.ip().is_loopback()))
                .collect();
            if addrs.is_empty() {
                return Err(std::io::Error::other(format!(
                    "{name} has no public address"
                )));
            }
            Ok(addrs.into_iter())
        })
    }
}

/// [`http`]'s client: the places allowed, and answers of bounded size.
struct Authority {
    client: HyperClient,
    /// The directory's origin when it is over plain HTTP (tests): then
    /// the only one called in the clear.
    plain: Option<http::uri::Authority>,
    loopback: bool,
}

impl Authority {
    /// Whether the manager calls `uri` for the authority. Host names are
    /// checked again once looked up, by [`Public`].
    fn allowed(&self, uri: &http::Uri) -> bool {
        match (uri.scheme_str(), &self.plain) {
            (Some("http"), Some(origin)) => uri.authority() == Some(origin),
            (Some("https"), _) => {
                let host = uri.host().unwrap_or_default();
                let host = host
                    .strip_prefix('[')
                    .and_then(|h| h.strip_suffix(']'))
                    .unwrap_or(host);
                match host.parse::<IpAddr>() {
                    Ok(ip) => public(ip) || (self.loopback && ip.is_loopback()),
                    Err(_) => {
                        !host.is_empty()
                            && (self.loopback || !host.eq_ignore_ascii_case("localhost"))
                    }
                }
            }
            _ => false,
        }
    }
}

impl HttpClient for Authority {
    fn request(
        &self,
        req: http::Request<BodyWrapper<bytes::Bytes>>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<instant_acme::BytesResponse, instant_acme::Error>> + Send>,
    > {
        if !self.allowed(req.uri()) {
            let why = format!("the authority sent the manager to {}", req.uri());
            return Box::pin(async move { Err(instant_acme::Error::Other(why.into())) });
        }
        let answer = self.client.request(req);
        Box::pin(async move {
            use http_body_util::BodyExt;
            let (parts, body) = answer
                .await
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?
                .into_parts();
            let body = http_body_util::Limited::new(body, MAX_ANSWER)
                .collect()
                .await
                .map_err(instant_acme::Error::Other)?
                .to_bytes();
            Ok(instant_acme::BytesResponse {
                parts,
                body: Box::new(body),
            })
        })
    }
}

/// The value of a Secret, if there is one.
fn read_data(store: &Store, key: &ObjectKey) -> Result<Option<Vec<u8>>, String> {
    let Some(o) = store.get(key).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let spec: DataSpec = serde_json::from_value(o.spec).map_err(|e| format!("{key}: {e}"))?;
    spec.bytes().map(Some).map_err(|e| format!("{key}: {e}"))
}

/// Writes `value` into a Secret, created if need be.
async fn put_secret(
    store: &Store,
    namespace: &str,
    name: &str,
    value: &[u8],
) -> Result<(), String> {
    let key = ObjectKey::new(Kind::Secret, namespace, name);
    let spec = serde_json::to_value(DataSpec {
        data: Some(STANDARD.encode(value)),
        text: None,
    })
    .expect("serializes");
    let (object, precondition) = match store.get(&key).map_err(|e| e.to_string())? {
        Some(mut o) => {
            let version = o.metadata.version;
            o.spec = spec;
            (o, Precondition::Version(version))
        }
        None => (
            Object::new(Kind::Secret, namespace, name, spec),
            Precondition::MustNotExist,
        ),
    };
    match write(store, object, precondition).await? {
        Outcome::Written(_) => Ok(()),
        other => Err(format!("{key}: {other:?}")),
    }
}

/// Changes the ACME status of `routes` with `change`, where it changes it.
async fn set_status(
    store: &Store,
    routes: &[ObjectKey],
    change: impl Fn(&mut AcmeStatus),
) -> Result<(), String> {
    change_status(store, routes, |s| {
        change(s.get_or_insert_with(Default::default))
    })
    .await
}

async fn change_status(
    store: &Store,
    routes: &[ObjectKey],
    change: impl Fn(&mut Option<AcmeStatus>),
) -> Result<(), String> {
    for key in routes {
        for _ in 0..3 {
            let Some(mut o) = store.get(key).map_err(|e| e.to_string())? else {
                break;
            };
            let mut status: RouteStatus =
                serde_json::from_value(o.status.clone()).unwrap_or_default();
            let mut acme = status.acme.clone();
            change(&mut acme);
            if status.acme == acme {
                break;
            }
            status.acme = acme;
            o.status = serde_json::to_value(status).expect("serializes");
            let version = o.metadata.version;
            match write(store, o, Precondition::Version(version)).await? {
                Outcome::Written(_) => break,
                Outcome::Conflict { .. } => continue,
                other => return Err(format!("{key}: {other:?}")),
            }
        }
    }
    Ok(())
}

async fn write(
    store: &Store,
    object: Object,
    precondition: Precondition,
) -> Result<Outcome, String> {
    let command = Command::Put {
        object,
        precondition,
    };
    let out = tokio::time::timeout(crate::COMMIT_TIMEOUT, store.raft.client_write(command))
        .await
        .map_err(|_| crate::no_quorum().message().to_owned())?
        .map_err(|e| e.to_string())?;
    Ok(out.data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_back_off_to_a_day() {
        assert_eq!(retry_after(1), FIRST_RETRY);
        assert_eq!(retry_after(2), FIRST_RETRY * 2);
        assert_eq!(retry_after(4), FIRST_RETRY * 8);
        assert_eq!(retry_after(9), LAST_RETRY);
        assert_eq!(retry_after(u32::MAX), LAST_RETRY);
    }

    #[test]
    fn routes_sharing_a_secret_keep_the_backoff_and_dropped_acme_goes() {
        let route = |name: &str, host: &str, acme: bool, status: serde_json::Value| {
            let tls = if acme {
                serde_json::json!({"secret": "cert", "acme": {}})
            } else {
                serde_json::json!({"secret": "cert"})
            };
            let mut o = Object::new(
                Kind::Route,
                "shop",
                name,
                serde_json::json!({"host": host, "service": "web", "tls": tls}),
            );
            o.status = status;
            o
        };
        let failed =
            serde_json::json!({"acme": {"failures": 3, "retryAt": "2026-11-04T10:00:00Z"}});
        // "a" sorts first and was just added: it has no status yet.
        let (wanted, stale) = wanted_of(vec![
            route("a", "new.example.org", true, serde_json::Value::Null),
            route("b", "www.example.org", true, failed.clone()),
            route("c", "old.example.org", false, failed),
        ]);
        assert_eq!(wanted.len(), 1);
        let w = &wanted[0];
        assert_eq!(w.status.failures, 3);
        assert!(w.status.retry_at.is_some());
        assert_eq!(w.hosts.len(), 2);
        assert_eq!(stale, vec![ObjectKey::new(Kind::Route, "shop", "c")]);
    }

    #[test]
    fn accounts_by_directory_and_email() {
        let acme = |directory: &str, email: Option<&str>| RouteAcme {
            directory: directory.into(),
            email: email.map(Into::into),
        };
        let a = account_name(&acme("https://a/dir", None));
        assert!(a.starts_with("acme-account-") && a.len() == 29, "{a}");
        legion_api::validate_name(&a).unwrap();
        assert_eq!(a, account_name(&acme("https://a/dir", None)));
        assert_ne!(a, account_name(&acme("https://a/dir", Some("x@y"))));
        assert_ne!(a, account_name(&acme("https://b/dir", None)));
    }

    fn authority(directory: &str, loopback: bool) -> Authority {
        let plain = directory
            .parse::<http::Uri>()
            .ok()
            .filter(|u| u.scheme_str() == Some("http"))
            .and_then(|u| u.authority().cloned());
        let connector = HttpConnector::new_with_resolver(Public {
            gai: GaiResolver::new(),
            loopback,
        });
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_provider_and_platform_verifier(
                tokio_rustls::rustls::crypto::ring::default_provider(),
            )
            .unwrap()
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        Authority {
            client: hyper_util::client::legacy::Client::builder(
                hyper_util::rt::TokioExecutor::new(),
            )
            .build(https),
            plain,
            loopback,
        }
    }

    fn allowed(a: &Authority, url: &str) -> bool {
        a.allowed(&url.parse().unwrap())
    }

    #[test]
    fn the_manager_calls_authorities_at_public_addresses_only() {
        for ip in [
            "127.0.0.1",
            "10.200.1.5",
            "192.168.1.1",
            "172.16.0.1",
            "169.254.169.254",
            "100.64.3.1",
            "0.0.0.0",
            "::1",
            "fd00:10:200:1::5",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!public(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["1.1.1.1", "172.32.0.1", "100.128.0.1", "2606:4700::1111"] {
            assert!(public(ip.parse().unwrap()), "{ip}");
        }

        let a = authority(LETS_ENCRYPT_TEST, false);
        assert!(allowed(&a, "https://acme.example.org/new-order"));
        assert!(!allowed(&a, "https://169.254.169.254/latest"));
        assert!(!allowed(&a, "https://[::1]:8443/x"));
        assert!(!allowed(&a, "https://localhost/x"));
        assert!(!allowed(&a, "http://acme.example.org/x"));
        assert!(http("http://127.0.0.1:14000/dir", false).is_err());

        // A test authority over plain HTTP is called at its own origin
        // alone.
        let a = authority("http://127.0.0.1:14000/dir", true);
        assert!(allowed(&a, "http://127.0.0.1:14000/nonce"));
        assert!(!allowed(&a, "http://127.0.0.1:9100/metrics"));
        assert!(!allowed(&a, "http://169.254.169.254/latest"));
    }

    const LETS_ENCRYPT_TEST: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";
}
