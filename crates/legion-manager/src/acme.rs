//! Certificates from an ACME authority for routes with `tls.acme`
//! (DESIGN.md §5.8), kept by the leader. Routes naming one Secret share a
//! certificate for all their hosts. The leader orders one when the Secret
//! holds none for every host, or one past two thirds of its life; puts the
//! authority's HTTP-01 challenges in the routes' status, from which the
//! dispatcher hands them to every ingress proxy to answer; and writes the
//! chain and its new key into the Secret, which the proxies read. A failed
//! order is tried again after ten minutes, then twice as long each time up
//! to a day, as the routes' status shows. Each directory and email has its
//! own account, kept in a Secret of `legion-system`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
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
/// `closing` turns true.
pub async fn run(store: Store, id: NodeId, closing: watch::Receiver<bool>) {
    crate::controller::while_leading(&store, id, closing, "ACME", || lead(&store)).await
}

async fn lead(store: &Store) {
    let mut accounts = Accounts::default();
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
    /// The first route's.
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
    let wanted = match wanted(store) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("reading routes: {e}");
            return Duration::from_secs(1);
        }
    };
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

/// The certificates routes ask for, by Secret.
fn wanted(store: &Store) -> Result<Vec<Wanted>, String> {
    let routes = store.list(Kind::Route, None).map_err(|e| e.to_string())?;
    let mut by_secret: BTreeMap<(String, String), Wanted> = BTreeMap::new();
    for o in routes.objects {
        let Ok(spec) = legion_api::parse::<RouteSpec>(&o.spec) else {
            continue;
        };
        let Some(tls) = spec.tls else { continue };
        let Some(acme) = tls.acme else { continue };
        let namespace = o.metadata.namespace.clone();
        let w = by_secret
            .entry((namespace.clone(), tls.secret.clone()))
            .or_insert_with(|| {
                let status: RouteStatus =
                    serde_json::from_value(o.status.clone()).unwrap_or_default();
                Wanted {
                    namespace,
                    secret: tls.secret,
                    hosts: BTreeSet::new(),
                    acme,
                    routes: Vec::new(),
                    status: status.acme.unwrap_or_default(),
                }
            });
        w.hosts.insert(spec.host);
        w.routes.push(o.key());
    }
    Ok(by_secret.into_values().collect())
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
#[derive(Default)]
struct Accounts(BTreeMap<(String, Option<String>), Account>);

impl Accounts {
    /// The account for `acme`: known, kept in the store, or created.
    async fn get(&mut self, store: &Store, acme: &RouteAcme) -> Result<Account, String> {
        let which = (acme.directory.clone(), acme.email.clone());
        if let Some(a) = self.0.get(&which) {
            return Ok(a.clone());
        }
        let key = ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, &account_name(acme));
        let account = match read_data(store, &key)? {
            Some(json) => {
                let credentials: AccountCredentials = serde_json::from_slice(&json)
                    .map_err(|e| format!("the account in {key}: {e}"))?;
                Account::builder_with_http(http()?)
                    .from_credentials(credentials)
                    .await
                    .map_err(|e| format!("the account in {key}: {e}"))?
            }
            None => {
                let contact = acme.email.as_ref().map(|e| format!("mailto:{e}"));
                let contact: Vec<&str> = contact.iter().map(String::as_str).collect();
                let (account, credentials) = Account::builder_with_http(http()?)
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
        self.0.insert(which, account.clone());
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

/// HTTPS to the authority, trusting what this host trusts; plain HTTP is
/// only allowed to loopback directories, by admission.
fn http() -> Result<Box<dyn HttpClient>, String> {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_platform_verifier(tokio_rustls::rustls::crypto::ring::default_provider())
        .map_err(|e| format!("TLS to the authority: {e}"))?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, BodyWrapper<bytes::Bytes>>(https);
    Ok(Box::new(client))
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
    for key in routes {
        for _ in 0..3 {
            let Some(mut o) = store.get(key).map_err(|e| e.to_string())? else {
                break;
            };
            let mut status: RouteStatus =
                serde_json::from_value(o.status.clone()).unwrap_or_default();
            let mut acme = status.acme.clone().unwrap_or_default();
            change(&mut acme);
            if status.acme.as_ref() == Some(&acme) {
                break;
            }
            status.acme = Some(acme);
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
}
