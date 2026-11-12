//! Joining a cluster (DESIGN.md §9).
//!
//! A join token is `LGN-1-<ca-digest>-<secret>`: the digest names the
//! cluster's CA, the secret the role (manager or worker). The secrets live
//! in the store, in the reserved namespace [`SYSTEM_NAMESPACE`], which the
//! API never shows or changes.
//!
//! A joining node has no certificate and does not know the CA yet. It
//! reads the CA from the chain a manager presents in its TLS handshake,
//! keeps it only if it matches the token's digest, then connects again,
//! verifying the manager with that CA, and sends the token with a
//! certificate request for its own key. The manager signs it for the
//! token's role. A manager also gets the CA key, so it can issue
//! certificates in turn, and becomes a Raft learner; the leader promotes
//! learners to voters once they have caught up.

use legion_api::protocol::Protocol;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::{Kind, Object, ObjectKey};
use legion_pki::{CERT_LIFETIME, Ca, Identity, Role, SERVER_NAME};
use legion_proto::v1::join_client::JoinClient;
use legion_proto::v1::{JoinRequest, JoinResponse};
use legion_store::{Command, NodeId, Outcome, Precondition, Raft, Store};
use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Response, Status};

use crate::Tls;

/// Where Legion keeps its own records; the API refuses it.
pub const SYSTEM_NAMESPACE: &str = "legion-system";
const TOKENS: &str = "join-tokens";
/// How long a manager's join waits for another membership change.
const MEMBERSHIP_WAIT: Duration = Duration::from_secs(30);

/// A parsed join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub ca_digest: String,
    pub secret: String,
}

impl Token {
    pub fn parse(s: &str) -> Result<Token, String> {
        let bad = || "not a Legion join token (LGN-1-<ca-digest>-<secret>)".to_owned();
        let mut parts = s.trim().splitn(4, '-');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("LGN"), Some("1"), Some(digest), Some(secret))
                if digest.len() == 64
                    && digest.bytes().all(|b| b.is_ascii_hexdigit())
                    && !secret.is_empty() =>
            {
                Ok(Token {
                    ca_digest: digest.to_ascii_lowercase(),
                    secret: secret.to_owned(),
                })
            }
            _ => Err(bad()),
        }
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LGN-1-{}-{}", self.ca_digest, self.secret)
    }
}

/// The secret of each role, as stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Secrets {
    manager: String,
    worker: String,
}

impl Secrets {
    fn get_mut(&mut self, role: Role) -> Option<&mut String> {
        match role {
            Role::Manager => Some(&mut self.manager),
            Role::Worker => Some(&mut self.worker),
            Role::Admin | Role::User => None,
        }
    }

    fn role_of(&self, secret: &str) -> Option<Role> {
        [(Role::Manager, &self.manager), (Role::Worker, &self.worker)]
            .into_iter()
            .find(|(_, s)| !s.is_empty() && same(s.as_bytes(), secret.as_bytes()))
            .map(|(r, _)| r)
    }
}

/// Compares without stopping at the first difference.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn tokens_key() -> ObjectKey {
    ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, TOKENS)
}

fn read_secrets(store: &Store) -> Result<(Secrets, Option<u64>), Status> {
    match store
        .get(&tokens_key())
        .map_err(|e| Status::internal(e.to_string()))?
    {
        Some(o) => {
            let s = serde_json::from_value(o.spec).map_err(|e| Status::internal(e.to_string()))?;
            Ok((s, Some(o.metadata.version)))
        }
        None => Ok((Secrets::default(), None)),
    }
}

/// The leader's half of the join protocol, with the CA in hand.
#[derive(Clone)]
pub(crate) struct Issuer {
    /// The CA it signs with, which a rotation swaps.
    pub ca: crate::ca::Authority,
    /// This manager's credentials: the CAs the cluster trusts, which a
    /// joining node is given.
    pub credentials: Arc<crate::Credentials>,
    pub store: Store,
}

impl Issuer {
    /// The CA to sign with: refused while this manager has not yet taken
    /// up the CA the store records (a rotation begun moments ago), so no
    /// certificate of the CA being left is issued after it began.
    pub fn current(&self) -> Result<Arc<Ca>, Status> {
        let ca = self.ca.ca();
        match crate::ca::current(&self.store).map_err(Status::internal)? {
            Some((record, _)) if record.digest() != ca.digest() => Err(Status::unavailable(
                "this manager is taking up the cluster's new CA; retry",
            )),
            _ => Ok(ca),
        }
    }

    /// The token for `role`, made on first use; `rotate` makes a new one.
    /// Leader only: it writes.
    pub async fn token(&self, role: Role, rotate: bool) -> Result<Token, Status> {
        if !matches!(role, Role::Manager | Role::Worker) {
            return Err(Status::invalid_argument(
                "join tokens are for managers and workers",
            ));
        }
        for _ in 0..5 {
            let (mut secrets, version) = read_secrets(&self.store)?;
            let slot = secrets.get_mut(role).expect("manager or worker");
            if slot.is_empty() || rotate {
                *slot = legion_pki::random_secret();
                let spec = serde_json::to_value(&secrets).expect("serialises");
                let object = Object::new(Kind::Secret, SYSTEM_NAMESPACE, TOKENS, spec);
                let precondition =
                    version.map_or(Precondition::MustNotExist, Precondition::Version);
                match write(&self.store, object, precondition).await? {
                    Outcome::Written(_) => {}
                    Outcome::Conflict { .. } => continue,
                    other => return Err(Status::internal(format!("unexpected {other:?}"))),
                }
            }
            let secret = match role {
                Role::Manager => secrets.manager,
                _ => secrets.worker,
            };
            return Ok(Token {
                ca_digest: self.current()?.digest(),
                secret,
            });
        }
        Err(Status::unavailable("join tokens keep changing; retry"))
    }

    /// Checks the token and certifies the node. Leader only: a manager
    /// joins Raft as a learner.
    pub async fn join(
        &self,
        api: &crate::api::ApiService,
        req: JoinRequest,
    ) -> Result<JoinResponse, Status> {
        let token = Token::parse(&req.token).map_err(Status::invalid_argument)?;
        let ca = self.current()?;
        if token.ca_digest != ca.digest() {
            // A token shown before a CA rotation names the old CA.
            let trusted = legion_pki::split_certs(&self.credentials.ca_pem())
                .iter()
                .any(|c| legion_pki::digest(c).is_ok_and(|d| d == token.ca_digest));
            return Err(Status::permission_denied(if trusted {
                "the token names the cluster's CA before its rotation: get a new one (`legion join-token`)"
            } else {
                "the token is for another cluster"
            }));
        }
        let (secrets, _) = read_secrets(&self.store)?;
        let role = secrets
            .role_of(&token.secret)
            .ok_or_else(|| Status::permission_denied("invalid or rotated join token"))?;
        legion_api::validate_name(&req.name)
            .map_err(|e| Status::invalid_argument(format!("node name: {e}")))?;
        // A node's name is who it is: a token certifies a new node, never
        // one in the cluster (which is removed first to join again).
        let node = ObjectKey::new(Kind::Node, "", &req.name);
        let in_cluster = || {
            Status::already_exists(format!(
                "node {0} is in the cluster; delete its Node (`legion delete node {0}`) to join it again",
                req.name
            ))
        };
        let registered = |store: &Store| {
            store
                .get(&node)
                .map(|n| n.is_some())
                .map_err(|e| Status::internal(e.to_string()))
        };
        if registered(&self.store)? {
            return Err(in_cluster());
        }
        if role == Role::Manager && (req.node_id == 0 || req.advertise.is_empty()) {
            return Err(Status::invalid_argument(
                "a manager joins with its node id and advertised address",
            ));
        }
        // A manager applies the log, which may hold commands of the
        // floor's protocol.
        let protocol = match role {
            Role::Manager => api.speaking.among_managers(),
            _ => api.protocol,
        };
        protocol
            .works_with(
                Protocol::said(req.protocol, req.oldest_protocol),
                &format!("node {}", req.name),
            )
            .map_err(Status::unimplemented)?;
        let id = Identity::new(&req.name, role);
        // A bad request is refused before anything is written.
        ca.sign(&req.csr_pem, &id, CERT_LIFETIME)
            .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
        // Run to the end even if the caller goes: stopped between its
        // writes, it would leave the name's earlier certificates refused
        // for a join that did not happen.
        let (issuer, api, task_req) = (self.clone(), api.clone(), req.clone());
        let cert_pem = tokio::spawn(async move { issuer.admit(&api, &task_req, role).await })
            .await
            .map_err(|e| Status::internal(e.to_string()))??;
        let mut ca_key_pem = String::new();
        if role == Role::Manager {
            ca_key_pem = ca.key_pem().to_owned();
        }
        tracing::info!(%id, node_id = req.node_id, advertise = %req.advertise, "node joined");
        Ok(JoinResponse {
            role: role.as_str().to_owned(),
            cert_pem,
            // The issuing CA first, then any other trusted while the CA is
            // rotated: the node trusts them all.
            ca_pem: trust_for(&ca, &self.credentials.ca_pem()),
            ca_key_pem,
            protocol: protocol.speaks,
            oldest_protocol: protocol.oldest,
        })
    }
}

impl Issuer {
    /// Certifies the node `req` joins, alone, and adds a manager to Raft
    /// as a learner; undoes the certifying's record if that fails.
    async fn admit(
        &self,
        api: &crate::api::ApiService,
        req: &JoinRequest,
        role: Role,
    ) -> Result<String, Status> {
        let node = ObjectKey::new(Kind::Node, "", &req.name);
        let in_cluster = || {
            Status::already_exists(format!(
                "node {0} is in the cluster; delete its Node (`legion delete node {0}`) to join it again",
                req.name
            ))
        };
        let registered = || {
            self.store
                .get(&node)
                .map(|n| n.is_some())
                .map_err(|e| Status::internal(e.to_string()))
        };
        let id = Identity::new(&req.name, role);
        let ca = self.current()?;
        let certify = || {
            ca.sign(&req.csr_pem, &id, CERT_LIFETIME)
                .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))
        };
        let (cert_pem, recorded) = self
            .certify_alone(api, &req.name, certify, registered, in_cluster)
            .await?;
        if role == Role::Manager
            && let Err(e) = self.add_learner(req).await
        {
            undo(api, &req.name, recorded).await;
            return Err(e);
        }
        Ok(cert_pem)
    }

    /// Adds the manager `req` joins to Raft as a learner.
    async fn add_learner(&self, req: &JoinRequest) -> Result<(), Status> {
        // Raft takes one membership change at a time: wait for the one in
        // flight (another manager joining or being promoted), which takes
        // a few round trips.
        let deadline = tokio::time::Instant::now() + MEMBERSHIP_WAIT;
        loop {
            let node = BasicNode {
                addr: req.advertise.clone(),
            };
            let add = self.store.raft.add_learner(req.node_id, node, false);
            let added = tokio::time::timeout(crate::COMMIT_TIMEOUT, add)
                .await
                .map_err(|_| crate::no_quorum())?;
            match added {
                Ok(_) => return Ok(()),
                Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                    ChangeMembershipError::InProgress(_),
                ))) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => {
                    return Err(Status::unavailable(format!("adding the manager: {e}")));
                }
            }
        }
    }

    /// The certificate `certify` makes for `node`, the only one of it in
    /// use: every earlier one, from a join whose node has not registered
    /// (two joins of a name before either registers would each get one),
    /// is refused from now, and so is this one once the name joins again.
    /// A join's record of the name comes first, and its certificate is
    /// issued in the very next second, so the next record, at least a
    /// second later, covers it; one issued later than that is not handed
    /// out. Refused if the name is joined again or removed meanwhile, or
    /// registered by then (`in_cluster`).
    /// Its record is undone if it fails (or the name turns out to be in
    /// the cluster); returned with the certificate, for the caller to undo
    /// if what follows fails.
    async fn certify_alone(
        &self,
        api: &crate::api::ApiService,
        node: &str,
        certify: impl Fn() -> Result<String, Status>,
        registered: impl Fn() -> Result<bool, Status>,
        in_cluster: impl Fn() -> Status,
    ) -> Result<(String, crate::removed::Recorded), Status> {
        // What undoes every record made so far: a retry's record replaces
        // the one before it.
        let mut made: Option<crate::removed::Recorded> = None;
        let certified = async {
            for _ in 0..3 {
                let recorded = crate::removed::supersede(api, node).await?;
                let recorded = match made {
                    Some(earlier) => recorded.after(earlier),
                    None => recorded,
                };
                made = Some(recorded);
                crate::removed::issue_after(&self.store, node).await?;
                let cert_pem = certify()?;
                let issued = issued_second(&cert_pem)?;
                if issued != recorded.at() + 1 {
                    // Too late: the next record refuses it.
                    continue;
                }
                api.consistent().await?;
                if crate::removed::at(&self.store, node)? != Some(recorded.at()) {
                    return Err(Status::aborted(format!(
                        "node {node} is joining again or being removed; retry"
                    )));
                }
                // Registered by an earlier join's node between the check
                // and the record: that node keeps its name.
                if registered()? {
                    return Err(in_cluster());
                }
                return Ok((cert_pem, recorded));
            }
            Err(Status::unavailable(format!(
                "certifying node {node} keeps taking too long; retry"
            )))
        }
        .await;
        if certified.is_err()
            && let Some(recorded) = made
        {
            undo(api, node, recorded).await;
        }
        certified
    }
}

/// Undoes a join's record of `node`, which certified nothing, tried a few
/// times; a failure leaves the name's earlier certificates refused, for
/// them to join again.
async fn undo(api: &crate::api::ApiService, node: &str, recorded: crate::removed::Recorded) {
    const TRIES: u32 = 3;
    for tried in 1..=TRIES {
        match crate::removed::forget(api, node, recorded).await {
            Ok(()) => return,
            Err(e) if tried == TRIES => {
                tracing::warn!(node, error = %e, "could not undo a failed join's record of the node");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250 * u64::from(tried))).await,
        }
    }
}

/// The second the certificate in `pem` was issued.
fn issued_second(pem: &str) -> Result<u64, Status> {
    let at = legion_pki::issued(pem).map_err(|e| Status::internal(e.to_string()))?;
    Ok(at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs())
}

/// The CAs a node is to trust, `ca` (which issues) first: `trust_pem` as
/// this manager has it, which begins with `ca` once it has taken it up.
fn trust_for(ca: &Ca, trust_pem: &str) -> String {
    let certs = legion_pki::split_certs(trust_pem);
    if certs.first().map(String::as_str) == Some(ca.cert_pem()) {
        trust_pem.to_owned()
    } else {
        ca.cert_pem().to_owned()
    }
}

async fn write(
    store: &Store,
    object: Object,
    precondition: Precondition,
) -> Result<Outcome, Status> {
    let put = store.raft.client_write(Command::Put {
        object,
        precondition,
    });
    tokio::time::timeout(crate::COMMIT_TIMEOUT, put)
        .await
        .map_err(|_| crate::no_quorum())?
        .map(|r| r.data)
        .map_err(|e| Status::unavailable(e.to_string()))
}

/// The `Join` service: open to callers without a certificate.
pub(crate) struct JoinService {
    pub api: crate::api::ApiService,
}

#[tonic::async_trait]
impl legion_proto::v1::join_server::Join for JoinService {
    async fn join(&self, req: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        // A joining node has no certificate yet, only a token: it is named
        // as it asks to be. Recorded where the join first arrives.
        let point = self.api.audit_point(
            &req,
            "join",
            crate::audit::target("Node", "", &req.get_ref().name),
        );
        let point = point.map(|(_, action, target)| {
            let actor = legion_api::audit::Actor {
                kind: "joining".into(),
                name: req.get_ref().name.clone(),
            };
            (actor, action, target)
        });
        let joined = self.api.join(req).await;
        self.api.audit_end(point, joined)
    }
}

/// While this manager leads, promotes learners that have caught up to
/// voters, until `closing`.
pub(crate) async fn promote_learners(raft: Raft, mut closing: tokio::sync::watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = closing.wait_for(|c| *c) => return,
        }
        let m = raft.metrics().borrow().clone();
        if m.current_leader != Some(m.id) {
            continue;
        }
        let membership = m.membership_config.membership();
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let last = m.last_log_index.unwrap_or(0);
        let ready: BTreeSet<NodeId> = membership
            .nodes()
            .map(|(id, _)| *id)
            .filter(|id| !voters.contains(id))
            .filter(|id| {
                m.replication
                    .as_ref()
                    .and_then(|r| r.get(id).cloned().flatten())
                    .is_some_and(|matched| matched.index + 10 >= last)
            })
            .collect();
        if ready.is_empty() {
            continue;
        }
        tracing::info!(?ready, "promoting caught-up managers to voters");
        // Without a quorum (the manager promoted stopped meanwhile) the
        // change waits for one: not past shutdown, which waits for this.
        tokio::select! {
            changed = raft.change_membership(ChangeMembers::AddVoterIds(ready), false) => {
                if let Err(e) = changed {
                    tracing::warn!("promoting managers: {e}");
                }
            }
            _ = closing.wait_for(|c| *c) => return,
        }
    }
}

/// What a node gets for joining.
#[derive(Debug, Clone)]
pub struct Joined {
    pub role: Role,
    /// The node's credentials; `ca_key_pem` is set for managers.
    pub tls: Tls,
}

/// Joins the cluster at `addr` (`host:port` of any manager) as `name`.
/// Managers pass their Raft id and the address other managers reach them
/// at.
/// How long a join waits out a cluster without a leader.
const JOIN_PATIENCE: Duration = Duration::from_secs(15);

pub async fn join(
    addr: &str,
    token: &str,
    name: &str,
    node_id: NodeId,
    advertise: &str,
) -> Result<Joined, crate::Error> {
    join_as(addr, token, name, node_id, advertise, Protocol::current()).await
}

/// [`join`], saying it speaks `protocol`: for tests posing as another
/// build.
pub async fn join_as(
    addr: &str,
    token: &str,
    name: &str,
    node_id: NodeId,
    advertise: &str,
    protocol: Protocol,
) -> Result<Joined, crate::Error> {
    let token = Token::parse(token)?;
    let ca_pem = fetch_ca(addr, &token.ca_digest).await?;
    let (key_pem, csr_pem) = legion_pki::request()?;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&ca_pem))
        .domain_name(SERVER_NAME);
    let channel = Channel::from_shared(format!("https://{addr}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    let request = JoinRequest {
        token: token.to_string(),
        name: name.to_owned(),
        csr_pem,
        node_id,
        advertise: advertise.to_owned(),
        protocol: protocol.speaks,
        oldest_protocol: protocol.oldest,
    };
    // A cluster between leaders answers Unavailable for a moment; a failed
    // try leaves nothing behind.
    let deadline = tokio::time::Instant::now() + JOIN_PATIENCE;
    let resp = loop {
        match JoinClient::new(channel.clone()).join(request.clone()).await {
            Err(s)
                if s.code() == tonic::Code::Unavailable
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            answer => break answer?.into_inner(),
        }
    };
    // A manager from before the handshake takes anyone: the node checks.
    protocol.works_with(
        Protocol::said(resp.protocol, resp.oldest_protocol),
        "the cluster",
    )?;
    let role = Role::parse(&resp.role).ok_or("the manager answered with an unknown role")?;
    if resp.ca_pem.trim() != ca_pem.trim() && legion_pki::digest(&resp.ca_pem)? != token.ca_digest {
        return Err("the manager answered with another CA".into());
    }
    Ok(Joined {
        role,
        tls: Tls {
            ca_pem,
            cert_pem: resp.cert_pem,
            key_pem,
            ca_key_pem: (!resp.ca_key_pem.is_empty()).then_some(resp.ca_key_pem),
        },
    })
}

/// The CA certificate a manager presents in its handshake, if it is the
/// one with `digest`. Nothing is sent before the CA is known.
async fn fetch_ca(addr: &str, digest: &str) -> Result<String, crate::Error> {
    use tokio_rustls::rustls;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let capture = Arc::new(Capture {
        certs: Mutex::default(),
        provider: provider.clone(),
    });
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(capture.clone())
        .with_no_client_auth();
    let tcp = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connecting to {addr}: timed out"))??;
    let name = rustls::pki_types::ServerName::try_from(SERVER_NAME)?;
    // The handshake may fail later on (no client certificate); the chain
    // is captured before that.
    let _ = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await;
    let certs = capture
        .certs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    certs
        .iter()
        .find(|der| legion_pki::digest_der(der) == digest)
        .map(|der| legion_pki::cert_pem(der))
        .ok_or_else(|| {
            format!("{addr} is not a manager of the cluster this token is for (CA digest mismatch)")
                .into()
        })
}

/// Accepts any server certificate and keeps the chain: only for reading a
/// manager's CA, which is then checked against the token.
#[derive(Debug)]
struct Capture {
    certs: Mutex<Vec<Vec<u8>>>,
    provider: Arc<tokio_rustls::rustls::crypto::CryptoProvider>,
}

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for Capture {
    fn verify_server_cert(
        &self,
        end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        let mut certs = self.certs.lock().unwrap_or_else(|e| e.into_inner());
        certs.push(end_entity.to_vec());
        certs.extend(intermediates.iter().map(|c| c.to_vec()));
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        tokio_rustls::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        tokio_rustls::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens() {
        let digest = "ab".repeat(32);
        let t = Token::parse(&format!("LGN-1-{digest}-s3cr3t")).unwrap();
        assert_eq!(t.ca_digest, digest);
        assert_eq!(t.secret, "s3cr3t");
        assert_eq!(t.to_string(), format!("LGN-1-{digest}-s3cr3t"));
        for bad in [
            "",
            "LGN-1-abc-s",
            &format!("LGN-2-{digest}-s"),
            &format!("LGN-1-{digest}-"),
        ] {
            assert!(Token::parse(bad).is_err(), "{bad}");
        }
        let s = Secrets {
            manager: "m".into(),
            worker: "w".into(),
        };
        assert_eq!(s.role_of("m"), Some(Role::Manager));
        assert_eq!(s.role_of("w"), Some(Role::Worker));
        assert_eq!(s.role_of("x"), None);
        assert_eq!(Secrets::default().role_of(""), None);
    }
}
