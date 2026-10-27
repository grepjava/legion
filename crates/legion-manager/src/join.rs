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

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::{Kind, Object, ObjectKey};
use legion_pki::{CERT_LIFETIME, Ca, Identity, Role, SERVER_NAME};
use legion_proto::v1::join_client::JoinClient;
use legion_proto::v1::{JoinRequest, JoinResponse};
use legion_store::{Command, NodeId, Outcome, Precondition, Raft, Store};
use openraft::{BasicNode, ChangeMembers};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Response, Status};

use crate::Tls;

/// Where Legion keeps its own records; the API refuses it.
pub const SYSTEM_NAMESPACE: &str = "legion-system";
const TOKENS: &str = "join-tokens";

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
            Role::Admin => None,
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
    pub ca: Arc<Ca>,
    pub store: Store,
}

impl Issuer {
    /// The token for `role`, made on first use; `rotate` makes a new one.
    /// Leader only: it writes.
    pub async fn token(&self, role: Role, rotate: bool) -> Result<Token, Status> {
        if role == Role::Admin {
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
                ca_digest: self.ca.digest(),
                secret,
            });
        }
        Err(Status::unavailable("join tokens keep changing; retry"))
    }

    /// Checks the token and certifies the node. Leader only: a manager
    /// joins Raft as a learner.
    pub async fn join(&self, req: JoinRequest) -> Result<JoinResponse, Status> {
        let token = Token::parse(&req.token).map_err(Status::invalid_argument)?;
        if token.ca_digest != self.ca.digest() {
            return Err(Status::permission_denied(
                "the token is for another cluster",
            ));
        }
        let (secrets, _) = read_secrets(&self.store)?;
        let role = secrets
            .role_of(&token.secret)
            .ok_or_else(|| Status::permission_denied("invalid or rotated join token"))?;
        legion_api::validate_name(&req.name)
            .map_err(|e| Status::invalid_argument(format!("node name: {e}")))?;
        let id = Identity::new(&req.name, role);
        let cert_pem = self
            .ca
            .sign(&req.csr_pem, &id, CERT_LIFETIME)
            .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
        let mut ca_key_pem = String::new();
        if role == Role::Manager {
            if req.node_id == 0 || req.advertise.is_empty() {
                return Err(Status::invalid_argument(
                    "a manager joins with its node id and advertised address",
                ));
            }
            self.store
                .raft
                .add_learner(
                    req.node_id,
                    BasicNode {
                        addr: req.advertise.clone(),
                    },
                    false,
                )
                .await
                .map_err(|e| Status::unavailable(format!("adding the manager: {e}")))?;
            ca_key_pem = self.ca.key_pem().to_owned();
        }
        tracing::info!(%id, node_id = req.node_id, advertise = %req.advertise, "node joined");
        Ok(JoinResponse {
            role: role.as_str().to_owned(),
            cert_pem,
            ca_pem: self.ca.cert_pem().to_owned(),
            ca_key_pem,
        })
    }
}

async fn write(
    store: &Store,
    object: Object,
    precondition: Precondition,
) -> Result<Outcome, Status> {
    store
        .raft
        .client_write(Command::Put {
            object,
            precondition,
        })
        .await
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
        self.api.join(req).await
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
        if let Err(e) = raft
            .change_membership(ChangeMembers::AddVoterIds(ready), false)
            .await
        {
            tracing::warn!("promoting managers: {e}");
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
pub async fn join(
    addr: &str,
    token: &str,
    name: &str,
    node_id: NodeId,
    advertise: &str,
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
    let resp = JoinClient::new(channel)
        .join(JoinRequest {
            token: token.to_string(),
            name: name.to_owned(),
            csr_pem,
            node_id,
            advertise: advertise.to_owned(),
        })
        .await?
        .into_inner();
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
