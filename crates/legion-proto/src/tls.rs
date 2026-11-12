//! The cluster's mutual TLS transport (DESIGN.md §9), as both ends use it:
//! managers serve with it and call each other with it, agents call
//! managers with it. A node's [`Credentials`] pick its certificate at each
//! handshake, so a renewed one takes over without a restart.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use legion_pki::SERVER_NAME;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::{ResolvesClientCert, WebPkiServerVerifier};
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A node's credentials, shared by its server and every client it makes:
/// the certificate is picked per handshake, so a renewed one takes over
/// each connection made after [`Credentials::renew`]; those already open
/// keep going. So are the CAs peers are checked against, which a CA
/// rotation changes ([`Credentials::trust`]).
#[derive(Debug)]
pub struct Credentials {
    keys: Arc<Keys>,
    trust: Arc<Trust>,
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

/// The CAs peers are checked against, as PEM, and the verifiers made from
/// them: one CA, or two while the cluster's CA is rotated.
#[derive(Debug)]
struct Trust(RwLock<Verifiers>);

#[derive(Debug)]
struct Verifiers {
    pem: String,
    server: Arc<dyn ServerCertVerifier>,
    client: Arc<dyn ClientCertVerifier>,
}

impl Trust {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Verifiers> {
        self.0.read().unwrap_or_else(|e| e.into_inner())
    }
}

fn verifiers(provider: &Arc<CryptoProvider>, trust_pem: &str) -> Result<Verifiers, Error> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(trust_pem.as_bytes()) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err("no CA to trust".into());
    }
    let roots = Arc::new(roots);
    // Client certificates are optional in the handshake, for `Join`;
    // every other service requires one.
    let client = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
        .allow_unauthenticated()
        .build()?;
    let server = WebPkiServerVerifier::builder_with_provider(roots, provider.clone()).build()?;
    Ok(Verifiers {
        pem: trust_pem.to_owned(),
        server,
        client,
    })
}

impl ServerCertVerifier for Trust {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let server = self.read().server.clone();
        server.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let server = self.read().server.clone();
        server.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let server = self.read().server.clone();
        server.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.read().server.supported_verify_schemes()
    }
}

impl ClientCertVerifier for Trust {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        false
    }

    /// No hints: a client presents the one certificate it has.
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let client = self.read().client.clone();
        client.verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let client = self.read().client.clone();
        client.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let client = self.read().client.clone();
        client.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.read().client.supported_verify_schemes()
    }
}

/// The current certificate and key.
#[derive(Debug)]
struct Keys(RwLock<Current>);

#[derive(Debug)]
struct Current {
    cert_pem: String,
    key_pem: String,
    /// The certificate and the CA, as the server presents them (joining
    /// nodes read the CA from the chain, and check it against their token,
    /// before they trust it).
    chain: Arc<CertifiedKey>,
}

impl Keys {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Current> {
        self.0.read().unwrap_or_else(|e| e.into_inner())
    }
}

impl ResolvesServerCert for Keys {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.read().chain.clone())
    }
}

impl ResolvesClientCert for Keys {
    fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(self.read().chain.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

const ALPN_H2: &[u8] = b"h2";

fn provider() -> Arc<CryptoProvider> {
    Arc::new(tokio_rustls::rustls::crypto::ring::default_provider())
}

/// The certificate with the CA after it, and the key, checked to belong
/// to the certificate, and the certificate checked to be the CA's.
fn certified(
    provider: &Arc<CryptoProvider>,
    ca_pem: &str,
    cert_pem: &str,
    key_pem: &str,
) -> Result<CertifiedKey, Error> {
    let mut certs =
        CertificateDer::pem_slice_iter(cert_pem.as_bytes()).collect::<Result<Vec<_>, _>>()?;
    let Some(leaf) = certs.first().cloned() else {
        return Err("no certificate".into());
    };
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        let cert = cert?;
        roots.add(cert.clone())?;
        certs.push(cert);
    }
    // Checked as of when it was issued: whether it has expired is for the
    // peers to say, its issuer is not.
    let issued = legion_pki::issued_at(&leaf)?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()?
        .verify_client_cert(&leaf, &certs[1..], UnixTime::since_unix_epoch(issued))
        .map_err(|e| format!("the certificate is not the cluster CA's: {e}"))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())?;
    let key = provider.key_provider.load_private_key(key)?;
    let certified = CertifiedKey::new(certs, key);
    certified
        .keys_match()
        .map_err(|e| format!("the key is not the certificate's: {e}"))?;
    Ok(certified)
}

impl Credentials {
    /// The cluster CA, which verifies peers (two while it is rotated, one
    /// after the other in `ca_pem`), and this node's certificate and key.
    pub fn new(ca_pem: &str, cert_pem: &str, key_pem: &str) -> Result<Arc<Credentials>, Error> {
        let provider = provider();
        let keys = Arc::new(Keys(RwLock::new(Current {
            cert_pem: cert_pem.to_owned(),
            key_pem: key_pem.to_owned(),
            chain: Arc::new(certified(&provider, ca_pem, cert_pem, key_pem)?),
        })));
        let trust = Arc::new(Trust(RwLock::new(verifiers(&provider, ca_pem)?)));
        let mut server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(trust.clone())
            .with_cert_resolver(keys.clone());
        server.alpn_protocols = vec![ALPN_H2.to_vec()];
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(trust.clone())
            .with_client_cert_resolver(keys.clone());
        client.alpn_protocols = vec![ALPN_H2.to_vec()];
        Ok(Arc::new(Credentials {
            keys,
            trust,
            server: Arc::new(server),
            client: Arc::new(client),
        }))
    }

    /// Puts a renewed certificate (from a CA trusted now) and its key in
    /// place; refused, the old ones kept, if they do not belong together.
    pub fn renew(&self, cert_pem: &str, key_pem: &str) -> Result<(), Error> {
        let trust = self.ca_pem();
        let chain = Arc::new(certified(&provider(), &trust, cert_pem, key_pem)?);
        *self.keys.0.write().unwrap_or_else(|e| e.into_inner()) = Current {
            cert_pem: cert_pem.to_owned(),
            key_pem: key_pem.to_owned(),
            chain,
        };
        Ok(())
    }

    /// Checks peers against the CAs in `trust_pem` from now on, as a CA
    /// rotation begins (both CAs) and ends (the new one): each handshake
    /// after takes them, connections already open keep going. Refused,
    /// nothing changed, if this node's own certificate is not of one of
    /// them: it would be refused by peers that trust the same.
    pub fn trust(&self, trust_pem: &str) -> Result<(), Error> {
        let provider = provider();
        let (cert_pem, key_pem) = self.cert_and_key();
        let chain = Arc::new(certified(&provider, trust_pem, &cert_pem, &key_pem)?);
        let verifiers = verifiers(&provider, trust_pem)?;
        let mut keys = self.keys.0.write().unwrap_or_else(|e| e.into_inner());
        // The chain presented carries the CAs trusted, which a joining node
        // reads its CA from.
        keys.chain = chain;
        *self.trust.0.write().unwrap_or_else(|e| e.into_inner()) = verifiers;
        Ok(())
    }

    /// The CAs peers are checked against, as PEM.
    pub fn ca_pem(&self) -> String {
        self.trust.read().pem.clone()
    }

    /// The current certificate and its key.
    pub fn cert_and_key(&self) -> (String, String) {
        let current = self.keys.read();
        (current.cert_pem.clone(), current.key_pem.clone())
    }

    /// A channel to the manager at `endpoint` (`http://host:port`, with its
    /// settings made), which connects when first used and presents the
    /// certificate current at each connection.
    pub fn channel(&self, endpoint: Endpoint) -> Channel {
        endpoint.connect_with_connector_lazy(Connector {
            config: self.client.clone(),
        })
    }

    /// The TLS connections `listener` accepts, each with the certificate
    /// current at its handshake. Handshakes run on their own, so a slow
    /// client holds up no other; ends when the stream is dropped.
    pub fn incoming(
        &self,
        listener: TcpListener,
    ) -> ReceiverStream<Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>> {
        let (tx, rx) = mpsc::channel(64);
        let acceptor = TlsAcceptor::from(self.server.clone());
        let handshakes = Arc::new(tokio::sync::Semaphore::new(MAX_HANDSHAKES));
        tokio::spawn(async move {
            loop {
                // A flood of connections waits in the backlog, not in tasks.
                let permit = tokio::select! {
                    p = handshakes.clone().acquire_owned() => p.expect("never closed"),
                    _ = tx.closed() => return,
                };
                let accepted = tokio::select! {
                    a = listener.accept() => a,
                    _ = tx.closed() => return,
                };
                let (tcp, peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        // Out of descriptors, say: wait rather than spin.
                        tracing::warn!("accepting a connection: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let _ = tcp.set_nodelay(true);
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    let handshake =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await;
                    drop(permit);
                    match handshake {
                        Ok(Ok(tls)) => {
                            let _ = tx.send(Ok(tls)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, "TLS handshake: {e}"),
                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                    }
                });
            }
        });
        ReceiverStream::new(rx)
    }
}

/// TLS handshakes a listener runs at once.
const MAX_HANDSHAKES: usize = 256;

/// How long a client has to finish its TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connects to a manager over TCP and TLS, for [`Credentials::channel`].
#[derive(Clone)]
struct Connector {
    config: Arc<ClientConfig>,
}

impl tower_service::Service<Uri> for Connector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let config = self.config.clone();
        Box::pin(async move {
            let host = uri.host().ok_or("no host to connect to")?;
            let host = host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let port = uri.port_u16().ok_or("no port to connect to")?;
            let connect = async {
                let tcp = TcpStream::connect((host.as_str(), port)).await?;
                tcp.set_nodelay(true)?;
                let name = ServerName::try_from(SERVER_NAME)?.to_owned();
                let tls = TlsConnector::from(config).connect(name, tcp).await?;
                Ok::<_, Error>(TokioIo::new(tls))
            };
            tokio::time::timeout(CONNECT_TIMEOUT, connect)
                .await
                .map_err(|_| format!("connecting to {host}:{port}: timed out"))?
        })
    }
}

#[cfg(test)]
mod tests {
    use legion_pki::{Ca, Identity, Role};

    use super::*;

    #[test]
    fn renewal_takes_a_matching_pair_only() {
        let ca = Ca::generate().unwrap();
        let first = ca
            .issue(
                &Identity::new("w1", Role::Worker),
                legion_pki::CERT_LIFETIME,
            )
            .unwrap();
        let creds = Credentials::new(ca.cert_pem(), &first.cert_pem, &first.key_pem).unwrap();
        let next = ca
            .issue(
                &Identity::new("w1", Role::Worker),
                legion_pki::CERT_LIFETIME,
            )
            .unwrap();
        // The new certificate with the old key: refused, the old pair kept.
        assert!(creds.renew(&next.cert_pem, &first.key_pem).is_err());
        assert_eq!(creds.cert_and_key().0, first.cert_pem);
        creds.renew(&next.cert_pem, &next.key_pem).unwrap();
        assert_eq!(creds.cert_and_key(), (next.cert_pem, next.key_pem));
        assert!(Credentials::new(ca.cert_pem(), &first.cert_pem, "not a key").is_err());
    }

    /// A rotation: both CAs trusted while nodes move, then only the new one.
    #[test]
    fn trust_follows_a_rotation() {
        let old = Ca::generate().unwrap();
        let (new, cross) = old.successor().unwrap();
        let w1 = Identity::new("w1", Role::Worker);
        let before = old.issue(&w1, legion_pki::CERT_LIFETIME).unwrap();
        let after = new.issue(&w1, legion_pki::CERT_LIFETIME).unwrap();
        let peer_old = old
            .issue(
                &Identity::new("w2", Role::Worker),
                legion_pki::CERT_LIFETIME,
            )
            .unwrap();
        let peer_new = new
            .issue(
                &Identity::new("w3", Role::Worker),
                legion_pki::CERT_LIFETIME,
            )
            .unwrap();
        let der = |pem: &str| CertificateDer::from_pem_slice(pem.as_bytes()).unwrap();
        let accepts = |creds: &Credentials, pem: &str| {
            creds
                .trust
                .verify_client_cert(&der(pem), &[], UnixTime::now())
                .is_ok()
        };
        let creds = Credentials::new(old.cert_pem(), &before.cert_pem, &before.key_pem).unwrap();
        assert!(accepts(&creds, &peer_old.cert_pem));
        assert!(!accepts(&creds, &peer_new.cert_pem));
        // Presented with the cross-signed certificate, the new CA's are
        // verified by a peer that trusts only the old CA.
        let with_cross = creds.trust.verify_client_cert(
            &der(&peer_new.cert_pem),
            &[der(&cross)],
            UnixTime::now(),
        );
        assert!(with_cross.is_ok(), "{with_cross:?}");
        // Not the new CA alone while this node's certificate is the old one's.
        assert!(creds.trust(new.cert_pem()).is_err());
        assert!(!accepts(&creds, &peer_new.cert_pem), "nothing changed");

        let both = format!("{}{}", old.cert_pem(), new.cert_pem());
        creds.trust(&both).unwrap();
        assert!(accepts(&creds, &peer_old.cert_pem));
        assert!(accepts(&creds, &peer_new.cert_pem));
        assert_eq!(creds.ca_pem(), both);
        // The chain presented carries both, for a joining node to find its own.
        let chain = creds.keys.read().chain.cert.len();
        assert_eq!(chain, 3);

        creds.renew(&after.cert_pem, &after.key_pem).unwrap();
        creds.trust(new.cert_pem()).unwrap();
        assert!(!accepts(&creds, &peer_old.cert_pem), "the old CA's refused");
        assert!(accepts(&creds, &peer_new.cert_pem));
        // A certificate of the old CA is no renewal now.
        assert!(creds.renew(&before.cert_pem, &before.key_pem).is_err());
    }

    #[test]
    fn credentials_take_the_cas_certificates_only() {
        let ca = Ca::generate().unwrap();
        let other = Ca::generate().unwrap();
        let id = Identity::new("w1", Role::Worker);
        let ours = ca.issue(&id, legion_pki::CERT_LIFETIME).unwrap();
        let theirs = other.issue(&id, legion_pki::CERT_LIFETIME).unwrap();
        assert!(Credentials::new(ca.cert_pem(), &theirs.cert_pem, &theirs.key_pem).is_err());
        // No certificate at all: not the CA's own taken in its place.
        assert!(Credentials::new(ca.cert_pem(), "", &ours.key_pem).is_err());
        let creds = Credentials::new(ca.cert_pem(), &ours.cert_pem, &ours.key_pem).unwrap();
        assert!(creds.renew(&theirs.cert_pem, &theirs.key_pem).is_err());
        assert_eq!(creds.cert_and_key().0, ours.cert_pem);
    }
}
