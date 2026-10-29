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
use tokio_rustls::rustls::client::ResolvesClientCert;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig, SignatureScheme};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A node's credentials, shared by its server and every client it makes:
/// the certificate is picked per handshake, so a renewed one takes over
/// each connection made after [`Credentials::renew`]; those already open
/// keep going.
#[derive(Debug)]
pub struct Credentials {
    ca_pem: String,
    keys: Arc<Keys>,
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
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
/// to the certificate.
fn certified(
    provider: &CryptoProvider,
    ca_pem: &str,
    cert_pem: &str,
    key_pem: &str,
) -> Result<CertifiedKey, Error> {
    let mut certs = Vec::new();
    for pem in [cert_pem, ca_pem] {
        for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            certs.push(cert?);
        }
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())?;
    let key = provider.key_provider.load_private_key(key)?;
    let certified = CertifiedKey::new(certs, key);
    certified
        .keys_match()
        .map_err(|e| format!("the key is not the certificate's: {e}"))?;
    Ok(certified)
}

impl Credentials {
    /// The cluster CA, which verifies peers, and this node's certificate
    /// and key.
    pub fn new(ca_pem: &str, cert_pem: &str, key_pem: &str) -> Result<Arc<Credentials>, Error> {
        let provider = provider();
        let keys = Arc::new(Keys(RwLock::new(Current {
            cert_pem: cert_pem.to_owned(),
            key_pem: key_pem.to_owned(),
            chain: Arc::new(certified(&provider, ca_pem, cert_pem, key_pem)?),
        })));
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
            roots.add(cert?)?;
        }
        let roots = Arc::new(roots);
        // Client certificates are optional in the handshake, for `Join`;
        // every other service requires one.
        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .allow_unauthenticated()
            .build()?;
        let mut server = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(keys.clone());
        server.alpn_protocols = vec![ALPN_H2.to_vec()];
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_cert_resolver(keys.clone());
        client.alpn_protocols = vec![ALPN_H2.to_vec()];
        Ok(Arc::new(Credentials {
            ca_pem: ca_pem.to_owned(),
            keys,
            server: Arc::new(server),
            client: Arc::new(client),
        }))
    }

    /// Puts a renewed certificate (from the same CA) and its key in place;
    /// refused, the old ones kept, if they do not belong together.
    pub fn renew(&self, cert_pem: &str, key_pem: &str) -> Result<(), Error> {
        let chain = Arc::new(certified(&provider(), &self.ca_pem, cert_pem, key_pem)?);
        *self.keys.0.write().unwrap_or_else(|e| e.into_inner()) = Current {
            cert_pem: cert_pem.to_owned(),
            key_pem: key_pem.to_owned(),
            chain,
        };
        Ok(())
    }

    pub fn ca_pem(&self) -> &str {
        &self.ca_pem
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
        tokio::spawn(async move {
            loop {
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
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
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
}
