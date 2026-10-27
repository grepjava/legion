//! mTLS for the API and the Raft transport (DESIGN.md §9). Every manager
//! presents its node certificate and requires one from its clients; who
//! may call what follows from the client certificate's role.

use legion_pki::{Identity, Role, SERVER_NAME};
use tonic::transport::{Certificate, ClientTlsConfig, Identity as TlsIdentity, ServerTlsConfig};
use tonic::{Request, Status};

/// A manager's credentials: the cluster CA and its own certificate.
#[derive(Debug, Clone)]
pub struct Tls {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    /// Managers hold the CA key, to certify joining nodes.
    pub ca_key_pem: Option<String>,
}

impl Tls {
    /// The chain includes the CA, which joining nodes read (and check
    /// against their token) before they trust it. Client certificates are
    /// optional in the handshake, for `Join`; every other service requires
    /// one.
    pub(crate) fn server(&self) -> ServerTlsConfig {
        let chain = format!("{}{}", self.cert_pem, self.ca_pem);
        ServerTlsConfig::new()
            .identity(TlsIdentity::from_pem(chain, &self.key_pem))
            .client_ca_root(Certificate::from_pem(&self.ca_pem))
            .client_auth_optional(true)
    }

    /// For calls to other managers, as this manager.
    pub fn client(&self) -> ClientTlsConfig {
        client_config(&self.ca_pem, &self.cert_pem, &self.key_pem)
    }
}

/// A client of a manager: verifies it against the cluster CA and presents
/// the client's certificate.
pub fn client_config(ca_pem: &str, cert_pem: &str, key_pem: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(TlsIdentity::from_pem(cert_pem, key_pem))
        .domain_name(SERVER_NAME)
}

/// The caller's identity, from its verified client certificate. `None`
/// when the server runs without TLS (`tls` false: tests only).
pub(crate) fn caller<T>(req: &Request<T>, tls: bool) -> Result<Option<Identity>, Status> {
    if !tls {
        return Ok(None);
    }
    let certs = req.peer_certs();
    let cert = certs
        .as_ref()
        .and_then(|c| c.first())
        .ok_or_else(|| Status::unauthenticated("a client certificate is required"))?;
    legion_pki::identity(cert)
        .map(Some)
        .map_err(|e| Status::unauthenticated(e.to_string()))
}

/// An interceptor that lets only callers with one of `roles` through.
pub(crate) fn only(
    roles: &'static [Role],
    tls: bool,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| match caller(&req, tls)? {
        Some(id) if !roles.contains(&id.role) => Err(Status::permission_denied(format!(
            "{id} may not call this service"
        ))),
        _ => Ok(req),
    }
}
