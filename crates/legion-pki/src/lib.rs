//! The cluster PKI (DESIGN.md §9): a root CA, and certificates it issues
//! to managers, workers and users.
//!
//! Every certificate names an [`Identity`]: the subject's common name is
//! its name and its organisation is its role. Certificates serve as both
//! server and client certificates and carry the DNS name [`SERVER_NAME`],
//! which clients verify whatever address they used, so managers need no
//! certificate per address.
//!
//! A [`Bundle`] is what a client needs in one PEM file: its certificate,
//! its key and the CA certificate.

use std::fmt;
use std::time::{Duration, SystemTime};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use sha2::{Digest, Sha256};
use x509_parser::prelude::{FromDer, X509Certificate};

/// The name every certificate is issued for and every client verifies.
pub const SERVER_NAME: &str = "legion";
/// Lifetime of node and user certificates (DESIGN.md §9).
pub const CERT_LIFETIME: Duration = Duration::from_secs(90 * 24 * 3600);
const CA_LIFETIME: Duration = Duration::from_secs(10 * 365 * 24 * 3600);
/// Clocks between machines disagree; certificates start a little early.
const BACKDATE: Duration = Duration::from_secs(3600);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("certificate: {0}")]
    Rcgen(#[from] rcgen::Error),
    #[error("{0}")]
    Invalid(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Manager,
    Worker,
    /// A person or tool using the API with full rights (RBAC narrows this
    /// in phase 3).
    Admin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Manager => "manager",
            Role::Worker => "worker",
            Role::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "manager" => Some(Role::Manager),
            "worker" => Some(Role::Worker),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who a certificate belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub name: String,
    pub role: Role,
}

impl Identity {
    pub fn new(name: &str, role: Role) -> Identity {
        Identity {
            name: name.to_owned(),
            role,
        }
    }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.role, self.name)
    }
}

/// A certificate and its private key, PEM-encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issued {
    pub cert_pem: String,
    pub key_pem: String,
}

/// The cluster's certificate authority.
pub struct Ca {
    cert_pem: String,
    key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl Ca {
    /// A new root CA.
    pub fn generate() -> Result<Ca> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "Legion cluster CA");
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let now = SystemTime::now();
        params.not_before = (now - BACKDATE).into();
        params.not_after = (now + CA_LIFETIME).into();
        let cert = params.self_signed(&key)?;
        Ca::load(&lf(cert.pem()), &lf(key.serialize_pem()))
    }

    /// A CA from its certificate and key.
    pub fn load(cert_pem: &str, key_pem: &str) -> Result<Ca> {
        let key = KeyPair::from_pem(key_pem)?;
        let issuer = Issuer::from_ca_cert_pem(cert_pem, key)?;
        Ok(Ca {
            cert_pem: cert_pem.to_owned(),
            key_pem: key_pem.to_owned(),
            issuer,
        })
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    pub fn key_pem(&self) -> &str {
        &self.key_pem
    }

    /// SHA-256 of the CA certificate, in hex: what a join token carries so
    /// a joining node can check the cluster it reached.
    pub fn digest(&self) -> String {
        digest(&self.cert_pem).expect("own certificate parses")
    }

    /// A certificate for `id`, valid for `lifetime`.
    pub fn issue(&self, id: &Identity, lifetime: Duration) -> Result<Issued> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, id.name.as_str());
        dn.push(DnType::OrganizationName, id.role.as_str());
        params.distinguished_name = dn;
        params.subject_alt_names = vec![SanType::DnsName(SERVER_NAME.try_into()?)];
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.use_authority_key_identifier_extension = true;
        let now = SystemTime::now();
        params.not_before = (now - BACKDATE).into();
        params.not_after = (now + lifetime).into();
        let cert = params.signed_by(&key, &self.issuer)?;
        Ok(Issued {
            cert_pem: lf(cert.pem()),
            key_pem: lf(key.serialize_pem()),
        })
    }
}

/// rcgen ends lines as the platform does; files Legion writes use `\n`.
fn lf(pem: String) -> String {
    pem.replace("\r\n", "\n")
}

/// The DER bytes of the first certificate in `pem`.
fn first_cert_der(pem: &str) -> Result<Vec<u8>> {
    blocks(pem)
        .into_iter()
        .find(|(label, _)| label == "CERTIFICATE")
        .map(|(_, der)| der)
        .ok_or_else(|| Error::Invalid("no certificate in PEM".into()))
}

/// SHA-256 of the first certificate in `pem`, in hex.
pub fn digest(cert_pem: &str) -> Result<String> {
    let der = first_cert_der(cert_pem)?;
    Ok(Sha256::digest(&der)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// The identity a certificate (DER) names.
pub fn identity(cert_der: &[u8]) -> Result<Identity> {
    let (_, cert) = X509Certificate::from_der(cert_der)
        .map_err(|e| Error::Invalid(format!("unreadable certificate: {e}")))?;
    let subject = cert.subject();
    let field = |it: &mut dyn Iterator<Item = &x509_parser::x509::AttributeTypeAndValue>| {
        it.next().and_then(|a| a.as_str().ok()).map(str::to_owned)
    };
    let name = field(&mut subject.iter_common_name())
        .ok_or_else(|| Error::Invalid("certificate has no common name".into()))?;
    let role = field(&mut subject.iter_organization())
        .and_then(|o| Role::parse(&o))
        .ok_or_else(|| Error::Invalid(format!("certificate of {name} has no Legion role")))?;
    Ok(Identity { name, role })
}

/// When the first certificate in `pem` expires, and how long it was issued for.
pub fn validity(cert_pem: &str) -> Result<(SystemTime, Duration)> {
    let der = first_cert_der(cert_pem)?;
    let (_, cert) = X509Certificate::from_der(&der)
        .map_err(|e| Error::Invalid(format!("unreadable certificate: {e}")))?;
    let v = cert.validity();
    let at = |t: x509_parser::time::ASN1Time| {
        SystemTime::UNIX_EPOCH + Duration::from_secs(t.timestamp().max(0) as u64)
    };
    let (from, to) = (at(v.not_before), at(v.not_after));
    Ok((to, to.duration_since(from).unwrap_or_default()))
}

/// Whether a certificate has used up two thirds of its life, when it is
/// renewed (DESIGN.md §9).
pub fn needs_renewal(cert_pem: &str, now: SystemTime) -> bool {
    match validity(cert_pem) {
        Ok((not_after, lifetime)) => {
            let renew_at = not_after - lifetime / 3;
            now >= renew_at
        }
        Err(_) => true,
    }
}

/// What a client needs, in one PEM file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    pub cert_pem: String,
    pub key_pem: String,
    pub ca_pem: String,
}

impl Bundle {
    pub fn new(issued: Issued, ca_pem: &str) -> Bundle {
        Bundle {
            cert_pem: issued.cert_pem,
            key_pem: issued.key_pem,
            ca_pem: ca_pem.to_owned(),
        }
    }

    /// The client's certificate, its key, then the CA certificate.
    pub fn to_pem(&self) -> String {
        format!(
            "# Legion credentials. Keep this file private.\n{}{}{}",
            self.cert_pem, self.key_pem, self.ca_pem
        )
    }

    pub fn parse(pem: &str) -> Result<Bundle> {
        let mut certs = Vec::new();
        let mut key = None;
        for (label, der) in blocks(pem) {
            let block = encode(&label, &der);
            match label.as_str() {
                "CERTIFICATE" => certs.push(block),
                l if l.ends_with("PRIVATE KEY") => key = Some(block),
                _ => {}
            }
        }
        match (certs.as_slice(), key) {
            ([cert, ca], Some(key)) => Ok(Bundle {
                cert_pem: cert.clone(),
                key_pem: key,
                ca_pem: ca.clone(),
            }),
            _ => Err(Error::Invalid(
                "a bundle holds a certificate, its key and the CA certificate".into(),
            )),
        }
    }

    pub fn identity(&self) -> Result<Identity> {
        identity(&first_cert_der(&self.cert_pem)?)
    }
}

/// `(label, DER)` of each PEM block.
fn blocks(pem: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut current: Option<(String, String)> = None;
    for line in pem.lines() {
        let line = line.trim();
        if let Some(label) = line
            .strip_prefix("-----BEGIN ")
            .and_then(|l| l.strip_suffix("-----"))
        {
            current = Some((label.to_owned(), String::new()));
        } else if line.starts_with("-----END ") {
            if let Some((label, b64)) = current.take()
                && let Some(der) = base64_decode(&b64)
            {
                out.push((label, der));
            }
        } else if let Some((_, b64)) = current.as_mut() {
            b64.push_str(line);
        }
    }
    out
}

fn encode(label: &str, der: &[u8]) -> String {
    let b64 = base64_encode(der);
    let mut s = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        s.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        s.push('\n');
    }
    s + &format!("-----END {label}-----\n")
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | u32::from(b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                s.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let (mut n, mut bits) = (0u32, 0);
    for c in s.bytes().filter(|&c| c != b'=') {
        let v = B64.iter().position(|&b| b == c)? as u32;
        n = n << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((n >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issues_identities() {
        let ca = Ca::generate().unwrap();
        let id = Identity::new("node-1", Role::Manager);
        let issued = ca.issue(&id, CERT_LIFETIME).unwrap();
        let der = first_cert_der(&issued.cert_pem).unwrap();
        assert_eq!(identity(&der).unwrap(), id);

        // The CA survives a round trip through its PEM files.
        let again = Ca::load(ca.cert_pem(), ca.key_pem()).unwrap();
        assert_eq!(again.digest(), ca.digest());
        assert_eq!(ca.digest().len(), 64);
        let user = again
            .issue(&Identity::new("alice", Role::Admin), CERT_LIFETIME)
            .unwrap();
        assert_eq!(
            identity(&first_cert_der(&user.cert_pem).unwrap()).unwrap(),
            Identity::new("alice", Role::Admin)
        );
    }

    #[test]
    fn renewal_after_two_thirds() {
        let ca = Ca::generate().unwrap();
        let issued = ca
            .issue(&Identity::new("w", Role::Worker), CERT_LIFETIME)
            .unwrap();
        let (not_after, lifetime) = validity(&issued.cert_pem).unwrap();
        // Issued an hour early, so the lifetime is an hour longer.
        assert_eq!(lifetime, CERT_LIFETIME + BACKDATE);
        let now = SystemTime::now();
        assert!(!needs_renewal(&issued.cert_pem, now));
        assert!(!needs_renewal(&issued.cert_pem, now + CERT_LIFETIME / 2));
        assert!(needs_renewal(&issued.cert_pem, not_after - lifetime / 3));
        assert!(needs_renewal("garbage", now));
    }

    #[test]
    fn bundles() {
        let ca = Ca::generate().unwrap();
        let issued = ca
            .issue(&Identity::new("admin", Role::Admin), CERT_LIFETIME)
            .unwrap();
        let b = Bundle::new(issued, ca.cert_pem());
        let parsed = Bundle::parse(&b.to_pem()).unwrap();
        assert_eq!(parsed, b);
        assert_eq!(
            parsed.identity().unwrap(),
            Identity::new("admin", Role::Admin)
        );
        assert!(Bundle::parse(ca.cert_pem()).is_err());
    }

    #[test]
    fn base64_round_trips() {
        for n in 0..10 {
            let data: Vec<u8> = (0..n).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(base64_decode(&base64_encode(&data)).unwrap(), data);
        }
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
    }
}
