//! A CA rotation as each node takes it up (DESIGN.md §9; the leader's half
//! is `legion_manager::ca`).
//!
//! A manager reads the recorded CA every few seconds. When a rotation
//! begins it writes the new CA to `pki` (the key sealed if autolock is on;
//! `ca.crt` with the new CA first, then the cross-signed certificate and
//! the old CA), signs with it, issues itself and the admin bundle new
//! certificates, and trusts both CAs; when the rotation ends it trusts the
//! new CA alone. A worker learns the same from its heartbeats: it trusts
//! both, renews at once, and drops the old CA once told.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use legion_manager::Credentials;
use legion_manager::ca::{Authority, Record};
use legion_pki::Ca;
use legion_store::Store;

use crate::pki::Pki;

/// How often a manager reads the recorded CA.
const PERIOD: Duration = Duration::from_secs(5);

/// The certificates of `first`, then those of `second` it lacks: what a
/// node trusts while it moves from `second`'s CAs to `first`'s.
pub fn union(first: &str, second: &str) -> String {
    let mut certs = legion_pki::split_certs(first);
    for c in legion_pki::split_certs(second) {
        if !certs.contains(&c) {
            certs.push(c);
        }
    }
    certs.concat()
}

/// Keeps a manager on the recorded CA. Runs until aborted.
pub async fn follow(
    store: Store,
    pki: Arc<Mutex<Pki>>,
    node: String,
    data_dir: PathBuf,
    credentials: Arc<Credentials>,
    authority: Authority,
) {
    loop {
        match legion_manager::ca::current(&store) {
            Ok(Some((record, _))) => {
                let pki = pki.lock().unwrap_or_else(|e| e.into_inner());
                match take_up(&record, &pki, &node, &data_dir, &credentials, &authority) {
                    Ok(true) if record.rotating() => {
                        tracing::info!(ca = %record.digest(), "CA rotation: this manager signs with the new CA")
                    }
                    Ok(true) => {
                        tracing::info!(ca = %record.digest(), "CA rotation done here: the old CA is trusted no more")
                    }
                    Ok(false) => {}
                    Err(e) => tracing::warn!("CA rotation: {e:#}"),
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("CA rotation: reading the cluster's CA: {e}"),
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Brings this manager in line with `record`; whether anything changed.
fn take_up(
    record: &Record,
    pki: &Pki,
    node: &str,
    data_dir: &std::path::Path,
    credentials: &Credentials,
    authority: &Authority,
) -> anyhow::Result<bool> {
    let trust = record.trust_pem();
    let new_ca = authority.ca().digest() != record.digest();
    if !new_ca && credentials.ca_pem() == trust {
        return Ok(false);
    }
    if new_ca {
        // On disk first: a restart signs with what it finds there.
        pki.write_ca(&trust, &record.key_pem)?;
        authority.set(Ca::load(&record.cert_pem, &record.key_pem).context("the new CA")?);
    } else {
        pki.write_trust(&trust)?;
    }
    let (mine, _) = credentials.cert_and_key();
    if legion_pki::issuer(&mine, &record.cert_pem).is_none() {
        // Trusting both meanwhile, so this certificate and the new one
        // are good alike.
        credentials
            .trust(&union(&credentials.ca_pem(), &trust))
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    // This manager's certificate and the admin bundle, of the new CA.
    let tls = pki.ensure(node, data_dir)?;
    credentials
        .renew(&tls.cert_pem, &tls.key_pem)
        .map_err(|e| anyhow::anyhow!(e))?;
    credentials.trust(&trust).map_err(|e| anyhow::anyhow!(e))?;
    Ok(true)
}

/// A worker's half: `advice` as heartbeats give it. Trusts the CAs it
/// names, after renewing from the issuing one if this worker's
/// certificate is not of it. Whether anything changed.
pub async fn worker_take_up(
    advice: &legion_agent::CaAdvice,
    pki: &Pki,
    credentials: &Credentials,
    control: &legion_agent::GrpcControl,
) -> anyhow::Result<bool> {
    let issuing = legion_pki::split_certs(&advice.trust_pem)
        .into_iter()
        .next()
        .context("no CA in the heartbeat's")?;
    if legion_pki::digest(&issuing)? != advice.issuer {
        anyhow::bail!("the heartbeat's CAs do not begin with the issuing one");
    }
    let mut changed = false;
    let (mine, _) = credentials.cert_and_key();
    if legion_pki::issuer(&mine, &issuing).is_none() {
        // On disk before the new certificate is: a restart then trusts
        // what it is of.
        let both = union(&advice.trust_pem, &credentials.ca_pem());
        pki.write_trust(&both)?;
        credentials.trust(&both).map_err(|e| anyhow::anyhow!(e))?;
        crate::pki::renew_worker_once(pki, credentials, control).await?;
        changed = true;
    }
    if credentials.ca_pem() != advice.trust_pem {
        credentials
            .trust(&advice.trust_pem)
            .map_err(|e| anyhow::anyhow!(e))?;
        pki.write_trust(&advice.trust_pem)?;
        changed = true;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_union_keeps_the_first_order_and_adds_what_is_missing() {
        let (a, b, c) = (
            Ca::generate().unwrap(),
            Ca::generate().unwrap(),
            Ca::generate().unwrap(),
        );
        let ab = format!("{}{}", a.cert_pem(), b.cert_pem());
        let bc = format!("{}{}", b.cert_pem(), c.cert_pem());
        assert_eq!(
            legion_pki::split_certs(&union(&ab, &bc)),
            [a.cert_pem(), b.cert_pem(), c.cert_pem()]
        );
        assert_eq!(union(&ab, ""), ab);
    }

    fn admin(data: &std::path::Path) -> legion_pki::Bundle {
        let pem = std::fs::read_to_string(data.join(crate::pki::ADMIN_BUNDLE)).unwrap();
        legion_pki::Bundle::parse(&pem).unwrap()
    }

    /// A manager through a whole rotation, on its own files: the new CA
    /// taken up, its certificate and the admin bundle renewed, both CAs
    /// trusted, then the new one alone; and a restart on the way.
    #[test]
    fn a_manager_takes_up_a_rotation_and_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let pki = Pki::new(data);
        let tls = pki.ensure("m1", data).unwrap();
        let old = Ca::load(&tls.ca_pem, tls.ca_key_pem.as_deref().unwrap()).unwrap();
        let credentials = Credentials::new(&tls.ca_pem, &tls.cert_pem, &tls.key_pem).unwrap();
        let authority = Authority::new(Ca::load(old.cert_pem(), old.key_pem()).unwrap());

        let (next, cross) = old.successor().unwrap();
        let rotating = Record {
            cert_pem: next.cert_pem().to_owned(),
            key_pem: next.key_pem().to_owned(),
            cross_pem: Some(cross),
            previous_pem: Some(old.cert_pem().to_owned()),
            since_ms: Some(1),
        };
        let take = |r: &Record| take_up(r, &pki, "m1", data, &credentials, &authority).unwrap();
        assert!(take(&rotating));
        assert_eq!(
            authority.ca().digest(),
            next.digest(),
            "signs with the new CA"
        );
        let trust = rotating.trust_pem();
        assert_eq!(credentials.ca_pem(), trust, "trusts both");
        let (mine, _) = credentials.cert_and_key();
        assert_eq!(legion_pki::issuer(&mine, &trust), Some(next.digest()));
        assert_eq!(
            std::fs::read_to_string(data.join("pki/ca.crt")).unwrap(),
            trust
        );
        let a = admin(data);
        assert_eq!(a.ca_pem, trust);
        assert!(legion_pki::issuer(&a.cert_pem, next.cert_pem()).is_some());
        assert!(!take(&rotating), "nothing more to do");

        // Restarted mid-rotation: the new CA signs, both are trusted.
        let again = Pki::new(data).ensure("m1", data).unwrap();
        assert_eq!(again.ca_pem, trust);
        assert_eq!(again.cert_pem, mine, "its certificate kept");
        assert_eq!(
            Ca::load(next.cert_pem(), again.ca_key_pem.as_deref().unwrap())
                .unwrap()
                .digest(),
            next.digest()
        );

        let done = Record {
            cross_pem: None,
            previous_pem: None,
            since_ms: None,
            ..rotating
        };
        assert!(take(&done));
        assert_eq!(credentials.ca_pem(), next.cert_pem(), "the new CA alone");
        assert_eq!(
            std::fs::read_to_string(data.join("pki/ca.crt")).unwrap(),
            next.cert_pem()
        );
        assert_eq!(admin(data).ca_pem, next.cert_pem());
        assert!(!take(&done));
    }

    /// A new CA written halfway (its key, not its certificate) is dropped;
    /// written whole but not yet in place, it is put in place.
    #[test]
    fn a_change_of_ca_cut_short_leaves_a_matching_pair() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let pki = Pki::new(data);
        let tls = pki.ensure("m1", data).unwrap();
        let old = Ca::load(&tls.ca_pem, tls.ca_key_pem.as_deref().unwrap()).unwrap();
        let (next, _) = old.successor().unwrap();

        std::fs::write(data.join("pki/ca.key.next"), next.key_pem()).unwrap();
        let tls = Pki::new(data).ensure("m1", data).unwrap();
        assert_eq!(tls.ca_pem, old.cert_pem(), "the key alone dropped");
        assert!(!data.join("pki/ca.key.next").exists());

        std::fs::write(data.join("pki/ca.key.next"), next.key_pem()).unwrap();
        std::fs::write(data.join("pki/ca.crt.next"), next.cert_pem()).unwrap();
        let tls = Pki::new(data).ensure("m1", data).unwrap();
        assert_eq!(tls.ca_pem, next.cert_pem(), "put in place whole");
        assert_eq!(tls.ca_key_pem.as_deref(), Some(next.key_pem()));
        assert!(legion_pki::issuer(&tls.cert_pem, next.cert_pem()).is_some());
    }
}
