//! The daemon's certificates, under `<data dir>/pki` (DESIGN.md §9).
//!
//! The first start of a node that has not joined a cluster creates a
//! cluster CA, this manager's certificate and `<data dir>/admin.pem`, the
//! CLI's credentials. `legion join` writes the cluster's CA and the node's
//! certificate instead, with the CA key only for managers. Once two thirds
//! of a certificate's life are gone, a manager issues itself a new one and
//! a worker asks a manager for one (`Renew`); the running daemon takes it
//! over without a restart.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use legion_manager::Credentials;
use legion_pki::{Bundle, CERT_LIFETIME, Ca, Identity, Role};

/// The CLI's credentials, written on first start.
pub const ADMIN_BUNDLE: &str = "admin.pem";

pub struct Pki {
    dir: PathBuf,
    /// The unlock key, while autolock seals the private keys.
    lock: Option<legion_store::StoreKey>,
}

/// The private keys autolock seals: the CA's, the node's and the store's.
const SEALABLE: [&str; 3] = ["ca.key", "node.key", "store.key"];

impl Pki {
    pub fn new(data_dir: &Path) -> Pki {
        Pki {
            dir: data_dir.join("pki"),
            lock: None,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn sealed_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.sealed"))
    }

    /// Whether autolock sealed this node's private keys: it needs the
    /// unlock key to start.
    pub fn locked(&self) -> bool {
        SEALABLE.iter().any(|f| self.sealed_path(f).exists())
    }

    /// The same, with its sealed keys opened by `key`; fails if `key` does
    /// not open them.
    pub fn unlock(self, key: legion_store::StoreKey) -> anyhow::Result<Pki> {
        for f in SEALABLE {
            let path = self.sealed_path(f);
            if let Ok(sealed) = std::fs::read(&path) {
                legion_store::open_sealed(&key, f, &sealed)
                    .map_err(|_| anyhow::anyhow!("the key does not unlock this manager"))?;
            }
        }
        Ok(Pki {
            lock: Some(key),
            ..self
        })
    }

    /// The unlock key the private keys are sealed with, if they are.
    pub fn lock(&self) -> Option<&legion_store::StoreKey> {
        self.lock.as_ref()
    }

    /// A private key, opened if sealed.
    fn read_secret(&self, name: &str) -> anyhow::Result<Option<String>> {
        let sealed = self.sealed_path(name);
        match (std::fs::read(&sealed), &self.lock) {
            (Ok(bytes), Some(key)) => {
                let plain = legion_store::open_sealed(key, name, &bytes)
                    .with_context(|| format!("opening {}", sealed.display()))?;
                Ok(Some(String::from_utf8(plain)?))
            }
            (Ok(_), None) if !self.path(name).exists() => {
                anyhow::bail!("{} is sealed: unlock the manager first", sealed.display())
            }
            _ => read(&self.path(name)),
        }
    }

    /// Writes a private key, sealed while autolock is on; then drops the
    /// other form, so a crash between leaves both rather than neither.
    fn write_secret(&self, name: &str, text: &str) -> anyhow::Result<()> {
        let (plain, sealed) = (self.path(name), self.sealed_path(name));
        match &self.lock {
            Some(key) => {
                let bytes = legion_store::seal(key, name, text.as_bytes())?;
                write_bytes(&sealed, &bytes, 0o600)?;
                remove(&plain)
            }
            None => {
                write_private(&plain, text)?;
                remove(&sealed)
            }
        }
    }

    /// Seals the private keys with `lock`, or with `None` writes them in
    /// the clear again.
    pub fn reseal(&mut self, lock: Option<legion_store::StoreKey>) -> anyhow::Result<()> {
        let mut keys = Vec::new();
        for f in SEALABLE {
            if let Some(text) = self.read_secret(f)? {
                keys.push((f, text));
            }
        }
        self.lock = lock;
        for (f, text) in keys {
            self.write_secret(f, &text)?;
        }
        Ok(())
    }

    /// The key this manager's store is sealed with, `store.key`, made on
    /// first use. A store written before it existed is read as it is and
    /// sealed as it is written again.
    pub fn store_key(&self) -> anyhow::Result<legion_store::StoreKey> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        if let Some(text) = self.read_secret("store.key")? {
            let bytes = STANDARD
                .decode(text.trim())
                .context("the store key is not base64")?;
            return legion_store::StoreKey::from_bytes(&bytes).context("reading the store key");
        }
        private_dir(&self.dir)?;
        let key = legion_store::StoreKey::generate();
        self.write_secret(
            "store.key",
            &format!("{}\n", STANDARD.encode(key.as_bytes())),
        )?;
        tracing::info!("made the store's key");
        Ok(key)
    }

    /// The node's role: a manager unless it joined as a worker.
    pub fn role(&self) -> anyhow::Result<Role> {
        Ok(match read(&self.path("role"))? {
            Some(r) => Role::parse(r.trim()).with_context(|| {
                format!("unknown role {r:?} in {}", self.path("role").display())
            })?,
            None => Role::Manager,
        })
    }

    /// Whether the node has joined a cluster or holds a CA of its own.
    pub fn exists(&self) -> bool {
        self.path("ca.crt").exists()
    }

    /// Stores what joining a cluster gave the node.
    pub fn write_joined(&self, joined: &legion_manager::Joined) -> anyhow::Result<()> {
        private_dir(&self.dir)?;
        let tls = &joined.tls;
        if let Some(key) = &tls.ca_key_pem {
            write_private(&self.path("ca.key"), key)?;
        }
        write_public(&self.path("ca.crt"), &tls.ca_pem)?;
        write_private(&self.path("node.key"), &tls.key_pem)?;
        write_public(&self.path("node.crt"), &tls.cert_pem)?;
        write_public(&self.path("role"), &format!("{}\n", joined.role))
    }

    /// A worker's name, from its certificate, and its credentials for
    /// calling managers.
    pub fn worker(&self) -> anyhow::Result<(String, Bundle)> {
        self.finish_renewal()?;
        let missing = || format!("{} holds no worker certificate", self.dir.display());
        let bundle = Bundle {
            cert_pem: read(&self.path("node.crt"))?.with_context(missing)?,
            key_pem: read(&self.path("node.key"))?.with_context(missing)?,
            ca_pem: read(&self.path("ca.crt"))?.with_context(missing)?,
        };
        let id = bundle
            .identity()
            .context("reading the worker certificate")?;
        if id.role != Role::Worker {
            anyhow::bail!(
                "{} certifies {id}, not a worker",
                self.path("node.crt").display()
            );
        }
        Ok((id.name, bundle))
    }

    /// Replaces a worker's certificate and key. Both are written beside
    /// the old ones first and then renamed over them, the key first, so a
    /// crash leaves either the old pair or the new one complete beside it,
    /// which the next start puts in place.
    pub fn write_worker(&self, cert_pem: &str, key_pem: &str) -> anyhow::Result<()> {
        write_private(&self.path(NEXT_KEY), key_pem)?;
        write_public(&self.path(NEXT_CERT), cert_pem)?;
        self.finish_renewal()
    }

    /// Puts a completely written new pair in place; drops a half-written
    /// one (a key without its certificate).
    fn finish_renewal(&self) -> anyhow::Result<()> {
        let (key, cert) = (self.path(NEXT_KEY), self.path(NEXT_CERT));
        if !cert.exists() {
            return remove(&key);
        }
        if key.exists() {
            rename(&key, &self.path("node.key"))?;
        }
        rename(&cert, &self.path("node.crt"))
    }

    /// The CA, this manager's credentials and the admin bundle, made or
    /// renewed as needed.
    pub fn ensure(&self, node: &str, data_dir: &Path) -> anyhow::Result<legion_manager::Tls> {
        private_dir(&self.dir)?;
        if self.role()? != Role::Manager {
            anyhow::bail!("this node joined as a worker and holds no manager credentials");
        }
        let ca = match (read(&self.path("ca.crt"))?, self.read_secret("ca.key")?) {
            (Some(cert), Some(key)) => Ca::load(&cert, &key).context("loading the cluster CA")?,
            (Some(_), None) => {
                anyhow::bail!("{} has no CA key: a manager needs one", self.dir.display())
            }
            _ => {
                let ca = Ca::generate()?;
                self.write_secret("ca.key", ca.key_pem())?;
                write_public(&self.path("ca.crt"), ca.cert_pem())?;
                tracing::info!(digest = %ca.digest(), "created a cluster CA");
                ca
            }
        };

        let id = Identity::new(node, Role::Manager);
        let current = match (read(&self.path("node.crt"))?, self.read_secret("node.key")?) {
            (Some(cert), Some(key)) => Some((cert, key)),
            _ => None,
        };
        // A crash between writing a new key and its certificate leaves a
        // pair that does not belong together: issued again.
        let usable = current.as_ref().is_some_and(|(cert, key)| {
            !legion_pki::needs_renewal(cert, SystemTime::now())
                && issued_as(cert, &id)
                && Credentials::new(ca.cert_pem(), cert, key).is_ok()
        });
        let (cert_pem, key_pem) = match current {
            Some(pair) if usable => pair,
            _ => {
                let issued = ca.issue(&id, CERT_LIFETIME)?;
                self.write_secret("node.key", &issued.key_pem)?;
                write_public(&self.path("node.crt"), &issued.cert_pem)?;
                tracing::info!(%id, "issued this manager's certificate");
                (issued.cert_pem, issued.key_pem)
            }
        };

        let admin = data_dir.join(ADMIN_BUNDLE);
        let admin_ok = read(&admin)?
            .and_then(|pem| Bundle::parse(&pem).ok())
            .is_some_and(|b| {
                b.ca_pem == ca.cert_pem()
                    && !legion_pki::needs_renewal(&b.cert_pem, SystemTime::now())
            });
        if !admin_ok {
            let issued = ca.issue(&Identity::new("admin", Role::Admin), CERT_LIFETIME)?;
            write_private(&admin, &Bundle::new(issued, ca.cert_pem()).to_pem())?;
            tracing::info!(path = %admin.display(), "wrote the admin credentials");
        }

        Ok(legion_manager::Tls {
            ca_pem: ca.cert_pem().to_owned(),
            cert_pem,
            key_pem,
            ca_key_pem: Some(ca.key_pem().to_owned()),
        })
    }
}

/// How often a running daemon checks whether its certificate needs
/// renewing.
const RENEWAL_CHECK: Duration = Duration::from_secs(3600);
/// After a failed renewal, how soon to try again.
const RENEWAL_RETRY: Duration = Duration::from_secs(300);

/// A worker's next certificate and key, while it is written.
const NEXT_CERT: &str = "node.crt.next";
const NEXT_KEY: &str = "node.key.next";

/// Keeps a manager's certificate current: past two thirds of its life,
/// a new one is issued with the CA, written as autolock wants it, and
/// served and called with from then on. The admin bundle is renewed
/// alongside. Runs until aborted.
pub async fn renew_manager(
    pki: Arc<Mutex<Pki>>,
    node: String,
    data_dir: PathBuf,
    credentials: Arc<Credentials>,
) {
    loop {
        let wait = if legion_pki::needs_renewal(&credentials.cert_and_key().0, SystemTime::now()) {
            let renewed = pki
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ensure(&node, &data_dir)
                .and_then(|tls| {
                    credentials
                        .renew(&tls.cert_pem, &tls.key_pem)
                        .map_err(|e| anyhow::anyhow!(e))
                });
            match renewed {
                Ok(()) => {
                    tracing::info!("renewed this manager's certificate");
                    RENEWAL_CHECK
                }
                Err(e) => {
                    tracing::warn!("renewing this manager's certificate: {e:#}");
                    RENEWAL_RETRY
                }
            }
        } else {
            RENEWAL_CHECK
        };
        tokio::time::sleep(wait).await;
    }
}

/// Keeps a worker's certificate current: past two thirds of its life, it
/// asks a manager to certify a new key under the same name, writes both
/// and calls with them from then on. Runs until aborted.
pub async fn renew_worker(
    pki: Pki,
    credentials: Arc<Credentials>,
    control: legion_agent::GrpcControl,
) {
    loop {
        let wait = if legion_pki::needs_renewal(&credentials.cert_and_key().0, SystemTime::now()) {
            match renew_worker_once(&pki, &credentials, &control).await {
                Ok(()) => {
                    tracing::info!("renewed this worker's certificate");
                    RENEWAL_CHECK
                }
                Err(e) => {
                    tracing::warn!("renewing this worker's certificate: {e:#}");
                    RENEWAL_RETRY
                }
            }
        } else {
            RENEWAL_CHECK
        };
        tokio::time::sleep(wait).await;
    }
}

async fn renew_worker_once(
    pki: &Pki,
    credentials: &Credentials,
    control: &legion_agent::GrpcControl,
) -> anyhow::Result<()> {
    let (key_pem, csr_pem) = legion_pki::request()?;
    let cert_pem = control
        .renew(csr_pem)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let current = Bundle {
        cert_pem: credentials.cert_and_key().0,
        key_pem: String::new(),
        ca_pem: String::new(),
    }
    .identity()?;
    if !issued_as(&cert_pem, &current) {
        anyhow::bail!("the manager certified someone other than {current}");
    }
    // Checked to belong together before anything is written.
    credentials
        .renew(&cert_pem, &key_pem)
        .map_err(|e| anyhow::anyhow!(e))?;
    pki.write_worker(&cert_pem, &key_pem)
}

/// The managers a worker calls, one `host:port` per line of
/// `<data dir>/managers`.
pub fn read_managers(data_dir: &Path) -> anyhow::Result<Vec<String>> {
    Ok(read(&data_dir.join(MANAGERS))?
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect())
}

pub fn write_managers(data_dir: &Path, managers: &[String]) -> anyhow::Result<()> {
    let mut text = managers.join("\n");
    text.push('\n');
    write_public(&data_dir.join(MANAGERS), &text)
}

const MANAGERS: &str = "managers";

/// Whether a certificate names `id`.
fn issued_as(cert_pem: &str, id: &Identity) -> bool {
    Bundle {
        cert_pem: cert_pem.to_owned(),
        key_pem: String::new(),
        ca_pem: String::new(),
    }
    .identity()
    .is_ok_and(|got| got == *id)
}

fn read(path: &Path) -> anyhow::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn private_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Written whole and then renamed, readable by the owner only.
pub(crate) fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
    write(path, contents, 0o600)
}

fn write_public(path: &Path, contents: &str) -> anyhow::Result<()> {
    write(path, contents, 0o644)
}

fn rename(from: &Path, to: &Path) -> anyhow::Result<()> {
    std::fs::rename(from, to)
        .with_context(|| format!("moving {} to {}", from.display(), to.display()))
}

fn remove(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            Err(e).with_context(|| format!("removing {}", path.display()))
        }
        _ => Ok(()),
    }
}

fn write(path: &Path, contents: &str, mode: u32) -> anyhow::Result<()> {
    write_bytes(path, contents.as_bytes(), mode)
}

fn write_bytes(path: &Path, contents: &[u8], mode: u32) -> anyhow::Result<()> {
    let tmp = path.with_extension("new");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut f = options
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::io::Write::write_all(&mut f, contents)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_then_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let pki = Pki::new(dir.path());
        let first = pki.ensure("n1", dir.path()).unwrap();
        let admin = std::fs::read_to_string(dir.path().join(ADMIN_BUNDLE)).unwrap();
        let bundle = Bundle::parse(&admin).unwrap();
        assert_eq!(
            bundle.identity().unwrap(),
            Identity::new("admin", Role::Admin)
        );
        assert_eq!(bundle.ca_pem, first.ca_pem);

        // A restart keeps everything.
        let again = pki.ensure("n1", dir.path()).unwrap();
        assert_eq!(again.cert_pem, first.cert_pem);
        assert_eq!(again.ca_pem, first.ca_pem);
        assert_eq!(
            std::fs::read_to_string(dir.path().join(ADMIN_BUNDLE)).unwrap(),
            admin
        );

        // A new node name gets a new certificate from the same CA.
        let renamed = pki.ensure("n2", dir.path()).unwrap();
        assert_ne!(renamed.cert_pem, first.cert_pem);
        assert_eq!(renamed.ca_pem, first.ca_pem);
        assert!(issued_as(
            &renamed.cert_pem,
            &Identity::new("n2", Role::Manager)
        ));

        // A key without its certificate (a crash between the writes): a
        // new pair, not one that fails every handshake.
        let (key_pem, _) = legion_pki::request().unwrap();
        write_private(&pki.path("node.key"), &key_pem).unwrap();
        let healed = pki.ensure("n2", dir.path()).unwrap();
        assert_ne!(healed.cert_pem, renamed.cert_pem);
        assert!(Credentials::new(&healed.ca_pem, &healed.cert_pem, &healed.key_pem).is_ok());
    }

    #[test]
    fn a_worker_renewal_survives_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let pki = Pki::new(dir.path());
        let ca = Ca::generate().unwrap();
        let issue = || {
            ca.issue(&Identity::new("w1", Role::Worker), CERT_LIFETIME)
                .unwrap()
        };
        let first = issue();
        pki.write_joined(&legion_manager::Joined {
            role: Role::Worker,
            tls: legion_manager::Tls {
                ca_pem: ca.cert_pem().into(),
                cert_pem: first.cert_pem.clone(),
                key_pem: first.key_pem.clone(),
                ca_key_pem: None,
            },
        })
        .unwrap();
        let current = |pki: &Pki| {
            let (name, b) = pki.worker().unwrap();
            assert_eq!(name, "w1");
            (b.cert_pem, b.key_pem)
        };

        let second = issue();
        pki.write_worker(&second.cert_pem, &second.key_pem).unwrap();
        assert_eq!(current(&pki), (second.cert_pem.clone(), second.key_pem));

        // Down before the certificate was written: the old pair stays.
        let third = issue();
        write_private(&pki.path(NEXT_KEY), &third.key_pem).unwrap();
        assert_eq!(current(&pki).0, second.cert_pem);
        assert!(!pki.path(NEXT_KEY).exists());
        // Down after both were written, or after the key was moved: the
        // new pair is put in place.
        write_private(&pki.path(NEXT_KEY), &third.key_pem).unwrap();
        write_public(&pki.path(NEXT_CERT), &third.cert_pem).unwrap();
        assert_eq!(current(&pki), (third.cert_pem.clone(), third.key_pem));
        let fourth = issue();
        write_private(&pki.path("node.key"), &fourth.key_pem).unwrap();
        write_public(&pki.path(NEXT_CERT), &fourth.cert_pem).unwrap();
        assert_eq!(current(&pki), (fourth.cert_pem, fourth.key_pem));
    }
}
