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

/// The private keys autolock seals: the CA's, the node's, the store's and
/// the node's WireGuard key.
const SEALABLE: [&str; 4] = ["ca.key", "node.key", "store.key", WIREGUARD_KEY];

/// A manager's WireGuard key, here; a worker's agent keeps its own in
/// `<data dir>/wireguard.key`, where managers kept theirs before autolock
/// sealed them.
const WIREGUARD_KEY: &str = "wireguard.key";

/// Marks a reseal whose keys are all written aside (`<file>.reseal`), and
/// says to which form, `sealed` or `plain`: it is carried through, never
/// undone. Without it, what is aside is dropped.
const RESEALING: &str = "reseal";

impl Pki {
    pub fn new(data_dir: &Path) -> Pki {
        let pki = Pki {
            dir: data_dir.join("pki"),
            lock: None,
        };
        // A reseal cut short is carried through or dropped before anything
        // reads the keys, which are then all under one unlock key.
        if let Err(e) = pki.settle() {
            tracing::warn!("autolock: finishing an interrupted reseal: {e:#}");
        }
        if let Err(e) = pki.settle_ca() {
            tracing::warn!("finishing an interrupted change of CA: {e:#}");
        }
        pki
    }

    /// Where a key's new form is written aside during a reseal (not
    /// `.next`, a worker's renewal's).
    fn next_path(path: &Path) -> PathBuf {
        let mut next = path.as_os_str().to_owned();
        next.push(".reseal");
        PathBuf::from(next)
    }

    /// Whether a reseal is begun but not finished.
    pub fn resealing(&self) -> bool {
        self.path(RESEALING).exists()
    }

    /// Carries a reseal through, if one was begun (each key's form aside
    /// put in place, its other form dropped, also for a key put in place
    /// before the reseal was cut short), or drops what an attempt left
    /// aside.
    pub fn settle(&self) -> anyhow::Result<()> {
        let sealing = read(&self.path(RESEALING))?.map(|to| to.trim() == "sealed");
        for f in SEALABLE {
            let forms = [
                (self.sealed_path(f), self.path(f)),
                (self.path(f), self.sealed_path(f)),
            ];
            match sealing {
                Some(sealing) => {
                    let (to, other) = &forms[usize::from(!sealing)];
                    let next = Self::next_path(to);
                    if next.exists() {
                        std::fs::rename(&next, to)
                            .with_context(|| format!("writing {}", to.display()))?;
                    }
                    if to.exists() {
                        remove(other)?;
                    }
                }
                None => {
                    for (to, _) in &forms {
                        remove(&Self::next_path(to))?;
                    }
                }
            }
        }
        remove(&self.path(RESEALING))
    }

    /// `settle`, if a reseal was cut short: what is left aside otherwise
    /// is only cleaned up, and need not stop a key being read or written.
    fn settle_cut_short(&self) -> anyhow::Result<()> {
        if self.resealing() {
            self.settle()?;
        }
        Ok(())
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
        // A reseal cut short is carried through first: its keys are
        // the ones to open.
        self.settle_cut_short()?;
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

    /// A private key, opened if sealed. A reseal cut short is carried
    /// through first, so the key is read in the form the lock says.
    fn read_secret(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.settle_cut_short()?;
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
    /// other form, so a crash between leaves both rather than neither. A
    /// reseal cut short is carried through first: else it would later put
    /// the key it wrote aside over this one.
    fn write_secret(&self, name: &str, text: &str) -> anyhow::Result<()> {
        self.settle_cut_short()?;
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
    /// the clear again. All are written aside first: failing there changes
    /// nothing (the old lock stays, to try again); once all are, the
    /// reseal is carried through, by `settle` if cut short.
    pub fn reseal(&mut self, lock: Option<legion_store::StoreKey>) -> anyhow::Result<()> {
        self.settle()?;
        let mut keys = Vec::new();
        for f in SEALABLE {
            if let Some(text) = self.read_secret(f)? {
                keys.push((f, text));
            }
        }
        let aside = || -> anyhow::Result<()> {
            for (f, text) in &keys {
                match &lock {
                    Some(key) => {
                        let bytes = legion_store::seal(key, f, text.as_bytes())?;
                        write_bytes(&Self::next_path(&self.sealed_path(f)), &bytes, 0o600)?;
                    }
                    None => write_private(&Self::next_path(&self.path(f)), text)?,
                }
            }
            let to = if lock.is_some() { "sealed" } else { "plain" };
            write(&self.path(RESEALING), to, 0o600)
        };
        if let Err(e) = aside() {
            let _ = remove(&self.path(RESEALING));
            let _ = self.settle();
            return Err(e);
        }
        self.lock = lock;
        self.settle()
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

    /// This manager's WireGuard private key, kept with its other private
    /// keys so that autolock seals it: taken over from
    /// `<data dir>/wireguard.key` where the agent made it, or made with
    /// `genkey` on first use.
    pub fn wireguard_key(
        &self,
        data_dir: &Path,
        genkey: impl FnOnce() -> anyhow::Result<String>,
    ) -> anyhow::Result<String> {
        if let Some(text) = self.read_secret(WIREGUARD_KEY)? {
            return Ok(text.trim().to_owned());
        }
        let agents = data_dir.join(WIREGUARD_KEY);
        let key = match read(&agents)? {
            Some(text) => text.trim().to_owned(),
            None => genkey().context("making the node's WireGuard key")?,
        };
        private_dir(&self.dir)?;
        self.write_secret(
            WIREGUARD_KEY,
            &format!(
                "{key}
"
            ),
        )?;
        remove(&agents)?;
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

    /// Takes up a new CA (a rotation begun): its key, and `trust_pem`, the
    /// CAs to trust with the new one first, as `ca.crt`. Both are written
    /// beside the old ones, the key first, and then put in place by
    /// [`Self::settle_ca`], which a start after a crash carries through:
    /// the key and the certificate it signs with always belong together.
    pub fn write_ca(&self, trust_pem: &str, key_pem: &str) -> anyhow::Result<()> {
        self.settle_cut_short()?;
        private_dir(&self.dir)?;
        let (plain, sealed) = self.next_ca_key();
        match &self.lock {
            Some(key) => {
                // Sealed as `ca.key`, the name it ends up under.
                let bytes = legion_store::seal(key, "ca.key", key_pem.as_bytes())?;
                write_bytes(&sealed, &bytes, 0o600)?;
            }
            None => write_private(&plain, key_pem)?,
        }
        write_public(&self.path(NEXT_CA_CERT), trust_pem)?;
        self.settle_ca()
    }

    /// The CAs to trust, the one that issues first, as `ca.crt`, the key
    /// unchanged: as a rotation ends, or for a worker as it begins.
    pub fn write_trust(&self, trust_pem: &str) -> anyhow::Result<()> {
        write_public(&self.path("ca.crt"), trust_pem)
    }

    fn next_ca_key(&self) -> (PathBuf, PathBuf) {
        (
            self.path("ca.key.next"),
            self.dir.join("ca.key.sealed.next"),
        )
    }

    /// Puts a new CA written aside by [`Self::write_ca`] in place, the key
    /// first; drops a key written without its certificate.
    fn settle_ca(&self) -> anyhow::Result<()> {
        let (plain, sealed) = self.next_ca_key();
        let cert = self.path(NEXT_CA_CERT);
        if !cert.exists() {
            remove(&plain)?;
            return remove(&sealed);
        }
        if sealed.exists() {
            rename(&sealed, &self.sealed_path("ca.key"))?;
            remove(&self.path("ca.key"))?;
        }
        if plain.exists() {
            rename(&plain, &self.path("ca.key"))?;
            remove(&self.sealed_path("ca.key"))?;
        }
        rename(&cert, &self.path("ca.crt"))
    }

    /// A worker promoted: its manager certificate, the CAs and the CA key,
    /// as a manager joining would have them. The daemon starts again as
    /// a manager on them, its store empty and filled from the others.
    pub fn write_promoted(
        &self,
        cert_pem: &str,
        key_pem: &str,
        ca_pem: &str,
        ca_key_pem: &str,
    ) -> anyhow::Result<()> {
        self.write_joined(&legion_manager::Joined {
            role: Role::Manager,
            tls: legion_manager::Tls {
                ca_pem: ca_pem.to_owned(),
                cert_pem: cert_pem.to_owned(),
                key_pem: key_pem.to_owned(),
                ca_key_pem: Some(ca_key_pem.to_owned()),
            },
        })
    }

    /// A manager demoted: a worker's credentials and its managers, and
    /// nothing of a manager left. The CA key, the store's key and the
    /// store go (set aside, as a restore does), so do its Raft id and the
    /// admin bundle; its WireGuard key moves to where a worker's agent
    /// keeps it, so its peers know it as before.
    pub fn write_demoted(
        &self,
        data_dir: &Path,
        demoted: &legion_manager::roles::Demoted,
    ) -> anyhow::Result<()> {
        if let Some(key) = self.read_secret(WIREGUARD_KEY)? {
            write_private(&data_dir.join(WIREGUARD_KEY), &key)?;
        }
        write_public(&self.path("ca.crt"), &demoted.ca_pem)?;
        write_private(&self.path(NEXT_KEY), &demoted.key_pem)?;
        write_public(&self.path(NEXT_CERT), &demoted.cert_pem)?;
        self.finish_renewal()?;
        write_public(&self.path("role"), "worker\n")?;
        for f in ["ca.key", "node.key", "store.key", WIREGUARD_KEY] {
            remove(&self.sealed_path(f))?;
        }
        for f in ["ca.key", "store.key", WIREGUARD_KEY] {
            remove(&self.path(f))?;
        }
        write_managers(data_dir, &demoted.managers)?;
        let stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        for f in ["store.redb", "node-id"] {
            let from = data_dir.join(f);
            if from.exists() {
                rename(&from, &data_dir.join(format!("{f}.demoted-{stamp}")))?;
            }
        }
        remove(&data_dir.join(ADMIN_BUNDLE))
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

    /// The name this manager's certificate was issued to, if it has one:
    /// a joined manager's may be a `--node-name` given only to `join`.
    pub fn manager_name(&self) -> anyhow::Result<Option<String>> {
        let Some(cert_pem) = read(&self.path("node.crt"))? else {
            return Ok(None);
        };
        let id = Bundle {
            cert_pem,
            key_pem: String::new(),
            ca_pem: String::new(),
        }
        .identity();
        Ok(id
            .ok()
            .filter(|id| id.role == Role::Manager)
            .map(|id| id.name))
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
        self.settle_ca()?;
        // `ca.crt` holds the CAs trusted, the one that issues first (two
        // while the cluster's CA is rotated).
        let (ca, trust) = match (read(&self.path("ca.crt"))?, self.read_secret("ca.key")?) {
            (Some(trust), Some(key)) => {
                let issuing = legion_pki::split_certs(&trust)
                    .into_iter()
                    .next()
                    .context("ca.crt holds no certificate")?;
                let ca = Ca::load(&issuing, &key).context("loading the cluster CA")?;
                (ca, trust)
            }
            (Some(_), None) => {
                anyhow::bail!("{} has no CA key: a manager needs one", self.dir.display())
            }
            _ => {
                let ca = Ca::generate()?;
                self.write_secret("ca.key", ca.key_pem())?;
                write_public(&self.path("ca.crt"), ca.cert_pem())?;
                tracing::info!(digest = %ca.digest(), "created a cluster CA");
                let trust = ca.cert_pem().to_owned();
                (ca, trust)
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
        // Of the CA that issues now, trusting what this manager trusts.
        let admin_ok = read(&admin)?
            .and_then(|pem| Bundle::parse(&pem).ok())
            .is_some_and(|b| {
                b.ca_pem == trust
                    && legion_pki::issuer(&b.cert_pem, ca.cert_pem()).is_some()
                    && !legion_pki::needs_renewal(&b.cert_pem, SystemTime::now())
            });
        if !admin_ok {
            let issued = ca.issue(&Identity::new("admin", Role::Admin), CERT_LIFETIME)?;
            write_private(&admin, &Bundle::new(issued, &trust).to_pem())?;
            tracing::info!(path = %admin.display(), "wrote the admin credentials");
        }

        Ok(legion_manager::Tls {
            ca_pem: trust,
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

/// A new CA's certificate (with the others trusted), while it is written.
const NEXT_CA_CERT: &str = "ca.crt.next";

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
        let now = SystemTime::now();
        let node_due = legion_pki::needs_renewal(&credentials.cert_and_key().0, now);
        let wait = if node_due || admin_due(&data_dir, now) {
            let renewed = pki
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ensure(&node, &data_dir)
                .and_then(|tls| {
                    if !node_due {
                        return Ok(());
                    }
                    credentials
                        .renew(&tls.cert_pem, &tls.key_pem)
                        .map_err(|e| anyhow::anyhow!(e))
                });
            match renewed {
                Ok(()) => {
                    tracing::info!("renewed this manager's credentials");
                    RENEWAL_CHECK
                }
                Err(e) => {
                    tracing::warn!("renewing this manager's credentials: {e:#}");
                    RENEWAL_RETRY
                }
            }
        } else {
            RENEWAL_CHECK
        };
        tokio::time::sleep(wait).await;
    }
}

/// The admin bundle is missing or needs renewing. It may have been issued
/// at another time than the node's certificate (written again after it was
/// removed, say), so it is checked on its own.
fn admin_due(data_dir: &Path, now: SystemTime) -> bool {
    !read(&data_dir.join(ADMIN_BUNDLE))
        .ok()
        .flatten()
        .and_then(|pem| Bundle::parse(&pem).ok())
        .is_some_and(|b| !legion_pki::needs_renewal(&b.cert_pem, now))
}

/// Keeps a worker's certificate current: past two thirds of its life, it
/// asks a manager to certify a new key under the same name, writes both
/// and calls with them from then on; and at once, of the new CA, when a
/// CA rotation begins (`crate::ca`). Runs until aborted.
pub async fn renew_worker(
    pki: Pki,
    credentials: Arc<Credentials>,
    control: legion_agent::GrpcControl,
) {
    let mut advice = control.cluster_ca();
    loop {
        let latest = advice.borrow_and_update().clone();
        let mut rotation_failed = false;
        if let Some(latest) = latest {
            match crate::ca::worker_take_up(&latest, &pki, &credentials, &control).await {
                Ok(true) => {
                    tracing::info!(ca = %latest.issuer, "this worker is on the cluster's CA")
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!("CA rotation: {e:#}");
                    rotation_failed = true;
                }
            }
        }
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
        // A rotation cut short (a manager not yet on the new CA, say) is
        // tried again soon.
        let wait = if rotation_failed {
            wait.min(ROTATION_RETRY)
        } else {
            wait
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            changed = advice.changed() => {
                if changed.is_err() {
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

/// After a step of a CA rotation failed on a worker, how soon to try again.
const ROTATION_RETRY: Duration = Duration::from_secs(10);

pub(crate) async fn renew_worker_once(
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

/// Writes `path` whole or not at all, through `<path>.new`. That file is
/// made new, so neither a leftover of another write (with other
/// permissions) nor a link someone put there is written through.
fn write_bytes(path: &Path, contents: &[u8], mode: u32) -> anyhow::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".new");
    let tmp = std::path::PathBuf::from(tmp);
    remove(&tmp)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    let mut f = options
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?;
    // The mode as asked, whatever the umask took away or left.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    std::io::Write::write_all(&mut f, contents)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key is written private even over a leftover `.new` file that is
    /// not, as an interrupted write of a certificate once left.
    #[cfg(unix)]
    #[test]
    fn keys_are_written_private_over_leftovers() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("node.key");
        let leftover = dir.path().join("node.key.new");
        std::fs::write(&leftover, "old").unwrap();
        std::fs::set_permissions(&leftover, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&key, "secret").unwrap();
        let mode = std::fs::metadata(&key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(std::fs::read_to_string(&key).unwrap(), "secret");
        assert!(!leftover.exists());
        // The certificate beside it has a temporary file of its own.
        write_public(&dir.path().join("node.crt"), "cert").unwrap();
        assert_eq!(std::fs::read_to_string(&key).unwrap(), "secret");
    }

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
    fn a_manager_knows_the_name_it_was_issued() {
        let dir = tempfile::tempdir().unwrap();
        let pki = Pki::new(dir.path());
        assert_eq!(pki.manager_name().unwrap(), None);
        pki.ensure("custom", dir.path()).unwrap();
        assert_eq!(pki.manager_name().unwrap().as_deref(), Some("custom"));
    }

    #[test]
    fn admin_bundle_renewed_on_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        assert!(admin_due(dir.path(), now), "missing");
        Pki::new(dir.path()).ensure("n1", dir.path()).unwrap();
        assert!(!admin_due(dir.path(), now));
        assert!(
            admin_due(dir.path(), now + CERT_LIFETIME),
            "past its renewal"
        );
    }

    #[test]
    fn the_wireguard_key_is_taken_over_and_sealed_with_the_others() {
        let dir = tempfile::tempdir().unwrap();
        let mut pki = Pki::new(dir.path());
        pki.store_key().unwrap();
        // The one the agent made before is the one kept.
        let agents = dir.path().join("wireguard.key");
        std::fs::write(
            &agents, "wg1
",
        )
        .unwrap();
        let made = || -> anyhow::Result<String> { panic!("a key was made") };
        assert_eq!(pki.wireguard_key(dir.path(), made).unwrap(), "wg1");
        assert!(!agents.exists());
        assert_eq!(pki.wireguard_key(dir.path(), made).unwrap(), "wg1");

        // Sealed and unsealed with the others.
        let key = legion_store::StoreKey::generate();
        pki.reseal(Some(key.clone())).unwrap();
        assert!(!pki.path("wireguard.key").exists());
        assert!(pki.sealed_path("wireguard.key").exists());
        let locked = Pki::new(dir.path());
        assert!(locked.locked());
        assert!(locked.wireguard_key(dir.path(), made).is_err());
        let unlocked = locked.unlock(key).unwrap();
        assert_eq!(unlocked.wireguard_key(dir.path(), made).unwrap(), "wg1");
        pki.reseal(None).unwrap();
        assert_eq!(
            std::fs::read_to_string(pki.path("wireguard.key")).unwrap(),
            "wg1
"
        );

        // Made where there is none.
        let other = tempfile::tempdir().unwrap();
        let pki = Pki::new(other.path());
        let made = pki
            .wireguard_key(other.path(), || Ok("wg2".to_owned()))
            .unwrap();
        assert_eq!(made, "wg2");
        assert_eq!(
            pki.wireguard_key(other.path(), || Ok("wg3".to_owned()))
                .unwrap(),
            "wg2"
        );
    }

    /// Sealing cut short after a key was put in place, before its clear
    /// form went: carried through, the clear form goes. A worker renewal's
    /// own files are not the reseal's.
    #[test]
    fn a_seal_cut_short_leaves_no_key_in_the_clear() {
        let dir = tempfile::tempdir().unwrap();
        let pki = Pki::new(dir.path());
        pki.store_key().unwrap();
        pki.wireguard_key(dir.path(), || Ok("wg".to_owned()))
            .unwrap();
        let renewal = pki.path(NEXT_KEY);
        write_private(&renewal, "renewed").unwrap();

        let key = legion_store::StoreKey::generate();
        let texts = ["store.key", WIREGUARD_KEY].map(|f| (f, pki.read_secret(f).unwrap().unwrap()));
        for (f, text) in texts {
            let bytes = legion_store::seal(&key, f, text.as_bytes()).unwrap();
            write_bytes(&Pki::next_path(&pki.sealed_path(f)), &bytes, 0o600).unwrap();
        }
        write(&pki.path(RESEALING), "sealed", 0o600).unwrap();
        let store = pki.sealed_path("store.key");
        std::fs::rename(Pki::next_path(&store), &store).unwrap();

        let reopened = Pki::new(dir.path());
        for f in ["store.key", WIREGUARD_KEY] {
            assert!(!reopened.path(f).exists(), "{f} in the clear");
            assert!(reopened.sealed_path(f).exists(), "{f} sealed");
        }
        assert!(reopened.unlock(key).is_ok());
        assert_eq!(std::fs::read_to_string(&renewal).unwrap(), "renewed");
    }

    /// A new unlock key that fails to seal every key changes nothing, to
    /// try again; one cut short once all are written aside is carried
    /// through: the keys are never under two unlock keys.
    #[test]
    fn a_reseal_cut_short_leaves_one_unlock_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut pki = Pki::new(dir.path());
        pki.store_key().unwrap();
        pki.wireguard_key(dir.path(), || Ok("wg".to_owned()))
            .unwrap();
        let (old, new) = (
            legion_store::StoreKey::generate(),
            legion_store::StoreKey::generate(),
        );
        pki.reseal(Some(old.clone())).unwrap();

        // The WireGuard key cannot be written aside.
        let blocked = Pki::next_path(&pki.sealed_path(WIREGUARD_KEY));
        std::fs::create_dir(&blocked).unwrap();
        assert!(pki.reseal(Some(new.clone())).is_err());
        assert_eq!(pki.lock(), Some(&old), "to be tried again");
        assert!(!Pki::next_path(&pki.sealed_path("store.key")).exists());
        assert!(Pki::new(dir.path()).unlock(old.clone()).is_ok());
        std::fs::remove_dir(&blocked).unwrap();

        // Written aside, then cut short with one key in place.
        let texts = ["store.key", WIREGUARD_KEY].map(|f| (f, pki.read_secret(f).unwrap().unwrap()));
        for (f, text) in texts {
            let bytes = legion_store::seal(&new, f, text.as_bytes()).unwrap();
            write_bytes(&Pki::next_path(&pki.sealed_path(f)), &bytes, 0o600).unwrap();
        }
        write(&pki.path(RESEALING), "sealed", 0o600).unwrap();
        let store = pki.sealed_path("store.key");
        std::fs::rename(Pki::next_path(&store), &store).unwrap();
        let reopened = Pki::new(dir.path());
        assert!(!reopened.resealing());
        assert!(Pki::new(dir.path()).unlock(old).is_err());
        let unlocked = reopened.unlock(new).unwrap();
        assert_eq!(
            unlocked
                .wireguard_key(dir.path(), || panic!("a key was made"))
                .unwrap(),
            "wg"
        );
    }

    /// A reseal whose settling failed, the new lock already in use: a key
    /// written then (a renewal, say) is not undone by the key written
    /// aside before it, nor is a key read from the form being left.
    #[test]
    fn a_key_written_while_a_reseal_is_unsettled_stays() {
        let dir = tempfile::tempdir().unwrap();
        let mut pki = Pki::new(dir.path());
        private_dir(&pki.dir).unwrap();
        pki.write_secret("node.key", "old").unwrap();
        let new = legion_store::StoreKey::generate();
        let bytes = legion_store::seal(&new, "node.key", b"old").unwrap();
        write_bytes(&Pki::next_path(&pki.sealed_path("node.key")), &bytes, 0o600).unwrap();
        write(&pki.path(RESEALING), "sealed", 0o600).unwrap();
        pki.lock = Some(new.clone());

        pki.write_secret("node.key", "renewed").unwrap();
        let reopened = Pki::new(dir.path()).unlock(new.clone()).unwrap();
        assert_eq!(
            reopened.read_secret("node.key").unwrap().unwrap(),
            "renewed"
        );

        // Unsealing, cut short: the clear form is still aside.
        write_private(&Pki::next_path(&pki.path("node.key")), "renewed").unwrap();
        write(&pki.path(RESEALING), "plain", 0o600).unwrap();
        pki.lock = None;
        assert_eq!(pki.read_secret("node.key").unwrap().unwrap(), "renewed");
        assert!(!pki.sealed_path("node.key").exists());
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
