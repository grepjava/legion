//! The daemon's certificates, under `<data dir>/pki` (DESIGN.md §9).
//!
//! The first start of a node that has not joined a cluster creates a
//! cluster CA, this manager's certificate and `<data dir>/admin.pem`, the
//! CLI's credentials. `legion join` writes the cluster's CA and the node's
//! certificate instead, with the CA key only for managers. Managers reissue
//! their own certificate once two thirds of its life are gone; workers
//! cannot renew theirs yet.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::Context;
use legion_pki::{Bundle, CERT_LIFETIME, Ca, Identity, Role};

/// The CLI's credentials, written on first start.
pub const ADMIN_BUNDLE: &str = "admin.pem";

pub struct Pki {
    dir: PathBuf,
}

impl Pki {
    pub fn new(data_dir: &Path) -> Pki {
        Pki {
            dir: data_dir.join("pki"),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
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
    pub fn worker(&self) -> anyhow::Result<(String, tonic::transport::ClientTlsConfig)> {
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
        if legion_pki::needs_renewal(&bundle.cert_pem, SystemTime::now()) {
            tracing::warn!(
                "this worker's certificate is past two thirds of its life; join again before it expires"
            );
        }
        let tls = legion_manager::client_config(&bundle.ca_pem, &bundle.cert_pem, &bundle.key_pem);
        Ok((id.name, tls))
    }

    /// The CA, this manager's credentials and the admin bundle, made or
    /// renewed as needed.
    pub fn ensure(&self, node: &str, data_dir: &Path) -> anyhow::Result<legion_manager::Tls> {
        private_dir(&self.dir)?;
        if self.role()? != Role::Manager {
            anyhow::bail!("this node joined as a worker and holds no manager credentials");
        }
        let ca = match (read(&self.path("ca.crt"))?, read(&self.path("ca.key"))?) {
            (Some(cert), Some(key)) => Ca::load(&cert, &key).context("loading the cluster CA")?,
            (Some(_), None) => {
                anyhow::bail!("{} has no CA key: a manager needs one", self.dir.display())
            }
            _ => {
                let ca = Ca::generate()?;
                write_private(&self.path("ca.key"), ca.key_pem())?;
                write_public(&self.path("ca.crt"), ca.cert_pem())?;
                tracing::info!(digest = %ca.digest(), "created a cluster CA");
                ca
            }
        };

        let id = Identity::new(node, Role::Manager);
        let current = match (read(&self.path("node.crt"))?, read(&self.path("node.key"))?) {
            (Some(cert), Some(key)) => Some((cert, key)),
            _ => None,
        };
        let usable = current.as_ref().is_some_and(|(cert, _)| {
            !legion_pki::needs_renewal(cert, SystemTime::now()) && issued_as(cert, &id)
        });
        let (cert_pem, key_pem) = match current {
            Some(pair) if usable => pair,
            _ => {
                let issued = ca.issue(&id, CERT_LIFETIME)?;
                write_private(&self.path("node.key"), &issued.key_pem)?;
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

/// The key this manager's store is sealed with, `pki/store.key`, made on
/// first use. A store written before it existed is read as it is and
/// sealed as it is written again.
pub fn store_key(data_dir: &Path) -> anyhow::Result<legion_store::StoreKey> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    let dir = data_dir.join("pki");
    let path = dir.join("store.key");
    if let Some(text) = read(&path)? {
        let bytes = STANDARD
            .decode(text.trim())
            .with_context(|| format!("{} is not base64", path.display()))?;
        return legion_store::StoreKey::from_bytes(&bytes)
            .with_context(|| format!("reading {}", path.display()));
    }
    private_dir(&dir)?;
    let key = legion_store::StoreKey::generate();
    write_private(&path, &format!("{}\n", STANDARD.encode(key.as_bytes())))?;
    tracing::info!(path = %path.display(), "made the store's key");
    Ok(key)
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

fn write(path: &Path, contents: &str, mode: u32) -> anyhow::Result<()> {
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
    std::io::Write::write_all(&mut f, contents.as_bytes())?;
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
    }
}
