//! A node's images: each layer a ZFS dataset cloned from the layer below
//! and snapshotted (`<dataset>/<chain id>@layer`), so images share their
//! common layers and a container's root is a clone of its image's top
//! layer. What images there are, and their configs, is kept in
//! `<dir>/index.json`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use legion_sys::Sys;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::oci::{self, Document, ImageConfig, Manifest};
use crate::registry::{Client, Credentials};
use crate::{Error, Reference, layer};

const SNAPSHOT: &str = "layer";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Image {
    /// The config's digest, as Podman's image ids are.
    pub id: String,
    /// The manifest's digest.
    pub digest: String,
    /// Canonical references naming it.
    pub references: Vec<String>,
    /// Chain ids (hex), bottom layer first.
    pub layers: Vec<String>,
    pub config: ImageConfig,
}

pub struct Store {
    sys: Arc<dyn Sys>,
    dataset: String,
    dir: PathBuf,
    client: Client,
    lock: tokio::sync::Mutex<()>,
}

enum Source<'a> {
    Registry {
        client: &'a Client,
        reference: &'a Reference,
        creds: Option<&'a Credentials>,
    },
    /// An OCI image layout directory (`podman save --format oci-dir`).
    Layout(&'a Path),
}

impl Source<'_> {
    async fn manifest(&self) -> Result<(String, Manifest), Error> {
        match self {
            Source::Registry {
                client,
                reference,
                creds,
            } => client.manifest(reference, *creds).await,
            Source::Layout(dir) => {
                let mut doc = Document::parse(&std::fs::read(dir.join("index.json"))?)?;
                let mut digest = String::new();
                // An index, possibly of indexes, down to a manifest.
                for _ in 0..3 {
                    let Some(manifests) = &doc.manifests else {
                        return Ok((digest, doc.into_manifest()?));
                    };
                    let chosen = match manifests.as_slice() {
                        [one] => one,
                        many => oci::select(many)?,
                    };
                    digest = chosen.digest.clone();
                    let bytes = std::fs::read(blob_path(dir, &digest)?)?;
                    check(&bytes, &digest, "manifest")?;
                    doc = Document::parse(&bytes)?;
                }
                Err(Error::Unsupported("indexes nested too deep".to_owned()))
            }
        }
    }

    async fn blob(&self, digest: &str, dest: &Path) -> Result<(), Error> {
        match self {
            Source::Registry {
                client,
                reference,
                creds,
            } => client.blob(reference, digest, dest, *creds).await,
            Source::Layout(dir) => {
                let from = blob_path(dir, digest)?;
                let dest = dest.to_owned();
                let digest = digest.to_owned();
                blocking(move || {
                    let mut hash = Sha256::new();
                    let mut input = std::fs::File::open(&from)?;
                    let mut out = std::fs::File::create(&dest)?;
                    std::io::copy(&mut input, &mut HashWriter(&mut out, &mut hash))?;
                    let got = format!("sha256:{:x}", hash.finalize());
                    if got != digest {
                        return Err(Error::Digest {
                            what: "blob".to_owned(),
                            expected: digest,
                            got,
                        });
                    }
                    Ok(())
                })
                .await
            }
        }
    }
}

struct HashWriter<'a>(&'a mut std::fs::File, &'a mut Sha256);

impl std::io::Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.0.write(buf)?;
        self.1.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

fn blob_path(dir: &Path, digest: &str) -> Result<PathBuf, Error> {
    let hex = hex_of(digest)?;
    Ok(dir.join("blobs").join("sha256").join(hex))
}

/// The hex of a `sha256:` digest, checked, so it is safe in paths and
/// dataset names.
fn hex_of(digest: &str) -> Result<&str, Error> {
    digest
        .strip_prefix("sha256:")
        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| Error::Unsupported(format!("digest {digest:?}")))
}

fn check(bytes: &[u8], digest: &str, what: &str) -> Result<(), Error> {
    let got = crate::registry::sha256(bytes);
    if got != digest {
        return Err(Error::Digest {
            what: what.to_owned(),
            expected: digest.to_owned(),
            got,
        });
    }
    Ok(())
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
}

/// The chain ids of layers with these `diff_id`s, bottom first.
pub fn chain_ids(diff_ids: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for d in diff_ids {
        let id = match out.last() {
            None => d.clone(),
            Some(parent) => format!(
                "sha256:{:x}",
                Sha256::digest(format!("{parent} {d}").as_bytes())
            ),
        };
        out.push(id);
    }
    out
}

impl Store {
    /// The store in ZFS dataset `dataset` (made if missing) with its index
    /// in `dir`.
    pub fn open(
        sys: Arc<dyn Sys>,
        dataset: &str,
        dir: &Path,
        client: Client,
    ) -> Result<Store, Error> {
        sys.zfs_create(dataset, &[("mountpoint", "none")])?;
        let _ = std::fs::remove_dir_all(dir.join("tmp"));
        std::fs::create_dir_all(dir.join("tmp"))?;
        std::fs::create_dir_all(dir.join("mnt"))?;
        Ok(Store {
            sys,
            dataset: dataset.to_owned(),
            dir: dir.to_owned(),
            client,
            lock: tokio::sync::Mutex::new(()),
        })
    }

    fn index_path(&self) -> PathBuf {
        self.dir.join("index.json")
    }

    pub fn images(&self) -> Result<Vec<Image>, Error> {
        match std::fs::read(self.index_path()) {
            Ok(b) => Ok(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn save(&self, images: &[Image]) -> Result<(), Error> {
        let tmp = self.dir.join("index.json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(images)?)?;
        std::fs::rename(tmp, self.index_path())?;
        Ok(())
    }

    /// The image `reference` names, if the store has it.
    pub fn get(&self, reference: &str) -> Result<Option<Image>, Error> {
        let name = Reference::parse(reference)?.to_string();
        Ok(self
            .images()?
            .into_iter()
            .find(|i| i.references.contains(&name)))
    }

    /// The snapshot a container of `image` is cloned from; `None` for an
    /// image without layers.
    pub fn snapshot(&self, image: &Image) -> Option<String> {
        let top = image.layers.last()?;
        Some(format!("{}/{top}@{SNAPSHOT}", self.dataset))
    }

    /// Pulls `reference` from its registry.
    pub async fn pull(&self, reference: &str, creds: Option<&Credentials>) -> Result<Image, Error> {
        let r = Reference::parse(reference)?;
        if r.is_local() {
            return Err(Error::NotFound(format!(
                "{r} is a local image: no registry serves it; load it onto the node"
            )));
        }
        let _held = self.hold().await?;
        let source = Source::Registry {
            client: &self.client,
            reference: &r,
            creds,
        };
        self.ingest(&source, &r).await
    }

    /// Adds the image in an OCI layout directory as `reference`.
    pub async fn import(&self, layout: &Path, reference: &str) -> Result<Image, Error> {
        let r = Reference::parse(reference)?;
        let _held = self.hold().await?;
        self.ingest(&Source::Layout(layout), &r).await
    }

    /// Adds the image in an OCI archive (`podman save --format
    /// oci-archive`, a tar of a layout), named `reference` or as the
    /// archive names it.
    pub async fn load(&self, archive: &Path, reference: Option<&str>) -> Result<Image, Error> {
        let _held = self.hold().await?;
        let tmp = tempfile::Builder::new()
            .prefix("load")
            .tempdir_in(self.dir.join("tmp"))?;
        let out = std::process::Command::new("tar")
            .arg("-xf")
            .arg(archive)
            .arg("-C")
            .arg(tmp.path())
            .output()?;
        if !out.status.success() {
            return Err(Error::Extract(
                String::from_utf8_lossy(&out.stderr).trim().to_owned(),
            ));
        }
        let index = tmp.path().join("index.json");
        if !index.exists() {
            return Err(Error::Unsupported(
                "not an OCI archive (no index.json): save it with --format oci-archive".to_owned(),
            ));
        }
        let name = match reference {
            Some(r) => r.to_owned(),
            None => {
                let doc = Document::parse(&std::fs::read(&index)?)?;
                doc.manifests
                    .unwrap_or_default()
                    .iter()
                    .find_map(|m| {
                        m.annotations
                            .get("org.opencontainers.image.ref.name")
                            .cloned()
                    })
                    .ok_or_else(|| {
                        Error::Unsupported("the archive names no image: give a name".to_owned())
                    })?
            }
        };
        let r = Reference::parse(&name)?;
        self.ingest(&Source::Layout(tmp.path()), &r).await
    }

    /// The store to itself: this process's other users wait, and so do
    /// other processes (`legion image load` beside a running daemon).
    async fn hold(&self) -> Result<Held<'_>, Error> {
        let inner = self.lock.lock().await;
        let path = self.dir.join("lock");
        let file = blocking(move || lock_file(&path)).await?;
        Ok(Held {
            _inner: inner,
            _file: file,
        })
    }

    async fn ingest(&self, source: &Source<'_>, r: &Reference) -> Result<Image, Error> {
        let (digest, manifest) = source.manifest().await?;
        let tmp = tempfile::Builder::new()
            .prefix("pull")
            .tempdir_in(self.dir.join("tmp"))?;
        let config_path = tmp.path().join("config.json");
        source.blob(&manifest.config.digest, &config_path).await?;
        let config: ImageConfig = serde_json::from_slice(&std::fs::read(&config_path)?)
            .map_err(|e| Error::Unsupported(format!("the image config: {e}")))?;
        let (os, _) = oci::platform();
        if !config.os.is_empty() && config.os != os {
            return Err(Error::Unsupported(format!(
                "{r} is an image for {}/{}, not {os}",
                config.os, config.architecture
            )));
        }
        if config.rootfs.diff_ids.len() != manifest.layers.len() {
            return Err(Error::Unsupported(format!(
                "{r}: {} layers but {} diff_ids",
                manifest.layers.len(),
                config.rootfs.diff_ids.len()
            )));
        }
        let chains = chain_ids(&config.rootfs.diff_ids);
        let mut layers = Vec::new();
        let mut parent: Option<String> = None;
        for (i, (desc, chain)) in manifest.layers.iter().zip(&chains).enumerate() {
            let hex = hex_of(chain)?.to_owned();
            let ds = format!("{}/{hex}", self.dataset);
            let sys = self.sys.clone();
            let have = {
                let ds = ds.clone();
                blocking(move || Ok(sys.zfs_snapshots(&ds)?.iter().any(|s| s == SNAPSHOT))).await?
            };
            if !have {
                let blob = tmp.path().join(format!("{i}.blob"));
                source.blob(&desc.digest, &blob).await?;
                let tar = tmp.path().join(format!("{i}.tar"));
                let diff_id = config.rootfs.diff_ids[i].clone();
                {
                    let (blob, tar) = (blob.clone(), tar.clone());
                    blocking(move || layer::decompress(&blob, &diff_id, &tar)).await?;
                }
                let _ = std::fs::remove_file(&blob);
                let mnt = self.dir.join("mnt").join(&hex);
                let sys = self.sys.clone();
                let parent_snap = parent.clone().map(|p| format!("{p}@{SNAPSHOT}"));
                let ds2 = ds.clone();
                blocking(move || apply_layer(&*sys, &ds2, parent_snap.as_deref(), &mnt, &tar))
                    .await?;
                tracing::info!(image = %r, layer = %hex, "layer added");
            }
            layers.push(hex);
            parent = Some(ds);
        }
        let image = Image {
            id: manifest.config.digest.clone(),
            digest,
            references: vec![r.to_string()],
            layers,
            config,
        };
        let mut images = self.images()?;
        let name = r.to_string();
        for i in &mut images {
            i.references.retain(|n| *n != name);
        }
        let image = match images.iter_mut().find(|i| i.id == image.id) {
            Some(i) => {
                i.references.push(name);
                i.clone()
            }
            None => {
                images.push(image.clone());
                image
            }
        };
        images.retain(|i| !i.references.is_empty());
        self.save(&images)?;
        Ok(image)
    }

    /// Forgets `reference` and destroys the layers no image uses any more
    /// (those that containers still use stay until a later prune).
    pub async fn remove(&self, reference: &str) -> Result<bool, Error> {
        let name = Reference::parse(reference)?.to_string();
        let _held = self.hold().await?;
        let mut images = self.images()?;
        let mut found = false;
        for i in &mut images {
            let before = i.references.len();
            i.references.retain(|n| *n != name);
            found |= i.references.len() != before;
        }
        images.retain(|i| !i.references.is_empty());
        self.save(&images)?;
        self.prune_held(&images).await?;
        Ok(found)
    }

    /// Destroys layers that no image uses and no container was cloned
    /// from; returns their chain ids.
    pub async fn prune(&self) -> Result<Vec<String>, Error> {
        let _held = self.hold().await?;
        let images = self.images()?;
        self.prune_held(&images).await
    }

    async fn prune_held(&self, images: &[Image]) -> Result<Vec<String>, Error> {
        let used: BTreeSet<String> = images.iter().flat_map(|i| i.layers.clone()).collect();
        let sys = self.sys.clone();
        let root = self.dataset.clone();
        blocking(move || {
            let prefix = format!("{root}/");
            let mut unused: Vec<String> = sys
                .zfs_list(&root)?
                .into_iter()
                .filter_map(|d| d.name.strip_prefix(&prefix).map(str::to_owned))
                .filter(|n| !n.contains('/') && !used.contains(n))
                .collect();
            // A layer goes once nothing is cloned from it: retry those that
            // the layers above them held until nothing changes.
            let mut gone = Vec::new();
            loop {
                let before = unused.len();
                unused.retain(|n| match sys.zfs_destroy(&format!("{root}/{n}"), true) {
                    Ok(()) => {
                        gone.push(n.clone());
                        false
                    }
                    Err(_) => true,
                });
                if unused.len() == before {
                    break;
                }
            }
            Ok(gone)
        })
        .await
    }
}

struct Held<'a> {
    _inner: tokio::sync::MutexGuard<'a, ()>,
    /// Holds the lock while open.
    _file: std::fs::File,
}

fn lock_file(path: &Path) -> Result<std::fs::File, Error> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: flock on a descriptor we own; released when it closes.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(file)
}

/// Makes layer dataset `ds` (a clone of `parent`, or new), applies `tar`
/// to it mounted at `mnt`, unmounts and snapshots it. A dataset left by an
/// interrupted attempt is made again.
fn apply_layer(
    sys: &dyn Sys,
    ds: &str,
    parent: Option<&str>,
    mnt: &Path,
    tar: &Path,
) -> Result<(), Error> {
    if !sys.zfs_list(ds)?.is_empty() {
        sys.zfs_destroy(ds, true)?;
    }
    std::fs::create_dir_all(mnt)?;
    let mountpoint = mnt.to_string_lossy().into_owned();
    let props = [("mountpoint", mountpoint.as_str())];
    match parent {
        Some(p) => sys.zfs_clone(p, ds, &props)?,
        None => sys.zfs_create(ds, &props)?,
    }
    let applied = layer::apply(tar, mnt)
        .and_then(|()| Ok(sys.zfs_set(ds, "mountpoint", "none")?))
        .and_then(|()| Ok(sys.zfs_snapshot(&format!("{ds}@{SNAPSHOT}"))?));
    if let Err(e) = applied {
        let _ = sys.zfs_destroy(ds, true);
        return Err(e);
    }
    let _ = std::fs::remove_dir(mnt);
    Ok(())
}
