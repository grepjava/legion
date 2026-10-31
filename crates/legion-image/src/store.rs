//! A node's images: each layer a ZFS dataset cloned from the layer below
//! and snapshotted (`<dataset>/<chain id>@layer`), so images share their
//! common layers and a container's root is a clone of its image's top
//! layer. What images there are, and their configs, is kept in
//! `<dir>/index.json`.
//!
//! For building and publishing images (jailer) a store can also keep its
//! layers as plain directories (`<dir>/layers/<chain id>`) where there is no
//! ZFS, keep the compressed blobs it pulls (`<dir>/blobs/sha256`), take
//! layers built on the node, push images and export them as archives.
//! None of that is on unless asked for.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use legion_sys::Sys;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::oci::{
    self, Descriptor, Document, ImageConfig, Manifest, OCI_CONFIG, OCI_INDEX, OCI_LAYER_GZIP,
    OCI_MANIFEST, Platform,
};
use crate::registry::{Client, Credentials, sha256};
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
    /// The compressed layers, in order, where the store keeps blobs: what
    /// a push or an export sends.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layer_descriptors: Vec<Descriptor>,
    /// References pulled with credentials, and who pulled them: only they
    /// may use the image without asking its registry again. Others are
    /// public (pulled without credentials, or loaded).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub private: BTreeMap<String, BTreeSet<String>>,
}

impl Image {
    /// Whether `user` may use the image as `reference` names it.
    pub fn usable_by(&self, reference: &str, user: &str) -> bool {
        self.private.get(reference).is_none_or(|u| u.contains(user))
    }
}

/// Where a store keeps its layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMode {
    /// Each layer a ZFS dataset `<dataset>/<chain id>@layer`.
    Zfs(String),
    /// Each layer a directory `<dir>/layers/<chain id>`, a full copy of the
    /// image's files up to it.
    Directory,
}

/// Names (chain ids, blob hex) that a store's user keeps beyond its images,
/// such as a build cache's; read from the store's directory at each prune.
pub type GcRoots = Arc<dyn Fn(&Path) -> Result<BTreeSet<String>, Error> + Send + Sync>;

pub struct Store {
    sys: Arc<dyn Sys>,
    mode: StorageMode,
    dir: PathBuf,
    client: Client,
    lock: tokio::sync::Mutex<()>,
    /// Operating systems whose images the node runs, preferred first.
    oses: Vec<&'static str>,
    /// Whether pulled blobs are kept in `<dir>/blobs/sha256`.
    keep_blobs: bool,
    roots: Option<GcRoots>,
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
    async fn manifest(&self, oses: &[&str]) -> Result<(String, Manifest), Error> {
        match self {
            Source::Registry {
                client,
                reference,
                creds,
            } => client.manifest(reference, *creds, oses).await,
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
                        many => oci::select(many, oses)?,
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
pub fn hex_of(digest: &str) -> Result<&str, Error> {
    digest
        .strip_prefix("sha256:")
        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| Error::Unsupported(format!("digest {digest:?}")))
}

fn check(bytes: &[u8], digest: &str, what: &str) -> Result<(), Error> {
    let got = sha256(bytes);
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

/// The platform an image is for, defaults filled in.
fn platform_of(config: &ImageConfig) -> Platform {
    Platform {
        architecture: if config.architecture.is_empty() {
            oci::arch().to_owned()
        } else {
            config.architecture.clone()
        },
        os: if config.os.is_empty() {
            "freebsd".to_owned()
        } else {
            config.os.clone()
        },
    }
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
        Store::new(sys, StorageMode::Zfs(dataset.to_owned()), dir, client)
    }

    /// The store with its layers as directories under `dir`, for hosts
    /// without ZFS.
    pub fn open_directory(sys: Arc<dyn Sys>, dir: &Path, client: Client) -> Result<Store, Error> {
        std::fs::create_dir_all(dir.join("layers"))?;
        Store::new(sys, StorageMode::Directory, dir, client)
    }

    fn new(
        sys: Arc<dyn Sys>,
        mode: StorageMode,
        dir: &Path,
        client: Client,
    ) -> Result<Store, Error> {
        let _ = std::fs::remove_dir_all(dir.join("tmp"));
        std::fs::create_dir_all(dir.join("tmp"))?;
        std::fs::create_dir_all(dir.join("mnt"))?;
        Ok(Store {
            sys,
            mode,
            dir: dir.to_owned(),
            client,
            lock: tokio::sync::Mutex::new(()),
            oses: vec!["freebsd"],
            keep_blobs: false,
            roots: None,
        })
    }

    /// Takes Linux images too, where the image offers no FreeBSD one: for
    /// nodes that run them through the Linux ABI.
    pub fn with_linux(mut self, linux: bool) -> Store {
        self.oses = if linux {
            vec!["freebsd", "linux"]
        } else {
            vec!["freebsd"]
        };
        self
    }

    /// Keeps the compressed blobs of pulled images, and their layer
    /// descriptors, so they can be pushed and exported as they came.
    pub fn keep_blobs(mut self) -> Store {
        self.keep_blobs = true;
        self
    }

    /// Keeps, at each prune, the layers and blobs `roots` names besides
    /// those of images and live [`Pin`]s.
    pub fn with_gc_roots(mut self, roots: GcRoots) -> Store {
        self.roots = Some(roots);
        self
    }

    /// Whether the node runs images for `os`.
    pub fn runs(&self, os: &str) -> bool {
        self.oses.contains(&os)
    }

    pub fn mode(&self) -> &StorageMode {
        &self.mode
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn sys(&self) -> &Arc<dyn Sys> {
        &self.sys
    }

    fn index_path(&self) -> PathBuf {
        self.dir.join("index.json")
    }

    /// Where the store keeps the blob `digest`.
    pub fn blob_path(&self, digest: &str) -> Result<PathBuf, Error> {
        blob_path(&self.dir, digest)
    }

    /// [`Self::blob_path`], its directory made.
    fn blob_dest(&self, digest: &str) -> Result<PathBuf, Error> {
        let path = self.blob_path(digest)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        Ok(path)
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

    /// The image `query` names as a person at a build host means it: a
    /// local name first (`app` is `localhost/app:latest`), then a registry
    /// reference (`nginx` is `docker.io/library/nginx:latest`), then an
    /// image id or a prefix of one of at least 6 hex digits.
    pub fn find(&self, query: &str) -> Result<Option<Image>, Error> {
        let images = self.images()?;
        let mut names = Vec::new();
        if let Ok(local) = Reference::parse_local_tag(query) {
            names.push(local.to_string());
        }
        if let Ok(r) = Reference::parse(query) {
            names.push(r.to_string());
        }
        for name in &names {
            if let Some(i) = images.iter().find(|i| i.references.contains(name)) {
                return Ok(Some(i.clone()));
            }
        }
        let hex = query.strip_prefix("sha256:").unwrap_or(query);
        if hex.len() >= 6 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            let prefix = format!("sha256:{hex}");
            return Ok(images.into_iter().find(|i| i.id.starts_with(&prefix)));
        }
        Ok(None)
    }

    /// The snapshot a container of `image` is cloned from; `None` for an
    /// image without layers, or a store without ZFS.
    pub fn snapshot(&self, image: &Image) -> Option<String> {
        let StorageMode::Zfs(dataset) = &self.mode else {
            return None;
        };
        let top = image.layers.last()?;
        Some(format!("{dataset}/{top}@{SNAPSHOT}"))
    }

    /// The directory of layer `chain_hex` in a store without ZFS.
    pub fn layer_dir(&self, chain_hex: &str) -> PathBuf {
        self.dir.join("layers").join(chain_hex)
    }

    /// Whether the store has layer `chain_hex`.
    pub fn has_layer(&self, chain_hex: &str) -> Result<bool, Error> {
        Ok(match &self.mode {
            StorageMode::Zfs(dataset) => self
                .sys
                .zfs_snapshots(&format!("{dataset}/{chain_hex}"))?
                .iter()
                .any(|s| s == SNAPSHOT),
            StorageMode::Directory => self.layer_dir(chain_hex).exists(),
        })
    }

    /// Makes `root` a writable copy of `image`'s files: a ZFS clone named
    /// `dataset` mounted there, or without a dataset (or ZFS) a plain
    /// directory copy.
    pub fn prepare_rootfs(
        &self,
        image: &Image,
        root: &Path,
        dataset: Option<&str>,
    ) -> Result<(), Error> {
        let top = image
            .layers
            .last()
            .ok_or_else(|| Error::Unsupported("an image without layers".to_owned()))?;
        std::fs::create_dir_all(root)?;
        match (&self.mode, dataset) {
            (StorageMode::Zfs(layers), Some(ds)) => {
                if let Some((parent, _)) = ds.rsplit_once('/') {
                    self.sys.zfs_create(parent, &[("mountpoint", "none")])?;
                }
                let mountpoint = root.to_string_lossy().into_owned();
                self.sys.zfs_clone(
                    &format!("{layers}/{top}@{SNAPSHOT}"),
                    ds,
                    &[("mountpoint", &mountpoint)],
                )?;
                Ok(())
            }
            (StorageMode::Zfs(_), None) => {
                self.with_layer_mounted(top, |mnt| layer::copy_tree(mnt, root))
            }
            (StorageMode::Directory, _) => layer::copy_tree(&self.layer_dir(top), root),
        }
    }

    /// Undoes [`Self::prepare_rootfs`].
    pub fn cleanup_rootfs(&self, root: &Path, dataset: Option<&str>) -> Result<(), Error> {
        if let (StorageMode::Zfs(_), Some(ds)) = (&self.mode, dataset)
            && !self.sys.zfs_list(ds)?.is_empty()
        {
            self.sys.zfs_destroy(ds, true)?;
        }
        layer::remove_tree(root)
    }

    /// Runs `f` on layer `chain_hex`'s files: its directory, or a temporary
    /// clone of its snapshot.
    fn with_layer_mounted<T>(
        &self,
        chain_hex: &str,
        f: impl FnOnce(&Path) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let StorageMode::Zfs(layers) = &self.mode else {
            return f(&self.layer_dir(chain_hex));
        };
        let tmp = tempfile::Builder::new()
            .prefix("mount")
            .tempdir_in(self.dir.join("tmp"))?;
        let name = tmp
            .path()
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let ds = format!("{layers}/tmp-{}-{name}", std::process::id());
        let mountpoint = tmp.path().to_string_lossy().into_owned();
        self.sys.zfs_clone(
            &format!("{layers}/{chain_hex}@{SNAPSHOT}"),
            &ds,
            &[("mountpoint", &mountpoint)],
        )?;
        let out = f(tmp.path());
        let _ = self.sys.zfs_destroy(&ds, true);
        out
    }

    /// The image `reference` names if the store has it and `user` may use
    /// it without asking its registry (see [`Image::private`]).
    pub fn usable(&self, reference: &str, user: &str) -> Result<Option<Image>, Error> {
        let name = Reference::parse(reference)?.to_string();
        Ok(self.get(&name)?.filter(|i| i.usable_by(&name, user)))
    }

    /// Pulls `reference` from its registry for `user`, with `creds` if
    /// given; an image pulled with credentials is then private to those
    /// who pulled it.
    pub async fn pull(
        &self,
        reference: &str,
        creds: Option<&Credentials>,
        user: &str,
    ) -> Result<Image, Error> {
        let r = Reference::parse(reference)?;
        if r.is_local() {
            return Err(Error::NotFound(format!(
                "{r} is a local image: no registry serves it; load it onto the node"
            )));
        }
        let _held = self.lock().await?;
        let source = Source::Registry {
            client: &self.client,
            reference: &r,
            creds,
        };
        self.ingest(&source, &r, creds.map(|_| user)).await
    }

    /// Adds the image in an OCI layout directory as `reference`.
    pub async fn import(&self, layout: &Path, reference: &str) -> Result<Image, Error> {
        let r = Reference::parse(reference)?;
        let _held = self.lock().await?;
        self.ingest(&Source::Layout(layout), &r, None).await
    }

    /// Adds the image in an OCI archive (`podman save --format
    /// oci-archive`, a tar of a layout), named `reference` or as the
    /// archive names it.
    pub async fn load(&self, archive: &Path, reference: Option<&str>) -> Result<Image, Error> {
        let _held = self.lock().await?;
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
        self.ingest(&Source::Layout(tmp.path()), &r, None).await
    }

    /// The store to itself: this process's other users wait, and so do
    /// other processes (`legion image load` beside a running daemon).
    pub async fn lock(&self) -> Result<StoreLock<'_>, Error> {
        let inner = self.lock.lock().await;
        let path = self.dir.join("lock");
        let file = blocking(move || lock_file(&path)).await?;
        Ok(StoreLock {
            _inner: inner,
            _file: file,
        })
    }

    /// Adds the image `source` holds as `r`, private to `private` if given.
    async fn ingest(
        &self,
        source: &Source<'_>,
        r: &Reference,
        private: Option<&str>,
    ) -> Result<Image, Error> {
        let (digest, manifest) = source.manifest(&self.oses).await?;
        let tmp = tempfile::Builder::new()
            .prefix("pull")
            .tempdir_in(self.dir.join("tmp"))?;
        let config_path = tmp.path().join("config.json");
        source.blob(&manifest.config.digest, &config_path).await?;
        let config_bytes = std::fs::read(&config_path)?;
        let config: ImageConfig = serde_json::from_slice(&config_bytes)
            .map_err(|e| Error::Unsupported(format!("the image config: {e}")))?;
        if !config.os.is_empty() && !self.oses.contains(&config.os.as_str()) {
            return Err(Error::Unsupported(format!(
                "{r} is an image for {}/{}; this node runs {} images",
                config.os,
                config.architecture,
                self.oses.join(" and ")
            )));
        }
        if config.rootfs.diff_ids.len() != manifest.layers.len() {
            return Err(Error::Unsupported(format!(
                "{r}: {} layers but {} diff_ids",
                manifest.layers.len(),
                config.rootfs.diff_ids.len()
            )));
        }
        if self.keep_blobs {
            let kept = self.blob_dest(&manifest.config.digest)?;
            if !kept.exists() {
                std::fs::write(&kept, &config_bytes)?;
            }
        }
        let chains = chain_ids(&config.rootfs.diff_ids);
        let mut layers = Vec::new();
        let mut parent: Option<String> = None;
        for (i, (desc, chain)) in manifest.layers.iter().zip(&chains).enumerate() {
            let hex = hex_of(chain)?.to_owned();
            let have = {
                let (store_mode, sys, dir) =
                    (self.mode.clone(), self.sys.clone(), self.dir.clone());
                let hex = hex.clone();
                blocking(move || match store_mode {
                    StorageMode::Zfs(dataset) => Ok(sys
                        .zfs_snapshots(&format!("{dataset}/{hex}"))?
                        .iter()
                        .any(|s| s == SNAPSHOT)),
                    StorageMode::Directory => Ok(dir.join("layers").join(&hex).exists()),
                })
                .await?
            };
            let kept = if self.keep_blobs {
                Some(self.blob_dest(&desc.digest)?)
            } else {
                None
            };
            let need_blob = kept.as_ref().is_some_and(|k| !k.exists());
            if !have || need_blob {
                let blob = tmp.path().join(format!("{i}.blob"));
                source.blob(&desc.digest, &blob).await?;
                if let Some(k) = kept.as_ref().filter(|k| !k.exists()) {
                    std::fs::copy(&blob, k)?;
                }
                if !have {
                    let tar = tmp.path().join(format!("{i}.tar"));
                    let diff_id = config.rootfs.diff_ids[i].clone();
                    {
                        let (blob, tar) = (blob.clone(), tar.clone());
                        blocking(move || layer::decompress(&blob, &diff_id, &tar)).await?;
                    }
                    let _ = std::fs::remove_file(&blob);
                    self.add_layer(parent.as_deref(), &hex, tar).await?;
                    tracing::info!(image = %r, layer = %hex, "layer added");
                }
            }
            layers.push(hex.clone());
            parent = Some(hex);
        }
        let image = Image {
            id: manifest.config.digest.clone(),
            digest,
            references: vec![r.to_string()],
            layers,
            config,
            layer_descriptors: if self.keep_blobs {
                manifest.layers
            } else {
                Vec::new()
            },
            private: BTreeMap::new(),
        };
        self.upsert(image, r, private)
    }

    /// Makes layer `hex` from the uncompressed layer `tar` on top of layer
    /// `parent` (hex), or of nothing.
    async fn add_layer(&self, parent: Option<&str>, hex: &str, tar: PathBuf) -> Result<(), Error> {
        match &self.mode {
            StorageMode::Zfs(dataset) => {
                let ds = format!("{dataset}/{hex}");
                let mnt = self.dir.join("mnt").join(hex);
                let sys = self.sys.clone();
                let parent_snap = parent.map(|p| format!("{dataset}/{p}@{SNAPSHOT}"));
                blocking(move || apply_layer(&*sys, &ds, parent_snap.as_deref(), &mnt, &tar)).await
            }
            StorageMode::Directory => {
                let dst = self.layer_dir(hex);
                let parent = parent.map(|p| self.layer_dir(p));
                blocking(move || {
                    // A directory left by an interrupted attempt is made again.
                    layer::remove_tree(&dst)?;
                    let made = match parent {
                        Some(p) => layer::copy_tree(&p, &dst),
                        None => std::fs::create_dir_all(&dst).map_err(Error::from),
                    }
                    .and_then(|()| layer::apply(&tar, &dst));
                    if made.is_err() {
                        let _ = layer::remove_tree(&dst);
                    }
                    made
                })
                .await
            }
        }
    }

    /// Records `image` as `r`, taking the name from any other image.
    fn upsert(&self, image: Image, r: &Reference, private: Option<&str>) -> Result<Image, Error> {
        let mut images = self.images()?;
        let name = r.to_string();
        for i in images.iter_mut().filter(|i| i.id != image.id) {
            i.references.retain(|n| *n != name);
            i.private.remove(&name);
        }
        let i = match images.iter().position(|i| i.id == image.id) {
            Some(i) => {
                if !image.layer_descriptors.is_empty() {
                    images[i].layer_descriptors = image.layer_descriptors;
                }
                i
            }
            None => {
                images.push(image);
                images.len() - 1
            }
        };
        let i = &mut images[i];
        if !i.references.contains(&name) {
            i.references.push(name.clone());
        }
        match private {
            Some(user) => {
                i.private.entry(name).or_default().insert(user.to_owned());
            }
            None => {
                i.private.remove(&name);
            }
        }
        let image = i.clone();
        images.retain(|i| !i.references.is_empty());
        self.save(&images)?;
        Ok(image)
    }

    /// Adds layer `chain_hex`, built on the node as the uncompressed
    /// `tar` and its gzip blob `gz`, on top of layer `parent`, and keeps the
    /// blob. Returns the layer's descriptor for a manifest.
    pub async fn commit_built_layer(
        &self,
        pin: &Pin,
        parent: Option<&str>,
        chain_hex: &str,
        tar: &Path,
        gz: &Path,
        artifact: &layer::LayerArtifact,
    ) -> Result<Descriptor, Error> {
        pin.add(&[chain_hex, hex_of(&artifact.blob_digest)?])?;
        let kept = self.blob_dest(&artifact.blob_digest)?;
        if !kept.exists() {
            std::fs::copy(gz, &kept)?;
        }
        if !self.has_layer(chain_hex)? {
            self.add_layer(parent, chain_hex, tar.to_owned()).await?;
        }
        Ok(Descriptor {
            media_type: OCI_LAYER_GZIP.to_owned(),
            digest: artifact.blob_digest.clone(),
            size: artifact.blob_size,
            platform: None,
            annotations: BTreeMap::new(),
        })
    }

    /// Records an image built on the node as `reference`: writes its config
    /// and manifest blobs and adds it to the index.
    pub async fn register_built_image(
        &self,
        pin: &Pin,
        reference: &Reference,
        config: ImageConfig,
        layers: Vec<String>,
        layer_descriptors: Vec<Descriptor>,
    ) -> Result<Image, Error> {
        let config_bytes = serde_json::to_vec_pretty(&config)?;
        let config_digest = sha256(&config_bytes);
        pin.add(&[hex_of(&config_digest)?])?;
        std::fs::write(self.blob_dest(&config_digest)?, &config_bytes)?;
        let manifest = Manifest::new(
            Descriptor {
                media_type: OCI_CONFIG.to_owned(),
                digest: config_digest.clone(),
                size: config_bytes.len() as u64,
                platform: None,
                annotations: BTreeMap::new(),
            },
            layer_descriptors.clone(),
        );
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_digest = sha256(&manifest_bytes);
        pin.add(&[hex_of(&manifest_digest)?])?;
        std::fs::write(self.blob_dest(&manifest_digest)?, &manifest_bytes)?;
        let image = Image {
            id: config_digest,
            digest: manifest_digest,
            references: vec![reference.to_string()],
            layers,
            config,
            layer_descriptors,
            private: BTreeMap::new(),
        };
        let _held = self.lock().await?;
        self.upsert(image, reference, None)
    }

    /// Starts a pin: layers and blobs added to it survive prunes, and so a
    /// concurrent `remove`, until it is dropped.
    pub fn pin(&self) -> Result<Pin, Error> {
        let dir = self.dir.join("pins");
        std::fs::create_dir_all(&dir)?;
        let (file, path) = tempfile::Builder::new()
            .prefix("pin")
            .tempfile_in(&dir)?
            .keep()
            .map_err(|e| e.error)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: flock on a descriptor we own; released when it closes.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(Pin { path, file })
    }

    /// The image [`Self::find`] finds for `query`, its layers and blobs
    /// pinned, atomically with respect to `remove`.
    pub async fn pin_image(&self, pin: &Pin, query: &str) -> Result<Option<Image>, Error> {
        let _held = self.lock().await?;
        let Some(image) = self.find(query)? else {
            return Ok(None);
        };
        let mut names: Vec<&str> = image.layers.iter().map(String::as_str).collect();
        for d in [&image.id, &image.digest]
            .into_iter()
            .chain(image.layer_descriptors.iter().map(|d| &d.digest))
        {
            names.extend(hex_of(d).ok());
        }
        pin.add(&names)?;
        Ok(Some(image))
    }

    /// Writes the image [`Self::find`] finds for `query` as an OCI archive
    /// at `out`, as `legion image load` and `podman load` take it, named
    /// `name` or by its first reference. Docker's `manifest.json` is in it
    /// too, as `docker save` writes it, for `docker load`. What it reads is
    /// pinned while it runs.
    pub async fn export(
        &self,
        query: &str,
        out: &Path,
        name: Option<&str>,
    ) -> Result<Image, Error> {
        let pin = self.pin()?;
        let mut image = self
            .pin_image(&pin, query)
            .await?
            .ok_or_else(|| Error::NotFound(format!("{query}: image not found")))?;
        let name = match name {
            Some(n) => Reference::parse_local_tag(n)?.to_string(),
            None => image
                .references
                .first()
                .cloned()
                .unwrap_or_else(|| query.to_owned()),
        };
        let (manifest, config_bytes) = self.ensure_blobs(&mut image, &pin)?;
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_digest = sha256(&manifest_bytes);

        let tmp = tempfile::Builder::new()
            .prefix("export")
            .tempdir_in(self.dir.join("tmp"))?;
        let blobs = tmp.path().join("blobs").join("sha256");
        std::fs::create_dir_all(&blobs)?;
        std::fs::write(
            tmp.path().join("oci-layout"),
            r#"{"imageLayoutVersion":"1.0.0"}"#,
        )?;
        std::fs::write(blobs.join(hex_of(&manifest.config.digest)?), &config_bytes)?;
        std::fs::write(blobs.join(hex_of(&manifest_digest)?), &manifest_bytes)?;
        for d in &manifest.layers {
            let to = blobs.join(hex_of(&d.digest)?);
            if !to.exists() {
                std::fs::copy(self.blob_path(&d.digest)?, &to)?;
            }
        }
        let layer_paths = manifest
            .layers
            .iter()
            .map(|d| Ok(format!("blobs/sha256/{}", hex_of(&d.digest)?)))
            .collect::<Result<Vec<_>, Error>>()?;
        let repo_tags: Vec<&str> = Some(name.as_str())
            .filter(|n| !n.contains('@'))
            .into_iter()
            .collect();
        std::fs::write(
            tmp.path().join("manifest.json"),
            serde_json::to_vec(&serde_json::json!([{
                "Config": format!("blobs/sha256/{}", hex_of(&manifest.config.digest)?),
                "RepoTags": repo_tags,
                "Layers": layer_paths,
            }]))?,
        )?;
        let index = Document {
            schema_version: 2,
            media_type: OCI_INDEX.to_owned(),
            manifests: Some(vec![Descriptor {
                media_type: OCI_MANIFEST.to_owned(),
                digest: manifest_digest,
                size: manifest_bytes.len() as u64,
                platform: Some(platform_of(&image.config)),
                annotations: BTreeMap::from([(
                    "org.opencontainers.image.ref.name".to_owned(),
                    name,
                )]),
            }]),
            config: None,
            layers: Vec::new(),
            annotations: BTreeMap::new(),
        };
        std::fs::write(
            tmp.path().join("index.json"),
            serde_json::to_vec_pretty(&index)?,
        )?;
        if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let mut archive = tar::Builder::new(File::create(out)?);
        archive.append_dir_all(".", tmp.path())?;
        archive.finish()?;
        Ok(image)
    }

    /// Pushes the image [`Self::find`] finds for `query` to `target`,
    /// returning the manifest's digest.
    pub async fn push(
        &self,
        query: &str,
        target: &Reference,
        creds: Option<&Credentials>,
    ) -> Result<String, Error> {
        refuse_local(target)?;
        let (manifest, _) = self.push_blobs(query, target, creds).await?;
        self.client.push_manifest(target, &manifest, creds).await
    }

    /// Pushes an image index to `target` naming one image per platform.
    /// Each source is a local image, pushed by digest into the target's
    /// repository, or an image or index already in that repository.
    pub async fn push_index(
        &self,
        target: &Reference,
        sources: &[String],
        creds: Option<&Credentials>,
    ) -> Result<String, Error> {
        refuse_local(target)?;
        let mut entries: Vec<Descriptor> = Vec::new();
        for src in sources {
            let found = if self.find(src)?.is_some() {
                let (manifest, platform) = self.push_blobs(src, target, creds).await?;
                let bytes = serde_json::to_vec_pretty(&manifest)?;
                let by_digest = Reference {
                    tag: None,
                    digest: Some(sha256(&bytes)),
                    ..target.clone()
                };
                let digest = self
                    .client
                    .push_manifest(&by_digest, &manifest, creds)
                    .await?;
                vec![Descriptor {
                    media_type: OCI_MANIFEST.to_owned(),
                    digest,
                    size: bytes.len() as u64,
                    platform: Some(platform),
                    annotations: BTreeMap::new(),
                }]
            } else {
                let r = Reference::parse(src)?;
                if r.registry != target.registry || r.repository != target.repository {
                    return Err(Error::Reference(format!(
                        "{src} is neither a local image nor in {}/{}: push it there first",
                        target.registry, target.repository
                    )));
                }
                self.client.describe(&r, creds).await?
            };
            for d in found {
                let p = d.platform.as_ref().ok_or_else(|| {
                    Error::Unsupported(format!("{src}: an index entry without a platform"))
                })?;
                if entries.iter().any(|e| e.platform == d.platform) {
                    return Err(Error::Unsupported(format!(
                        "two images for {}/{}: give one per platform",
                        p.os, p.architecture
                    )));
                }
                entries.push(d);
            }
        }
        let index = Document {
            schema_version: 2,
            media_type: OCI_INDEX.to_owned(),
            manifests: Some(entries),
            config: None,
            layers: Vec::new(),
            annotations: BTreeMap::new(),
        };
        self.client
            .push_document(target, serde_json::to_vec_pretty(&index)?, OCI_INDEX, creds)
            .await
    }

    /// Uploads the layer and config blobs of the image `query` finds to
    /// `target`'s repository, returning its manifest and platform. They are
    /// pinned while it runs.
    async fn push_blobs(
        &self,
        query: &str,
        target: &Reference,
        creds: Option<&Credentials>,
    ) -> Result<(Manifest, Platform), Error> {
        let pin = self.pin()?;
        let mut image = self
            .pin_image(&pin, query)
            .await?
            .ok_or_else(|| Error::NotFound(format!("{query}: image not found")))?;
        let (manifest, _) = self.ensure_blobs(&mut image, &pin)?;
        for d in manifest.layers.iter().chain([&manifest.config]) {
            let uploaded = self
                .client
                .push_blob(target, &d.digest, &self.blob_path(&d.digest)?, creds)
                .await?;
            tracing::info!(digest = %d.digest, uploaded, "blob pushed");
        }
        Ok((manifest, platform_of(&image.config)))
    }

    /// Makes sure the store has `image`'s layer and config blobs and
    /// returns its manifest and config. Where a layer blob is not kept, the
    /// image's files are packed again as one layer (a layer's files are the
    /// whole image up to it, not what it changed), which changes the
    /// config, and so the image's digest. What it writes, no image names:
    /// `pin` keeps it, as it keeps the image's own.
    fn ensure_blobs(&self, image: &mut Image, pin: &Pin) -> Result<(Manifest, Vec<u8>), Error> {
        let mut kept = image.layer_descriptors.len() == image.layers.len();
        for d in &image.layer_descriptors {
            kept &= self.blob_path(&d.digest)?.exists();
        }
        if !kept {
            let top = image
                .layers
                .last()
                .ok_or_else(|| Error::Unsupported("an image without layers".to_owned()))?;
            let tmp = tempfile::Builder::new()
                .prefix("pack")
                .tempdir_in(self.dir.join("tmp"))?;
            let (tar, gz) = (
                tmp.path().join("layer.tar"),
                tmp.path().join("layer.tar.gz"),
            );
            let art = self.with_layer_mounted(top, |root| layer::pack(root, &tar, &gz))?;
            pin.add(&[hex_of(&art.blob_digest)?])?;
            put_blob(&gz, &self.blob_dest(&art.blob_digest)?)?;
            image.config.rootfs.diff_ids = vec![art.diff_id];
            for h in &mut image.config.history {
                h.empty_layer = true;
            }
            image.config.history.push(oci::History {
                created_by: "the image's layers, packed again as one".to_owned(),
                ..Default::default()
            });
            image.layer_descriptors = vec![Descriptor {
                media_type: OCI_LAYER_GZIP.to_owned(),
                digest: art.blob_digest,
                size: art.blob_size,
                platform: None,
                annotations: BTreeMap::new(),
            }];
        }
        let descriptors = image.layer_descriptors.clone();
        let config_bytes = serde_json::to_vec_pretty(&image.config)?;
        let config_digest = sha256(&config_bytes);
        pin.add(&[hex_of(&config_digest)?])?;
        let kept = self.blob_dest(&config_digest)?;
        if !kept.exists() {
            let tmp = tempfile::NamedTempFile::new_in(self.dir.join("tmp"))?;
            std::fs::write(tmp.path(), &config_bytes)?;
            put_blob(tmp.path(), &kept)?;
        }
        let manifest = Manifest::new(
            Descriptor {
                media_type: OCI_CONFIG.to_owned(),
                digest: config_digest,
                size: config_bytes.len() as u64,
                platform: None,
                annotations: BTreeMap::new(),
            },
            descriptors,
        );
        Ok((manifest, config_bytes))
    }

    /// Forgets `reference` and destroys the layers no image uses any more
    /// (those that containers still use stay until a later prune).
    pub async fn remove(&self, reference: &str) -> Result<bool, Error> {
        let name = Reference::parse(reference)?.to_string();
        let held = self.lock().await?;
        let mut images = self.images()?;
        let mut found = false;
        for i in &mut images {
            let before = i.references.len();
            i.references.retain(|n| *n != name);
            i.private.remove(&name);
            found |= i.references.len() != before;
        }
        images.retain(|i| !i.references.is_empty());
        self.save(&images)?;
        self.prune_locked(&held).await?;
        Ok(found)
    }

    /// Destroys layers that no image uses and no container was cloned
    /// from; returns their chain ids.
    pub async fn prune(&self) -> Result<Vec<String>, Error> {
        let held = self.lock().await?;
        self.prune_locked(&held).await
    }

    /// [`Self::prune`] while the caller holds the store's lock. Besides
    /// images, live [`Pin`]s and the GC roots keep what they name; so do
    /// they kept blobs.
    pub async fn prune_locked(&self, _held: &StoreLock<'_>) -> Result<Vec<String>, Error> {
        let images = self.images()?;
        let roots = match &self.roots {
            Some(f) => f(&self.dir)?,
            None => BTreeSet::new(),
        };
        self.prune_blobs(&images, &roots)?;
        let used: BTreeSet<String> = images
            .iter()
            .flat_map(|i| i.layers.clone())
            .chain(roots)
            .collect();
        let dir = self.dir.clone();
        match &self.mode {
            StorageMode::Zfs(root) => {
                let sys = self.sys.clone();
                let root = root.clone();
                blocking(move || {
                    let prefix = format!("{root}/");
                    // What might go is listed before the pins are read, as
                    // a pin names a layer before the layer is made.
                    let listed = sys.zfs_list(&root)?;
                    let pinned = live_pins(&dir)?;
                    let mut unused: Vec<String> = listed
                        .into_iter()
                        .filter_map(|d| d.name.strip_prefix(&prefix).map(str::to_owned))
                        .filter(|n| !n.contains('/') && !used.contains(n) && !pinned.contains(n))
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
            StorageMode::Directory => {
                blocking(move || {
                    let layers = dir.join("layers");
                    let mut gone = Vec::new();
                    let Ok(listed) = std::fs::read_dir(&layers) else {
                        return Ok(gone);
                    };
                    let listed = listed.collect::<Result<Vec<_>, _>>()?;
                    let pinned = live_pins(&dir)?;
                    for e in listed {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if !used.contains(&name) && !pinned.contains(&name) {
                            layer::remove_tree(&e.path())?;
                            gone.push(name);
                        }
                    }
                    Ok(gone)
                })
                .await
            }
        }
    }

    /// Removes kept blobs that no image, live pin or GC root names.
    fn prune_blobs(&self, images: &[Image], roots: &BTreeSet<String>) -> Result<(), Error> {
        let blobs = self.dir.join("blobs").join("sha256");
        let Ok(listed) = std::fs::read_dir(&blobs) else {
            return Ok(());
        };
        let listed = listed.collect::<Result<Vec<_>, _>>()?;
        let pinned = live_pins(&self.dir)?;
        let mut used = roots.clone();
        for i in images {
            for d in [&i.id, &i.digest]
                .into_iter()
                .chain(i.layer_descriptors.iter().map(|d| &d.digest))
            {
                used.extend(hex_of(d).ok().map(str::to_owned));
            }
        }
        for e in listed {
            let name = e.file_name().to_string_lossy().into_owned();
            if !used.contains(&name) && !pinned.contains(&name) {
                std::fs::remove_file(e.path())?;
            }
        }
        Ok(())
    }
}

/// Puts the file `from`, in the store's `tmp`, at the blob path `dest`,
/// unless the blob is there already: by renaming it, so that another
/// process reading the blob (a push or export of the same image) never
/// sees it half written.
fn put_blob(from: &Path, dest: &Path) -> Result<(), Error> {
    if !dest.exists() {
        std::fs::rename(from, dest)?;
    }
    Ok(())
}

fn refuse_local(target: &Reference) -> Result<(), Error> {
    if target.is_local() {
        return Err(Error::Reference(format!(
            "{target} is a localhost reference; give a registry destination (e.g. ghcr.io/me/app:tag)"
        )));
    }
    Ok(())
}

/// Layers and blobs unfinished work depends on, one name per line, in a
/// file under `<dir>/pins` locked while the work runs.
pub struct Pin {
    path: PathBuf,
    file: File,
}

impl Pin {
    /// Adds layer chain ids or blob hex digests to the pin. Names must be
    /// pinned before what they name is made.
    pub fn add(&self, names: &[&str]) -> Result<(), Error> {
        use std::io::Write;
        let mut text = String::new();
        for n in names {
            text.push_str(n);
            text.push('\n');
        }
        (&self.file).write_all(text.as_bytes())?;
        Ok(())
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Names pinned by live pins. Pins whose work died (their lock is free) are
/// removed. Callers list what they might delete before calling this.
fn live_pins(dir: &Path) -> Result<BTreeSet<String>, Error> {
    let mut names = BTreeSet::new();
    let Ok(listed) = std::fs::read_dir(dir.join("pins")) else {
        return Ok(names);
    };
    for e in listed {
        let path = e?.path();
        let Ok(file) = File::open(&path) else {
            continue;
        };
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: flock on a descriptor we own; released when it closes.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                let _ = std::fs::remove_file(&path);
                continue;
            }
        }
        drop(file);
        if let Ok(text) = std::fs::read_to_string(&path) {
            names.extend(text.lines().map(str::to_owned));
        }
    }
    Ok(names)
}

/// The store's lock ([`Store::lock`]), held while this lives.
pub struct StoreLock<'a> {
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
