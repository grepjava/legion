//! Build bhyve VM OCI images (`legion build vm`).
//!
//! A Legion VM image is a standard OCI image whose root filesystem holds
//! `/disk.img` (see `legion_jail::vm`). This module packages either an
//! existing raw disk image (`--disk`) or a FreeBSD rootfs directory (`--rootfs`,
//! turned into a UFS image via `makefs`) into an OCI image in the local store.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::layer::{self, Compression};
use crate::oci::{self, History, ImageConfig};
use crate::store::{Image, Store, chain_ids, hex_of};
use crate::{Error, Reference};

/// Options for building a bhyve VM OCI image.
#[derive(Debug, Clone)]
pub struct VmBuildOptions {
    pub tag: String,
    pub disk: Option<PathBuf>,
    pub rootfs: Option<PathBuf>,
    /// Disk image size in bytes for `makefs -s` when building from `rootfs`.
    pub size: Option<u64>,
    pub compression: Compression,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub timestamp: Option<i64>,
}

/// Builds a bhyve VM OCI image containing `/disk.img` and registers it in `store`.
pub async fn build_vm(store: &Store, opts: VmBuildOptions) -> Result<Image, Error> {
    if opts.disk.is_some() && opts.rootfs.is_some() {
        return Err(Error::Build(
            "give either --disk or --rootfs, not both".to_owned(),
        ));
    }
    if opts.disk.is_none() && opts.rootfs.is_none() {
        return Err(Error::Build(
            "give --disk <PATH> or --rootfs <DIR> to build a VM image".to_owned(),
        ));
    }
    if opts.size.is_some() && opts.rootfs.is_none() {
        return Err(Error::Build(
            "--size applies only when building from --rootfs".to_owned(),
        ));
    }

    std::fs::create_dir_all(store.dir().join("tmp"))?;
    let build_tmp = tempfile::Builder::new()
        .prefix("vm-build-")
        .tempdir_in(store.dir().join("tmp"))?;
    let root = build_tmp.path().join("root");
    std::fs::create_dir_all(&root)?;
    let disk_dest = root.join("disk.img");

    if let Some(disk_src) = &opts.disk {
        if !disk_src.is_file() {
            return Err(Error::Build(format!(
                "--disk {}: not a file",
                disk_src.display()
            )));
        }
        // A link, not a copy: a disk is large, and the layer reads it once.
        if std::fs::hard_link(disk_src, &disk_dest).is_err() {
            std::fs::copy(disk_src, &disk_dest)?;
        }
    } else if let Some(rootfs_dir) = &opts.rootfs {
        if !rootfs_dir.is_dir() {
            return Err(Error::Build(format!(
                "--rootfs {}: not a directory",
                rootfs_dir.display()
            )));
        }
        let mut cmd = tokio::process::Command::new("makefs");
        cmd.args(["-t", "ffs", "-o", "version=2"]);
        if let Some(bytes) = opts.size {
            cmd.arg("-s").arg(bytes.to_string());
        }
        cmd.arg(&disk_dest).arg(rootfs_dir);
        let out = cmd.output().await.map_err(|e| {
            Error::Build(format!(
                "running makefs to build {}: {e}",
                disk_dest.display()
            ))
        })?;
        if !out.status.success() {
            return Err(Error::Build(format!(
                "makefs failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
    }

    let tar_out = build_tmp.path().join("vm.tar");
    let compressed_out = build_tmp.path().join("vm.tar.compressed");
    let art = layer::pack_directory_layer(&root, &tar_out, &compressed_out, opts.compression)?;

    let pin = store.pin()?;
    let mut config = ImageConfig {
        architecture: oci::arch().to_owned(),
        os: "freebsd".to_owned(),
        ..Default::default()
    };
    config.rootfs.kind = "layers".to_owned();
    config.rootfs.diff_ids.push(art.diff_id.clone());

    let chains = chain_ids(&config.rootfs.diff_ids);
    let chain_hex = hex_of(chains.last().unwrap())?.to_owned();
    let mut desc = store
        .commit_built_layer(&pin, None, &chain_hex, &tar_out, &compressed_out, &art)
        .await?;
    desc.media_type = opts.compression.media_type().to_owned();

    config
        .config
        .labels
        .insert("legion.vm.image".to_owned(), "true".to_owned());
    config.config.labels.extend(opts.labels.clone());
    let created = opts
        .timestamp
        .map(legion_api::clock::format)
        .unwrap_or_default();
    config.created.clone_from(&created);
    config.history.push(History {
        created,
        created_by: "legion build vm".to_owned(),
        empty_layer: false,
    });

    let target_ref = Reference::parse_local_tag(&opts.tag)?;
    Ok(store
        .register_built_image_with(
            &pin,
            &target_ref,
            config,
            vec![chain_hex],
            vec![desc],
            opts.annotations,
        )
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn builds_vm_image_from_disk_file() {
        let dir = tempfile::tempdir().unwrap();
        let disk = dir.path().join("guest.img");
        std::fs::write(&disk, b"fake-disk-image").unwrap();

        let store = crate::store::open(
            Arc::new(legion_sys::FakeSys::new()),
            None,
            &dir.path().join("store"),
            crate::Client::new().unwrap(),
        )
        .unwrap();

        let image = build_vm(
            &store,
            VmBuildOptions {
                tag: "localhost/myvm:1".to_owned(),
                disk: Some(disk),
                rootfs: None,
                size: None,
                compression: Compression::Gzip,
                labels: BTreeMap::from([("app".to_owned(), "vm".to_owned())]),
                annotations: BTreeMap::new(),
                timestamp: Some(1_700_000_000),
            },
        )
        .await
        .unwrap();

        assert!(image.references.iter().any(|r| r == "localhost/myvm:1"));
        assert_eq!(image.layers.len(), 1);
        assert_eq!(
            image
                .config
                .config
                .labels
                .get("legion.vm.image")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            image.config.config.labels.get("app").map(String::as_str),
            Some("vm")
        );
    }

    #[tokio::test]
    async fn rejects_missing_or_conflicting_vm_sources() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::open(
            Arc::new(legion_sys::FakeSys::new()),
            None,
            &dir.path().join("store"),
            crate::Client::new().unwrap(),
        )
        .unwrap();

        let err = build_vm(
            &store,
            VmBuildOptions {
                tag: "localhost/myvm:1".to_owned(),
                disk: None,
                rootfs: None,
                size: None,
                compression: Compression::Gzip,
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                timestamp: None,
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--disk"), "{err}");
    }
}
