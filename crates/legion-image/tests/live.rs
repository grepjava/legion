//! The image store on real ZFS. Ignored by default: needs root, and
//! `LEGION_IMAGE_ZFS_ROOT` naming a dataset it may create children under
//! (a throwaway pool). `pulls_from_docker_hub` also needs the network.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use legion_image::{Client, Store};
use legion_sys::{Shell, Sys};
use serde_json::json;
use sha2::{Digest, Sha256};

fn sh(cmd: &str) -> String {
    let out = Command::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    assert!(
        out.status.success(),
        "{cmd}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn root() -> Option<String> {
    let r = std::env::var("LEGION_IMAGE_ZFS_ROOT").ok();
    if r.is_none() {
        eprintln!("LEGION_IMAGE_ZFS_ROOT not set; skipping");
    }
    r
}

/// A clone of `snapshot` mounted at `mnt`, for looking inside.
fn clone(sys: &Shell, snapshot: &str, ds: &str, mnt: &Path) {
    std::fs::create_dir_all(mnt).unwrap();
    sys.zfs_clone(snapshot, ds, &[("mountpoint", &mnt.to_string_lossy())])
        .unwrap();
}

#[tokio::test]
#[ignore = "needs root, LEGION_IMAGE_ZFS_ROOT and the network"]
async fn pulls_from_docker_hub() {
    let Some(root) = root() else { return };
    let base = format!("{root}/limg{}", std::process::id());
    let dir = tempfile::tempdir_in("/var/tmp").unwrap();
    let sys = Arc::new(Shell::new());
    let store = Store::open(
        sys.clone() as Arc<dyn Sys>,
        &format!("{base}/images"),
        dir.path(),
        Client::new().unwrap(),
    )
    .unwrap();
    let started = std::time::Instant::now();
    let image = store
        .pull("freebsd/freebsd-runtime:15.1", None, "t")
        .await
        .unwrap();
    eprintln!("pulled in {:?}: {image:?}", started.elapsed());
    assert_eq!(image.references, ["docker.io/freebsd/freebsd-runtime:15.1"]);
    assert_eq!(image.config.config.cmd, ["/bin/sh"]);
    let top = store.snapshot(&image).unwrap();

    sys.zfs_create(&format!("{base}/ctr"), &[("mountpoint", "none")])
        .unwrap();
    let mnt = dir.path().join("c1");
    clone(&sys, &top, &format!("{base}/ctr/c1"), &mnt);
    assert!(mnt.join("bin/sh").exists());
    // File flags and set-uid bits came through.
    let flagged = sh(&format!("find {} -flags +schg | head -1", mnt.display()));
    assert!(!flagged.trim().is_empty(), "no schg files");
    let setuid = sh(&format!("find {} -perm -4000 | head -1", mnt.display()));
    assert!(!setuid.trim().is_empty(), "no setuid files");
    // Owners by number: root's files are 0:0.
    assert_eq!(
        sh(&format!("stat -f %u:%g {}/bin/sh", mnt.display())).trim(),
        "0:0"
    );

    // In use by the clone, the layers outlive the image.
    assert!(store.remove("freebsd/freebsd-runtime:15.1").await.unwrap());
    assert!(
        !sys.zfs_list(&format!("{base}/images/{}", image.layers[0]))
            .unwrap()
            .is_empty()
    );
    sys.zfs_destroy(&format!("{base}/ctr/c1"), false).unwrap();
    assert_eq!(store.prune().await.unwrap(), image.layers);
    sys.zfs_destroy(&base, true).unwrap();
}

fn digest(b: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(b))
}

/// An uncompressed layer: files with contents and file flags, and
/// whiteouts (names starting `.wh.`).
fn layer(files: &[(&str, &str, Option<&str>)]) -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    for (p, data, flags) in files {
        if let Some(f) = flags {
            b.append_pax_extensions([("SCHILY.fflags", f.as_bytes())])
                .unwrap();
        }
        let mut h = tar::Header::new_ustar();
        h.set_mode(0o644);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, p, data.as_bytes()).unwrap();
    }
    b.into_inner().unwrap()
}

#[tokio::test]
#[ignore = "needs root and LEGION_IMAGE_ZFS_ROOT"]
async fn whiteouts_and_replacements_of_flagged_files() {
    let Some(root) = root() else { return };
    let base = format!("{root}/limw{}", std::process::id());
    let layout = tempfile::tempdir_in("/var/tmp").unwrap();
    let blobs = layout.path().join("blobs/sha256");
    std::fs::create_dir_all(&blobs).unwrap();
    let put = |data: &[u8]| {
        let d = digest(data);
        std::fs::write(blobs.join(&d[7..]), data).unwrap();
        d
    };
    let lower = layer(&[
        ("etc/gone", "g", Some("schg")),
        ("etc/replaced", "old", Some("schg")),
        ("etc/kept", "k", Some("uarch")),
    ]);
    let upper = layer(&[("etc/.wh.gone", "", None), ("etc/replaced", "new", None)]);
    let diff_ids = [digest(&lower), digest(&upper)];
    let cfg = put(
        &serde_json::to_vec(&json!({"os": "freebsd", "rootfs": {"diff_ids": diff_ids}})).unwrap(),
    );
    let layers: Vec<_> = [&lower, &upper]
        .iter()
        .map(|l| json!({"digest": put(l)}))
        .collect();
    let manifest = put(&serde_json::to_vec(
        &json!({"schemaVersion": 2, "config": {"digest": cfg}, "layers": layers}),
    )
    .unwrap());
    std::fs::write(
        layout.path().join("index.json"),
        serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [{"digest": manifest}]}))
            .unwrap(),
    )
    .unwrap();

    let dir = tempfile::tempdir_in("/var/tmp").unwrap();
    let sys = Arc::new(Shell::new());
    let store = Store::open(
        sys.clone() as Arc<dyn Sys>,
        &format!("{base}/images"),
        dir.path(),
        Client::new().unwrap(),
    )
    .unwrap();
    let image = store
        .import(layout.path(), "localhost/flags:1")
        .await
        .unwrap();
    sys.zfs_create(&format!("{base}/ctr"), &[("mountpoint", "none")])
        .unwrap();
    let mnt = dir.path().join("c1");
    clone(
        &sys,
        &store.snapshot(&image).unwrap(),
        &format!("{base}/ctr/c1"),
        &mnt,
    );
    assert!(!mnt.join("etc/gone").exists());
    assert_eq!(
        std::fs::read_to_string(mnt.join("etc/replaced")).unwrap(),
        "new"
    );
    assert_eq!(std::fs::read_to_string(mnt.join("etc/kept")).unwrap(), "k");
    let flags = sh(&format!("ls -lo {}/etc", mnt.display()));
    assert!(
        flags
            .lines()
            .any(|l| l.ends_with("kept") && l.contains("uarch")),
        "{flags}"
    );
    sys.zfs_destroy(&format!("{base}/ctr/c1"), false).unwrap();
    store.remove("localhost/flags:1").await.unwrap();
    sys.zfs_destroy(&base, true).unwrap();
}

/// An OCI layout of a two-layer image whose upper layer removes a `schg`
/// file and replaces another, as `whiteouts_and_replacements_of_flagged_files`
/// imports it.
fn flagged_layout() -> tempfile::TempDir {
    let layout = tempfile::tempdir_in("/var/tmp").unwrap();
    let blobs = layout.path().join("blobs/sha256");
    std::fs::create_dir_all(&blobs).unwrap();
    let put = |data: &[u8]| {
        let d = digest(data);
        std::fs::write(blobs.join(&d[7..]), data).unwrap();
        d
    };
    let lower = layer(&[
        ("etc/gone", "g", Some("schg")),
        ("etc/replaced", "old", Some("schg")),
        ("etc/kept", "k", Some("uarch")),
    ]);
    let upper = layer(&[("etc/.wh.gone", "", None), ("etc/replaced", "new", None)]);
    let diff_ids = [digest(&lower), digest(&upper)];
    let cfg = put(
        &serde_json::to_vec(&json!({"os": "freebsd", "rootfs": {"diff_ids": diff_ids}})).unwrap(),
    );
    let layers: Vec<_> = [&lower, &upper]
        .iter()
        .map(|l| json!({"digest": put(l)}))
        .collect();
    let manifest = put(&serde_json::to_vec(
        &json!({"schemaVersion": 2, "config": {"digest": cfg}, "layers": layers}),
    )
    .unwrap());
    std::fs::write(
        layout.path().join("index.json"),
        serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [{"digest": manifest}]}))
            .unwrap(),
    )
    .unwrap();
    layout
}

#[tokio::test]
#[ignore = "needs root and LEGION_IMAGE_ZFS_ROOT"]
async fn rootfs_copies_built_layers_and_exports_on_zfs() {
    let Some(root) = root() else { return };
    let base = format!("{root}/limx{}", std::process::id());
    let layout = flagged_layout();
    let dir = tempfile::tempdir_in("/var/tmp").unwrap();
    let sys = Arc::new(Shell::new());
    let store = Store::open(
        sys.clone() as Arc<dyn Sys>,
        &format!("{base}/images"),
        dir.path(),
        Client::new().unwrap(),
    )
    .unwrap();
    let image = store
        .import(layout.path(), "localhost/flags:1")
        .await
        .unwrap();
    let flags_of = |root: &Path| sh(&format!("ls -lo {}/etc", root.display()));
    let check = |root: &Path| {
        assert!(!root.join("etc/gone").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("etc/replaced")).unwrap(),
            "new"
        );
        let flags = flags_of(root);
        assert!(
            flags
                .lines()
                .any(|l| l.ends_with("kept") && l.contains("uarch")),
            "{flags}"
        );
    };

    // A container's root as a clone, and as a plain copy.
    let cloned = dir.path().join("cloned");
    let ds = format!("{base}/ctr/c1");
    store.prepare_rootfs(&image, &cloned, Some(&ds)).unwrap();
    check(&cloned);
    store.cleanup_rootfs(&cloned, Some(&ds)).unwrap();
    assert!(sys.zfs_list(&ds).unwrap().is_empty());
    let copied = dir.path().join("copied");
    store.prepare_rootfs(&image, &copied, None).unwrap();
    check(&copied);

    // A layer built from that copy, committed on top of the image.
    std::fs::write(copied.join("etc/built"), "b").unwrap();
    let work = tempfile::tempdir_in("/var/tmp").unwrap();
    let (tar, gz) = (work.path().join("l.tar"), work.path().join("l.tar.gz"));
    let art = legion_image::layer::pack(&copied, &tar, &gz).unwrap();
    let mut diff_ids = image.config.rootfs.diff_ids.clone();
    diff_ids.push(art.diff_id.clone());
    let chain = legion_image::chain_ids(&diff_ids);
    let hex = legion_image::hex_of(chain.last().unwrap())
        .unwrap()
        .to_owned();
    let pin = store.pin().unwrap();
    store
        .commit_built_layer(
            &pin,
            image.layers.last().map(String::as_str),
            &hex,
            &tar,
            &gz,
            &art,
        )
        .await
        .unwrap();
    assert!(store.has_layer(&hex).unwrap());
    drop(pin);
    store.cleanup_rootfs(&copied, None).unwrap();

    // Exported without kept blobs, the image's files are packed again as
    // one layer, flags and whiteouts as they were.
    let archive = dir.path().join("flags.tar");
    store
        .export("localhost/flags:1", &archive, None)
        .await
        .unwrap();
    let other_dir = tempfile::tempdir_in("/var/tmp").unwrap();
    let other = Store::open_directory(
        sys.clone() as Arc<dyn Sys>,
        other_dir.path(),
        Client::new().unwrap(),
    )
    .unwrap()
    .keep_blobs();
    let loaded = other.load(&archive, None).await.unwrap();
    assert_eq!(loaded.layers.len(), 1);
    let out = dir.path().join("loaded");
    other.prepare_rootfs(&loaded, &out, None).unwrap();
    check(&out);
    other.cleanup_rootfs(&out, None).unwrap();

    store.remove("localhost/flags:1").await.unwrap();
    assert!(
        !store.has_layer(&hex).unwrap(),
        "the built layer, unpinned, goes"
    );
    sys.zfs_destroy(&base, true).unwrap();
}
