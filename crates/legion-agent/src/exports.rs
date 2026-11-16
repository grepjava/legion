//! What this node exports for the cluster (DESIGN.md §6.7): the NFS
//! shares and iSCSI LUNs of the Volumes it provides (`nfs: { node }`,
//! `iscsi: { node }`). The leader sends the whole list now and then
//! (`ExportSync`), and the node makes its exports exactly that: each
//! Volume's ZFS dataset under `<zfs root>/exports/<namespace>/<name>`,
//! shared over NFS with the cluster's nodes (`sharenfs`), or its ZFS
//! volume, formatted UFS when first made, a target in the file of targets
//! ctld includes (`--iscsi-targets`), ctld reloaded when it changes. What
//! is there and no longer listed stops being exported; its data stays, as
//! a deleted ZFS Volume's does.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use legion_api::quantity::Bytes;
use legion_api::volume::VolumeSpec;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::{ExportSync, SessionFrame};
use legion_sys::Sys;
use serde::Serialize;
use tokio::sync::{Mutex, mpsc};
use tonic::Status;

/// How this node exports iSCSI LUNs.
#[derive(Debug, Clone, Default)]
pub struct IscsiConfig {
    /// The file of targets Legion writes, which ctld's own configuration
    /// includes (UCL, `.include`); none: no LUNs are exported here.
    pub targets: Option<PathBuf>,
    /// The portal group, of ctld's configuration, the targets are on.
    pub portal_group: String,
    /// `host[:port]` the nodes reach it at; by default this node's address.
    pub portal: Option<String>,
}

/// The default portal group's name.
pub const PORTAL_GROUP: &str = "legion";

/// The start of every target Legion makes: `<namespace>.<name>` follows.
pub const TARGET_PREFIX: &str = "iqn.2026-11.invalid.legion:";

/// What the node says of each export it was asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Said {
    pub namespace: String,
    pub volume: String,
    /// The shared directory, for NFS.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// The target, for iSCSI, and where it is reached if configured.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub portal: String,
    /// Why it isn't exported.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// A manager's session making this node's exports so: what it says of
/// each as JSON on `stdout`, then `exit_code` 0.
pub(crate) async fn sync(
    v: &crate::transfer::Volumes,
    iscsi: &IscsiConfig,
    opts: ExportSync,
    tx: &mpsc::Sender<SessionFrame>,
) -> Result<(), Status> {
    let root = v.zfs_root.clone().ok_or_else(|| {
        Status::failed_precondition("this node has no ZFS dataset to export volumes from")
    })?;
    let (lock, iscsi) = (v.lock.clone(), iscsi.clone());
    let said =
        crate::task::blocking(&v.sys, move |sys| apply(sys, &lock, &root, &iscsi, &opts)).await;
    let json = serde_json::to_vec(&said).map_err(|e| Status::internal(e.to_string()))?;
    for f in [Frame::Stdout(json), Frame::ExitCode(0)] {
        let _ = tx.send(SessionFrame { frame: Some(f) }).await;
    }
    Ok(())
}

/// Makes the exports what `opts` lists; what it says of each.
pub(crate) fn apply(
    sys: &dyn Sys,
    lock: &Mutex<()>,
    root: &str,
    iscsi: &IscsiConfig,
    opts: &ExportSync,
) -> Vec<Said> {
    let base = format!("{root}/exports");
    let _guard = lock.blocking_lock();
    let mut said = Vec::new();
    let mut luns: Vec<(String, String)> = Vec::new();
    let mut wanted = BTreeSet::new();
    for w in &opts.exports {
        let ds = format!("{base}/{}/{}", w.namespace, w.volume);
        wanted.insert(ds.clone());
        let mut s = Said {
            namespace: w.namespace.clone(),
            volume: w.volume.clone(),
            ..Default::default()
        };
        let size = (w.size > 0).then_some(w.size);
        match w.kind.as_str() {
            "nfs" => match share(sys, &ds, size, &opts.clients) {
                Ok(path) => s.path = path,
                Err(e) => s.message = e,
            },
            "iscsi" if iscsi.targets.is_none() => {
                s.message = "this node exports no iSCSI LUNs: start it with --iscsi-targets".into();
            }
            "iscsi" => match lun(sys, &ds, size.unwrap_or(0)) {
                Ok(device) => {
                    s.target = target(&w.namespace, &w.volume);
                    s.portal = iscsi.portal.clone().unwrap_or_default();
                    luns.push((s.target.clone(), device));
                }
                Err(e) => s.message = e,
            },
            other => s.message = format!("no such export: {other}"),
        }
        said.push(s);
    }
    // No longer listed: not shared any more, its data kept.
    if let Ok(found) = sys.zfs_list(&base) {
        for d in found {
            let depth = d
                .name
                .strip_prefix(&format!("{base}/"))
                .map(|r| r.split('/').count());
            if depth != Some(2) || wanted.contains(&d.name) {
                continue;
            }
            if sys
                .zfs_get(&d.name, "sharenfs")
                .ok()
                .flatten()
                .is_some_and(|v| v != "off" && v != "-")
                && let Err(e) = sys.zfs_set(&d.name, "sharenfs", "off")
            {
                tracing::warn!(dataset = %d.name, "no longer sharing it: {e}");
            }
        }
    }
    if let Some(file) = &iscsi.targets
        && let Err(e) = write_targets(sys, file, &iscsi.portal_group, &luns)
    {
        for s in said.iter_mut().filter(|s| !s.target.is_empty()) {
            s.message = format!("ctld: {e}");
        }
    }
    said
}

/// The target a provided LUN is.
pub fn target(namespace: &str, volume: &str) -> String {
    format!("{TARGET_PREFIX}{namespace}.{volume}")
}

/// Makes `ds` (its quota `size`) and shares it with `clients` over NFS;
/// where it is mounted.
fn share(sys: &dyn Sys, ds: &str, size: Option<u64>, clients: &[String]) -> Result<String, String> {
    let spec = VolumeSpec {
        size: size.map(Bytes),
        ..Default::default()
    };
    let path = crate::volumes::zfs(sys, ds, &spec)?;
    if clients.is_empty() {
        return Err("no node to share it with".to_owned());
    }
    // exports(5): what the hosts may do; root is root, as on a node's own
    // dataset (the jails' users are numbered as on the node).
    let value = format!("-maproot=root {}", clients.join(" "));
    if sys
        .zfs_get(ds, "sharenfs")
        .map_err(|e| e.to_string())?
        .as_deref()
        != Some(value.as_str())
    {
        sys.zfs_set(ds, "sharenfs", &value)
            .map_err(|e| e.to_string())?;
    }
    Ok(path)
}

/// Makes the ZFS volume `ds` of `size` bytes, formatted UFS when it holds
/// no file system; its device.
fn lun(sys: &dyn Sys, ds: &str, size: u64) -> Result<String, String> {
    if size == 0 {
        return Err("a LUN needs a size".to_owned());
    }
    match sys
        .zfs_get(ds, "type")
        .map_err(|e| e.to_string())?
        .as_deref()
    {
        None => sys.zfs_create_volume(ds, size).map_err(|e| e.to_string())?,
        Some("volume") => {}
        Some(other) => return Err(format!("{ds} is a {other}, not a ZFS volume")),
    }
    let device = format!("/dev/zvol/{ds}");
    // The device comes a moment after the volume.
    let mut tries = 0;
    let fs = loop {
        match sys.fs_type(&device) {
            Ok(fs) => break fs,
            Err(e) if tries >= 50 => return Err(format!("{device}: {e}")),
            Err(_) => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    match fs.as_deref() {
        Some("ufs") => {}
        // Made here, empty: formatted once. What holds anything else is
        // someone's, and left alone.
        None => {
            sys.run_program("newfs", &["-U", &device], "")
                .map_err(|e| format!("formatting {device}: {e}"))?;
        }
        Some(other) => return Err(format!("{device} holds {other}, not UFS: left alone")),
    }
    Ok(device)
}

/// Makes the file of targets, empty, if it is not there: ctld does not
/// start with a configuration including a file that is missing.
pub fn ensure_targets(file: &Path, group: &str) -> std::io::Result<()> {
    if file.exists() {
        return Ok(());
    }
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(file, targets(group, &[]))
}

/// Writes ctld's included targets, `luns` on `group`, and has ctld reload
/// them if they changed.
fn write_targets(
    sys: &dyn Sys,
    file: &Path,
    group: &str,
    luns: &[(String, String)],
) -> Result<(), String> {
    let text = targets(group, luns);
    if std::fs::read_to_string(file).is_ok_and(|now| now == text) {
        return Ok(());
    }
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let tmp = file.with_extension("new");
    std::fs::write(&tmp, &text).map_err(|e| format!("writing {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, file).map_err(|e| format!("writing {}: {e}", file.display()))?;
    // `onereload`: whether or not rc.conf enables ctld.
    sys.run_program("service", &["ctld", "onereload"], "")
        .map(drop)
        .map_err(|e| format!("reloading: {e}"))
}

/// ctld's UCL for `luns` (target, device) on portal group `group`: no
/// authentication, as the portal group's own settings (which addresses it
/// listens on) say who reaches it.
pub fn targets(group: &str, luns: &[(String, String)]) -> String {
    let mut out = String::from("# Written by Legion: the LUNs this node provides.\ntarget {\n");
    for (target, device) in luns {
        out.push_str(&format!(
            "  \"{target}\" {{\n    auth-group = no-authentication\n    portal-group = {group}\n    lun = {{\n      0 {{ path = \"{device}\" }}\n    }}\n  }}\n"
        ));
    }
    out.push_str("}\n");
    out
}

#[cfg(test)]
mod tests {
    use legion_proto::v1::ExportWanted;
    use legion_sys::FakeSys;

    use super::*;

    fn wanted(ns: &str, volume: &str, kind: &str, size: u64) -> ExportWanted {
        ExportWanted {
            namespace: ns.into(),
            volume: volume.into(),
            kind: kind.into(),
            size,
        }
    }

    #[test]
    fn exports_are_made_as_listed_and_others_no_longer_shared() {
        let sys = FakeSys::new();
        sys.zfs_create("tank/legion", &[]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("targets.ucl");
        let iscsi = IscsiConfig {
            targets: Some(file.clone()),
            portal_group: "pg0".into(),
            portal: None,
        };
        let lock = Mutex::new(());
        let opts = ExportSync {
            exports: vec![
                wanted("shop", "files", "nfs", 1 << 30),
                wanted("shop", "db", "iscsi", 4 << 30),
            ],
            clients: vec!["10.0.0.1".into(), "10.0.0.2".into()],
        };
        let said = apply(&sys, &lock, "tank/legion", &iscsi, &opts);
        assert_eq!(said[0].path, "/tank/legion/exports/shop/files", "{said:?}");
        assert_eq!(said[0].message, "");
        assert_eq!(
            said[1].target, "iqn.2026-11.invalid.legion:shop.db",
            "{said:?}"
        );
        let files = "tank/legion/exports/shop/files";
        assert_eq!(
            sys.zfs_get(files, "sharenfs").unwrap().as_deref(),
            Some("-maproot=root 10.0.0.1 10.0.0.2")
        );
        assert_eq!(sys.zfs_list(files).unwrap()[0].quota, Some(1 << 30));
        // The LUN formatted once, and in ctld's targets, reloaded.
        let ran = sys.programs_run();
        assert!(
            ran.contains(&"newfs -U /dev/zvol/tank/legion/exports/shop/db".to_owned()),
            "{ran:?}"
        );
        assert!(
            ran.contains(&"service ctld onereload".to_owned()),
            "{ran:?}"
        );
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(
            text.contains("\"iqn.2026-11.invalid.legion:shop.db\""),
            "{text}"
        );
        assert!(text.contains("portal-group = pg0"), "{text}");
        assert!(
            text.contains("path = \"/dev/zvol/tank/legion/exports/shop/db\""),
            "{text}"
        );

        // Again, the same: nothing formatted or reloaded again.
        apply(&sys, &lock, "tank/legion", &iscsi, &opts);
        assert_eq!(sys.programs_run(), ran);

        // Neither listed: not shared, no target, the data kept.
        let none = ExportSync {
            exports: Vec::new(),
            clients: opts.clients.clone(),
        };
        apply(&sys, &lock, "tank/legion", &iscsi, &none);
        assert_eq!(
            sys.zfs_get(files, "sharenfs").unwrap().as_deref(),
            Some("off")
        );
        assert!(!std::fs::read_to_string(&file).unwrap().contains("shop.db"));
        assert!(
            !sys.zfs_list("tank/legion/exports/shop/db")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn without_a_targets_file_no_lun_is_exported() {
        let sys = FakeSys::new();
        sys.zfs_create("tank/legion", &[]).unwrap();
        let opts = ExportSync {
            exports: vec![wanted("shop", "db", "iscsi", 1 << 30)],
            clients: vec!["10.0.0.1".into()],
        };
        let said = apply(
            &sys,
            &Mutex::new(()),
            "tank/legion",
            &IscsiConfig::default(),
            &opts,
        );
        assert!(said[0].message.contains("--iscsi-targets"), "{said:?}");
        assert!(sys.zfs_list("tank/legion/exports").unwrap().is_empty());
    }
}
