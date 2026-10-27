//! Pod volumes (DESIGN.md §6.4): ZFS-backed `Volume`s and memory-backed
//! `tmpfs` volumes, bind-mounted (nullfs) into the containers that mount
//! them.
//!
//! A `Volume` is the dataset `<zfs root>/volumes/<namespace>/<name>`. The
//! first node to use it creates it, with the volume's quota, and records
//! itself in the volume's status; the data stays on that node, so tasks
//! elsewhere are rejected. The dataset outlives tasks: deleting the
//! `Volume` object leaves it in place.
//!
//! A `tmpfs` volume lives as long as its pod, at
//! `<files dir>/<task uid>/volumes/<name>`, and is shared by the pod's
//! containers.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use legion_api::volume::VolumeSpec;
use legion_podman::Mount;
use legion_sys::Sys;

use crate::control::{Assignment, Control, VolumeClaim};
use crate::files;

/// Why the volumes could not be prepared.
pub(crate) enum Error {
    /// May work later (a missing volume, a failed command).
    Failed(String),
    /// Cannot work on this node.
    Rejected(String),
}

/// Where each pod volume is on the host, by pod volume name.
pub(crate) type Sources = BTreeMap<String, String>;

/// The dataset of a volume.
pub fn dataset(root: &str, namespace: &str, name: &str) -> String {
    format!("{root}/volumes/{namespace}/{name}")
}

/// Creates or finds the pod's volumes.
pub(crate) async fn prepare<C: Control>(
    control: &C,
    sys: &Arc<dyn Sys>,
    node: &str,
    zfs_root: Option<&str>,
    files_dir: &Path,
    a: &Assignment,
) -> Result<Sources, Error> {
    let mut sources = Sources::new();
    for v in &a.spec.pod.volumes {
        let source = if let Some(src) = &v.volume {
            let root = zfs_root.ok_or_else(|| {
                Error::Rejected(format!(
                    "volume {}: this node has no ZFS dataset for volumes",
                    src.name
                ))
            })?;
            let dataset = dataset(root, &a.key.namespace, &src.name);
            let spec = match control
                .claim_volume(&a.key.namespace, &src.name, node, &dataset)
                .await
            {
                Ok(VolumeClaim::Claimed(spec)) => spec,
                Ok(VolumeClaim::Missing) => {
                    return Err(Error::Failed(format!("volume {} not found", src.name)));
                }
                Ok(VolumeClaim::Elsewhere(other)) => {
                    return Err(Error::Rejected(format!(
                        "volume {} lives on node {other}",
                        src.name
                    )));
                }
                Err(e) => {
                    return Err(Error::Failed(format!("volume {}: {e}", src.name)));
                }
            };
            let name = src.name.clone();
            crate::task::blocking(sys, move |sys| zfs(sys, &dataset, &spec))
                .await
                .map_err(|e| Error::Failed(format!("volume {name}: {e}")))?
        } else {
            let size = v.tmpfs.as_ref().and_then(|t| t.size).map(|b| b.0);
            let dir = files::task_dir(files_dir, &a.uid)
                .join("volumes")
                .join(&v.name);
            let name = v.name.clone();
            crate::task::blocking(sys, move |sys| tmpfs(sys, &dir, size))
                .await
                .map_err(|e| Error::Failed(format!("tmpfs volume {name}: {e}")))?
        };
        sources.insert(v.name.clone(), source);
    }
    Ok(sources)
}

/// Creates the dataset if needed, keeps its quota in line with the spec and
/// returns where it is mounted.
fn zfs(sys: &dyn Sys, dataset: &str, spec: &VolumeSpec) -> Result<String, String> {
    let quota = spec.size.map_or("none".to_owned(), |b| b.0.to_string());
    let props: &[(&str, &str)] = match spec.size {
        Some(_) => &[("quota", &quota)],
        None => &[],
    };
    sys.zfs_create(dataset, props).map_err(|e| e.to_string())?;
    let ds = sys
        .zfs_list(dataset)
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|d| d.name == dataset)
        .ok_or_else(|| format!("{dataset} missing after creating it"))?;
    if ds.quota != spec.size.map(|b| b.0) {
        sys.zfs_set(dataset, "quota", &quota)
            .map_err(|e| e.to_string())?;
    }
    ds.mountpoint
        .ok_or_else(|| format!("{dataset} is not mounted"))
}

/// Mounts a tmpfs any container user may write to, unless already there.
fn tmpfs(sys: &dyn Sys, dir: &Path, size: Option<u64>) -> Result<String, String> {
    let path = dir
        .to_str()
        .ok_or("the files directory is not UTF-8")?
        .to_owned();
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {path}: {e}"))?;
    let mounted = sys.mounts(&path).map_err(|e| e.to_string())?;
    if !mounted.contains(&path) {
        sys.mount_tmpfs(&path, size, 0o1777)
            .map_err(|e| e.to_string())?;
    }
    Ok(path)
}

/// The mounts of a container's `volumeMounts`.
pub(crate) fn mounts(c: &legion_api::service::Container, sources: &Sources) -> Vec<Mount> {
    c.volume_mounts
        .iter()
        .filter_map(|m| {
            let source = sources.get(&m.name)?;
            Some(Mount::nullfs(source, &m.path, m.read_only))
        })
        .collect()
}
