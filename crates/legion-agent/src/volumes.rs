//! Pod volumes (DESIGN.md §6.4, §6.7): `Volume`s and memory-backed `tmpfs`
//! volumes, bind-mounted (nullfs) into the containers that mount them.
//!
//! A `Volume` comes from its driver:
//!
//! - **zfs**: the dataset `<zfs root>/volumes/<namespace>/<name>`; a
//!   stateful task's claim mounts its own Volume, named after the claim,
//!   the service and the task's ordinal. The first node to use it creates
//!   it, with the volume's quota, and records itself in the volume's
//!   status; the data stays on that node, so tasks elsewhere are rejected.
//!   The dataset outlives tasks: deleting the `Volume` object leaves it in
//!   place.
//! - **nfs**: the export, mounted for each task that uses it, in the
//!   task's directory, so it goes with the task's other mounts.
//! - **iscsi**: the node logs in to the target and mounts the LUN's UFS
//!   file system once, for all its tasks using it; one node at a time, so
//!   the Volume is bound to the node, and the node lets go (unmounts,
//!   logs out, then tells the manager) once its last task using it is
//!   gone. A LUN without UFS is refused, never formatted: what fstyp(8)
//!   does not know may still be someone's data.
//! - **plugin**: the executable named in the node's plugin directory,
//!   run as `<plugin> mount <dir>` and `<plugin> unmount <dir>` for each
//!   task, with `{"namespace", "volume", "options"}` as JSON on its
//!   standard input; it must leave a file system mounted at `<dir>`. An
//!   unshared one is bound to the node as iSCSI's are.
//!
//! What a task attached that its teardown must undo (iSCSI and plugin
//! volumes) is written to its directory before attaching, so a teardown
//! after a failure or an agent restart still finds it. One lock keeps a
//! starting task's claim and attach apart from a stopping task's detach and
//! release.
//!
//! An update that snapshots (`update.snapshot`) has its tasks snapshot
//! their claimed Volumes before they run, unless done already, dropping
//! older updates' snapshots; a rollback's tasks return their Volumes to the
//! snapshot and drop it, so a rollback task that restarts keeps its data.
//!
//! A `tmpfs` volume lives as long as its pod, at
//! `<files dir>/<task uid>/volumes/<name>`, and is shared by the pod's
//! containers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use legion_api::service::{PodVolume, UPDATE_SNAPSHOT};
use legion_api::task::VolumeSnapshot;
use legion_api::volume::{Access, IscsiSource, NfsSource, VolumeDriver, VolumeSpec};
use legion_sys::Sys;
use serde::{Deserialize, Serialize};

use crate::control::{Assignment, Control, VolumeClaim};
use crate::files;
use crate::runtime::Mount;

/// How long an iSCSI session may take to connect and show its LUNs.
const ISCSI_WAIT: Duration = Duration::from_secs(30);
/// Lists what a task attached, in its directory.
const ATTACHED: &str = "attached.json";

/// Why the volumes could not be prepared.
pub(crate) enum Error {
    /// May work later (a missing volume, a failed command).
    Failed(String),
    /// Cannot work on this node.
    Rejected(String),
}

/// Where each pod volume is on the host, by pod volume name.
pub(crate) type Sources = BTreeMap<String, String>;

/// What the node's volumes need, from the agent's context.
pub(crate) struct Places<'a> {
    pub node: &'a str,
    pub zfs_root: Option<&'a str>,
    /// Per-task directories (see [`files`]).
    pub files_dir: &'a Path,
    /// Holds what the node mounts once for all its tasks.
    pub volumes_dir: &'a Path,
    pub plugin_dir: &'a Path,
    pub lock: &'a tokio::sync::Mutex<()>,
}

/// Something a task attached that its teardown undoes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "driver", rename_all = "lowercase")]
enum Attached {
    /// Claimed for the node, one node at a time, before any of it is
    /// attached: let go of by the teardown, should the start fail before
    /// it records more, or the agent go.
    Claimed { namespace: String, volume: String },
    Iscsi {
        namespace: String,
        volume: String,
        target: String,
        /// Records written before it was kept have none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lun: Option<u32>,
    },
    Plugin {
        namespace: String,
        volume: String,
        plugin: String,
        dir: String,
        shared: bool,
        /// Handed to `unmount` as to `mount`; records written before
        /// they were kept have none.
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
}

impl Attached {
    fn volume(&self) -> (&str, &str) {
        match self {
            Attached::Claimed { namespace, volume }
            | Attached::Iscsi {
                namespace, volume, ..
            }
            | Attached::Plugin {
                namespace, volume, ..
            } => (namespace, volume),
        }
    }
}

/// The dataset of a volume.
pub fn dataset(root: &str, namespace: &str, name: &str) -> String {
    format!("{root}/volumes/{namespace}/{name}")
}

/// Where the node mounts an iSCSI volume for its tasks.
fn node_mount(volumes_dir: &Path, namespace: &str, name: &str) -> PathBuf {
    volumes_dir.join("iscsi").join(namespace).join(name)
}

/// Creates, finds or mounts the pod's volumes.
pub(crate) async fn prepare<C: Control>(
    control: &C,
    sys: &Arc<dyn Sys>,
    p: &Places<'_>,
    a: &Assignment,
) -> Result<Sources, Error> {
    let mut sources = Sources::new();
    for v in &a.spec.pod.volumes {
        let source = if let Some(name) = a.spec.volume_name(v) {
            volume(control, sys, p, a, v, &name).await?
        } else if v.claim.is_some() {
            return Err(Error::Rejected(format!(
                "volume {}: only a stateful service's tasks claim volumes",
                v.name
            )));
        } else {
            let size = v.tmpfs.as_ref().and_then(|t| t.size).map(|b| b.0);
            let dir = task_volume(p.files_dir, &a.uid, &v.name);
            let name = v.name.clone();
            crate::task::blocking(sys, move |sys| tmpfs(sys, &dir, size))
                .await
                .map_err(|e| Error::Failed(format!("tmpfs volume {name}: {e}")))?
        };
        sources.insert(v.name.clone(), source);
    }
    Ok(sources)
}

fn task_volume(files_dir: &Path, uid: &str, name: &str) -> PathBuf {
    files::task_dir(files_dir, uid).join("volumes").join(name)
}

/// One `Volume`: claimed for the node, then made ready by its driver.
async fn volume<C: Control>(
    control: &C,
    sys: &Arc<dyn Sys>,
    p: &Places<'_>,
    a: &Assignment,
    v: &PodVolume,
    name: &str,
) -> Result<String, Error> {
    let namespace = a.key.namespace.as_str();
    let failed = |e: String| Error::Failed(format!("volume {name}: {e}"));
    // Without a ZFS root no dataset is named, and a ZFS volume stays
    // unbound.
    let dataset = p.zfs_root.map(|root| dataset(root, namespace, name));
    let guard = p.lock.lock().await;
    let spec = match control
        .claim_volume(namespace, name, p.node, dataset.as_deref().unwrap_or(""))
        .await
    {
        Ok(VolumeClaim::Claimed(spec)) => *spec,
        Ok(VolumeClaim::Missing) => {
            return Err(Error::Failed(format!("volume {name} not found")));
        }
        Ok(VolumeClaim::Elsewhere(other)) => {
            return Err(Error::Rejected(format!(
                "volume {name} is held by node {other}"
            )));
        }
        Err(e) => return Err(failed(e.to_string())),
    };
    // Recorded before anything can fail: the teardown, which lets go only
    // of what is recorded, then lets go of it, trying again until it can.
    if spec.access() == Access::Single {
        let claimed = Attached::Claimed {
            namespace: namespace.to_owned(),
            volume: name.to_owned(),
        };
        if let Err(e) = record(p.files_dir, &a.uid, claimed) {
            if let Err(r) = control.release_volume(namespace, name, p.node).await {
                tracing::warn!(uid = %a.uid, "letting go of volume {name}: {r}");
            }
            return Err(failed(e));
        }
    }
    ready(sys, p, a, v, name, dataset, spec, guard).await
}

/// Makes a claimed volume ready by its driver.
#[allow(clippy::too_many_arguments)]
async fn ready(
    sys: &Arc<dyn Sys>,
    p: &Places<'_>,
    a: &Assignment,
    v: &PodVolume,
    name: &str,
    dataset: Option<String>,
    spec: VolumeSpec,
    guard: tokio::sync::MutexGuard<'_, ()>,
) -> Result<String, Error> {
    let namespace = a.key.namespace.as_str();
    let failed = |e: String| Error::Failed(format!("volume {name}: {e}"));
    if v.claim.is_some() && spec.driver != VolumeDriver::Zfs {
        return Err(Error::Rejected(format!(
            "volume {name}: a claim is for a ZFS volume, not {}",
            spec.driver.as_str()
        )));
    }
    let task_dir = task_volume(p.files_dir, &a.uid, &v.name);
    match spec.driver {
        // Under the lock: two tasks here starting on one Volume would race
        // to create and mount it, and to drop a move's snapshots.
        VolumeDriver::Zfs => {
            let dataset = dataset.ok_or_else(|| {
                Error::Rejected(format!(
                    "volume {name}: this node has no ZFS dataset for volumes"
                ))
            })?;
            let step = a.spec.snapshot.clone().filter(|_| v.claim.is_some());
            let result = crate::task::blocking(sys, move |sys| {
                let path = zfs(sys, &dataset, &spec)?;
                if let Some(step) = &step {
                    snapshot(sys, &dataset, step)?;
                }
                Ok(path)
            })
            .await;
            drop(guard);
            result.map_err(failed)
        }
        VolumeDriver::Nfs => {
            drop(guard);
            let nfs = spec.nfs.ok_or_else(|| failed("no nfs section".into()))?;
            crate::task::blocking(sys, move |sys| mount_nfs(sys, &nfs, &task_dir))
                .await
                .map_err(failed)
        }
        VolumeDriver::Iscsi => {
            let iscsi = spec
                .iscsi
                .ok_or_else(|| failed("no iscsi section".into()))?;
            // The node mounts it once for all its tasks: only from the
            // source they mounted it from. A mount none of them records is
            // a leftover, of whatever source, and goes.
            let recorded: Vec<Attached> = files::present(p.files_dir)
                .into_iter()
                .flat_map(|task| attached(&files::task_dir(p.files_dir, &task)).unwrap_or_default())
                .filter(|entry| entry.volume() == (namespace, name))
                // A claim alone mounts nothing.
                .filter(|entry| !matches!(entry, Attached::Claimed { .. }))
                .collect();
            for entry in &recorded {
                if let Attached::Iscsi { target, lun, .. } = entry
                    && (*target != iscsi.target || lun.is_some_and(|l| l != iscsi.lun))
                {
                    return Err(failed(format!(
                        "mounted on this node from {target}{} for other tasks; its new source is used once they have ended",
                        lun.map_or(String::new(), |l| format!(" LUN {l}"))
                    )));
                }
            }
            let fresh = recorded.is_empty();
            record(
                p.files_dir,
                &a.uid,
                Attached::Iscsi {
                    namespace: namespace.to_owned(),
                    volume: name.to_owned(),
                    target: iscsi.target.clone(),
                    lun: Some(iscsi.lun),
                },
            )
            .map_err(failed)?;
            let dir = node_mount(p.volumes_dir, namespace, name);
            let result =
                crate::task::blocking(sys, move |sys| mount_iscsi(sys, &iscsi, &dir, fresh)).await;
            drop(guard);
            result.map_err(failed)
        }
        VolumeDriver::Plugin => {
            let plugin = spec
                .plugin
                .ok_or_else(|| failed("no plugin section".into()))?;
            // Joined with `/` on every platform: names are validated.
            let program = PathBuf::from(format!("{}/{}", p.plugin_dir.display(), plugin.name));
            let dir = path_str(&task_dir).map_err(failed)?;
            record(
                p.files_dir,
                &a.uid,
                Attached::Plugin {
                    namespace: namespace.to_owned(),
                    volume: name.to_owned(),
                    plugin: path_str(&program).map_err(failed)?,
                    dir: dir.clone(),
                    shared: plugin.shared,
                    options: plugin.options.clone(),
                },
            )
            .map_err(failed)?;
            if plugin.shared {
                drop(guard);
            }
            let input = serde_json::json!({
                "namespace": namespace,
                "volume": name,
                "options": plugin.options,
            })
            .to_string();
            crate::task::blocking(sys, move |sys| {
                mount_plugin(sys, &program, &task_dir, &dir, &input)
            })
            .await
            .map_err(failed)
        }
    }
}

fn path_str(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{} is not UTF-8", path.display()))
}

/// Adds to what the task attached.
fn record(files_dir: &Path, uid: &str, entry: Attached) -> Result<(), String> {
    let dir = files::task_dir(files_dir, uid);
    std::fs::create_dir_all(&dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    let mut all = attached(&dir)?;
    if !all.contains(&entry) {
        all.push(entry);
    }
    write_attached(&dir, &all)
}

/// Writes the record whole or not at all: a half-written one, as an agent
/// stopped midway would leave, could not be read to undo any of it.
fn write_attached(dir: &Path, all: &[Attached]) -> Result<(), String> {
    let path = dir.join(ATTACHED);
    let next = dir.join(format!("{ATTACHED}.next"));
    let text = serde_json::to_vec(all).map_err(|e| e.to_string())?;
    std::fs::write(&next, text).map_err(|e| format!("writing {}: {e}", next.display()))?;
    std::fs::rename(&next, &path).map_err(|e| format!("writing {}: {e}", path.display()))
}

/// What the task with directory `dir` attached.
fn attached(dir: &Path) -> Result<Vec<Attached>, String> {
    let path = dir.join(ATTACHED);
    match std::fs::read(&path) {
        Ok(text) => {
            serde_json::from_slice(&text).map_err(|e| format!("reading {}: {e}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("reading {}: {e}", path.display())),
    }
}

fn mount_nfs(sys: &dyn Sys, nfs: &NfsSource, dir: &Path) -> Result<String, String> {
    let path = path_str(dir)?;
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {path}: {e}"))?;
    if sys
        .mounts(&path)
        .map_err(|e| e.to_string())?
        .contains(&path)
    {
        return Ok(path);
    }
    let server = if nfs.server.contains(':') {
        format!("[{}]", nfs.server)
    } else {
        nfs.server.clone()
    };
    let options = if nfs.options.is_empty() {
        "nfsv4".to_owned()
    } else {
        nfs.options.join(",")
    };
    let source = format!("{server}:{}", nfs.path);
    sys.mount_fs("nfs", &source, &path, &options)
        .map_err(|e| e.to_string())?;
    Ok(path)
}

/// Mounts the LUN at `dir`, unless there already; a mount there is not
/// taken as the LUN's if `fresh` (no task here records it).
fn mount_iscsi(
    sys: &dyn Sys,
    iscsi: &IscsiSource,
    dir: &Path,
    fresh: bool,
) -> Result<String, String> {
    let e = |e: legion_sys::Error| e.to_string();
    let path = path_str(dir)?;
    if sys.mounts(&path).map_err(e)?.contains(&path) {
        if !fresh {
            return Ok(path);
        }
        tracing::info!(%path, "unmounting a volume mount no task records");
        sys.unmount(&path).map_err(e)?;
    }
    sys.kld_load("iscsi").map_err(e)?;
    let session = |sys: &dyn Sys| -> Result<_, String> {
        Ok(sys
            .iscsi_sessions()
            .map_err(e)?
            .into_iter()
            .find(|s| s.target == iscsi.target))
    };
    if session(sys)?.is_none() {
        tracing::info!(target = %iscsi.target, portal = %iscsi.portal, "logging in to the iSCSI target");
        sys.iscsi_attach(&iscsi.portal, &iscsi.target).map_err(e)?;
    }
    let deadline = std::time::Instant::now() + ISCSI_WAIT;
    let device = loop {
        let s = session(sys)?.ok_or("the iSCSI session went away")?;
        if s.connected()
            && let Some(device) = s.device(iscsi.lun)
        {
            break format!("/dev/{device}");
        }
        if std::time::Instant::now() > deadline {
            let why = if !s.connected() {
                format!(
                    "not connected after {}s ({}; is iscsid running?)",
                    ISCSI_WAIT.as_secs(),
                    if s.failure.is_empty() {
                        &s.state
                    } else {
                        &s.failure
                    }
                )
            } else {
                format!("the target has no LUN {}", iscsi.lun)
            };
            return Err(format!("{}: {why}", iscsi.target));
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    match sys.fs_type(&device).map_err(e)?.as_deref() {
        Some("ufs") => {}
        Some(other) => {
            return Err(format!(
                "LUN {} of {} holds {other}; Legion mounts UFS",
                iscsi.lun, iscsi.target
            ));
        }
        None => {
            return Err(format!(
                "LUN {} of {} holds no file system; make a UFS one on it first (newfs)",
                iscsi.lun, iscsi.target
            ));
        }
    }
    // A node that went down using it leaves it unclean, which mounts
    // read-only at best: checked first, as boot checks its own.
    sys.fsck_preen(&device).map_err(e)?;
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {path}: {e}"))?;
    sys.mount_fs("ufs", &device, &path, "").map_err(e)?;
    Ok(path)
}

fn mount_plugin(
    sys: &dyn Sys,
    program: &Path,
    dir: &Path,
    path: &str,
    input: &str,
) -> Result<String, String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {path}: {e}"))?;
    if sys
        .mounts(path)
        .map_err(|e| e.to_string())?
        .iter()
        .any(|m| m == path)
    {
        return Ok(path.to_owned());
    }
    let program = path_str(program)?;
    sys.run_program(&program, &["mount", path], input)
        .map_err(|e| match e {
            legion_sys::Error::NotFound(_) => format!("no volume plugin {program} on this node"),
            e => e.to_string(),
        })?;
    if !sys
        .mounts(path)
        .map_err(|e| e.to_string())?
        .iter()
        .any(|m| m == path)
    {
        return Err(format!("{program} mounted nothing at {path}"));
    }
    Ok(path.to_owned())
}

/// Undoes what a task attached, before its directory goes: plugins
/// unmount, and a volume used one node at a time that no other task here
/// uses any more is unmounted, detached and let go. An iSCSI target is
/// logged out of once no volume here, of any of its LUNs, is on it. An
/// error leaves the record, for the next teardown to finish.
pub(crate) async fn release<C: Control>(
    control: &C,
    sys: &Arc<dyn Sys>,
    p: &Places<'_>,
    uid: &str,
) -> Result<(), String> {
    let dir = files::task_dir(p.files_dir, uid);
    let mine = attached(&dir)?;
    if mine.is_empty() {
        return Ok(());
    }
    let _guard = p.lock.lock().await;
    // What the node's other tasks still use.
    let others: Vec<Attached> = files::present(p.files_dir)
        .into_iter()
        .filter(|other| other != uid)
        .flat_map(|other| attached(&files::task_dir(p.files_dir, &other)).unwrap_or_default())
        .collect();
    for (i, entry) in mine.iter().enumerate() {
        if let Err(e) = release_one(control, sys, p, entry, &others, &mine[i + 1..]).await {
            // What is undone already stays undone, and goes from the
            // record: another task here would still count it in use.
            if i > 0 {
                write_attached(&dir, &mine[i..])?;
            }
            return Err(e);
        }
    }
    let path = dir.join(ATTACHED);
    std::fs::remove_file(&path).map_err(|e| format!("removing {}: {e}", path.display()))
}

/// Undoes one thing a task attached; `others` is what the node's other
/// tasks attached, `rest` what this one attached after it.
async fn release_one<C: Control>(
    control: &C,
    sys: &Arc<dyn Sys>,
    p: &Places<'_>,
    entry: &Attached,
    others: &[Attached],
    rest: &[Attached],
) -> Result<(), String> {
    // Held by another task here: one that only claimed it holds the claim,
    // not the node's mount.
    let in_use = |entry: &Attached| others.iter().any(|o| o.volume() == entry.volume());
    let mounted = |entry: &Attached| {
        others
            .iter()
            .any(|o| o.volume() == entry.volume() && !matches!(o, Attached::Claimed { .. }))
    };
    let (namespace, volume) = entry.volume();
    let single = match entry {
        Attached::Claimed { .. } => true,
        Attached::Plugin {
            plugin,
            dir,
            namespace,
            volume,
            shared,
            options,
        } => {
            let (plugin, dir) = (plugin.clone(), dir.clone());
            let input = serde_json::json!({
                "namespace": namespace,
                "volume": volume,
                "options": options,
            })
            .to_string();
            crate::task::blocking(sys, move |sys| unmount_plugin(sys, &plugin, &dir, &input))
                .await?;
            !shared
        }
        Attached::Iscsi { target, .. } => {
            if !mounted(entry) {
                let path = path_str(&node_mount(p.volumes_dir, namespace, volume))?;
                let on_target =
                    |a: &Attached| matches!(a, Attached::Iscsi { target: t, .. } if t == target);
                let last = !others.iter().chain(rest).any(on_target);
                let target = target.clone();
                crate::task::blocking(sys, move |sys| {
                    let e = |e: legion_sys::Error| e.to_string();
                    if sys.mounts(&path).map_err(e)?.contains(&path) {
                        sys.unmount(&path).map_err(e)?;
                    }
                    if last {
                        tracing::info!(%target, "logging out of the iSCSI target");
                        sys.iscsi_detach(&target).map_err(e)?;
                    }
                    Ok::<(), String>(())
                })
                .await?;
            }
            true
        }
    };
    // Not while this task mounts it again further on, as a pod naming the
    // same plugin volume twice does.
    let again = rest.iter().any(|a| a.volume() == entry.volume());
    if single && !in_use(entry) && !again {
        control
            .release_volume(namespace, volume, p.node)
            .await
            .map_err(|e| format!("letting go of volume {volume}: {e}"))?;
    }
    Ok(())
}

fn unmount_plugin(sys: &dyn Sys, program: &str, dir: &str, input: &str) -> Result<(), String> {
    let mounted = |sys: &dyn Sys| -> Result<bool, String> {
        Ok(sys
            .mounts(dir)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|m| m == dir))
    };
    match sys.run_program(program, &["unmount", dir], input) {
        Ok(_) => {}
        // Gone from the node: unmount it ourselves.
        Err(legion_sys::Error::NotFound(_)) => {
            tracing::warn!(program, "volume plugin missing; unmounting {dir} directly");
        }
        // Nothing to unmount (its mount failed, or an unmount went
        // through before), which a plugin may well refuse: done.
        Err(e) if !mounted(sys)? => {
            tracing::warn!(program, "unmounting {dir}: {e}; nothing is mounted there");
        }
        Err(e) => return Err(e.to_string()),
    }
    if mounted(sys)? {
        sys.unmount(dir).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Creates the dataset if needed, keeps its quota in line with the spec and
/// returns where it is mounted.
pub(crate) fn zfs(sys: &dyn Sys, dataset: &str, spec: &VolumeSpec) -> Result<String, String> {
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
    // One received from another node may not be mounted yet.
    sys.zfs_mount(dataset).map_err(|e| e.to_string())?;
    // A move's two snapshots: the move is over, the Volume being used
    // here (received, or kept by a move given up). Only a full copy's is
    // one still under way.
    let snaps = sys.zfs_snapshots(dataset).map_err(|e| e.to_string())?;
    let moves = [crate::transfer::FULL, crate::transfer::LAST];
    if moves.iter().all(|m| snaps.iter().any(|s| s == m)) {
        for m in moves {
            sys.zfs_destroy(&format!("{dataset}@{m}"), false)
                .map_err(|e| e.to_string())?;
        }
    }
    let ds = sys
        .zfs_list(dataset)
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|d| d.name == dataset)
        .ok_or_else(|| format!("{dataset} missing after mounting it"))?;
    ds.mountpoint
        .ok_or_else(|| format!("{dataset} is not mounted"))
}

/// Takes an update's snapshot of a dataset, or returns it to one.
fn snapshot(sys: &dyn Sys, dataset: &str, step: &VolumeSnapshot) -> Result<(), String> {
    let e = |e: legion_sys::Error| e.to_string();
    let names = sys.zfs_snapshots(dataset).map_err(e)?;
    let at = |name: &str| format!("{dataset}@{name}");
    let taken = names.contains(&step.name);
    if step.restore {
        // Not there if returned to already, or if the update never reached
        // this task's ordinal (or the Volume has moved since).
        if taken {
            tracing::info!(dataset, snapshot = %step.name, "returning the volume to its snapshot");
            sys.zfs_rollback(&at(&step.name)).map_err(e)?;
            sys.zfs_destroy(&at(&step.name), false).map_err(e)?;
        }
    } else if !taken {
        for old in names.iter().filter(|n| n.starts_with(UPDATE_SNAPSHOT)) {
            sys.zfs_destroy(&at(old), false).map_err(e)?;
        }
        sys.zfs_snapshot(&at(&step.name)).map_err(e)?;
    }
    Ok(())
}

/// Mounts a tmpfs any container user may write to, unless already there.
fn tmpfs(sys: &dyn Sys, dir: &Path, size: Option<u64>) -> Result<String, String> {
    let path = path_str(dir)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_replaced_whole() {
        let dir = tempfile::tempdir().unwrap();
        let entry = |volume: &str| Attached::Iscsi {
            namespace: "shop".into(),
            volume: volume.into(),
            target: "iqn.2026-10.lan.legion:block".into(),
            lun: Some(0),
        };
        write_attached(dir.path(), &[entry("a"), entry("b")]).unwrap();
        write_attached(dir.path(), &[entry("b")]).unwrap();
        assert_eq!(attached(dir.path()).unwrap(), [entry("b")]);
        // Nothing but the record is left.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, [ATTACHED]);
    }
}
