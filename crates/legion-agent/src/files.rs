//! Secrets and configs for a task's containers (DESIGN.md §3.4).
//!
//! Before the pod starts, the agent fetches every `Secret` and `Config` its
//! containers name and writes them to a tmpfs of its own at
//! `<files dir>/<task uid>`, so nothing secret touches disk. Each file a
//! container asks for is bind-mounted (nullfs) read-only at its target; a
//! container sees only its own. Values asked for as environment variables
//! are passed to the runtime, which keeps them in the container's record.
//!
//! The values are read once per pod start: a changed Secret reaches a task
//! when its pod next starts.
//!
//! The same tmpfs holds the pod's `resolv.conf`, mounted at
//! `/etc/resolv.conf` in every container that does not mount its own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::runtime::{ContainerSpec, Mount};
use legion_api::Kind;
use legion_api::service::{Container, PodSpec};
use legion_sys::Sys;

use crate::control::{Assignment, Control};

/// Room on a task's tmpfs beyond its files.
const SLACK: u64 = 1 << 20;

/// What a file takes at most on a tmpfs: whole pages, of up to this.
const PAGE: u64 = 16 << 10;

/// The size of a tmpfs for files of these lengths: tmpfs counts each in
/// whole pages, so many small files take far more than their bytes.
fn tmpfs_size(lengths: impl Iterator<Item = usize>) -> u64 {
    lengths
        .map(|n| (n as u64).div_ceil(PAGE).max(1) * PAGE)
        .sum::<u64>()
        + SLACK
}

/// What a pod's containers get, by container name.
#[derive(Debug, Default)]
pub(crate) struct Files {
    env: BTreeMap<String, BTreeMap<String, String>>,
    mounts: BTreeMap<String, Vec<Mount>>,
}

impl Files {
    /// Adds a container's secrets and configs to its spec.
    pub fn apply(&self, container: &str, spec: &mut ContainerSpec) {
        if let Some(env) = self.env.get(container) {
            spec.env.extend(env.clone());
        }
        if let Some(mounts) = self.mounts.get(container) {
            spec.mounts.extend(mounts.iter().cloned());
        }
    }
}

/// A task's directory, the mount point of its tmpfs.
/// Whether two absolute paths name the same place, however spelt
/// (`/etc/`, `//etc`, `/./etc`).
fn same_path(a: &str, b: &str) -> bool {
    let parts = |p: &str| -> Vec<String> {
        p.split('/')
            .filter(|c| !matches!(*c, "" | "."))
            .map(str::to_owned)
            .collect()
    };
    parts(a) == parts(b)
}

pub(crate) fn task_dir(root: &Path, uid: &str) -> PathBuf {
    root.join(uid)
}

fn dir_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Secret => "secrets",
        _ => "configs",
    }
}

fn wants_files(pod: &PodSpec) -> bool {
    pod.containers()
        .any(|c| !c.secrets.is_empty() || !c.configs.is_empty())
}

const RESOLV_CONF: &str = "/etc/resolv.conf";

/// Fetches the pod's secrets and configs and writes the files, with
/// `resolv_conf` if given. The error says what is missing or failed.
pub(crate) async fn prepare<C: Control>(
    control: &C,
    sys: &std::sync::Arc<dyn Sys>,
    root: &Path,
    a: &Assignment,
    resolv_conf: Option<String>,
) -> Result<Files, String> {
    let pod = &a.spec.pod;
    let mut values = BTreeMap::new();
    for (kind, name) in pod.container_data_refs() {
        let what = kind.as_str().to_lowercase();
        match control.data(&a.key.namespace, kind, &name).await {
            Ok(Some(v)) => {
                values.insert((kind, name), v);
            }
            Ok(None) => return Err(format!("{what} {name} not found")),
            Err(e) => return Err(format!("reading {what} {name}: {e}")),
        }
    }

    let mut files = Files::default();
    for c in pod.containers() {
        let mut env = BTreeMap::new();
        for e in &c.env {
            let key = match (&e.secret, &e.config) {
                (Some(n), _) => (Kind::Secret, n.clone()),
                (_, Some(n)) => (Kind::Config, n.clone()),
                _ => continue,
            };
            let text = String::from_utf8(values[&key].clone()).map_err(|_| {
                format!(
                    "container {}: env {} needs text, and {} {} is binary",
                    c.name,
                    e.name,
                    key.0.as_str().to_lowercase(),
                    key.1
                )
            })?;
            env.insert(e.name.clone(), text);
        }
        if !env.is_empty() {
            files.env.insert(c.name.clone(), env);
        }
    }
    if !wants_files(pod) && resolv_conf.is_none() {
        return Ok(files);
    }

    let dir = task_dir(root, &a.uid);
    for c in pod.containers() {
        let mut m = mounts(c, &dir);
        // By parts: a spec admitted before paths had one spelling may say
        // `/etc/`.
        let own = m.iter().any(|m| m.destination == RESOLV_CONF)
            || c.volume_mounts
                .iter()
                .any(|v| same_path(&v.path, RESOLV_CONF) || same_path(&v.path, "/etc"));
        if resolv_conf.is_some() && !own {
            let source = dir.join("resolv.conf");
            m.push(Mount::nullfs(&source.to_string_lossy(), RESOLV_CONF, true));
        }
        files.mounts.insert(c.name.clone(), m);
    }
    let size = tmpfs_size(
        values
            .values()
            .map(Vec::len)
            .chain(resolv_conf.as_ref().map(String::len)),
    );
    let write = move |sys: &dyn Sys| -> Result<(), String> {
        let path = dir.to_str().ok_or("the files directory is not UTF-8")?;
        std::fs::create_dir_all(&dir).map_err(|e| format!("creating {path}: {e}"))?;
        let mounted = sys.mounts(path).map_err(|e| e.to_string())?;
        if !mounted.iter().any(|m| m == path) {
            sys.mount_tmpfs(path, Some(size), 0o700)
                .map_err(|e| format!("mounting the task's tmpfs: {e}"))?;
        }
        for ((kind, name), value) in &values {
            let sub = dir.join(dir_name(*kind));
            std::fs::create_dir_all(&sub).map_err(|e| format!("creating {sub:?}: {e}"))?;
            write_file(&sub.join(name), value).map_err(|e| format!("writing {name}: {e}"))?;
        }
        if let Some(conf) = &resolv_conf {
            write_file(&dir.join("resolv.conf"), conf.as_bytes())
                .map_err(|e| format!("writing resolv.conf: {e}"))?;
        }
        Ok(())
    };
    crate::task::blocking(sys, write).await?;
    Ok(files)
}

fn mounts(c: &Container, dir: &Path) -> Vec<Mount> {
    let file = |kind, f: &legion_api::service::FileRef| {
        let source = dir.join(dir_name(kind)).join(&f.name);
        let target = f.target.clone().expect("admission sets the target");
        Mount::nullfs(&source.to_string_lossy(), &target, true)
    };
    c.secrets
        .iter()
        .map(|f| file(Kind::Secret, f))
        .chain(c.configs.iter().map(|f| file(Kind::Config, f)))
        .collect()
}

/// Writes a file readable by any user in the container: images often run
/// as a user of their own, and the container sees only its own files.
fn write_file(path: &Path, value: &[u8]) -> std::io::Result<()> {
    // Names are DNS labels, so no value is called `.<name>.new`.
    let name = path.file_name().expect("a file name").to_string_lossy();
    let tmp = path.with_file_name(format!(".{name}.new"));
    std::fs::write(&tmp, value)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::rename(&tmp, path)
}

/// What the agent keeps of a container's restarts, to go on with after it
/// restarts itself: a pod's containers are counted each on its own.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Restarts {
    /// Restarts so far, for `maxAttempts`.
    pub attempts: u32,
    /// Its exit counted and its restart due.
    pub pending: bool,
    /// How it finished for good, once it has.
    pub done: Option<Done>,
}

/// How a container finished for good.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Done {
    /// Its exit code.
    pub code: i32,
    /// What happened, for the task's status.
    pub what: String,
    /// It failed: a code other than 0, or a run ended for it (a failed
    /// probe, a memory kill), whatever code that left.
    pub failed: bool,
}

/// Where a task's restarts are kept: beside its files, gone with them.
fn restarts_path(root: &Path, uid: &str) -> PathBuf {
    task_dir(root, uid).join("restarts.json")
}

/// Keeps a task's restarts, by container name.
pub(crate) fn save_restarts(
    root: &Path,
    uid: &str,
    restarts: &BTreeMap<String, Restarts>,
) -> std::io::Result<()> {
    let path = restarts_path(root, uid);
    std::fs::create_dir_all(task_dir(root, uid))?;
    let tmp = path.with_file_name(".restarts.json.new");
    std::fs::write(&tmp, serde_json::to_vec(restarts).expect("serialises"))?;
    std::fs::rename(&tmp, &path)
}

/// The restarts kept for a task; none if there are none, or they can't be
/// read.
pub(crate) fn load_restarts(root: &Path, uid: &str) -> BTreeMap<String, Restarts> {
    std::fs::read(restarts_path(root, uid))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

/// Removes a task's files: unmounts its tmpfs and deletes the directory.
/// Its containers must be gone, or their mounts would keep the files busy.
pub(crate) fn release(sys: &dyn Sys, root: &Path, uid: &str) -> Result<(), String> {
    let dir = task_dir(root, uid);
    let Some(path) = dir.to_str() else {
        return Err("the files directory is not UTF-8".into());
    };
    if !dir.exists() {
        return Ok(());
    }
    // Deepest first, in case anything was mounted inside.
    let mut mounted = sys.mounts(path).map_err(|e| e.to_string())?;
    mounted.sort_by_key(|m| std::cmp::Reverse(m.len()));
    for m in mounted {
        sys.unmount(&m).map_err(|e| e.to_string())?;
    }
    std::fs::remove_dir_all(&dir).map_err(|e| format!("removing {path}: {e}"))
}

/// Task uids that have a files directory.
pub(crate) fn present(root: &Path) -> Vec<String> {
    match std::fs::read_dir(root) {
        Ok(entries) => entries
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PAGE, SLACK, same_path, tmpfs_size};

    #[test]
    fn a_tmpfs_holds_each_file_in_whole_pages() {
        // Many small files: their pages, not their bytes.
        let small = tmpfs_size(std::iter::repeat_n(10, 300));
        assert_eq!(small, 300 * PAGE + SLACK);
        assert_eq!(tmpfs_size([0].into_iter()), PAGE + SLACK);
        assert_eq!(
            tmpfs_size([PAGE as usize + 1].into_iter()),
            2 * PAGE + SLACK
        );
        assert_eq!(tmpfs_size(std::iter::empty()), SLACK);
    }

    #[test]
    fn paths_compare_by_parts() {
        for etc in ["/etc", "/etc/", "//etc", "/./etc/"] {
            assert!(same_path(etc, "/etc"), "{etc}");
        }
        assert!(!same_path("/etc2", "/etc"));
        assert!(!same_path("/etc/resolv.conf", "/etc"));
    }
}
