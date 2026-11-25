//! A restore to a point in time (DESIGN.md 6.9, `replication.restore`),
//! run by the leader: before a replicated Service's first member runs, a
//! backup of one of Service `from`'s members' Volumes is restored as the
//! first member's Volume (`BackupRestore`), and the log `from` archived is
//! fetched next to the data (`ArchiveFetch`, into [`RESTORE_LOG`]). The
//! controller holds the Service's tasks back until then; the plugin then
//! replays the log to `to` and the member becomes the primary. Once it is
//! (the controller says `done`), the fetched log is removed. Where it
//! stands is the Service's `status.restore`, which this writes but for
//! `done`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::replication::{RESTORE_LOG, Restore, RestorePhase, RestoreStatus};
use legion_api::service::{ServiceSpec, claim_volume};
use legion_api::volume::{VolumeSpec, VolumeStatus, stamp_ms};
use legion_api::{Kind, Object, ObjectKey};
use legion_proto::v1::session_open::Action;
use legion_store::{Command, Outcome, Precondition, Store};
use tokio::task::JoinSet;

use crate::backups::{access, open, ready_nodes, said};
use crate::session::Hub;

const PERIOD: Duration = Duration::from_secs(10);
/// Members of a Service gone whose Volumes' backups are looked for.
const GONE_MEMBERS: u32 = 3;

/// What a try came to: the status to record.
type Done = (ObjectKey, Result<RestoreStatus, String>);

fn now_ms() -> i64 {
    crate::console::now_ms()
}

pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>) {
    let mut runs: JoinSet<Done> = JoinSet::new();
    let mut running: HashSet<ObjectKey> = HashSet::new();
    // When each Service's last try failed: tried again after a backoff.
    let mut failed: HashMap<ObjectKey, i64> = HashMap::new();
    loop {
        while let Some(done) = runs.try_join_next() {
            let Ok((key, result)) = done else { continue };
            running.remove(&key);
            match &result {
                Ok(_) => {
                    failed.remove(&key);
                }
                Err(e) => {
                    tracing::warn!(service = %key, "restore: {e}");
                    failed.insert(key.clone(), now_ms());
                }
            }
            if let Err(e) = record(store, &key, result).await {
                tracing::debug!(service = %key, "recording the restore: {e}");
            }
        }
        let services = match store.list(Kind::Service, None) {
            Ok(l) => l.objects,
            Err(e) => {
                tracing::warn!("reading services: {e}");
                tokio::time::sleep(PERIOD).await;
                continue;
            }
        };
        let now = now_ms();
        for s in services {
            let Ok(spec) = legion_api::parse::<ServiceSpec>(&s.spec) else {
                continue;
            };
            if spec
                .replication
                .as_ref()
                .and_then(|r| r.restore.as_ref())
                .is_none()
            {
                continue;
            }
            let key = s.key();
            if running.contains(&key) {
                continue;
            }
            let previous: Option<RestoreStatus> = s
                .status
                .get("restore")
                .and_then(|v| serde_json::from_value(v.clone()).ok());
            let due = match &previous {
                None => true,
                Some(p) if p.phase == RestorePhase::Restoring => {
                    failed.get(&key).is_none_or(|at| {
                        *at + crate::migrate::backoff(p.failures).as_millis() as i64 <= now
                    })
                }
                Some(p) => p.phase == RestorePhase::Done && !p.cleaned && !p.volume.is_empty(),
            };
            if !due {
                continue;
            }
            running.insert(key.clone());
            let (store, hub) = (store.clone(), hub.clone());
            runs.spawn(async move {
                let result = match previous.as_ref().map(|p| p.phase) {
                    Some(RestorePhase::Done) => {
                        clean(&store, &hub, &s, previous.clone().unwrap_or_default()).await
                    }
                    _ => restore(&store, &hub, &s, &spec, previous).await,
                };
                (s.key(), result)
            });
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// The parts of a restore that depend only on the specs: the claim, the
/// targets, where the container sees the fetched log.
#[derive(Debug, PartialEq)]
pub(crate) struct Plan {
    pub claim: String,
    /// The first member's Volume, restored.
    pub volume: String,
    /// The BackupTarget of the log, and of the Volume backups.
    pub log_target: String,
    pub backup_target: String,
    /// `from`'s members' Volumes, whose backups are looked among.
    pub sources: Vec<String>,
    /// The fetched log, as the container running the plugin sees it.
    pub log: String,
}

/// What restoring `svc` (spec `spec`) from `from` (its spec, if it is
/// still there) takes.
pub(crate) fn plan(
    svc: &str,
    spec: &ServiceSpec,
    from: Option<&ServiceSpec>,
) -> Result<Plan, String> {
    let r = spec.replication.as_ref().ok_or("no replication")?;
    let want = r.restore.as_ref().ok_or("no restore")?;
    let claims: Vec<&str> = spec.pod.claims().map(|(n, _)| n).collect();
    let claim = want
        .claim(&claims)
        .ok_or("name the claim holding the database (restore.claim)")?
        .to_owned();
    let from_archive = from
        .and_then(|f| f.replication.as_ref())
        .and_then(|r| r.archive.as_ref())
        .map(|a| a.target.clone());
    let log_target = want
        .target
        .clone()
        .or(from_archive)
        .or_else(|| r.archive.as_ref().map(|a| a.target.clone()))
        .ok_or_else(|| {
            format!(
                "no target holds {}'s log: name it (restore.target)",
                want.from
            )
        })?;
    let claim_backup = |s: &ServiceSpec| {
        s.pod
            .claims()
            .find(|(n, _)| *n == claim)
            .and_then(|(_, c)| c.backup.as_ref())
            .map(|b| b.target.clone())
    };
    let backup_target = want
        .backups
        .clone()
        .or_else(|| from.and_then(claim_backup))
        .or_else(|| claim_backup(spec))
        .unwrap_or_else(|| log_target.clone());
    let members = from.map_or(GONE_MEMBERS, |f| f.replicas.unwrap_or(1).max(1));
    let sources = match &want.backup {
        Some(b) => vec![b.split_once('@').map_or(b.as_str(), |(v, _)| v).to_owned()],
        None => (0..members)
            .map(|k| claim_volume(&claim, &want.from, k))
            .collect(),
    };
    let containers: Vec<&str> = spec
        .pod
        .containers
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let container = r
        .container(&containers)
        .ok_or("name the container running the database (replication.container)")?;
    let mount = spec
        .pod
        .containers
        .iter()
        .find(|c| c.name == container)
        .and_then(|c| c.volume_mounts.iter().find(|m| m.name == claim))
        .ok_or_else(|| format!("container {container} does not mount the claim {claim}"))?;
    Ok(Plan {
        volume: claim_volume(&claim, svc, 0),
        claim,
        log_target,
        backup_target,
        sources,
        log: format!("{}/{RESTORE_LOG}", mount.path.trim_end_matches('/')),
    })
}

/// The backup to restore: of those listed for each source Volume, the
/// newest taken at or before `to` (all of them, without it). `listed` is
/// each source Volume's backups as the target names them.
pub(crate) fn choose(
    want: &Restore,
    listed: &[(String, Vec<String>)],
) -> Result<(String, String, i64), String> {
    let to = want.to_ms();
    if let Some(b) = &want.backup {
        let (vol, name) = b.split_once('@').ok_or("restore.backup: VOLUME@NAME")?;
        let held = listed
            .iter()
            .filter(|(v, _)| v == vol)
            .flat_map(|(_, l)| l)
            .find(|h| {
                h.rsplit(['@', '/'])
                    .next()
                    .map(|n| n.trim_end_matches(".zfs"))
                    == Some(name)
            })
            .ok_or_else(|| format!("no backup {b} on the target"))?;
        let taken = stamp_ms(held).unwrap_or(0);
        if to.is_some_and(|to| taken > to) {
            return Err(format!(
                "the backup {b} was taken after {}",
                want.to.as_deref().unwrap_or("")
            ));
        }
        return Ok((vol.to_owned(), held.clone(), taken));
    }
    listed
        .iter()
        .flat_map(|(v, l)| l.iter().map(move |h| (v, h)))
        .filter_map(|(v, h)| Some((v.clone(), h.clone(), stamp_ms(h)?)))
        .filter(|(_, _, taken)| to.is_none_or(|to| *taken <= to))
        .max_by_key(|(v, _, taken)| (*taken, std::cmp::Reverse(v.clone())))
        .ok_or_else(|| match &want.to {
            Some(to) => format!("no backup of {}'s members was taken before {to}", want.from),
            None => format!("no backup of {}'s members", want.from),
        })
}

/// The ready nodes taking new tasks, by name: where a restored member can
/// start.
fn active(store: &Store, ready: &HashSet<String>) -> Vec<String> {
    let mut nodes: Vec<String> = store
        .list(Kind::Node, None)
        .map(|l| l.objects)
        .unwrap_or_default()
        .into_iter()
        .filter(|n| ready.contains(&n.metadata.name))
        .filter(|n| {
            legion_api::parse::<legion_api::node::NodeSpec>(&n.spec)
                .is_ok_and(|s| s.availability == legion_api::node::Availability::Active)
        })
        .map(|n| n.metadata.name)
        .collect();
    nodes.sort();
    nodes
}

async fn restore(
    store: &Store,
    hub: &Arc<Hub>,
    s: &Object,
    spec: &ServiceSpec,
    previous: Option<RestoreStatus>,
) -> Result<RestoreStatus, String> {
    let (ns, svc) = (s.metadata.namespace.clone(), s.metadata.name.clone());
    let r = spec.replication.as_ref().ok_or("no replication")?;
    let want = r.restore.clone().ok_or("no restore")?;
    let mut st = previous
        .filter(|p| p.id == want.id())
        .unwrap_or_else(|| RestoreStatus {
            id: want.id(),
            ..Default::default()
        });
    st.at = now_ms();
    let from: Option<ServiceSpec> = store
        .get(&ObjectKey::new(Kind::Service, &ns, &want.from))
        .map_err(|e| e.to_string())?
        .and_then(|o| legion_api::parse(&o.spec).ok());
    let p = plan(&svc, spec, from.as_ref())?;
    let vkey = ObjectKey::new(Kind::Volume, &ns, &p.volume);
    let existing = store.get(&vkey).map_err(|e| e.to_string())?;
    if existing.is_some() && st.volume.is_empty() {
        // Not restored by this: the data there stays as it is.
        st.phase = RestorePhase::Skipped;
        st.message = format!("Volume {} was there already: nothing restored", p.volume);
        return Ok(st);
    }
    let ready = ready_nodes(store).ok_or("no nodes read")?;
    let node = match &existing {
        Some(v) => serde_json::from_value::<VolumeStatus>(v.status.clone())
            .map(|v| v.node)
            .unwrap_or_default(),
        None => match &want.node {
            Some(n) if ready.contains(n) => n.clone(),
            Some(n) => return Err(format!("node {n} is not ready")),
            None => active(store, &ready)
                .first()
                .cloned()
                .ok_or("no ready node takes new tasks")?,
        },
    };
    if !ready.contains(&node) {
        return Err(format!("node {node}, holding {}, is not ready", p.volume));
    }

    if existing.is_none() {
        let backups = access(store, &p.backup_target)?;
        let mut listed = Vec::new();
        for v in &p.sources {
            let mut a = open(
                hub,
                &node,
                &ns,
                Action::BackupList(legion_proto::v1::BackupList {
                    volume: v.clone(),
                    target: Some(backups.clone()),
                }),
            )
            .await?;
            let (_, l) = said(&mut a, &node).await?;
            listed.push((v.clone(), l));
        }
        let (source, held, taken) = choose(&want, &listed)?;
        let name = held
            .rsplit(['@', '/'])
            .next()
            .unwrap_or(&held)
            .trim_end_matches(".zfs")
            .to_owned();
        tracing::info!(service = %svc, %node, from = %held, "restore: restoring {source}'s backup as {}", p.volume);
        let mut a = open(
            hub,
            &node,
            &ns,
            Action::BackupRestore(legion_proto::v1::BackupRestore {
                volume: p.volume.clone(),
                target: Some(backups),
                of: format!("{ns}.{source}"),
                held: held.clone(),
            }),
        )
        .await?;
        said(&mut a, &node).await?;
        // The data is there: the Volume, the Service's own, as its claim
        // would have made it.
        let c = spec
            .pod
            .claims()
            .find(|(n, _)| *n == p.claim)
            .map(|(_, c)| c.clone())
            .unwrap_or_default();
        let vspec = VolumeSpec {
            size: c.size,
            snapshots: c.snapshots.clone(),
            backup: c.backup.clone(),
            ..Default::default()
        };
        let mut o = Object::new(
            Kind::Volume,
            &ns,
            &p.volume,
            serde_json::to_value(&vspec).map_err(|e| e.to_string())?,
        );
        o.metadata.labels.insert(
            crate::controller::plan::LABEL_SERVICE.to_owned(),
            svc.clone(),
        );
        o.status = serde_json::to_value(VolumeStatus {
            node: node.clone(),
            ..Default::default()
        })
        .map_err(|e| e.to_string())?;
        write(
            store,
            Command::Put {
                object: o,
                precondition: Precondition::MustNotExist,
            },
        )
        .await?;
        st.backup = format!("{source}@{name}");
        st.taken_ms = Some(taken);
        st.volume = p.volume.clone();
        st.node = node.clone();
        // Recorded now: a try after a failure from here on fetches the log
        // into this Volume rather than skip it as one there already.
        let _ = record(store, &s.key(), Ok(st.clone())).await;
    }

    let log = access(store, &p.log_target)?;
    let mut a = open(
        hub,
        &node,
        &ns,
        Action::ArchiveFetch(legion_proto::v1::ArchiveFetch {
            volume: p.volume.clone(),
            path: RESTORE_LOG.to_owned(),
            target: Some(log),
            of: format!("{ns}.{}", want.from),
            remove: false,
        }),
    )
    .await?;
    let (out, _) = said(&mut a, &node).await?;
    #[derive(serde::Deserialize, Default)]
    struct Fetched {
        #[serde(default)]
        files: u32,
    }
    let f: Fetched = serde_json::from_slice(&out).unwrap_or_default();
    if f.files == 0 {
        return Err(format!(
            "{} holds none of {}'s log (under {}.{}/wal/)",
            p.log_target, want.from, ns, want.from
        ));
    }
    st.fetched = f.files;
    st.log = p.log;
    st.phase = RestorePhase::Replaying;
    st.failures = 0;
    st.message.clear();
    st.at = now_ms();
    tracing::info!(service = %svc, files = f.files, "restore: the log fetched; the first member replays it");
    Ok(st)
}

/// Removes the fetched log once replayed.
async fn clean(
    store: &Store,
    hub: &Arc<Hub>,
    s: &Object,
    mut st: RestoreStatus,
) -> Result<RestoreStatus, String> {
    let ns = &s.metadata.namespace;
    let node = store
        .get(&ObjectKey::new(Kind::Volume, ns, &st.volume))
        .map_err(|e| e.to_string())?
        .and_then(|v| serde_json::from_value::<VolumeStatus>(v.status).ok())
        .map(|v| v.node)
        .ok_or_else(|| format!("no Volume {}", st.volume))?;
    let mut a = open(
        hub,
        &node,
        ns,
        Action::ArchiveFetch(legion_proto::v1::ArchiveFetch {
            volume: st.volume.clone(),
            path: RESTORE_LOG.to_owned(),
            target: None,
            of: String::new(),
            remove: true,
        }),
    )
    .await?;
    said(&mut a, &node).await?;
    st.cleaned = true;
    st.at = now_ms();
    Ok(st)
}

async fn write(store: &Store, command: Command) -> Result<(), String> {
    let out = tokio::time::timeout(crate::COMMIT_TIMEOUT, store.client_write(command))
        .await
        .map_err(|_| crate::no_quorum().message().to_owned())?
        .map_err(|e| e.to_string())?;
    match out.data {
        Outcome::Written(_) => Ok(()),
        other => Err(format!("{other:?}")),
    }
}

/// What a try came to, in the Service's `status.restore`: a failure counted
/// against the status as it was.
async fn record(
    store: &Store,
    key: &ObjectKey,
    result: Result<RestoreStatus, String>,
) -> Result<(), String> {
    for _ in 0..5 {
        let Some(mut o) = store.get(key).map_err(|e| e.to_string())? else {
            return Ok(());
        };
        let previous: Option<RestoreStatus> = o
            .status
            .get("restore")
            .and_then(|v| serde_json::from_value(v.clone()).ok());
        // The controller's word stands: what it says is done stays so.
        if previous
            .as_ref()
            .is_some_and(|p| p.phase == RestorePhase::Done)
            && result.as_ref().is_ok_and(|r| r.phase != RestorePhase::Done)
        {
            return Ok(());
        }
        let st = match &result {
            Ok(st) => st.clone(),
            Err(e) => {
                let mut p = previous.unwrap_or_default();
                p.failures += 1;
                p.message = e.clone();
                p.at = now_ms();
                p
            }
        };
        if !o.status.is_object() {
            o.status = serde_json::json!({});
        }
        o.status["restore"] = serde_json::to_value(&st).map_err(|e| e.to_string())?;
        let version = o.metadata.version;
        let command = Command::Put {
            object: o,
            precondition: Precondition::Version(version),
        };
        let out = tokio::time::timeout(crate::COMMIT_TIMEOUT, store.client_write(command))
            .await
            .map_err(|_| crate::no_quorum().message().to_owned())?
            .map_err(|e| e.to_string())?;
        match out.data {
            Outcome::Written(_) => return Ok(()),
            Outcome::Conflict { .. } => continue,
            other => return Err(format!("{other:?}")),
        }
    }
    Err(format!("{key} keeps changing"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: &str = "
mode: stateful
replicas: 2
replication:
  plugin: postgres
  restore: { from: pg, to: '2026-11-25T10:00:00Z' }
pod:
  onPartition: stop
  containers:
    - name: pg
      image: pg:17
      volumeMounts: [{ name: data, path: /var/db/pg/ }]
  volumes:
    - { name: data, claim: { size: 1Gi, backup: { target: vault, every: 1h, keep: 3 } } }
";

    fn spec(yaml: &str) -> ServiceSpec {
        legion_api::parse(&serde_yaml_ng::from_str::<serde_json::Value>(yaml).unwrap()).unwrap()
    }

    #[test]
    fn a_restore_finds_its_targets_volumes_and_log_from_the_specs() {
        let new = spec(DB);
        let old = spec(
            &DB.replace(
                "restore: { from: pg, to: '2026-11-25T10:00:00Z' }",
                "archive: { target: wal }",
            )
            .replace("replicas: 2", "replicas: 3"),
        );
        let p = plan("pg2", &new, Some(&old)).unwrap();
        assert_eq!(
            p,
            Plan {
                claim: "data".into(),
                volume: "data-pg2-0".into(),
                log_target: "wal".into(),
                backup_target: "vault".into(),
                sources: vec!["data-pg-0".into(), "data-pg-1".into(), "data-pg-2".into()],
                log: "/var/db/pg/legion_restore".into(),
            }
        );
        // `from` gone, and no archive of its own: the log's target is to be
        // named.
        let e = plan("pg2", &new, None).unwrap_err();
        assert!(e.contains("restore.target"), "{e}");
        let named = spec(&DB.replace("from: pg,", "from: pg, target: wal, backup: data-pg-1@x,"));
        let p = plan("pg2", &named, None).unwrap();
        assert_eq!(
            (p.log_target.as_str(), p.sources.clone()),
            ("wal", vec!["data-pg-1".to_owned()])
        );
    }

    #[test]
    fn the_newest_backup_before_the_time_is_restored() {
        let want: Restore =
            serde_json::from_value(serde_json::json!({"from": "pg", "to": "2026-11-25T10:00:00Z"}))
                .unwrap();
        let listed = vec![
            (
                "data-pg-0".to_owned(),
                vec![
                    "lab/default.data-pg-0/legion-bk-vault-20261125T080000Z.zfs".to_owned(),
                    "lab/default.data-pg-0/legion-bk-vault-20261125T110000Z.zfs".to_owned(),
                ],
            ),
            (
                "data-pg-1".to_owned(),
                vec!["lab/default.data-pg-1/legion-bk-vault-20261125T093000Z.zfs".to_owned()],
            ),
        ];
        let (vol, held, taken) = choose(&want, &listed).unwrap();
        assert_eq!(vol, "data-pg-1");
        assert!(held.ends_with("20261125T093000Z.zfs"));
        assert_eq!(
            Some(taken),
            legion_api::volume::iso_ms("2026-11-25T09:30:00Z")
        );
        // Before every backup: none to start from.
        let early = Restore {
            to: Some("2026-11-25T07:00:00Z".into()),
            ..want.clone()
        };
        assert!(
            choose(&early, &listed)
                .unwrap_err()
                .contains("taken before")
        );
        // Named: that one, if it was taken before the time.
        let named = Restore {
            backup: Some("data-pg-0@legion-bk-vault-20261125T080000Z".into()),
            ..want.clone()
        };
        assert_eq!(choose(&named, &listed).unwrap().0, "data-pg-0");
        let late = Restore {
            backup: Some("data-pg-0@legion-bk-vault-20261125T110000Z".into()),
            ..want.clone()
        };
        assert!(choose(&late, &listed).unwrap_err().contains("after"));
        // No time: the newest of all.
        let end = Restore { to: None, ..want };
        assert!(choose(&end, &listed).unwrap().1.ends_with("110000Z.zfs"));
    }
}
