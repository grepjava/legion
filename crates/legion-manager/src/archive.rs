//! A replicated database's log archiving (DESIGN.md 6.9,
//! `replication.archive`), run by the leader: every 15 s, for each member
//! whose plugin says where it spools its log (`spool` in its `status`), the
//! node holding the member's Volume is asked to ship the spool to the
//! target (`ArchiveRun`), and once an hour one of them to prune the
//! target to `keep`. What came of it is the Service's `status.archive`,
//! which only this writes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::replication::{ArchiveStatus, ReplicationStatus};
use legion_api::service::ServiceSpec;
use legion_api::volume::VolumeStatus;
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::session_open::Action;
use legion_store::{Command, Outcome, Precondition, Store};
use tokio::task::JoinSet;

use crate::backups::{access, open, ready_nodes, said};
use crate::session::Hub;

const PERIOD: Duration = Duration::from_secs(15);
/// How often the target is pruned to `keep`.
const PRUNE_EVERY_MS: i64 = 3_600_000;

/// What a node said a run did.
#[derive(serde::Deserialize, Default)]
struct Ran {
    #[serde(default)]
    last: String,
    #[serde(default)]
    pending: u32,
}

/// Where member `ordinal`'s spool is: the Volume (`<claim>-<service>-<n>`)
/// holding the path `spool` of the container `container`, and the path in
/// it. None when no claim of the pod is mounted above it.
pub(crate) fn spool_in(
    spec: &ServiceSpec,
    container: &str,
    service: &str,
    ordinal: u32,
    spool: &str,
) -> Option<(String, String)> {
    let c = spec.pod.containers.iter().find(|c| c.name == container)?;
    let claims: HashSet<&str> = spec.pod.claims().map(|(name, _)| name).collect();
    let (mount, rel) = c
        .volume_mounts
        .iter()
        .filter(|m| claims.contains(m.name.as_str()))
        .filter_map(|m| {
            let base = m.path.trim_end_matches('/');
            let rest = spool.strip_prefix(base)?;
            (rest.is_empty() || rest.starts_with('/')).then(|| (m, rest.trim_start_matches('/')))
        })
        .max_by_key(|(m, _)| m.path.len())?;
    if rel.split('/').any(|p| p == "..") {
        return None;
    }
    Some((
        legion_api::service::claim_volume(&mount.name, service, ordinal),
        rel.to_owned(),
    ))
}

type Done = (ObjectKey, String, String, i64, bool, Result<Ran, String>);

pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>) {
    let mut runs: JoinSet<Done> = JoinSet::new();
    let mut running: HashSet<(ObjectKey, String)> = HashSet::new();
    // When each member's last run failed, and how many in a row: tried
    // again after a backoff.
    let mut failed: HashMap<(ObjectKey, String), (i64, u32)> = HashMap::new();
    loop {
        while let Some(done) = runs.try_join_next() {
            let Ok((key, target, member, at_ms, pruning, result)) = done else {
                continue;
            };
            running.remove(&(key.clone(), member.clone()));
            match &result {
                Ok(_) => {
                    failed.remove(&(key.clone(), member.clone()));
                }
                Err(_) => {
                    let f = failed
                        .entry((key.clone(), member.clone()))
                        .or_insert((0, 0));
                    *f = (at_ms, f.1 + 1);
                }
            }
            if let Err(e) = record(store, &key, &target, &member, at_ms, pruning, &result).await {
                tracing::debug!(service = %key, "recording the archive: {e}");
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
        let Some(ready) = ready_nodes(store) else {
            tokio::time::sleep(PERIOD).await;
            continue;
        };
        let now = crate::console::now_ms();
        for s in services {
            let Ok(spec) = legion_api::parse::<ServiceSpec>(&s.spec) else {
                continue;
            };
            let Some(r) = spec.replication.as_ref() else {
                continue;
            };
            let Some(archive) = r.archive.as_ref() else {
                continue;
            };
            let key = s.key();
            let (ns, svc) = (&s.metadata.namespace, &s.metadata.name);
            let st: Option<ReplicationStatus> = s
                .status
                .get("replication")
                .and_then(|v| serde_json::from_value(v.clone()).ok());
            let Some(st) = st else { continue };
            let previous: Option<ArchiveStatus> = s
                .status
                .get("archive")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .filter(|a: &ArchiveStatus| a.target == archive.target);
            let containers: Vec<&str> = spec
                .pod
                .containers
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            let Some(container) = r.container(&containers) else {
                continue;
            };
            let mut prune = previous
                .as_ref()
                .and_then(|a| a.pruned_ms)
                .is_none_or(|at| at + PRUNE_EVERY_MS <= now)
                && !running.iter().any(|(k, _)| *k == key);
            for (member, m) in &st.members {
                let Some(spool) = m.status.spool.as_deref() else {
                    continue;
                };
                let id = (key.clone(), member.clone());
                if running.contains(&id) {
                    continue;
                }
                if let Some((at, n)) = failed.get(&id)
                    && *at + crate::migrate::backoff(*n).as_millis() as i64 > now
                {
                    continue;
                }
                let Some(ordinal) = member
                    .strip_prefix(svc.as_str())
                    .and_then(|r| r.strip_prefix('-'))
                    .and_then(|n| n.parse::<u32>().ok())
                else {
                    continue;
                };
                let Some((volume, path)) = spool_in(&spec, container, svc, ordinal, spool) else {
                    tracing::debug!(service = %key, %member, %spool, "no claim of the pod holds the spool");
                    continue;
                };
                let vstatus: Option<VolumeStatus> = store
                    .get(&ObjectKey::new(Kind::Volume, ns, &volume))
                    .ok()
                    .flatten()
                    .and_then(|v| serde_json::from_value(v.status).ok());
                let Some(node) = vstatus
                    .map(|v| v.node)
                    .filter(|n| !n.is_empty() && ready.contains(n))
                else {
                    continue;
                };
                let target = access(store, &archive.target);
                let (hub, k, name, of) = (
                    hub.clone(),
                    key.clone(),
                    archive.target.clone(),
                    format!("{ns}.{svc}"),
                );
                let (keep, pruning) = (archive.keep.as_std().as_secs(), prune);
                prune = false;
                let member = member.clone();
                running.insert(id);
                runs.spawn(async move {
                    let result = match target {
                        Ok(t) => {
                            run(
                                &hub,
                                &k.namespace,
                                &node,
                                legion_proto::v1::ArchiveRun {
                                    volume,
                                    path,
                                    target: Some(t),
                                    of,
                                    keep_seconds: keep,
                                    prune: pruning,
                                },
                            )
                            .await
                        }
                        Err(e) => Err(e),
                    };
                    (k, name, member, now, pruning, result)
                });
            }
        }
        tokio::time::sleep(PERIOD).await;
    }
}

async fn run(
    hub: &Arc<Hub>,
    namespace: &str,
    node: &str,
    o: legion_proto::v1::ArchiveRun,
) -> Result<Ran, String> {
    let mut a = open(hub, node, namespace, Action::ArchiveRun(o)).await?;
    let (out, _) = said(&mut a, node).await?;
    Ok(serde_json::from_slice(&out).unwrap_or_default())
}

/// What a run of `member`'s came to, in the Service's `status.archive`.
async fn record(
    store: &Store,
    key: &ObjectKey,
    target: &str,
    member: &str,
    at_ms: i64,
    pruning: bool,
    result: &Result<Ran, String>,
) -> Result<(), String> {
    for _ in 0..5 {
        let Some(mut o) = store.get(key).map_err(|e| e.to_string())? else {
            return Ok(());
        };
        let mut a: ArchiveStatus = o
            .status
            .get("archive")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .filter(|a: &ArchiveStatus| a.target == target)
            .unwrap_or_else(|| ArchiveStatus {
                target: target.to_owned(),
                ..Default::default()
            });
        apply(&mut a, member, at_ms, pruning, result);
        if !o.status.is_object() {
            o.status = serde_json::json!({});
        }
        o.status["archive"] = serde_json::to_value(&a).map_err(|e| e.to_string())?;
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

fn apply(
    a: &mut ArchiveStatus,
    member: &str,
    at_ms: i64,
    pruning: bool,
    result: &Result<Ran, String>,
) {
    match result {
        Ok(ran) => {
            if ran.last > a.last {
                a.last = ran.last.clone();
            }
            a.shipped_ms = Some(at_ms);
            a.pending = ran.pending;
            a.failures = 0;
            a.message.clear();
            if pruning {
                a.pruned_ms = Some(at_ms);
            }
        }
        Err(e) => {
            a.failures += 1;
            a.message = format!("{member}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: &str = "
mode: stateful
replicas: 2
replication: { plugin: postgres, archive: { target: offsite } }
pod:
  onPartition: stop
  containers:
    - name: pg
      image: pg:17
      volumeMounts: [{ name: pg, path: /var/db/pg }, { name: conf, path: /var/db/pg/data/conf.d }]
  volumes:
    - { name: pg, claim: { size: 1Gi } }
    - { name: conf, tmpfs: { size: 1Mi } }
";

    fn spec() -> ServiceSpec {
        legion_api::parse(&serde_yaml_ng::from_str::<serde_json::Value>(DB).unwrap()).unwrap()
    }

    #[test]
    fn a_members_spool_is_found_in_the_claim_mounted_above_it() {
        let s = spec();
        assert_eq!(
            spool_in(&s, "pg", "db", 1, "/var/db/pg/data/legion_wal"),
            Some(("pg-db-1".to_owned(), "data/legion_wal".to_owned()))
        );
        // Not under a claim: nowhere Legion can ship from.
        assert_eq!(spool_in(&s, "pg", "db", 1, "/tmp/legion_wal"), None);
        assert_eq!(spool_in(&s, "pg", "db", 1, "/var/db/pgx/legion_wal"), None);
        assert_eq!(spool_in(&s, "pg", "db", 1, "/var/db/pg/../etc"), None);
        assert_eq!(
            spool_in(&s, "other", "db", 1, "/var/db/pg/data/legion_wal"),
            None
        );
    }

    #[test]
    fn a_run_moves_the_last_on_and_a_failure_is_counted_and_said() {
        let mut a = ArchiveStatus {
            target: "offsite".into(),
            last: "000000010000000000000005".into(),
            ..Default::default()
        };
        // A replica's run, behind the primary's: the newest stays.
        apply(
            &mut a,
            "db-1",
            10,
            false,
            &Ok(Ran {
                last: "000000010000000000000003".into(),
                pending: 0,
            }),
        );
        assert_eq!(
            (a.last.as_str(), a.shipped_ms, a.pruned_ms),
            ("000000010000000000000005", Some(10), None)
        );
        apply(&mut a, "db-0", 20, false, &Err("refused".into()));
        apply(&mut a, "db-0", 30, false, &Err("refused".into()));
        assert_eq!((a.failures, a.message.as_str()), (2, "db-0: refused"));
        apply(
            &mut a,
            "db-0",
            40,
            true,
            &Ok(Ran {
                last: "000000010000000000000006".into(),
                pending: 2,
            }),
        );
        assert_eq!(a.last, "000000010000000000000006");
        assert_eq!((a.failures, a.pending, a.pruned_ms), (0, 2, Some(40)));
        assert!(a.message.is_empty());
    }
}
