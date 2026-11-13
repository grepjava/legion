//! Admission against namespace quotas (DESIGN.md §4.2): a Service or Volume
//! is refused when what its namespace would then ask for in all goes over
//! the quota. The scheduler holds tasks to the quota again as it places
//! them; volumes are held to it only here, so the API server admits and
//! writes quota-counted objects one at a time. A stateful service's claims
//! count as the Volumes they will make, before the controller makes them.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use legion_api::admit::NamespaceSpec;
use legion_api::job::{CronJobSpec, JobSpec};
use legion_api::quantity::Bytes;
use legion_api::quota::{Quota, Usage, describe};
use legion_api::service::{PodSpec, ServiceMode, ServiceSpec, claim_volume};
use legion_api::volume::{VolumeDriver, VolumeSpec};
use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_store::Store;

use crate::controller::plan::LABEL_SERVICE;
use tonic::Status;

/// Whether quotas count objects of `kind`.
pub(crate) fn counts(kind: Kind) -> bool {
    matches!(kind, Kind::Service | Kind::Volume)
}

/// Refuses `desired` if, with it in place of its current version, its
/// namespace would go over its quota.
pub(crate) fn check(store: &Store, desired: &Object) -> Result<(), Status> {
    let namespace = &desired.metadata.namespace;
    // A Job's tasks are held to the quota as they are placed, which only
    // holds if they say what they need.
    let job_pod = match desired.kind {
        Kind::Job => Some(
            legion_api::parse::<JobSpec>(&desired.spec)
                .map_err(internal)?
                .pod,
        ),
        Kind::CronJob => Some(
            legion_api::parse::<CronJobSpec>(&desired.spec)
                .map_err(internal)?
                .job
                .pod,
        ),
        _ => None,
    };
    if let Some(pod) = job_pod {
        return match quota_of(store, namespace)? {
            Some(quota) => needs_requests(&quota, &pod, namespace),
            None => Ok(()),
        };
    }
    if !counts(desired.kind) {
        return Ok(());
    }
    let Some(quota) = quota_of(store, namespace)? else {
        return Ok(());
    };
    let list = |kind| -> Result<Vec<Object>, Status> {
        Ok(store.list(kind, Some(namespace)).map_err(internal)?.objects)
    };
    // Services and Volumes, before and after.
    let services: Vec<(String, ServiceSpec)> = list(Kind::Service)?
        .into_iter()
        .filter_map(|o| Some((o.metadata.name, legion_api::parse(&o.spec).ok()?)))
        .collect();
    let volumes: Vec<CountedVolume> = list(Kind::Volume)?
        .into_iter()
        .filter_map(|o| {
            let v: VolumeSpec = legion_api::parse(&o.spec).ok()?;
            Some(counted(&o, &v))
        })
        .collect();
    let mut services_after = services.clone();
    let mut volumes_after = volumes.clone();
    let name = &desired.metadata.name;
    match desired.kind {
        Kind::Service => {
            let spec: ServiceSpec = legion_api::parse(&desired.spec).map_err(internal)?;
            needs_requests(&quota, &spec.pod, namespace)?;
            services_after.retain(|(n, _)| n != name);
            services_after.push((name.clone(), spec));
        }
        Kind::Volume => {
            let spec: VolumeSpec = legion_api::parse(&desired.spec).map_err(internal)?;
            // Only ZFS volumes take the cluster's storage, and only they
            // may have a size.
            if quota.storage.is_some() && spec.driver == VolumeDriver::Zfs && spec.size.is_none() {
                return Err(Status::failed_precondition(format!(
                    "namespace {namespace} has a storage quota: the volume needs a size"
                )));
            }
            volumes_after.retain(|(n, ..)| n != name);
            volumes_after.push(counted(desired, &spec));
        }
        _ => unreachable!("counted kinds"),
    }
    let nodes = store
        .list(Kind::Node, None)
        .map_err(internal)?
        .objects
        .len() as u32;
    let before = usage(&services, &volumes, nodes);
    let after = usage(&services_after, &volumes_after, nodes);
    let over = quota.exceeded(&before, &after);
    if over.is_empty() {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "over the quota of namespace {namespace}: {}",
            describe(&over)
        )))
    }
}

fn quota_of(store: &Store, namespace: &str) -> Result<Option<Quota>, Status> {
    let key = ObjectKey::new(Kind::Namespace, CLUSTER_SCOPE, namespace);
    let Some(o) = store.get(&key).map_err(internal)? else {
        return Ok(None);
    };
    let spec: NamespaceSpec = legion_api::parse(&o.spec).map_err(internal)?;
    Ok(spec.quota)
}

/// A Volume as quotas count it: its name, its size, and the stateful
/// service whose claims the controller resizes it to, if any.
type CountedVolume = (String, Option<Bytes>, Option<String>);

fn counted(o: &Object, v: &VolumeSpec) -> CountedVolume {
    // As the controller resizes them: its own ZFS ones.
    let owner = o
        .metadata
        .labels
        .get(LABEL_SERVICE)
        .filter(|_| v.driver == VolumeDriver::Zfs)
        .cloned();
    (o.metadata.name.clone(), v.size, owner)
}

/// What a namespace's Services and Volumes take: the services' tasks, and
/// the Volumes, with those stateful services' claims would make.
fn usage(services: &[(String, ServiceSpec)], volumes: &[CountedVolume], nodes: u32) -> Usage {
    let mut u = Usage::default();
    let mut all: BTreeMap<String, (Option<Bytes>, Option<String>)> = volumes
        .iter()
        .map(|(name, size, owner)| (name.clone(), (*size, owner.clone())))
        .collect();
    for (name, s) in services {
        add_service(&mut u, s, nodes);
        if s.mode != ServiceMode::Stateful {
            continue;
        }
        for n in 0..s.replicas.unwrap_or(1) {
            // The controller resizes a claim's own Volume to the claim, so
            // it counts at the claim's size; at its own until then, when
            // larger: a size applied to the Volume holds until the
            // controller's next pass, and a node may give it all.
            for (claim, src) in s.pod.claims() {
                match all.entry(claim_volume(claim, name, n)) {
                    Entry::Vacant(e) => {
                        e.insert((src.size, Some(name.clone())));
                    }
                    Entry::Occupied(mut e) if e.get().1.as_ref() == Some(name) => {
                        e.get_mut().0 = e.get().0.max(src.size);
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
    }
    for (size, _) in all.values() {
        u.add_volume(*size);
    }
    u
}

/// Under a CPU or memory quota every task must say what it needs, or the
/// quota would not hold it; under a storage quota, every claim its size.
fn needs_requests(quota: &Quota, pod: &PodSpec, namespace: &str) -> Result<(), Status> {
    if quota.storage.is_some()
        && let Some((name, _)) = pod.claims().find(|(_, c)| c.size.is_none())
    {
        return Err(Status::failed_precondition(format!(
            "namespace {namespace} has a storage quota: claim {name} needs a size"
        )));
    }
    // Every container: one asking nothing would run outside the quota.
    for c in pod.containers() {
        let r = &c.resources.requests;
        for (limited, missing, what) in [
            (quota.cpu.is_some(), r.cpu.is_none_or(|n| n.0 == 0), "cpu"),
            (
                quota.memory.is_some(),
                r.memory.is_none_or(|n| n.0 == 0),
                "memory",
            ),
        ] {
            if limited && missing {
                return Err(Status::failed_precondition(format!(
                    "namespace {namespace} has a {what} quota: container {} \
                     must request {what} (resources.requests.{what})",
                    c.name
                )));
            }
        }
    }
    Ok(())
}

/// A service's tasks: its replicas, or one per node if global.
fn add_service(u: &mut Usage, s: &ServiceSpec, nodes: u32) {
    let n = match s.mode {
        ServiceMode::Global => nodes,
        // Autoscaled: as many as it may grow to.
        _ => s
            .autoscale
            .as_ref()
            .map_or(s.replicas.unwrap_or(1), |a| a.max),
    };
    u.add_tasks(s.pod.requests(), n);
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

/// What each namespace uses now of what quotas limit: the requests of its
/// tasks placed and not ended, and its Volumes. The metrics and
/// `Cluster.Quotas` both report it.
pub(crate) fn in_use(tasks: &[Object], volumes: &[Object]) -> BTreeMap<String, Usage> {
    use legion_api::task::{TaskSpec, TaskState, TaskStatus};
    let mut used: BTreeMap<String, Usage> = BTreeMap::new();
    for o in tasks {
        let Ok(spec) = legion_api::parse::<TaskSpec>(&o.spec) else {
            continue;
        };
        let status: TaskStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        if status.state != TaskState::Pending && !status.state.is_terminal() {
            used.entry(o.metadata.namespace.clone())
                .or_default()
                .add_tasks(spec.pod.requests(), 1);
        }
    }
    for o in volumes {
        if let Ok(spec) = legion_api::parse::<VolumeSpec>(&o.spec) {
            used.entry(o.metadata.namespace.clone())
                .or_default()
                .add_volume(spec.size);
        }
    }
    used
}

/// Each resource `q` limits, with its limit and what `u` uses of it: CPU
/// in cores, memory and storage in bytes.
pub(crate) fn values(q: &Quota, u: &Usage) -> Vec<(&'static str, f64, f64)> {
    [
        (
            "cpu",
            q.cpu.map(|c| c.0 as f64 / 1000.0),
            u.cpu.0 as f64 / 1000.0,
        ),
        ("memory", q.memory.map(|b| b.0 as f64), u.memory.0 as f64),
        ("tasks", q.tasks.map(f64::from), f64::from(u.tasks)),
        ("volumes", q.volumes.map(f64::from), f64::from(u.volumes)),
        ("storage", q.storage.map(|b| b.0 as f64), u.storage.0 as f64),
    ]
    .into_iter()
    .filter_map(|(r, limit, used)| Some((r, limit?, used)))
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> ServiceSpec {
        let value: serde_json::Value = serde_yaml_ng::from_str(yaml).unwrap();
        legion_api::parse(&value).unwrap()
    }

    #[test]
    fn every_container_requests_what_a_quota_holds() {
        let quota = Quota {
            cpu: Some(legion_api::quantity::Cpu(4000)),
            ..Default::default()
        };
        let pod = |yaml: &str| spec(&format!("pod: {yaml}")).pod;
        let one = pod(
            "{ containers: [{ name: a, image: a, resources: { requests: { cpu: \"1m\" } } }] }",
        );
        needs_requests(&quota, &one, "shop").unwrap();
        let two = pod(
            "{ containers: [{ name: a, image: a, resources: { requests: { cpu: \"1m\" } } }, { name: b, image: b }] }",
        );
        let why = needs_requests(&quota, &two, "shop").unwrap_err();
        assert!(
            why.message().contains("container b must request cpu"),
            "{why}"
        );
    }

    #[test]
    fn claims_count_at_the_size_the_controller_gives_their_volumes() {
        let gb = |n: u64| Some(Bytes(n * 1_000_000_000));
        let db = spec(
            "mode: stateful\nreplicas: 2\npod: { containers: [{ name: db, image: pg }], volumes: [{ name: data, claim: { size: 1G } }] }",
        );
        let services = [("db".to_owned(), db)];
        // Its own Volume grows to the claim, and counts as it is until it
        // shrinks to it (a size applied to it is given until then); one it
        // does not own stays as it is.
        let volumes = [
            ("data-db-0".to_owned(), gb(3), Some("db".to_owned())),
            ("data-db-1".to_owned(), gb(5), None),
        ];
        let u = usage(&services, &volumes, 1);
        assert_eq!(u.storage, Bytes(8_000_000_000));
        let volumes = [
            ("data-db-0".to_owned(), None, Some("db".to_owned())),
            ("data-db-1".to_owned(), gb(5), None),
        ];
        let u = usage(&services, &volumes, 1);
        assert_eq!(u.storage, Bytes(6_000_000_000));
    }
}
