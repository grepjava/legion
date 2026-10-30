//! Admission against namespace quotas (DESIGN.md §4.2): a Service or Volume
//! is refused when what its namespace would then ask for in all goes over
//! the quota. The scheduler holds tasks to the quota again as it places
//! them; volumes are held to it only here, so the API server admits and
//! writes quota-counted objects one at a time. A stateful service's claims
//! count as the Volumes they will make, before the controller makes them.

use std::collections::BTreeMap;

use legion_api::admit::NamespaceSpec;
use legion_api::job::{CronJobSpec, JobSpec};
use legion_api::quantity::Bytes;
use legion_api::quota::{Quota, Usage, describe};
use legion_api::service::{PodSpec, ServiceMode, ServiceSpec, claim_volume};
use legion_api::volume::{VolumeDriver, VolumeSpec};
use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_store::Store;
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
    let volumes: Vec<(String, Option<Bytes>)> = list(Kind::Volume)?
        .into_iter()
        .filter_map(|o| {
            let v: VolumeSpec = legion_api::parse(&o.spec).ok()?;
            Some((o.metadata.name, v.size))
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
            volumes_after.retain(|(n, _)| n != name);
            volumes_after.push((name.clone(), spec.size));
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

/// What a namespace's Services and Volumes take: the services' tasks, and
/// the Volumes, with those stateful services' claims would make.
fn usage(
    services: &[(String, ServiceSpec)],
    volumes: &[(String, Option<Bytes>)],
    nodes: u32,
) -> Usage {
    let mut u = Usage::default();
    let mut all: BTreeMap<String, Option<Bytes>> = volumes.iter().cloned().collect();
    for (name, s) in services {
        add_service(&mut u, s, nodes);
        if s.mode != ServiceMode::Stateful {
            continue;
        }
        for n in 0..s.replicas.unwrap_or(1) {
            for (claim, src) in s.pod.claims() {
                all.entry(claim_volume(claim, name, n)).or_insert(src.size);
            }
        }
    }
    for size in all.values() {
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
    let r = pod.requests();
    for (limited, missing, what) in [
        (quota.cpu.is_some(), r.cpu.0 == 0, "cpu"),
        (quota.memory.is_some(), r.memory.0 == 0, "memory"),
    ] {
        if limited && missing {
            return Err(Status::failed_precondition(format!(
                "namespace {namespace} has a {what} quota: \
                 the pod's containers must request {what} (resources.requests.{what})"
            )));
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
