//! Admission against namespace quotas (DESIGN.md §4.2): a Service or Volume
//! is refused when what its namespace would then ask for in all goes over
//! the quota. The scheduler holds tasks to the quota again as it places
//! them; volumes are held to it only here, so the API server admits and
//! writes quota-counted objects one at a time.

use legion_api::admit::NamespaceSpec;
use legion_api::quota::{Quota, Usage, describe};
use legion_api::service::{ServiceMode, ServiceSpec};
use legion_api::volume::VolumeSpec;
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
    if !counts(desired.kind) {
        return Ok(());
    }
    let namespace = &desired.metadata.namespace;
    let Some(quota) = quota_of(store, namespace)? else {
        return Ok(());
    };
    let others = store
        .list(desired.kind, Some(namespace))
        .map_err(internal)?
        .objects;
    let mut before = Usage::default();
    let mut after = Usage::default();
    match desired.kind {
        Kind::Service => {
            let nodes = store
                .list(Kind::Node, None)
                .map_err(internal)?
                .objects
                .len() as u32;
            let spec: ServiceSpec = legion_api::parse(&desired.spec).map_err(internal)?;
            needs_requests(&quota, &spec, namespace)?;
            for o in &others {
                let Ok(s) = legion_api::parse::<ServiceSpec>(&o.spec) else {
                    continue;
                };
                add_service(&mut before, &s, nodes);
                if o.metadata.name != desired.metadata.name {
                    add_service(&mut after, &s, nodes);
                }
            }
            add_service(&mut after, &spec, nodes);
        }
        Kind::Volume => {
            let spec: VolumeSpec = legion_api::parse(&desired.spec).map_err(internal)?;
            if quota.storage.is_some() && spec.size.is_none() {
                return Err(Status::failed_precondition(format!(
                    "namespace {namespace} has a storage quota: the volume needs a size"
                )));
            }
            for o in &others {
                let Ok(v) = legion_api::parse::<VolumeSpec>(&o.spec) else {
                    continue;
                };
                before.add_volume(v.size);
                if o.metadata.name != desired.metadata.name {
                    after.add_volume(v.size);
                }
            }
            after.add_volume(spec.size);
        }
        _ => unreachable!("counted kinds"),
    }
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

/// Under a CPU or memory quota every task must say what it needs, or the
/// quota would not hold it.
fn needs_requests(quota: &Quota, spec: &ServiceSpec, namespace: &str) -> Result<(), Status> {
    let r = spec.pod.requests();
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
        _ => s.replicas.unwrap_or(1),
    };
    u.add_tasks(s.pod.requests(), n);
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
