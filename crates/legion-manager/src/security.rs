//! Admission against namespace jail allowances (DESIGN.md §9): a Service,
//! Job or CronJob whose containers loosen their jails further than their
//! namespace's `security.allow` is refused. A namespace is cluster-scoped,
//! so only the cluster admin decides what its teams may loosen.

use legion_api::admit::NamespaceSpec;
use legion_api::job::{CronJobSpec, JobSpec};
use legion_api::service::ServiceSpec;
use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_store::Store;
use tonic::Status;

pub(crate) fn check(store: &Store, desired: &Object) -> Result<(), Status> {
    if desired.kind == Kind::Volume {
        return check_volume(store, desired);
    }
    let pod = match desired.kind {
        Kind::Service => legion_api::parse::<ServiceSpec>(&desired.spec).map(|s| s.pod),
        Kind::Job => legion_api::parse::<JobSpec>(&desired.spec).map(|s| s.pod),
        Kind::CronJob => legion_api::parse::<CronJobSpec>(&desired.spec).map(|s| s.job.pod),
        _ => return Ok(()),
    }
    .map_err(internal)?;
    if legion_api::security::pod_loosenings(&pod).is_empty() {
        return Ok(());
    }
    let namespace = &desired.metadata.namespace;
    match allowed(store, namespace)?.refusal(namespace, &pod) {
        None => Ok(()),
        Some(why) => Err(Status::permission_denied(format!(
            "{why} (the cluster admin sets spec.security.allow on the Namespace)"
        ))),
    }
}

/// A Volume the host mounts (NFS, iSCSI, a plugin's) only where its
/// namespace allows that driver.
fn check_volume(store: &Store, desired: &Object) -> Result<(), Status> {
    let spec =
        legion_api::parse::<legion_api::volume::VolumeSpec>(&desired.spec).map_err(internal)?;
    let Some(needs) = legion_api::security::Loosening::of_volume(spec.driver) else {
        return Ok(());
    };
    let namespace = &desired.metadata.namespace;
    if allowed(store, namespace)?.allow.contains(&needs) {
        return Ok(());
    }
    Err(Status::permission_denied(format!(
        "namespace {namespace} does not allow {} Volumes: the host mounts them ({}; the cluster admin sets spec.security.allow on the Namespace)",
        spec.driver.as_str(),
        needs.as_str()
    )))
}

/// What `namespace` allows.
fn allowed(
    store: &Store,
    namespace: &str,
) -> Result<legion_api::security::NamespaceSecurity, Status> {
    let key = ObjectKey::new(Kind::Namespace, CLUSTER_SCOPE, namespace);
    Ok(match store.get(&key).map_err(internal)? {
        Some(o) => {
            legion_api::parse::<NamespaceSpec>(&o.spec)
                .map_err(internal)?
                .security
        }
        None => Default::default(),
    })
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
