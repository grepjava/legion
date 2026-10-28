//! Admission against namespace jail allowances (DESIGN.md §9): a Service,
//! Job or CronJob whose containers loosen their jails further than their
//! namespace's `security.allow` is refused. A namespace is cluster-scoped,
//! so only the cluster admin decides what its teams may loosen.

use legion_api::admit::NamespaceSpec;
use legion_api::job::{CronJobSpec, JobSpec};
use legion_api::security::Loosening;
use legion_api::service::ServiceSpec;
use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_store::Store;
use tonic::Status;

pub(crate) fn check(store: &Store, desired: &Object) -> Result<(), Status> {
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
    let key = ObjectKey::new(Kind::Namespace, CLUSTER_SCOPE, namespace);
    let allowed = match store.get(&key).map_err(internal)? {
        Some(o) => {
            legion_api::parse::<NamespaceSpec>(&o.spec)
                .map_err(internal)?
                .security
        }
        None => Default::default(),
    };
    let refused = allowed.refused(&pod);
    if refused.is_empty() {
        return Ok(());
    }
    let names: Vec<&str> = refused.iter().map(|l| Loosening::as_str(*l)).collect();
    Err(Status::permission_denied(format!(
        "namespace {namespace} does not allow its containers' jails {} \
         (the cluster admin sets spec.security.allow on the Namespace)",
        names.join(", ")
    )))
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}
