//! Changes made of several API calls, shared by the CLI (`legion
//! rollback`, `legion top`) and the console's gateway, so both make them
//! the same way. Each runs as whoever the client speaks for, and each
//! writes only over the object as it just read it: another write meanwhile
//! (a controller's status, a node's heartbeat) has it read again.

use std::future::Future;

use legion_api::service::ServiceStatus;
use legion_api::{Kind, Object};
use legion_proto::v1 as pb;
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{ApplyRequest, ObjectRef};
use tonic::Status;
use tonic::transport::Channel;

/// The calls the changes are made of; the CLI's client retries them while
/// the API is unavailable.
pub trait ObjectsApi {
    fn get(&mut self, r: ObjectRef) -> impl Future<Output = Result<pb::Object, Status>> + Send;
    fn apply(
        &mut self,
        r: ApplyRequest,
    ) -> impl Future<Output = Result<pb::ApplyResponse, Status>> + Send;
    fn delete(
        &mut self,
        r: pb::DeleteRequest,
    ) -> impl Future<Output = Result<pb::DeleteResponse, Status>> + Send;
}

impl ObjectsApi for ObjectsClient<Channel> {
    async fn get(&mut self, r: ObjectRef) -> Result<pb::Object, Status> {
        Ok(ObjectsClient::get(self, r).await?.into_inner())
    }

    async fn apply(&mut self, r: ApplyRequest) -> Result<pb::ApplyResponse, Status> {
        Ok(ObjectsClient::apply(self, r).await?.into_inner())
    }

    async fn delete(&mut self, r: pb::DeleteRequest) -> Result<pb::DeleteResponse, Status> {
        Ok(ObjectsClient::delete(self, r).await?.into_inner())
    }
}

fn read(o: pb::Object) -> Result<Object, Status> {
    Object::try_from(o).map_err(|e| Status::internal(e.to_string()))
}

/// Writes `object` if it is still at the version it was read at: `None`
/// when it moved on, to read it again.
async fn write_over(
    api: &mut impl ObjectsApi,
    object: &Object,
) -> Result<Option<pb::ApplyResponse>, Status> {
    match api
        .apply(ApplyRequest {
            object: Some(object.into()),
            expected_version: Some(object.metadata.version),
            ..Default::default()
        })
        .await
    {
        Ok(r) => Ok(Some(r)),
        Err(s) if s.code() == tonic::Code::Aborted => Ok(None),
        Err(s) => Err(s),
    }
}

/// Sets `field` of an object's spec to `value`. Returns the object's key,
/// and whether the field changed.
pub async fn set_spec(
    api: &mut impl ObjectsApi,
    r#ref: ObjectRef,
    field: &str,
    value: serde_json::Value,
) -> Result<(String, bool), Status> {
    loop {
        let mut object = read(api.get(r#ref.clone()).await?)?;
        if object.spec[field] == value {
            return Ok((object.key().to_string(), false));
        }
        object.spec[field] = value.clone();
        if write_over(api, &object).await?.is_some() {
            return Ok((object.key().to_string(), true));
        }
    }
}

/// Sets (`Some`) or removes (`None`) labels of an object; its key, and
/// whether anything changed.
pub async fn set_labels(
    api: &mut impl ObjectsApi,
    r#ref: ObjectRef,
    changes: &[(String, Option<String>)],
) -> Result<(String, bool), Status> {
    loop {
        let mut object = read(api.get(r#ref.clone()).await?)?;
        let before = object.metadata.labels.clone();
        for (k, v) in changes {
            match v {
                Some(v) => object.metadata.labels.insert(k.clone(), v.clone()),
                None => object.metadata.labels.remove(k),
            };
        }
        if object.metadata.labels == before {
            return Ok((object.key().to_string(), false));
        }
        if write_over(api, &object).await?.is_some() {
            return Ok((object.key().to_string(), true));
        }
    }
}

/// Puts a Service back on the spec before its latest update; what was
/// done, for the user.
pub async fn rollback(
    api: &mut impl ObjectsApi,
    namespace: String,
    service: String,
) -> Result<String, Status> {
    let r#ref = ObjectRef {
        kind: Kind::Service.to_string(),
        namespace,
        name: service,
    };
    // The generation the rollback undoes the latest update of.
    let mut generation = None;
    loop {
        let mut object = read(api.get(r#ref.clone()).await?)?;
        if generation.is_some_and(|g| g != object.metadata.generation) {
            return Err(Status::aborted(format!(
                "{} changed meanwhile; nothing rolled back",
                object.key()
            )));
        }
        generation = Some(object.metadata.generation);
        let status: ServiceStatus =
            serde_json::from_value(object.status.clone()).unwrap_or_default();
        let Some(update) = status.update else {
            return Err(Status::failed_precondition(format!(
                "{} has had no update to roll back",
                object.key()
            )));
        };
        let previous = serde_json::to_value(&update.previous).expect("a spec serialises");
        if previous == object.spec {
            return Err(Status::failed_precondition(format!(
                "{} already runs the spec before its latest update",
                object.key()
            )));
        }
        object.spec = previous;
        if let Some(resp) = write_over(api, &object).await? {
            return Ok(format!(
                "{} rolling back (version {})",
                object.key(),
                resp.object.unwrap_or_default().version
            ));
        }
    }
}

/// A user made by `create_user`: their credentials, and what became of the
/// RoleBinding asked for with them.
pub struct NewUser {
    pub bundle: legion_pki::Bundle,
    /// `created`, `updated` or `unchanged`.
    pub binding: Option<&'static str>,
}

fn applied(r: &pb::ApplyResponse) -> &'static str {
    match r.result() {
        pb::ApplyResult::Created => "created",
        pb::ApplyResult::Updated => "updated",
        pb::ApplyResult::Unchanged => "unchanged",
        pb::ApplyResult::Unspecified => "applied",
    }
}

/// `legion user create`: credentials for user `name`, valid `days` (0 for
/// the default), and with `binding` (a role in a namespace) their
/// RoleBinding too. The binding comes first: one refused leaves no
/// credentials behind to block trying again. Credentials that fail put the
/// binding back as it was.
pub async fn create_user(
    objects: &mut impl ObjectsApi,
    cluster: &mut ClusterClient<Channel>,
    name: &str,
    binding: Option<(&str, &str)>,
    days: u32,
) -> Result<NewUser, Status> {
    let mut bound = None;
    let mut binding_result = None;
    if let Some((role, namespace)) = binding {
        if legion_api::rbac::NamespaceRole::parse(role).is_none() {
            return Err(Status::invalid_argument(format!(
                "role {role:?}: one of view, edit and admin"
            )));
        }
        let spec = serde_json::json!({ "user": name, "role": role });
        let mut wanted = Object::new(Kind::RoleBinding, namespace, name, spec);
        let before = match objects.get((&wanted.key()).into()).await {
            Ok(o) => Some(read(o)?),
            Err(s) if s.code() == tonic::Code::NotFound => None,
            Err(s) => return Err(s),
        };
        // An apply sets the labels too: a binding there keeps its.
        if let Some(before) = &before {
            wanted.metadata.labels = before.metadata.labels.clone();
        }
        let r = objects
            .apply(ApplyRequest {
                object: Some((&wanted).into()),
                ..Default::default()
            })
            .await?;
        binding_result = Some(applied(&r));
        bound = Some((wanted, before));
    }
    let made = async {
        let (key_pem, csr_pem) =
            legion_pki::request().map_err(|e| Status::internal(e.to_string()))?;
        let issued = cluster
            .issue_user(pb::IssueUserRequest {
                name: name.to_owned(),
                csr_pem,
                days,
            })
            .await?
            .into_inner();
        Ok::<_, Status>(legion_pki::Bundle {
            cert_pem: issued.cert_pem,
            key_pem,
            ca_pem: issued.ca_pem,
        })
    }
    .await;
    match (made, bound) {
        (Ok(bundle), _) => Ok(NewUser {
            bundle,
            binding: binding_result,
        }),
        (Err(e), None) => Err(e),
        (Err(e), Some((wanted, before))) => {
            let undone = match before {
                Some(before) => {
                    let mut back = Object::new(
                        Kind::RoleBinding,
                        &before.metadata.namespace,
                        &before.metadata.name,
                        before.spec,
                    );
                    back.metadata.labels = before.metadata.labels;
                    objects
                        .apply(ApplyRequest {
                            object: Some((&back).into()),
                            ..Default::default()
                        })
                        .await
                        .map(drop)
                }
                None => objects
                    .delete(pb::DeleteRequest {
                        r#ref: Some((&wanted.key()).into()),
                        expected_version: None,
                    })
                    .await
                    .map(drop),
            };
            match undone {
                Ok(()) => Err(e),
                Err(u) => Err(Status::new(
                    e.code(),
                    format!(
                        "{}; {} was not put back: {}",
                        e.message(),
                        wanted.key(),
                        u.message()
                    ),
                )),
            }
        }
    }
}
