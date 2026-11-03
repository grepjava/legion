//! Nodes removed from the cluster, and when: a worker's certificate issued
//! before its node was removed is refused, renewals included, so deleting
//! a Node takes the node out until it joins again (with a token, for a new
//! certificate). Kept as a Config in the system namespace, by node name.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use legion_api::data::DataSpec;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, Outcome, Precondition, Store};
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

const NAME: &str = "removed-nodes";

fn key() -> ObjectKey {
    ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, NAME)
}

/// The removals, by node: seconds since the epoch.
fn read(store: &Store) -> Result<(BTreeMap<String, u64>, Option<u64>), Status> {
    let Some(o) = store
        .get(&key())
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return Ok((BTreeMap::new(), None));
    };
    let spec: DataSpec =
        serde_json::from_value(o.spec).map_err(|e| Status::internal(e.to_string()))?;
    let removed = serde_json::from_str(spec.text.as_deref().unwrap_or("{}"))
        .map_err(|e| Status::internal(format!("{NAME}: {e}")))?;
    Ok((removed, Some(o.metadata.version)))
}

/// Refuses a worker certificate of `node` issued before the node was removed.
pub(crate) fn check(store: &Store, node: &str, issued: SystemTime) -> Result<(), Status> {
    let (removed, _) = read(store)?;
    let Some(&at) = removed.get(node) else {
        return Ok(());
    };
    let issued = issued
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if issued <= at {
        return Err(Status::permission_denied(format!(
            "node {node} was removed from the cluster; to come back it joins again"
        )));
    }
    Ok(())
}

/// Waits, if `node` was removed this very second, until a certificate
/// issued now would postdate that ([`check`] counts whole seconds).
pub(crate) async fn issue_after(store: &Store, node: &str) -> Result<(), Status> {
    let (removed, _) = read(store)?;
    let Some(&at) = removed.get(node) else {
        return Ok(());
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let after = std::time::Duration::from_secs(at + 1);
    match after.checked_sub(now) {
        None => Ok(()),
        Some(wait) if wait <= std::time::Duration::from_secs(2) => {
            tokio::time::sleep(wait).await;
            Ok(())
        }
        Some(_) => Err(Status::unavailable(format!(
            "node {node} was removed at a time still ahead of this manager's clock; retry"
        ))),
    }
}

/// Records that `node` is being removed, now. Run before its Node goes, so
/// the node cannot come back in between. Returns the time recorded, for
/// [`forget`].
pub(crate) async fn record(api: &ApiService, node: &str) -> Result<u64, Status> {
    change(api, |removed| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        removed.insert(node.to_owned(), now);
        Some(now)
    })
    .await
    .map(|at| at.expect("always recorded"))
}

/// Undoes [`record`] of `node` at `at` for a removal that did not happen,
/// unless it was recorded again since.
pub(crate) async fn forget(api: &ApiService, node: &str, at: u64) -> Result<(), Status> {
    change(api, |removed| {
        if removed.get(node) != Some(&at) {
            return None;
        }
        removed.remove(node);
        Some(at)
    })
    .await
    .map(drop)
}

/// Writes the removals as `edit` leaves them, retried on conflicts; `edit`
/// returning `None` writes nothing.
async fn change<T>(
    api: &ApiService,
    mut edit: impl FnMut(&mut BTreeMap<String, u64>) -> Option<T>,
) -> Result<Option<T>, Status> {
    for _ in 0..10 {
        let (mut removed, version) = read(&api.store)?;
        let Some(done) = edit(&mut removed) else {
            return Ok(None);
        };
        let spec = DataSpec {
            data: None,
            text: Some(serde_json::to_string(&removed).expect("serializes")),
        };
        let object = Object::new(
            Kind::Config,
            SYSTEM_NAMESPACE,
            NAME,
            serde_json::to_value(spec).expect("serializes"),
        );
        let precondition = version.map_or(Precondition::MustNotExist, Precondition::Version);
        match api
            .write(Command::Put {
                object,
                precondition,
            })
            .await?
        {
            Outcome::Written(_) => return Ok(Some(done)),
            Outcome::Conflict { .. } => continue,
            other => return Err(Status::internal(format!("unexpected outcome {other:?}"))),
        }
    }
    Err(Status::aborted(format!(
        "{NAME} is changing too fast; retry"
    )))
}
