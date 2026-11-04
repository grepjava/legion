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
    check_against(at(store, node)?, node, issued)
}

/// [`check`] against `removed`, the node's removal as read.
pub(crate) fn check_against(
    removed: Option<u64>,
    node: &str,
    issued: SystemTime,
) -> Result<(), Status> {
    let Some(at) = removed else {
        return Ok(());
    };
    let issued = issued
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if issued <= at {
        return Err(Status::permission_denied(format!(
            "node {node} was removed from the cluster, or joined again, since this certificate was issued; to come back it joins again"
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
/// the node cannot come back in between. Returns what [`forget`] needs to
/// undo it.
pub(crate) async fn record(api: &ApiService, node: &str) -> Result<Recorded, Status> {
    change(api, |removed| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Never earlier than a removal recorded before (a join's may be a
        // second ahead): what that one refused stays refused.
        let before = removed.get(node).copied();
        let at = before.map_or(now, |b| now.max(b));
        removed.insert(node.to_owned(), at);
        Some(Recorded { at, before })
    })
    .await
    .map(|r| r.expect("always recorded"))
}

/// Refuses every certificate of `node` issued so far, for a join to
/// certify it anew: a node that joined and has not registered yet, or
/// joins again, leaves only the newest certificate in use. Recorded after
/// any earlier record, even within a second, so the certificate a join
/// issues the second after (see [`Recorded::at`]) is refused by the next.
pub(crate) async fn supersede(api: &ApiService, node: &str) -> Result<Recorded, Status> {
    change(api, |removed| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let before = removed.get(node).copied();
        let at = before.map_or(now, |b| now.max(b + 1));
        removed.insert(node.to_owned(), at);
        Some(Recorded { at, before })
    })
    .await
    .map(|r| r.expect("always recorded"))
}

/// When `node`'s certificates were last refused up to, if ever.
pub(crate) fn at(store: &Store, node: &str) -> Result<Option<u64>, Status> {
    Ok(read(store)?.0.get(node).copied())
}

/// A removal [`record`] wrote: when, and the earlier one it replaced.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Recorded {
    at: u64,
    before: Option<u64>,
}

impl Recorded {
    /// The second certificates of the node were refused up to, inclusive.
    pub(crate) fn at(&self) -> u64 {
        self.at
    }

    /// This record, made right after `earlier` (a retry's), undone to what
    /// came before both: unless another record came between them, which
    /// stands.
    pub(crate) fn after(self, earlier: Recorded) -> Recorded {
        if self.before == Some(earlier.at) {
            Recorded {
                at: self.at,
                before: earlier.before,
            }
        } else {
            self
        }
    }
}

/// Undoes [`record`] of `node` for a removal that did not happen, unless
/// it was recorded again since: an earlier removal stands, so certificates
/// from before it stay refused.
pub(crate) async fn forget(api: &ApiService, node: &str, r: Recorded) -> Result<(), Status> {
    change(api, |removed| {
        if removed.get(node) != Some(&r.at) {
            return None;
        }
        match r.before {
            Some(before) => removed.insert(node.to_owned(), before),
            None => removed.remove(node),
        };
        Some(())
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

#[cfg(test)]
mod tests {
    use super::Recorded;

    #[test]
    fn a_retry_undoes_to_before_both_records_unless_another_came_between() {
        let first = Recorded {
            at: 10,
            before: Some(4),
        };
        let retry = Recorded {
            at: 11,
            before: Some(10),
        };
        let undo = retry.after(first);
        assert_eq!((undo.at, undo.before), (11, Some(4)));
        // Another join recorded 11 between: the retry undoes to that.
        let retry = Recorded {
            at: 12,
            before: Some(11),
        };
        let undo = retry.after(first);
        assert_eq!((undo.at, undo.before), (12, Some(11)));
    }
}
