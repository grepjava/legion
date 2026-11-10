//! The managers by name (PLAN.md phase 7): each member's Raft id and
//! address, as the member itself says (`Cluster.Status`), so that deleting a
//! manager's Node takes it out of the membership. The leader keeps it, asking
//! each member it does not know yet. Kept as a Config in the system
//! namespace, by node name; a name that joins again under a new id has it
//! replaced.

use std::collections::BTreeMap;
use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

const NAME: &str = "managers";
/// How often the leader looks for members it does not know by name.
const PERIOD: Duration = Duration::from_secs(5);

fn key() -> ObjectKey {
    ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, NAME)
}

/// A manager, as it said it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Member {
    pub id: NodeId,
    pub addr: String,
}

/// The managers by name, and the record's version.
fn read(store: &Store) -> Result<(BTreeMap<String, Member>, Option<u64>), Status> {
    let Some(o) = store
        .get(&key())
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return Ok((BTreeMap::new(), None));
    };
    let spec: DataSpec =
        serde_json::from_value(o.spec).map_err(|e| Status::internal(e.to_string()))?;
    let managers = serde_json::from_str(spec.text.as_deref().unwrap_or("{}"))
        .map_err(|e| Status::internal(format!("{NAME}: {e}")))?;
    Ok((managers, Some(o.metadata.version)))
}

/// The Raft id of the manager named `name`, if it is known.
pub(crate) fn id_of(store: &Store, name: &str) -> Result<Option<NodeId>, Status> {
    Ok(read(store)?.0.get(name).map(|m| m.id))
}

/// Writes `managers` over the record at `version`; a write that lost a race
/// is left for the next pass.
async fn write(
    api: &ApiService,
    managers: &BTreeMap<String, Member>,
    version: Option<u64>,
) -> Result<(), Status> {
    let spec = DataSpec {
        data: None,
        text: Some(serde_json::to_string(managers).expect("serializes")),
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
        Outcome::Written(_) | Outcome::Conflict { .. } => Ok(()),
        other => Err(Status::internal(format!("unexpected outcome {other:?}"))),
    }
}

/// `known` with each member of `members` that `named` (asked) gives a name
/// for added, or moved to its new id; `None` if nothing changed.
pub(crate) fn learned(
    known: &BTreeMap<String, Member>,
    named: &[(String, Member)],
) -> Option<BTreeMap<String, Member>> {
    let mut next = known.clone();
    for (name, member) in named {
        if !name.is_empty() && next.get(name) != Some(member) {
            next.insert(name.clone(), member.clone());
        }
    }
    (next != *known).then_some(next)
}

/// While this manager leads, asks every member not yet known by its id what
/// it is called, and records it.
pub(crate) async fn run(api: ApiService, closing: tokio::sync::watch::Receiver<bool>) {
    let store = api.store.clone();
    crate::controller::while_leading(&store, api.id, closing, "manager names", || {
        let api = api.clone();
        async move {
            loop {
                if let Err(e) = pass(&api).await {
                    tracing::debug!("manager names: {e}");
                }
                tokio::time::sleep(PERIOD).await;
            }
        }
    })
    .await
}

async fn pass(api: &ApiService) -> Result<(), Status> {
    let (known, version) = read(&api.store)?;
    let ids: Vec<NodeId> = known.values().map(|m| m.id).collect();
    let members: Vec<(NodeId, String)> = {
        let m = api.store.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .nodes()
            .filter(|(id, _)| !ids.contains(id))
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    };
    let mut named = Vec::new();
    for (id, addr) in members {
        let name = if id == api.id {
            api.name.clone()
        } else {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(
                crate::raft_net::connect_lazy(&addr, api.peer_tls.as_ref()),
            );
            let asked = c.status(legion_proto::v1::StatusRequest {});
            match tokio::time::timeout(Duration::from_secs(5), asked).await {
                // Only a member that says it is that id: an address
                // reused by another manager names nothing.
                Ok(Ok(r)) if r.get_ref().node_id == id => r.into_inner().name,
                _ => continue,
            }
        };
        named.push((name, Member { id, addr }));
    }
    if let Some(next) = learned(&known, &named) {
        write(api, &next, version).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn members_are_learned_by_name_and_a_name_joined_again_moves() {
        let m = |id, addr: &str| Member {
            id,
            addr: addr.into(),
        };
        let known = BTreeMap::from([("m1".to_owned(), m(1, "a:1"))]);
        assert_eq!(learned(&known, &[]), None);
        // An old build names nothing.
        assert_eq!(learned(&known, &[(String::new(), m(2, "b:1"))]), None);
        let next = learned(&known, &[("m2".into(), m(2, "b:1"))]).unwrap();
        assert_eq!(next["m2"], m(2, "b:1"));
        let again = learned(&next, &[("m2".into(), m(7, "b:1"))]).unwrap();
        assert_eq!(again["m2"], m(7, "b:1"));
        assert_eq!(again["m1"], m(1, "a:1"));
    }
}
