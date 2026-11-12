//! Changing a node's role in place (DESIGN.md §3.7, roles): `legion node
//! promote` makes a worker a manager, `legion node demote` a manager a
//! worker, its tasks running on.
//!
//! The leader records the change asked for (a Config in the reserved
//! namespace, by node). A worker learns it from its heartbeats and asks to
//! be promoted (`Dispatcher.Promote`): the leader certifies it as a manager,
//! gives it the CA, and adds it to Raft as a learner, as a joining manager.
//! A manager reads it from its store and asks to be demoted
//! (`Cluster.Demote`): the leader certifies it as a worker and takes it out
//! of the membership. Either way every certificate the node held before is
//! refused from then (`removed::supersede`), and its daemon starts again in
//! its new role on the same data directory, adopting its pods.

use std::collections::BTreeMap;

use legion_api::data::DataSpec;
use legion_api::protocol::Protocol;
use legion_api::{Kind, Object, ObjectKey};
use legion_pki::{CERT_LIFETIME, Identity, Role};
use legion_proto::v1 as pb;
use legion_store::{Command, Outcome, Precondition, Store};
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

const NAME: &str = "role-changes";

fn key() -> ObjectKey {
    ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, NAME)
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

/// The changes asked for, by node, and the record's version.
fn read(store: &Store) -> Result<(BTreeMap<String, String>, Option<u64>), Status> {
    let Some(o) = store.get(&key()).map_err(internal)? else {
        return Ok((BTreeMap::new(), None));
    };
    let spec: DataSpec = serde_json::from_value(o.spec).map_err(internal)?;
    let changes = serde_json::from_str(spec.text.as_deref().unwrap_or("{}"))
        .map_err(|e| Status::internal(format!("{NAME}: {e}")))?;
    Ok((changes, Some(o.metadata.version)))
}

/// The role `node` is asked to take, if any.
pub fn wanted(store: &Store, node: &str) -> Result<Option<Role>, Status> {
    Ok(read(store)?.0.get(node).and_then(|r| Role::parse(r)))
}

/// Changes the record with `f`, retrying a write that lost a race.
async fn change(api: &ApiService, f: impl Fn(&mut BTreeMap<String, String>)) -> Result<(), Status> {
    for _ in 0..5 {
        let (mut changes, version) = read(&api.store)?;
        let before = changes.clone();
        f(&mut changes);
        if changes == before {
            return Ok(());
        }
        let spec = DataSpec {
            text: Some(serde_json::to_string(&changes).expect("serialises")),
            ..Default::default()
        };
        let object = Object::new(
            Kind::Config,
            SYSTEM_NAMESPACE,
            NAME,
            serde_json::to_value(spec).expect("serialises"),
        );
        let precondition = version.map_or(Precondition::MustNotExist, Precondition::Version);
        match api
            .write(Command::Put {
                object,
                precondition,
            })
            .await?
        {
            Outcome::Written(_) => return Ok(()),
            Outcome::Conflict { .. } => continue,
            other => return Err(Status::internal(format!("unexpected {other:?}"))),
        }
    }
    Err(Status::aborted("role changes keep changing; retry"))
}

/// The node's Node: its spec (its role) and status (its state).
fn node_of(
    store: &Store,
    node: &str,
) -> Result<(legion_api::node::NodeSpec, legion_api::node::NodeStatus), Status> {
    let o = store
        .get(&ObjectKey::new(Kind::Node, "", node))
        .map_err(internal)?
        .ok_or_else(|| Status::not_found(format!("node {node} is not in the cluster")))?;
    let spec = legion_api::parse(&o.spec).map_err(internal)?;
    let status = serde_json::from_value(o.status).unwrap_or_default();
    Ok((spec, status))
}

/// The leader's address, or `None` if this manager leads.
fn leader(api: &ApiService) -> Result<Option<String>, Status> {
    let m = api.store.raft.metrics().borrow().clone();
    match m.current_leader {
        Some(id) if id == api.id => Ok(None),
        Some(id) => m
            .membership_config
            .membership()
            .get_node(&id)
            .map(|n| Some(n.addr.clone()))
            .ok_or_else(|| Status::unavailable("the leader is not known yet; retry")),
        None => Err(Status::unavailable("no leader; retry")),
    }
}

/// Records, on the leader, that `node` is to become a manager or a
/// worker. Refused for a role it has, and for demoting the last voter.
pub(crate) async fn request(api: &ApiService, node: &str, role: &str) -> Result<(), Status> {
    let role = match Role::parse(role) {
        Some(r @ (Role::Manager | Role::Worker)) => r,
        _ => {
            return Err(Status::invalid_argument(
                "the role is `manager` or `worker`",
            ));
        }
    };
    let (spec, status) = node_of(&api.store, node)?;
    let has = match spec.role {
        legion_api::node::Role::Manager => Role::Manager,
        legion_api::node::Role::Worker => Role::Worker,
    };
    if has == role {
        // Asked back before it was taken up: nothing to do any more.
        change(api, |c| {
            c.remove(node);
        })
        .await?;
        return Err(Status::already_exists(format!(
            "node {node} is a {} already",
            role.as_str()
        )));
    }
    if role == Role::Manager && status.state != legion_api::node::NodeState::Ready {
        return Err(Status::failed_precondition(format!(
            "node {node} is not ready: it takes up a promotion from its heartbeats"
        )));
    }
    if role == Role::Worker {
        let Some(id) = crate::managers::id_of(&api.store, node)? else {
            return Err(Status::failed_precondition(format!(
                "manager {node} is not known by name yet; retry in a few seconds"
            )));
        };
        let m = api.store.raft.metrics().borrow().clone();
        let voters: Vec<u64> = m.membership_config.membership().voter_ids().collect();
        if voters == [id] {
            return Err(Status::failed_precondition(format!(
                "manager {node} is the cluster's last voter: promote another first"
            )));
        }
    }
    change(api, |c| {
        c.insert(node.to_owned(), role.as_str().to_owned());
    })
    .await?;
    tracing::info!(node, role = role.as_str(), "role change asked");
    Ok(())
}

/// The calling worker `node`, asked to become a manager, certified as one
/// and added as a Raft learner; on the leader.
pub(crate) async fn promote(
    api: &ApiService,
    node: &str,
    req: pb::PromoteRequest,
) -> Result<pb::PromoteResponse, Status> {
    if wanted(&api.store, node)? != Some(Role::Manager) {
        return Err(Status::failed_precondition(format!(
            "node {node} is not asked to become a manager"
        )));
    }
    if req.node_id == 0 || req.advertise.is_empty() {
        return Err(Status::invalid_argument(
            "a manager comes with its Raft id and advertised address",
        ));
    }
    api.speaking
        .among_managers()
        .works_with(
            Protocol::said(req.protocol, req.oldest_protocol),
            &format!("node {node}"),
        )
        .map_err(Status::unimplemented)?;
    let issuer = api
        .issuer
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("this manager cannot issue certificates"))?;
    let ca = issuer.current()?;
    let id = Identity::new(node, Role::Manager);
    // A bad request is refused before anything is recorded.
    ca.sign(&req.csr_pem, &id, CERT_LIFETIME)
        .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
    // Its worker certificate refused from now: the manager's is the one.
    crate::removed::supersede(api, node).await?;
    crate::removed::issue_after(&api.store, node).await?;
    let cert_pem = ca
        .sign(&req.csr_pem, &id, CERT_LIFETIME)
        .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
    issuer
        .add_learner(&pb::JoinRequest {
            name: node.to_owned(),
            node_id: req.node_id,
            advertise: req.advertise.clone(),
            ..Default::default()
        })
        .await?;
    change(api, |c| {
        c.remove(node);
    })
    .await?;
    tracing::info!(node, node_id = req.node_id, advertise = %req.advertise, "worker promoted");
    Ok(pb::PromoteResponse {
        cert_pem,
        ca_pem: issuer.credentials.ca_pem(),
        ca_key_pem: ca.key_pem().to_owned(),
    })
}

/// The calling manager `node`, asked to become a worker, certified as one
/// and taken out of the membership; on the leader.
pub(crate) async fn demote(
    api: &ApiService,
    node: &str,
    req: pb::DemoteRequest,
) -> Result<pb::DemoteResponse, Status> {
    if wanted(&api.store, node)? != Some(Role::Worker) {
        return Err(Status::failed_precondition(format!(
            "manager {node} is not asked to become a worker"
        )));
    }
    let issuer = api
        .issuer
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("this manager cannot issue certificates"))?;
    let ca = issuer.current()?;
    let id = Identity::new(node, Role::Worker);
    ca.sign(&req.csr_pem, &id, CERT_LIFETIME)
        .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
    let managers: Vec<String> = {
        let own = crate::managers::id_of(&api.store, node)?;
        let m = api.store.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .nodes()
            .filter(|(id, _)| Some(**id) != own)
            .map(|(_, n)| n.addr.clone())
            .collect()
    };
    crate::removed::supersede(api, node).await?;
    crate::removed::issue_after(&api.store, node).await?;
    let cert_pem = ca
        .sign(&req.csr_pem, &id, CERT_LIFETIME)
        .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
    api.leave_membership(node).await?;
    change(api, |c| {
        c.remove(node);
    })
    .await?;
    tracing::info!(node, "manager demoted");
    Ok(pb::DemoteResponse {
        cert_pem,
        ca_pem: issuer.credentials.ca_pem(),
        managers,
    })
}

/// What a demoted manager's daemon writes before it starts again as a
/// worker.
#[derive(Debug, Clone)]
pub struct Demoted {
    pub cert_pem: String,
    pub key_pem: String,
    pub ca_pem: String,
    pub managers: Vec<String>,
}

/// While this manager runs, asks the leader to demote it once it is asked
/// to become a worker, and hands what it got to the daemon through `tx`.
pub(crate) async fn follow(
    api: ApiService,
    tx: std::sync::Arc<tokio::sync::watch::Sender<Option<Demoted>>>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let asked = matches!(wanted(&api.store, &api.name), Ok(Some(Role::Worker)));
        if asked && tx.borrow().is_none() {
            match ask_demotion(&api).await {
                Ok(d) => {
                    tracing::info!("demoted: this node starts again as a worker");
                    let _ = tx.send(Some(d));
                }
                Err(e) => tracing::warn!("asking to be demoted: {e}"),
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            _ = closing.changed() => return,
        }
    }
}

async fn ask_demotion(api: &ApiService) -> Result<Demoted, Status> {
    let (key_pem, csr_pem) = legion_pki::request().map_err(internal)?;
    let req = pb::DemoteRequest { csr_pem };
    // Through the leader, as this manager calls it (the leader too, itself).
    let addr = match leader(api)? {
        Some(addr) => addr,
        None => api.advertise.clone(),
    };
    let mut c = pb::cluster_client::ClusterClient::new(crate::raft_net::connect_lazy(
        &addr,
        api.peer_tls.as_ref(),
    ));
    let r = c.demote(req).await?.into_inner();
    Ok(Demoted {
        cert_pem: r.cert_pem,
        key_pem,
        ca_pem: r.ca_pem,
        managers: r.managers,
    })
}
