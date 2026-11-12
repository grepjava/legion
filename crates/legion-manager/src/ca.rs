//! The cluster CA and its rotation (DESIGN.md §9).
//!
//! Until a first rotation the CA is the one each manager holds in its
//! `pki` directory. `legion ca rotate` has the leader make a successor and
//! record it in the store (a Secret in the reserved namespace, so every
//! manager reads it and none shows it): the new CA, its certificate
//! cross-signed by the old one, and the old one's certificate. Every
//! manager then signs with the new CA and trusts both (the daemon's half,
//! with the files, is `legion`'s); workers learn it from their heartbeats
//! and renew at once. Once every manager and node holds a certificate of
//! the new CA, the leader records the rotation done: the old CA and the
//! cross-signed certificate go, and nothing of the old CA is trusted.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use legion_api::{Kind, Object, ObjectKey};
use legion_pki::Ca;
use legion_proto::v1 as pb;
use legion_store::{Command, Outcome, Precondition, Store};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

const NAME: &str = "cluster-ca";
/// How often the leader checks whether a rotation can end.
const PERIOD: Duration = Duration::from_secs(5);

/// The cluster CA as recorded once a rotation has begun.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    /// The CA that issues certificates.
    pub cert_pem: String,
    pub key_pem: String,
    /// While a rotation runs: the new CA's certificate signed by the old
    /// CA, which peers that trust only the old one verify it through.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_pem: Option<String>,
    /// While a rotation runs: the old CA's certificate, still trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_pem: Option<String>,
    /// When the rotation began, in ms since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_ms: Option<u64>,
}

impl Record {
    /// The CAs every node trusts: the issuing one first, then while
    /// rotating the cross-signed certificate and the old CA.
    pub fn trust_pem(&self) -> String {
        let mut pem = self.cert_pem.clone();
        for extra in [&self.cross_pem, &self.previous_pem].into_iter().flatten() {
            if !pem.ends_with('\n') {
                pem.push('\n');
            }
            pem.push_str(extra);
        }
        pem
    }

    pub fn digest(&self) -> String {
        legion_pki::digest(&self.cert_pem).unwrap_or_default()
    }

    pub fn rotating(&self) -> bool {
        self.previous_pem.is_some()
    }
}

fn object_key() -> ObjectKey {
    ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, NAME)
}

/// The recorded CA, if a rotation has ever begun, and its version.
pub fn current(store: &Store) -> Result<Option<(Record, u64)>, String> {
    let Some(o) = store.get(&object_key()).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let record = serde_json::from_value(o.spec).map_err(|e| format!("{NAME}: {e}"))?;
    Ok(Some((record, o.metadata.version)))
}

/// The CA this manager signs with: swapped when a rotation begins, so
/// joins, renewals and users' certificates take the new one from then.
#[derive(Clone)]
pub struct Authority(Arc<RwLock<Arc<Ca>>>);

impl Authority {
    pub fn new(ca: Ca) -> Authority {
        Authority(Arc::new(RwLock::new(Arc::new(ca))))
    }

    pub fn ca(&self) -> Arc<Ca> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn set(&self, ca: Ca) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(ca);
    }
}

/// The CA each node's certificate was of at its last heartbeat to this
/// manager, by node: what the leader waits on to end a rotation.
pub(crate) type Seen = Arc<Mutex<HashMap<String, String>>>;

/// The digest of the CA `cert_pem` is of, among `trust_pem`; empty if none.
pub(crate) fn issuer_of(cert_pem: &str, trust_pem: &str) -> String {
    legion_pki::issuer(cert_pem, trust_pem).unwrap_or_default()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Begins a rotation, on the leader: a successor of the CA that issues
/// now, recorded with the old CA beside it. Refused while one runs.
pub(crate) async fn start(api: &ApiService) -> Result<(), Status> {
    let authority = api
        .issuer
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("this manager holds no CA"))?
        .ca
        .clone();
    let now = current(&api.store).map_err(Status::internal)?;
    if let Some((record, _)) = &now
        && record.rotating()
    {
        return Err(Status::failed_precondition(
            "a CA rotation is under way; `legion ca status` says how far",
        ));
    }
    // The leader's CA is the cluster's: the record's, once there is one,
    // which every manager has taken by the time it is asked to lead.
    let old = authority.ca();
    if let Some((record, _)) = &now
        && record.digest() != old.digest()
    {
        return Err(Status::unavailable(
            "this manager has not taken up the recorded CA yet; retry",
        ));
    }
    let (next, cross) = old
        .successor()
        .map_err(|e| Status::internal(format!("making the new CA: {e}")))?;
    let record = Record {
        cert_pem: next.cert_pem().to_owned(),
        key_pem: next.key_pem().to_owned(),
        cross_pem: Some(cross),
        previous_pem: Some(old.cert_pem().to_owned()),
        since_ms: Some(now_ms()),
    };
    let precondition = now.as_ref().map_or(Precondition::MustNotExist, |(_, v)| {
        Precondition::Version(*v)
    });
    put(api, &record, precondition).await?;
    tracing::info!(
        from = %old.digest(),
        to = %record.digest(),
        "CA rotation begun"
    );
    Ok(())
}

async fn put(api: &ApiService, record: &Record, precondition: Precondition) -> Result<(), Status> {
    let object = Object::new(
        Kind::Secret,
        SYSTEM_NAMESPACE,
        NAME,
        serde_json::to_value(record).expect("serialises"),
    );
    match api
        .write(Command::Put {
            object,
            precondition,
        })
        .await?
    {
        Outcome::Written(_) => Ok(()),
        Outcome::Conflict { .. } => Err(Status::aborted("the CA changed meanwhile; retry")),
        other => Err(Status::internal(format!("unexpected {other:?}"))),
    }
}

/// Where the cluster's CA stands, on the leader: which CA issues, the one
/// before while a rotation runs, and the CA of each manager's and node's
/// certificate.
pub(crate) async fn status(api: &ApiService) -> Result<pb::RotateCaResponse, Status> {
    let authority = api
        .issuer
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("this manager holds no CA"))?
        .ca
        .ca();
    let record = current(&api.store)
        .map_err(Status::internal)?
        .map(|(r, _)| r);
    let previous_digest = record
        .as_ref()
        .and_then(|r| r.previous_pem.as_deref())
        .and_then(|p| legion_pki::digest(p).ok())
        .unwrap_or_default();
    Ok(pb::RotateCaResponse {
        digest: record
            .as_ref()
            .map_or_else(|| authority.digest(), Record::digest),
        previous_digest,
        since_ms: record.as_ref().and_then(|r| r.since_ms).unwrap_or_default(),
        holders: holders(api).await?,
    })
}

/// Every manager (by its `Cluster.Status`) and every node (by its last
/// heartbeat here), with the CA its certificate is of.
async fn holders(api: &ApiService) -> Result<Vec<pb::CaHolder>, Status> {
    let mut out = Vec::new();
    let names = crate::managers::names(&api.store)?;
    let members: Vec<(u64, String)> = {
        let m = api.store.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    };
    for (id, addr) in members {
        let (name, issuer) = if id == api.id {
            (api.name.clone(), own_issuer(api))
        } else {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(
                crate::raft_net::connect_lazy(&addr, api.peer_tls.as_ref()),
            );
            let asked = c.status(pb::StatusRequest {});
            let name = names.get(&id).cloned().unwrap_or_else(|| format!("#{id}"));
            match tokio::time::timeout(Duration::from_secs(5), asked).await {
                Ok(Ok(r)) if r.get_ref().node_id == id => {
                    let r = r.into_inner();
                    (if r.name.is_empty() { name } else { r.name }, r.ca_issuer)
                }
                _ => (name, String::new()),
            }
        };
        out.push(pb::CaHolder {
            name,
            kind: "manager".into(),
            issuer,
        });
    }
    let seen = api
        .ca_seen
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let nodes = api
        .store
        .list(Kind::Node, None)
        .map_err(|e| Status::internal(e.to_string()))?
        .objects;
    for n in nodes {
        let name = n.metadata.name;
        out.push(pb::CaHolder {
            issuer: seen.get(&name).cloned().unwrap_or_default(),
            name,
            kind: "node".into(),
        });
    }
    Ok(out)
}

/// The CA this manager's own certificate is of.
pub(crate) fn own_issuer(api: &ApiService) -> String {
    match &api.peer_tls {
        Some(c) => issuer_of(&c.cert_and_key().0, &c.ca_pem()),
        None => String::new(),
    }
}

/// While this manager leads, ends a rotation once every manager and node
/// holds a certificate of the new CA.
pub(crate) async fn run(api: ApiService, closing: tokio::sync::watch::Receiver<bool>) {
    let store = api.store.clone();
    crate::controller::while_leading(&store, api.id, closing, "CA rotation", || {
        let api = api.clone();
        async move {
            loop {
                if let Err(e) = finish(&api).await {
                    tracing::debug!("CA rotation: {e}");
                }
                tokio::time::sleep(PERIOD).await;
            }
        }
    })
    .await
}

/// Ends the rotation if every holder is on the new CA; says who is not.
async fn finish(api: &ApiService) -> Result<(), Status> {
    let Some((record, version)) = current(&api.store).map_err(Status::internal)? else {
        return Ok(());
    };
    if !record.rotating() {
        return Ok(());
    }
    let holders = holders(api).await?;
    let behind: BTreeMap<&str, &str> = holders
        .iter()
        .filter(|h| h.issuer != record.digest())
        .map(|h| (h.name.as_str(), h.kind.as_str()))
        .collect();
    if !behind.is_empty() {
        return Err(Status::unavailable(format!(
            "waiting on {}",
            behind
                .iter()
                .map(|(n, k)| format!("{k} {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let done = Record {
        cross_pem: None,
        previous_pem: None,
        since_ms: None,
        ..record.clone()
    };
    put(api, &done, Precondition::Version(version)).await?;
    tracing::info!(ca = %done.digest(), "CA rotation done: the old CA is trusted no more");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_issuing_ca_comes_first_and_the_old_goes_at_the_end() {
        let old = Ca::generate().unwrap();
        let (next, cross) = old.successor().unwrap();
        let rotating = Record {
            cert_pem: next.cert_pem().to_owned(),
            key_pem: next.key_pem().to_owned(),
            cross_pem: Some(cross.clone()),
            previous_pem: Some(old.cert_pem().to_owned()),
            since_ms: Some(1),
        };
        let trust = rotating.trust_pem();
        assert_eq!(
            legion_pki::split_certs(&trust),
            [next.cert_pem().to_owned(), cross, old.cert_pem().to_owned()]
        );
        assert_eq!(legion_pki::digest(&trust).unwrap(), next.digest());
        assert!(rotating.rotating());
        let done = Record {
            cross_pem: None,
            previous_pem: None,
            since_ms: None,
            ..rotating
        };
        assert_eq!(done.trust_pem(), next.cert_pem());
        assert!(!done.rotating());
        let json = serde_json::to_value(&done).unwrap();
        assert!(json.get("previousPem").is_none(), "{json}");
        // Who holds what: a node's certificate of either CA is told apart.
        let w = old
            .issue(
                &legion_pki::Identity::new("w1", legion_pki::Role::Worker),
                legion_pki::CERT_LIFETIME,
            )
            .unwrap();
        assert_eq!(issuer_of(&w.cert_pem, &trust), old.digest());
        assert_eq!(issuer_of(&w.cert_pem, &done.trust_pem()), "");
    }
}
