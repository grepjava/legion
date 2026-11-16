//! The exporter (DESIGN.md §6.7): while this manager leads, it has each
//! node provide the NFS and iSCSI Volumes whose spec names it (`nfs: {
//! node }`, `iscsi: { node }`). Every node providing any, and every node
//! once at the start (one may still export what was deleted meanwhile),
//! is sent the whole list (`ExportSync`) when it changes, and every
//! [`AGAIN`] besides; what the node says of each Volume goes to its
//! `status.export`, which the dispatcher fills a mounting node's spec from.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use legion_api::node::{NodeState, NodeStatus};
use legion_api::volume::{Export, VolumeDriver, VolumeSpec, VolumeStatus};
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{ExportSync, ExportWanted, SessionOffer, SessionOpen};
use legion_store::Store;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_stream::StreamExt;

use crate::migrate::update;
use crate::session::Hub;

const PERIOD: Duration = Duration::from_secs(1);
/// How often a node is sent its list though nothing changed: what it
/// exports may have been changed by hand, or lost with a restart.
const AGAIN: Duration = Duration::from_secs(30);
/// How long a node has to answer.
const ANSWER: Duration = Duration::from_secs(120);
/// The port iSCSI portals are on unless a node says otherwise.
pub const ISCSI_PORT: u16 = 3260;

/// What a node says of one export (`legion_agent::exports::Said`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct Said {
    pub namespace: String,
    pub volume: String,
    pub path: String,
    pub target: String,
    pub portal: String,
    pub message: String,
}

/// Each providing node's list, as it is sent: the exports, then the
/// clients.
pub(crate) fn wanted(
    volumes: &[(ObjectKey, VolumeSpec)],
    nodes: &BTreeMap<String, NodeStatus>,
) -> (BTreeMap<String, Vec<ExportWanted>>, Vec<String>) {
    let mut by_node: BTreeMap<String, Vec<ExportWanted>> = BTreeMap::new();
    for (key, spec) in volumes {
        let Some(node) = spec.provider() else {
            continue;
        };
        let kind = match spec.driver {
            VolumeDriver::Nfs => "nfs",
            VolumeDriver::Iscsi => "iscsi",
            _ => continue,
        };
        by_node
            .entry(node.to_owned())
            .or_default()
            .push(ExportWanted {
                namespace: key.namespace.clone(),
                volume: key.name.clone(),
                kind: kind.to_owned(),
                size: spec.size.map_or(0, |b| b.0),
            });
    }
    let clients: BTreeSet<String> = nodes
        .values()
        .map(|s| s.addr.clone())
        .filter(|a| !a.is_empty())
        .collect();
    (by_node, clients.into_iter().collect())
}

/// The export a node's word makes of a Volume it provides.
pub(crate) fn export(node: &str, addr: &str, said: &Said) -> Export {
    let host = if addr.contains(':') {
        format!("[{addr}]")
    } else {
        addr.to_owned()
    };
    let target = !said.target.is_empty();
    Export {
        node: node.to_owned(),
        server: if said.path.is_empty() {
            String::new()
        } else {
            addr.to_owned()
        },
        path: said.path.clone(),
        portal: match (target, said.portal.is_empty()) {
            (false, _) => String::new(),
            (true, false) => said.portal.clone(),
            (true, true) => format!("{host}:{ISCSI_PORT}"),
        },
        target: said.target.clone(),
        message: said.message.clone(),
    }
}

/// Keeps nodes' exports as the Volumes say while this manager leads.
pub(crate) async fn lead(store: &Store, hub: &Arc<Hub>) {
    // What each node was last sent, and when.
    let mut sent: BTreeMap<String, (Vec<ExportWanted>, Vec<String>, Instant)> = BTreeMap::new();
    let mut syncs: JoinSet<(String, Result<Vec<Said>, String>)> = JoinSet::new();
    let mut running: BTreeSet<String> = BTreeSet::new();
    // Nodes not yet sent anything by this leader: each once, so one
    // exporting what was deleted meanwhile stops.
    let mut unseen: Option<BTreeSet<String>> = None;
    loop {
        while let Some(done) = syncs.try_join_next() {
            let Ok((node, result)) = done else { continue };
            running.remove(&node);
            match result {
                Ok(said) => record(store, &node, &said).await,
                Err(e) => {
                    tracing::warn!(%node, "its exports: {e}");
                    // Sent again at the next round.
                    sent.remove(&node);
                }
            }
        }
        let (Ok(volumes), Ok(nodes)) =
            (store.list(Kind::Volume, None), store.list(Kind::Node, None))
        else {
            tokio::time::sleep(PERIOD).await;
            continue;
        };
        let nodes: BTreeMap<String, NodeStatus> = nodes
            .objects
            .into_iter()
            .map(|o| {
                let s = serde_json::from_value(o.status).unwrap_or_default();
                (o.metadata.name, s)
            })
            .collect();
        let specs: Vec<(ObjectKey, VolumeSpec)> = volumes
            .objects
            .iter()
            .filter_map(|v| Some((v.key(), legion_api::parse(&v.spec).ok()?)))
            .collect();
        let (by_node, clients) = wanted(&specs, &nodes);
        let unseen = unseen.get_or_insert_with(|| nodes.keys().cloned().collect());
        let mut to_sync: BTreeSet<String> = by_node.keys().cloned().collect();
        to_sync.extend(sent.keys().cloned());
        to_sync.extend(unseen.iter().cloned());
        for node in to_sync {
            let Some(status) = nodes.get(&node) else {
                sent.remove(&node);
                unseen.remove(&node);
                continue;
            };
            if status.state != NodeState::Ready || running.contains(&node) {
                continue;
            }
            let list = by_node.get(&node).cloned().unwrap_or_default();
            let due = match sent.get(&node) {
                Some((l, c, at)) => *l != list || *c != clients || at.elapsed() >= AGAIN,
                None => true,
            };
            if !due {
                continue;
            }
            // A node that had none and still has none needs nothing more.
            if list.is_empty()
                && !unseen.contains(&node)
                && sent.get(&node).is_some_and(|(l, ..)| l.is_empty())
            {
                sent.remove(&node);
                continue;
            }
            unseen.remove(&node);
            sent.insert(
                node.clone(),
                (list.clone(), clients.clone(), Instant::now()),
            );
            let (hub, n, c) = (hub.clone(), node.clone(), clients.clone());
            syncs.spawn(async move {
                let r = tokio::time::timeout(ANSWER, sync(&hub, &n, list, c))
                    .await
                    .unwrap_or_else(|_| Err("no answer".to_owned()));
                (n, r)
            });
            running.insert(node);
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// Sends `node` its list; what it says of each.
async fn sync(
    hub: &Arc<Hub>,
    node: &str,
    exports: Vec<ExportWanted>,
    clients: Vec<String>,
) -> Result<Vec<Said>, String> {
    let mut a = hub
        .offer(
            node,
            SessionOffer {
                id: legion_pki::random_secret(),
                open: Some(SessionOpen {
                    action: Some(Action::ExportSync(ExportSync { exports, clients })),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let mut out = Vec::new();
    while let Some(f) = a.from_agent.next().await {
        match f.map_err(|e| e.message().to_owned())?.frame {
            Some(Frame::Stdout(b)) => out.extend(b),
            Some(Frame::ExitCode(0)) => {
                return serde_json::from_slice(&out).map_err(|e| format!("its answer: {e}"));
            }
            Some(Frame::ExitCode(c)) => return Err(format!("exit {c}")),
            Some(Frame::Error(e)) => return Err(e.message),
            _ => {}
        }
    }
    Err(format!("{node} ended the session early"))
}

/// Writes what `node` said to each Volume it still provides.
async fn record(store: &Store, node: &str, said: &[Said]) {
    let addr = store
        .get(&ObjectKey::new(Kind::Node, "", node))
        .ok()
        .flatten()
        .and_then(|o| serde_json::from_value::<NodeStatus>(o.status).ok())
        .map(|s| s.addr)
        .unwrap_or_default();
    for s in said {
        let key = ObjectKey::new(Kind::Volume, &s.namespace, &s.volume);
        let provided = store
            .get(&key)
            .ok()
            .flatten()
            .and_then(|o| legion_api::parse::<VolumeSpec>(&o.spec).ok())
            .is_some_and(|spec| spec.provider() == Some(node));
        if !provided {
            continue;
        }
        let e = export(node, &addr, s);
        if !e.message.is_empty() {
            tracing::warn!(volume = %key, node, "exporting it: {}", e.message);
        }
        let r = update(store, &key, |st: &mut VolumeStatus| {
            if st.export.as_ref() == Some(&e) {
                return false;
            }
            st.export = Some(e.clone());
            true
        })
        .await;
        if let Err(e) = r {
            tracing::debug!(volume = %key, "recording its export: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> VolumeSpec {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn each_node_is_sent_what_it_provides_and_whom_to_share_with() {
        let key = |n: &str| ObjectKey::new(Kind::Volume, "shop", n);
        let volumes = [
            (
                key("files"),
                spec("driver: nfs\nsize: 1Gi\nnfs: { node: s1 }"),
            ),
            (
                key("db"),
                spec("driver: iscsi\nsize: 2Gi\niscsi: { node: s1 }"),
            ),
            (key("other"), spec("driver: nfs\nnfs: { node: s2 }")),
            (
                key("plain"),
                spec("driver: nfs\nnfs: { server: a, path: /x }"),
            ),
            (key("local"), spec("size: 1Gi")),
        ];
        let node = |addr: &str| NodeStatus {
            addr: addr.into(),
            ..Default::default()
        };
        let nodes = BTreeMap::from([
            ("s1".to_owned(), node("10.0.0.2")),
            ("w1".to_owned(), node("10.0.0.1")),
            ("new".to_owned(), node("")),
        ]);
        let (by_node, clients) = wanted(&volumes, &nodes);
        assert_eq!(clients, ["10.0.0.1", "10.0.0.2"]);
        let s1: Vec<(&str, &str, u64)> = by_node["s1"]
            .iter()
            .map(|w| (w.volume.as_str(), w.kind.as_str(), w.size))
            .collect();
        assert_eq!(s1, [("files", "nfs", 1 << 30), ("db", "iscsi", 2 << 30)]);
        assert_eq!(by_node["s2"][0].size, 0);
        assert_eq!(by_node.len(), 2);
    }

    #[test]
    fn what_a_node_says_is_where_nodes_mount_it() {
        let nfs = Said {
            path: "/tank/legion/exports/shop/files".into(),
            ..Default::default()
        };
        let e = export("s1", "10.0.0.2", &nfs);
        assert_eq!((e.server.as_str(), e.portal.as_str()), ("10.0.0.2", ""));
        assert!(e.ready());
        let lun = Said {
            target: "iqn.2026-11.invalid.legion:shop.db".into(),
            ..Default::default()
        };
        assert_eq!(export("s1", "10.0.0.2", &lun).portal, "10.0.0.2:3260");
        assert_eq!(export("s1", "fd00::2", &lun).portal, "[fd00::2]:3260");
        let configured = Said {
            portal: "10.9.9.9:3261".into(),
            ..lun.clone()
        };
        assert_eq!(
            export("s1", "10.0.0.2", &configured).portal,
            "10.9.9.9:3261"
        );
        let failed = Said {
            message: "no node to share it with".into(),
            ..Default::default()
        };
        assert!(!export("s1", "10.0.0.2", &failed).ready());
    }
}
