//! A follower's copy of what the agents report: their heartbeats go to the
//! leader alone, so a follower follows the leader's usage (WatchUsage, the
//! system namespace's too) and keeps each node's telemetry as the leader
//! last sent it, for its metrics. Dropped when it stops following: the
//! leader changed, or this manager leads. A witness, which serves no
//! clients and sits across a slow link, keeps none.

use std::collections::BTreeMap;
use std::time::Duration;

use legion_api::dispatch::{NodeTelemetry, VolumeTelemetry};
use legion_proto::v1 as pb;
use legion_store::NodeId;

use crate::api::{ApiService, forwarded};
use crate::raft_net::{MAX_MESSAGE, connect_lazy};

/// How long after a stream fails before following the leader again.
const RETRY: Duration = Duration::from_secs(2);

pub(crate) async fn run(api: ApiService) {
    if api.witness {
        return;
    }
    let mut closing = api.closing.clone();
    let mut raft = api.store.raft.metrics();
    loop {
        let leader = {
            let m = raft.borrow_and_update();
            match m.current_leader {
                Some(l) if l != api.id => m
                    .membership_config
                    .membership()
                    .get_node(&l)
                    .map(|n| (l, n.addr.clone())),
                _ => None,
            }
        };
        let Some((leader, addr)) = leader else {
            api.usage.mirror(BTreeMap::new());
            tokio::select! {
                r = raft.changed() => if r.is_err() { return },
                _ = closing.wait_for(|c| *c) => return,
            }
            continue;
        };
        let failed = tokio::select! {
            why = follow(&api, &addr) => Some(why),
            () = moved_on(&mut raft, leader) => None,
            _ = closing.wait_for(|c| *c) => return,
        };
        api.usage.mirror(BTreeMap::new());
        if let Some(why) = failed {
            tracing::debug!(%addr, "following the leader's usage ended: {why}");
            tokio::select! {
                () = tokio::time::sleep(RETRY) => {}
                _ = closing.wait_for(|c| *c) => return,
            }
        }
    }
}

/// Once `leader` no longer leads.
async fn moved_on(
    raft: &mut tokio::sync::watch::Receiver<openraft::RaftMetrics<NodeId, openraft::BasicNode>>,
    leader: NodeId,
) {
    loop {
        if raft.changed().await.is_err() {
            return std::future::pending().await;
        }
        if raft.borrow().current_leader != Some(leader) {
            return;
        }
    }
}

/// Follows the leader's usage until the stream ends; says why.
async fn follow(api: &ApiService, addr: &str) -> String {
    let mut c = pb::cluster_client::ClusterClient::new(connect_lazy(addr, api.peer_tls.as_ref()))
        .max_decoding_message_size(MAX_MESSAGE);
    let request = forwarded(pb::UsageRequest {
        namespace: String::new(),
        system: true,
    });
    let mut stream = match c.watch_usage(request).await {
        Ok(r) => r.into_inner(),
        Err(e) => return e.to_string(),
    };
    loop {
        match stream.message().await {
            Ok(Some(u)) => api.usage.mirror(telemetry(u)),
            Ok(None) => return "the leader closed it".to_owned(),
            Err(e) => return e.to_string(),
        }
    }
}

/// Each node's telemetry in a usage answer.
fn telemetry(u: pb::UsageResponse) -> BTreeMap<String, NodeTelemetry> {
    u.nodes
        .into_iter()
        .map(|n| {
            let t = NodeTelemetry {
                zfs_used_bytes: n.zfs_used_bytes,
                zfs_avail_bytes: n.zfs_avail_bytes,
                net_rx_bps: n.net_rx_bps,
                net_tx_bps: n.net_tx_bps,
                zfs_read_bps: n.zfs_read_bps,
                zfs_write_bps: n.zfs_write_bps,
                zfs_read_iops: n.zfs_read_iops,
                zfs_write_iops: n.zfs_write_iops,
                volumes: n
                    .volumes
                    .into_iter()
                    .map(|v| VolumeTelemetry {
                        namespace: v.namespace,
                        volume: v.volume,
                        used_bytes: v.used_bytes,
                        avail_bytes: v.avail_bytes,
                    })
                    .collect(),
                images_bytes: n.images_bytes,
                jails_bytes: n.jails_bytes,
                build_bytes: n.build_bytes,
                load: [n.load1, n.load5, n.load15],
            };
            (n.node, t)
        })
        .collect()
}
