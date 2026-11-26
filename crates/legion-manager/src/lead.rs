//! Handing leadership to another manager (`legion node lead NODE`,
//! docs/HA.md), with nothing brought down.
//!
//! Raft has the followers refuse to vote while they still trust the
//! leader's lease (`election_timeout_max`), so another manager can't be
//! elected while this one leads, and openraft 0.9 has no call that hands
//! leadership over. So this leader goes quiet: it stops its heartbeats,
//! writes nothing and confirms nothing (`Store::hush`), until every lease
//! on it has run out; then it asks the manager it hands over to to stand
//! (`Cluster.Campaign`), which, its log as long as any, wins. Meanwhile
//! nothing is placed or changed, for about a lease; no node is marked
//! down, as a new leader counts nodes' silence only from when it took
//! over, and running tasks carry on. Whatever happens, this manager is
//! itself again at the end: heartbeats and elections back on.

use std::time::{Duration, Instant};

use legion_store::NodeId;
use tonic::Status;

use crate::api::ApiService;
use crate::raft_net::connect_lazy;

/// Heartbeats and elections back on when dropped, as this manager was.
struct Restore {
    raft: legion_store::Raft,
    witness: bool,
}

impl Drop for Restore {
    fn drop(&mut self) {
        self.raft.runtime_config().heartbeat(true);
        // A witness never stands.
        self.raft.runtime_config().elect(!self.witness);
    }
}

/// Hands leadership from this manager, the leader, to the manager `to`
/// reached at `addr`. Returns how long it took.
pub(crate) async fn hand_over(
    api: &ApiService,
    to: NodeId,
    addr: &str,
) -> Result<Duration, Status> {
    // One at a time: a second, ending first, would wake this leader in the
    // middle of the first's quiet (found by an audit).
    let Ok(_one) = api.handing_over.try_lock() else {
        return Err(Status::aborted(
            "leadership is being handed over already; retry once it is done",
        ));
    };
    let raft = api.store.raft.clone();
    let lease = Duration::from_millis(raft.config().election_timeout_max);
    let began = Instant::now();

    // It must be able to win: caught up with this leader's log.
    let caught_up = |m: &openraft::RaftMetrics<NodeId, openraft::BasicNode>| {
        let last = m.last_log_index;
        m.replication
            .as_ref()
            .and_then(|r| r.get(&to))
            .is_some_and(|matched| matched.map(|l| l.index) >= last)
    };
    if !caught_up(&raft.metrics().borrow()) {
        raft.wait(Some(Duration::from_secs(5)))
            .metrics(|m| caught_up(m), "the new leader to catch up")
            .await
            .map_err(|_| {
                Status::failed_precondition(
                    "the manager to hand over to is not caught up with the leader's log; is it reachable?",
                )
            })?;
    }

    let hush = api.store.hush();
    raft.runtime_config().heartbeat(false);
    raft.runtime_config().elect(false);
    let restore = Restore {
        raft: raft.clone(),
        witness: api.witness,
    };
    tracing::info!(
        to,
        "handing leadership on: quiet until the followers' leases run out"
    );

    // What was being sent when it went quiet arrives; then every lease on
    // this leader runs out.
    tokio::time::sleep(lease + Duration::from_millis(500)).await;

    let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
        addr,
        api.peer_tls.as_ref(),
    ));
    tokio::time::timeout(
        Duration::from_secs(5),
        c.campaign(legion_proto::v1::CampaignRequest {}),
    )
    .await
    .map_err(|_| Status::unavailable("asking it to stand timed out"))??;

    let elected = raft
        .wait(Some(lease * 2))
        .metrics(|m| m.current_leader == Some(to), "the new leader")
        .await;
    drop(restore);
    drop(hush);
    match elected {
        Ok(_) => {
            tracing::info!(to, took = ?began.elapsed(), "leadership handed on");
            Ok(began.elapsed())
        }
        Err(_) => {
            let now = raft.metrics().borrow().current_leader;
            Err(Status::aborted(format!(
                "it was not elected (the leader is {now:?}); this manager leads on if it still can"
            )))
        }
    }
}
