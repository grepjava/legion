//! The audit trail (PLAN.md phase 7, docs/DESIGN.md, Audit): each change,
//! session and refusal a manager decides becomes an event
//! (legion_api::audit), kept in the replicated store. Events are batched:
//! once a second the leader writes what it and the followers decided as one
//! Config in the system namespace (`audit-<time>-<n>`), a follower handing
//! its own to the leader (`RecordAudit`). The leader drops batches older
//! than the retention, and the oldest while there are more events than the
//! cap. Events name things, never their contents.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use legion_api::audit::{Actor, AuditEvent, Filter, Outcome, Target};
use legion_api::data::DataSpec;
use legion_api::{Kind, Object, ObjectKey};
use legion_pki::Role;
use legion_store::{Command, Precondition, Store};
use tokio::sync::mpsc;
use tonic::{Code, Request, Status};

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

/// Batches are named `audit-<ms>-<n>`.
const PREFIX: &str = "audit-";
/// The label holding a batch's number of events.
const COUNT: &str = "legion.audit/count";
/// How often what was recorded is written.
const FLUSH: Duration = Duration::from_secs(1);
/// Events held before they are written; past it the oldest go, counted.
const HELD: usize = 10_000;
/// The most events in one batch.
const BATCH: usize = 1_000;

/// How long events are kept, and how many at most.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub age: Duration,
    pub events: u64,
    /// How often the leader drops what is past it.
    pub every: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            age: Duration::from_secs(30 * 24 * 3600),
            events: 100_000,
            every: Duration::from_secs(60),
        }
    }
}

/// Where a manager records events, for its writer to keep.
#[derive(Clone)]
pub(crate) struct Audit {
    tx: mpsc::Sender<AuditEvent>,
    /// This manager's node name, which each event gives.
    manager: Arc<str>,
    /// Events lost as more were recorded than could be kept.
    lost: Arc<AtomicU64>,
    /// Numbers this manager's batches, so no two share a name.
    seq: Arc<AtomicU64>,
}

impl Audit {
    pub(crate) fn new(manager: &str) -> (Audit, mpsc::Receiver<AuditEvent>) {
        let (tx, rx) = mpsc::channel(HELD);
        (
            Audit {
                tx,
                manager: manager.into(),
                lost: Default::default(),
                seq: Default::default(),
            },
            rx,
        )
    }

    /// The count of events lost, for the writer to report.
    pub(crate) fn lost(&self) -> Arc<AtomicU64> {
        self.lost.clone()
    }

    /// Records that `actor` asked for `action` on `target`, ending as `result`.
    pub(crate) fn record<T>(
        &self,
        actor: Actor,
        action: &str,
        target: Target,
        result: &Result<T, Status>,
    ) {
        let (outcome, message) = match result {
            Ok(_) => (Outcome::Ok, ""),
            Err(e) if matches!(e.code(), Code::PermissionDenied | Code::Unauthenticated) => {
                (Outcome::Denied, e.message())
            }
            Err(e) => (Outcome::Error, e.message()),
        };
        let event = AuditEvent {
            time_ms: now_ms(),
            actor,
            action: action.to_owned(),
            target,
            result: outcome,
            message: String::new(),
            manager: self.manager.to_string(),
        }
        .with_message(message);
        if self.tx.try_send(event).is_err() {
            self.lost.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Who made `req`, as its certificate names them; a user a manager
/// forwards for is that user.
pub(crate) fn actor<T>(req: &Request<T>, tls: bool) -> Actor {
    let Ok(Some(id)) = crate::tls::caller(req, tls) else {
        return Actor {
            kind: "none".into(),
            name: String::new(),
        };
    };
    if id.role == Role::Manager
        && let Some(user) = req
            .metadata()
            .get(crate::authz::FORWARDED_FOR)
            .and_then(|v| v.to_str().ok())
    {
        return Actor {
            kind: "user".into(),
            name: user.to_owned(),
        };
    }
    Actor {
        kind: id.role.as_str().to_owned(),
        name: id.name,
    }
}

/// The target `kind`/`namespace`/`name`.
pub(crate) fn target(kind: &str, namespace: &str, name: &str) -> Target {
    Target {
        kind: kind.to_owned(),
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    }
}

/// Writes what is recorded, as the leader, or hands it to the leader; and,
/// leading, drops what is past `retention`. Runs until `closing` turns.
pub(crate) async fn run(
    api: ApiService,
    mut rx: mpsc::Receiver<AuditEvent>,
    lost: Arc<AtomicU64>,
    retention: Retention,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let mut held: VecDeque<AuditEvent> = VecDeque::new();
    let mut pruned = tokio::time::Instant::now();
    loop {
        let closed = tokio::select! {
            () = tokio::time::sleep(FLUSH) => false,
            _ = closing.wait_for(|c| *c) => true,
        };
        if closed {
            // What is held is written if it can be, once.
            while let Ok(e) = rx.try_recv() {
                held.push_back(e);
            }
            let _ = flush(&api, &mut held).await;
            return;
        }
        while let Ok(e) = rx.try_recv() {
            held.push_back(e);
        }
        let dropped = lost.swap(0, Ordering::Relaxed);
        if held.len() > HELD {
            let over = held.len() - HELD;
            held.drain(..over);
            lost.fetch_add(over as u64, Ordering::Relaxed);
        }
        if dropped > 0 {
            tracing::warn!(
                dropped,
                "audit: events lost, more were recorded than could be kept"
            );
        }
        if let Err(e) = flush(&api, &mut held).await {
            tracing::debug!("audit: {e}");
        }
        if pruned.elapsed() >= retention.every && leads(&api) {
            pruned = tokio::time::Instant::now();
            if let Err(e) = prune(&api, retention).await {
                tracing::debug!("audit: pruning: {e}");
            }
        }
    }
}

fn leads(api: &ApiService) -> bool {
    api.store.raft.metrics().borrow().current_leader == Some(api.id)
}

/// Writes `held`, in batches, as the leader, or hands it to the leader. What
/// could not be kept stays held for the next try.
async fn flush(api: &ApiService, held: &mut VecDeque<AuditEvent>) -> Result<(), Status> {
    while !held.is_empty() {
        let n = held.len().min(BATCH);
        let batch: Vec<AuditEvent> = held.iter().take(n).cloned().collect();
        if leads(api) {
            write(api, &batch).await?;
        } else {
            hand_to_leader(api, &batch).await?;
        }
        held.drain(..n);
    }
    Ok(())
}

/// Writes `batch` as one Config, as the leader.
pub(crate) async fn write(api: &ApiService, batch: &[AuditEvent]) -> Result<(), Status> {
    let seq = api.audit.seq.fetch_add(1, Ordering::Relaxed);
    let text = serde_json::to_string(batch).expect("events serialize");
    let spec = DataSpec {
        data: None,
        text: Some(text),
    };
    let name = format!("{PREFIX}{}-{}-{seq}", now_ms(), api.id % 1_000_000);
    let mut object = Object::new(
        Kind::Config,
        SYSTEM_NAMESPACE,
        &name,
        serde_json::to_value(spec).expect("serializes"),
    );
    object
        .metadata
        .labels
        .insert(COUNT.to_owned(), batch.len().to_string());
    api.write(Command::Put {
        object,
        precondition: Precondition::MustNotExist,
    })
    .await?;
    Ok(())
}

async fn hand_to_leader(api: &ApiService, batch: &[AuditEvent]) -> Result<(), Status> {
    let addr = {
        let m = api.store.raft.metrics().borrow().clone();
        let leader = m
            .current_leader
            .ok_or_else(|| Status::unavailable("no leader to keep the audit events"))?;
        m.membership_config
            .membership()
            .get_node(&leader)
            .map(|n| n.addr.clone())
            .ok_or_else(|| Status::unavailable("the leader's address is not known"))?
    };
    let mut c = legion_proto::v1::cluster_client::ClusterClient::new(
        crate::raft_net::connect_lazy(&addr, api.peer_tls.as_ref()),
    );
    let events_json = batch
        .iter()
        .map(|e| serde_json::to_vec(e).expect("events serialize"))
        .collect();
    let sent = c.record_audit(legion_proto::v1::RecordAuditRequest { events_json });
    tokio::time::timeout(Duration::from_secs(10), sent)
        .await
        .map_err(|_| Status::unavailable("the leader did not take the audit events"))??;
    Ok(())
}

/// The batches, oldest first: their keys, times and event counts.
fn batches(store: &Store) -> Result<Vec<(Object, u64, u64)>, Status> {
    let list = store
        .list(Kind::Config, Some(SYSTEM_NAMESPACE))
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut out: Vec<(Object, u64, u64)> = list
        .objects
        .into_iter()
        .filter_map(|o| {
            let rest = o.metadata.name.strip_prefix(PREFIX)?;
            let time: u64 = rest.split('-').next()?.parse().ok()?;
            let count = o
                .metadata
                .labels
                .get(COUNT)
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            Some((o, time, count))
        })
        .collect();
    out.sort_by(|a, b| (a.1, &a.0.metadata.name).cmp(&(b.1, &b.0.metadata.name)));
    Ok(out)
}

/// Which of the batches (time, count), oldest first, to drop at `now`: those
/// older than the retention's age, then the oldest while the rest hold more
/// events than its cap.
pub(crate) fn past(batches: &[(u64, u64)], now_ms: u64, retention: Retention) -> usize {
    let cutoff = now_ms.saturating_sub(retention.age.as_millis() as u64);
    let mut drop = batches.iter().take_while(|(t, _)| *t < cutoff).count();
    let mut left: u64 = batches[drop..].iter().map(|(_, c)| c).sum();
    while left > retention.events && drop < batches.len() {
        left -= batches[drop].1;
        drop += 1;
    }
    drop
}

async fn prune(api: &ApiService, retention: Retention) -> Result<(), Status> {
    let all = batches(&api.store)?;
    let times: Vec<(u64, u64)> = all.iter().map(|(_, t, c)| (*t, *c)).collect();
    let drop = past(&times, now_ms(), retention);
    for (o, _, _) in all.into_iter().take(drop) {
        api.write(Command::Delete {
            key: ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, &o.metadata.name),
            precondition: Precondition::None,
        })
        .await?;
    }
    Ok(())
}

/// The events `filter` matches, oldest first, at most `limit`; and whether
/// more matched.
pub(crate) fn list(
    store: &Store,
    filter: &Filter,
    limit: usize,
) -> Result<(Vec<AuditEvent>, bool), Status> {
    let mut out = Vec::new();
    for (o, time, _) in batches(store)? {
        // A batch is written after its events: none before `since` holds
        // one after it, but for the second it waited.
        if (time + 2 * FLUSH.as_millis() as u64) < filter.since_ms {
            continue;
        }
        let spec: DataSpec = match serde_json::from_value(o.spec) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let events: Vec<AuditEvent> =
            serde_json::from_str(spec.text.as_deref().unwrap_or("[]")).unwrap_or_default();
        for e in events.into_iter().filter(|e| filter.matches(e)) {
            if out.len() == limit {
                return Ok((out, true));
            }
            out.push(e);
        }
    }
    Ok((out, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_go_by_age_then_by_count() {
        let day = 24 * 3600 * 1000;
        let retention = Retention {
            age: Duration::from_secs(30 * 24 * 3600),
            events: 100,
            every: Duration::from_secs(60),
        };
        let now = 100 * day;
        // Old ones go first.
        let b = [(now - 40 * day, 10), (now - 31 * day, 10), (now - day, 10)];
        assert_eq!(past(&b, now, retention), 2);
        // Within the age, the oldest go while the rest are over the cap.
        let b = [(now - 3 * day, 60), (now - 2 * day, 50), (now - day, 40)];
        assert_eq!(past(&b, now, retention), 1);
        let b = [(now - day, 30), (now, 30)];
        assert_eq!(past(&b, now, retention), 0);
        // One batch over the cap on its own goes too: nothing is kept past it.
        let b = [(now, 150)];
        assert_eq!(past(&b, now, retention), 1);
        assert_eq!(past(&[], now, retention), 0);
    }
}
