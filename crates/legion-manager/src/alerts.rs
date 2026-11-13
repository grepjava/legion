//! A manager's alerts (`legion_api::alert`, `Cluster.WatchAlerts`): worked
//! out from its own replica, the restarts it has seen and its view of
//! Raft, so any manager answers, one cut off from the leader too.
//!
//! Restarts are counted as this manager sees each task's `status.restarts`
//! rise; those before it first looked don't count, so a manager that has
//! just started raises a crash loop once it has seen the restarts itself.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use legion_api::alert::{self, Alert, CRASH_LOOP_RESTARTS, CRASH_LOOP_WINDOW, Inputs};
use legion_api::node::NodeState;
use legion_api::{Kind, Object};
use legion_store::{Event, Store};
use tokio::time::Instant;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

/// How often the list is worked out again with nothing else happening:
/// the restarts' window moving on, Raft's view of the other managers.
pub(crate) const RECHECK: Duration = Duration::from_secs(2);

#[derive(Default)]
pub(crate) struct State {
    /// By `namespace/task`: its restarts when last seen, and when each seen
    /// since rose, within the window.
    restarts: Mutex<HashMap<String, Seen>>,
    /// By alert key: when this manager first raised it, in ms since the epoch.
    since: Mutex<HashMap<String, i64>>,
}

struct Seen {
    count: u32,
    rises: VecDeque<Instant>,
}

fn task_key(o: &Object) -> String {
    format!("{}/{}", o.metadata.namespace, o.metadata.name)
}

fn restarts_of(o: &Object) -> u32 {
    o.status["restarts"].as_u64().unwrap_or(0) as u32
}

impl State {
    fn observe(&self, key: String, count: u32) {
        let mut m = self.restarts.lock().unwrap();
        match m.get_mut(&key) {
            Some(s) => {
                // A burst counts in full, up to what makes the alert twice.
                let rose = count.saturating_sub(s.count).min(CRASH_LOOP_RESTARTS * 2);
                let now = Instant::now();
                s.rises.extend((0..rose).map(|_| now));
                s.count = count;
            }
            None => {
                m.insert(
                    key,
                    Seen {
                        count,
                        rises: VecDeque::new(),
                    },
                );
            }
        }
    }

    fn forget(&self, key: &str) {
        self.restarts.lock().unwrap().remove(key);
    }

    /// The task's restarts seen within the window.
    fn recent(&self, key: &str) -> u32 {
        let mut m = self.restarts.lock().unwrap();
        let Some(s) = m.get_mut(key) else { return 0 };
        while s
            .rises
            .front()
            .is_some_and(|t| t.elapsed() > CRASH_LOOP_WINDOW)
        {
            s.rises.pop_front();
        }
        s.rises.len() as u32
    }

    /// When each alert was first raised, keeping only those raised now.
    fn since(&self, alerts: &[Alert]) -> Vec<i64> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        let mut m = self.since.lock().unwrap();
        let keys: Vec<String> = alerts.iter().map(Alert::key).collect();
        m.retain(|k, _| keys.contains(k));
        keys.into_iter()
            .map(|k| *m.entry(k).or_insert(now))
            .collect()
    }
}

/// Follows the replica's tasks, counting their restarts as they rise.
pub(crate) async fn track(
    store: Store,
    state: std::sync::Arc<State>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *closing.borrow() {
            return;
        }
        // Listed, then watched from the listing's version; again after a
        // snapshot replaced the state or the watch fell behind.
        let watched = store
            .list(Kind::Task, None)
            .map_err(|e| e.to_string())
            .and_then(|listing| {
                for o in &listing.objects {
                    state.observe(task_key(o), restarts_of(o));
                }
                store
                    .watch(listing.version, Some(Kind::Task))
                    .map_err(|e| e.to_string())
            });
        let mut watcher = match watched {
            Ok(w) => w,
            Err(e) => {
                tracing::debug!("alerts: listing tasks: {e}");
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    _ = closing.wait_for(|c| *c) => return,
                }
            }
        };
        loop {
            let event = tokio::select! {
                e = watcher.next() => e,
                _ = closing.wait_for(|c| *c) => return,
            };
            match event {
                Ok(Event::Put(o)) => state.observe(task_key(&o), restarts_of(&o)),
                Ok(Event::Delete(o)) => state.forget(&task_key(&o)),
                Ok(Event::Reset(_)) | Err(_) => break,
            }
        }
    }
}

/// The alerts as this manager sees them now, with when each was first raised.
pub(crate) fn current(api: &ApiService) -> Vec<(Alert, i64)> {
    let store = &api.store;
    let objects = |kind| {
        store
            .list(kind, None)
            .map(|l| l.objects)
            .unwrap_or_default()
    };
    let (nodes, tasks, volumes) = (
        objects(Kind::Node),
        objects(Kind::Task),
        objects(Kind::Volume),
    );
    let mut inputs = Inputs::default();
    for n in &nodes {
        let state: NodeState =
            serde_json::from_value(n.status["state"].clone()).unwrap_or_default();
        inputs.nodes.push((&n.metadata.name, state));
    }
    let recent: Vec<u32> = tasks
        .iter()
        .map(|t| api.alerts.recent(&task_key(t)))
        .collect();
    for (t, recent) in tasks.iter().zip(recent) {
        if t.metadata.namespace == SYSTEM_NAMESPACE || recent == 0 {
            continue;
        }
        inputs.tasks.push(alert::TaskInput {
            namespace: &t.metadata.namespace,
            name: &t.metadata.name,
            recent_restarts: recent,
            message: t.status["message"].as_str().unwrap_or(""),
        });
    }
    for v in &volumes {
        inputs.volumes.push(alert::VolumeInput {
            namespace: &v.metadata.namespace,
            name: &v.metadata.name,
            lost: v.status["lost"].as_str().unwrap_or(""),
        });
    }
    if let Some(c) = &api.peer_tls
        && let Ok((not_after, _)) = legion_pki::validity(&c.ca_pem())
    {
        inputs.ca_left = Some(
            not_after
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO),
        );
    }
    let m = store.raft.metrics().borrow().clone();
    inputs.leader = m.current_leader.is_some();
    if m.current_leader == Some(api.id) {
        inputs.quorum_silence = m.millis_since_quorum_ack.map(Duration::from_millis);
    }
    let alerts = alert::alerts(&inputs);
    let since = api.alerts.since(&alerts);
    alerts.into_iter().zip(since).collect()
}
