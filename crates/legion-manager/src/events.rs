//! A manager's events (`legion_api::event`, `Cluster.Events`): worked out
//! from the changes its replica sees, each object's summary compared with
//! the one before, and kept in memory: the last hour, at most
//! [`PER_NAMESPACE`] a namespace. Nothing is written, so every manager has
//! its own, from when it started; the objects there when it first looked
//! give none.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use legion_api::event::{self, Event, Summary};
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Event as Change, Store};
use tokio::sync::broadcast;

/// Kept a namespace (the cluster's objects as one), newest last.
pub(crate) const PER_NAMESPACE: usize = 1000;
/// Kept at most this long.
pub(crate) const KEPT: Duration = Duration::from_secs(3600);

pub(crate) struct State {
    /// By namespace, oldest first.
    events: Mutex<HashMap<String, VecDeque<Event>>>,
    /// Each event as it comes, for those following.
    new: broadcast::Sender<Event>,
}

impl Default for State {
    fn default() -> State {
        State {
            events: Mutex::default(),
            new: broadcast::channel(1024).0,
        }
    }
}

impl State {
    fn add(&self, event: Event) {
        let now = legion_api::clock::now() * 1000;
        let kept_ms = KEPT.as_millis() as i64;
        {
            let mut all = self.events.lock().unwrap_or_else(|e| e.into_inner());
            let q = all.entry(event.namespace.clone()).or_default();
            q.push_back(event.clone());
            while q.len() > PER_NAMESPACE || q.front().is_some_and(|e| now - e.at_ms > kept_ms) {
                q.pop_front();
            }
        }
        let _ = self.new.send(event);
    }

    /// The events of `namespace` that `wanted` takes, oldest first.
    pub(crate) fn of(&self, namespace: &str, wanted: impl Fn(&Event) -> bool) -> Vec<Event> {
        let now = legion_api::clock::now() * 1000;
        let kept_ms = KEPT.as_millis() as i64;
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(namespace)
            .map(|q| {
                q.iter()
                    .filter(|e| now - e.at_ms <= kept_ms && wanted(e))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The events still to come.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.new.subscribe()
    }
}

/// What a change to `key` makes: `before` the summary kept, `after` the
/// object now (`None`: gone).
fn record(
    state: &State,
    summaries: &mut HashMap<ObjectKey, Summary>,
    key: ObjectKey,
    after: Option<&Object>,
) {
    let now = after.and_then(Summary::of);
    let before = summaries.get(&key);
    let at_ms = legion_api::clock::now() * 1000;
    for (reason, message) in event::changes(key.kind, before, now.as_ref()) {
        let owner = now
            .as_ref()
            .or(before)
            .map(|s| s.owner.clone())
            .unwrap_or_default();
        state.add(Event {
            at_ms,
            kind: key.kind,
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            reason,
            message,
            owner,
        });
    }
    match now {
        Some(s) => {
            summaries.insert(key, s);
        }
        None => {
            summaries.remove(&key);
        }
    }
}

/// Follows the replica, working out events as it changes.
pub(crate) async fn track(
    store: Store,
    state: std::sync::Arc<State>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let mut summaries: HashMap<ObjectKey, Summary> = HashMap::new();
    let mut first = true;
    loop {
        if *closing.borrow() {
            return;
        }
        // Listed, then watched from the listing's version; again after a
        // snapshot replaced the state or the watch fell behind, comparing
        // with what was known, so what changed meanwhile still counts.
        let watched = (|| {
            let version = store.applied_version().map_err(|e| e.to_string())?;
            let mut seen = HashMap::new();
            for kind in event::KINDS {
                for o in store.list(kind, None).map_err(|e| e.to_string())?.objects {
                    if let Some(s) = Summary::of(&o) {
                        seen.insert(o.key(), (s, o));
                    }
                }
            }
            let watcher = store.watch(version, None).map_err(|e| e.to_string())?;
            Ok::<_, String>((seen, watcher))
        })();
        let (seen, mut watcher) = match watched {
            Ok(w) => w,
            Err(e) => {
                tracing::debug!("events: listing: {e}");
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    _ = closing.wait_for(|c| *c) => return,
                }
            }
        };
        if first {
            // The first look: what is there now is no event.
            summaries = seen.into_iter().map(|(k, (s, _))| (k, s)).collect();
            first = false;
        } else {
            let gone: Vec<ObjectKey> = summaries
                .keys()
                .filter(|k| !seen.contains_key(k))
                .cloned()
                .collect();
            for key in gone {
                record(&state, &mut summaries, key, None);
            }
            for (key, (_, o)) in seen {
                record(&state, &mut summaries, key, Some(&o));
            }
        }
        loop {
            let change = tokio::select! {
                c = watcher.next() => c,
                _ = closing.wait_for(|c| *c) => return,
            };
            match change {
                Ok(Change::Put(o)) if event::KINDS.contains(&o.kind) => {
                    record(&state, &mut summaries, o.key(), Some(&o));
                }
                Ok(Change::Delete(o)) if event::KINDS.contains(&o.kind) => {
                    record(&state, &mut summaries, o.key(), None);
                }
                Ok(Change::Put(_) | Change::Delete(_)) => {}
                Ok(Change::Reset(_)) | Err(_) => break,
            }
        }
    }
}

/// Whether `e` is about the object `kind/name`, or for a Service or Job one
/// of its tasks.
pub(crate) fn about(e: &Event, kind: Kind, name: &str) -> bool {
    (e.kind == kind && e.name == name)
        || (matches!(kind, Kind::Service | Kind::Job) && e.kind == Kind::Task && e.owner == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(name: &str, state: &str) -> Object {
        let mut t = Object::new(
            Kind::Task,
            "shop",
            name,
            serde_json::json!({ "service": "web", "node": "n1" }),
        );
        t.status = serde_json::json!({ "state": state });
        t
    }

    #[test]
    fn changes_are_kept_by_namespace_and_found_by_owner() {
        let state = State::default();
        let mut summaries = HashMap::new();
        let mut follower = state.subscribe();
        let t = task("web-1", "pending");
        record(&state, &mut summaries, t.key(), Some(&t));
        let t = task("web-1", "running");
        record(&state, &mut summaries, t.key(), Some(&t));
        record(&state, &mut summaries, t.key(), None);
        let all = state.of("shop", |_| true);
        let reasons: Vec<&str> = all.iter().map(|e| e.reason.as_str()).collect();
        assert_eq!(reasons, ["Created", "Running", "Deleted"]);
        assert!(all.iter().all(|e| about(e, Kind::Service, "web")));
        assert!(!all.iter().any(|e| about(e, Kind::Service, "api")));
        assert!(state.of("other", |_| true).is_empty());
        assert_eq!(follower.try_recv().unwrap().reason, "Created");
    }

    #[test]
    fn a_namespace_keeps_its_newest() {
        let state = State::default();
        let mut summaries = HashMap::new();
        for i in 0..PER_NAMESPACE + 5 {
            let t = task(&format!("t-{i}"), "pending");
            record(&state, &mut summaries, t.key(), Some(&t));
        }
        let all = state.of("shop", |_| true);
        assert_eq!(all.len(), PER_NAMESPACE);
        assert_eq!(all[0].name, "t-5");
    }
}
