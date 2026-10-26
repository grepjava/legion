//! Watch streams over applied changes.
//!
//! The state machine keeps the most recent events in memory. A watcher asks
//! for everything after a version: if that range is still held it gets the
//! backlog and then live events, with no gap, because events are published and
//! subscribed under the same lock.

use std::collections::VecDeque;
use std::sync::Mutex;

use legion_api::{Kind, Object};
use tokio::sync::broadcast;

/// How many events stay in memory for watchers that resume.
pub(crate) const HISTORY: usize = 4096;
const CHANNEL: usize = 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Created or replaced; `metadata.version` is the write's version.
    Put(Object),
    /// Removed; `metadata.version` is the delete's version.
    Delete(Object),
    /// The state was replaced by a snapshot at this version. Watchers must
    /// list again.
    Reset(u64),
}

impl Event {
    pub fn version(&self) -> u64 {
        match self {
            Event::Put(o) | Event::Delete(o) => o.metadata.version,
            Event::Reset(v) => *v,
        }
    }

    fn matches(&self, kind: Option<Kind>) -> bool {
        match (self, kind) {
            (_, None) | (Event::Reset(_), _) => true,
            (Event::Put(o) | Event::Delete(o), Some(k)) => o.kind == k,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchError {
    #[error("changes after version {requested} are no longer held (oldest is {floor}); list again")]
    Compacted { requested: u64, floor: u64 },
    #[error("watcher fell behind; list again")]
    Lagged,
    #[error("store reset from a snapshot at version {0}; list again")]
    Reset(u64),
    #[error("store closed")]
    Closed,
}

pub(crate) struct Hub {
    inner: Mutex<HubState>,
}

struct HubState {
    history: VecDeque<Event>,
    /// Every event with a version at or below this is gone from `history`.
    floor: u64,
    tx: broadcast::Sender<Event>,
}

impl Hub {
    pub(crate) fn new(floor: u64) -> Self {
        Hub {
            inner: Mutex::new(HubState {
                history: VecDeque::new(),
                floor,
                tx: broadcast::channel(CHANNEL).0,
            }),
        }
    }

    pub(crate) fn publish(&self, events: impl IntoIterator<Item = Event>) {
        let mut st = self.inner.lock().unwrap();
        for ev in events {
            if let Event::Reset(v) = ev {
                st.history.clear();
                st.floor = v;
            } else {
                if st.history.len() == HISTORY
                    && let Some(old) = st.history.pop_front()
                {
                    st.floor = old.version();
                }
                st.history.push_back(ev.clone());
            }
            // No receivers is fine.
            let _ = st.tx.send(ev);
        }
    }

    pub(crate) fn watch(
        &self,
        from_version: u64,
        kind: Option<Kind>,
    ) -> Result<Watcher, WatchError> {
        let st = self.inner.lock().unwrap();
        if from_version < st.floor {
            return Err(WatchError::Compacted {
                requested: from_version,
                floor: st.floor,
            });
        }
        let backlog = st
            .history
            .iter()
            .filter(|e| e.version() > from_version && e.matches(kind))
            .cloned()
            .collect();
        Ok(Watcher {
            backlog,
            rx: st.tx.subscribe(),
            kind,
            last: from_version,
        })
    }
}

pub struct Watcher {
    backlog: VecDeque<Event>,
    rx: broadcast::Receiver<Event>,
    kind: Option<Kind>,
    /// Version of the last event delivered, or the starting version.
    last: u64,
}

impl Watcher {
    /// The next matching change. Errors are terminal for this watcher.
    pub async fn next(&mut self) -> Result<Event, WatchError> {
        if let Some(ev) = self.backlog.pop_front() {
            self.last = ev.version();
            return Ok(ev);
        }
        loop {
            let ev = match self.rx.recv().await {
                Ok(ev) => ev,
                Err(broadcast::error::RecvError::Lagged(_)) => return Err(WatchError::Lagged),
                Err(broadcast::error::RecvError::Closed) => return Err(WatchError::Closed),
            };
            if let Event::Reset(v) = ev {
                return Err(WatchError::Reset(v));
            }
            // A change is committed before it is published, so a reader can
            // list it and then subscribe in time to receive it live as well.
            if ev.version() <= self.last {
                continue;
            }
            if ev.matches(self.kind) {
                self.last = ev.version();
                return Ok(ev);
            }
        }
    }
}
