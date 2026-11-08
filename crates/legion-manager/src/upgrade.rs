//! The managers' protocol floor (DESIGN.md 3.8, Versions). Once every
//! manager speaks a protocol, the leader records it as the floor, and from
//! then on the log may hold commands of that protocol: managers below it
//! are refused, as they could not apply them. The floor only rises; a
//! manager started on an older build than it takes no part.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::protocol::Protocol;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::join::SYSTEM_NAMESPACE;

/// How recently a manager must have answered to count as speaking what it
/// said.
const RECENT: Duration = Duration::from_secs(10);
/// How often the floor is read, and, while leading, raised if it can be.
const PERIOD: Duration = Duration::from_secs(1);

/// The Config holding the floor.
pub(crate) fn key() -> ObjectKey {
    ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, "protocol")
}

/// The floor in the store: 1 until raised.
pub(crate) fn read(store: &Store) -> u32 {
    let Ok(Some(o)) = store.get(&key()) else {
        return 1;
    };
    serde_json::from_value::<DataSpec>(o.spec)
        .ok()
        .and_then(|s| s.text)
        .and_then(|t| t.trim().parse().ok())
        .unwrap_or(1)
}

/// What this manager speaks, and the floor as last read: shared by the
/// Raft transport, the API and the join service.
#[derive(Clone, Debug)]
pub(crate) struct Speaking {
    pub mine: Protocol,
    floor: Arc<AtomicU32>,
}

impl Speaking {
    pub(crate) fn new(mine: Protocol) -> Self {
        Speaking {
            mine,
            floor: Arc::new(AtomicU32::new(1)),
        }
    }

    pub(crate) fn floor(&self) -> u32 {
        self.floor.load(Ordering::Relaxed)
    }

    /// What this manager works with among managers: its own oldest, or the
    /// floor if higher.
    pub(crate) fn among_managers(&self) -> Protocol {
        Protocol {
            speaks: self.mine.speaks,
            oldest: Protocol::said(self.mine.speaks, self.mine.oldest)
                .oldest
                .max(self.floor()),
        }
    }

    fn set_floor(&self, floor: u32) {
        self.floor.store(floor, Ordering::Relaxed);
    }
}

/// What each manager last said it speaks, and when, as its Raft answers
/// say.
pub(crate) type Heard = Arc<Mutex<HashMap<NodeId, (Protocol, Instant)>>>;

/// Keeps `speaking`'s floor as the store has it and, while this manager
/// leads, raises it to what every manager speaks, until `closing`.
pub(crate) async fn run(
    store: Store,
    id: NodeId,
    speaking: Speaking,
    heard: Heard,
    mut closing: watch::Receiver<bool>,
) {
    loop {
        speaking.set_floor(read(&store));
        if let Some(to) = raisable(&store, id, &speaking, &heard) {
            raise(&store, to).await;
            speaking.set_floor(read(&store));
        }
        tokio::select! {
            () = tokio::time::sleep(PERIOD) => {}
            _ = closing.wait_for(|c| *c) => return,
        }
    }
}

/// The floor every manager speaks, if this manager leads and it is above
/// the floor now. A manager not heard from lately counts as speaking 1.
fn raisable(store: &Store, id: NodeId, speaking: &Speaking, heard: &Heard) -> Option<u32> {
    let m = store.raft.metrics().borrow().clone();
    if m.current_leader != Some(id) {
        return None;
    }
    let heard = heard.lock().unwrap();
    let all = m
        .membership_config
        .membership()
        .nodes()
        .map(|(n, _)| *n)
        .filter(|n| *n != id)
        .map(|n| match heard.get(&n) {
            Some((p, at)) if at.elapsed() < RECENT => p.speaks,
            _ => 1,
        })
        .fold(speaking.mine.speaks, u32::min);
    (all > speaking.floor()).then_some(all)
}

async fn raise(store: &Store, to: u32) {
    let spec = DataSpec {
        data: None,
        text: Some(to.to_string()),
    };
    let (object, precondition) = match store.get(&key()) {
        Ok(Some(mut o)) => {
            let version = o.metadata.version;
            o.spec = serde_json::to_value(spec).expect("serializes");
            (o, Precondition::Version(version))
        }
        Ok(None) => (
            Object::new(
                Kind::Config,
                SYSTEM_NAMESPACE,
                "protocol",
                serde_json::to_value(spec).expect("serializes"),
            ),
            Precondition::MustNotExist,
        ),
        Err(e) => {
            tracing::warn!("reading the protocol floor: {e}");
            return;
        }
    };
    let write = store.raft.client_write(Command::Put {
        object,
        precondition,
    });
    match tokio::time::timeout(crate::COMMIT_TIMEOUT, write).await {
        Ok(Ok(r)) if matches!(r.data, Outcome::Written(_)) => {
            tracing::info!(
                protocol = to,
                "every manager speaks protocol {to}: the floor now"
            );
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::debug!("raising the protocol floor: {e}"),
        Err(_) => tracing::debug!("raising the protocol floor: no quorum"),
    }
}
