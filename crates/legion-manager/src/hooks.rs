//! Pause points for tests that drive races (the `test-hooks` feature, which
//! only this crate's own tests turn on): a test arms a point for a key, and
//! the next run through it stops there until the test lets it go.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::oneshot;

type Gate = (oneshot::Sender<()>, oneshot::Receiver<()>);

static GATES: Mutex<Option<HashMap<(&'static str, String), Gate>>> = Mutex::new(None);

/// A point armed for one run, which stops there until this is released
/// or dropped.
pub struct Paused {
    reached: Option<oneshot::Receiver<()>>,
    release: oneshot::Sender<()>,
}

impl Paused {
    /// Resolves once a run has stopped at the point.
    pub async fn reached(&mut self) {
        if let Some(r) = self.reached.take() {
            let _ = r.await;
        }
    }

    /// Lets the run go on.
    pub fn release(self) {
        let _ = self.release.send(());
    }
}

/// Arms `point` for the next run through it for `key` (a node's name).
pub fn arm(point: &'static str, key: &str) -> Paused {
    let (reached_tx, reached) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    GATES
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .insert((point, key.to_owned()), (reached_tx, release_rx));
    Paused {
        reached: Some(reached),
        release,
    }
}

/// Stops here if a test armed `point` for `key`, until it lets go.
pub(crate) async fn pause(point: &'static str, key: &str) {
    let gate = GATES
        .lock()
        .unwrap()
        .as_mut()
        .and_then(|g| g.remove(&(point, key.to_owned())));
    if let Some((reached, release)) = gate {
        let _ = reached.send(());
        let _ = release.await;
    }
}
