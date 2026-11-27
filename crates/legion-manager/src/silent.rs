//! Managers the leader has not heard from (the `manager-silent` alert). Only
//! the leader calls the others, so only it knows which answer; it records
//! those that have not for [`MANAGER_SILENCE`], and since when, as a Config
//! in the system namespace, so every manager raises the same alerts,
//! whichever the console or the CLI asks. Written only when that set
//! changes, not each time it looks. A voter gone silent used to raise
//! nothing while the others still made a majority, though the cluster could
//! then lose no other manager (found 2026-11-27, a witness's VPS destroyed).

use std::collections::BTreeMap;
use std::time::Duration;

use legion_api::alert::MANAGER_SILENCE;
use legion_api::data::DataSpec;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, NodeId, Outcome, Precondition, Store};
use tokio::time::Instant;
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;
use crate::upgrade::Heard;

const NAME: &str = "managers-silent";
/// How often the leader looks.
const PERIOD: Duration = Duration::from_secs(5);

fn key() -> ObjectKey {
    ObjectKey::new(Kind::Config, SYSTEM_NAMESPACE, NAME)
}

/// The managers recorded as silent, by Raft id: since when, in ms since
/// the epoch; and the record's version.
fn read_versioned(store: &Store) -> Result<(BTreeMap<NodeId, i64>, Option<u64>), Status> {
    let Some(o) = store
        .get(&key())
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return Ok((BTreeMap::new(), None));
    };
    let spec: DataSpec =
        serde_json::from_value(o.spec).map_err(|e| Status::internal(e.to_string()))?;
    let silent = serde_json::from_str(spec.text.as_deref().unwrap_or("{}"))
        .map_err(|e| Status::internal(format!("{NAME}: {e}")))?;
    Ok((silent, Some(o.metadata.version)))
}

/// The managers recorded as silent: since when, by Raft id.
pub(crate) fn read(store: &Store) -> Result<BTreeMap<NodeId, i64>, Status> {
    read_versioned(store).map(|(s, _)| s)
}

/// Which of `members` are silent at `now`, given when each last answered
/// (`heard`) and when this manager started leading: one never heard since
/// then is given [`MANAGER_SILENCE`] from then. Each keeps the time it was
/// first recorded silent (`was`); one new to it, when it was last heard.
pub(crate) fn silent(
    members: &[NodeId],
    heard: &BTreeMap<NodeId, Instant>,
    leading_since: Instant,
    was: &BTreeMap<NodeId, i64>,
    now: Instant,
    now_ms: i64,
) -> BTreeMap<NodeId, i64> {
    members
        .iter()
        .filter_map(|id| {
            let last = heard
                .get(id)
                .copied()
                .map_or(leading_since, |h| h.max(leading_since));
            let quiet = now.saturating_duration_since(last);
            (quiet >= MANAGER_SILENCE).then(|| {
                let since = was
                    .get(id)
                    .copied()
                    .unwrap_or(now_ms - quiet.as_millis() as i64);
                (*id, since)
            })
        })
        .collect()
}

pub(crate) async fn run(
    api: ApiService,
    heard: Heard,
    closing: tokio::sync::watch::Receiver<bool>,
) {
    let store = api.store.clone();
    crate::controller::while_leading(&store, api.id, closing, "silent managers", || {
        let (api, heard) = (api.clone(), heard.clone());
        async move {
            let leading_since = Instant::now();
            loop {
                tokio::time::sleep(PERIOD).await;
                if let Err(e) = pass(&api, &heard, leading_since).await {
                    tracing::debug!("silent managers: {e}");
                }
            }
        }
    })
    .await
}

async fn pass(api: &ApiService, heard: &Heard, leading_since: Instant) -> Result<(), Status> {
    let (was, version) = read_versioned(&api.store)?;
    let members: Vec<NodeId> = {
        let m = api.store.raft.metrics().borrow().clone();
        m.membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .filter(|id| *id != api.id)
            .collect()
    };
    let heard: BTreeMap<NodeId, Instant> = heard
        .lock()
        .unwrap()
        .iter()
        .map(|(id, (_, at))| (*id, *at))
        .collect();
    let now_ms = crate::console::now_ms();
    let next = silent(
        &members,
        &heard,
        leading_since,
        &was,
        Instant::now(),
        now_ms,
    );
    if next == was {
        return Ok(());
    }
    for id in next.keys().filter(|id| !was.contains_key(id)) {
        tracing::warn!(
            manager = id,
            "a manager has not answered for {MANAGER_SILENCE:?}"
        );
    }
    let spec = DataSpec {
        data: None,
        text: Some(serde_json::to_string(&next).expect("serializes")),
    };
    let object = Object::new(
        Kind::Config,
        SYSTEM_NAMESPACE,
        NAME,
        serde_json::to_value(spec).expect("serializes"),
    );
    let precondition = version.map_or(Precondition::MustNotExist, Precondition::Version);
    match api
        .write(Command::Put {
            object,
            precondition,
        })
        .await?
    {
        Outcome::Written(_) | Outcome::Conflict { .. } => Ok(()),
        other => Err(Status::internal(format!("unexpected outcome {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manager_unheard_for_the_silence_is_silent_since_it_was_last_heard() {
        let now = Instant::now();
        let s = |secs| now - Duration::from_secs(secs);
        let start = s(100);
        let heard = BTreeMap::from([(1, s(1)), (2, s(45)), (3, s(90))]);
        // Leading for 10 s: each is given the silence from then, whenever
        // it was last heard, so none is silent yet.
        let leading = s(10);
        let got = silent(
            &[1, 2, 3, 4],
            &heard,
            leading,
            &BTreeMap::new(),
            now,
            1_000_000,
        );
        assert!(got.is_empty(), "{got:?}");
        // Leading for 100 s: 2 and 3 since they were last heard, 4 (never
        // heard) since this manager led.
        let leading = start;
        let got = silent(
            &[1, 2, 3, 4],
            &heard,
            leading,
            &BTreeMap::new(),
            now,
            1_000_000,
        );
        assert_eq!(
            got,
            BTreeMap::from([(2, 955_000), (3, 910_000), (4, 900_000)])
        );
        // One already recorded keeps when it was first.
        let was = BTreeMap::from([(2, 7)]);
        let got = silent(&[2], &heard, leading, &was, now, 1_000_000);
        assert_eq!(got, BTreeMap::from([(2, 7)]));
        // Heard again: not silent.
        let heard = BTreeMap::from([(2, now)]);
        assert!(silent(&[2], &heard, leading, &was, now, 1_000_000).is_empty());
    }
}
