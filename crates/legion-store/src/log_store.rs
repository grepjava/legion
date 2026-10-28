//! Raft log and vote, persisted in redb.
//!
//! Every write commits with redb's default immediate durability (fsync), so
//! `append` reports the entries flushed before it returns. Writes run on the
//! blocking pool; reads do not fsync and run inline.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError, StorageIOError,
    Vote,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{NodeId, TypeConfig};

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE: &str = "vote";
const LAST_PURGED: &str = "last_purged";

type StResult<T> = Result<T, StorageError<NodeId>>;

/// Cheap to clone; clones share the database and serve as log readers.
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    pub fn open(db: Arc<Database>) -> Result<Self, crate::Error> {
        let txn = db.begin_write().map_err(crate::Error::db)?;
        txn.open_table(LOG).map_err(crate::Error::db)?;
        txn.open_table(META).map_err(crate::Error::db)?;
        txn.commit().map_err(crate::Error::db)?;
        Ok(LogStore { db })
    }

    /// Writes a restored log into a new store: `vote`, and nothing but what
    /// was purged up to `purged`.
    pub(crate) fn restore_in(
        txn: &redb::WriteTransaction,
        vote: Vote<NodeId>,
        purged: LogId<NodeId>,
    ) -> Result<(), crate::Error> {
        let mut table = txn.open_table(META).map_err(crate::Error::db)?;
        for (key, value) in [
            (VOTE, serde_json::to_vec(&vote)?),
            (LAST_PURGED, serde_json::to_vec(&purged)?),
        ] {
            table
                .insert(key, value.as_slice())
                .map_err(crate::Error::db)?;
        }
        Ok(())
    }

    fn get_meta<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, AnyError> {
        let txn = self.db.begin_read().map_err(any)?;
        let table = txn.open_table(META).map_err(any)?;
        match table.get(key).map_err(any)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value()).map_err(any)?)),
            None => Ok(None),
        }
    }

    async fn put_meta<T: Serialize>(&self, key: &'static str, value: &T) -> Result<(), AnyError> {
        let bytes = serde_json::to_vec(value).map_err(any)?;
        let db = self.db.clone();
        blocking(move || {
            let txn = db.begin_write().map_err(any)?;
            txn.open_table(META)
                .map_err(any)?
                .insert(key, bytes.as_slice())
                .map_err(any)?;
            txn.commit().map_err(any)
        })
        .await
    }

    fn last_entry_id(&self) -> Result<Option<LogId<NodeId>>, AnyError> {
        let txn = self.db.begin_read().map_err(any)?;
        let table = txn.open_table(LOG).map_err(any)?;
        match table.last().map_err(any)? {
            Some((_, v)) => {
                let entry: Entry<TypeConfig> = serde_json::from_slice(v.value()).map_err(any)?;
                Ok(Some(entry.log_id))
            }
            None => Ok(None),
        }
    }
}

pub(crate) fn any(e: impl std::error::Error + 'static) -> AnyError {
    AnyError::new(&e)
}

/// Runs redb work that commits (and so fsyncs) on the blocking pool, keeping
/// it off the async worker threads the API server shares.
pub(crate) async fn blocking<T, F>(f: F) -> Result<T, AnyError>
where
    F: FnOnce() -> Result<T, AnyError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        // The runtime is shutting down.
        Err(e) => Err(any(e)),
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StResult<Vec<Entry<TypeConfig>>> {
        let read = || -> Result<Vec<Entry<TypeConfig>>, AnyError> {
            let txn = self.db.begin_read().map_err(any)?;
            let table = txn.open_table(LOG).map_err(any)?;
            let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
            let mut out = Vec::new();
            for item in table.range::<u64>(bounds).map_err(any)? {
                let (_, v) = item.map_err(any)?;
                out.push(serde_json::from_slice(v.value()).map_err(any)?);
            }
            Ok(out)
        };
        read().map_err(|e| StorageIOError::read_logs(e).into())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> StResult<LogState<TypeConfig>> {
        let last_purged: Option<LogId<NodeId>> = self
            .get_meta(LAST_PURGED)
            .map_err(StorageIOError::read_logs)?;
        let last = self.last_entry_id().map_err(StorageIOError::read_logs)?;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StResult<()> {
        self.put_meta(VOTE, vote)
            .await
            .map_err(|e| StorageIOError::write_vote(e).into())
    }

    async fn read_vote(&mut self) -> StResult<Option<Vote<NodeId>>> {
        self.get_meta(VOTE)
            .map_err(|e| StorageIOError::read_vote(e).into())
    }

    // `save_committed` / `read_committed` keep openraft's no-op defaults: the
    // state machine persists its last applied id in the same transaction as
    // the objects, so the committed id need not survive a restart, and
    // saving it would cost one more fsync per write on every node.

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let db = self.db.clone();
        blocking(move || {
            let txn = db.begin_write().map_err(any)?;
            {
                let mut table = txn.open_table(LOG).map_err(any)?;
                for entry in entries {
                    let bytes = serde_json::to_vec(&entry).map_err(any)?;
                    table
                        .insert(entry.log_id.index, bytes.as_slice())
                        .map_err(any)?;
                }
            }
            txn.commit().map_err(any)
        })
        .await
        .map_err(StorageIOError::write_logs)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StResult<()> {
        tracing::debug!(%log_id, "truncate log");
        let db = self.db.clone();
        blocking(move || {
            let txn = db.begin_write().map_err(any)?;
            txn.open_table(LOG)
                .map_err(any)?
                .retain_in(log_id.index.., |_, _| false)
                .map_err(any)?;
            txn.commit().map_err(any)
        })
        .await
        .map_err(|e| StorageIOError::write_logs(e).into())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StResult<()> {
        tracing::debug!(%log_id, "purge log");
        let db = self.db.clone();
        blocking(move || {
            let bytes = serde_json::to_vec(&log_id).map_err(any)?;
            let txn = db.begin_write().map_err(any)?;
            txn.open_table(META)
                .map_err(any)?
                .insert(LAST_PURGED, bytes.as_slice())
                .map_err(any)?;
            txn.open_table(LOG)
                .map_err(any)?
                .retain_in(..=log_id.index, |_, _| false)
                .map_err(any)?;
            txn.commit().map_err(any)
        })
        .await
        .map_err(|e| StorageIOError::write_logs(e).into())
    }
}
