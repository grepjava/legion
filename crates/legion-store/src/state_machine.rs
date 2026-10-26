//! The persistent state machine: objects keyed by `Kind/namespace/name`.
//!
//! Each `apply` batch is one redb transaction that also records the last
//! applied log id, so the state on disk is always consistent with a log
//! prefix and nothing needs replaying after a restart.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use legion_api::{Kind, Object, ObjectKey};
use openraft::storage::RaftStateMachine;
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use crate::log_store::{any, blocking};
use crate::watch::{Event, Hub, WatchError, Watcher};
use openraft::BasicNode;

use crate::{Command, Error, NodeId, Outcome, Precondition, TypeConfig};

const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const SM_META: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_meta");

const LAST_APPLIED: &str = "last_applied";
const MEMBERSHIP: &str = "membership";
const SNAPSHOT_META: &str = "snapshot_meta";
const SNAPSHOT_DATA: &str = "snapshot_data";

type StResult<T> = Result<T, StorageError<NodeId>>;
type Membership = StoredMembership<NodeId, BasicNode>;

/// Snapshot body. Meta (last log id, membership) travels separately.
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    objects: Vec<Object>,
}

/// Objects read at one version.
#[derive(Debug, Clone, PartialEq)]
pub struct Listing {
    pub version: u64,
    pub objects: Vec<Object>,
}

/// Cheap to clone: the store keeps one clone for reads and watches, openraft
/// owns another.
#[derive(Clone)]
pub struct StateMachine {
    db: Arc<Database>,
    hub: Arc<Hub>,
    snapshot_seq: Arc<AtomicU64>,
}

impl StateMachine {
    pub fn open(db: Arc<Database>) -> Result<Self, Error> {
        let txn = db.begin_write().map_err(Error::db)?;
        txn.open_table(OBJECTS).map_err(Error::db)?;
        txn.open_table(SM_META).map_err(Error::db)?;
        txn.commit().map_err(Error::db)?;
        let mut sm = StateMachine {
            db,
            hub: Arc::new(Hub::new(0)),
            snapshot_seq: Arc::new(AtomicU64::new(0)),
        };
        // Changes from before this process started are not in memory.
        sm.hub = Arc::new(Hub::new(sm.applied_version()?));
        Ok(sm)
    }

    pub fn get(&self, key: &ObjectKey) -> Result<Option<Object>, Error> {
        let txn = self.db.begin_read().map_err(Error::db)?;
        let table = txn.open_table(OBJECTS).map_err(Error::db)?;
        match table.get(key.to_string().as_str()).map_err(Error::db)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    /// All objects of `kind`, optionally in one namespace, and the version
    /// they were read at. Watch from that version to follow changes.
    pub fn list(&self, kind: Kind, namespace: Option<&str>) -> Result<Listing, Error> {
        let txn = self.db.begin_read().map_err(Error::db)?;
        let meta = txn.open_table(SM_META).map_err(Error::db)?;
        let version = read_json::<Option<LogId<NodeId>>>(&meta, LAST_APPLIED)?
            .flatten()
            .map_or(0, |id| id.index);
        let table = txn.open_table(OBJECTS).map_err(Error::db)?;
        let prefix = ObjectKey::prefix(kind, namespace);
        let mut objects = Vec::new();
        for item in table.range(prefix.as_str()..).map_err(Error::db)? {
            let (k, v) = item.map_err(Error::db)?;
            if !k.value().starts_with(&prefix) {
                break;
            }
            objects.push(serde_json::from_slice(v.value())?);
        }
        Ok(Listing { version, objects })
    }

    pub fn applied_version(&self) -> Result<u64, Error> {
        let txn = self.db.begin_read().map_err(Error::db)?;
        let meta = txn.open_table(SM_META).map_err(Error::db)?;
        Ok(read_json::<Option<LogId<NodeId>>>(&meta, LAST_APPLIED)?
            .flatten()
            .map_or(0, |id| id.index))
    }

    pub fn watch(&self, from_version: u64, kind: Option<Kind>) -> Result<Watcher, WatchError> {
        self.hub.watch(from_version, kind)
    }

    pub(crate) fn db_handle(&self) -> std::sync::Weak<Database> {
        Arc::downgrade(&self.db)
    }

    fn read_meta<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<T>, AnyError> {
        let txn = self.db.begin_read().map_err(any)?;
        let meta = txn.open_table(SM_META).map_err(any)?;
        read_json(&meta, key).map_err(any)
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<Option<T>, Error> {
    match table.get(key).map_err(Error::db)? {
        Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
        None => Ok(None),
    }
}

fn write_json<T: Serialize>(txn: &WriteTransaction, key: &str, value: &T) -> Result<(), AnyError> {
    let bytes = serde_json::to_vec(value).map_err(any)?;
    txn.open_table(SM_META)
        .map_err(any)?
        .insert(key, bytes.as_slice())
        .map_err(any)?;
    Ok(())
}

/// Applies one command at `index` inside `txn`. Returns the outcome and the
/// event to publish once the transaction commits.
fn apply_command(
    txn: &WriteTransaction,
    index: u64,
    cmd: Command,
) -> Result<(Outcome, Option<Event>), AnyError> {
    let mut table = txn.open_table(OBJECTS).map_err(any)?;
    let key = match &cmd {
        Command::Put { object, .. } => object.key(),
        Command::Delete { key, .. } => key.clone(),
    };
    let key_str = key.to_string();
    let current: Option<Object> = match table.get(key_str.as_str()).map_err(any)? {
        Some(v) => Some(serde_json::from_slice(v.value()).map_err(any)?),
        None => None,
    };
    let current_version = current.as_ref().map(|o| o.metadata.version);
    let precondition = match &cmd {
        Command::Put { precondition, .. } | Command::Delete { precondition, .. } => *precondition,
    };
    let holds = match precondition {
        Precondition::None => true,
        Precondition::MustNotExist => current.is_none(),
        Precondition::Version(v) => current_version == Some(v),
    };
    if !holds {
        return Ok((
            Outcome::Conflict {
                current: current_version,
            },
            None,
        ));
    }

    match cmd {
        Command::Put { mut object, .. } => {
            let meta = &mut object.metadata;
            meta.version = index;
            match &current {
                Some(cur) => {
                    meta.uid = cur.metadata.uid.clone();
                    meta.generation = if cur.spec == object.spec {
                        cur.metadata.generation
                    } else {
                        cur.metadata.generation + 1
                    };
                }
                None => {
                    // Unique and identical on every replica.
                    meta.uid = format!("{index:016x}");
                    meta.generation = 1;
                }
            }
            let bytes = serde_json::to_vec(&object).map_err(any)?;
            table
                .insert(key_str.as_str(), bytes.as_slice())
                .map_err(any)?;
            Ok((Outcome::Written(object.clone()), Some(Event::Put(object))))
        }
        Command::Delete { .. } => match current {
            Some(mut object) => {
                table.remove(key_str.as_str()).map_err(any)?;
                object.metadata.version = index;
                Ok((
                    Outcome::Deleted { version: index },
                    Some(Event::Delete(object)),
                ))
            }
            None => Ok((Outcome::NotFound, None)),
        },
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = StateMachine;

    async fn applied_state(&mut self) -> StResult<(Option<LogId<NodeId>>, Membership)> {
        let read = || -> Result<_, AnyError> {
            let last = self
                .read_meta::<Option<LogId<NodeId>>>(LAST_APPLIED)?
                .flatten();
            let membership = self
                .read_meta::<Membership>(MEMBERSHIP)?
                .unwrap_or_default();
            Ok((last, membership))
        };
        read().map_err(|e| StorageIOError::read_state_machine(e).into())
    }

    async fn apply<I>(&mut self, entries: I) -> StResult<Vec<Outcome>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let last = entries.last().map(|e| e.log_id);
        let db = self.db.clone();
        let applied = blocking(move || {
            let mut outcomes = Vec::new();
            let mut events = Vec::new();
            let txn = db.begin_write().map_err(any)?;
            for entry in entries {
                match entry.payload {
                    EntryPayload::Blank => outcomes.push(Outcome::Nothing),
                    EntryPayload::Normal(cmd) => {
                        let (outcome, event) = apply_command(&txn, entry.log_id.index, cmd)?;
                        outcomes.push(outcome);
                        events.extend(event);
                    }
                    EntryPayload::Membership(m) => {
                        write_json(&txn, MEMBERSHIP, &Membership::new(Some(entry.log_id), m))?;
                        outcomes.push(Outcome::Nothing);
                    }
                }
            }
            if last.is_some() {
                write_json(&txn, LAST_APPLIED, &last)?;
            }
            txn.commit().map_err(any)?;
            Ok((outcomes, events))
        })
        .await;
        let (outcomes, events) = applied.map_err(|e| match last {
            Some(id) => StorageIOError::apply(id, e),
            None => StorageIOError::write_state_machine(e),
        })?;
        self.hub.publish(events);
        Ok(outcomes)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StResult<()> {
        tracing::info!(snapshot = %meta.snapshot_id, "installing snapshot");
        let bytes = snapshot.into_inner();
        let db = self.db.clone();
        let owned_meta = meta.clone();
        let install = blocking(move || {
            let meta = &owned_meta;
            let data: SnapshotData = serde_json::from_slice(&bytes).map_err(any)?;
            let txn = db.begin_write().map_err(any)?;
            {
                let mut table = txn.open_table(OBJECTS).map_err(any)?;
                table.retain(|_, _| false).map_err(any)?;
                for object in &data.objects {
                    let value = serde_json::to_vec(object).map_err(any)?;
                    table
                        .insert(object.key().to_string().as_str(), value.as_slice())
                        .map_err(any)?;
                }
            }
            write_json(&txn, LAST_APPLIED, &meta.last_log_id)?;
            write_json(&txn, MEMBERSHIP, &meta.last_membership)?;
            write_json(&txn, SNAPSHOT_META, meta)?;
            txn.open_table(SM_META)
                .map_err(any)?
                .insert(SNAPSHOT_DATA, bytes.as_slice())
                .map_err(any)?;
            txn.commit().map_err(any)
        });
        install
            .await
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), e))?;
        self.hub
            .publish([Event::Reset(meta.last_log_id.map_or(0, |id| id.index))]);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StResult<Option<Snapshot<TypeConfig>>> {
        let read = || -> Result<Option<Snapshot<TypeConfig>>, AnyError> {
            let txn = self.db.begin_read().map_err(any)?;
            let table = txn.open_table(SM_META).map_err(any)?;
            let Some(meta) =
                read_json::<SnapshotMeta<NodeId, BasicNode>>(&table, SNAPSHOT_META).map_err(any)?
            else {
                return Ok(None);
            };
            let data = table
                .get(SNAPSHOT_DATA)
                .map_err(any)?
                .map(|v| v.value().to_vec())
                .unwrap_or_default();
            Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }))
        };
        read().map_err(|e| StorageIOError::read_snapshot(None, e).into())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> StResult<Snapshot<TypeConfig>> {
        let db = self.db.clone();
        let seq = self.snapshot_seq.fetch_add(1, Ordering::Relaxed);
        let build = blocking(move || {
            // One read transaction: objects and meta from the same version.
            let txn = db.begin_read().map_err(any)?;
            let meta_table = txn.open_table(SM_META).map_err(any)?;
            let last: Option<LogId<NodeId>> =
                read_json::<Option<LogId<NodeId>>>(&meta_table, LAST_APPLIED)
                    .map_err(any)?
                    .flatten();
            let membership: Membership = read_json(&meta_table, MEMBERSHIP)
                .map_err(any)?
                .unwrap_or_default();
            let objects_table = txn.open_table(OBJECTS).map_err(any)?;
            let mut objects = Vec::new();
            for item in objects_table.iter().map_err(any)? {
                let (_, v) = item.map_err(any)?;
                objects.push(serde_json::from_slice(v.value()).map_err(any)?);
            }
            drop(txn);

            let data = serde_json::to_vec(&SnapshotData { objects }).map_err(any)?;
            let snapshot_id = match last {
                Some(id) => format!("{}-{}-{seq}", id.leader_id, id.index),
                None => format!("none-{seq}"),
            };
            let meta = SnapshotMeta {
                last_log_id: last,
                last_membership: membership,
                snapshot_id,
            };

            let txn = db.begin_write().map_err(any)?;
            // An installed snapshot may have overtaken this build.
            let stored: Option<SnapshotMeta<NodeId, BasicNode>> =
                read_json(&txn.open_table(SM_META).map_err(any)?, SNAPSHOT_META).map_err(any)?;
            if stored.is_none_or(|s| s.last_log_id <= meta.last_log_id) {
                write_json(&txn, SNAPSHOT_META, &meta)?;
                txn.open_table(SM_META)
                    .map_err(any)?
                    .insert(SNAPSHOT_DATA, data.as_slice())
                    .map_err(any)?;
            }
            txn.commit().map_err(any)?;
            tracing::info!(snapshot = %meta.snapshot_id, bytes = data.len(), "built snapshot");
            Ok(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            })
        });
        build
            .await
            .map_err(|e| StorageIOError::write_snapshot(None, e).into())
    }
}
