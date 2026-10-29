//! Backups of the object set, and restoring a manager from one (DESIGN.md
//! §3.7).
//!
//! A backup is every object read at one version, with the managers of the
//! cluster it came from. Restoring writes a new store in which the backup's
//! objects are what the state machine has applied at that version, the log
//! is purged up to it and this manager is the only voter: the daemon then
//! starts it as the leader of a cluster of one, and versions carry on above
//! the backup's. Other managers join it again from empty data directories.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use legion_api::Object;
use openraft::{BasicNode, CommittedLeaderId, LogId, Membership, StoredMembership, Vote};
use serde::{Deserialize, Serialize};

use crate::cipher::{Codec, StoreKey};
use crate::log_store::LogStore;
use crate::state_machine::StateMachine;
use crate::{Error, NodeId};

/// The backup format this build writes and reads.
pub const FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Backup {
    pub format: u32,
    /// The Raft index the objects were read at.
    pub version: u64,
    /// The managers of the cluster backed up, by node id, with their
    /// addresses.
    pub managers: BTreeMap<NodeId, String>,
    pub objects: Vec<Object>,
}

impl Backup {
    /// What the store in `dir` holds, read with its daemon stopped and
    /// its key: a manager restarting a cluster from its own data, when the
    /// others are lost.
    pub fn of_store(dir: &Path, key: Option<&StoreKey>) -> Result<Backup, Error> {
        let path = dir.join("store.redb");
        if !path.exists() {
            return Err(Error::Db(format!("{} does not exist", path.display())));
        }
        let db = match redb::Database::open(&path) {
            Ok(db) => Arc::new(db),
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => return Err(in_use(&path)),
            Err(e) => return Err(Error::db(e)),
        };
        StateMachine::open(db, key)?.backup()
    }

    /// Checks that this build can restore the backup.
    pub fn check(&self) -> Result<(), Error> {
        if self.format != FORMAT {
            return Err(Error::Db(format!(
                "backup format {} (this build reads {FORMAT})",
                self.format
            )));
        }
        if self.version == 0 {
            return Err(Error::Db("the backup holds no cluster".into()));
        }
        Ok(())
    }
}

/// Renames the store in `dir`, if there is one, to `store.redb.<suffix>`
/// so that a restore can write a new one. Fails while a daemon has it open.
pub fn set_aside(dir: &Path, suffix: &str) -> Result<Option<std::path::PathBuf>, Error> {
    let path = dir.join("store.redb");
    if !path.exists() {
        return Ok(None);
    }
    match redb::Database::open(&path) {
        Ok(db) => drop(db),
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => return Err(in_use(&path)),
        // A damaged store is exactly what a restore replaces.
        Err(_) => {}
    }
    let mut aside = dir.join(format!("store.redb.{suffix}"));
    for n in 1.. {
        if !aside.exists() {
            break;
        }
        aside = dir.join(format!("store.redb.{suffix}-{n}"));
    }
    std::fs::rename(&path, &aside).map_err(|e| Error::Db(format!("{}: {e}", aside.display())))?;
    Ok(Some(aside))
}

fn in_use(path: &Path) -> Error {
    Error::Db(format!(
        "{} is in use: stop the daemon first",
        path.display()
    ))
}

/// Writes a new store in `dir` holding `backup`, sealed with `key`, with
/// this manager, `id` at `addr`, the only voter. `dir` must hold no store.
pub fn restore(
    dir: &Path,
    id: NodeId,
    addr: &str,
    backup: &Backup,
    key: Option<&StoreKey>,
) -> Result<(), Error> {
    backup.check()?;
    let path = dir.join("store.redb");
    if path.exists() {
        return Err(Error::Db(format!(
            "{} exists; move it aside first",
            path.display()
        )));
    }
    let db = Arc::new(redb::Database::create(&path).map_err(Error::db)?);
    let codec = Arc::new(Codec::new(key));
    LogStore::open_with(db.clone(), codec.clone())?;
    StateMachine::open_with(db.clone(), codec.clone())?;
    let last = LogId::new(CommittedLeaderId::new(1, id), backup.version);
    let membership = StoredMembership::new(
        Some(last),
        Membership::new(
            vec![BTreeSet::from([id])],
            BTreeMap::from([(
                id,
                BasicNode {
                    addr: addr.to_owned(),
                },
            )]),
        ),
    );
    let txn = db.begin_write().map_err(Error::db)?;
    StateMachine::restore_in(&txn, &codec, last, membership, &backup.objects)?;
    LogStore::restore_in(&txn, &codec, Vote::new_committed(1, id), last)?;
    txn.commit().map_err(Error::db)
}
