//! Legion's replicated object store (DESIGN.md §3.2).
//!
//! An `openraft` state machine whose log, vote and objects all live in one
//! `redb` file per manager. Every write gets a Raft index, which becomes the
//! object's `metadata.version`. Applied changes feed watch streams that
//! controllers and the API server consume.
//!
//! The Raft transport is left to the caller ([`openraft::RaftNetworkFactory`]):
//! the manager plugs in gRPC, tests plug in an in-process router.

mod log_store;
mod state_machine;
mod watch;

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use legion_api::{Kind, Object, ObjectKey};
use openraft::{BasicNode, RaftNetworkFactory};
use serde::{Deserialize, Serialize};

pub use log_store::LogStore;
pub use state_machine::{Listing, StateMachine};
pub use watch::{Event, WatchError, Watcher};

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Outcome,
        NodeId = NodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type Raft = openraft::Raft<TypeConfig>;

/// A change proposed to the store. Everything that varies between replicas
/// (versions, uids) is derived from the log index while applying, so every
/// replica computes the same result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put {
        object: Object,
        precondition: Precondition,
    },
    Delete {
        key: ObjectKey,
        precondition: Precondition,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Precondition {
    #[default]
    None,
    MustNotExist,
    /// The object must exist at exactly this version (optimistic concurrency).
    Version(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    /// Membership and blank entries.
    Nothing,
    Written(Object),
    Deleted {
        version: u64,
    },
    /// The precondition failed; `current` is the version found, if any.
    Conflict {
        current: Option<u64>,
    },
    NotFound,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database: {0}")]
    Db(String),
    #[error("corrupt record: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("store still open after shutdown: another Store clone is alive")]
    StillOpen,
}

impl Error {
    pub(crate) fn db(e: impl Into<redb::Error>) -> Self {
        Error::Db(e.into().to_string())
    }
}

/// One manager's store: the Raft handle plus local reads and watches.
///
/// Reads come from the local state machine. A caller that needs a
/// linearizable read calls `raft.ensure_linearizable()` first.
#[derive(Clone)]
pub struct Store {
    pub raft: Raft,
    state: StateMachine,
}

impl Store {
    /// Opens (or creates) `dir/store.redb` and starts the Raft node.
    pub async fn open<N>(
        id: NodeId,
        dir: &Path,
        config: openraft::Config,
        network: N,
    ) -> Result<Store, Box<dyn std::error::Error + Send + Sync>>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let db = Arc::new(redb::Database::create(dir.join("store.redb"))?);
        let log = LogStore::open(db.clone())?;
        let state = StateMachine::open(db)?;
        let config = Arc::new(config.validate()?);
        let raft = Raft::new(id, config, network, log, state.clone()).await?;
        Ok(Store { raft, state })
    }

    pub fn get(&self, key: &ObjectKey) -> Result<Option<Object>, Error> {
        self.state.get(key)
    }

    pub fn list(&self, kind: Kind, namespace: Option<&str>) -> Result<Listing, Error> {
        self.state.list(kind, namespace)
    }

    /// Index of the last entry applied locally.
    pub fn applied_version(&self) -> Result<u64, Error> {
        self.state.applied_version()
    }

    /// Streams changes with a version above `from_version`. Fails with
    /// [`WatchError::Compacted`] when those changes are no longer held in
    /// memory; the caller then lists and watches from the list's version.
    pub fn watch(&self, from_version: u64, kind: Option<Kind>) -> Result<Watcher, WatchError> {
        self.state.watch(from_version, kind)
    }

    /// Stops the Raft node and waits until the database file is closed, so
    /// the same directory can be opened again.
    ///
    /// `Raft::shutdown` alone is not enough: openraft's state machine worker,
    /// snapshot builder and replication tasks hold their own handles and exit
    /// a little later. Fails with [`Error::StillOpen`] if another clone of
    /// this `Store` is still alive.
    pub async fn shutdown(self) -> Result<(), Error> {
        let db = self.state.db_handle();
        // A join error means the Raft core panicked; it has stopped either way.
        let _ = self.raft.shutdown().await;
        drop(self);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while db.strong_count() > 0 {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::StillOpen);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }
}
