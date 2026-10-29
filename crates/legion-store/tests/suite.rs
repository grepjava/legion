//! openraft's storage conformance suite against the redb log and state
//! machine, encrypted at rest.

use std::sync::Arc;

use legion_store::{LogStore, NodeId, StateMachine, StoreKey, TypeConfig};
use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachine), StorageError<NodeId>> {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("store.redb")).unwrap());
        let key = StoreKey::generate();
        let log = LogStore::open(db.clone(), Some(&key)).unwrap();
        let sm = StateMachine::open(db, Some(&key)).unwrap();
        Ok((dir, log, sm))
    }
}

#[test]
fn conformance() {
    Suite::test_all(Builder).unwrap();
}
