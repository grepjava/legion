//! openraft's storage conformance suite against the redb log and state machine.

use std::sync::Arc;

use legion_store::{LogStore, NodeId, StateMachine, TypeConfig};
use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, TempDir> for Builder {
    async fn build(&self) -> Result<(TempDir, LogStore, StateMachine), StorageError<NodeId>> {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("store.redb")).unwrap());
        let log = LogStore::open(db.clone()).unwrap();
        let sm = StateMachine::open(db).unwrap();
        Ok((dir, log, sm))
    }
}

#[test]
fn conformance() {
    Suite::test_all(Builder).unwrap();
}
