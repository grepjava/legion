//! In-process Raft transport and cluster harness for the store tests.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, NodeId, Outcome, Precondition, Raft, Store, StoreKey, TypeConfig};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, ServerState};
use tempfile::TempDir;

pub const TIMEOUT: Duration = Duration::from_secs(10);

/// Routes RPCs between Raft handles in this process. A node can be cut off
/// from everyone (`isolate`) to simulate a partition.
#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<Mutex<RouterState>>,
}

#[derive(Default)]
struct RouterState {
    nodes: BTreeMap<NodeId, Raft>,
    isolated: BTreeSet<NodeId>,
}

impl Router {
    fn target(&self, from: NodeId, to: NodeId) -> Result<Raft, Unreachable> {
        let st = self.inner.lock().unwrap();
        let unreachable = || Unreachable::new(&io::Error::from(io::ErrorKind::ConnectionRefused));
        if st.isolated.contains(&from) || st.isolated.contains(&to) {
            return Err(unreachable());
        }
        st.nodes.get(&to).cloned().ok_or_else(unreachable)
    }

    pub fn isolate(&self, id: NodeId) {
        self.inner.lock().unwrap().isolated.insert(id);
    }

    pub fn heal(&self, id: NodeId) {
        self.inner.lock().unwrap().isolated.remove(&id);
    }
}

/// The network factory handed to one node.
pub struct NodeNet {
    id: NodeId,
    router: Router,
}

impl RaftNetworkFactory<TypeConfig> for NodeNet {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Conn {
        Conn {
            from: self.id,
            to: target,
            router: self.router.clone(),
        }
    }
}

pub struct Conn {
    from: NodeId,
    to: NodeId,
    router: Router,
}

type RpcResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>;

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<NodeId>> {
        let raft = self.router.target(self.from, self.to)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RemoteError::new(self.to, e).into())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<NodeId>, InstallSnapshotError> {
        let raft = self.router.target(self.from, self.to)?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RemoteError::new(self.to, e).into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> RpcResult<VoteResponse<NodeId>> {
        let raft = self.router.target(self.from, self.to)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RemoteError::new(self.to, e).into())
    }
}

pub fn config() -> openraft::Config {
    openraft::Config {
        cluster_name: "legion-test".into(),
        heartbeat_interval: 50,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    }
}

/// A set of store nodes, each with its own data directory that outlives
/// restarts of the node.
pub struct Cluster {
    pub router: Router,
    pub config: openraft::Config,
    dirs: BTreeMap<NodeId, TempDir>,
    pub nodes: BTreeMap<NodeId, Store>,
    /// The key nodes seal their stores with, if any.
    pub key: Option<StoreKey>,
}

impl Cluster {
    pub fn new(config: openraft::Config) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn".into()),
            )
            .with_test_writer()
            .try_init();
        Cluster {
            router: Router::default(),
            config,
            dirs: BTreeMap::new(),
            nodes: BTreeMap::new(),
            key: None,
        }
    }

    pub fn dir(&self, id: NodeId) -> PathBuf {
        self.dirs[&id].path().to_owned()
    }

    /// Starts node `id`, reusing its data directory if it ran before.
    pub async fn start(&mut self, id: NodeId) -> Store {
        let dir = self
            .dirs
            .entry(id)
            .or_insert_with(|| tempfile::tempdir().unwrap())
            .path()
            .to_owned();
        let net = NodeNet {
            id,
            router: self.router.clone(),
        };
        let store = Store::open_with(id, &dir, self.key.as_ref(), self.config.clone(), net)
            .await
            .unwrap();
        self.router
            .inner
            .lock()
            .unwrap()
            .nodes
            .insert(id, store.raft.clone());
        self.nodes.insert(id, store.clone());
        store
    }

    /// Stops node `id` the way a crash would look to its peers. Its data
    /// directory is kept.
    pub async fn stop(&mut self, id: NodeId) {
        self.router.inner.lock().unwrap().nodes.remove(&id);
        let store = self.nodes.remove(&id).unwrap();
        store.shutdown().await.unwrap();
    }

    pub fn store(&self, id: NodeId) -> &Store {
        &self.nodes[&id]
    }

    pub async fn initialize(&self, ids: &[NodeId]) {
        let members: BTreeMap<NodeId, BasicNode> =
            ids.iter().map(|&id| (id, BasicNode::default())).collect();
        self.store(ids[0]).raft.initialize(members).await.unwrap();
    }

    /// Waits until every running node agrees on a leader other than `not`.
    pub async fn leader(&self, not: Option<NodeId>) -> NodeId {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let leaders: BTreeSet<Option<NodeId>> = self
                .nodes
                .values()
                .map(|s| s.raft.metrics().borrow().current_leader)
                .collect();
            if leaders.len() == 1
                && let Some(Some(id)) = leaders.first().copied()
                && Some(id) != not
                && self.nodes.contains_key(&id)
                && self.store(id).raft.metrics().borrow().state == ServerState::Leader
            {
                return id;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no agreed leader: {leaders:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until every running node has applied at least `index`.
    pub async fn applied_everywhere(&self, index: u64) {
        for (id, store) in &self.nodes {
            store
                .raft
                .wait(Some(TIMEOUT))
                .applied_index_at_least(Some(index), format!("node {id} applies {index}"))
                .await
                .unwrap();
        }
    }

    /// Asserts that every running node holds exactly the same objects.
    pub fn assert_converged(&self, kind: Kind) {
        let mut listings = self
            .nodes
            .iter()
            .map(|(id, s)| (id, s.list(kind, None).unwrap().objects));
        let (first_id, first) = listings.next().unwrap();
        for (id, objects) in listings {
            assert_eq!(objects, first, "node {id} differs from node {first_id}");
        }
    }
}

pub fn service(name: &str, replicas: u64) -> Object {
    Object::new(
        Kind::Service,
        "default",
        name,
        serde_json::json!({ "replicas": replicas }),
    )
}

pub async fn put(store: &Store, object: Object, precondition: Precondition) -> Outcome {
    store
        .raft
        .client_write(Command::Put {
            object,
            precondition,
        })
        .await
        .unwrap()
        .data
}

pub async fn delete(store: &Store, key: ObjectKey, precondition: Precondition) -> Outcome {
    store
        .raft
        .client_write(Command::Delete { key, precondition })
        .await
        .unwrap()
        .data
}

/// Writes `count` services named `{prefix}-{n}` through `store`; returns the
/// version of the last write.
pub async fn put_many(store: &Store, prefix: &str, count: usize) -> u64 {
    let mut last = 0;
    for n in 0..count {
        match put(
            store,
            service(&format!("{prefix}-{n}"), 1),
            Precondition::None,
        )
        .await
        {
            Outcome::Written(o) => last = o.metadata.version,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    last
}
