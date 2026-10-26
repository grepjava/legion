//! The Legion manager (DESIGN.md §3.2): the replicated store, the public API
//! and the Raft transport, served together on one gRPC port.
//!
//! Phase 1 serves plaintext gRPC; mTLS arrives with the cluster PKI in phase 2.

mod api;
mod raft_net;

use std::future::Future;
use std::path::{Path, PathBuf};

use legion_proto::v1::cluster_server::ClusterServer;
use legion_proto::v1::objects_server::ObjectsServer;
use legion_proto::v1::raft_server::RaftServer;
use legion_store::{NodeId, Store};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub use raft_net::{GrpcNetwork, MAX_MESSAGE};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct ManagerConfig {
    pub data_dir: PathBuf,
    /// `host:port` other managers use to reach this one.
    pub advertise: String,
    pub raft: openraft::Config,
}

/// Raft timing for managers on a LAN. Snapshot chunks stay well under the
/// gRPC message limit after JSON encoding.
pub fn default_raft_config() -> openraft::Config {
    openraft::Config {
        cluster_name: "legion".into(),
        heartbeat_interval: 250,
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(10_000),
        snapshot_max_chunk_size: 1024 * 1024,
        ..Default::default()
    }
}

pub struct Manager {
    id: NodeId,
    advertise: String,
    store: Store,
}

impl Manager {
    /// Opens the store in `data_dir`, creating the node's identity on first
    /// start. The manager joins no cluster until `Cluster.Init` is called.
    pub async fn open(config: ManagerConfig) -> Result<Manager, Error> {
        std::fs::create_dir_all(&config.data_dir)?;
        let id = node_id(&config.data_dir)?;
        let store = Store::open(id, &config.data_dir, config.raft, GrpcNetwork::default()).await?;
        tracing::info!(node_id = id, advertise = %config.advertise, "manager open");
        Ok(Manager {
            id,
            advertise: config.advertise,
            store,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Serves the API and Raft transport on `listener` until `shutdown`
    /// resolves, then stops the store.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        let (closing_tx, closing) = tokio::sync::watch::channel(false);
        let api = api::ApiService {
            id: self.id,
            advertise: self.advertise.clone(),
            store: self.store.clone(),
            closing,
        };
        let raft = raft_net::RaftService {
            raft: self.store.raft.clone(),
        };
        tonic::transport::Server::builder()
            .add_service(
                ObjectsServer::new(api.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            )
            .add_service(ClusterServer::new(api))
            .add_service(
                RaftServer::new(raft)
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                shutdown.await;
                let _ = closing_tx.send(true);
            })
            .await?;
        tracing::info!("API stopped; shutting down the store");
        self.store.shutdown().await?;
        Ok(())
    }
}

/// The node's Raft id, generated once and kept in `data_dir/node-id`.
fn node_id(dir: &Path) -> Result<NodeId, Error> {
    let path = dir.join("node-id");
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(s.trim().parse()?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            use std::hash::{BuildHasher, Hasher};
            // RandomState is seeded randomly per process. Zero is avoided so
            // an id is never mistaken for an unset value.
            let id = std::hash::RandomState::new().build_hasher().finish().max(1);
            std::fs::write(&path, format!("{id}\n"))?;
            Ok(id)
        }
        Err(e) => Err(e.into()),
    }
}
