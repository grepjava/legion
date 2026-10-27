//! The Legion manager (DESIGN.md §3.2): the replicated store, the public API
//! and the Raft transport, served together on one gRPC port, and the
//! controllers, which run while this manager leads.
//!
//! With [`Tls`], every connection is mutual TLS with certificates from the
//! cluster CA: the API takes admins and managers, the Raft transport only
//! managers. Without it (tests only) everything is plaintext.

mod api;
pub mod controller;
mod raft_net;
mod tls;

use std::future::Future;
use std::path::{Path, PathBuf};

use legion_proto::v1::cluster_server::ClusterServer;
use legion_proto::v1::objects_server::ObjectsServer;
use legion_proto::v1::raft_server::RaftServer;
use legion_store::{NodeId, Store};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub use raft_net::{GrpcNetwork, MAX_MESSAGE};
pub use tls::{Tls, client_config};

use legion_pki::Role;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct ManagerConfig {
    pub data_dir: PathBuf,
    /// `host:port` other managers use to reach this one.
    pub advertise: String,
    pub raft: openraft::Config,
    /// `None` serves and calls plaintext: for tests.
    pub tls: Option<Tls>,
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
    tls: Option<Tls>,
}

impl Manager {
    /// Opens the store in `data_dir`, creating the node's identity on first
    /// start. The manager joins no cluster until `Cluster.Init` is called.
    pub async fn open(config: ManagerConfig) -> Result<Manager, Error> {
        std::fs::create_dir_all(&config.data_dir)?;
        let id = node_id(&config.data_dir)?;
        let network = GrpcNetwork::new(config.tls.as_ref().map(Tls::client));
        let store = Store::open(id, &config.data_dir, config.raft, network).await?;
        tracing::info!(node_id = id, advertise = %config.advertise, tls = config.tls.is_some(), "manager open");
        Ok(Manager {
            id,
            advertise: config.advertise,
            store,
            tls: config.tls,
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
        let controllers = tokio::spawn(controller::run(
            self.store.clone(),
            self.id,
            closing.clone(),
        ));
        let api = api::ApiService {
            id: self.id,
            advertise: self.advertise.clone(),
            store: self.store.clone(),
            closing,
            peer_tls: self.tls.as_ref().map(Tls::client),
        };
        let raft = raft_net::RaftService {
            raft: self.store.raft.clone(),
        };
        let mut server = tonic::transport::Server::builder();
        if let Some(tls) = &self.tls {
            server = server.tls_config(tls.server())?;
        }
        let on = self.tls.is_some();
        // Managers forward writes to the leader as themselves.
        const API: &[Role] = &[Role::Admin, Role::Manager];
        const RAFT: &[Role] = &[Role::Manager];
        server
            .add_service(tonic::service::interceptor::InterceptedService::new(
                ObjectsServer::new(api.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
                tls::only(API, on),
            ))
            .add_service(tonic::service::interceptor::InterceptedService::new(
                ClusterServer::new(api),
                tls::only(API, on),
            ))
            .add_service(tonic::service::interceptor::InterceptedService::new(
                RaftServer::new(raft)
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
                tls::only(RAFT, on),
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), {
                let closing_tx = closing_tx.clone();
                async move {
                    shutdown.await;
                    let _ = closing_tx.send(true);
                }
            })
            .await?;
        // The controllers hold a store handle; they stop on `closing`.
        let _ = closing_tx.send(true);
        let _ = controllers.await;
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
