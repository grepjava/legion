//! The Legion manager (DESIGN.md §3.2): the replicated store, the public API
//! and the Raft transport, served together on one gRPC port, and the
//! controllers, which run while this manager leads, and the dispatcher,
//! which agents follow their work through.
//!
//! With [`Tls`], every connection is mutual TLS with certificates from the
//! cluster CA: the API takes admins and managers, the dispatcher workers and
//! managers, the Raft transport only managers. Without it (tests only) everything is plaintext.

mod api;
mod authz;
pub mod controller;
mod dispatcher;
pub mod join;
pub mod metrics;
mod migrate;
mod quota;
mod raft_net;
mod security;
mod session;
mod tls;

use std::future::Future;
use std::path::{Path, PathBuf};

use legion_proto::v1::cluster_server::ClusterServer;
use legion_proto::v1::objects_server::ObjectsServer;
use legion_proto::v1::raft_server::RaftServer;
use legion_store::{NodeId, Store};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub use dispatcher::HEARTBEAT_PERIOD;
pub use join::{Joined, SYSTEM_NAMESPACE, join};
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
    /// How often agents send heartbeats; a node silent for three periods is
    /// down. [`HEARTBEAT_PERIOD`] unless testing.
    pub heartbeat: std::time::Duration,
    /// Whether to run the controllers while leading; off only in tests that
    /// write tasks themselves.
    pub controllers: bool,
}

impl ManagerConfig {
    /// Defaults for everything but where the manager keeps its data and is
    /// reached; plaintext until `tls` is set.
    pub fn new(data_dir: impl Into<PathBuf>, advertise: impl Into<String>) -> ManagerConfig {
        ManagerConfig {
            data_dir: data_dir.into(),
            advertise: advertise.into(),
            raft: default_raft_config(),
            tls: None,
            heartbeat: HEARTBEAT_PERIOD,
            controllers: true,
        }
    }
}

/// How long a write waits to be committed. Without a quorum it never is,
/// and callers get an error rather than hang (the write may still apply
/// if a quorum comes back).
pub(crate) const COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The error for a write or read that timed out waiting for a quorum.
pub(crate) fn no_quorum() -> tonic::Status {
    tonic::Status::unavailable(format!(
        "no quorum of managers answered within {}s; it may still apply",
        COMMIT_TIMEOUT.as_secs()
    ))
}

/// Raft timing for managers on a LAN or across sites. openraft also uses
/// the heartbeat interval as the timeout of each replication call, so it
/// must exceed the slowest round trip between managers (with TLS and a
/// disk write); at 250 ms a 225 ms link never kept a leader. Elections
/// then come after 5 to 10 s of silence, close to Swarm's default.
/// Snapshot chunks stay well under the gRPC message limit after JSON
/// encoding.
pub fn default_raft_config() -> openraft::Config {
    openraft::Config {
        cluster_name: "legion".into(),
        heartbeat_interval: 1000,
        election_timeout_min: 5000,
        election_timeout_max: 10_000,
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
    heartbeat: std::time::Duration,
    controllers: bool,
    /// The store as metrics read it, emptied before the store shuts down.
    metrics: metrics::Slot,
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
            tls: config.tls,
            heartbeat: config.heartbeat,
            controllers: config.controllers,
            metrics: std::sync::Arc::new(std::sync::RwLock::new(Some(store.clone()))),
            store,
        })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Reads this manager's metrics until its store shuts down.
    pub fn metrics(&self) -> metrics::ManagerMetrics {
        metrics::ManagerMetrics {
            id: self.id,
            store: self.metrics.clone(),
        }
    }

    /// Serves the API and Raft transport on `listener` until `shutdown`
    /// resolves, then stops the store.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        let (closing_tx, closing) = tokio::sync::watch::channel(false);
        let controllers = self.controllers.then(|| {
            tokio::spawn(controller::run(
                self.store.clone(),
                self.id,
                closing.clone(),
            ))
        });
        let issuer = match self
            .tls
            .as_ref()
            .and_then(|t| t.ca_key_pem.as_ref().map(|k| (t, k)))
        {
            Some((t, key)) => Some(join::Issuer {
                ca: std::sync::Arc::new(legion_pki::Ca::load(&t.ca_pem, key)?),
                store: self.store.clone(),
            }),
            None => None,
        };
        let promote = tokio::spawn(join::promote_learners(
            self.store.raft.clone(),
            closing.clone(),
        ));
        let sessions: std::sync::Arc<session::Hub> = Default::default();
        let migrator = self.controllers.then(|| {
            tokio::spawn(migrate::run(
                self.store.clone(),
                sessions.clone(),
                self.id,
                closing.clone(),
            ))
        });
        let api = api::ApiService {
            id: self.id,
            advertise: self.advertise.clone(),
            store: self.store.clone(),
            closing,
            peer_tls: self.tls.as_ref().map(Tls::client),
            issuer,
            quota_lock: Default::default(),
            sessions,
        };
        let (dispatcher, dispatching) =
            dispatcher::DispatcherService::start(api.clone(), self.tls.is_some(), self.heartbeat);
        let raft = raft_net::RaftService {
            store: self.store.clone(),
        };
        let mut server = tonic::transport::Server::builder();
        if let Some(tls) = &self.tls {
            server = server.tls_config(tls.server())?;
        }
        let on = self.tls.is_some();
        // Managers forward writes to the leader as themselves. Users get
        // what their RoleBindings give (authz.rs).
        const API: &[Role] = &[Role::Admin, Role::Manager, Role::User];
        const RAFT: &[Role] = &[Role::Manager];
        const DISPATCHER: &[Role] = &[Role::Worker, Role::Manager];
        server
            .add_service(tonic::service::interceptor::InterceptedService::new(
                ObjectsServer::new(api.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
                tls::only(API, on),
            ))
            .add_service(tonic::service::interceptor::InterceptedService::new(
                ClusterServer::new(api.clone()),
                tls::only(API, on),
            ))
            .add_service(tonic::service::interceptor::InterceptedService::new(
                legion_proto::v1::dispatcher_server::DispatcherServer::new(dispatcher)
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
                tls::only(DISPATCHER, on),
            ))
            .add_service(tonic::service::interceptor::InterceptedService::new(
                legion_proto::v1::pods_server::PodsServer::new(session::PodsService {
                    api: api.clone(),
                }),
                tls::only(API, on),
            ))
            .add_service(legion_proto::v1::join_server::JoinServer::new(
                join::JoinService { api },
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
        if let Some(controllers) = controllers {
            let _ = controllers.await;
        }
        if let Some(migrator) = migrator {
            let _ = migrator.await;
        }
        let _ = promote.await;
        let _ = dispatching.await;
        tracing::info!("API stopped; shutting down the store");
        *self.metrics.write().unwrap_or_else(|e| e.into_inner()) = None;
        self.store.shutdown().await?;
        Ok(())
    }
}

/// The node's Raft id, generated once and kept in `data_dir/node-id`.
pub fn node_id(dir: &Path) -> Result<NodeId, Error> {
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
