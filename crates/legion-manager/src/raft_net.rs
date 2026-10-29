//! Raft transport between managers over gRPC. openraft's messages travel as
//! JSON inside `RaftPayload`; the response carries the remote `Result`.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use legion_proto::v1::RaftPayload;
use legion_proto::v1::raft_client::RaftClient;
use legion_proto::v1::raft_server;
use legion_store::{NodeId, Store, TypeConfig};
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Vote};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::transport::{Channel, Endpoint};

use crate::Credentials;
use tonic::{Request, Response, Status};

/// Largest gRPC message a manager accepts, for log batches and snapshot chunks.
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// Hands openraft one lazily connected channel per peer.
#[derive(Default)]
pub struct GrpcNetwork {
    channels: HashMap<(NodeId, String), Channel>,
    tls: Option<Arc<Credentials>>,
}

impl GrpcNetwork {
    /// Calls peers with mutual TLS when `tls` is given.
    pub fn new(tls: Option<Arc<Credentials>>) -> Self {
        GrpcNetwork {
            channels: HashMap::new(),
            tls,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> PeerClient {
        let channel = self
            .channels
            .entry((target, node.addr.clone()))
            .or_insert_with(|| connect_lazy(&node.addr, self.tls.as_ref()))
            .clone();
        PeerClient {
            target,
            client: RaftClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        }
    }
}

/// `addr` is `host:port`. A malformed address yields a channel that never
/// connects, which openraft sees as an unreachable peer.
pub(crate) fn connect_lazy(addr: &str, tls: Option<&Arc<Credentials>>) -> Channel {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap_or_else(|_| Endpoint::from_static("http://invalid.invalid:1"));
    match tls {
        Some(tls) => tls.channel(endpoint),
        None => endpoint.connect_lazy(),
    }
}

pub struct PeerClient {
    target: NodeId,
    client: RaftClient<Channel>,
}

type RpcErr<E> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

impl PeerClient {
    // The error type is fixed by openraft's `RaftNetwork` signatures.
    #[allow(clippy::result_large_err)]
    async fn call<Req, Resp, E>(
        &mut self,
        method: Method,
        req: &Req,
        option: &RPCOption,
    ) -> Result<Resp, RpcErr<E>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let json = serde_json::to_vec(req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut request = Request::new(RaftPayload { json });
        request.set_timeout(option.hard_ttl());
        let response = match method {
            Method::Append => self.client.append_entries(request).await,
            Method::Vote => self.client.vote(request).await,
            Method::Snapshot => self.client.install_snapshot(request).await,
        };
        let payload = response.map_err(status_to_rpc)?.into_inner();
        let result: Result<Resp, RaftError<NodeId, E>> = serde_json::from_slice(&payload.json)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RemoteError::new(self.target, e).into())
    }
}

enum Method {
    Append,
    Vote,
    Snapshot,
}

fn status_to_rpc<E: std::error::Error>(status: Status) -> RpcErr<E> {
    let err = io::Error::other(status.to_string());
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => {
            RPCError::Unreachable(Unreachable::new(&err))
        }
        _ => RPCError::Network(NetworkError::new(&err)),
    }
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcErr<openraft::error::Infallible>> {
        self.call(Method::Append, &rpc, &option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcErr<InstallSnapshotError>> {
        self.call(Method::Snapshot, &rpc, &option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcErr<openraft::error::Infallible>> {
        self.call(Method::Vote, &rpc, &option).await
    }
}

/// Serves Raft RPCs from peers into the local node.
pub struct RaftService {
    pub store: Store,
}

fn decode<T: DeserializeOwned>(req: Request<RaftPayload>) -> Result<T, Status> {
    serde_json::from_slice(&req.into_inner().json)
        .map_err(|e| Status::invalid_argument(format!("bad raft payload: {e}")))
}

impl RaftService {
    /// Refuses calls from managers this one does not know: those of the
    /// cluster a restore left (DESIGN.md §3.7), which would otherwise vote
    /// it back into the old cluster.
    fn known(&self, vote: &Vote<NodeId>) -> Result<(), Status> {
        let from = vote.leader_id().voted_for();
        if self.store.knows(from) {
            return Ok(());
        }
        Err(Status::permission_denied(format!(
            "manager {} is not a member of this cluster",
            from.map_or("?".into(), |f| f.to_string())
        )))
    }
}

fn encode<T: Serialize>(result: T) -> Result<Response<RaftPayload>, Status> {
    let json = serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(RaftPayload { json }))
}

#[tonic::async_trait]
impl raft_server::Raft for RaftService {
    async fn append_entries(
        &self,
        req: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let rpc: AppendEntriesRequest<TypeConfig> = decode(req)?;
        self.known(&rpc.vote)?;
        encode(self.store.raft.append_entries(rpc).await)
    }

    async fn vote(&self, req: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let rpc: VoteRequest<NodeId> = decode(req)?;
        self.known(&rpc.vote)?;
        encode(self.store.raft.vote(rpc).await)
    }

    async fn install_snapshot(
        &self,
        req: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let rpc: InstallSnapshotRequest<TypeConfig> = decode(req)?;
        self.known(&rpc.vote)?;
        encode(self.store.raft.install_snapshot(rpc).await)
    }
}
