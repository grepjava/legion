//! Raft transport between managers over gRPC. openraft's messages travel as
//! JSON inside `RaftPayload`; the response carries the remote `Result`.
//! Each call and answer says what its manager speaks
//! (`legion_api::protocol`), and managers that cannot work together refuse
//! each other: a manager upgraded too far ahead of the others stays out.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex};

use legion_api::protocol::Protocol;
use legion_proto::v1::RaftPayload;
use legion_proto::v1::raft_client::RaftClient;
use legion_proto::v1::raft_server;
use legion_store::{NodeId, Store, TypeConfig};
use openraft::error::{
    InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError, RemoteError,
    Unreachable,
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
use crate::upgrade::{Heard, Speaking};
use tonic::{Request, Response, Status};

/// Largest gRPC message a manager accepts, for log batches and snapshot chunks.
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// Largest log batch a leader sends in one call, once encoded. openraft
/// batches up to `max_payload_entries` whatever their size, and objects of
/// up to a MiB each would pass [`MAX_MESSAGE`] together, which the peer
/// refuses, and no smaller batch would follow. Each call must also finish
/// within the heartbeat interval (openraft's timeout for it). A batch above
/// this is sent again in fewer entries; one entry alone goes whatever its
/// size.
pub(crate) const MAX_BATCH: usize = 4 * 1024 * 1024;

/// The header saying what a manager speaks: `speaks/oldest`.
const PROTOCOL_HEADER: &str = "legion-protocol";

fn say(m: &mut tonic::metadata::MetadataMap, protocol: Protocol) {
    if let Ok(v) = format!("{}/{}", protocol.speaks, protocol.oldest).parse() {
        m.insert(PROTOCOL_HEADER, v);
    }
}

/// What a peer said it speaks; nothing said is protocol 1.
fn heard(m: &tonic::metadata::MetadataMap) -> Protocol {
    let said = m.get(PROTOCOL_HEADER).and_then(|v| v.to_str().ok());
    let (speaks, oldest) = said
        .and_then(|s| s.split_once('/'))
        .map(|(s, o)| (s.parse().unwrap_or(0), o.parse().unwrap_or(0)))
        .unwrap_or((0, 0));
    Protocol::said(speaks, oldest)
}

/// Why peers were refused, each logged once.
pub(crate) type Refusals = Arc<Mutex<HashSet<String>>>;

fn refused(seen: &Refusals, why: &str) {
    if seen.lock().unwrap().insert(why.to_owned()) {
        tracing::error!("{why}");
    }
}

/// Hands openraft one lazily connected channel per peer.
pub struct GrpcNetwork {
    channels: HashMap<(NodeId, String), Channel>,
    tls: Option<Arc<Credentials>>,
    speaking: Speaking,
    heard: Heard,
    refusals: Refusals,
}

impl GrpcNetwork {
    /// Calls peers with mutual TLS when `tls` is given, saying it speaks
    /// what `speaking` says, and noting in `heard` what peers answer.
    pub(crate) fn new(tls: Option<Arc<Credentials>>, speaking: Speaking, heard: Heard) -> Self {
        GrpcNetwork {
            channels: HashMap::new(),
            tls,
            speaking,
            heard,
            refusals: Refusals::default(),
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
            speaking: self.speaking.clone(),
            heard: self.heard.clone(),
            refusals: self.refusals.clone(),
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
    speaking: Speaking,
    heard: Heard,
    refusals: Refusals,
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
        self.send(method, json, option).await
    }

    #[allow(clippy::result_large_err)]
    async fn send<Resp, E>(
        &mut self,
        method: Method,
        json: Vec<u8>,
        option: &RPCOption,
    ) -> Result<Resp, RpcErr<E>>
    where
        Resp: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    {
        let mut request = Request::new(RaftPayload { json });
        request.set_timeout(option.hard_ttl());
        let mine = self.speaking.among_managers();
        say(request.metadata_mut(), mine);
        let response = match method {
            Method::Append => self.client.append_entries(request).await,
            Method::Vote => self.client.vote(request).await,
            Method::Snapshot => self.client.install_snapshot(request).await,
        };
        let peer = format!("manager {}", self.target);
        let response = response.map_err(|s| {
            if s.code() == tonic::Code::Unimplemented {
                refused(
                    &self.refusals,
                    &format!("{peer} refused this one: {}", s.message()),
                );
            }
            status_to_rpc(s)
        })?;
        // A peer from before the handshake answers anyone: refused here,
        // it is unreachable, and counts toward no quorum.
        let theirs = heard(response.metadata());
        self.heard
            .lock()
            .unwrap()
            .insert(self.target, (theirs, tokio::time::Instant::now()));
        if let Err(why) = mine.works_with(theirs, &peer) {
            refused(&self.refusals, &why);
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                why,
            ))));
        }
        let payload = response.into_inner();
        let result: Result<Resp, RaftError<NodeId, E>> = serde_json::from_slice(&payload.json)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RemoteError::new(self.target, e).into())
    }
}

/// How many of a batch of `entries` encoding to `size` bytes to send
/// instead, to stay within `limit`: in proportion, and always fewer, so a
/// batch of entries larger than the average shrinks again on the next try;
/// `None` for a batch that fits or one entry.
fn fewer_entries(entries: usize, size: usize, limit: usize) -> Option<u64> {
    if size <= limit || entries <= 1 {
        return None;
    }
    let fit = (entries as u128 * limit as u128 / size as u128) as usize;
    Some(fit.clamp(1, entries - 1) as u64)
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
        // From the start: encoding a large batch takes its share too.
        let deadline = tokio::time::Instant::now() + option.hard_ttl().mul_f64(0.8);
        let json =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let entries = rpc.entries.len();
        if let Some(hint) = fewer_entries(entries, json.len(), MAX_BATCH) {
            return Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(hint),
            ));
        }
        if entries <= 1 {
            return self.send(Method::Append, json, &option).await;
        }
        // openraft gives each call the heartbeat interval and, past it,
        // sends the same batch again: on a link too slow for it, never
        // anything. A batch that runs out of time goes again in half.
        match tokio::time::timeout_at(deadline, self.send(Method::Append, json, &option)).await {
            Ok(result) => result,
            Err(_) => Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint((entries / 2) as u64),
            )),
        }
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
    /// What this manager speaks: calls from managers it cannot work with
    /// are refused.
    pub(crate) speaking: Speaking,
    pub(crate) refusals: Refusals,
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

    /// Refuses calls from a manager whose build this one cannot work with
    /// (it said `peer`).
    fn compatible(&self, peer: Protocol, vote: &Vote<NodeId>) -> Result<(), Status> {
        let from = vote
            .leader_id()
            .voted_for()
            .map_or("?".into(), |f| f.to_string());
        self.speaking
            .among_managers()
            .works_with(peer, &format!("manager {from}"))
            .map_err(|why| {
                refused(&self.refusals, &why);
                Status::unimplemented(why)
            })
    }

    /// The answer, saying what this manager speaks.
    fn answer<T: Serialize>(&self, result: T) -> Result<Response<RaftPayload>, Status> {
        let mut response = encode(result)?;
        say(response.metadata_mut(), self.speaking.among_managers());
        Ok(response)
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
        let peer = heard(req.metadata());
        let rpc: AppendEntriesRequest<TypeConfig> = decode(req)?;
        self.known(&rpc.vote)?;
        self.compatible(peer, &rpc.vote)?;
        self.answer(self.store.raft.append_entries(rpc).await)
    }

    async fn vote(&self, req: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let peer = heard(req.metadata());
        let rpc: VoteRequest<NodeId> = decode(req)?;
        self.known(&rpc.vote)?;
        self.compatible(peer, &rpc.vote)?;
        self.answer(self.store.raft.vote(rpc).await)
    }

    async fn install_snapshot(
        &self,
        req: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let peer = heard(req.metadata());
        let rpc: InstallSnapshotRequest<TypeConfig> = decode(req)?;
        self.known(&rpc.vote)?;
        self.compatible(peer, &rpc.vote)?;
        self.answer(self.store.raft.install_snapshot(rpc).await)
    }

    async fn read_index(&self, _: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        // Confirmed with a quorum: a deposed leader answers with an error.
        let read: Result<Option<u64>, String> = self
            .store
            .raft
            .get_read_log_id()
            .await
            .map(|(read, _)| read.map(|l| l.index))
            .map_err(|e| e.to_string());
        encode(read)
    }
}

/// The read index of the leader at `addr` ([`raft_server::Raft::read_index`]).
pub(crate) async fn read_index(
    addr: &str,
    tls: Option<&Arc<Credentials>>,
) -> Result<Option<u64>, Status> {
    let mut client = RaftClient::new(connect_lazy(addr, tls));
    let payload = client
        .read_index(RaftPayload::default())
        .await?
        .into_inner();
    let read: Result<Option<u64>, String> = serde_json::from_slice(&payload.json)
        .map_err(|e| Status::internal(format!("the leader's read index: {e}")))?;
    read.map_err(|e| Status::unavailable(format!("the leader's read index: {e}")))
}

#[cfg(test)]
mod tests {
    use super::fewer_entries;

    #[test]
    fn batches_above_the_limit_shrink_until_one_entry() {
        assert_eq!(fewer_entries(300, 100, 100), None);
        assert_eq!(fewer_entries(1, 1000, 100), None);
        // 70 entries of a MiB against 8 MiB: 8 of them.
        assert_eq!(fewer_entries(70, 70 << 20, 8 << 20), Some(8));
        // Just over the limit still drops one.
        assert_eq!(fewer_entries(10, 101, 100), Some(9));
        // Far over: one.
        assert_eq!(fewer_entries(2, 10_000, 100), Some(1));
        // A shrinking batch always reaches one entry.
        let (mut n, per) = (300usize, 3usize << 20);
        while let Some(h) = fewer_entries(n, n * per, 8 << 20) {
            assert!((h as usize) < n);
            n = h as usize;
        }
        assert!(n * per <= 8 << 20 || n == 1);
    }
}
