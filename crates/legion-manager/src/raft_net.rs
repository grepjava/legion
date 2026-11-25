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
/// refuses, and no smaller batch would follow. One entry alone goes
/// whatever its size.
pub(crate) const MAX_BATCH: usize = 4 * 1024 * 1024;

/// The least a peer's [`Budgets`] falls to.
const MIN_BUDGET: usize = 16 * 1024;

/// How many bytes of log a call to each peer may carry, learned from how
/// long calls take. Each call must finish within the heartbeat interval
/// (openraft's timeout for it), and a peer far away (a witness across the
/// internet, 200 ms off) can't take [`MAX_BATCH`] in that time, least of
/// all a peer catching up after a restart. A leader sends what fits and
/// says how far it got ([`AppendEntriesResponse::PartialSuccess`]), so
/// openraft goes on from there at once; a call that ran out of time halves
/// the budget, and one that took under a quarter of it while near the
/// budget doubles it. Shared by the peer's clients, as openraft makes a new
/// one for each replication stream.
pub(crate) type Budgets = Arc<Mutex<HashMap<NodeId, usize>>>;

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
    budgets: Budgets,
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
            budgets: Budgets::default(),
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
            budgets: self.budgets.clone(),
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
    budgets: Budgets,
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

/// How many of the leading entries of `sizes` (each one's encoded size)
/// fit in `budget` together: always at least one.
fn fitting(sizes: impl IntoIterator<Item = usize>, budget: usize) -> usize {
    let (mut n, mut total) = (0, 0usize);
    for size in sizes {
        total = total.saturating_add(size);
        if n > 0 && total > budget {
            break;
        }
        n += 1;
    }
    n
}

/// A peer's budget after a call of `size` bytes that took `took` of the
/// `allowed` time, or ran out of it (`None`): see [`Budgets`].
fn next_budget(
    budget: usize,
    size: usize,
    took: Option<std::time::Duration>,
    allowed: std::time::Duration,
) -> usize {
    match took {
        None => (size / 2).min(budget).max(MIN_BUDGET),
        Some(t) if t < allowed / 4 && size >= budget / 2 => budget.saturating_mul(2).min(MAX_BATCH),
        Some(_) => budget,
    }
}

/// The length of `value` encoded, without keeping it.
fn encoded_len<T: Serialize>(value: &T) -> usize {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    let _ = serde_json::to_writer(&mut count, value);
    count.0
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
        mut rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcErr<openraft::error::Infallible>> {
        // From the start: encoding a large batch takes its share too.
        let start = tokio::time::Instant::now();
        let allowed = option.hard_ttl().mul_f64(0.8);
        let budget = *self
            .budgets
            .lock()
            .unwrap()
            .entry(self.target)
            .or_insert(MAX_BATCH);
        let entries = rpc.entries.len();
        let keep = fitting(rpc.entries.iter().map(encoded_len), budget);
        // What doesn't fit goes in the next call, at once: openraft goes on
        // from the last entry this one says it sent.
        let sent = (keep < entries).then(|| {
            rpc.entries.truncate(keep);
            rpc.entries.last().map(|e| e.log_id)
        });
        let json =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let size = json.len();
        let partly = |r: AppendEntriesResponse<NodeId>| match (r, sent) {
            (AppendEntriesResponse::Success, Some(last)) => {
                AppendEntriesResponse::PartialSuccess(last)
            }
            (r, _) => r,
        };
        if keep <= 1 {
            return self.send(Method::Append, json, &option).await.map(partly);
        }
        // openraft gives each call the heartbeat interval and, past it,
        // sends the same batch again: on a link too slow for it, never
        // anything. A batch that runs out of time halves the budget and
        // goes again at once in fewer entries.
        let result =
            tokio::time::timeout_at(start + allowed, self.send(Method::Append, json, &option))
                .await;
        let took = result.is_ok().then(|| start.elapsed());
        self.budgets
            .lock()
            .unwrap()
            .insert(self.target, next_budget(budget, size, took, allowed));
        match result {
            Ok(result) => result.map(partly),
            Err(_) => Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint((keep / 2).max(1) as u64),
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
        // Handing leadership on: confirming would reach the followers.
        if self.store.hushed() {
            return encode(Err::<Option<u64>, String>(
                "leadership is being handed to another manager; retry in a few seconds".into(),
            ));
        }
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
    use super::{MAX_BATCH, MIN_BUDGET, encoded_len, fitting, next_budget};
    use std::time::Duration;

    #[test]
    fn a_call_carries_the_leading_entries_that_fit() {
        assert_eq!(fitting([10; 30], 100), 10);
        assert_eq!(fitting([10; 5], 100), 5);
        assert_eq!(fitting([60, 50, 10], 100), 1);
        // One entry goes whatever its size.
        assert_eq!(fitting([1000, 1], 100), 1);
        assert_eq!(fitting([], 100), 0);
        // 70 entries of a MiB within MAX_BATCH: 4 of them.
        assert_eq!(fitting(std::iter::repeat_n(1 << 20, 70), MAX_BATCH), 4);
    }

    #[test]
    fn the_budget_follows_what_the_link_carries_in_time() {
        let allowed = Duration::from_millis(800);
        // Out of time: half of what was sent, never below the floor.
        assert_eq!(next_budget(MAX_BATCH, 1 << 20, None, allowed), 1 << 19);
        assert_eq!(next_budget(MAX_BATCH, 1000, None, allowed), MIN_BUDGET);
        // Quick with a full call: double, up to the most.
        let quick = Some(Duration::from_millis(100));
        assert_eq!(next_budget(1 << 19, 1 << 19, quick, allowed), 1 << 20);
        assert_eq!(next_budget(MAX_BATCH, MAX_BATCH, quick, allowed), MAX_BATCH);
        // Quick with a small call says nothing about the link.
        assert_eq!(next_budget(1 << 19, 1000, quick, allowed), 1 << 19);
        // In time but not quick: kept.
        let slow = Some(Duration::from_millis(500));
        assert_eq!(next_budget(1 << 19, 1 << 19, slow, allowed), 1 << 19);
        // A link that keeps timing out settles at the floor.
        let mut b = MAX_BATCH;
        for _ in 0..20 {
            b = next_budget(b, b, None, allowed);
        }
        assert_eq!(b, MIN_BUDGET);
    }

    #[test]
    fn encoded_len_counts_the_json() {
        let v = serde_json::json!({"a": [1, 2, 3], "b": "x"});
        assert_eq!(encoded_len(&v), serde_json::to_vec(&v).unwrap().len());
    }
}
