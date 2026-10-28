//! The agent's view of the manager: register the node, follow the tasks
//! assigned to it and the cluster's endpoints and peers, report their
//! status.
//!
//! The agent is written against [`Control`]; [`GrpcControl`] implements it
//! over the managers' dispatcher.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use legion_api::dispatch::{
    Assignment, Assignments, EndpointPort, Endpoints, Peer, Peers, Policies, ServiceEndpoints,
    VolumeClaim,
};
use legion_api::node::NodeStatus;
use legion_api::task::TaskStatus;
use legion_api::{Kind, ObjectKey};
use legion_proto::v1::dispatcher_client::DispatcherClient;
use legion_proto::v1::{
    AssignmentsRequest, ClaimVolumeRequest, DataRequest, EndpointsRequest, HeartbeatRequest,
    PeersRequest, PoliciesRequest, RegisterRequest, ReportRequest, SessionFrame, SessionOffer,
    SessionsRequest,
};
use legion_sys::Ipv4Net;
use tokio::sync::{mpsc, watch};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// The manager cannot take writes right now (no cluster yet, no leader);
    /// retry later.
    #[error("manager unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Other(String),
}

/// What the manager gives a node when it registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The node's pod subnet.
    pub pod_cidr: Ipv4Net,
}

pub trait Control: Send + Sync + 'static {
    /// Creates the node's `Node` object, or updates its status, keeping the
    /// spec and labels operators set and the pod subnet assigned on first
    /// registration.
    fn register(
        &self,
        name: &str,
        status: &NodeStatus,
    ) -> impl Future<Output = Result<Registration, ControlError>> + Send;

    /// The tasks assigned to `node`, kept current. The receiver's initial
    /// value is empty and not yet marked changed; the first change is the
    /// first complete set, which may be empty too.
    fn assignments(&self, node: &str) -> watch::Receiver<Assignments>;

    /// Every service's endpoints, kept current; empty until first known.
    fn endpoints(&self) -> watch::Receiver<Endpoints>;

    /// Every node of the WireGuard mesh, this one included, kept current;
    /// empty until first known.
    fn peers(&self) -> watch::Receiver<Peers>;

    /// Namespaces' isolation and every network policy, kept current; `None`
    /// until first known.
    fn policies(&self) -> watch::Receiver<Option<Policies>>;

    /// Writes a task's status. A task that is gone, or was replaced by one
    /// with another uid, is ignored.
    fn report(
        &self,
        key: &ObjectKey,
        uid: &str,
        status: &TaskStatus,
    ) -> impl Future<Output = Result<(), ControlError>> + Send;

    /// Binds a `Volume` to `node`, recording its dataset there, unless
    /// another node holds it.
    fn claim_volume(
        &self,
        namespace: &str,
        name: &str,
        node: &str,
        dataset: &str,
    ) -> impl Future<Output = Result<VolumeClaim, ControlError>> + Send;

    /// The value of a `Secret` or `Config`; `None` if it does not exist.
    fn data(
        &self,
        namespace: &str,
        kind: Kind,
        name: &str,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, ControlError>> + Send;

    /// Tells the managers `node` is alive; returns how long to wait before
    /// the next heartbeat.
    fn heartbeat(&self, node: &str) -> impl Future<Output = Result<Duration, ControlError>> + Send;

    /// Sessions users open with the node's tasks, as any manager offers
    /// them.
    fn sessions(&self, node: &str) -> mpsc::Receiver<Offer>;
}

/// A session a manager offers the agent.
pub struct Offer {
    pub offer: SessionOffer,
    manager: DispatcherClient<Channel>,
}

impl Offer {
    /// Opens the session's stream with the manager that offered it.
    /// `frames` start with the session's id (`attach`).
    pub async fn attach(
        &self,
        frames: mpsc::Receiver<SessionFrame>,
    ) -> Result<tonic::Streaming<SessionFrame>, ControlError> {
        let mut manager = self.manager.clone();
        match tokio::time::timeout(
            CALL_TIMEOUT,
            manager.attach(tokio_stream::wrappers::ReceiverStream::new(frames)),
        )
        .await
        {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(status)) => Err(error(status)),
            Err(_) => Err(ControlError::Unavailable(
                "the manager did not answer".into(),
            )),
        }
    }
}

/// How often the agent looks for managers to follow sessions from.
const SESSION_MANAGERS: Duration = Duration::from_secs(5);

/// How long a call to a manager may take before the agent tries another.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest dispatcher message, matching the managers' limit.
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// [`Control`] over the managers' dispatcher. Calls go to one manager at a
/// time; when it fails, the next one in the list takes over. Heartbeat
/// responses keep the list current.
#[derive(Clone)]
pub struct GrpcControl {
    node: String,
    tls: Option<ClientTlsConfig>,
    conn: Arc<Mutex<Conn>>,
}

struct Conn {
    managers: Vec<String>,
    at: usize,
    /// Bumped on every switch, so concurrent failures switch only once.
    generation: u64,
    channel: Channel,
}

impl GrpcControl {
    /// Calls `managers` (`host:port`) for `node`, with mutual TLS when
    /// `tls` is given.
    pub fn new(
        node: &str,
        managers: Vec<String>,
        tls: Option<ClientTlsConfig>,
    ) -> Result<GrpcControl, ControlError> {
        if managers.is_empty() {
            return Err(ControlError::Other("no manager address".into()));
        }
        let channel = connect(&managers[0], tls.as_ref())?;
        Ok(GrpcControl {
            node: node.to_owned(),
            tls,
            conn: Arc::new(Mutex::new(Conn {
                managers,
                at: 0,
                generation: 0,
                channel,
            })),
        })
    }

    /// The managers, starting with the one in use.
    pub fn managers(&self) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut list = conn.managers.clone();
        list.rotate_left(conn.at);
        list
    }

    fn client(&self) -> (u64, DispatcherClient<Channel>) {
        let conn = self.conn.lock().unwrap();
        let client = DispatcherClient::new(conn.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        (conn.generation, client)
    }

    /// Moves to the next manager after a failure on `generation`'s.
    fn failed(&self, generation: u64) {
        let mut conn = self.conn.lock().unwrap();
        if conn.generation != generation || conn.managers.len() < 2 {
            return;
        }
        let next = (conn.at + 1) % conn.managers.len();
        match connect(&conn.managers[next], self.tls.as_ref()) {
            Ok(channel) => {
                tracing::debug!(manager = %conn.managers[next], "switching manager");
                conn.at = next;
                conn.generation += 1;
                conn.channel = channel;
            }
            Err(e) => tracing::warn!("{e}"),
        }
    }

    /// Takes the managers' current addresses, keeping the one in use.
    fn set_managers(&self, mut managers: Vec<String>) {
        if managers.is_empty() {
            return;
        }
        managers.sort();
        managers.dedup();
        let mut conn = self.conn.lock().unwrap();
        let current = conn.managers[conn.at].clone();
        match managers.iter().position(|m| *m == current) {
            Some(at) => {
                conn.managers = managers;
                conn.at = at;
            }
            // The one in use is not listed (it may be reached under another
            // address); keep it until it fails.
            None => {
                managers.insert(0, current);
                conn.managers = managers;
                conn.at = 0;
            }
        }
    }

    /// Runs one unary call against the current manager, moving on to the
    /// next if the manager cannot be reached.
    async fn call<T, F>(
        &self,
        f: impl FnOnce(DispatcherClient<Channel>) -> F,
    ) -> Result<T, ControlError>
    where
        F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let (generation, client) = self.client();
        match tokio::time::timeout(CALL_TIMEOUT, f(client)).await {
            Ok(Ok(resp)) => Ok(resp.into_inner()),
            Ok(Err(status)) => {
                let err = error(status);
                if matches!(err, ControlError::Unavailable(_)) {
                    self.failed(generation);
                }
                Err(err)
            }
            Err(_) => {
                self.failed(generation);
                Err(ControlError::Unavailable(
                    "the manager did not answer".into(),
                ))
            }
        }
    }

    /// Keeps `tx` equal to what a dispatcher stream sends, opening it again
    /// whenever it ends. Ends when every receiver is gone.
    async fn follow<T, M, S, Fut>(
        self,
        tx: watch::Sender<T>,
        open: impl Fn(DispatcherClient<Channel>) -> Fut + Send,
        decode: impl Fn(M) -> Result<T, String> + Send,
        publish: impl Fn(&watch::Sender<T>, T) + Send,
    ) where
        T: Send + Sync,
        M: Send,
        S: futures::Stream<Item = Result<M, tonic::Status>> + Send + Unpin,
        Fut: Future<Output = Result<tonic::Response<S>, tonic::Status>> + Send,
    {
        use futures::StreamExt;
        loop {
            let (generation, client) = self.client();
            let opened = tokio::select! {
                o = tokio::time::timeout(CALL_TIMEOUT, open(client)) => o,
                _ = tx.closed() => return,
            };
            match opened {
                Ok(Ok(resp)) => {
                    let mut stream = resp.into_inner();
                    loop {
                        let next = tokio::select! {
                            n = stream.next() => n,
                            _ = tx.closed() => return,
                        };
                        match next {
                            Some(Ok(m)) => match decode(m) {
                                Ok(v) => publish(&tx, v),
                                Err(e) => {
                                    tracing::warn!("undecodable dispatcher message: {e}");
                                    break;
                                }
                            },
                            Some(Err(status)) => {
                                tracing::debug!("dispatcher stream ended: {status}");
                                self.failed(generation);
                                break;
                            }
                            None => break,
                        }
                    }
                }
                Ok(Err(status)) => {
                    tracing::debug!("opening a dispatcher stream: {status}");
                    if matches!(error(status), ControlError::Unavailable(_)) {
                        self.failed(generation);
                    }
                }
                Err(_) => self.failed(generation),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                _ = tx.closed() => return,
            }
        }
    }
}

/// Passes on the sessions `manager` offers, following its stream again
/// whenever it ends, until `tx` is closed.
async fn follow_sessions(
    manager: String,
    tls: Option<ClientTlsConfig>,
    node: String,
    tx: mpsc::Sender<Offer>,
) {
    use futures::StreamExt;
    let channel = match connect(&manager, tls.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("{e}");
            return;
        }
    };
    let client = DispatcherClient::new(channel);
    loop {
        let mut c = client.clone();
        let opened = tokio::select! {
            o = c.sessions(SessionsRequest { node: node.clone() }) => o,
            _ = tx.closed() => return,
        };
        match opened {
            Ok(resp) => {
                let mut offers = resp.into_inner();
                loop {
                    let next = tokio::select! {
                        n = offers.next() => n,
                        _ = tx.closed() => return,
                    };
                    match next {
                        Some(Ok(offer)) => {
                            let offer = Offer {
                                offer,
                                manager: client.clone(),
                            };
                            if tx.send(offer).await.is_err() {
                                return;
                            }
                        }
                        Some(Err(status)) => {
                            tracing::debug!(%manager, "sessions stream ended: {status}");
                            break;
                        }
                        None => break,
                    }
                }
            }
            Err(status) => tracing::debug!(%manager, "following sessions: {status}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = tx.closed() => return,
        }
    }
}

/// A lazily connected channel to a manager, with keepalives so a manager
/// that vanishes without closing the connection is noticed.
fn connect(addr: &str, tls: Option<&ClientTlsConfig>) -> Result<Channel, ControlError> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let bad = |e: tonic::transport::Error| ControlError::Other(format!("manager {addr}: {e}"));
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{addr}"))
        .map_err(bad)?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if let Some(tls) = tls {
        endpoint = endpoint.tls_config(tls.clone()).map_err(bad)?;
    }
    Ok(endpoint.connect_lazy())
}

/// Codes worth retrying, against this manager or another, are
/// [`ControlError::Unavailable`].
fn error(status: tonic::Status) -> ControlError {
    use tonic::Code;
    match status.code() {
        Code::Unavailable
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::Cancelled
        | Code::Aborted
        | Code::FailedPrecondition => ControlError::Unavailable(status.message().to_owned()),
        _ => ControlError::Other(status.message().to_owned()),
    }
}

fn decode<T: serde::de::DeserializeOwned>(json: &[u8]) -> Result<T, String> {
    serde_json::from_slice(json).map_err(|e| e.to_string())
}

/// Publishes `set` if it differs from what `tx` holds.
fn publish_changed<T: PartialEq>(tx: &watch::Sender<T>, set: T) {
    tx.send_if_modified(|old| {
        if *old == set {
            false
        } else {
            *old = set;
            true
        }
    });
}

impl Control for GrpcControl {
    async fn register(
        &self,
        name: &str,
        status: &NodeStatus,
    ) -> Result<Registration, ControlError> {
        let req = RegisterRequest {
            node: name.to_owned(),
            status_json: serde_json::to_vec(status)
                .map_err(|e| ControlError::Other(e.to_string()))?,
        };
        let resp = self
            .call(|mut c| async move { c.register(req).await })
            .await?;
        Ok(Registration {
            pod_cidr: resp
                .pod_cidr
                .parse()
                .map_err(|e| ControlError::Other(format!("pod subnet {:?}: {e}", resp.pod_cidr)))?,
        })
    }

    fn assignments(&self, node: &str) -> watch::Receiver<Assignments> {
        let (tx, rx) = watch::channel(Assignments::new());
        let node = node.to_owned();
        tokio::spawn(self.clone().follow(
            tx,
            move |mut c| {
                let node = node.clone();
                async move { c.assignments(AssignmentsRequest { node }).await }
            },
            |m: legion_proto::v1::AssignmentsUpdate| {
                let list: Vec<Assignment> = decode(&m.assignments_json)?;
                Ok(list.into_iter().map(|a| (a.uid.clone(), a)).collect())
            },
            // Every set is published, even if equal: the first one after
            // (re)connecting tells the agent the set is complete.
            |tx, set| {
                tx.send_replace(set);
            },
        ));
        rx
    }

    fn endpoints(&self) -> watch::Receiver<Endpoints> {
        let (tx, rx) = watch::channel(Endpoints::new());
        let node = self.node.clone();
        tokio::spawn(self.clone().follow(
            tx,
            move |mut c| {
                let node = node.clone();
                async move { c.endpoints(EndpointsRequest { node }).await }
            },
            |m: legion_proto::v1::EndpointsUpdate| decode(&m.endpoints_json),
            publish_changed,
        ));
        rx
    }

    fn peers(&self) -> watch::Receiver<Peers> {
        let (tx, rx) = watch::channel(Peers::new());
        let node = self.node.clone();
        tokio::spawn(self.clone().follow(
            tx,
            move |mut c| {
                let node = node.clone();
                async move { c.peers(PeersRequest { node }).await }
            },
            |m: legion_proto::v1::PeersUpdate| decode(&m.peers_json),
            publish_changed,
        ));
        rx
    }

    fn policies(&self) -> watch::Receiver<Option<Policies>> {
        let (tx, rx) = watch::channel(None);
        let node = self.node.clone();
        tokio::spawn(self.clone().follow(
            tx,
            move |mut c| {
                let node = node.clone();
                async move { c.policies(PoliciesRequest { node }).await }
            },
            |m: legion_proto::v1::PoliciesUpdate| decode(&m.policies_json).map(Some),
            publish_changed,
        ));
        rx
    }

    async fn report(
        &self,
        key: &ObjectKey,
        uid: &str,
        status: &TaskStatus,
    ) -> Result<(), ControlError> {
        let req = ReportRequest {
            node: self.node.clone(),
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: uid.to_owned(),
            status_json: serde_json::to_vec(status)
                .map_err(|e| ControlError::Other(e.to_string()))?,
        };
        self.call(|mut c| async move { c.report(req).await })
            .await
            .map(|_| ())
    }

    async fn claim_volume(
        &self,
        namespace: &str,
        name: &str,
        node: &str,
        dataset: &str,
    ) -> Result<VolumeClaim, ControlError> {
        let req = ClaimVolumeRequest {
            node: node.to_owned(),
            namespace: namespace.to_owned(),
            name: name.to_owned(),
            dataset: dataset.to_owned(),
        };
        let resp = self
            .call(|mut c| async move { c.claim_volume(req).await })
            .await?;
        decode(&resp.claim_json).map_err(ControlError::Other)
    }

    async fn data(
        &self,
        namespace: &str,
        kind: Kind,
        name: &str,
    ) -> Result<Option<Vec<u8>>, ControlError> {
        let req = DataRequest {
            node: self.node.clone(),
            namespace: namespace.to_owned(),
            kind: kind.to_string(),
            name: name.to_owned(),
        };
        let resp = self.call(|mut c| async move { c.data(req).await }).await?;
        Ok(resp.value)
    }

    fn sessions(&self, node: &str) -> mpsc::Receiver<Offer> {
        let (tx, rx) = mpsc::channel(16);
        let this = self.clone();
        let node = node.to_owned();
        // One stream per manager, so any of them can reach the node; the
        // heartbeats keep the list of managers current.
        tokio::spawn(async move {
            let mut following: std::collections::HashMap<String, tokio::task::JoinHandle<()>> =
                Default::default();
            loop {
                let managers = this.managers();
                following.retain(|m, h| {
                    let keep = managers.contains(m);
                    if !keep {
                        h.abort();
                    }
                    keep && !h.is_finished()
                });
                for m in managers {
                    following.entry(m.clone()).or_insert_with(|| {
                        tokio::spawn(follow_sessions(
                            m,
                            this.tls.clone(),
                            node.clone(),
                            tx.clone(),
                        ))
                    });
                }
                tokio::select! {
                    _ = tokio::time::sleep(SESSION_MANAGERS) => {}
                    _ = tx.closed() => break,
                }
            }
            for h in following.values() {
                h.abort();
            }
        });
        rx
    }

    async fn heartbeat(&self, node: &str) -> Result<Duration, ControlError> {
        let req = HeartbeatRequest {
            node: node.to_owned(),
        };
        let resp = self
            .call(|mut c| async move { c.heartbeat(req).await })
            .await?;
        self.set_managers(resp.managers);
        Ok(Duration::from_millis(resp.period_ms.max(100).into()))
    }
}
