//! The public API: `Objects` and `Cluster` gRPC services over the store.
//!
//! Any manager accepts any request. Writes and consistent reads are served
//! by the leader, so a follower forwards them once (marking the request so it
//! is never forwarded twice). Watches are served from the local replica.

use legion_api::protocol::Protocol;
use std::collections::BTreeMap;

use legion_api::{CLUSTER_SCOPE, Kind, Object, ObjectKey};
use legion_pki::Role;
use legion_proto::v1::cluster_server::Cluster;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::objects_server::Objects;
use legion_proto::v1::{
    self as pb, ApplyRequest, ApplyResponse, ApplyResult, DeleteRequest, DeleteResponse,
    InitRequest, ListRequest, ListResponse, ObjectRef, StatusRequest, StatusResponse, WatchRequest,
    watch_event,
};
use legion_store::{Command, Event, NodeId, Outcome, Precondition, Store, WatchError};
use openraft::BasicNode;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::authz::{Caller, Verb};
use crate::join::SYSTEM_NAMESPACE;
use crate::raft_net::{MAX_MESSAGE, connect_lazy};

const FORWARDED: &str = "legion-forwarded";

/// Attempts at a server-side apply that keeps losing a race with other writers.
const APPLY_ATTEMPTS: usize = 5;

#[derive(Clone)]
pub struct ApiService {
    pub(crate) id: NodeId,
    /// This manager's node name, which `status` gives other managers.
    pub(crate) name: String,
    /// Where it records what it decides, for the audit trail.
    pub(crate) audit: crate::audit::Audit,
    /// What tasks and nodes used, kept 30 days.
    pub(crate) history: crate::history::Shared,
    /// `host:port` peers and forwarded requests reach this manager at.
    pub(crate) advertise: String,
    pub(crate) store: Store,
    /// Becomes true when the manager shuts down; open watches then end so
    /// the server's graceful shutdown can finish.
    pub(crate) closing: tokio::sync::watch::Receiver<bool>,
    /// For forwarding to the leader.
    pub(crate) peer_tls: Option<std::sync::Arc<crate::Credentials>>,
    /// Certifies joining nodes; managers without the CA key cannot.
    pub(crate) issuer: Option<crate::join::Issuer>,
    /// Held from quota or route host check to write, so two applies cannot
    /// both pass.
    pub(crate) quota_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// The agents connected to this manager, for sessions with their tasks.
    pub(crate) sessions: std::sync::Arc<crate::session::Hub>,
    /// What tasks use, as heartbeats report it.
    pub(crate) usage: std::sync::Arc<crate::usage::Usage>,
    /// What this manager speaks and works with, with agents.
    pub(crate) protocol: legion_api::protocol::Protocol,
    /// What it speaks and works with among managers: the floor counts.
    pub(crate) speaking: crate::upgrade::Speaking,
    /// The CA each node's certificate was of at its last heartbeat here.
    pub(crate) ca_seen: crate::ca::Seen,
    /// The CARP state of each load balancer address, as each node's
    /// heartbeats here report it.
    pub(crate) balancers: std::sync::Arc<crate::balancers::Held>,
    /// What happened to objects lately, as this manager saw it.
    pub(crate) events: std::sync::Arc<crate::events::State>,
    /// Long operations, as this manager follows them (`Cluster.Activity`).
    pub(crate) activity: std::sync::Arc<crate::activity::State>,
    /// The restarts this manager has seen, for its alerts.
    pub(crate) alerts: std::sync::Arc<crate::alerts::State>,
    /// The logs of the builds this manager runs as leader.
    pub(crate) builds: std::sync::Arc<crate::build::Logs>,
    pub(crate) registry_loopback: bool,
    pub(crate) insecure_registries: std::sync::Arc<[String]>,
    /// A witness: it votes, runs no tasks and never leads (docs/HA.md).
    pub(crate) witness: bool,
    /// Held while leadership is handed over ([`crate::lead::hand_over`]),
    /// and while a manager leaves the membership: one at a time, for this
    /// manager (not the process: tests run several in one).
    pub(crate) handing_over: std::sync::Arc<tokio::sync::Mutex<()>>,
    pub(crate) leaving: std::sync::Arc<tokio::sync::Mutex<()>>,
}

pub(crate) enum Route {
    Local,
    /// A client for the leader, and its address as routing saw it.
    Leader(ObjectsClient<Channel>, String),
}

/// The reserved namespace holds Legion's own records; no caller sees it.
pub(crate) fn check_namespace(namespace: &str) -> Result<(), Status> {
    if namespace == SYSTEM_NAMESPACE {
        Err(Status::permission_denied(format!(
            "namespace {SYSTEM_NAMESPACE} is reserved for Legion"
        )))
    } else {
        Ok(())
    }
}

/// Whether the voter `id` (`name`) may leave the membership `voters`, as
/// the manager `me`: see [`ApiService::leave_membership`].
fn leaving_voter(
    name: &str,
    id: NodeId,
    me: NodeId,
    voters: &std::collections::BTreeSet<NodeId>,
    witnesses: &std::collections::HashSet<NodeId>,
) -> Result<(), Status> {
    if voters.len() == 1 {
        return Err(Status::failed_precondition(format!(
            "manager {name} is the cluster's last voter: it cannot be removed"
        )));
    }
    if !voters.iter().any(|v| *v != id && !witnesses.contains(v)) {
        return Err(Status::failed_precondition(format!(
            "manager {name} is the last that may lead, the others witnesses: promote another first"
        )));
    }
    if id == me {
        return Err(Status::failed_precondition(format!(
            "manager {name} leads: hand leadership to another first (`legion node lead MANAGER`)"
        )));
    }
    Ok(())
}

impl ApiService {
    /// The managers as Raft's membership and their own word have them, by
    /// name, and the leader's name (empty when there is none).
    pub(crate) fn managers_seen(&self) -> Result<(Vec<legion_api::ha::Manager>, String), Status> {
        let m = self.store.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let names = crate::managers::names(&self.store)?;
        let witnesses = crate::managers::witnesses(&self.store)?;
        let name_of = |id: &NodeId| {
            if *id == self.id {
                self.name.clone()
            } else {
                names.get(id).cloned().unwrap_or_else(|| format!("#{id}"))
            }
        };
        let managers = membership
            .nodes()
            .map(|(id, _)| legion_api::ha::Manager {
                name: name_of(id),
                voter: membership.voter_ids().any(|v| v == *id),
                witness: witnesses.contains(id) || (*id == self.id && self.witness),
            })
            .collect();
        let leader = m.current_leader.as_ref().map(name_of).unwrap_or_default();
        Ok((managers, leader))
    }

    /// The cluster's single points of failure as `caller` may see them: of
    /// `namespace` (every namespace when empty) where they may read its
    /// services, the cluster's own (managers, nodes) where they may read
    /// nodes.
    pub(crate) fn availability_seen(
        &self,
        caller: &Caller,
        namespace: &str,
    ) -> Result<legion_api::ha::Report, Status> {
        let list = |kind| -> Result<Vec<Object>, Status> {
            Ok(self.store.list(kind, None).map_err(internal)?.objects)
        };
        let nodes = list(Kind::Node)?;
        let (managers, _) = self.managers_seen()?;
        let found = legion_api::ha::check(&legion_api::ha::Cluster {
            managers: &managers,
            nodes: &nodes,
            services: &list(Kind::Service)?,
            tasks: &list(Kind::Task)?,
            volumes: &list(Kind::Volume)?,
        });
        let see_nodes = caller.may(&self.store, Verb::Read, Kind::Node, "");
        let mut report = legion_api::ha::Report::default();
        for f in found {
            let visible = if f.namespace.is_empty() {
                see_nodes
            } else {
                (namespace.is_empty() || f.namespace == namespace)
                    && caller.may(&self.store, Verb::Read, Kind::Service, &f.namespace)
            };
            if visible {
                report.findings.push(f);
            } else if !f.namespace.is_empty() && (namespace.is_empty() || f.namespace == namespace)
            {
                report.hidden += 1;
            }
        }
        if see_nodes {
            report.nodes = legion_api::ha::task_nodes(&nodes).len() as u32;
            report.managers = managers;
        }
        Ok(report)
    }

    /// The samples kept of what tasks and nodes used over `range_ms` (the
    /// last hour when 0), as the leader keeps them: what `caller` may read
    /// of `namespace` (every namespace when empty), `task` alone when set.
    pub(crate) async fn history_seen(
        &self,
        caller: &Caller,
        namespace: &str,
        task: &str,
        range_ms: i64,
    ) -> Result<pb::UsageHistoryResponse, Status> {
        // Each stored task's service, requests and limits.
        let specs: std::collections::HashMap<String, legion_api::task::TaskSpec> = self
            .store
            .list(Kind::Task, (!namespace.is_empty()).then_some(namespace))
            .map(|l| l.objects)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|t| {
                let key = format!("{}/{}", t.metadata.namespace, t.metadata.name);
                Some((key, legion_api::parse(&t.spec).ok()?))
            })
            .collect();
        // Up to an hour, the leader's own samples; past it, those kept.
        let hour = crate::usage::HISTORY.as_millis() as i64;
        let (step_ms, task_series, node_series) = if range_ms <= hour {
            let tasks = self
                .usage
                .history()
                .into_iter()
                .map(|(key, (node, samples))| {
                    let samples = samples
                        .iter()
                        .map(|s| (s.at_ms, f64::from(s.cpu), s.memory))
                        .collect::<Vec<_>>();
                    (key, (node, String::new(), samples))
                })
                .collect::<Vec<_>>();
            let nodes = self.usage.node_history().into_iter().collect::<Vec<_>>();
            (crate::usage::STEP.as_millis() as i64, tasks, nodes)
        } else {
            let step = crate::history::step_for(std::time::Duration::from_millis(range_ms as u64));
            let step_ms = step.as_millis() as i64;
            let from = crate::history::now_ms() - range_ms;
            let archive = self.history.clone();
            let kept =
                tokio::task::spawn_blocking(move || archive.lock().unwrap().range(from, step_ms))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            let mut tasks = Vec::new();
            let mut nodes = Vec::new();
            for (key, s) in kept {
                if let Some(task) = key.strip_prefix("t/") {
                    let samples = s
                        .points
                        .iter()
                        .filter(|(_, v)| v.len() >= 2)
                        .map(|(at, v)| (*at, v[0], v[1] as u64))
                        .collect::<Vec<_>>();
                    tasks.push((task.to_owned(), (s.node, s.service, samples)));
                } else if let Some(node) = key.strip_prefix("n/") {
                    let samples = s
                        .points
                        .iter()
                        .filter_map(|(at, v)| crate::history::node_sample(*at, v))
                        .collect::<Vec<_>>();
                    nodes.push((node.to_owned(), samples));
                }
            }
            (step_ms, tasks, nodes)
        };
        let tasks = task_series
            .into_iter()
            .filter_map(|(key, (node, kept_service, samples))| {
                let (ns, name) = key.split_once('/')?;
                let spec = specs.get(&key);
                let kept_service = (!kept_service.is_empty()).then_some(kept_service);
                let service = spec
                    .map(|s| s.service.clone())
                    .or(kept_service)
                    .unwrap_or_else(|| {
                        let mut parts = name.rsplitn(3, '-');
                        let (_, _, svc) = (parts.next(), parts.next(), parts.next());
                        svc.unwrap_or(name).to_owned()
                    });
                let wanted = ns != SYSTEM_NAMESPACE
                    && (namespace.is_empty() || ns == namespace)
                    && (task.is_empty() || name == task)
                    && caller.may(&self.store, Verb::Read, Kind::Task, ns);
                wanted.then(|| pb::TaskSeries {
                    namespace: ns.to_owned(),
                    task: name.to_owned(),
                    node,
                    service,
                    cpu_request_percent: spec.map_or(0.0, |s| s.pod.requests().cpu.0 as f64 / 10.0),
                    memory_request_bytes: spec.map_or(0, |s| s.pod.requests().memory.0),
                    cpu_limit_percent: spec
                        .and_then(|s| pod_limits(&s.pod).0)
                        .map_or(0.0, |c| c as f64 / 10.0),
                    memory_limit_bytes: spec.and_then(|s| pod_limits(&s.pod).1).unwrap_or(0),
                    samples: samples
                        .iter()
                        .map(|&(at_ms, cpu_percent, memory_bytes)| pb::UsageSample {
                            at_ms,
                            cpu_percent,
                            memory_bytes,
                        })
                        .collect(),
                })
            })
            .collect();
        let nodes = if task.is_empty() && caller.may(&self.store, Verb::Read, Kind::Node, "") {
            node_series
                .into_iter()
                .map(|(node, samples)| pb::NodeSeries {
                    node,
                    samples: samples
                        .into_iter()
                        .map(|s| pb::NodeSample {
                            at_ms: s.at_ms,
                            zfs_used_bytes: s.zfs_used_bytes,
                            zfs_avail_bytes: s.zfs_avail_bytes,
                            net_rx_bps: s.net_rx_bps,
                            net_tx_bps: s.net_tx_bps,
                            zfs_read_bps: s.zfs_read_bps,
                            zfs_write_bps: s.zfs_write_bps,
                            zfs_read_iops: s.zfs_read_iops,
                            zfs_write_iops: s.zfs_write_iops,
                        })
                        .collect(),
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(pb::UsageHistoryResponse {
            tasks,
            step_ms,
            nodes,
        })
    }

    /// A namespace's load balancer addresses and the CARP state each node
    /// last reported of them, as the leader heard it.
    pub(crate) fn balancers_seen(
        &self,
        namespace: &str,
    ) -> Result<pb::LoadBalancersResponse, Status> {
        let all = |kind| -> Result<BTreeMap<ObjectKey, Object>, Status> {
            Ok(self
                .store
                .list(kind, None)
                .map_err(internal)?
                .objects
                .into_iter()
                .map(|o| (o.key(), o))
                .collect())
        };
        let services: BTreeMap<ObjectKey, Object> = all(Kind::Service)?
            .into_iter()
            .filter(|(k, _)| k.namespace == namespace)
            .collect();
        // The nodes that may hold each address, as the dispatcher gives
        // them out; tasks don't matter to that.
        let endpoints =
            crate::dispatcher::endpoints(&services, &BTreeMap::new(), &all(Kind::Node)?);
        let reported = self.balancers.fresh();
        let mut balancers: Vec<pb::LoadBalancer> = endpoints
            .into_iter()
            .filter_map(|e| {
                let lb = e.load_balancer?;
                let mut nodes: Vec<pb::LoadBalancerNode> = lb
                    .skews
                    .iter()
                    .map(|(node, skew)| pb::LoadBalancerNode {
                        node: node.clone(),
                        skew: (*skew).into(),
                        state: reported
                            .get(node)
                            .and_then(|r| r.get(&lb.address))
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect();
                nodes.sort_by_key(|n| n.skew);
                Some(pb::LoadBalancer {
                    service: e.name,
                    address: lb.address.to_string(),
                    vhid: lb.vhid.into(),
                    nodes,
                })
            })
            .collect();
        balancers.sort_by(|a, b| a.service.cmp(&b.service));
        Ok(pb::LoadBalancersResponse { balancers })
    }

    /// What tasks and nodes use now, as the leader heard it: what
    /// `caller` may read of `namespace` (every namespace when empty).
    pub(crate) fn usage_seen(
        &self,
        caller: &Caller,
        namespace: &str,
        system: bool,
    ) -> pb::UsageResponse {
        let mut usage = {
            let mut nodes: BTreeMap<String, Vec<pb::TaskUsage>> = BTreeMap::new();
            for (key, (node, u)) in self.usage.fresh() {
                let Some((ns, task)) = key.split_once('/') else {
                    continue;
                };
                if (ns == SYSTEM_NAMESPACE && !system) || (!namespace.is_empty() && ns != namespace)
                {
                    continue;
                }
                nodes.entry(node).or_default().push(pb::TaskUsage {
                    namespace: ns.to_owned(),
                    task: task.to_owned(),
                    cpu_percent: u.cpu,
                    memory_bytes: u.memory,
                });
            }
            let mut node_telem = self.usage.fresh_nodes();
            for n in node_telem.keys() {
                nodes.entry(n.clone()).or_default();
            }
            pb::UsageResponse {
                nodes: nodes
                    .into_iter()
                    .map(|(node, tasks)| {
                        let t = node_telem.remove(&node).unwrap_or_default();
                        let volumes = t
                            .volumes
                            .into_iter()
                            .filter(|v| {
                                (v.namespace != SYSTEM_NAMESPACE || system)
                                    && (namespace.is_empty() || v.namespace == namespace)
                            })
                            .map(|v| pb::VolumeTelemetry {
                                namespace: v.namespace,
                                volume: v.volume,
                                used_bytes: v.used_bytes,
                                avail_bytes: v.avail_bytes,
                            })
                            .collect();
                        pb::NodeUsage {
                            node,
                            tasks,
                            zfs_used_bytes: t.zfs_used_bytes,
                            zfs_avail_bytes: t.zfs_avail_bytes,
                            net_rx_bps: t.net_rx_bps,
                            net_tx_bps: t.net_tx_bps,
                            zfs_read_bps: t.zfs_read_bps,
                            zfs_write_bps: t.zfs_write_bps,
                            zfs_read_iops: t.zfs_read_iops,
                            zfs_write_iops: t.zfs_write_iops,
                            volumes,
                            images_bytes: t.images_bytes,
                            jails_bytes: t.jails_bytes,
                            build_bytes: t.build_bytes,
                            load1: t.load[0],
                            load5: t.load[1],
                            load15: t.load[2],
                        }
                    })
                    .collect(),
            }
        };
        // A user sees the tasks and volumes of the namespaces they may read in,
        // and node-level ZFS/network metrics only when permitted to read Nodes.
        if *caller != Caller::All {
            let can_nodes = caller.may(&self.store, Verb::Read, Kind::Node, "");
            for n in &mut usage.nodes {
                n.tasks
                    .retain(|t| caller.may(&self.store, Verb::Read, Kind::Task, &t.namespace));
                n.volumes
                    .retain(|v| caller.may(&self.store, Verb::Read, Kind::Volume, &v.namespace));
                if !can_nodes {
                    n.zfs_used_bytes = 0;
                    n.zfs_avail_bytes = 0;
                    n.net_rx_bps = 0;
                    n.net_tx_bps = 0;
                    (n.zfs_read_bps, n.zfs_write_bps) = (0, 0);
                    (n.zfs_read_iops, n.zfs_write_iops) = (0, 0);
                    n.images_bytes = 0;
                    n.jails_bytes = 0;
                    n.build_bytes = 0;
                    (n.load1, n.load5, n.load15) = (0, 0, 0);
                }
            }
            if !can_nodes {
                usage
                    .nodes
                    .retain(|n| !n.tasks.is_empty() || !n.volumes.is_empty());
            }
        }
        usage
    }

    /// The leader's address, or `None` if this manager leads.
    pub(crate) fn leader_addr<T>(&self, req: &Request<T>) -> Result<Option<String>, Status> {
        match self.route(req)? {
            Route::Local => Ok(None),
            // The address of the leader routing found, not read again: an
            // election may have cleared it since.
            Route::Leader(_, addr) => Ok(Some(addr)),
        }
    }

    fn issuer(&self) -> Result<&crate::join::Issuer, Status> {
        self.issuer
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("this manager cannot issue certificates"))
    }

    /// A certificate for `id` again, for a new key: any manager with the CA
    /// signs it, leader or not, as nothing is written.
    pub(crate) fn renew(&self, csr_pem: &str, id: &legion_pki::Identity) -> Result<String, Status> {
        self.issuer()?
            .current()?
            .sign(csr_pem, id, legion_pki::CERT_LIFETIME)
            .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))
    }

    pub(crate) async fn join(
        &self,
        req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        // Refused here as by the leader, before anything is signed; answered
        // with what both work with.
        let mine = self.protocol;
        let theirs = Protocol::said(req.get_ref().protocol, req.get_ref().oldest_protocol);
        mine.works_with(theirs, &format!("node {}", req.get_ref().name))
            .map_err(Status::unimplemented)?;
        let mut answer = if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::join_client::JoinClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            c.join(forwarded(req.into_inner())).await?.into_inner()
        } else {
            // The tokens and removals it checks are the latest: a worker's
            // join writes nothing that would fail on a deposed leader.
            self.consistent().await?;
            self.issuer()?.join(self, req.into_inner()).await?
        };
        let leader = Protocol::said(answer.protocol, answer.oldest_protocol);
        answer.protocol = leader.speaks.min(mine.speaks);
        answer.oldest_protocol = leader.oldest.max(mine.oldest);
        Ok(Response::new(answer))
    }

    /// Where a leader-only request should run.
    pub(crate) fn route<T>(&self, req: &Request<T>) -> Result<Route, Status> {
        let metrics = self.store.raft.metrics().borrow().clone();
        match metrics.current_leader {
            Some(leader) if leader == self.id => Ok(Route::Local),
            Some(leader) => {
                if req.metadata().contains_key(FORWARDED) {
                    return Err(Status::unavailable("leadership is changing; retry"));
                }
                let addr = metrics
                    .membership_config
                    .membership()
                    .get_node(&leader)
                    .map(|n| n.addr.clone())
                    .ok_or_else(|| {
                        Status::unavailable(format!("leader {leader} has no address"))
                    })?;
                Ok(Route::Leader(
                    ObjectsClient::new(connect_lazy(&addr, self.peer_tls.as_ref()))
                        .max_decoding_message_size(MAX_MESSAGE)
                        .max_encoding_message_size(MAX_MESSAGE),
                    addr,
                ))
            }
            None if metrics
                .membership_config
                .membership()
                .voter_ids()
                .next()
                .is_none() =>
            {
                Err(Status::failed_precondition(
                    "this manager is not part of a cluster; run `legion init`",
                ))
            }
            None => Err(Status::unavailable("no leader elected; retry")),
        }
    }

    /// Waits until this manager's store holds all the leader has committed
    /// (a linearizable read from then on). The leader checks it still
    /// leads; a follower, whose replica may lag or be cut off, asks the
    /// leader how far that is and waits to have applied as much.
    pub(crate) async fn consistent(&self) -> Result<(), Status> {
        tokio::time::timeout(crate::COMMIT_TIMEOUT, self.linearize())
            .await
            .map_err(|_| crate::no_quorum())?
    }

    async fn linearize(&self) -> Result<(), Status> {
        let Some(leader) = self.leader_addr(&Request::new(()))? else {
            // Confirming would reach the followers, which must hear
            // nothing while leadership is handed on.
            if self.store.hushed() {
                return Err(Status::unavailable(
                    "leadership is being handed to another manager; retry in a few seconds",
                ));
            }
            return self
                .store
                .raft
                .ensure_linearizable()
                .await
                .map(|_| ())
                .map_err(|e| Status::unavailable(e.to_string()));
        };
        let index = crate::raft_net::read_index(&leader, self.peer_tls.as_ref()).await?;
        self.store
            .raft
            .wait(None)
            .applied_index_at_least(index, "the leader's read index")
            .await
            .map(drop)
            .map_err(|e| Status::unavailable(e.to_string()))
    }

    pub(crate) async fn write(&self, cmd: Command) -> Result<Outcome, Status> {
        let written = tokio::time::timeout(crate::COMMIT_TIMEOUT, self.store.client_write(cmd))
            .await
            .map_err(|_| crate::no_quorum())?;
        match written {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id,
                ..
            }))) => Err(Status::unavailable(format!(
                "leadership moved to {leader_id:?}; retry"
            ))),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn apply_local(&self, req: ApplyRequest) -> Result<ApplyResponse, Status> {
        let wire = req
            .object
            .ok_or_else(|| Status::invalid_argument("object is required"))?;
        let mut desired = Object::try_from(wire).map_err(invalid)?;
        default_namespace(desired.kind, &mut desired.metadata.namespace);
        check_namespace(&desired.metadata.namespace)?;
        if desired.kind == Kind::Namespace {
            check_namespace(&desired.metadata.name)?;
        }
        legion_api::admit(&mut desired).map_err(invalid)?;
        if desired.kind == Kind::ImagePolicy {
            check_policy_keys(&desired)?;
        }
        let key = desired.key();
        // Applies that check other objects than their own (quotas, a
        // route's host) take turns, so two cannot pass on the same state;
        // so do those of what they are checked against (a Namespace's
        // quota and security) and of Jobs, checked against those.
        let _counting = match crate::quota::counts(desired.kind)
            || matches!(
                desired.kind,
                Kind::Route | Kind::Namespace | Kind::Job | Kind::CronJob | Kind::Volume
            ) {
            true => Some(self.quota_lock.lock().await),
            false => None,
        };

        for _ in 0..APPLY_ATTEMPTS {
            let current = self.store.get(&key).map_err(internal)?;
            // Version 0 is no object's (versions are log indexes): only
            // made, never written over.
            if let Some(expected) = req.expected_version
                && current.as_ref().map_or(0, |o| o.metadata.version) != expected
            {
                return Err(conflict(&key, current.as_ref()));
            }
            let (object, precondition, result) = match &current {
                Some(cur) => {
                    if cur.spec == desired.spec && cur.metadata.labels == desired.metadata.labels {
                        return Ok(ApplyResponse {
                            object: Some(cur.into()),
                            result: ApplyResult::Unchanged.into(),
                        });
                    }
                    let mut next = cur.clone();
                    next.spec = desired.spec.clone();
                    next.metadata.labels = desired.metadata.labels.clone();
                    (
                        next,
                        Precondition::Version(cur.metadata.version),
                        ApplyResult::Updated,
                    )
                }
                // Status is the cluster's: controllers trust it, so a
                // caller's own never goes in.
                None => (
                    Object {
                        status: serde_json::Value::Null,
                        ..desired.clone()
                    },
                    Precondition::MustNotExist,
                    ApplyResult::Created,
                ),
            };
            if let Some(cur) = &current {
                check_storage(&self.store, cur, &object)?;
                check_switch_over(&self.store, cur, &object)?;
            }
            crate::quota::check(&self.store, &object)?;
            crate::security::check(&self.store, &object)?;
            check_host(&self.store, &object)?;
            check_alias(&self.store, &object)?;
            if req.dry_run {
                return Ok(ApplyResponse {
                    object: Some((&object).into()),
                    result: result.into(),
                });
            }
            match self
                .write(Command::Put {
                    object,
                    precondition,
                })
                .await?
            {
                Outcome::Written(o) => {
                    return Ok(ApplyResponse {
                        object: Some((&o).into()),
                        result: result.into(),
                    });
                }
                // Someone else wrote in between; apply on top of their write.
                Outcome::Conflict { .. } if req.expected_version.is_none() => continue,
                Outcome::Conflict { .. } => {
                    return Err(conflict(
                        &key,
                        self.store.get(&key).map_err(internal)?.as_ref(),
                    ));
                }
                other => return Err(Status::internal(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(Status::aborted(format!(
            "{key} is changing too fast; retry"
        )))
    }
}

/// Refuses a route a host that routes of another namespace have, or that
/// covers or is covered by one of theirs: whoever routes a host gets its
/// requests, its certificate and its ACME orders.
fn check_host(store: &Store, next: &Object) -> Result<(), Status> {
    use legion_api::route::{RouteSpec, hosts_overlap};
    if next.kind != Kind::Route {
        return Ok(());
    }
    let spec: RouteSpec = legion_api::parse(&next.spec).map_err(invalid)?;
    let routes = store.list(Kind::Route, None).map_err(internal)?.objects;
    for r in routes {
        if r.metadata.namespace == next.metadata.namespace {
            continue;
        }
        let Ok(theirs) = legion_api::parse::<RouteSpec>(&r.spec) else {
            continue;
        };
        if hosts_overlap(&spec.host, &theirs.host) {
            return Err(Status::failed_precondition(format!(
                "host {}: namespace {} routes {}, and a host belongs to one namespace",
                spec.host, r.metadata.namespace, theirs.host
            )));
        }
    }
    Ok(())
}

/// Refuses a change that would call off a Volume's move past its
/// switch-over (`crate::activity::past_switch_over`): the Volume asked to
/// be elsewhere, or the node it moves off for a drain made active or
/// paused. The controller would carry the move through regardless, its
/// tasks having stopped for the last part, so the change is refused rather
/// than taken and not done.
fn check_switch_over(store: &Store, cur: &Object, next: &Object) -> Result<(), Status> {
    let volumes = || {
        store
            .list(Kind::Volume, None)
            .map(|l| l.objects)
            .map_err(|e| e.to_string())
    };
    match crate::activity::past_switch_over(volumes, cur, next) {
        Ok(None) => Ok(()),
        Ok(Some(why)) => Err(Status::failed_precondition(why)),
        Err(e) => Err(Status::internal(e)),
    }
}

/// Refuses a Volume naming storage one node at a time uses (an iSCSI LUN,
/// a plugin's volume) that another Volume, in any namespace, names already:
/// claims hold a Volume, and two of them could otherwise be held by two
/// nodes mounting the one LUN. The other is not named: it may be in a
/// namespace the caller cannot read. Claims check the same on their own.
fn check_alias(store: &Store, next: &Object) -> Result<(), Status> {
    use legion_api::volume::VolumeSpec;
    if next.kind != Kind::Volume {
        return Ok(());
    }
    let spec: VolumeSpec = legion_api::parse(&next.spec).map_err(invalid)?;
    let Some(storage) = spec.exclusive_storage() else {
        return Ok(());
    };
    let key = next.key();
    for other in store.list(Kind::Volume, None).map_err(internal)?.objects {
        if other.key() == key {
            continue;
        }
        let Ok(theirs) = legion_api::parse::<VolumeSpec>(&other.spec) else {
            continue;
        };
        if theirs.exclusive_storage().as_deref() == Some(storage.as_str()) {
            return Err(Status::already_exists(format!(
                "{key}: another Volume names this storage already, and it is used by one \
                 node at a time; use that Volume"
            )));
        }
    }
    Ok(())
}

/// Refuses a Volume another storage while it is in use: a node holds it
/// (its dataset, or the LUN or plugin volume it has attached) or a task
/// that has not ended mounts it. Agents keep what they mounted, so the
/// change would not reach them.
fn check_storage(store: &Store, cur: &Object, next: &Object) -> Result<(), Status> {
    use legion_api::volume::{VolumeSpec, VolumeStatus};
    if cur.kind != Kind::Volume {
        return Ok(());
    }
    let (before, after): (VolumeSpec, VolumeSpec) = (
        legion_api::parse(&cur.spec).map_err(internal)?,
        legion_api::parse(&next.spec).map_err(invalid)?,
    );
    if before.same_storage(&after) {
        return Ok(());
    }
    let key = cur.key();
    let refuse = |why: String| {
        Err(Status::failed_precondition(format!(
            "{key} {why}: its driver and source change only once nothing uses it"
        )))
    };
    let status: VolumeStatus = serde_json::from_value(cur.status.clone()).unwrap_or_default();
    if !status.node.is_empty() {
        return refuse(format!("is held by node {}", status.node));
    }
    if let Some(task) = mounted_by(store, cur, false)? {
        return refuse(format!("is mounted by task {task}"));
    }
    Ok(())
}

/// A task that has not ended and mounts the Volume `cur`, if any. With
/// `placed`, only one on a node of the cluster counts: one on no node yet
/// mounts nothing (a lost Volume's waits so), nor does one on a node
/// removed, which is heard no more; so such a Volume may be deleted.
fn mounted_by(store: &Store, cur: &Object, placed: bool) -> Result<Option<String>, Status> {
    use legion_api::task::{TaskSpec, TaskStatus};
    let tasks = store
        .list(Kind::Task, Some(&cur.metadata.namespace))
        .map_err(internal)?;
    let nodes: std::collections::BTreeSet<String> = store
        .list(Kind::Node, None)
        .map_err(internal)?
        .objects
        .into_iter()
        .map(|n| n.metadata.name)
        .collect();
    for t in &tasks.objects {
        let (Ok(spec), Ok(state)) = (
            serde_json::from_value::<TaskSpec>(t.spec.clone()),
            serde_json::from_value::<TaskStatus>(t.status.clone()),
        ) else {
            continue;
        };
        if !state.state.is_terminal()
            && spec.uses_volume(&cur.metadata.name)
            && (!placed || nodes.contains(&spec.node))
        {
            return Ok(Some(t.metadata.name.clone()));
        }
    }
    Ok(None)
}

/// An `ImagePolicy`'s keys must each read as a P-256 public key: admission
/// checks only that they are PEM, as the key reader lives in legion-image.
fn check_policy_keys(policy: &Object) -> Result<(), Status> {
    let spec: legion_api::image::ImagePolicySpec =
        legion_api::parse(&policy.spec).map_err(internal)?;
    for (i, k) in spec.keys.iter().enumerate() {
        if let Err(e) = legion_image::cosign::PublicKey::from_pem(k) {
            return Err(Status::invalid_argument(format!(
                "ImagePolicy {}: spec.keys[{i}]: {e}",
                policy.metadata.name
            )));
        }
    }
    Ok(())
}

/// Refuses to delete a Volume in use, and returns the version checked, for
/// the delete to be made on: a task that has not ended mounts it, or a
/// node still in the cluster holds the LUN or plugin volume it attached.
/// Deleted then, it would come back unheld, for another node to attach
/// while the first still writes to it. (A ZFS volume's node keeps its
/// dataset whatever its tasks do, and the Volume may go once they have.)
fn check_volume_delete(store: &Store, key: &ObjectKey) -> Result<Option<u64>, Status> {
    use legion_api::volume::{Access, VolumeSpec, VolumeStatus};
    let Some(cur) = store.get(key).map_err(internal)? else {
        return Ok(None);
    };
    let refuse = |why: String| {
        Err(Status::failed_precondition(format!(
            "{key} {why}: it is deleted only once nothing uses it"
        )))
    };
    if let Some(task) = mounted_by(store, &cur, true)? {
        return refuse(format!("is mounted by task {task}"));
    }
    let spec: VolumeSpec = legion_api::parse(&cur.spec).map_err(internal)?;
    let status: VolumeStatus = serde_json::from_value(cur.status.clone()).unwrap_or_default();
    // Its tasks stopped for the last part of a move, which goes through.
    if let Some(m) = status
        .migration
        .as_ref()
        .filter(|m| m.phase == legion_api::volume::MigrationPhase::Finishing)
    {
        return Err(Status::failed_precondition(format!(
            "{key} is past its switch-over from {} to {}: it is deleted once its move is done",
            status.node, m.to
        )));
    }
    if spec.access() == Access::Single
        && !status.node.is_empty()
        && store
            .get(&ObjectKey::new(Kind::Node, CLUSTER_SCOPE, &status.node))
            .map_err(internal)?
            .is_some()
    {
        return refuse(format!(
            "is held by node {}, which lets go of it once its tasks have ended (or remove the node)",
            status.node
        ));
    }
    Ok(Some(cur.metadata.version))
}

fn default_namespace(kind: Kind, namespace: &mut String) {
    if kind.is_cluster_scoped() {
        if namespace.is_empty() {
            *namespace = CLUSTER_SCOPE.to_owned();
        }
    } else if namespace.is_empty() {
        *namespace = "default".to_owned();
    }
}

fn parse_ref(r: Option<ObjectRef>) -> Result<ObjectKey, Status> {
    let r = r.ok_or_else(|| Status::invalid_argument("ref is required"))?;
    let mut key = ObjectKey::try_from(r).map_err(invalid)?;
    default_namespace(key.kind, &mut key.namespace);
    key.validate().map_err(invalid)?;
    Ok(key)
}

fn parse_kind(kind: &str) -> Result<Kind, Status> {
    kind.parse().map_err(invalid)
}

fn invalid(e: impl std::fmt::Display) -> Status {
    Status::invalid_argument(e.to_string())
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn not_found(key: &ObjectKey) -> Status {
    Status::not_found(format!("{key} not found"))
}

fn conflict(key: &ObjectKey, current: Option<&Object>) -> Status {
    match current {
        Some(o) => Status::aborted(format!("{key} is at version {}", o.metadata.version)),
        None => Status::aborted(format!("{key} does not exist")),
    }
}

/// Single sign-on's settings as the API gives them: never the secret.
fn oidc_response(r: Option<crate::console::OidcRecord>) -> pb::ConsoleOidcResponse {
    match r {
        Some(r) => pb::ConsoleOidcResponse {
            oidc_json: serde_json::to_vec(&r.oidc).expect("serialises"),
            has_secret: !r.client_secret.is_empty(),
        },
        None => pb::ConsoleOidcResponse::default(),
    }
}

/// Whether a usage request asks for the system namespace's too, which
/// only the cluster admin and managers may.
fn system_usage(req: &Request<pb::UsageRequest>, caller: &Caller) -> Result<bool, Status> {
    let system = req.get_ref().system;
    if system {
        caller.require_all("read the system namespace's usage")?;
    }
    Ok(system)
}

pub(crate) fn forwarded<T>(inner: T) -> Request<T> {
    let mut req = Request::new(inner);
    req.metadata_mut()
        .insert(FORWARDED, tonic::metadata::MetadataValue::from_static("1"));
    req
}

/// [`forwarded`], for `caller`: a user's name goes with it.
pub(crate) fn forwarded_for<T>(inner: T, caller: &Caller) -> Result<Request<T>, Status> {
    let mut req = forwarded(inner);
    if let Caller::User(name) = caller {
        let name = tonic::metadata::MetadataValue::try_from(name.as_str())
            .map_err(|_| Status::invalid_argument("a user name is not a header value"))?;
        req.metadata_mut().insert(crate::authz::FORWARDED_FOR, name);
    }
    Ok(req)
}

impl ApiService {
    /// What to audit of a request recorded where it first arrives (its
    /// caller's name is not forwarded): nothing on a manager it was
    /// forwarded to, which the first records.
    pub(crate) fn audit_point<T>(
        &self,
        req: &Request<T>,
        action: &'static str,
        target: legion_api::audit::Target,
    ) -> Option<(
        legion_api::audit::Actor,
        &'static str,
        legion_api::audit::Target,
    )> {
        if req.metadata().contains_key(FORWARDED) {
            return None;
        }
        let actor = crate::audit::actor(req, self.peer_tls.is_some());
        Some((actor, action, target))
    }

    /// Records `result` at `point`, if any, and passes it on.
    pub(crate) fn audit_end<T>(
        &self,
        point: Option<(
            legion_api::audit::Actor,
            &'static str,
            legion_api::audit::Target,
        )>,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        match point {
            Some((actor, action, target)) => self.audited(actor, action, target, result),
            None => result,
        }
    }

    /// Records `result` in the audit trail and passes it on.
    pub(crate) fn audited<T>(
        &self,
        actor: legion_api::audit::Actor,
        action: &str,
        target: legion_api::audit::Target,
        result: Result<T, Status>,
    ) -> Result<T, Status> {
        self.audit.record(actor, action, target, &result);
        result
    }

    /// Refuses any caller but a manager (or anyone, without TLS: tests).
    fn only_managers<T>(&self, req: &Request<T>, what: &str) -> Result<(), Status> {
        self.caller(req)?;
        match crate::tls::caller(req, self.peer_tls.is_some())? {
            Some(id) if id.role != Role::Manager => Err(Status::permission_denied(format!(
                "{id} may not {what}: managers only"
            ))),
            _ => Ok(()),
        }
    }

    pub(crate) fn caller<T>(&self, req: &Request<T>) -> Result<Caller, Status> {
        let tls = self.peer_tls.is_some();
        // A removed manager's certificate is refused, and so is what it
        // forwards (PLAN.md phase 7); workers never reach the API.
        if let Some(id) = crate::tls::caller(req, tls)?.filter(|id| id.role == Role::Manager)
            && let Some(issued) = crate::tls::caller_issued(req, tls)?
        {
            crate::removed::check(&self.store, &id.name, issued)?;
        }
        Caller::of(req, tls)
    }

    /// Deleting a Task that has not ended asks it to stop instead: its
    /// pod may still run, and its owner replaces it only once the agent
    /// has torn it down (a stateful task's volumes are then free). The
    /// stopped task is then deleted as any other, or with its owner.
    /// Returns the version written, or `None` for a task to delete now.
    /// Deletes a Node, recording first that the node is removed: its
    /// certificate is refused from then, so it cannot come back between
    /// the two writes. Only once the delete would go through, not for a
    /// stale version; if it then does not (the Node changed meanwhile, or
    /// the write failed and the Node is still there), the node is let back
    /// in.
    async fn remove_node(
        &self,
        key: &ObjectKey,
        expected_version: Option<u64>,
    ) -> Result<Outcome, Status> {
        let precondition = expected_version.map_or(Precondition::None, Precondition::Version);
        let Some(node) = self.store.get(key).map_err(internal)? else {
            return self
                .remove_manager_without_node(&key.name, expected_version)
                .await;
        };
        if expected_version.is_some_and(|v| v != node.metadata.version) {
            return Ok(Outcome::Conflict {
                current: Some(node.metadata.version),
            });
        }
        let recorded = crate::removed::record(self, &key.name).await?;
        // A manager leaves the Raft membership too, before its Node goes:
        // it no longer votes, and its calls are refused (PLAN.md phase 7).
        if let Err(e) = self.leave_membership(&key.name).await {
            crate::removed::forget(self, &key.name, recorded).await?;
            return Err(e);
        }
        let deleted = self
            .write(Command::Delete {
                key: key.clone(),
                precondition,
            })
            .await;
        // Not when the Node is gone: this delete or another took it, and
        // the removal stands.
        let stays = match &deleted {
            Ok(Outcome::Conflict { current: Some(_) }) => true,
            Ok(_) => false,
            // The delete may have gone through before the error: only what
            // the store holds once current says.
            Err(_) => {
                self.consistent().await?;
                self.store.get(key).map_err(internal)?.is_some()
            }
        };
        if stays {
            crate::removed::forget(self, &key.name, recorded).await?;
        } else if let Err(e) = crate::volumes::mark_lost(self, &key.name).await {
            // A node joining under the name marks them before it is let in.
            tracing::warn!(node = %key.name, "marking its Volumes lost: {e}");
        }
        deleted
    }

    /// Removes the manager named `name` that has no Node, as it runs no
    /// agent (a witness, or a manager started with `--no-agent`): out of
    /// the membership, its certificate refused, as a manager's Node
    /// deleted does. Such a manager lost for good (its machine gone) could
    /// not be removed before, so it kept counting toward the quorum without
    /// ever answering (found 2026-11-27, a witness's VPS destroyed). Not
    /// found when `name` is no manager, or a version was asked for: there is
    /// no object to have one.
    async fn remove_manager_without_node(
        &self,
        name: &str,
        expected_version: Option<u64>,
    ) -> Result<Outcome, Status> {
        let member = crate::managers::id_of(&self.store, name)?.is_some_and(|id| {
            let m = self.store.raft.metrics().borrow().clone();
            m.membership_config
                .membership()
                .nodes()
                .any(|(n, _)| *n == id)
        });
        if expected_version.is_some() || !member {
            return Ok(Outcome::NotFound);
        }
        let recorded = crate::removed::record(self, name).await?;
        if let Err(e) = self.leave_membership(name).await {
            crate::removed::forget(self, name, recorded).await?;
            return Err(e);
        }
        Ok(Outcome::Deleted { version: 0 })
    }

    /// Takes the manager named `name` out of the Raft membership, if it is
    /// a member: never the last voter, never the last that may lead (the
    /// others witnesses, which never stand), never this leader itself (it
    /// steps down once out, before it could write anything after: it hands
    /// leadership over first). One at a time, so that two removals cannot
    /// each leave the other's manager the last (found by an audit).
    pub(crate) async fn leave_membership(&self, name: &str) -> Result<(), Status> {
        use openraft::ChangeMembers;
        let _one = self.leaving.lock().await;
        let Some(id) = crate::managers::id_of(&self.store, name)? else {
            return Ok(());
        };
        let m = self.store.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let voters: std::collections::BTreeSet<NodeId> = membership.voter_ids().collect();
        let change = if voters.contains(&id) {
            let witnesses = crate::managers::witnesses(&self.store)?;
            leaving_voter(name, id, m.id, &voters, &witnesses)?;
            ChangeMembers::RemoveVoters([id].into())
        } else if membership.nodes().any(|(n, _)| *n == id) {
            ChangeMembers::RemoveNodes([id].into())
        } else {
            return Ok(());
        };
        tracing::info!(manager = name, id, "removing a manager from the membership");
        if self.store.hushed() {
            return Err(Status::unavailable(
                "leadership is being handed to another manager; retry in a few seconds",
            ));
        }
        let changed = self.store.raft.change_membership(change, false);
        tokio::time::timeout(crate::COMMIT_TIMEOUT, changed)
            .await
            .map_err(|_| crate::no_quorum())?
            .map_err(|e| Status::unavailable(format!("removing manager {name}: {e}")))?;
        Ok(())
    }

    async fn stop_task(
        &self,
        key: &ObjectKey,
        expected: Option<u64>,
    ) -> Result<Option<u64>, Status> {
        use legion_api::task::{DesiredState, TaskSpec, TaskStatus};
        for _ in 0..APPLY_ATTEMPTS {
            let Some(mut task) = self.store.get(key).map_err(internal)? else {
                return Ok(None);
            };
            if expected.is_some_and(|v| v != task.metadata.version) {
                return Err(conflict(key, Some(&task)));
            }
            let status: TaskStatus =
                serde_json::from_value(task.status.clone()).unwrap_or_default();
            if status.state.is_terminal() {
                return Ok(None);
            }
            let mut spec: TaskSpec = serde_json::from_value(task.spec.clone()).map_err(internal)?;
            if spec.desired_state == DesiredState::Shutdown {
                return Ok(Some(task.metadata.version));
            }
            spec.desired_state = DesiredState::Shutdown;
            task.spec = serde_json::to_value(&spec).map_err(internal)?;
            let precondition = Precondition::Version(task.metadata.version);
            match self
                .write(Command::Put {
                    object: task,
                    precondition,
                })
                .await?
            {
                Outcome::Written(o) => return Ok(Some(o.metadata.version)),
                Outcome::Conflict { .. } if expected.is_none() => continue,
                Outcome::Conflict { .. } => {
                    return Err(conflict(
                        key,
                        self.store.get(key).map_err(internal)?.as_ref(),
                    ));
                }
                other => return Err(Status::internal(format!("unexpected outcome {other:?}"))),
            }
        }
        Err(Status::aborted(format!(
            "{key} is changing too fast; retry"
        )))
    }

    async fn list_local(&self, req: ListRequest) -> Result<ListResponse, Status> {
        let kind = parse_kind(&req.kind)?;
        check_namespace(&req.namespace)?;
        let namespace = (!req.namespace.is_empty()).then_some(req.namespace.as_str());
        let listing = self.store.list(kind, namespace).map_err(internal)?;
        let selector: BTreeMap<_, _> = req.match_labels.into_iter().collect();
        let objects = listing
            .objects
            .iter()
            .filter(|o| o.metadata.namespace != SYSTEM_NAMESPACE)
            .filter(|o| {
                selector
                    .iter()
                    .all(|(k, v)| o.metadata.labels.get(k) == Some(v))
            })
            .map(pb::Object::from)
            .collect();
        Ok(ListResponse {
            objects,
            version: listing.version,
        })
    }
}

#[tonic::async_trait]
impl Objects for ApiService {
    async fn apply(&self, req: Request<ApplyRequest>) -> Result<Response<ApplyResponse>, Status> {
        // Audited where it first arrives, which knows the caller, with
        // the answer the leader gave if it went there.
        let target = req
            .get_ref()
            .object
            .as_ref()
            .map(|o| crate::audit::target(&o.kind, &o.namespace, &o.name))
            .unwrap_or_default();
        let audit = self.audit_point(&req, "apply", target);
        let applied = async {
            // A user's rights are checked where the request is served: a
            // follower forwards them, its own copy may lag a new
            // RoleBinding.
            let caller = self.caller(&req)?;
            if let Route::Leader(mut c, _) = self.route(&req)? {
                return c.apply(forwarded_for(req.into_inner(), &caller)?).await;
            }
            // The rights it checks, what it compares with, and the quotas
            // and policies, must be the latest, not what this replica has
            // applied.
            self.consistent().await?;
            if let Some(o) = &req.get_ref().object {
                let kind = parse_kind(&o.kind)?;
                let mut namespace = o.namespace.clone();
                default_namespace(kind, &mut namespace);
                caller.check(&self.store, Verb::Write, kind, &namespace)?;
            }
            Ok(Response::new(self.apply_local(req.into_inner()).await?))
        }
        .await;
        self.audit_end(audit, applied)
    }

    async fn get(&self, req: Request<ObjectRef>) -> Result<Response<pb::Object>, Status> {
        check_namespace(&req.get_ref().namespace)?;
        let key = parse_ref(Some(req.get_ref().clone()))?;
        let caller = self.caller(&req)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.get(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Read, key.kind, &key.namespace)?;
        let key = parse_ref(Some(req.into_inner()))?;
        match self.store.get(&key).map_err(internal)? {
            Some(o) => Ok(Response::new((&o).into())),
            None => Err(not_found(&key)),
        }
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let caller = self.caller(&req)?;
        let kind = parse_kind(&req.get_ref().kind)?;
        if let Route::Leader(mut c, _) = self.route(&req)? {
            return c.list(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        if !req.get_ref().namespace.is_empty() || kind.is_cluster_scoped() {
            let mut namespace = req.get_ref().namespace.clone();
            default_namespace(kind, &mut namespace);
            caller.check(&self.store, Verb::Read, kind, &namespace)?;
        }
        let mut listed = Response::new(self.list_local(req.into_inner()).await?);
        // Across namespaces, a user sees those they may read in.
        if caller != Caller::All {
            listed
                .get_mut()
                .objects
                .retain(|o| caller.may(&self.store, Verb::Read, kind, &o.namespace));
        }
        Ok(listed)
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let key = parse_ref(req.get_ref().r#ref.clone())?;
        // Audited where it first arrives, as an apply is.
        let target = crate::audit::target(key.kind.as_str(), &key.namespace, &key.name);
        let audit = self.audit_point(&req, "delete", target);
        let deleted = async {
            let caller = self.caller(&req)?;
            if let Route::Leader(mut c, _) = self.route(&req)? {
                return c.delete(forwarded_for(req.into_inner(), &caller)?).await;
            }
            self.delete_served(&caller, key, req).await
        }
        .await;
        self.audit_end(audit, deleted)
    }

    type WatchStream = ReceiverStream<Result<pb::WatchEvent, Status>>;

    async fn watch(
        &self,
        req: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let caller = self.caller(&req)?;
        // Rights are checked against this replica's store, as each event
        // goes out: first brought up to what the leader has committed, so
        // a RoleBinding revoked there is revoked here before any of the
        // history is served. From then on the store is applied in order,
        // so no event comes out ahead of a revocation committed before it.
        self.consistent().await?;
        let req = req.into_inner();
        let kind = if req.kind.is_empty() {
            None
        } else {
            Some(parse_kind(&req.kind)?)
        };
        if let Some(k) = kind.filter(|k| k.is_cluster_scoped()) {
            caller.check(&self.store, Verb::Read, k, "")?;
        }
        let store = self.store.clone();
        let mut watcher = self
            .store
            .watch(req.from_version, kind)
            .map_err(|e| Status::aborted(e.to_string()))?;
        // A user's rights as the watch opens, raised as they grow: once a
        // role it ran with is lowered or taken, the watch ends saying so,
        // rather than leave out what the user may no longer see, which the
        // client could not tell from nothing happening. Checked as each
        // event goes out and every few seconds.
        let mut rights = caller.roles(&self.store)?;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut closing = self.closing.clone();
        tokio::spawn(async move {
            let mut lowered = |store: &Store| -> Option<Status> {
                let (Caller::User(user), Some(before)) = (&caller, &mut rights) else {
                    return None;
                };
                let now = match caller.roles(store) {
                    Ok(now) => now.unwrap_or_default(),
                    Err(e) => return Some(e),
                };
                if let Some(why) = crate::authz::lowered(user, before, &now) {
                    return Some(why);
                }
                *before = now;
                None
            };
            loop {
                // `Some(None)`: time to check the rights again.
                let next = tokio::select! {
                    next = watcher.next() => Some(Some(next)),
                    _ = closing.wait_for(|c| *c) => None,
                    // Gone, though nothing it may see has happened since.
                    () = tx.closed() => return,
                    () = tokio::time::sleep(crate::authz::REVOCATION_CHECK) => Some(None),
                };
                if let Some(why) = lowered(&store) {
                    let _ = tx.send(Err(why)).await;
                    return;
                }
                let next = match next {
                    Some(Some(next)) => Some(next),
                    Some(None) => continue,
                    None => None,
                };
                // A client too slow to take this ends with the stream.
                let Some(next) = next else {
                    let _ = tx.try_send(Err(Status::unavailable("manager shutting down")));
                    return;
                };
                let item = match next {
                    Ok(Event::Put(o) | Event::Delete(o))
                        if o.metadata.namespace == SYSTEM_NAMESPACE
                            || !caller.may(&store, Verb::Read, o.kind, &o.metadata.namespace) =>
                    {
                        continue;
                    }
                    Ok(Event::Put(o)) => Ok(pb::WatchEvent {
                        r#type: watch_event::Type::Put.into(),
                        object: Some((&o).into()),
                    }),
                    Ok(Event::Delete(o)) => Ok(pb::WatchEvent {
                        r#type: watch_event::Type::Delete.into(),
                        object: Some((&o).into()),
                    }),
                    Ok(Event::Reset(v)) => Err(Status::aborted(WatchError::Reset(v).to_string())),
                    Err(WatchError::Closed) => return,
                    Err(e) => Err(Status::aborted(e.to_string())),
                };
                let last = item.is_err();
                // The client went away, or stopped reading as the manager
                // shuts down: either way the stream ends.
                let sent = tokio::select! {
                    sent = tx.send(item) => sent.is_ok(),
                    _ = closing.wait_for(|c| *c) => false,
                };
                if !sent || last {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

impl ApiService {
    /// A delete this manager, the leader, serves: see `Objects::delete`.
    async fn delete_served(
        &self,
        caller: &Caller,
        key: ObjectKey,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.consistent().await?;
        caller.check(&self.store, Verb::Write, key.kind, &key.namespace)?;
        let req = req.into_inner();
        let key = parse_ref(req.r#ref)?;
        check_namespace(&key.namespace)?;
        if key.kind == Kind::Task
            && let Some(version) = self.stop_task(&key, req.expected_version).await?
        {
            return Ok(Response::new(DeleteResponse { version }));
        }
        let mut attempts = APPLY_ATTEMPTS;
        let deleted = loop {
            let mut precondition = req
                .expected_version
                .map_or(Precondition::None, Precondition::Version);
            // Deleted on the version checked: a node claiming it meanwhile
            // changes the version. The check is then made again, which
            // refuses it, as a caller who named no version asked for nothing
            // that changing the Volume made untrue.
            if key.kind == Kind::Volume
                && let Some(checked) = check_volume_delete(&self.store, &key)?
                && req.expected_version.is_none_or(|v| v == checked)
            {
                precondition = Precondition::Version(checked);
            }
            #[cfg(feature = "test-hooks")]
            crate::hooks::pause("volume-delete", &key.name).await;
            let deleted = if key.kind == Kind::Node {
                // Run to the end even if the caller goes: stopped between its
                // writes, it would leave a live node refused.
                let api = self.clone();
                let key = key.clone();
                tokio::spawn(async move { api.remove_node(&key, req.expected_version).await })
                    .await
                    .map_err(internal)??
            } else {
                self.write(Command::Delete {
                    key: key.clone(),
                    precondition,
                })
                .await?
            };
            attempts -= 1;
            let again = matches!(deleted, Outcome::Conflict { .. })
                && key.kind == Kind::Volume
                && req.expected_version.is_none();
            if !again || attempts == 0 {
                break deleted;
            }
        };
        match deleted {
            Outcome::Deleted { version } => Ok(Response::new(DeleteResponse { version })),
            Outcome::NotFound => Err(not_found(&key)),
            Outcome::Conflict { .. } => Err(conflict(
                &key,
                self.store.get(&key).map_err(internal)?.as_ref(),
            )),
            other => Err(Status::internal(format!("unexpected outcome {other:?}"))),
        }
    }
}

#[tonic::async_trait]
impl Cluster for ApiService {
    async fn init(&self, req: Request<InitRequest>) -> Result<Response<StatusResponse>, Status> {
        let audit = self.audit_point(&req, "init", Default::default());
        let result = async {
            self.caller(&req)?.require_all("initialise the cluster")?;
            if self.witness {
                return Err(Status::failed_precondition(
                    "a witness never leads: initialise the cluster on a full manager, then join this one with --witness",
                ));
            }
            let raft = &self.store.raft;
            if raft.is_initialized().await.map_err(internal)? {
                return Err(Status::already_exists(
                    "this manager already belongs to a cluster",
                ));
            }
            let members = BTreeMap::from([(
                self.id,
                BasicNode {
                    addr: self.advertise.clone(),
                },
            )]);
            raft.initialize(members).await.map_err(internal)?;
            raft.wait(Some(std::time::Duration::from_secs(10)))
                .current_leader(self.id, "become leader of the new cluster")
                .await
                .map_err(internal)?;
            Ok(Response::new(self.status()?))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn status(
        &self,
        _req: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(self.status()?))
    }

    async fn join_token(
        &self,
        req: Request<pb::JoinTokenRequest>,
    ) -> Result<Response<pb::JoinTokenResponse>, Status> {
        let audit = self.audit_point(
            &req,
            "join-token",
            crate::audit::target("JoinToken", "", &req.get_ref().role),
        );
        let result = async {
            self.caller(&req)?.require_all("see join tokens")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.join_token(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            let req = req.into_inner();
            let role = Role::parse(&req.role)
                .filter(|r| matches!(r, Role::Manager | Role::Worker))
                .ok_or_else(|| Status::invalid_argument("the role is `manager` or `worker`"))?;
            let token = self.issuer()?.token(role, req.rotate).await?;
            Ok(Response::new(pb::JoinTokenResponse {
                token: token.to_string(),
            }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn rotate_ca(
        &self,
        req: Request<pb::RotateCaRequest>,
    ) -> Result<Response<pb::RotateCaResponse>, Status> {
        let start = req.get_ref().start;
        let audit = self.audit_point(
            &req,
            if start { "ca-rotate" } else { "ca-status" },
            Default::default(),
        );
        let result = async {
            self.caller(&req)?.require_all("rotate the cluster CA")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.rotate_ca(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            if start {
                crate::ca::start(self).await?;
            }
            Ok(Response::new(crate::ca::status(self).await?))
        }
        .await;
        // Asking only reads: not a change to audit.
        if start {
            self.audit_end(audit, result)
        } else {
            result
        }
    }

    async fn change_role(
        &self,
        req: Request<pb::ChangeRoleRequest>,
    ) -> Result<Response<pb::ChangeRoleResponse>, Status> {
        let audit = self.audit_point(
            &req,
            "change-role",
            crate::audit::target("Node", "", &req.get_ref().node),
        );
        let result = async {
            self.caller(&req)?.require_all("change a node's role")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.change_role(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            let req = req.into_inner();
            crate::roles::request(self, &req.node, &req.role).await?;
            Ok(Response::new(pb::ChangeRoleResponse {}))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn lead(
        &self,
        req: Request<pb::LeadRequest>,
    ) -> Result<Response<pb::LeadResponse>, Status> {
        let audit = self.audit_point(
            &req,
            "lead",
            crate::audit::target("Node", "", &req.get_ref().node),
        );
        let result = async {
            self.caller(&req)?.require_all("hand leadership on")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.lead(forwarded(req.into_inner())).await;
            }
            let node = req.into_inner().node;
            if node == self.name {
                return Err(Status::already_exists(format!("{node} leads already")));
            }
            let id = crate::managers::id_of(&self.store, &node)?.ok_or_else(|| {
                Status::not_found(format!(
                    "no manager {node} (a worker, or not known by name yet)"
                ))
            })?;
            if crate::managers::witnesses(&self.store)?.contains(&id) {
                return Err(Status::failed_precondition(format!(
                    "{node} is a witness: it never leads"
                )));
            }
            let addr = {
                let m = self.store.raft.metrics().borrow().clone();
                let membership = m.membership_config.membership();
                if !membership.voter_ids().any(|v| v == id) {
                    return Err(Status::failed_precondition(format!(
                        "{node} does not vote yet: it can't be elected"
                    )));
                }
                membership
                    .nodes()
                    .find(|(n, _)| **n == id)
                    .map(|(_, n)| n.addr.clone())
                    .unwrap_or_default()
            };
            let took = crate::lead::hand_over(self, id, &addr).await?;
            Ok(Response::new(pb::LeadResponse {
                leader: node,
                took_ms: took.as_millis() as u64,
            }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn campaign(
        &self,
        req: Request<pb::CampaignRequest>,
    ) -> Result<Response<pb::CampaignResponse>, Status> {
        // The leader handing leadership on, by its manager's certificate:
        // not one removed since, and the leader this manager follows
        // (found by an audit: any manager's certificate did).
        let tls = self.peer_tls.is_some();
        match crate::tls::caller(&req, tls)? {
            Some(id) if id.role == Role::Manager => {
                if let Some(issued) = crate::tls::caller_issued(&req, tls)? {
                    crate::removed::check(&self.store, &id.name, issued)?;
                }
                let leader = self.store.raft.metrics().borrow().current_leader;
                let names = crate::managers::names(&self.store)?;
                if let Some(leading) = leader.and_then(|l| names.get(&l))
                    && *leading != id.name
                {
                    return Err(Status::permission_denied(format!(
                        "only the leader asks another to stand: {leading} leads, not {}",
                        id.name
                    )));
                }
            }
            _ => {
                return Err(Status::permission_denied(
                    "only a manager asks another to stand",
                ));
            }
        }
        if self.witness {
            return Err(Status::failed_precondition(
                "a witness never stands for leader",
            ));
        }
        self.store
            .raft
            .trigger()
            .elect()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(Response::new(pb::CampaignResponse {}))
    }

    async fn demote(
        &self,
        req: Request<pb::DemoteRequest>,
    ) -> Result<Response<pb::DemoteResponse>, Status> {
        // Its own demotion, by the manager's certificate: only the leader
        // answers, as one forwarding would call with its own.
        let id = match crate::tls::caller(&req, self.peer_tls.is_some())? {
            Some(id) if id.role == Role::Manager => id,
            _ => {
                return Err(Status::permission_denied(
                    "only a manager asks to be demoted",
                ));
            }
        };
        let m = self.store.raft.metrics().borrow().clone();
        if m.current_leader != Some(self.id) {
            return Err(Status::unavailable("not the leader; ask the leader"));
        }
        self.consistent().await?;
        if let Some(issued) = crate::tls::caller_issued(&req, self.peer_tls.is_some())? {
            crate::removed::check(&self.store, &id.name, issued)?;
        }
        let r = crate::roles::demote(self, &id.name, req.into_inner()).await?;
        Ok(Response::new(r))
    }

    async fn test_alert_receiver(
        &self,
        req: Request<pb::TestAlertReceiverRequest>,
    ) -> Result<Response<pb::TestAlertReceiverResponse>, Status> {
        let caller = self.caller(&req)?;
        let name = req.into_inner().name;
        caller.check(&self.store, Verb::Write, Kind::AlertReceiver, "")?;
        crate::notify::test(&self.store, &name)
            .await
            .map_err(|e| Status::failed_precondition(format!("AlertReceiver {name}: {e}")))?;
        Ok(Response::new(pb::TestAlertReceiverResponse {}))
    }

    async fn autolock(
        &self,
        req: Request<pb::AutolockRequest>,
    ) -> Result<Response<pb::AutolockResponse>, Status> {
        let audit = self.audit_point(&req, "autolock", Default::default());
        let result = async {
            self.caller(&req)?.require_all("change autolock")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.autolock(forwarded(req.into_inner())).await;
            }
            self.consistent().await?;
            let key = crate::autolock::apply(self, &req.into_inner().action).await?;
            Ok(Response::new(pb::AutolockResponse {
                key: key
                    .as_ref()
                    .map(crate::autolock::encode)
                    .unwrap_or_default(),
            }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn reach(
        &self,
        req: Request<pb::ReachRequest>,
    ) -> Result<Response<pb::ReachResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.into_inner().namespace;
        if namespace.is_empty() {
            return Err(Status::invalid_argument("which namespace?"));
        }
        check_namespace(&namespace)?;
        caller.check(&self.store, Verb::Read, Kind::Service, &namespace)?;
        // Built as the dispatcher builds what the nodes' guards take.
        let all = |kind| -> Result<BTreeMap<ObjectKey, Object>, Status> {
            Ok(self
                .store
                .list(kind, None)
                .map_err(internal)?
                .objects
                .into_iter()
                .map(|o| (o.key(), o))
                .collect())
        };
        let policies =
            crate::dispatcher::policies(&all(Kind::Namespace)?, &all(Kind::NetworkPolicy)?);
        let endpoints = crate::dispatcher::endpoints(
            &all(Kind::Service)?,
            &all(Kind::Task)?,
            &all(Kind::Node)?,
        );
        let mut services: Vec<String> = endpoints
            .iter()
            .filter(|e| e.namespace == namespace && !e.job)
            .map(|e| e.name.clone())
            .collect();
        services.sort();
        let edges = legion_api::policy::reach(&policies, &endpoints, &namespace)
            .into_iter()
            .map(|r| pb::ReachEdge {
                from: r.from.unwrap_or_default(),
                to: r.to,
                all_ports: r.all_ports,
                ports: r
                    .ports
                    .into_iter()
                    .map(|(protocol, port)| pb::ReachPort {
                        // As the API spells it: `tcp`, `udp`.
                        protocol: serde_json::to_value(protocol)
                            .ok()
                            .and_then(|v| v.as_str().map(str::to_owned))
                            .unwrap_or_default(),
                        port: port.into(),
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(pb::ReachResponse { services, edges }))
    }

    async fn connections(
        &self,
        req: Request<pb::ConnectionsRequest>,
    ) -> Result<Response<pb::ConnectionsResponse>, Status> {
        crate::connections::connections(self, req).await
    }

    async fn availability(
        &self,
        req: Request<pb::AvailabilityRequest>,
    ) -> Result<Response<pb::AvailabilityResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.into_inner().namespace;
        check_namespace(&namespace)?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Service, &namespace)?;
        }
        // Any manager, once caught up with the leader.
        self.consistent().await?;
        let report = self.availability_seen(&caller, &namespace)?;
        Ok(Response::new(pb::AvailabilityResponse {
            report_json: serde_json::to_vec(&report).expect("serialises"),
        }))
    }

    async fn load_balancers(
        &self,
        req: Request<pb::LoadBalancersRequest>,
    ) -> Result<Response<pb::LoadBalancersResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.get_ref().namespace.clone();
        if namespace.is_empty() {
            return Err(Status::invalid_argument("which namespace?"));
        }
        check_namespace(&namespace)?;
        // Only the leader hears heartbeats; it checks the user's rights.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c
                .load_balancers(forwarded_for(req.into_inner(), &caller)?)
                .await;
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Read, Kind::Service, &namespace)?;
        Ok(Response::new(self.balancers_seen(&namespace)?))
    }

    async fn usage(
        &self,
        req: Request<pb::UsageRequest>,
    ) -> Result<Response<pb::UsageResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.get_ref().namespace.clone();
        check_namespace(&namespace)?;
        let system = system_usage(&req, &caller)?;
        // Only the leader hears heartbeats; it checks the user's rights.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.usage(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        Ok(Response::new(self.usage_seen(&caller, &namespace, system)))
    }

    type WatchUsageStream = ReceiverStream<Result<pb::UsageResponse, Status>>;

    async fn watch_usage(
        &self,
        req: Request<pb::UsageRequest>,
    ) -> Result<Response<Self::WatchUsageStream>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.get_ref().namespace.clone();
        check_namespace(&namespace)?;
        let system = system_usage(&req, &caller)?;
        // Only the leader hears heartbeats: followed there, passed on.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let from_leader = c
                .watch_usage(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            return Ok(Response::new(passed_on(from_leader, self.closing.clone())));
        }
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        let wake = Wake {
            leader: true,
            reports: true,
            // A report grows too old to show though none comes.
            every: crate::usage::STEP,
            ..Wake::default()
        };
        Ok(Response::new(self.followed(
            wake,
            |_| None,
            move |api| {
                let (caller, namespace) = (caller.clone(), namespace.clone());
                async move { Ok(api.usage_seen(&caller, &namespace, system)) }
            },
        )))
    }

    type WatchStatusStream = ReceiverStream<Result<StatusResponse, Status>>;

    async fn watch_status(
        &self,
        _req: Request<StatusRequest>,
    ) -> Result<Response<Self::WatchStatusStream>, Status> {
        // This manager's own view, as Status gives it: any caller, any
        // manager, no leader needed.
        let wake = Wake {
            raft: true,
            // What Raft's view does not hold: the CA a rotation moved
            // this manager to, the protocol floor.
            every: std::time::Duration::from_secs(60),
            ..Wake::default()
        };
        Ok(Response::new(self.followed(
            wake,
            |_| None,
            |api| async move { api.status() },
        )))
    }

    type WatchCaStream = ReceiverStream<Result<pb::RotateCaResponse, Status>>;

    async fn watch_ca(
        &self,
        req: Request<pb::WatchCaRequest>,
    ) -> Result<Response<Self::WatchCaStream>, Status> {
        self.caller(&req)?.require_all("follow the cluster CA")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let from_leader = c.watch_ca(forwarded(req.into_inner())).await?.into_inner();
            return Ok(Response::new(passed_on(from_leader, self.closing.clone())));
        }
        self.consistent().await?;
        // Nodes tell their CA in their heartbeats, managers when asked:
        // asked again often while a rotation runs.
        let wake = Wake {
            leader: true,
            reports: true,
            store: true,
            every: std::time::Duration::from_secs(30),
            settle: std::time::Duration::from_secs(1),
            ..Wake::default()
        };
        let sooner = |r: &pb::RotateCaResponse| {
            (!r.previous_digest.is_empty()).then_some(std::time::Duration::from_secs(3))
        };
        Ok(Response::new(self.followed(
            wake,
            sooner,
            |api| async move { crate::ca::status(&api).await },
        )))
    }

    type WatchUsageHistoryStream = ReceiverStream<Result<pb::UsageHistoryResponse, Status>>;

    async fn watch_usage_history(
        &self,
        req: Request<pb::UsageHistoryRequest>,
    ) -> Result<Response<Self::WatchUsageHistoryStream>, Status> {
        let caller = self.caller(&req)?;
        let (namespace, task, range_ms) = history_asked(req.get_ref())?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let from_leader = c
                .watch_usage_history(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            return Ok(Response::new(passed_on(from_leader, self.closing.clone())));
        }
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        // A sample is added each step: looked at again as often, and at
        // least each minute.
        let wake = Wake {
            leader: true,
            every: crate::usage::STEP,
            ..Wake::default()
        };
        let every = |r: &pb::UsageHistoryResponse| {
            Some(std::time::Duration::from_millis(
                r.step_ms
                    .clamp(crate::usage::STEP.as_millis() as i64, 60_000) as u64,
            ))
        };
        Ok(Response::new(self.followed(wake, every, move |api| {
            let (caller, namespace, task) = (caller.clone(), namespace.clone(), task.clone());
            async move { api.history_seen(&caller, &namespace, &task, range_ms).await }
        })))
    }

    type WatchConnectionsStream = ReceiverStream<Result<pb::ConnectionsResponse, Status>>;

    async fn watch_connections(
        &self,
        req: Request<pb::ConnectionsRequest>,
    ) -> Result<Response<Self::WatchConnectionsStream>, Status> {
        let caller = self.caller(&req)?;
        let namespace = crate::connections::asked(self, &caller, req.get_ref())?;
        if let Some(mut leader) = crate::image_ops::leader_client(self, &req)? {
            let from_leader = leader
                .watch_connections(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            return Ok(Response::new(passed_on(from_leader, self.closing.clone())));
        }
        self.consistent().await?;
        // pf says nothing as connections come and go: the nodes are asked
        // again every 10 s while someone follows.
        let wake = Wake {
            leader: true,
            every: std::time::Duration::from_secs(10),
            ..Wake::default()
        };
        let followed = self.followed(wake, |_| None, {
            let namespace = namespace.clone();
            move |api| {
                let namespace = namespace.clone();
                async move { crate::connections::seen(&api, &namespace).await }
            }
        });
        Ok(Response::new(self.revocable(
            followed,
            caller,
            Verb::Read,
            Kind::Service,
            namespace,
        )))
    }

    type WatchLoadBalancersStream = ReceiverStream<Result<pb::LoadBalancersResponse, Status>>;

    async fn watch_load_balancers(
        &self,
        req: Request<pb::LoadBalancersRequest>,
    ) -> Result<Response<Self::WatchLoadBalancersStream>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.get_ref().namespace.clone();
        if namespace.is_empty() {
            return Err(Status::invalid_argument("which namespace?"));
        }
        check_namespace(&namespace)?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let from_leader = c
                .watch_load_balancers(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            return Ok(Response::new(passed_on(from_leader, self.closing.clone())));
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Read, Kind::Service, &namespace)?;
        // A heartbeat brings each node's CARP states; a Service changed
        // changes its addresses; a node silent long enough says nothing.
        let wake = Wake {
            leader: true,
            reports: true,
            store: true,
            every: std::time::Duration::from_secs(10),
            settle: std::time::Duration::from_secs(1),
            ..Wake::default()
        };
        let followed = self.followed(wake, |_| None, {
            let namespace = namespace.clone();
            move |api| {
                let namespace = namespace.clone();
                async move { api.balancers_seen(&namespace) }
            }
        });
        Ok(Response::new(self.revocable(
            followed,
            caller,
            Verb::Read,
            Kind::Service,
            namespace,
        )))
    }

    async fn usage_history(
        &self,
        req: Request<pb::UsageHistoryRequest>,
    ) -> Result<Response<pb::UsageHistoryResponse>, Status> {
        let caller = self.caller(&req)?;
        let (namespace, task, range_ms) = history_asked(req.get_ref())?;
        // Only the leader hears heartbeats; it checks the user's rights.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c
                .usage_history(forwarded_for(req.into_inner(), &caller)?)
                .await;
        }
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        Ok(Response::new(
            self.history_seen(&caller, &namespace, &task, range_ms)
                .await?,
        ))
    }

    async fn quotas(
        &self,
        req: Request<pb::QuotasRequest>,
    ) -> Result<Response<pb::QuotasResponse>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.into_inner().namespace;
        check_namespace(&namespace)?;
        self.consistent().await?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        let list = |kind| {
            self.store
                .list(kind, None)
                .map(|l| l.objects)
                .map_err(|e| Status::internal(e.to_string()))
        };
        let used = crate::quota::in_use(&list(Kind::Task)?, &list(Kind::Volume)?);
        let mut namespaces = Vec::new();
        for o in list(Kind::Namespace)? {
            let ns = o.metadata.name;
            if (!namespace.is_empty() && ns != namespace)
                || !caller.may(&self.store, Verb::Read, Kind::Task, &ns)
            {
                continue;
            }
            let Some(q) = legion_api::parse::<legion_api::admit::NamespaceSpec>(&o.spec)
                .ok()
                .and_then(|s| s.quota)
            else {
                continue;
            };
            let u = used.get(&ns).copied().unwrap_or_default();
            namespaces.push(pb::NamespaceQuota {
                namespace: ns,
                resources: crate::quota::values(&q, &u)
                    .into_iter()
                    .map(|(resource, limit, used)| pb::QuotaUse {
                        resource: resource.into(),
                        limit,
                        used,
                    })
                    .collect(),
            });
        }
        Ok(Response::new(pb::QuotasResponse { namespaces }))
    }

    type EventsStream = ReceiverStream<Result<pb::EventsResponse, Status>>;

    async fn events(
        &self,
        req: Request<pb::EventsRequest>,
    ) -> Result<Response<Self::EventsStream>, Status> {
        let caller = self.caller(&req)?;
        let pb::EventsRequest {
            namespace,
            kind,
            name,
            follow,
        } = req.into_inner();
        let kind: Option<Kind> = if kind.is_empty() {
            None
        } else {
            Some(
                kind.parse()
                    .map_err(|e: legion_api::Error| Status::invalid_argument(e.to_string()))?,
            )
        };
        if kind.is_some_and(Kind::is_cluster_scoped) {
            if !namespace.is_empty() {
                return Err(Status::invalid_argument(
                    "a cluster's objects are in no namespace",
                ));
            }
        } else if namespace.is_empty() {
            return Err(Status::invalid_argument("which namespace?"));
        } else {
            check_namespace(&namespace)?;
        }
        if kind.is_none() && !name.is_empty() {
            return Err(Status::invalid_argument("a name is of a kind"));
        }
        // Who may read the kind there; a namespace's events, its tasks.
        caller.check(
            &self.store,
            Verb::Read,
            kind.unwrap_or(Kind::Task),
            &namespace,
        )?;
        let namespace_checked = namespace.clone();
        let wanted = move |e: &legion_api::event::Event| match kind {
            None => true,
            Some(k) if name.is_empty() => e.kind == k,
            Some(k) => crate::events::about(e, k, &name),
        };
        let to_pb = |e: legion_api::event::Event| pb::ObjectEvent {
            at_ms: e.at_ms,
            kind: e.kind.to_string(),
            namespace: e.namespace,
            name: e.name,
            reason: e.reason,
            message: e.message,
            owner: e.owner,
        };
        // Following from before the backlog is read, so none falls between.
        let mut new = self.events.subscribe();
        let kept = self.events.of(&namespace, &wanted);
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let mut closing = self.closing.clone();
        let last = kept.last().map(|e| e.at_ms);
        let backlog: Vec<legion_api::event::Event> = kept
            .iter()
            .filter(|e| Some(e.at_ms) == last)
            .cloned()
            .collect();
        tx.send(Ok(pb::EventsResponse {
            events: kept.into_iter().map(to_pb).collect(),
        }))
        .await
        .map_err(|_| Status::cancelled("gone"))?;
        if follow {
            tokio::spawn(async move {
                loop {
                    let e = tokio::select! {
                        e = new.recv() => e,
                        () = tx.closed() => return,
                        _ = closing.wait_for(|c| *c) => return,
                    };
                    let e = match e {
                        Ok(e) => e,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let _ = tx
                                .send(Err(Status::resource_exhausted(
                                    "too many events to follow; open them again",
                                )))
                                .await;
                            return;
                        }
                        Err(_) => return,
                    };
                    // Sent with the backlog already.
                    if last.is_some_and(|l| e.at_ms < l) || backlog.contains(&e) {
                        continue;
                    }
                    if e.namespace != namespace || !wanted(&e) {
                        continue;
                    }
                    if tx
                        .send(Ok(pb::EventsResponse {
                            events: vec![to_pb(e)],
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
        Ok(Response::new(self.revocable(
            ReceiverStream::new(rx),
            caller,
            Verb::Read,
            kind.unwrap_or(Kind::Task),
            namespace_checked,
        )))
    }

    async fn dry_run(
        &self,
        req: Request<pb::DryRunRequest>,
    ) -> Result<Response<pb::DryRunResponse>, Status> {
        use legion_api::dryrun::Change;
        let caller = self.caller(&req)?;
        let change: Change = serde_json::from_slice(&req.get_ref().change_json)
            .map_err(|e| Status::invalid_argument(format!("the change: {e}")))?;
        // Asked by who may make the change.
        match &change {
            Change::Drain { .. } | Change::RemoveNode { .. } => {
                caller.check(&self.store, Verb::Write, Kind::Node, "")?
            }
            Change::Demote { .. } => caller.require_all("change a node's role")?,
            Change::MoveServices { namespace, .. } => {
                check_namespace(namespace)?;
                caller.check(&self.store, Verb::Write, Kind::Service, namespace)?;
                caller.check(&self.store, Verb::Write, Kind::Volume, namespace)?
            }
            Change::MoveVolume { namespace, .. } => {
                check_namespace(namespace)?;
                caller.check(&self.store, Verb::Write, Kind::Volume, namespace)?
            }
            Change::DeleteService { namespace, .. } => {
                check_namespace(namespace)?;
                caller.check(&self.store, Verb::Write, Kind::Service, namespace)?
            }
        }
        // The leader hears what the nodes' pools and datasets hold.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.dry_run(forwarded_for(req.into_inner(), &caller)?).await;
        }
        self.consistent().await?;
        let cluster = crate::controller::read_cluster(&self.store).map_err(internal)?;
        let facts = crate::controller::plan::dryrun::Facts {
            used: self
                .usage
                .volume_space()
                .into_iter()
                .map(|(k, (used, _))| (k, used))
                .collect(),
            free: self
                .usage
                .fresh_nodes()
                .into_iter()
                .map(|(n, t)| (n, t.zfs_avail_bytes))
                .filter(|(_, free)| *free > 0)
                .collect(),
        };
        let now = tokio::time::Instant::now();
        let asked = change.clone();
        let done = tokio::task::spawn_blocking(move || {
            crate::controller::plan::dryrun::dry_run(cluster, &asked, &facts, now)
        })
        .await
        .map_err(internal)?;
        use crate::controller::plan::dryrun::Refused;
        let mut d = match done {
            Ok(d) => d,
            // A manager with no Node (a witness, or one without an agent):
            // removing it moves nothing; only what the managers are left
            // with, below.
            Err(Refused::NotFound(_))
                if matches!(&change, Change::RemoveNode { node }
                    if crate::managers::id_of(&self.store, node)?.is_some()) =>
            {
                legion_api::dryrun::DryRun {
                    change: change.clone(),
                    findings: Vec::new(),
                    moves: Vec::new(),
                    stopped: 0,
                    hidden: 0,
                }
            }
            Err(Refused::NotFound(m)) => return Err(Status::not_found(m)),
            Err(Refused::Invalid(m)) => return Err(Status::invalid_argument(m)),
        };
        // A manager leaving the membership: what the managers are left with.
        if let Change::Demote { node } | Change::RemoveNode { node } = &change {
            let (managers, leader) = self.managers_seen()?;
            d.findings
                .extend(legion_api::dryrun::membership(node, &managers, &leader));
            d.findings.sort_by_key(|f| f.severity);
        }
        // Each as its object may be read; the rest counted.
        let readable = |object: &str, namespace: &str| {
            let kind = object.split(' ').next().unwrap_or("");
            match kind.parse::<Kind>() {
                Ok(kind) => caller.may(&self.store, Verb::Read, kind, namespace),
                Err(_) => true,
            }
        };
        let before = d.findings.len() + d.moves.len();
        d.findings.retain(|f| readable(&f.object, &f.namespace));
        d.moves
            .retain(|m| caller.may(&self.store, Verb::Read, Kind::Volume, &m.namespace));
        d.hidden = u32::try_from(before - d.findings.len() - d.moves.len()).unwrap_or(u32::MAX);
        Ok(Response::new(pb::DryRunResponse {
            dry_run_json: serde_json::to_vec(&d).expect("serialises"),
        }))
    }

    type ActivityStream = ReceiverStream<Result<pb::ActivityResponse, Status>>;

    async fn activity(
        &self,
        req: Request<pb::ActivityRequest>,
    ) -> Result<Response<Self::ActivityStream>, Status> {
        let caller = self.caller(&req)?;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        // The leader runs the moves and restores and hears what they send.
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let mut from_leader = c
                .activity(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            tokio::spawn(async move {
                loop {
                    let r = from_leader.message().await.transpose();
                    let Some(r) = r else { return };
                    let last = r.is_err();
                    if tx.send(r).await.is_err() || last {
                        return;
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let follow = req.into_inner().follow;
        let store = self.store.clone();
        // Each as its target may be read; who asked, for the admin.
        let shown = move |mut op: legion_api::activity::Operation| {
            let kind: Kind = op.target.kind.parse().ok()?;
            if !caller.may(&store, Verb::Read, kind, &op.target.namespace) {
                return None;
            }
            if caller != Caller::All {
                op.by.clear();
            }
            Some(serde_json::to_vec(&op).expect("serialises"))
        };
        let mut new = self.activity.subscribe();
        let kept: Vec<Vec<u8>> = self.activity.all().into_iter().filter_map(&shown).collect();
        tx.send(Ok(pb::ActivityResponse {
            operations_json: kept,
        }))
        .await
        .map_err(|_| Status::cancelled("gone"))?;
        if follow {
            let mut closing = self.closing.clone();
            tokio::spawn(async move {
                loop {
                    let op = tokio::select! {
                        op = new.recv() => op,
                        () = tx.closed() => return,
                        _ = closing.wait_for(|c| *c) => return,
                    };
                    let op = match op {
                        Ok(op) => op,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            let _ = tx
                                .send(Err(Status::resource_exhausted(
                                    "the activity changed too fast to follow; open it again",
                                )))
                                .await;
                            return;
                        }
                        Err(_) => return,
                    };
                    let Some(json) = shown(op) else { continue };
                    if tx
                        .send(Ok(pb::ActivityResponse {
                            operations_json: vec![json],
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type WatchAlertsStream = ReceiverStream<Result<pb::AlertsResponse, Status>>;

    async fn watch_alerts(
        &self,
        req: Request<pb::AlertsRequest>,
    ) -> Result<Response<Self::WatchAlertsStream>, Status> {
        let caller = self.caller(&req)?;
        let namespace = req.into_inner().namespace;
        check_namespace(&namespace)?;
        if !namespace.is_empty() {
            caller.check(&self.store, Verb::Read, Kind::Task, &namespace)?;
        }
        // From this replica as it is: no wait for the leader, as an alert
        // matters most when there is none.
        let api = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut closing = api.closing.clone();
            let mut raft = api.store.raft.metrics();
            let mut sent: Option<Vec<pb::Alert>> = None;
            let mut watcher = None;
            loop {
                let visible: Vec<pb::Alert> = crate::alerts::current(&api)
                    .into_iter()
                    .filter(|(a, _)| alert_visible(&caller, &api.store, &namespace, a))
                    .map(|(a, since_ms)| pb::Alert {
                        kind: a.kind.as_str().into(),
                        severity: a.severity.as_str().into(),
                        namespace: a.namespace,
                        object: a.object,
                        message: a.message,
                        since_ms,
                    })
                    .collect();
                if sent.as_ref() != Some(&visible) {
                    let msg = pb::AlertsResponse {
                        alerts: visible.clone(),
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                    sent = Some(visible);
                }
                if watcher.is_none() {
                    watcher = api
                        .store
                        .applied_version()
                        .ok()
                        .and_then(|v| api.store.watch(v, None).ok());
                }
                // Whatever may change the list: a change to the store,
                // Raft's view, the restarts' window moving on.
                let changed = async {
                    match watcher.as_mut() {
                        Some(w) => w.next().await.is_ok(),
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    ok = changed => if !ok { watcher = None },
                    _ = raft.changed() => {}
                    () = tokio::time::sleep(crate::alerts::RECHECK) => {}
                    () = tx.closed() => return,
                    _ = closing.wait_for(|c| *c) => return,
                }
                // Changes come in bursts: one look after them.
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn audit(
        &self,
        req: Request<pb::AuditRequest>,
    ) -> Result<Response<pb::AuditResponse>, Status> {
        self.caller(&req)?.require_all("read the audit trail")?;
        // Any manager answers, once caught up with the leader.
        self.consistent().await?;
        let req = req.into_inner();
        let filter: legion_api::audit::Filter = if req.filter_json.is_empty() {
            Default::default()
        } else {
            serde_json::from_slice(&req.filter_json)
                .map_err(|e| Status::invalid_argument(format!("the filter: {e}")))?
        };
        let limit = if req.limit == 0 {
            1000
        } else {
            req.limit as usize
        };
        let (events, truncated) = crate::audit::list(&self.store, &filter, limit)?;
        Ok(Response::new(pb::AuditResponse {
            events_json: events
                .iter()
                .map(|e| serde_json::to_vec(e).expect("events serialize"))
                .collect(),
            truncated,
        }))
    }

    async fn record_audit(
        &self,
        req: Request<pb::RecordAuditRequest>,
    ) -> Result<Response<pb::RecordAuditResponse>, Status> {
        // From another manager only: nobody else adds to the trail.
        let tls = self.peer_tls.is_some();
        if let Some(id) = crate::tls::caller(&req, tls)?
            && id.role != Role::Manager
        {
            return Err(Status::permission_denied(format!(
                "{id} may not add to the audit trail"
            )));
        }
        self.caller(&req)?;
        if self.leader_addr(&req)?.is_some() {
            return Err(Status::unavailable("not the leader; hand the events to it"));
        }
        let events = req
            .into_inner()
            .events_json
            .iter()
            .map(|e| serde_json::from_slice(e))
            .collect::<Result<Vec<legion_api::audit::AuditEvent>, _>>()
            .map_err(|e| Status::invalid_argument(format!("an audit event: {e}")))?;
        if !events.is_empty() {
            crate::audit::write(self, &events).await?;
        }
        Ok(Response::new(pb::RecordAuditResponse {}))
    }

    async fn usage_archive(
        &self,
        req: Request<pb::UsageArchiveRequest>,
    ) -> Result<Response<pb::UsageArchiveResponse>, Status> {
        // From another manager only: what tasks used, unfiltered, is not
        // for users.
        let tls = self.peer_tls.is_some();
        if let Some(id) = crate::tls::caller(&req, tls)?
            && id.role != Role::Manager
        {
            return Err(Status::permission_denied(format!(
                "{id} may not read the usage archive"
            )));
        }
        self.caller(&req)?;
        let since_ms = req.into_inner().since_ms;
        let archive = self.history.clone();
        let entries = tokio::task::spawn_blocking(move || {
            archive
                .lock()
                .unwrap()
                .entries_since(since_ms, crate::history::BATCH)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let entries_json =
            serde_json::to_vec(&entries).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::UsageArchiveResponse { entries_json }))
    }

    async fn me(&self, req: Request<pb::MeRequest>) -> Result<Response<pb::MeResponse>, Status> {
        let caller = self.caller(&req)?;
        let id = crate::tls::caller(&req, self.peer_tls.is_some())?;
        let (name, role) = match &id {
            Some(id) => (id.name.clone(), id.role.as_str().to_owned()),
            None => (String::new(), "admin".to_owned()),
        };
        // As of every change committed before the question, so a role just
        // given shows at once, whichever manager is asked.
        self.consistent().await?;
        let roles = caller.roles(&self.store)?;
        Ok(Response::new(pb::MeResponse {
            name,
            role,
            all: roles.is_none(),
            namespaces: roles
                .unwrap_or_default()
                .into_iter()
                .map(|(ns, r)| (ns, r.to_string()))
                .collect(),
        }))
    }

    async fn console_code(
        &self,
        req: Request<pb::ConsoleCodeRequest>,
    ) -> Result<Response<pb::ConsoleCodeResponse>, Status> {
        let tls = self.peer_tls.is_some();
        self.caller(&req)?;
        let id = crate::tls::caller(&req, tls)?
            .ok_or_else(|| Status::failed_precondition("the console needs TLS"))?;
        let audit = self.audit_point(
            &req,
            "console-code",
            crate::audit::target("User", "", &id.name),
        );
        let result = async {
            // Who asked: a person with their own certificate, or one a
            // manager forwards for, as it says.
            let (name, role, until_ms) = match id.role {
                Role::Admin | Role::User => {
                    let until = req
                        .peer_certs()
                        .and_then(|c| c.first().cloned())
                        .and_then(|der| legion_pki::validity(&legion_pki::cert_pem(&der)).ok())
                        .map(|(to, _)| crate::console::ms(to))
                        .ok_or_else(|| Status::unauthenticated("no client certificate"))?;
                    (id.name.clone(), id.role.as_str().to_owned(), until)
                }
                Role::Manager if !req.get_ref().for_name.is_empty() => {
                    let r = req.get_ref();
                    (r.for_name.clone(), r.for_role.clone(), r.for_until_ms)
                }
                _ => {
                    return Err(Status::permission_denied(format!(
                        "{id} may not sign in to the console"
                    )));
                }
            };
            if !matches!(role.as_str(), "admin" | "user") {
                return Err(Status::invalid_argument("the role is `admin` or `user`"));
            }
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c
                    .console_code(forwarded(pb::ConsoleCodeRequest {
                        for_name: name,
                        for_role: role,
                        for_until_ms: until_ms,
                    }))
                    .await;
            }
            self.consistent().await?;
            let (code, expires_ms) = crate::console::new_code(self, &name, &role, until_ms).await?;
            Ok(Response::new(pb::ConsoleCodeResponse { code, expires_ms }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn console_session(
        &self,
        req: Request<pb::ConsoleSessionRequest>,
    ) -> Result<Response<pb::ConsoleSessionResponse>, Status> {
        self.only_managers(&req, "start console sessions")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.console_session(forwarded(req.into_inner())).await;
        }
        self.consistent().await?;
        let (session, signed) = crate::console::redeem(self, &req.get_ref().code).await?;
        tracing::info!(name = %signed.name, role = %signed.role, "a console session began");
        Ok(Response::new(pb::ConsoleSessionResponse {
            session,
            name: signed.name,
            role: signed.role,
            expires_ms: signed.expires_ms,
        }))
    }

    async fn console_end(
        &self,
        req: Request<pb::ConsoleEndRequest>,
    ) -> Result<Response<pb::ConsoleEndResponse>, Status> {
        self.only_managers(&req, "end console sessions")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.console_end(forwarded(req.into_inner())).await;
        }
        crate::console::end(self, &req.get_ref().session).await?;
        Ok(Response::new(pb::ConsoleEndResponse {}))
    }

    async fn console_settings(
        &self,
        req: Request<pb::ConsoleSettingsRequest>,
    ) -> Result<Response<pb::ConsoleSettingsResponse>, Status> {
        self.caller(&req)?;
        let settings = crate::console::settings(&self.store)?;
        Ok(Response::new(pb::ConsoleSettingsResponse {
            settings_json: serde_json::to_vec(&settings).expect("serialises"),
        }))
    }

    async fn console_oidc(
        &self,
        req: Request<pb::ConsoleOidcRequest>,
    ) -> Result<Response<pb::ConsoleOidcResponse>, Status> {
        self.caller(&req)?
            .require_all("read single sign-on's settings")?;
        Ok(Response::new(oidc_response(crate::console::oidc(
            &self.store,
        )?)))
    }

    async fn set_console_oidc(
        &self,
        req: Request<pb::SetConsoleOidcRequest>,
    ) -> Result<Response<pb::ConsoleOidcResponse>, Status> {
        let audit = self.audit_point(&req, "console-oidc", Default::default());
        let result = async {
            self.caller(&req)?
                .require_all("change single sign-on's settings")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.set_console_oidc(forwarded(req.into_inner())).await;
            }
            let r = req.into_inner();
            let oidc = if r.remove {
                None
            } else {
                Some(
                    serde_json::from_slice::<legion_api::console::Oidc>(&r.oidc_json)
                        .map_err(|e| Status::invalid_argument(format!("the OIDC settings: {e}")))?,
                )
            };
            self.consistent().await?;
            crate::console::set_oidc(self, oidc, &r.client_secret).await?;
            Ok(Response::new(oidc_response(crate::console::oidc(
                &self.store,
            )?)))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn console_oidc_session(
        &self,
        req: Request<pb::ConsoleOidcSessionRequest>,
    ) -> Result<Response<pb::ConsoleSessionResponse>, Status> {
        self.only_managers(&req, "start console sessions")?;
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            return c.console_oidc_session(forwarded(req.into_inner())).await;
        }
        self.consistent().await?;
        let r = req.get_ref();
        let (session, signed) = crate::console::oidc_session(self, &r.name, &r.groups).await?;
        tracing::info!(name = %signed.name, groups = ?r.groups, "a console session began by single sign-on");
        Ok(Response::new(pb::ConsoleSessionResponse {
            session,
            name: signed.name,
            role: signed.role,
            expires_ms: signed.expires_ms,
        }))
    }

    async fn set_console_settings(
        &self,
        req: Request<pb::SetConsoleSettingsRequest>,
    ) -> Result<Response<pb::ConsoleSettingsResponse>, Status> {
        let audit = self.audit_point(&req, "console-settings", Default::default());
        let result = async {
            self.caller(&req)?
                .require_all("change the console's settings")?;
            if let Some(addr) = self.leader_addr(&req)? {
                let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                    &addr,
                    self.peer_tls.as_ref(),
                ));
                return c.set_console_settings(forwarded(req.into_inner())).await;
            }
            let settings: legion_api::console::Settings =
                serde_json::from_slice(&req.get_ref().settings_json)
                    .map_err(|e| Status::invalid_argument(format!("the settings: {e}")))?;
            self.consistent().await?;
            crate::console::set_settings(self, &settings).await?;
            Ok(Response::new(pb::ConsoleSettingsResponse {
                settings_json: serde_json::to_vec(&settings).expect("serialises"),
            }))
        }
        .await;
        self.audit_end(audit, result)
    }

    type BuildLogsStream = ReceiverStream<Result<pb::BuildLogsChunk, Status>>;

    async fn build_logs(
        &self,
        req: Request<pb::BuildLogsRequest>,
    ) -> Result<Response<Self::BuildLogsStream>, Status> {
        let caller = self.caller(&req)?;
        let r = req.get_ref().clone();
        check_namespace(&r.namespace)?;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        // To the leader as the caller, whose rights it checks once caught
        // up (found by an audit: a follower checked its own replica and
        // forwarded the call as itself).
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let mut chunks = c
                .build_logs(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            tokio::spawn(async move {
                loop {
                    let chunk = chunks.message().await.transpose();
                    let Some(chunk) = chunk else { return };
                    let last = chunk.is_err();
                    if tx.send(chunk).await.is_err() || last {
                        return;
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        self.consistent().await?;
        caller.check(&self.store, Verb::Read, Kind::Build, &r.namespace)?;
        let namespace = r.namespace.clone();
        let key = ObjectKey::new(Kind::Build, &r.namespace, &r.name);
        let o = self.store.get(&key).map_err(internal)?.ok_or_else(|| {
            Status::not_found(format!("no build {} in namespace {}", r.name, r.namespace))
        })?;
        let logs = self.builds.clone();
        let mut closing = self.closing.clone();
        match logs.reader(&key) {
            Some(mut reader) => {
                tokio::spawn(async move {
                    loop {
                        let Some((data, done)) = logs.read(&mut reader) else {
                            return;
                        };
                        if !data.is_empty()
                            && tx.send(Ok(pb::BuildLogsChunk { data })).await.is_err()
                        {
                            return;
                        }
                        if done || !r.follow {
                            return;
                        }
                        tokio::select! {
                            changed = reader.changed.changed() => if changed.is_err() { return },
                            _ = closing.wait_for(|c| *c) => return,
                        }
                    }
                });
            }
            None => {
                let data = crate::build::status_log(&o).into_bytes();
                tokio::spawn(async move {
                    if !data.is_empty() {
                        let _ = tx.send(Ok(pb::BuildLogsChunk { data })).await;
                    }
                });
            }
        }
        Ok(Response::new(self.revocable(
            ReceiverStream::new(rx),
            caller,
            Verb::Read,
            Kind::Build,
            namespace,
        )))
    }

    type BackupStream = ReceiverStream<Result<pb::BackupChunk, Status>>;

    async fn backup(
        &self,
        req: Request<pb::BackupRequest>,
    ) -> Result<Response<Self::BackupStream>, Status> {
        // Secrets and join tokens included.
        self.caller(&req)?.require_all("back up the cluster")?;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        if let Some(addr) = self.leader_addr(&req)? {
            let mut c = legion_proto::v1::cluster_client::ClusterClient::new(connect_lazy(
                &addr,
                self.peer_tls.as_ref(),
            ));
            let mut chunks = c.backup(forwarded(req.into_inner())).await?.into_inner();
            tokio::spawn(async move {
                loop {
                    let chunk = chunks.message().await.transpose();
                    let Some(chunk) = chunk else { return };
                    let last = chunk.is_err();
                    if tx.send(chunk).await.is_err() || last {
                        return;
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        self.consistent().await?;
        let backup = self.store.backup().map_err(internal)?;
        let json = serde_json::to_vec(&backup).map_err(internal)?;
        tracing::info!(
            version = backup.version,
            objects = backup.objects.len(),
            bytes = json.len(),
            "backup"
        );
        tokio::spawn(async move {
            for data in json.chunks(1 << 20) {
                let chunk = pb::BackupChunk {
                    data: data.to_vec(),
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn issue_user(
        &self,
        req: Request<pb::IssueUserRequest>,
    ) -> Result<Response<pb::IssueUserResponse>, Status> {
        let audit = self.audit_point(
            &req,
            "issue-user",
            crate::audit::target("User", "", &req.get_ref().name),
        );
        let result = async {
            self.caller(&req)?.require_all("issue user certificates")?;
            let req = req.into_inner();
            legion_api::validate_name(&req.name)
                .map_err(|e| Status::invalid_argument(format!("user name: {e}")))?;
            let lifetime = match req.days {
                0 => legion_pki::CERT_LIFETIME,
                days => std::time::Duration::from_secs(u64::from(days) * 24 * 3600),
            };
            // Any manager with the CA key can sign: nothing is written.
            let ca = self.issuer()?.current()?;
            let id = legion_pki::Identity::new(&req.name, Role::User);
            let cert_pem = ca
                .sign(&req.csr_pem, &id, lifetime)
                .map_err(|e| Status::invalid_argument(format!("certificate request: {e}")))?;
            tracing::info!(%id, days = lifetime.as_secs() / 86_400, "issued a user certificate");
            Ok(Response::new(pb::IssueUserResponse {
                cert_pem,
                ca_pem: ca.cert_pem().to_owned(),
            }))
        }
        .await;
        self.audit_end(audit, result)
    }

    async fn node_images(
        &self,
        req: Request<pb::NodeImagesRequest>,
    ) -> Result<Response<pb::NodeImagesResponse>, Status> {
        crate::image_ops::node_images(self, req).await
    }

    type ExportImageStream = ReceiverStream<Result<pb::BackupChunk, Status>>;

    async fn export_image(
        &self,
        req: Request<pb::ExportImageRequest>,
    ) -> Result<Response<Self::ExportImageStream>, Status> {
        crate::image_ops::export_image(self, req).await
    }

    async fn load_image(
        &self,
        req: Request<tonic::Streaming<pb::LoadImageChunk>>,
    ) -> Result<Response<pb::NodeImagesResponse>, Status> {
        crate::image_ops::load_image(self, req).await
    }

    async fn search_images(
        &self,
        req: Request<pb::SearchImagesRequest>,
    ) -> Result<Response<pb::SearchImagesResponse>, Status> {
        crate::image_ops::search_images(self, req).await
    }

    async fn inspect_registry_image(
        &self,
        req: Request<pb::InspectRegistryImageRequest>,
    ) -> Result<Response<pb::InspectRegistryImageResponse>, Status> {
        crate::image_ops::inspect_registry_image(self, req).await
    }

    async fn scan_image(
        &self,
        req: Request<pb::ScanImageRequest>,
    ) -> Result<Response<pb::ScanImageResponse>, Status> {
        crate::image_ops::scan_image(self, req).await
    }

    async fn diagnose(
        &self,
        req: Request<pb::DiagnoseRequest>,
    ) -> Result<Response<pb::DiagnoseResponse>, Status> {
        crate::diagnostics::diagnose(self, req).await
    }

    async fn support_bundle(
        &self,
        req: Request<pb::SupportBundleRequest>,
    ) -> Result<Response<pb::SupportBundleResponse>, Status> {
        crate::diagnostics::support_bundle(self, req).await
    }
}

impl ApiService {
    fn status(&self) -> Result<StatusResponse, Status> {
        let m = self.store.raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let witnesses = crate::managers::witnesses(&self.store).unwrap_or_default();
        let members = membership
            .nodes()
            .map(|(id, node)| pb::Member {
                id: *id,
                addr: node.addr.clone(),
                voter: membership.voter_ids().any(|v| v == *id),
                witness: witnesses.contains(id) || (*id == self.id && self.witness),
            })
            .collect();
        Ok(StatusResponse {
            node_id: self.id,
            leader_id: m.current_leader,
            state: format!("{:?}", m.state),
            term: m.current_term,
            applied_version: self.store.applied_version().map_err(internal)?,
            members,
            protocol: self.speaking.mine.speaks,
            protocol_floor: crate::upgrade::read(&self.store),
            name: self.name.clone(),
            ca_issuer: crate::ca::own_issuer(self),
            witness: self.witness,
            leader_name: match m.current_leader {
                Some(id) if id == self.id => self.name.clone(),
                Some(id) => crate::managers::names(&self.store)
                    .ok()
                    .and_then(|n| n.get(&id).cloned())
                    .unwrap_or_default(),
                None => String::new(),
            },
        })
    }
}

/// What wakes a followed answer (`ApiService::followed`) to be worked out
/// again; sent only when it changed.
#[derive(Clone, Copy)]
pub(crate) struct Wake {
    /// Leader's work: ended once this manager no longer leads, to be
    /// followed again on the leader there is.
    pub leader: bool,
    /// Raft's view changing: leader, term, members, a version applied.
    pub raft: bool,
    /// A node's heartbeat taken (`Usage::reports`).
    pub reports: bool,
    /// A change to the store.
    pub store: bool,
    /// Worked out again after this though nothing woke it.
    pub every: std::time::Duration,
    /// Once woken, how long to wait for what comes with it: changes and
    /// heartbeats come in bursts.
    pub settle: std::time::Duration,
}

impl Default for Wake {
    fn default() -> Self {
        Wake {
            leader: false,
            raft: false,
            reports: false,
            store: false,
            every: std::time::Duration::from_secs(60),
            settle: std::time::Duration::from_secs(1),
        }
    }
}

impl ApiService {
    /// Follows an answer: `look` works it out, sent first, then again
    /// each time it is different, worked out again whenever `wake` says
    /// (`sooner`, given the last answer, may ask for it sooner than
    /// `wake.every`). Ends when the follower goes, the manager closes,
    /// `look` fails (its error the last message), or, for the leader's
    /// work, another manager leads.
    /// `inner`, ended once `caller` may no longer `verb` `kind` in
    /// `namespace`, as a watch or a session ends: checked every
    /// [`crate::authz::REVOCATION_CHECK`], however busy the stream (found
    /// by an audit: event, build log, connection and load balancer streams
    /// went on after their rights were gone).
    pub(crate) fn revocable<T: Send + 'static>(
        &self,
        mut inner: ReceiverStream<Result<T, Status>>,
        caller: Caller,
        verb: Verb,
        kind: Kind,
        namespace: String,
    ) -> ReceiverStream<Result<T, Status>> {
        use tokio_stream::StreamExt;
        let store = self.store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(crate::authz::REVOCATION_CHECK);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            every.tick().await;
            loop {
                tokio::select! {
                    item = inner.next() => {
                        let Some(item) = item else { return };
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                    _ = every.tick() => {
                        if let Err(e) = caller.check(&store, verb, kind, &namespace) {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                    () = tx.closed() => return,
                }
            }
        });
        ReceiverStream::new(rx)
    }

    pub(crate) fn followed<T, F, Fut>(
        &self,
        wake: Wake,
        sooner: impl Fn(&T) -> Option<std::time::Duration> + Send + 'static,
        mut look: F,
    ) -> ReceiverStream<Result<T, Status>>
    where
        T: PartialEq + Clone + Send + 'static,
        F: FnMut(ApiService) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Status>> + Send,
    {
        let api = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut closing = api.closing.clone();
            let mut reports = api.usage.reports();
            let mut raft = api.store.raft.metrics();
            let mut watcher = None;
            let mut sent: Option<T> = None;
            loop {
                let leads = raft.borrow().current_leader == Some(api.id);
                if wake.leader && !leads {
                    let _ = tx
                        .send(Err(Status::unavailable(
                            "leadership moved; follow it again",
                        )))
                        .await;
                    return;
                }
                let now = match look(api.clone()).await {
                    Ok(now) => now,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let every = sooner(&now).map_or(wake.every, |s| s.min(wake.every));
                if sent.as_ref() != Some(&now) {
                    if tx.send(Ok(now.clone())).await.is_err() {
                        return;
                    }
                    sent = Some(now);
                }
                if wake.store && watcher.is_none() {
                    watcher = api
                        .store
                        .applied_version()
                        .ok()
                        .and_then(|v| api.store.watch(v, None).ok());
                }
                let changed = async {
                    match watcher.as_mut() {
                        Some(w) => w.next().await.is_ok(),
                        None => std::future::pending().await,
                    }
                };
                let reported = async {
                    if wake.reports {
                        reports.changed().await.is_ok()
                    } else {
                        std::future::pending().await
                    }
                };
                // The leader's work ends when another leads: Raft is
                // watched for that too.
                let viewed = async {
                    if wake.raft || wake.leader {
                        raft.changed().await.is_ok()
                    } else {
                        std::future::pending().await
                    }
                };
                tokio::select! {
                    ok = changed => if !ok { watcher = None },
                    ok = reported => if !ok { return },
                    ok = viewed => if !ok { return },
                    () = tokio::time::sleep(every) => {}
                    () = tx.closed() => return,
                    _ = closing.wait_for(|c| *c) => return,
                }
                tokio::select! {
                    () = tokio::time::sleep(wake.settle) => {}
                    () = tx.closed() => return,
                    _ = closing.wait_for(|c| *c) => return,
                }
            }
        });
        ReceiverStream::new(rx)
    }
}

/// A stream followed on the leader, passed on as it comes, ended with it
/// (its error the last message), when the follower goes, or when this
/// manager closes.
fn passed_on<T: Send + 'static>(
    mut from: tonic::Streaming<T>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) -> ReceiverStream<Result<T, Status>> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        loop {
            // Ended with this manager, as every stream it serves is: one
            // left open would hold its shutdown.
            let r = tokio::select! {
                r = from.message() => r.transpose(),
                () = tx.closed() => return,
                _ = closing.wait_for(|c| *c) => return,
            };
            let Some(r) = r else { return };
            let last = r.is_err();
            if tx.send(r).await.is_err() || last {
                return;
            }
        }
    });
    ReceiverStream::new(rx)
}

/// A usage history request's namespace, task and range, checked.
fn history_asked(req: &pb::UsageHistoryRequest) -> Result<(String, String, i64), Status> {
    let (namespace, task) = (req.namespace.clone(), req.task.clone());
    check_namespace(&namespace)?;
    if !task.is_empty() && namespace.is_empty() {
        return Err(Status::invalid_argument("a task is named in its namespace"));
    }
    let range_ms = req.range_ms;
    if range_ms < 0 || range_ms > crate::history::KEEP.as_millis() as i64 {
        return Err(Status::invalid_argument(format!(
            "a range is up to {} days",
            crate::history::KEEP.as_secs() / 86_400
        )));
    }
    Ok((namespace, task, range_ms))
}

/// Whether `caller` may see `a`: a namespace's alerts by its tasks' rights,
/// a node's by the rights to read nodes, the cluster's own by an admin.
/// With `namespace` set, only that namespace's.
fn alert_visible(
    caller: &Caller,
    store: &Store,
    namespace: &str,
    a: &legion_api::alert::Alert,
) -> bool {
    if !namespace.is_empty() && a.namespace != namespace {
        return false;
    }
    if !a.namespace.is_empty() {
        return caller.may(store, Verb::Read, Kind::Task, &a.namespace);
    }
    if a.object.starts_with("Node/") {
        return caller.may(store, Verb::Read, Kind::Node, "");
    }
    *caller == Caller::All
}

/// A pod's limits in all, CPU in millicores and memory in bytes: each only
/// when every container has one, as one without is not held to any.
fn pod_limits(pod: &legion_api::service::PodSpec) -> (Option<u64>, Option<u64>) {
    let sum = |f: &dyn Fn(&legion_api::service::Container) -> Option<u64>| {
        pod.containers.iter().map(f).sum::<Option<u64>>()
    };
    (
        sum(&|c| c.resources.limits.cpu.map(|v| v.0)),
        sum(&|c| c.resources.limits.memory.map(|v| v.0)),
    )
}

#[cfg(test)]
mod tests {
    use tonic::Status;

    /// Found by an audit: deleting the last manager that may lead left
    /// witnesses alone, which never stand; deleting or demoting the leader
    /// had it step down before it could write so down.
    #[test]
    fn a_voter_leaves_only_with_one_that_may_lead_left_and_not_the_leader() {
        let set = |ids: &[u64]| {
            ids.iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
        };
        let none = std::collections::HashSet::new();
        let witness: std::collections::HashSet<u64> = [3].into();
        let e = |r: Result<(), Status>| r.unwrap_err().message().to_owned();
        assert!(e(super::leaving_voter("n1", 1, 2, &set(&[1]), &none)).contains("last voter"));
        // Two full managers and a witness: one may go, not both.
        assert!(super::leaving_voter("n2", 2, 1, &set(&[1, 2, 3]), &witness).is_ok());
        assert!(
            e(super::leaving_voter("n2", 2, 1, &set(&[2, 3]), &witness))
                .contains("last that may lead")
        );
        // Never the leader itself.
        assert!(e(super::leaving_voter("n1", 1, 1, &set(&[1, 2, 3]), &witness)).contains("leads"));
        assert!(super::leaving_voter("n3", 3, 1, &set(&[1, 2, 3]), &witness).is_ok());
    }
}
