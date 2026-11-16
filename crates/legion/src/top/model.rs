//! The cluster as `legion top` last heard of it, and the log of what
//! changed.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use legion_api::data::DataSpec;
use legion_api::job::{JobStatus, LABEL_JOB};
use legion_api::node::{NodeSpec, NodeStatus};
use legion_api::service::ServiceStatus;
use legion_api::task::{TaskState, TaskStatus};
use legion_api::volume::VolumeStatus;
use legion_api::{Kind, Object};
use legion_proto::v1 as pb;
use serde::{Deserialize, Serialize};

/// Changes kept in the log.
const LOG: usize = 500;
/// Usage totals kept for the header's graphs, one per poll.
pub(super) const HISTORY: usize = 240;
/// The label tasks of a service carry.
pub(super) const LABEL_SERVICE: &str = "legion.service";

/// An object's kind, namespace and name.
pub type Key = (Kind, String, String);

/// Host ZFS pool and network throughput telemetry reported by a node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct NodeMetrics {
    pub(super) zfs_used: u64,
    pub(super) zfs_avail: u64,
    pub(super) net_rx_bps: u64,
    pub(super) net_tx_bps: u64,
}

/// The cluster as last heard.
#[derive(Default)]
pub struct Model {
    /// Only this namespace's objects, if given.
    pub(super) namespace: Option<String>,
    pub(super) objects: BTreeMap<Key, Object>,
    /// What tasks use, by namespace and task: percent of a CPU, bytes.
    pub(super) usage: BTreeMap<String, BTreeMap<String, (f64, u64)>>,
    /// Host ZFS and network metrics by node name.
    pub(super) node_telemetry: BTreeMap<String, NodeMetrics>,
    /// Per-volume ZFS `(used_bytes, avail_bytes)` by `(namespace, volume)`.
    pub(super) volume_telemetry: BTreeMap<(String, String), (u64, u64)>,
    /// What all tasks used, at each poll.
    pub(super) history: VecDeque<(f64, u64)>,
    /// Cluster-wide `(zfs_used, zfs_avail, net_rx_bps, net_tx_bps)` at each poll.
    pub(super) telemetry_history: VecDeque<(u64, u64, u64, u64)>,
    /// What changed, newest first, with when (Unix seconds).
    pub(super) log: VecDeque<(i64, String)>,
    pub(super) cluster: Option<pb::StatusResponse>,
    /// Kinds the caller may not list.
    pub(super) hidden: BTreeSet<Kind>,
    /// Why it is not current, while it is not.
    pub(super) problem: Option<String>,
    /// Why the cluster's status or use is not current, while it is not.
    pub(super) polled: Option<String>,
}

impl Model {
    pub fn new(namespace: Option<String>) -> Self {
        Model {
            namespace,
            ..Default::default()
        }
    }

    fn shown(&self, o: &Object) -> bool {
        o.kind.is_cluster_scoped()
            || self
                .namespace
                .as_ref()
                .is_none_or(|ns| *ns == o.metadata.namespace)
    }

    /// Everything listed again, after (re)connecting.
    pub(super) fn listed(&mut self, objects: Vec<Object>, hidden: BTreeSet<Kind>) {
        self.objects = objects
            .into_iter()
            .filter(|o| self.shown(o))
            .map(|o| (key(&o), sanitized(o)))
            .collect();
        self.hidden = hidden;
        self.problem = None;
    }

    /// A change from the watch, at `at`. Older than what it has (the watch
    /// starts from the oldest listing) changes nothing.
    pub(super) fn changed(&mut self, put: bool, o: Object, at: i64) {
        if !self.shown(&o) {
            return;
        }
        let o = sanitized(o);
        let k = key(&o);
        let before = self.objects.get(&k);
        if before.is_some_and(|b| b.metadata.version > o.metadata.version) {
            return;
        }
        if put && before.is_some_and(|b| b.metadata.version == o.metadata.version) {
            return;
        }
        let what = if put {
            describe(before, &o)
        } else {
            // Gone before it was seen: no news.
            (o.kind != Kind::Task && before.is_some()).then(|| format!("{} deleted", name(&o)))
        };
        if let Some(what) = what {
            self.log.push_front((at, what));
            self.log.truncate(LOG);
        }
        if put {
            // Readable now: a role binding granted since the listing.
            self.hidden.remove(&o.kind);
            self.objects.insert(k, o);
        } else {
            self.objects.remove(&k);
        }
    }

    pub(super) fn usage(&mut self, u: pb::UsageResponse) {
        self.usage.clear();
        self.node_telemetry.clear();
        self.volume_telemetry.clear();
        let mut total = (0.0, 0);
        let mut zfs_used_sum = 0u64;
        let mut zfs_avail_sum = 0u64;
        let mut net_rx_sum = 0u64;
        let mut net_tx_sum = 0u64;
        for n in u.nodes {
            zfs_used_sum = zfs_used_sum.saturating_add(n.zfs_used_bytes);
            zfs_avail_sum = zfs_avail_sum.saturating_add(n.zfs_avail_bytes);
            net_rx_sum = net_rx_sum.saturating_add(n.net_rx_bps);
            net_tx_sum = net_tx_sum.saturating_add(n.net_tx_bps);
            if n.zfs_used_bytes > 0 || n.zfs_avail_bytes > 0 || n.net_rx_bps > 0 || n.net_tx_bps > 0
            {
                self.node_telemetry.insert(
                    n.node.clone(),
                    NodeMetrics {
                        zfs_used: n.zfs_used_bytes,
                        zfs_avail: n.zfs_avail_bytes,
                        net_rx_bps: n.net_rx_bps,
                        net_tx_bps: n.net_tx_bps,
                    },
                );
            }
            for v in n.volumes {
                if v.used_bytes > 0 || v.avail_bytes > 0 {
                    self.volume_telemetry
                        .insert((v.namespace, v.volume), (v.used_bytes, v.avail_bytes));
                }
            }
            for t in n.tasks {
                total = (total.0 + t.cpu_percent, total.1 + t.memory_bytes);
                self.usage
                    .entry(t.namespace)
                    .or_default()
                    .insert(t.task, (t.cpu_percent, t.memory_bytes));
            }
        }
        self.history.push_back(total);
        while self.history.len() > HISTORY {
            self.history.pop_front();
        }
        self.telemetry_history
            .push_back((zfs_used_sum, zfs_avail_sum, net_rx_sum, net_tx_sum));
        while self.telemetry_history.len() > HISTORY {
            self.telemetry_history.pop_front();
        }
    }

    pub(super) fn zfs_totals(&self) -> (u64, u64) {
        self.telemetry_history
            .back()
            .map_or((0, 0), |(u, a, _, _)| (*u, *a))
    }

    pub(super) fn net_totals(&self) -> (u64, u64) {
        self.telemetry_history
            .back()
            .map_or((0, 0), |(_, _, rx, tx)| (*rx, *tx))
    }

    pub(super) fn node_metrics(&self, node: &str) -> Option<NodeMetrics> {
        self.node_telemetry.get(node).copied()
    }

    pub(super) fn volume_space(&self, namespace: &str, volume: &str) -> Option<(u64, u64)> {
        self.volume_telemetry
            .get(&(namespace.to_owned(), volume.to_owned()))
            .copied()
    }

    pub(super) fn of(&self, kind: Kind) -> impl Iterator<Item = &Object> {
        self.objects
            .range((kind, String::new(), String::new())..)
            .take_while(move |((k, _, _), _)| *k == kind)
            .map(|(_, o)| o)
    }

    pub(super) fn get(&self, k: &Key) -> Option<&Object> {
        self.objects.get(k)
    }

    /// What a task uses, if reported.
    pub(super) fn task_usage(&self, t: &Object) -> Option<(f64, u64)> {
        self.usage
            .get(&t.metadata.namespace)?
            .get(&t.metadata.name)
            .copied()
    }

    /// What the nodes can hold together: millicores, bytes.
    pub(super) fn capacity(&self) -> (u64, u64) {
        self.of(Kind::Node)
            .map(status::<NodeStatus>)
            .fold((0, 0), |(c, m), n| {
                (c + n.allocatable.cpu.millis(), m + n.allocatable.memory.0)
            })
    }
}

pub(super) fn key(o: &Object) -> Key {
    (
        o.kind,
        o.metadata.namespace.clone(),
        o.metadata.name.clone(),
    )
}

/// A Secret's value never reaches the screen: only its size is kept.
fn sanitized(mut o: Object) -> Object {
    if o.kind == Kind::Secret {
        let size = DataSpec::deserialize(&o.spec)
            .ok()
            .and_then(|d| d.bytes().ok())
            .map(|b| b.len());
        o.spec = serde_json::json!({ "bytes": size });
    }
    o
}

/// `node n1`, `service shop/web`.
pub(super) fn name(o: &Object) -> String {
    let kind = kind_word(o.kind);
    if o.metadata.namespace.is_empty() {
        format!("{kind} {}", o.metadata.name)
    } else {
        format!("{kind} {}/{}", o.metadata.namespace, o.metadata.name)
    }
}

/// `service`, `cron job`, `network policy`.
pub(super) fn kind_word(kind: Kind) -> &'static str {
    match kind {
        Kind::Namespace => "namespace",
        Kind::Node => "node",
        Kind::Service => "service",
        Kind::Job => "job",
        Kind::CronJob => "cron job",
        Kind::Task => "task",
        Kind::Secret => "secret",
        Kind::Config => "config",
        Kind::Volume => "volume",
        Kind::NetworkPolicy => "network policy",
        Kind::Route => "route",
        Kind::RoleBinding => "role binding",
        Kind::ImagePolicy => "image policy",
        Kind::Build => "build",
    }
}

pub(super) fn status<T: serde::de::DeserializeOwned + Default>(o: &Object) -> T {
    T::deserialize(&o.status).unwrap_or_default()
}

pub(super) fn spec<T: serde::de::DeserializeOwned + Default>(o: &Object) -> T {
    T::deserialize(&o.spec).unwrap_or_default()
}

pub(super) fn task_state(t: &Object) -> TaskState {
    TaskState::deserialize(&t.status["state"]).unwrap_or_default()
}

/// An enum as it is written in the API: `ready`, `drain`.
pub(super) fn word(v: &impl Serialize) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        _ => String::new(),
    }
}

/// The service or Job a task is of.
pub(super) fn owner(t: &Object) -> Option<(Kind, &str)> {
    if let Some(job) = t.metadata.labels.get(LABEL_JOB) {
        return Some((Kind::Job, job));
    }
    t.metadata
        .labels
        .get(LABEL_SERVICE)
        .map(String::as_str)
        .or_else(|| t.spec["service"].as_str())
        .map(|s| (Kind::Service, s))
}

/// The containers of an object's pod, by name: a task's, service's or
/// Job's; then, with `init`, its init containers.
pub(super) fn containers(o: &Object, init: bool) -> Vec<String> {
    let pod = match o.kind {
        Kind::CronJob => &o.spec["job"]["pod"],
        _ => &o.spec["pod"],
    };
    let names = |key: &str| {
        pod[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| c["name"].as_str().map(str::to_owned))
            .collect::<Vec<_>>()
    };
    let mut all = names("containers");
    if init {
        all.extend(names("initContainers"));
    }
    all
}

/// What changed in `o`, worth a line in the log.
fn describe(before: Option<&Object>, o: &Object) -> Option<String> {
    let who = name(o);
    let Some(before) = before else {
        return match o.kind {
            Kind::Node => Some(format!("{who} joined")),
            // Every new task starts pending: only where it goes is news.
            Kind::Task => task_change(None, o),
            _ => Some(format!("{who} created")),
        };
    };
    match o.kind {
        Kind::Node => {
            let (spec_now, now): (NodeSpec, NodeStatus) = (spec(o), status(o));
            let (spec_then, then): (NodeSpec, NodeStatus) = (spec(before), status(before));
            let mut changes = Vec::new();
            if then.state != now.state {
                changes.push(format!("{} → {}", word(&then.state), word(&now.state)));
            }
            if spec_then.availability != spec_now.availability {
                changes.push(format!(
                    "{} → {}",
                    word(&spec_then.availability),
                    word(&spec_now.availability)
                ));
            }
            if before.metadata.labels != o.metadata.labels {
                changes.push("labels changed".into());
            }
            let lacking = |st: &NodeStatus, sp: &NodeSpec| {
                st.blocking(sp).map(|l| l.id.clone()).collect::<Vec<_>>()
            };
            let (was, is) = (lacking(&then, &spec_then), lacking(&now, &spec_now));
            if was != is {
                changes.push(match is.is_empty() {
                    true => "the host has what it requires again".into(),
                    false => format!("the host lacks {}: no new tasks", is.join(" ")),
                });
            }
            (!changes.is_empty()).then(|| format!("{who}: {}", changes.join(", ")))
        }
        Kind::Service => {
            if before.metadata.generation != o.metadata.generation {
                return Some(format!("{who} changed"));
            }
            let (then, now): (ServiceStatus, ServiceStatus) = (status(before), status(o));
            let state = |s: &ServiceStatus| s.update.as_ref().map(|u| (u.state, u.message.clone()));
            match (state(&then), state(&now)) {
                (a, Some((b, why))) if a.as_ref().map(|a| a.0) != Some(b) => {
                    Some(format!("{who} update {}{}", word(&b), because(&why)))
                }
                _ => None,
            }
        }
        Kind::Job => {
            let (then, now): (JobStatus, JobStatus) = (status(before), status(o));
            (then.state != now.state)
                .then(|| format!("{who} {}{}", word(&now.state), because(&now.message)))
        }
        Kind::CronJob => {
            let suspended = |o: &Object| o.spec["suspend"].as_bool().unwrap_or(false);
            if suspended(before) != suspended(o) {
                let what = if suspended(o) { "suspended" } else { "resumed" };
                return Some(format!("{who} {what}"));
            }
            (before.metadata.generation != o.metadata.generation).then(|| format!("{who} changed"))
        }
        Kind::Volume => {
            let (then, now): (VolumeStatus, VolumeStatus) = (status(before), status(o));
            let phase = |s: &VolumeStatus| s.migration.as_ref().map(|m| (m.phase, m.to.clone()));
            if phase(&then) != phase(&now) {
                return Some(match &now.migration {
                    Some(m) => format!("{who}: moving to {}, {}", m.to, word(&m.phase)),
                    None if then.node != now.node => {
                        format!("{who}: now on {}", now.node)
                    }
                    None => format!("{who}: move called off"),
                });
            }
            (before.metadata.generation != o.metadata.generation).then(|| format!("{who} changed"))
        }
        Kind::Task => task_change(Some(before), o),
        _ => {
            (before.metadata.generation != o.metadata.generation).then(|| format!("{who} changed"))
        }
    }
}

fn task_change(before: Option<&Object>, o: &Object) -> Option<String> {
    let who = name(o);
    let now: TaskStatus = status(o);
    let then: TaskStatus = before.map(status).unwrap_or_default();
    let node = o.spec["node"].as_str().unwrap_or_default();
    let at = if node.is_empty() {
        String::new()
    } else {
        format!(" on {node}")
    };
    if before.is_none() || then.state != now.state {
        // The steps on the way to running are not news.
        if !(now.state == TaskState::Running || now.state.is_terminal()) {
            return None;
        }
        let failed = matches!(now.state, TaskState::Failed | TaskState::Rejected);
        let why = if failed || now.state == TaskState::Running {
            because(&now.message)
        } else {
            String::new()
        };
        return Some(format!("{who}{at}: {}{why}", word(&now.state)));
    }
    if now.restarts > then.restarts {
        return Some(format!("{who}{at}: restarted{}", because(&now.message)));
    }
    if now.state == TaskState::Running && now.ready != then.ready {
        let ready = if now.ready { "ready" } else { "not ready" };
        return Some(format!("{who}{at}: {ready}"));
    }
    None
}

/// `: why`, or nothing.
fn because(why: &str) -> String {
    if why.is_empty() {
        String::new()
    } else {
        format!(": {why}")
    }
}
