//! The manager's metrics, for Prometheus. Every manager reports its own
//! view of Raft; only the leader reports the cluster's objects, tasks,
//! nodes and quotas, so summing a metric across managers counts each thing
//! once, and what is reported is what the controllers act on.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use legion_api::admit::NamespaceSpec;
use legion_api::metrics::Metrics;
use legion_api::node::{NodeSpec, NodeState, NodeStatus};
use legion_api::quota::Usage;
use legion_api::service::{ServiceMode, ServiceSpec};
use legion_api::task::{TaskSpec, TaskState, TaskStatus};
use legion_api::volume::VolumeSpec;
use legion_api::{Kind, Object};
use legion_store::{NodeId, Store};

/// Where metrics find the store: empty once it is shutting down, which a
/// clone of the store itself would prevent.
pub(crate) type Slot = Arc<RwLock<Option<Store>>>;

/// Reads a manager's metrics; cheap to clone.
#[derive(Clone)]
pub struct ManagerMetrics {
    pub(crate) id: NodeId,
    pub(crate) store: Slot,
}

impl ManagerMetrics {
    /// The metrics as they are now. Reads the store: call it off the
    /// async threads.
    /// Nothing once the manager has stopped.
    pub fn collect(&self) -> Metrics {
        let mut m = Metrics::default();
        let slot = self.store.read().unwrap_or_else(|e| e.into_inner());
        let Some(store) = slot.as_ref() else {
            return m;
        };
        let r = store.raft.metrics().borrow().clone();
        let leader = r.current_leader == Some(self.id);
        m.gauge(
            "legion_raft_leader",
            "1 if this manager leads the cluster, else 0.",
        )
        .sample(&[], f64::from(u8::from(leader)));
        m.gauge(
            "legion_raft_has_leader",
            "1 if this manager knows of a leader, else 0.",
        )
        .sample(&[], f64::from(u8::from(r.current_leader.is_some())));
        m.gauge("legion_raft_term", "The Raft term this manager is in.")
            .sample(&[], r.current_term as f64);
        m.gauge(
            "legion_raft_last_log_index",
            "Index of the last entry in this manager's Raft log.",
        )
        .sample(&[], r.last_log_index.unwrap_or(0) as f64);
        let applied = r.last_applied.map_or(0, |l| l.index);
        m.gauge(
            "legion_raft_applied_index",
            "Index of the last entry this manager applied to its store.",
        )
        .sample(&[], applied as f64);
        m.gauge(
            "legion_raft_voters",
            "Managers voting in the cluster, as this manager knows them.",
        )
        .sample(
            &[],
            r.membership_config.membership().voter_ids().count() as f64,
        );
        if let Some(replication) = r.replication.as_ref().filter(|_| leader) {
            let mut lag = m.gauge(
                "legion_raft_follower_lag",
                "Entries a manager's log lacks of the leader's (leader only).",
            );
            for (peer, matched) in replication {
                if *peer == self.id {
                    continue;
                }
                let behind = r
                    .last_log_index
                    .unwrap_or(0)
                    .saturating_sub(matched.map_or(0, |l| l.index));
                lag.sample(&[("manager", &peer.to_string())], behind as f64);
            }
        }
        if leader {
            match cluster(store, &mut m) {
                Ok(()) => {}
                Err(e) => tracing::warn!("reading the store for metrics: {e}"),
            }
        }
        m
    }
}

fn cluster(store: &Store, m: &mut Metrics) -> Result<(), legion_store::Error> {
    let mut objects: BTreeMap<Kind, Vec<Object>> = BTreeMap::new();
    for kind in Kind::ALL {
        if kind == Kind::Route {
            continue;
        }
        objects.insert(kind, store.list(kind, None)?.objects);
    }
    let of = |k: Kind| objects.get(&k).map_or(&[][..], Vec::as_slice);

    let mut f = m.gauge(
        "legion_objects",
        "Objects in the store, by kind and namespace.",
    );
    for (kind, list) in &objects {
        let mut by_ns: BTreeMap<&str, u32> = BTreeMap::new();
        for o in list {
            *by_ns.entry(&o.metadata.namespace).or_default() += 1;
        }
        for (ns, n) in by_ns {
            f.sample(&[("kind", kind.as_str()), ("namespace", ns)], f64::from(n));
        }
    }

    // Tasks: how many in each state, restarts of those not ended, and
    // what is placed, for the quotas.
    let tasks: Vec<(&Object, TaskSpec, TaskStatus)> = of(Kind::Task)
        .iter()
        .filter_map(|o| {
            let spec = legion_api::parse::<TaskSpec>(&o.spec).ok()?;
            let status = serde_json::from_value(o.status.clone()).unwrap_or_default();
            Some((o, spec, status))
        })
        .collect();
    let mut states: BTreeMap<&str, BTreeMap<TaskState, u32>> = BTreeMap::new();
    let mut used: BTreeMap<&str, Usage> = BTreeMap::new();
    // Ready tasks of each service.
    let mut ready: BTreeMap<(&str, &str), u32> = BTreeMap::new();
    for (o, spec, status) in &tasks {
        let ns = o.metadata.namespace.as_str();
        *states
            .entry(ns)
            .or_default()
            .entry(status.state)
            .or_default() += 1;
        if status.state != TaskState::Pending && !status.state.is_terminal() {
            used.entry(ns)
                .or_default()
                .add_tasks(spec.pod.requests(), 1);
        }
        if !spec.job && status.state == TaskState::Running && status.ready {
            *ready.entry((ns, spec.service.as_str())).or_default() += 1;
        }
    }
    let mut f = m.gauge("legion_tasks", "Tasks, by namespace and state.");
    for (ns, counts) in &states {
        for state in TaskState::ALL {
            let n = counts.get(&state).copied().unwrap_or(0);
            f.sample(
                &[("namespace", ns), ("state", state.as_str())],
                f64::from(n),
            );
        }
    }
    let mut f = m.gauge(
        "legion_task_restarts",
        "Container restarts in a task that has not ended, under its restart policy.",
    );
    for (o, spec, status) in &tasks {
        if status.state.is_terminal() {
            continue;
        }
        f.sample(
            &[
                ("namespace", &o.metadata.namespace),
                ("task", &o.metadata.name),
                ("service", &spec.service),
                ("node", &spec.node),
            ],
            f64::from(status.restarts),
        );
    }

    let services: Vec<(&Object, ServiceSpec)> = of(Kind::Service)
        .iter()
        .filter_map(|o| Some((o, legion_api::parse::<ServiceSpec>(&o.spec).ok()?)))
        .collect();
    let mut f = m.gauge(
        "legion_service_replicas",
        "Tasks a replicated or stateful service asks for.",
    );
    for (o, spec) in &services {
        if spec.mode != ServiceMode::Global {
            f.sample(
                &[
                    ("namespace", &o.metadata.namespace),
                    ("service", &o.metadata.name),
                ],
                f64::from(spec.replicas.unwrap_or(1)),
            );
        }
    }
    let mut f = m.gauge(
        "legion_service_ready_tasks",
        "A service's tasks that run and pass their readiness probes.",
    );
    for (o, _) in &services {
        let (ns, name) = (o.metadata.namespace.as_str(), o.metadata.name.as_str());
        let n = ready.get(&(ns, name)).copied().unwrap_or(0);
        f.sample(&[("namespace", ns), ("service", name)], f64::from(n));
    }

    let mut counts: BTreeMap<&str, u32> = BTreeMap::new();
    let mut f = m.gauge("legion_node_ready", "1 if the node is ready, else 0.");
    for o in of(Kind::Node) {
        let spec: NodeSpec = legion_api::parse(&o.spec).unwrap_or_default();
        let status: NodeStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        *counts.entry(status.state.as_str()).or_default() += 1;
        f.sample(
            &[("node", &o.metadata.name), ("role", spec.role.as_str())],
            f64::from(u8::from(status.state == NodeState::Ready)),
        );
    }
    let mut f = m.gauge("legion_nodes", "Nodes, by state.");
    for state in NodeState::ALL {
        let n = counts.get(state.as_str()).copied().unwrap_or(0);
        f.sample(&[("state", state.as_str())], f64::from(n));
    }

    for o in of(Kind::Volume) {
        if let Ok(spec) = legion_api::parse::<VolumeSpec>(&o.spec) {
            used.entry(&o.metadata.namespace)
                .or_default()
                .add_volume(spec.size);
        }
    }
    let quotas: Vec<(&str, legion_api::quota::Quota)> = of(Kind::Namespace)
        .iter()
        .filter_map(|o| {
            let spec: NamespaceSpec = legion_api::parse(&o.spec).ok()?;
            Some((o.metadata.name.as_str(), spec.quota?))
        })
        .collect();
    let mut f = m.gauge(
        "legion_quota_limit",
        "A namespace's quota (cpu in cores, memory and storage in bytes).",
    );
    for (ns, q) in &quotas {
        for (resource, limit) in quota_values(q) {
            if let Some(limit) = limit {
                f.sample(&[("namespace", ns), ("resource", resource)], limit);
            }
        }
    }
    let mut f = m.gauge(
        "legion_quota_used",
        "What a namespace with a quota uses of it: placed tasks' requests and volumes.",
    );
    for (ns, q) in &quotas {
        let u = used.get(ns).copied().unwrap_or_default();
        for ((resource, limit), value) in quota_values(q).into_iter().zip(usage_values(&u)) {
            if limit.is_some() {
                f.sample(&[("namespace", ns), ("resource", resource)], value);
            }
        }
    }
    Ok(())
}

fn quota_values(q: &legion_api::quota::Quota) -> [(&'static str, Option<f64>); 5] {
    [
        ("cpu", q.cpu.map(|c| c.0 as f64 / 1000.0)),
        ("memory", q.memory.map(|b| b.0 as f64)),
        ("tasks", q.tasks.map(f64::from)),
        ("volumes", q.volumes.map(f64::from)),
        ("storage", q.storage.map(|b| b.0 as f64)),
    ]
}

fn usage_values(u: &Usage) -> [f64; 5] {
    [
        u.cpu.0 as f64 / 1000.0,
        u.memory.0 as f64,
        f64::from(u.tasks),
        f64::from(u.volumes),
        u.storage.0 as f64,
    ]
}
