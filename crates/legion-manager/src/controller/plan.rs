//! The Service controller and the scheduler as one pure function: from the
//! current Services, Tasks and Nodes, the writes that move the cluster
//! toward what the Services ask for (DESIGN.md §3.3, §3.5).
//!
//! Every call looks at the whole state, so a lost write or a missed event
//! only delays convergence until the next call.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::time::Duration;

use legion_api::node::{Availability, NodeSpec, NodeState, NodeStatus};
use legion_api::quota::{Quota, Usage};
use legion_api::service::FailureAction;
use legion_api::service::{
    Constraint, EndpointMode, NodeAttribute, PUBLISHED_PORTS, PodSpec, Publish, PublishedPort,
    RestartCondition, SERVICE_NET, SERVICE_PREFIX, ServiceMode, ServiceSpec, ServiceStatus,
    TaintEffect, UpdateOrder, UpdateState, UpdateStatus, claim_volume, update_snapshot,
};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus, VolumeSnapshot};
use legion_api::volume::{Access, VolumeDriver, VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use tokio::time::Instant;

mod autoscale;
mod jobs;
mod migrate;
pub use jobs::{CronJob, Job, Jobs};

/// Terminal tasks kept per slot, as Swarm's task history limit.
pub const HISTORY: usize = 5;
/// Longest wait before replacing a failed task.
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Label tying a task to its service, for `legion list tasks -l`.
pub const LABEL_SERVICE: &str = "legion.service";

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Create(Object),
    /// Replace an object last written at `version`.
    Update(Object, u64),
    Delete(ObjectKey, u64),
}

impl Action {
    fn key(&self) -> ObjectKey {
        match self {
            Action::Create(o) | Action::Update(o, _) => o.key(),
            Action::Delete(k, _) => k.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Service {
    pub object: Object,
    pub spec: ServiceSpec,
    /// Platforms (`Platform::runs`) its tasks of its images were rejected
    /// on, as the images offer nothing for them: kept off nodes of them.
    /// Set by [`plan`].
    pub refused: BTreeSet<(String, String, bool)>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub object: Object,
    pub spec: TaskSpec,
    pub status: TaskStatus,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub object: Object,
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

#[derive(Debug, Clone)]
pub struct Volume {
    pub object: Object,
    pub spec: VolumeSpec,
    pub status: VolumeStatus,
}

/// Every Volume, by namespace and name.
pub type Volumes = BTreeMap<(String, String), Volume>;

/// The node holding a Volume's data, once a task has used it, or using a
/// volume only one node may use at a time; none for a shared one.
fn home<'a>(volumes: &'a Volumes, namespace: &str, name: &str) -> Option<&'a str> {
    let v = volumes.get(&(namespace.to_owned(), name.to_owned()))?;
    match (v.spec.access(), v.status.node.as_str()) {
        (Access::Shared, _) | (Access::Single, "") => None,
        (Access::Local, "") => v.spec.node.as_deref(),
        (_, node) => Some(node),
    }
}

/// Namespace quotas, by namespace.
pub type Quotas = BTreeMap<String, Quota>;

impl Node {
    fn name(&self) -> &str {
        &self.object.metadata.name
    }

    /// Whether the node has a taint of `effect` that `s` does not tolerate.
    fn repels(&self, s: &Service, effect: TaintEffect) -> bool {
        self.spec
            .taints
            .iter()
            .any(|t| t.effect == effect && !t.tolerated_by(&s.spec.placement.tolerations))
    }
}

impl Task {
    /// On a node, as far as resources and quotas go: placed and not ended.
    fn placed(&self) -> bool {
        self.status.state != TaskState::Pending && !self.status.state.is_terminal()
    }

    fn active(&self) -> bool {
        self.spec.desired_state == DesiredState::Running && !self.status.state.is_terminal()
    }

    fn stopping(&self) -> bool {
        self.spec.desired_state == DesiredState::Shutdown && !self.status.state.is_terminal()
    }

    fn failed(&self) -> bool {
        matches!(self.status.state, TaskState::Failed | TaskState::Rejected)
    }

    fn uid(&self) -> &str {
        &self.object.metadata.uid
    }

    fn owner(&self) -> (&str, &str) {
        (&self.object.metadata.namespace, &self.spec.service)
    }
}

impl Service {
    fn key(&self) -> (&str, &str) {
        (&self.object.metadata.namespace, &self.object.metadata.name)
    }

    fn owns(&self, t: &Task) -> bool {
        t.owner() == self.key() && t.spec.service_uid == self.object.metadata.uid
    }
}

/// What the controller remembers between calls: when it first saw each
/// task fail, for backoff, and run, for rolling updates' delay and monitor
/// window. A new leader starts afresh, which only makes it wait longer.
#[derive(Debug, Default)]
pub struct Memory {
    pub failed_at: BTreeMap<String, Instant>,
    pub running_at: BTreeMap<String, Instant>,
    /// What tasks use, by `namespace/task`, with the node that reported
    /// it; the controller sets it before each pass.
    pub usage: BTreeMap<String, (String, legion_api::dispatch::TaskUsage)>,
    /// The autoscaler's recent recommendations, by service uid.
    pub recommended: BTreeMap<String, Vec<(Instant, u32)>>,
    /// This leader's first pass: tasks failed by then were judged by the
    /// leader before, which knew how long they ran.
    pub began: Option<Instant>,
}

#[derive(Debug, Default)]
pub struct Plan {
    pub actions: Vec<Action>,
    /// Call again by then, even if nothing changes.
    pub wake: Option<Instant>,
}

impl Plan {
    fn wake_at(&mut self, t: Instant) {
        self.wake = Some(self.wake.map_or(t, |w| w.min(t)));
    }
}

/// Where a task belongs in its service.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Replica(u32),
    Node(String),
}

impl Slot {
    /// How the slot appears in task names and in the service's status.
    fn key(&self) -> String {
        match self {
            Slot::Replica(r) => r.to_string(),
            Slot::Node(node) => node.clone(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn plan(
    services: &[Service],
    jobs: &Jobs,
    tasks: &[Task],
    nodes: &[Node],
    volumes: &Volumes,
    quotas: &Quotas,
    memory: &mut Memory,
    now: Instant,
) -> Plan {
    let mut plan = Plan::default();
    memory.began.get_or_insert(now);
    let (tasks, orphaned) = orphan(tasks, nodes, &mut plan);
    let tasks = tasks.as_slice();
    let services: &[Service] = &refused(services, tasks);
    let first = plan.actions.len();
    let live: BTreeSet<&str> = tasks.iter().map(Task::uid).collect();
    memory
        .failed_at
        .retain(|uid, _| live.contains(uid.as_str()));
    for t in tasks.iter().filter(|t| t.failed()) {
        memory.failed_at.entry(t.uid().to_owned()).or_insert(now);
    }
    memory
        .running_at
        .retain(|uid, _| live.contains(uid.as_str()));
    for t in tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Running)
    {
        memory.running_at.entry(t.uid().to_owned()).or_insert(now);
    }

    // A service gone takes its autoscaling history with it.
    let service_uids: BTreeSet<&str> = services
        .iter()
        .map(|s| s.object.metadata.uid.as_str())
        .collect();
    memory
        .recommended
        .retain(|uid, _| service_uids.contains(uid.as_str()));

    migrate::migrations(services, tasks, nodes, volumes, &mut plan);
    let mut addresses = addresses(services);
    let mut created: Vec<Task> = Vec::new();
    for s in services {
        let mine: Vec<&Task> = tasks.iter().filter(|t| s.owns(t)).collect();
        let before = predecessors(s, tasks);
        let addrs = addresses
            .remove(s.object.metadata.uid.as_str())
            .unwrap_or_default();
        service(
            s,
            &mine,
            &before,
            nodes,
            volumes,
            memory,
            now,
            addrs,
            &mut plan,
            &mut created,
        );
    }

    // Jobs own tasks as services do; the scheduler sees them alike.
    let job_owners: Vec<Service> = jobs.jobs.iter().map(Job::owner).collect();
    let job_owners = refused(&job_owners, tasks);
    let mut jobs_after = jobs.jobs.clone();
    for ((j, owner), after) in jobs.jobs.iter().zip(&job_owners).zip(&mut jobs_after) {
        let mine: Vec<&Task> = tasks.iter().filter(|t| owner.owns(t)).collect();
        let before = predecessors(owner, tasks);
        after.status = jobs::job(
            j,
            owner,
            &mine,
            &before,
            nodes,
            volumes,
            memory,
            now,
            jobs.wall,
            &mut plan,
            &mut created,
        );
    }
    for c in &jobs.cron_jobs {
        jobs::cron_job(c, &jobs_after, now, jobs.wall, &mut plan);
    }
    jobs::abandoned(jobs, &mut plan);
    let owners: Vec<&Service> = services.iter().chain(&job_owners).collect();

    // Tasks whose owner is gone: stop, then delete once stopped.
    for t in tasks.iter().filter(|t| !owners.iter().any(|s| s.owns(t))) {
        if t.active() {
            plan.actions.push(stop(t));
        } else if t.status.state.is_terminal() {
            plan.actions.push(delete(t));
        }
    }

    schedule(&owners, tasks, created, nodes, volumes, quotas, &mut plan);

    // An orphaned task is written once per pass; what else was planned for
    // it waits for the next one.
    let later = plan.actions.split_off(first);
    plan.actions
        .extend(later.into_iter().filter(|a| !orphaned.contains(&a.key())));
    // An object deleted is not also written: that would only have the
    // delete, of the version read, conflict.
    let deleted: BTreeSet<ObjectKey> = plan
        .actions
        .iter()
        .filter(|a| matches!(a, Action::Delete(..)))
        .map(Action::key)
        .collect();
    plan.actions
        .retain(|a| !matches!(a, Action::Update(o, _) if deleted.contains(&o.key())));
    plan
}

/// `services` with the platforms each one's images were found not to run
/// on: tasks of its current images rejected so, on nodes of them as they
/// were then (a node may run more since).
fn refused(services: &[Service], tasks: &[Task]) -> Vec<Service> {
    services
        .iter()
        .map(|s| {
            let images = s.spec.pod.images();
            let refused = tasks
                .iter()
                .filter(|t| s.owns(t) && t.spec.pod.images() == images)
                .filter_map(|t| t.status.wrong_platform.as_ref())
                .map(|p| {
                    let (os, arch, linux) = p.runs();
                    (os.to_owned(), arch.to_owned(), linux)
                })
                .collect();
            Service {
                refused,
                ..s.clone()
            }
        })
        .collect()
}

/// Tasks on a node that is down, or removed, are lost: they are marked
/// orphaned and told to stop, so they leave the endpoints, their slots get
/// new tasks elsewhere, and a node that comes back removes their pods.
/// Returns the tasks as they are after these writes, and the orphaned ones'
/// keys.
fn orphan(tasks: &[Task], nodes: &[Node], plan: &mut Plan) -> (Vec<Task>, BTreeSet<ObjectKey>) {
    let state: BTreeMap<&str, NodeState> =
        nodes.iter().map(|n| (n.name(), n.status.state)).collect();
    let mut keys = BTreeSet::new();
    let tasks = tasks
        .iter()
        .map(|t| {
            if t.status.state.is_terminal() || t.spec.node.is_empty() {
                return t.clone();
            }
            let why = match state.get(t.spec.node.as_str()) {
                Some(NodeState::Down) => "is down",
                // Its certificate is refused: nothing will hear from it.
                None => "was removed",
                Some(_) => return t.clone(),
            };
            let mut t = t.clone();
            t.spec.desired_state = DesiredState::Shutdown;
            t.status = TaskStatus {
                state: TaskState::Orphaned,
                message: format!("node {} {why}", t.spec.node),
                ..Default::default()
            };
            t.object.spec = serde_json::to_value(&t.spec).expect("serialises");
            t.object.status = serde_json::to_value(&t.status).expect("serialises");
            plan.actions
                .push(Action::Update(t.object.clone(), t.object.metadata.version));
            keys.insert(t.object.key());
            t
        })
        .collect();
    (tasks, keys)
}

/// A service's VIP and published ports.
#[derive(Debug, Default)]
struct Addresses {
    vip: Option<Ipv4Addr>,
    published: Vec<PublishedPort>,
    /// Ports the service names that an older service holds.
    conflicts: Vec<String>,
}

/// VIPs and published ports for every service, by uid. What a service
/// already has it keeps; what it newly needs it gets, lowest free first;
/// what it no longer needs is released.
fn addresses(services: &[Service]) -> BTreeMap<&str, Addresses> {
    let previous = |s: &Service| -> ServiceStatus {
        serde_json::from_value(s.object.status.clone()).unwrap_or_default()
    };
    let wants_vip = |s: &Service| {
        s.spec
            .endpoint
            .as_ref()
            .is_some_and(|e| e.mode == EndpointMode::Vip && !e.ports.is_empty())
    };
    let in_range = |ip: Ipv4Addr| {
        let host_bits = 32 - u32::from(SERVICE_PREFIX);
        u32::from(ip) >> host_bits == u32::from(SERVICE_NET) >> host_bits
    };

    // Kept first, in creation order, so a clash cannot move an old one.
    let mut by_uid: Vec<&Service> = services.iter().collect();
    by_uid.sort_by(|a, b| a.object.metadata.uid.cmp(&b.object.metadata.uid));
    let mut out: BTreeMap<&str, Addresses> = BTreeMap::new();
    let mut vips: BTreeSet<Ipv4Addr> = BTreeSet::new();
    let mut ports: BTreeSet<(u16, legion_api::service::Protocol)> = BTreeSet::new();
    // Explicitly published ports are the service's own, the oldest
    // service's when two name the same one; picked ones yield.
    let mut owners: BTreeMap<(u16, legion_api::service::Protocol), &str> = BTreeMap::new();
    for s in &by_uid {
        for p in s.spec.endpoint.iter().flat_map(|e| &e.ports) {
            if let Some(n) = p.published_port {
                owners
                    .entry((n, p.protocol))
                    .or_insert(&s.object.metadata.uid);
                ports.insert((n, p.protocol));
            }
        }
    }
    for s in &by_uid {
        let old = previous(s);
        let mut a = Addresses::default();
        let uid = s.object.metadata.uid.as_str();
        if wants_vip(s)
            && let Some(ip) = old.virtual_ip.as_deref().and_then(|v| v.parse().ok())
            && in_range(ip)
            && vips.insert(ip)
        {
            a.vip = Some(ip);
        }
        for p in s.spec.endpoint.iter().flat_map(|e| &e.ports) {
            if p.publish == Publish::None {
                continue;
            }
            let kept = old
                .published_ports
                .iter()
                .find(|o| o.port == p.port && o.protocol == p.protocol)
                .map(|o| o.published_port);
            let n = match (p.published_port, kept) {
                (Some(n), _) if owners[&(n, p.protocol)] == uid => Some(n),
                (Some(n), _) => {
                    let owner = by_uid
                        .iter()
                        .find(|o| o.object.metadata.uid == owners[&(n, p.protocol)])
                        .expect("an owner is a service");
                    a.conflicts.push(format!(
                        "published port {n}/{} is taken by {}/{}",
                        p.protocol, owner.object.metadata.namespace, owner.object.metadata.name
                    ));
                    None
                }
                (None, Some(n)) if !ports.contains(&(n, p.protocol)) => {
                    ports.insert((n, p.protocol));
                    Some(n)
                }
                _ => None,
            };
            if let Some(n) = n {
                a.published.push(PublishedPort {
                    port: p.port,
                    protocol: p.protocol,
                    published_port: n,
                });
            }
        }
        out.insert(s.object.metadata.uid.as_str(), a);
    }
    // New needs, lowest free first.
    let size = 1u32 << (32 - u32::from(SERVICE_PREFIX));
    let base = u32::from(SERVICE_NET);
    let mut next_vip = (1..size - 1).map(|n| Ipv4Addr::from(base + n));
    for s in &by_uid {
        let a = out.get_mut(s.object.metadata.uid.as_str()).expect("added");
        if wants_vip(s) && a.vip.is_none() {
            a.vip = next_vip.by_ref().find(|ip| !vips.contains(ip));
            if let Some(ip) = a.vip {
                vips.insert(ip);
            }
        }
        for p in s.spec.endpoint.iter().flat_map(|e| &e.ports) {
            // A port named but taken is not swapped for another.
            if p.publish == Publish::None
                || p.published_port.is_some()
                || a.published
                    .iter()
                    .any(|o| o.port == p.port && o.protocol == p.protocol)
            {
                continue;
            }
            if let Some(n) = PUBLISHED_PORTS
                .clone()
                .find(|n| !ports.contains(&(*n, p.protocol)))
            {
                ports.insert((n, p.protocol));
                a.published.push(PublishedPort {
                    port: p.port,
                    protocol: p.protocol,
                    published_port: n,
                });
            }
        }
        // In the order of the spec's ports.
        let order: Vec<(u16, legion_api::service::Protocol)> = s
            .spec
            .endpoint
            .iter()
            .flat_map(|e| &e.ports)
            .map(|p| (p.port, p.protocol))
            .collect();
        a.published
            .sort_by_key(|o| order.iter().position(|k| *k == (o.port, o.protocol)));
    }
    out
}

/// Tasks of another owner of `s`'s name (the one `s` replaced, deleted
/// and created again, or a Job or Service sharing the name) still there. Their names stay taken, and those of `s`'s kind that have
/// not ended hold their slot: what they mount is still theirs.
fn predecessors<'a>(s: &Service, tasks: &'a [Task]) -> Vec<&'a Task> {
    tasks
        .iter()
        .filter(|t| t.owner() == s.key() && !s.owns(t))
        .collect()
}

/// Whether a predecessor's task (see [`predecessors`]) of the owner's kind
/// still runs in `slot`, or has yet to stop.
fn held(before: &[&Task], job: bool, slot: &Slot) -> bool {
    before
        .iter()
        .any(|t| t.spec.job == job && !t.status.state.is_terminal() && slot_of(t) == *slot)
}

#[allow(clippy::too_many_arguments)]
fn service(
    s: &Service,
    tasks: &[&Task],
    before: &[&Task],
    nodes: &[Node],
    volumes: &Volumes,
    memory: &mut Memory,
    now: Instant,
    addrs: Addresses,
    plan: &mut Plan,
    created: &mut Vec<Task>,
) {
    let generation = s.object.metadata.generation;
    let previous: ServiceStatus =
        serde_json::from_value(s.object.status.clone()).unwrap_or_default();
    let mut update = match track_update(s, tasks, &previous, memory, plan) {
        Some(update) => update,
        None => return,
    };
    let mut last_tasks = BTreeMap::new();
    let mut by_slot: BTreeMap<Slot, Vec<&Task>> = BTreeMap::new();
    for t in tasks {
        by_slot.entry(slot_of(t)).or_default().push(t);
    }
    for list in by_slot.values_mut() {
        // Uids are Raft indexes in hex: creation order.
        list.sort_by(|a, b| a.uid().cmp(b.uid()));
    }

    let (replicas, autoscale) = autoscale::replicas(s, tasks, &previous, memory, now);
    let wanted: Vec<Slot> = match s.spec.mode {
        ServiceMode::Global => nodes
            .iter()
            .filter(|n| eligible(n, s, volumes, &shared_volumes(s)).is_ok())
            .map(|n| Slot::Node(n.object.metadata.name.clone()))
            .collect(),
        ServiceMode::Replicated => (1..=replicas).map(Slot::Replica).collect(),
        ServiceMode::Stateful => (0..s.spec.replicas.unwrap_or(1))
            .map(Slot::Replica)
            .collect(),
    };
    let wanted_set: BTreeSet<&Slot> = wanted.iter().collect();
    let stateful = s.spec.mode == ServiceMode::Stateful;
    if stateful {
        claims(s, &wanted, volumes, plan);
    }

    // Slots no longer wanted: stop what runs there and drop the history. A
    // node that only takes no new tasks (paused, suspect, or tainted
    // `NoSchedule`) keeps its global task and history (see
    // [`kept_global`]), and once a task of the current pod there is ready
    // the outdated ones go, as in a wanted slot. A stateful service stops
    // its highest ordinal first, one at a time.
    let mut unwanted: Vec<(&Slot, &Vec<&Task>)> = by_slot
        .iter()
        .filter(|(slot, _)| !wanted_set.contains(slot))
        .collect();
    if stateful {
        unwanted.reverse();
    }
    let mut one_stopping = unwanted
        .iter()
        .any(|(_, list)| list.iter().any(|t| t.stopping()));
    for (slot, list) in unwanted {
        // Kept, with its history; one waiting to be placed never is there,
        // and goes.
        if kept_global(slot, list, nodes, s, volumes) {
            for t in list.iter().filter(|t| t.active() && !t.placed()) {
                plan.actions.push(stop(t));
            }
            let current = |t: &Task| t.active() && t.placed() && t.spec.pod == s.spec.pod;
            if list
                .iter()
                .any(|t| current(t) && t.status.state == TaskState::Running && t.status.ready)
            {
                for t in list
                    .iter()
                    .filter(|t| t.active() && t.placed() && !current(t))
                {
                    plan.actions.push(stop(t));
                }
            }
            continue;
        }
        for t in list {
            if t.active() {
                if stateful {
                    if one_stopping {
                        continue;
                    }
                    one_stopping = true;
                }
                plan.actions.push(stop(t));
            } else if t.status.state.is_terminal() {
                plan.actions.push(delete(t));
            }
        }
    }

    // Replicated tasks on a node whose taints evict them stop now; those on
    // a draining node move under the disruption budget, below.
    let on_node = |t: &Task, f: &dyn Fn(&Node) -> bool| {
        t.active() && t.spec.slot.is_some() && nodes.iter().any(|n| n.name() == t.spec.node && f(n))
    };
    let evicted = |t: &Task| on_node(t, &|n| n.repels(s, TaintEffect::NoExecute));
    // Off a draining node, or with its data moving off its node.
    let draining = |t: &Task| {
        !evicted(t)
            && (on_node(t, &|n| n.spec.availability == Availability::Drain)
                || (on_node(t, &|_| true) && migrate::leaving(t, volumes)))
    };
    // Mounting a Volume whose data stays on its node, or that its node
    // holds, which no other node has until the task lets go.
    let pinned = |t: &Task| {
        t.spec
            .volumes()
            .iter()
            .any(|v| home(volumes, &t.object.metadata.namespace, v) == Some(t.spec.node.as_str()))
    };
    for t in tasks.iter().filter(|t| evicted(t)) {
        plan.actions.push(stop(t));
    }

    // Disruption budget: updates and drains take a ready task away only
    // while fewer than `maxUnavailable` slots lack one; past that, the
    // replacement starts first.
    let up = |t: &Task| {
        t.active() && t.status.state == TaskState::Running && t.status.ready && !evicted(t)
    };
    let unavailable = wanted
        .iter()
        .filter(|slot| !by_slot.get(slot).is_some_and(|l| l.iter().any(|t| up(t))))
        .count();
    let mut disruptions = s
        .spec
        .max_unavailable
        .map_or(usize::MAX, |n| (n as usize).saturating_sub(unavailable));

    // A task is current if it runs the service's pod as it is now. Other
    // spec changes (replicas, placement, update settings) bump the
    // generation too, but replace nothing.
    let current_pod = |t: &Task| t.spec.pod == s.spec.pod;

    // Rolling update budget: slots mid-update count against parallelism,
    // until their new task is ready and has run for the update's delay.
    let delay = s.spec.update.delay.as_std();
    let settling = |t: &Task| {
        if t.status.state != TaskState::Running || !t.status.ready {
            return true;
        }
        let since = memory.running_at.get(t.uid()).copied().unwrap_or(now);
        since + delay > now
    };
    // A stateful service starts a task only once every lower ordinal has a
    // ready one, and updates from its highest ordinal down.
    let ready_below = |slot: &Slot| {
        !stateful
            || wanted
                .iter()
                .take_while(|w| *w != slot)
                .all(|w| by_slot.get(w).is_some_and(|l| l.iter().any(|t| up(t))))
    };

    // Whether a slot with nothing running gets a task: not once its last
    // ended for good under the restart policy. One that ran another pod
    // (an update since, or a rollback) is replaced whatever the policy.
    let policy = &s.spec.pod.restart_policy;
    let replaces = |list: &[&Task]| match list.last() {
        None => true,
        Some(last) => match last.status.state {
            TaskState::Complete => policy.condition == RestartCondition::Any || !current_pod(last),
            TaskState::Shutdown | TaskState::Orphaned => true,
            _ => {
                last.failed() && (policy.condition != RestartCondition::None || !current_pod(last))
            }
        },
    };

    let parallelism = match s.spec.update.parallelism {
        0 => usize::MAX,
        n => n as usize,
    };
    let mut in_flight = 0;
    for slot in &wanted {
        let Some(list) = by_slot.get(slot) else {
            continue;
        };
        // Between an old task stopping and its replacement starting, or a
        // new one failing and its retry, nothing runs in the slot. A
        // stateful slot that waits for a lower ordinal is not under way:
        // the lower one gets the budget, and one whose last task ended for
        // good is not under way either.
        let gap = !list.iter().any(|t| t.active())
            && list.last().is_some_and(|t| !current_pod(t) || t.failed())
            && replaces(list)
            && ready_below(slot);
        if gap || list.iter().any(|t| t.stopping()) {
            in_flight += 1;
        } else if let Some(t) = list
            .iter()
            .find(|t| current_pod(t) && t.active() && settling(t))
        {
            in_flight += 1;
            if let Some(since) = memory.running_at.get(t.uid())
                && t.status.state == TaskState::Running
                && t.status.ready
            {
                plan.wake_at(*since + delay);
            }
        }
    }
    let paused = update.as_ref().is_some_and(|u| u.state.paused());
    let mut budget = if paused {
        0
    } else {
        parallelism.saturating_sub(in_flight)
    };

    let mut order: Vec<&Slot> = wanted.iter().collect();
    if stateful {
        order.reverse();
    }

    // An update that snapshots has its tasks take the snapshot before they
    // run, and its rollback's tasks return to it.
    let step = update.as_ref().filter(|_| stateful).and_then(|u| {
        let name = u.snapshot.clone()?;
        let restore = match u.state {
            UpdateState::Updating | UpdateState::Paused => false,
            UpdateState::RollingBack | UpdateState::RollbackPaused => true,
            UpdateState::Completed | UpdateState::RolledBack => return None,
        };
        Some(VolumeSnapshot { name, restore })
    });
    let new_task = |slot: &Slot, list: &[&Task]| {
        let mut history = list.to_vec();
        history.extend(before.iter().filter(|t| slot_of(t) == *slot));
        let mut t = new_task(s, slot, &history, &previous.last_tasks);
        if step.is_some() {
            t.spec.snapshot = step.clone();
            t.object.spec = serde_json::to_value(&t.spec).expect("spec serialises");
        }
        t
    };

    for slot in order {
        let list: &[&Task] = by_slot.get(slot).map_or(&[], Vec::as_slice);
        let (moving, staying): (Vec<&Task>, Vec<&Task>) = list
            .iter()
            .copied()
            .filter(|t| t.active() && !evicted(t))
            .partition(|t| draining(t));
        let (current, outdated): (Vec<&Task>, Vec<&Task>) =
            staying.into_iter().partition(|t| current_pod(t));
        // Whether stopping these takes the slot's ready task away, and the
        // budget lets it.
        let mut may_stop = |old: &[&Task]| {
            if !old.iter().any(|t| up(t)) {
                return true;
            }
            let ok = disruptions > 0;
            if ok {
                disruptions -= 1;
            }
            ok
        };
        let start_first = s.spec.update.order == UpdateOrder::StartFirst;
        // A stateful task's replacement needs its volumes, and so does one
        // whose data is moving: it can only start once they have, so the
        // task waits for them to be copied, then stops first.
        // So is one whose data stays on its node or is held there: a
        // replacement elsewhere cannot have it until the task lets go.
        let bound = stateful
            || moving
                .iter()
                .any(|t| migrate::leaving(t, volumes) || pinned(t));
        let copied = !bound || moving.iter().all(|t| migrate::copied(t, volumes));

        // Never more than one current task per slot.
        for extra in current.iter().skip(1) {
            plan.actions.push(stop(extra));
        }
        if let Some(new) = current.first() {
            // Started first: the old task goes once the new one is ready.
            // One that cannot be placed before the data has moved has the
            // old task stop once it is copied.
            if new.status.state == TaskState::Running && new.status.ready {
                for old in outdated.iter().chain(&moving) {
                    plan.actions.push(stop(old));
                }
            } else {
                if outdated.iter().any(|t| up(t)) {
                    // One of an update overtaken by another and not ready
                    // only holds room the new one may need: the ready one
                    // stays.
                    for old in outdated.iter().filter(|t| !up(t)) {
                        plan.actions.push(stop(old));
                    }
                }
                if bound && !moving.is_empty() && !new.placed() && copied && may_stop(&moving) {
                    for old in &moving {
                        plan.actions.push(stop(old));
                    }
                } else if !bound {
                    // Off a draining node and not ready: nothing to wait
                    // for.
                    for old in moving.iter().filter(|t| !up(t)) {
                        plan.actions.push(stop(old));
                    }
                }
            }
        } else if !moving.is_empty() {
            // Off a draining node: stopped first if the budget allows,
            // else replaced first, as a failed replacement is, after its
            // backoff. One bound to its data waits for the budget, and for
            // the data to be copied to where it goes. Started first only
            // to keep a ready task: one not ready (an update's new task,
            // say, beside a ready old one) stops first all the same.
            let ready = moving.iter().any(|t| up(t));
            if copied && (bound || !start_first || !ready) && may_stop(&moving) {
                for old in &moving {
                    plan.actions.push(stop(old));
                }
            } else if !bound {
                let failures = list.iter().rev().take_while(|t| t.failed()).count();
                let due = list.last().map_or(now, |last| {
                    backoff(last, failures, policy.delay.as_std(), memory, now)
                });
                if due <= now {
                    created.push(new_task(slot, list));
                } else {
                    plan.wake_at(due);
                }
            }
        } else if !outdated.is_empty() {
            // The budget goes to what moves: a stateful slot the
            // disruption budget holds back, or whose new task could not
            // start before a lower ordinal is ready, leaves it to one
            // below.
            if budget > 0 && ready_below(slot) {
                if !start_first && may_stop(&outdated) {
                    budget -= 1;
                    for old in &outdated {
                        plan.actions.push(stop(old));
                    }
                } else if !stateful {
                    budget -= 1;
                    created.push(new_task(slot, list));
                }
            }
        } else if list.iter().any(|t| t.stopping()) || held(before, false, slot) {
            // Wait for the stopping task (stop-first update, drain, or one
            // of the service this one replaced) to end.
        } else {
            // Nothing runs in the slot: start a task, unless the last one
            // ended for good or the next try is not due yet.
            if replaces(list) && ready_below(slot) {
                let failures = list.iter().rev().take_while(|t| t.failed()).count();
                let due = list.last().map_or(now, |last| {
                    backoff(last, failures, policy.delay.as_std(), memory, now)
                });
                if due <= now {
                    created.push(new_task(slot, list));
                } else {
                    plan.wake_at(due);
                }
            }
        }

        // The slot's newest number, counting the task just created.
        let newest = created
            .iter()
            .filter(|t| t.spec.service_uid == s.object.metadata.uid && slot_of(t) == *slot)
            .chain(list.iter().copied())
            .filter_map(|t| task_number(&t.object.metadata.name))
            .chain(previous.last_tasks.get(&slot.key()).copied())
            .max();
        if let Some(n) = newest {
            last_tasks.insert(slot.key(), n);
        }

        // History: keep the newest terminal tasks of the slot.
        let terminal: Vec<&&Task> = list
            .iter()
            .filter(|t| t.status.state.is_terminal())
            .collect();
        for old in terminal.iter().rev().skip(HISTORY) {
            plan.actions.push(delete(old));
        }
    }

    // A slot keeps its number though no longer wanted, so its next task's
    // name is one no task of it had: a replica's for good, a node's while
    // the node exists (its old pod may still be there when it is again).
    for (key, n) in &previous.last_tasks {
        if s.spec.mode != ServiceMode::Global || nodes.iter().any(|node| node.name() == key) {
            last_tasks.entry(key.clone()).or_insert(*n);
        }
    }

    // The update is done once every slot runs a new task only, ready and
    // past the monitor window.
    if let Some(u) = update.as_mut().filter(|u| u.state.in_progress()) {
        let monitor = s.spec.update.monitor.as_std();
        let mut done = true;
        for slot in &wanted {
            let list: &[&Task] = by_slot.get(slot).map_or(&[], Vec::as_slice);
            if list.iter().any(|t| t.active() && !current_pod(t)) {
                done = false;
                continue;
            }
            let new = list.iter().find(|t| t.active() && current_pod(t));
            match new.and_then(|t| {
                (t.status.state == TaskState::Running && t.status.ready)
                    .then(|| memory.running_at.get(t.uid()).copied())
                    .flatten()
            }) {
                Some(since) if since + monitor <= now => {}
                Some(since) => {
                    done = false;
                    plan.wake_at(since + monitor);
                }
                None => done = false,
            }
        }
        if done {
            u.state = match u.state {
                UpdateState::RollingBack => UpdateState::RolledBack,
                _ => UpdateState::Completed,
            };
        }
    }

    // Status, from the tasks as they are now.
    let running: Vec<&&Task> = tasks
        .iter()
        .filter(|t| t.active() && t.status.state == TaskState::Running)
        .collect();
    let status = ServiceStatus {
        observed_generation: generation,
        desired: wanted.len() as u32,
        running: running.len() as u32,
        ready: running.iter().filter(|t| t.status.ready).count() as u32,
        virtual_ip: addrs.vip.map(|ip| ip.to_string()),
        published_ports: addrs.published,
        conflicts: addrs.conflicts,
        last_tasks,
        spec: Some(s.spec.clone()),
        update,
        autoscale,
    };
    let status = serde_json::to_value(&status).expect("status serialises");
    if s.object.status != status {
        let mut o = s.object.clone();
        o.status = status;
        plan.actions
            .push(Action::Update(o, s.object.metadata.version));
    }
}

/// The Volumes a stateful service's claims ask for, one per claim and
/// wanted ordinal: made when missing, and resized with the claim. They stay
/// when the service shrinks or goes, as the data in them may be wanted
/// again.
fn claims(s: &Service, wanted: &[Slot], volumes: &Volumes, plan: &mut Plan) {
    let (namespace, svc) = s.key();
    for slot in wanted {
        let Slot::Replica(n) = slot else { continue };
        for (claim, src) in s.spec.pod.claims() {
            let name = claim_volume(claim, svc, *n);
            match volumes.get(&(namespace.to_owned(), name.clone())) {
                None => {
                    let spec = VolumeSpec {
                        size: src.size,
                        ..Default::default()
                    };
                    let mut o = Object::new(
                        Kind::Volume,
                        namespace,
                        &name,
                        serde_json::to_value(&spec).expect("serialises"),
                    );
                    o.metadata
                        .labels
                        .insert(LABEL_SERVICE.to_owned(), svc.to_owned());
                    plan.actions.push(Action::Create(o));
                }
                Some(v)
                    if v.spec.size != src.size
                        && v.spec.driver == VolumeDriver::Zfs
                        && v.object
                            .metadata
                            .labels
                            .get(LABEL_SERVICE)
                            .map(String::as_str)
                            == Some(svc) =>
                {
                    let mut o = v.object.clone();
                    o.spec = serde_json::to_value(VolumeSpec {
                        size: src.size,
                        ..v.spec.clone()
                    })
                    .expect("serialises");
                    plan.actions
                        .push(Action::Update(o, v.object.metadata.version));
                }
                Some(_) => {}
            }
        }
    }
}

/// The service's rolling update after this pass: started when the pod in
/// the spec changed since the last pass (a rollback when it went back to
/// the spec the update came from), and stopped by a new task failing
/// before it outlived the monitor window, as `failureAction` says. `None`
/// when the service's spec was just written back for a rollback, which the
/// next pass picks up.
fn track_update(
    s: &Service,
    tasks: &[&Task],
    previous: &ServiceStatus,
    memory: &Memory,
    plan: &mut Plan,
) -> Option<Option<UpdateStatus>> {
    let mut update = previous.update.clone();
    if let Some(old) = previous.spec.as_ref()
        && old.pod != s.spec.pod
    {
        // Back to the spec before a forward update. Going to the spec a
        // rollback undoes, under way or done, is a new update: the
        // rollback's tasks have returned to its snapshot, or drop it.
        let back = update.as_ref().is_some_and(|u| {
            u.previous == s.spec
                && matches!(
                    u.state,
                    UpdateState::Updating | UpdateState::Paused | UpdateState::Completed
                )
        });
        // A rollback returns to the snapshot of the update it undoes.
        let snapshot = match &update {
            Some(u) if back => u.snapshot.clone(),
            _ => (s.spec.mode == ServiceMode::Stateful && s.spec.update.snapshot)
                .then(|| update_snapshot(s.object.metadata.generation)),
        };
        update = Some(UpdateStatus {
            state: if back {
                UpdateState::RollingBack
            } else {
                UpdateState::Updating
            },
            message: String::new(),
            generation: s.object.metadata.generation,
            previous: old.clone(),
            snapshot,
        });
    }
    let Some(u) = update.as_mut().filter(|u| u.state.in_progress()) else {
        return Some(update);
    };
    let monitor = s.spec.update.monitor.as_std();
    let failed = tasks.iter().find(|t| {
        t.spec.service_generation >= u.generation
            && t.spec.pod == s.spec.pod
            && t.failed()
            && match (
                memory.running_at.get(t.uid()),
                memory.failed_at.get(t.uid()),
            ) {
                (Some(ran), Some(failed)) => *failed < *ran + monitor,
                // Failed before this leader's time: not news.
                (None, Some(failed)) => memory.began.is_none_or(|b| *failed > b),
                _ => true,
            }
    });
    let Some(t) = failed else {
        return Some(update);
    };
    let why = match t.status.message.as_str() {
        "" => format!("task {} failed", t.object.metadata.name),
        m => format!("task {} failed: {m}", t.object.metadata.name),
    };
    match (u.state, s.spec.update.failure_action) {
        (UpdateState::RollingBack, _) => {
            u.state = UpdateState::RollbackPaused;
            u.message = format!("rollback paused: {why}");
        }
        (_, FailureAction::Continue) => {}
        (_, FailureAction::Pause) => {
            u.state = UpdateState::Paused;
            u.message = format!("update paused: {why}");
        }
        (_, FailureAction::Rollback) => {
            // The spec goes back as it was, and the status with it: the
            // rollback is under way, from the spec it undoes, and the next
            // pass sees no change to track.
            u.state = UpdateState::RollingBack;
            u.message = format!("rolled back: {why}");
            let back = std::mem::replace(&mut u.previous, s.spec.clone());
            let mut o = s.object.clone();
            o.spec = serde_json::to_value(&back).expect("serialises");
            let mut status = previous.clone();
            status.spec = Some(back);
            status.update = update;
            o.status = serde_json::to_value(&status).expect("serialises");
            plan.actions
                .push(Action::Update(o, s.object.metadata.version));
            return None;
        }
    }
    Some(update)
}

/// When a slot whose last task failed gets a new one: the restart delay
/// (at least a second), doubled for each failure in a row, counted from
/// when the failure was first seen.
fn backoff(
    last: &Task,
    failures: usize,
    delay: Duration,
    memory: &Memory,
    now: Instant,
) -> Instant {
    if failures == 0 {
        return now;
    }
    let base = delay.max(Duration::from_secs(1));
    let wait = base
        .saturating_mul(1 << (failures - 1).min(16))
        .min(MAX_BACKOFF);
    memory.failed_at.get(last.uid()).copied().unwrap_or(now) + wait
}

/// The `n` of a task named `<service>-<slot>-<n>`.
fn task_number(name: &str) -> Option<u64> {
    name.rsplit('-').next()?.parse().ok()
}

/// The message of a task stopped before it was placed: one a global
/// service or job names its node for from the start, though it never ran
/// there.
pub const NEVER_PLACED: &str = "stopped before it was scheduled";

fn slot_of(t: &Task) -> Slot {
    match t.spec.slot {
        Some(n) => Slot::Replica(n),
        None => Slot::Node(t.spec.node.clone()),
    }
}

/// A new task for `slot`, named `<service>-<slot>-<n>` with `n` counting up
/// within the slot, past every task it has had (`last`, from the service's
/// status, remembers deleted ones).
/// `<service>-<slot>-<n>`. Past the 63 bytes a name may have, the front
/// is cut and a hash of what was cut off keeps it apart from other
/// services' and slots' (long names once took each other's).
fn task_name(svc: &str, slot: &str, n: u64) -> String {
    let base = format!("{svc}-{slot}");
    let suffix = format!("-{n}");
    if base.len() + suffix.len() <= 63 {
        return format!("{base}{suffix}");
    }
    use sha2::Digest;
    let digest = sha2::Sha256::digest(base.as_bytes());
    let hash: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
    let keep = 63 - suffix.len() - hash.len() - 1;
    format!("{}-{hash}{suffix}", base[..keep].trim_end_matches('-'))
}

fn new_task(s: &Service, slot: &Slot, history: &[&Task], last: &BTreeMap<String, u64>) -> Task {
    let svc = &s.object.metadata.name;
    let n = history
        .iter()
        .filter_map(|t| task_number(&t.object.metadata.name))
        .chain(last.get(&slot.key()).copied())
        .max()
        .unwrap_or(0)
        + 1;
    let (slot_part, replica, node) = match slot {
        Slot::Replica(r) => (r.to_string(), Some(*r), String::new()),
        Slot::Node(node) => (node.clone(), None, node.clone()),
    };
    let name = task_name(svc, &slot_part, n);
    let spec = TaskSpec {
        service: svc.clone(),
        service_uid: s.object.metadata.uid.clone(),
        service_generation: s.object.metadata.generation,
        job: false,
        slot: replica,
        stateful: s.spec.mode == ServiceMode::Stateful,
        node,
        desired_state: DesiredState::Running,
        pod: s.spec.pod.clone(),
        snapshot: None,
    };
    let mut object = Object::new(
        Kind::Task,
        &s.object.metadata.namespace,
        &name,
        serde_json::to_value(&spec).expect("spec serialises"),
    );
    object
        .metadata
        .labels
        .insert(LABEL_SERVICE.to_owned(), svc.clone());
    Task {
        object,
        spec,
        status: TaskStatus::default(),
    }
}

fn stop(t: &Task) -> Action {
    let mut o = t.object.clone();
    o.spec["desiredState"] = serde_json::to_value(DesiredState::Shutdown).expect("serialises");
    Action::Update(o, t.object.metadata.version)
}

fn delete(t: &Task) -> Action {
    Action::Delete(t.object.key(), t.object.metadata.version)
}

/// The Volume objects a pod mounts that are not a stateful task's own.
pub(super) fn shared_volumes(s: &Service) -> Vec<String> {
    s.spec
        .pod
        .volumes
        .iter()
        .filter_map(|v| Some(v.volume.as_ref()?.name.clone()))
        .collect()
}

/// Whether the global tasks of `s` in `slot` (`list`) stay, though their
/// node takes no new one: it is paused, suspect or tainted `NoSchedule`,
/// which keep new tasks off and no more. They go from a node drained, down
/// or vanished, one whose taints evict them or that the service may no
/// longer run on, one the data they mount is no longer on, or once that
/// data, moving off it, has been copied.
fn kept_global(
    slot: &Slot,
    list: &[&Task],
    nodes: &[Node],
    s: &Service,
    volumes: &Volumes,
) -> bool {
    let Slot::Node(name) = slot else {
        return false;
    };
    let Some(n) = nodes.iter().find(|n| n.name() == name) else {
        return false;
    };
    let namespace = &s.object.metadata.namespace;
    let (os, arch, linux) = n.status.platform.runs();
    matches!(n.status.state, NodeState::Ready | NodeState::Suspect)
        && !s.refused.contains(&(os.to_owned(), arch.to_owned(), linux))
        && n.spec.availability != Availability::Drain
        && !n.repels(s, TaintEffect::NoExecute)
        && !(n.status.platform.no_limits && s.spec.pod.limited())
        && constraints_hold(s, n)
        && shared_volumes(s)
            .iter()
            .filter_map(|v| home(volumes, namespace, v))
            .all(|home| home == name)
        && !list
            .iter()
            .any(|t| t.active() && migrate::leaving(t, volumes) && migrate::copied(t, volumes))
}

fn constraints_hold(s: &Service, n: &Node) -> bool {
    s.spec
        .placement
        .constraints
        .iter()
        .all(|c| c.parse::<Constraint>().is_ok_and(|c| matches(&c, n)))
}

/// Whether `n` may run tasks of `s` that mount the Volumes `mounts` at
/// all; requests are checked when placing.
fn eligible(
    n: &Node,
    s: &Service,
    volumes: &Volumes,
    mounts: &[String],
) -> Result<(), &'static str> {
    if n.status.state != NodeState::Ready {
        return Err("not ready");
    }
    if n.spec.availability != Availability::Active {
        return Err("not active");
    }
    if n.repels(s, TaintEffect::NoSchedule) || n.repels(s, TaintEffect::NoExecute) {
        return Err("taints");
    }
    if n.status.platform.no_limits && s.spec.pod.limited() {
        return Err("no limits (kern.racct.enable)");
    }
    let (os, arch, linux) = n.status.platform.runs();
    if s.refused.contains(&(os.to_owned(), arch.to_owned(), linux)) {
        return Err("no image for its platform");
    }
    let namespace = &s.object.metadata.namespace;
    // A task starting now would hold the data where it no longer stays.
    let moving = mounts.iter().any(|v| {
        volumes
            .get(&(namespace.clone(), v.clone()))
            .is_some_and(|v| v.status.migration.is_some())
    });
    if moving {
        return Err("volume moving");
    }
    let elsewhere = mounts
        .iter()
        .filter_map(|v| home(volumes, namespace, v))
        .any(|home| home != n.name());
    if elsewhere {
        return Err("volume on another node");
    }
    if !constraints_hold(s, n) {
        return Err("constraints");
    }
    Ok(())
}

fn matches(c: &Constraint, n: &Node) -> bool {
    let actual = match &c.attribute {
        NodeAttribute::Name => Some(n.object.metadata.name.as_str()),
        NodeAttribute::Hostname => Some(n.status.hostname.as_str()),
        NodeAttribute::Role => Some(n.spec.role.as_str()),
        NodeAttribute::Os => Some(n.status.platform.os.as_str()),
        NodeAttribute::Arch => Some(n.status.platform.arch.as_str()),
        NodeAttribute::Linux => Some(if n.status.platform.linux {
            "true"
        } else {
            "false"
        }),
        NodeAttribute::Label(k) => n.object.metadata.labels.get(k).map(String::as_str),
    };
    (actual == Some(c.value.as_str())) == c.equal
}

/// The values of the labels `keys` on `n`, for a spread preference.
fn group<'a>(n: &'a Node, keys: &[&str]) -> Vec<Option<&'a String>> {
    keys.iter()
        .map(|k| n.object.metadata.labels.get(*k))
        .collect()
}

/// Requests placed on a node, and its tasks per owner (Service or Job, by
/// uid: one of each may share a name).
#[derive(Debug, Default, Clone)]
struct Load {
    cpu: u64,
    memory: u64,
    per_service: BTreeMap<String, u32>,
}

impl Load {
    fn add(&mut self, t: &Task) {
        let r = t.spec.pod.requests();
        self.cpu = self.cpu.saturating_add(r.cpu.0);
        self.memory = self.memory.saturating_add(r.memory.0);
        *self
            .per_service
            .entry(t.spec.service_uid.clone())
            .or_default() += 1;
    }
}

/// Lower is better: an untolerated `PreferNoSchedule` taint, tasks of the
/// same service in the node's group for each spread preference, on the
/// node, then requested CPU and memory in thousandths.
type Score = (bool, Vec<u32>, u32, u64, u64);

/// A container of `pod` that requests nothing of what `quota` limits, CPU
/// or memory, and which.
fn unrequested<'a>(quota: &Quota, pod: &'a PodSpec) -> Option<(&'a str, &'static str)> {
    pod.containers().find_map(|c| {
        let r = &c.resources.requests;
        if quota.cpu.is_some() && r.cpu.is_none_or(|n| n.0 == 0) {
            Some((c.name.as_str(), "cpu"))
        } else if quota.memory.is_some() && r.memory.is_none_or(|n| n.0 == 0) {
            Some((c.name.as_str(), "memory"))
        } else {
            None
        }
    })
}

/// Places pending tasks: hold those their namespace's quota has no room
/// for, filter nodes (eligible, requests fit), then prefer a node without
/// an untolerated `PreferNoSchedule` taint, then one whose group under
/// each spread preference in turn (nodes with the same values of its
/// label and the ones before it) has the fewest tasks of the same
/// service, then the node with the fewest, then the least requested. New
/// tasks are created already placed; existing pending ones are updated. A
/// global task names its node from the start, but is pending, and not
/// sent to the agent, until placed there.
#[allow(clippy::too_many_arguments)]
fn schedule(
    services: &[&Service],
    tasks: &[Task],
    created: Vec<Task>,
    nodes: &[Node],
    volumes: &Volumes,
    quotas: &Quotas,
    plan: &mut Plan,
) {
    let mut load: BTreeMap<String, Load> = BTreeMap::new();
    let mut used: BTreeMap<String, Usage> = BTreeMap::new();
    for t in tasks.iter().filter(|t| t.placed()) {
        load.entry(t.spec.node.clone()).or_default().add(t);
        used.entry(t.object.metadata.namespace.clone())
            .or_default()
            .add_tasks(t.spec.pod.requests(), 1);
    }

    // Never-placed tasks told to stop have no agent to report for them.
    for t in tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Pending && t.stopping())
    {
        let mut o = t.object.clone();
        o.status = serde_json::to_value(TaskStatus {
            state: TaskState::Shutdown,
            message: NEVER_PLACED.into(),
            ..Default::default()
        })
        .expect("serialises");
        plan.actions
            .push(Action::Update(o, t.object.metadata.version));
    }

    let touched: BTreeSet<ObjectKey> = plan.actions.iter().map(Action::key).collect();
    let existing = tasks.iter().filter(|t| {
        t.active() && t.status.state == TaskState::Pending && !touched.contains(&t.object.key())
    });
    let pending: Vec<(Task, Option<&Task>)> = existing
        .map(|t| (t.clone(), Some(t)))
        .chain(created.into_iter().map(|t| (t, None)))
        .collect();

    // Volumes one node at a time may have, not yet on one, that a placed
    // task took, in this pass or before its node claimed them: other
    // tasks of theirs go to the same node.
    let mut taken: BTreeMap<(String, String), String> = BTreeMap::new();
    let take = |taken: &mut BTreeMap<_, _>, t: &Task| {
        for v in t.spec.volumes() {
            let key = (t.object.metadata.namespace.clone(), v);
            if volumes
                .get(&key)
                .is_some_and(|v| v.spec.access() != Access::Shared)
                && home(volumes, &key.0, &key.1).is_none()
            {
                taken.insert(key, t.spec.node.clone());
            }
        }
    };
    for t in tasks.iter().filter(|t| t.placed()) {
        take(&mut taken, t);
    }
    for (mut t, original) in pending {
        let Some(s) = services.iter().find(|s| s.owns(&t)) else {
            continue;
        };
        let requests = t.spec.pod.requests();
        let namespace = t.object.metadata.namespace.clone();
        let before = used.get(&namespace).copied().unwrap_or_default();
        let mut after = before;
        after.add_tasks(requests, 1);
        let over = quotas
            .get(&namespace)
            .map(|q| q.exceeded(&before, &after))
            .unwrap_or_default();
        // One asking for nothing a quota limits would run outside it: from
        // before the quota, or past admission while it was set.
        let unasked = quotas
            .get(&namespace)
            .and_then(|q| unrequested(q, &t.spec.pod));
        let owner = &s.object.metadata.uid;
        let on = |node: &str| {
            load.get(node)
                .and_then(|l| l.per_service.get(owner))
                .copied()
                .unwrap_or(0)
        };
        let spread: Vec<&str> = s
            .spec
            .placement
            .preferences
            .iter()
            .map(|p| p.label())
            .collect();
        let mut reasons: BTreeMap<&str, usize> = BTreeMap::new();
        let mut best: Option<(&str, Score)> = None;
        for n in nodes
            .iter()
            .filter(|_| over.is_empty() && unasked.is_none())
        {
            let name = n.object.metadata.name.as_str();
            if !t.spec.node.is_empty() && t.spec.node != name {
                continue;
            }
            if let Err(why) = eligible(n, s, volumes, &t.spec.volumes()) {
                *reasons.entry(why).or_default() += 1;
                continue;
            }
            if t.spec.volumes().into_iter().any(|v| {
                taken
                    .get(&(namespace.clone(), v))
                    .is_some_and(|node| node != name)
            }) {
                *reasons.entry("volume on another node").or_default() += 1;
                continue;
            }
            let l = load.get(name).cloned().unwrap_or_default();
            let alloc = n.status.allocatable;
            // A sum past u64 fits nowhere.
            let exceeds = |load: u64, request: u64, alloc: u64| {
                alloc > 0 && load.checked_add(request).is_none_or(|sum| sum > alloc)
            };
            if exceeds(l.cpu, requests.cpu.0, alloc.cpu.0)
                || exceeds(l.memory, requests.memory.0, alloc.memory.0)
            {
                *reasons.entry("insufficient resources").or_default() += 1;
                continue;
            }
            let groups = (1..=spread.len())
                .map(|depth| {
                    let mine = group(n, &spread[..depth]);
                    nodes
                        .iter()
                        .filter(|m| group(m, &spread[..depth]) == mine)
                        .map(|m| on(m.name()))
                        .sum()
                })
                .collect();
            let score = (
                n.repels(s, TaintEffect::PreferNoSchedule),
                groups,
                on(name),
                l.cpu.saturating_mul(1000) / alloc.cpu.0.max(1),
                l.memory.saturating_mul(1000) / alloc.memory.0.max(1),
            );
            if best.as_ref().is_none_or(|(_, b)| score < *b) {
                best = Some((name, score));
            }
        }
        t.status = match best {
            Some((name, _)) => {
                t.spec.node = name.to_owned();
                load.entry(name.to_owned()).or_default().add(&t);
                take(&mut taken, &t);
                used.insert(namespace, after);
                TaskStatus {
                    state: TaskState::Assigned,
                    ..Default::default()
                }
            }
            None if let Some((container, what)) = unasked => TaskStatus {
                state: TaskState::Pending,
                message: format!(
                    "namespace {namespace} has a {what} quota: container {container} must \
                     request {what}"
                ),
                ..Default::default()
            },
            None if !over.is_empty() => TaskStatus {
                state: TaskState::Pending,
                message: format!(
                    "over the quota of namespace {namespace}: {}",
                    legion_api::quota::describe(&over)
                ),
                ..Default::default()
            },
            None => {
                let why = if reasons.is_empty() {
                    "no nodes".to_owned()
                } else {
                    let r: Vec<String> = reasons.iter().map(|(r, n)| format!("{n} {r}")).collect();
                    r.join(", ")
                };
                TaskStatus {
                    state: TaskState::Pending,
                    message: format!("no node can run it: {why}"),
                    ..Default::default()
                }
            }
        };
        let mut o = t.object.clone();
        o.spec = serde_json::to_value(&t.spec).expect("serialises");
        o.status = serde_json::to_value(&t.status).expect("serialises");
        match original {
            None => plan.actions.push(Action::Create(o)),
            Some(orig) if orig.object != o => {
                plan.actions
                    .push(Action::Update(o, orig.object.metadata.version));
            }
            Some(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_task_names_stay_apart() {
        assert_eq!(task_name("web", "2", 7), "web-2-7");
        let long = |end: &str| format!("{}{end}", "s".repeat(58));
        let (a, b) = (task_name(&long("a"), "1", 1), task_name(&long("b"), "1", 1));
        assert_ne!(a, b);
        // A node's name as the slot, as global services have.
        let node = "n".repeat(63);
        let c = task_name("web", &node, 12);
        for name in [&a, &b, &c] {
            legion_api::validate_name(name).unwrap();
            assert_eq!(task_number(name), Some(if *name == c { 12 } else { 1 }));
        }
    }
}
