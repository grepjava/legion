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
    Constraint, EndpointMode, NodeAttribute, PUBLISHED_PORTS, Publish, PublishedPort,
    RestartCondition, SERVICE_NET, SERVICE_PREFIX, ServiceMode, ServiceSpec, ServiceStatus,
    TaintEffect, UpdateOrder, UpdateState, UpdateStatus, claim_volume,
};
use legion_api::task::{DesiredState, TaskSpec, TaskState, TaskStatus};
use legion_api::volume::{VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use tokio::time::Instant;

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

/// The node holding a Volume's data, once a task has used it.
fn home<'a>(volumes: &'a Volumes, namespace: &str, name: &str) -> Option<&'a str> {
    volumes
        .get(&(namespace.to_owned(), name.to_owned()))
        .map(|v| v.status.node.as_str())
        .filter(|n| !n.is_empty())
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
    let (tasks, orphaned) = orphan(tasks, nodes, &mut plan);
    let tasks = tasks.as_slice();
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

    migrate::migrations(services, tasks, nodes, volumes, &mut plan);
    let mut addresses = addresses(services);
    let mut created: Vec<Task> = Vec::new();
    for s in services {
        let mine: Vec<&Task> = tasks.iter().filter(|t| s.owns(t)).collect();
        let addrs = addresses
            .remove(s.object.metadata.uid.as_str())
            .unwrap_or_default();
        service(
            s,
            &mine,
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
    for (j, owner) in jobs.jobs.iter().zip(&job_owners) {
        let mine: Vec<&Task> = tasks.iter().filter(|t| owner.owns(t)).collect();
        jobs::job(
            j,
            owner,
            &mine,
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
        jobs::cron_job(c, &jobs.jobs, now, jobs.wall, &mut plan);
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
    plan
}

/// Tasks on a node that is down are lost: they are marked orphaned and told
/// to stop, so they leave the endpoints, their slots get new tasks
/// elsewhere, and a node that comes back removes their pods. Returns the
/// tasks as they are after these writes, and the orphaned ones' keys.
fn orphan(tasks: &[Task], nodes: &[Node], plan: &mut Plan) -> (Vec<Task>, BTreeSet<ObjectKey>) {
    let down: BTreeSet<&str> = nodes
        .iter()
        .filter(|n| n.status.state == NodeState::Down)
        .map(Node::name)
        .collect();
    let mut keys = BTreeSet::new();
    let tasks = tasks
        .iter()
        .map(|t| {
            if t.status.state.is_terminal() || !down.contains(t.spec.node.as_str()) {
                return t.clone();
            }
            let mut t = t.clone();
            t.spec.desired_state = DesiredState::Shutdown;
            t.status = TaskStatus {
                state: TaskState::Orphaned,
                message: format!("node {} is down", t.spec.node),
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

#[allow(clippy::too_many_arguments)]
fn service(
    s: &Service,
    tasks: &[&Task],
    nodes: &[Node],
    volumes: &Volumes,
    memory: &Memory,
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

    let wanted: Vec<Slot> = match s.spec.mode {
        ServiceMode::Global => nodes
            .iter()
            .filter(|n| eligible(n, s, volumes, &shared_volumes(s)).is_ok())
            .map(|n| Slot::Node(n.object.metadata.name.clone()))
            .collect(),
        ServiceMode::Replicated => (1..=s.spec.replicas.unwrap_or(1))
            .map(Slot::Replica)
            .collect(),
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
    // paused node keeps its global task; a drained or vanished one does
    // not, nor one whose taints evict it. A stateful service stops its
    // highest ordinal first, one at a time.
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
        let paused = matches!(slot, Slot::Node(name) if nodes.iter().any(|n| {
            n.name() == name
                && n.spec.availability == Availability::Pause
                && !n.repels(s, TaintEffect::NoExecute)
        }));
        for t in list {
            if t.active() && !paused {
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
    let draining =
        |t: &Task| !evicted(t) && on_node(t, &|n| n.spec.availability == Availability::Drain);
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
        // new one failing and its retry, nothing runs in the slot.
        let gap = !list.iter().any(|t| t.active())
            && list.last().is_some_and(|t| !current_pod(t) || t.failed());
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

    // A stateful service starts a task only once every lower ordinal has a
    // ready one, and updates from its highest ordinal down.
    let ready_below = |slot: &Slot| {
        !stateful
            || wanted
                .iter()
                .take_while(|w| *w != slot)
                .all(|w| by_slot.get(w).is_some_and(|l| l.iter().any(|t| up(t))))
    };
    let mut order: Vec<&Slot> = wanted.iter().collect();
    if stateful {
        order.reverse();
    }

    let policy = &s.spec.pod.restart_policy;
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

        // Never more than one current task per slot.
        for extra in current.iter().skip(1) {
            plan.actions.push(stop(extra));
        }
        if let Some(new) = current.first() {
            // Started first: the old task goes once the new one is ready.
            if new.status.state == TaskState::Running && new.status.ready {
                for old in outdated.iter().chain(&moving) {
                    plan.actions.push(stop(old));
                }
            }
        } else if !moving.is_empty() {
            // Off a draining node: stopped first if the budget allows,
            // else replaced first, as a failed replacement is, after its
            // backoff. A stateful task's replacement needs its volumes, so
            // it waits for the budget, and for them to be copied to where
            // they go.
            let copied = !stateful || moving.iter().all(|t| migrate::copied(t, volumes));
            if copied && !start_first && may_stop(&moving) {
                for old in &moving {
                    plan.actions.push(stop(old));
                }
            } else if !stateful {
                let failures = list.iter().rev().take_while(|t| t.failed()).count();
                let due = list.last().map_or(now, |last| {
                    backoff(last, failures, policy.delay.as_std(), memory, now)
                });
                if due <= now {
                    created.push(new_task(s, slot, list, &previous.last_tasks));
                } else {
                    plan.wake_at(due);
                }
            }
        } else if !outdated.is_empty() {
            if budget > 0 {
                budget -= 1;
                if !start_first && may_stop(&outdated) {
                    for old in &outdated {
                        plan.actions.push(stop(old));
                    }
                } else if !stateful {
                    created.push(new_task(s, slot, list, &previous.last_tasks));
                }
            }
        } else if list.iter().any(|t| t.stopping()) {
            // Wait for the stopping task (stop-first update, drain) to end.
        } else {
            // Nothing runs in the slot: start a task, unless the last one
            // ended for good or the next try is not due yet.
            let replace = match list.last() {
                None => true,
                Some(last) => match last.status.state {
                    TaskState::Complete => policy.condition == RestartCondition::Any,
                    TaskState::Shutdown | TaskState::Orphaned => true,
                    _ => last.failed() && policy.condition != RestartCondition::None,
                },
            };
            if replace && ready_below(slot) {
                let failures = list.iter().rev().take_while(|t| t.failed()).count();
                let due = list.last().map_or(now, |last| {
                    backoff(last, failures, policy.delay.as_std(), memory, now)
                });
                if due <= now {
                    created.push(new_task(s, slot, list, &previous.last_tasks));
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

    // A node's slot keeps its number while the node exists, though no
    // longer wanted: its old pod may still be there when it is again.
    if s.spec.mode == ServiceMode::Global {
        for (key, n) in &previous.last_tasks {
            if nodes.iter().any(|node| node.name() == key) {
                last_tasks.entry(key.clone()).or_insert(*n);
            }
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
        let back = update.as_ref().is_some_and(|u| u.previous == s.spec);
        let message = match &update {
            Some(u) if back && u.state == UpdateState::RollingBack => u.message.clone(),
            _ => String::new(),
        };
        update = Some(UpdateStatus {
            state: if back {
                UpdateState::RollingBack
            } else {
                UpdateState::Updating
            },
            message,
            generation: s.object.metadata.generation,
            previous: old.clone(),
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
            // The spec goes back as it was; the status keeps the spec seen
            // last, so the next pass sees the change as a rollback.
            u.state = UpdateState::RollingBack;
            u.message = format!("rolled back: {why}");
            let mut o = s.object.clone();
            o.spec = serde_json::to_value(&u.previous).expect("serialises");
            let mut status = previous.clone();
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

fn slot_of(t: &Task) -> Slot {
    match t.spec.slot {
        Some(n) => Slot::Replica(n),
        None => Slot::Node(t.spec.node.clone()),
    }
}

/// A new task for `slot`, named `<service>-<slot>-<n>` with `n` counting up
/// within the slot, past every task it has had (`last`, from the service's
/// status, remembers deleted ones).
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
    let suffix = format!("-{slot_part}-{n}");
    let keep = 63usize.saturating_sub(suffix.len()).min(svc.len());
    let name = format!("{}{suffix}", svc[..keep].trim_end_matches('-'));
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
    let namespace = &s.object.metadata.namespace;
    let finishing = mounts.iter().any(|v| {
        volumes
            .get(&(namespace.clone(), v.clone()))
            .and_then(|v| v.status.migration.as_ref())
            .is_some_and(|m| m.phase == legion_api::volume::MigrationPhase::Finishing)
    });
    if finishing {
        return Err("volume moving");
    }
    let elsewhere = mounts
        .iter()
        .filter_map(|v| home(volumes, namespace, v))
        .any(|home| home != n.name());
    if elsewhere {
        return Err("volume on another node");
    }
    let hold = s
        .spec
        .placement
        .constraints
        .iter()
        .all(|c| c.parse::<Constraint>().is_ok_and(|c| matches(&c, n)));
    if !hold {
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
        NodeAttribute::Label(k) => n.object.metadata.labels.get(k).map(String::as_str),
    };
    (actual == Some(c.value.as_str())) == c.equal
}

/// Requests placed on a node, and its tasks per service.
#[derive(Debug, Default, Clone)]
struct Load {
    cpu: u64,
    memory: u64,
    per_service: BTreeMap<(String, String), u32>,
}

impl Load {
    fn add(&mut self, t: &Task) {
        let r = t.spec.pod.requests();
        self.cpu += r.cpu.0;
        self.memory += r.memory.0;
        let (ns, svc) = t.owner();
        *self
            .per_service
            .entry((ns.to_owned(), svc.to_owned()))
            .or_default() += 1;
    }
}

/// Lower is better: an untolerated `PreferNoSchedule` taint, tasks of the
/// same service, then requested CPU and memory in thousandths.
type Score = (bool, u32, u64, u64);

/// Places pending tasks: hold those their namespace's quota has no room
/// for, filter nodes (eligible, requests fit), then prefer a node without
/// an untolerated `PreferNoSchedule` taint, then the one with the fewest
/// tasks of the same service, then the least requested. New tasks are
/// created already placed; existing pending ones are updated. A global
/// task names its node from the start, but is pending, and not sent to
/// the agent, until placed there.
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
            message: "stopped before it was scheduled".into(),
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
        let mut reasons: BTreeMap<&str, usize> = BTreeMap::new();
        let mut best: Option<(&str, Score)> = None;
        for n in nodes.iter().filter(|_| over.is_empty()) {
            let name = n.object.metadata.name.as_str();
            if !t.spec.node.is_empty() && t.spec.node != name {
                continue;
            }
            if let Err(why) = eligible(n, s, volumes, &t.spec.volumes()) {
                *reasons.entry(why).or_default() += 1;
                continue;
            }
            let l = load.get(name).cloned().unwrap_or_default();
            let alloc = n.status.allocatable;
            if (alloc.cpu.0 > 0 && l.cpu + requests.cpu.0 > alloc.cpu.0)
                || (alloc.memory.0 > 0 && l.memory + requests.memory.0 > alloc.memory.0)
            {
                *reasons.entry("insufficient resources").or_default() += 1;
                continue;
            }
            let same = l
                .per_service
                .get(&(s.key().0.to_owned(), s.key().1.to_owned()))
                .copied()
                .unwrap_or(0);
            let score = (
                n.repels(s, TaintEffect::PreferNoSchedule),
                same,
                l.cpu * 1000 / alloc.cpu.0.max(1),
                l.memory * 1000 / alloc.memory.0.max(1),
            );
            if best.as_ref().is_none_or(|(_, b)| score < *b) {
                best = Some((name, score));
            }
        }
        t.status = match best {
            Some((name, _)) => {
                t.spec.node = name.to_owned();
                load.entry(name.to_owned()).or_default().add(&t);
                used.insert(namespace, after);
                TaskStatus {
                    state: TaskState::Assigned,
                    ..Default::default()
                }
            }
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
