//! Checks before disruption (CONSOLE.md phase 11, `legion_api::dryrun`):
//! the controller run over two copies of the cluster, as it is and with
//! the change, each until it settles ([`Sim`]: agents start and stop what
//! they are told, the migrator finishes each phase of a move). What the
//! change does is what differs at the end: a service running fewer tasks,
//! a drain that does not complete, a Volume that stays or moves, data a
//! removed node takes with it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use legion_api::dryrun::{Change, DryRun, Finding, Move, Severity};
use legion_api::node::{Availability, DrainPhase, NodeState, Role};
use legion_api::task::{DesiredState, TaskState};
use legion_api::volume::VolumeDriver;
use tokio::time::Instant;

use super::{Jobs, LABEL_SERVICE, Namespaces, Node, Service, Task, Volumes, eligible};
use crate::controller::simulate::Sim;
use crate::volumes::{Removal, on_removal};

/// The cluster as read, as the controller reads it.
#[derive(Clone)]
pub struct Cluster {
    pub services: Vec<Service>,
    pub jobs: Jobs,
    pub tasks: Vec<Task>,
    pub nodes: Vec<Node>,
    pub volumes: Volumes,
    pub namespaces: Namespaces,
}

/// What the leader knows besides the store: what each Volume's dataset
/// uses (`namespace/name`) and what each node's pools have free.
#[derive(Debug, Default)]
pub struct Facts {
    pub used: HashMap<String, u64>,
    pub free: HashMap<String, u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    NotFound(String),
    Invalid(String),
}

/// Passes in a row before the agents and the migrator are let act again.
const PASSES: usize = 200;
/// Times the migrator acts, or a planned wait ends.
const ROUNDS: usize = 100;
/// Waits are skipped up to this far ahead: a budget's delay, a backoff.
const HORIZON: Duration = Duration::from_secs(3600);

pub fn dry_run(
    cluster: Cluster,
    change: &Change,
    facts: &Facts,
    now: Instant,
) -> Result<DryRun, Refused> {
    let mut findings = Vec::new();
    let mut changed = cluster.clone();
    let node_named = |c: &Cluster, name: &str| -> Result<usize, Refused> {
        c.nodes
            .iter()
            .position(|n| n.name() == name)
            .ok_or_else(|| Refused::NotFound(format!("Node {name} not found")))
    };
    match change {
        Change::Drain { node } => {
            let i = node_named(&changed, node)?;
            set_availability(&mut changed.nodes[i], Availability::Drain);
        }
        Change::MoveVolume {
            namespace,
            volume,
            node,
        } => {
            let key = (namespace.clone(), volume.clone());
            let Some(v) = changed.volumes.get_mut(&key) else {
                return Err(Refused::NotFound(format!(
                    "Volume {namespace}/{volume} not found"
                )));
            };
            // Asked for a node not in the cluster (yet), an unused Volume
            // waits for it; one holding data stays, as `refusal` says.
            if v.status.node.is_empty() && !cluster.nodes.iter().any(|n| n.name() == node) {
                findings.push(Finding {
                    severity: Severity::Warning,
                    reason: "absent".into(),
                    object: format!("Volume {namespace}/{volume}"),
                    namespace: namespace.clone(),
                    message: format!(
                        "node {node} is not in the cluster: its first task waits for it"
                    ),
                });
            }
            if v.spec.driver != VolumeDriver::Zfs {
                return Err(Refused::Invalid(format!(
                    "Volume {namespace}/{volume}: only a ZFS Volume's data moves"
                )));
            }
            let cur = v.object.clone();
            v.spec.node = Some(node.clone());
            v.object.spec = serde_json::to_value(&v.spec).expect("serialises");
            // Past its switch-over, its move goes through, and the API
            // refuses the ask: said as the API says it.
            if let Ok(Some(why)) =
                crate::activity::past_switch_over(|| Ok(Vec::new()), &cur, &v.object)
            {
                return Ok(refused(
                    change,
                    format!("Volume {namespace}/{volume}"),
                    namespace,
                    why,
                ));
            }
        }
        Change::MoveServices {
            namespace,
            services,
            node,
        } => {
            node_named(&changed, node)?;
            let mut moved: BTreeSet<String> = BTreeSet::new();
            for name in services {
                let Some(s) = changed
                    .services
                    .iter_mut()
                    .find(|s| s.key() == (namespace.as_str(), name.as_str()))
                else {
                    return Err(Refused::NotFound(format!(
                        "Service {namespace}/{name} not found"
                    )));
                };
                s.spec
                    .placement
                    .constraints
                    .retain(|c| !c.trim_start().starts_with("node.name"));
                s.spec
                    .placement
                    .constraints
                    .push(format!("node.name == {node}"));
                s.object.spec = serde_json::to_value(&s.spec).expect("serialises");
                for v in &s.spec.pod.volumes {
                    if let Some(source) = &v.volume {
                        moved.insert(source.name.clone());
                    }
                }
            }
            for name in moved {
                let Some(v) = changed.volumes.get_mut(&(namespace.clone(), name.clone())) else {
                    continue;
                };
                if v.spec.driver != VolumeDriver::Zfs {
                    continue;
                }
                v.spec.node = Some(node.clone());
                v.object.spec = serde_json::to_value(&v.spec).expect("serialises");
            }
        }
        // A manager made a worker keeps its tasks: what it does to the
        // managers is added by the API, which knows them.
        Change::Demote { node } => {
            node_named(&changed, node)?;
        }
        Change::RemoveNode { node } => {
            let i = node_named(&changed, node)?;
            let n = changed.nodes.remove(i);
            if n.spec.role == Role::Manager {
                findings.push(Finding {
                    severity: Severity::Warning,
                    reason: "manager".into(),
                    object: format!("Node {node}"),
                    namespace: String::new(),
                    message: "a manager: it leaves the Raft membership".into(),
                });
            }
            let up = n.status.state == NodeState::Ready;
            let left: BTreeSet<String> =
                changed.nodes.iter().map(|n| n.name().to_owned()).collect();
            for ((namespace, name), v) in &mut changed.volumes {
                let Some((status, fate)) =
                    on_removal(&v.spec, &v.status, node, |n| left.contains(n))
                else {
                    continue;
                };
                let object = format!("Volume {namespace}/{name}");
                match fate {
                    Removal::Tidied => {}
                    Removal::Lost => findings.push(Finding {
                        // A node still up has its data to give: drain it.
                        severity: if up {
                            Severity::Blocking
                        } else {
                            Severity::Warning
                        },
                        reason: "lost".into(),
                        object,
                        namespace: namespace.clone(),
                        message: if up {
                            format!(
                                "its data goes with node {node}, which is up: drain the node first to move it"
                            )
                        } else {
                            format!("its data goes with node {node}")
                        },
                    }),
                    Removal::Promoted(_) => findings.push(Finding {
                        severity: Severity::Warning,
                        reason: "replica".into(),
                        object,
                        namespace: namespace.clone(),
                        message: format!(
                            "carries on {}: what was written since is lost",
                            status.promoted
                        ),
                    }),
                }
                v.status = status;
                v.object.status = serde_json::to_value(&v.status).expect("serialises");
            }
        }
        Change::DeleteService { namespace, service } => {
            let before = changed.services.len();
            changed
                .services
                .retain(|s| s.key() != (namespace.as_str(), service.as_str()));
            if changed.services.len() == before {
                return Err(Refused::NotFound(format!(
                    "Service {namespace}/{service} not found"
                )));
            }
            for ((ns, name), v) in &cluster.volumes {
                if ns != namespace || v.object.metadata.labels.get(LABEL_SERVICE) != Some(service) {
                    continue;
                }
                let size = facts
                    .used
                    .get(&format!("{ns}/{name}"))
                    .map(|b| format!(" ({})", legion_api::activity::bytes(*b)))
                    .unwrap_or_default();
                findings.push(Finding {
                    severity: Severity::Warning,
                    reason: "kept".into(),
                    object: format!("Volume {ns}/{name}"),
                    namespace: ns.clone(),
                    message: format!(
                        "kept{size}: deleting the Volume frees it, or the Service made again uses it"
                    ),
                });
            }
        }
    }

    let (base, _, _) = run(sim(cluster.clone(), now));
    let (after, budget, settled) = run(sim(changed, now));
    if !settled {
        findings.push(Finding {
            severity: Severity::Warning,
            reason: "unsettled".into(),
            object: String::new(),
            namespace: String::new(),
            message: "the check stopped before the cluster settled: this is as far as it got"
                .into(),
        });
    }

    // Services running fewer tasks than they would.
    let deleted = match change {
        Change::DeleteService { namespace, service } => Some((namespace, service)),
        _ => None,
    };
    let (ran, runs) = (running(&base), running(&after));
    for ((namespace, service), n) in &ran {
        if deleted == Some((namespace, service)) {
            continue;
        }
        let m = runs
            .get(&(namespace.clone(), service.clone()))
            .copied()
            .unwrap_or(0);
        if m >= *n {
            continue;
        }
        let why: BTreeSet<&str> = after
            .tasks
            .iter()
            .filter(|t| {
                t.object.metadata.namespace == *namespace
                    && t.spec.service == *service
                    && !t.spec.job
                    && t.status.state == TaskState::Pending
                    && t.spec.desired_state == DesiredState::Running
            })
            .map(|t| t.status.message.as_str())
            .filter(|m| !m.is_empty())
            .collect();
        let why: Vec<&str> = why.into_iter().take(2).collect();
        let why = if why.is_empty() {
            String::new()
        } else {
            format!(": {}", why.join("; "))
        };
        findings.push(Finding {
            severity: Severity::Blocking,
            reason: "unplaced".into(),
            object: format!("Service {namespace}/{service}"),
            namespace: namespace.clone(),
            message: format!("{m} of its {n} running task(s) would run{why}"),
        });
    }

    // A drain that would not complete: what holds it.
    if let Change::Drain { node } = change
        && let Some(n) = after.nodes.iter().find(|n| n.name() == node)
        && let Some(d) = &n.status.drain
        && d.phase != DrainPhase::Completed
    {
        // Not ready, what it runs is not known: its drain waits for it to
        // be back, which is no reason to refuse it.
        if d.reason == "unreachable" {
            findings.push(Finding {
                severity: Severity::Warning,
                reason: "unreachable".into(),
                object: format!("Node {node}"),
                namespace: String::new(),
                message:
                    "not ready: what it runs is not known, so its drain completes once it is back"
                        .into(),
            });
        }
        for r in d.remaining.iter().filter(|_| d.reason != "unreachable") {
            let (reason, message) = match r.kind.as_str() {
                // On its way, it waits for what holds its tasks (said of
                // them); with nowhere to go, it stays.
                "Volume" => match after
                    .volumes
                    .get(&(r.namespace.clone(), r.name.clone()))
                    .and_then(|v| v.status.migration.as_ref())
                {
                    Some(m) => (
                        "held",
                        format!("its move to {} waits for its tasks to stop", m.to),
                    ),
                    None => (
                        "stranded",
                        format!(
                            "stays on {node}: {}",
                            nowhere(&after, &r.namespace, &r.name, node)
                        ),
                    ),
                },
                _ => ("stays", format!("keeps running on {node} ({})", r.reason)),
            };
            findings.push(Finding {
                severity: Severity::Blocking,
                reason: reason.into(),
                object: format!("{} {}/{}", r.kind, r.namespace, r.name),
                namespace: r.namespace.clone(),
                message,
            });
        }
    }

    // A Volume asked elsewhere that would stay.
    if let Change::MoveVolume {
        namespace,
        volume,
        node,
    } = change
    {
        let key = (namespace.clone(), volume.clone());
        let at = &after.volumes[&key].status.node;
        if !at.is_empty() && at != node {
            findings.push(Finding {
                severity: Severity::Blocking,
                reason: "stranded".into(),
                object: format!("Volume {namespace}/{volume}"),
                namespace: namespace.clone(),
                message: format!("stays on {at}: {}", refusal(&after, &key, node)),
            });
        }
    }

    // What moves, and whether there is room for it.
    let mut moves = Vec::new();
    for (key, v) in &after.volumes {
        let (Some(was), Some(would)) = (cluster.volumes.get(key), base.volumes.get(key)) else {
            continue;
        };
        // Where it ends, or goes: a move held on the way (by a budget, say)
        // still sends its data there.
        let ends = |v: &super::Volume| {
            v.status
                .migration
                .as_ref()
                .map_or(v.status.node.clone(), |m| m.to.clone())
        };
        let (from, to) = (&was.status.node, &ends(v));
        if from.is_empty() || to.is_empty() || to == from || *to == ends(would) {
            continue;
        }
        moves.push(Move {
            namespace: key.0.clone(),
            volume: key.1.clone(),
            from: from.clone(),
            to: to.clone(),
            bytes: facts.used.get(&format!("{}/{}", key.0, key.1)).copied(),
        });
    }
    let mut arriving: BTreeMap<&str, u64> = BTreeMap::new();
    for m in &moves {
        *arriving.entry(m.to.as_str()).or_default() += m.bytes.unwrap_or(0);
    }
    for (to, bytes) in arriving {
        if let Some(free) = facts.free.get(to)
            && bytes > *free
        {
            findings.push(Finding {
                severity: Severity::Blocking,
                reason: "no-room".into(),
                object: format!("Node {to}"),
                namespace: String::new(),
                message: format!(
                    "{} would move there; its pools have {} free",
                    legion_api::activity::bytes(bytes),
                    legion_api::activity::bytes(*free)
                ),
            });
        }
    }
    if matches!(change, Change::MoveVolume { .. }) && !moves.is_empty() {
        findings.push(Finding {
            severity: Severity::Warning,
            reason: "stops".into(),
            object: String::new(),
            namespace: String::new(),
            message: "its tasks stop while what changed since the full copy is sent, then start on the new node".into(),
        });
    }

    // What a disruption budget holds back on the way.
    let mut held: BTreeMap<(String, String), u32> = BTreeMap::new();
    for uid in budget {
        if let Some(t) = cluster.tasks.iter().find(|t| t.uid() == uid) {
            *held
                .entry((t.object.metadata.namespace.clone(), t.spec.service.clone()))
                .or_default() += 1;
        }
    }
    for ((namespace, service), k) in held {
        let most = cluster
            .services
            .iter()
            .find(|s| s.key() == (namespace.as_str(), service.as_str()))
            .and_then(|s| s.spec.max_unavailable);
        let most = most
            .map(|m| format!(" (maxUnavailable {m})"))
            .unwrap_or_default();
        findings.push(Finding {
            severity: Severity::Warning,
            reason: "budget".into(),
            object: format!("Service {namespace}/{service}"),
            namespace: namespace.clone(),
            message: format!(
                "its disruption budget{most} holds {k} of its task(s) back until the others run again: they move a few at a time"
            ),
        });
    }

    // Tasks running now that would have stopped.
    let still: BTreeSet<&str> = base
        .tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Running)
        .map(Task::uid)
        .collect();
    let kept: BTreeSet<&str> = after
        .tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Running && on_a_node(&after, t))
        .map(Task::uid)
        .collect();
    let stopped = cluster
        .tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Running)
        .filter(|t| still.contains(t.uid()) && !kept.contains(t.uid()))
        .count();

    let mut d = DryRun {
        change: change.clone(),
        findings,
        moves,
        stopped: u32::try_from(stopped).unwrap_or(u32::MAX),
        hidden: 0,
    };
    d.sort();
    Ok(d)
}

/// A dry run of a change the API would refuse past a move's switch-over.
fn refused(change: &Change, object: String, namespace: &str, why: String) -> DryRun {
    DryRun {
        change: change.clone(),
        findings: vec![Finding {
            severity: Severity::Blocking,
            reason: "switch-over".into(),
            object,
            namespace: namespace.to_owned(),
            message: why,
        }],
        moves: Vec::new(),
        stopped: 0,
        hidden: 0,
    }
}

fn set_availability(n: &mut Node, a: Availability) {
    n.spec.availability = a;
    n.object.spec = serde_json::to_value(&n.spec).expect("serialises");
}

fn sim(c: Cluster, now: Instant) -> Sim {
    let mut s = Sim::new(now);
    s.index = c
        .services
        .iter()
        .map(|o| &o.object)
        .chain(c.jobs.jobs.iter().map(|o| &o.object))
        .chain(c.jobs.cron_jobs.iter().map(|o| &o.object))
        .chain(c.tasks.iter().map(|o| &o.object))
        .chain(c.nodes.iter().map(|o| &o.object))
        .chain(c.volumes.values().map(|o| &o.object))
        .map(|o| o.metadata.version)
        .max()
        .unwrap_or(0);
    s.services = c.services;
    s.jobs = c.jobs;
    s.tasks = c.tasks;
    s.nodes = c.nodes;
    s.volumes = c.volumes;
    s.namespaces = c.namespaces;
    s
}

/// Runs `s` until it settles: what it ends as, the uids of the tasks a
/// disruption budget held on the way, and whether it settled.
fn run(mut s: Sim) -> (Sim, BTreeSet<String>, bool) {
    let until = s.now + HORIZON;
    let mut budget = BTreeSet::new();
    for _ in 0..ROUNDS {
        let mut rest = false;
        for _ in 0..PASSES {
            let p = s.plan();
            budget.extend(
                p.waits
                    .iter()
                    .filter(|(_, why)| **why == "budget")
                    .map(|(uid, _)| uid.clone()),
            );
            let acted = !p.actions.is_empty();
            s.apply(p.actions);
            if !s.run_agents() && !acted {
                rest = true;
                break;
            }
        }
        if !rest {
            return (s, budget, false);
        }
        if s.migrate() {
            continue;
        }
        // At rest, unless a wait planned ends within the horizon.
        match s.plan().wake {
            Some(w) if w > s.now && w <= until => s.now = w,
            _ => return (s, budget, true),
        }
    }
    (s, budget, false)
}

/// Running tasks of each Service, by namespace and name, on nodes there.
fn running(s: &Sim) -> BTreeMap<(String, String), u32> {
    let mut m = BTreeMap::new();
    for t in s.tasks.iter().filter(|t| {
        !t.spec.job
            && t.status.state == TaskState::Running
            && t.spec.desired_state == DesiredState::Running
            && on_a_node(s, t)
    }) {
        *m.entry((t.object.metadata.namespace.clone(), t.spec.service.clone()))
            .or_default() += 1;
    }
    m
}

fn on_a_node(s: &Sim, t: &Task) -> bool {
    s.nodes
        .iter()
        .any(|n| n.name() == t.spec.node && n.status.state != NodeState::Down)
}

/// The stateful Service whose ordinal the Volume is.
fn owner<'a>(s: &'a Sim, namespace: &str, volume: &str) -> Option<&'a Service> {
    let v = s.volumes.get(&(namespace.to_owned(), volume.to_owned()))?;
    let name = v.object.metadata.labels.get(LABEL_SERVICE)?;
    s.services
        .iter()
        .find(|svc| svc.key() == (namespace, name.as_str()))
}

/// Why no node takes a Volume leaving `from`: each other node's reason,
/// counted as the scheduler counts them.
fn nowhere(s: &Sim, namespace: &str, volume: &str, from: &str) -> String {
    let Some(svc) = owner(s, namespace, volume) else {
        return "no node takes it".into();
    };
    let mut reasons: BTreeMap<&str, usize> = BTreeMap::new();
    for n in s.nodes.iter().filter(|n| n.name() != from) {
        let why = eligible(n, svc, &s.volumes, &[])
            .err()
            .unwrap_or("no room for its requests");
        *reasons.entry(why).or_default() += 1;
    }
    if reasons.is_empty() {
        return "no other node".into();
    }
    let r: Vec<String> = reasons.iter().map(|(r, n)| format!("{n} {r}")).collect();
    format!("no node takes it: {}", r.join(", "))
}

/// Why the Volume does not go to `to`.
fn refusal(s: &Sim, key: &(String, String), to: &str) -> String {
    let Some(n) = s.nodes.iter().find(|n| n.name() == to) else {
        return format!("node {to} is not in the cluster");
    };
    if let Some(svc) = owner(s, &key.0, &key.1)
        && let Err(why) = eligible(n, svc, &s.volumes, &[])
    {
        return format!("node {to} does not take its tasks: {why}");
    }
    if n.status.state != NodeState::Ready {
        return format!("node {to} is not ready");
    }
    if n.spec.availability != Availability::Active {
        return format!("node {to} takes no new work");
    }
    format!("node {to} does not take it")
}
