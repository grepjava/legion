//! A replicated database's members (DESIGN.md §6.9): the leader names the
//! primary, tells each member's task its role and the topology in its spec,
//! and when the primary is lost fences it, promotes the replica furthest
//! ahead and points the others at it. What the members say comes with the
//! heartbeats ([`Memory::members`]); what the leader decided is recorded in
//! the Service's `status.replication` before any member acts on it.

use std::collections::BTreeMap;
use std::time::Duration;

use legion_api::replication::spi::{self, Role};
use legion_api::replication::{
    Change, ChangeKind, LastChange, MemberReport, MemberStatus, Phase, Replication,
    ReplicationStatus, TaskReplication,
};
use legion_api::service::OnPartition;
use legion_api::task::TaskState;
use legion_api::volume::Failover;
use tokio::time::Instant;

use super::{Action, Memory, Node, Plan, Service, Task, fenced_until, stop};

/// How old a member's report may be for it to count: a candidate for
/// promotion must have spoken lately.
pub const FRESH: Duration = Duration::from_secs(15);
/// The protocol from which an agent reads a task's `replication`.
const REPLICATES: u32 = 4;
/// Positions and lag alone are written at most this often.
const WRITE_EVERY: Duration = Duration::from_secs(30);

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The member of ordinal `i`.
fn member(svc: &str, i: u32) -> String {
    format!("{svc}-{i}")
}

fn ordinal(svc: &str, member: &str) -> Option<u32> {
    member.strip_prefix(svc)?.strip_prefix('-')?.parse().ok()
}

/// Plans a replicated Service's members; returns its `status.replication`
/// to record, or none for a Service that does not replicate.
pub(super) fn pass(
    s: &Service,
    tasks: &[&Task],
    nodes: &[Node],
    memory: &mut Memory,
    now: Instant,
    plan: &mut Plan,
) -> Option<ReplicationStatus> {
    let r = s.spec.replication.as_ref()?;
    let (ns, svc) = (&s.object.metadata.namespace, &s.object.metadata.name);
    let uid = s.object.metadata.uid.clone();
    let previous: Option<ReplicationStatus> = s
        .object
        .status
        .get("replication")
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let mut st = previous.clone().unwrap_or_default();
    let n = if s.spec.stopped {
        0
    } else {
        s.spec.replicas.unwrap_or(1)
    };

    // Each ordinal's task: the newest one wanted running.
    let mut current: BTreeMap<u32, &Task> = BTreeMap::new();
    for t in tasks {
        if let Some(i) = t.spec.slot
            && t.active()
            && current.get(&i).is_none_or(|c| c.uid() < t.uid())
        {
            current.insert(i, t);
        }
    }
    let report = |i: u32| -> Option<&MemberReport> {
        let t = current.get(&i)?;
        let (_, age, r) = memory
            .members
            .get(&format!("{ns}/{}", t.object.metadata.name))?;
        (*age < FRESH).then_some(r)
    };
    let status_of = |i: u32| report(i).and_then(|r| r.status.as_ref());

    // What each member last said, kept while it says nothing new.
    for i in 0..n {
        if let (Some(t), Some(rep)) = (current.get(&i), report(i)) {
            let mut m = MemberStatus {
                task: t.object.metadata.name.clone(),
                at: rep.at,
                status: rep.status.clone().unwrap_or_default(),
                diverged: false,
                error: rep.error.clone(),
            };
            if rep.status.is_none() {
                m.status.role = Role::Unknown;
            }
            st.members.insert(member(svc, i), m);
        }
    }
    st.members
        .retain(|m, _| ordinal(svc, m).is_some_and(|i| i < n));

    // The first primary: ordinal 0.
    if st.primary.is_none() && st.change.is_none() && n > 0 {
        st.primary = Some(member(svc, 0));
        st.epoch = st.epoch.max(1);
        st.phase = Phase::Forming;
    }
    let primary_ord = st.primary.as_deref().and_then(|p| ordinal(svc, p));
    // The tasks of ordinal `p` that may run its database: each must stop
    // before another member is promoted.
    let fence_of = |p: Option<u32>| -> Vec<String> {
        p.map(|p| {
            tasks
                .iter()
                .filter(|t| {
                    t.spec.slot == Some(p)
                        && (!t.status.state.is_terminal() || t.status.state == TaskState::Orphaned)
                })
                .map(|t| t.uid().to_owned())
                .collect()
        })
        .unwrap_or_default()
    };

    // The operator's request, acted on once.
    if let Some(q) = &r.request
        && st.handled != Some(q.at)
        && st.change.is_none()
    {
        st.handled = Some(q.at);
        st.refused.clear();
        let target = q.to.as_deref().map(|m| (m, ordinal(svc, m)));
        let why = match target {
            Some((m, None)) => Some(format!("{m} is not a member of {svc}")),
            Some((m, Some(i))) if i >= n => Some(format!("{m} is not a member of {svc}")),
            Some((m, Some(i))) if Some(i) == primary_ord => {
                Some(format!("{m} is the primary already"))
            }
            None if q.kind == ChangeKind::Failover => {
                Some("name the member to promote (--to)".to_owned())
            }
            _ => None,
        };
        match why {
            Some(why) => st.refused = why,
            None => {
                st.change = Some(Change {
                    kind: q.kind,
                    from: st.primary.clone(),
                    to: None,
                    reason: match q.kind {
                        ChangeKind::Switchover => "a switchover was asked for".to_owned(),
                        ChangeKind::Failover => "a failover was asked for".to_owned(),
                    },
                    started: wall_ms(),
                    force: q.force,
                    fence: fence_of(primary_ord),
                    target: q.to.clone(),
                });
                memory.unhealthy_since.remove(&uid);
            }
        }
    }

    // The primary's health, and the change it calls for.
    let mut manual = None;
    if st.change.is_none() {
        let healthy = primary_ord
            .and_then(status_of)
            .is_some_and(|x| x.role == Role::Primary && x.healthy);
        if healthy {
            memory.unhealthy_since.remove(&uid);
            if let Some(p) = primary_ord.and_then(status_of).and_then(|x| x.position)
                && st.primary_position.is_none_or(|q| p > q)
            {
                st.primary_position = Some(p);
            }
        } else {
            let since = *memory.unhealthy_since.entry(uid.clone()).or_insert(now);
            // Lost only once it served: before, there is nothing to fail
            // over to.
            let served = st.primary_position.is_some();
            let gone = primary_ord.is_none_or(|i| i >= n);
            let due = since + r.failover_after.as_std();
            if served && (gone || now >= due) {
                if r.failover == Failover::Automatic {
                    let from = st.primary.clone();
                    let fence = fence_of(primary_ord);
                    st.change = Some(Change {
                        kind: ChangeKind::Failover,
                        reason: format!(
                            "the primary {} was not healthy for {}",
                            from.as_deref().unwrap_or("?"),
                            r.failover_after
                        ),
                        from,
                        to: None,
                        started: wall_ms(),
                        force: false,
                        fence,
                        target: None,
                    });
                    memory.unhealthy_since.remove(&uid);
                } else {
                    manual = Some(format!(
                        "the primary {} is not healthy; promote a replica: legion replication failover {svc} --to MEMBER",
                        st.primary.as_deref().unwrap_or("?")
                    ));
                }
            } else if served {
                plan.wake_at(due);
            }
        }
    }

    // A change under way: fence, choose, promote, done.
    let mut waiting = String::new();
    if let Some(c) = st.change.as_mut() {
        if c.to.is_none() {
            let mut fenced = true;
            for uid in &c.fence {
                let Some(t) = tasks.iter().find(|t| t.uid() == uid) else {
                    continue;
                };
                match t.status.state {
                    TaskState::Orphaned => {
                        if t.spec.pod.on_partition != OnPartition::Stop {
                            fenced = false;
                            waiting = format!(
                                "{} is cut off and may run on (onPartition: run)",
                                t.object.metadata.name
                            );
                        } else if let Some(until) = fenced_until(t, memory, now)
                            && now < until
                        {
                            fenced = false;
                            plan.wake_at(until);
                            waiting = format!(
                                "{} is cut off: waiting for it to stop",
                                t.object.metadata.name
                            );
                        }
                    }
                    state if state.is_terminal() => {}
                    _ => {
                        fenced = false;
                        waiting =
                            format!("stopping the old primary's task {}", t.object.metadata.name);
                        let key = t.object.key();
                        if !t.stopping() && !plan.actions.iter().any(|a| a.key() == key) {
                            plan.actions.push(stop(t));
                        }
                    }
                }
            }
            if fenced {
                let from = c.from.as_deref().and_then(|f| ordinal(svc, f));
                let choice = Choice {
                    sync: st
                        .synchronous
                        .iter()
                        .filter_map(|m| ordinal(svc, m))
                        .collect(),
                    force: c.force,
                    n,
                    from,
                    target: c.target.as_deref().and_then(|t| ordinal(svc, t)),
                    planned: c.kind == ChangeKind::Switchover,
                };
                match candidate(r, &st.primary_position, &choice, &status_of) {
                    Ok(i) => {
                        st.epoch += 1;
                        st.primary = Some(member(svc, i));
                        c.to = Some(member(svc, i));
                    }
                    Err(why) => waiting = why,
                }
            }
        } else if let Some(to) = c.to.clone() {
            let done = ordinal(svc, &to)
                .and_then(status_of)
                .is_some_and(|x| x.role == Role::Primary && x.healthy);
            if done {
                let position = ordinal(svc, &to)
                    .and_then(status_of)
                    .and_then(|x| x.position);
                st.last_change = Some(LastChange {
                    kind: c.kind,
                    from: c.from.clone(),
                    to: to.clone(),
                    reason: c.reason.clone(),
                    position,
                    at: wall_ms(),
                });
                st.primary_position = position;
                st.change = None;
            } else {
                waiting = format!("promoting {to}");
            }
        }
    }
    // Named primary, as members are told: none while the old one is fenced.
    let named: Option<u32> = match &st.change {
        Some(c) if c.to.is_none() => None,
        _ => st.primary.as_deref().and_then(|p| ordinal(svc, p)),
    };

    // A member saying it is primary that is not named so is stopped.
    let mut split = Vec::new();
    for (i, t) in &current {
        if Some(*i) != named && status_of(*i).is_some_and(|x| x.role == Role::Primary) {
            split.push(member(svc, *i));
            let key = t.object.key();
            if !t.stopping() && !plan.actions.iter().any(|a| a.key() == key) {
                plan.actions.push(stop(t));
            }
        }
    }

    // Each member's task told its role and the topology.
    let addr = |i: u32| spi::Member {
        member: member(svc, i),
        ordinal: i,
        address: current
            .get(&i)
            .and_then(|t| t.status.ip.clone())
            .unwrap_or_default(),
        // Whole, so no search domain is needed.
        dns: format!("{svc}-{i}.{svc}.{ns}.legion"),
    };
    // The replicas the primary waits for: those healthy and streaming,
    // seen while it is healthy; kept as they were meanwhile.
    if r.synchronous > 0 && st.change.is_none() {
        let healthy = named
            .and_then(status_of)
            .is_some_and(|x| x.role == Role::Primary && x.healthy);
        if healthy {
            st.synchronous = (0..n)
                .filter(|i| Some(*i) != named)
                .filter(|i| {
                    status_of(*i)
                        .is_some_and(|x| x.role == Role::Replica && x.healthy && x.streaming)
                })
                .map(|i| member(svc, i))
                .collect();
        }
    } else if r.synchronous == 0 {
        st.synchronous.clear();
    }
    let waited_for: Vec<String> = st
        .synchronous
        .iter()
        .filter(|m| named.is_none_or(|p| ordinal(svc, m) != Some(p)))
        .cloned()
        .collect();
    let topology = spi::Topology {
        primary: named.map(addr),
        members: (0..n).map(addr).collect(),
        synchronous: (r.synchronous > 0 && !waited_for.is_empty()).then(|| spi::Synchronous {
            count: r.synchronous.min(waited_for.len() as u32),
            members: waited_for,
        }),
    };
    for (i, t) in &current {
        let want = TaskReplication {
            // The operator's request is the leader's alone.
            config: Replication {
                request: None,
                ..r.clone()
            },
            epoch: st.epoch,
            role: if Some(*i) == named {
                Role::Primary
            } else {
                Role::Replica
            },
            topology: topology.clone(),
        };
        if t.spec.replication.as_ref() == Some(&want) || !reads_replication(t, nodes) {
            continue;
        }
        let key = t.object.key();
        if plan.actions.iter().any(|a| a.key() == key) {
            continue;
        }
        let mut o = t.object.clone();
        o.spec["replication"] = serde_json::to_value(&want).expect("serialises");
        plan.actions
            .push(Action::Update(o, t.object.metadata.version));
    }

    // The phase, and why.
    let all_well = (0..n).all(|i| {
        status_of(i).is_some_and(|x| {
            x.healthy && (Some(i) == named || (x.role == Role::Replica && behind_ok(r, &st, x)))
        })
    });
    let (phase, message) = if let Some(c) = &st.change {
        (
            Phase::FailingOver,
            format!(
                "{}: {}",
                c.reason,
                if waiting.is_empty() {
                    "under way"
                } else {
                    &waiting
                }
            ),
        )
    } else if !split.is_empty() {
        (
            Phase::Split,
            format!(
                "{} say they are primary, not being named so: stopped",
                split.join(", ")
            ),
        )
    } else if st.primary_position.is_none() {
        (Phase::Forming, "waiting for the first primary".to_owned())
    } else if let Some(m) = manual {
        (Phase::Degraded, m)
    } else if all_well {
        (Phase::Healthy, String::new())
    } else {
        let missing: Vec<String> = (0..n)
            .filter(|i| !status_of(*i).is_some_and(|x| x.healthy))
            .map(|i| member(svc, i))
            .collect();
        (
            Phase::Degraded,
            if missing.is_empty() {
                "a replica is behind".to_owned()
            } else {
                format!("not heard from, or not healthy: {}", missing.join(", "))
            },
        )
    };
    st.phase = phase;
    st.message = message;

    // Positions and lag alone are written now and then, the rest at once.
    let quiet = |x: &ReplicationStatus| {
        let mut x = x.clone();
        x.primary_position = None;
        for m in x.members.values_mut() {
            m.at = 0;
            m.status.position = None;
            m.status.lag = None;
            m.status.detail = Default::default();
        }
        x
    };
    if let Some(p) = &previous
        && quiet(p) == quiet(&st)
        && memory
            .replication_written
            .get(&uid)
            .is_some_and(|at| now < *at + WRITE_EVERY)
    {
        return previous;
    }
    if previous.as_ref() != Some(&st) {
        memory.replication_written.insert(uid, now);
    }
    Some(st)
}

/// Whether a replica is within `maximumLag` of the primary.
fn behind_ok(r: &Replication, st: &ReplicationStatus, x: &spi::Status) -> bool {
    match (r.maximum_lag, &st.primary_position, &x.position) {
        (Some(max), Some(p), Some(q)) => q.behind(p).is_some_and(|b| b <= max.0),
        _ => true,
    }
}

/// What a change of primary chooses among.
struct Choice {
    /// Pass over the lag, synchronous and catching-up checks.
    force: bool,
    /// The members: ordinals below it.
    n: u32,
    /// The old primary's ordinal.
    from: Option<u32>,
    /// The member the operator named.
    target: Option<u32>,
    /// A switchover: the new primary must have all the old one wrote.
    planned: bool,
    /// The replicas the old primary waited for, if it did.
    sync: Vec<u32>,
}

/// The replica to promote: of those heard from lately, not the old primary,
/// the one furthest ahead (or the one named), the lowest ordinal of equals;
/// within `maximumLag` of the last primary position seen, and, with
/// synchronous replicas, only once enough replicas reported that the
/// furthest ahead surely holds every confirmed commit. A switchover's waits
/// until it holds all the old primary wrote: as far as any other replica
/// and the last position seen of the primary. `force` passes over these.
fn candidate<'a>(
    r: &Replication,
    primary_position: &Option<spi::Position>,
    c: &Choice,
    status_of: &dyn Fn(u32) -> Option<&'a spi::Status>,
) -> Result<u32, String> {
    let (force, n, from) = (c.force, c.n, c.from);
    // With synchronous replicas, only those the old primary waited for
    // surely hold what it confirmed.
    let k = r.synchronous;
    let pool: Vec<u32> = if k > 0 && !force && !c.sync.is_empty() {
        c.sync.clone()
    } else {
        (0..n).collect()
    };
    let replicas: Vec<(u32, spi::Position)> = pool
        .iter()
        .copied()
        .filter(|i| Some(*i) != from && *i < n)
        .filter_map(|i| {
            let x = status_of(i)?;
            (x.role == Role::Replica).then_some((i, x.position?))
        })
        .collect();
    if k > 0 && !force && !c.sync.is_empty() {
        let s = c.sync.len() as u32;
        let needed = s - k.min(s) + 1;
        if (replicas.len() as u32) < needed {
            return Err(format!(
                "{} of {s} synchronous replicas heard from; {needed} must be, to be sure the one furthest ahead holds every confirmed commit",
                replicas.len()
            ));
        }
    }
    let eligible: Vec<&(u32, spi::Position)> = replicas
        .iter()
        .filter(|(_, p)| {
            force
                || match (r.maximum_lag, primary_position) {
                    (Some(max), Some(pp)) => p.behind(pp).is_some_and(|b| b <= max.0),
                    _ => true,
                }
        })
        .collect();
    let furthest = replicas.iter().map(|(_, p)| *p).max();
    if let Some(t) = c.target {
        let Some((_, p)) = replicas.iter().find(|(i, _)| *i == t) else {
            return Err(format!(
                "the member asked for, ordinal {t}, is not heard from as a replica"
            ));
        };
        if !force {
            if !eligible.iter().any(|(i, _)| *i == t) {
                return Err(format!("ordinal {t} is further behind than maximumLag"));
            }
            if c.planned && (Some(*p) < furthest || primary_position.is_some_and(|q| *p < q)) {
                return Err(format!("waiting for ordinal {t} to catch up"));
            }
        }
        return Ok(t);
    }
    let best = eligible
        .iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        .map(|(i, p)| (*i, *p));
    if let Some((_, p)) = best
        && c.planned
        && !force
        && primary_position.is_some_and(|q| p < q)
    {
        return Err("waiting for a replica to catch up with the primary".to_owned());
    }
    best.map(|(i, _)| i).ok_or_else(|| {
        if replicas.is_empty() {
            "no replica heard from lately".to_owned()
        } else {
            "every replica is further behind than maximumLag".to_owned()
        }
    })
}

/// Whether the task's node reads a task's `replication`: an agent before
/// would refuse its spec.
fn reads_replication(t: &Task, nodes: &[Node]) -> bool {
    nodes
        .iter()
        .find(|n| n.name() == t.spec.node)
        .is_some_and(reads)
}

/// Whether a node's agent reads a task's `replication`.
pub(super) fn reads(n: &Node) -> bool {
    legion_api::protocol::Protocol::said(n.status.protocol.speaks, n.status.protocol.oldest).speaks
        >= REPLICATES
}

/// What a new task of ordinal `slot` of a replicated Service is told at
/// first: its role as recorded (ordinal 0 before any primary is named;
/// none while one is being replaced), the epoch, and no topology yet,
/// which the pass gives it, so a replica waits at `prepare`.
pub(super) fn initial(s: &Service, slot: Option<u32>) -> Option<TaskReplication> {
    let r = s.spec.replication.as_ref()?;
    let svc = &s.object.metadata.name;
    let st: Option<ReplicationStatus> = s
        .object
        .status
        .get("replication")
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let named = match &st {
        None => Some(0),
        Some(st) if st.change.as_ref().is_some_and(|c| c.to.is_none()) => None,
        Some(st) => st.primary.as_deref().and_then(|p| ordinal(svc, p)),
    };
    Some(TaskReplication {
        config: Replication {
            request: None,
            ..r.clone()
        },
        epoch: st.as_ref().map_or(1, |st| st.epoch.max(1)),
        role: if named.is_some() && named == slot {
            Role::Primary
        } else {
            Role::Replica
        },
        topology: spi::Topology::default(),
    })
}
