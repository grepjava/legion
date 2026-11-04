//! Moving stateful tasks' Volumes off draining nodes (DESIGN.md §4.3). A
//! wanted ordinal whose Volume lives on a draining node gets a migration to
//! a node its service may run on. The leader's migrator copies the data
//! while the task runs (`copying`, then `copied`); the task then stops, as
//! a drain stops it, and once nothing on a node uses the Volume the
//! migration is `finishing`: the migrator sends what changed and hands the
//! Volume to the new node, where the slot's next task goes.
//!
//! A migration whose new node is no longer ready and active, or for a
//! stateful service's Volume one its tasks may not run on, is dropped, and
//! so is one not yet finishing that nothing calls for any more;
//! the next pass starts another if one is still needed. What the dropped
//! one's new node received is left over, for the migrator to drop. One
//! whose old node is removed after the full copy ends there: that copy is
//! the Volume. One whose old node is down waits for it.

use std::collections::BTreeMap;

use legion_api::node::{Availability, NodeState};
use legion_api::service::{ServiceMode, TaintEffect, claim_volume};
use legion_api::volume::{Access, Migration, MigrationPhase, VolumeStatus};

use super::{
    Action, LABEL_SERVICE, Node, Plan, Service, Task, Volume, Volumes, eligible, shared_volumes,
};

pub(super) fn migrations(
    services: &[Service],
    tasks: &[Task],
    nodes: &[Node],
    volumes: &Volumes,
    plan: &mut Plan,
) {
    let node = |name: &str| nodes.iter().find(|n| n.name() == name);
    let draining =
        |name: &str| node(name).is_some_and(|n| n.spec.availability == Availability::Drain);
    let usable = |name: &str| {
        node(name).is_some_and(|n| {
            n.status.state == NodeState::Ready && n.spec.availability == Availability::Active
        })
    };
    // A node a stateful service's tasks must leave: draining, or with a
    // taint that evicts them.
    let leaves = |s: &Service, name: &str| {
        draining(name) || node(name).is_some_and(|n| n.repels(s, TaintEffect::NoExecute))
    };
    let stateful = |v: &Volume| {
        services.iter().find(|s| {
            s.spec.mode == ServiceMode::Stateful
                && s.object.metadata.namespace == v.object.metadata.namespace
                && v.object.metadata.labels.get(LABEL_SERVICE) == Some(&s.object.metadata.name)
        })
    };
    // A node a stateful service's ordinal may live on: one its tasks may
    // run on, by the node alone (where its other data is aside).
    let holds =
        |s: &Service, to: &str| node(to).is_some_and(|n| eligible(n, s, volumes, &[]).is_ok());
    // A node a Volume may go to: ready and active, and for a stateful
    // service's, one its tasks may run on. Data moved where its task cannot
    // follow would only stop it, or leave again.
    let usable_for = |v: &Volume, to: &str| match stateful(v) {
        Some(s) => holds(s, to),
        None => usable(to),
    };
    // Requests placed on each node.
    let mut load: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for t in tasks.iter().filter(|t| t.placed()) {
        let r = t.spec.pod.requests();
        let l = load.entry(t.spec.node.as_str()).or_default();
        l.0 = l.0.saturating_add(r.cpu.0);
        l.1 = l.1.saturating_add(r.memory.0);
    }

    // A volume used one node at a time stays with its node until the node
    // lets go of it, or is removed: removing a node is the word that it
    // no longer uses anything.
    for v in volumes.values() {
        if v.spec.access() == Access::Single
            && !v.status.node.is_empty()
            && node(&v.status.node).is_none()
        {
            let mut o = v.object.clone();
            o.status = serde_json::to_value(VolumeStatus::default()).expect("serialises");
            plan.actions
                .push(Action::Update(o, v.object.metadata.version));
        }
    }

    // Advance or drop the migrations under way. One not yet finishing goes
    // when neither the Volume's `node` nor its stateful service leaving its
    // node calls for it any more. An ask for another node that can take
    // the Volume wins over a drain's choice: the Volume leaves either way.
    for ((namespace, name), v) in volumes {
        let Some(m) = &v.status.migration else {
            continue;
        };
        // The node it moves from was removed, and its data with it, after
        // the full copy: that copy is the Volume now. A node down, not
        // removed, is waited for; it still has the latest data.
        if matches!(m.phase, MigrationPhase::Copied | MigrationPhase::Finishing)
            && !v.status.node.is_empty()
            && node(&v.status.node).is_none()
            && node(&m.to).is_some()
        {
            let mut o = v.object.clone();
            o.status = serde_json::to_value(VolumeStatus {
                node: m.to.clone(),
                dataset: String::new(),
                migration: None,
                leftovers: v
                    .status
                    .leftovers
                    .iter()
                    .filter(|n| **n != m.to)
                    .cloned()
                    .collect(),
            })
            .expect("serialises");
            plan.actions
                .push(Action::Update(o, v.object.metadata.version));
            continue;
        }
        let wanted = match requested(v) {
            Some(to) if to == m.to => true,
            Some(to) if usable_for(v, to) => false,
            _ => stateful(v).is_some_and(|s| leaves(s, &v.status.node)),
        };
        let next = if !usable_for(v, &m.to) || (m.phase != MigrationPhase::Finishing && !wanted) {
            None
        } else if m.phase == MigrationPhase::Copied
            && !tasks.iter().any(|t| {
                t.placed() && t.object.metadata.namespace == *namespace && t.spec.uses_volume(name)
            })
        {
            Some(Migration {
                phase: MigrationPhase::Finishing,
                message: String::new(),
                ..m.clone()
            })
        } else {
            continue;
        };
        write(v, next, plan);
    }

    // Start one for each Volume asked to be on another node that can take
    // it.
    let mut started = std::collections::BTreeSet::new();
    for (key, v) in volumes {
        if let Some(to) = requested(v)
            && v.status.migration.is_none()
            && usable_for(v, to)
        {
            let m = Migration {
                to: to.to_owned(),
                phase: MigrationPhase::Copying,
                message: String::new(),
            };
            write(v, Some(m), plan);
            started.insert(key);
        }
    }

    // And one for each wanted ordinal whose Volumes are on a draining
    // node, even one asked to be there, where it returns once the node is
    // active again; all of an ordinal's Volumes go to the same node. Each
    // ordinal counts where its Volumes go or are, else where its task is,
    // so one whose data has arrived counts before its task starts there.
    for s in services
        .iter()
        .filter(|s| s.spec.mode == ServiceMode::Stateful)
    {
        let (namespace, svc) = s.key();
        let ordinal_volumes = |n| {
            s.spec.pod.claims().filter_map(move |(claim, _)| {
                volumes.get(&(namespace.to_owned(), claim_volume(claim, svc, n)))
            })
        };
        let mut homes: BTreeMap<String, usize> = BTreeMap::new();
        for n in 0..s.spec.replicas.unwrap_or(1) {
            let going = ordinal_volumes(n).find_map(|v| v.status.migration.as_ref());
            let home = going
                .map(|m| m.to.as_str())
                .or_else(|| {
                    ordinal_volumes(n)
                        .map(|v| v.status.node.as_str())
                        .find(|node| !node.is_empty())
                })
                .or_else(|| {
                    tasks
                        .iter()
                        .find(|t| t.placed() && s.owns(t) && t.spec.slot == Some(n))
                        .map(|t| t.spec.node.as_str())
                });
            if let Some(home) = home {
                *homes.entry(home.to_owned()).or_default() += 1;
            }
        }
        for n in 0..s.spec.replicas.unwrap_or(1) {
            let stuck: Vec<&Volume> = s
                .spec
                .pod
                .claims()
                .filter_map(|(claim, _)| {
                    let key = (namespace.to_owned(), claim_volume(claim, svc, n));
                    volumes.get(&key).filter(|_| !started.contains(&key))
                })
                .filter(|v| {
                    v.status.migration.is_none()
                        && !v.status.node.is_empty()
                        && leaves(s, &v.status.node)
                })
                .collect();
            let Some(from) = stuck.first().map(|v| v.status.node.as_str()) else {
                continue;
            };
            // Where the ordinal's other Volumes go or are, if not off a
            // draining node: these join them, or wait until they may.
            let together = s.spec.pod.claims().find_map(|(claim, _)| {
                let key = (namespace.to_owned(), claim_volume(claim, svc, n));
                let v = volumes.get(&key)?;
                if stuck.iter().any(|st| std::ptr::eq(*st, v)) {
                    return None;
                }
                let going = match &v.status.migration {
                    Some(m) => Some(m.to.as_str()),
                    None if started.contains(&key) => requested(v),
                    None => None,
                };
                going.or_else(|| {
                    Some(v.status.node.as_str()).filter(|node| !node.is_empty() && !leaves(s, node))
                })
            });
            let to = match together {
                Some(to) if holds(s, to) => to,
                Some(_) => continue,
                None => match target(s, from, nodes, volumes, &homes, &load) {
                    Some(to) => to,
                    None => continue,
                },
            };
            *homes.entry(to.to_owned()).or_default() += 1;
            let r = s.spec.pod.requests();
            let l = load.entry(to).or_default();
            l.0 = l.0.saturating_add(r.cpu.0);
            l.1 = l.1.saturating_add(r.memory.0);
            for v in stuck.iter().filter(|v| v.status.node == from) {
                let m = Migration {
                    to: to.to_owned(),
                    phase: MigrationPhase::Copying,
                    message: String::new(),
                };
                write(v, Some(m), plan);
            }
        }
    }
}

/// Where a Volume is asked to be, when that is not where it is.
fn requested(v: &Volume) -> Option<&str> {
    v.spec
        .node
        .as_deref()
        .filter(|to| !v.status.node.is_empty() && *to != v.status.node)
}

/// Where an ordinal of `s` moves from `from`: a node its tasks may run on,
/// with room for its requests beside what is placed there (`load`, CPU
/// and memory), home to the fewest of its ordinals (`homes`).
fn target<'a>(
    s: &Service,
    from: &str,
    nodes: &'a [Node],
    volumes: &Volumes,
    homes: &BTreeMap<String, usize>,
    load: &BTreeMap<&str, (u64, u64)>,
) -> Option<&'a str> {
    let mounts = shared_volumes(s);
    let r = s.spec.pod.requests();
    let fits = |n: &Node| {
        let (cpu, memory) = load.get(n.name()).copied().unwrap_or_default();
        let alloc = n.status.allocatable;
        let fits = |load: u64, request: u64, alloc: u64| {
            alloc == 0 || load.checked_add(request).is_some_and(|sum| sum <= alloc)
        };
        fits(cpu, r.cpu.0, alloc.cpu.0) && fits(memory, r.memory.0, alloc.memory.0)
    };
    nodes
        .iter()
        .filter(|n| n.name() != from && eligible(n, s, volumes, &mounts).is_ok() && fits(n))
        .min_by_key(|n| (homes.get(n.name()).copied().unwrap_or(0), n.name()))
        .map(Node::name)
}

/// Writes a Volume's migration. One dropped or sent elsewhere leaves its
/// new node whatever it received, a leftover the migrator drops; kept in
/// the status, so a new leader drops it too.
fn write(v: &Volume, migration: Option<Migration>, plan: &mut Plan) {
    let mut leftovers = v.status.leftovers.clone();
    if let Some(was) = &v.status.migration
        && migration.as_ref().is_none_or(|m| m.to != was.to)
        && was.to != v.status.node
        && !leftovers.contains(&was.to)
    {
        leftovers.push(was.to.clone());
    }
    let mut o = v.object.clone();
    o.status = serde_json::to_value(VolumeStatus {
        migration,
        leftovers,
        ..v.status.clone()
    })
    .expect("serialises");
    plan.actions
        .push(Action::Update(o, v.object.metadata.version));
}

/// Whether one of the Volumes a task uses is moving off its node.
pub(super) fn leaving(t: &Task, volumes: &Volumes) -> bool {
    let namespace = &t.object.metadata.namespace;
    t.spec.volumes().iter().any(|name| {
        volumes
            .get(&(namespace.clone(), name.clone()))
            .is_some_and(|v| v.status.node == t.spec.node && v.status.migration.is_some())
    })
}

/// Whether a task on a draining node may stop: its Volumes there have been
/// copied to where they go.
pub(super) fn copied(t: &Task, volumes: &Volumes) -> bool {
    let namespace = &t.object.metadata.namespace;
    t.spec.volumes().iter().all(|name| {
        volumes
            .get(&(namespace.clone(), name.clone()))
            .is_none_or(|v| {
                v.spec.access() != Access::Local
                    || v.status.node != t.spec.node
                    || v.status.migration.as_ref().is_some_and(|m| {
                        matches!(m.phase, MigrationPhase::Copied | MigrationPhase::Finishing)
                    })
            })
    })
}
