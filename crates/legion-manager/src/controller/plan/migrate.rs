//! Moving stateful tasks' Volumes off draining nodes (DESIGN.md §4.3). A
//! wanted ordinal whose Volume lives on a draining node gets a migration to
//! a node its service may run on. The leader's migrator copies the data
//! while the task runs (`copying`, then `copied`); the task then stops, as
//! a drain stops it, and once nothing on a node uses the Volume the
//! migration is `finishing`: the migrator sends what changed and hands the
//! Volume to the new node, where the slot's next task goes.
//!
//! A migration whose new node is no longer ready and active is dropped,
//! and so is one not yet finishing whose old node is no longer draining;
//! the next pass starts another if one is still needed.

use std::collections::BTreeMap;

use legion_api::node::{Availability, NodeState};
use legion_api::service::{ServiceMode, claim_volume};
use legion_api::volume::{Migration, MigrationPhase, VolumeStatus};

use super::{Action, Node, Plan, Service, Task, Volume, Volumes, eligible, shared_volumes};

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

    // Advance or drop the migrations under way. One not yet finishing goes
    // when neither a drain nor the Volume's `node` calls for it any more.
    for ((namespace, name), v) in volumes {
        let Some(m) = &v.status.migration else {
            continue;
        };
        let wanted = requested(v) == Some(m.to.as_str()) || draining(&v.status.node);
        let next = if !usable(&m.to) || (m.phase != MigrationPhase::Finishing && !wanted) {
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
            && usable(to)
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
    // active again; all of an ordinal's Volumes go to the same node.
    for s in services
        .iter()
        .filter(|s| s.spec.mode == ServiceMode::Stateful)
    {
        let (namespace, svc) = s.key();
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
                        && draining(&v.status.node)
                })
                .collect();
            let Some(from) = stuck.first().map(|v| v.status.node.as_str()) else {
                continue;
            };
            let Some(to) = target(s, from, tasks, nodes, volumes) else {
                continue;
            };
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
/// with the fewest of them.
fn target<'a>(
    s: &Service,
    from: &str,
    tasks: &[Task],
    nodes: &'a [Node],
    volumes: &Volumes,
) -> Option<&'a str> {
    let mut count: BTreeMap<&str, usize> = BTreeMap::new();
    for t in tasks.iter().filter(|t| t.placed() && s.owns(t)) {
        *count.entry(t.spec.node.as_str()).or_default() += 1;
    }
    let mounts = shared_volumes(s);
    nodes
        .iter()
        .filter(|n| n.name() != from && eligible(n, s, volumes, &mounts).is_ok())
        .min_by_key(|n| (count.get(n.name()).copied().unwrap_or(0), n.name()))
        .map(Node::name)
}

fn write(v: &Volume, migration: Option<Migration>, plan: &mut Plan) {
    let mut o = v.object.clone();
    o.status = serde_json::to_value(VolumeStatus {
        migration,
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
                v.status.node != t.spec.node
                    || v.status.migration.as_ref().is_some_and(|m| {
                        matches!(m.phase, MigrationPhase::Copied | MigrationPhase::Finishing)
                    })
            })
    })
}
