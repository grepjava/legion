//! Where each draining node's drain stands (PLAN.md, phase 5), written to
//! its status for `legion node drain --wait` and other tools to follow. The
//! moving itself is the Service controller's and the migrator's; this only
//! says what is left, from what the store shows. Unknown never counts as
//! released: a node that isn't ready, or a task orphaned there and not yet
//! seen to stop, blocks the drain until it is known.

use std::collections::BTreeSet;

use legion_api::node::{Availability, DrainItem, DrainPhase, DrainStatus, NodeState};
use legion_api::service::{ServiceMode, claim_volume};
use legion_api::task::DesiredState;
use legion_api::task::TaskState;
use legion_api::volume::MigrationPhase;

use super::{Action, Node, Plan, Service, Task, Volumes};

pub(super) fn drains(
    services: &[Service],
    tasks: &[Task],
    nodes: &[Node],
    volumes: &Volumes,
    plan: &mut Plan,
) {
    // The Volumes of stateful services' wanted ordinals: on a draining
    // node, each is moved off, so the drain waits for it even between one
    // move called off (its target no longer ready) and the next.
    let moved: BTreeSet<(String, String)> = services
        .iter()
        .filter(|s| s.spec.mode == ServiceMode::Stateful)
        .flat_map(|s| {
            let (namespace, svc) = s.key();
            (0..s.spec.replicas.unwrap_or(1)).flat_map(move |n| {
                s.spec
                    .pod
                    .claims()
                    .map(move |(claim, _)| (namespace.to_owned(), claim_volume(claim, svc, n)))
            })
        })
        .collect();
    for n in nodes {
        let wanted = (n.spec.availability == Availability::Drain)
            .then(|| status(n, tasks, volumes, &moved, &plan.waits));
        if wanted == n.status.drain {
            continue;
        }
        let mut status = n.status.clone();
        status.drain = wanted;
        let mut o = n.object.clone();
        o.status = serde_json::to_value(status).expect("serialises");
        plan.actions
            .push(Action::Update(o, n.object.metadata.version));
    }
}

/// `waits`: why the planner left a task running this pass, by uid.
fn status(
    n: &Node,
    tasks: &[Task],
    volumes: &Volumes,
    moved: &BTreeSet<(String, String)>,
    waits: &std::collections::BTreeMap<String, &'static str>,
) -> DrainStatus {
    let mut remaining = Vec::new();
    for t in tasks.iter().filter(|t| t.spec.node == n.name()) {
        let reason = if t.status.state == TaskState::Orphaned {
            "unconfirmed"
        } else if t.status.state.is_terminal() || t.status.state == TaskState::Pending {
            continue;
        } else if t.spec.desired_state == DesiredState::Shutdown {
            "stopping"
        } else {
            waits.get(t.uid()).copied().unwrap_or("running")
        };
        remaining.push(DrainItem {
            kind: "Task".into(),
            namespace: t.object.metadata.namespace.clone(),
            name: t.object.metadata.name.clone(),
            reason: reason.into(),
            message: String::new(),
        });
    }
    for (key, v) in volumes {
        if v.status.node != n.name() {
            continue;
        }
        let (reason, message) = match &v.status.migration {
            Some(m) => (
                match m.phase {
                    MigrationPhase::Copying | MigrationPhase::Copied => "copying",
                    MigrationPhase::Finishing => "finishing",
                },
                m.message.clone(),
            ),
            None if moved.contains(key) => ("waiting", String::new()),
            None => continue,
        };
        remaining.push(DrainItem {
            kind: "Volume".into(),
            namespace: key.0.clone(),
            name: key.1.clone(),
            reason: reason.into(),
            message,
        });
    }
    let reason = if n.status.state != NodeState::Ready {
        "unreachable"
    } else if remaining.iter().any(|r| r.reason == "unconfirmed") {
        "unconfirmed"
    } else {
        ""
    };
    let phase = if !reason.is_empty() {
        DrainPhase::Blocked
    } else if remaining.is_empty() {
        DrainPhase::Completed
    } else {
        DrainPhase::Draining
    };
    DrainStatus {
        phase,
        reason: reason.into(),
        remaining,
    }
}
