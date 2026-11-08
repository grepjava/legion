//! Where each draining node's drain stands (PLAN.md, phase 5), written to
//! its status for `legion node drain --wait` and other tools to follow. The
//! moving itself is the Service controller's and the migrator's; this only
//! says what is left, from what the store shows. Unknown never counts as
//! released: a node that isn't ready, or a task orphaned there and not yet
//! seen to stop, blocks the drain until it is known.

use legion_api::node::{Availability, DrainItem, DrainPhase, DrainStatus, NodeState};
use legion_api::task::DesiredState;
use legion_api::task::TaskState;
use legion_api::volume::MigrationPhase;

use super::{Action, Node, Plan, Task, Volumes};

pub(super) fn drains(tasks: &[Task], nodes: &[Node], volumes: &Volumes, plan: &mut Plan) {
    for n in nodes {
        let wanted = (n.spec.availability == Availability::Drain)
            .then(|| status(n, tasks, volumes, &plan.waits));
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
        });
    }
    for ((namespace, name), v) in volumes {
        let Some(m) = &v.status.migration else {
            continue;
        };
        if v.status.node != n.name() {
            continue;
        }
        let reason = match m.phase {
            MigrationPhase::Copying | MigrationPhase::Copied => "copying",
            MigrationPhase::Finishing => "finishing",
        };
        remaining.push(DrainItem {
            kind: "Volume".into(),
            namespace: namespace.clone(),
            name: name.clone(),
            reason: reason.into(),
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
