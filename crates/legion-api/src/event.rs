//! Events (CONSOLE.md phase 2): what happened to an object, for people
//! asking why. Nothing writes them: each manager works them out from the
//! changes its replica sees, comparing an object's [`Summary`] before and
//! after, and keeps the recent ones in memory (`Cluster.Events`). A manager
//! started since knows only what happened since.

use serde::{Deserialize, Serialize};

use crate::{Kind, Object};

/// One thing that happened to an object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    /// When this manager saw it, in milliseconds since the epoch.
    pub at_ms: i64,
    pub kind: Kind,
    /// Empty for a cluster's objects (Nodes).
    pub namespace: String,
    pub name: String,
    /// What happened, in a word or two: `Created`, `Running`, `Restarted`.
    pub reason: String,
    /// Why, or more of what, for people; may be empty.
    pub message: String,
    /// A task's Service or Job, so their pages show their tasks' events;
    /// empty for other objects.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
}

/// The kinds events are worked out for.
pub const KINDS: [Kind; 6] = [
    Kind::Task,
    Kind::Service,
    Kind::Job,
    Kind::Node,
    Kind::Volume,
    Kind::Build,
];

/// What of an object events are worked out from, small enough to keep for
/// every object a manager follows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    /// A task's, node's or job's state, a build's phase.
    pub state: String,
    pub message: String,
    /// A task's node, a volume's.
    pub node: String,
    pub restarts: u64,
    pub ready: bool,
    pub generation: u64,
    /// A service's rollout state.
    pub update: String,
    /// A node's availability.
    pub availability: String,
    /// Why a volume is lost, empty while it is not.
    pub lost: String,
    pub owner: String,
}

fn text(v: &serde_json::Value) -> String {
    v.as_str().unwrap_or_default().to_owned()
}

impl Summary {
    /// The summary of `o`, if events are worked out for its kind.
    pub fn of(o: &Object) -> Option<Summary> {
        let (spec, status) = (&o.spec, &o.status);
        let mut s = Summary {
            message: text(&status["message"]),
            ..Summary::default()
        };
        match o.kind {
            Kind::Task => {
                s.state = text(&status["state"]);
                s.node = text(&spec["node"]);
                s.restarts = status["restarts"].as_u64().unwrap_or(0);
                s.ready = status["ready"].as_bool().unwrap_or(false);
                s.owner = text(&spec["service"]);
            }
            Kind::Service => {
                s.generation = o.metadata.generation;
                s.update = text(&status["update"]["state"]);
                s.message = text(&status["update"]["message"]);
            }
            Kind::Job => s.state = text(&status["state"]),
            Kind::Build => s.state = text(&status["phase"]),
            Kind::Node => {
                s.state = text(&status["state"]);
                s.availability = text(&spec["availability"]);
                s.message = String::new();
            }
            Kind::Volume => {
                s.node = text(&status["node"]);
                s.lost = text(&status["lost"]);
                s.message = String::new();
            }
            _ => return None,
        }
        Some(s)
    }
}

/// Capitalised, as a reason: `running` → `Running`.
fn reason(word: &str) -> String {
    let mut c = word.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

/// What happened to an object of `kind` going from `before` to `after`
/// (`None`: not there): each a reason and a message.
pub fn changes(
    kind: Kind,
    before: Option<&Summary>,
    after: Option<&Summary>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let (b, a) = match (before, after) {
        (None, None) => return out,
        (Some(_), None) => {
            out.push(("Deleted".into(), String::new()));
            return out;
        }
        (None, Some(a)) => {
            let what = match kind {
                Kind::Task if !a.node.is_empty() => format!("on {}", a.node),
                Kind::Task => a.message.clone(),
                _ => String::new(),
            };
            out.push(("Created".into(), what));
            return out;
        }
        (Some(b), Some(a)) => (b, a),
    };
    match kind {
        Kind::Task => {
            if a.node != b.node && !a.node.is_empty() {
                out.push(("Placed".into(), format!("on {}", a.node)));
            }
            if a.state != b.state && !a.state.is_empty() {
                out.push((reason(&a.state), a.message.clone()));
            } else if a.message != b.message && !a.message.is_empty() {
                // Still in its state, for another reason: why it waits.
                out.push((reason(&a.state), a.message.clone()));
            }
            if a.restarts > b.restarts {
                out.push((
                    "Restarted".into(),
                    format!(
                        "{} time{} in all",
                        a.restarts,
                        if a.restarts == 1 { "" } else { "s" }
                    ),
                ));
            }
            if a.ready != b.ready {
                let r = if a.ready { "Ready" } else { "NotReady" };
                out.push((r.into(), String::new()));
            }
        }
        Kind::Service => {
            if a.generation > b.generation {
                out.push(("Changed".into(), format!("generation {}", a.generation)));
            }
            if a.update != b.update && !a.update.is_empty() {
                out.push((format!("Rollout{}", reason(&a.update)), a.message.clone()));
            }
        }
        Kind::Job | Kind::Build => {
            if a.state != b.state && !a.state.is_empty() {
                out.push((reason(&a.state), a.message.clone()));
            }
        }
        Kind::Node => {
            if a.state != b.state && !a.state.is_empty() {
                out.push((reason(&a.state), String::new()));
            }
            if a.availability != b.availability && !a.availability.is_empty() {
                out.push((reason(&a.availability), String::new()));
            }
        }
        Kind::Volume => {
            if a.node != b.node && !a.node.is_empty() {
                out.push(("OnNode".into(), a.node.clone()));
            }
            if a.lost != b.lost && !a.lost.is_empty() {
                out.push(("Lost".into(), a.lost.clone()));
            }
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(state: &str, node: &str, message: &str, restarts: u64, ready: bool) -> Summary {
        Summary {
            state: state.into(),
            node: node.into(),
            message: message.into(),
            restarts,
            ready,
            owner: "web".into(),
            ..Summary::default()
        }
    }

    fn reasons(v: Vec<(String, String)>) -> Vec<String> {
        v.into_iter().map(|(r, m)| format!("{r}: {m}")).collect()
    }

    #[test]
    fn a_task_waits_is_placed_runs_and_restarts() {
        let waiting = task("pending", "", "no node can run it: memory", 0, false);
        assert_eq!(
            reasons(changes(Kind::Task, None, Some(&waiting))),
            ["Created: no node can run it: memory"]
        );
        let other = task("pending", "", "no node can run it: platform", 0, false);
        assert_eq!(
            reasons(changes(Kind::Task, Some(&waiting), Some(&other))),
            ["Pending: no node can run it: platform"]
        );
        let placed = task("assigned", "n2", "", 0, false);
        assert_eq!(
            reasons(changes(Kind::Task, Some(&other), Some(&placed))),
            ["Placed: on n2", "Assigned: "]
        );
        let running = task("running", "n2", "", 0, true);
        assert_eq!(
            reasons(changes(Kind::Task, Some(&placed), Some(&running))),
            ["Running: ", "Ready: "]
        );
        let restarted = task("running", "n2", "", 2, true);
        assert_eq!(
            reasons(changes(Kind::Task, Some(&running), Some(&restarted))),
            ["Restarted: 2 times in all"]
        );
        assert!(changes(Kind::Task, Some(&restarted), Some(&restarted)).is_empty());
        assert_eq!(
            reasons(changes(Kind::Task, Some(&restarted), None)),
            ["Deleted: "]
        );
    }

    #[test]
    fn nodes_services_and_volumes() {
        let ready = Summary {
            state: "ready".into(),
            availability: "active".into(),
            ..Summary::default()
        };
        let drained = Summary {
            state: "down".into(),
            availability: "drain".into(),
            ..Summary::default()
        };
        assert_eq!(
            reasons(changes(Kind::Node, Some(&ready), Some(&drained))),
            ["Down: ", "Drain: "]
        );
        let one = Summary {
            generation: 1,
            ..Summary::default()
        };
        let rolling = Summary {
            generation: 2,
            update: "updating".into(),
            ..Summary::default()
        };
        assert_eq!(
            reasons(changes(Kind::Service, Some(&one), Some(&rolling))),
            ["Changed: generation 2", "RolloutUpdating: "]
        );
        let lost = Summary {
            node: "n1".into(),
            lost: "its node was removed".into(),
            ..Summary::default()
        };
        assert_eq!(
            reasons(changes(
                Kind::Volume,
                Some(&Summary {
                    node: "n1".into(),
                    ..Summary::default()
                }),
                Some(&lost)
            )),
            ["Lost: its node was removed"]
        );
    }

    #[test]
    fn summaries_read_the_objects() {
        let mut t = Object::new(
            Kind::Task,
            "shop",
            "web-1",
            serde_json::json!({ "service": "web", "node": "n1" }),
        );
        t.status = serde_json::json!({ "state": "running", "restarts": 1, "ready": true });
        let s = Summary::of(&t).unwrap();
        assert_eq!(
            (
                s.state.as_str(),
                s.node.as_str(),
                s.restarts,
                s.ready,
                s.owner.as_str()
            ),
            ("running", "n1", 1, true, "web")
        );
        let c = Object::new(Kind::Config, "shop", "c", serde_json::json!({}));
        assert!(Summary::of(&c).is_none());
    }
}
