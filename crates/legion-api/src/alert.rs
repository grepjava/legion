//! Alerts: what an operator acts on, worked out by a manager from the
//! cluster's objects as its replica holds them, the restarts it has seen
//! and its own view of Raft (`Cluster.WatchAlerts`, `legion alerts`, the
//! console's alerts). Nothing here is stored: each manager works its
//! alerts out the same way, so any of them answers, a follower cut off from
//! the leader too, which is when the quorum alert matters.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::node::NodeState;

/// Restarts within [`CRASH_LOOP_WINDOW`] that make a task crash-looping.
pub const CRASH_LOOP_RESTARTS: u32 = 3;
/// How far back restarts count, and how long a task stays crash-looping
/// after its last.
pub const CRASH_LOOP_WINDOW: Duration = Duration::from_secs(300);
/// A CA expiring sooner than this is a warning; sooner than
/// [`CA_CRITICAL`], critical.
pub const CA_WARNING: Duration = Duration::from_secs(30 * 24 * 3600);
pub const CA_CRITICAL: Duration = Duration::from_secs(7 * 24 * 3600);
/// A leader that has not heard from a quorum for longer than this.
pub const QUORUM_SILENCE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Critical,
    Warning,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Critical => "critical",
            Severity::Warning => "warning",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AlertKind {
    NoLeader,
    NoQuorum,
    NodeDown,
    NodeSuspect,
    CrashLoop,
    VolumeLost,
    CaExpiring,
}

impl AlertKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertKind::NoLeader => "no-leader",
            AlertKind::NoQuorum => "no-quorum",
            AlertKind::NodeDown => "node-down",
            AlertKind::NodeSuspect => "node-suspect",
            AlertKind::CrashLoop => "crash-loop",
            AlertKind::VolumeLost => "volume-lost",
            AlertKind::CaExpiring => "ca-expiring",
        }
    }
}

/// One thing to act on. `object` names what it is about as `Kind/name`
/// (in `namespace` when the kind has one), empty for the cluster itself:
/// the way to the cause.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alert {
    pub kind: AlertKind,
    pub severity: Severity,
    pub namespace: String,
    pub object: String,
    pub message: String,
}

impl Alert {
    /// What makes it the same alert from one look to the next.
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.kind.as_str(), self.namespace, self.object)
    }
}

/// A task as alerts look at it.
pub struct TaskInput<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    /// Its restarts seen within [`CRASH_LOOP_WINDOW`].
    pub recent_restarts: u32,
    /// Why it is as it is, from its status.
    pub message: &'a str,
}

/// A Volume as alerts look at it: `lost` names the node it was lost with.
pub struct VolumeInput<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub lost: &'a str,
}

/// What a manager knows that alerts are worked out from.
#[derive(Default)]
pub struct Inputs<'a> {
    pub nodes: Vec<(&'a str, NodeState)>,
    pub tasks: Vec<TaskInput<'a>>,
    pub volumes: Vec<VolumeInput<'a>>,
    /// How long the cluster's CA has left, if this manager holds it.
    pub ca_left: Option<Duration>,
    /// Whether this manager knows of a leader.
    pub leader: bool,
    /// As leader: since it last heard from a quorum.
    pub quorum_silence: Option<Duration>,
}

/// The alerts `i` raises, most severe first, then by kind and object.
pub fn alerts(i: &Inputs) -> Vec<Alert> {
    let mut out = Vec::new();
    let cluster = |kind, severity, message: String| Alert {
        kind,
        severity,
        namespace: String::new(),
        object: String::new(),
        message,
    };
    if !i.leader {
        out.push(cluster(
            AlertKind::NoLeader,
            Severity::Critical,
            "no manager leads: the cluster takes no changes until a majority of managers \
             reach each other"
                .into(),
        ));
    }
    if let Some(s) = i.quorum_silence.filter(|s| *s > QUORUM_SILENCE) {
        out.push(cluster(
            AlertKind::NoQuorum,
            Severity::Critical,
            format!(
                "the leader has not heard from a majority of managers for {} s",
                s.as_secs()
            ),
        ));
    }
    for (name, state) in &i.nodes {
        let (kind, severity, message) = match state {
            NodeState::Down => (
                AlertKind::NodeDown,
                Severity::Critical,
                format!("node {name} is down: its tasks are being placed elsewhere"),
            ),
            NodeState::Suspect => (
                AlertKind::NodeSuspect,
                Severity::Warning,
                format!("node {name} has missed its heartbeats: it gets no new tasks"),
            ),
            _ => continue,
        };
        out.push(Alert {
            kind,
            severity,
            namespace: String::new(),
            object: format!("Node/{name}"),
            message,
        });
    }
    for t in &i.tasks {
        if t.recent_restarts < CRASH_LOOP_RESTARTS {
            continue;
        }
        let why = if t.message.is_empty() {
            String::new()
        } else {
            format!(": {}", t.message)
        };
        out.push(Alert {
            kind: AlertKind::CrashLoop,
            severity: Severity::Critical,
            namespace: t.namespace.to_owned(),
            object: format!("Task/{}", t.name),
            message: format!(
                "task {} restarted {} times in {} min{why}",
                t.name,
                t.recent_restarts,
                CRASH_LOOP_WINDOW.as_secs() / 60
            ),
        });
    }
    for v in i.volumes.iter().filter(|v| !v.lost.is_empty()) {
        out.push(Alert {
            kind: AlertKind::VolumeLost,
            severity: Severity::Critical,
            namespace: v.namespace.to_owned(),
            object: format!("Volume/{}", v.name),
            message: format!(
                "Volume {} was lost with node {}: restore a backup into it on another node",
                v.name, v.lost
            ),
        });
    }
    if let Some(left) = i.ca_left.filter(|l| *l < CA_WARNING) {
        let severity = if left < CA_CRITICAL {
            Severity::Critical
        } else {
            Severity::Warning
        };
        out.push(cluster(
            AlertKind::CaExpiring,
            severity,
            format!(
                "the cluster's CA expires in {} days: rotate it (legion ca rotate)",
                left.as_secs() / 86400
            ),
        ));
    }
    out.sort_by(|a, b| {
        (a.severity, a.kind, &a.namespace, &a.object).cmp(&(
            b.severity,
            b.kind,
            &b.namespace,
            &b.object,
        ))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_healthy_cluster_raises_nothing() {
        let i = Inputs {
            nodes: vec![("n1", NodeState::Ready)],
            tasks: vec![TaskInput {
                namespace: "a",
                name: "web-1",
                recent_restarts: CRASH_LOOP_RESTARTS - 1,
                message: "",
            }],
            volumes: vec![VolumeInput {
                namespace: "a",
                name: "data",
                lost: "",
            }],
            ca_left: Some(CA_WARNING * 2),
            leader: true,
            quorum_silence: Some(Duration::from_secs(1)),
        };
        assert_eq!(alerts(&i), []);
    }

    #[test]
    fn each_problem_raises_its_alert_most_severe_first() {
        let i = Inputs {
            nodes: vec![
                ("n1", NodeState::Ready),
                ("n2", NodeState::Suspect),
                ("n3", NodeState::Down),
            ],
            tasks: vec![TaskInput {
                namespace: "a",
                name: "web-1",
                recent_restarts: CRASH_LOOP_RESTARTS,
                message: "container c exited with 1; restarting in 1s",
            }],
            volumes: vec![VolumeInput {
                namespace: "a",
                name: "data",
                lost: "n3",
            }],
            ca_left: Some(CA_CRITICAL * 2),
            leader: true,
            quorum_silence: Some(QUORUM_SILENCE * 2),
        };
        let got: Vec<_> = alerts(&i)
            .iter()
            .map(|a| (a.severity.as_str(), a.kind.as_str(), a.object.clone()))
            .collect();
        assert_eq!(
            got,
            [
                ("critical", "no-quorum", String::new()),
                ("critical", "node-down", "Node/n3".into()),
                ("critical", "crash-loop", "Task/web-1".into()),
                ("critical", "volume-lost", "Volume/data".into()),
                ("warning", "node-suspect", "Node/n2".into()),
                ("warning", "ca-expiring", String::new()),
            ]
        );
        let crash = &alerts(&i)[2];
        assert_eq!(crash.namespace, "a");
        assert!(
            crash.message.contains("restarted 3 times"),
            "{}",
            crash.message
        );
        assert!(crash.message.contains("exited with 1"), "{}", crash.message);
    }

    #[test]
    fn a_manager_with_no_leader_says_so() {
        let got = alerts(&Inputs::default());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, AlertKind::NoLeader);
    }
}
