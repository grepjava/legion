//! Checks before disruption (CONSOLE.md phase 11): what a drain, a
//! Volume's move, a node's removal or a Service's deletion would do, worked
//! out by the leader before anything is changed (`Cluster.DryRun`). The
//! controller is run over a copy of the cluster, as it is and with the
//! change, until each settles; what differs is the change's doing: work no
//! node would take, a Volume that would stay behind or be lost, a node
//! without room for what moves there (blocking), and what a disruption
//! budget holds back or what a deletion leaves (warning). `legion ...
//! --dry-run` prints it; the console's confirmation shows it, and does not
//! confirm while anything blocks.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::activity::bytes;

/// The change checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "kebab-case")]
pub enum Change {
    /// `legion node drain NODE`.
    Drain { node: String },
    /// `legion volume move VOLUME NODE`.
    #[serde(rename_all = "camelCase")]
    MoveVolume {
        namespace: String,
        volume: String,
        node: String,
    },
    /// `legion delete node NODE`.
    RemoveNode { node: String },
    /// `legion delete service SERVICE`.
    #[serde(rename_all = "camelCase")]
    DeleteService { namespace: String, service: String },
    /// `legion node demote NODE`: a manager made a worker.
    Demote { node: String },
    /// Services pinned to `node` (`placement.constraints: node.name ==
    /// NODE`, any other node pin replaced), every Volume they mount moved
    /// there with them: the console's Move to.
    #[serde(rename_all = "camelCase")]
    MoveServices {
        namespace: String,
        services: Vec<String>,
        node: String,
    },
}

impl Change {
    /// In words, as a confirmation asks: `drain node n2`.
    pub fn describe(&self) -> String {
        match self {
            Change::Drain { node } => format!("drain node {node}"),
            Change::MoveVolume {
                namespace,
                volume,
                node,
            } => format!("move Volume {namespace}/{volume} to {node}"),
            Change::RemoveNode { node } => format!("remove node {node}"),
            Change::DeleteService { namespace, service } => {
                format!("delete Service {namespace}/{service}")
            }
            Change::Demote { node } => format!("demote node {node} to a worker"),
            Change::MoveServices {
                namespace,
                services,
                node,
            } => format!(
                "move {} in {namespace} to {node}",
                services
                    .iter()
                    .map(|s| format!("Service {s}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// The change would not end where it was asked to: refused, unless
    /// forced (`--force`).
    Blocking,
    /// It would, but it is worth knowing first.
    Warning,
}

/// One thing the change would do that is worth knowing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub severity: Severity,
    /// A stable word a tool can act on: `unplaced` (tasks no node would
    /// take), `stranded` (a Volume the change would leave where it is),
    /// `held` (a Volume's move waiting on tasks that may not stop), `stays`
    /// (a task left on a drained node),
    /// `no-room` (a node without the space for what moves there), `lost`
    /// (data that goes with a removed node), `unreachable` (a node whose
    /// tasks are not known), `budget` (tasks that move a few at a time),
    /// `switch-over` (a move past it, which the change would call off),
    /// `replica` (a Volume carrying on from an older copy), `stops` (a
    /// move's tasks stopping for its last part), `kept`
    /// (Volumes a deletion leaves), `manager` (a manager leaving Raft),
    /// `unsettled` (the check could not see the end).
    pub reason: String,
    /// What it is about, as `Kind namespace/name` (`Kind name` for a
    /// Node), empty for the change as a whole.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub object: String,
    /// The object's namespace, for who may read it; empty for a Node.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    pub message: String,
}

/// What a manager leaving the Raft membership (demoted, or its node
/// removed) does to the managers: `managers` as they are, `leader` the
/// one that leads now. The leader leaving means an election, about 16 s
/// with no leader (`leader`); fewer than three voters left can lose none
/// (`quorum`); one manager left that may lead, the others witnesses, can't
/// lose it (`leaders`), and none blocks. Nothing for a node that is no
/// voter.
pub fn membership(node: &str, managers: &[crate::ha::Manager], leader: &str) -> Vec<Finding> {
    let Some(leaving) = managers.iter().find(|m| m.name == node && m.voter) else {
        return Vec::new();
    };
    let left: Vec<_> = managers
        .iter()
        .filter(|m| m.voter && m.name != leaving.name)
        .collect();
    let voters = left.len();
    let leaders = left.iter().filter(|m| !m.witness).count();
    let finding = |severity, reason: &str, message: String| Finding {
        severity,
        reason: reason.into(),
        object: format!("Node {node}"),
        namespace: String::new(),
        message,
    };
    let mut out = Vec::new();
    if voters > 0 && leaders == 0 {
        out.push(finding(
            Severity::Blocking,
            "leaders",
            "it is the last manager that may lead: only witnesses would be left, and they never \
             lead"
                .into(),
        ));
    } else if voters >= 2 && leaders == 1 {
        out.push(finding(
            Severity::Warning,
            "leaders",
            "one manager that may lead would be left, the others witnesses: if it is lost, no \
             leader is chosen"
                .into(),
        ));
    }
    match voters {
        1 => out.push(finding(
            Severity::Warning,
            "quorum",
            "one voting manager would be left: if it is lost, nothing is placed or changed until \
             it is back"
                .into(),
        )),
        2 => out.push(finding(
            Severity::Warning,
            "quorum",
            "two voting managers would be left: losing either then stops the cluster".into(),
        )),
        _ => {}
    }
    if leaving.name == leader && voters > 0 {
        out.push(finding(
            Severity::Warning,
            "leader",
            "it leads: the other managers elect another, about 16 s with no leader (nothing is \
             placed or changed; running tasks carry on)"
                .into(),
        ));
    }
    out
}

/// A Volume whose data would move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Move {
    pub namespace: String,
    pub volume: String,
    pub from: String,
    pub to: String,
    /// What its dataset uses, as its node last said; none if not known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

/// What the change would do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DryRun {
    pub change: Change,
    /// Blocking first.
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub moves: Vec<Move>,
    /// Tasks the change would stop, to run again elsewhere or not at all.
    #[serde(default)]
    pub stopped: u32,
    /// Findings left out, in namespaces the caller may not read.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hidden: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl DryRun {
    /// Anything blocking.
    pub fn blocks(&self) -> bool {
        self.findings
            .iter()
            .any(|f| f.severity == Severity::Blocking)
    }

    /// Sorted as shown: blocking first, then by object.
    pub fn sort(&mut self) {
        self.findings.sort_by(|a, b| {
            (a.severity, &a.reason, &a.object).cmp(&(b.severity, &b.reason, &b.object))
        });
        self.moves
            .sort_by(|a, b| (&a.namespace, &a.volume).cmp(&(&b.namespace, &b.volume)));
    }

    /// As `--dry-run` prints it.
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "To {}:", self.change.describe());
        if self.stopped > 0 {
            let _ = writeln!(s, "  {} task(s) stop where they run", self.stopped);
        }
        for m in &self.moves {
            let size = m.bytes.map_or_else(|| "size not known".to_owned(), bytes);
            let _ = writeln!(
                s,
                "  Volume {}/{} moves from {} to {} ({size})",
                m.namespace, m.volume, m.from, m.to
            );
        }
        for f in &self.findings {
            let word = match f.severity {
                Severity::Blocking => "BLOCKS",
                Severity::Warning => "warning",
            };
            let about = if f.object.is_empty() {
                String::new()
            } else {
                format!("{}: ", f.object)
            };
            let _ = writeln!(s, "  {word} ({}): {about}{}", f.reason, f.message);
        }
        if self.hidden > 0 {
            let _ = writeln!(
                s,
                "  and {} more in namespaces you may not read",
                self.hidden
            );
        }
        if self.findings.is_empty() && self.moves.is_empty() && self.stopped == 0 {
            let _ = writeln!(s, "  nothing else changes");
        }
        let _ = write!(
            s,
            "{}",
            if self.blocks() {
                "It would not complete: refused (--force to go ahead anyway)."
            } else {
                "Nothing blocks it."
            }
        );
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_change_reads_back_as_written() {
        for (c, json) in [
            (
                Change::Drain { node: "n2".into() },
                r#"{"change":"drain","node":"n2"}"#,
            ),
            (
                Change::MoveVolume {
                    namespace: "data".into(),
                    volume: "pg-db-0".into(),
                    node: "n3".into(),
                },
                r#"{"change":"move-volume","namespace":"data","volume":"pg-db-0","node":"n3"}"#,
            ),
            (
                Change::RemoveNode { node: "n2".into() },
                r#"{"change":"remove-node","node":"n2"}"#,
            ),
            (
                Change::DeleteService {
                    namespace: "shop".into(),
                    service: "db".into(),
                },
                r#"{"change":"delete-service","namespace":"shop","service":"db"}"#,
            ),
        ] {
            assert_eq!(serde_json::to_string(&c).unwrap(), json);
            assert_eq!(serde_json::from_str::<Change>(json).unwrap(), c);
        }
    }

    #[test]
    fn it_says_what_blocks_first() {
        let mut d = DryRun {
            change: Change::Drain { node: "n2".into() },
            findings: vec![
                Finding {
                    severity: Severity::Warning,
                    reason: "budget".into(),
                    object: "Service shop/web".into(),
                    namespace: "shop".into(),
                    message: "1 task at a time (maxUnavailable 1)".into(),
                },
                Finding {
                    severity: Severity::Blocking,
                    reason: "stranded".into(),
                    object: "Volume shop/data-db-0".into(),
                    namespace: "shop".into(),
                    message: "stays on n2: no node takes it (n1 constraints)".into(),
                },
            ],
            moves: vec![Move {
                namespace: "shop".into(),
                volume: "data-db-1".into(),
                from: "n2".into(),
                to: "n3".into(),
                bytes: Some(3 << 20),
            }],
            stopped: 2,
            hidden: 0,
        };
        d.sort();
        assert!(d.blocks());
        assert_eq!(
            d.render(),
            "To drain node n2:\n  2 task(s) stop where they run\n  Volume shop/data-db-1 moves from n2 to n3 (3.0 MiB)\n  BLOCKS (stranded): Volume shop/data-db-0: stays on n2: no node takes it (n1 constraints)\n  warning (budget): Service shop/web: 1 task at a time (maxUnavailable 1)\nIt would not complete: refused (--force to go ahead anyway)."
        );
        d.findings.remove(0);
        assert!(!d.blocks());
        assert!(d.render().ends_with("Nothing blocks it."));
    }
}

#[cfg(test)]
mod membership_tests {
    use super::*;
    use crate::ha::Manager;

    fn m(name: &str, witness: bool) -> Manager {
        Manager {
            name: name.into(),
            voter: true,
            witness,
        }
    }

    fn reasons(f: &[Finding]) -> Vec<(&str, Severity)> {
        f.iter().map(|f| (f.reason.as_str(), f.severity)).collect()
    }

    #[test]
    fn a_manager_leaving_says_what_the_others_are_left_with() {
        let three = [m("m1", false), m("m2", false), m("w", true)];
        // The leader of two managers and a witness: an election, two voters
        // left, one of them able to lead.
        assert_eq!(
            reasons(&membership("m1", &three, "m1")),
            [
                ("leaders", Severity::Warning),
                ("quorum", Severity::Warning),
                ("leader", Severity::Warning)
            ]
        );
        // A follower: no election.
        assert!(!reasons(&membership("m2", &three, "m1")).contains(&("leader", Severity::Warning)));
        // Of one manager and two witnesses, the manager: none left to lead.
        let witnessed = [m("m1", false), m("w1", true), m("w2", true)];
        assert_eq!(
            reasons(&membership("m1", &witnessed, "m1"))[0],
            ("leaders", Severity::Blocking)
        );
        // Of five full managers, one: nothing worth saying.
        let five: Vec<_> = (1..=5).map(|i| m(&format!("m{i}"), false)).collect();
        assert!(membership("m5", &five, "m1").is_empty());
        // A node that is no voter: nothing.
        assert!(membership("n9", &three, "m1").is_empty());
    }
}
