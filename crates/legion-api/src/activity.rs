//! Long operations (CONSOLE.md phase 10): a node's drain, a Volume's move,
//! a restore and a replica's whole copy, each with the stage it is at, the
//! bytes it has sent, when it began and ended and how, and what it said on
//! the way. The leader follows them, as it runs the moves (`Cluster.
//! Activity`); a new leader starts with those under way.

use serde::{Deserialize, Serialize};

use crate::audit::Target;
use crate::volume::MigrationPhase;

/// The most lines an operation keeps of what it said.
pub const OUTPUT_MAX: usize = 200;

/// One long operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    /// Stable while it runs: its kind, its target and when it began.
    pub id: String,
    pub kind: OperationKind,
    pub target: Target,
    /// What it does, in words: `drain node n2`, `move Volume data/pg-db-0
    /// from n2 to n3`.
    pub what: String,
    /// Who asked for it, as the audit trail names them: given to cluster
    /// admins only, as the trail is theirs. Empty for what the cluster does
    /// of its own (a replica's copy) or who is not known yet.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
    /// The operation this one is part of: a drain, for the moves of the
    /// Volumes it sends off its node.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub part_of: String,
    pub began_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_ms: Option<i64>,
    pub state: OperationState,
    /// Where it is (see [`Stage`]); for one ended, the last it reached.
    pub stage: Stage,
    /// Sent so far by the transfer under way; for one ended, by the last.
    #[serde(default)]
    pub bytes: u64,
    /// Why it waits or failed, or how it ended.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Each stage it went through, in turn, with when it began.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stages: Vec<StageMark>,
    /// What it said, oldest first, at most [`OUTPUT_MAX`] lines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output: Vec<Line>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    Drain,
    Move,
    Restore,
    ReplicaCopy,
    Backup,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OperationKind::Drain => "drain",
            OperationKind::Move => "move",
            OperationKind::Restore => "restore",
            OperationKind::ReplicaCopy => "replica-copy",
            OperationKind::Backup => "backup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationState {
    Running,
    Succeeded,
    Failed,
    /// Called off: a drain ended by making the node active again, a move
    /// its Volume no longer needs, a restore whose sender went.
    Cancelled,
}

impl OperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            OperationState::Running => "running",
            OperationState::Succeeded => "succeeded",
            OperationState::Failed => "failed",
            OperationState::Cancelled => "cancelled",
        }
    }
}

/// Where an operation is.
///
/// A move: `preparing` (decided, nothing sent yet, or waiting to try
/// again), `full-copy` (the whole Volume sent while its tasks run),
/// `stopping` (the copy is there; its tasks stop for the rest), then
/// `switch-over` (its tasks stopped: what changed since the full copy is
/// sent and the Volume becomes the new node's; it can no longer be called
/// off), and `cleanup` (it runs on the new node; the old copy is
/// dropped). A drain: `draining` or `blocked`, then `completed`. A restore
/// or a replica's copy: `receiving` (`full-copy` for a replica), then
/// `registering` for a restore, as the Volume is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    Preparing,
    FullCopy,
    Stopping,
    SwitchOver,
    Cleanup,
    Draining,
    Blocked,
    Completed,
    Receiving,
    Registering,
    Sending,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Preparing => "preparing",
            Stage::FullCopy => "full-copy",
            Stage::Stopping => "stopping",
            Stage::SwitchOver => "switch-over",
            Stage::Cleanup => "cleanup",
            Stage::Draining => "draining",
            Stage::Blocked => "blocked",
            Stage::Completed => "completed",
            Stage::Receiving => "receiving",
            Stage::Registering => "registering",
            Stage::Sending => "sending",
        }
    }

    /// The stage of a move whose migration is at `phase`, `sending` while
    /// the leader's migrator has a transfer of it under way.
    pub fn of_move(phase: MigrationPhase, sending: bool) -> Stage {
        match phase {
            MigrationPhase::Copying if sending => Stage::FullCopy,
            MigrationPhase::Copying => Stage::Preparing,
            MigrationPhase::Copied => Stage::Stopping,
            MigrationPhase::Finishing => Stage::SwitchOver,
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StageMark {
    pub stage: Stage,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Line {
    pub at_ms: i64,
    pub text: String,
}

impl Operation {
    /// A new one, running from `at_ms` at `stage`.
    pub fn new(
        kind: OperationKind,
        target: Target,
        what: String,
        at_ms: i64,
        stage: Stage,
    ) -> Self {
        let id = format!(
            "{}/{}/{}/{}/{at_ms}",
            kind.as_str(),
            target.kind,
            target.namespace,
            target.name
        );
        Operation {
            id,
            kind,
            target,
            what,
            by: String::new(),
            part_of: String::new(),
            began_ms: at_ms,
            ended_ms: None,
            state: OperationState::Running,
            stage,
            bytes: 0,
            message: String::new(),
            stages: vec![StageMark { stage, at_ms }],
            output: vec![Line {
                at_ms,
                text: stage.as_str().to_owned(),
            }],
        }
    }

    pub fn running(&self) -> bool {
        self.state == OperationState::Running
    }

    /// Says `text`, at `at_ms`; the oldest lines go past [`OUTPUT_MAX`].
    pub fn say(&mut self, at_ms: i64, text: impl Into<String>) {
        self.output.push(Line {
            at_ms,
            text: text.into(),
        });
        if self.output.len() > OUTPUT_MAX {
            let over = self.output.len() - OUTPUT_MAX;
            self.output.drain(..over);
        }
    }

    /// Moves on to `stage`, saying so; whether it is a new one. What the
    /// stage before sent is said with it, and the count starts over.
    pub fn enter(&mut self, at_ms: i64, stage: Stage) -> bool {
        if self.stage == stage {
            return false;
        }
        if self.bytes > 0 {
            let sent = format!("{}: {} sent", self.stage, bytes(self.bytes));
            self.say(at_ms, sent);
        }
        self.stage = stage;
        self.bytes = 0;
        self.stages.push(StageMark { stage, at_ms });
        self.say(at_ms, stage.as_str());
        true
    }

    /// Ends it as `state`, saying `message`.
    pub fn end(&mut self, at_ms: i64, state: OperationState, message: impl Into<String>) {
        let message = message.into();
        if self.bytes > 0 {
            let sent = format!("{}: {} sent", self.stage, bytes(self.bytes));
            self.say(at_ms, sent);
        }
        self.state = state;
        self.ended_ms = Some(at_ms);
        let line = match message.is_empty() {
            true => state.as_str().to_owned(),
            false => format!("{}: {message}", state.as_str()),
        };
        self.say(at_ms, line);
        self.message = message;
    }

    /// Notes why it waits, once each time it changes; an empty one clears.
    pub fn note(&mut self, at_ms: i64, message: &str) {
        if self.message == message {
            return;
        }
        message.clone_into(&mut self.message);
        if !message.is_empty() {
            self.say(at_ms, message.to_owned());
        }
    }
}

/// Bytes in the largest binary unit that keeps a whole number before the
/// point: `512 B`, `3.2 MiB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64 / 1024.0;
    let mut unit = UNITS[0];
    for u in &UNITS[1..] {
        if v < 1024.0 {
            break;
        }
        v /= 1024.0;
        unit = u;
    }
    format!("{v:.1} {unit}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volume() -> Target {
        Target {
            kind: "Volume".into(),
            namespace: "data".into(),
            name: "pg-db-0".into(),
        }
    }

    #[test]
    fn a_move_says_each_stage_in_turn_and_what_each_sent() {
        let mut op = Operation::new(
            OperationKind::Move,
            volume(),
            "move".into(),
            1_000,
            Stage::Preparing,
        );
        assert_eq!(op.id, "move/Volume/data/pg-db-0/1000");
        assert!(op.enter(2_000, Stage::of_move(MigrationPhase::Copying, true)));
        op.bytes = 3 * 1024 * 1024 + 200 * 1024;
        assert!(!op.enter(2_500, Stage::FullCopy), "the same stage");
        assert!(op.enter(3_000, Stage::of_move(MigrationPhase::Copied, false)));
        assert_eq!(op.bytes, 0, "counted again");
        assert!(op.enter(4_000, Stage::of_move(MigrationPhase::Finishing, false)));
        op.end(5_000, OperationState::Succeeded, "");
        let stages: Vec<&str> = op.stages.iter().map(|m| m.stage.as_str()).collect();
        assert_eq!(
            stages,
            ["preparing", "full-copy", "stopping", "switch-over"]
        );
        let said: Vec<&str> = op.output.iter().map(|l| l.text.as_str()).collect();
        assert_eq!(
            said,
            [
                "preparing",
                "full-copy",
                "full-copy: 3.2 MiB sent",
                "stopping",
                "switch-over",
                "succeeded"
            ]
        );
        assert_eq!(op.ended_ms, Some(5_000));
    }

    #[test]
    fn a_note_is_said_once_and_the_output_keeps_the_newest() {
        let mut op = Operation::new(
            OperationKind::Drain,
            volume(),
            "drain".into(),
            0,
            Stage::Draining,
        );
        op.note(1, "waiting");
        op.note(2, "waiting");
        op.note(3, "");
        assert_eq!(op.output.len(), 2, "its stage, then the note");
        for i in 0..(OUTPUT_MAX as i64 + 5) {
            op.say(i, format!("line {i}"));
        }
        assert_eq!(op.output.len(), OUTPUT_MAX);
        assert_eq!(
            op.output.last().unwrap().text,
            format!("line {}", OUTPUT_MAX + 4)
        );
    }

    #[test]
    fn bytes_read_in_binary_units() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(5 * 1024 * 1024 * 1024), "5.0 GiB");
    }

    #[test]
    fn an_operation_reads_as_camel_case_json() {
        let op = Operation::new(
            OperationKind::ReplicaCopy,
            volume(),
            "copy".into(),
            7,
            Stage::FullCopy,
        );
        let v = serde_json::to_value(&op).unwrap();
        assert_eq!(v["kind"], "replica-copy");
        assert_eq!(v["stage"], "full-copy");
        assert_eq!(v["beganMs"], 7);
        assert_eq!(v["state"], "running");
        let back: Operation = serde_json::from_value(v).unwrap();
        assert_eq!(back, op);
    }
}
