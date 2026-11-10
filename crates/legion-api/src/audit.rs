//! Audit events (PLAN.md phase 7): who did what to which object, and how it
//! ended. Kept by the managers in their replicated store for a while, and
//! listed by `legion audit`. An event names things and never holds what
//! they contain: no spec, and so no Secret's or Config's value.

use serde::{Deserialize, Serialize};

/// The longest message an event keeps, in bytes.
pub const MESSAGE_MAX: usize = 512;

/// One audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    /// When it was decided: milliseconds since the Unix epoch.
    pub time_ms: u64,
    pub actor: Actor,
    /// What was asked: `apply`, `delete`, `exec`, `logs`, `port-forward`,
    /// `init`, `join-token`, `issue-user`, `join`, `volume-backup`,
    /// `volume-restore`, `autolock`.
    pub action: String,
    #[serde(default, skip_serializing_if = "Target::is_empty")]
    pub target: Target,
    pub result: Outcome,
    /// Why it was refused or failed; empty when it went through.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// The manager that decided it.
    pub manager: String,
}

/// Who asked: as their certificate names them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    /// `admin`, `user`, `manager`, `worker`, or `none` (no TLS: tests).
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

/// What it was done to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

impl Target {
    pub fn is_empty(&self) -> bool {
        self.kind.is_empty() && self.namespace.is_empty() && self.name.is_empty()
    }
}

/// How it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Ok,
    /// Refused for the caller's rights.
    Denied,
    /// It failed otherwise.
    Error,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Denied => "denied",
            Outcome::Error => "error",
        }
    }
}

impl AuditEvent {
    /// `message`, cut to [`MESSAGE_MAX`] bytes at a character boundary.
    pub fn with_message(mut self, message: &str) -> AuditEvent {
        let mut end = message.len().min(MESSAGE_MAX);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        self.message = message[..end].to_owned();
        self
    }
}

/// Which events to list; each field set narrows it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    /// From this time on (milliseconds since the epoch).
    #[serde(default)]
    pub since_ms: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Outcome>,
}

impl Filter {
    pub fn matches(&self, e: &AuditEvent) -> bool {
        let is = |want: &str, got: &str| want.is_empty() || want == got;
        e.time_ms >= self.since_ms
            && is(&self.actor, &e.actor.name)
            && is(&self.action, &e.action)
            && is(&self.namespace, &e.target.namespace)
            && (self.kind.is_empty() || self.kind.eq_ignore_ascii_case(&e.target.kind))
            && self.result.is_none_or(|r| r == e.result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> AuditEvent {
        AuditEvent {
            time_ms: 1_000,
            actor: Actor {
                kind: "user".into(),
                name: "alice".into(),
            },
            action: "apply".into(),
            target: Target {
                kind: "Secret".into(),
                namespace: "shop".into(),
                name: "db".into(),
            },
            result: Outcome::Ok,
            message: String::new(),
            manager: "m1".into(),
        }
    }

    #[test]
    fn events_are_written_compactly_and_read_back() {
        let e = event();
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(
            json,
            r#"{"timeMs":1000,"actor":{"kind":"user","name":"alice"},"action":"apply","target":{"kind":"Secret","namespace":"shop","name":"db"},"result":"ok","manager":"m1"}"#
        );
        assert_eq!(serde_json::from_str::<AuditEvent>(&json).unwrap(), e);
    }

    #[test]
    fn a_filter_narrows_by_each_field_it_sets() {
        let e = event();
        assert!(Filter::default().matches(&e));
        let f = |f: Filter| f.matches(&e);
        assert!(f(Filter {
            actor: "alice".into(),
            namespace: "shop".into(),
            kind: "secret".into(),
            result: Some(Outcome::Ok),
            ..Default::default()
        }));
        assert!(!f(Filter {
            actor: "bob".into(),
            ..Default::default()
        }));
        assert!(!f(Filter {
            since_ms: 1_001,
            ..Default::default()
        }));
        assert!(!f(Filter {
            result: Some(Outcome::Denied),
            ..Default::default()
        }));
        assert!(!f(Filter {
            action: "delete".into(),
            ..Default::default()
        }));
    }

    #[test]
    fn a_message_is_cut_short_on_a_character() {
        let long = "é".repeat(MESSAGE_MAX);
        let e = event().with_message(&long);
        assert!(e.message.len() <= MESSAGE_MAX);
        assert!(e.message.chars().all(|c| c == 'é'));
    }
}
