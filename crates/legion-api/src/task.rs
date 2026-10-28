//! `Task`: one run of a service's pod. Created by the Service controller,
//! placed by the scheduler, run by an agent; never written through the API.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};
use crate::service::PodSpec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskSpec {
    /// The owning Service, or Job if `job` is set, in the task's namespace.
    pub service: String,
    pub service_uid: String,
    /// The owner's generation whose pod this task runs.
    pub service_generation: u64,
    /// The owner is a Job: the task is no endpoint of a Service.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub job: bool,
    /// Replica number of a replicated service, from 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,
    /// The node it runs on; empty until scheduled. Fixed at creation for
    /// global services.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    #[serde(default)]
    pub desired_state: DesiredState,
    pub pod: PodSpec,
}

impl Spec for TaskSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        self.pod.admit("pod")
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredState {
    #[default]
    Running,
    Shutdown,
}

/// Lifecycle (DESIGN.md §7):
///
/// ```text
/// pending → assigned → accepted → preparing → starting → running → complete
///                                     │            │          │
///                                     └────────────┴──────────┴──► failed / rejected
///                        (any state) → shutdown          node lost → orphaned
/// ```
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    #[default]
    Pending,
    Assigned,
    Accepted,
    Preparing,
    Starting,
    Running,
    Complete,
    Failed,
    Rejected,
    Shutdown,
    Orphaned,
}

impl TaskState {
    pub const ALL: [TaskState; 11] = [
        TaskState::Pending,
        TaskState::Assigned,
        TaskState::Accepted,
        TaskState::Preparing,
        TaskState::Starting,
        TaskState::Running,
        TaskState::Complete,
        TaskState::Failed,
        TaskState::Rejected,
        TaskState::Shutdown,
        TaskState::Orphaned,
    ];

    /// The task will not run again; its service needs a new task to replace it.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Complete
                | TaskState::Failed
                | TaskState::Rejected
                | TaskState::Shutdown
                | TaskState::Orphaned
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Assigned => "assigned",
            TaskState::Accepted => "accepted",
            TaskState::Preparing => "preparing",
            TaskState::Starting => "starting",
            TaskState::Running => "running",
            TaskState::Complete => "complete",
            TaskState::Failed => "failed",
            TaskState::Rejected => "rejected",
            TaskState::Shutdown => "shutdown",
            TaskState::Orphaned => "orphaned",
        }
    }
}

/// Written by the scheduler (until assigned) and then by the agent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    /// Why the task is in this state, for people.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// The pod's address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    /// Set by the readiness probe while running.
    #[serde(default)]
    pub ready: bool,
    /// Container restarts by the agent under the restart policy.
    #[serde(default)]
    pub restarts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}
