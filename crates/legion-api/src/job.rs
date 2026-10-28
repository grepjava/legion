//! `Job` and `CronJob` (DESIGN.md §3.3): pods that run to completion, and
//! Jobs made on a schedule.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, field};
use crate::quantity::Duration;
use crate::service::{Placement, PodSpec, RestartCondition};

/// Label on a Job's tasks naming the Job.
pub const LABEL_JOB: &str = "legion.job";
/// Label on a CronJob's Jobs naming the CronJob.
pub const LABEL_CRON_JOB: &str = "legion.cronjob";
/// A CronJob's Jobs are named `<cronjob>-<minutes since the epoch>`; this
/// leaves room for the suffix within a name's 63 characters.
pub const CRON_JOB_NAME_MAX: usize = 52;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobSpec {
    #[serde(default)]
    pub mode: JobMode,
    /// Replicated jobs: tasks that must complete; defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completions: Option<u32>,
    /// Replicated jobs: at most this many tasks at once; defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    /// Failed tasks allowed before the Job fails.
    #[serde(default = "default_backoff_limit")]
    pub backoff_limit: u32,
    /// The Job fails if it runs longer than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_deadline: Option<Duration>,
    /// Its restart condition defaults to `none`: a failed task is retried
    /// as a new task, under `backoffLimit`.
    pub pod: PodSpec,
    #[serde(default, skip_serializing_if = "Placement::is_empty")]
    pub placement: Placement,
}

fn default_backoff_limit() -> u32 {
    6
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobMode {
    /// `completions` tasks, `parallelism` at a time.
    #[default]
    Replicated,
    /// One task on every eligible node.
    Global,
}

impl Spec for JobSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        self.admit_at("")
    }
}

impl JobSpec {
    pub(crate) fn admit_at(&mut self, path: &str) -> Result<(), Invalid> {
        match self.mode {
            JobMode::Replicated => {
                for (name, v) in [
                    ("completions", &mut self.completions),
                    ("parallelism", &mut self.parallelism),
                ] {
                    if *v.get_or_insert(1) == 0 {
                        return Err(Invalid::new(field(path, name), "must be at least 1"));
                    }
                }
            }
            JobMode::Global => {
                for (name, v) in [
                    ("completions", self.completions),
                    ("parallelism", self.parallelism),
                ] {
                    if v.is_some() {
                        return Err(Invalid::new(
                            field(path, name),
                            "a global job runs one task per node and takes no count",
                        ));
                    }
                }
            }
        }
        if self.active_deadline.is_some_and(|d| d.0 == 0) {
            return Err(Invalid::new(
                field(path, "activeDeadline"),
                "must be longer than 0s",
            ));
        }
        let pod = field(path, "pod");
        self.pod.admit(&pod)?;
        if self.pod.restart_policy.condition == RestartCondition::Any {
            return Err(Invalid::new(
                field(&field(&pod, "restartPolicy"), "condition"),
                "a Job's pod runs to completion: the condition must be on-failure or none",
            ));
        }
        self.placement.admit(&field(path, "placement"))
    }

    pub fn completions(&self) -> u32 {
        self.completions.unwrap_or(1)
    }

    pub fn parallelism(&self) -> u32 {
        self.parallelism.unwrap_or(1)
    }
}

/// Written by the Job controller.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub state: JobState,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// When the controller first saw the Job, RFC 3339.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started: Option<String>,
    /// When it completed or failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished: Option<String>,
    /// Tasks running or about to.
    #[serde(default)]
    pub active: u32,
    /// Tasks that completed, one per completion slot at most.
    #[serde(default)]
    pub succeeded: u32,
    #[serde(default)]
    pub failed: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    #[default]
    Running,
    Complete,
    Failed,
}

impl JobState {
    pub fn is_finished(self) -> bool {
        self != JobState::Running
    }

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Running => "running",
            JobState::Complete => "complete",
            JobState::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronJobSpec {
    /// Five-field cron schedule, in UTC (see [`crate::cron`]).
    pub schedule: String,
    #[serde(default)]
    pub concurrency_policy: ConcurrencyPolicy,
    /// A run that could not start within this of its time is skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_deadline: Option<Duration>,
    /// No new runs while set; running Jobs go on.
    #[serde(default)]
    pub suspend: bool,
    /// Finished Jobs kept, by outcome.
    #[serde(default = "default_successful_history")]
    pub successful_jobs_history_limit: u32,
    #[serde(default = "default_failed_history")]
    pub failed_jobs_history_limit: u32,
    /// The Job each run makes.
    pub job: JobSpec,
}

fn default_successful_history() -> u32 {
    3
}

fn default_failed_history() -> u32 {
    1
}

/// What a run does while an earlier run's Job is still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConcurrencyPolicy {
    /// Runs alongside it.
    #[default]
    Allow,
    /// Is skipped.
    Forbid,
    /// Stops it and takes its place.
    Replace,
}

impl Spec for CronJobSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        self.schedule
            .parse::<crate::cron::Schedule>()
            .map_err(|e| Invalid::new("schedule", e.to_string()))?;
        self.job.admit_at("job")
    }
}

impl CronJobSpec {
    pub fn parsed_schedule(&self) -> Option<crate::cron::Schedule> {
        self.schedule.parse().ok()
    }
}

/// Written by the CronJob controller.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJobStatus {
    /// The latest run time dealt with (run or skipped), RFC 3339. Set when
    /// the controller first sees the CronJob: earlier runs are not owed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_schedule: Option<String>,
    /// The Job of the latest run.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_job: String,
    /// Why the latest run did not happen, if it did not.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}
