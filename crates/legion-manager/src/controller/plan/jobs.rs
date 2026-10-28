//! The Job and CronJob controllers, as part of [`super::plan`] (DESIGN.md
//! §3.3).
//!
//! A replicated Job has completion slots `1..=completions`, a global one a
//! slot per eligible node; each slot needs one task to complete, and at most
//! `parallelism` run at once. A failed task is retried in its slot with
//! backoff; the Job fails once more than `backoffLimit` tasks have failed,
//! or when it outlives `activeDeadline`, and its running tasks are stopped.
//! Tasks lost with their node, or moved off a draining one, are retried
//! without counting as failures. A finished Job keeps its tasks until it is
//! deleted.
//!
//! A CronJob makes a Job named `<cronjob>-<minutes since the epoch>` for the
//! latest run due since the one it dealt with last, so a controller that
//! was away owes one run, not every one it missed, and a repeated pass makes
//! the same Job, which the store refuses twice. It keeps the newest
//! finished Jobs its history limits allow and deletes the rest; deleting
//! the CronJob deletes its Jobs.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use legion_api::clock;
use legion_api::job::{
    ConcurrencyPolicy, CronJobSpec, CronJobStatus, JobMode, JobSpec, JobState, JobStatus,
    LABEL_CRON_JOB, LABEL_JOB,
};
use legion_api::node::Availability;
use legion_api::service::{ServiceMode, ServiceSpec, TaintEffect};
use legion_api::task::TaskState;
use legion_api::{Kind, Object};
use tokio::time::Instant;

use super::{Action, Homes, Memory, Node, Plan, Service, Slot, Task, eligible, stop};

/// The first retry of a failed task waits this long, doubling per failure
/// in a row in its slot, up to [`super::MAX_BACKOFF`].
const RETRY_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Job {
    pub object: Object,
    pub spec: JobSpec,
    pub status: JobStatus,
}

#[derive(Debug, Clone)]
pub struct CronJob {
    pub object: Object,
    pub spec: CronJobSpec,
    pub status: CronJobStatus,
}

/// Jobs and CronJobs, and the wall-clock time their deadlines and schedules
/// are measured in, in seconds since the epoch.
#[derive(Debug, Clone, Default)]
pub struct Jobs {
    pub jobs: Vec<Job>,
    pub cron_jobs: Vec<CronJob>,
    pub wall: i64,
}

impl Job {
    /// The Job as the scheduler sees owners: its pod and placement.
    pub(super) fn owner(&self) -> Service {
        Service {
            object: self.object.clone(),
            spec: ServiceSpec {
                mode: match self.spec.mode {
                    JobMode::Replicated => ServiceMode::Replicated,
                    JobMode::Global => ServiceMode::Global,
                },
                replicas: None,
                pod: self.spec.pod.clone(),
                placement: self.spec.placement.clone(),
                endpoint: None,
                update: Default::default(),
                max_unavailable: None,
            },
        }
    }
}

/// Plans one Job: its tasks (new ones into `created`, for the scheduler)
/// and its status.
#[allow(clippy::too_many_arguments)]
pub(super) fn job(
    j: &Job,
    owner: &Service,
    tasks: &[&Task],
    nodes: &[Node],
    homes: &Homes,
    memory: &Memory,
    now: Instant,
    wall: i64,
    plan: &mut Plan,
    created: &mut Vec<Task>,
) {
    let mut status = j.status.clone();
    let started = status
        .started
        .as_deref()
        .and_then(clock::parse)
        .unwrap_or(wall);
    status.started = Some(clock::format(started));

    let mut by_slot: BTreeMap<Slot, Vec<&Task>> = BTreeMap::new();
    for t in tasks {
        by_slot.entry(super::slot_of(t)).or_default().push(t);
    }
    for list in by_slot.values_mut() {
        list.sort_by(|a, b| a.uid().cmp(b.uid()));
    }
    let done = |slot: &Slot| {
        by_slot
            .get(slot)
            .is_some_and(|list| list.iter().any(|t| t.status.state == TaskState::Complete))
    };
    let wanted: Vec<Slot> = match j.spec.mode {
        JobMode::Replicated => (1..=j.spec.completions()).map(Slot::Replica).collect(),
        // Nodes that may run it now, and those it already completed on.
        JobMode::Global => {
            let mut slots: BTreeSet<Slot> = nodes
                .iter()
                .filter(|n| eligible(n, owner, homes).is_ok())
                .map(|n| Slot::Node(n.name().to_owned()))
                .collect();
            slots.extend(by_slot.keys().filter(|s| done(s)).cloned());
            slots.into_iter().collect()
        }
    };
    let failures: Vec<&&Task> = tasks.iter().filter(|t| t.failed()).collect();
    status.failed = failures.len() as u32;
    status.succeeded = wanted.iter().filter(|s| done(s)).count() as u32;

    if !status.state.is_finished() {
        let deadline = j
            .spec
            .active_deadline
            .map(|d| started + d.as_std().as_secs() as i64);
        let outcome = if status.failed > j.spec.backoff_limit {
            let last = failures
                .iter()
                .max_by(|a, b| a.uid().cmp(b.uid()))
                .map(|t| format!("; the last: {}", t.status.message))
                .unwrap_or_default();
            Some((
                JobState::Failed,
                format!(
                    "{} tasks failed, more than its backoffLimit of {}{last}",
                    status.failed, j.spec.backoff_limit
                ),
            ))
        } else if deadline.is_some_and(|d| wall >= d) {
            Some((
                JobState::Failed,
                format!(
                    "ran longer than its activeDeadline of {}",
                    j.spec.active_deadline.expect("set")
                ),
            ))
        } else if !wanted.is_empty() && wanted.iter().all(&done) {
            Some((JobState::Complete, String::new()))
        } else {
            None
        };
        match outcome {
            Some((state, message)) => {
                status.state = state;
                status.message = message;
                status.finished = Some(clock::format(wall));
            }
            None => {
                if let Some(d) = deadline {
                    plan.wake_at(now + Duration::from_secs((d - wall).max(0) as u64));
                }
                status.message = if wanted.is_empty() {
                    "no node can run it".into()
                } else {
                    String::new()
                };
            }
        }
    }

    if status.state.is_finished() {
        // Whatever still runs goes: a deadline or failure ends the Job.
        for t in tasks.iter().filter(|t| t.active()) {
            plan.actions.push(stop(t));
        }
    } else {
        start(
            j, owner, &wanted, &by_slot, nodes, memory, now, plan, created,
        );
    }

    status.active = tasks.iter().filter(|t| t.active()).count() as u32
        + created
            .iter()
            .filter(|t| t.spec.service_uid == j.object.metadata.uid)
            .count() as u32;
    let value = serde_json::to_value(&status).expect("status serialises");
    if j.object.status != value {
        let mut o = j.object.clone();
        o.status = value;
        plan.actions
            .push(Action::Update(o, j.object.metadata.version));
    }
}

/// Moves tasks off nodes they may no longer run on and starts tasks for
/// slots that need one, `parallelism` at a time.
#[allow(clippy::too_many_arguments)]
fn start(
    j: &Job,
    owner: &Service,
    wanted: &[Slot],
    by_slot: &BTreeMap<Slot, Vec<&Task>>,
    nodes: &[Node],
    memory: &Memory,
    now: Instant,
    plan: &mut Plan,
    created: &mut Vec<Task>,
) {
    let wanted_set: BTreeSet<&Slot> = wanted.iter().collect();
    let mut active = 0usize;
    for (slot, list) in by_slot {
        for t in list.iter().filter(|t| t.active()) {
            let leaving = nodes.iter().any(|n| {
                n.name() == t.spec.node
                    && (n.spec.availability == Availability::Drain
                        || n.repels(owner, TaintEffect::NoExecute))
            });
            if leaving || !wanted_set.contains(slot) {
                plan.actions.push(stop(t));
            }
            active += 1;
        }
    }
    let mut budget = match j.spec.mode {
        JobMode::Replicated => (j.spec.parallelism() as usize).saturating_sub(active),
        JobMode::Global => usize::MAX,
    };
    for slot in wanted {
        if budget == 0 {
            break;
        }
        let list: &[&Task] = by_slot.get(slot).map_or(&[], Vec::as_slice);
        let busy = list
            .iter()
            .any(|t| t.active() || t.status.state == TaskState::Complete);
        if busy {
            continue;
        }
        let failures = list.iter().rev().take_while(|t| t.failed()).count();
        let due = list.last().map_or(now, |last| {
            super::backoff(last, failures, RETRY_DELAY, memory, now)
        });
        if due > now {
            plan.wake_at(due);
            continue;
        }
        budget -= 1;
        let mut t = super::new_task(owner, slot, list, &BTreeMap::new());
        t.spec.job = true;
        let labels = &mut t.object.metadata.labels;
        labels.remove(super::LABEL_SERVICE);
        labels.insert(LABEL_JOB.to_owned(), j.object.metadata.name.clone());
        t.object.spec = serde_json::to_value(&t.spec).expect("spec serialises");
        created.push(t);
    }
}

/// Plans one CronJob: the Job of a run that is due, the history, and its
/// status.
pub(super) fn cron_job(c: &CronJob, jobs: &[Job], now: Instant, wall: i64, plan: &mut Plan) {
    let Some(schedule) = c.spec.parsed_schedule() else {
        return;
    };
    let name = &c.object.metadata.name;
    let namespace = &c.object.metadata.namespace;
    let mine: Vec<&Job> = jobs
        .iter()
        .filter(|j| {
            &j.object.metadata.namespace == namespace
                && j.object.metadata.labels.get(LABEL_CRON_JOB) == Some(name)
        })
        .collect();
    let mut status = c.status.clone();
    let since = status
        .last_schedule
        .as_deref()
        .and_then(clock::parse)
        .unwrap_or(wall);
    status.last_schedule = Some(clock::format(since));

    let mut created = None;
    if let Some(due) = schedule.latest_in(since, wall) {
        status.last_schedule = Some(clock::format(due));
        let when = clock::format(due);
        let running: Vec<&&Job> = mine
            .iter()
            .filter(|j| !j.status.state.is_finished())
            .collect();
        status.message = if c.spec.suspend {
            format!("suspended: skipped the run due at {when}")
        } else if c
            .spec
            .starting_deadline
            .is_some_and(|d| wall - due > d.as_std().as_secs() as i64)
        {
            format!(
                "skipped the run due at {when}: more than its startingDeadline of {} late",
                c.spec.starting_deadline.expect("set")
            )
        } else if c.spec.concurrency_policy == ConcurrencyPolicy::Forbid && !running.is_empty() {
            let names: Vec<&str> = running
                .iter()
                .map(|j| j.object.metadata.name.as_str())
                .collect();
            format!(
                "skipped the run due at {when}: {} still running",
                names.join(", ")
            )
        } else {
            if c.spec.concurrency_policy == ConcurrencyPolicy::Replace {
                // Deleting a Job stops its tasks.
                for j in &running {
                    plan.actions
                        .push(Action::Delete(j.object.key(), j.object.metadata.version));
                }
            }
            let job = new_job(c, due);
            status.last_job = job.metadata.name.clone();
            created = Some(job.metadata.name.clone());
            plan.actions.push(Action::Create(job));
            String::new()
        };
    }
    if let Some(next) = schedule.next_after(since.max(wall)) {
        plan.wake_at(now + Duration::from_secs((next - wall).max(0) as u64));
    }

    // History: the newest finished Jobs of each outcome.
    for (state, keep) in [
        (JobState::Complete, c.spec.successful_jobs_history_limit),
        (JobState::Failed, c.spec.failed_jobs_history_limit),
    ] {
        let mut finished: Vec<&&Job> = mine
            .iter()
            .filter(|j| {
                j.status.state == state && Some(&j.object.metadata.name) != created.as_ref()
            })
            .collect();
        finished.sort_by_key(|j| run_minute(&j.object.metadata.name));
        let excess = finished.len().saturating_sub(keep as usize);
        for j in &finished[..excess] {
            plan.actions
                .push(Action::Delete(j.object.key(), j.object.metadata.version));
        }
    }

    let value = serde_json::to_value(&status).expect("status serialises");
    if c.object.status != value {
        let mut o = c.object.clone();
        o.status = value;
        plan.actions
            .push(Action::Update(o, c.object.metadata.version));
    }
}

/// Jobs a CronJob made whose CronJob is gone.
pub(super) fn abandoned(jobs: &Jobs, plan: &mut Plan) {
    for j in &jobs.jobs {
        let Some(owner) = j.object.metadata.labels.get(LABEL_CRON_JOB) else {
            continue;
        };
        let exists = jobs.cron_jobs.iter().any(|c| {
            c.object.metadata.namespace == j.object.metadata.namespace
                && &c.object.metadata.name == owner
        });
        if !exists {
            plan.actions
                .push(Action::Delete(j.object.key(), j.object.metadata.version));
        }
    }
}

/// The Job of the run due at `due`.
fn new_job(c: &CronJob, due: i64) -> Object {
    let name = format!("{}-{}", c.object.metadata.name, due.div_euclid(60));
    let mut o = Object::new(
        Kind::Job,
        &c.object.metadata.namespace,
        &name,
        serde_json::to_value(&c.spec.job).expect("spec serialises"),
    );
    o.metadata.labels = c.object.metadata.labels.clone();
    o.metadata
        .labels
        .insert(LABEL_CRON_JOB.to_owned(), c.object.metadata.name.clone());
    o
}

/// The minute a CronJob's Job was due, from its name.
fn run_minute(name: &str) -> i64 {
    name.rsplit('-')
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}
