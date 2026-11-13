//! What tasks use, as agents report it with their heartbeats. Only the
//! leader's table is read, by the autoscaler and `Cluster.Usage`, and its
//! history, a sample every [`STEP`] for the last [`HISTORY`], by
//! `Cluster.UsageHistory`; none of it is stored, and a new leader starts
//! empty.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use legion_api::dispatch::TaskUsage;
use tokio::time::Instant;

/// Reports older than this are left out: the task stopped, or its node
/// stopped reporting.
pub const FRESH: Duration = Duration::from_secs(60);

/// The most tasks one heartbeat reports on that are believed.
const MAX_REPORTED: usize = 4096;

/// How far back the history goes.
pub const HISTORY: Duration = Duration::from_secs(3600);
/// How often the history takes a sample of each task.
pub const STEP: Duration = Duration::from_secs(15);

#[derive(Default)]
pub struct Usage {
    /// By `namespace/task`: what the node reported, and when.
    tasks: Mutex<HashMap<String, Report>>,
    /// By `namespace/task`: its node and samples, oldest first.
    history: Mutex<HashMap<String, Series>>,
}

/// What a task used at a moment, in milliseconds since the epoch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    pub at_ms: i64,
    pub cpu: f32,
    pub memory: u64,
}

struct Series {
    node: String,
    samples: VecDeque<(Instant, Sample)>,
}

#[derive(Clone)]
struct Report {
    node: String,
    usage: TaskUsage,
    at: Instant,
}

impl Usage {
    /// What `node` reports of its tasks, by `namespace/task`: only those
    /// `runs_on` says are on `node`, and only usage that is a number of
    /// both kinds, so no node speaks for another's tasks or feeds the
    /// autoscaler nonsense.
    pub fn record(
        &self,
        node: &str,
        reported: impl IntoIterator<Item = (String, TaskUsage)>,
        runs_on: impl Fn(&str) -> bool,
    ) {
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.record_at(node, reported, runs_on, Instant::now(), at_ms);
    }

    fn record_at(
        &self,
        node: &str,
        reported: impl IntoIterator<Item = (String, TaskUsage)>,
        runs_on: impl Fn(&str) -> bool,
        now: Instant,
        at_ms: i64,
    ) {
        let mut tasks = self.tasks.lock().unwrap();
        let mut history = self.history.lock().unwrap();
        tasks.retain(|_, r| now.duration_since(r.at) < FRESH);
        history.retain(|_, s| {
            while s
                .samples
                .front()
                .is_some_and(|(t, _)| now.duration_since(*t) > HISTORY)
            {
                s.samples.pop_front();
            }
            !s.samples.is_empty()
        });
        for (task, usage) in reported.into_iter().take(MAX_REPORTED) {
            if !usage.cpu.is_finite() || usage.cpu < 0.0 || !runs_on(&task) {
                continue;
            }
            let series = history.entry(task.clone()).or_insert_with(|| Series {
                node: node.to_owned(),
                samples: VecDeque::new(),
            });
            if series
                .samples
                .back()
                .is_none_or(|(t, _)| now.duration_since(*t) >= STEP)
            {
                series.node = node.to_owned();
                series.samples.push_back((
                    now,
                    Sample {
                        at_ms,
                        cpu: usage.cpu as f32,
                        memory: usage.memory,
                    },
                ));
            }
            let report = Report {
                node: node.to_owned(),
                usage,
                at: now,
            };
            tasks.insert(task, report);
        }
    }

    /// Each task's samples within [`HISTORY`], oldest first, by
    /// `namespace/task`, with the node that took the last.
    pub fn history(&self) -> BTreeMap<String, (String, Vec<Sample>)> {
        self.history_at(Instant::now())
    }

    fn history_at(&self, now: Instant) -> BTreeMap<String, (String, Vec<Sample>)> {
        self.history
            .lock()
            .unwrap()
            .iter()
            .map(|(task, s)| {
                let samples = s
                    .samples
                    .iter()
                    .filter(|(t, _)| now.duration_since(*t) <= HISTORY)
                    .map(|(_, sample)| *sample)
                    .collect::<Vec<_>>();
                (task.clone(), (s.node.clone(), samples))
            })
            .filter(|(_, (_, samples))| !samples.is_empty())
            .collect()
    }

    /// The fresh reports, by `namespace/task`, with the node that made
    /// each: only a task's own node is believed.
    pub fn fresh(&self) -> BTreeMap<String, (String, TaskUsage)> {
        let now = Instant::now();
        self.tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| now.duration_since(r.at) < FRESH)
            .map(|(task, r)| (task.clone(), (r.node.clone(), r.usage)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tasks_node_is_not_spoken_for_by_another() {
        let u = Usage::default();
        let used = |cpu| TaskUsage { cpu, memory: 1 };
        // a/web-1 runs on w1.
        let on = |node: &'static str| move |task: &str| task != "a/web-1" || node == "w1";
        u.record("w2", [("a/web-1".to_owned(), used(99.0))], on("w2"));
        u.record("w1", [("a/web-1".to_owned(), used(10.0))], on("w1"));
        u.record("w2", [("a/web-1".to_owned(), used(99.0))], on("w2"));
        let fresh = u.fresh();
        assert_eq!(fresh["a/web-1"].0, "w1");
        assert_eq!(fresh["a/web-1"].1.cpu, 10.0);
        // Usage that is no number of percent is not believed.
        for cpu in [f64::NAN, f64::INFINITY, -1.0] {
            u.record("w1", [("a/web-1".to_owned(), used(cpu))], on("w1"));
        }
        assert_eq!(u.fresh()["a/web-1"].1.cpu, 10.0);
        // And a heartbeat reports on so many tasks at most.
        u.record(
            "w3",
            (0..2 * MAX_REPORTED).map(|n| (format!("b/t-{n}"), used(1.0))),
            on("w3"),
        );
        assert_eq!(u.fresh().len(), 1 + MAX_REPORTED);
    }

    #[test]
    fn the_history_keeps_a_sample_a_step_for_an_hour() {
        let u = Usage::default();
        let used = |cpu| TaskUsage { cpu, memory: 7 };
        let any = |_: &str| true;
        let start = Instant::now();
        let mut now = start;
        // Heartbeats every 2 s: one sample each step.
        for i in 0..(STEP.as_secs() as u32 * 3 / 2) {
            u.record_at(
                "w1",
                [("a/web-1".to_owned(), used(f64::from(i)))],
                any,
                now,
                0,
            );
            now += Duration::from_secs(2);
        }
        let h = u.history_at(now);
        let (node, samples) = &h["a/web-1"];
        assert_eq!(node, "w1");
        assert_eq!(samples.len(), 3, "{samples:?}");
        assert_eq!(samples[0].cpu, 0.0);
        assert_eq!(samples[1].cpu, 8.0);
        assert_eq!(samples[0].memory, 7);
        // An hour later, a task no longer reported has none left.
        now += HISTORY + STEP;
        u.record_at("w1", [("a/web-2".to_owned(), used(1.0))], any, now, 0);
        assert!(!u.history_at(now).contains_key("a/web-1"));
        assert_eq!(u.history_at(now)["a/web-2"].1.len(), 1);
    }
}
