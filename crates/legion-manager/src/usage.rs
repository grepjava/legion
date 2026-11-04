//! What tasks use, as agents report it with their heartbeats. Only the
//! leader's table is read, by the autoscaler; it is not stored, and a new
//! leader starts empty.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use legion_api::dispatch::TaskUsage;
use tokio::time::Instant;

/// Reports older than this are left out: the task stopped, or its node
/// stopped reporting.
pub const FRESH: Duration = Duration::from_secs(60);

/// The most tasks one heartbeat reports on that are believed.
const MAX_REPORTED: usize = 4096;

#[derive(Default)]
pub struct Usage {
    /// By `namespace/task`: what the node reported, and when.
    tasks: Mutex<HashMap<String, Report>>,
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
        let now = Instant::now();
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|_, r| now.duration_since(r.at) < FRESH);
        for (task, usage) in reported.into_iter().take(MAX_REPORTED) {
            if !usage.cpu.is_finite() || usage.cpu < 0.0 || !runs_on(&task) {
                continue;
            }
            let report = Report {
                node: node.to_owned(),
                usage,
                at: now,
            };
            tasks.insert(task, report);
        }
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
}
