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
    /// What `node` reports of its tasks, by `namespace/task`.
    pub fn record(&self, node: &str, reported: impl IntoIterator<Item = (String, TaskUsage)>) {
        let now = Instant::now();
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|_, r| now.duration_since(r.at) < FRESH);
        for (task, usage) in reported {
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
