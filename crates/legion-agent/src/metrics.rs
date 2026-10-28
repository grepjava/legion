//! The agent's metrics, for Prometheus: what each running container uses,
//! read from rctl on its own jail (libpod's stats are zeros on FreeBSD;
//! DESIGN.md §6.4). Needs `kern.racct.enable=1`, as limits do.

use std::collections::BTreeMap;
use std::sync::Arc;

use legion_api::metrics::Metrics;
use legion_sys::Sys;

use crate::pod::{
    self, LABEL_CONTAINER, LABEL_NODE, LABEL_ROLE, LABEL_TASK, LABEL_TASK_NAME, Role,
};
use crate::runtime::Runtime;

/// rctl resources reported, and as what.
const USAGE: [(&str, &str, &str, bool); 12] = [
    (
        "cputime",
        "legion_container_cpu_seconds_total",
        "CPU time the container's processes have used.",
        true,
    ),
    (
        "pcpu",
        "legion_container_cpu_percent",
        "The container's CPU use, in percent of one CPU.",
        false,
    ),
    (
        "memoryuse",
        "legion_container_memory_bytes",
        "Resident memory of the container's processes.",
        false,
    ),
    (
        "vmemoryuse",
        "legion_container_virtual_memory_bytes",
        "Address space of the container's processes.",
        false,
    ),
    (
        "swapuse",
        "legion_container_swap_bytes",
        "Swap reserved by the container's processes.",
        false,
    ),
    (
        "maxproc",
        "legion_container_processes",
        "Processes in the container.",
        false,
    ),
    (
        "nthr",
        "legion_container_threads",
        "Threads in the container.",
        false,
    ),
    (
        "openfiles",
        "legion_container_open_files",
        "Files the container's processes hold open.",
        false,
    ),
    (
        "readbps",
        "legion_container_read_bytes_per_second",
        "Bytes the container reads from storage per second.",
        false,
    ),
    (
        "writebps",
        "legion_container_write_bytes_per_second",
        "Bytes the container writes to storage per second.",
        false,
    ),
    (
        "readiops",
        "legion_container_read_operations_per_second",
        "Storage reads per second.",
        false,
    ),
    (
        "writeiops",
        "legion_container_write_operations_per_second",
        "Storage writes per second.",
        false,
    ),
];

/// Reads an agent's metrics; cheap to clone.
pub struct AgentMetrics<R> {
    pub(crate) runtime: Arc<R>,
    pub(crate) sys: Arc<dyn Sys>,
    pub(crate) node: String,
}

impl<R> Clone for AgentMetrics<R> {
    fn clone(&self) -> Self {
        AgentMetrics {
            runtime: self.runtime.clone(),
            sys: self.sys.clone(),
            node: self.node.clone(),
        }
    }
}

/// A running container of a pod.
struct Running {
    namespace: String,
    task: String,
    container: String,
    jail: String,
}

impl<R: Runtime> AgentMetrics<R> {
    pub async fn collect(&self) -> Metrics {
        let mut m = Metrics::default();
        let found = match self.runtime.list(&[(LABEL_NODE, &self.node)]).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("listing containers for metrics: {e}");
                m.gauge(
                    "legion_agent_runtime_up",
                    "1 if the agent reached its Podman, else 0.",
                )
                .sample(&[], 0.0);
                return m;
            }
        };
        m.gauge(
            "legion_agent_runtime_up",
            "1 if the agent reached its Podman, else 0.",
        )
        .sample(&[], 1.0);
        let role = |c: &legion_podman::ContainerSummary| {
            c.labels.get(LABEL_ROLE).and_then(|r| Role::parse(r))
        };
        let infra: BTreeMap<&str, &str> = found
            .iter()
            .filter(|c| role(c) == Some(Role::Infra) && c.state == "running")
            .filter_map(|c| Some((c.labels.get(LABEL_TASK)?.as_str(), c.id.as_str())))
            .collect();
        m.gauge("legion_agent_pods", "Pods running on the node.")
            .sample(&[], infra.len() as f64);
        let running: Vec<Running> = found
            .iter()
            .filter(|c| matches!(role(c), Some(Role::Init | Role::App)) && c.state == "running")
            .filter_map(|c| {
                let infra = infra.get(c.labels.get(LABEL_TASK)?.as_str())?;
                let (namespace, task) = c.labels.get(LABEL_TASK_NAME)?.split_once('/')?;
                Some(Running {
                    namespace: namespace.to_owned(),
                    task: task.to_owned(),
                    container: c.labels.get(LABEL_CONTAINER)?.clone(),
                    jail: pod::child_jail(infra, &c.id),
                })
            })
            .collect();
        m.gauge(
            "legion_agent_containers",
            "Containers of pods running on the node, the infra containers aside.",
        )
        .sample(&[], running.len() as f64);

        let jails: Vec<String> = running.iter().map(|r| r.jail.clone()).collect();
        let usage = crate::task::blocking(&self.sys, move |sys| {
            jails
                .iter()
                .map(|j| sys.rctl_usage(j).map_err(|e| e.to_string()))
                .collect::<Vec<_>>()
        })
        .await;
        let mut unread = 0;
        let mut read = Vec::new();
        for (r, u) in running.iter().zip(usage) {
            match u {
                Ok(u) => read.push((r, u)),
                Err(e) => {
                    tracing::debug!(task = %r.task, container = %r.container, "reading usage: {e}");
                    unread += 1;
                }
            }
        }
        m.gauge(
            "legion_agent_usage_unread",
            "Running containers whose usage rctl did not give (is kern.racct.enable set?).",
        )
        .sample(&[], f64::from(unread));
        for (key, name, help, counter) in USAGE {
            let mut f = if counter {
                m.counter(name, help)
            } else {
                m.gauge(name, help)
            };
            for (r, u) in &read {
                if let Some(v) = u.get(key) {
                    f.sample(
                        &[
                            ("namespace", &r.namespace),
                            ("task", &r.task),
                            ("container", &r.container),
                        ],
                        *v as f64,
                    );
                }
            }
        }
        m
    }
}
