//! The agent's metrics, for Prometheus: what each running container uses,
//! read from rctl on its own jail (libpod's stats are zeros on FreeBSD;
//! DESIGN.md §6.4). Needs `kern.racct.enable=1`, as limits do.

use std::collections::BTreeMap;
use std::sync::Arc;

use legion_api::dispatch::TaskUsage;
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
        "Resident memory of the container's processes, each page counted once.",
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
    /// An app container, not an init container.
    app: bool,
    namespace: String,
    task: String,
    container: String,
    jail: String,
}

/// What rctl gave of a container, by resource.
type Reading = Result<BTreeMap<String, u64>, String>;

/// Running containers with what rctl gave of each, and the pods.
struct Read {
    pods: usize,
    containers: Vec<(Running, Reading)>,
}

impl<R: Runtime> AgentMetrics<R> {
    pub async fn collect(&self) -> Metrics {
        let mut m = Metrics::default();
        let read = match self.read().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("listing containers for metrics: {e}");
                m.gauge(
                    "legion_agent_runtime_up",
                    "1 if the agent reached its container runtime, else 0.",
                )
                .sample(&[], 0.0);
                return m;
            }
        };
        m.gauge(
            "legion_agent_runtime_up",
            "1 if the agent reached its container runtime, else 0.",
        )
        .sample(&[], 1.0);
        m.gauge("legion_agent_pods", "Pods running on the node.")
            .sample(&[], read.pods as f64);
        m.gauge(
            "legion_agent_containers",
            "Containers of pods running on the node, the infra containers aside.",
        )
        .sample(&[], read.containers.len() as f64);
        let mut unread = 0;
        let mut usage = Vec::new();
        for (r, u) in &read.containers {
            match u {
                Ok(u) => usage.push((r, u)),
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
            for (r, u) in &usage {
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

    /// What each running task's app containers use together, by
    /// `namespace/task`, for the autoscaler. A task with a container rctl
    /// gave nothing for is left out.
    pub async fn tasks(&self) -> Vec<(String, TaskUsage)> {
        let Ok(read) = self.read().await else {
            return Vec::new();
        };
        let mut tasks: BTreeMap<String, Option<TaskUsage>> = BTreeMap::new();
        for (r, u) in &read.containers {
            if !r.app {
                continue;
            }
            let entry = tasks
                .entry(format!("{}/{}", r.namespace, r.task))
                .or_insert(Some(TaskUsage::default()));
            match (entry.as_mut(), u) {
                (Some(t), Ok(u)) => {
                    t.cpu += u.get("pcpu").copied().unwrap_or(0) as f64;
                    t.memory += u.get("memoryuse").copied().unwrap_or(0);
                }
                _ => *entry = None,
            }
        }
        tasks
            .into_iter()
            .filter_map(|(task, u)| Some((task, u?)))
            .collect()
    }

    async fn read(&self) -> Result<Read, String> {
        let found = self
            .runtime
            .list(&[(LABEL_NODE, &self.node)])
            .await
            .map_err(|e| e.to_string())?;
        let role = |c: &crate::runtime::ContainerSummary| {
            c.labels.get(LABEL_ROLE).and_then(|r| Role::parse(r))
        };
        let infra: BTreeMap<&str, &str> = found
            .iter()
            .filter(|c| role(c) == Some(Role::Infra) && c.state == "running")
            .filter_map(|c| Some((c.labels.get(LABEL_TASK)?.as_str(), c.id.as_str())))
            .collect();
        let running: Vec<Running> = found
            .iter()
            .filter(|c| matches!(role(c), Some(Role::Init | Role::App)) && c.state == "running")
            .filter_map(|c| {
                let infra = infra.get(c.labels.get(LABEL_TASK)?.as_str())?;
                let (namespace, task) = c.labels.get(LABEL_TASK_NAME)?.split_once('/')?;
                Some(Running {
                    app: role(c) == Some(Role::App),
                    namespace: namespace.to_owned(),
                    task: task.to_owned(),
                    container: c.labels.get(LABEL_CONTAINER)?.clone(),
                    jail: pod::child_jail(infra, &c.id),
                })
            })
            .collect();
        let jails: Vec<String> = running.iter().map(|r| r.jail.clone()).collect();
        let usage = crate::task::blocking(&self.sys, move |sys| {
            // rctl's memoryuse counts shared pages once per process: the
            // container's memory is counted instead, when it can be.
            let memory = sys.jail_memory(&jails).unwrap_or_else(|e| {
                tracing::debug!("counting containers' memory: {e}");
                BTreeMap::new()
            });
            jails
                .iter()
                .map(|j| {
                    let mut u = sys.rctl_usage(j).map_err(|e| e.to_string())?;
                    if let Some(m) = memory.get(j) {
                        u.insert("memoryuse".into(), m.bytes);
                    }
                    Ok(u)
                })
                .collect::<Vec<_>>()
        })
        .await;
        Ok(Read {
            pods: infra.len(),
            containers: running.into_iter().zip(usage).collect(),
        })
    }
}
