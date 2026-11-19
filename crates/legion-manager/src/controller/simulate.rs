//! The cluster in memory, for [`plan`] to run against: its writes applied
//! as the store would (versions, uids, generations, a write on an older
//! version refused), agents moving assigned tasks to running and stopping
//! ones to shutdown, and the migrator finishing each phase of a Volume's
//! move. The planner's tests drive it pass by pass; a dry run
//! ([`super::dryrun`]) lets it settle, over a copy of the cluster.

use legion_api::node::NodeState;
use legion_api::task::{DesiredState, TaskState, TaskStatus};
use legion_api::{Kind, Object};
use tokio::time::Instant;

use super::plan::{
    Action, Job, Jobs, Memory, Namespaces, Node, Service, Task, Volume, Volumes, plan,
};

pub struct Sim {
    pub services: Vec<Service>,
    pub jobs: Jobs,
    pub tasks: Vec<Task>,
    pub nodes: Vec<Node>,
    pub volumes: Volumes,
    pub namespaces: Namespaces,
    /// The store's last version.
    pub index: u64,
    pub memory: Memory,
    pub now: Instant,
    /// Agents run what they are given; off to watch tasks stay pending.
    pub agents: bool,
    /// Writes the store would not have taken as planned: a create of what
    /// exists, an update of what is gone, a spec that does not parse. The
    /// planner's tests take any for a bug.
    pub faults: Vec<String>,
}

/// A Task as read from its object.
pub fn task(o: Object) -> Option<Task> {
    Some(Task {
        spec: legion_api::parse(&o.spec).ok()?,
        status: serde_json::from_value(o.status.clone()).unwrap_or_default(),
        object: o,
    })
}

impl Sim {
    pub fn new(now: Instant) -> Self {
        Sim {
            services: vec![],
            jobs: Jobs::default(),
            tasks: vec![],
            nodes: vec![],
            volumes: Volumes::new(),
            namespaces: Namespaces::default(),
            index: 0,
            memory: Memory::default(),
            now,
            agents: true,
            faults: vec![],
        }
    }

    pub fn next(&mut self) -> u64 {
        self.index += 1;
        self.index
    }

    /// One plan, as the controller would make it now.
    pub fn plan(&mut self) -> super::plan::Plan {
        plan(
            &self.services,
            &self.jobs,
            &self.tasks,
            &self.nodes,
            &self.volumes,
            &self.namespaces,
            &mut self.memory,
            self.now,
        )
    }

    pub fn apply(&mut self, actions: Vec<Action>) {
        for a in actions {
            let index = self.next();
            match a {
                Action::Create(mut o) if o.kind == Kind::Job => {
                    if self.jobs.jobs.iter().any(|j| j.object.key() == o.key()) {
                        self.faults.push(format!("duplicate job {}", o.key()));
                        continue;
                    }
                    let Ok(spec) = legion_api::parse(&o.spec) else {
                        self.faults.push(format!("job {} does not parse", o.key()));
                        continue;
                    };
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    o.metadata.generation = 1;
                    self.jobs.jobs.push(Job {
                        spec,
                        status: Default::default(),
                        covered: false,
                        object: o,
                    });
                }
                Action::Create(mut o) if o.kind == Kind::Volume => {
                    let key = (o.metadata.namespace.clone(), o.metadata.name.clone());
                    if self.volumes.contains_key(&key) {
                        self.faults.push(format!("duplicate volume {key:?}"));
                        continue;
                    }
                    let Ok(spec) = legion_api::parse(&o.spec) else {
                        self.faults.push(format!("volume {key:?} does not parse"));
                        continue;
                    };
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    self.volumes.insert(
                        key,
                        Volume {
                            spec,
                            status: Default::default(),
                            object: o,
                        },
                    );
                }
                Action::Create(mut o) => {
                    if o.kind != Kind::Task || self.tasks.iter().any(|t| t.object.key() == o.key())
                    {
                        self.faults
                            .push(format!("unexpected create of {}", o.key()));
                        continue;
                    }
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    o.metadata.generation = 1;
                    match task(o) {
                        Some(t) => self.tasks.push(t),
                        None => self.faults.push("a task that does not parse".into()),
                    }
                }
                Action::Update(o, version) => self.update(o, version, index),
                Action::Delete(key, version) => {
                    self.tasks
                        .retain(|t| t.object.key() != key || t.object.metadata.version != version);
                    self.jobs
                        .jobs
                        .retain(|j| j.object.key() != key || j.object.metadata.version != version);
                }
            }
        }
    }

    fn update(&mut self, mut o: Object, version: u64, index: u64) {
        o.metadata.version = index;
        let key = o.key();
        let gone = || format!("update of {key}, which is gone");
        match o.kind {
            Kind::Task => {
                let Some(t) = self.tasks.iter_mut().find(|t| t.object.key() == key) else {
                    return;
                };
                if t.object.metadata.version != version {
                    return; // conflict: the next pass retries
                }
                match task(o) {
                    Some(new) => *t = new,
                    None => self.faults.push(format!("task {key} does not parse")),
                }
            }
            Kind::Service => {
                let Some(s) = self.services.iter_mut().find(|s| s.object.key() == key) else {
                    self.faults.push(gone());
                    return;
                };
                if s.object.metadata.version == version {
                    // As the store: a new spec is a new generation.
                    if o.spec != s.object.spec {
                        o.metadata.generation += 1;
                        match legion_api::parse(&o.spec) {
                            Ok(spec) => s.spec = spec,
                            Err(_) => self.faults.push(format!("service {key} does not parse")),
                        }
                    }
                    s.object = o;
                }
            }
            Kind::Job => {
                let Some(j) = self.jobs.jobs.iter_mut().find(|j| j.object.key() == key) else {
                    self.faults.push(gone());
                    return;
                };
                if j.object.metadata.version == version {
                    if o.spec != j.object.spec {
                        self.faults
                            .push(format!("job {key}: only the status changes"));
                    }
                    j.status = serde_json::from_value(o.status.clone()).unwrap_or_default();
                    j.object = o;
                }
            }
            Kind::Volume => {
                let k = (o.metadata.namespace.clone(), o.metadata.name.clone());
                let Some(v) = self.volumes.get_mut(&k) else {
                    self.faults.push(gone());
                    return;
                };
                if v.object.metadata.version == version {
                    match legion_api::parse(&o.spec) {
                        Ok(spec) => v.spec = spec,
                        Err(_) => self.faults.push(format!("volume {key} does not parse")),
                    }
                    v.status = serde_json::from_value(o.status.clone()).unwrap_or_default();
                    v.object = o;
                }
            }
            Kind::CronJob => {
                let Some(c) = self
                    .jobs
                    .cron_jobs
                    .iter_mut()
                    .find(|c| c.object.key() == key)
                else {
                    self.faults.push(gone());
                    return;
                };
                if c.object.metadata.version == version {
                    if o.spec != c.object.spec {
                        self.faults
                            .push(format!("cron job {key}: only the status changes"));
                    }
                    c.status = serde_json::from_value(o.status.clone()).unwrap_or_default();
                    c.object = o;
                }
            }
            Kind::Node => {
                let Some(n) = self.nodes.iter_mut().find(|n| n.object.key() == key) else {
                    self.faults.push(gone());
                    return;
                };
                if n.object.metadata.version == version {
                    if o.spec != n.object.spec {
                        self.faults
                            .push(format!("node {key}: only the status changes"));
                    }
                    n.status = serde_json::from_value(o.status.clone()).unwrap_or_default();
                    n.object = o;
                }
            }
            k => self.faults.push(format!("unexpected update of {k}")),
        }
    }

    /// Agents: assigned tasks start; stopping tasks stop. A node that is
    /// down or gone does neither, and pending tasks are not sent to
    /// agents. True if any task changed.
    pub fn run_agents(&mut self) -> bool {
        if !self.agents {
            return false;
        }
        let mut changed = false;
        for i in 0..self.tasks.len() {
            let t = &self.tasks[i];
            let up = self.nodes.iter().any(|n| {
                n.object.metadata.name == t.spec.node && n.status.state != NodeState::Down
            });
            if !up || t.status.state.is_terminal() || t.status.state == TaskState::Pending {
                continue;
            }
            let next = match t.spec.desired_state {
                DesiredState::Running if t.status.state != TaskState::Running => TaskState::Running,
                DesiredState::Shutdown => TaskState::Shutdown,
                _ => continue,
            };
            // A task's first start claims its volumes for its node.
            if next == TaskState::Running {
                let (ns, node) = (t.object.metadata.namespace.clone(), t.spec.node.clone());
                for name in t.spec.volumes() {
                    if let Some(v) = self.volumes.get_mut(&(ns.clone(), name))
                        && v.status.node.is_empty()
                    {
                        v.status.node = node.clone();
                        v.object.status = serde_json::to_value(&v.status).expect("serialises");
                    }
                }
            }
            let version = self.next();
            let t = &mut self.tasks[i];
            t.status = TaskStatus {
                state: next,
                ready: next == TaskState::Running,
                ..Default::default()
            };
            t.object.status = serde_json::to_value(&t.status).expect("serialises");
            t.object.metadata.version = version;
            changed = true;
        }
        changed
    }

    /// The leader's migrator, done with each migration's current phase.
    /// True if any moved on.
    pub fn migrate(&mut self) -> bool {
        use legion_api::volume::MigrationPhase;
        let mut changed = false;
        for v in self.volumes.values_mut() {
            let Some(m) = &mut v.status.migration else {
                continue;
            };
            match m.phase {
                MigrationPhase::Copying => m.phase = MigrationPhase::Copied,
                MigrationPhase::Finishing => {
                    v.status.node = m.to.clone();
                    v.status.dataset.clear();
                    v.status.migration = None;
                }
                MigrationPhase::Copied => continue,
            }
            v.object.status = serde_json::to_value(&v.status).expect("serialises");
            self.index += 1;
            v.object.metadata.version = self.index;
            changed = true;
        }
        changed
    }
}
