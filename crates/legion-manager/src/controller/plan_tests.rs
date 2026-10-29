//! [`plan`] against a simulated cluster: actions are applied as the store
//! would (versions, uids, generations), and agents move assigned tasks to
//! running and stopping tasks to shutdown.

use std::time::Duration;

use legion_api::node::{Availability, Capacity, NodeSpec, NodeState, NodeStatus, Taint};
use legion_api::quantity::{Bytes, Cpu};
use legion_api::service::{ServiceSpec, ServiceStatus, TaintEffect};
use legion_api::task::{DesiredState, TaskState, TaskStatus};
use legion_api::{Kind, Object, Spec};
use tokio::time::Instant;

use super::plan::{
    Action, CronJob, HISTORY, Job, Jobs, Memory, Node, Quotas, Service, Task, Volume, Volumes, plan,
};

struct World {
    services: Vec<Service>,
    jobs: Jobs,
    tasks: Vec<Task>,
    nodes: Vec<Node>,
    volumes: Volumes,
    quotas: Quotas,
    index: u64,
    memory: Memory,
    now: Instant,
    /// Agents run what they are given; off to watch tasks stay pending.
    agents: bool,
}

impl World {
    fn new() -> Self {
        World {
            services: vec![],
            jobs: Jobs {
                wall: legion_api::clock::parse("2026-10-28T09:31:00Z").unwrap(),
                ..Default::default()
            },
            tasks: vec![],
            nodes: vec![],
            volumes: Volumes::new(),
            quotas: Quotas::new(),
            index: 0,
            memory: Memory::default(),
            now: Instant::now(),
            agents: true,
        }
    }

    fn next(&mut self) -> u64 {
        self.index += 1;
        self.index
    }

    fn node(&mut self, name: &str, labels: &[(&str, &str)]) {
        let mut o = Object::new(Kind::Node, "", name, serde_json::json!({}));
        o.metadata.labels = labels
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect();
        o.metadata.version = self.next();
        self.nodes.push(Node {
            object: o,
            spec: NodeSpec::default(),
            status: NodeStatus {
                state: NodeState::Ready,
                allocatable: Capacity {
                    cpu: Cpu(4000),
                    memory: Bytes(8 << 30),
                },
                ..Default::default()
            },
        });
    }

    fn node_mut(&mut self, name: &str) -> &mut Node {
        self.nodes
            .iter_mut()
            .find(|n| n.object.metadata.name == name)
            .unwrap()
    }

    fn availability(&mut self, name: &str, a: Availability) {
        self.node_mut(name).spec.availability = a;
    }

    fn state(&mut self, name: &str, state: NodeState) {
        self.node_mut(name).status.state = state;
    }

    fn taint(&mut self, name: &str, key: &str, effect: TaintEffect) {
        self.node_mut(name).spec.taints.push(Taint {
            key: key.into(),
            value: String::new(),
            effect,
        });
    }

    /// Nodes of the running tasks of `service`, sorted.
    fn placed(&self, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| t.spec.service == service && t.status.state == TaskState::Running)
            .map(|t| t.spec.node.clone())
            .collect();
        v.sort();
        v
    }

    /// Creates or updates a service from a YAML spec, as an apply would.
    fn service(&mut self, name: &str, spec: &str) {
        let value: serde_json::Value = serde_yaml_ng::from_str(spec).unwrap();
        let mut spec: ServiceSpec = legion_api::parse(&value).unwrap();
        spec.admit().unwrap();
        let index = self.next();
        match self
            .services
            .iter_mut()
            .find(|s| s.object.metadata.name == name)
        {
            Some(s) => {
                if s.spec != spec {
                    s.object.metadata.generation += 1;
                }
                s.object.spec = serde_json::to_value(&spec).unwrap();
                s.object.metadata.version = index;
                s.spec = spec;
            }
            None => {
                let mut o = Object::new(
                    Kind::Service,
                    "shop",
                    name,
                    serde_json::to_value(&spec).unwrap(),
                );
                o.metadata.uid = format!("{index:016x}");
                o.metadata.version = index;
                o.metadata.generation = 1;
                self.services.push(Service { object: o, spec });
            }
        }
    }

    /// A Volume whose data is on `node`.
    fn volume(&mut self, name: &str, node: &str) {
        let mut o = Object::new(Kind::Volume, "shop", name, serde_json::json!({}));
        o.metadata.version = self.next();
        let status = legion_api::volume::VolumeStatus {
            node: node.into(),
            dataset: format!("tank/legion/volumes/shop/{name}"),
            migration: None,
        };
        o.status = serde_json::to_value(&status).unwrap();
        self.volumes.insert(
            ("shop".into(), name.into()),
            Volume {
                object: o,
                spec: Default::default(),
                status,
            },
        );
    }

    /// A Volume of `spec`, held by `node` if not empty.
    fn volume_of(&mut self, name: &str, spec: serde_json::Value, node: &str) {
        self.volume(name, node);
        let v = self
            .volumes
            .get_mut(&("shop".to_owned(), name.to_owned()))
            .unwrap();
        v.spec = legion_api::parse(&spec).unwrap();
        v.object.spec = spec;
        v.status.dataset.clear();
        v.object.status = serde_json::to_value(&v.status).unwrap();
    }

    /// The leader's migrator, done with each migration's current phase.
    fn migrate(&mut self) {
        use legion_api::volume::MigrationPhase;
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
            v.object.status = serde_json::to_value(&v.status).unwrap();
            self.index += 1;
            v.object.metadata.version = self.index;
        }
    }

    /// The migration of a Volume: its node and phase.
    fn migration(&self, name: &str) -> Option<(String, legion_api::volume::MigrationPhase)> {
        let v = &self.volumes[&("shop".to_owned(), name.to_owned())];
        v.status.migration.as_ref().map(|m| (m.to.clone(), m.phase))
    }

    /// One pass, and the agents acting on it.
    fn step(&mut self) {
        let p = plan(
            &self.services,
            &self.jobs,
            &self.tasks,
            &self.nodes,
            &self.volumes,
            &self.quotas,
            &mut self.memory,
            self.now,
        );
        self.apply(p.actions);
        self.agents();
    }

    /// Names of the tasks of `service` that are told to run, sorted.
    fn wanted(&self, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| {
                t.spec.service == service
                    && t.spec.desired_state == DesiredState::Running
                    && !t.status.state.is_terminal()
            })
            .map(|t| t.object.metadata.name.clone())
            .collect();
        v.sort();
        v
    }

    fn placed_task(&self, name: &str) -> &Task {
        self.tasks
            .iter()
            .find(|t| t.object.metadata.name == name)
            .unwrap_or_else(|| panic!("no task {name}: {:?}", self.tasks()))
    }

    fn delete_service(&mut self, name: &str) {
        self.services.retain(|s| s.object.metadata.name != name);
    }

    fn apply(&mut self, actions: Vec<Action>) {
        for a in actions {
            let index = self.next();
            match a {
                Action::Create(mut o) if o.kind == Kind::Job => {
                    assert!(
                        !self.jobs.jobs.iter().any(|j| j.object.key() == o.key()),
                        "duplicate job {}",
                        o.key()
                    );
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    o.metadata.generation = 1;
                    self.jobs.jobs.push(Job {
                        spec: legion_api::parse(&o.spec).unwrap(),
                        status: Default::default(),
                        object: o,
                    });
                }
                Action::Create(mut o) if o.kind == Kind::Volume => {
                    let key = (o.metadata.namespace.clone(), o.metadata.name.clone());
                    assert!(!self.volumes.contains_key(&key), "duplicate volume {key:?}");
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    self.volumes.insert(
                        key,
                        Volume {
                            spec: legion_api::parse(&o.spec).unwrap(),
                            status: Default::default(),
                            object: o,
                        },
                    );
                }
                Action::Create(mut o) => {
                    assert_eq!(o.kind, Kind::Task);
                    assert!(
                        !self.tasks.iter().any(|t| t.object.key() == o.key()),
                        "duplicate task {}",
                        o.key()
                    );
                    o.metadata.uid = format!("{index:016x}");
                    o.metadata.version = index;
                    o.metadata.generation = 1;
                    self.tasks.push(task(o));
                }
                Action::Update(mut o, version) => {
                    o.metadata.version = index;
                    match o.kind {
                        Kind::Task => {
                            let Some(t) = self.tasks.iter_mut().find(|t| t.object.key() == o.key())
                            else {
                                continue;
                            };
                            if t.object.metadata.version != version {
                                continue; // conflict: the next pass retries
                            }
                            *t = task(o);
                        }
                        Kind::Service => {
                            let s = self
                                .services
                                .iter_mut()
                                .find(|s| s.object.key() == o.key())
                                .unwrap();
                            if s.object.metadata.version == version {
                                // As the store: a new spec is a new generation.
                                if o.spec != s.object.spec {
                                    o.metadata.generation += 1;
                                    s.spec = legion_api::parse(&o.spec).unwrap();
                                }
                                s.object = o;
                            }
                        }
                        Kind::Job => {
                            let j = self
                                .jobs
                                .jobs
                                .iter_mut()
                                .find(|j| j.object.key() == o.key())
                                .unwrap();
                            if j.object.metadata.version == version {
                                assert_eq!(o.spec, j.object.spec, "only the status changes");
                                j.status = serde_json::from_value(o.status.clone()).unwrap();
                                j.object = o;
                            }
                        }
                        Kind::Volume => {
                            let key = (o.metadata.namespace.clone(), o.metadata.name.clone());
                            let v = self.volumes.get_mut(&key).unwrap();
                            if v.object.metadata.version == version {
                                v.spec = legion_api::parse(&o.spec).unwrap();
                                v.status =
                                    serde_json::from_value(o.status.clone()).unwrap_or_default();
                                v.object = o;
                            }
                        }
                        Kind::CronJob => {
                            let c = self
                                .jobs
                                .cron_jobs
                                .iter_mut()
                                .find(|c| c.object.key() == o.key())
                                .unwrap();
                            if c.object.metadata.version == version {
                                assert_eq!(o.spec, c.object.spec, "only the status changes");
                                c.status = serde_json::from_value(o.status.clone()).unwrap();
                                c.object = o;
                            }
                        }
                        k => panic!("unexpected update of {k}"),
                    }
                }
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

    /// Agents: assigned tasks start; stopping tasks stop. A node that is
    /// down does neither, and pending tasks are not sent to agents.
    fn agents(&mut self) -> bool {
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
                        v.object.status = serde_json::to_value(&v.status).unwrap();
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
            t.object.status = serde_json::to_value(&t.status).unwrap();
            t.object.metadata.version = version;
            changed = true;
        }
        changed
    }

    /// Plans and applies until nothing changes; returns the passes taken.
    fn settle(&mut self) -> usize {
        for pass in 1..100 {
            let p = plan(
                &self.services,
                &self.jobs,
                &self.tasks,
                &self.nodes,
                &self.volumes,
                &self.quotas,
                &mut self.memory,
                self.now,
            );
            let acted = !p.actions.is_empty();
            self.apply(p.actions);
            if !self.agents() && !acted {
                return pass;
            }
        }
        panic!("did not settle");
    }

    fn fail(&mut self, name: &str) {
        let version = self.next();
        let t = self
            .tasks
            .iter_mut()
            .find(|t| t.object.metadata.name == name)
            .unwrap();
        t.status = TaskStatus {
            state: TaskState::Failed,
            message: "container exited with 1".into(),
            ..Default::default()
        };
        t.object.status = serde_json::to_value(&t.status).unwrap();
        t.object.metadata.version = version;
    }

    fn wake(&mut self) -> Option<Instant> {
        plan(
            &self.services,
            &self.jobs,
            &self.tasks,
            &self.nodes,
            &self.volumes,
            &self.quotas,
            &mut self.memory,
            self.now,
        )
        .wake
    }

    /// `name state node` of every task, sorted.
    fn tasks(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .map(|t| {
                format!(
                    "{} {} {}",
                    t.object.metadata.name,
                    t.status.state.as_str(),
                    t.spec.node
                )
            })
            .collect();
        v.sort();
        v
    }

    fn running(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| t.status.state == TaskState::Running)
            .map(|t| t.object.metadata.name.clone())
            .collect();
        v.sort();
        v
    }

    fn status(&self, name: &str) -> ServiceStatus {
        let s = self
            .services
            .iter()
            .find(|s| s.object.metadata.name == name)
            .unwrap();
        serde_json::from_value(s.object.status.clone()).unwrap()
    }
}

impl World {
    /// The service's update state and message.
    fn update(&self, name: &str) -> (String, String) {
        let u = self.status(name).update.expect("an update");
        (u.state.as_str().to_owned(), u.message)
    }

    /// Images of the running tasks of `service`, sorted.
    fn images(&self, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| t.spec.service == service && t.status.state == TaskState::Running)
            .map(|t| t.spec.pod.containers[0].image.clone())
            .collect();
        v.sort();
        v
    }

    /// The running task of `service` with `image`.
    fn running_with(&self, service: &str, image: &str) -> String {
        self.tasks
            .iter()
            .find(|t| {
                t.spec.service == service
                    && t.status.state == TaskState::Running
                    && t.spec.pod.containers[0].image == image
            })
            .unwrap()
            .object
            .metadata
            .name
            .clone()
    }
}

fn web(image: &str, update: &str) -> String {
    format!(
        "replicas: 2\npod: {{ containers: [{{ name: web, image: \"{image}\" }}], restartPolicy: {{ delay: 0s }} }}\nupdate: {{ {update} }}"
    )
}

#[test]
fn updates_complete_after_the_monitor_window() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &web("web:1", ""));
    w.settle();
    assert!(w.status("web").update.is_none(), "creating is no update");

    w.service("web", &web("web:2", ""));
    w.settle();
    assert_eq!(w.images("web"), ["web:2", "web:2"]);
    assert_eq!(
        w.update("web").0,
        "updating",
        "still watching the new tasks"
    );
    let u = w.status("web").update.unwrap();
    assert_eq!(u.previous.pod.containers[0].image, "web:1");
    assert_eq!(w.wake(), Some(w.now + Duration::from_secs(5)));

    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.update("web").0, "completed");

    // Other changes are no update.
    w.service("web", &web("web:2", "parallelism: 2"));
    w.settle();
    assert_eq!(w.update("web").0, "completed");
}

#[test]
fn delay_spaces_the_slots() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &web("web:1", "delay: 10s"));
    w.settle();
    w.service("web", &web("web:2", "delay: 10s"));
    w.settle();
    assert_eq!(w.images("web"), ["web:1", "web:2"]);
    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.images("web"), ["web:1", "web:2"]);
    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.images("web"), ["web:2", "web:2"]);
}

#[test]
fn a_failing_update_pauses() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &web("web:1", "delay: 10s"));
    w.settle();
    w.service("web", &web("web:2", "delay: 10s"));
    w.settle();
    let new = w.running_with("web", "web:2");
    w.fail(&new);
    w.settle();
    assert_eq!(
        w.update("web"),
        (
            "paused".to_owned(),
            format!("update paused: task {new} failed: container exited with 1")
        )
    );
    // The old task is left alone; the failed slot is retried as usual.
    w.now += Duration::from_secs(60);
    w.settle();
    assert_eq!(w.images("web"), ["web:1", "web:2"]);

    // A new spec starts a new update.
    w.service("web", &web("web:3", "delay: 10s"));
    w.settle();
    assert_eq!(w.update("web").0, "updating");
}

#[test]
fn a_failing_update_rolls_back() {
    let mut w = World::new();
    w.node("n1", &[]);
    let update = "delay: 10s, failureAction: rollback";
    w.service("web", &web("web:1", update));
    w.settle();
    w.service("web", &web("web:2", update));
    w.settle();
    // web-1-2 runs web:2; web-2-1 still runs web:1, for the delay.
    w.now += Duration::from_secs(10);
    w.settle();
    // web-1-2 failing now, past the monitor window, does not count.
    w.fail("web-1-2");
    w.settle();
    assert_eq!(w.update("web").0, "updating");
    w.now += Duration::from_secs(1);
    w.settle();
    assert_eq!(w.running(), ["web-1-3", "web-2-2"]);

    // web-2-2 failing within it does: the spec goes back and the tasks
    // follow.
    w.fail("web-2-2");
    w.settle();
    let s = w
        .services
        .iter()
        .find(|s| s.object.metadata.name == "web")
        .unwrap();
    assert_eq!(s.spec.pod.containers[0].image, "web:1");
    assert_eq!(
        w.update("web"),
        (
            "rolling-back".to_owned(),
            "rolled back: task web-2-2 failed: container exited with 1".to_owned()
        )
    );
    // The spec rolled back to spaces its slots too.
    for _ in 0..12 {
        w.now += Duration::from_secs(1);
        w.settle();
    }
    assert_eq!(w.images("web"), ["web:1", "web:1"]);
    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.update("web").0, "rolled-back");
    assert_eq!(
        w.status("web").update.unwrap().previous.pod.containers[0].image,
        "web:2"
    );
}

#[test]
fn a_rollback_replaces_a_failed_task_whatever_the_restart_policy() {
    let mut w = World::new();
    w.node("n1", &[]);
    let spec = |image: &str| {
        format!(
            "pod: {{ containers: [{{ name: web, image: \"{image}\" }}], restartPolicy: {{ condition: none }} }}
update: {{ failureAction: rollback }}"
        )
    };
    w.service("web", &spec("web:1"));
    w.settle();
    w.service("web", &spec("web:2"));
    w.settle();
    w.fail("web-1-2");
    for _ in 0..15 {
        w.now += Duration::from_secs(1);
        w.settle();
    }
    assert_eq!(w.images("web"), ["web:1"]);
    assert_eq!(w.update("web").0, "rolled-back");
    // A task of the pod as it is now that fails stays failed.
    w.fail("web-1-3");
    w.now += Duration::from_secs(60);
    w.settle();
    assert!(w.running().is_empty());
}

#[test]
fn rollback_by_hand() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &web("web:1", ""));
    w.settle();
    w.service("web", &web("web:2", ""));
    w.settle();
    w.now += Duration::from_secs(5);
    w.settle();
    // As `legion rollback` does: apply the spec the update came from.
    let previous = w.status("web").update.unwrap().previous;
    w.service("web", &serde_json::to_string(&previous).unwrap());
    w.settle();
    assert_eq!(w.update("web").0, "rolling-back");
    assert_eq!(w.images("web"), ["web:1", "web:1"]);
    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.update("web").0, "rolled-back");
}

fn task(o: Object) -> Task {
    Task {
        spec: legion_api::parse(&o.spec).unwrap(),
        status: serde_json::from_value(o.status.clone()).unwrap_or_default(),
        object: o,
    }
}

const POD: &str = "pod: { containers: [{ name: web, image: \"web:1\" }] }";

#[test]
fn replicated_scale_up_and_down() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &format!("replicas: 3\n{POD}"));
    w.settle();
    assert_eq!(
        w.tasks(),
        [
            "web-1-1 running n1",
            "web-2-1 running n1",
            "web-3-1 running n1"
        ]
    );
    let st = w.status("web");
    assert_eq!(
        (st.desired, st.running, st.ready, st.observed_generation),
        (3, 3, 3, 1)
    );

    w.service("web", &format!("replicas: 1\n{POD}"));
    w.settle();
    assert_eq!(
        w.tasks(),
        ["web-1-1 running n1"],
        "removed slots leave no history"
    );
    assert_eq!(w.status("web").desired, 1);
}

#[test]
fn failed_tasks_are_replaced_after_a_backoff() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &format!("replicas: 1\n{POD}"));
    w.settle();

    w.fail("web-1-1");
    w.settle();
    assert!(w.running().is_empty(), "not replaced at once");
    let wake = w.wake().expect("a wake-up is planned");
    assert_eq!(wake - w.now, Duration::from_secs(5), "the restart delay");

    w.now = wake;
    w.settle();
    assert_eq!(w.tasks(), ["web-1-1 failed n1", "web-1-2 running n1"]);

    // A second failure in a row doubles the wait.
    w.fail("web-1-2");
    w.settle();
    assert_eq!(w.wake().unwrap() - w.now, Duration::from_secs(10));
}

#[test]
fn deleted_tasks_leave_their_names_unused() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service(
        "web",
        &format!(
            "replicas: 1
{POD}"
        ),
    );
    w.settle();
    // An operator deletes the slot's only task.
    w.tasks.retain(|t| t.object.metadata.name != "web-1-1");
    w.settle();
    assert_eq!(w.tasks(), ["web-1-2 running n1"]);
    assert_eq!(w.status("web").last_tasks["1"], 2);
}

#[test]
fn history_is_bounded() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service(
        "web",
        "replicas: 1\npod: { containers: [{ name: web, image: \"web:1\" }], restartPolicy: { delay: 0s } }",
    );
    w.settle();
    for n in 1..=8 {
        w.fail(&format!("web-1-{n}"));
        w.settle();
        w.now += Duration::from_secs(120);
        w.settle();
    }
    let failed = w.tasks().iter().filter(|t| t.contains("failed")).count();
    assert_eq!(failed, HISTORY);
    assert_eq!(w.running(), ["web-1-9"]);
}

#[test]
fn no_replacement_when_the_policy_says_none() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("job", "replicas: 1\npod: { containers: [{ name: j, image: \"j:1\" }], restartPolicy: { condition: none } }");
    w.settle();
    w.fail("job-1-1");
    w.now += Duration::from_secs(600);
    w.settle();
    assert_eq!(w.tasks(), ["job-1-1 failed n1"]);
    assert!(w.wake().is_none());
}

#[test]
fn rolling_update_stop_first() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &format!("replicas: 3\n{POD}"));
    w.settle();
    w.agents = false;
    w.service(
        "web",
        "replicas: 3\npod: { containers: [{ name: web, image: \"web:2\" }] }",
    );
    // One slot at a time: the first pass stops one old task only.
    let p = plan(
        &w.services,
        &w.jobs,
        &w.tasks,
        &w.nodes,
        &w.volumes,
        &w.quotas,
        &mut w.memory,
        w.now,
    );
    let stops = p
        .actions
        .iter()
        .filter(|a| matches!(a, Action::Update(o, _) if o.kind == Kind::Task))
        .count();
    assert_eq!(stops, 1);
    w.agents = true;
    w.settle();
    assert_eq!(w.running(), ["web-1-2", "web-2-2", "web-3-2"]);
    assert!(
        w.tasks
            .iter()
            .filter(|t| t.status.state == TaskState::Running)
            .all(|t| {
                t.spec.service_generation == 2 && t.spec.pod.containers[0].image == "web:2"
            })
    );
    assert_eq!(w.status("web").observed_generation, 2);
}

#[test]
fn rolling_update_start_first() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service(
        "web",
        &format!("replicas: 1\n{POD}\nupdate: {{ order: start-first }}"),
    );
    w.settle();
    w.agents = false;
    w.service("web", "replicas: 1\npod: { containers: [{ name: web, image: \"web:2\" }] }\nupdate: { order: start-first }");
    let p = plan(
        &w.services,
        &w.jobs,
        &w.tasks,
        &w.nodes,
        &w.volumes,
        &w.quotas,
        &mut w.memory,
        w.now,
    );
    w.apply(p.actions);
    // The new task exists while the old one still runs.
    assert_eq!(w.tasks(), ["web-1-1 running n1", "web-1-2 assigned n1"]);
    w.agents = true;
    w.settle();
    assert_eq!(w.tasks(), ["web-1-1 shutdown n1", "web-1-2 running n1"]);
}

#[test]
fn global_follows_nodes() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[("disk", "ssd")]);
    w.service(
        "agent",
        "mode: global\npod: { containers: [{ name: a, image: \"a:1\" }] }",
    );
    w.settle();
    assert_eq!(
        w.tasks(),
        ["agent-n1-1 running n1", "agent-n2-1 running n2"]
    );

    w.node("n3", &[]);
    w.settle();
    assert_eq!(w.running().len(), 3);

    // Paused keeps its task; drained loses it.
    w.availability("n1", Availability::Pause);
    w.availability("n3", Availability::Drain);
    w.settle();
    assert_eq!(w.running(), ["agent-n1-1", "agent-n2-1"]);
    assert_eq!(w.status("agent").desired, 1, "only n2 is eligible");

    // Constraints narrow the nodes.
    w.service(
        "ssd",
        "mode: global\npod: { containers: [{ name: a, image: \"a:1\" }] }\nplacement: { constraints: [\"node.labels.disk == ssd\"] }",
    );
    w.settle();
    assert!(w.tasks().contains(&"ssd-n2-1 running n2".to_owned()));
    assert!(!w.tasks().iter().any(|t| t.starts_with("ssd-n1")));

    // So does the Linux ABI.
    w.node_mut("n2").status.platform.linux = true;
    w.service(
        "linux",
        "mode: global
pod: { containers: [{ name: a, image: \"a:1\" }] }
placement: { constraints: [\"node.platform.linux == true\"] }",
    );
    w.settle();
    assert!(w.tasks().contains(&"linux-n2-1 running n2".to_owned()));
    assert!(
        !w.tasks()
            .iter()
            .any(|t| t.starts_with("linux-n1") || t.starts_with("linux-n3"))
    );
    w.service(
        "native",
        "mode: global
pod: { containers: [{ name: a, image: \"a:1\" }] }
placement: { constraints: [\"node.platform.linux == false\"] }",
    );
    w.settle();
    assert!(
        !w.tasks().iter().any(|t| t.starts_with("native-")),
        "n2 runs Linux"
    );
}

#[test]
fn draining_moves_replicated_tasks() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("web", &format!("replicas: 2\n{POD}"));
    w.settle();
    // Spread: one per node.
    let nodes: Vec<&str> = w.tasks.iter().map(|t| t.spec.node.as_str()).collect();
    assert!(nodes.contains(&"n1") && nodes.contains(&"n2"), "{nodes:?}");

    w.availability("n1", Availability::Drain);
    w.settle();
    let running: Vec<&Task> = w
        .tasks
        .iter()
        .filter(|t| t.status.state == TaskState::Running)
        .collect();
    assert_eq!(running.len(), 2);
    assert!(running.iter().all(|t| t.spec.node == "n2"));
}

#[test]
fn tasks_of_a_lost_node_move() {
    let mut w = World::new();
    for n in ["n1", "n2", "n3"] {
        w.node(n, &[]);
    }
    w.service("web", &format!("replicas: 3\n{POD}"));
    w.service(
        "agent",
        "mode: global\npod: { containers: [{ name: a, image: \"a:1\" }] }",
    );
    w.settle();
    assert_eq!(w.placed("web"), ["n1", "n2", "n3"]);
    let lost = w
        .tasks
        .iter()
        .find(|t| t.spec.service == "web" && t.spec.node == "n2")
        .unwrap()
        .object
        .metadata
        .name
        .clone();

    w.state("n2", NodeState::Down);
    w.settle();
    // The lost task is orphaned and told to stop, so the node removes its
    // pod if it comes back; its slot runs elsewhere at once.
    let old = w
        .tasks
        .iter()
        .find(|t| t.object.metadata.name == lost)
        .unwrap();
    assert_eq!(old.status.state, TaskState::Orphaned);
    assert_eq!(old.status.message, "node n2 is down");
    assert_eq!(old.spec.desired_state, DesiredState::Shutdown);
    assert_eq!(w.placed("web").len(), 3);
    assert!(!w.placed("web").contains(&"n2".to_owned()));
    assert_eq!(w.status("web").running, 3);
    // The global task goes with its node.
    assert_eq!(w.placed("agent"), ["n1", "n3"]);
    assert!(!w.tasks().iter().any(|t| t.starts_with("agent-n2")));

    // Back: the global task returns under a new name; web stays put.
    w.state("n2", NodeState::Ready);
    w.settle();
    assert_eq!(w.placed("agent"), ["n1", "n2", "n3"]);
    assert!(w.tasks().contains(&"agent-n2-2 running n2".to_owned()));
    assert!(!w.placed("web").contains(&"n2".to_owned()));
}

#[test]
fn lost_tasks_are_replaced_whatever_the_restart_policy() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("job", "replicas: 1\npod: { containers: [{ name: j, image: \"j:1\" }], restartPolicy: { condition: none } }");
    w.settle();
    let first = w.placed("job")[0].clone();
    w.state(&first, NodeState::Down);
    w.settle();
    assert_eq!(w.placed("job").len(), 1);
    assert_ne!(w.placed("job")[0], first);
    assert!(w.wake().is_none(), "no backoff");
}

#[test]
fn taints_keep_tasks_off() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.taint("n2", "dedicated", TaintEffect::NoSchedule);
    w.service("web", &format!("replicas: 2\n{POD}"));
    w.service(
        "db",
        &format!("replicas: 2\n{POD}\nplacement: {{ tolerations: [{{ key: dedicated }}] }}"),
    );
    w.settle();
    assert_eq!(w.placed("web"), ["n1", "n1"]);
    assert_eq!(w.placed("db"), ["n1", "n2"]);

    // PreferNoSchedule: a last choice, yet used when nothing else fits.
    w.node("n3", &[]);
    w.taint("n3", "spare", TaintEffect::PreferNoSchedule);
    w.service("cache", &format!("replicas: 1\n{POD}"));
    w.settle();
    assert_eq!(w.placed("cache"), ["n1"]);
    w.availability("n1", Availability::Drain);
    w.settle();
    assert_eq!(w.placed("cache"), ["n3"]);
    assert_eq!(w.placed("web"), ["n3", "n3"]);

    // NoExecute moves running tasks that do not tolerate it.
    w.availability("n1", Availability::Active);
    w.taint("n3", "broken", TaintEffect::NoExecute);
    w.settle();
    assert_eq!(w.placed("web"), ["n1", "n1"]);
    assert_eq!(w.placed("cache"), ["n1"]);
    let db = w.tasks.iter().find(|t| t.spec.service == "db").unwrap();
    assert_eq!(db.status.message, "");

    // An untolerated taint of any effect but PreferNoSchedule is reported.
    w.service(
        "only",
        &format!("replicas: 1\n{POD}\nplacement: {{ constraints: [\"node.name == n3\"] }}"),
    );
    w.settle();
    let only = w.tasks.iter().find(|t| t.spec.service == "only").unwrap();
    assert_eq!(
        only.status.message,
        "no node can run it: 1 constraints, 2 taints"
    );
}

#[test]
fn tasks_go_where_their_volume_is() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.volume("pgdata", "n2");
    w.service(
        "db",
        "replicas: 1\npod: { containers: [{ name: db, image: \"pg:17\" }], volumes: [{ name: data, volume: { name: pgdata } }] }",
    );
    w.settle();
    assert_eq!(w.placed("db"), ["n2"]);

    // Its node lost, it waits for it.
    w.state("n2", NodeState::Down);
    w.settle();
    assert!(w.placed("db").is_empty());
    let pending = w
        .tasks
        .iter()
        .find(|t| t.status.state == TaskState::Pending)
        .unwrap();
    assert_eq!(
        pending.status.message,
        "no node can run it: 1 not ready, 1 volume on another node"
    );
    w.state("n2", NodeState::Ready);
    w.settle();
    assert_eq!(w.placed("db"), ["n2"]);
}

#[test]
fn network_volumes_place_by_their_access() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    // NFS: anywhere, held by no node.
    w.volume_of(
        "files",
        serde_json::json!({ "driver": "nfs", "nfs": { "server": "files.lan", "path": "/x" } }),
        "",
    );
    w.service(
        "web",
        "replicas: 2
pod: { containers: [{ name: web, image: \"web:1\" }], volumes: [{ name: f, volume: { name: files } }] }",
    );
    // iSCSI: where it is held, while it is.
    w.volume_of(
        "block",
        serde_json::json!({ "driver": "iscsi", "iscsi": { "portal": "a", "target": "iqn.x" } }),
        "n2",
    );
    w.service(
        "db",
        "replicas: 1
pod: { containers: [{ name: db, image: \"pg:17\" }], volumes: [{ name: data, volume: { name: block } }] }",
    );
    w.settle();
    let mut web = w.placed("web");
    web.sort();
    assert_eq!(web, ["n1", "n2"]);
    assert_eq!(w.placed("db"), ["n2"]);
    // A drain neither waits for a copy nor moves it: the task stops, and
    // its next one waits for n2 to let go.
    w.availability("n2", Availability::Drain);
    w.settle();
    assert!(w.placed("db").is_empty());
    assert_eq!(w.volume_node("block"), "n2");
    assert!(
        w.volumes[&("shop".to_owned(), "block".to_owned())]
            .status
            .migration
            .is_none()
    );
    // Once it has, the task goes to n1.
    w.volume_of(
        "block",
        serde_json::json!({ "driver": "iscsi", "iscsi": { "portal": "a", "target": "iqn.x" } }),
        "",
    );
    w.settle();
    assert_eq!(w.placed("db"), ["n1"]);
    // A removed node lets go of what it held.
    w.volume_of(
        "held",
        serde_json::json!({ "driver": "iscsi", "iscsi": { "portal": "a", "target": "iqn.y" } }),
        "n3",
    );
    w.settle();
    assert_eq!(w.volume_node("held"), "");
}

#[test]
fn placement_reports_why_a_task_cannot_run() {
    let mut w = World::new();
    w.node("n1", &[("disk", "hdd")]);
    w.service(
        "db",
        "replicas: 1\npod: { containers: [{ name: db, image: \"pg:17\" }] }\nplacement: { constraints: [\"node.labels.disk == ssd\"] }",
    );
    w.settle();
    let t = &w.tasks[0];
    assert_eq!(t.status.state, TaskState::Pending);
    assert_eq!(t.status.message, "no node can run it: 1 constraints");

    // Requests must fit what the node can give.
    w.service(
        "big",
        "replicas: 1\npod: { containers: [{ name: b, image: \"b:1\", resources: { requests: { memory: 16Gi } } }] }",
    );
    w.settle();
    let big = w.tasks.iter().find(|t| t.spec.service == "big").unwrap();
    assert_eq!(
        big.status.message,
        "no node can run it: 1 insufficient resources"
    );

    // A suitable node appears: the pending task is placed on it.
    w.node("n2", &[("disk", "ssd")]);
    w.settle();
    let db = w.tasks.iter().find(|t| t.spec.service == "db").unwrap();
    assert_eq!(
        (db.status.state, db.spec.node.as_str()),
        (TaskState::Running, "n2")
    );
}

#[test]
fn deleting_a_service_removes_its_tasks() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", &format!("replicas: 2\n{POD}"));
    w.settle();
    w.delete_service("web");
    w.settle();
    assert!(w.tasks.is_empty());
}

#[test]
fn unscheduled_tasks_stop_without_an_agent() {
    let mut w = World::new();
    w.service("web", &format!("replicas: 2\n{POD}"));
    w.settle();
    assert!(w.tasks.iter().all(|t| t.status.state == TaskState::Pending));
    w.delete_service("web");
    w.settle();
    assert!(w.tasks.is_empty(), "{:?}", w.tasks());
}

fn status(w: &World, name: &str) -> ServiceStatus {
    let s = w
        .services
        .iter()
        .find(|s| s.object.metadata.name == name)
        .unwrap();
    serde_json::from_value(s.object.status.clone()).unwrap()
}

fn vip(w: &World, name: &str) -> Option<String> {
    status(w, name).virtual_ip
}

fn published(w: &World, name: &str) -> Vec<(u16, u16)> {
    status(w, name)
        .published_ports
        .iter()
        .map(|p| (p.port, p.published_port))
        .collect()
}

const PORTS: &str = r#"
pod:
  containers: [{ name: app, image: "app:1", ports: [{ containerPort: 8080 }] }]
endpoint:
  ports: [{ port: 80, targetPort: 8080 }, { port: 81, targetPort: 8080, publish: ingress }]
"#;

#[test]
fn services_get_vips_and_published_ports() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("a", PORTS);
    w.service("b", PORTS);
    // No ports, or dnsrr: no VIP.
    w.service(
        "c",
        r#"pod: { containers: [{ name: app, image: "app:1" }] }"#,
    );
    w.service(
        "d",
        r#"
pod: { containers: [{ name: app, image: "app:1", ports: [{ containerPort: 80 }] }] }
endpoint: { mode: dnsrr, ports: [{ port: 80, publish: host, publishedPort: 8080 }] }
"#,
    );
    w.settle();
    assert_eq!(vip(&w, "a").as_deref(), Some("10.201.0.1"));
    assert_eq!(vip(&w, "b").as_deref(), Some("10.201.0.2"));
    assert_eq!(vip(&w, "c"), None);
    assert_eq!(vip(&w, "d"), None);
    assert_eq!(published(&w, "a"), [(81, 30000)]);
    assert_eq!(published(&w, "b"), [(81, 30001)]);
    assert_eq!(published(&w, "d"), [(80, 8080)]);

    // Addresses stay put; freed ones are reused.
    w.delete_service("a");
    w.service("e", PORTS);
    w.settle();
    assert_eq!(vip(&w, "b").as_deref(), Some("10.201.0.2"));
    assert_eq!(vip(&w, "e").as_deref(), Some("10.201.0.1"));
    assert_eq!(published(&w, "b"), [(81, 30001)]);
    assert_eq!(published(&w, "e"), [(81, 30000)]);

    // Dropping the ports releases the VIP.
    w.service(
        "b",
        r#"pod: { containers: [{ name: app, image: "app:1", ports: [{ containerPort: 8080 }] }] }"#,
    );
    w.settle();
    assert_eq!(vip(&w, "b"), None);
    assert!(published(&w, "b").is_empty());
}

#[test]
fn a_published_port_belongs_to_one_service() {
    let fixed = r#"
pod:
  containers: [{ name: app, image: "app:1", ports: [{ containerPort: 8080 }] }]
endpoint:
  ports: [{ port: 80, targetPort: 8080, publish: ingress, publishedPort: 30080 }]
"#;
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("old", fixed);
    w.settle();
    w.service("new", fixed);
    w.settle();
    // The older one keeps it; the newer goes without, and says why.
    assert_eq!(published(&w, "old"), [(80, 30080)]);
    assert!(published(&w, "new").is_empty());
    assert!(status(&w, "old").conflicts.is_empty());
    assert_eq!(
        status(&w, "new").conflicts,
        ["published port 30080/tcp is taken by shop/old"]
    );
    // Released, it moves.
    w.delete_service("old");
    w.settle();
    assert_eq!(published(&w, "new"), [(80, 30080)]);
    assert!(status(&w, "new").conflicts.is_empty());
}

#[test]
fn a_namespace_quota_holds_tasks() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    let quota = |y: &str| serde_yaml_ng::from_str(y).unwrap();
    w.quotas
        .insert("shop".into(), quota("{ cpu: 1, tasks: 3 }"));
    w.service(
        "web",
        "replicas: 3
pod: { containers: [{ name: web, image: \"web:1\", resources: { requests: { cpu: 400m } } }] }",
    );
    w.service(
        "agent",
        "mode: global
pod: { containers: [{ name: a, image: \"a:1\" }] }",
    );
    w.settle();
    // Two of web fit the CPU, one of agent the task count; the rest wait,
    // agent's on the node it names, unstarted.
    assert_eq!(
        w.tasks(),
        [
            "agent-n1-1 running n1",
            "agent-n2-1 pending n2",
            "web-1-1 running n1",
            "web-2-1 running n2",
            "web-3-1 pending ",
        ]
    );
    let message = |w: &World, name: &str| {
        let t = w.tasks.iter().find(|t| t.object.metadata.name == name);
        t.unwrap().status.message.clone()
    };
    assert_eq!(
        message(&w, "web-3-1"),
        "over the quota of namespace shop: cpu 1200m of 1, tasks 4 of 3"
    );
    assert_eq!(
        message(&w, "agent-n2-1"),
        "over the quota of namespace shop: tasks 4 of 3"
    );

    // A larger quota lets them run.
    w.quotas
        .insert("shop".into(), quota("{ cpu: 2, tasks: 5 }"));
    w.settle();
    assert_eq!(w.running().len(), 5);

    // A smaller one stops nothing, but a replacement waits for room.
    w.quotas.insert("shop".into(), quota("{ tasks: 4 }"));
    w.settle();
    assert_eq!(w.running().len(), 5);
    w.fail("web-1-1");
    w.settle();
    w.now += Duration::from_secs(60);
    w.settle();
    assert_eq!(w.running().len(), 4);
    assert_eq!(
        message(&w, "web-1-2"),
        "over the quota of namespace shop: tasks 5 of 4"
    );
}

impl World {
    /// Creates or replaces a Job, as an apply would.
    fn job(&mut self, name: &str, spec: &str) {
        let value: serde_json::Value = serde_yaml_ng::from_str(spec).unwrap();
        let mut o = Object::new(Kind::Job, "shop", name, value);
        legion_api::admit(&mut o).unwrap();
        let index = self.next();
        o.metadata.uid = format!("{index:016x}");
        o.metadata.version = index;
        o.metadata.generation = 1;
        self.jobs.jobs.retain(|j| j.object.metadata.name != name);
        self.jobs.jobs.push(Job {
            spec: legion_api::parse(&o.spec).unwrap(),
            status: Default::default(),
            object: o,
        });
    }

    /// Creates or updates a CronJob, as an apply would: an update keeps
    /// the status.
    fn cron_job(&mut self, name: &str, spec: &str) {
        let value: serde_json::Value = serde_yaml_ng::from_str(spec).unwrap();
        let mut o = Object::new(Kind::CronJob, "shop", name, value);
        legion_api::admit(&mut o).unwrap();
        let index = self.next();
        o.metadata.version = index;
        let spec = legion_api::parse(&o.spec).unwrap();
        match self
            .jobs
            .cron_jobs
            .iter_mut()
            .find(|c| c.object.metadata.name == name)
        {
            Some(c) => {
                o.metadata.uid = c.object.metadata.uid.clone();
                o.metadata.generation = c.object.metadata.generation + 1;
                o.status = c.object.status.clone();
                c.object = o;
                c.spec = spec;
            }
            None => {
                o.metadata.uid = format!("{index:016x}");
                o.metadata.generation = 1;
                self.jobs.cron_jobs.push(CronJob {
                    object: o,
                    spec,
                    status: Default::default(),
                });
            }
        }
    }

    fn advance(&mut self, secs: u64) {
        self.now += Duration::from_secs(secs);
        self.jobs.wall += secs as i64;
    }

    /// The task `name` ends in `state`, as its agent would report.
    fn end(&mut self, name: &str, state: TaskState) {
        let version = self.next();
        let t = self
            .tasks
            .iter_mut()
            .find(|t| t.object.metadata.name == name)
            .unwrap();
        t.status = TaskStatus {
            state,
            ..Default::default()
        };
        t.object.status = serde_json::to_value(&t.status).unwrap();
        t.object.metadata.version = version;
    }

    fn job_status(&self, name: &str) -> legion_api::job::JobStatus {
        let j = self
            .jobs
            .jobs
            .iter()
            .find(|j| j.object.metadata.name == name)
            .unwrap();
        serde_json::from_value(j.object.status.clone()).unwrap()
    }

    fn cron_status(&self, name: &str) -> legion_api::job::CronJobStatus {
        let c = self
            .jobs
            .cron_jobs
            .iter()
            .find(|c| c.object.metadata.name == name)
            .unwrap();
        serde_json::from_value(c.object.status.clone()).unwrap()
    }

    fn job_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .jobs
            .jobs
            .iter()
            .map(|j| j.object.metadata.name.clone())
            .collect();
        v.sort();
        v
    }
}

fn job(extra: &str) -> String {
    format!("{extra}\npod: {{ containers: [{{ name: dump, image: \"tools:1\" }}] }}")
}

fn at(time: &str) -> i64 {
    legion_api::clock::parse(time).unwrap()
}

#[test]
fn a_job_runs_its_completions() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.job("backup", &job("completions: 3\nparallelism: 2"));
    w.settle();
    assert_eq!(w.running(), ["backup-1-1", "backup-2-1"]);
    let t = w
        .tasks
        .iter()
        .find(|t| t.object.metadata.name == "backup-1-1")
        .unwrap();
    assert!(t.spec.job);
    assert_eq!(
        t.object.metadata.labels.get("legion.job").unwrap(),
        "backup"
    );
    assert!(!t.object.metadata.labels.contains_key("legion.service"));
    let st = w.job_status("backup");
    assert_eq!(
        (st.state.as_str(), st.active, st.succeeded),
        ("running", 2, 0)
    );
    assert_eq!(st.started.as_deref(), Some("2026-10-28T09:31:00Z"));

    // As one completes, the next starts.
    w.end("backup-1-1", TaskState::Complete);
    w.settle();
    assert_eq!(w.running(), ["backup-2-1", "backup-3-1"]);
    w.advance(30);
    w.end("backup-2-1", TaskState::Complete);
    w.end("backup-3-1", TaskState::Complete);
    w.settle();
    let st = w.job_status("backup");
    assert_eq!(
        (st.state.as_str(), st.active, st.succeeded, st.failed),
        ("complete", 0, 3, 0)
    );
    assert_eq!(st.finished.as_deref(), Some("2026-10-28T09:31:30Z"));
    // Its tasks stay, for the record, until the Job goes.
    assert_eq!(
        w.tasks(),
        [
            "backup-1-1 complete n1",
            "backup-2-1 complete n1",
            "backup-3-1 complete n1"
        ]
    );
    w.jobs.jobs.clear();
    w.settle();
    assert!(w.tasks.is_empty());
}

#[test]
fn a_job_retries_until_its_backoff_limit() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.job("flaky", &job("backoffLimit: 1"));
    w.settle();
    w.fail("flaky-1-1");
    w.settle();
    assert!(w.running().is_empty(), "the retry waits");
    assert_eq!(w.wake(), Some(w.now + Duration::from_secs(10)));
    w.advance(10);
    w.settle();
    assert_eq!(w.running(), ["flaky-1-2"]);
    let st = w.job_status("flaky");
    assert_eq!((st.state.as_str(), st.failed), ("running", 1));

    w.fail("flaky-1-2");
    w.settle();
    let st = w.job_status("flaky");
    assert_eq!(st.state.as_str(), "failed");
    assert_eq!(
        st.message,
        "2 tasks failed, more than its backoffLimit of 1; the last: container exited with 1"
    );
    w.advance(600);
    w.settle();
    assert!(w.running().is_empty(), "a failed Job runs no more");
}

#[test]
fn a_job_stops_at_its_deadline() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.job("slow", &job("activeDeadline: 1m"));
    w.settle();
    assert_eq!(w.running(), ["slow-1-1"]);
    assert_eq!(w.wake(), Some(w.now + Duration::from_secs(60)));
    w.advance(60);
    w.settle();
    let st = w.job_status("slow");
    assert_eq!(st.state.as_str(), "failed");
    assert_eq!(st.message, "ran longer than its activeDeadline of 1m");
    assert_eq!(w.tasks(), ["slow-1-1 shutdown n1"]);
}

#[test]
fn a_global_job_runs_once_per_node() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.job("scan", &job("mode: global"));
    w.settle();
    assert_eq!(w.running(), ["scan-n1-1", "scan-n2-1"]);
    w.end("scan-n1-1", TaskState::Complete);
    w.settle();
    assert_eq!(w.job_status("scan").state.as_str(), "running");
    w.end("scan-n2-1", TaskState::Complete);
    w.settle();
    let st = w.job_status("scan");
    assert_eq!((st.state.as_str(), st.succeeded), ("complete", 2));
    // A node that comes later finds the Job done.
    w.node("n3", &[]);
    w.settle();
    assert!(w.running().is_empty());
}

#[test]
fn a_drained_node_moves_a_job_task_without_a_failure() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.job("backup", &job(""));
    w.settle();
    assert_eq!(w.tasks(), ["backup-1-1 running n1"]);
    w.node("n2", &[]);
    w.availability("n1", Availability::Drain);
    w.settle();
    assert_eq!(
        w.tasks(),
        ["backup-1-1 shutdown n1", "backup-1-2 running n2"]
    );
    assert_eq!(w.job_status("backup").failed, 0);
}

fn report(extra: &str) -> String {
    format!(
        "schedule: \"*/5 * * * *\"\n{extra}\njob:\n  pod: {{ containers: [{{ name: r, image: \"tools:1\" }}] }}"
    )
}

/// The name of the run of `report` due at `time`.
fn run(time: &str) -> String {
    format!("report-{}", at(time) / 60)
}

/// Completes the running tasks of the Job `job`.
fn complete_job(w: &mut World, job: &str) {
    let names: Vec<String> = w
        .tasks
        .iter()
        .filter(|t| t.spec.service == job && t.status.state == TaskState::Running)
        .map(|t| t.object.metadata.name.clone())
        .collect();
    for n in names {
        w.end(&n, TaskState::Complete);
    }
    w.settle();
}

#[test]
fn a_cron_job_runs_on_schedule() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.cron_job("report", &report("successfulJobsHistoryLimit: 1"));
    w.settle();
    // Runs before it was made are not owed.
    assert!(w.job_names().is_empty());
    let st = w.cron_status("report");
    assert_eq!(st.last_schedule.as_deref(), Some("2026-10-28T09:31:00Z"));
    assert_eq!(w.wake(), Some(w.now + Duration::from_secs(4 * 60)));

    w.advance(4 * 60);
    w.settle();
    let first = run("2026-10-28T09:35:00Z");
    assert_eq!(w.job_names(), std::slice::from_ref(&first));
    let j = &w.jobs.jobs[0];
    assert_eq!(
        j.object.metadata.labels.get("legion.cronjob").unwrap(),
        "report"
    );
    assert_eq!(w.running(), [format!("{first}-1-1")]);
    let st = w.cron_status("report");
    assert_eq!(st.last_schedule.as_deref(), Some("2026-10-28T09:35:00Z"));
    assert_eq!(st.last_job, first);
    complete_job(&mut w, &first);
    assert_eq!(w.job_status(&first).state.as_str(), "complete");

    // The next run; history keeps one finished Job, so the first goes,
    // its tasks with it.
    w.advance(5 * 60);
    w.settle();
    let second = run("2026-10-28T09:40:00Z");
    complete_job(&mut w, &second);
    assert_eq!(w.job_names(), std::slice::from_ref(&second));
    assert_eq!(w.tasks(), [format!("{second}-1-1 complete n1")]);

    // A controller away for long owes only the latest run.
    w.advance(23 * 60);
    w.settle();
    let late = run("2026-10-28T10:00:00Z");
    assert_eq!(w.job_names(), [second.clone(), late.clone()]);

    // Deleting the CronJob deletes its Jobs.
    w.jobs.cron_jobs.clear();
    w.settle();
    assert!(w.job_names().is_empty());
    assert!(w.tasks.is_empty());
}

#[test]
fn concurrency_policies() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.cron_job("report", &report("concurrencyPolicy: forbid"));
    w.settle();
    w.advance(4 * 60);
    w.settle();
    let first = run("2026-10-28T09:35:00Z");
    assert_eq!(w.job_names(), std::slice::from_ref(&first));

    // Still running: forbid skips the run.
    w.advance(5 * 60);
    w.settle();
    assert_eq!(w.job_names(), std::slice::from_ref(&first));
    let st = w.cron_status("report");
    assert_eq!(
        st.message,
        format!("skipped the run due at 2026-10-28T09:40:00Z: {first} still running")
    );
    assert_eq!(st.last_schedule.as_deref(), Some("2026-10-28T09:40:00Z"));

    // Replace: the running Job goes, its task stopped, and the new run
    // takes its place.
    w.cron_job("report", &report("concurrencyPolicy: replace"));
    w.advance(5 * 60);
    w.settle();
    let third = run("2026-10-28T09:45:00Z");
    assert_eq!(w.job_names(), std::slice::from_ref(&third));
    assert_eq!(w.running(), [format!("{third}-1-1")]);
    assert!(w.cron_status("report").message.is_empty());

    // Allow: runs side by side.
    w.cron_job("report", &report("concurrencyPolicy: allow"));
    w.advance(5 * 60);
    w.settle();
    let fourth = run("2026-10-28T09:50:00Z");
    assert_eq!(w.job_names(), [third.clone(), fourth.clone()]);
    assert_eq!(w.running().len(), 2);
}

#[test]
fn suspended_and_late_runs_are_skipped() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.cron_job("report", &report("suspend: true\nstartingDeadline: 30s"));
    w.settle();
    w.advance(4 * 60);
    w.settle();
    assert!(w.job_names().is_empty());
    assert_eq!(
        w.cron_status("report").message,
        "suspended: skipped the run due at 2026-10-28T09:35:00Z"
    );

    // Resumed, but the next pass comes 40s after the run was due.
    w.cron_job("report", &report("startingDeadline: 30s"));
    w.advance(5 * 60 + 40);
    w.settle();
    assert!(w.job_names().is_empty());
    assert_eq!(
        w.cron_status("report").message,
        "skipped the run due at 2026-10-28T09:40:00Z: more than its startingDeadline of 30s late"
    );
    // On time again.
    w.advance(5 * 60 - 40);
    w.settle();
    assert_eq!(w.job_names(), [run("2026-10-28T09:45:00Z")]);
}

impl World {
    /// Settles, returning how many of `service`'s tasks are ready after each
    /// pass's writes, before the agents act on them.
    fn ready_per_pass(&mut self, service: &str) -> Vec<usize> {
        let mut seen = Vec::new();
        for _ in 1..100 {
            let p = plan(
                &self.services,
                &self.jobs,
                &self.tasks,
                &self.nodes,
                &self.volumes,
                &self.quotas,
                &mut self.memory,
                self.now,
            );
            let acted = !p.actions.is_empty();
            self.apply(p.actions);
            seen.push(
                self.tasks
                    .iter()
                    .filter(|t| {
                        t.spec.service == service && t.spec.desired_state == DesiredState::Running
                    })
                    .filter(|t| t.status.state == TaskState::Running && t.status.ready)
                    .count(),
            );
            if !self.agents() && !acted {
                return seen;
            }
        }
        panic!("did not settle");
    }

    fn nodes_of(&self, service: &str) -> Vec<String> {
        let mut v: Vec<String> = self
            .tasks
            .iter()
            .filter(|t| t.spec.service == service && t.status.state == TaskState::Running)
            .map(|t| t.spec.node.clone())
            .collect();
        v.sort();
        v
    }
}

#[test]
fn a_disruption_budget_paces_drains() {
    for (budget, least) in [("", 2), ("maxUnavailable: 1", 3), ("maxUnavailable: 0", 4)] {
        let mut w = World::new();
        w.node("n1", &[]);
        w.node("n2", &[]);
        w.service("web", &format!("replicas: 4\n{POD}\n{budget}"));
        w.settle();
        assert_eq!(w.nodes_of("web"), ["n1", "n1", "n2", "n2"]);

        w.availability("n1", Availability::Drain);
        let ready = w.ready_per_pass("web");
        assert_eq!(ready.iter().min(), Some(&least), "{budget:?}: {ready:?}");
        assert_eq!(w.nodes_of("web"), ["n2", "n2", "n2", "n2"], "{budget:?}");
    }
}

#[test]
fn a_budget_spent_elsewhere_drains_start_first() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("web", &format!("replicas: 2\n{POD}\nmaxUnavailable: 1"));
    w.settle();
    // One slot is already down: draining the other must not stop it first.
    let on_n2 = w.tasks.iter().find(|t| t.spec.node == "n2").unwrap();
    let name = on_n2.object.metadata.name.clone();
    w.agents = false;
    w.fail(&name);
    w.availability("n1", Availability::Drain);
    let p = plan(
        &w.services,
        &w.jobs,
        &w.tasks,
        &w.nodes,
        &w.volumes,
        &w.quotas,
        &mut w.memory,
        w.now,
    );
    w.apply(p.actions);
    let on_n1: Vec<String> = w
        .tasks
        .iter()
        .filter(|t| t.spec.node == "n1")
        .map(|t| format!("{} {:?}", t.status.state.as_str(), t.spec.desired_state))
        .collect();
    assert_eq!(on_n1, ["running Running"]);
    w.agents = true;
    w.now += Duration::from_secs(60);
    w.settle();
    assert_eq!(w.nodes_of("web"), ["n2", "n2"]);
}

#[test]
fn a_disruption_budget_paces_updates() {
    // Parallelism 0 replaces every slot at once; the budget holds it back.
    for (budget, least) in [("", 0), ("maxUnavailable: 1", 2), ("maxUnavailable: 0", 3)] {
        let mut w = World::new();
        w.node("n1", &[]);
        let spec = |image: &str| {
            format!(
                "replicas: 3\npod: {{ containers: [{{ name: web, image: \"{image}\" }}] }}\nupdate: {{ parallelism: 0, monitor: 0s }}\n{budget}"
            )
        };
        w.service("web", &spec("web:1"));
        w.settle();
        w.service("web", &spec("web:2"));
        let ready = w.ready_per_pass("web");
        assert_eq!(ready.iter().min(), Some(&least), "{budget:?}: {ready:?}");
        assert_eq!(w.images("web"), ["web:2", "web:2", "web:2"], "{budget:?}");
        assert_eq!(w.update("web").0, "completed", "{budget:?}");
    }
}

const STATEFUL: &str = "mode: stateful\nreplicas: 3\npod: { containers: [{ name: db, image: \"pg:17\" }], volumes: [{ name: data, claim: { size: 1G } }] }";

#[test]
fn stateful_tasks_start_in_order_with_their_own_volumes() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.node("n3", &[]);
    w.agents = false;
    w.service("db", STATEFUL);
    w.step();
    // Every ordinal's Volume is made at once; only ordinal 0 starts.
    let mut names: Vec<&String> = w.volumes.keys().map(|(_, n)| n).collect();
    names.sort();
    assert_eq!(names, ["data-db-0", "data-db-1", "data-db-2"]);
    let v = &w.volumes[&("shop".to_owned(), "data-db-1".to_owned())];
    assert_eq!(v.spec.size, Some(Bytes(1_000_000_000)));
    assert_eq!(v.object.metadata.labels["legion.service"], "db");
    assert_eq!(w.wanted("db"), ["db-0-1"]);
    // Not before it is ready.
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-1"]);
    w.agents = true;
    w.agents();
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-1", "db-1-1"]);
    w.settle();
    assert_eq!(w.wanted("db"), ["db-0-1", "db-1-1", "db-2-1"]);

    // Each task knows its ordinal and claims its own Volume.
    let t = w
        .tasks
        .iter()
        .find(|t| t.object.metadata.name == "db-1-1")
        .unwrap();
    assert!(t.spec.stateful);
    assert_eq!(t.spec.slot, Some(1));
    assert_eq!(t.spec.hostname().as_deref(), Some("db-1"));
    assert_eq!(t.spec.volumes(), ["data-db-1"]);
    let home = &w.volumes[&("shop".to_owned(), "data-db-1".to_owned())]
        .status
        .node;
    assert_eq!(*home, t.spec.node);
    // Spread over the nodes, as replicas are.
    assert_eq!(w.placed("db"), ["n1", "n2", "n3"]);

    // A replacement goes where its Volume is.
    let node = t.spec.node.clone();
    w.fail("db-1-1");
    w.settle();
    w.now += Duration::from_secs(5);
    w.settle();
    let t = w
        .tasks
        .iter()
        .find(|t| t.object.metadata.name == "db-1-2")
        .unwrap();
    assert_eq!(t.spec.node, node);
}

#[test]
fn a_failed_ordinal_holds_back_the_ones_above() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.agents = false;
    w.service("db", STATEFUL);
    w.step();
    w.fail("db-0-1");
    w.step();
    w.now += Duration::from_secs(5);
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-2"]);
    w.fail("db-0-2");
    w.step();
    w.now += Duration::from_secs(10);
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-3"]);
}

#[test]
fn stateful_services_shrink_from_the_top_one_at_a_time() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("db", STATEFUL);
    w.settle();
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.agents = false;
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-1", "db-1-1"]);
    w.step();
    assert_eq!(
        w.wanted("db"),
        ["db-0-1", "db-1-1"],
        "waits for db-2 to stop"
    );
    w.agents = true;
    w.agents();
    w.agents = false;
    w.step();
    assert_eq!(w.wanted("db"), ["db-0-1"]);
    w.agents = true;
    w.settle();
    assert_eq!(w.wanted("db"), ["db-0-1"]);
    // The Volumes stay, for the data in them; growing again finds them.
    assert_eq!(w.volumes.len(), 3);
    w.service("db", STATEFUL);
    w.settle();
    assert_eq!(w.wanted("db"), ["db-0-1", "db-1-1", "db-2-1"]);
    assert_eq!(w.volumes.len(), 3);

    // Gone, the service leaves them too; a resized claim resizes them.
    w.service("db", &STATEFUL.replace("1G", "2G"));
    w.settle();
    assert!(
        w.volumes
            .values()
            .all(|v| v.spec.size == Some(Bytes(2_000_000_000)))
    );
    w.delete_service("db");
    w.settle();
    assert!(w.wanted("db").is_empty());
    assert_eq!(w.volumes.len(), 3);
}

#[test]
fn stateful_updates_go_from_the_highest_ordinal_down() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("db", STATEFUL);
    w.settle();
    w.service("db", &STATEFUL.replace("pg:17", "pg:18"));
    // Stop-first, one slot at a time, highest first.
    let mut order = vec![];
    for _ in 0..30 {
        w.step();
        for t in &w.tasks {
            let name = &t.object.metadata.name;
            if t.spec.pod.containers[0].image == "pg:18"
                && t.status.state == TaskState::Running
                && !order.contains(name)
            {
                order.push(name.clone());
            }
        }
        let active = |slot| {
            w.tasks
                .iter()
                .filter(|t| t.spec.slot == Some(slot) && !t.status.state.is_terminal())
                .count()
        };
        assert!((0..3).all(|s| active(s) <= 1), "two tasks share a slot");
    }
    assert_eq!(order, ["db-2-2", "db-1-2", "db-0-2"]);
}

#[test]
fn stateful_updates_snapshot_and_their_rollbacks_restore() {
    let mut w = World::new();
    w.node("n1", &[]);
    let spec = format!("{STATEFUL}\nupdate: {{ snapshot: true, failureAction: rollback }}");
    w.service("db", &spec);
    w.settle();
    let steps = |w: &World| -> Vec<String> {
        let mut v: Vec<String> = w
            .tasks
            .iter()
            .filter(|t| !t.status.state.is_terminal())
            .map(|t| {
                let step = match &t.spec.snapshot {
                    None => "-".to_owned(),
                    Some(s) if s.restore => format!("restore {}", s.name),
                    Some(s) => format!("take {}", s.name),
                };
                format!("{} {step}", t.object.metadata.name)
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(steps(&w), ["db-0-1 -", "db-1-1 -", "db-2-1 -"]);

    // The update's tasks snapshot their Volumes first.
    w.service("db", &spec.replace("pg:17", "pg:18"));
    w.settle();
    assert_eq!(
        w.status("db").update.unwrap().snapshot.as_deref(),
        Some("legion-update-2")
    );
    assert_eq!(
        steps(&w),
        [
            "db-0-2 take legion-update-2",
            "db-1-2 take legion-update-2",
            "db-2-2 take legion-update-2"
        ]
    );

    // One failing within the monitor window rolls back, and the rollback's
    // tasks return to the snapshot.
    w.fail("db-0-2");
    for _ in 0..12 {
        w.now += Duration::from_secs(1);
        w.settle();
    }
    assert_eq!(w.update("db").0, "rolled-back");
    assert_eq!(
        steps(&w),
        [
            "db-0-3 restore legion-update-2",
            "db-1-3 restore legion-update-2",
            "db-2-3 restore legion-update-2"
        ]
    );
    // Tasks after the rollback leave the Volumes be.
    w.fail("db-2-3");
    w.settle();
    w.now += Duration::from_secs(10);
    w.settle();
    assert_eq!(steps(&w)[2], "db-2-4 -");

    // Without `snapshot`, an update takes none.
    w.service("db", &STATEFUL.replace("pg:17", "pg:19"));
    w.settle();
    assert_eq!(w.status("db").update.unwrap().snapshot, None);
    assert!(steps(&w).iter().all(|s| s.ends_with(" -")));
}

#[test]
fn stateful_admission() {
    let admit = |spec: &str| -> Result<(), String> {
        let value: serde_json::Value = serde_yaml_ng::from_str(spec).unwrap();
        let mut s: ServiceSpec = legion_api::parse(&value).unwrap();
        s.admit().map_err(|e| e.to_string())
    };
    assert_eq!(
        admit(&format!("{STATEFUL}\nupdate: {{ order: start-first }}")).unwrap_err(),
        "spec.update.order: a stateful task's volumes are its own: its replacement starts after it stops"
    );
    assert_eq!(
        admit(&STATEFUL.replace("mode: stateful", "mode: replicated")).unwrap_err(),
        "spec.pod.volumes: volume \"data\": only a stateful service's tasks claim volumes"
    );
    assert_eq!(
        admit(&format!("{POD}\nupdate: {{ snapshot: true }}")).unwrap_err(),
        "spec.update.snapshot: only a stateful service's tasks have volumes of their own to snapshot"
    );
    assert!(admit(STATEFUL).is_ok());
}

#[test]
fn stateful_tasks_move_with_their_data_off_a_draining_node() {
    use legion_api::volume::MigrationPhase::*;
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.node("n3", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 2"));
    w.settle();
    let from = w.placed_task("db-0-1").spec.node.clone();
    let busy = w.placed_task("db-1-1").spec.node.clone();
    let free = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != from && *n != busy)
        .unwrap();

    // Draining its node starts a copy to the node with fewest of its
    // tasks, while the task runs on.
    w.availability(&from, Availability::Drain);
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((free.to_owned(), Copying)));
    assert_eq!(w.wanted("db"), ["db-0-1", "db-1-1"]);

    // Copied, the task stops; with nothing using the Volume the last part
    // goes, and its next task waits for it.
    w.migrate();
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((free.to_owned(), Finishing)));
    let next = w.placed_task("db-0-2");
    assert_eq!(next.status.state, TaskState::Pending);
    assert_eq!(
        next.status.message,
        "no node can run it: 1 not active, 2 volume moving"
    );

    // Moved, the task runs where its data is.
    w.migrate();
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);
    assert_eq!(
        w.volumes[&("shop".to_owned(), "data-db-0".to_owned())]
            .status
            .node,
        free
    );
    assert_eq!(w.placed_task("db-0-2").spec.node, free);
    assert_eq!(w.placed_task("db-0-2").status.state, TaskState::Running);
}

#[test]
fn migrations_stop_when_no_longer_needed_or_possible() {
    use legion_api::volume::MigrationPhase::*;
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let from = w.placed_task("db-0-1").spec.node.clone();
    let to = if from == "n1" { "n2" } else { "n1" };

    // The drain called off, the copy is dropped and the task stays.
    w.availability(&from, Availability::Drain);
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((to.to_owned(), Copying)));
    w.availability(&from, Availability::Active);
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);
    assert_eq!(w.wanted("db"), ["db-0-1"]);

    // The new node lost, the copy is dropped; with no other node, none
    // starts, and the task runs on.
    w.availability(&from, Availability::Drain);
    w.settle();
    w.state(to, NodeState::Down);
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);
    assert_eq!(w.placed_task("db-0-1").status.state, TaskState::Running);
}

impl World {
    /// Every running task of `service` reports this use (percent of a CPU,
    /// bytes) from its node.
    fn report(&mut self, service: &str, cpu: f64, memory: u64) {
        self.memory.usage = self
            .tasks
            .iter()
            .filter(|t| t.spec.service == service && t.status.state == TaskState::Running)
            .map(|t| {
                (
                    format!("shop/{}", t.object.metadata.name),
                    (
                        t.spec.node.clone(),
                        legion_api::dispatch::TaskUsage { cpu, memory },
                    ),
                )
            })
            .collect();
    }

    fn scaled(&self, service: &str) -> legion_api::service::AutoscaleStatus {
        self.status(service).autoscale.expect("autoscaled")
    }
}

const SCALED: &str = "autoscale: { min: 1, max: 5, cpu: 50, downDelay: 60s }
pod:
  containers:
    - { name: web, image: web, resources: { requests: { cpu: 500m, memory: 64Mi } } }";

#[test]
fn autoscaling_grows_at_once_and_shrinks_after_the_delay() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("web", SCALED);
    w.settle();
    assert_eq!(w.running().len(), 1, "starts at min");
    let st = w.scaled("web");
    assert_eq!(st.replicas, 1);
    assert!(st.message.contains("no use reported"), "{st:?}");

    // One task at a whole CPU: 200% of its request, four times the target.
    w.report("web", 100.0, 32 << 20);
    w.settle();
    assert_eq!(w.running().len(), 4, "{:?}", w.tasks());
    let st = w.scaled("web");
    assert_eq!(
        (st.replicas, st.cpu, st.message.as_str()),
        (4, Some(200), "")
    );
    assert_eq!(w.status("web").desired, 4);
    // More passes on the same reports change nothing.
    w.now += Duration::from_secs(5);
    w.settle();
    assert_eq!(w.running().len(), 4);

    // Within a tenth of the target: stays.
    w.report("web", 27.0, 32 << 20);
    w.settle();
    assert_eq!(w.scaled("web").replicas, 4);

    // Idle: shrinks only once low for the whole delay.
    w.report("web", 5.0, 32 << 20);
    for _ in 0..11 {
        w.now += Duration::from_secs(5);
        w.settle();
        assert_eq!(w.running().len(), 4, "too soon");
    }
    w.now += Duration::from_secs(10);
    w.settle();
    // 4 tasks at 10% of their request, a fifth of the target: one.
    assert_eq!(w.running().len(), 1, "{:?}", w.tasks());
    assert_eq!(w.scaled("web").cpu, Some(10));

    // Far over the target: up to max, no further.
    w.report("web", 1000.0, 32 << 20);
    w.settle();
    assert_eq!(w.running().len(), 5);
}

#[test]
fn autoscaling_counts_only_reports_it_can_believe() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("web", &SCALED.replace("min: 1", "min: 2"));
    w.settle();
    assert_eq!(w.running().len(), 2);

    // A report from another node than the task's is ignored.
    w.report("web", 100.0, 0);
    for (node, _) in w.memory.usage.values_mut() {
        *node = "elsewhere".into();
    }
    w.settle();
    assert_eq!(w.running().len(), 2);
    assert!(w.scaled("web").message.contains("no use reported"));

    // One busy task reports, the other does not: grows by the busy one
    // alone (the silent one counts as idle).
    w.report("web", 100.0, 0);
    let first = w.memory.usage.keys().next().unwrap().clone();
    w.memory.usage.retain(|k, _| *k == first);
    w.settle();
    assert_eq!(w.running().len(), 4);

    // Idle, but the other tasks silent: they count as on target, so it
    // keeps three of the four, and a delay later two of the three.
    w.report("web", 0.0, 0);
    let first = w.memory.usage.keys().next().unwrap().clone();
    w.memory.usage.retain(|k, _| *k == first);
    w.now += Duration::from_secs(120);
    w.settle();
    assert_eq!(w.running().len(), 3, "{:?}", w.tasks());
    w.now += Duration::from_secs(61);
    w.settle();
    assert_eq!(w.running().len(), 2, "{:?}", w.tasks());

    // A new leader waits the delay before shrinking.
    w.report("web", 100.0, 0);
    w.settle();
    assert_eq!(w.running().len(), 5, "8 wanted, 5 at most");
    w.memory = Memory::default();
    w.report("web", 0.0, 0);
    w.settle();
    w.now += Duration::from_secs(30);
    w.settle();
    assert_eq!(w.running().len(), 5, "too soon for a new leader");
    w.now += Duration::from_secs(31);
    w.settle();
    assert_eq!(w.running().len(), 2, "down to min");

    // Without autoscale, replicas rules again.
    w.service("web", &format!("replicas: 3\n{POD}"));
    w.settle();
    assert_eq!(w.running().len(), 3);
    assert!(w.status("web").autoscale.is_none());
}

#[test]
fn autoscale_admission() {
    let admit = |spec: &str| {
        let value: serde_json::Value = serde_yaml_ng::from_str(spec).unwrap();
        let mut spec: ServiceSpec = legion_api::parse(&value).unwrap();
        spec.admit().map(|()| spec).map_err(|e| e.to_string())
    };
    let ok = admit(SCALED).unwrap();
    assert_eq!(ok.replicas, Some(1), "starts at min");
    for (bad, why) in [
        (SCALED.replace("min: 1", "min: 0"), "at least 1"),
        (SCALED.replace("max: 5", "max: 0"), "min at most max"),
        (SCALED.replace("cpu: 50, ", ""), "cpu or memory"),
        (
            SCALED
                .replace("cpu: 50, ", "memory: 80, ")
                .replace("memory: 64Mi", ""),
            "requests no memory",
        ),
        (SCALED.replace("cpu: 500m, ", ""), "requests no cpu"),
        (format!("mode: global\n{SCALED}"), "only a replicated"),
    ] {
        let err = admit(&bad).unwrap_err();
        assert!(err.contains(why), "{bad}: {err}");
    }
}

impl World {
    /// Asks for a Volume's data to be on `node`, as an apply would.
    fn ask(&mut self, volume: &str, node: Option<&str>) {
        let index = self.next();
        let v = self
            .volumes
            .get_mut(&("shop".to_owned(), volume.to_owned()))
            .unwrap();
        v.spec.node = node.map(str::to_owned);
        v.object.spec = serde_json::to_value(&v.spec).unwrap();
        v.object.metadata.version = index;
    }

    fn volume_node(&self, volume: &str) -> String {
        self.volumes[&("shop".to_owned(), volume.to_owned())]
            .status
            .node
            .clone()
    }
}

#[test]
fn a_volume_moves_where_it_is_asked_to_be_with_its_task() {
    use legion_api::volume::MigrationPhase::*;
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let from = w.placed_task("db-0-1").spec.node.clone();
    let to = if from == "n1" { "n2" } else { "n1" };

    // Asked to be where it is: nothing.
    w.ask("data-db-0", Some(&from));
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);

    // Asked elsewhere: copied while the task runs, then the task stops and
    // its next one waits for the rest, and runs there.
    w.ask("data-db-0", Some(to));
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((to.to_owned(), Copying)));
    assert_eq!(w.wanted("db"), ["db-0-1"]);
    w.migrate();
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((to.to_owned(), Finishing)));
    assert_eq!(
        w.placed_task("db-0-2").status.message,
        "no node can run it: 2 volume moving"
    );
    w.migrate();
    w.settle();
    assert_eq!(w.volume_node("data-db-0"), to);
    assert_eq!(w.placed_task("db-0-2").spec.node, to);
    assert_eq!(w.placed_task("db-0-2").status.state, TaskState::Running);

    // Asked back, then not before the copy is done: dropped, nothing moves.
    w.ask("data-db-0", Some(&from));
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((from.clone(), Copying)));
    w.ask("data-db-0", None);
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);
    assert_eq!(w.wanted("db"), ["db-0-2"]);

    // Asked to a node that cannot take it: waits for it.
    w.ask("data-db-0", Some(&from));
    w.availability(&from, Availability::Pause);
    w.settle();
    assert_eq!(w.migration("data-db-0"), None);
    w.availability(&from, Availability::Active);
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((from.clone(), Copying)));
}

#[test]
fn a_drain_moves_a_volume_asked_to_be_there_and_it_comes_back() {
    use legion_api::volume::MigrationPhase::*;
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let home = w.placed_task("db-0-1").spec.node.clone();
    let other = if home == "n1" { "n2" } else { "n1" };
    w.ask("data-db-0", Some(&home));
    w.availability(&home, Availability::Drain);
    for _ in 0..2 {
        w.settle();
        w.migrate();
    }
    w.settle();
    assert_eq!(w.volume_node("data-db-0"), other);
    assert_eq!(
        w.migration("data-db-0"),
        None,
        "stays while its node drains"
    );
    w.availability(&home, Availability::Active);
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((home.clone(), Copying)));
    for _ in 0..2 {
        w.migrate();
        w.settle();
    }
    assert_eq!(w.volume_node("data-db-0"), home);
    assert_eq!(w.running(), ["db-0-3"]);
}

#[test]
fn a_new_volume_asked_to_be_on_a_node_takes_its_first_task_there() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.volume("pgdata", "");
    w.ask("pgdata", Some("n2"));
    w.service(
        "db",
        "replicas: 1\npod: { containers: [{ name: db, image: \"pg:17\" }], volumes: [{ name: data, volume: { name: pgdata } }] }",
    );
    w.settle();
    assert_eq!(w.placed("db"), ["n2"]);
    assert_eq!(w.volume_node("pgdata"), "n2", "bound by its first task");
}
