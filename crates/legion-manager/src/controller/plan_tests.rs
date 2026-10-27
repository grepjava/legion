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

use super::plan::{Action, HISTORY, Homes, Memory, Node, Service, Task, plan};

struct World {
    services: Vec<Service>,
    tasks: Vec<Task>,
    nodes: Vec<Node>,
    homes: Homes,
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
            tasks: vec![],
            nodes: vec![],
            homes: Homes::new(),
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

    fn delete_service(&mut self, name: &str) {
        self.services.retain(|s| s.object.metadata.name != name);
    }

    fn apply(&mut self, actions: Vec<Action>) {
        for a in actions {
            let index = self.next();
            match a {
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
                        k => panic!("unexpected update of {k}"),
                    }
                }
                Action::Delete(key, version) => {
                    self.tasks
                        .retain(|t| t.object.key() != key || t.object.metadata.version != version);
                }
            }
        }
    }

    /// Agents: assigned tasks start; stopping tasks stop. A node that is
    /// down does neither.
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
            if !up || t.status.state.is_terminal() {
                continue;
            }
            let next = match t.spec.desired_state {
                DesiredState::Running if t.status.state != TaskState::Running => TaskState::Running,
                DesiredState::Shutdown => TaskState::Shutdown,
                _ => continue,
            };
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
                &self.tasks,
                &self.nodes,
                &self.homes,
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
            &self.tasks,
            &self.nodes,
            &self.homes,
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
        &w.tasks,
        &w.nodes,
        &w.homes,
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
        &w.tasks,
        &w.nodes,
        &w.homes,
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
    w.homes
        .insert(("shop".into(), "pgdata".into()), "n2".into());
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
