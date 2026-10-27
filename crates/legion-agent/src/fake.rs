//! An in-memory [`Runtime`] for tests. It behaves like Podman with ocijail
//! where the agent depends on it: starting a container creates its jail in
//! a [`FakeSys`] (with a VNET when the infra annotation asks for one, as a
//! child of the infra jail when `parentJail` is set), a stopped container's
//! jail goes away, stopping the infra jail kills the pod, and every death
//! is reported as a `died` event carrying the container's labels. Starting
//! a container clears its jail's rctl rules, as ocijail with rctl support
//! does. Exec runs nothing: each container answers with the exit code
//! set by [`FakeRuntime::set_exec`], 0 by default.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use legion_podman::{
    Actor, ContainerConfig, ContainerInspect, ContainerSpec, ContainerState, ContainerSummary,
    Error, Event, ExecOutput, Result,
};
use legion_sys::FakeSys;
use tokio::sync::broadcast;

use crate::pod::{ANNOTATION_PARENT, ANNOTATION_VNET, LABEL_ROLE, child_jail};
use crate::runtime::Runtime;

pub struct FakeRuntime {
    sys: Arc<FakeSys>,
    state: Mutex<State>,
    events: broadcast::Sender<Event>,
}

#[derive(Default)]
struct State {
    images: BTreeSet<String>,
    /// Pulls of these fail.
    unpullable: BTreeSet<String>,
    next_id: u64,
    containers: BTreeMap<String, Container>,
    /// Ignore the VNET annotation, as a runtime without support would.
    ignore_vnet: bool,
    /// Exit code of init containers, which exit as soon as they start.
    init_exit_code: i32,
    created: usize,
    /// Exit code of exec in containers, by container name.
    exec: BTreeMap<String, i32>,
    execs: usize,
}

#[derive(Debug, Clone)]
struct Container {
    spec: ContainerSpec,
    running: bool,
    exit_code: i32,
    starts: u32,
    jail: Option<String>,
}

fn api(status: u16, message: impl Into<String>) -> Error {
    Error::Api {
        status,
        message: message.into(),
    }
}

impl FakeRuntime {
    pub fn new(sys: Arc<FakeSys>) -> Self {
        FakeRuntime {
            sys,
            state: Mutex::new(State::default()),
            events: broadcast::channel(1024).0,
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn add_image(&self, reference: &str) {
        self.state().images.insert(reference.to_owned());
    }

    pub fn fail_pulls_of(&self, reference: &str) {
        self.state().unpullable.insert(reference.to_owned());
    }

    pub fn ignore_vnet(&self) {
        self.state().ignore_vnet = true;
    }

    pub fn set_init_exit_code(&self, code: i32) {
        self.state().init_exit_code = code;
    }

    /// Exec in the container `name` exits with `code` from now on.
    pub fn set_exec(&self, name: &str, code: i32) {
        self.state().exec.insert(name.to_owned(), code);
    }

    /// Execs run so far.
    pub fn execs(&self) -> usize {
        self.state().execs
    }

    /// Containers created so far, removed ones included.
    pub fn created(&self) -> usize {
        self.state().created
    }

    /// `(name, running, starts)` of every container, by name.
    pub fn containers(&self) -> Vec<(String, bool, u32)> {
        let mut v: Vec<_> = self
            .state()
            .containers
            .values()
            .map(|c| (c.spec.name.clone(), c.running, c.starts))
            .collect();
        v.sort();
        v
    }

    /// The spec a container was created with.
    pub fn spec(&self, name: &str) -> Option<ContainerSpec> {
        self.state()
            .containers
            .values()
            .find(|c| c.spec.name == name)
            .map(|c| c.spec.clone())
    }

    pub fn id_of(&self, name: &str) -> Option<String> {
        self.state()
            .containers
            .iter()
            .find(|(_, c)| c.spec.name == name)
            .map(|(id, _)| id.clone())
    }

    /// The container's process exits with `code`, as if on its own.
    pub fn exit(&self, name: &str, code: i32) {
        let id = self.id_of(name).expect("container exists");
        let mut s = self.state();
        self.die(&mut s, &id, code);
    }

    fn die(&self, s: &mut State, id: &str, code: i32) {
        let Some(c) = s.containers.get_mut(id) else {
            return;
        };
        if !c.running {
            return;
        }
        c.running = false;
        c.exit_code = code;
        let jail = c.jail.take();
        let labels = c.spec.labels.clone();
        let name = c.spec.name.clone();
        // An infra jail takes its children with it.
        if c.spec.annotations.contains_key(ANNOTATION_VNET) {
            let children: Vec<String> = s
                .containers
                .iter()
                .filter(|(_, c)| {
                    c.running
                        && c.spec
                            .annotations
                            .get(ANNOTATION_PARENT)
                            .map(String::as_str)
                            == Some(id)
                })
                .map(|(id, _)| id.clone())
                .collect();
            for child in children {
                self.die(s, &child, 137);
            }
        }
        if let Some(j) = jail {
            self.sys.remove_jail(&j);
        }
        let mut attributes = labels;
        attributes.insert("name".into(), name);
        attributes.insert("containerExitCode".into(), code.to_string());
        let _ = self.events.send(Event {
            kind: "container".into(),
            action: "died".into(),
            actor: Actor {
                id: id.to_owned(),
                attributes,
            },
            time_nano: 0,
        });
    }

    fn get(&self, id: &str) -> Result<Container> {
        self.state().containers.get(id).cloned().ok_or_else(|| {
            api(
                404,
                format!("no container with name or ID \"{id}\" found: no such container"),
            )
        })
    }
}

impl Runtime for FakeRuntime {
    async fn image_exists(&self, reference: &str) -> Result<bool> {
        Ok(self.state().images.contains(reference))
    }

    async fn pull(&self, reference: &str) -> Result<()> {
        let mut s = self.state();
        if s.unpullable.contains(reference) {
            return Err(Error::Pull {
                image: reference.to_owned(),
                message: "requested access to the resource is denied".into(),
            });
        }
        s.images.insert(reference.to_owned());
        Ok(())
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        let mut s = self.state();
        if !s.images.contains(&spec.image) {
            return Err(api(
                404,
                format!("no such image: {}: image not known", spec.image),
            ));
        }
        if s.containers.values().any(|c| c.spec.name == spec.name) {
            return Err(api(
                409,
                format!("the container name \"{}\" is already in use", spec.name),
            ));
        }
        s.next_id += 1;
        s.created += 1;
        let id = format!("{:064x}", s.next_id);
        s.containers.insert(
            id.clone(),
            Container {
                spec: spec.clone(),
                running: false,
                exit_code: 0,
                starts: 0,
                jail: None,
            },
        );
        Ok(id)
    }

    async fn start(&self, id: &str) -> Result<()> {
        let mut s = self.state();
        let ignore_vnet = s.ignore_vnet;
        let init_exit = s.init_exit_code;
        let c = s
            .containers
            .get(id)
            .cloned()
            .ok_or_else(|| api(404, "no such container"))?;
        if c.running {
            return Ok(());
        }
        let jail = match c.spec.annotations.get(ANNOTATION_PARENT) {
            Some(parent) => {
                if !s.containers.get(parent).is_some_and(|p| p.running) {
                    return Err(api(500, format!("parent jail {parent} is not running")));
                }
                let name = child_jail(parent, id);
                self.sys.add_jail(&name, false);
                name
            }
            None => {
                let vnet =
                    c.spec.annotations.get(ANNOTATION_VNET).map(String::as_str) == Some("new");
                self.sys.add_jail(id, vnet && !ignore_vnet);
                id.to_owned()
            }
        };
        // Like ocijail with rctl support: a new jail starts with only the
        // container's own OCI limits, none here.
        let _ = legion_sys::Sys::rctl_remove_jail(self.sys.as_ref(), &jail);
        let c = s.containers.get_mut(id).expect("exists");
        c.running = true;
        c.starts += 1;
        c.jail = Some(jail);
        if c.spec.labels.get(LABEL_ROLE).map(String::as_str) == Some("init") {
            self.die(&mut s, id, init_exit);
        }
        Ok(())
    }

    async fn stop(&self, id: &str, _timeout: Duration) -> Result<()> {
        let mut s = self.state();
        if !s.containers.contains_key(id) {
            return Err(api(404, "no such container"));
        }
        self.die(&mut s, id, 143);
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<()> {
        let mut s = self.state();
        if !s.containers.contains_key(id) {
            return Err(api(404, "no such container"));
        }
        self.die(&mut s, id, 137);
        s.containers.remove(id);
        Ok(())
    }

    async fn inspect(&self, id: &str) -> Result<ContainerInspect> {
        let c = self.get(id)?;
        Ok(ContainerInspect {
            id: id.to_owned(),
            name: c.spec.name.clone(),
            state: ContainerState {
                status: if c.running { "running" } else { "exited" }.into(),
                running: c.running,
                exit_code: c.exit_code,
                ..Default::default()
            },
            config: ContainerConfig {
                labels: c.spec.labels.clone(),
                annotations: c.spec.annotations.clone(),
            },
        })
    }

    async fn wait(&self, id: &str) -> Result<i32> {
        loop {
            let c = self.get(id)?;
            if !c.running {
                return Ok(c.exit_code);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn exec(&self, id: &str, _cmd: &[String]) -> Result<ExecOutput> {
        let mut s = self.state();
        let c = s
            .containers
            .get(id)
            .ok_or_else(|| api(404, "no such container"))?;
        if !c.running {
            return Err(api(500, "container state improper"));
        }
        let code = s.exec.get(&c.spec.name).copied().unwrap_or(0);
        s.execs += 1;
        Ok(ExecOutput {
            exit_code: code,
            stderr: if code == 0 {
                vec![]
            } else {
                b"unhealthy
"
                .to_vec()
            },
            ..Default::default()
        })
    }

    async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<ContainerSummary>> {
        Ok(self
            .state()
            .containers
            .iter()
            .filter(|(_, c)| {
                labels
                    .iter()
                    .all(|(k, v)| c.spec.labels.get(*k).map(String::as_str) == Some(*v))
            })
            .map(|(id, c)| ContainerSummary {
                id: id.clone(),
                names: vec![c.spec.name.clone()],
                image: c.spec.image.clone(),
                labels: c.spec.labels.clone(),
                state: if c.running { "running" } else { "exited" }.into(),
                exit_code: c.exit_code,
                exited: !c.running,
                pid: 0,
            })
            .collect())
    }

    async fn events(&self, labels: &[(&str, &str)]) -> Result<BoxStream<'static, Result<Event>>> {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let rx = self.events.subscribe();
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(e) => return Some((Ok(e), rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .filter(move |e| {
            let keep = match e {
                Ok(e) => labels
                    .iter()
                    .all(|(k, v)| e.actor.attributes.get(k) == Some(v)),
                Err(_) => true,
            };
            async move { keep }
        })
        .boxed())
    }
}
