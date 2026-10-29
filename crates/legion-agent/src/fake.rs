//! An in-memory [`Runtime`] for tests. It behaves like the jail runtime
//! where the agent depends on it: starting a container creates its jail in
//! a [`FakeSys`] (with a VNET when the infra annotation asks for one, as a
//! child of the infra jail when `parentJail` is set), a stopped container's
//! jail goes away, stopping the infra jail kills the pod, and every death
//! is reported as a `died` event carrying the container's labels. To keep
//! the agent honest it is less kind than the jail runtime: starting a
//! container clears its jail's rctl rules (an OCI runtime may), and a
//! child jail gets loose parameters (raw sockets, chflags,
//! `enforce_statfs=1`), then those of the `legion.jail` annotation, unless
//! [`FakeRuntime::skip_jail_hook`]. Exec runs nothing: each container answers with the exit code
//! set by [`FakeRuntime::set_exec`], 0 by default. An attached exec of
//! `cat` echoes its stdin; any other command prints itself and the
//! terminal size, if it has one. Logs are what [`FakeRuntime::log`] wrote.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::runtime::{
    Actor, Attached, Chunk, ContainerConfig, ContainerInspect, ContainerSpec, ContainerState,
    ContainerSummary, Error, Event, ExecOutput, Output, Result,
};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use legion_sys::FakeSys;
use tokio::sync::broadcast;

use crate::pod::{ANNOTATION_JAIL, ANNOTATION_PARENT, ANNOTATION_VNET, LABEL_ROLE, child_jail};
use crate::runtime::{Login, Runtime};

pub struct FakeRuntime {
    sys: Arc<FakeSys>,
    state: Arc<Mutex<State>>,
    events: broadcast::Sender<Event>,
}

#[derive(Default)]
struct State {
    images: BTreeSet<String>,
    /// Pulls of these fail.
    unpullable: BTreeSet<String>,
    /// Images pulled only with this account (username, password), and who
    /// pulled them.
    private: BTreeMap<String, ((String, String), BTreeSet<String>)>,
    next_id: u64,
    containers: BTreeMap<String, Container>,
    /// Ignore the VNET annotation, as a runtime without support would.
    ignore_vnet: bool,
    /// Ignore the `legion.jail` annotation, as a runtime that does not set
    /// a jail's parameters would.
    no_hook: bool,
    /// Exit code of init containers, which exit as soon as they start.
    init_exit_code: i32,
    /// Init containers run until stopped instead.
    hold_init: bool,
    created: usize,
    /// Exit code of exec in containers, by container name.
    exec: BTreeMap<String, i32>,
    execs: usize,
    /// Output so far, by container name.
    logs: BTreeMap<String, Vec<Chunk>>,
    /// Attached execs: their exit code once done, and terminal size.
    sessions: BTreeMap<String, Session>,
    /// Events held back, while holding them, as a slow event stream would.
    held: Option<Vec<Event>>,
    /// The network policy anchor's rules as each container (by name) last
    /// started.
    policy_at_start: BTreeMap<String, String>,
}

/// An attached exec's view of its session.
struct SessionHandle {
    runtime: Arc<Mutex<State>>,
    exec_id: String,
}

impl SessionHandle {
    fn with<T>(&self, f: impl FnOnce(&mut Session) -> T) -> Option<T> {
        let mut s = self.runtime.lock().unwrap_or_else(|e| e.into_inner());
        s.sessions.get_mut(&self.exec_id).map(f)
    }

    fn size(&self) -> Option<(u16, u16)> {
        self.with(|s| s.size).flatten()
    }

    fn exited(&self, code: i32) {
        self.with(|s| s.exit_code = Some(code));
    }
}

#[derive(Debug, Default)]
struct Session {
    exit_code: Option<i32>,
    size: Option<(u16, u16)>,
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
    let message = message.into();
    match status {
        404 => Error::NotFound(message),
        409 => Error::WrongState(message),
        _ => Error::Failed(message),
    }
}

impl FakeRuntime {
    pub fn new(sys: Arc<FakeSys>) -> Self {
        FakeRuntime {
            sys,
            state: Default::default(),
            events: broadcast::channel(1024).0,
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn state_handle(&self) -> Arc<Mutex<State>> {
        self.state.clone()
    }

    pub fn add_image(&self, reference: &str) {
        self.state().images.insert(reference.to_owned());
    }

    /// `reference` pulls only with this account.
    pub fn private_image(&self, reference: &str, username: &str, password: &str) {
        self.state().private.insert(
            reference.to_owned(),
            ((username.to_owned(), password.to_owned()), BTreeSet::new()),
        );
    }

    pub fn fail_pulls_of(&self, reference: &str) {
        self.state().unpullable.insert(reference.to_owned());
    }

    pub fn ignore_vnet(&self) {
        self.state().ignore_vnet = true;
    }

    pub fn skip_jail_hook(&self) {
        self.state().no_hook = true;
    }

    pub fn set_init_exit_code(&self, code: i32) {
        self.state().init_exit_code = code;
    }

    /// Holds back events from now on, as a slow event stream would; they
    /// go out when `hold` is false again.
    pub fn hold_events(&self, hold: bool) {
        let held = {
            let mut s = self.state();
            if hold {
                s.held.get_or_insert_with(Vec::new);
                return;
            }
            s.held.take().unwrap_or_default()
        };
        for e in held {
            let _ = self.events.send(e);
        }
    }

    /// Init containers started from now on run until stopped.
    pub fn hold_init(&self, hold: bool) {
        self.state().hold_init = hold;
    }

    /// Exec in the container `name` exits with `code` from now on.
    pub fn set_exec(&self, name: &str, code: i32) {
        self.state().exec.insert(name.to_owned(), code);
    }

    /// Execs run so far.
    pub fn execs(&self) -> usize {
        self.state().execs
    }

    /// The container `name` writes `data`.
    pub fn log(&self, name: &str, stream: Output, data: &str) {
        self.state()
            .logs
            .entry(name.to_owned())
            .or_default()
            .push(Chunk {
                stream,
                data: Bytes::from(data.to_owned()),
            });
    }

    /// Attached execs still running.
    /// Exec sessions the runtime still keeps, ended or not.
    pub fn exec_sessions(&self) -> usize {
        self.state().sessions.len()
    }

    pub fn attached(&self) -> usize {
        self.state()
            .sessions
            .values()
            .filter(|s| s.exit_code.is_none())
            .count()
    }

    /// The `legion/policy` anchor's rules when the container `name` last
    /// started, for checking that a pod was guarded before it ran.
    pub fn policy_at_start(&self, name: &str) -> Option<String> {
        self.state().policy_at_start.get(name).cloned()
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
        let event = Event {
            kind: "container".into(),
            action: "died".into(),
            actor: Actor {
                id: id.to_owned(),
                attributes,
            },
            time_nano: 0,
        };
        match s.held.as_mut() {
            Some(held) => held.push(event),
            None => {
                let _ = self.events.send(event);
            }
        }
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
    async fn image_usable(&self, reference: &str, user: &str) -> Result<bool> {
        let s = self.state();
        Ok(s.images.contains(reference)
            && s.private
                .get(reference)
                .is_none_or(|(_, users)| users.contains(user)))
    }

    async fn pull(&self, reference: &str, user: &str, logins: &[Login]) -> Result<()> {
        let mut s = self.state();
        let denied = || Error::Pull {
            image: reference.to_owned(),
            message: "requested access to the resource is denied".into(),
        };
        if s.unpullable.contains(reference) {
            return Err(denied());
        }
        if let Some(((username, password), users)) = s.private.get_mut(reference) {
            if !logins
                .iter()
                .any(|l| l.username == *username && l.password == *password)
            {
                return Err(denied());
            }
            users.insert(user.to_owned());
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
        let init_exit = (!s.hold_init).then_some(s.init_exit_code);
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
                let mut params = vec![
                    ("allow.raw_sockets", "true"),
                    ("allow.chflags", "true"),
                    ("enforce_statfs", "1"),
                    ("securelevel", "-1"),
                ];
                if !s.no_hook
                    && let Some(hooked) = c.spec.annotations.get(ANNOTATION_JAIL)
                {
                    params.extend(hooked.split(' ').filter_map(|p| p.split_once('=')));
                }
                legion_sys::Sys::jail_set(self.sys.as_ref(), &name, &params)
                    .expect("the jail exists");
                name
            }
            None => {
                let vnet =
                    c.spec.annotations.get(ANNOTATION_VNET).map(String::as_str) == Some("new");
                self.sys.add_jail(id, vnet && !ignore_vnet);
                id.to_owned()
            }
        };
        // As an OCI runtime may: a new jail starts with only the container's
        // own OCI limits, none here.
        let _ = legion_sys::Sys::rctl_remove_jail(self.sys.as_ref(), &jail);
        let policy = self
            .sys
            .anchor(legion_net::policy::ANCHOR)
            .unwrap_or_default();
        s.policy_at_start.insert(c.spec.name.clone(), policy);
        let c = s.containers.get_mut(id).expect("exists");
        c.running = true;
        c.starts += 1;
        c.jail = Some(jail);
        if c.spec.labels.get(LABEL_ROLE).map(String::as_str) == Some("init")
            && let Some(code) = init_exit
        {
            self.die(&mut s, id, code);
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

    async fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        _tty: bool,
    ) -> Result<Attached> {
        use tokio::io::AsyncReadExt;
        let (exec_id, code) = {
            let mut s = self.state();
            let c = s
                .containers
                .get(id)
                .ok_or_else(|| api(404, "no such container"))?;
            if !c.running {
                return Err(api(500, "container state improper"));
            }
            let code = s.exec.get(&c.spec.name).copied().unwrap_or(0);
            s.next_id += 1;
            let exec_id = format!("exec{}", s.next_id);
            s.sessions.insert(exec_id.clone(), Session::default());
            (exec_id, code)
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Chunk>>(16);
        let (input, mut process_stdin) = tokio::io::duplex(1024);
        let chunk = |stream, data: Vec<u8>| Chunk {
            stream,
            data: Bytes::from(data),
        };
        let cat = cmd == ["cat"];
        let line = cmd.join(" ");
        let sessions = SessionHandle {
            runtime: self.state_handle(),
            exec_id: exec_id.clone(),
        };
        tokio::spawn(async move {
            let mut code = code;
            if cat {
                let mut buf = [0u8; 1024];
                loop {
                    match process_stdin.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if tx
                                .send(Ok(chunk(Output::Stdout, buf[..n].to_vec())))
                                .await
                                .is_err()
                            {
                                code = 141;
                                break;
                            }
                        }
                    }
                }
            } else {
                // Gives the caller a moment to size the terminal.
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut out = format!("ran {line}\n");
                if let Some((w, h)) = sessions.size() {
                    out.push_str(&format!("{h} {w}\n"));
                }
                let _ = tx.send(Ok(chunk(Output::Stdout, out.into_bytes()))).await;
                if code != 0 {
                    let _ = tx
                        .send(Ok(chunk(Output::Stderr, b"failed\n".to_vec())))
                        .await;
                }
            }
            sessions.exited(code);
        });
        Ok(Attached {
            id: exec_id,
            output: futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|c| (c, rx))
            })
            .boxed(),
            stdin: stdin
                .then(|| Box::pin(input) as std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>),
        })
    }

    async fn exec_resize(&self, exec_id: &str, width: u16, height: u16) -> Result<()> {
        let mut s = self.state();
        let session = s
            .sessions
            .get_mut(exec_id)
            .ok_or_else(|| api(404, "no such exec session"))?;
        session.size = Some((width, height));
        Ok(())
    }

    async fn exec_exit_code(&self, exec_id: &str) -> Result<i32> {
        for _ in 0..200 {
            let done = {
                let mut s = self.state();
                match s.sessions.get(exec_id) {
                    None => Some(Err(api(404, "no such exec session"))),
                    Some(Session {
                        exit_code: Some(code),
                        ..
                    }) => {
                        let code = *code;
                        s.sessions.remove(exec_id);
                        Some(Ok(code))
                    }
                    Some(_) => None,
                }
            };
            if let Some(done) = done {
                return done;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err(Error::Failed("exec still running".into()))
    }

    async fn exec_release(&self, exec_id: &str) {
        self.state().sessions.remove(exec_id);
    }

    async fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<BoxStream<'static, Result<Chunk>>> {
        let name = self.get(id)?.spec.name;
        let mut written = self.state().logs.get(&name).cloned().unwrap_or_default();
        if let Some(n) = tail {
            written = written.split_off(written.len().saturating_sub(n));
        }
        let past = futures::stream::iter(written.into_iter().map(Ok));
        if !follow {
            return Ok(past.boxed());
        }
        // Then what comes later, until the container stops.
        let state = self.state_handle();
        let id = id.to_owned();
        let from = self.state().logs.get(&name).map_or(0, Vec::len);
        let later = futures::stream::unfold(from, move |at| {
            let state = state.clone();
            let id = id.clone();
            let name = name.clone();
            async move {
                loop {
                    {
                        let s = state.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(c) = s.logs.get(&name).and_then(|l| l.get(at)) {
                            return Some((Ok(c.clone()), at + 1));
                        }
                        if !s.containers.get(&id).is_some_and(|c| c.running) {
                            return None;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });
        Ok(past.chain(later).boxed())
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
