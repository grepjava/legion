//! One worker per assigned task. It brings the pod up (or adopts the one
//! already running after an agent restart), restarts containers under the
//! pod's restart policy, and tears the pod down when the task ends or is
//! stopped. Every state change is reported to the manager.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use legion_api::service::{PodSpec, RestartCondition};
use legion_api::task::{DesiredState, TaskState, TaskStatus};
use legion_net::PodNetwork;
use legion_sys::Sys;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Instrument;

use crate::control::{Assignment, Control, ControlError};
use crate::pod::{self, LABEL_CONTAINER, LABEL_ROLE, LABEL_TASK, Role};
use crate::runtime::Runtime;

/// What the agent tells a worker.
#[derive(Debug)]
pub(crate) enum Msg {
    /// A container of this task died.
    Died { id: String, code: Option<i32> },
    /// Check the containers against what the worker believes; events may
    /// have been missed.
    Resync,
    /// The task is no longer wanted here: tear the pod down.
    Stop,
}

pub(crate) struct Ctx<R, C> {
    pub runtime: Arc<R>,
    pub sys: Arc<dyn Sys>,
    pub control: Arc<C>,
    pub node: String,
    pub infra_image: String,
    /// Set once the node has registered, before any worker starts.
    pub net: Arc<OnceLock<PodNetwork>>,
    /// Pod addresses in use, by task uid.
    pub ips: Arc<Mutex<BTreeMap<String, Ipv4Addr>>>,
}

impl<R, C> Clone for Ctx<R, C> {
    fn clone(&self) -> Self {
        Ctx {
            runtime: self.runtime.clone(),
            sys: self.sys.clone(),
            control: self.control.clone(),
            node: self.node.clone(),
            infra_image: self.infra_image.clone(),
            net: self.net.clone(),
            ips: self.ips.clone(),
        }
    }
}

/// An app container of a running pod.
#[derive(Debug)]
struct Member {
    name: String,
    id: String,
    running: bool,
    /// Restarts so far, for `maxAttempts`.
    attempts: u32,
    restart_at: Option<Instant>,
    /// Exit code once it has finished for good.
    done: Option<i32>,
}

struct Pod {
    infra: String,
    members: Vec<Member>,
}

/// Why a pod could not be brought up.
enum Abort {
    /// Ends the task as failed.
    Failed(String),
    /// The node cannot run the task at all.
    Rejected(String),
    /// Stopped while starting.
    Stopped,
}

struct Worker<R, C> {
    ctx: Ctx<R, C>,
    a: Assignment,
    status: TaskStatus,
    rx: mpsc::Receiver<Msg>,
}

pub(crate) async fn run<R: Runtime, C: Control>(
    ctx: Ctx<R, C>,
    a: Assignment,
    rx: mpsc::Receiver<Msg>,
) {
    let status = a.status.clone();
    let mut w = Worker { ctx, a, status, rx };
    let span = tracing::info_span!("task", task = %w.a.key, uid = %w.a.uid);
    w.main().instrument(span).await;
}

impl<R: Runtime, C: Control> Worker<R, C> {
    async fn main(&mut self) {
        if self.a.spec.desired_state == DesiredState::Shutdown {
            return self.stop().await;
        }
        if let Some(why) = pod::unsupported(&self.a.spec.pod) {
            teardown(&self.ctx, &self.a.uid, self.grace()).await;
            return self.finish(TaskState::Rejected, why, None).await;
        }
        let pod = match self.adopt().await {
            Some(pod) => pod,
            None => match self.start().await {
                Ok(pod) => pod,
                Err(Abort::Stopped) => return self.stop().await,
                Err(Abort::Failed(why)) => {
                    teardown(&self.ctx, &self.a.uid, self.grace()).await;
                    return self.finish(TaskState::Failed, why, None).await;
                }
                Err(Abort::Rejected(why)) => {
                    teardown(&self.ctx, &self.a.uid, self.grace()).await;
                    return self.finish(TaskState::Rejected, why, None).await;
                }
            },
        };
        self.monitor(pod).await;
    }

    fn grace(&self) -> Duration {
        self.a.spec.pod.stop_grace_period.as_std()
    }

    fn pod(&self) -> &PodSpec {
        &self.a.spec.pod
    }

    async fn report(&mut self, state: TaskState, message: impl Into<String>) {
        self.status.state = state;
        self.status.message = message.into();
        self.status.ready = state == TaskState::Running
            && self
                .pod()
                .containers
                .iter()
                .all(|c| c.probes.readiness.is_none());
        report(&self.ctx, &self.a, &self.status).await;
    }

    async fn finish(&mut self, state: TaskState, message: String, exit_code: Option<i32>) {
        tracing::info!(state = state.as_str(), "{message}");
        self.status.exit_code = exit_code;
        self.report(state, message).await;
    }

    async fn stop(&mut self) {
        teardown(&self.ctx, &self.a.uid, self.grace()).await;
        if !self.status.state.is_terminal() {
            self.finish(TaskState::Shutdown, "stopped".into(), None)
                .await;
        }
    }

    /// True if a stop arrived. Other messages are irrelevant while starting:
    /// the containers are checked once the pod is up.
    fn stop_requested(&mut self) -> bool {
        while let Ok(msg) = self.rx.try_recv() {
            if matches!(msg, Msg::Stop) {
                return true;
            }
        }
        false
    }

    /// Takes over a pod left running by a previous agent. A pod that is
    /// incomplete or whose infra container has stopped is removed instead,
    /// and started again from scratch.
    async fn adopt(&mut self) -> Option<Pod> {
        let found = match self.ctx.runtime.list(&[(LABEL_TASK, &self.a.uid)]).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("listing containers: {e}");
                return None;
            }
        };
        if found.is_empty() {
            return None;
        }
        let role = |c: &legion_podman::ContainerSummary| {
            c.labels.get(LABEL_ROLE).and_then(|r| Role::parse(r))
        };
        let infra = found
            .iter()
            .find(|c| role(c) == Some(Role::Infra) && c.state == "running");
        let mut members = Vec::new();
        if let Some(infra) = infra {
            for spec in &self.pod().containers {
                let Some(c) = found.iter().find(|c| {
                    role(c) == Some(Role::App) && c.labels.get(LABEL_CONTAINER) == Some(&spec.name)
                }) else {
                    break;
                };
                members.push(Member {
                    name: spec.name.clone(),
                    id: c.id.clone(),
                    running: c.state == "running",
                    attempts: 0,
                    restart_at: None,
                    done: None,
                });
            }
            if members.len() == self.pod().containers.len() {
                tracing::info!("adopted running pod");
                if let Err(e) = self.attach(&infra.id).await {
                    let why = match e {
                        Abort::Failed(m) | Abort::Rejected(m) => m,
                        Abort::Stopped => "stopped".into(),
                    };
                    tracing::warn!("adopted pod's network: {why}");
                }
                return Some(Pod {
                    infra: infra.id.clone(),
                    members,
                });
            }
        }
        tracing::info!("removing an incomplete pod left by a previous run");
        teardown(&self.ctx, &self.a.uid, self.grace()).await;
        None
    }

    async fn start(&mut self) -> Result<Pod, Abort> {
        let rt = self.ctx.runtime.clone();
        self.report(TaskState::Preparing, "pulling images").await;
        let images: BTreeSet<String> = std::iter::once(self.ctx.infra_image.clone())
            .chain(self.pod().containers().map(|c| c.image.clone()))
            .collect();
        for image in &images {
            let present = rt
                .image_exists(image)
                .await
                .map_err(|e| Abort::Failed(format!("checking image {image}: {e}")))?;
            if !present {
                tracing::info!(%image, "pulling");
                rt.pull(image)
                    .await
                    .map_err(|e| Abort::Failed(format!("pulling {image}: {e}")))?;
            }
            if self.stop_requested() {
                return Err(Abort::Stopped);
            }
        }

        let infra = rt
            .create(&pod::infra_spec(
                &self.ctx.node,
                &self.a,
                &self.ctx.infra_image,
            ))
            .await
            .map_err(|e| Abort::Failed(format!("creating the infra container: {e}")))?;
        rt.start(&infra)
            .await
            .map_err(|e| Abort::Failed(format!("starting the infra container: {e}")))?;
        self.check_infra_jail(&infra).await?;
        self.attach(&infra).await?;

        self.report(TaskState::Starting, "starting containers")
            .await;
        let init: Vec<_> = self.pod().init_containers.clone();
        for c in &init {
            let id = rt
                .create(&pod::container_spec(
                    &self.ctx.node,
                    &self.a,
                    c,
                    Role::Init,
                    &infra,
                ))
                .await
                .map_err(|e| Abort::Failed(format!("creating init container {}: {e}", c.name)))?;
            rt.start(&id)
                .await
                .map_err(|e| Abort::Failed(format!("starting init container {}: {e}", c.name)))?;
            self.limit(c, &infra, &id).await;
            let code = rt.wait(&id).await.map_err(|e| {
                Abort::Failed(format!("waiting for init container {}: {e}", c.name))
            })?;
            if code != 0 {
                return Err(Abort::Failed(format!(
                    "init container {} exited with {code}",
                    c.name
                )));
            }
            if self.stop_requested() {
                return Err(Abort::Stopped);
            }
        }

        let mut members = Vec::new();
        let apps: Vec<_> = self.pod().containers.clone();
        for c in &apps {
            let id = rt
                .create(&pod::container_spec(
                    &self.ctx.node,
                    &self.a,
                    c,
                    Role::App,
                    &infra,
                ))
                .await
                .map_err(|e| Abort::Failed(format!("creating container {}: {e}", c.name)))?;
            rt.start(&id)
                .await
                .map_err(|e| Abort::Failed(format!("starting container {}: {e}", c.name)))?;
            self.limit(c, &infra, &id).await;
            members.push(Member {
                name: c.name.clone(),
                id,
                running: true,
                attempts: 0,
                restart_at: None,
                done: None,
            });
        }
        self.report(TaskState::Running, "").await;
        Ok(Pod { infra, members })
    }

    /// Gives the pod its address: the one it had (an adopted pod, or a task
    /// whose status carries one) or the lowest free one.
    async fn attach(&mut self, infra: &str) -> Result<(), Abort> {
        let net = self
            .ctx
            .net
            .get()
            .expect("network set at registration")
            .clone();
        let ip = {
            let mut ips = self.ctx.ips.lock().unwrap_or_else(|e| e.into_inner());
            let own = self
                .status
                .ip
                .as_deref()
                .and_then(|s| s.parse::<Ipv4Addr>().ok())
                .filter(|ip| {
                    net.contains(*ip)
                        && !ips
                            .iter()
                            .any(|(uid, other)| other == ip && *uid != self.a.uid)
                });
            let ip = match own {
                Some(ip) => ip,
                None => {
                    let used: BTreeSet<Ipv4Addr> = ips.values().copied().collect();
                    net.allocate(&used).ok_or_else(|| {
                        Abort::Rejected(format!("no free address in {}", net.subnet))
                    })?
                }
            };
            ips.insert(self.a.uid.clone(), ip);
            ip
        };
        let (uid, jail) = (self.a.uid.clone(), infra.to_owned());
        blocking(&self.ctx.sys, move |sys| net.attach(sys, &uid, &jail, ip))
            .await
            .map_err(|e| Abort::Failed(format!("attaching the pod network: {e}")))?;
        self.status.ip = Some(ip.to_string());
        Ok(())
    }

    /// The infra jail must have its own VNET: without the annotation taking
    /// effect, `--network none` means the host's network (DESIGN.md §6.2).
    async fn check_infra_jail(&self, infra: &str) -> Result<(), Abort> {
        let needed = (self.pod().containers.len() + self.pod().init_containers.len()) as u32;
        let id = infra.to_owned();
        let check = move |sys: &dyn Sys| -> Result<(), Abort> {
            let jails = sys
                .jails()
                .map_err(|e| Abort::Failed(format!("listing jails: {e}")))?;
            let jail = jails.iter().find(|j| j.name == id).ok_or_else(|| {
                Abort::Rejected(format!("the infra container's jail {id} is not running"))
            })?;
            if jail.vnet != "new" {
                return Err(Abort::Rejected(format!(
                    "the infra jail has vnet={}, not its own network stack; refusing to run \
                     the pod on the host's network",
                    jail.vnet
                )));
            }
            if jail.children_max < needed {
                sys.jail_set(&id, &[("children.max", &needed.to_string())])
                    .map_err(|e| Abort::Failed(format!("allowing child jails: {e}")))?;
            }
            Ok(())
        };
        blocking(&self.ctx.sys, check).await
    }

    /// Sets rctl limits on a container's jail. This runs after every start:
    /// an OCI runtime may clear a jail's rules when it creates the jail
    /// (ocijail with rctl support replaces them with the container's OCI
    /// limits), and Legion's rules must hold for every run. Adding a rule
    /// again replaces it, so repeating this is harmless.
    async fn limit(&self, c: &legion_api::service::Container, infra: &str, id: &str) {
        let rules = pod::rctl_rules(c, &pod::child_jail(infra, id));
        if rules.is_empty() {
            return;
        }
        let result = blocking(&self.ctx.sys, move |sys| {
            rules.iter().try_for_each(|r| sys.rctl_add(r))
        })
        .await;
        if let Err(e) = result {
            tracing::warn!(container = %c.name, "setting limits: {e}");
        }
    }

    async fn monitor(&mut self, mut pod: Pod) {
        // Limits are reapplied for adopted containers, in case they were
        // lost (rules of a jail removed by hand, a host reboot).
        for m in pod.members.iter().filter(|m| m.running) {
            if let Some(c) = self.pod().containers.iter().find(|c| c.name == m.name) {
                self.limit(c, &pod.infra, &m.id).await;
            }
        }
        // Containers found stopped on adoption count as exits now.
        for i in 0..pod.members.len() {
            if !pod.members[i].running && pod.members[i].done.is_none() {
                let code = self.exit_code(&pod.members[i].id).await;
                self.exited(&mut pod, i, code).await;
            }
        }
        loop {
            if let Some((state, message, code)) = finished(&pod) {
                teardown(&self.ctx, &self.a.uid, self.grace()).await;
                return self.finish(state, message, code).await;
            }
            let next_restart = pod.members.iter().filter_map(|m| m.restart_at).min();
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    // The agent is shutting down; the pod keeps running.
                    None => return,
                    Some(Msg::Stop) => return self.stop().await,
                    Some(Msg::Died { id, .. }) if id == pod.infra => {
                        teardown(&self.ctx, &self.a.uid, self.grace()).await;
                        return self
                            .finish(TaskState::Failed, "the infra container exited".into(), None)
                            .await;
                    }
                    Some(Msg::Died { id, code }) => {
                        if let Some(i) = pod.members.iter().position(|m| m.id == id && m.running) {
                            let code = match code {
                                Some(c) => c,
                                None => self.exit_code(&id).await,
                            };
                            self.exited(&mut pod, i, code).await;
                        }
                    }
                    Some(Msg::Resync) => {
                        if !self.is_running(&pod.infra).await {
                            teardown(&self.ctx, &self.a.uid, self.grace()).await;
                            return self
                                .finish(TaskState::Failed, "the infra container exited".into(), None)
                                .await;
                        }
                        for i in 0..pod.members.len() {
                            if pod.members[i].running && !self.is_running(&pod.members[i].id).await {
                                let code = self.exit_code(&pod.members[i].id).await;
                                self.exited(&mut pod, i, code).await;
                            }
                        }
                    }
                },
                _ = sleep_until(next_restart) => {
                    let now = Instant::now();
                    for i in 0..pod.members.len() {
                        if pod.members[i].restart_at.is_some_and(|t| t <= now) {
                            self.restart(&mut pod, i).await;
                        }
                    }
                }
            }
        }
    }

    /// Applies the restart policy to a container that exited.
    async fn exited(&mut self, pod: &mut Pod, i: usize, code: i32) {
        let policy = self.pod().restart_policy.clone();
        let m = &mut pod.members[i];
        m.running = false;
        let wants = match policy.condition {
            RestartCondition::Any => true,
            RestartCondition::OnFailure => code != 0,
            RestartCondition::None => false,
        };
        let allowed = policy.max_attempts == 0 || m.attempts < policy.max_attempts;
        if wants && allowed {
            m.attempts += 1;
            m.restart_at = Some(Instant::now() + policy.delay.as_std());
            let message = format!(
                "container {} exited with {code}; restarting in {}",
                m.name, policy.delay
            );
            tracing::info!("{message}");
            self.status.restarts += 1;
            self.report(TaskState::Running, message).await;
        } else {
            tracing::info!(container = %m.name, code, "container finished");
            m.done = Some(code);
        }
    }

    async fn restart(&mut self, pod: &mut Pod, i: usize) {
        let m = &mut pod.members[i];
        m.restart_at = None;
        match self.ctx.runtime.start(&m.id).await {
            Ok(()) => {
                m.running = true;
                let (name, id) = (m.name.clone(), m.id.clone());
                if let Some(c) = self
                    .pod()
                    .containers
                    .iter()
                    .find(|c| c.name == name)
                    .cloned()
                {
                    self.limit(&c, &pod.infra, &id).await;
                }
                self.report(TaskState::Running, "").await;
            }
            Err(e) => {
                let message = format!("restarting container {}: {e}", m.name);
                m.done = Some(-1);
                tracing::warn!("{message}");
                self.status.message = message;
            }
        }
    }

    async fn is_running(&self, id: &str) -> bool {
        match self.ctx.runtime.inspect(id).await {
            Ok(c) => c.state.running,
            Err(e) if e.is_not_found() => false,
            // Podman unreachable: assume nothing changed until it answers.
            Err(_) => true,
        }
    }

    async fn exit_code(&self, id: &str) -> i32 {
        match self.ctx.runtime.inspect(id).await {
            Ok(c) => c.state.exit_code,
            Err(_) => -1,
        }
    }
}

/// The task's end state once every app container has finished for good.
fn finished(pod: &Pod) -> Option<(TaskState, String, Option<i32>)> {
    let codes: Vec<(&str, i32)> = pod
        .members
        .iter()
        .map(|m| m.done.map(|c| (m.name.as_str(), c)))
        .collect::<Option<_>>()?;
    match codes.iter().find(|(_, c)| *c != 0) {
        None => Some((
            TaskState::Complete,
            "all containers exited successfully".into(),
            Some(0),
        )),
        Some((name, code)) => Some((
            TaskState::Failed,
            format!("container {name} exited with {code}"),
            Some(*code),
        )),
    }
}

async fn sleep_until(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// Removes every container of the task and their rctl rules. Failures are
/// logged; the next reconcile retries whatever is left.
pub(crate) async fn teardown<R: Runtime, C: Control>(ctx: &Ctx<R, C>, uid: &str, grace: Duration) {
    // The epair goes first: destroying its host end removes both ends, even
    // while the other is still in the jail.
    if let Some(net) = ctx.net.get().cloned() {
        let id = uid.to_owned();
        if let Err(e) = blocking(&ctx.sys, move |sys| net.detach(sys, &id)).await {
            tracing::warn!(uid, "removing the pod network: {e}");
        }
    }
    ctx.ips
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(uid);
    let found = match ctx.runtime.list(&[(LABEL_TASK, uid)]).await {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(uid, "listing containers for teardown: {e}");
            return;
        }
    };
    let infra = found
        .iter()
        .find(|c| c.labels.get(LABEL_ROLE).map(String::as_str) == Some(Role::Infra.as_str()))
        .map(|c| c.id.clone());
    let mut jails = Vec::new();
    for c in found.iter().filter(|c| Some(&c.id) != infra.as_ref()) {
        if c.state == "running"
            && let Err(e) = ctx.runtime.stop(&c.id, grace).await
        {
            tracing::warn!(container = %c.name(), "stopping: {e}");
        }
        if let Some(infra) = &infra {
            jails.push(pod::child_jail(infra, &c.id));
        }
    }
    if let Some(infra) = &infra {
        if let Err(e) = ctx.runtime.stop(infra, Duration::from_secs(1)).await {
            tracing::warn!("stopping the infra container: {e}");
        }
        jails.push(infra.clone());
    }
    for c in &found {
        if let Err(e) = ctx.runtime.remove(&c.id).await
            && !e.is_not_found()
        {
            tracing::warn!(container = %c.name(), "removing: {e}");
        }
    }
    let result = blocking(&ctx.sys, move |sys| {
        jails.iter().try_for_each(|j| sys.rctl_remove_jail(j))
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(uid, "removing limits: {e}");
    }
}

/// Retries reports the manager cannot take right now for a short while;
/// the next state change or resync reports again anyway.
async fn report<R, C: Control>(ctx: &Ctx<R, C>, a: &Assignment, status: &TaskStatus) {
    for attempt in 0..10 {
        match ctx.control.report(&a.key, &a.uid, status).await {
            Ok(()) => return,
            Err(ControlError::Unavailable(e)) if attempt < 9 => {
                tracing::debug!("reporting status: {e}; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                tracing::warn!(task = %a.key, "reporting status: {e}");
                return;
            }
        }
    }
}

/// Runs a [`Sys`] call on the blocking pool.
pub(crate) async fn blocking<T, F>(sys: &Arc<dyn Sys>, f: F) -> T
where
    F: FnOnce(&dyn Sys) -> T + Send + 'static,
    T: Send + 'static,
{
    let sys = sys.clone();
    match tokio::task::spawn_blocking(move || f(sys.as_ref())).await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("blocking system call cancelled: {e}"),
    }
}
