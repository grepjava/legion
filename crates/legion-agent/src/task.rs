//! One worker per assigned task. It brings the pod up (or adopts the one
//! already running after an agent restart), restarts containers under the
//! pod's restart policy, and tears the pod down when the task ends or is
//! stopped. Every state change is reported to the manager. Running
//! containers are probed (see [`crate::probe`]): a failing liveness probe
//! stops the container, which then restarts under the policy, and readiness
//! decides the task's `ready`.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
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
use crate::probe::{self, Outcome, Prober};
use crate::runtime::Runtime;
use crate::{files, logins, output, volumes};

/// How long a new pod waits for its guard: the policy pass retries a
/// failed load every few seconds.
const GUARD_WAIT: Duration = Duration::from_secs(60);

/// The longest a worker waits before trying again to remove a pod.
const TEARDOWN_RETRY: Duration = Duration::from_secs(30);

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
    pub files_dir: std::path::PathBuf,
    pub logs_dir: Option<std::path::PathBuf>,
    pub zfs_root: Option<String>,
    /// What the node mounts once for all its tasks (iSCSI volumes).
    pub volumes_dir: std::path::PathBuf,
    /// Volume plugins, by name.
    pub plugin_dir: std::path::PathBuf,
    /// Keeps attaching and letting go of volumes apart.
    pub volume_lock: Arc<tokio::sync::Mutex<()>>,
    /// Set once the node has registered, before any worker starts.
    pub net: Arc<OnceLock<PodNetwork>>,
    /// Pod addresses in use, by task uid.
    pub ips: Arc<Mutex<BTreeMap<String, Ipv4Addr>>>,
    /// Holds new pods back until they are guarded.
    pub guarding: Arc<crate::policing::Guarding>,
    /// Containers' memory limits, kept by the agent.
    pub memory: Arc<crate::memory::Limits>,
    /// rctl cannot hold limits here (`kern.racct.enable` is not 1).
    pub no_limits: bool,
    /// What the node registers as its platform, for tasks rejected for it.
    pub platform: legion_api::node::Platform,
    /// How long a ready task told to stop keeps running first.
    pub drain: std::time::Duration,
}

impl<R, C> Ctx<R, C> {
    pub(crate) fn places(&self) -> volumes::Places<'_> {
        volumes::Places {
            node: &self.node,
            zfs_root: self.zfs_root.as_deref(),
            files_dir: &self.files_dir,
            volumes_dir: &self.volumes_dir,
            plugin_dir: &self.plugin_dir,
            lock: &self.volume_lock,
        }
    }
}

impl<R, C> Clone for Ctx<R, C> {
    fn clone(&self) -> Self {
        Ctx {
            runtime: self.runtime.clone(),
            sys: self.sys.clone(),
            control: self.control.clone(),
            node: self.node.clone(),
            infra_image: self.infra_image.clone(),
            files_dir: self.files_dir.clone(),
            logs_dir: self.logs_dir.clone(),
            zfs_root: self.zfs_root.clone(),
            volumes_dir: self.volumes_dir.clone(),
            plugin_dir: self.plugin_dir.clone(),
            volume_lock: self.volume_lock.clone(),
            net: self.net.clone(),
            ips: self.ips.clone(),
            guarding: self.guarding.clone(),
            memory: self.memory.clone(),
            no_limits: self.no_limits,
            platform: self.platform.clone(),
            drain: self.drain,
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
    /// Exit code, and what happened, once it has finished for good.
    done: Option<(i32, String)>,
    /// Counts the container's runs, to tell current probe reports from
    /// stale ones.
    run: u64,
    prober: Option<Prober>,
    /// Passes its readiness probe, or has none.
    ready: bool,
    /// Why a probe had the container stopped.
    unhealthy: Option<String>,
}

impl Member {
    fn new(name: &str, id: &str, running: bool) -> Member {
        Member {
            name: name.to_owned(),
            id: id.to_owned(),
            running,
            attempts: 0,
            restart_at: None,
            done: None,
            run: 0,
            prober: None,
            ready: false,
            unhealthy: None,
        }
    }
}

struct Pod {
    infra: String,
    members: Vec<Member>,
    /// Why the pod may not run on, found while it runs.
    refused: Option<String>,
}

impl Pod {
    /// Every app container runs and is ready.
    fn ready(&self) -> bool {
        self.members.iter().all(|m| m.running && m.ready)
    }
}

/// Why a pod could not be brought up.
enum Abort {
    /// Ends the task as failed.
    Failed(String),
    /// The node cannot run the task at all.
    Rejected(String),
    /// Nor can any of its platform: the images offer none for it.
    WrongPlatform(String),
    /// Stopped while starting.
    Stopped,
    /// The agent shuts down while starting: the pod is left as it is, for
    /// the next agent to remove as incomplete.
    Closing,
}

struct Worker<R, C> {
    ctx: Ctx<R, C>,
    a: Assignment,
    status: TaskStatus,
    rx: mpsc::Receiver<Msg>,
    /// The pod's readiness, reported while running.
    ready: bool,
    /// The manager has not taken the status last reported.
    unsent: bool,
    /// A stop arrived: it is sent once, so one taken while waiting for
    /// something else still counts.
    stopping: bool,
    probe_tx: mpsc::UnboundedSender<probe::Report>,
    probe_rx: mpsc::UnboundedReceiver<probe::Report>,
}

/// How a worker ended: the state it last reported, and that status if the
/// manager has not taken it, for the agent to report again.
pub(crate) struct Ended {
    pub state: TaskState,
    pub unsent: Option<TaskStatus>,
}

pub(crate) async fn run<R: Runtime, C: Control>(
    ctx: Ctx<R, C>,
    a: Assignment,
    rx: mpsc::Receiver<Msg>,
) -> Ended {
    let status = a.status.clone();
    let (probe_tx, probe_rx) = mpsc::unbounded_channel();
    let mut w = Worker {
        ctx,
        a,
        status,
        rx,
        ready: false,
        unsent: false,
        stopping: false,
        probe_tx,
        probe_rx,
    };
    let span = tracing::info_span!("task", task = %w.a.key, uid = %w.a.uid);
    w.main().instrument(span).await;
    // What it last reported.
    Ended {
        state: w.status.state,
        unsent: w.unsent.then(|| w.status.clone()),
    }
}

impl<R: Runtime, C: Control> Worker<R, C> {
    async fn main(&mut self) {
        if self.a.spec.desired_state == DesiredState::Shutdown {
            return self.stop().await;
        }
        if let Some(why) = pod::unsupported(&self.a.spec.pod) {
            if !self.remove_pod().await {
                return;
            }
            return self.finish(TaskState::Rejected, why, None).await;
        }
        // The scheduler places such a pod elsewhere; one assigned before
        // this node said so is not run without its limits.
        if self.ctx.no_limits && self.a.spec.pod.limited() {
            if !self.remove_pod().await {
                return;
            }
            let why = "this node cannot hold the pod's limits: kern.racct.enable is not 1";
            return self.finish(TaskState::Rejected, why.into(), None).await;
        }
        let (pod, adopted) = match self.adopt().await {
            Err(Abort::Stopped) => return self.stop().await,
            Err(_) => return,
            Ok(Some(pod)) => (pod, true),
            Ok(None) if self.stopping => return self.stop().await,
            Ok(None) => match self.start().await {
                Ok(pod) => (pod, false),
                Err(Abort::Stopped) => return self.stop().await,
                Err(Abort::Closing) => return,
                Err(Abort::Failed(why)) => {
                    if !self.remove_pod().await {
                        return;
                    }
                    return self.finish(TaskState::Failed, why, None).await;
                }
                Err(Abort::Rejected(why)) => {
                    if !self.remove_pod().await {
                        return;
                    }
                    return self.finish(TaskState::Rejected, why, None).await;
                }
                Err(Abort::WrongPlatform(why)) => {
                    if !self.remove_pod().await {
                        return;
                    }
                    self.status.wrong_platform = Some(self.ctx.platform.clone());
                    return self.finish(TaskState::Rejected, why, None).await;
                }
            },
        };
        self.monitor(pod, adopted).await;
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
        self.status.ready = state == TaskState::Running && self.ready;
        self.unsent = !report(&self.ctx, &self.a, &self.status).await;
    }

    /// Reports again what the manager did not take, if anything.
    async fn resend(&mut self) {
        if self.unsent {
            self.unsent = !report(&self.ctx, &self.a, &self.status).await;
        }
    }

    /// Tears the pod down, again and again until all of it is gone: only
    /// then may the task be reported ended, for what it held (its volumes
    /// above all) is free once it has. False if the agent shuts down
    /// meanwhile, leaving the rest to the next.
    async fn remove_pod(&mut self) -> bool {
        let mut wait = Duration::from_secs(1);
        loop {
            match teardown(&self.ctx, &self.a.uid, self.grace(), true).await {
                Ok(()) => return true,
                Err(e) => tracing::warn!("removing the pod: {e}; trying again in {wait:?}"),
            }
            if !self.pause(wait).await {
                return false;
            }
            wait = (wait * 2).min(TEARDOWN_RETRY);
        }
    }

    /// Waits before trying again; false if the agent shuts down meanwhile.
    /// What is retried decides the task's end, but a stop is kept for what
    /// comes after.
    async fn pause(&mut self, wait: Duration) -> bool {
        let pause = tokio::time::sleep(wait);
        tokio::pin!(pause);
        loop {
            tokio::select! {
                () = &mut pause => return true,
                msg = self.rx.recv() => match msg {
                    None => return false,
                    Some(Msg::Stop) => self.stopping = true,
                    Some(_) => {}
                },
            }
        }
    }

    async fn finish(&mut self, state: TaskState, message: String, exit_code: Option<i32>) {
        tracing::info!(state = state.as_str(), "{message}");
        self.status.exit_code = exit_code;
        // Its jail is gone, or soon will be.
        self.status.jid = None;
        self.report(state, message).await;
    }

    /// Before a ready task of a service stops, other nodes learn that it is
    /// leaving their balancing (it left the endpoints as it was told to
    /// stop) at their own pace; until they have, they still send it new
    /// connections, which a pod already gone would leave hanging. So it
    /// runs on for a moment, serving them.
    async fn drain(&self) {
        if self.status.ready && !self.a.spec.job && !self.ctx.drain.is_zero() {
            tracing::info!(drain = ?self.ctx.drain, "draining before the stop");
            tokio::time::sleep(self.ctx.drain).await;
        }
    }

    async fn stop(&mut self) {
        if !self.remove_pod().await {
            return;
        }
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
                self.stopping = true;
            }
        }
        self.stopping
    }

    /// Resolves once a stop arrives, or the agent shuts down; other
    /// messages are dropped, as in [`Self::stop_requested`].
    async fn stopped(&mut self) -> Abort {
        if self.stopping {
            return Abort::Stopped;
        }
        loop {
            match self.rx.recv().await {
                Some(Msg::Stop) => return Abort::Stopped,
                Some(_) => {}
                None => return Abort::Closing,
            }
        }
    }

    /// Takes over a pod left running by a previous agent. A pod that is
    /// incomplete (a container missing, or created but never started: the
    /// agent went between the two), whose infra container has stopped, or
    /// whose address is not known to be its own (the agent went before
    /// reporting it) is removed instead, and started again from scratch.
    /// Errs only as stopped, or closing.
    async fn adopt(&mut self) -> Result<Option<Pod>, Abort> {
        // Not knowing is no reason to start over: a pod still running would
        // be in the way of the new one, and go with its failure.
        let mut wait = Duration::from_secs(1);
        let found = loop {
            match self.ctx.runtime.list(&[(LABEL_TASK, &self.a.uid)]).await {
                Ok(f) => break f,
                Err(e) => tracing::warn!("listing containers: {e}; trying again in {wait:?}"),
            }
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                abort = self.stopped() => return Err(abort),
            }
            wait = (wait * 2).min(TEARDOWN_RETRY);
        };
        if found.is_empty() {
            // A start the agent before went during, ahead of the first
            // container, leaves the task's files and volumes: they go
            // first, or the new start would find them as another's.
            if !files::task_dir(&self.ctx.files_dir, &self.a.uid).exists() {
                return Ok(None);
            }
            tracing::info!("removing what a start left by a previous run");
            if !self.remove_pod().await {
                return Err(Abort::Closing);
            }
            return Ok(None);
        }
        // Restarts are counted for the pod, not each container: those of
        // its one container, then.
        let restarts = match self.pod().containers.len() {
            1 => self.status.restarts,
            _ => 0,
        };
        let role = |c: &crate::runtime::ContainerSummary| {
            c.labels.get(LABEL_ROLE).and_then(|r| Role::parse(r))
        };
        let infra = found
            .iter()
            .find(|c| role(c) == Some(Role::Infra) && c.state == "running");
        let mut members = Vec::new();
        if let Some(infra) = infra {
            for spec in &self.pod().containers {
                let Some(c) = found.iter().find(|c| {
                    role(c) == Some(Role::App)
                        && c.labels.get(LABEL_CONTAINER) == Some(&spec.name)
                        && (c.state == "running" || c.exited)
                }) else {
                    break;
                };
                let mut m = Member::new(&spec.name, &c.id, c.state == "running");
                m.attempts = restarts;
                // Its exit was counted, and its restart due, as the agent
                // before went: it is restarted, not counted again.
                if !m.running && restart_pending(&self.status.message, &spec.name) {
                    let delay = self.pod().restart_policy.delay.as_std();
                    m.restart_at = Some(Instant::now() + delay);
                }
                members.push(m);
            }
            if members.len() == self.pod().containers.len() {
                match self.attach(&infra.id, true).await {
                    Ok(()) => {
                        tracing::info!("adopted running pod");
                        return Ok(Some(Pod {
                            infra: infra.id.clone(),
                            members,
                            refused: None,
                        }));
                    }
                    Err(Abort::Failed(why) | Abort::Rejected(why) | Abort::WrongPlatform(why)) => {
                        tracing::warn!("adopting the pod's network: {why}");
                    }
                    Err(Abort::Stopped) => self.stopping = true,
                    Err(Abort::Closing) => return Err(Abort::Closing),
                }
            }
        }
        tracing::info!("removing an incomplete pod left by a previous run");
        if !self.remove_pod().await {
            return Err(Abort::Closing);
        }
        // Its jail is gone; its address is taken again if still free.
        self.status.jid = None;
        Ok(None)
    }

    async fn start(&mut self) -> Result<Pod, Abort> {
        let rt = self.ctx.runtime.clone();
        self.report(TaskState::Preparing, "pulling images").await;
        let images: BTreeSet<String> = std::iter::once(self.ctx.infra_image.clone())
            .chain(self.pod().containers().map(|c| c.image.clone()))
            .collect();
        let namespace = self.a.key.namespace.clone();
        // Read when an image needs pulling: a private image another
        // namespace pulled is pulled again, so the registry checks this
        // namespace's access.
        let mut logins = None;
        for image in &images {
            let usable = rt
                .image_usable(image, &namespace)
                .await
                .map_err(|e| Abort::Failed(format!("checking image {image}: {e}")))?;
            if !usable {
                if logins.is_none() {
                    let read = logins::read(self.ctx.control.as_ref(), &self.a)
                        .await
                        .map_err(Abort::Failed)?;
                    logins = Some(read);
                }
                tracing::info!(%image, "pulling");
                rt.pull(image, &namespace, logins.as_deref().unwrap_or_default())
                    .await
                    .map_err(|e| match e {
                        legion_runtime::Error::Platform { .. } => {
                            Abort::WrongPlatform(e.to_string())
                        }
                        e => Abort::Failed(format!("pulling {image}: {e}")),
                    })?;
            }
            if self.stop_requested() {
                return Err(Abort::Stopped);
            }
        }

        let resolv_conf = self
            .ctx
            .net
            .get()
            .map(|n| legion_net::dns::pod_resolv_conf(&self.a.key.namespace, n.gateway()));
        let files = files::prepare(
            self.ctx.control.as_ref(),
            &self.ctx.sys,
            &self.ctx.files_dir,
            &self.a,
            resolv_conf,
        )
        .await
        .map_err(Abort::Failed)?;
        // After the files: their tmpfs would hide mount points made before.
        let sources = volumes::prepare(
            self.ctx.control.as_ref(),
            &self.ctx.sys,
            &self.ctx.places(),
            &self.a,
        )
        .await
        .map_err(|e| match e {
            volumes::Error::Failed(m) => Abort::Failed(m),
            volumes::Error::Rejected(m) => Abort::Rejected(m),
        })?;

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
        self.attach(&infra, false).await?;
        self.guarded().await?;

        let init: Vec<_> = self.pod().init_containers.clone();
        for (i, c) in init.iter().enumerate() {
            let step = format!("init container {} ({}/{})", c.name, i + 1, init.len());
            self.report(TaskState::Starting, step).await;
            let mut spec = pod::container_spec(&self.ctx.node, &self.a, c, Role::Init, &infra);
            spec.mounts.extend(volumes::mounts(c, &sources));
            files.apply(&c.name, &mut spec);
            let id = rt
                .create(&spec)
                .await
                .map_err(|e| Abort::Failed(format!("creating init container {}: {e}", c.name)))?;
            rt.start(&id)
                .await
                .map_err(|e| Abort::Failed(format!("starting init container {}: {e}", c.name)))?;
            self.limit(c, &infra, &id).await.map_err(Abort::Failed)?;
            self.check_jail(c, &infra, &id, false)
                .await
                .map_err(Abort::Rejected)?;
            // It may run for long, or never end: a stop must not wait for it.
            let code = tokio::select! {
                code = rt.wait(&id) => code.map_err(|e| {
                    Abort::Failed(format!("waiting for init container {}: {e}", c.name))
                })?,
                abort = self.stopped() => return Err(abort),
            };
            let oom = self.ctx.memory.killed(&pod::child_jail(&infra, &id));
            if code != 0 {
                // The pod and its output go with the failure: keep the end.
                let tail = last_output(rt.as_ref(), &id).await;
                let why = oom.map(|why| format!(": {why};")).unwrap_or_default();
                return Err(Abort::Failed(format!(
                    "init container {}{why} exited with {code}{tail}",
                    c.name
                )));
            }
        }

        self.report(TaskState::Starting, "starting containers")
            .await;

        let mut members = Vec::new();
        let apps: Vec<_> = self.pod().containers.clone();
        for c in &apps {
            let mut spec = pod::container_spec(&self.ctx.node, &self.a, c, Role::App, &infra);
            spec.mounts.extend(volumes::mounts(c, &sources));
            files.apply(&c.name, &mut spec);
            let id = rt
                .create(&spec)
                .await
                .map_err(|e| Abort::Failed(format!("creating container {}: {e}", c.name)))?;
            rt.start(&id)
                .await
                .map_err(|e| Abort::Failed(format!("starting container {}: {e}", c.name)))?;
            self.limit(c, &infra, &id).await.map_err(Abort::Failed)?;
            self.check_jail(c, &infra, &id, false)
                .await
                .map_err(Abort::Rejected)?;
            let mut m = Member::new(&c.name, &id, true);
            self.probe(&mut m);
            members.push(m);
        }
        let pod = Pod {
            infra,
            members,
            refused: None,
        };
        self.ready = pod.ready();
        self.report(TaskState::Running, "").await;
        Ok(pod)
    }

    /// Gives the pod its address: the one it had (an adopted pod, or a task
    /// whose status carries one) or the lowest free one. An adopted pod
    /// (`adopting`) must have had one: its jail holds it already.
    async fn attach(&mut self, infra: &str, adopting: bool) -> Result<(), Abort> {
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
                None if adopting => {
                    return Err(Abort::Failed(
                        "its address is not known, or is another pod's".into(),
                    ));
                }
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
        self.status.jid = match self.ctx.runtime.inspect(infra).await {
            Ok(i) if i.state.jid != 0 => Some(i.state.jid),
            _ => None,
        };
        self.ctx
            .guarding
            .attach(&self.a.uid, crate::policing::LocalPod::of(&self.a, ip));
        Ok(())
    }

    /// Waits until the pod's host end is guarded at its address, before
    /// anything in the pod runs: until then pf lets anyone reach it, and it
    /// may send from any address. A pod not guarded in time fails.
    async fn guarded(&mut self) -> Result<(), Abort> {
        let Some(ip) = self.status.ip.as_deref().and_then(|s| s.parse().ok()) else {
            return Err(Abort::Failed("the pod has no address".into()));
        };
        self.report(TaskState::Starting, "loading the pod's network policy")
            .await;
        let uid = self.a.uid.clone();
        let mut passes = self.ctx.guarding.passes();
        let wait = async {
            passes
                .wait_for(|p| p.guarded.get(&uid) == Some(&ip))
                .await
                .is_ok()
        };
        let guarded = tokio::select! {
            r = tokio::time::timeout(GUARD_WAIT, wait) => r.unwrap_or(false),
            abort = self.stopped() => return Err(abort),
        };
        if guarded {
            return Ok(());
        }
        let why = match passes.borrow().error.clone() {
            Some(e) => format!(": {e}"),
            None => " (are the network policies known?)".into(),
        };
        Err(Abort::Failed(format!(
            "the pod's network policy was not loaded within {}s{why}",
            GUARD_WAIT.as_secs()
        )))
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
    /// a runtime may clear a jail's rules when it creates the jail, and
    /// Legion's rules must hold for every run. Adding a rule
    /// again replaces it, so repeating this is harmless. A container whose
    /// limits could not be set must not run without them: the error is
    /// the caller's to fail or refuse the pod with.
    async fn limit(
        &self,
        c: &legion_api::service::Container,
        infra: &str,
        id: &str,
    ) -> Result<(), String> {
        let jail = pod::child_jail(infra, id);
        self.ctx
            .memory
            .started(&jail, c.resources.limits.memory.map(|m| m.0));
        let rules = pod::rctl_rules(c, &jail);
        if rules.is_empty() {
            return Ok(());
        }
        blocking(&self.ctx.sys, move |sys| {
            rules.iter().try_for_each(|r| sys.rctl_add(r))
        })
        .await
        .map_err(|e| format!("setting container {}'s limits: {e}", c.name))
    }

    /// Checks that a container's jail has its profile (DESIGN.md §9), which
    /// the runtime sets when it creates the jail. A jail without it is
    /// refused. With `fix`, for an adopted pod, the running jail is
    /// tightened instead. A jail
    /// already gone has nothing left to check.
    async fn check_jail(
        &self,
        c: &legion_api::service::Container,
        infra: &str,
        id: &str,
        fix: bool,
    ) -> Result<(), String> {
        let want = c.security.jail_params();
        let jail = pod::child_jail(infra, id);
        let name = c.name.clone();
        blocking(&self.ctx.sys, move |sys| {
            let names = want.each_ref().map(|(k, _)| *k);
            let got = match sys.jail_get(&jail, &names) {
                Ok(got) => got,
                Err(legion_sys::Error::NotFound(_)) => return Ok(()),
                Err(e) => return Err(format!("reading container {name}'s jail: {e}")),
            };
            let Some((k, v)) = want.iter().find(|(k, v)| got.get(*k) != Some(v)) else {
                return Ok(());
            };
            if fix {
                let params: Vec<(&str, &str)> =
                    want.iter().map(|(k, v)| (*k, v.as_str())).collect();
                tracing::info!(container = %name, "setting the jail profile on an adopted container");
                return sys
                    .jail_set(&jail, &params)
                    .map_err(|e| format!("setting container {name}'s jail profile: {e}"));
            }
            Err(format!(
                "container {name}'s jail has {k}={}, not {v}: Legion's jail hook did not run, so the jail is not locked down",
                got.get(*k).map_or("?", String::as_str)
            ))
        })
        .await
    }

    /// Starts probing a container run that has just begun. Until its
    /// readiness probe passes, the container is not ready.
    fn probe(&self, m: &mut Member) {
        let Some(c) = self.pod().containers.iter().find(|c| c.name == m.name) else {
            return;
        };
        m.run += 1;
        // Ready once started, with no readiness probe to pass.
        m.ready = c.probes.readiness.is_none() && c.probes.startup.is_none();
        m.unhealthy = None;
        let ip = self
            .status
            .ip
            .as_deref()
            .and_then(|s| s.parse::<IpAddr>().ok());
        m.prober = probe::start(
            &self.ctx.runtime,
            c,
            self.pod(),
            ip,
            &m.id,
            m.run,
            self.probe_tx.clone(),
        );
    }

    /// Reports the pod's readiness if it changed.
    async fn update_ready(&mut self, pod: &Pod) {
        self.ready = pod.ready();
        if self.status.state == TaskState::Running && self.status.ready != self.ready {
            let message = self.status.message.clone();
            self.report(TaskState::Running, message).await;
        }
    }

    async fn probed(&mut self, pod: &mut Pod, r: probe::Report) {
        let Some(m) = pod
            .members
            .iter_mut()
            .find(|m| m.id == r.id && m.run == r.run && m.running)
        else {
            return;
        };
        match r.outcome {
            Outcome::Started => {
                tracing::info!(container = %m.name, "started");
                let gated = self
                    .pod()
                    .containers
                    .iter()
                    .any(|c| c.name == m.name && c.probes.readiness.is_some());
                if !gated {
                    m.ready = true;
                    self.update_ready(pod).await;
                }
            }
            Outcome::Ready(ready) => {
                tracing::info!(container = %m.name, ready, "readiness changed");
                m.ready = ready;
                self.update_ready(pod).await;
            }
            Outcome::Unhealthy(why) => {
                tracing::info!(container = %m.name, "{why}; stopping it");
                m.prober = None;
                m.ready = false;
                m.unhealthy = Some(why);
                let id = m.id.clone();
                if let Err(e) = self.ctx.runtime.stop(&id, self.grace()).await {
                    tracing::warn!("stopping an unhealthy container: {e}");
                }
                self.update_ready(pod).await;
            }
        }
    }

    async fn monitor(&mut self, mut pod: Pod, adopted: bool) {
        // Limits are reapplied for adopted containers, in case they were
        // lost (rules of a jail removed by hand, a host reboot), and their
        // probes start over; a pod just started has had both.
        for i in 0..pod.members.len() {
            if !adopted || !pod.members[i].running {
                continue;
            }
            let (name, id) = (pod.members[i].name.clone(), pod.members[i].id.clone());
            if let Some(c) = self
                .pod()
                .containers
                .iter()
                .find(|c| c.name == name)
                .cloned()
            {
                let checked = match self.limit(&c, &pod.infra, &id).await {
                    Ok(()) => self.check_jail(&c, &pod.infra, &id, true).await,
                    Err(why) => Err(why),
                };
                if let Err(why) = checked {
                    pod.refused = Some(why);
                }
            }
            self.probe(&mut pod.members[i]);
        }
        // A refused pod is torn down below, never reported running, nor
        // its stopped containers restarted.
        if pod.refused.is_none() {
            // Containers found stopped on adoption count as exits now.
            for i in 0..pod.members.len() {
                if !pod.members[i].running
                    && pod.members[i].done.is_none()
                    && pod.members[i].restart_at.is_none()
                {
                    let code = self.exit_code(&pod.members[i].id).await;
                    self.exited(&mut pod, i, code).await;
                }
            }
            // The agent before may have gone before saying it runs.
            self.ready = pod.ready();
            let said = self.status.state == TaskState::Running;
            // Its jail's id too, which an agent before this one did not
            // report.
            let jid = self.status.jid != self.a.status.jid;
            if finished(&pod).is_none() && (!said || self.status.ready != self.ready || jid) {
                let message = if said {
                    self.status.message.clone()
                } else {
                    String::new()
                };
                self.report(TaskState::Running, message).await;
            }
        }
        loop {
            if self.stopping {
                self.drain().await;
                return self.stop().await;
            }
            if let Some(why) = pod.refused.take() {
                if !self.remove_pod().await {
                    return;
                }
                return self.finish(TaskState::Rejected, why, None).await;
            }
            if let Some((state, message, code)) = finished(&pod) {
                if !self.remove_pod().await {
                    return;
                }
                return self.finish(state, message, code).await;
            }
            let next_restart = pod.members.iter().filter_map(|m| m.restart_at).min();
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    // The agent is shutting down; the pod keeps running.
                    None => return,
                    Some(Msg::Stop) => {
                        self.drain().await;
                        return self.stop().await;
                    }
                    Some(Msg::Died { id, .. }) if id == pod.infra => {
                        if !self.remove_pod().await {
                            return;
                        }
                        return self
                            .finish(TaskState::Failed, "the infra container exited".into(), None)
                            .await;
                    }
                    Some(Msg::Died { id, code }) => {
                        // A resync may have seen the exit first and the
                        // container been restarted since: the event is
                        // then about the run before, and acting on it
                        // would restart the new run.
                        if let Some(i) = pod.members.iter().position(|m| m.id == id && m.running)
                            && !self.is_running(&id).await
                        {
                            // The new run may have ended as well, and the
                            // code is the one it ended with: the runtime's,
                            // not the event's, unless the container is gone.
                            let code = match self.ctx.runtime.inspect(&id).await {
                                Ok(c) => c.state.exit_code,
                                Err(_) => code.unwrap_or(-1),
                            };
                            self.exited(&mut pod, i, code).await;
                        }
                    }
                    Some(Msg::Resync) => {
                        if !self.is_running(&pod.infra).await {
                            if !self.remove_pod().await {
                                return;
                            }
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
                        self.resend().await;
                    }
                },
                Some(r) = self.probe_rx.recv() => self.probed(&mut pod, r).await,
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
        m.prober = None;
        let oom = self.ctx.memory.killed(&pod::child_jail(&pod.infra, &m.id));
        let what = match (m.unhealthy.take(), oom) {
            (Some(why), _) => format!("container {}: {why}", m.name),
            (None, Some(why)) => format!("container {}: {why}; exited with {code}", m.name),
            (None, None) => format!("container {} exited with {code}", m.name),
        };
        let wants = match policy.condition {
            RestartCondition::Any => true,
            RestartCondition::OnFailure => code != 0,
            RestartCondition::None => false,
        };
        let allowed = policy.max_attempts == 0 || m.attempts < policy.max_attempts;
        if wants && allowed {
            m.attempts += 1;
            m.restart_at = Some(Instant::now() + policy.delay.as_std());
            let message = format!("{what}{RESTARTING}{}", policy.delay);
            tracing::info!("{message}");
            self.status.restarts += 1;
            self.ready = pod.ready();
            self.report(TaskState::Running, message).await;
        } else {
            tracing::info!(code, "container finished: {what}");
            m.done = Some((code, what));
            self.update_ready(pod).await;
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
                    let checked = match self.limit(&c, &pod.infra, &id).await {
                        Ok(()) => self.check_jail(&c, &pod.infra, &id, false).await,
                        Err(why) => Err(why),
                    };
                    if let Err(why) = checked {
                        // Torn down as the monitor's next turn finds it,
                        // never reported running again.
                        pod.refused = Some(why);
                        return;
                    }
                }
                self.probe(&mut pod.members[i]);
                self.ready = pod.ready();
                self.report(TaskState::Running, "").await;
            }
            Err(e) => {
                let message = format!("restarting container {}: {e}", m.name);
                m.done = Some((-1, message.clone()));
                tracing::warn!("{message}");
                if finished(pod).is_none() {
                    // The task runs on without it (other containers run or
                    // wait to restart): the manager learns why.
                    self.ready = pod.ready();
                    self.report(TaskState::Running, message).await;
                } else {
                    self.status.message = message;
                }
            }
        }
    }

    async fn is_running(&self, id: &str) -> bool {
        match self.ctx.runtime.inspect(id).await {
            Ok(c) => c.state.running,
            Err(e) if e.is_not_found() => false,
            // The runtime unreachable: assume nothing changed until it answers.
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
/// The last lines a container wrote, as `": <line>; <line>"`, short enough
/// for a status message; empty if there are none.
async fn last_output<R: Runtime>(rt: &R, id: &str) -> String {
    const LINES: usize = 5;
    const MAX: usize = 400;
    let Ok(chunks) = rt.logs(id, false, Some(LINES)).await else {
        return String::new();
    };
    let chunks: Vec<_> = futures::StreamExt::collect(chunks).await;
    let text: String = chunks
        .into_iter()
        .filter_map(Result::ok)
        .map(|c| String::from_utf8_lossy(&c.data).into_owned())
        .collect();
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    let mut out = lines[lines.len().saturating_sub(LINES)..].join("; ");
    if out.len() > MAX {
        let mut at = out.len() - MAX;
        while !out.is_char_boundary(at) {
            at += 1;
        }
        out = format!("...{}", &out[at..]);
    }
    if out.is_empty() {
        out
    } else {
        format!(": {out}")
    }
}

/// What a status message says of a container whose restart is due.
const RESTARTING: &str = "; restarting in ";

/// The status reported says the container `name` exited and is to be
/// restarted: that exit is counted.
fn restart_pending(message: &str, name: &str) -> bool {
    let named = message
        .strip_prefix("container ")
        .and_then(|m| m.strip_prefix(name))
        .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with(':'));
    named && message.contains(RESTARTING)
}

fn finished(pod: &Pod) -> Option<(TaskState, String, Option<i32>)> {
    let done: Vec<&(i32, String)> = pod
        .members
        .iter()
        .map(|m| m.done.as_ref())
        .collect::<Option<_>>()?;
    match done.into_iter().find(|(c, _)| *c != 0) {
        None => Some((
            TaskState::Complete,
            "all containers exited successfully".into(),
            Some(0),
        )),
        Some((code, what)) => Some((TaskState::Failed, what.clone(), Some(*code))),
    }
}

async fn sleep_until(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// Removes every container of the task and their rctl rules, keeping their
/// output first if `keep` (see [`crate::output`]), then lets go of its
/// volumes and files. An error says what is left: what the pod mounts is
/// let go of only once no container is, and its address (and guard) only
/// once its network is gone. Callers try again.
pub(crate) async fn teardown<R: Runtime, C: Control>(
    ctx: &Ctx<R, C>,
    uid: &str,
    grace: Duration,
    keep: bool,
) -> Result<(), String> {
    let found = ctx
        .runtime
        .list(&[(LABEL_TASK, uid)])
        .await
        .map_err(|e| format!("listing containers: {e}"))?;
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
    if let Some(root) = ctx.logs_dir.as_ref().filter(|_| keep) {
        let containers: Vec<(String, String)> = found
            .iter()
            .filter(|c| Some(&c.id) != infra.as_ref())
            .filter_map(|c| Some((c.labels.get(LABEL_CONTAINER)?.clone(), c.id.clone())))
            .collect();
        output::save(ctx.runtime.as_ref(), root, uid, &containers).await;
    }
    if let Some(infra) = &infra {
        if let Err(e) = ctx.runtime.stop(infra, Duration::from_secs(1)).await {
            tracing::warn!("stopping the infra container: {e}");
        }
        jails.push(infra.clone());
    }
    // The network goes once the containers have stopped, which they may
    // need it for; destroying the epair's host end removes both ends, even
    // while the other is still in a jail. Its address is free only then.
    let mut network = Ok(());
    if let Some(net) = ctx.net.get().cloned() {
        let id = uid.to_owned();
        network = blocking(&ctx.sys, move |sys| net.detach(sys, &id))
            .await
            .map_err(|e| format!("removing the pod network: {e}"));
    }
    if network.is_ok() {
        ctx.ips
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(uid);
        ctx.guarding.detach(uid);
    }
    let mut left = Vec::new();
    for c in &found {
        if let Err(e) = ctx.runtime.remove(&c.id).await
            && !e.is_not_found()
        {
            left.push(format!("{}: {e}", c.name()));
        }
    }
    if !left.is_empty() {
        return Err(format!("removing containers: {}", left.join("; ")));
    }
    // Watched until gone: a container a failed stop left running keeps
    // its limit.
    if let Some(infra) = &infra {
        ctx.memory.removed(infra);
    }
    let result = blocking(&ctx.sys, move |sys| {
        jails.iter().try_for_each(|j| sys.rctl_remove_jail(j))
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(uid, "removing limits: {e}");
    }
    // Volumes first: what they record is in the task's directory.
    volumes::release(ctx.control.as_ref(), &ctx.sys, &ctx.places(), uid)
        .await
        .map_err(|e| format!("letting go of volumes: {e}"))?;
    let (root, id) = (ctx.files_dir.clone(), uid.to_owned());
    blocking(&ctx.sys, move |sys| files::release(sys, &root, &id))
        .await
        .map_err(|e| format!("removing secrets and configs: {e}"))?;
    network
}

/// Retries reports the manager cannot take right now for a short while;
/// false if it still has not, for a resync to report again.
pub(crate) async fn report<R, C: Control>(
    ctx: &Ctx<R, C>,
    a: &Assignment,
    status: &TaskStatus,
) -> bool {
    for attempt in 0..10 {
        match ctx.control.report(&a.key, &a.uid, status).await {
            Ok(()) => return true,
            Err(ControlError::Unavailable(e)) if attempt < 9 => {
                tracing::debug!("reporting status: {e}; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                tracing::warn!(task = %a.key, "reporting status: {e}");
                return false;
            }
        }
    }
    false
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
