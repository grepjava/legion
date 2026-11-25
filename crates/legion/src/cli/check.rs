//! `legion replication check` (docs/REPLICATION.md, Conformance): a
//! replication plugin run through its contract on this cluster, in real
//! jails, by Legion itself. The replicated Service of a file (three
//! members, failing over on its own) is applied; once formed, each
//! member's plugin is called directly, inside its container, as its agent
//! calls it: `describe`, `status` twice, `configure` twice and `promote`
//! twice on the primary, and with the plugin's `probe`, a marker written on
//! the primary is read on every replica. The primary is then stopped dead
//! (every process of its container sent SIGSTOP): Legion must fence it,
//! promote a replica, the other must follow, and a marker written just
//! before must be there when the Service has synchronous replicas. Last,
//! the old primary must come back as a replica holding what was written
//! since. What it applied is removed after, unless `--keep`, or a step
//! failed (to look at).

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use legion_api::replication::spi::{self, Describe, Probe, Probed, Role};
use legion_api::replication::{ReplicationStatus, TaskReplication};
use legion_api::service::ServiceSpec;
use legion_api::task::{TaskSpec, TaskState, TaskStatus};
use legion_api::{Kind, Object};
use legion_proto::v1::{ListRequest, ObjectRef};
use tokio::time::Instant;
use tonic::transport::Channel;

use super::{Objects, apply, delete, read_objects};

/// Members the check runs: a primary, and two replicas for a third to
/// follow the one promoted.
const MEMBERS: u32 = 3;

struct Check<'a> {
    objects: &'a mut Objects,
    channel: &'a Channel,
    namespace: String,
    service: String,
    /// The container running the database and the plugin.
    container: String,
    /// Whether the plugin answers `probe`.
    probe: bool,
    /// The Service's synchronous replicas.
    synchronous: u32,
    failover_after: Duration,
    /// Markers written, to tell them apart from a run before.
    run: String,
}

/// A member's running task, as the check calls its plugin.
struct Member {
    task: String,
    ordinal: u32,
    ip: String,
    told: TaskReplication,
}

fn ok(what: &str) {
    println!("ok    {what}");
}

fn skipped(what: &str) {
    println!("skip  {what}");
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub(super) async fn run(
    objects: &mut Objects,
    channel: &Channel,
    file: &Path,
    namespace: Option<String>,
    keep: bool,
) -> anyhow::Result<()> {
    let mut list = read_objects(file)?;
    if let Some(ns) = &namespace {
        for o in list.iter_mut().filter(|o| !o.kind.is_cluster_scoped()) {
            o.metadata.namespace = ns.clone();
        }
    }
    let services: Vec<&Object> = list
        .iter()
        .filter(|o| o.kind == Kind::Service && o.spec.get("replication").is_some())
        .collect();
    let [service] = services.as_slice() else {
        bail!(
            "{} holds {} replicated Services; the check takes one",
            file.display(),
            services.len()
        );
    };
    let spec: ServiceSpec =
        legion_api::parse(&service.spec).map_err(|e| anyhow::anyhow!("{}: {e}", service.key()))?;
    let r = spec.replication.clone().expect("replicated");
    if spec.replicas != Some(MEMBERS) {
        bail!(
            "the check needs replicas: {MEMBERS}, to promote one replica and have the other follow it"
        );
    }
    if r.failover != legion_api::volume::Failover::Automatic {
        bail!("the check needs failover: automatic, as it stops the primary dead");
    }
    let containers: Vec<&str> = spec
        .pod
        .containers
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let container = r
        .container(&containers)
        .context("name the database's container: replication.container")?
        .to_owned();

    println!("applying {}", file.display());
    for o in &list {
        apply(objects, o).await?;
    }
    let mut c = Check {
        objects,
        channel,
        namespace: service.metadata.namespace.clone(),
        service: service.metadata.name.clone(),
        container,
        probe: false,
        synchronous: r.synchronous,
        failover_after: r.failover_after.as_std(),
        run: format!("check{}", now_ms()),
    };
    match c.steps().await {
        Ok(()) => {
            println!("passed: the plugin did all the contract asks");
            if keep {
                println!("kept: {} and what came with it", service.key());
            } else {
                c.clean(&list, &spec).await;
            }
            Ok(())
        }
        Err(e) => {
            eprintln!(
                "kept to look at: {}; remove it with legion delete service {} -n {}",
                service.key(),
                c.service,
                c.namespace
            );
            Err(e)
        }
    }
}

impl Check<'_> {
    async fn steps(&mut self) -> anyhow::Result<()> {
        // 1. describe, on the first member up.
        let first = self
            .wait("member 0 running", Duration::from_secs(900), |c| {
                Box::pin(async move { Ok(c.member(0).await?.map(|_| ())) })
            })
            .await;
        first?;
        let m0 = self.member(0).await?.context("member 0 gone")?;
        let d: Describe = self.call_as(&m0, "describe", None).await?;
        if !d.spi_versions.iter().any(|v| v == spi::VERSION) {
            bail!(
                "describe: the plugin speaks SPI {:?}, not {}",
                d.spi_versions,
                spi::VERSION
            );
        }
        self.probe = d.capabilities.probe;
        let caps = d.capabilities;
        let said: Vec<&str> = [
            (caps.synchronous, "synchronous"),
            (caps.readable_replicas, "readableReplicas"),
            (caps.rewind, "rewind"),
            (caps.seed_from_snapshot, "seedFromSnapshot"),
            (caps.probe, "probe"),
            (caps.archive, "archive"),
            (caps.restore, "restore"),
        ]
        .iter()
        .filter(|(on, _)| *on)
        .map(|(_, n)| *n)
        .collect();
        ok(&format!(
            "describe: {}, SPI {}, capabilities: {}",
            d.database,
            d.spi_versions.join(", "),
            if said.is_empty() {
                "none".to_owned()
            } else {
                said.join(", ")
            }
        ));
        if self.synchronous > 0 && !caps.synchronous {
            bail!("the Service asks for synchronous replicas, which the plugin does not offer");
        }

        // 2. Formed: Legion prepared and started every member.
        self.healthy("formed", Duration::from_secs(1800)).await?;
        let st = self.status().await?;
        let primary = ordinal(&self.service, st.primary.as_deref().unwrap_or_default())
            .context("no primary named")?;
        ok(&format!(
            "formed: {} primary, every member prepared and started, healthy",
            st.primary.as_deref().unwrap_or_default()
        ));

        // 3. status, twice on each, and positions ordered.
        let members = self.members().await?;
        let mut replica_positions = Vec::new();
        for m in members.iter().filter(|m| m.ordinal != primary) {
            let (a, b) = self.status_twice(m).await?;
            if a.role != Role::Replica || b.role != Role::Replica {
                bail!(
                    "status: {}-{} told replica says {:?}",
                    self.service,
                    m.ordinal,
                    a.role
                );
            }
            replica_positions.push((m.ordinal, a.position));
        }
        let p = members
            .iter()
            .find(|m| m.ordinal == primary)
            .context("the primary's task is gone")?;
        let (a, _) = self.status_twice(p).await?;
        if a.role != Role::Primary {
            bail!(
                "status: the primary {}-{primary} says {:?}",
                self.service,
                a.role
            );
        }
        for (i, q) in &replica_positions {
            if let (Some(q), Some(pp)) = (q, a.position)
                && *q > pp
            {
                bail!(
                    "status: replica {}-{i} at {q} is past its primary at {pp}: positions must be comparable",
                    self.service
                );
            }
        }
        ok("status: twice on each member, the same; replicas no further than the primary");

        // 4. Data travels.
        if self.probe {
            let key = format!("{}-a", self.run);
            self.write(p, &key).await?;
            for m in members.iter().filter(|m| m.ordinal != primary) {
                self.seen(m.ordinal, &key, Duration::from_secs(120)).await?;
            }
            ok("probe: a marker written on the primary is read on both replicas");
        } else {
            skipped("data travelling: the plugin does not offer probe");
        }

        // 5. Repeated calls leave things as they were.
        for m in &members {
            let a: serde_json::Value = self.call_as(m, "configure", None).await?;
            let b: serde_json::Value = self.call_as(m, "configure", None).await?;
            if a != b {
                bail!(
                    "configure on {}-{} answered {a} then {b}",
                    self.service,
                    m.ordinal
                );
            }
        }
        let _: serde_json::Value = self.call_as(p, "promote", None).await?;
        let _: serde_json::Value = self.call_as(p, "promote", None).await?;
        self.healthy("still healthy after the repeats", Duration::from_secs(120))
            .await?;
        ok(
            "configure twice on each member and promote twice on the primary: the same, still healthy",
        );

        // 6. The primary stopped dead; a replica promoted.
        let synced = format!("{}-b", self.run);
        if self.probe {
            self.write(p, &synced).await?;
        }
        let old = self.status().await?;
        let (code, _, err) = crate::session::run(
            self.channel,
            &self.target(&p.task),
            vec!["kill".into(), "-STOP".into(), "-1".into()],
            Vec::new(),
        )
        .await?;
        if code != 0 {
            bail!(
                "stopping the primary's processes (kill -STOP -1 in {}): {}",
                p.task,
                String::from_utf8_lossy(&err).trim()
            );
        }
        let stopped = Instant::now();
        println!(
            "      the primary {}-{primary} stopped dead; waiting for Legion to fence it and promote a replica",
            self.service
        );
        let service = self.service.clone();
        let promoted = self
            .wait(
                "a replica promoted",
                self.failover_after + Duration::from_secs(600),
                move |c| {
                    let service = service.clone();
                    Box::pin(async move {
                        let st = c.status().await?;
                        Ok(st
                            .change
                            .is_none()
                            .then_some(())
                            .and(st.last_change.filter(|l| {
                                l.kind == legion_api::replication::ChangeKind::Failover
                                    && l.from.as_deref() == Some(&format!("{service}-{primary}"))
                            }))
                            .and_then(|l| ordinal(&service, &l.to)))
                    })
                },
            )
            .await?;
        let st = self.status().await?;
        if st.epoch <= old.epoch {
            bail!("the epoch did not go up: {} then {}", old.epoch, st.epoch);
        }
        ok(&format!(
            "failover: {}-{promoted} promoted {:.0?} after the primary stopped, epoch {}",
            self.service,
            stopped.elapsed(),
            st.epoch
        ));
        if self.probe {
            let found = self.read(promoted, &synced).await?;
            match (found, self.synchronous > 0) {
                (true, _) => ok("the marker written just before the stop is on the new primary"),
                (false, true) => bail!(
                    "the marker written just before the stop, confirmed by a synchronous replica, is not on the new primary"
                ),
                (false, false) => skipped(
                    "the marker written just before the stop is not on the new primary (asynchronous: it may be lost)",
                ),
            }
            // The other replica follows the new primary.
            let after = format!("{}-c", self.run);
            let np = self
                .member(promoted)
                .await?
                .context("the new primary's task is gone")?;
            self.write(&np, &after).await?;
            let other = (0..MEMBERS)
                .find(|i| *i != promoted && *i != primary)
                .expect("three members");
            self.seen(other, &after, Duration::from_secs(180)).await?;
            ok(&format!(
                "{}-{other} configured to follow the new primary: it has what was written there",
                self.service
            ));
        }

        // 7. The old primary back, as a replica.
        self.healthy(
            "the old primary back as a replica",
            Duration::from_secs(1800),
        )
        .await?;
        let back = self
            .member(primary)
            .await?
            .context("the old primary's task is gone")?;
        let (s, _) = self.status_twice(&back).await?;
        if s.role != Role::Replica {
            bail!("{}-{primary} came back as {:?}", self.service, s.role);
        }
        if self.probe {
            self.seen(
                primary,
                &format!("{}-c", self.run),
                Duration::from_secs(180),
            )
            .await?;
            ok(&format!(
                "{}-{primary} prepared as a replica of the new primary, with what was written since",
                self.service
            ));
        } else {
            ok(&format!(
                "{}-{primary} prepared as a replica of the new primary",
                self.service
            ));
        }
        Ok(())
    }

    fn target(&self, task: &str) -> crate::session::Target {
        crate::session::Target {
            namespace: self.namespace.clone(),
            task: task.to_owned(),
            container: self.container.clone(),
        }
    }

    /// `operation` on member `m`, as its agent calls it, its response read.
    async fn call_as<T: serde::de::DeserializeOwned>(
        &self,
        m: &Member,
        operation: &str,
        probe: Option<Probe>,
    ) -> anyhow::Result<T> {
        let svc = &self.service;
        let me = spi::Member {
            member: format!("{svc}-{}", m.ordinal),
            ordinal: m.ordinal,
            address: m.ip.clone(),
            dns: format!("{svc}-{}.{svc}.{}.legion", m.ordinal, self.namespace),
        };
        let mut req = m
            .told
            .request(operation, &self.namespace, svc, me, now_ms());
        req.probe = probe;
        let (code, out, err) = crate::session::run(
            self.channel,
            &self.target(&m.task),
            m.told.config.argv(operation),
            serde_json::to_vec(&req)?,
        )
        .await
        .with_context(|| format!("{operation} on {svc}-{}", m.ordinal))?;
        let said = String::from_utf8_lossy(&out);
        if code != 0 {
            let why = serde_json::from_str::<spi::Failure>(&said)
                .map(|f| format!("{}: {}", f.error.code, f.error.message))
                .unwrap_or_else(|_| {
                    let e = String::from_utf8_lossy(&err);
                    e.lines().rev().take(5).collect::<Vec<_>>().join(" / ")
                });
            bail!("{operation} on {svc}-{} exited {code}: {why}", m.ordinal);
        }
        let said = if said.trim().is_empty() {
            "{}"
        } else {
            said.trim()
        };
        serde_json::from_str(said)
            .with_context(|| format!("{operation} on {svc}-{} answered {said:?}", m.ordinal))
    }

    async fn status_twice(&self, m: &Member) -> anyhow::Result<(spi::Status, spi::Status)> {
        let a: spi::Status = self.call_as(m, "status", None).await?;
        let b: spi::Status = self.call_as(m, "status", None).await?;
        if a.role != b.role || a.healthy != b.healthy {
            bail!(
                "status on {}-{} said {:?}/{} then {:?}/{}",
                self.service,
                m.ordinal,
                a.role,
                a.healthy,
                b.role,
                b.healthy
            );
        }
        if !a.healthy {
            bail!("status on {}-{}: not healthy", self.service, m.ordinal);
        }
        Ok((a, b))
    }

    async fn write(&self, m: &Member, key: &str) -> anyhow::Result<()> {
        let p: Probed = self
            .call_as(m, "probe", Some(Probe::Write(key.to_owned())))
            .await?;
        if !p.found {
            bail!(
                "probe on {}-{}: the marker {key} was not written",
                self.service,
                m.ordinal
            );
        }
        Ok(())
    }

    async fn read(&mut self, ordinal: u32, key: &str) -> anyhow::Result<bool> {
        let m = self
            .member(ordinal)
            .await?
            .with_context(|| format!("{}-{ordinal} is not running", self.service))?;
        let p: Probed = self
            .call_as(&m, "probe", Some(Probe::Read(key.to_owned())))
            .await?;
        Ok(p.found)
    }

    /// Until member `ordinal` has the marker `key`.
    async fn seen(&mut self, ordinal: u32, key: &str, within: Duration) -> anyhow::Result<()> {
        let key = key.to_owned();
        let what = format!("{}-{ordinal} to have the marker {key}", self.service);
        self.wait(&what, within, move |c| {
            let key = key.clone();
            Box::pin(async move { Ok(c.read(ordinal, &key).await.unwrap_or(false).then_some(())) })
        })
        .await
    }

    /// Until the database is healthy with every member heard from.
    async fn healthy(&mut self, what: &str, within: Duration) -> anyhow::Result<()> {
        self.wait(what, within, |c| {
            Box::pin(async move {
                let st = c.status().await?;
                let all = st.members.len() == MEMBERS as usize
                    && st.members.values().all(|m| m.status.healthy);
                Ok((st.phase == legion_api::replication::Phase::Healthy && all).then_some(()))
            })
        })
        .await
    }

    /// Tries `f` every 2 s until it gives something, for up to `within`;
    /// on running out, says where the database stood.
    async fn wait<T>(
        &mut self,
        what: &str,
        within: Duration,
        mut f: impl for<'c> FnMut(
            &'c mut Self,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<Option<T>>> + 'c>,
        >,
    ) -> anyhow::Result<T> {
        let deadline = Instant::now() + within;
        loop {
            if let Some(v) = f(self).await? {
                return Ok(v);
            }
            if Instant::now() >= deadline {
                let st = self.status().await.unwrap_or_default();
                bail!(
                    "{what}: not within {within:?}; the database is {} ({}), primary {}",
                    st.phase.as_str(),
                    st.message,
                    st.primary.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn status(&mut self) -> anyhow::Result<ReplicationStatus> {
        let o = self
            .objects
            .get(ObjectRef {
                kind: Kind::Service.as_str().into(),
                namespace: self.namespace.clone(),
                name: self.service.clone(),
            })
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
        let o = Object::try_from(o.into_inner())?;
        Ok(o.status
            .get("replication")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default())
    }

    /// Each member's running task told its role.
    async fn members(&mut self) -> anyhow::Result<Vec<Member>> {
        let mut out = Vec::new();
        for i in 0..MEMBERS {
            out.push(
                self.member(i)
                    .await?
                    .with_context(|| format!("{}-{i} is not running", self.service))?,
            );
        }
        Ok(out)
    }

    /// Member `ordinal`'s newest task running, once told its role.
    async fn member(&mut self, ordinal: u32) -> anyhow::Result<Option<Member>> {
        let list = self
            .objects
            .list(ListRequest {
                kind: Kind::Task.to_string(),
                namespace: self.namespace.clone(),
                ..Default::default()
            })
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?
            .into_inner();
        let mut best: Option<(String, Member)> = None;
        for o in list.objects {
            let o = Object::try_from(o)?;
            let Ok(spec) = legion_api::parse::<TaskSpec>(&o.spec) else {
                continue;
            };
            let status: TaskStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
            if spec.service != self.service
                || spec.slot != Some(ordinal)
                || spec.desired_state != legion_api::task::DesiredState::Running
                || status.state != TaskState::Running
            {
                continue;
            }
            let Some(told) = spec.replication else {
                continue;
            };
            let uid = o.metadata.uid.clone();
            if best.as_ref().is_none_or(|(u, _)| *u < uid) {
                best = Some((
                    uid,
                    Member {
                        task: o.metadata.name.clone(),
                        ordinal,
                        ip: status.ip.unwrap_or_default(),
                        told,
                    },
                ));
            }
        }
        Ok(best.map(|(_, m)| m))
    }

    /// Removes what the check applied, and the members' Volumes.
    async fn clean(&mut self, list: &[Object], spec: &ServiceSpec) {
        for o in list.iter().rev().filter(|o| o.kind != Kind::Namespace) {
            if let Err(e) = delete(self.objects, o).await {
                eprintln!("{e:#}");
            }
        }
        // Each member's claim, once its task let go of it.
        let claims: Vec<&str> = spec
            .pod
            .volumes
            .iter()
            .filter(|v| v.claim.is_some())
            .map(|v| v.name.as_str())
            .collect();
        let deadline = Instant::now() + Duration::from_secs(300);
        for claim in claims {
            for i in 0..MEMBERS {
                let mut v = Object::new(
                    Kind::Volume,
                    &self.namespace,
                    &format!("{claim}-{}-{i}", self.service),
                    serde_json::json!({}),
                );
                v.metadata.namespace = self.namespace.clone();
                loop {
                    match delete(self.objects, &v).await {
                        Ok(()) => break,
                        Err(e) if Instant::now() < deadline => {
                            let _ = e;
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                        Err(e) => {
                            eprintln!("{e:#}");
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// The ordinal of member `<service>-<n>`.
fn ordinal(service: &str, member: &str) -> Option<u32> {
    member
        .strip_prefix(service)?
        .strip_prefix('-')?
        .parse()
        .ok()
}
