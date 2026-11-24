//! A task that is a member of a replicated database (DESIGN.md §6.9): its
//! plugin's `prepare` before the database's container starts, then every
//! 5 s its `status`, and `promote` and `configure` as the task's spec
//! names its role and the topology. What it says goes to the leader with
//! the node's heartbeats ([`super::Members`]).

use legion_api::replication::spi::{self, Role};
use legion_api::replication::{MemberReport, PLUGIN_MOUNT, TaskReplication};
use legion_api::service::Container;
use legion_runtime::{Mount, Output};

use super::{Abort, Pod, Runtime, TaskState, Worker};
use crate::control::Control;
use crate::pod::{self, Role as PodRole};
use crate::{files, volumes};

/// How often a running member is asked where it stands.
pub(super) const EVERY: std::time::Duration = std::time::Duration::from_secs(5);
/// How long a `prepare` that must wait (`retry`) waits before trying again.
const RETRY: std::time::Duration = std::time::Duration::from_secs(5);

/// What a plugin call came to.
#[derive(Debug)]
pub(super) enum Said {
    Ok(serde_json::Value),
    /// A code the contract names, or `failed`, and the message.
    Failed(String, String),
}

/// A call's response from its exit code and output.
fn said(code: i32, stdout: &str, stderr: &str) -> Said {
    let last = stdout
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("");
    if code == 0 {
        return match serde_json::from_str(last) {
            Ok(v) => Said::Ok(v),
            Err(_) if last.is_empty() => Said::Ok(serde_json::Value::Object(Default::default())),
            Err(e) => Said::Failed("failed".into(), format!("its answer is not JSON: {e}")),
        };
    }
    if let Ok(f) = serde_json::from_str::<spi::Failure>(last) {
        return Said::Failed(f.error.code, f.error.message);
    }
    let tail: Vec<&str> = stderr.trim().lines().collect();
    let message = tail[tail.len().saturating_sub(3)..].join(" / ");
    Said::Failed(
        "failed".into(),
        if message.is_empty() {
            format!("exited with {code}")
        } else {
            message
        },
    )
}

/// What the worker keeps of its member.
#[derive(Default)]
pub(super) struct State {
    /// The epoch and topology last applied (`configure`).
    pub applied: Option<(u64, spi::Topology)>,
    /// When it is next asked.
    pub due: Option<tokio::time::Instant>,
}

impl<R: Runtime, C: Control> Worker<R, C> {
    fn told(&self) -> Option<&TaskReplication> {
        self.a.spec.replication.as_ref()
    }

    /// The container the plugin runs in, if this task is a member.
    pub(super) fn member_container(&self) -> Option<String> {
        let r = self.told()?;
        let names: Vec<&str> = self
            .pod()
            .containers
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        r.config.container(&names).map(str::to_owned)
    }

    /// This member, as the plugin is told of it.
    fn me(&self) -> spi::Member {
        let svc = &self.a.spec.service;
        let ordinal = self.a.spec.slot.unwrap_or(0);
        spi::Member {
            member: format!("{svc}-{ordinal}"),
            ordinal,
            address: self.status.ip.clone().unwrap_or_default(),
            dns: format!("{svc}-{ordinal}.{svc}.{}", self.a.key.namespace),
        }
    }

    fn request(&self, r: &TaskReplication, operation: &str, reclone: bool) -> Vec<u8> {
        let now = chrono_ms();
        let mut req = r.request(
            operation,
            &self.a.key.namespace,
            &self.a.spec.service,
            self.me(),
            now,
        );
        req.reclone = reclone;
        serde_json::to_vec(&req).expect("serialises")
    }

    /// The node's plugins, mounted read-only where the plugin's command
    /// finds them, for a member whose plugin is the node's.
    pub(super) fn plugin_mount(&self) -> Result<Option<Mount>, Abort> {
        let Some(r) = self.told() else {
            return Ok(None);
        };
        let Some(name) = &r.config.plugin else {
            return Ok(None);
        };
        let dir = &self.ctx.replication_dir;
        if !dir.join(name).is_file() {
            return Err(Abort::Failed(format!(
                "replication plugin {name} is not on this node ({})",
                dir.display()
            )));
        }
        Ok(Some(Mount::nullfs(
            &dir.display().to_string(),
            PLUGIN_MOUNT,
            true,
        )))
    }

    /// Runs `prepare` for container `c` before it starts, in a container of
    /// its own made as `c` is (its image, user, environment, secrets and
    /// mounts), until it succeeds: waiting while the plugin says to, once
    /// copying anew a member whose data diverged if the Service allows it.
    pub(super) async fn prepare_member(
        &mut self,
        c: &Container,
        infra: &str,
        sources: &volumes::Sources,
        files: &files::Files,
    ) -> Result<(), Abort> {
        let Some(r) = self.told().cloned() else {
            return Ok(());
        };
        let rt = self.ctx.runtime.clone();
        let mut reclone = false;
        loop {
            let role = self.told().map_or(r.role, |r| r.role);
            self.report(
                TaskState::Starting,
                format!("replication: preparing as {}", role.as_str()),
            )
            .await;
            let told = self.told().cloned().unwrap_or_else(|| r.clone());
            let mut hook = c.clone();
            hook.name = format!("{}.prepare", c.name);
            let mut spec =
                pod::container_spec(&self.ctx.node, &self.a, &hook, PodRole::Init, infra);
            // Its own name, the member's labels: found as the member's.
            spec.entrypoint = told.config.argv("prepare");
            spec.command = Vec::new();
            self.fence(&mut spec);
            spec.mounts.extend(volumes::mounts(c, sources));
            files.apply(&c.name, &mut spec);
            spec.mounts.extend(self.plugin_mount()?);
            let id = rt
                .create(&spec)
                .await
                .map_err(|e| Abort::Failed(format!("creating the replication prepare: {e}")))?;
            let input = self.request(&told, "prepare", reclone);
            let ran = async {
                rt.set_input(&id, &input).await.map_err(|e| e.to_string())?;
                self.limit(c, infra, &id).await?;
                rt.start(&id).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(())
            }
            .await;
            let outcome = match ran {
                Err(e) => Err(Abort::Failed(format!(
                    "starting the replication prepare: {e}"
                ))),
                Ok(()) => {
                    let deadline = spi::deadline("prepare");
                    tokio::select! {
                        code = tokio::time::timeout(deadline, rt.wait(&id)) => match code {
                            Ok(Ok(code)) => {
                                let (out, err) = output_of(rt.as_ref(), &id).await;
                                Ok(said(code, &out, &err))
                            }
                            Ok(Err(e)) => Err(Abort::Failed(format!("waiting for the replication prepare: {e}"))),
                            Err(_) => Ok(Said::Failed("failed".into(), "prepare did not finish in time".into())),
                        },
                        abort = self.stopped() => Err(abort),
                    }
                }
            };
            let _ = rt.stop(&id, std::time::Duration::from_secs(1)).await;
            let _ = rt.remove(&id).await;
            match outcome? {
                Said::Ok(v) => {
                    tracing::info!(
                        action = v["action"].as_str().unwrap_or(""),
                        "replication: prepared as {}",
                        told.role.as_str()
                    );
                    return Ok(());
                }
                Said::Failed(code, message) if code == spi::RETRY => {
                    self.report(
                        TaskState::Starting,
                        format!("replication: waiting: {message}"),
                    )
                    .await;
                    tokio::select! {
                        _ = tokio::time::sleep(RETRY) => {}
                        abort = self.stopped() => return Err(abort),
                    }
                }
                Said::Failed(code, message)
                    if code == spi::DIVERGED
                        && !reclone
                        && told.role == Role::Replica
                        && told.config.reclone == legion_api::replication::Reclone::Allow =>
                {
                    tracing::warn!("replication: diverged, copied anew: {message}");
                    reclone = true;
                }
                Said::Failed(code, message) => {
                    let how = if code == spi::DIVERGED {
                        "; its data cannot follow the primary: set `replication.reclone: allow`, or empty its Volume"
                    } else {
                        ""
                    };
                    return Err(Abort::Failed(format!(
                        "replication prepare: {code}: {message}{how}"
                    )));
                }
            }
        }
    }

    /// One plugin call in the running member's container.
    async fn call(&self, id: &str, r: &TaskReplication, operation: &str) -> Said {
        let input = self.request(r, operation, false);
        let argv = r.config.argv(operation);
        let run = self.ctx.runtime.exec_input(id, &argv, &input);
        match tokio::time::timeout(spi::deadline(operation), run).await {
            Ok(Ok(o)) => said(
                o.exit_code,
                &String::from_utf8_lossy(&o.stdout),
                &String::from_utf8_lossy(&o.stderr),
            ),
            Ok(Err(e)) => Said::Failed("failed".into(), e.to_string()),
            Err(_) => Said::Failed(
                "failed".into(),
                format!("{operation} did not finish in time"),
            ),
        }
    }

    /// Asks the running member where it stands, then brings it to what it
    /// is told: `promote` when named primary while a replica, `configure`
    /// when the topology changed. Returns why the task must end, when the
    /// plugin says it must start again (`restart`).
    pub(super) async fn tend_member(&mut self, pod: &Pod) -> Option<String> {
        let r = self.told()?.clone();
        let name = self.member_container()?;
        let m = pod.members.iter().find(|m| m.name == name && m.running)?;
        let id = m.id.clone();
        let key = format!("{}/{}", self.a.key.namespace, self.a.key.name);
        let mut report = MemberReport {
            task: key,
            member: self.me().member,
            applied: self.member.applied.as_ref().map_or(0, |(e, _)| *e),
            at: chrono_ms(),
            ..Default::default()
        };
        let mut end = None;
        match self.call(&id, &r, "status").await {
            Said::Ok(v) => match serde_json::from_value::<spi::Status>(v) {
                Ok(s) => report.status = Some(s),
                Err(e) => {
                    report.code = "failed".into();
                    report.error = format!("status: its answer: {e}");
                }
            },
            Said::Failed(code, message) => {
                report.code = code;
                report.error = format!("status: {message}");
            }
        }
        let role = report.status.as_ref().map(|s| s.role);
        let mut act = |op: &str, said: Said, report: &mut MemberReport| -> bool {
            match said {
                Said::Ok(_) => true,
                Said::Failed(code, message) => {
                    if code == spi::RESTART {
                        end = Some(format!("replication: {op}: {message}"));
                    }
                    report.code = code;
                    report.error = format!("{op}: {message}");
                    false
                }
            }
        };
        if r.role == Role::Primary && role == Some(Role::Replica) {
            let said = self.call(&id, &r, "promote").await;
            if act("promote", said, &mut report) {
                tracing::info!(epoch = r.epoch, "replication: promoted");
                self.member.applied = None;
            }
        }
        let current = (r.epoch, r.topology.clone());
        if role.is_some() && self.member.applied.as_ref() != Some(&current) {
            let said = self.call(&id, &r, "configure").await;
            if act("configure", said, &mut report) {
                self.member.applied = Some(current);
                report.applied = r.epoch;
            }
        }
        self.ctx
            .members
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(self.a.uid.clone(), report);
        end
    }

    /// The member's report goes with the task.
    pub(super) fn forget_member(&self) {
        self.ctx
            .members
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.a.uid);
    }
}

/// A container's whole output so far: what it wrote out, and to error.
async fn output_of<R: Runtime>(rt: &R, id: &str) -> (String, String) {
    let Ok(chunks) = rt.logs(id, false, None).await else {
        return Default::default();
    };
    let chunks: Vec<_> = futures::StreamExt::collect(chunks).await;
    let (mut out, mut err) = (String::new(), String::new());
    for c in chunks.into_iter().flatten() {
        let text = String::from_utf8_lossy(&c.data);
        match c.stream {
            Output::Stdout => out.push_str(&text),
            _ => err.push_str(&text),
        }
    }
    (out, err)
}

fn chrono_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_is_read_from_its_exit_and_output() {
        assert!(matches!(said(0, "log\n{\"a\":1}\n", ""), Said::Ok(v) if v["a"] == 1));
        assert!(matches!(said(0, "", ""), Said::Ok(_)));
        assert!(matches!(said(0, "not json", ""), Said::Failed(c, _) if c == "failed"));
        assert!(matches!(
            said(1, "{\"error\":{\"code\":\"retry\",\"message\":\"later\"}}", "x"),
            Said::Failed(c, m) if c == "retry" && m == "later"
        ));
        assert!(matches!(
            said(2, "", "a\nb\nc\nd\n"),
            Said::Failed(c, m) if c == "failed" && m == "b / c / d"
        ));
        assert!(matches!(said(3, "", ""), Said::Failed(_, m) if m == "exited with 3"));
    }
}
