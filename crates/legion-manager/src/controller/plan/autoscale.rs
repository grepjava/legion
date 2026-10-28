//! The autoscaler (DESIGN.md §3.3): a replicated service with `autoscale`
//! runs the replica count that brings its tasks' average use to the
//! targets, which are percentages of what the pods' containers request.
//! Use comes from the tasks' nodes with their heartbeats (rctl's `pcpu`
//! and `memoryuse`), counted only for ready tasks and only from the node
//! each runs on.
//!
//! The count is `ceil(measured × use / target)`, over the ready tasks with
//! a report, for the target asking for most, between `min` and `max`; it
//! stays as it is while use is within a tenth of every target. Passes come
//! far more often than reports, so the count follows from the reports
//! alone and stays put until the next ones. Ready tasks without a report
//! count as idle for growing and as on target for shrinking. The count
//! grows at once; it shrinks only to the most recommended over the last
//! `downDelay`, and a new leader, having seen nothing yet, waits that long
//! before shrinking anything.

use legion_api::dispatch::TaskUsage;
use legion_api::service::{AutoscaleStatus, ServiceStatus};
use legion_api::task::TaskState;
use tokio::time::Instant;

use super::{Memory, Service, Task};

/// Use this close to a target, relatively, changes nothing.
const TOLERANCE: f64 = 0.1;

/// The replica count `s` runs now, and its autoscaling status.
pub(super) fn replicas(
    s: &Service,
    tasks: &[&Task],
    previous: &ServiceStatus,
    memory: &mut Memory,
    now: Instant,
) -> (u32, Option<AutoscaleStatus>) {
    let uid = &s.object.metadata.uid;
    let Some(a) = &s.spec.autoscale else {
        memory.recommended.remove(uid);
        return (s.spec.replicas.unwrap_or(1), None);
    };
    let current = previous
        .autoscale
        .as_ref()
        .map_or(s.spec.replicas.unwrap_or(a.min), |x| x.replicas)
        .clamp(a.min, a.max);

    let ready: Vec<&&Task> = tasks
        .iter()
        .filter(|t| t.active() && t.status.state == TaskState::Running && t.status.ready)
        .collect();
    let measured: Vec<(&Task, TaskUsage)> = ready
        .iter()
        .filter_map(|t| {
            let key = format!("{}/{}", t.object.metadata.namespace, t.object.metadata.name);
            let (node, usage) = memory.usage.get(&key)?;
            (*node == t.spec.node).then_some((**t, *usage))
        })
        .collect();
    let mut status = AutoscaleStatus {
        replicas: current,
        ..Default::default()
    };
    let mut recommended = current;
    if measured.is_empty() {
        status.message = "no use reported for the service's ready tasks".into();
    } else {
        // Both in thousandths: of a CPU, and of a byte.
        let (mut cpu, mut cpu_requested, mut mem, mut mem_requested) = (0.0, 0.0, 0.0, 0.0);
        for (t, u) in &measured {
            cpu += u.cpu * 10.0;
            mem += u.memory as f64;
            for c in &t.spec.pod.containers {
                let r = &c.resources.requests;
                cpu_requested += r.cpu.map_or(0, |c| c.millis()) as f64;
                mem_requested += r.memory.map_or(0, |m| m.0) as f64;
            }
        }
        let mut wanted = Vec::new();
        for (target, used, requested, out) in [
            (a.cpu, cpu, cpu_requested, &mut status.cpu),
            (a.memory, mem, mem_requested, &mut status.memory),
        ] {
            let Some(target) = target else { continue };
            if requested <= 0.0 {
                continue;
            }
            let percent = 100.0 * used / requested;
            *out = Some(percent.round() as u32);
            let ratio = percent / f64::from(target);
            let n = measured.len() as f64;
            let missing = (ready.len() - measured.len()) as f64;
            wanted.push(if (ratio - 1.0).abs() <= TOLERANCE {
                current
            } else if ratio > 1.0 {
                current.max((n * ratio).ceil() as u32)
            } else {
                current.min((n * ratio + missing).ceil() as u32)
            });
        }
        if let Some(most) = wanted.into_iter().max() {
            recommended = most.clamp(a.min, a.max);
        }
    }

    // Recommendations over the last `downDelay`, starting with the count
    // as it was when this leader first saw the service.
    let window = memory
        .recommended
        .entry(uid.clone())
        .or_insert_with(|| vec![(now, current)]);
    let delay = a.down_delay.as_std();
    window.retain(|(at, _)| *at + delay > now);
    if window
        .last()
        .is_none_or(|(at, r)| *r != recommended || now.duration_since(*at).as_secs() >= 1)
    {
        window.push((now, recommended));
    }
    status.replicas = if recommended >= current {
        recommended
    } else {
        let most = window.iter().map(|(_, r)| *r).max().unwrap_or(recommended);
        most.min(current)
    };
    (status.replicas, Some(status))
}
