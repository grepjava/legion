//! Namespace quotas (DESIGN.md §4.2): how much a namespace's tasks may
//! request and how many volumes, of what size, it may hold. Set on the
//! `Namespace`, which only the cluster admin may change.
//!
//! The scheduler holds a task that would take its namespace over the quota;
//! admission refuses a Service or Volume whose desired state would.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::quantity::{Bytes, Cpu};
use crate::service::PodRequests;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quota {
    /// CPU the namespace's tasks may request in all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
    /// Memory the namespace's tasks may request in all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
    /// Tasks placed on nodes at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<u32>,
    /// The sizes of the namespace's volumes in all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Bytes>,
}

/// What a namespace uses, or would, of what quotas limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub cpu: Cpu,
    pub memory: Bytes,
    pub tasks: u32,
    pub volumes: u32,
    pub storage: Bytes,
}

impl Usage {
    /// Adds `n` tasks requesting `r` each.
    pub fn add_tasks(&mut self, r: PodRequests, n: u32) {
        self.cpu = Cpu(self.cpu.0 + r.cpu.0 * u64::from(n));
        self.memory = Bytes(self.memory.0 + r.memory.0 * u64::from(n));
        self.tasks += n;
    }

    pub fn add_volume(&mut self, size: Option<Bytes>) {
        self.volumes += 1;
        self.storage = Bytes(self.storage.0 + size.map_or(0, |b| b.0));
    }
}

/// One limit gone over: `cpu 4500m of 4`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exceeded {
    pub resource: &'static str,
    pub used: String,
    pub limit: String,
}

impl fmt::Display for Exceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} of {}", self.resource, self.used, self.limit)
    }
}

impl Quota {
    /// The limits `after` goes over where it uses more than `before`: a
    /// change that only shrinks what a namespace uses is never refused,
    /// even when a lowered quota leaves it over.
    pub fn exceeded(&self, before: &Usage, after: &Usage) -> Vec<Exceeded> {
        let mut out = Vec::new();
        let mut check =
            |resource, used: u64, was: u64, limit: Option<u64>, show: &dyn Fn(u64) -> String| {
                if let Some(limit) = limit
                    && used > limit
                    && used > was
                {
                    out.push(Exceeded {
                        resource,
                        used: show(used),
                        limit: show(limit),
                    });
                }
            };
        check(
            "cpu",
            after.cpu.0,
            before.cpu.0,
            self.cpu.map(|c| c.0),
            &|n| Cpu(n).to_string(),
        );
        check(
            "memory",
            after.memory.0,
            before.memory.0,
            self.memory.map(|b| b.0),
            &|n| Bytes(n).to_string(),
        );
        check(
            "tasks",
            after.tasks.into(),
            before.tasks.into(),
            self.tasks.map(u64::from),
            &|n| n.to_string(),
        );
        check(
            "volumes",
            after.volumes.into(),
            before.volumes.into(),
            self.volumes.map(u64::from),
            &|n| n.to_string(),
        );
        check(
            "storage",
            after.storage.0,
            before.storage.0,
            self.storage.map(|b| b.0),
            &|n| Bytes(n).to_string(),
        );
        out
    }
}

/// `cpu 5 of 4, tasks 9 of 8`.
pub fn describe(over: &[Exceeded]) -> String {
    over.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(cpu: u64, mem: u64) -> PodRequests {
        PodRequests {
            cpu: Cpu(cpu),
            memory: Bytes(mem),
        }
    }

    #[test]
    fn only_growth_past_a_limit_counts() {
        let q: Quota = serde_yaml_ng::from_str("{ cpu: 2, memory: 1Gi, tasks: 3 }").unwrap();
        let mut before = Usage::default();
        before.add_tasks(pod(500, 256 << 20), 2);
        let mut after = before;
        after.add_tasks(pod(500, 256 << 20), 1);
        assert_eq!(q.exceeded(&before, &after), []);

        // A fourth task goes over on tasks only.
        let mut more = after;
        more.add_tasks(pod(500, 256 << 20), 1);
        assert_eq!(describe(&q.exceeded(&after, &more)), "tasks 4 of 3");
        let mut big = after;
        big.add_tasks(pod(1500, 1 << 30), 1);
        assert_eq!(
            describe(&q.exceeded(&after, &big)),
            "cpu 3 of 2, memory 1792Mi of 1Gi, tasks 4 of 3"
        );

        // Over a lowered quota, shrinking is still allowed.
        let low = Quota {
            tasks: Some(1),
            ..q
        };
        assert_eq!(low.exceeded(&after, &before), []);
        assert_eq!(describe(&low.exceeded(&before, &after)), "tasks 3 of 1");
    }

    #[test]
    fn volumes_count_and_sizes() {
        let q: Quota = serde_yaml_ng::from_str("{ volumes: 1, storage: 10Gi }").unwrap();
        let before = Usage::default();
        let mut one = before;
        one.add_volume(Some(Bytes(10 << 30)));
        assert_eq!(q.exceeded(&before, &one), []);
        let mut two = one;
        two.add_volume(Some(Bytes(1 << 30)));
        assert_eq!(
            describe(&q.exceeded(&one, &two)),
            "volumes 2 of 1, storage 11Gi of 10Gi"
        );
        assert_eq!(
            serde_json::to_value(q).unwrap(),
            serde_json::json!({"volumes": 1, "storage": "10Gi"})
        );
    }
}
