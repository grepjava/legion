//! How a task's pod maps onto Podman containers (DESIGN.md §6.2): names,
//! labels, container specs and rctl rules.

use std::collections::BTreeMap;

use legion_api::service::{Container, PodSpec};
use legion_podman::ContainerSpec;
use legion_sys::{RctlRule, Resource};

use crate::control::Assignment;

pub const LABEL_NODE: &str = "legion.node.id";
pub const LABEL_TASK: &str = "legion.task.id";
/// `namespace/name` of the task, for people.
pub const LABEL_TASK_NAME: &str = "legion.task";
pub const LABEL_ROLE: &str = "legion.role";
pub const LABEL_CONTAINER: &str = "legion.container";

pub const ANNOTATION_VNET: &str = "org.freebsd.jail.vnet";
pub const ANNOTATION_PARENT: &str = "org.freebsd.parentJail";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Infra,
    Init,
    App,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Infra => "infra",
            Role::Init => "init",
            Role::App => "app",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        match s {
            "infra" => Some(Role::Infra),
            "init" => Some(Role::Init),
            "app" => Some(Role::App),
            _ => None,
        }
    }
}

/// Podman name of a pod member: `<ns>_<task>` for the infra container,
/// `<ns>_<task>_<container>` for the others. `_` cannot occur in object
/// names, so the forms cannot collide.
pub fn container_name(a: &Assignment, container: Option<&str>) -> String {
    match container {
        None => format!("{}_{}", a.key.namespace, a.key.name),
        Some(c) => format!("{}_{}_{c}", a.key.namespace, a.key.name),
    }
}

fn labels(node: &str, a: &Assignment, role: Role, container: &str) -> BTreeMap<String, String> {
    [
        (LABEL_NODE, node.to_owned()),
        (LABEL_TASK, a.uid.clone()),
        (
            LABEL_TASK_NAME,
            format!("{}/{}", a.key.namespace, a.key.name),
        ),
        (LABEL_ROLE, role.as_str().to_owned()),
        (LABEL_CONTAINER, container.to_owned()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

/// The infra container: an empty jail with its own VNET that the pod's
/// containers run inside.
pub fn infra_spec(node: &str, a: &Assignment, image: &str) -> ContainerSpec {
    ContainerSpec {
        name: container_name(a, None),
        image: image.to_owned(),
        entrypoint: vec!["/bin/sleep".into()],
        command: vec!["2147483647".into()],
        labels: labels(node, a, Role::Infra, ""),
        annotations: [(ANNOTATION_VNET.to_owned(), "new".to_owned())].into(),
        stop_timeout: Some(1),
        ..Default::default()
    }
}

/// A pod member running as a child jail of the infra container.
pub fn container_spec(
    node: &str,
    a: &Assignment,
    c: &Container,
    role: Role,
    infra_id: &str,
) -> ContainerSpec {
    ContainerSpec {
        name: container_name(a, Some(&c.name)),
        image: c.image.clone(),
        entrypoint: c.command.clone(),
        command: c.args.clone(),
        env: c
            .env
            .iter()
            .filter_map(|e| Some((e.name.clone(), e.value.clone()?)))
            .collect(),
        work_dir: c.working_dir.clone(),
        user: c.user.clone(),
        labels: labels(node, a, role, &c.name),
        annotations: [(ANNOTATION_PARENT.to_owned(), infra_id.to_owned())].into(),
        stop_timeout: Some(a.spec.pod.stop_grace_period.millis().div_ceil(1000)),
        ..Default::default()
    }
}

/// The jail a pod member runs in: a child of the infra jail. ocijail names
/// a container's jail after the container id.
pub fn child_jail(infra_id: &str, container_id: &str) -> String {
    format!("{infra_id}.{container_id}")
}

/// rctl rules for a container's limits (DESIGN.md §6.3). They are set on the
/// container's own jail, where they are enforced.
pub fn rctl_rules(c: &Container, jail: &str) -> Vec<RctlRule> {
    let l = &c.resources.limits;
    let rule = |resource, amount| RctlRule {
        jail: jail.to_owned(),
        resource,
        amount,
    };
    let mut rules = Vec::new();
    if let Some(m) = l.memory {
        rules.push(rule(Resource::VMemoryUse, m.0));
    }
    if let Some(cpu) = l.cpu {
        // pcpu is a percentage of one CPU.
        rules.push(rule(Resource::Pcpu, (cpu.millis() / 10).max(1)));
    }
    if let Some(n) = l.maxproc {
        rules.push(rule(Resource::MaxProc, n.into()));
    }
    if let Some(n) = l.openfiles {
        rules.push(rule(Resource::OpenFiles, n.into()));
    }
    rules
}

/// Pod features later phase-1 steps add (PHASE.md). A task using one is
/// rejected rather than run without it.
pub fn unsupported(pod: &PodSpec) -> Option<String> {
    if !pod.volumes.is_empty() {
        return Some("volumes are not supported yet".into());
    }
    for c in pod.containers() {
        if !c.volume_mounts.is_empty() {
            return Some(format!(
                "container {}: volume mounts are not supported yet",
                c.name
            ));
        }
        let probes = &c.probes;
        if [&probes.startup, &probes.liveness, &probes.readiness]
            .into_iter()
            .flatten()
            .filter_map(|p| p.http.as_ref())
            .any(|h| h.scheme == legion_api::service::HttpScheme::Https)
        {
            return Some(format!(
                "container {}: HTTPS probes are not supported yet",
                c.name
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use legion_api::quantity::{Bytes, Cpu};
    use legion_api::service::{Limits, Resources};

    use super::*;

    #[test]
    fn rules_from_limits() {
        let c = Container {
            name: "web".into(),
            image: "web:1".into(),
            command: vec![],
            args: vec![],
            working_dir: None,
            user: None,
            env: vec![],
            ports: vec![],
            resources: Resources {
                limits: Limits {
                    cpu: Some(Cpu(1500)),
                    memory: Some(Bytes(512 << 20)),
                    maxproc: Some(64),
                    openfiles: None,
                },
                ..Default::default()
            },
            probes: Default::default(),
            volume_mounts: vec![],
            secrets: vec![],
            configs: vec![],
        };
        let rules: Vec<String> = rctl_rules(&c, "i.c")
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            rules,
            [
                "jail:i.c:vmemoryuse:deny=536870912",
                "jail:i.c:pcpu:deny=150",
                "jail:i.c:maxproc:deny=64",
            ]
        );
    }
}
