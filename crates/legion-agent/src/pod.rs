//! How a task's pod maps onto the runtime's containers (DESIGN.md §6.2): names,
//! labels, container specs and rctl rules.

use std::collections::BTreeMap;

use crate::runtime::ContainerSpec;
use legion_api::security::{Allow, Security};
use legion_api::service::{Container, PodSpec};
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
/// The jail parameters the runtime gives a container's jail when it makes
/// it (see [`jail_params`]).
pub const ANNOTATION_JAIL: &str = "legion.jail";
const ANNOTATION_MLOCK: &str = "org.freebsd.jail.allow.mlock";
const ANNOTATION_SYSV: [&str; 3] = [
    "org.freebsd.jail.sysvmsg",
    "org.freebsd.jail.sysvsem",
    "org.freebsd.jail.sysvshm",
];

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

/// The runtime's name of a pod member: `<ns>_<task>` for the infra container,
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
/// containers run inside. A child jail may have only what its parent has,
/// so it allows what any of the pod's containers needs from it: mlock,
/// and SysV IPC, which those containers then share.
pub fn infra_spec(node: &str, a: &Assignment, image: &str) -> ContainerSpec {
    let mut annotations = BTreeMap::from([(ANNOTATION_VNET.to_owned(), "new".to_owned())]);
    let allows = |x| {
        a.spec
            .pod
            .containers()
            .any(|c| c.security.allow.contains(&x))
    };
    if allows(Allow::Mlock) {
        annotations.insert(ANNOTATION_MLOCK.to_owned(), "true".to_owned());
    }
    if allows(Allow::Sysvipc) {
        for k in ANNOTATION_SYSV {
            annotations.insert(k.to_owned(), "new".to_owned());
        }
    }
    ContainerSpec {
        name: container_name(a, None),
        image: image.to_owned(),
        entrypoint: vec!["/bin/sleep".into()],
        command: vec!["2147483647".into()],
        hostname: a.spec.hostname(),
        labels: labels(node, a, Role::Infra, ""),
        annotations,
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
        hostname: a.spec.hostname(),
        labels: labels(node, a, role, &c.name),
        annotations: container_annotations(&c.security, infra_id),
        stop_timeout: Some(a.spec.pod.stop_grace_period.millis().div_ceil(1000)),
        ..Default::default()
    }
}

fn container_annotations(s: &Security, infra_id: &str) -> BTreeMap<String, String> {
    let params = jail_params(s)
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut annotations = BTreeMap::from([
        (ANNOTATION_PARENT.to_owned(), infra_id.to_owned()),
        (ANNOTATION_JAIL.to_owned(), params),
    ]);
    if s.allow.contains(&Allow::Mlock) {
        annotations.insert(ANNOTATION_MLOCK.to_owned(), "true".to_owned());
    }
    if s.allow.contains(&Allow::Sysvipc) {
        for k in ANNOTATION_SYSV {
            annotations.insert(k.to_owned(), "inherit".to_owned());
        }
    }
    annotations
}

/// The parameters a container's jail must have (DESIGN.md §9). The
/// runtime sets them when it creates the jail (from the `legion.jail`
/// annotation), and the agent checks them once the container runs. mlock
/// and SysV IPC are off unless their annotations ask for them.
pub fn jail_params(s: &Security) -> [(&'static str, String); 4] {
    let allow = |a| s.allow.contains(&a).to_string();
    [
        ("allow.raw_sockets", allow(Allow::RawSockets)),
        ("allow.chflags", allow(Allow::Chflags)),
        ("enforce_statfs", s.enforce_statfs.to_string()),
        ("securelevel", s.securelevel.to_string()),
    ]
}

/// The jail a pod member runs in: a child of the infra jail. The runtime
/// names a container's jail after the container id.
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
        rules.push(rule(Resource::MemoryUse, m.0));
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
    for c in pod.containers() {
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
    fn jail_annotations() {
        let mut s = Security::default();
        let a = container_annotations(&s, "i");
        assert_eq!(
            a[ANNOTATION_JAIL],
            "allow.raw_sockets=false allow.chflags=false enforce_statfs=2 securelevel=2"
        );
        assert_eq!(a.len(), 2);
        s.allow = [Allow::RawSockets, Allow::Mlock, Allow::Sysvipc].into();
        s.securelevel = -1;
        let a = container_annotations(&s, "i");
        assert_eq!(
            a[ANNOTATION_JAIL],
            "allow.raw_sockets=true allow.chflags=false enforce_statfs=2 securelevel=-1"
        );
        assert_eq!(a[ANNOTATION_MLOCK], "true");
        assert_eq!(a["org.freebsd.jail.sysvshm"], "inherit");
    }

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
            security: Default::default(),
        };
        let rules: Vec<String> = rctl_rules(&c, "i.c")
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            rules,
            [
                "jail:i.c:memoryuse:sigkill=536870912/process",
                "jail:i.c:pcpu:deny=150",
                "jail:i.c:maxproc:deny=64",
            ]
        );
    }
}
