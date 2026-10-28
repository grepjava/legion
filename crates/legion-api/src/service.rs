//! `Service` and the pod it runs (DESIGN.md §7).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::admit::{Invalid, Spec, check_absolute, check_name, field, index};
use crate::quantity::{Bytes, Cpu, Duration};
use crate::security::Security;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceSpec {
    #[serde(default)]
    pub mode: ServiceMode,
    /// Replicated services only; defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
    pub pod: PodSpec,
    #[serde(default, skip_serializing_if = "Placement::is_empty")]
    pub placement: Placement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<Endpoint>,
    #[serde(default)]
    pub update: UpdateConfig,
    /// Most slots that may lack a ready task before an update or a drain
    /// takes another one's away; past it, replacements start first. Unset,
    /// there is no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_unavailable: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceMode {
    #[default]
    Replicated,
    Global,
    Stateful,
}

impl Spec for ServiceSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        match self.mode {
            ServiceMode::Replicated => {
                self.replicas.get_or_insert(1);
            }
            ServiceMode::Global => {
                if self.replicas.is_some() {
                    return Err(Invalid::new(
                        "replicas",
                        "a global service runs one task per node and takes no replica count",
                    ));
                }
            }
            ServiceMode::Stateful => {
                return Err(Invalid::new(
                    "mode",
                    "stateful services are not supported yet (roadmap phase 4)",
                ));
            }
        }
        self.pod.admit("pod")?;
        self.placement.admit("placement")?;
        if let Some(endpoint) = &mut self.endpoint {
            endpoint.admit("endpoint", &self.pod)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodSpec {
    /// Run to completion, in order, before `containers` start.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_containers: Vec<Container>,
    pub containers: Vec<Container>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<PodVolume>,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    /// Time between the stop signal and SIGKILL.
    #[serde(default = "default_stop_grace")]
    pub stop_grace_period: Duration,
}

fn default_stop_grace() -> Duration {
    Duration::from_secs(10)
}

impl PodSpec {
    pub fn admit(&mut self, path: &str) -> Result<(), Invalid> {
        if self.containers.is_empty() {
            return Err(Invalid::new(
                field(path, "containers"),
                "a pod needs at least one container",
            ));
        }
        let mut volumes = BTreeSet::new();
        for (i, v) in self.volumes.iter().enumerate() {
            let p = index(path, "volumes", i);
            v.admit(&p)?;
            if !volumes.insert(v.name.as_str()) {
                return Err(Invalid::new(
                    field(&p, "name"),
                    format!("duplicate volume {:?}", v.name),
                ));
            }
        }

        // Containers share one network stack, so port names and numbers are
        // unique across the pod.
        let mut names = BTreeSet::new();
        let mut port_names = BTreeSet::new();
        let mut port_numbers = BTreeSet::new();
        let all = self
            .init_containers
            .iter_mut()
            .enumerate()
            .map(|(i, c)| (index(path, "initContainers", i), c, true))
            .chain(
                self.containers
                    .iter_mut()
                    .enumerate()
                    .map(|(i, c)| (index(path, "containers", i), c, false)),
            );
        for (p, c, init) in all {
            c.admit(&p, init, &volumes)?;
            if !names.insert(c.name.clone()) {
                return Err(Invalid::new(
                    field(&p, "name"),
                    format!("duplicate container {:?}", c.name),
                ));
            }
            for (j, port) in c.ports.iter().enumerate() {
                let pp = index(&p, "ports", j);
                if let Some(name) = &port.name
                    && !port_names.insert(name.clone())
                {
                    return Err(Invalid::new(
                        field(&pp, "name"),
                        format!("duplicate port name {name:?} in the pod"),
                    ));
                }
                if !port_numbers.insert((port.container_port, port.protocol)) {
                    return Err(Invalid::new(
                        field(&pp, "containerPort"),
                        format!(
                            "port {}/{} is already used in the pod",
                            port.container_port, port.protocol
                        ),
                    ));
                }
            }
        }

        // Probes may name any port in the pod.
        for (i, c) in self.containers.iter().enumerate() {
            let p = field(&index(path, "containers", i), "probes");
            for (name, probe) in c.probes.iter() {
                probe.check_ports(&field(&p, name), self)?;
            }
        }
        Ok(())
    }

    pub fn containers(&self) -> impl Iterator<Item = &Container> {
        self.init_containers.iter().chain(&self.containers)
    }

    /// Every secret and config the pod's containers use.
    pub fn data_refs(&self) -> std::collections::BTreeSet<(crate::Kind, String)> {
        use crate::Kind;
        let mut refs = std::collections::BTreeSet::new();
        for c in self.containers() {
            refs.extend(c.secrets.iter().map(|f| (Kind::Secret, f.name.clone())));
            refs.extend(c.configs.iter().map(|f| (Kind::Config, f.name.clone())));
            for e in &c.env {
                if let Some(n) = &e.secret {
                    refs.insert((Kind::Secret, n.clone()));
                }
                if let Some(n) = &e.config {
                    refs.insert((Kind::Config, n.clone()));
                }
            }
        }
        refs
    }

    /// Whether the pod mounts the `Volume` object `name`.
    pub fn uses_volume(&self, name: &str) -> bool {
        self.volumes
            .iter()
            .any(|v| v.volume.as_ref().is_some_and(|s| s.name == name))
    }

    /// Resolves a port reference against the pod's container ports.
    pub fn resolve_port(&self, port: &PortRef) -> Option<u16> {
        match port {
            PortRef::Number(n) => Some(*n),
            PortRef::Name(name) => self
                .containers()
                .flat_map(|c| &c.ports)
                .find(|p| p.name.as_deref() == Some(name.as_str()))
                .map(|p| p.container_port),
        }
    }

    /// Sum of the containers' requests; init containers run before the
    /// others, so they count only when they ask for more.
    pub fn requests(&self) -> PodRequests {
        let sum = |cs: &[Container]| {
            cs.iter()
                .fold(PodRequests::default(), |acc, c| PodRequests {
                    cpu: Cpu(acc.cpu.0 + c.resources.requests.cpu.unwrap_or_default().0),
                    memory: Bytes(acc.memory.0 + c.resources.requests.memory.unwrap_or_default().0),
                })
        };
        let main = sum(&self.containers);
        let init = self
            .init_containers
            .iter()
            .fold(PodRequests::default(), |acc, c| PodRequests {
                cpu: acc.cpu.max(c.resources.requests.cpu.unwrap_or_default()),
                memory: acc
                    .memory
                    .max(c.resources.requests.memory.unwrap_or_default()),
            });
        PodRequests {
            cpu: main.cpu.max(init.cpu),
            memory: main.memory.max(init.memory),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Container {
    pub name: String,
    pub image: String,
    /// Replaces the image's entrypoint.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// Replaces the image's command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ContainerPort>,
    #[serde(default, skip_serializing_if = "Resources::is_empty")]
    pub resources: Resources,
    #[serde(default, skip_serializing_if = "Probes::is_empty")]
    pub probes: Probes,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    /// Mounted read-only at `target`, by default `/run/secrets/<name>`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<FileRef>,
    /// Mounted read-only at `target`, by default `/run/configs/<name>`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub configs: Vec<FileRef>,
    /// How its jail is loosened from Legion's defaults.
    #[serde(default, skip_serializing_if = "Security::is_default")]
    pub security: Security,
}

impl Container {
    fn admit(&mut self, path: &str, init: bool, volumes: &BTreeSet<&str>) -> Result<(), Invalid> {
        check_name(&field(path, "name"), &self.name)?;
        if self.image.is_empty() || self.image.contains(char::is_whitespace) {
            return Err(Invalid::new(
                field(path, "image"),
                "must be a non-empty image reference",
            ));
        }
        if let Some(dir) = &self.working_dir {
            check_absolute(&field(path, "workingDir"), dir)?;
        }

        let mut env = BTreeSet::new();
        for (i, e) in self.env.iter().enumerate() {
            let p = index(path, "env", i);
            e.admit(&p)?;
            if !env.insert(e.name.as_str()) {
                return Err(Invalid::new(
                    field(&p, "name"),
                    format!("duplicate variable {:?}", e.name),
                ));
            }
        }
        for (i, port) in self.ports.iter().enumerate() {
            let p = index(path, "ports", i);
            if let Some(name) = &port.name {
                check_name(&field(&p, "name"), name)?;
            }
            if port.container_port == 0 {
                return Err(Invalid::new(field(&p, "containerPort"), "must be 1-65535"));
            }
        }
        self.resources.admit(&field(path, "resources"))?;
        self.security.admit(&field(path, "security"))?;
        if init && !self.probes.is_empty() {
            return Err(Invalid::new(
                field(path, "probes"),
                "init containers run to completion and take no probes",
            ));
        }
        for (name, probe) in self.probes.iter_mut() {
            probe.admit(&field(&field(path, "probes"), name))?;
        }

        // Every mount target in the container must be distinct.
        let mut targets = BTreeSet::new();
        for (i, m) in self.volume_mounts.iter().enumerate() {
            let p = index(path, "volumeMounts", i);
            if !volumes.contains(m.name.as_str()) {
                return Err(Invalid::new(
                    field(&p, "name"),
                    format!("no pod volume named {:?}", m.name),
                ));
            }
            check_absolute(&field(&p, "path"), &m.path)?;
            if !targets.insert(m.path.clone()) {
                return Err(Invalid::new(
                    field(&p, "path"),
                    format!("{:?} is mounted twice", m.path),
                ));
            }
        }
        for (list, dir, files) in [
            ("secrets", "/run/secrets", &mut self.secrets),
            ("configs", "/run/configs", &mut self.configs),
        ] {
            for (i, f) in files.iter_mut().enumerate() {
                let p = index(path, list, i);
                check_name(&field(&p, "name"), &f.name)?;
                let target = f.target.get_or_insert_with(|| format!("{dir}/{}", f.name));
                check_absolute(&field(&p, "target"), target)?;
                if !targets.insert(target.clone()) {
                    return Err(Invalid::new(
                        field(&p, "target"),
                        format!("{target:?} is mounted twice"),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// An environment variable: a literal `value`, or the contents of a `secret`
/// or `config` in the same namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVar {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
}

impl EnvVar {
    fn admit(&self, path: &str) -> Result<(), Invalid> {
        if self.name.is_empty() || self.name.contains(['=', '\0']) {
            return Err(Invalid::new(
                field(path, "name"),
                "must be non-empty and contain no `=`",
            ));
        }
        match (&self.value, &self.secret, &self.config) {
            (Some(_), None, None) => Ok(()),
            (None, Some(name), None) => check_name(&field(path, "secret"), name),
            (None, None, Some(name)) => check_name(&field(path, "config"), name),
            _ => Err(Invalid::new(
                path,
                "set exactly one of `value`, `secret` and `config`",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainerPort {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub container_port: u16,
    #[serde(default)]
    pub protocol: Protocol,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Tcp,
    Udp,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        })
    }
}

/// A port by number, or by the name of a container port in the pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortRef {
    Number(u16),
    Name(String),
}

impl Serialize for PortRef {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            PortRef::Number(n) => s.serialize_u16(*n),
            PortRef::Name(name) => s.serialize_str(name),
        }
    }
}

impl<'de> Deserialize<'de> for PortRef {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = PortRef;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a port number or a port name")
            }
            fn visit_u64<E: de::Error>(self, n: u64) -> Result<PortRef, E> {
                u16::try_from(n)
                    .ok()
                    .filter(|&n| n > 0)
                    .map(PortRef::Number)
                    .ok_or_else(|| E::custom(format!("port {n} is not in 1-65535")))
            }
            fn visit_i64<E: de::Error>(self, n: i64) -> Result<PortRef, E> {
                Err(E::custom(format!("port {n} is not in 1-65535")))
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<PortRef, E> {
                match s.parse() {
                    Ok(n) => self.visit_u64(n),
                    Err(_) => Ok(PortRef::Name(s.to_owned())),
                }
            }
        }
        d.deserialize_any(V)
    }
}

impl fmt::Display for PortRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortRef::Number(n) => write!(f, "{n}"),
            PortRef::Name(name) => f.write_str(name),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    /// What the scheduler reserves on the node.
    #[serde(default, skip_serializing_if = "ResourceRequests::is_empty")]
    pub requests: ResourceRequests,
    /// What rctl enforces on the container's jail (DESIGN.md §6.3).
    #[serde(default, skip_serializing_if = "Limits::is_empty")]
    pub limits: Limits,
}

impl Resources {
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.limits.is_empty()
    }

    fn admit(&self, path: &str) -> Result<(), Invalid> {
        if let (Some(r), Some(l)) = (self.requests.cpu, self.limits.cpu)
            && r > l
        {
            return Err(Invalid::new(
                field(path, "requests.cpu"),
                format!("request {r} is above the limit {l}"),
            ));
        }
        if let (Some(r), Some(l)) = (self.requests.memory, self.limits.memory)
            && r > l
        {
            return Err(Invalid::new(
                field(path, "requests.memory"),
                format!("request {r} is above the limit {l}"),
            ));
        }
        for (name, v) in [
            ("maxproc", self.limits.maxproc),
            ("openfiles", self.limits.openfiles),
        ] {
            if v == Some(0) {
                return Err(Invalid::new(
                    field(path, &format!("limits.{name}")),
                    "must be at least 1",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequests {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
}

impl ResourceRequests {
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none()
    }
}

/// Total requests of a pod.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PodRequests {
    pub cpu: Cpu,
    pub memory: Bytes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maxproc: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub openfiles: Option<u32>,
}

impl Limits {
    pub fn is_empty(&self) -> bool {
        *self == Limits::default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probes {
    /// Holds off the other probes until it first succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup: Option<Probe>,
    /// Failing restarts the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<Probe>,
    /// Failing takes the task out of its service's endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<Probe>,
}

impl Probes {
    pub fn is_empty(&self) -> bool {
        self.startup.is_none() && self.liveness.is_none() && self.readiness.is_none()
    }

    fn iter(&self) -> impl Iterator<Item = (&'static str, &Probe)> {
        [
            ("startup", &self.startup),
            ("liveness", &self.liveness),
            ("readiness", &self.readiness),
        ]
        .into_iter()
        .filter_map(|(n, p)| p.as_ref().map(|p| (n, p)))
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&'static str, &mut Probe)> {
        [
            ("startup", &mut self.startup),
            ("liveness", &mut self.liveness),
            ("readiness", &mut self.readiness),
        ]
        .into_iter()
        .filter_map(|(n, p)| p.as_mut().map(|p| (n, p)))
    }
}

/// One check: exactly one of `http`, `tcp`, `grpc` and `exec`. Network
/// checks run from the host against the pod's address; `exec` runs in the
/// container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Probe {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp: Option<TcpCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc: Option<GrpcCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecCheck>,
    #[serde(default)]
    pub initial_delay: Duration,
    #[serde(default = "default_period")]
    pub period: Duration,
    #[serde(default = "default_timeout")]
    pub timeout: Duration,
    #[serde(default = "default_failures")]
    pub failure_threshold: u32,
    #[serde(default = "one")]
    pub success_threshold: u32,
}

fn default_period() -> Duration {
    Duration::from_secs(10)
}
fn default_timeout() -> Duration {
    Duration::from_secs(1)
}
fn default_failures() -> u32 {
    3
}
fn one() -> u32 {
    1
}

impl Probe {
    fn admit(&mut self, path: &str) -> Result<(), Invalid> {
        let handlers = [
            self.http.is_some(),
            self.tcp.is_some(),
            self.grpc.is_some(),
            self.exec.is_some(),
        ];
        if handlers.iter().filter(|&&h| h).count() != 1 {
            return Err(Invalid::new(
                path,
                "set exactly one of `http`, `tcp`, `grpc` and `exec`",
            ));
        }
        if let Some(http) = &self.http
            && !http.path.starts_with('/')
        {
            return Err(Invalid::new(
                field(path, "http.path"),
                "must start with `/`",
            ));
        }
        if let Some(exec) = &self.exec
            && exec.command.is_empty()
        {
            return Err(Invalid::new(
                field(path, "exec.command"),
                "must not be empty",
            ));
        }
        for (name, d) in [("period", self.period), ("timeout", self.timeout)] {
            if d.millis() == 0 {
                return Err(Invalid::new(field(path, name), "must be above zero"));
            }
        }
        for (name, n) in [
            ("failureThreshold", self.failure_threshold),
            ("successThreshold", self.success_threshold),
        ] {
            if n == 0 {
                return Err(Invalid::new(field(path, name), "must be at least 1"));
            }
        }
        Ok(())
    }

    fn check_ports(&self, path: &str, pod: &PodSpec) -> Result<(), Invalid> {
        let port = [
            self.http.as_ref().map(|h| ("http.port", &h.port)),
            self.tcp.as_ref().map(|t| ("tcp.port", &t.port)),
            self.grpc.as_ref().map(|g| ("grpc.port", &g.port)),
        ];
        for (name, port) in port.into_iter().flatten() {
            if pod.resolve_port(port).is_none() {
                return Err(Invalid::new(
                    field(path, name),
                    format!("no container port named `{port}` in the pod"),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCheck {
    #[serde(default = "root")]
    pub path: String,
    pub port: PortRef,
    #[serde(default)]
    pub scheme: HttpScheme,
}

fn root() -> String {
    "/".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HttpScheme {
    #[default]
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpCheck {
    pub port: PortRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcCheck {
    pub port: PortRef,
    /// Service name sent in the health check request; empty checks the server.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub service: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecCheck {
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VolumeMount {
    pub name: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

/// A secret or config mounted as a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// A volume the pod's containers can mount: exactly one of a `Volume` object
/// (`volume`) or a memory-backed `tmpfs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PodVolume {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<VolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmpfs: Option<TmpfsSource>,
}

impl PodVolume {
    fn admit(&self, path: &str) -> Result<(), Invalid> {
        check_name(&field(path, "name"), &self.name)?;
        match (&self.volume, &self.tmpfs) {
            (Some(v), None) => check_name(&field(path, "volume.name"), &v.name),
            (None, Some(_)) => Ok(()),
            _ => Err(Invalid::new(
                path,
                "set exactly one of `volume` and `tmpfs`",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSource {
    /// A `Volume` in the same namespace.
    pub name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TmpfsSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestartPolicy {
    #[serde(default)]
    pub condition: RestartCondition,
    /// 0 means no limit.
    #[serde(default)]
    pub max_attempts: u32,
    #[serde(default = "default_restart_delay")]
    pub delay: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        RestartPolicy {
            condition: RestartCondition::default(),
            max_attempts: 0,
            delay: default_restart_delay(),
        }
    }
}

fn default_restart_delay() -> Duration {
    Duration::from_secs(5)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RestartCondition {
    #[default]
    Any,
    OnFailure,
    None,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// Each must hold for a node to be eligible, e.g.
    /// `node.labels.disk == ssd`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preferences: Vec<Preference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
}

impl Placement {
    pub fn is_empty(&self) -> bool {
        *self == Placement::default()
    }

    pub(crate) fn admit(&self, path: &str) -> Result<(), Invalid> {
        for (i, c) in self.constraints.iter().enumerate() {
            c.parse::<Constraint>()
                .map_err(|e| Invalid::new(index(path, "constraints", i), e))?;
        }
        for (i, p) in self.preferences.iter().enumerate() {
            if !p.spread.starts_with("node.labels.") || p.spread.len() == "node.labels.".len() {
                return Err(Invalid::new(
                    field(&index(path, "preferences", i), "spread"),
                    "must be `node.labels.<key>`",
                ));
            }
        }
        for (i, t) in self.tolerations.iter().enumerate() {
            if t.key.is_empty() {
                return Err(Invalid::new(
                    field(&index(path, "tolerations", i), "key"),
                    "must not be empty",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preference {
    pub spread: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Toleration {
    pub key: String,
    /// Empty tolerates any value of the key.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    /// Empty tolerates every effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<TaintEffect>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaintEffect {
    NoSchedule,
    PreferNoSchedule,
    NoExecute,
}

/// A parsed placement constraint: `<attribute> == <value>` or `!=`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constraint {
    pub attribute: NodeAttribute,
    pub equal: bool,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAttribute {
    Name,
    Hostname,
    Role,
    Os,
    Arch,
    Label(String),
}

impl FromStr for Constraint {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let (lhs, equal, rhs) = if let Some((l, r)) = s.split_once("==") {
            (l, true, r)
        } else if let Some((l, r)) = s.split_once("!=") {
            (l, false, r)
        } else {
            return Err(format!("{s:?}: expected `<attribute> == <value>` or `!=`"));
        };
        let (lhs, value) = (lhs.trim(), rhs.trim());
        let attribute = match lhs {
            "node.name" => NodeAttribute::Name,
            "node.hostname" => NodeAttribute::Hostname,
            "node.role" => NodeAttribute::Role,
            "node.platform.os" => NodeAttribute::Os,
            "node.platform.arch" => NodeAttribute::Arch,
            _ => match lhs.strip_prefix("node.labels.") {
                Some(key) if !key.is_empty() => NodeAttribute::Label(key.to_owned()),
                _ => {
                    return Err(format!(
                        "{lhs:?}: expected node.name, node.hostname, node.role, \
                         node.platform.os, node.platform.arch or node.labels.<key>"
                    ));
                }
            },
        };
        if value.is_empty() || value.contains(char::is_whitespace) {
            return Err(format!("{s:?}: the value must be one non-empty word"));
        }
        Ok(Constraint {
            attribute,
            equal,
            value: value.to_owned(),
        })
    }
}

/// How clients reach the service's tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    #[serde(default)]
    pub mode: EndpointMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ServicePort>,
}

impl Endpoint {
    fn admit(&mut self, path: &str, pod: &PodSpec) -> Result<(), Invalid> {
        let mut names = BTreeSet::new();
        let mut ports = BTreeSet::new();
        let mut published = BTreeMap::new();
        for (i, port) in self.ports.iter_mut().enumerate() {
            let p = index(path, "ports", i);
            if let Some(name) = &port.name {
                check_name(&field(&p, "name"), name)?;
                if !names.insert(name.clone()) {
                    return Err(Invalid::new(
                        field(&p, "name"),
                        format!("duplicate port name {name:?}"),
                    ));
                }
            }
            if port.port == 0 {
                return Err(Invalid::new(field(&p, "port"), "must be 1-65535"));
            }
            if !ports.insert((port.port, port.protocol)) {
                return Err(Invalid::new(
                    field(&p, "port"),
                    format!("port {}/{} is listed twice", port.port, port.protocol),
                ));
            }
            let target = port
                .target_port
                .get_or_insert(PortRef::Number(port.port))
                .clone();
            if pod.resolve_port(&target).is_none() {
                return Err(Invalid::new(
                    field(&p, "targetPort"),
                    format!("no container port named `{target}` in the pod"),
                ));
            }
            match (port.publish, port.published_port) {
                (Publish::None, Some(_)) => {
                    return Err(Invalid::new(
                        field(&p, "publishedPort"),
                        "set `publish: ingress` or `publish: host` to publish a port",
                    ));
                }
                (Publish::Host, None) => {
                    return Err(Invalid::new(
                        field(&p, "publishedPort"),
                        "`publish: host` needs a published port",
                    ));
                }
                (_, Some(0)) => {
                    return Err(Invalid::new(field(&p, "publishedPort"), "must be 1-65535"));
                }
                (_, Some(n)) => {
                    if let Some(prev) = published.insert((n, port.protocol), i) {
                        return Err(Invalid::new(
                            field(&p, "publishedPort"),
                            format!("{n} is already published by ports[{prev}]"),
                        ));
                    }
                }
                _ => {}
            }
        }
        if self.mode == EndpointMode::Dnsrr
            && self.ports.iter().any(|p| p.publish == Publish::Ingress)
        {
            return Err(Invalid::new(
                field(path, "mode"),
                "`dnsrr` has no virtual IP for the routing mesh; publish with `host` instead",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointMode {
    /// One virtual IP, load-balanced by pf.
    #[default]
    Vip,
    /// DNS returns every ready task's address.
    Dnsrr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServicePort {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The port on the service address.
    pub port: u16,
    /// The container port; defaults to `port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_port: Option<PortRef>,
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default)]
    pub publish: Publish,
    /// For `ingress`, chosen by Legion when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_port: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Publish {
    /// Reachable inside the cluster only.
    #[default]
    None,
    /// On every node, through the routing mesh.
    Ingress,
    /// On the nodes running a task, directly.
    Host,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateConfig {
    /// Tasks replaced at a time; 0 replaces all at once.
    #[serde(default = "one")]
    pub parallelism: u32,
    #[serde(default)]
    pub delay: Duration,
    #[serde(default)]
    pub order: UpdateOrder,
    #[serde(default)]
    pub failure_action: FailureAction,
    /// How long a new task must stay running before it counts as updated.
    #[serde(default = "default_monitor")]
    pub monitor: Duration,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        UpdateConfig {
            parallelism: 1,
            delay: Duration::default(),
            order: UpdateOrder::default(),
            failure_action: FailureAction::default(),
            monitor: default_monitor(),
        }
    }
}

fn default_monitor() -> Duration {
    Duration::from_secs(5)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateOrder {
    #[default]
    StopFirst,
    StartFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailureAction {
    #[default]
    Pause,
    Continue,
    Rollback,
}

/// Where service VIPs come from (DESIGN.md §5.2): `10.201.0.0/16`.
pub const SERVICE_NET: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 201, 0, 0);
pub const SERVICE_PREFIX: u8 = 16;
/// Ports Legion picks from for `publish: ingress` without `publishedPort`.
pub const PUBLISHED_PORTS: std::ops::RangeInclusive<u16> = 30000..=32767;

/// A port the service publishes outside the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedPort {
    /// The service port it leads to.
    pub port: u16,
    pub protocol: Protocol,
    pub published_port: u16,
}

/// Observed state, written by the Service controller.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    /// The spec generation the counts below reflect.
    pub observed_generation: u64,
    pub desired: u32,
    pub running: u32,
    pub ready: u32,
    /// Assigned by the allocator when `endpoint.mode` is `vip` and the
    /// service has ports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_ip: Option<String>,
    /// Every published port, those Legion picked included.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub published_ports: Vec<PublishedPort>,
    /// What the service asks for that another service holds, such as a
    /// published port; it goes without until that is released.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
    /// The number of the newest task of each slot (replica number or
    /// node), so a deleted task's name is never given to another.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub last_tasks: BTreeMap<String, u64>,
    /// The spec as the controller last saw it. When the Service's spec
    /// differs in its pod, an update starts from this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<ServiceSpec>,
    /// The latest rolling update, once there has been one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateStatus>,
}

/// How a rolling update went (DESIGN.md §3.3, Update).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatus {
    pub state: UpdateState,
    /// Why it paused or rolled back.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// The Service's generation when the update started: tasks of this or
    /// a later one are the update's.
    pub generation: u64,
    /// The spec updated from, which a rollback restores.
    pub previous: ServiceSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateState {
    Updating,
    /// A new task failed and `failureAction` is `pause`: no more tasks are
    /// replaced until the spec changes again.
    Paused,
    Completed,
    /// Going back to the previous spec, after a failure or by request.
    RollingBack,
    /// A task failed while rolling back.
    RollbackPaused,
    RolledBack,
}

impl UpdateState {
    pub fn as_str(self) -> &'static str {
        match self {
            UpdateState::Updating => "updating",
            UpdateState::Paused => "paused",
            UpdateState::Completed => "completed",
            UpdateState::RollingBack => "rolling-back",
            UpdateState::RollbackPaused => "rollback-paused",
            UpdateState::RolledBack => "rolled-back",
        }
    }

    /// Whether tasks are being replaced.
    pub fn in_progress(self) -> bool {
        matches!(self, UpdateState::Updating | UpdateState::RollingBack)
    }

    pub fn paused(self) -> bool {
        matches!(self, UpdateState::Paused | UpdateState::RollbackPaused)
    }
}
