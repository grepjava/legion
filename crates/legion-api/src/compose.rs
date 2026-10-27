//! Compose files as Legion objects (DESIGN.md §8), for `legion stack
//! deploy` and `legion stack convert`.
//!
//! Each compose service becomes a `Service` in the stack's namespace, under
//! its own name, so services find each other by name as in a compose
//! network. Top-level volumes, secrets and configs become `Volume`,
//! `Secret` and `Config` objects. Every object carries the label
//! `legion.stack=<stack>`. What Legion cannot honour is reported as a
//! warning; `x-legion` on a service is merged into the generated spec for
//! what compose cannot say.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Map, Value, json};

use crate::{Kind, Object};

/// Label tying objects to their stack.
pub const LABEL_STACK: &str = "legion.stack";

/// Where a compose file's outside inputs come from.
pub struct Inputs<'a> {
    /// Variables for `${VAR}` interpolation.
    pub env: &'a dyn Fn(&str) -> Option<String>,
    /// Files named by `secrets`, `configs` and `env_file`, as written in
    /// the compose file (relative to it).
    pub read: &'a dyn Fn(&str) -> Result<Vec<u8>, String>,
}

/// The objects of a stack, in the order to apply them.
#[derive(Debug)]
pub struct Stack {
    pub objects: Vec<Object>,
    pub warnings: Vec<String>,
}

/// Converts a compose file into the objects of `stack` in `namespace`.
pub fn convert(yaml: &str, stack: &str, namespace: &str, inputs: &Inputs) -> Result<Stack, String> {
    let mut doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("reading the compose file: {e}"))?;
    doc.apply_merge()
        .map_err(|e| format!("reading the compose file: {e}"))?;
    let mut doc: Value =
        serde_json::to_value(&doc).map_err(|e| format!("reading the compose file: {e}"))?;
    let mut c = Converter {
        stack: stack.to_owned(),
        namespace: namespace.to_owned(),
        inputs,
        warnings: Vec::new(),
        unset: BTreeSet::new(),
        volumes: BTreeMap::new(),
        secrets: BTreeMap::new(),
        configs: BTreeMap::new(),
    };
    c.interpolate(&mut doc, "")?;
    let top = doc
        .as_object()
        .ok_or("the compose file must be a mapping")?;

    for key in top.keys() {
        match key.as_str() {
            "version" | "name" | "services" | "volumes" | "secrets" | "configs" | "networks" => {}
            k if k.starts_with("x-") => {}
            k => c.warn(format!("{k}: not supported, ignored")),
        }
    }

    let mut objects = Vec::new();
    for (name, v) in entries(top.get("volumes")) {
        if let Some(o) = c.volume(name, v)? {
            objects.push(o);
        }
    }
    for (name, v) in entries(top.get("secrets")) {
        if let Some(o) = c.data(Kind::Secret, name, v)? {
            objects.push(o);
        }
    }
    for (name, v) in entries(top.get("configs")) {
        if let Some(o) = c.data(Kind::Config, name, v)? {
            objects.push(o);
        }
    }
    let networks = entries(top.get("networks")).count();
    let services = entries(top.get("services")).collect::<Vec<_>>();
    if services.is_empty() {
        return Err("services: the compose file has none".into());
    }
    let mut names = BTreeSet::new();
    for (name, v) in services {
        let o = c.service(name, v)?;
        if !names.insert(o.metadata.name.clone()) {
            return Err(format!(
                "services.{name}: another service is also named {}",
                o.metadata.name
            ));
        }
        objects.push(o);
    }
    if networks > 0 {
        c.warn(
            "networks: not separated yet; every service of the namespace reaches every other"
                .into(),
        );
    }
    for o in &mut objects {
        crate::admit(o).map_err(|e| format!("{}: {e}", o.key()))?;
    }
    Ok(Stack {
        objects,
        warnings: c.warnings,
    })
}

/// The entries of an optional mapping, null values as empty mappings.
fn entries(v: Option<&Value>) -> impl Iterator<Item = (&str, &Value)> {
    v.and_then(Value::as_object)
        .into_iter()
        .flat_map(|m| m.iter().map(|(k, v)| (k.as_str(), v)))
}

struct Converter<'a> {
    stack: String,
    namespace: String,
    inputs: &'a Inputs<'a>,
    warnings: Vec<String>,
    /// Variables already warned about.
    unset: BTreeSet<String>,
    /// Compose names of top-level volumes, secrets and configs to Legion's.
    volumes: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    configs: BTreeMap<String, String>,
}

impl Converter<'_> {
    fn warn(&mut self, w: String) {
        self.warnings.push(w);
    }

    fn object(&self, kind: Kind, name: &str, spec: Value) -> Object {
        let mut o = Object::new(kind, &self.namespace, name, spec);
        o.metadata
            .labels
            .insert(LABEL_STACK.to_owned(), self.stack.clone());
        o
    }

    /// A Legion name for a compose one: names are DNS labels, so `_` and
    /// `.` become `-` and letters lower case.
    fn name(&mut self, path: &str, raw: &str) -> Result<String, String> {
        let name: String = raw
            .chars()
            .map(|ch| match ch {
                '_' | '.' => '-',
                ch => ch.to_ascii_lowercase(),
            })
            .collect();
        let name = name.trim_matches('-').to_owned();
        crate::validate_name(&name).map_err(|e| format!("{path}: {raw:?}: {e}"))?;
        if name != raw {
            self.warn(format!("{path}: named {name} in Legion"));
        }
        Ok(name)
    }

    /// `${VAR}`, `${VAR:-default}`, `${VAR-default}`, `${VAR:?error}`,
    /// `${VAR?error}`, `$VAR` and `$$`, in every string value.
    fn interpolate(&mut self, v: &mut Value, path: &str) -> Result<(), String> {
        match v {
            Value::String(s) if s.contains('$') => {
                *s = self.expand(s).map_err(|e| format!("{path}: {e}"))?;
            }
            Value::Array(items) => {
                for (i, item) in items.iter_mut().enumerate() {
                    self.interpolate(item, &format!("{path}[{i}]"))?;
                }
            }
            Value::Object(map) => {
                for (k, item) in map.iter_mut() {
                    let path = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{path}.{k}")
                    };
                    self.interpolate(item, &path)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn expand(&mut self, s: &str) -> Result<String, String> {
        let mut out = String::new();
        let mut rest = s;
        while let Some(i) = rest.find('$') {
            out.push_str(&rest[..i]);
            rest = &rest[i + 1..];
            if let Some(r) = rest.strip_prefix('$') {
                out.push('$');
                rest = r;
            } else if let Some(r) = rest.strip_prefix('{') {
                let end = r.find('}').ok_or_else(|| format!("{s:?}: unclosed ${{"))?;
                let expr = &r[..end];
                rest = &r[end + 1..];
                let ops = [":-", "-", ":?", "?"];
                let (var, op, arg) = ops
                    .iter()
                    .filter_map(|op| expr.find(op).map(|at| (at, *op)))
                    .min_by_key(|(at, op)| (*at, std::cmp::Reverse(op.len())))
                    .map_or((expr, "", ""), |(at, op)| {
                        (&expr[..at], op, &expr[at + op.len()..])
                    });
                let value = (self.inputs.env)(var);
                let unset_or_empty = value.as_deref().is_none_or(str::is_empty);
                match op {
                    ":-" if unset_or_empty => out.push_str(arg),
                    "-" if value.is_none() => out.push_str(arg),
                    ":?" if unset_or_empty => return Err(format!("{var}: {arg}")),
                    "?" if value.is_none() => return Err(format!("{var}: {arg}")),
                    _ => out.push_str(&self.lookup(var, value)),
                }
            } else {
                let len = rest
                    .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                    .unwrap_or(rest.len());
                if len == 0 {
                    out.push('$');
                    continue;
                }
                let var = &rest[..len];
                rest = &rest[len..];
                let value = (self.inputs.env)(var);
                out.push_str(&self.lookup(var, value));
            }
        }
        out.push_str(rest);
        Ok(out)
    }

    fn lookup(&mut self, var: &str, value: Option<String>) -> String {
        value.unwrap_or_else(|| {
            if self.unset.insert(var.to_owned()) {
                self.warn(format!("${var} is not set: an empty string is used"));
            }
            String::new()
        })
    }

    fn volume(&mut self, key: &str, v: &Value) -> Result<Option<Object>, String> {
        let path = format!("volumes.{key}");
        let mut spec = json!({});
        for (k, val) in entries(Some(v)) {
            match k {
                "external" | "name" | "labels" => {}
                "driver" if val.as_str() == Some("local") => {}
                "x-legion" => {
                    if let Some(size) = val.get("size") {
                        spec["size"] = size.clone();
                    }
                }
                k => self.warn(format!("{path}.{k}: not supported, ignored")),
            }
        }
        let raw = v.get("name").and_then(Value::as_str).unwrap_or(key);
        let name = self.name(&path, raw)?;
        self.volumes.insert(key.to_owned(), name.clone());
        if external(v) {
            return Ok(None);
        }
        Ok(Some(self.object(Kind::Volume, &name, spec)))
    }

    fn data(&mut self, kind: Kind, key: &str, v: &Value) -> Result<Option<Object>, String> {
        let section = if kind == Kind::Secret {
            "secrets"
        } else {
            "configs"
        };
        let path = format!("{section}.{key}");
        let raw = v.get("name").and_then(Value::as_str).unwrap_or(key);
        let name = self.name(&path, raw)?;
        let map = if kind == Kind::Secret {
            &mut self.secrets
        } else {
            &mut self.configs
        };
        map.insert(key.to_owned(), name.clone());
        if external(v) {
            return Ok(None);
        }
        for k in entries(Some(v)).map(|(k, _)| k) {
            if !matches!(
                k,
                "file" | "environment" | "content" | "name" | "external" | "labels"
            ) {
                self.warn(format!("{path}.{k}: not supported, ignored"));
            }
        }
        let bytes = if let Some(file) = v.get("file").and_then(Value::as_str) {
            (self.inputs.read)(file).map_err(|e| format!("{path}.file: {file}: {e}"))?
        } else if let Some(var) = v.get("environment").and_then(Value::as_str) {
            (self.inputs.env)(var)
                .ok_or_else(|| format!("{path}.environment: {var} is not set"))?
                .into_bytes()
        } else if let Some(content) = v.get("content").and_then(Value::as_str) {
            content.as_bytes().to_vec()
        } else {
            return Err(format!(
                "{path}: needs `file`, `environment`, `content` or `external`"
            ));
        };
        let spec = json!({ "data": STANDARD.encode(bytes) });
        Ok(Some(self.object(kind, &name, spec)))
    }

    fn service(&mut self, key: &str, v: &Value) -> Result<Object, String> {
        let path = format!("services.{key}");
        let svc = v
            .as_object()
            .ok_or_else(|| format!("{path}: must be a mapping"))?;
        let name = self.name(&path, key)?;
        let image = svc
            .get("image")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{path}.image: required (Legion does not build images)"))?;

        let mut container = Map::new();
        container.insert("name".into(), json!(name));
        container.insert("image".into(), json!(image));
        let mut pod = Map::new();
        let mut spec = Map::new();
        let mut volumes: Vec<Value> = Vec::new();
        let mut mounts: Vec<Value> = Vec::new();
        let mut container_ports: Vec<Value> = Vec::new();
        let mut service_ports: Vec<Value> = Vec::new();
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        let mut labels = BTreeMap::new();

        for (k, val) in svc {
            let at = format!("{path}.{k}");
            match k.as_str() {
                "image" => {}
                "command" => {
                    container.insert("args".into(), json!(words(val, &at)?));
                }
                "entrypoint" => {
                    container.insert("command".into(), json!(words(val, &at)?));
                }
                "working_dir" => {
                    container.insert("workingDir".into(), val.clone());
                }
                "user" => {
                    container.insert("user".into(), json!(scalar(val)));
                }
                "env_file" => {
                    for file in strings(val) {
                        let text =
                            (self.inputs.read)(&file).map_err(|e| format!("{at}: {file}: {e}"))?;
                        let text = String::from_utf8_lossy(&text).into_owned();
                        for line in text.lines() {
                            let line = line.trim();
                            if line.is_empty() || line.starts_with('#') {
                                continue;
                            }
                            if let Some((k, v)) = line.split_once('=') {
                                env.insert(k.trim().to_owned(), v.to_owned());
                            } else if let Some(v) = (self.inputs.env)(line) {
                                env.insert(line.to_owned(), v);
                            }
                        }
                    }
                }
                "environment" => {} // after env_file, which it overrides
                "ports" => {
                    for (i, p) in val.as_array().into_iter().flatten().enumerate() {
                        let (port, published) = self.port(p, &format!("{at}[{i}]"))?;
                        add_port(&mut container_ports, &port);
                        service_ports.push(published);
                    }
                }
                "expose" => {
                    for p in val.as_array().into_iter().flatten() {
                        let s = scalar(p);
                        let (n, proto) = s.split_once('/').unwrap_or((&s, "tcp"));
                        let n: u16 = n
                            .parse()
                            .map_err(|_| format!("{at}: {s:?}: expected a port"))?;
                        add_port(
                            &mut container_ports,
                            &json!({ "containerPort": n, "protocol": proto }),
                        );
                    }
                }
                "volumes" => {
                    for (i, m) in val.as_array().into_iter().flatten().enumerate() {
                        self.mount(m, &format!("{at}[{i}]"), &mut volumes, &mut mounts)?;
                    }
                }
                "tmpfs" => {
                    for target in strings(val) {
                        let target = target.split(':').next().unwrap_or_default().to_owned();
                        let vname = format!("tmpfs-{}", volumes.len());
                        volumes.push(json!({ "name": vname, "tmpfs": {} }));
                        mounts.push(json!({ "name": vname, "path": target }));
                    }
                }
                "secrets" | "configs" => {
                    let refs = self.files(k, val, &at)?;
                    container.insert(k.clone(), Value::Array(refs));
                }
                "healthcheck" => {
                    if let Some(probe) = healthcheck(val, &at)? {
                        container.insert(
                            "probes".into(),
                            json!({ "liveness": probe, "readiness": probe }),
                        );
                    }
                }
                "stop_grace_period" => {
                    pod.insert("stopGracePeriod".into(), val.clone());
                }
                "deploy" => {
                    self.deploy(val, &at, &mut spec, &mut pod, &mut container, &mut labels)?
                }
                "networks" | "x-legion" => {}
                "depends_on" => self.warn(format!(
                    "{at}: ignored, as by docker stack deploy: services start in any order"
                )),
                "restart" => self.warn(format!(
                    "{at}: ignored, as by docker stack deploy: use deploy.restart_policy"
                )),
                "container_name" | "hostname" | "labels" | "logging" | "init" | "stop_signal"
                | "tty" | "stdin_open" | "read_only" | "shm_size" | "dns" | "dns_search"
                | "extra_hosts" | "domainname" | "mac_address" | "platform" | "profiles"
                | "pull_policy" | "external_links" | "links" | "ulimits" | "sysctls" => {
                    self.warn(format!("{at}: not supported, ignored"))
                }
                k if k.starts_with("x-") => {}
                _ => self.warn(format!(
                    "{at}: not supported (Legion cannot honour it), ignored"
                )),
            }
        }
        if let Some(val) = svc.get("environment") {
            match val {
                Value::Object(m) => {
                    for (k, v) in m {
                        match v {
                            Value::Null => {
                                if let Some(v) = (self.inputs.env)(k) {
                                    env.insert(k.clone(), v);
                                }
                            }
                            v => {
                                env.insert(k.clone(), scalar(v));
                            }
                        }
                    }
                }
                _ => {
                    for item in strings(val) {
                        match item.split_once('=') {
                            Some((k, v)) => {
                                env.insert(k.to_owned(), v.to_owned());
                            }
                            None => {
                                if let Some(v) = (self.inputs.env)(&item) {
                                    env.insert(item, v);
                                }
                            }
                        }
                    }
                }
            }
        }
        if !env.is_empty() {
            container.insert(
                "env".into(),
                Value::Array(
                    env.into_iter()
                        .map(|(name, value)| json!({ "name": name, "value": value }))
                        .collect(),
                ),
            );
        }
        if !container_ports.is_empty() {
            container.insert("ports".into(), Value::Array(container_ports));
        }
        if !mounts.is_empty() {
            container.insert("volumeMounts".into(), Value::Array(mounts));
        }
        pod.insert("containers".into(), json!([container]));
        if !volumes.is_empty() {
            pod.insert("volumes".into(), Value::Array(volumes));
        }
        spec.insert("pod".into(), Value::Object(pod));
        if !service_ports.is_empty() {
            let endpoint = spec
                .entry("endpoint")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .expect("an object");
            endpoint.insert("ports".into(), Value::Array(service_ports));
        }
        let mut spec = Value::Object(spec);
        if let Some(extra) = svc.get("x-legion") {
            merge(&mut spec, extra);
        }
        let mut o = self.object(Kind::Service, &name, spec);
        o.metadata.labels.extend(labels);
        Ok(o)
    }

    /// A `ports` entry: the container port and the service port.
    fn port(&mut self, p: &Value, at: &str) -> Result<(Value, Value), String> {
        let (target, published, protocol, mode) = match p {
            Value::Object(m) => {
                let target = m
                    .get("target")
                    .and_then(|t| scalar(t).parse::<u16>().ok())
                    .ok_or_else(|| format!("{at}.target: expected a port"))?;
                let published = m
                    .get("published")
                    .map(|p| {
                        scalar(p)
                            .parse::<u16>()
                            .map_err(|_| format!("{at}.published: expected a port"))
                    })
                    .transpose()?;
                let protocol = m.get("protocol").map_or("tcp".to_owned(), scalar);
                let mode = m.get("mode").map_or("ingress".to_owned(), scalar);
                (target, published, protocol, mode)
            }
            v => {
                let s = scalar(v);
                let (spec, protocol) = s.split_once('/').unwrap_or((&s, "tcp"));
                if spec.contains('-') {
                    return Err(format!("{at}: {s:?}: port ranges are not supported"));
                }
                let parts: Vec<&str> = spec.split(':').collect();
                let bad = || format!("{at}: {s:?}: expected [[ip:]published:]target");
                let (published, target) = match parts.as_slice() {
                    [t] => (None, *t),
                    [p, t] => (Some(*p), *t),
                    [_, p, t] => {
                        self.warn(format!("{at}: the host address is ignored"));
                        (Some(*p), *t)
                    }
                    _ => return Err(bad()),
                };
                let target: u16 = target.parse().map_err(|_| bad())?;
                let published = match published {
                    Some("") | None => None,
                    Some(p) => Some(p.parse::<u16>().map_err(|_| bad())?),
                };
                (target, published, protocol.to_owned(), "ingress".to_owned())
            }
        };
        let publish = match mode.as_str() {
            "ingress" => "ingress",
            "host" => "host",
            m => return Err(format!("{at}.mode: {m:?}: expected ingress or host")),
        };
        let mut service = json!({
            "port": target,
            "targetPort": target,
            "protocol": protocol,
            "publish": publish,
        });
        if let Some(p) = published {
            service["publishedPort"] = json!(p);
        }
        Ok((
            json!({ "containerPort": target, "protocol": protocol }),
            service,
        ))
    }

    /// A `volumes` entry of a service.
    fn mount(
        &mut self,
        m: &Value,
        at: &str,
        volumes: &mut Vec<Value>,
        mounts: &mut Vec<Value>,
    ) -> Result<(), String> {
        let (kind, source, target, read_only, size) = match m {
            Value::Object(o) => {
                let kind = o.get("type").map_or("volume".to_owned(), scalar);
                let source = o.get("source").map(scalar).unwrap_or_default();
                let target = o
                    .get("target")
                    .map(scalar)
                    .ok_or_else(|| format!("{at}.target: required"))?;
                let read_only = o.get("read_only").and_then(Value::as_bool).unwrap_or(false);
                let size = o.get("tmpfs").and_then(|t| t.get("size")).cloned();
                (kind, source, target, read_only, size)
            }
            v => {
                let s = scalar(v);
                let parts: Vec<&str> = s.split(':').collect();
                let (source, target, mode) = match parts.as_slice() {
                    [t] => ("", *t, ""),
                    [s, t] => (*s, *t, ""),
                    [s, t, m] => (*s, *t, *m),
                    _ => return Err(format!("{at}: {s:?}: expected source:target[:mode]")),
                };
                let kind = if source.is_empty() {
                    "anonymous"
                } else if source.starts_with(['/', '.', '~']) {
                    "bind"
                } else {
                    "volume"
                };
                let read_only = mode.split(',').any(|m| m == "ro");
                (
                    kind.to_owned(),
                    source.to_owned(),
                    target.to_owned(),
                    read_only,
                    None,
                )
            }
        };
        match kind.as_str() {
            "volume" if !source.is_empty() => {
                let name = self
                    .volumes
                    .get(&source)
                    .cloned()
                    .ok_or_else(|| format!("{at}: volume {source} is not declared under volumes"))?;
                if !volumes.iter().any(|v| v["name"] == json!(name)) {
                    volumes.push(json!({ "name": name, "volume": { "name": name } }));
                }
                mounts.push(json!({ "name": name, "path": target, "readOnly": read_only }));
            }
            "tmpfs" => {
                let vname = format!("tmpfs-{}", volumes.len());
                let tmpfs = match size {
                    Some(size) => json!({ "size": docker_bytes(&size, at)? }),
                    None => json!({}),
                };
                volumes.push(json!({ "name": vname, "tmpfs": tmpfs }));
                mounts.push(json!({ "name": vname, "path": target }));
            }
            "bind" => self.warn(format!(
                "{at}: bind mounts of host paths are not supported, {target} is not mounted"
            )),
            _ => self.warn(format!(
                "{at}: anonymous volumes are not supported, {target} is not mounted; name the volume"
            )),
        }
        Ok(())
    }

    /// A service's `secrets` or `configs`. Targets are as in Swarm:
    /// `/run/secrets/<source>` and `/<source>` unless given.
    fn files(&mut self, section: &str, val: &Value, at: &str) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        for (i, item) in val.as_array().into_iter().flatten().enumerate() {
            let (source, target) = match item {
                Value::Object(m) => {
                    for k in m.keys() {
                        if !matches!(k.as_str(), "source" | "target") {
                            self.warn(format!("{at}[{i}].{k}: not supported, ignored"));
                        }
                    }
                    let source = m
                        .get("source")
                        .map(scalar)
                        .ok_or_else(|| format!("{at}[{i}].source: required"))?;
                    (source, m.get("target").map(scalar))
                }
                v => (scalar(v), None),
            };
            let names = if section == "secrets" {
                &self.secrets
            } else {
                &self.configs
            };
            let name = names
                .get(&source)
                .cloned()
                .ok_or_else(|| format!("{at}[{i}]: {source} is not declared under {section}"))?;
            let target = match (section, target) {
                (_, Some(t)) if t.starts_with('/') => t,
                ("secrets", Some(t)) => format!("/run/secrets/{t}"),
                ("secrets", None) => format!("/run/secrets/{source}"),
                (_, Some(t)) => format!("/{t}"),
                (_, None) => format!("/{source}"),
            };
            out.push(json!({ "name": name, "target": target }));
        }
        Ok(out)
    }

    fn deploy(
        &mut self,
        val: &Value,
        at: &str,
        spec: &mut Map<String, Value>,
        pod: &mut Map<String, Value>,
        container: &mut Map<String, Value>,
        labels: &mut BTreeMap<String, String>,
    ) -> Result<(), String> {
        for (k, v) in entries(Some(val)) {
            let at = format!("{at}.{k}");
            match k {
                "mode" => match v.as_str() {
                    Some(m @ ("replicated" | "global")) => {
                        spec.insert("mode".into(), json!(m));
                    }
                    _ => return Err(format!("{at}: {v}: expected replicated or global")),
                },
                "replicas" => {
                    spec.insert("replicas".into(), v.clone());
                }
                "endpoint_mode" => {
                    spec.insert("endpoint".into(), json!({ "mode": v }));
                }
                "labels" => match v {
                    Value::Object(m) => {
                        for (k, v) in m {
                            labels.insert(k.clone(), scalar(v));
                        }
                    }
                    v => {
                        for item in strings(v) {
                            let (k, v) = item.split_once('=').unwrap_or((&item, ""));
                            labels.insert(k.to_owned(), v.to_owned());
                        }
                    }
                },
                "placement" => {
                    let mut placement = Map::new();
                    for (pk, pv) in entries(Some(v)) {
                        match pk {
                            "constraints" => {
                                placement.insert("constraints".into(), json!(strings(pv)));
                            }
                            "preferences" => {
                                placement.insert("preferences".into(), pv.clone());
                            }
                            pk => self.warn(format!("{at}.{pk}: not supported, ignored")),
                        }
                    }
                    spec.insert("placement".into(), Value::Object(placement));
                }
                "resources" => {
                    let mut resources = Map::new();
                    for (rk, legion) in [("limits", "limits"), ("reservations", "requests")] {
                        let mut out = Map::new();
                        for (qk, qv) in entries(v.get(rk)) {
                            match qk {
                                "cpus" => {
                                    out.insert("cpu".into(), json!(scalar(qv)));
                                }
                                "memory" => {
                                    out.insert(
                                        "memory".into(),
                                        json!(docker_bytes(qv, &format!("{at}.{rk}.memory"))?),
                                    );
                                }
                                "pids" if rk == "limits" => {
                                    out.insert("maxproc".into(), qv.clone());
                                }
                                qk => self.warn(format!("{at}.{rk}.{qk}: not supported, ignored")),
                            }
                        }
                        if !out.is_empty() {
                            resources.insert(legion.into(), Value::Object(out));
                        }
                    }
                    if !resources.is_empty() {
                        container.insert("resources".into(), Value::Object(resources));
                    }
                }
                "restart_policy" => {
                    let mut policy = Map::new();
                    for (rk, rv) in entries(Some(v)) {
                        match rk {
                            "condition" => {
                                policy.insert("condition".into(), rv.clone());
                            }
                            "delay" => {
                                policy.insert("delay".into(), rv.clone());
                            }
                            "max_attempts" => {
                                policy.insert("maxAttempts".into(), rv.clone());
                            }
                            rk => self.warn(format!("{at}.{rk}: not supported, ignored")),
                        }
                    }
                    pod.insert("restartPolicy".into(), Value::Object(policy));
                }
                "update_config" => {
                    let mut update = Map::new();
                    for (uk, uv) in entries(Some(v)) {
                        let key = match uk {
                            "parallelism" | "delay" | "monitor" | "order" => uk,
                            "failure_action" => "failureAction",
                            "max_failure_ratio" if uv.as_f64() == Some(0.0) => continue,
                            uk => {
                                self.warn(format!("{at}.{uk}: not supported, ignored"));
                                continue;
                            }
                        };
                        update.insert(key.into(), uv.clone());
                    }
                    spec.insert("update".into(), Value::Object(update));
                }
                "rollback_config" => self.warn(format!(
                    "{at}: not supported, ignored: a rollback uses the update_config of the spec it returns to"
                )),
                _ => self.warn(format!("{at}: not supported, ignored")),
            }
        }
        Ok(())
    }
}

fn external(v: &Value) -> bool {
    match v.get("external") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Object(_)) => true,
        _ => false,
    }
}

/// A scalar as a string: compose writes ports and numbers either way.
fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        v => v.to_string(),
    }
}

/// A string or a list of strings.
fn strings(v: &Value) -> Vec<String> {
    match v {
        Value::Array(items) => items.iter().map(scalar).collect(),
        Value::Null => Vec::new(),
        v => vec![scalar(v)],
    }
}

/// A command: a list, or a string split as a shell would (quotes and
/// backslashes, no expansion).
fn words(v: &Value, at: &str) -> Result<Vec<String>, String> {
    let Value::String(s) = v else {
        return Ok(strings(v));
    };
    let mut out = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = s.chars();
    let mut quote: Option<char> = None;
    while let Some(ch) = chars.next() {
        match (quote, ch) {
            (Some(q), ch) if ch == q => quote = None,
            (Some('"'), '\\') | (None, '\\') => {
                if let Some(next) = chars.next() {
                    word.push(next);
                }
                in_word = true;
            }
            (Some(_), ch) => word.push(ch),
            (None, '\'' | '"') => {
                quote = Some(ch);
                in_word = true;
            }
            (None, ch) if ch.is_whitespace() => {
                if in_word {
                    out.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            (None, ch) => {
                word.push(ch);
                in_word = true;
            }
        }
    }
    if quote.is_some() {
        return Err(format!("{at}: {s:?}: unclosed quote"));
    }
    if in_word {
        out.push(word);
    }
    Ok(out)
}

fn add_port(ports: &mut Vec<Value>, port: &Value) {
    if !ports.contains(port) {
        ports.push(port.clone());
    }
}

/// Docker's byte sizes (`512m`, `1G`, `100kb`) are binary.
fn docker_bytes(v: &Value, at: &str) -> Result<String, String> {
    let s = scalar(v).to_ascii_lowercase();
    let s = s.trim();
    let digits = s
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(s.len());
    let n: f64 = s[..digits]
        .parse()
        .map_err(|_| format!("{at}: {s:?}: expected a size such as 512M"))?;
    let unit: u64 = match s[digits..].trim_end_matches('b') {
        "" => 1,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        "t" => 1 << 40,
        _ => return Err(format!("{at}: {s:?}: expected a size such as 512M")),
    };
    Ok(((n * unit as f64) as u64).to_string())
}

/// A healthcheck as a probe: run in the container, as Docker does.
fn healthcheck(v: &Value, at: &str) -> Result<Option<Value>, String> {
    if v.get("disable").and_then(Value::as_bool) == Some(true) {
        return Ok(None);
    }
    let Some(test) = v.get("test") else {
        return Ok(None);
    };
    let command: Vec<String> = match test {
        Value::String(s) => vec!["/bin/sh".into(), "-c".into(), s.clone()],
        test => {
            let parts = strings(test);
            match parts.first().map(String::as_str) {
                Some("NONE") => return Ok(None),
                Some("CMD") => parts[1..].to_vec(),
                Some("CMD-SHELL") => {
                    vec!["/bin/sh".into(), "-c".into(), parts[1..].join(" ")]
                }
                _ => return Err(format!("{at}.test: expected CMD, CMD-SHELL or NONE first")),
            }
        }
    };
    let mut probe = json!({ "exec": { "command": command } });
    for (k, legion) in [
        ("interval", "period"),
        ("timeout", "timeout"),
        ("start_period", "initialDelay"),
        ("retries", "failureThreshold"),
    ] {
        if let Some(val) = v.get(k) {
            probe[legion] = val.clone();
        }
    }
    // Docker's defaults where Legion's differ.
    for (legion, default) in [("period", "30s"), ("timeout", "30s")] {
        if probe.get(legion).is_none() {
            probe[legion] = json!(default);
        }
    }
    if probe.get("failureThreshold").is_none() {
        probe["failureThreshold"] = json!(3);
    }
    Ok(Some(probe))
}

/// Merges `extra` into `base`: mappings key by key, anything else replaced.
fn merge(base: &mut Value, extra: &Value) {
    match (base, extra) {
        (Value::Object(b), Value::Object(e)) => {
            for (k, v) in e {
                merge(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, e) => *b = e.clone(),
    }
}

#[cfg(test)]
#[path = "compose_tests.rs"]
mod tests;
