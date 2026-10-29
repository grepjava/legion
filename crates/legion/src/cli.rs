//! CLI commands: thin gRPC calls plus formatting.

use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use legion_api::service::ServiceStatus;
use legion_api::{API_VERSION, Kind, Object};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    self as pb, ApplyRequest, ApplyResult, DeleteRequest, InitRequest, ListRequest, ObjectRef,
    StatusRequest, StatusResponse, WatchRequest, watch_event,
};
use tonic::transport::Channel;

use crate::{Command, StackCommand, UserCommand};

/// How long a command keeps asking while the API is unavailable: during
/// an election, or while a manager waits on a quorum (DESIGN.md §3.8).
const RETRY_FOR: Duration = Duration::from_secs(30);

/// Calls `call` again while it fails as unavailable, for up to
/// [`RETRY_FOR`].
async fn retrying<T, Fut>(mut call: impl FnMut() -> Fut) -> Result<T, tonic::Status>
where
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let deadline = tokio::time::Instant::now() + RETRY_FOR;
    loop {
        match call().await {
            Err(s)
                if s.code() == tonic::Code::Unavailable
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            r => return r,
        }
    }
}

/// The Objects API, its single calls retried while it is unavailable.
/// Applying again is harmless; a delete that went through before its
/// answer was lost finds nothing the second time, which counts as done.
#[derive(Clone)]
struct Objects(ObjectsClient<Channel>);

type Answer<T> = Result<tonic::Response<T>, tonic::Status>;

impl Objects {
    async fn apply(&mut self, req: ApplyRequest) -> Answer<pb::ApplyResponse> {
        let client = &self.0;
        retrying(|| {
            let (mut c, req) = (client.clone(), req.clone());
            async move { c.apply(req).await }
        })
        .await
    }

    /// Sets `field` of an object's spec to `value`, over the object as just
    /// read: another write meanwhile (a node's heartbeat, a Volume's
    /// migration) has it read again. Returns the object's key, and whether
    /// the field changed.
    async fn set_spec(
        &mut self,
        r#ref: ObjectRef,
        field: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<(String, bool)> {
        loop {
            let current = self.get(r#ref.clone()).await?.into_inner();
            let mut object = Object::try_from(current)?;
            if object.spec[field] == value {
                return Ok((object.key().to_string(), false));
            }
            object.spec[field] = value.clone();
            match self
                .apply(ApplyRequest {
                    object: Some((&object).into()),
                    expected_version: Some(object.metadata.version),
                    ..Default::default()
                })
                .await
            {
                Ok(_) => return Ok((object.key().to_string(), true)),
                Err(s) if s.code() == tonic::Code::Aborted => continue,
                Err(s) => return Err(s.into()),
            }
        }
    }

    async fn get(&mut self, req: ObjectRef) -> Answer<pb::Object> {
        let client = &self.0;
        retrying(|| {
            let (mut c, req) = (client.clone(), req.clone());
            async move { c.get(req).await }
        })
        .await
    }

    async fn list(&mut self, req: ListRequest) -> Answer<pb::ListResponse> {
        let client = &self.0;
        retrying(|| {
            let (mut c, req) = (client.clone(), req.clone());
            async move { c.list(req).await }
        })
        .await
    }

    async fn delete(&mut self, req: DeleteRequest) -> Answer<pb::DeleteResponse> {
        let deadline = tokio::time::Instant::now() + RETRY_FOR;
        let mut retried = false;
        loop {
            match self.0.delete(req.clone()).await {
                Err(s)
                    if s.code() == tonic::Code::Unavailable
                        && tokio::time::Instant::now() < deadline =>
                {
                    retried = true;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(s) if s.code() == tonic::Code::NotFound && retried => {
                    return Ok(tonic::Response::new(pb::DeleteResponse::default()));
                }
                r => return r,
            }
        }
    }

    async fn watch(
        &mut self,
        req: WatchRequest,
    ) -> Answer<tonic::codec::Streaming<pb::WatchEvent>> {
        self.0.watch(req).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Table,
    Yaml,
    Json,
}

pub async fn run(server: &str, identity: &Path, command: Command) -> anyhow::Result<()> {
    // Needs no cluster.
    if let Command::Stack {
        command:
            StackCommand::Convert {
                stack,
                file,
                namespace,
            },
    } = command
    {
        return print_converted(&file, &stack, namespace);
    }
    let channel = connect(server, identity).await?;
    let mut objects = Objects(ObjectsClient::new(channel.clone()));
    let mut cluster = ClusterClient::new(channel.clone());
    match command {
        Command::Daemon { .. } => unreachable!("handled in main"),
        Command::Init => {
            let status = cluster.init(InitRequest {}).await?.into_inner();
            println!(
                "cluster initialized; this manager is node {}",
                status.node_id
            );
        }
        Command::Status => {
            let status = retrying(|| {
                let mut cluster = cluster.clone();
                async move { cluster.status(StatusRequest {}).await }
            })
            .await?;
            print_status(&status.into_inner())
        }
        Command::JoinToken { role, rotate } => {
            let token = cluster
                .join_token(pb::JoinTokenRequest { role, rotate })
                .await?
                .into_inner()
                .token;
            println!("{token}");
        }
        Command::Backup { output } => {
            let mut chunks = cluster.backup(pb::BackupRequest {}).await?.into_inner();
            let mut json = Vec::new();
            while let Some(chunk) = chunks.message().await? {
                json.extend_from_slice(&chunk.data);
            }
            let backup: legion_store::Backup =
                serde_json::from_slice(&json).context("reading the backup")?;
            crate::pki::write_private(&output, &String::from_utf8(json)?)?;
            println!(
                "backed up {} objects at version {} to {}",
                backup.objects.len(),
                backup.version,
                output.display()
            );
        }
        Command::Autolock { action } => {
            let key = cluster
                .autolock(pb::AutolockRequest {
                    action: action.clone(),
                })
                .await?
                .into_inner()
                .key;
            match (action.as_str(), key.is_empty()) {
                (_, true) => println!("autolock is off"),
                ("show", false) => println!("{key}"),
                (_, false) => {
                    println!("{key}");
                    eprintln!(
                        "keep this key safe: a manager restarted from now on starts only \
                         after `legion unlock` with it"
                    );
                }
            }
        }
        Command::Join { .. }
        | Command::Restore { .. }
        | Command::Unlock { .. }
        | Command::Image { .. } => {
            unreachable!("handled in main")
        }
        Command::Logs {
            task,
            namespace,
            container,
            follow,
            tail,
        } => {
            let target = target(task, namespace, container);
            crate::session::logs(&channel, &target, follow, tail).await?;
        }
        Command::Exec {
            task,
            namespace,
            container,
            interactive,
            tty,
            command,
        } => {
            let target = target(task, namespace, container);
            let code = crate::session::exec(&channel, &target, command, interactive, tty).await?;
            // Without waiting for a stdin read that may never return.
            std::process::exit(code);
        }
        Command::PortForward {
            task,
            ports,
            namespace,
            address,
        } => {
            let ports = ports
                .iter()
                .map(|p| crate::session::parse_ports(p))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let target = target(task, namespace, None);
            crate::session::port_forward(&channel, target, &address, &ports).await?;
        }
        Command::User {
            command:
                UserCommand::Create {
                    name,
                    role,
                    namespace,
                    output,
                    days,
                },
        } => {
            let path = output.unwrap_or_else(|| format!("{name}.pem").into());
            if path.exists() {
                bail!("{} exists; choose another with -o", path.display());
            }
            let (key_pem, csr_pem) = legion_pki::request()?;
            let issued = cluster
                .issue_user(pb::IssueUserRequest {
                    name: name.clone(),
                    csr_pem,
                    days,
                })
                .await?
                .into_inner();
            let bundle = legion_pki::Bundle {
                cert_pem: issued.cert_pem,
                key_pem,
                ca_pem: issued.ca_pem,
            };
            crate::pki::write_private(&path, &bundle.to_pem())?;
            println!(
                "wrote {}: user {name}'s credentials for {days} days; hand it to them for --identity",
                path.display()
            );
            if let (Some(role), Some(namespace)) = (role, namespace) {
                let spec = serde_json::json!({ "user": name, "role": role });
                let binding = Object::new(Kind::RoleBinding, &namespace, &name, spec);
                apply(&mut objects, &binding).await?;
            }
        }
        Command::Apply { files } => {
            for file in files {
                for object in read_objects(&file)? {
                    apply(&mut objects, &object).await?;
                }
            }
        }
        Command::Stack { command } => stack(&mut objects, command).await?,
        Command::Diff { files } => {
            let mut changed = false;
            for file in files {
                for object in read_objects(&file)? {
                    changed |= diff(&mut objects, &object).await?;
                }
            }
            if changed {
                std::process::exit(1);
            }
        }
        Command::Get {
            kind,
            name,
            namespace,
            output,
        } => {
            let kind = parse_kind(&kind)?;
            let o = objects
                .get(ObjectRef {
                    kind: kind.to_string(),
                    namespace: namespace.unwrap_or_default(),
                    name,
                })
                .await?
                .into_inner();
            print_objects(&[o], output, kind)?;
        }
        Command::List {
            kind,
            namespace,
            all_namespaces,
            selector,
            output,
        } => {
            let kind = parse_kind(&kind)?;
            let namespace = match (kind.is_cluster_scoped(), all_namespaces, namespace) {
                (true, _, _) | (false, true, _) => String::new(),
                (false, false, ns) => ns.unwrap_or_else(|| "default".into()),
            };
            let resp = objects
                .list(ListRequest {
                    kind: kind.to_string(),
                    namespace,
                    match_labels: parse_selector(&selector)?,
                })
                .await?
                .into_inner();
            print_objects(&resp.objects, output, kind)?;
        }
        Command::Delete {
            kind,
            name,
            namespace,
        } => {
            let kind = parse_kind(&kind)?;
            let resp = objects
                .delete(DeleteRequest {
                    r#ref: Some(ObjectRef {
                        kind: kind.to_string(),
                        namespace: namespace.unwrap_or_default(),
                        name: name.clone(),
                    }),
                    expected_version: None,
                })
                .await?
                .into_inner();
            println!("{kind}/{name} deleted (version {})", resp.version);
        }
        Command::Node { availability, node } => {
            let availability = match availability.as_str() {
                "activate" => "active",
                other => other,
            };
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node,
            };
            match objects
                .set_spec(r#ref, "availability", availability.into())
                .await?
            {
                (key, true) => println!("{key} {availability}"),
                (key, false) => println!("{key} is already {availability}"),
            }
        }
        Command::Volume {
            command:
                crate::VolumeCommand::Move {
                    volume,
                    node,
                    namespace,
                },
        } => {
            let r#ref = ObjectRef {
                kind: Kind::Volume.to_string(),
                namespace: namespace.unwrap_or_default(),
                name: volume,
            };
            match objects.set_spec(r#ref, "node", node.clone().into()).await? {
                (key, true) => println!("{key} moves to {node}"),
                (key, false) => println!("{key} is already asked to be on {node}"),
            }
        }
        Command::Rollback { service, namespace } => {
            let r#ref = ObjectRef {
                kind: Kind::Service.to_string(),
                namespace: namespace.unwrap_or_default(),
                name: service,
            };
            // The generation the rollback undoes the latest update of.
            let mut generation = None;
            loop {
                let current = objects.get(r#ref.clone()).await?.into_inner();
                let mut object = Object::try_from(current)?;
                if generation.is_some_and(|g| g != object.metadata.generation) {
                    bail!("{} changed meanwhile; nothing rolled back", object.key());
                }
                generation = Some(object.metadata.generation);
                let status: ServiceStatus =
                    serde_json::from_value(object.status.clone()).unwrap_or_default();
                let Some(update) = status.update else {
                    bail!("{} has had no update to roll back", object.key());
                };
                if serde_json::to_value(&update.previous)? == object.spec {
                    bail!(
                        "{} already runs the spec before its latest update",
                        object.key()
                    );
                }
                object.spec = serde_json::to_value(&update.previous)?;
                // Only over the spec just read: a newer change wins. The
                // controllers write the status meanwhile, which moves the
                // version too; then it is read again.
                let resp = match objects
                    .apply(ApplyRequest {
                        object: Some((&object).into()),
                        expected_version: Some(object.metadata.version),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(resp) => resp.into_inner(),
                    Err(s) if s.code() == tonic::Code::Aborted => continue,
                    Err(s) => return Err(s.into()),
                };
                println!(
                    "{} rolling back (version {})",
                    object.key(),
                    resp.object.unwrap_or_default().version
                );
                break;
            }
        }
        Command::Watch { kind, from } => {
            let kind = kind.as_deref().map(parse_kind).transpose()?;
            let from_version = match from {
                Some(v) => v,
                None => {
                    cluster
                        .status(StatusRequest {})
                        .await?
                        .into_inner()
                        .applied_version
                }
            };
            let mut stream = objects
                .watch(WatchRequest {
                    kind: kind.map(|k| k.to_string()).unwrap_or_default(),
                    from_version,
                })
                .await?
                .into_inner();
            while let Some(ev) = stream.message().await? {
                let verb = match ev.r#type() {
                    watch_event::Type::Put => "PUT",
                    watch_event::Type::Delete => "DELETE",
                    watch_event::Type::Unspecified => "?",
                };
                let o = ev.object.unwrap_or_default();
                println!(
                    "{:<8} {:>8}  {}/{}",
                    verb,
                    o.version,
                    o.kind,
                    display_name(&o)
                );
            }
        }
    }
    Ok(())
}

async fn apply(objects: &mut Objects, object: &Object) -> anyhow::Result<()> {
    let key = object.key();
    let resp = objects
        .apply(ApplyRequest {
            object: Some(object.into()),
            ..Default::default()
        })
        .await
        .with_context(|| format!("applying {key}"))?
        .into_inner();
    let verb = match resp.result() {
        ApplyResult::Created => "created",
        ApplyResult::Updated => "updated",
        ApplyResult::Unchanged => "unchanged",
        ApplyResult::Unspecified => "applied",
    };
    let o = resp.object.unwrap_or_default();
    println!(
        "{}/{} {verb} (version {})",
        o.kind,
        display_name(&o),
        o.version
    );
    Ok(())
}

async fn stack(objects: &mut Objects, command: StackCommand) -> anyhow::Result<()> {
    match command {
        StackCommand::Deploy {
            stack,
            file,
            namespace,
            prune,
            strict,
        } => {
            let namespace = namespace.unwrap_or_else(|| stack.clone());
            let converted = convert(&file, &stack, &namespace)?;
            if strict && !converted.warnings.is_empty() {
                bail!("{} cannot be deployed as it is (--strict)", file.display());
            }
            // Made only when missing, so its isolation stays as set. A user
            // may not even look: one with a role in it deploys into it as
            // the cluster admin made it.
            let found = objects
                .get(ObjectRef {
                    kind: Kind::Namespace.as_str().into(),
                    namespace: String::new(),
                    name: namespace.clone(),
                })
                .await;
            match found {
                Err(s) if s.code() == tonic::Code::NotFound => {
                    let ns = Object::new(Kind::Namespace, "", &namespace, serde_json::json!({}));
                    apply(objects, &ns).await?;
                }
                Err(s) if s.code() != tonic::Code::PermissionDenied => {
                    return Err(anyhow::Error::from(s).context(format!("namespace {namespace}")));
                }
                _ => {}
            }
            for object in &converted.objects {
                apply(objects, object).await?;
            }
            if prune {
                let keep: Vec<_> = converted.objects.iter().map(Object::key).collect();
                for o in stack_objects(objects, &stack, &namespace, &[Kind::Service]).await? {
                    if !keep.contains(&o.key()) {
                        delete(objects, &o).await?;
                    }
                }
            }
        }
        StackCommand::Convert { .. } => unreachable!("handled in run"),
        StackCommand::Rm { stack, namespace } => {
            let namespace = namespace.unwrap_or_else(|| stack.clone());
            let kinds = [Kind::Service, Kind::Secret, Kind::Config];
            let found = stack_objects(objects, &stack, &namespace, &kinds).await?;
            if found.is_empty() {
                bail!("no stack {stack} in namespace {namespace}");
            }
            for o in found {
                delete(objects, &o).await?;
            }
        }
    }
    Ok(())
}

/// `stack convert`: the objects as Legion YAML documents.
fn print_converted(file: &Path, stack: &str, namespace: Option<String>) -> anyhow::Result<()> {
    let namespace = namespace.unwrap_or_else(|| stack.to_owned());
    let converted = convert(file, stack, &namespace)?;
    for o in &converted.objects {
        print!(
            "---\n{}",
            serde_yaml_ng::to_string(&serde_json::json!({
                "apiVersion": o.api_version,
                "kind": o.kind,
                "metadata": {
                    "name": o.metadata.name,
                    "namespace": o.metadata.namespace,
                    "labels": o.metadata.labels,
                },
                "spec": o.spec,
            }))?
        );
    }
    Ok(())
}

/// Converts a compose file, printing what Legion cannot honour.
fn convert(
    file: &Path,
    stack: &str,
    namespace: &str,
) -> anyhow::Result<legion_api::compose::Stack> {
    let text = if file == Path::new("-") {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        s
    } else {
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?
    };
    let dir = file.parent().map(Path::to_path_buf).unwrap_or_default();
    let env = |k: &str| std::env::var(k).ok();
    let read = |p: &str| std::fs::read(dir.join(p)).map_err(|e| e.to_string());
    let converted = legion_api::compose::convert(
        &text,
        stack,
        namespace,
        &legion_api::compose::Inputs {
            env: &env,
            read: &read,
        },
    )
    .map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
    for w in &converted.warnings {
        eprintln!("warning: {w}");
    }
    Ok(converted)
}

/// The objects of `kinds` that carry the stack's label.
async fn stack_objects(
    objects: &mut Objects,
    stack: &str,
    namespace: &str,
    kinds: &[Kind],
) -> anyhow::Result<Vec<Object>> {
    let mut out = Vec::new();
    for kind in kinds {
        let resp = objects
            .list(ListRequest {
                kind: kind.to_string(),
                namespace: namespace.to_owned(),
                match_labels: [(
                    legion_api::compose::LABEL_STACK.to_owned(),
                    stack.to_owned(),
                )]
                .into(),
            })
            .await?
            .into_inner();
        for o in resp.objects {
            out.push(Object::try_from(o)?);
        }
    }
    Ok(out)
}

async fn delete(objects: &mut Objects, o: &Object) -> anyhow::Result<()> {
    objects
        .delete(DeleteRequest {
            r#ref: Some(ObjectRef {
                kind: o.kind.to_string(),
                namespace: o.metadata.namespace.clone(),
                name: o.metadata.name.clone(),
            }),
            expected_version: None,
        })
        .await
        .with_context(|| format!("deleting {}", o.key()))?;
    println!("{} deleted", o.key());
    Ok(())
}

/// Prints how applying `object` would change the stored one; true if it would.
async fn diff(objects: &mut Objects, object: &Object) -> anyhow::Result<bool> {
    let key = object.key();
    let resp = objects
        .apply(ApplyRequest {
            object: Some(object.into()),
            dry_run: true,
            ..Default::default()
        })
        .await
        .with_context(|| format!("checking {key}"))?
        .into_inner();
    let result = resp.result();
    let after = resp.object.unwrap_or_default();
    let name = format!("{}/{}", after.kind, display_name(&after));
    let before = match result {
        ApplyResult::Unchanged => return Ok(false),
        ApplyResult::Created => String::new(),
        _ => {
            let live = objects
                .get(ObjectRef {
                    kind: after.kind.clone(),
                    namespace: after.namespace.clone(),
                    name: after.name.clone(),
                })
                .await
                .with_context(|| format!("reading {name}"))?
                .into_inner();
            manifest_yaml(live)?
        }
    };
    let after = manifest_yaml(after)?;
    let text = similar::TextDiff::from_lines(&before, &after);
    print!(
        "{}",
        text.unified_diff()
            .context_radius(3)
            .header(&format!("live {name}"), &format!("applied {name}"))
    );
    Ok(true)
}

/// The parts of an object a manifest sets, as YAML.
fn manifest_yaml(o: pb::Object) -> anyhow::Result<String> {
    let o = Object::try_from(o)?;
    let mut metadata = serde_json::json!({ "name": o.metadata.name });
    if !o.metadata.namespace.is_empty() {
        metadata["namespace"] = o.metadata.namespace.into();
    }
    if !o.metadata.labels.is_empty() {
        metadata["labels"] = serde_json::to_value(&o.metadata.labels)?;
    }
    Ok(serde_yaml_ng::to_string(&serde_json::json!({
        "apiVersion": o.api_version,
        "kind": o.kind,
        "metadata": metadata,
        "spec": o.spec,
    }))?)
}

fn target(
    task: String,
    namespace: Option<String>,
    container: Option<String>,
) -> crate::session::Target {
    crate::session::Target {
        namespace: namespace.unwrap_or_else(|| "default".into()),
        task,
        container: container.unwrap_or_default(),
    }
}

async fn connect(server: &str, identity: &Path) -> anyhow::Result<Channel> {
    let pem = std::fs::read_to_string(identity).with_context(|| {
        format!(
            "reading credentials from {} (pass --identity or set LEGION_IDENTITY; \
             the daemon writes <data-dir>/admin.pem)",
            identity.display()
        )
    })?;
    let bundle = legion_pki::Bundle::parse(&pem)
        .with_context(|| format!("reading credentials from {}", identity.display()))?;
    let tls = legion_manager::client_config(&bundle.ca_pem, &bundle.cert_pem, &bundle.key_pem);
    let addr = server
        .strip_prefix("https://")
        .or_else(|| server.strip_prefix("http://"))
        .unwrap_or(server);
    let url = format!("https://{addr}");
    Channel::from_shared(url.clone())
        .with_context(|| format!("bad server address {server:?}"))?
        .tls_config(tls)?
        .connect()
        .await
        .with_context(|| format!("connecting to {url}"))
}

/// Accepts `Service`, `service`, `services` and short forms such as `svc`.
fn parse_kind(s: &str) -> anyhow::Result<Kind> {
    let lower = s.to_ascii_lowercase();
    let alias = match lower.as_str() {
        "svc" => Some(Kind::Service),
        "ns" => Some(Kind::Namespace),
        "netpol" => Some(Kind::NetworkPolicy),
        "rb" => Some(Kind::RoleBinding),
        "cj" => Some(Kind::CronJob),
        _ => None,
    };
    if let Some(kind) = alias {
        return Ok(kind);
    }
    let singular = lower.strip_suffix('s').unwrap_or(&lower);
    Kind::ALL
        .into_iter()
        .find(|k| {
            let name = k.as_str().to_ascii_lowercase();
            name == lower || name == singular
        })
        .with_context(|| {
            let kinds: Vec<_> = Kind::ALL.iter().map(|k| k.as_str()).collect();
            format!("unknown kind {s:?}; one of {}", kinds.join(", "))
        })
}

fn parse_selector(
    selector: &[String],
) -> anyhow::Result<std::collections::HashMap<String, String>> {
    selector
        .iter()
        .map(|s| match s.split_once('=') {
            Some((k, v)) => Ok((k.to_owned(), v.to_owned())),
            None => bail!("selector {s:?} must be key=value"),
        })
        .collect()
}

/// Every document in a YAML stream (JSON is YAML too).
fn read_objects(path: &Path) -> anyhow::Result<Vec<Object>> {
    let text = if path == Path::new("-") {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        s
    } else {
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?
    };
    let mut objects = Vec::new();
    for (n, doc) in serde_yaml_ng::Deserializer::from_str(&text).enumerate() {
        let value: serde_json::Value = serde::Deserialize::deserialize(doc)
            .with_context(|| format!("{}: document {}", path.display(), n + 1))?;
        if value.is_null() {
            continue; // empty document, e.g. a trailing `---`
        }
        let object: Object = serde_json::from_value(value)
            .with_context(|| format!("{}: document {}", path.display(), n + 1))?;
        if object.api_version != API_VERSION {
            bail!(
                "{}: document {}: apiVersion must be {API_VERSION}",
                path.display(),
                n + 1
            );
        }
        objects.push(object);
    }
    Ok(objects)
}

fn display_name(o: &pb::Object) -> String {
    if o.namespace.is_empty() {
        o.name.clone()
    } else {
        format!("{}/{}", o.namespace, o.name)
    }
}

fn print_objects(objects: &[pb::Object], format: Format, kind: Kind) -> anyhow::Result<()> {
    if format == Format::Table {
        if objects.is_empty() {
            println!("no {kind} objects");
            return Ok(());
        }
        let objects: Vec<Object> = objects
            .iter()
            .cloned()
            .map(Object::try_from)
            .collect::<Result<_, _>>()?;
        let field = |v: &serde_json::Value, path: &[&str]| -> String {
            let mut v = v;
            for p in path {
                v = &v[*p];
            }
            match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            }
        };
        let count = |v: &serde_json::Value, key: &str| v[key].as_u64().unwrap_or(0);
        match kind {
            Kind::Service => {
                let rows: Vec<[String; 6]> = objects
                    .iter()
                    .map(|o| {
                        let images: Vec<String> = o.spec["pod"]["containers"]
                            .as_array()
                            .map(|cs| cs.iter().map(|c| field(c, &["image"])).collect())
                            .unwrap_or_default();
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.spec, &["mode"]),
                            format!(
                                "{}/{}",
                                count(&o.status, "ready"),
                                count(&o.status, "desired")
                            ),
                            images.join(","),
                            field(&o.status, &["update", "state"]),
                        ]
                    })
                    .collect();
                print_table(
                    ["NAMESPACE", "NAME", "MODE", "READY", "IMAGES", "UPDATE"],
                    &rows,
                );
            }
            Kind::Task => {
                let rows: Vec<[String; 6]> = objects
                    .iter()
                    .map(|o| {
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.spec, &["node"]),
                            field(&o.status, &["state"]),
                            field(&o.status, &["ip"]),
                            field(&o.status, &["message"]),
                        ]
                    })
                    .collect();
                print_table(
                    ["NAMESPACE", "NAME", "NODE", "STATE", "IP", "MESSAGE"],
                    &rows,
                );
            }
            Kind::Job => {
                let rows: Vec<[String; 7]> = objects
                    .iter()
                    .map(|o| {
                        let done = count(&o.status, "succeeded");
                        let completions = match o.spec["completions"].as_u64() {
                            Some(n) => format!("{done}/{n}"),
                            None => done.to_string(),
                        };
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.status, &["state"]),
                            completions,
                            count(&o.status, "active").to_string(),
                            count(&o.status, "failed").to_string(),
                            field(&o.status, &["message"]),
                        ]
                    })
                    .collect();
                print_table(
                    [
                        "NAMESPACE",
                        "NAME",
                        "STATE",
                        "COMPLETIONS",
                        "ACTIVE",
                        "FAILED",
                        "MESSAGE",
                    ],
                    &rows,
                );
            }
            Kind::CronJob => {
                let rows: Vec<[String; 7]> = objects
                    .iter()
                    .map(|o| {
                        let suspended = o.spec["suspend"].as_bool() == Some(true);
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.spec, &["schedule"]),
                            if suspended { "suspended" } else { "" }.to_owned(),
                            field(&o.status, &["lastSchedule"]),
                            field(&o.status, &["lastJob"]),
                            field(&o.status, &["message"]),
                        ]
                    })
                    .collect();
                print_table(
                    [
                        "NAMESPACE",
                        "NAME",
                        "SCHEDULE",
                        "SUSPEND",
                        "LAST SCHEDULE",
                        "LAST JOB",
                        "MESSAGE",
                    ],
                    &rows,
                );
            }
            Kind::Volume => {
                let rows: Vec<[String; 5]> = objects
                    .iter()
                    .map(|o| {
                        let m = &o.status["migration"];
                        let moving = match (field(m, &["phase"]), field(m, &["message"])) {
                            (phase, _) if phase.is_empty() => String::new(),
                            (phase, why) if why.is_empty() => {
                                format!("{phase} to {}", field(m, &["to"]))
                            }
                            (phase, why) => format!("{phase} to {}: {why}", field(m, &["to"])),
                        };
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.spec, &["size"]),
                            field(&o.status, &["node"]),
                            moving,
                        ]
                    })
                    .collect();
                print_table(["NAMESPACE", "NAME", "SIZE", "NODE", "MIGRATION"], &rows);
            }
            Kind::Node => {
                let rows: Vec<[String; 5]> = objects
                    .iter()
                    .map(|o| {
                        [
                            o.metadata.name.clone(),
                            field(&o.status, &["state"]),
                            field(&o.spec, &["availability"]),
                            field(&o.spec, &["role"]),
                            format!(
                                "{}/{}",
                                field(&o.status, &["platform", "os"]),
                                field(&o.status, &["platform", "arch"])
                            ),
                        ]
                    })
                    .collect();
                print_table(["NAME", "STATE", "AVAILABILITY", "ROLE", "PLATFORM"], &rows);
            }
            _ => {
                let rows: Vec<[String; 5]> = objects
                    .iter()
                    .map(|o| {
                        let labels: Vec<_> = o
                            .metadata
                            .labels
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect();
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            o.metadata.version.to_string(),
                            o.metadata.generation.to_string(),
                            labels.join(","),
                        ]
                    })
                    .collect();
                print_table(["NAMESPACE", "NAME", "VERSION", "GEN", "LABELS"], &rows);
            }
        }
        return Ok(());
    }
    let objects: Vec<Object> = objects
        .iter()
        .cloned()
        .map(Object::try_from)
        .collect::<Result<_, _>>()?;
    match format {
        Format::Json if objects.len() == 1 => {
            println!("{}", serde_json::to_string_pretty(&objects[0])?)
        }
        Format::Json => println!("{}", serde_json::to_string_pretty(&objects)?),
        _ => {
            for (i, o) in objects.iter().enumerate() {
                if i > 0 {
                    println!("---");
                }
                print!("{}", serde_yaml_ng::to_string(o)?);
            }
        }
    }
    Ok(())
}

fn print_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) {
    let mut widths = header.map(str::len);
    for row in rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.len());
        }
    }
    let line = |cells: Vec<&str>| {
        let padded: Vec<String> = cells
            .iter()
            .zip(widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        println!("{}", padded.join("  ").trim_end());
    };
    line(header.to_vec());
    for row in rows {
        line(row.iter().map(String::as_str).collect());
    }
}

fn print_status(s: &StatusResponse) {
    println!("node:     {}", s.node_id);
    println!("state:    {}", s.state);
    println!(
        "leader:   {}",
        s.leader_id.map_or("none".into(), |l| l.to_string())
    );
    println!("term:     {}", s.term);
    println!("version:  {}", s.applied_version);
    if s.members.is_empty() {
        println!("members:  none (run `legion init`)");
    } else {
        println!("members:");
        for m in &s.members {
            let role = if m.voter { "voter" } else { "learner" };
            println!("  {:<20} {:<22} {role}", m.id, m.addr);
        }
    }
}

/// `legion unlock`: sends a locked manager on this host its unlock key, in
/// the clear, as the manager has no credentials to offer until unlocked.
pub async fn unlock(server: &str, key_file: Option<&Path>) -> anyhow::Result<()> {
    let key = match key_file {
        Some(f) => {
            std::fs::read_to_string(f).with_context(|| format!("reading {}", f.display()))?
        }
        None => {
            let mut key = String::new();
            std::io::stdin().read_line(&mut key)?;
            key
        }
    };
    let channel = Channel::from_shared(format!("http://{server}"))?
        .connect()
        .await
        .with_context(|| format!("reaching the manager at {server}"))?;
    legion_proto::v1::unlock_client::UnlockClient::new(channel)
        .unlock(pb::UnlockRequest {
            key: key.trim().to_owned(),
        })
        .await
        .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
    println!("unlocked");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retries_only_while_unavailable() {
        let calls = std::cell::Cell::new(0);
        let r = retrying(|| {
            calls.set(calls.get() + 1);
            let n = calls.get();
            async move {
                if n < 3 {
                    Err(tonic::Status::unavailable("electing"))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 3);

        calls.set(0);
        let r: Result<(), _> = retrying(|| {
            calls.set(calls.get() + 1);
            async { Err(tonic::Status::not_found("gone")) }
        })
        .await;
        assert_eq!(r.unwrap_err().code(), tonic::Code::NotFound);
        assert_eq!(calls.get(), 1, "other errors are not retried");
    }
}
