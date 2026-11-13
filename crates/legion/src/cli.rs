//! CLI commands: thin gRPC calls plus formatting.

use std::future::Future;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use legion_api::{Kind, Object};
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

/// Follows a node's drain until it completes (0), `deadline` passes (2),
/// or the node is no longer draining (1): printing what is left as it
/// changes, or with `json` only the last status. Read from whichever
/// manager answers, so a new leader's word counts as the old one's did.
async fn wait_for_drain(
    mut objects: Objects,
    r#ref: ObjectRef,
    deadline: Option<tokio::time::Instant>,
    json: bool,
) -> anyhow::Result<i32> {
    use legion_api::node::{Availability, DrainPhase, DrainStatus, NodeSpec, NodeStatus};
    let name = r#ref.name.clone();
    let mut last: Option<DrainStatus> = None;
    loop {
        let o = Object::try_from(objects.get(r#ref.clone()).await?.into_inner())
            .map_err(|e| anyhow::anyhow!("node {name}: {e}"))?;
        let spec: NodeSpec =
            legion_api::parse(&o.spec).map_err(|e| anyhow::anyhow!("node {name}: {e}"))?;
        let status: NodeStatus = serde_json::from_value(o.status).unwrap_or_default();
        let done = |code: i32, drain: &Option<DrainStatus>| -> anyhow::Result<i32> {
            if json {
                let mut v = serde_json::to_value(drain)?;
                if let Some(m) = v.as_object_mut() {
                    m.insert("node".into(), name.clone().into());
                }
                println!("{v}");
            }
            Ok(code)
        };
        if spec.availability != Availability::Drain {
            if !json {
                eprintln!("node {name} is no longer draining");
            }
            return done(1, &status.drain);
        }
        if status.drain != last {
            if let (Some(d), false) = (&status.drain, json) {
                println!("{}", drain_line(&name, d));
            }
            last = status.drain.clone();
        }
        if last
            .as_ref()
            .is_some_and(|d| d.phase == DrainPhase::Completed)
        {
            return done(0, &last);
        }
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            if !json {
                eprintln!("node {name}: timed out waiting for the drain");
            }
            return done(2, &last);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// One line on where a drain stands: `n2 draining: 2 left (Task shop/web-2-1
/// stopping, Volume shop/data copying)`.
fn drain_line(node: &str, d: &legion_api::node::DrainStatus) -> String {
    let phase = serde_json::to_value(d.phase)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default();
    let mut line = format!("{node} {phase}");
    if !d.reason.is_empty() {
        line.push_str(&format!(" ({})", d.reason));
    }
    if !d.remaining.is_empty() {
        let items: Vec<String> = d
            .remaining
            .iter()
            .map(|r| {
                let mut item = format!("{} {}/{} {}", r.kind, r.namespace, r.name, r.reason);
                if !r.message.is_empty() {
                    item.push_str(&format!(": {}", r.message));
                }
                item
            })
            .collect();
        line.push_str(&format!(": {} left ({})", items.len(), items.join(", ")));
    }
    line
}

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
pub(crate) struct Objects(ObjectsClient<Channel>);

type Answer<T> = Result<tonic::Response<T>, tonic::Status>;

/// The Objects API, taking objects as large as managers send.
pub(crate) fn objects_client(channel: Channel) -> ObjectsClient<Channel> {
    ObjectsClient::new(channel)
        .max_decoding_message_size(legion_manager::MAX_MESSAGE)
        .max_encoding_message_size(legion_manager::MAX_MESSAGE)
}

/// The Cluster API, likewise.
pub(crate) fn cluster_client(channel: Channel) -> ClusterClient<Channel> {
    ClusterClient::new(channel)
        .max_decoding_message_size(legion_manager::MAX_MESSAGE)
        .max_encoding_message_size(legion_manager::MAX_MESSAGE)
}

impl Objects {
    pub(crate) fn new(channel: Channel) -> Self {
        Objects(objects_client(channel))
    }

    async fn apply(&mut self, req: ApplyRequest) -> Answer<pb::ApplyResponse> {
        let client = &self.0;
        retrying(|| {
            let (mut c, req) = (client.clone(), req.clone());
            async move { c.apply(req).await }
        })
        .await
    }

    /// Sets `field` of an object's spec to `value`
    /// (`legion_manager::actions::set_spec`): its key, and whether the
    /// field changed.
    pub(crate) async fn set_spec(
        &mut self,
        r#ref: ObjectRef,
        field: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<(String, bool)> {
        Ok(legion_manager::actions::set_spec(self, r#ref, field, value).await?)
    }

    /// Sets (`Some`) or removes (`None`) labels of an object
    /// (`legion_manager::actions::set_labels`); whether anything changed.
    pub(crate) async fn set_labels(
        &mut self,
        r#ref: ObjectRef,
        changes: &[(String, Option<String>)],
    ) -> anyhow::Result<(String, bool)> {
        Ok(legion_manager::actions::set_labels(self, r#ref, changes).await?)
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

    pub(crate) async fn delete(&mut self, req: DeleteRequest) -> Answer<pb::DeleteResponse> {
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

/// The shared changes' calls, each retried while the API is unavailable.
impl legion_manager::actions::ObjectsApi for Objects {
    async fn get(&mut self, r: ObjectRef) -> Result<pb::Object, tonic::Status> {
        Ok(Objects::get(self, r).await?.into_inner())
    }

    async fn apply(&mut self, r: ApplyRequest) -> Result<pb::ApplyResponse, tonic::Status> {
        Ok(Objects::apply(self, r).await?.into_inner())
    }

    async fn delete(&mut self, r: DeleteRequest) -> Result<pb::DeleteResponse, tonic::Status> {
        Ok(Objects::delete(self, r).await?.into_inner())
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
    let mut objects = Objects::new(channel.clone());
    let mut cluster = cluster_client(channel.clone());
    match command {
        Command::Daemon { .. } | Command::NodeResolver { .. } => unreachable!("handled in main"),
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
        Command::Audit {
            since,
            actor,
            action,
            namespace,
            kind,
            denied,
            limit,
            output,
        } => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            let filter = legion_api::audit::Filter {
                since_ms: since.map_or(0, |d| now_ms.saturating_sub(d.millis())),
                actor: actor.unwrap_or_default(),
                action: action.unwrap_or_default(),
                namespace: namespace.unwrap_or_default(),
                kind: kind.unwrap_or_default(),
                result: denied.then_some(legion_api::audit::Outcome::Denied),
            };
            let answer = cluster
                .audit(pb::AuditRequest {
                    filter_json: serde_json::to_vec(&filter)?,
                    limit,
                })
                .await?
                .into_inner();
            let events = answer
                .events_json
                .iter()
                .map(|e| serde_json::from_slice(e))
                .collect::<Result<Vec<legion_api::audit::AuditEvent>, _>>()
                .context("reading the audit events")?;
            print_audit(&events, output)?;
            if answer.truncated {
                eprintln!("more events matched than --limit {limit}: narrow it, or use --since");
            }
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
        Command::Console { action: _, url } => {
            let code = cluster
                .console_code(pb::ConsoleCodeRequest::default())
                .await?
                .into_inner();
            let base = match url {
                Some(u) => u.trim_end_matches('/').to_owned(),
                None => {
                    let host = host_port(server)
                        .rsplit_once(':')
                        .map_or(host_port(server), |(h, _)| h);
                    format!("https://{host}:7480")
                }
            };
            // In the fragment: never sent to a server, nor kept in its logs.
            println!("{base}/login#code={}", code.code);
            eprintln!("open it within 60 s; it signs you in once");
        }
        Command::Ca { action } => {
            let r = cluster
                .rotate_ca(pb::RotateCaRequest {
                    start: action == "rotate",
                })
                .await?
                .into_inner();
            print!("{}", ca_status(&r));
            if action == "rotate" {
                eprintln!(
                    "every node moves to the new CA by itself; the old one stays trusted \
                     until all have. Users' certificates and copies of admin.pem from \
                     before are refused then: issue them again (`legion user create`)"
                );
            }
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
        | Command::Doctor { .. }
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
            let binding = match (&role, &namespace) {
                (Some(role), Some(namespace)) => Some((role.as_str(), namespace.as_str())),
                (None, None) => None,
                _ => bail!("--role and --namespace go together"),
            };
            let made = legion_manager::actions::create_user(
                &mut objects,
                &mut cluster,
                &name,
                binding,
                days,
            )
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
            if let (Some(result), Some(namespace)) = (made.binding, &namespace) {
                println!("RoleBinding/{namespace}/{name} {result}");
            }
            let bundle = made.bundle;
            crate::pki::write_private(&path, &bundle.to_pem())?;
            // As issued, to the nearest day: 0 asks for the default.
            let days = legion_pki::validity(&bundle.cert_pem)
                .map_or(u64::from(days), |(_, life)| {
                    (life.as_secs() + 43_200) / 86_400
                });
            println!(
                "wrote {}: user {name}'s credentials for {days} days; hand it to them for --identity",
                path.display()
            );
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
        Command::Node {
            command:
                ref command @ (crate::NodeCommand::Promote { ref node }
                | crate::NodeCommand::Demote { ref node }),
        } => {
            let role = match command {
                crate::NodeCommand::Promote { .. } => "manager",
                _ => "worker",
            };
            cluster
                .change_role(pb::ChangeRoleRequest {
                    node: node.clone(),
                    role: role.to_owned(),
                })
                .await?;
            println!(
                "node {node} becomes a {role}: its daemon takes it up and starts again (`legion list nodes` shows its role)"
            );
        }
        Command::Node {
            command: crate::NodeCommand::Label { node, labels },
        } => {
            let changes = labels
                .iter()
                .map(|l| label_change(l))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node,
            };
            match objects.set_labels(r#ref, &changes).await? {
                (key, true) => println!("{key} labelled"),
                (key, false) => println!("{key} already has those labels"),
            }
        }
        Command::Node {
            command:
                crate::NodeCommand::Drain {
                    node,
                    wait: true,
                    timeout,
                    output,
                },
        } => {
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node.clone(),
            };
            let json = matches!(output, Format::Json);
            let (key, changed) = objects
                .set_spec(r#ref.clone(), "availability", "drain".into())
                .await?;
            if !json {
                match changed {
                    true => println!("{key} drain"),
                    false => println!("{key} is already drain"),
                }
            }
            let deadline = timeout.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
            let code = wait_for_drain(objects.clone(), r#ref, deadline, json).await?;
            if code != 0 {
                std::process::exit(code);
            }
        }
        Command::Node { command } => {
            let (availability, node) = match command {
                crate::NodeCommand::Drain { node, .. } => ("drain", node),
                crate::NodeCommand::Pause { node } => ("pause", node),
                crate::NodeCommand::Activate { node } => ("active", node),
                crate::NodeCommand::Label { .. }
                | crate::NodeCommand::Promote { .. }
                | crate::NodeCommand::Demote { .. } => unreachable!("handled above"),
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
        Command::Volume {
            command:
                crate::VolumeCommand::Backup {
                    volume,
                    output,
                    namespace,
                },
        } => {
            crate::volume_backup::backup(
                channel.clone(),
                namespace.unwrap_or_default(),
                volume,
                &output,
            )
            .await?;
        }
        Command::Volume {
            command:
                crate::VolumeCommand::Restore {
                    file,
                    node,
                    namespace,
                    name,
                    replace,
                },
        } => {
            crate::volume_backup::restore(channel.clone(), &file, node, namespace, name, replace)
                .await?;
        }
        Command::Rollback { service, namespace } => {
            println!(
                "{}",
                rollback(&mut objects, namespace.unwrap_or_default(), service).await?
            );
        }
        Command::Top { namespace, once } if once => crate::top::once(channel, namespace).await?,
        Command::Top { namespace, .. } => crate::top::run(channel, namespace).await?,
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

/// Rolls a service back to the spec its latest update came from; what
/// happened, for people.
pub(crate) async fn rollback(
    objects: &mut Objects,
    namespace: String,
    service: String,
) -> anyhow::Result<String> {
    legion_manager::actions::rollback(objects, namespace, service)
        .await
        .map_err(|s| anyhow::anyhow!("{}", s.message()))
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
                // What `stack rm` would remove, Volumes and their data
                // staying.
                let keep: Vec<_> = converted.objects.iter().map(Object::key).collect();
                let kinds = [Kind::Service, Kind::Secret, Kind::Config];
                for o in stack_objects(objects, &stack, &namespace, &kinds).await? {
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
    Ok(legion_api::manifest::yaml(&Object::try_from(o)?))
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
    let url = format!("https://{}", host_port(server));
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
    // `policies`, as `NetworkPolicy` and `ImagePolicy` are listed.
    let ies = lower.strip_suffix("ies").map(|s| format!("{s}y"));
    Kind::ALL
        .into_iter()
        .find(|k| {
            let name = k.as_str().to_ascii_lowercase();
            name == lower || name == singular || ies.as_deref() == Some(name.as_str())
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

/// `key=value` sets a label, `key-` removes it.
pub(crate) fn label_change(s: &str) -> anyhow::Result<(String, Option<String>)> {
    match (s.split_once('='), s.strip_suffix('-')) {
        (Some((k, v)), _) if !k.is_empty() => Ok((k.to_owned(), Some(v.to_owned()))),
        (None, Some(k)) if !k.is_empty() => Ok((k.to_owned(), None)),
        _ => bail!("label {s:?} must be key=value, or key- to remove it"),
    }
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
    legion_api::manifest::parse(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
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

/// Audit events as a table, YAML or JSON.
fn print_audit(events: &[legion_api::audit::AuditEvent], format: Format) -> anyhow::Result<()> {
    match format {
        Format::Json => println!("{}", serde_json::to_string_pretty(events)?),
        Format::Yaml => print!("{}", serde_yaml_ng::to_string(events)?),
        Format::Table => {
            println!(
                "TIME                  ACTOR               ACTION          TARGET                                   RESULT  MESSAGE"
            );
            for e in events {
                let time = legion_api::clock::format((e.time_ms / 1000) as i64);
                let actor = match e.actor.name.as_str() {
                    "" => e.actor.kind.clone(),
                    name => format!("{}:{name}", e.actor.kind),
                };
                let target = [
                    e.target.kind.as_str(),
                    e.target.namespace.as_str(),
                    e.target.name.as_str(),
                ]
                .iter()
                .filter(|s| !s.is_empty())
                .copied()
                .collect::<Vec<_>>()
                .join("/");
                println!(
                    "{time:<21} {actor:<19} {action:<15} {target:<40} {result:<7} {message}",
                    action = e.action,
                    result = e.result.as_str(),
                    message = e.message,
                );
            }
        }
    }
    Ok(())
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
    if s.protocol > 0 {
        println!(
            "protocol: {} (the managers' floor: {})",
            s.protocol, s.protocol_floor
        );
    }
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
/// `--server` as `host:port`, with any scheme given dropped.
fn host_port(server: &str) -> &str {
    server
        .strip_prefix("https://")
        .or_else(|| server.strip_prefix("http://"))
        .unwrap_or(server)
}

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
    let channel = Channel::from_shared(format!("http://{}", host_port(server)))?
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

/// Where the cluster's CA stands, as `legion ca status` shows it.
fn ca_status(r: &pb::RotateCaResponse) -> String {
    use std::fmt::Write;
    let short = |d: &str| d.get(..16).unwrap_or(d).to_owned();
    let mut out = String::new();
    let _ = writeln!(out, "CA: {}", r.digest);
    if r.previous_digest.is_empty() {
        let _ = writeln!(out, "no rotation under way");
    } else {
        let behind = r.holders.iter().filter(|h| h.issuer != r.digest).count();
        let _ = writeln!(
            out,
            "rotating from {} since {}: {} of {} on the new CA",
            short(&r.previous_digest),
            legion_api::clock::format((r.since_ms / 1000) as i64),
            r.holders.len() - behind,
            r.holders.len()
        );
    }
    let _ = writeln!(out, "{:<24} {:<8} CA", "NAME", "KIND");
    for h in &r.holders {
        let ca = if h.issuer.is_empty() {
            "unknown".to_owned()
        } else if h.issuer == r.digest {
            "new".to_owned()
        } else if h.issuer == r.previous_digest {
            "old".to_owned()
        } else {
            short(&h.issuer)
        };
        let ca = if r.previous_digest.is_empty() && ca == "new" {
            "current".to_owned()
        } else {
            ca
        };
        let _ = writeln!(out, "{:<24} {:<8} {ca}", h.name, h.kind);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_drain_line_says_what_is_left_and_why_a_move_failed() {
        use legion_api::node::{DrainItem, DrainPhase, DrainStatus};
        let item = |kind: &str, name: &str, reason: &str, message: &str| DrainItem {
            kind: kind.into(),
            namespace: "shop".into(),
            name: name.into(),
            reason: reason.into(),
            message: message.into(),
        };
        let d = DrainStatus {
            phase: DrainPhase::Draining,
            reason: String::new(),
            remaining: vec![
                item("Task", "db-0-1", "data", ""),
                item(
                    "Volume",
                    "data-db-0",
                    "copying",
                    "node n1: zfs send: Broken pipe",
                ),
            ],
        };
        assert_eq!(
            drain_line("n1", &d),
            "n1 draining: 2 left (Task shop/db-0-1 data, Volume shop/data-db-0 copying: node n1: zfs send: Broken pipe)"
        );
        let d = DrainStatus {
            phase: DrainPhase::Blocked,
            reason: "unreachable".into(),
            remaining: vec![],
        };
        assert_eq!(drain_line("n1", &d), "n1 blocked (unreachable)");
    }

    #[test]
    fn a_server_may_name_its_scheme() {
        for server in [
            "127.0.0.1:7470",
            "http://127.0.0.1:7470",
            "https://127.0.0.1:7470",
        ] {
            assert_eq!(host_port(server), "127.0.0.1:7470");
        }
    }

    #[test]
    fn label_changes() {
        assert_eq!(
            label_change("zone=a").unwrap(),
            ("zone".into(), Some("a".into()))
        );
        assert_eq!(
            label_change("legion.ingress=true").unwrap(),
            ("legion.ingress".into(), Some("true".into()))
        );
        assert_eq!(label_change("zone-").unwrap(), ("zone".into(), None));
        assert_eq!(
            label_change("a=b-").unwrap(),
            ("a".into(), Some("b-".into()))
        );
        for bad in ["zone", "=a", "-", ""] {
            assert!(label_change(bad).is_err(), "{bad}");
        }
    }

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
