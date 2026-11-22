//! CLI commands: thin gRPC calls plus formatting.

use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use legion_api::dryrun::{Change, DryRun};
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

/// A pool's row in `legion node pools`: its node, name, state, how full,
/// its errors and when it was last scrubbed (or what scans it now).
pub(crate) fn pool_row(node: &str, p: &legion_api::node::Pool, now_ms: i64) -> [String; 6] {
    let gib = |b: u64| format!("{:.1}", b as f64 / f64::from(1u32 << 30));
    let used = format!(
        "{}% ({} of {} GiB)",
        p.percent_used(),
        gib(p.alloc_bytes),
        gib(p.size_bytes)
    );
    let errors = if p.errors() == 0 {
        "none".to_owned()
    } else {
        format!(
            "{} read, {} write, {} checksum",
            p.read_errors, p.write_errors, p.checksum_errors
        )
    };
    let scrubbed = match (&p.scan, p.scrubbed_ms()) {
        (Some(s), _) if s.state == "scanning" => format!("{} running", s.function),
        (_, Some(at)) => format!(
            "{} ago",
            legion_api::clock::span(std::time::Duration::from_millis((now_ms - at).max(0) as u64))
        ),
        _ => "never".to_owned(),
    };
    [
        node.to_owned(),
        p.name.clone(),
        p.state.clone(),
        used,
        errors,
        scrubbed,
    ]
}

/// A Service's image as its lists say it: a Linux image marked, with the
/// native recipe that does what it does if there is one, and a tag that
/// names another image now than the one its tasks run (a restart runs it).
pub(crate) fn marked_image(image: &str, status: &serde_json::Value) -> String {
    let mut marked = image.to_owned();
    if let Some(recipe) = status["linuxImages"].get(image) {
        marked = match recipe.as_str() {
            Some(r) if !r.is_empty() => format!("{marked} (Linux; native: {r})"),
            _ => format!("{marked} (Linux)"),
        };
    }
    if status["images"]["moved"].get(image).is_some() {
        marked = format!("{marked} (tag moved)");
    }
    marked
}

/// A Volume's replica in a cell: its node and how old its last copy is,
/// or why it has none; then, as they hold, that it is stale, a copy runs,
/// and automatic failover is blocked (`legion get volume` says why).
pub(crate) fn replica_cell(r: &serde_json::Value, now_ms: i64) -> String {
    let Some(node) = r["node"].as_str() else {
        return String::new();
    };
    let age = match r["takenMs"].as_i64() {
        Some(t) => {
            let s = ((now_ms - t) / 1000).max(0);
            if s < 120 {
                format!("{s}s ago")
            } else if s < 7200 {
                format!("{}m ago", s / 60)
            } else if s < 172_800 {
                format!("{}h ago", s / 3600)
            } else {
                format!("{}d ago", s / 86_400)
            }
        }
        None => "no copy yet".to_owned(),
    };
    let mut cell = match r["message"].as_str().filter(|m| !m.is_empty()) {
        Some(why) if node.is_empty() => why.to_owned(),
        Some(why) => format!("{node}, {age}: {why}"),
        None => format!("{node}, {age}"),
    };
    if r["state"] == "stale" {
        cell.push_str(", stale");
    }
    if r["copyingMs"].is_i64() {
        cell.push_str(", copying");
    }
    if r["blocked"].as_array().is_some_and(|b| !b.is_empty()) {
        cell.push_str(", failover blocked");
    }
    cell
}

/// A node's state, with what its host lacks that keeps new tasks off.
fn node_state(o: &Object, state: String) -> String {
    let status = serde_json::from_value::<legion_api::node::NodeStatus>(o.status.clone());
    let spec = legion_api::parse::<legion_api::node::NodeSpec>(&o.spec);
    let (Ok(status), Ok(spec)) = (status, spec) else {
        return state;
    };
    let lacking: Vec<&str> = status.blocking(&spec).map(|l| l.id.as_str()).collect();
    match lacking.is_empty() {
        true => state,
        false => format!("{state}, lacks {}", lacking.join(" ")),
    }
}

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

    pub(crate) async fn apply(&mut self, req: ApplyRequest) -> Answer<pb::ApplyResponse> {
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

    pub(crate) async fn get(&mut self, req: ObjectRef) -> Answer<pb::Object> {
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
    if let Some((change, dry_run, force)) = disruption(&command)? {
        let blocks = check_first(&mut cluster, change, dry_run, force).await?;
        if dry_run {
            if blocks {
                std::process::exit(1);
            }
            return Ok(());
        }
    }
    match command {
        Command::Daemon { .. } | Command::NodeResolver { .. } => unreachable!("handled in main"),
        Command::Init => {
            let status = cluster.init(InitRequest {}).await?.into_inner();
            println!(
                "cluster initialized; this manager is node {}",
                status.node_id
            );
            // On the manager's own host, pf is this host's to set up.
            let host = host_port(server).rsplit_once(':').map_or(host_port(server), |(h, _)| h);
            if matches!(host, "127.0.0.1" | "localhost" | "[::1]") {
                crate::doctor::offer_pf_fix();
            }
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
        Command::Console {
            action,
            file,
            remove,
            ..
        } if action == "oidc" => {
            let r = if remove {
                cluster
                    .set_console_oidc(pb::SetConsoleOidcRequest {
                        remove: true,
                        ..Default::default()
                    })
                    .await?
            } else if let Some(file) = file {
                let text = if file == Path::new("-") {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s)?;
                    s
                } else {
                    std::fs::read_to_string(&file)
                        .with_context(|| format!("reading {}", file.display()))?
                };
                let mut v: serde_json::Value = serde_yaml_ng::from_str(&text)
                    .with_context(|| format!("reading {}", file.display()))?;
                let secret = v
                    .as_object_mut()
                    .and_then(|m| m.remove("clientSecret"))
                    .map(|s| s.as_str().map(str::to_owned).unwrap_or_default())
                    .unwrap_or_default();
                let oidc: legion_api::console::Oidc = serde_json::from_value(v)
                    .with_context(|| format!("reading {}", file.display()))?;
                oidc.check()
                    .map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
                cluster
                    .set_console_oidc(pb::SetConsoleOidcRequest {
                        oidc_json: serde_json::to_vec(&oidc)?,
                        client_secret: secret,
                        remove: false,
                    })
                    .await?
            } else {
                cluster.console_oidc(pb::ConsoleOidcRequest {}).await?
            }
            .into_inner();
            if r.oidc_json.is_empty() {
                eprintln!("single sign-on is off");
            } else {
                let oidc: legion_api::console::Oidc = serde_json::from_slice(&r.oidc_json)?;
                print!("{}", serde_yaml_ng::to_string(&oidc)?);
                println!(
                    "clientSecret: {}",
                    if r.has_secret { "(set)" } else { "(none)" }
                );
                eprintln!(
                    "the provider must allow {} as a redirect; bind users as oidc:<{}> \
                     and groups from {:?} in RoleBindings",
                    oidc.redirect_uri(),
                    oidc.username_claim,
                    oidc.groups_claim
                );
            }
        }
        Command::Console { action, file, .. } if action == "settings" || action == "configure" => {
            let r = if action == "settings" {
                cluster
                    .console_settings(pb::ConsoleSettingsRequest {})
                    .await?
            } else {
                let file = file.ok_or_else(|| anyhow::anyhow!("configure needs -f FILE"))?;
                let text = if file == Path::new("-") {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s)?;
                    s
                } else {
                    std::fs::read_to_string(&file)
                        .with_context(|| format!("reading {}", file.display()))?
                };
                let settings: legion_api::console::Settings = serde_yaml_ng::from_str(&text)
                    .with_context(|| format!("reading {}", file.display()))?;
                settings
                    .check()
                    .map_err(|e| anyhow::anyhow!("{}: {e}", file.display()))?;
                cluster
                    .set_console_settings(pb::SetConsoleSettingsRequest {
                        settings_json: serde_json::to_vec(&settings)?,
                    })
                    .await?
            };
            let settings: legion_api::console::Settings =
                serde_json::from_slice(&r.into_inner().settings_json)?;
            if settings == legion_api::console::Settings::default() {
                eprintln!("no settings: the console's defaults (12h sessions)");
            } else {
                print!("{}", serde_yaml_ng::to_string(&settings)?);
            }
        }
        Command::BuildLogs {
            build,
            namespace,
            follow,
        } => {
            let mut chunks = cluster
                .build_logs(pb::BuildLogsRequest {
                    namespace: namespace.unwrap_or_else(|| "default".into()),
                    name: build,
                    follow,
                })
                .await?
                .into_inner();
            let mut out = std::io::stdout();
            while let Some(chunk) = chunks.message().await? {
                std::io::Write::write_all(&mut out, &chunk.data)?;
                std::io::Write::flush(&mut out)?;
            }
        }
        Command::Console { url, .. } => {
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
        | Command::App {
            command:
                crate::AppCommand::List { .. }
                | crate::AppCommand::Show { .. }
                | crate::AppCommand::Deploy { dry_run: true, .. },
        }
        | Command::Unlock { .. } => {
            unreachable!("handled in main")
        }
        Command::Image { command } => {
            run_image_command(&mut cluster, command).await?;
        }
        Command::Diagnose { node, json } => {
            let nodes = cluster
                .diagnose(pb::DiagnoseRequest {
                    node: node.unwrap_or_default(),
                })
                .await?
                .into_inner()
                .nodes;
            if json {
                let said: Vec<_> = nodes
                    .iter()
                    .map(|n| {
                        let findings: Vec<_> = n
                            .findings
                            .iter()
                            .map(|f| {
                                serde_json::json!({
                                    "id": f.id, "state": f.state, "need": f.need,
                                    "detail": f.detail, "remedy": f.remedy,
                                })
                            })
                            .collect();
                        serde_json::json!({
                            "node": n.node, "error": n.error, "ready": n.ready,
                            "findings": findings,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&said)?);
            } else {
                print!("{}", crate::doctor::diagnosis(&nodes));
            }
            let short: Vec<_> = nodes
                .iter()
                .filter(|n| !n.ready)
                .map(|n| n.node.as_str())
                .collect();
            if !short.is_empty() {
                anyhow::bail!("not ready: {}", short.join(", "));
            }
        }
        Command::SupportBundle { output } => {
            let json = cluster
                .support_bundle(pb::SupportBundleRequest {})
                .await?
                .into_inner()
                .json;
            let path = output.unwrap_or_else(|| {
                let at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                PathBuf::from(format!("legion-support-{at}.json"))
            });
            std::fs::write(&path, &json).with_context(|| format!("writing {}", path.display()))?;
            println!(
                "{} written ({} bytes): read it before sending it",
                path.display(),
                json.len()
            );
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
        Command::LogsSearch {
            service,
            text,
            namespace,
            since,
            until,
            limit,
        } => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64);
            let r = legion_manager::logsearch::search(
                channel.clone(),
                &legion_manager::logsearch::Search {
                    namespace: namespace.unwrap_or_else(|| "default".into()),
                    service,
                    since_ms: Some(now_ms - since.millis() as i64),
                    until_ms: until.map(|u| now_ms - u.millis() as i64),
                    text: text.unwrap_or_default(),
                    limit,
                },
            )
            .await?;
            for l in &r.lines {
                let time = legion_api::clock::format(l.at_ms / 1000);
                let stream = if l.stderr { "!" } else { " " };
                println!("{time} {}{stream} {}", l.task, l.text);
            }
            if r.truncated {
                eprintln!("more lines matched than --limit {limit}: the oldest were left out");
            }
            for u in &r.unread {
                eprintln!("not searched: {u}");
            }
        }
        Command::Files {
            task,
            path,
            namespace,
            container,
        } => {
            let target = target(task, namespace, container);
            let entries = crate::session::list_files(&channel, &target, &path).await?;
            print!("{}", files_table(&entries));
        }
        Command::Cp {
            from,
            to,
            namespace,
            container,
        } => match (remote(&from), remote(&to)) {
            (Some((task, path)), None) => {
                let target = target(task.to_owned(), namespace, container);
                let n = if to == "-" {
                    crate::session::download(&channel, &target, path, &mut std::io::stdout().lock())
                        .await?
                } else {
                    let mut file =
                        std::fs::File::create(&to).with_context(|| format!("creating {to}"))?;
                    crate::session::download(&channel, &target, path, &mut file).await?
                };
                if to != "-" {
                    eprintln!("{n} bytes copied to {to}");
                }
            }
            (None, Some((task, path))) => {
                let target = target(task.to_owned(), namespace, container);
                let n = if from == "-" {
                    crate::session::upload(&channel, &target, path, tokio::io::stdin()).await?
                } else {
                    let file = tokio::fs::File::open(&from)
                        .await
                        .with_context(|| format!("opening {from}"))?;
                    crate::session::upload(&channel, &target, path, file).await?
                };
                eprintln!("{n} bytes copied to {to}");
            }
            _ => bail!("one side, and only one, is a task's: TASK:PATH"),
        },
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
                // A file that can't be written puts the binding back.
                |bundle| {
                    crate::pki::write_private(&path, &bundle.to_pem()).map_err(|e| format!("{e:#}"))
                },
            )
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
            if let (Some(result), Some(namespace)) = (made.binding, &namespace) {
                println!("RoleBinding/{namespace}/{name} {result}");
            }
            let bundle = made.bundle;
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
        Command::App {
            command:
                crate::AppCommand::Deploy {
                    app,
                    name,
                    namespace,
                    set,
                    variant,
                    dry_run,
                    catalog,
                },
        } => {
            let stack = name.unwrap_or_else(|| app.clone());
            let namespace = namespace.unwrap_or_else(|| stack.clone());
            let (converted, env) =
                crate::app::stack(&catalog, &app, &set, variant.as_deref(), &stack, &namespace).await?;
            let _ = dry_run;
            // Made only when missing, allowing what its containers ask
            // for; one there is keeps what it allows, as the admin set it.
            let found = objects
                .get(ObjectRef {
                    kind: Kind::Namespace.as_str().into(),
                    namespace: String::new(),
                    name: namespace.clone(),
                })
                .await;
            if let Err(s) = &found
                && s.code() == tonic::Code::NotFound
            {
                let spec = converted.namespace_spec();
                let ns = Object::new(Kind::Namespace, "", &namespace, spec);
                apply(&mut objects, &ns).await?;
            } else if let Err(s) = found
                && s.code() != tonic::Code::PermissionDenied
            {
                return Err(anyhow::Error::from(s).context(format!("namespace {namespace}")));
            }
            for object in &converted.objects {
                apply(&mut objects, object).await?;
            }
            let secrets: Vec<_> = env
                .iter()
                .filter(|(k, _)| !set.iter().any(|s| s.starts_with(&format!("{k}="))))
                .filter(|(k, _)| k.contains("PASSWORD") || k.contains("SECRET") || k.contains("TOKEN") || k.ends_with("_KEY"))
                .collect();
            for (k, v) in secrets {
                println!("{k}={v}  (made up: keep it)");
            }
        }
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
            ..
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
            command: crate::NodeCommand::Pools { node },
        } => {
            let resp = objects
                .list(ListRequest {
                    kind: Kind::Node.to_string(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64);
            let mut rows = Vec::new();
            for o in resp.objects {
                let o = Object::try_from(o).map_err(|e| anyhow::anyhow!("{e}"))?;
                if node.as_ref().is_some_and(|n| *n != o.metadata.name) {
                    continue;
                }
                let status: legion_api::node::NodeStatus =
                    serde_json::from_value(o.status).unwrap_or_default();
                for p in &status.pools {
                    rows.push(pool_row(&o.metadata.name, p, now_ms));
                }
            }
            if rows.is_empty() {
                println!(
                    "no pools reported{}",
                    node.map(|n| format!(" by {n}")).unwrap_or_default()
                );
            } else {
                print_table(
                    ["NODE", "POOL", "STATE", "USED", "ERRORS", "SCRUBBED"],
                    &rows,
                );
            }
        }
        Command::Node {
            command: crate::NodeCommand::ErrorsSeen { node },
        } => {
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node.clone(),
            };
            let o = Object::try_from(objects.get(r#ref.clone()).await?.into_inner())
                .map_err(|e| anyhow::anyhow!("node {node}: {e}"))?;
            let status: legion_api::node::NodeStatus =
                serde_json::from_value(o.status).unwrap_or_default();
            let seen: serde_json::Map<String, serde_json::Value> = status
                .pools
                .iter()
                .filter(|p| p.errors() > 0)
                .map(|p| (p.name.clone(), p.errors().into()))
                .collect();
            let said = if seen.is_empty() {
                "no errors to take as seen".to_owned()
            } else {
                seen.iter()
                    .map(|(p, n)| format!("{p}: {n}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            match objects
                .set_spec(r#ref, "poolErrorsSeen", seen.into())
                .await?
            {
                (key, true) => println!("{key}: errors taken as seen ({said})"),
                (key, false) => println!("{key}: already so ({said})"),
            }
        }
        Command::Node {
            command:
                crate::NodeCommand::Scrub {
                    node,
                    every,
                    never: _,
                },
        } => {
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node,
            };
            let said = every.map_or("no scheduled scrubs".to_owned(), |d| {
                format!("its pools scrubbed every {d} days")
            });
            let value = every.map_or(serde_json::Value::Null, Into::into);
            match objects.set_spec(r#ref, "scrubEveryDays", value).await? {
                (key, true) => println!("{key}: {said}"),
                (key, false) => println!("{key} already has {said}"),
            }
        }
        Command::Node {
            command: crate::NodeCommand::Require { node, checks },
        } => {
            let r#ref = ObjectRef {
                kind: Kind::Node.to_string(),
                namespace: String::new(),
                name: node,
            };
            let said = match checks.is_empty() {
                true => "no advised check".to_owned(),
                false => checks.join(", "),
            };
            match objects.set_spec(r#ref, "require", checks.into()).await? {
                (key, true) => println!("{key} requires {said}"),
                (key, false) => println!("{key} already requires {said}"),
            }
        }
        Command::Node {
            command:
                crate::NodeCommand::Drain {
                    node,
                    wait: true,
                    timeout,
                    output,
                    ..
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
                | crate::NodeCommand::Require { .. }
                | crate::NodeCommand::Pools { .. }
                | crate::NodeCommand::ErrorsSeen { .. }
                | crate::NodeCommand::Scrub { .. }
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
                    ..
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
                crate::VolumeCommand::Promote {
                    volume,
                    namespace,
                    yes,
                },
        } => {
            let namespace = namespace.unwrap_or_else(|| "default".into());
            if !yes {
                use std::io::{BufRead, IsTerminal, Write};
                if !std::io::stdin().is_terminal() {
                    anyhow::bail!(
                        "promoting {namespace}/{volume} hands it to its replica: confirm with --yes"
                    );
                }
                print!(
                    "Hand {namespace}/{volume} to its replica? What runs on its node now writes                      to a copy that is dropped. Type the Volume's name to go on: "
                );
                std::io::stdout().flush()?;
                let mut typed = String::new();
                std::io::stdin().lock().read_line(&mut typed)?;
                if typed.trim() != volume {
                    anyhow::bail!("not promoted");
                }
            }
            let r = pb::volumes_client::VolumesClient::new(channel.clone())
                .promote(pb::VolumePromoteRequest {
                    namespace: namespace.clone(),
                    name: volume.clone(),
                })
                .await?
                .into_inner();
            println!(
                "{namespace}/{volume} carries on on {}: {}",
                r.node, r.promoted
            );
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
                    from: Some(target),
                    namespace,
                    name,
                    ..
                },
        } => {
            let source = file.to_string_lossy().into_owned();
            let (volume, backup) = source
                .split_once('@')
                .ok_or_else(|| anyhow::anyhow!("with --from, give the backup as VOLUME@BACKUP"))?;
            let ns = namespace.unwrap_or_else(|| "default".into());
            let into = name.unwrap_or_else(|| volume.to_owned());
            pb::volumes_client::VolumesClient::new(channel.clone())
                .restore_from(pb::VolumeRestoreFromRequest {
                    namespace: ns.clone(),
                    name: volume.to_owned(),
                    target: target.clone(),
                    backup: backup.to_owned(),
                    node: node.clone(),
                    into: into.clone(),
                })
                .await?;
            println!("Volume/{ns}/{into} restored on {node} from {target}: {backup}");
        }
        Command::Volume {
            command:
                crate::VolumeCommand::Restore {
                    file,
                    node,
                    namespace,
                    name,
                    replace,
                    from: None,
                },
        } => {
            crate::volume_backup::restore(channel.clone(), &file, node, namespace, name, replace)
                .await?;
        }
        Command::Volume {
            command:
                crate::VolumeCommand::Backups {
                    volume,
                    from,
                    namespace,
                },
        } => {
            let ns = namespace.unwrap_or_else(|| "default".into());
            let r = pb::volumes_client::VolumesClient::new(channel.clone())
                .backups(pb::VolumeBackupsRequest {
                    namespace: ns.clone(),
                    name: volume.clone(),
                    target: from.unwrap_or_default(),
                })
                .await?
                .into_inner();
            if r.snapshots.is_empty() {
                println!("Volume/{ns}/{volume} has no backups there yet");
            } else {
                let rows: Vec<[String; 2]> = r
                    .snapshots
                    .into_iter()
                    .map(|held| {
                        let name = held
                            .rsplit(['@', '/'])
                            .next()
                            .unwrap_or(&held)
                            .trim_end_matches(".zfs")
                            .to_owned();
                        [name, held]
                    })
                    .collect();
                print_table(["BACKUP", "HELD AS"], &rows);
            }
        }
        Command::Volume { command } => {
            run_volume_snapshot(channel.clone(), command).await?;
        }
        Command::Rollback { service, namespace } => {
            println!(
                "{}",
                rollback(&mut objects, namespace.unwrap_or_default(), service).await?
            );
        }
        Command::Restart { service, namespace } => {
            let done = legion_manager::actions::restart(
                &mut objects,
                namespace.unwrap_or_default(),
                service,
            )
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
            println!("{done}");
        }
        Command::Stop { service, namespace } => {
            println!("{}", stop(&mut objects, namespace, service, true).await?);
        }
        Command::Start { service, namespace } => {
            println!("{}", stop(&mut objects, namespace, service, false).await?);
        }
        Command::Pause { service, namespace } => {
            println!("{}", pause(&mut objects, namespace, service, true).await?);
        }
        Command::Resume { service, namespace } => {
            println!("{}", pause(&mut objects, namespace, service, false).await?);
        }
        Command::Cronjob {
            command: crate::CronJobCommand::Run { cronjob, namespace },
        } => {
            let done = legion_manager::actions::run_now(
                &mut objects,
                namespace.unwrap_or_default(),
                cronjob,
            )
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
            println!("{done}");
        }
        Command::Reach { namespace } => {
            let namespace = namespace.unwrap_or_else(|| "default".into());
            let r = cluster
                .reach(pb::ReachRequest {
                    namespace: namespace.clone(),
                })
                .await?
                .into_inner();
            let rows: Vec<[String; 3]> = r
                .edges
                .iter()
                .map(|e| {
                    let from = if e.from.is_empty() {
                        "(other namespaces)".to_owned()
                    } else {
                        e.from.clone()
                    };
                    let ports = if e.all_ports {
                        "all".to_owned()
                    } else {
                        e.ports
                            .iter()
                            .map(|p| format!("{}/{}", p.port, p.protocol))
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    [from, e.to.clone(), ports]
                })
                .collect();
            if rows.is_empty() {
                println!(
                    "nothing reaches {namespace}'s {} services",
                    r.services.len()
                );
            } else {
                print_table(["FROM", "TO", "PORTS"], &rows);
            }
        }
        Command::Topology { namespace } => {
            let namespace = namespace.unwrap_or_else(|| "default".into());
            let read = |kind: Kind| {
                let mut objects = objects.clone();
                let namespace = namespace.clone();
                async move {
                    let resp = objects
                        .list(ListRequest {
                            kind: kind.to_string(),
                            namespace,
                            match_labels: Default::default(),
                        })
                        .await?
                        .into_inner();
                    resp.objects
                        .into_iter()
                        .map(Object::try_from)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(anyhow::Error::from)
                }
            };
            let services = read(Kind::Service).await?;
            let tasks = read(Kind::Task).await?;
            let routes = read(Kind::Route).await?;
            let volumes = read(Kind::Volume).await?;
            let reach = cluster
                .reach(pb::ReachRequest {
                    namespace: namespace.clone(),
                })
                .await?
                .into_inner()
                .edges;
            // A manager from before connections says nothing of them.
            let seen = cluster
                .connections(pb::ConnectionsRequest {
                    namespace: namespace.clone(),
                })
                .await
                .ok()
                .map(|r| r.into_inner().edges);
            let lines = crate::topology::lines(&crate::topology::Namespace {
                services: &services,
                tasks: &tasks,
                routes: &routes,
                volumes: &volumes,
                reach: &reach,
                seen: seen.as_deref(),
            });
            if lines.is_empty() {
                println!("no services in {namespace}");
            }
            for line in lines {
                println!("{line}");
            }
        }
        Command::Connections { namespace } => {
            let namespace = namespace.unwrap_or_else(|| "default".into());
            let r = cluster
                .connections(pb::ConnectionsRequest {
                    namespace: namespace.clone(),
                })
                .await?
                .into_inner();
            let rows: Vec<[String; 6]> = r
                .edges
                .iter()
                .map(|e| {
                    [
                        connection_end(e.from.as_ref()),
                        connection_end(e.to.as_ref()),
                        format!("{}/{}", e.port, e.protocol),
                        if e.via.is_empty() {
                            "-".to_owned()
                        } else {
                            e.via.clone()
                        },
                        e.connections.to_string(),
                        e.clients.to_string(),
                    ]
                })
                .collect();
            if rows.is_empty() {
                println!("no connections of {namespace}'s tasks");
            } else {
                print_table(
                    ["FROM", "TO", "PORT", "VIA", "CONNECTIONS", "CLIENTS"],
                    &rows,
                );
            }
            for n in &r.unanswered {
                eprintln!("node {} did not say: {}", n.node, n.error);
            }
        }
        Command::Events {
            object,
            namespace,
            watch,
        } => {
            let (kind, name) = match object.as_deref() {
                None => (None, String::new()),
                Some(o) => {
                    let (k, n) = o.split_once('/').unwrap_or((o, ""));
                    (Some(parse_kind(k)?), n.to_owned())
                }
            };
            let namespace = if kind.is_some_and(legion_api::Kind::is_cluster_scoped) {
                String::new()
            } else {
                namespace.unwrap_or_else(|| "default".into())
            };
            let mut stream = cluster
                .events(pb::EventsRequest {
                    namespace,
                    kind: kind.map(|k| k.to_string()).unwrap_or_default(),
                    name,
                    follow: watch,
                })
                .await?
                .into_inner();
            let mut first = true;
            while let Some(r) = stream.message().await? {
                let rows: Vec<[String; 4]> = r
                    .events
                    .iter()
                    .map(|e| {
                        [
                            legion_api::clock::format(e.at_ms.div_euclid(1000)),
                            format!("{}/{}", e.kind, e.name),
                            e.reason.clone(),
                            e.message.clone(),
                        ]
                    })
                    .collect();
                if first && rows.is_empty() && !watch {
                    println!("no events kept");
                } else if first {
                    print_table(["TIME", "OBJECT", "REASON", "MESSAGE"], &rows);
                } else {
                    for [time, object, reason, message] in rows {
                        println!("{time}  {object}  {reason}  {message}");
                    }
                }
                first = false;
            }
        }
        Command::Activity { id, watch, output } => {
            crate::activity::run(channel.clone(), id, watch, output).await?;
        }
        Command::LoadBalancers { namespace } => {
            let namespace = namespace.unwrap_or_else(|| "default".into());
            let r = cluster
                .load_balancers(pb::LoadBalancersRequest {
                    namespace: namespace.clone(),
                })
                .await?
                .into_inner();
            let rows: Vec<[String; 5]> = r
                .balancers
                .iter()
                .map(|b| {
                    let held: Vec<&str> = b
                        .nodes
                        .iter()
                        .filter(|n| n.state == "master")
                        .map(|n| n.node.as_str())
                        .collect();
                    let nodes = b
                        .nodes
                        .iter()
                        .map(|n| {
                            let state = if n.state.is_empty() { "?" } else { &n.state };
                            format!("{}({state})", n.node)
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    [
                        b.service.clone(),
                        b.address.clone(),
                        b.vhid.to_string(),
                        if held.is_empty() {
                            "-".to_owned()
                        } else {
                            held.join(",")
                        },
                        nodes,
                    ]
                })
                .collect();
            if rows.is_empty() {
                println!("{namespace} has no load balancer addresses");
            } else {
                print_table(["SERVICE", "ADDRESS", "VHID", "HELD BY", "NODES"], &rows);
            }
        }
        Command::Alerts {
            command: Some(crate::AlertsCommand::Test { receiver }),
            ..
        } => {
            cluster
                .test_alert_receiver(pb::TestAlertReceiverRequest {
                    name: receiver.clone(),
                })
                .await?;
            println!("AlertReceiver {receiver} took the test");
        }
        Command::Alerts {
            command: None,
            namespace,
            watch,
            output,
        } => {
            let mut stream = cluster
                .watch_alerts(pb::AlertsRequest {
                    namespace: namespace.unwrap_or_default(),
                })
                .await?
                .into_inner();
            let mut first = true;
            loop {
                let Some(r) = stream.message().await? else {
                    anyhow::bail!("the manager ended the alerts");
                };
                if !first && output == Format::Table {
                    println!();
                }
                first = false;
                print_alerts(&r.alerts, output)?;
                if !watch {
                    break;
                }
            }
        }
        Command::Quota { namespace, output } => {
            let r = cluster
                .quotas(pb::QuotasRequest {
                    namespace: namespace.unwrap_or_default(),
                })
                .await?
                .into_inner();
            print_quotas(&r, output)?;
        }
        Command::Usage {
            task,
            namespace,
            range,
            output,
        } => {
            let namespace = match (&task, namespace) {
                (Some(_), None) => "default".to_owned(),
                (_, ns) => ns.unwrap_or_default(),
            };
            let r = cluster
                .usage_history(pb::UsageHistoryRequest {
                    namespace,
                    task: task.unwrap_or_default(),
                    range_ms: range.map_or(0, |r| r.millis() as i64),
                })
                .await?
                .into_inner();
            print_usage_history(&r, output)?;
        }
        Command::Top { namespace, once } if once => crate::top::once(channel, namespace).await?,
        Command::Top { namespace, .. } => {
            let connection = crate::top::Connection {
                server: server.to_owned(),
                identity: identity.to_owned(),
            };
            crate::top::run(channel, namespace, connection).await?
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
        Command::Local(_) | Command::Build(_) => unreachable!("handled in main"),
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
                    let ns = Object::new(Kind::Namespace, "", &namespace, converted.namespace_spec());
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
        StackCommand::Ls { namespace } => {
            let resp = objects
                .list(ListRequest {
                    kind: Kind::Service.to_string(),
                    namespace: namespace.unwrap_or_default(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            let services = resp
                .objects
                .into_iter()
                .map(Object::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            print_table(
                ["NAMESPACE", "STACK", "SERVICES", "READY", "STATE"],
                &stacks(&services),
            );
        }
        StackCommand::Stop { stack, namespace } => {
            stop_stack(objects, stack, namespace, true).await?
        }
        StackCommand::Start { stack, namespace } => {
            stop_stack(objects, stack, namespace, false).await?
        }
    }
    Ok(())
}

/// `legion stack stop` (`stop`) or `start`: each of the stack's services.
async fn stop_stack(
    objects: &mut Objects,
    stack: String,
    namespace: Option<String>,
    stop_it: bool,
) -> anyhow::Result<()> {
    let namespace = namespace.unwrap_or_else(|| stack.clone());
    let services = stack_objects(objects, &stack, &namespace, &[Kind::Service]).await?;
    if services.is_empty() {
        bail!("no stack {stack} in namespace {namespace}");
    }
    for s in services {
        let said = stop(
            objects,
            Some(namespace.clone()),
            s.metadata.name.clone(),
            stop_it,
        )
        .await?;
        println!("{said}");
    }
    Ok(())
}

/// The stacks among `services`, a row each: namespace, stack, its
/// services, ready of desired tasks, and `stopped`, `running` or `partly
/// stopped`.
fn stacks(services: &[Object]) -> Vec<[String; 5]> {
    let mut by: std::collections::BTreeMap<(String, String), Vec<&Object>> = Default::default();
    for s in services {
        if let Some(stack) = s.metadata.labels.get(legion_api::compose::LABEL_STACK) {
            by.entry((s.metadata.namespace.clone(), stack.clone()))
                .or_default()
                .push(s);
        }
    }
    by.into_iter()
        .map(|((ns, stack), list)| {
            let n = |o: &Object, k: &str| o.status[k].as_u64().unwrap_or(0);
            let ready: u64 = list.iter().map(|o| n(o, "ready")).sum();
            let desired: u64 = list.iter().map(|o| n(o, "desired")).sum();
            let flagged = |field: &str| {
                list.iter()
                    .filter(|o| o.spec[field].as_bool() == Some(true))
                    .count()
            };
            let state = match (flagged("stopped"), flagged("paused")) {
                (0, 0) => "running",
                (s, _) if s == list.len() => "stopped",
                (0, p) if p == list.len() => "paused",
                (0, _) => "partly paused",
                _ => "partly stopped",
            };
            [
                ns,
                stack,
                list.len().to_string(),
                format!("{ready}/{desired}"),
                state.to_owned(),
            ]
        })
        .collect()
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

/// The change a command would make that is checked first (CONSOLE.md
/// phase 11), with its `--dry-run` and `--force`.
fn disruption(command: &Command) -> anyhow::Result<Option<(Change, bool, bool)>> {
    let namespace = |n: &Option<String>| {
        n.clone()
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| "default".into())
    };
    Ok(match command {
        Command::Node {
            command:
                crate::NodeCommand::Drain {
                    node,
                    dry_run,
                    force,
                    ..
                },
        } => Some((Change::Drain { node: node.clone() }, *dry_run, *force)),
        Command::Volume {
            command:
                crate::VolumeCommand::Move {
                    volume,
                    node,
                    namespace: ns,
                    dry_run,
                    force,
                },
        } => Some((
            Change::MoveVolume {
                namespace: namespace(ns),
                volume: volume.clone(),
                node: node.clone(),
            },
            *dry_run,
            *force,
        )),
        Command::Delete {
            kind,
            name,
            namespace: ns,
            dry_run,
            force,
        } => match parse_kind(kind)? {
            Kind::Node => Some((Change::RemoveNode { node: name.clone() }, *dry_run, *force)),
            Kind::Service => Some((
                Change::DeleteService {
                    namespace: namespace(ns),
                    service: name.clone(),
                },
                *dry_run,
                *force,
            )),
            _ if *dry_run => {
                anyhow::bail!("--dry-run is for a node's removal or a Service's deletion")
            }
            _ => None,
        },
        _ => None,
    })
}

/// What the leader says `change` would do; none from a manager from
/// before the check.
pub(crate) async fn dry_run_of(
    cluster: &mut ClusterClient<Channel>,
    change: &Change,
) -> anyhow::Result<Option<DryRun>> {
    let asked = cluster
        .dry_run(pb::DryRunRequest {
            change_json: serde_json::to_vec(change)?,
        })
        .await;
    match asked {
        Ok(r) => Ok(Some(
            serde_json::from_slice(&r.into_inner().dry_run_json).context("reading the dry run")?,
        )),
        Err(s) if s.code() == tonic::Code::Unimplemented => Ok(None),
        Err(s) => Err(s).with_context(|| format!("checking first: {}", change.describe())),
    }
}

/// Asks the leader what `change` would do. With `dry_run`, prints it all;
/// otherwise says what it found, if anything, and refuses one that would
/// block unless `force`. True if it would block. A manager from before
/// the check is not asked.
async fn check_first(
    cluster: &mut ClusterClient<Channel>,
    change: Change,
    dry_run: bool,
    force: bool,
) -> anyhow::Result<bool> {
    let d = match dry_run_of(cluster, &change).await? {
        Some(d) => d,
        None if !dry_run => return Ok(false),
        None => bail!("this manager does not check changes first: it is from before the check"),
    };
    let blocks = d.blocks();
    if dry_run {
        println!("{}", d.render());
    } else if blocks && !force {
        eprintln!("{}", d.render());
        std::process::exit(1);
    } else if blocks {
        let text = d.render();
        let found = text
            .rsplit_once('\n')
            .map_or(text.as_str(), |(found, _)| found);
        eprintln!("{found}\nGoing ahead anyway, as --force asks.");
    } else if !d.findings.is_empty() {
        eprintln!("{}", d.render());
    }
    Ok(blocks)
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
                            .map(|cs| {
                                cs.iter()
                                    .map(|c| marked_image(&field(c, &["image"]), &o.status))
                                    .collect()
                            })
                            .unwrap_or_default();
                        [
                            o.metadata.namespace.clone(),
                            o.metadata.name.clone(),
                            field(&o.spec, &["mode"]),
                            if o.spec["paused"].as_bool() == Some(true)
                                && o.spec["stopped"].as_bool() != Some(true)
                            {
                                // How many of those running are paused yet.
                                format!(
                                    "paused {}/{}",
                                    count(&o.status, "paused"),
                                    count(&o.status, "running")
                                )
                            } else {
                                format!(
                                    "{}/{}",
                                    count(&o.status, "ready"),
                                    count(&o.status, "desired")
                                )
                            },
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
                            if o.status["paused"].as_bool() == Some(true) {
                                "paused".to_owned()
                            } else {
                                field(&o.status, &["state"])
                            },
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
                let rows: Vec<[String; 6]> = objects
                    .iter()
                    .map(|o| {
                        let m = &o.status["migration"];
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.as_millis() as i64);
                        let replica = replica_cell(&o.status["replica"], now_ms);
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
                            replica,
                        ]
                    })
                    .collect();
                print_table(
                    ["NAMESPACE", "NAME", "SIZE", "NODE", "MIGRATION", "REPLICA"],
                    &rows,
                );
            }
            Kind::Node => {
                let rows: Vec<[String; 5]> = objects
                    .iter()
                    .map(|o| {
                        [
                            o.metadata.name.clone(),
                            node_state(o, field(&o.status, &["state"])),
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

/// An end of a connection as `legion connections` prints it: a service
/// by name, the others by kind.
pub(crate) fn connection_end(end: Option<&pb::ConnectionEnd>) -> String {
    let Some(e) = end else { return "?".to_owned() };
    match e.kind.as_str() {
        "service" => e.name.clone(),
        "namespace" => "(other namespaces)".to_owned(),
        "outside" => "(outside)".to_owned(),
        kind => format!("{kind}/{}", e.name),
    }
}

pub(crate) fn print_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) {
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
fn print_quotas(r: &pb::QuotasResponse, format: Format) -> anyhow::Result<()> {
    let json: Vec<serde_json::Value> = r
        .namespaces
        .iter()
        .map(|n| {
            let resources: Vec<_> = n
                .resources
                .iter()
                .map(|q| serde_json::json!({ "resource": q.resource, "limit": q.limit, "used": q.used }))
                .collect();
            serde_json::json!({ "namespace": n.namespace, "resources": resources })
        })
        .collect();
    match format {
        Format::Json => println!("{}", serde_json::to_string_pretty(&json)?),
        Format::Yaml => print!("{}", serde_yaml_ng::to_string(&json)?),
        Format::Table if r.namespaces.is_empty() => println!("no namespace with a quota"),
        Format::Table => {
            let show = |resource: &str, v: f64| match resource {
                "memory" | "storage" => format!("{:.1}Gi", v / (1u64 << 30) as f64),
                "cpu" => format!("{v:.2}"),
                _ => format!("{v}"),
            };
            let rows: Vec<[String; 5]> = r
                .namespaces
                .iter()
                .flat_map(|n| {
                    n.resources.iter().map(|q| {
                        let pct = if q.limit > 0.0 {
                            q.used / q.limit * 100.0
                        } else {
                            0.0
                        };
                        [
                            n.namespace.clone(),
                            q.resource.clone(),
                            show(&q.resource, q.used),
                            show(&q.resource, q.limit),
                            format!("{pct:.0}%"),
                        ]
                    })
                })
                .collect();
            print_table(["NAMESPACE", "RESOURCE", "USED", "LIMIT", "USE"], &rows);
        }
    }
    Ok(())
}

/// How long samples span, read short: `45 min`, `6 h`, `7 d`.
fn span(ms: i64) -> String {
    let min = ms / 60_000;
    if min < 120 {
        format!("{min} min")
    } else if min < 48 * 60 {
        format!("{} h", min / 60)
    } else {
        format!("{} d", min / (24 * 60))
    }
}

fn print_usage_history(r: &pb::UsageHistoryResponse, format: Format) -> anyhow::Result<()> {
    let json: Vec<serde_json::Value> = r
        .tasks
        .iter()
        .map(|t| {
            let samples: Vec<_> = t
                .samples
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "atMs": s.at_ms, "cpuPercent": s.cpu_percent, "memoryBytes": s.memory_bytes,
                    })
                })
                .collect();
            serde_json::json!({
                "namespace": t.namespace, "task": t.task, "node": t.node,
                "service": t.service, "samples": samples,
            })
        })
        .collect();
    match format {
        Format::Json => println!("{}", serde_json::to_string_pretty(&json)?),
        Format::Yaml => print!("{}", serde_yaml_ng::to_string(&json)?),
        Format::Table if r.tasks.is_empty() => {
            println!("no usage heard: no task ran, or the leader started since")
        }
        Format::Table => {
            let mib = |b: f64| format!("{:.1}Mi", b / (1024.0 * 1024.0));
            let rows: Vec<[String; 8]> = r
                .tasks
                .iter()
                .filter(|t| !t.samples.is_empty())
                .map(|t| {
                    let n = t.samples.len() as f64;
                    let cpu: Vec<f64> = t.samples.iter().map(|s| s.cpu_percent).collect();
                    let mem: Vec<f64> = t.samples.iter().map(|s| s.memory_bytes as f64).collect();
                    let max = |v: &[f64]| v.iter().copied().fold(0.0, f64::max);
                    let last = |v: &[f64]| v.last().copied().unwrap_or(0.0);
                    [
                        format!("{}/{}", t.namespace, t.task),
                        t.node.clone(),
                        format!("{:.1}%", last(&cpu)),
                        format!("{:.1}%", cpu.iter().sum::<f64>() / n),
                        format!("{:.1}%", max(&cpu)),
                        mib(last(&mem)),
                        mib(max(&mem)),
                        span(t.samples.len() as i64 * r.step_ms),
                    ]
                })
                .collect();
            print_table(
                ["TASK", "NODE", "CPU", "AVG", "MAX", "MEMORY", "MAX", "OVER"],
                &rows,
            );
        }
    }
    Ok(())
}

fn print_alerts(alerts: &[pb::Alert], format: Format) -> anyhow::Result<()> {
    let json: Vec<serde_json::Value> = alerts
        .iter()
        .map(|a| {
            serde_json::json!({
                "kind": a.kind, "severity": a.severity, "namespace": a.namespace,
                "object": a.object, "message": a.message, "sinceMs": a.since_ms,
            })
        })
        .collect();
    match format {
        Format::Json => println!("{}", serde_json::to_string_pretty(&json)?),
        Format::Yaml => print!("{}", serde_yaml_ng::to_string(&json)?),
        Format::Table if alerts.is_empty() => println!("no alerts"),
        Format::Table => {
            let rows: Vec<[String; 5]> = alerts
                .iter()
                .map(|a| {
                    let object = match (a.namespace.as_str(), a.object.as_str()) {
                        (_, "") => "(cluster)".to_owned(),
                        ("", o) => o.to_owned(),
                        (ns, o) => format!("{ns}/{o}"),
                    };
                    let since = legion_api::clock::format(a.since_ms / 1000);
                    [
                        a.severity.clone(),
                        a.kind.clone(),
                        object,
                        since,
                        a.message.clone(),
                    ]
                })
                .collect();
            print_table(["SEVERITY", "KIND", "OBJECT", "SINCE", "MESSAGE"], &rows);
        }
    }
    Ok(())
}

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

async fn run_volume_snapshot(
    channel: Channel,
    command: crate::VolumeCommand,
) -> anyhow::Result<()> {
    let mut client = pb::volumes_client::VolumesClient::new(channel);
    let mut into = String::new();
    let (volume, namespace, op, snapshot) = match command {
        crate::VolumeCommand::Snapshots { volume, namespace } => {
            (volume, namespace, "list", String::new())
        }
        crate::VolumeCommand::Snapshot {
            volume,
            snapshot,
            namespace,
        } => (volume, namespace, "create", snapshot),
        crate::VolumeCommand::Rollback {
            volume,
            snapshot,
            namespace,
            yes,
        } => {
            // What goes with it: the snapshots after it.
            let ns = namespace.clone().unwrap_or_else(|| "default".into());
            let listed = client
                .snapshot(pb::VolumeSnapshotRequest {
                    namespace: ns.clone(),
                    name: volume.clone(),
                    op: "list".to_owned(),
                    snapshot: String::new(),
                    into: String::new(),
                })
                .await?
                .into_inner()
                .snapshots;
            let later: Vec<_> = listed
                .iter()
                .skip_while(|s| **s != snapshot)
                .skip(1)
                .cloned()
                .collect();
            if !yes {
                use std::io::{BufRead, IsTerminal, Write};
                let gone = if later.is_empty() {
                    String::new()
                } else {
                    format!(
                        " Destroyed with it: {}; a replica's or backup's base after it too, so their next copy is whole.",
                        later.join(", ")
                    )
                };
                if !std::io::stdin().is_terminal() {
                    anyhow::bail!(
                        "rolling Volume/{ns}/{volume} back to @{snapshot} loses what was written since.{gone} Confirm with --yes"
                    );
                }
                print!(
                    "Roll Volume/{ns}/{volume} back to @{snapshot}? What was written since is lost.{gone} [y/N] "
                );
                std::io::stdout().flush()?;
                let mut said = String::new();
                std::io::stdin().lock().read_line(&mut said)?;
                if !matches!(said.trim(), "y" | "Y" | "yes") {
                    anyhow::bail!("not rolled back");
                }
            }
            (volume, namespace, "rollback", snapshot)
        }
        crate::VolumeCommand::Clone {
            source,
            name,
            namespace,
        } => {
            let (volume, snapshot) = source
                .split_once('@')
                .ok_or_else(|| anyhow::anyhow!("give the snapshot as VOLUME@SNAPSHOT"))?;
            into = name;
            (volume.to_owned(), namespace, "clone", snapshot.to_owned())
        }
        crate::VolumeCommand::RmSnapshot {
            volume,
            snapshot,
            namespace,
        } => (volume, namespace, "delete", snapshot),
        _ => unreachable!("handled above"),
    };
    let ns = namespace.unwrap_or_else(|| "default".into());
    let r = client
        .snapshot(pb::VolumeSnapshotRequest {
            namespace: ns.clone(),
            name: volume.clone(),
            op: op.to_owned(),
            snapshot: snapshot.clone(),
            into: into.clone(),
        })
        .await?
        .into_inner();
    match op {
        "list" => {
            if r.snapshots.is_empty() {
                println!("Volume/{ns}/{volume} has no snapshots");
            } else {
                let rows: Vec<[String; 1]> = r.snapshots.into_iter().map(|s| [s]).collect();
                print_table(["SNAPSHOT"], &rows);
            }
        }
        "create" => println!("Volume/{ns}/{volume}@{snapshot} created"),
        "rollback" => println!("Volume/{ns}/{volume} rolled back to @{snapshot}"),
        "delete" => println!("Volume/{ns}/{volume}@{snapshot} deleted"),
        "clone" => println!("Volume/{ns}/{into} made from {volume}@{snapshot}"),
        _ => {}
    }
    Ok(())
}

async fn run_image_command(
    cluster: &mut ClusterClient<Channel>,
    command: crate::ImageCommand,
) -> anyhow::Result<()> {
    match command {
        crate::ImageCommand::Load {
            file,
            name,
            node: Some(node),
            ..
        } => {
            use pb::load_image_chunk::Part;
            let bytes =
                std::fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            let send = |part| pb::LoadImageChunk { part: Some(part) };
            let reference = name.unwrap_or_default();
            let node_name = node.clone();
            tokio::spawn(async move {
                if tx
                    .send(send(Part::Start(pb::LoadImageStart {
                        node: node_name,
                        reference,
                    })))
                    .await
                    .is_err()
                {
                    return;
                }
                for chunk in bytes.chunks(1 << 20) {
                    if tx.send(send(Part::Data(chunk.to_vec()))).await.is_err() {
                        return;
                    }
                }
            });
            let r = cluster
                .load_image(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await?
                .into_inner();
            let loaded = r
                .images
                .first()
                .map(|i| i.reference.as_str())
                .unwrap_or("(loaded)");
            println!("loaded {loaded} on {node} ({})", r.digest);
        }
        crate::ImageCommand::Load { .. } => unreachable!("local load handled in main"),
        crate::ImageCommand::Ls { node, output } => {
            let r = cluster
                .node_images(pb::NodeImagesRequest {
                    op: "list".to_owned(),
                    node: node.unwrap_or_default(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            let items: Vec<serde_json::Value> = r
                .images
                .iter()
                .map(|i| {
                    serde_json::json!({
                        "node": i.node,
                        "reference": i.reference,
                        "digest": i.digest,
                        "os": i.os,
                        "arch": i.arch,
                        "layers": i.layers,
                    })
                })
                .collect();
            match output {
                Format::Json => println!("{}", serde_json::to_string_pretty(&items)?),
                Format::Yaml => print!("{}", serde_yaml_ng::to_string(&items)?),
                Format::Table => {
                    if r.images.is_empty() {
                        println!("no cached images");
                    } else {
                        let rows: Vec<[String; 5]> = r
                            .images
                            .iter()
                            .map(|i| {
                                [
                                    i.node.clone(),
                                    i.reference.clone(),
                                    format!("{}/{}", i.os, i.arch),
                                    i.layers.to_string(),
                                    i.digest.chars().take(19).collect(),
                                ]
                            })
                            .collect();
                        print_table(["NODE", "IMAGE", "PLATFORM", "LAYERS", "DIGEST"], &rows);
                    }
                }
            }
        }
        crate::ImageCommand::Pull {
            image,
            node,
            namespace,
            secret,
        } => {
            let scope = node.clone().unwrap_or_default();
            let r = cluster
                .node_images(pb::NodeImagesRequest {
                    op: "pull".to_owned(),
                    node: scope.clone(),
                    image: image.clone(),
                    namespace: namespace.unwrap_or_default(),
                    secret: secret.unwrap_or_default(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            let where_ = if scope.is_empty() {
                "all ready nodes"
            } else {
                &scope
            };
            println!("pulled {image} onto {where_} ({})", r.digest);
        }
        crate::ImageCommand::Push {
            image,
            target,
            node,
            namespace,
            secret,
        } => {
            let r = cluster
                .node_images(pb::NodeImagesRequest {
                    op: "push".to_owned(),
                    node: node.clone(),
                    image: image.clone(),
                    target: target.clone(),
                    namespace: namespace.unwrap_or_default(),
                    secret: secret.unwrap_or_default(),
                })
                .await?
                .into_inner();
            println!("pushed {image} from {node} -> {target} ({})", r.digest);
        }
        crate::ImageCommand::Rm { image, node } => {
            let scope = node.unwrap_or_default();
            let r = cluster
                .node_images(pb::NodeImagesRequest {
                    op: "remove".to_owned(),
                    node: scope,
                    image: image.clone(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            println!("removed {image} ({} node(s))", r.removed);
        }
        crate::ImageCommand::Prune { node, builds } => {
            let r = cluster
                .node_images(pb::NodeImagesRequest {
                    op: if builds { "prune-builds" } else { "prune" }.to_owned(),
                    node: node.unwrap_or_default(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            if builds {
                println!("pruned the build store");
            } else {
                println!("pruned {} unused layer(s)", r.removed);
            }
        }
        crate::ImageCommand::Scan { image, node, json } => {
            let r = cluster
                .scan_image(pb::ScanImageRequest { node, image })
                .await?
                .into_inner();
            if json {
                let issues = |p: &pb::VulnerablePackage| -> Vec<serde_json::Value> {
                    p.issues
                        .iter()
                        .map(|i| {
                            serde_json::json!({
                                "description": i.description, "cves": i.cves,
                                "url": i.url, "affected": i.affected,
                            })
                        })
                        .collect()
                };
                let said = serde_json::json!({
                    "image": r.image,
                    "node": r.node,
                    "packages": r.packages.iter()
                        .map(|p| serde_json::json!({ "name": p.name, "version": p.version }))
                        .collect::<Vec<_>>(),
                    "vulnerable": r.vulnerable.iter()
                        .map(|p| serde_json::json!({
                            "name": p.name, "version": p.version, "issues": issues(p),
                        }))
                        .collect::<Vec<_>>(),
                    "used_by": r.used_by,
                });
                println!("{}", serde_json::to_string_pretty(&said)?);
            } else {
                print!("{}", scan_text(&r));
            }
            if !r.vulnerable.is_empty() {
                bail!("{} vulnerable package(s)", r.vulnerable.len());
            }
        }
        crate::ImageCommand::Export {
            image,
            node,
            output,
            tag,
        } => {
            let mut stream = cluster
                .export_image(pb::ExportImageRequest {
                    node: node.clone(),
                    image: image.clone(),
                    tag: tag.unwrap_or_default(),
                })
                .await?
                .into_inner();
            let mut file = std::fs::File::create(&output)
                .with_context(|| format!("creating {}", output.display()))?;
            let mut bytes = 0u64;
            while let Some(chunk) = stream.message().await? {
                std::io::Write::write_all(&mut file, &chunk.data)?;
                bytes += chunk.data.len() as u64;
            }
            println!(
                "exported {image} from {node} to {} ({bytes} bytes)",
                output.display()
            );
        }
        crate::ImageCommand::Search {
            query,
            limit,
            namespace,
            secret,
        } => {
            let ns = secret
                .as_ref()
                .map(|_| namespace.as_deref().unwrap_or("default").to_owned())
                .unwrap_or_default();
            let r = cluster
                .search_images(pb::SearchImagesRequest {
                    query: query.unwrap_or_default(),
                    limit,
                    namespace: ns,
                    secret: secret.unwrap_or_default(),
                })
                .await?
                .into_inner();
            if r.results.is_empty() {
                println!("no matching images");
            } else {
                let rows: Vec<[String; 5]> = r
                    .results
                    .into_iter()
                    .map(|h| {
                        [
                            h.name,
                            h.suggested_tag,
                            if h.os.is_empty() {
                                "?".to_owned()
                            } else {
                                h.os
                            },
                            h.source,
                            h.description.chars().take(60).collect(),
                        ]
                    })
                    .collect();
                print_table(["IMAGE", "TAG", "OS", "SOURCE", "DESCRIPTION"], &rows);
            }
        }
        crate::ImageCommand::Sbom {
            image,
            namespace,
            secret,
            json,
        } => {
            let r = cluster
                .inspect_registry_image(pb::InspectRegistryImageRequest {
                    image,
                    namespace: namespace.unwrap_or_default(),
                    secret: secret.unwrap_or_default(),
                    node: String::new(),
                })
                .await?
                .into_inner();
            let Some(sbom) = sbom_of(&r) else {
                bail!("{} ({}) has no SBOM attested", r.image, r.digest);
            };
            if json {
                let packages: Vec<_> = sbom
                    .packages
                    .iter()
                    .map(|p| serde_json::json!({ "name": p.name, "version": p.version }))
                    .collect();
                let out = serde_json::json!({
                    "image": r.image,
                    "digest": r.digest,
                    "predicateType": sbom.predicate_type,
                    "verifiedKey": sbom.verified_key,
                    "packages": packages,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Image:     {}", r.image);
                println!("Digest:    {}", r.digest);
                println!("SBOM:      {}", attestation_line(sbom));
                let rows: Vec<[String; 2]> = sbom
                    .packages
                    .iter()
                    .map(|p| [p.name.clone(), p.version.clone()])
                    .collect();
                println!();
                print_table(["PACKAGE", "VERSION"], &rows);
            }
        }
        crate::ImageCommand::Inspect {
            image,
            namespace,
            secret,
            node,
        } => {
            let r = cluster
                .inspect_registry_image(pb::InspectRegistryImageRequest {
                    image,
                    namespace: namespace.unwrap_or_default(),
                    secret: secret.unwrap_or_default(),
                    node: node.unwrap_or_default(),
                })
                .await?
                .into_inner();
            println!("Image:     {}", r.image);
            println!("Digest:    {}", r.digest);
            println!(
                "Signed:    {}",
                if r.signed { "verified (cosign)" } else { "no" }
            );
            if !r.platforms.is_empty() {
                let plats: Vec<String> = r
                    .platforms
                    .iter()
                    .map(|p| format!("{}/{}", p.os, p.arch))
                    .collect();
                println!("Platforms: {}", plats.join(", "));
            }
            if !r.tags.is_empty() {
                println!("Tags:      {}", r.tags.join(", "));
            }
            print!("{}", image_details(&r));
        }
    }
    Ok(())
}

/// What `legion image scan` says of an image.
fn scan_text(r: &pb::ScanImageResponse) -> String {
    let mut out = format!("{} on {}: ", r.image, r.node);
    if r.packages.is_empty() {
        out += "no FreeBSD package database: nothing to audit\n";
    } else if r.vulnerable.is_empty() {
        out += &format!(
            "{} packages, none with a known vulnerability\n",
            r.packages.len()
        );
    } else {
        out += &format!(
            "{} packages, {} with known vulnerabilities\n",
            r.packages.len(),
            r.vulnerable.len()
        );
        for p in &r.vulnerable {
            out += &format!("\n{}-{}\n", p.name, p.version);
            for i in &p.issues {
                out += &format!("  {}\n", i.description);
                if !i.cves.is_empty() {
                    out += &format!("    {}\n", i.cves.join(" "));
                }
                if !i.url.is_empty() {
                    out += &format!("    {}\n", i.url);
                }
            }
        }
    }
    if !r.used_by.is_empty() {
        out += &format!("\nUsed by: {}\n", r.used_by.join(", "));
    }
    out
}

/// `TASK:PATH`, a task's file, or `None` for a local path. A drive's
/// letter (`C:\x`) and a path with a separator before the colon are local.
fn remote(arg: &str) -> Option<(&str, &str)> {
    let (task, path) = arg.split_once(':')?;
    let local = task.len() < 2 || task.contains(['/', '\\']);
    (!local).then_some((task, path))
}

/// A listing as `ls -l` would show it, near enough.
fn files_table(entries: &[serde_json::Value]) -> String {
    let mut out = String::new();
    for e in entries {
        let s = |k: &str| e[k].as_str().unwrap_or_default().to_owned();
        let kind = match s("kind").as_str() {
            "dir" => 'd',
            "link" => 'l',
            "file" => '-',
            _ => '?',
        };
        let name = match s("kind").as_str() {
            "dir" => format!("{}/", s("name")),
            "link" => format!("{} -> {}", s("name"), s("target")),
            _ => s("name"),
        };
        out += &format!(
            "{kind}{:>5} {:>10}  {name}\n",
            s("mode"),
            e["size"].as_u64().unwrap_or(0)
        );
    }
    out
}

/// `legion stop` (`stop`) or `legion start`: the service's `stopped` set;
/// what was done, for the user.
async fn stop(
    objects: &mut Objects,
    namespace: Option<String>,
    service: String,
    stop: bool,
) -> anyhow::Result<String> {
    let state = if stop { "stopped" } else { "started" };
    set_flag(objects, namespace, service, "stopped", stop, state).await
}

/// `legion pause` (`pause`) or `legion resume`: the service's `paused` set;
/// what was done, for the user.
async fn pause(
    objects: &mut Objects,
    namespace: Option<String>,
    service: String,
    pause: bool,
) -> anyhow::Result<String> {
    let state = if pause { "paused" } else { "resumed" };
    set_flag(objects, namespace, service, "paused", pause, state).await
}

/// A service's spec `field` set to `on`; `state` says what that made it.
async fn set_flag(
    objects: &mut Objects,
    namespace: Option<String>,
    service: String,
    field: &str,
    on: bool,
    state: &str,
) -> anyhow::Result<String> {
    let (key, changed) = legion_manager::actions::set_spec(
        objects,
        pb::ObjectRef {
            kind: "Service".into(),
            namespace: namespace.unwrap_or_default(),
            name: service,
        },
        field,
        on.into(),
    )
    .await
    .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
    Ok(if changed {
        format!("{key} {state}")
    } else {
        format!("{key} was already {state}")
    })
}

/// How an image was built and runs, and what uses it, as `legion image
/// inspect` shows it.
/// An attestation's type and who signed it, on one line.
fn attestation_line(a: &pb::ImageAttestation) -> String {
    let signed = if !a.verified_key.is_empty() {
        format!("signed by {}", a.verified_key)
    } else if !a.why.is_empty() {
        format!("NOT VERIFIED: {}", a.why)
    } else {
        "unverified: no ImagePolicy covers the image".to_owned()
    };
    let packages = match a.packages.len() {
        0 => String::new(),
        n => format!(", {n} packages"),
    };
    format!("{}{packages} ({signed})", a.predicate_type)
}

/// The SBOM to show of an image's attestations: one verified, if any is.
fn sbom_of(r: &pb::InspectRegistryImageResponse) -> Option<&pb::ImageAttestation> {
    let sboms = || {
        r.attestations
            .iter()
            .filter(|a| legion_image::attest::is_sbom(&a.predicate_type))
    };
    sboms()
        .find(|a| !a.verified_key.is_empty())
        .or_else(|| sboms().next())
}

fn image_details(r: &pb::InspectRegistryImageResponse) -> String {
    let mut out = String::new();
    let mut line = |name: &str, v: &str| {
        if !v.is_empty() {
            out += &format!("{name:<11}{v}\n");
        }
    };
    if !r.os.is_empty() {
        line("Runs on:", &format!("{}/{}", r.os, r.arch));
    }
    if r.size > 0 {
        line(
            "Size:",
            &format!("{} (compressed)", crate::top::bytes(r.size)),
        );
    }
    if let Some(run) = &r.run {
        line("Entrypoint:", &run.entrypoint.join(" "));
        line("Command:", &run.cmd.join(" "));
        line("User:", &run.user);
        line("Directory:", &run.working_dir);
        line("Ports:", &run.exposed_ports.join(", "));
    }
    if r.used_by.is_empty() {
        line("Used by:", "nothing you can see");
    } else {
        line("Used by:", &r.used_by.join(", "));
    }
    for (i, a) in r.attestations.iter().enumerate() {
        let name = if i == 0 { "Attested:" } else { "" };
        out += &format!("{name:<11}{}\n", attestation_line(a));
    }
    if !r.history.is_empty() {
        out += "\nHistory, first step first:\n";
        for s in &r.history {
            let added = if s.empty_layer {
                "-".to_owned()
            } else if s.size > 0 {
                crate::top::bytes(s.size)
            } else {
                "?".to_owned()
            };
            out += &format!("  {added:>9}  {}\n", s.created_by.trim());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pools_row_says_how_full_its_errors_and_its_last_scrub() {
        let mut p = legion_api::node::Pool {
            name: "tank".into(),
            state: "DEGRADED".into(),
            size_bytes: 100 << 30,
            alloc_bytes: 25 << 30,
            checksum_errors: 3,
            scan: Some(legion_api::node::PoolScan {
                function: "scrub".into(),
                state: "finished".into(),
                end_ms: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pool_row("n1", &p, 2 * 86_400_000),
            [
                "n1",
                "tank",
                "DEGRADED",
                "25% (25.0 of 100.0 GiB)",
                "0 read, 0 write, 3 checksum",
                "2d ago"
            ]
        );
        p.scan.as_mut().unwrap().function = "resilver".into();
        p.scan.as_mut().unwrap().state = "scanning".into();
        p.checksum_errors = 0;
        let row = pool_row("n1", &p, 0);
        assert_eq!(
            (row[4].as_str(), row[5].as_str()),
            ("none", "resilver running")
        );
        p.scan = None;
        assert_eq!(pool_row("n1", &p, 0)[5], "never");
    }

    #[test]
    fn a_volumes_replica_says_where_and_how_old() {
        let r = |v: serde_json::Value| replica_cell(&v, 1_000_000);
        assert_eq!(r(serde_json::Value::Null), "");
        assert_eq!(r(serde_json::json!({"node": "n2"})), "n2, no copy yet");
        assert_eq!(
            r(serde_json::json!({"node": "n2", "takenMs": 955_000})),
            "n2, 45s ago"
        );
        assert_eq!(
            r(serde_json::json!({"node": "n2", "takenMs": 400_000, "message": "out of space"})),
            "n2, 10m ago: out of space"
        );
        assert_eq!(
            r(serde_json::json!({"node": "", "message": "no other node may keep it"})),
            "no other node may keep it"
        );
        assert_eq!(
            r(serde_json::json!({
                "node": "n2", "takenMs": 400_000, "state": "stale", "copyingMs": 999_000,
                "blocked": ["web uses it with onPartition: run"]
            })),
            "n2, 10m ago, stale, copying, failover blocked"
        );
    }

    #[test]
    fn an_images_attestations_say_who_signed_them_and_the_sbom_is_found() {
        use legion_image::attest::{SLSA_PROVENANCE_1, SPDX};
        let a = |t: &str, key: &str, why: &str, n: usize| pb::ImageAttestation {
            predicate_type: t.into(),
            verified_key: key.into(),
            why: why.into(),
            packages: (0..n)
                .map(|i| pb::ImagePackage {
                    name: format!("p{i}"),
                    version: "1".into(),
                })
                .collect(),
            predicate_json: String::new(),
        };
        let r = pb::InspectRegistryImageResponse {
            attestations: vec![
                a(SPDX, "", "made by none of the keys given", 1),
                a(SLSA_PROVENANCE_1, "", "", 0),
                a(SPDX, "sha256:k", "", 2),
            ],
            ..Default::default()
        };
        let out = image_details(&r);
        assert!(
            out.contains(&format!(
                "Attested:  {SPDX}, 1 packages (NOT VERIFIED: made by none of the keys given)"
            )),
            "{out}"
        );
        assert!(
            out.contains(&format!(
                "           {SLSA_PROVENANCE_1} (unverified: no ImagePolicy covers the image)"
            )),
            "{out}"
        );
        assert_eq!(sbom_of(&r).unwrap().verified_key, "sha256:k");
        assert!(sbom_of(&pb::InspectRegistryImageResponse::default()).is_none());
    }

    #[test]
    fn a_stack_is_its_labelled_services_together() {
        let service = |ns: &str, name: &str, stack: Option<&str>, stopped: bool, ready: u64| {
            let mut o = Object::new(
                Kind::Service,
                ns,
                name,
                serde_json::json!({ "stopped": stopped }),
            );
            if let Some(stack) = stack {
                o.metadata
                    .labels
                    .insert(legion_api::compose::LABEL_STACK.into(), stack.into());
            }
            o.status =
                serde_json::json!({ "ready": ready, "desired": if stopped { 0 } else { 2 } });
            o
        };
        let rows = stacks(&[
            service("shop", "web", Some("shop"), false, 2),
            service("shop", "db", Some("shop"), false, 1),
            service("blog", "app", Some("blog"), true, 0),
            service("blog", "loose", None, false, 2),
            service("mixed", "a", Some("mixed"), true, 0),
            service("mixed", "b", Some("mixed"), false, 2),
        ]);
        let row = |r: &[String; 5]| r.join(" ");
        let rows: Vec<_> = rows.iter().map(row).collect();
        assert_eq!(
            rows,
            [
                "blog blog 1 0/0 stopped",
                "mixed mixed 2 2/2 partly stopped",
                "shop shop 2 3/4 running",
            ]
        );
    }

    #[test]
    fn a_tasks_file_is_told_from_a_local_one() {
        assert_eq!(
            remote("web-1-1:/etc/rc.conf"),
            Some(("web-1-1", "/etc/rc.conf"))
        );
        assert_eq!(remote("./rc.conf"), None);
        assert_eq!(remote(r"C:\Users\me\rc.conf"), None);
        assert_eq!(remote("dir/x:y"), None);
        assert_eq!(remote("-"), None);
    }

    #[test]
    fn a_listing_reads_as_ls_does() {
        let entries = vec![
            serde_json::json!({ "name": "etc", "kind": "dir", "size": 512, "mode": "755" }),
            serde_json::json!({ "name": "rc.conf", "kind": "file", "size": 20, "mode": "644" }),
            serde_json::json!({ "name": "lib", "kind": "link", "size": 7, "mode": "777", "target": "usr/lib" }),
        ];
        let t = files_table(&entries);
        let lines: Vec<_> = t.lines().collect();
        assert_eq!(lines[0], "d  755        512  etc/");
        assert_eq!(lines[1], "-  644         20  rc.conf");
        assert_eq!(lines[2], "l  777          7  lib -> usr/lib");
    }

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
