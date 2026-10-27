//! CLI commands: thin gRPC calls plus formatting.

use std::io::Read;
use std::path::Path;

use anyhow::{Context, bail};
use legion_api::{API_VERSION, Kind, Object};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    self as pb, ApplyRequest, ApplyResult, DeleteRequest, InitRequest, ListRequest, ObjectRef,
    StatusRequest, StatusResponse, WatchRequest, watch_event,
};
use tonic::transport::Channel;

use crate::Command;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Table,
    Yaml,
    Json,
}

pub async fn run(server: &str, identity: &Path, command: Command) -> anyhow::Result<()> {
    let channel = connect(server, identity).await?;
    let mut objects = ObjectsClient::new(channel.clone());
    let mut cluster = ClusterClient::new(channel);
    match command {
        Command::Daemon { .. } => unreachable!("handled in main"),
        Command::Init => {
            let status = cluster.init(InitRequest {}).await?.into_inner();
            println!(
                "cluster initialized; this manager is node {}",
                status.node_id
            );
        }
        Command::Status => print_status(&cluster.status(StatusRequest {}).await?.into_inner()),
        Command::Apply { files } => {
            for file in files {
                for object in read_objects(&file)? {
                    let key = object.key();
                    let resp = objects
                        .apply(ApplyRequest {
                            object: Some((&object).into()),
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
                }
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

/// Prints how applying `object` would change the stored one; true if it would.
async fn diff(objects: &mut ObjectsClient<Channel>, object: &Object) -> anyhow::Result<bool> {
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
                let rows: Vec<[String; 5]> = objects
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
                        ]
                    })
                    .collect();
                print_table(["NAMESPACE", "NAME", "MODE", "READY", "IMAGES"], &rows);
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
