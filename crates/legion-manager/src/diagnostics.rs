//! The cluster's diagnostics (`Diagnose`): what `legion doctor` finds on
//! each node, asked of its agent; and the support bundle (`SupportBundle`)
//! sent with a report of trouble, the cluster as it stands with what could
//! be secret left out.

use std::time::Duration;

use legion_api::Kind;
use legion_api::node::{NodeState, NodeStatus};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{self as pb, DoctorOptions, SessionFrame};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::api::{ApiService, forwarded_for};
use crate::image_ops::{attach, leader_client};
use crate::session::Attachment;

/// How long a node has to say what it finds.
const ANSWER: Duration = Duration::from_secs(30);

/// Kinds whose objects go into a support bundle: all but Secrets.
const BUNDLED: [Kind; 13] = [
    Kind::Namespace,
    Kind::Node,
    Kind::Service,
    Kind::Job,
    Kind::CronJob,
    Kind::Task,
    Kind::Config,
    Kind::Volume,
    Kind::NetworkPolicy,
    Kind::Route,
    Kind::RoleBinding,
    Kind::ImagePolicy,
    Kind::Build,
];

pub(crate) async fn diagnose(
    api: &ApiService,
    req: Request<pb::DiagnoseRequest>,
) -> Result<Response<pb::DiagnoseResponse>, Status> {
    let caller = api.caller(&req)?;
    if let Some(mut leader) = leader_client(api, &req)? {
        return leader
            .diagnose(forwarded_for(req.into_inner(), &caller)?)
            .await;
    }
    caller.require_all("diagnose the cluster's nodes")?;
    api.consistent().await?;
    let nodes = diagnose_nodes(api, &req.into_inner().node).await?;
    Ok(Response::new(pb::DiagnoseResponse { nodes }))
}

/// Each node's diagnosis, or `only`'s; asked at once.
async fn diagnose_nodes(api: &ApiService, only: &str) -> Result<Vec<pb::NodeDiagnosis>, Status> {
    let list = api
        .store
        .list(Kind::Node, None)
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut nodes: Vec<(String, Option<NodeState>)> = list
        .objects
        .into_iter()
        .filter(|o| only.is_empty() || o.metadata.name == only)
        .map(|o| {
            let state = serde_json::from_value::<NodeStatus>(o.status.clone())
                .ok()
                .map(|s| s.state);
            (o.metadata.name, state)
        })
        .collect();
    if !only.is_empty() && nodes.is_empty() {
        return Err(Status::not_found(format!("no node {only}")));
    }
    nodes.sort_by(|a, b| a.0.cmp(&b.0));
    let asked = nodes.into_iter().map(|(node, state)| async move {
        match state {
            Some(NodeState::Ready | NodeState::Suspect) => match ask(api, &node).await {
                Ok(said) => pb::NodeDiagnosis {
                    ready: said.ready,
                    findings: said.findings,
                    pf_fix_json: said.pf_fix.map(|v| v.to_string()).unwrap_or_default(),
                    node,
                    error: String::new(),
                },
                Err(e) => pb::NodeDiagnosis {
                    error: e.message().to_owned(),
                    node,
                    ..Default::default()
                },
            },
            state => pb::NodeDiagnosis {
                error: match state {
                    Some(s) => format!("not asked: the node is {}", word(&s)),
                    None => "not asked: the node has not registered".to_owned(),
                },
                node,
                ..Default::default()
            },
        }
    });
    Ok(futures::future::join_all(asked).await)
}

fn word<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// What a node's agent says its doctor found (`legion_agent::doctor`).
#[derive(Deserialize)]
struct Said {
    ready: bool,
    #[serde(deserialize_with = "findings")]
    findings: Vec<pb::DoctorFinding>,
    /// What `--fix pf` would do there; agents before it say nothing.
    #[serde(default, rename = "pfFix")]
    pf_fix: Option<serde_json::Value>,
}

/// Findings as `legion doctor --json` writes them.
fn findings<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<pb::DoctorFinding>, D::Error> {
    #[derive(Deserialize)]
    struct Written {
        id: String,
        state: String,
        need: String,
        #[serde(default)]
        detail: String,
        #[serde(default)]
        remedy: Option<String>,
    }
    let written = Vec::<Written>::deserialize(d)?;
    Ok(written
        .into_iter()
        .map(|w| pb::DoctorFinding {
            id: w.id,
            state: w.state,
            need: w.need,
            detail: w.detail,
            remedy: w.remedy.unwrap_or_default(),
        })
        .collect())
}

/// What the doctor finds on `node`, asked of its agent.
async fn ask(api: &ApiService, node: &str) -> Result<Said, Status> {
    let asking = async {
        let Attachment { mut from_agent, .. } =
            attach(api, node, "", Action::Doctor(DoctorOptions {})).await?;
        let mut out = Vec::new();
        loop {
            match from_agent.next().await {
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Stdout(data)),
                })) => out.extend_from_slice(&data),
                Some(Ok(SessionFrame {
                    frame: Some(Frame::ExitCode(0)),
                })) => break,
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Error(e)),
                })) => {
                    return Err(Status::new(
                        tonic::Code::from_i32(e.code),
                        format!("node {node}: {}", e.message),
                    ));
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(Status::unavailable(format!(
                        "node {node}'s session ended early"
                    )));
                }
            }
        }
        serde_json::from_slice(&out)
            .map_err(|e| Status::internal(format!("node {node}'s findings: {e}")))
    };
    tokio::time::timeout(ANSWER, asking).await.map_err(|_| {
        Status::deadline_exceeded(format!("node {node} did not answer in {ANSWER:?}"))
    })?
}

pub(crate) async fn support_bundle(
    api: &ApiService,
    req: Request<pb::SupportBundleRequest>,
) -> Result<Response<pb::SupportBundleResponse>, Status> {
    let caller = api.caller(&req)?;
    if let Some(mut leader) = leader_client(api, &req)? {
        return leader
            .support_bundle(forwarded_for(req.into_inner(), &caller)?)
            .await;
    }
    caller.require_all("make a support bundle")?;
    api.consistent().await?;
    let mut objects = serde_json::Map::new();
    let mut namespaces = Vec::new();
    for kind in BUNDLED {
        let list = api
            .store
            .list(kind, None)
            .map_err(|e| Status::internal(e.to_string()))?;
        if kind == Kind::Namespace {
            namespaces = list
                .objects
                .iter()
                .map(|o| o.metadata.name.clone())
                .collect();
        }
        let kept: Vec<Value> = list
            .objects
            .iter()
            .map(|o| masked(serde_json::to_value(o).unwrap_or_default()))
            .collect();
        objects.insert(kind.to_string(), Value::Array(kept));
    }
    let mut events = Vec::new();
    for ns in std::iter::once(String::new()).chain(namespaces) {
        events.extend(api.events.of(&ns, |_| true));
    }
    events.sort_by_key(|e| e.at_ms);
    let alerts: Vec<Value> = crate::alerts::current(api)
        .into_iter()
        .map(|(a, since)| json!({ "alert": a, "since": since }))
        .collect();
    let bundle = json!({
        "made": legion_api::clock::format(legion_api::clock::now()),
        "version": env!("CARGO_PKG_VERSION"),
        "objects": objects,
        "alerts": alerts,
        "events": events,
        "diagnosis": diagnose_nodes(api, "")
            .await?
            .into_iter()
            .map(|d| json!({
                "node": d.node,
                "error": d.error,
                "ready": d.ready,
                "findings": d.findings.iter().map(|f| json!({
                    "id": f.id, "state": f.state, "need": f.need,
                    "detail": f.detail, "remedy": f.remedy,
                })).collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
    });
    let json = serde_json::to_vec_pretty(&bundle).map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(pb::SupportBundleResponse { json }))
}

/// Words in a field's name that say its value may be a secret.
const SECRET_WORDS: [&str; 6] = [
    "password",
    "token",
    "secret",
    "auth",
    "credential",
    "private",
];

/// `v` with what could be secret replaced: every environment variable's
/// value, a Config's data, a field whose name says secret, and the login in
/// a URL (`https://user:token@host`).
fn masked(mut v: Value) -> Value {
    fn walk(v: &mut Value, key: &str) {
        match v {
            Value::Object(map) => {
                for (k, child) in map.iter_mut() {
                    let lower = k.to_ascii_lowercase();
                    if key == "env" && k == "value"
                        || (k == "data" && child.is_object())
                        || SECRET_WORDS.iter().any(|w| lower.contains(w))
                    {
                        *child = Value::String("<masked>".to_owned());
                    } else {
                        walk(child, k);
                    }
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, key);
                }
            }
            Value::String(s) => {
                if let Some(i) = s.find("://")
                    && let Some(at) = s[i + 3..].find('@')
                    && !s[i + 3..i + 3 + at].contains('/')
                {
                    *s = format!("{}<masked>@{}", &s[..i + 3], &s[i + 3 + at + 1..]);
                }
            }
            _ => {}
        }
    }
    walk(&mut v, "");
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_could_be_secret_is_masked() {
        let spec = json!({
            "pod": { "containers": [{
                "name": "web",
                "image": "nginx",
                "env": [{ "name": "DB_PASSWORD", "value": "hunter2" }, { "name": "MODE", "value": "prod" }],
                "args": ["--url", "postgres://app:hunter2@db:5432/app"],
            }]},
            "registryAuth": "Zm9vOmJhcg==",
            "git": "https://github.com/me/app",
            "data": { "app.conf": "listen 80" },
        });
        let m = masked(spec);
        let text = m.to_string();
        assert!(!text.contains("hunter2"), "{text}");
        assert!(!text.contains("Zm9vOmJhcg=="), "{text}");
        assert!(!text.contains("listen 80"), "{text}");
        // What says how it runs stays.
        let c = &m["pod"]["containers"][0];
        assert_eq!(c["env"][0]["name"], "DB_PASSWORD");
        assert_eq!(c["env"][1]["value"], "<masked>");
        assert_eq!(c["args"][1], "postgres://<masked>@db:5432/app");
        assert_eq!(m["git"], "https://github.com/me/app");
        assert_eq!(c["image"], "nginx");
    }

    #[test]
    fn what_a_node_says_is_read_as_the_doctor_writes_it() {
        // As legion_agent::doctor::send writes it.
        let said: Said = serde_json::from_value(json!({
            "ready": false,
            "findings": [
                { "need": "required", "id": "module.pf", "state": "unmet",
                  "detail": "pf is not loaded", "remedy": "kldload pf" },
                { "need": "advised", "id": "racct", "state": "met", "detail": "on" },
            ],
        }))
        .unwrap();
        assert!(!said.ready);
        let f = &said.findings;
        assert_eq!(
            (
                f[0].state.as_str(),
                f[0].need.as_str(),
                f[0].remedy.as_str()
            ),
            ("unmet", "required", "kldload pf")
        );
        assert_eq!((f[1].id.as_str(), f[1].remedy.as_str()), ("racct", ""));
        assert!(said.pf_fix.is_none(), "an agent from before --fix pf says nothing of it");

        // An agent with it says what `--fix pf` would do, carried as it is.
        let hooks = legion_sys::pffix::Hooks { anchor: "legion/*", lines: legion_net::lb::HOOKS };
        let plan = json!({
            "path": "/nonexistent/pf.conf",
            "plan": legion_sys::pffix::plan(None, &[hooks]),
        });
        let said: Said = serde_json::from_value(json!({
            "ready": true, "findings": [], "pfFix": plan,
        }))
        .unwrap();
        let fix = said.pf_fix.unwrap();
        assert_eq!(fix["path"], "/nonexistent/pf.conf");
        assert_eq!(fix["plan"]["action"], "change");
        assert!(fix["plan"]["added"].as_array().unwrap().contains(&json!("anchor \"legion/*\"")));
    }
}
