//! Manager-side cluster image operations (`NodeImages`, `ExportImage`,
//! `LoadImage`, `SearchImages`, `InspectRegistryImage`).

use std::time::Duration;

use legion_api::data::DataSpec;
use legion_api::login::docker_config;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::{Kind, ObjectKey};
use legion_image::{Credentials, Reference, cosign};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::load_image_chunk::Part;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{
    self as pb, BackupChunk, ImageNodeAction, ImageNodeResult, InputClosed, LoadImageChunk,
    LoadImageStart, NodeImageItem, NodeImagesRequest, NodeImagesResponse, RegistryPlatform,
    RegistrySearchHit, SessionError, SessionFrame, SessionOffer, SessionOpen,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::api::{ApiService, Route, check_namespace, forwarded_for};
use crate::authz::{Caller, Verb};
use crate::raft_net::{MAX_MESSAGE, connect_lazy};
use crate::session::Attachment;

const START_TIMEOUT: Duration = Duration::from_secs(30);

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

pub(crate) fn leader_client<T>(
    api: &ApiService,
    req: &Request<T>,
) -> Result<Option<ClusterClient<tonic::transport::Channel>>, Status> {
    Ok(match api.route(req)? {
        Route::Local => None,
        Route::Leader(_, addr) => Some(
            ClusterClient::new(connect_lazy(&addr, api.peer_tls.as_ref()))
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        ),
    })
}

pub(crate) async fn attach(
    api: &ApiService,
    node: &str,
    namespace: &str,
    action: Action,
) -> Result<Attachment, Status> {
    let offer = SessionOffer {
        id: legion_pki::random_secret(),
        open: Some(SessionOpen {
            namespace: namespace.to_owned(),
            action: Some(action),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut closing = api.closing.clone();
    tokio::select! {
        a = api.sessions.offer(node, offer) => a,
        _ = closing.wait_for(|c| *c) => Err(Status::unavailable("the manager is shutting down")),
    }
}

fn ready_nodes(api: &ApiService) -> Result<Vec<String>, Status> {
    let list = api.store.list(Kind::Node, None).map_err(internal)?;
    let mut names: Vec<String> = list
        .objects
        .into_iter()
        .filter(|o| {
            serde_json::from_value::<NodeStatus>(o.status.clone())
                .is_ok_and(|s| s.state == NodeState::Ready)
        })
        .map(|o| o.metadata.name)
        .collect();
    names.sort();
    Ok(names)
}

/// The ready nodes labelled builders, whose `legion local` stores builds.
fn ready_builders(api: &ApiService) -> Result<Vec<String>, Status> {
    let ready = ready_nodes(api)?;
    let list = api.store.list(Kind::Node, None).map_err(internal)?;
    Ok(list
        .objects
        .into_iter()
        .filter(|o| {
            o.metadata
                .labels
                .get(legion_api::build::BUILDER_LABEL)
                .is_some_and(|v| v == "true")
                && ready.contains(&o.metadata.name)
        })
        .map(|o| o.metadata.name)
        .collect())
}

fn ensure_ready_node(api: &ApiService, node: &str) -> Result<(), Status> {
    if node.trim().is_empty() {
        return Err(Status::invalid_argument("a node name is required"));
    }
    let ready = api
        .store
        .get(&ObjectKey::new(Kind::Node, "", node))
        .map_err(internal)?
        .and_then(|n| serde_json::from_value::<NodeStatus>(n.status).ok())
        .is_some_and(|s| s.state == NodeState::Ready);
    if !ready {
        return Err(Status::failed_precondition(format!(
            "node {node} is not ready"
        )));
    }
    Ok(())
}

fn secret_auth(
    api: &ApiService,
    caller: &Caller,
    namespace: &str,
    secret: &str,
) -> Result<Vec<u8>, Status> {
    if secret.trim().is_empty() {
        return Ok(Vec::new());
    }
    let ns = if namespace.is_empty() {
        "default"
    } else {
        namespace
    };
    check_namespace(ns)?;
    caller.check(&api.store, Verb::Read, Kind::Secret, ns)?;
    let obj = api
        .store
        .get(&ObjectKey::new(Kind::Secret, ns, secret))
        .map_err(internal)?
        .ok_or_else(|| Status::not_found(format!("no Secret {secret} in namespace {ns}")))?;
    let data: DataSpec = legion_api::parse(&obj.spec).map_err(internal)?;
    data.bytes()
        .map_err(|e| Status::invalid_argument(format!("Secret {secret}: {}", e.message)))
}

async fn run_node_op(
    api: &ApiService,
    node: &str,
    namespace: &str,
    action: ImageNodeAction,
) -> Result<ImageNodeResult, Status> {
    let Attachment { mut from_agent, .. } =
        attach(api, node, namespace, Action::ImageOp(action)).await?;
    let mut got: Option<ImageNodeResult> = None;
    loop {
        match from_agent.next().await {
            Some(Ok(SessionFrame {
                frame: Some(Frame::Images(res)),
            })) => got = Some(res),
            Some(Ok(SessionFrame {
                frame: Some(Frame::ExitCode(0)),
            })) => return Ok(got.unwrap_or_default()),
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
}

pub(crate) async fn node_images(
    api: &ApiService,
    req: Request<NodeImagesRequest>,
) -> Result<Response<NodeImagesResponse>, Status> {
    let is_read = req.get_ref().op.is_empty() || req.get_ref().op == "list";
    let audit = (!is_read).then(|| {
        api.audit_point(
            &req,
            "node-image",
            crate::audit::target("Node", "", &req.get_ref().node),
        )
    });
    let result = async {
        let caller = api.caller(&req)?;
        if let Some(mut leader) = leader_client(api, &req)? {
            return leader
                .node_images(forwarded_for(req.into_inner(), &caller)?)
                .await;
        }
        let req = req.into_inner();
        api.consistent().await?;
        let op = if req.op.is_empty() {
            "list"
        } else {
            req.op.as_str()
        };

        if op == "list" {
            caller.check(&api.store, Verb::Read, Kind::Node, "")?;
            let explicit = !req.node.trim().is_empty();
            let nodes = if explicit {
                ensure_ready_node(api, &req.node)?;
                vec![req.node.clone()]
            } else {
                ready_nodes(api)?
            };
            let mut items = Vec::new();
            let pods = pod_images(api, &caller)?;
            for node in nodes {
                let action = ImageNodeAction {
                    op: "list".to_owned(),
                    ..Default::default()
                };
                match run_node_op(api, &node, "", action).await {
                    Ok(res) => {
                        for i in res.images {
                            items.push(NodeImageItem {
                                node: node.clone(),
                                used_by: used_by(&pods, &i.reference, &i.digest),
                                reference: i.reference,
                                digest: i.digest,
                                os: i.os,
                                arch: i.arch,
                                layers: i.layers,
                                vm: i.vm,
                            });
                        }
                    }
                    Err(e) if explicit => return Err(e),
                    Err(_) => {}
                }
            }
            items.sort_by(|a, b| (&a.node, &a.reference).cmp(&(&b.node, &b.reference)));
            return Ok(Response::new(NodeImagesResponse {
                images: items,
                removed: 0,
                digest: String::new(),
            }));
        }

        if !matches!(op, "pull" | "push" | "remove" | "prune" | "prune-builds") {
            return Err(Status::invalid_argument(format!(
                "unknown image operation {op:?}: expected list, pull, push, remove, prune or prune-builds"
            )));
        }
        caller.require_all("manage node images")?;
        let auth = secret_auth(api, &caller, &req.namespace, &req.secret)?;
        let nodes = if op == "push" || !req.node.trim().is_empty() {
            ensure_ready_node(api, &req.node)?;
            vec![req.node.clone()]
        } else if op == "prune-builds" {
            let builders = ready_builders(api)?;
            if builders.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "no ready builder: none is labelled {}=true",
                    legion_api::build::BUILDER_LABEL
                )));
            }
            builders
        } else {
            let all = ready_nodes(api)?;
            if all.is_empty() {
                return Err(Status::failed_precondition("no ready nodes in the cluster"));
            }
            all
        };

        let mut total_removed = 0u64;
        let mut last_digest = String::new();
        for node in nodes {
            let action = ImageNodeAction {
                op: op.to_owned(),
                image: req.image.clone(),
                target: req.target.clone(),
                auth: auth.clone(),
                plain_http: api.insecure_registries.iter().cloned().collect(),
            };
            let res = run_node_op(api, &node, &req.namespace, action).await?;
            total_removed += res.removed;
            if !res.digest.is_empty() {
                last_digest = res.digest;
            }
        }
        Ok(Response::new(NodeImagesResponse {
            images: Vec::new(),
            removed: total_removed,
            digest: last_digest,
        }))
    }
    .await;
    match audit {
        Some(a) => api.audit_end(a, result),
        None => result,
    }
}

pub(crate) async fn export_image(
    api: &ApiService,
    req: Request<pb::ExportImageRequest>,
) -> Result<Response<ReceiverStream<Result<BackupChunk, Status>>>, Status> {
    let audit = api.audit_point(
        &req,
        "image-export",
        crate::audit::target("Node", "", &req.get_ref().node),
    );
    let result = async {
        let caller = api.caller(&req)?;
        if let Some(mut leader) = leader_client(api, &req)? {
            let mut chunks = leader
                .export_image(forwarded_for(req.into_inner(), &caller)?)
                .await?
                .into_inner();
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                while let Some(chunk) = chunks.next().await {
                    let last = chunk.is_err();
                    if tx.send(chunk).await.is_err() || last {
                        return;
                    }
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let req = req.into_inner();
        api.consistent().await?;
        caller.require_all("export node images")?;
        ensure_ready_node(api, &req.node)?;
        if req.image.trim().is_empty() {
            return Err(Status::invalid_argument("an image reference is required"));
        }
        let Attachment { mut from_agent, .. } = attach(
            api,
            &req.node,
            "",
            Action::ImageOp(ImageNodeAction {
                op: "export".to_owned(),
                image: req.image,
                target: req.tag,
                ..Default::default()
            }),
        )
        .await?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let end = loop {
                let data = match from_agent.next().await {
                    Some(Ok(SessionFrame {
                        frame: Some(Frame::Stdout(data)),
                    })) => data,
                    Some(Ok(SessionFrame {
                        frame: Some(Frame::ExitCode(0)),
                    })) => return,
                    Some(Ok(SessionFrame {
                        frame: Some(Frame::Error(e)),
                    })) => {
                        break Status::new(
                            tonic::Code::from_i32(e.code),
                            format!("the node failed: {}", e.message),
                        );
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => break e,
                    None => break Status::unavailable("the node's stream ended early"),
                };
                if tx.send(Ok(BackupChunk { data })).await.is_err() {
                    return;
                }
            };
            let _ = tx.send(Err(end)).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
    .await;
    api.audit_end(audit, result)
}

pub(crate) async fn load_image(
    api: &ApiService,
    req: Request<Streaming<LoadImageChunk>>,
) -> Result<Response<NodeImagesResponse>, Status> {
    let audit = api.audit_point(&req, "image-load", crate::audit::target("Node", "", ""));
    let result = async {
        let caller = api.caller(&req)?;
        let leader = leader_client(api, &req)?;
        let mut from_user = req.into_inner();
        let start = match tokio::time::timeout(START_TIMEOUT, from_user.next()).await {
            Ok(Some(Ok(LoadImageChunk {
                part: Some(Part::Start(s)),
            }))) => s,
            Ok(Some(Err(e))) => return Err(e),
            _ => {
                return Err(Status::invalid_argument(
                    "an image load starts with `start`",
                ));
            }
        };
        if let Some(mut leader) = leader {
            let first = LoadImageChunk {
                part: Some(Part::Start(start)),
            };
            let rest = from_user.map_while(Result::ok);
            let all = tokio_stream::once(first).chain(rest);
            return leader.load_image(forwarded_for(all, &caller)?).await;
        }
        let LoadImageStart { node, reference } = start;
        api.consistent().await?;
        caller.require_all("load node images")?;
        ensure_ready_node(api, &node)?;

        let Attachment {
            mut from_agent,
            to_agent,
        } = attach(
            api,
            &node,
            "",
            Action::ImageOp(ImageNodeAction {
                op: "load".to_owned(),
                image: reference,
                ..Default::default()
            }),
        )
        .await?;

        let send = |f| SessionFrame { frame: Some(f) };
        let fed = async {
            while let Some(chunk) = from_user.next().await {
                match chunk {
                    Ok(LoadImageChunk {
                        part: Some(Part::Data(data)),
                    }) => {
                        if to_agent.send(Ok(send(Frame::Input(data)))).await.is_err() {
                            return Err(Status::unavailable("the node stopped taking the archive"));
                        }
                    }
                    Ok(_) => return Err(Status::invalid_argument("only data follows `start`")),
                    Err(e) => return Err(e),
                }
            }
            let _ = to_agent
                .send(Ok(send(Frame::InputClosed(InputClosed {}))))
                .await;
            Ok(())
        };
        if let Err(e) = fed.await {
            let _ = to_agent
                .send(Ok(send(Frame::Error(SessionError {
                    code: e.code() as i32,
                    message: e.message().to_owned(),
                }))))
                .await;
            return Err(e);
        }

        let mut got: Option<ImageNodeResult> = None;
        loop {
            match from_agent.next().await {
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Images(res)),
                })) => got = Some(res),
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
                        "node {node}'s stream ended early"
                    )));
                }
            }
        }
        let res = got.unwrap_or_default();
        let items = res
            .images
            .into_iter()
            .map(|i| NodeImageItem {
                node: node.clone(),
                reference: i.reference,
                digest: i.digest,
                os: i.os,
                arch: i.arch,
                layers: i.layers,
                vm: i.vm,
                used_by: Vec::new(),
            })
            .collect();
        Ok(Response::new(NodeImagesResponse {
            images: items,
            removed: 0,
            digest: res.digest,
        }))
    }
    .await;
    api.audit_end(audit, result)
}

pub(crate) async fn search_images(
    api: &ApiService,
    req: Request<pb::SearchImagesRequest>,
) -> Result<Response<pb::SearchImagesResponse>, Status> {
    let caller = api.caller(&req)?;
    let req = req.into_inner();
    let auth = secret_auth(api, &caller, &req.namespace, &req.secret)?;
    let ghcr_token = if auth.is_empty() {
        None
    } else {
        let logins = docker_config(&auth)
            .map_err(|e| Status::invalid_argument(format!("Secret {}: {e}", req.secret)))?;
        Credentials::for_registry(&logins, "ghcr.io").map(|c| c.password)
    };
    let client = legion_image::Client::new().map_err(internal)?;
    let limit = if req.limit == 0 {
        20
    } else {
        (req.limit as usize).min(50)
    };
    let hits = client
        .search_with_ghcr(&req.query, limit, ghcr_token.as_deref())
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let results = hits
        .into_iter()
        .map(|h| RegistrySearchHit {
            name: h.name,
            description: h.description,
            stars: h.stars,
            official: h.official,
            source: h.source,
            suggested_tag: h.suggested_tag,
        })
        .collect();
    Ok(Response::new(pb::SearchImagesResponse { results }))
}

fn registry_client(
    api: &ApiService,
    reference: &Reference,
) -> Result<legion_image::Client, Status> {
    let mut client = legion_image::Client::new().map_err(internal)?;
    for r in api.insecure_registries.iter() {
        client = client.plain_http(r);
    }
    let host = reference.host();
    if api.registry_loopback && (host.starts_with("127.0.0.1:") || host.starts_with("localhost:")) {
        client = client.plain_http(&reference.registry);
    }
    Ok(client)
}

pub(crate) async fn inspect_registry_image(
    api: &ApiService,
    req: Request<pb::InspectRegistryImageRequest>,
) -> Result<Response<pb::InspectRegistryImageResponse>, Status> {
    let caller = api.caller(&req)?;
    if let Some(mut leader) = leader_client(api, &req)? {
        return leader
            .inspect_registry_image(forwarded_for(req.into_inner(), &caller)?)
            .await;
    }
    let req = req.into_inner();
    if !req.node.trim().is_empty() {
        return inspect_node_image(api, &caller, &req)
            .await
            .map(Response::new);
    }
    let r = Reference::parse(&req.image)
        .map_err(|e| Status::invalid_argument(format!("image reference: {e}")))?;
    if r.is_local() {
        return Err(Status::invalid_argument(
            "localhost references live only in a node's image store, not a remote registry",
        ));
    }
    api.consistent().await?;
    let auth = secret_auth(api, &caller, &req.namespace, &req.secret)?;
    let logins = if auth.is_empty() {
        Vec::new()
    } else {
        docker_config(&auth)
            .map_err(|e| Status::invalid_argument(format!("Secret {}: {e}", req.secret)))?
    };
    let creds = Credentials::for_registry(&logins, &r.registry);
    let client = registry_client(api, &r)?;

    let (digest, _) = client
        .document(&r, r.target(), creds.as_ref())
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let descriptors = client
        .describe(&r, creds.as_ref())
        .await
        .unwrap_or_default();
    let platforms: Vec<RegistryPlatform> = descriptors
        .into_iter()
        .map(|d| {
            let (os, arch) = d
                .platform
                .map_or((String::new(), String::new()), |p| (p.os, p.architecture));
            RegistryPlatform {
                os,
                arch,
                digest: d.digest,
            }
        })
        .collect();
    let mut tags = client
        .list_tags(&r, creds.as_ref())
        .await
        .unwrap_or_default();
    if tags.len() > 60 {
        tags.truncate(60);
    }

    let repo_name = format!("{}/{}", r.registry, r.repository);
    let all_policies = crate::images::policies(&api.store).unwrap_or_default();
    let covering: Vec<_> = all_policies
        .iter()
        .filter(|p| {
            p.images
                .iter()
                .any(|pat| legion_api::image::matches(pat, &repo_name))
        })
        .collect();
    let keys: Vec<cosign::PublicKey> = covering.iter().flat_map(|p| p.keys.clone()).collect();
    let attestations =
        match legion_image::attest::fetch(&client, &r, &digest, &keys, creds.as_ref()).await {
            Ok(found) => found.into_iter().map(attestation).collect(),
            Err(e) => {
                tracing::debug!(image = %r, "attestations: {e}");
                Vec::new()
            }
        };
    let signed = if covering.is_empty() {
        false
    } else {
        let mut ok = true;
        for p in covering {
            if cosign::verify(&client, &r, &digest, &p.keys, creds.as_ref())
                .await
                .is_err()
            {
                ok = false;
                break;
            }
        }
        ok
    };

    // How it was built, of the platform a node would run.
    let mut out = match client.config(&r, creds.as_ref(), RUNS_ON).await {
        Ok((_, manifest, config)) => {
            let sizes: Vec<u64> = manifest.layers.iter().map(|l| l.size).collect();
            details(&config, &sizes)
        }
        Err(_) => pb::InspectRegistryImageResponse::default(),
    };
    let pods = pod_images(api, &caller)?;
    out.used_by = used_by(&pods, &r.to_string(), &digest);
    out.image = r.to_string();
    out.digest = digest;
    out.platforms = platforms;
    out.tags = tags;
    out.signed = signed;
    out.attestations = attestations;
    Ok(Response::new(out))
}

/// How much of a predicate that is no SBOM is sent.
const PREDICATE_SHOWN: usize = 64 * 1024;

/// An attestation as the API sends it: an SBOM's packages, or the
/// predicate.
fn attestation(f: legion_image::attest::Found) -> pb::ImageAttestation {
    use legion_image::attest;
    let sbom = attest::is_sbom(&f.predicate_type);
    let packages = attest::sbom_packages(&f.predicate_type, &f.predicate)
        .into_iter()
        .map(|(name, version)| pb::ImagePackage { name, version })
        .collect();
    let predicate_json = if sbom || f.predicate.is_null() {
        String::new()
    } else {
        let mut json = serde_json::to_string_pretty(&f.predicate).unwrap_or_default();
        if json.len() > PREDICATE_SHOWN {
            let mut end = PREDICATE_SHOWN;
            while !json.is_char_boundary(end) {
                end -= 1;
            }
            json.truncate(end);
        }
        json
    };
    pb::ImageAttestation {
        predicate_type: f.predicate_type,
        verified_key: f.verified.unwrap_or_default(),
        why: f.why.unwrap_or_default(),
        packages,
        predicate_json,
    }
}

/// A node's cached image audited (`ScanImage`): its packages, those with
/// known vulnerabilities, and what uses it.
pub(crate) async fn scan_image(
    api: &ApiService,
    req: Request<pb::ScanImageRequest>,
) -> Result<Response<pb::ScanImageResponse>, Status> {
    let caller = api.caller(&req)?;
    if let Some(mut leader) = leader_client(api, &req)? {
        return leader
            .scan_image(forwarded_for(req.into_inner(), &caller)?)
            .await;
    }
    caller.check(&api.store, Verb::Read, Kind::Node, "")?;
    let req = req.into_inner();
    if req.image.trim().is_empty() {
        return Err(Status::invalid_argument("scan needs an image reference"));
    }
    api.consistent().await?;
    ensure_ready_node(api, &req.node)?;
    let action = ImageNodeAction {
        op: "scan".to_owned(),
        image: req.image.clone(),
        ..Default::default()
    };
    let res = run_node_op(api, &req.node, "", action).await?;
    let packages = res
        .packages
        .iter()
        .filter_map(|p| p.split_once('\t'))
        .map(|(name, version)| pb::ImagePackage {
            name: name.to_owned(),
            version: version.to_owned(),
        })
        .collect();
    let vulnerable = audited(&res.audit)
        .map_err(|e| internal(format!("node {}: pkg audit's answer: {e}", req.node)))?;
    let pods = pod_images(api, &caller)?;
    Ok(Response::new(pb::ScanImageResponse {
        used_by: used_by(&pods, &req.image, ""),
        image: req.image,
        node: req.node,
        packages,
        vulnerable,
    }))
}

/// The vulnerable packages in `pkg audit --raw=json-compact`'s answer;
/// none for an empty one.
fn audited(json: &str) -> Result<Vec<pb::VulnerablePackage>, serde_json::Error> {
    #[derive(serde::Deserialize)]
    struct Audit {
        #[serde(default)]
        packages: std::collections::BTreeMap<String, Package>,
    }
    #[derive(serde::Deserialize)]
    struct Package {
        #[serde(default)]
        version: String,
        #[serde(default)]
        issues: Vec<Issue>,
    }
    #[derive(serde::Deserialize)]
    struct Issue {
        #[serde(default)]
        description: String,
        #[serde(default)]
        cve: Vec<String>,
        #[serde(default)]
        url: String,
        #[serde(default, rename = "Affected versions")]
        affected: Vec<String>,
    }
    if json.trim().is_empty() {
        return Ok(Vec::new());
    }
    let audit: Audit = serde_json::from_str(json)?;
    Ok(audit
        .packages
        .into_iter()
        .filter(|(_, p)| !p.issues.is_empty())
        .map(|(name, p)| pb::VulnerablePackage {
            name,
            version: p.version,
            issues: p
                .issues
                .into_iter()
                .map(|i| pb::Vulnerability {
                    description: i.description,
                    cves: i.cve,
                    url: i.url,
                    affected: i.affected,
                })
                .collect(),
        })
        .collect())
}

/// The systems a node runs images of, in the order it would pick one.
const RUNS_ON: &[&str] = &["freebsd", "linux"];

/// A node's cached copy of an image, read from its store.
async fn inspect_node_image(
    api: &ApiService,
    caller: &Caller,
    req: &pb::InspectRegistryImageRequest,
) -> Result<pb::InspectRegistryImageResponse, Status> {
    caller.check(&api.store, Verb::Read, Kind::Node, "")?;
    if req.image.trim().is_empty() {
        return Err(Status::invalid_argument("inspect needs an image reference"));
    }
    api.consistent().await?;
    ensure_ready_node(api, &req.node)?;
    let action = ImageNodeAction {
        op: "inspect".to_owned(),
        image: req.image.clone(),
        ..Default::default()
    };
    let res = run_node_op(api, &req.node, "", action).await?;
    let config: legion_image::ImageConfig = serde_json::from_str(&res.config)
        .map_err(|e| internal(format!("node {}: {}'s config: {e}", req.node, req.image)))?;
    let mut out = details(&config, &res.layer_sizes);
    let pods = pod_images(api, caller)?;
    out.used_by = used_by(&pods, &req.image, "");
    out.image = req.image.clone();
    Ok(out)
}

/// What an image's config says of how it was built and how it runs.
/// `layer_sizes` are its layers', bottom first, matched in order to the
/// history's steps that added one.
fn details(
    config: &legion_image::ImageConfig,
    layer_sizes: &[u64],
) -> pb::InspectRegistryImageResponse {
    let mut sizes = layer_sizes.iter();
    let history = config
        .history
        .iter()
        .map(|h| pb::ImageStep {
            created: h.created.clone(),
            created_by: h.created_by.clone(),
            empty_layer: h.empty_layer,
            size: if h.empty_layer {
                0
            } else {
                sizes.next().copied().unwrap_or(0)
            },
        })
        .collect();
    let c = &config.config;
    pb::InspectRegistryImageResponse {
        os: config.os.clone(),
        arch: config.architecture.clone(),
        created: config.created.clone(),
        history,
        run: Some(pb::ImageRun {
            entrypoint: c.entrypoint.clone(),
            cmd: c.cmd.clone(),
            env: c.env.clone(),
            user: c.user.clone(),
            working_dir: c.working_dir.clone(),
            exposed_ports: c.exposed_ports.keys().cloned().collect(),
            labels: c.labels.clone().into_iter().collect(),
        }),
        size: layer_sizes.iter().sum(),
        ..Default::default()
    }
}

/// Services, Jobs and cron jobs as `Kind namespace/name`, each with the
/// references its pod names.
type PodImages = Vec<(String, Vec<String>)>;

/// Each service's, Job's and cron job's images, of those `caller` may read.
fn pod_images(api: &ApiService, caller: &Caller) -> Result<PodImages, Status> {
    let mut out = Vec::new();
    for kind in [Kind::Service, Kind::Job, Kind::CronJob] {
        let objects = api.store.list(kind, None).map_err(internal)?.objects;
        for o in objects {
            let ns = &o.metadata.namespace;
            if !caller.may(&api.store, Verb::Read, kind, ns) {
                continue;
            }
            let pod = if kind == Kind::CronJob {
                &o.spec["job"]["pod"]
            } else {
                &o.spec["pod"]
            };
            out.push((
                format!("{kind} {ns}/{}", o.metadata.name),
                written_and_resolved(pod, &o.status["images"]["resolved"]),
            ));
        }
    }
    Ok(out)
}

/// A pod's images as written, each followed by the same image by digest
/// where the leader resolved it (`status.images.resolved`): a node caches
/// what it pulled, the digest, so a tag alone would match nothing there.
fn written_and_resolved(pod: &serde_json::Value, resolved: &serde_json::Value) -> Vec<String> {
    ["initContainers", "containers"]
        .iter()
        .filter_map(|k| pod[*k].as_array())
        .flatten()
        .filter_map(|c| c["image"].as_str())
        .flat_map(|written| {
            let by_digest = resolved[written].as_str().map(str::to_owned);
            std::iter::once(written.to_owned()).chain(by_digest)
        })
        .collect()
}

/// Those of `pods` naming `image`, cached under that reference with the
/// manifest `digest` (empty when not known).
fn used_by(pods: &[(String, Vec<String>)], image: &str, digest: &str) -> Vec<String> {
    let Ok(image) = Reference::parse(image) else {
        return Vec::new();
    };
    pods.iter()
        .filter(|(_, refs)| refs.iter().any(|r| same_image(r, &image, digest)))
        .map(|(who, _)| who.clone())
        .collect()
}

/// Whether a pod's reference `named` runs `image` (whose manifest is
/// `digest`, empty when not known): the same repository, and the digest it
/// is pinned to or else the same tag.
fn same_image(named: &str, image: &Reference, digest: &str) -> bool {
    let Ok(named) = Reference::parse(named) else {
        return false;
    };
    if (&named.registry, &named.repository) != (&image.registry, &image.repository) {
        return false;
    }
    match &named.digest {
        Some(d) => image.digest.as_ref() == Some(d) || digest == d,
        None => {
            let tag = |r: &Reference| r.tag.clone().unwrap_or_else(|| "latest".to_owned());
            image.tag.is_some() && tag(&named) == tag(image)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PINNED: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000001";

    fn pods() -> PodImages {
        let one = |who: &str, image: String| (who.to_owned(), vec![image]);
        vec![
            one("Service a/web", "docker.io/library/nginx:1.27".into()),
            one("Service a/latest", "nginx".into()),
            one(
                "Job b/pinned",
                format!("docker.io/library/nginx:old@{PINNED}"),
            ),
            one("CronJob b/other", "ghcr.io/me/nginx:1.27".into()),
        ]
    }

    #[test]
    fn an_image_is_used_by_pods_naming_its_tag_or_pinned_to_its_digest() {
        let pods = pods();
        assert_eq!(used_by(&pods, "nginx:1.27", ""), ["Service a/web"]);
        assert_eq!(
            used_by(&pods, "docker.io/library/nginx:latest", ""),
            ["Service a/latest"]
        );
        // Pinned: by the digest, whatever tag it was cached under.
        assert_eq!(
            used_by(&pods, "docker.io/library/nginx:1.27", PINNED),
            ["Service a/web", "Job b/pinned"]
        );
        assert_eq!(
            used_by(&pods, &format!("docker.io/library/nginx@{PINNED}"), ""),
            ["Job b/pinned"]
        );
        assert!(used_by(&pods, "redis:7", "").is_empty());
    }

    #[test]
    fn a_pod_naming_a_tag_uses_the_digest_it_was_resolved_to() {
        // As in the lab: hello runs a tag; its node caches the digest the
        // leader resolved it to, by that digest alone.
        let tag = "docker.io/freebsd/freebsd-notoolchain:15.1";
        let pulled = format!("docker.io/freebsd/freebsd-notoolchain@{PINNED}");
        let pod = serde_json::json!({ "containers": [{ "name": "c", "image": tag }] });
        let resolved = serde_json::json!({ tag: pulled });
        let refs = written_and_resolved(&pod, &resolved);
        assert_eq!(refs, [tag.to_owned(), pulled.clone()]);
        let pods = vec![("Service team-a/hello".to_owned(), refs)];
        assert_eq!(used_by(&pods, &pulled, ""), ["Service team-a/hello"]);
        // Not yet resolved: the tag alone, which the digest doesn't match.
        let unresolved = written_and_resolved(&pod, &serde_json::Value::Null);
        assert_eq!(unresolved, [tag]);
        let pods = vec![("Service team-a/hello".to_owned(), unresolved)];
        assert!(used_by(&pods, &pulled, "").is_empty());
    }

    #[test]
    fn pkg_audits_answer_is_read_package_by_package() {
        // As pkg 2.8 writes it (`pkg audit --raw=json-compact`).
        let json = r#"{"pkg_count":2,"packages":{
            "nginx":{"version":"1.27.0","issue_count":1,"issues":[
                {"Affected versions":["< 1.30.1,3"],"description":"www/nginx -- Remote Code Execution/DoS",
                 "cve":["CVE-2026-42926"],"url":"https://vuxml.FreeBSD.org/freebsd/3414ac89.html"}]},
            "pcre2":{"version":"10.45","issue_count":0,"issues":[]}}}"#;
        let v = audited(json).unwrap();
        assert_eq!(v.len(), 1, "{v:?}");
        assert_eq!(
            (v[0].name.as_str(), v[0].version.as_str()),
            ("nginx", "1.27.0")
        );
        let i = &v[0].issues[0];
        assert_eq!(i.cves, ["CVE-2026-42926"]);
        assert_eq!(i.affected, ["< 1.30.1,3"]);
        assert!(i.url.starts_with("https://vuxml.FreeBSD.org/"));
        assert!(audited("").unwrap().is_empty());
        assert!(audited(r#"{"pkg_count":0}"#).unwrap().is_empty());
    }

    #[test]
    fn the_steps_get_the_layers_they_added() {
        let config: legion_image::ImageConfig = serde_json::from_value(serde_json::json!({
            "os": "freebsd", "architecture": "amd64", "created": "2026-11-15T00:00:00Z",
            "config": { "Entrypoint": ["/bin/sh"], "User": "www",
                        "ExposedPorts": { "80/tcp": {} }, "Labels": { "a": "b" } },
            "history": [
                { "created_by": "FROM base" },
                { "created_by": "ENV A=1", "empty_layer": true },
                { "created_by": "RUN pkg install nginx" }
            ]
        }))
        .unwrap();
        let d = details(&config, &[100, 20]);
        let steps: Vec<_> = d
            .history
            .iter()
            .map(|s| (s.created_by.as_str(), s.size))
            .collect();
        assert_eq!(
            steps,
            [
                ("FROM base", 100),
                ("ENV A=1", 0),
                ("RUN pkg install nginx", 20)
            ]
        );
        assert_eq!(d.size, 120);
        let run = d.run.unwrap();
        assert_eq!(run.user, "www");
        assert_eq!(run.exposed_ports, ["80/tcp"]);
        assert_eq!(run.labels.get("a").map(String::as_str), Some("b"));
        assert_eq!((d.os.as_str(), d.arch.as_str()), ("freebsd", "amd64"));
    }
}
