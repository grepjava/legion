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

fn leader_client<T>(
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

async fn attach(
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
                                reference: i.reference,
                                digest: i.digest,
                                os: i.os,
                                arch: i.arch,
                                layers: i.layers,
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

        if !matches!(op, "pull" | "push" | "remove" | "prune") {
            return Err(Status::invalid_argument(format!(
                "unknown image operation {op:?}: expected list, pull, push, remove or prune"
            )));
        }
        caller.require_all("manage node images")?;
        let auth = secret_auth(api, &caller, &req.namespace, &req.secret)?;
        let nodes = if op == "push" || !req.node.trim().is_empty() {
            ensure_ready_node(api, &req.node)?;
            vec![req.node.clone()]
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
    let _caller = api.caller(&req)?;
    let req = req.into_inner();
    let client = legion_image::Client::new().map_err(internal)?;
    let limit = if req.limit == 0 {
        20
    } else {
        (req.limit as usize).min(50)
    };
    let hits = client
        .search(&req.query, limit)
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

    Ok(Response::new(pb::InspectRegistryImageResponse {
        image: r.to_string(),
        digest,
        platforms,
        tags,
        signed,
    }))
}
