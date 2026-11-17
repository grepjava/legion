//! The connections a namespace's tasks open or answer (`Connections`), as
//! the nodes' pf keeps them: each ready node's agent is asked at once for
//! the connections it has state for with a pod at an end
//! (`legion_agent::connections`), and the leader names the ends from the
//! tasks and nodes it knows.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use legion_api::Kind;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{TaskSpec, TaskState, TaskStatus};
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{self as pb, ConnectionsOptions, SessionFrame};
use serde::Deserialize;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::api::{ApiService, check_namespace, forwarded_for};
use crate::authz::Verb;
use crate::image_ops::{attach, leader_client};
use crate::session::Attachment;

/// How long a node has to say.
const ANSWER: Duration = Duration::from_secs(15);

/// One connection as a node's agent says it (`legion_agent::connections::Seen`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Seen {
    pub proto: String,
    pub client: SocketAddr,
    #[serde(default)]
    pub translated: Option<SocketAddr>,
    pub server: SocketAddr,
    #[serde(default)]
    pub addressed: Option<SocketAddr>,
}

pub(crate) async fn connections(
    api: &ApiService,
    req: Request<pb::ConnectionsRequest>,
) -> Result<Response<pb::ConnectionsResponse>, Status> {
    let caller = api.caller(&req)?;
    let namespace = req.get_ref().namespace.clone();
    if namespace.is_empty() {
        return Err(Status::invalid_argument("which namespace?"));
    }
    check_namespace(&namespace)?;
    caller.check(&api.store, Verb::Read, Kind::Service, &namespace)?;
    if let Some(mut leader) = leader_client(api, &req)? {
        return leader
            .connections(forwarded_for(req.into_inner(), &caller)?)
            .await;
    }
    api.consistent().await?;
    let list = |kind| {
        api.store
            .list(kind, None)
            .map(|l| l.objects)
            .map_err(|e| Status::internal(e.to_string()))
    };
    let (nodes, tasks) = (list(Kind::Node)?, list(Kind::Task)?);
    let mut statuses: Vec<(String, NodeStatus)> = nodes
        .into_iter()
        .map(|o| {
            let s = serde_json::from_value(o.status).unwrap_or_default();
            (o.metadata.name, s)
        })
        .collect();
    statuses.sort_by(|a, b| a.0.cmp(&b.0));
    let asked = statuses
        .iter()
        .filter(|(_, s)| matches!(s.state, NodeState::Ready | NodeState::Suspect))
        .map(|(node, _)| async move { (node.clone(), ask(api, node).await) });
    let mut seen = Vec::new();
    let mut unanswered = Vec::new();
    for (node, said) in futures::future::join_all(asked).await {
        match said {
            Ok(s) => seen.extend(s),
            Err(error) => unanswered.push(pb::NodeError { node, error }),
        }
    }
    let directory = Directory::new(
        tasks.into_iter().filter_map(|t| {
            let spec = legion_api::parse::<TaskSpec>(&t.spec).ok()?;
            let status: TaskStatus = serde_json::from_value(t.status).unwrap_or_default();
            let ip = status.ip.as_deref()?.parse().ok()?;
            Some(Pod {
                ip,
                namespace: t.metadata.namespace,
                name: spec.service,
                job: spec.job,
                running: status.state == TaskState::Running,
            })
        }),
        statuses.iter().map(|(n, s)| (n.as_str(), s)),
    );
    Ok(Response::new(pb::ConnectionsResponse {
        edges: edges(&directory, &namespace, seen),
        unanswered,
    }))
}

/// What `node`'s agent says.
async fn ask(api: &ApiService, node: &str) -> Result<Vec<Seen>, String> {
    let asking = async {
        let Attachment { mut from_agent, .. } =
            attach(api, node, "", Action::Connections(ConnectionsOptions {}))
                .await
                .map_err(|e| e.message().to_owned())?;
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
                })) if e.message.contains("no action") => {
                    return Err(
                        "its agent is from before connections could be asked: upgrade it"
                            .to_owned(),
                    );
                }
                Some(Ok(SessionFrame {
                    frame: Some(Frame::Error(e)),
                })) => return Err(e.message),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e.message().to_owned()),
                None => return Err("its session ended early".to_owned()),
            }
        }
        serde_json::from_slice(&out).map_err(|e| format!("its answer: {e}"))
    };
    tokio::time::timeout(ANSWER, asking)
        .await
        .map_err(|_| format!("no answer in {ANSWER:?}"))?
}

/// A task with an address.
pub(crate) struct Pod {
    pub ip: Ipv4Addr,
    pub namespace: String,
    /// Its service's or Job's.
    pub name: String,
    pub job: bool,
    pub running: bool,
}

/// Who has each address.
pub(crate) struct Directory {
    pods: BTreeMap<Ipv4Addr, Pod>,
    nodes: BTreeMap<IpAddr, String>,
}

impl Directory {
    /// A running task's address over any other's, an address being taken
    /// again once its task ends.
    pub fn new<'a>(
        pods: impl IntoIterator<Item = Pod>,
        nodes: impl IntoIterator<Item = (&'a str, &'a NodeStatus)>,
    ) -> Self {
        let mut by_ip: BTreeMap<Ipv4Addr, Pod> = BTreeMap::new();
        for p in pods {
            if p.running || !by_ip.contains_key(&p.ip) {
                by_ip.insert(p.ip, p);
            }
        }
        let mut addrs = BTreeMap::new();
        for (name, s) in nodes {
            if let Ok(ip) = s.addr.parse::<IpAddr>() {
                addrs.insert(ip, name.to_owned());
            }
            // The node's own addresses on its bridge and the mesh: probes,
            // and what it forwards.
            if let Some(subnet) = s
                .pod_cidr
                .as_deref()
                .and_then(|c| c.parse::<legion_sys::Ipv4Net>().ok())
            {
                let gateway = Ipv4Addr::from(u32::from(subnet.network()) + 1);
                addrs.insert(gateway.into(), name.to_owned());
                if let Some(u) = legion_net::mesh::underlay_addr(subnet) {
                    addrs.insert(u.into(), name.to_owned());
                }
            }
        }
        Directory {
            pods: by_ip,
            nodes: addrs,
        }
    }

    /// `ip` as an end seen from `namespace`; `None` for an address of
    /// Legion's no one has, which can't be named.
    fn end(&self, ip: IpAddr, namespace: &str) -> Option<pb::ConnectionEnd> {
        let end = |kind: &str, name: &str| pb::ConnectionEnd {
            kind: kind.to_owned(),
            name: name.to_owned(),
        };
        if let IpAddr::V4(v4) = ip
            && let Some(p) = self.pods.get(&v4)
        {
            return Some(if p.namespace != namespace {
                end("namespace", "")
            } else if p.job {
                end("job", &p.name)
            } else {
                end("service", &p.name)
            });
        }
        if let Some(n) = self.nodes.get(&ip) {
            return Some(end("node", n));
        }
        match ip {
            IpAddr::V4(v4)
                if legion_net::CLUSTER_POD_CIDR.contains(v4)
                    || legion_net::SERVICE_CIDR.contains(v4)
                    || legion_net::mesh::UNDERLAY.contains(v4) =>
            {
                None
            }
            _ => Some(end("outside", "")),
        }
    }
}

/// The edges of `namespace`'s tasks among what the nodes saw: each
/// connection once, though more than one node may have seen it, and one
/// NATed on its way to another node counted as from its first client.
pub(crate) fn edges(
    directory: &Directory,
    namespace: &str,
    seen: Vec<Seen>,
) -> Vec<pb::ConnectionEdge> {
    // A node that NATed a connection on to another knows both clients.
    let first: HashMap<(String, SocketAddr, SocketAddr), SocketAddr> = seen
        .iter()
        .filter_map(|s| Some(((s.proto.clone(), s.translated?, s.server), s.client)))
        .collect();
    let mut conns: BTreeMap<(String, SocketAddr, SocketAddr), Option<SocketAddr>> = BTreeMap::new();
    for s in seen {
        let client = first
            .get(&(s.proto.clone(), s.client, s.server))
            .copied()
            .unwrap_or(s.client);
        let addressed = conns.entry((s.proto, client, s.server)).or_default();
        *addressed = addressed.or(s.addressed);
    }
    type Key = (String, String, String, String, String, u16, &'static str);
    let mut counted: BTreeMap<Key, (u32, BTreeSet<IpAddr>)> = BTreeMap::new();
    for ((proto, client, server), addressed) in conns {
        let (Some(from), Some(to)) = (
            directory.end(client.ip(), namespace),
            directory.end(server.ip(), namespace),
        ) else {
            continue;
        };
        let ours = |e: &pb::ConnectionEnd| e.kind == "service" || e.kind == "job";
        if !ours(&from) && !ours(&to) {
            continue;
        }
        let via = match addressed {
            None => "",
            Some(a) if matches!(a.ip(), IpAddr::V4(v4) if legion_net::SERVICE_CIDR.contains(v4)) => {
                "service"
            }
            Some(_) => "published",
        };
        let c = counted
            .entry((
                from.kind,
                from.name,
                to.kind,
                to.name,
                proto,
                server.port(),
                via,
            ))
            .or_default();
        c.0 += 1;
        c.1.insert(client.ip());
    }
    counted
        .into_iter()
        .map(
            |((from_kind, from, to_kind, to, protocol, port, via), (connections, clients))| {
                pb::ConnectionEdge {
                    from: Some(pb::ConnectionEnd {
                        kind: from_kind,
                        name: from,
                    }),
                    to: Some(pb::ConnectionEnd {
                        kind: to_kind,
                        name: to,
                    }),
                    protocol,
                    port: port.into(),
                    via: via.to_owned(),
                    connections,
                    clients: u32::try_from(clients.len()).unwrap_or(u32::MAX),
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn seen(client: &str, server: &str) -> Seen {
        Seen {
            proto: "tcp".into(),
            client: a(client),
            translated: None,
            server: a(server),
            addressed: None,
        }
    }

    fn pod(ip: &str, namespace: &str, name: &str) -> Pod {
        Pod {
            ip: ip.parse().unwrap(),
            namespace: namespace.into(),
            name: name.into(),
            job: false,
            running: true,
        }
    }

    fn node(addr: &str, cidr: &str) -> NodeStatus {
        NodeStatus {
            addr: addr.into(),
            pod_cidr: Some(cidr.into()),
            ..Default::default()
        }
    }

    /// As `legion connections` prints them.
    fn lines(edges: &[pb::ConnectionEdge]) -> Vec<String> {
        let end = |e: &Option<pb::ConnectionEnd>| {
            let e = e.as_ref().unwrap();
            format!("{}:{}", e.kind, e.name)
        };
        edges
            .iter()
            .map(|e| {
                format!(
                    "{} -> {} {}/{} via {:?} x{} from {}",
                    end(&e.from),
                    end(&e.to),
                    e.port,
                    e.protocol,
                    e.via,
                    e.connections,
                    e.clients
                )
            })
            .collect()
    }

    #[test]
    fn connections_are_named_and_counted_once_whichever_nodes_saw_them() {
        let (n1, n2) = (
            node("192.168.1.1", "10.200.0.0/24"),
            node("192.168.1.2", "10.200.1.0/24"),
        );
        let mut job = pod("10.200.1.9", "shop", "migrate");
        job.job = true;
        let mut ended = pod("10.200.0.5", "shop", "old-web");
        ended.running = false;
        let directory = Directory::new(
            [
                ended,
                pod("10.200.0.3", "shop", "nginx"),
                pod("10.200.0.5", "shop", "web"),
                pod("10.200.1.2", "shop", "db"),
                pod("10.200.1.7", "other", "spy"),
                job,
            ],
            [("n1", &n1), ("n2", &n2)],
        );
        let from_outside = |client: &str| Seen {
            addressed: Some(a("192.168.1.1:8080")),
            ..seen(client, "10.200.0.3:80")
        };
        let said = vec![
            // Two clients outside to nginx's published port on n1.
            from_outside("203.0.113.5:4000"),
            from_outside("203.0.113.5:4001"),
            from_outside("203.0.113.6:4000"),
            // nginx to web through the service's address, the same
            // connection told twice by n1.
            Seen {
                addressed: Some(a("10.201.0.3:3000")),
                ..seen("10.200.0.3:30236", "10.200.0.5:3000")
            },
            Seen {
                translated: Some(a("10.200.0.1:56952")),
                ..seen("10.200.0.3:30236", "10.200.0.5:3000")
            },
            // web to db on n2, seen by both nodes.
            seen("10.200.0.5:45986", "10.200.1.2:5432"),
            seen("10.200.0.5:45986", "10.200.1.2:5432"),
            // From outside to a port published on n1 that sent it on to
            // db on n2: n1 NATed it onto the mesh, so n2 sees n1's
            // underlay address as the client.
            Seen {
                translated: Some(a("100.64.0.1:51000")),
                addressed: Some(a("192.168.1.1:5432")),
                ..seen("203.0.113.7:5000", "10.200.1.2:5432")
            },
            seen("100.64.0.1:51000", "10.200.1.2:5432"),
            // n1's probes of web.
            seen("10.200.0.1:40000", "10.200.0.5:3000"),
            // Another namespace's task to db, a Job's to db, web out.
            seen("10.200.1.7:6000", "10.200.1.2:5432"),
            seen("10.200.1.9:6000", "10.200.1.2:5432"),
            Seen {
                proto: "udp".into(),
                ..seen("10.200.0.5:5353", "8.8.8.8:53")
            },
            // An address no task has: not named, not counted.
            seen("10.200.0.77:7000", "10.200.0.5:3000"),
            // Others' business: neither end is the namespace's.
            seen("10.200.1.7:6001", "8.8.8.8:443"),
        ];
        assert_eq!(
            lines(&edges(&directory, "shop", said)),
            [
                "job:migrate -> service:db 5432/tcp via \"\" x1 from 1",
                "namespace: -> service:db 5432/tcp via \"\" x1 from 1",
                "node:n1 -> service:web 3000/tcp via \"\" x1 from 1",
                "outside: -> service:db 5432/tcp via \"published\" x1 from 1",
                "outside: -> service:nginx 80/tcp via \"published\" x3 from 2",
                "service:nginx -> service:web 3000/tcp via \"service\" x1 from 1",
                "service:web -> outside: 53/udp via \"\" x1 from 1",
                "service:web -> service:db 5432/tcp via \"\" x1 from 1",
            ]
        );
    }
}
