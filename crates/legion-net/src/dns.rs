//! The node resolver (DESIGN.md §5.5). It listens on the bridge gateway,
//! which every pod uses as its name server, answers cluster names from the
//! service endpoints and relays every other query to the host's resolvers,
//! so pods resolve outside names even without a route out.
//!
//! Cluster names live under `legion.`:
//!
//! | Name | Answer |
//! |------|--------|
//! | `<svc>.<ns>.legion` | the service's address: its VIP, or its ready tasks |
//! | `tasks.<svc>.<ns>.legion` | the service's ready tasks |
//! | `<svc>-<n>.<svc>.<ns>.legion` | a stateful service's ready task `n` |
//!
//! `<svc>.<ns>` and `tasks.<svc>.<ns>` answer too when the service exists,
//! and pods search `<ns>.legion` and `legion`, so `<svc>` works within a
//! namespace. Unknown names under `legion.` get NXDOMAIN rather than being
//! relayed.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::{RData, Record, RecordType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;

/// The cluster's DNS domain.
pub const DOMAIN: &str = "legion";
/// Cluster answers change as tasks come and go; keep caches short.
pub const TTL: u32 = 5;
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(3);

/// A service as DNS publishes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAddrs {
    pub namespace: String,
    pub name: String,
    /// The VIP, if it has one.
    pub vip: Option<Ipv4Addr>,
    /// Ready tasks.
    pub tasks: Vec<Ipv4Addr>,
    /// A stateful service's ready tasks, by ordinal.
    pub ordinals: Vec<(u32, Ipv4Addr)>,
}

/// The cluster names and their addresses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Zone {
    /// Lowercase, without the trailing dot.
    names: BTreeMap<String, Vec<Ipv4Addr>>,
    namespaces: BTreeSet<String>,
}

/// What the zone says about a name.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<'a> {
    /// A cluster name, with these addresses (possibly none).
    Found(&'a [Ipv4Addr]),
    /// Under `legion.`, but no such name.
    NoSuchName,
    /// Not a cluster name: relay it.
    NotOurs,
}

impl Zone {
    pub fn new(services: impl IntoIterator<Item = ServiceAddrs>) -> Zone {
        let mut zone = Zone::default();
        for s in services {
            let base = format!("{}.{}.{DOMAIN}", s.name, s.namespace).to_lowercase();
            let mut tasks = s.tasks;
            tasks.sort();
            tasks.dedup();
            let addrs = match s.vip {
                Some(vip) => vec![vip],
                None => tasks.clone(),
            };
            zone.names.insert(format!("tasks.{base}"), tasks);
            for (n, ip) in s.ordinals {
                let name = format!("{}-{n}.{base}", s.name.to_lowercase());
                zone.names.insert(name, vec![ip]);
            }
            zone.names.insert(base, addrs);
            zone.namespaces.insert(s.namespace.to_lowercase());
        }
        zone
    }

    pub fn lookup(&self, name: &str) -> Lookup<'_> {
        let name = name.trim_end_matches('.').to_lowercase();
        if name == DOMAIN {
            return Lookup::Found(&[]);
        }
        if let Some(rest) = name.strip_suffix(&format!(".{DOMAIN}")) {
            return match self.names.get(&name) {
                Some(a) => Lookup::Found(a),
                // A namespace, or the `tasks` label above services: names
                // exist below, so it is not NXDOMAIN.
                None if self.namespaces.contains(rest) => Lookup::Found(&[]),
                None => Lookup::NoSuchName,
            };
        }
        match self.names.get(&format!("{name}.{DOMAIN}")) {
            Some(a) => Lookup::Found(a),
            None => Lookup::NotOurs,
        }
    }
}

/// Name servers from a `resolv.conf`.
pub fn resolv_conf_servers(text: &str) -> Vec<SocketAddr> {
    text.lines()
        .filter_map(|l| {
            let mut words = l.split_whitespace();
            (words.next()? == "nameserver").then_some(())?;
            let ip: std::net::IpAddr = words.next()?.parse().ok()?;
            Some(SocketAddr::new(ip, 53))
        })
        .collect()
}

/// A pod's `resolv.conf`.
pub fn pod_resolv_conf(namespace: &str, server: Ipv4Addr) -> String {
    // ndots:2 sends `<svc>.<ns>` through the search list before going out.
    format!("search {namespace}.{DOMAIN} {DOMAIN}\nnameserver {server}\noptions ndots:2\n")
}

/// Serves DNS on `udp` and `tcp` until the zone's sender goes away.
pub async fn serve(
    udp: UdpSocket,
    tcp: TcpListener,
    zone: watch::Receiver<Arc<Zone>>,
    upstream: Vec<SocketAddr>,
) {
    let udp = Arc::new(udp);
    let upstream = Arc::new(upstream);
    let mut closed = zone.clone();
    let mut buf = vec![0; 4096];
    loop {
        tokio::select! {
            r = udp.recv_from(&mut buf) => match r {
                Ok((n, peer)) => {
                    let query = buf[..n].to_vec();
                    let (udp, zone, upstream) = (udp.clone(), zone.clone(), upstream.clone());
                    tokio::spawn(async move {
                        let zone = zone.borrow().clone();
                        if let Some(resp) = answer(&query, &zone, &upstream, false).await {
                            let _ = udp.send_to(&resp, peer).await;
                        }
                    });
                }
                Err(e) => tracing::debug!("dns: receiving: {e}"),
            },
            r = tcp.accept() => match r {
                Ok((stream, _)) => {
                    let (zone, upstream) = (zone.clone(), upstream.clone());
                    tokio::spawn(async move {
                        if let Err(e) = serve_tcp(stream, zone, &upstream).await {
                            tracing::debug!("dns: tcp: {e}");
                        }
                    });
                }
                Err(e) => tracing::debug!("dns: accepting: {e}"),
            },
            r = closed.changed() => if r.is_err() {
                return;
            },
        }
    }
}

async fn serve_tcp(
    mut stream: TcpStream,
    zone: watch::Receiver<Arc<Zone>>,
    upstream: &[SocketAddr],
) -> std::io::Result<()> {
    loop {
        let query =
            match tokio::time::timeout(Duration::from_secs(10), read_framed(&mut stream)).await {
                Ok(Ok(q)) => q,
                // Closed or idle: done.
                Ok(Err(_)) | Err(_) => return Ok(()),
            };
        let zone = zone.borrow().clone();
        if let Some(resp) = answer(&query, &zone, upstream, true).await {
            write_framed(&mut stream, &resp).await?;
        }
    }
}

async fn read_framed(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let len = stream.read_u16().await?;
    let mut msg = vec![0; len.into()];
    stream.read_exact(&mut msg).await?;
    Ok(msg)
}

async fn write_framed(stream: &mut TcpStream, msg: &[u8]) -> std::io::Result<()> {
    let len = u16::try_from(msg.len()).map_err(std::io::Error::other)?;
    let mut framed = Vec::with_capacity(msg.len() + 2);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(msg);
    stream.write_all(&framed).await
}

/// The response to one query message: from the zone, or relayed. `None`
/// drops it (unparsable, or no upstream answered).
pub async fn answer(
    query: &[u8],
    zone: &Zone,
    upstream: &[SocketAddr],
    tcp: bool,
) -> Option<Vec<u8>> {
    let Ok(msg) = Message::from_vec(query) else {
        return None;
    };
    if msg.metadata.message_type != MessageType::Query
        || msg.metadata.op_code != OpCode::Query
        || msg.queries.len() != 1
    {
        return relay(query, upstream, tcp).await;
    }
    let q = &msg.queries[0];
    let found = match zone.lookup(&q.name().to_ascii()) {
        Lookup::NotOurs => return relay(query, upstream, tcp).await,
        Lookup::NoSuchName => None,
        Lookup::Found(addrs) => Some(addrs),
    };
    let mut resp = Message::response(msg.metadata.id, OpCode::Query);
    resp.metadata.authoritative = true;
    resp.metadata.recursion_desired = msg.metadata.recursion_desired;
    resp.metadata.recursion_available = true;
    resp.add_query(q.clone());
    match found {
        None => resp.metadata.response_code = ResponseCode::NXDomain,
        Some(addrs) => {
            if matches!(q.query_type(), RecordType::A | RecordType::ANY) {
                for ip in addrs {
                    resp.add_answer(Record::from_rdata(q.name().clone(), TTL, RData::A(A(*ip))));
                }
            }
        }
    }
    let bytes = resp.to_vec().ok()?;
    let limit = if tcp {
        usize::from(u16::MAX)
    } else {
        usize::from(msg.max_payload())
    };
    if bytes.len() <= limit {
        return Some(bytes);
    }
    // Too many addresses for UDP: the client retries over TCP.
    resp.answers.clear();
    resp.metadata.truncation = true;
    resp.to_vec().ok()
}

/// Relays a query to the first upstream that answers.
async fn relay(query: &[u8], upstream: &[SocketAddr], tcp: bool) -> Option<Vec<u8>> {
    for server in upstream {
        let r = tokio::time::timeout(UPSTREAM_TIMEOUT, async {
            if tcp {
                let mut s = TcpStream::connect(server).await?;
                write_framed(&mut s, query).await?;
                read_framed(&mut s).await
            } else {
                let bind: SocketAddr = if server.is_ipv4() {
                    "0.0.0.0:0".parse().unwrap()
                } else {
                    "[::]:0".parse().unwrap()
                };
                let s = UdpSocket::bind(bind).await?;
                s.connect(server).await?;
                s.send(query).await?;
                let mut buf = vec![0; 65535];
                // Skip anything that is not the answer to this query.
                loop {
                    let n = s.recv(&mut buf).await?;
                    if n >= 2 && query.len() >= 2 && buf[..2] == query[..2] {
                        return Ok(buf[..n].to_vec());
                    }
                }
            }
        })
        .await;
        match r {
            Ok(Ok(resp)) => return Some(resp),
            Ok(Err(e)) => tracing::debug!(%server, "dns: relaying: {e}"),
            Err(_) => tracing::debug!(%server, "dns: relaying: timed out"),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use hickory_proto::op::Query;
    use hickory_proto::rr::Name;

    use super::*;

    fn zone() -> Zone {
        Zone::new([
            ServiceAddrs {
                namespace: "shop".into(),
                name: "web".into(),
                vip: None,
                tasks: vec![Ipv4Addr::new(10, 200, 0, 3), Ipv4Addr::new(10, 200, 0, 2)],
                ordinals: vec![],
            },
            ServiceAddrs {
                namespace: "shop".into(),
                name: "db".into(),
                vip: Some(Ipv4Addr::new(10, 201, 0, 7)),
                tasks: vec![Ipv4Addr::new(10, 200, 0, 9)],
                ordinals: vec![(0, Ipv4Addr::new(10, 200, 0, 9))],
            },
            ServiceAddrs {
                namespace: "shop".into(),
                name: "idle".into(),
                vip: None,
                tasks: vec![],
                ordinals: vec![],
            },
        ])
    }

    #[test]
    fn lookups() {
        let z = zone();
        let web = [Ipv4Addr::new(10, 200, 0, 2), Ipv4Addr::new(10, 200, 0, 3)];
        assert_eq!(z.lookup("web.shop.legion."), Lookup::Found(&web));
        assert_eq!(z.lookup("WEB.Shop.legion"), Lookup::Found(&web));
        assert_eq!(z.lookup("web.shop."), Lookup::Found(&web));
        assert_eq!(z.lookup("tasks.web.shop.legion."), Lookup::Found(&web));
        let db0 = [Ipv4Addr::new(10, 200, 0, 9)];
        assert_eq!(z.lookup("db-0.db.shop.legion."), Lookup::Found(&db0));
        assert_eq!(z.lookup("db-0.db.shop."), Lookup::Found(&db0));
        assert_eq!(z.lookup("db-1.db.shop.legion."), Lookup::NoSuchName);
        assert_eq!(
            z.lookup("db.shop.legion."),
            Lookup::Found(&[Ipv4Addr::new(10, 201, 0, 7)])
        );
        assert_eq!(
            z.lookup("tasks.db.shop.legion."),
            Lookup::Found(&[Ipv4Addr::new(10, 200, 0, 9)])
        );
        assert_eq!(z.lookup("idle.shop.legion."), Lookup::Found(&[]));
        assert_eq!(z.lookup("shop.legion."), Lookup::Found(&[]));
        assert_eq!(z.lookup("legion."), Lookup::Found(&[]));
        assert_eq!(z.lookup("nope.shop.legion."), Lookup::NoSuchName);
        assert_eq!(z.lookup("web.other.legion."), Lookup::NoSuchName);
        assert_eq!(z.lookup("example.com."), Lookup::NotOurs);
        assert_eq!(z.lookup("web.other."), Lookup::NotOurs);
    }

    #[test]
    fn config_files() {
        assert_eq!(
            resolv_conf_servers(
                "# x\nsearch lan\nnameserver 127.0.0.1\nnameserver ::1\noptions edns0\n"
            ),
            ["127.0.0.1:53".parse().unwrap(), "[::1]:53".parse().unwrap()]
        );
        assert_eq!(
            pod_resolv_conf("shop", Ipv4Addr::new(10, 200, 0, 1)),
            "search shop.legion legion\nnameserver 10.200.0.1\noptions ndots:2\n"
        );
    }

    fn query(name: &str, kind: RecordType) -> Vec<u8> {
        let mut m = Message::query();
        m.metadata.recursion_desired = true;
        m.add_query(Query::query(Name::from_ascii(name).unwrap(), kind));
        m.to_vec().unwrap()
    }

    /// An upstream that answers every query with 192.0.2.1.
    async fn upstream() -> SocketAddr {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = s.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0; 4096];
            loop {
                let (n, peer) = s.recv_from(&mut buf).await.unwrap();
                let q = Message::from_vec(&buf[..n]).unwrap();
                let mut r = Message::response(q.metadata.id, OpCode::Query);
                r.add_query(q.queries[0].clone());
                r.add_answer(Record::from_rdata(
                    q.queries[0].name().clone(),
                    60,
                    RData::A(A::new(192, 0, 2, 1)),
                ));
                s.send_to(&r.to_vec().unwrap(), peer).await.unwrap();
            }
        });
        addr
    }

    fn addrs(resp: &Message) -> Vec<Ipv4Addr> {
        resp.answers
            .iter()
            .filter_map(|r| match &r.data {
                RData::A(a) => Some(a.0),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn serves_and_relays() {
        let up = upstream().await;
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (udp_addr, tcp_addr) = (udp.local_addr().unwrap(), tcp.local_addr().unwrap());
        let (tx, rx) = watch::channel(Arc::new(zone()));
        tokio::spawn(serve(udp, tcp, rx, vec![up]));

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ask = async |name: &str, kind| {
            client.send_to(&query(name, kind), udp_addr).await.unwrap();
            let mut buf = vec![0; 4096];
            let (n, _) = client.recv_from(&mut buf).await.unwrap();
            Message::from_vec(&buf[..n]).unwrap()
        };

        let r = ask("web.shop.legion.", RecordType::A).await;
        assert!(r.metadata.authoritative);
        assert_eq!(r.metadata.response_code, ResponseCode::NoError);
        assert_eq!(
            addrs(&r),
            [Ipv4Addr::new(10, 200, 0, 2), Ipv4Addr::new(10, 200, 0, 3)]
        );
        assert!(r.answers.iter().all(|a| a.ttl == TTL));
        // AAAA for a cluster name: the name exists, with no such records.
        let r = ask("web.shop.legion.", RecordType::AAAA).await;
        assert_eq!(r.metadata.response_code, ResponseCode::NoError);
        assert!(r.answers.is_empty());
        let r = ask("nope.shop.legion.", RecordType::A).await;
        assert_eq!(r.metadata.response_code, ResponseCode::NXDomain);
        // Everything else goes upstream.
        let r = ask("example.com.", RecordType::A).await;
        assert!(!r.metadata.authoritative);
        assert_eq!(addrs(&r), [Ipv4Addr::new(192, 0, 2, 1)]);

        // Over TCP too.
        let mut s = TcpStream::connect(tcp_addr).await.unwrap();
        write_framed(&mut s, &query("tasks.db.shop.legion.", RecordType::A))
            .await
            .unwrap();
        let r = Message::from_vec(&read_framed(&mut s).await.unwrap()).unwrap();
        assert_eq!(addrs(&r), [Ipv4Addr::new(10, 200, 0, 9)]);

        // The zone follows its sender.
        tx.send(Arc::new(Zone::default())).unwrap();
        let r = ask("web.shop.legion.", RecordType::A).await;
        assert_eq!(r.metadata.response_code, ResponseCode::NXDomain);
    }

    #[tokio::test]
    async fn truncates_large_udp_answers() {
        let many = Zone::new([ServiceAddrs {
            namespace: "shop".into(),
            name: "web".into(),
            vip: None,
            tasks: (2..200).map(|n| Ipv4Addr::new(10, 200, 0, n)).collect(),
            ordinals: vec![],
        }]);
        let q = query("web.shop.legion.", RecordType::A);
        let r = Message::from_vec(&answer(&q, &many, &[], false).await.unwrap()).unwrap();
        assert!(r.metadata.truncation && r.answers.is_empty());
        let r = Message::from_vec(&answer(&q, &many, &[], true).await.unwrap()).unwrap();
        assert_eq!(r.answers.len(), 198);
    }
}
