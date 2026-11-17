//! A manager's session asking for this node's connections (`Connections`):
//! those pf keeps state for that a pod's address is an end of, open or
//! closed in about the last minute and a half (TCP's TIME_WAIT). The
//! manager names the ends from what it knows of tasks and nodes.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use legion_sys::PfConn;
use serde::{Deserialize, Serialize};

/// One connection, as the manager reads it (`legion_manager::connections`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    pub proto: String,
    /// The end that opened it, before any NAT.
    pub client: SocketAddr,
    /// The client as it went on, when NAT changed it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub translated: Option<SocketAddr>,
    /// The end that answered.
    pub server: SocketAddr,
    /// Where the client sent it, when a redirect sent it on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addressed: Option<SocketAddr>,
}

/// Each connection once, though pf keeps a state for it on each interface
/// it crosses, IPv6 pod, service and mesh addresses written as their IPv4
/// twins, and only those with a pod at an end: the host's own are not the
/// cluster's business.
pub fn seen(conns: Vec<PfConn>) -> Vec<Seen> {
    let fold = |a: SocketAddr| match a.ip() {
        IpAddr::V6(v6) => legion_net::v4(v6).map_or(a, |v4| SocketAddr::new(v4.into(), a.port())),
        IpAddr::V4(_) => a,
    };
    let pod = |a: &SocketAddr| match a.ip() {
        IpAddr::V4(v4) => legion_net::CLUSTER_POD_CIDR.contains(v4),
        IpAddr::V6(_) => false,
    };
    let mut by: BTreeMap<(String, SocketAddr, SocketAddr), Seen> = BTreeMap::new();
    for c in conns {
        let (client, server) = (fold(c.client), fold(c.server));
        if !pod(&client) && !pod(&server) {
            continue;
        }
        let s = by
            .entry((c.proto.clone(), client, server))
            .or_insert_with(|| Seen {
                proto: c.proto,
                client,
                translated: None,
                server,
                addressed: None,
            });
        s.translated = s.translated.or(c.translated.map(fold));
        s.addressed = s.addressed.or(c.addressed.map(fold));
    }
    by.into_values().collect()
}

/// Answers the manager: the [`Seen`] list as JSON on `stdout`, then
/// `exit_code` 0.
pub(crate) async fn send(
    volumes: &crate::transfer::Volumes,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), tonic::Status> {
    use legion_proto::v1::SessionFrame;
    use legion_proto::v1::session_frame::Frame;
    let conns = crate::task::blocking(&volumes.sys, |sys| sys.pf_connections())
        .await
        .map_err(|e| tonic::Status::unavailable(format!("reading pf's states: {e}")))?;
    let json = serde_json::to_vec(&seen(conns))
        .map_err(|e| tonic::Status::internal(format!("the connections: {e}")))?;
    for frame in [Frame::Stdout(json), Frame::ExitCode(0)] {
        let _ = tx.send(SessionFrame { frame: Some(frame) }).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn conn(client: &str, server: &str) -> PfConn {
        PfConn {
            proto: "tcp".into(),
            client: a(client),
            translated: None,
            server: a(server),
            addressed: None,
            state: "ESTABLISHED:ESTABLISHED".into(),
        }
    }

    #[test]
    fn a_connection_is_counted_once_with_what_each_of_its_states_says() {
        let seen = seen(vec![
            // To a service's address: the redirect's state, then the
            // hairpin NAT's.
            PfConn {
                addressed: Some(a("10.201.0.3:3000")),
                ..conn("10.200.0.3:30236", "10.200.0.5:3000")
            },
            PfConn {
                translated: Some(a("10.200.0.1:56952")),
                ..conn("10.200.0.3:30236", "10.200.0.5:3000")
            },
            // Over IPv6, to a service's address.
            PfConn {
                addressed: Some(a("[fd00:10:201::3]:3000")),
                ..conn("[fd00:10:200:0::3]:41000", "[fd00:10:200:0::5]:3000")
            },
            // The host's own: not the cluster's.
            conn("192.168.100.219:22", "192.168.100.18:53778"),
            conn("[fd00::1]:22", "[fd00::2]:5000"),
            // From outside, to a published port.
            PfConn {
                addressed: Some(a("192.168.100.219:8080")),
                ..conn("192.168.100.100:55434", "10.200.0.3:80")
            },
            conn("192.168.100.100:55434", "10.200.0.3:80"),
            // A pod going out.
            conn("10.200.0.5:45986", "8.8.8.8:53"),
        ]);
        let one =
            |client: &str, translated: Option<&str>, server: &str, addressed: Option<&str>| Seen {
                proto: "tcp".into(),
                client: a(client),
                translated: translated.map(a),
                server: a(server),
                addressed: addressed.map(a),
            };
        assert_eq!(
            seen,
            [
                one(
                    "10.200.0.3:30236",
                    Some("10.200.0.1:56952"),
                    "10.200.0.5:3000",
                    Some("10.201.0.3:3000")
                ),
                one(
                    "10.200.0.3:41000",
                    None,
                    "10.200.0.5:3000",
                    Some("10.201.0.3:3000")
                ),
                one("10.200.0.5:45986", None, "8.8.8.8:53", None),
                one(
                    "192.168.100.100:55434",
                    None,
                    "10.200.0.3:80",
                    Some("192.168.100.219:8080")
                ),
            ]
        );
    }
}
