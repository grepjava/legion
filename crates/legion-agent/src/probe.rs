//! Container probes (DESIGN.md §5): `startup`, `liveness` and `readiness`.
//! Network checks (HTTP, TCP, gRPC) run from the host against the pod's
//! address, which the host reaches over `legion0`; exec checks run in the
//! container, whose images are often too small for a shell or client.
//!
//! One prober runs per running container. It counts consecutive results
//! against the probe's thresholds and tells the worker only of transitions:
//! started, unhealthy (the worker stops the container) and ready or not.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use legion_api::service::{Container, PodSpec, Probe, Probes};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::runtime::Runtime;

/// What a prober reports for one container run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The startup probe succeeded; liveness and readiness probes run now.
    Started,
    /// The startup or liveness probe failed: the container must go.
    Unhealthy(String),
    /// The readiness probe changed its mind.
    Ready(bool),
}

/// An outcome for the container run `run` of container `id`; outcomes of
/// earlier runs are stale.
#[derive(Debug)]
pub(crate) struct Report {
    pub id: String,
    pub run: u64,
    pub outcome: Outcome,
}

/// One check, resolved against the running pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Check {
    Http {
        addr: SocketAddr,
        path: String,
    },
    Tcp {
        addr: SocketAddr,
    },
    Grpc {
        addr: SocketAddr,
        service: String,
    },
    Exec {
        container: String,
        command: Vec<String>,
    },
}

impl Check {
    /// Resolves a probe of a container: ports by name, against the pod's
    /// address.
    pub fn new(
        probe: &Probe,
        pod: &PodSpec,
        ip: Option<IpAddr>,
        id: &str,
    ) -> Result<Check, String> {
        let addr = |port| {
            let port = pod
                .resolve_port(port)
                .ok_or_else(|| format!("no container port {port}"))?;
            let ip = ip.ok_or("the pod has no address")?;
            Ok::<_, String>(SocketAddr::new(ip, port))
        };
        Ok(if let Some(h) = &probe.http {
            Check::Http {
                addr: addr(&h.port)?,
                path: h.path.clone(),
            }
        } else if let Some(t) = &probe.tcp {
            Check::Tcp {
                addr: addr(&t.port)?,
            }
        } else if let Some(g) = &probe.grpc {
            Check::Grpc {
                addr: addr(&g.port)?,
                service: g.service.clone(),
            }
        } else if let Some(e) = &probe.exec {
            Check::Exec {
                container: id.to_owned(),
                command: e.command.clone(),
            }
        } else {
            return Err("the probe has no check".into());
        })
    }

    /// Runs the check once; the error says why it failed.
    pub async fn run<R: Runtime>(&self, rt: &R, timeout: Duration) -> Result<(), String> {
        match tokio::time::timeout(timeout, self.attempt(rt)).await {
            Ok(r) => r,
            Err(_) => Err(format!("timed out after {timeout:?}")),
        }
    }

    async fn attempt<R: Runtime>(&self, rt: &R) -> Result<(), String> {
        match self {
            Check::Tcp { addr } => TcpStream::connect(addr)
                .await
                .map(drop)
                .map_err(|e| format!("connecting to {addr}: {e}")),
            Check::Http { addr, path } => http(*addr, path).await,
            Check::Grpc { addr, service } => grpc(*addr, service).await,
            Check::Exec { container, command } => {
                let out = rt
                    .exec(container, command)
                    .await
                    .map_err(|e| format!("exec: {e}"))?;
                if out.exit_code == 0 {
                    return Ok(());
                }
                let mut why = format!("{:?} exited with {}", command[0], out.exit_code);
                let text = String::from_utf8_lossy(if out.stderr.is_empty() {
                    &out.stdout
                } else {
                    &out.stderr
                });
                if let Some(line) = text.lines().map(str::trim).find(|l| !l.is_empty()) {
                    why += ": ";
                    why += line;
                }
                Err(why)
            }
        }
    }
}

/// A GET of `path`; any 2xx or 3xx status passes.
async fn http(addr: SocketAddr, path: &str) -> Result<(), String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connecting to {addr}: {e}"))?;
    let (mut send, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| format!("HTTP to {addr}: {e}"))?;
    tokio::spawn(conn);
    let req = http::Request::get(path)
        .header(http::header::HOST, addr.to_string())
        .header(http::header::USER_AGENT, "legion-probe")
        .body(Empty::<Bytes>::new())
        .map_err(|e| format!("HTTP request for {path}: {e}"))?;
    let resp = send
        .send_request(req)
        .await
        .map_err(|e| format!("GET {path} from {addr}: {e}"))?;
    let status = resp.status();
    if status.is_success() || status.is_redirection() {
        Ok(())
    } else {
        Err(format!("GET {path} from {addr}: {status}"))
    }
}

/// The standard `grpc.health.v1.Health/Check`; only `SERVING` passes.
async fn grpc(addr: SocketAddr, service: &str) -> Result<(), String> {
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| format!("connecting to {addr}: {e}"))?;
    let resp = HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await
        .map_err(|s| format!("health check at {addr}: {}", s.message()))?;
    let status = resp.into_inner().status();
    if status == ServingStatus::Serving {
        Ok(())
    } else {
        Err(format!("health check at {addr}: {}", status.as_str_name()))
    }
}

/// The prober of one container run. Dropping it stops the probes.
#[derive(Debug)]
pub(crate) struct Prober(JoinHandle<()>);

impl Drop for Prober {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Resolved probes of a container.
struct Checks {
    startup: Option<(Probe, Check)>,
    liveness: Option<(Probe, Check)>,
    readiness: Option<(Probe, Check)>,
}

/// Starts probing a container that has just started (or been adopted), or
/// returns `None` if it has no probes. A probe that cannot be resolved is
/// reported as failing right away.
pub(crate) fn start<R: Runtime>(
    rt: &Arc<R>,
    c: &Container,
    pod: &PodSpec,
    ip: Option<IpAddr>,
    id: &str,
    run: u64,
    tx: mpsc::UnboundedSender<Report>,
) -> Option<Prober> {
    let Probes {
        startup,
        liveness,
        readiness,
    } = &c.probes;
    if c.probes.is_empty() {
        return None;
    }
    let send = {
        let id = id.to_owned();
        move |outcome| {
            let _ = tx.send(Report {
                id: id.clone(),
                run,
                outcome,
            });
        }
    };
    let resolve = |p: &Option<Probe>| -> Result<Option<(Probe, Check)>, String> {
        p.as_ref()
            .map(|p| Check::new(p, pod, ip, id).map(|c| (p.clone(), c)))
            .transpose()
    };
    let checks = match (resolve(startup), resolve(liveness), resolve(readiness)) {
        (Ok(startup), Ok(liveness), Ok(readiness)) => Checks {
            startup,
            liveness,
            readiness,
        },
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            send(Outcome::Unhealthy(format!("probe: {e}")));
            return None;
        }
    };
    let rt = rt.clone();
    Some(Prober(tokio::spawn(probe(rt, checks, send))))
}

async fn probe<R: Runtime>(rt: Arc<R>, checks: Checks, send: impl Fn(Outcome) + Send + Sync) {
    if let Some((p, check)) = &checks.startup {
        tokio::time::sleep(p.initial_delay.as_std()).await;
        let mut failures = 0;
        loop {
            match check.run(rt.as_ref(), p.timeout.as_std()).await {
                // One success is enough: the container has started.
                Ok(()) => break,
                Err(e) => {
                    failures += 1;
                    if failures >= p.failure_threshold {
                        return send(Outcome::Unhealthy(format!("startup probe failed: {e}")));
                    }
                }
            }
            tokio::time::sleep(p.period.as_std()).await;
        }
        send(Outcome::Started);
    }
    let liveness = async {
        let Some((p, check)) = &checks.liveness else {
            return std::future::pending().await;
        };
        tokio::time::sleep(p.initial_delay.as_std()).await;
        let mut failures = 0;
        loop {
            match check.run(rt.as_ref(), p.timeout.as_std()).await {
                Ok(()) => failures = 0,
                Err(e) => {
                    failures += 1;
                    tracing::debug!(failures, "liveness probe failed: {e}");
                    if failures >= p.failure_threshold {
                        return send(Outcome::Unhealthy(format!("liveness probe failed: {e}")));
                    }
                }
            }
            tokio::time::sleep(p.period.as_std()).await;
        }
    };
    let readiness = async {
        let Some((p, check)) = &checks.readiness else {
            return std::future::pending().await;
        };
        tokio::time::sleep(p.initial_delay.as_std()).await;
        let (mut ready, mut successes, mut failures) = (false, 0, 0);
        loop {
            match check.run(rt.as_ref(), p.timeout.as_std()).await {
                Ok(()) => {
                    (successes, failures) = (successes + 1, 0);
                    if !ready && successes >= p.success_threshold {
                        ready = true;
                        send(Outcome::Ready(true));
                    }
                }
                Err(e) => {
                    (successes, failures) = (0, failures + 1);
                    tracing::debug!(failures, "readiness probe failed: {e}");
                    if ready && failures >= p.failure_threshold {
                        ready = false;
                        send(Outcome::Ready(false));
                    }
                }
            }
            tokio::time::sleep(p.period.as_std()).await;
        }
    };
    // Liveness ends the prober; readiness runs until the container goes.
    tokio::select! {
        () = liveness => {}
        () = readiness => {}
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::fake::FakeRuntime;

    fn rt() -> FakeRuntime {
        FakeRuntime::new(Arc::new(legion_sys::FakeSys::new()))
    }

    /// Answers every connection with `status`, once per connection.
    async fn server(status: &'static str) -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0; 1024];
                    let _ = s.read(&mut buf).await;
                    let resp = format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\n\r\n");
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        addr
    }

    const T: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn http_status_decides() {
        let rt = rt();
        for (status, ok) in [
            ("200 OK", true),
            ("302 Found", true),
            ("500 Internal Server Error", false),
            ("404 Not Found", false),
        ] {
            let addr = server(status).await;
            let check = Check::Http {
                addr,
                path: "/healthz".into(),
            };
            let r = check.run(&rt, T).await;
            assert_eq!(r.is_ok(), ok, "{status}: {r:?}");
        }
    }

    #[tokio::test]
    async fn tcp_needs_a_listener() {
        let rt = rt();
        let addr = server("200 OK").await;
        assert_eq!(Check::Tcp { addr }.run(&rt, T).await, Ok(()));
        // A port nothing listens on, taken from a listener just closed.
        let gone = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = gone.local_addr().unwrap();
        drop(gone);
        // Refused at once, or (Windows retries refused connections) late.
        let err = Check::Tcp { addr }.run(&rt, T).await.unwrap_err();
        assert!(
            err.starts_with("connecting to") || err.starts_with("timed out"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn checks_time_out() {
        let rt = rt();
        // Accepts but never answers.
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let check = Check::Http {
            addr,
            path: "/".into(),
        };
        let err = check
            .run(&rt, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(err.starts_with("timed out"), "{err}");
        drop(l);
    }

    #[tokio::test]
    async fn grpc_health() {
        let rt = rt();
        let (reporter, service) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("legion.Test", tonic_health::ServingStatus::NotServing)
            .await;
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(l)),
        );
        let check = |service: &str| Check::Grpc {
            addr,
            service: service.into(),
        };
        assert_eq!(check("").run(&rt, T).await, Ok(()));
        let err = check("legion.Test").run(&rt, T).await.unwrap_err();
        assert!(err.ends_with("NOT_SERVING"), "{err}");
        let err = check("legion.Unknown").run(&rt, T).await.unwrap_err();
        assert!(err.contains("health check"), "{err}");
    }

    #[test]
    fn resolves_against_the_pod() {
        let pod: PodSpec = serde_yaml_ng::from_str(
            "containers:\n  - name: web\n    image: web:1\n    ports: [{ name: http, containerPort: 8080 }]\n",
        )
        .unwrap();
        let probe: Probe = serde_yaml_ng::from_str("http: { port: http, path: /ready }").unwrap();
        let ip = Some(IpAddr::V4(Ipv4Addr::new(10, 200, 0, 2)));
        assert_eq!(
            Check::new(&probe, &pod, ip, "c1"),
            Ok(Check::Http {
                addr: "10.200.0.2:8080".parse().unwrap(),
                path: "/ready".into()
            })
        );
        assert!(Check::new(&probe, &pod, None, "c1").is_err());
        let probe: Probe = serde_yaml_ng::from_str("exec: { command: [/healthy] }").unwrap();
        assert_eq!(
            Check::new(&probe, &pod, None, "c1"),
            Ok(Check::Exec {
                container: "c1".into(),
                command: vec!["/healthy".into()]
            })
        );
    }
}
