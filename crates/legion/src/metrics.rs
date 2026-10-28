//! The daemon's Prometheus endpoint: `GET /metrics` over plain HTTP, with
//! the manager's metrics and the agent's, whichever the daemon runs.

use std::convert::Infallible;
use std::sync::Arc;

use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use legion_api::metrics::{CONTENT_TYPE, Metrics};
use tokio::net::TcpListener;

/// Collects the metrics for one scrape.
pub type Collect = Arc<dyn Fn() -> BoxFuture<'static, Metrics> + Send + Sync>;

/// Collects from the manager and the agent the daemon runs.
pub fn collect(
    manager: Option<legion_manager::metrics::ManagerMetrics>,
    agent: Option<legion_agent::metrics::AgentMetrics<legion_podman::Podman>>,
) -> Collect {
    Arc::new(move || {
        let (manager, agent) = (manager.clone(), agent.clone());
        Box::pin(async move {
            let mut m = Metrics::default();
            if let Some(manager) = manager {
                match tokio::task::spawn_blocking(move || manager.collect()).await {
                    Ok(mm) => m.extend(mm),
                    Err(e) => tracing::warn!("collecting the manager's metrics: {e}"),
                }
            }
            if let Some(agent) = agent {
                m.extend(agent.collect().await);
            }
            m
        })
    })
}

/// Serves `/metrics` on `listener` until the task is dropped.
pub async fn serve(listener: TcpListener, collect: Collect) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!("accepting a metrics connection: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        let collect = collect.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                let collect = collect.clone();
                async move { Ok::<_, Infallible>(respond(req, &collect).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!("metrics connection: {e}");
            }
        });
    }
}

async fn respond(req: Request<Incoming>, collect: &Collect) -> Response<Full<Bytes>> {
    let reply = |status, kind: &'static str, body: String| {
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, kind)
            .body(Full::new(Bytes::from(body)))
            .expect("a valid response")
    };
    if req.uri().path() != "/metrics" {
        return reply(
            StatusCode::NOT_FOUND,
            "text/plain",
            "metrics are at /metrics\n".into(),
        );
    }
    if req.method() != Method::GET && req.method() != Method::HEAD {
        return reply(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "GET only\n".into(),
        );
    }
    reply(StatusCode::OK, CONTENT_TYPE, collect().await.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn serves_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let collect: Collect = Arc::new(|| {
            Box::pin(async {
                let mut m = Metrics::default();
                m.gauge("legion_up", "Up.").sample(&[], 1.0);
                m
            })
        });
        let server = tokio::spawn(serve(listener, collect));
        let ok = get(addr, "/metrics").await;
        assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
        assert!(ok.contains("text/plain; version=0.0.4"), "{ok}");
        assert!(
            ok.ends_with("# TYPE legion_up gauge\nlegion_up 1\n"),
            "{ok}"
        );
        assert!(get(addr, "/").await.starts_with("HTTP/1.1 404"));
        server.abort();
    }
}
