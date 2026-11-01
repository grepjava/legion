//! Counts the manager's gRPC requests by method and answer, and times how
//! long each took to be answered, for the metrics. A call is timed until
//! its answer starts: the whole call for a unary one, until the stream
//! starts for a streaming one. It is counted when it ends, with the code it
//! ended with; a stream the client drops is `Cancelled`. Calls to methods
//! the manager does not serve are counted as `unknown`, so what a client
//! sends cannot add labels.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http_body::{Body, Frame, SizeHint};
use legion_api::metrics::Metrics;
use tonic::Code;

/// The upper bounds of the latency buckets, in seconds.
const BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A method: its service (`legion.v1.Objects`) and its name (`Apply`).
type Method = (String, String);

/// The requests a manager has answered since it started.
#[derive(Default)]
pub struct Requests {
    methods: Mutex<BTreeMap<Method, Calls>>,
}

#[derive(Default)]
struct Calls {
    codes: BTreeMap<i32, u64>,
    /// Answers no later than each bucket's bound, not yet cumulative.
    buckets: [u64; BUCKETS.len()],
    sum: f64,
    count: u64,
}

impl Requests {
    /// Writes `legion_api_requests_total` and
    /// `legion_api_request_duration_seconds`.
    pub fn collect(&self, m: &mut Metrics) {
        let methods = self.methods.lock().unwrap_or_else(|e| e.into_inner());
        let mut total = m.counter(
            "legion_api_requests_total",
            "gRPC calls this manager answered, by method and the code they ended with.",
        );
        for ((service, method), calls) in methods.iter() {
            for (code, n) in &calls.codes {
                let code = format!("{:?}", Code::from_i32(*code));
                total.sample(
                    &[("service", service), ("method", method), ("code", &code)],
                    *n as f64,
                );
            }
        }
        let mut seconds = m.histogram(
            "legion_api_request_duration_seconds",
            "How long this manager took to answer gRPC calls, by method: \
             until the answer starts (a stream's first message).",
        );
        for ((service, method), calls) in methods.iter() {
            let mut so_far = 0;
            let buckets: Vec<(f64, u64)> = BUCKETS
                .iter()
                .zip(calls.buckets)
                .map(|(le, n)| {
                    so_far += n;
                    (*le, so_far)
                })
                .collect();
            seconds.observed(
                &[("service", service), ("method", method)],
                &buckets,
                calls.sum,
                calls.count,
            );
        }
    }

    /// Times a call to `path` answered after `took`, and counts it if its
    /// answer already holds its code. Returns the method it counts under.
    fn answered(&self, path: &str, code: Option<i32>, took: Duration) -> Method {
        let method = match path.strip_prefix('/').and_then(|p| p.split_once('/')) {
            Some((service, method)) if code != Some(Code::Unimplemented as i32) => {
                (service.to_owned(), method.to_owned())
            }
            _ => ("unknown".to_owned(), "unknown".to_owned()),
        };
        let mut methods = self.methods.lock().unwrap_or_else(|e| e.into_inner());
        let calls = methods.entry(method.clone()).or_default();
        let secs = took.as_secs_f64();
        if let Some(i) = BUCKETS.iter().position(|le| secs <= *le) {
            calls.buckets[i] += 1;
        }
        calls.sum += secs;
        calls.count += 1;
        if let Some(code) = code {
            *calls.codes.entry(code).or_default() += 1;
        }
        method
    }

    fn ended(&self, method: &Method, code: i32) {
        let mut methods = self.methods.lock().unwrap_or_else(|e| e.into_inner());
        *methods
            .entry(method.clone())
            .or_default()
            .codes
            .entry(code)
            .or_default() += 1;
    }
}

/// Counts the requests of the services it wraps into [`Requests`].
#[derive(Clone)]
pub struct Layer(pub Arc<Requests>);

impl<S> tower_layer::Layer<S> for Layer {
    type Service = Counted<S>;

    fn layer(&self, inner: S) -> Counted<S> {
        Counted {
            inner,
            requests: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Counted<S> {
    inner: S,
    requests: Arc<Requests>,
}

impl<S, B, R> tower_service::Service<http::Request<B>> for Counted<S>
where
    S: tower_service::Service<http::Request<B>, Response = http::Response<R>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Watched<R>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let started = Instant::now();
        let answer = self.inner.call(req);
        let requests = self.requests.clone();
        Box::pin(async move {
            let res = answer.await?;
            let code = grpc_status(res.headers());
            let method = requests.answered(&path, code, started.elapsed());
            let open = code.is_none().then_some(Open {
                requests,
                method,
                ended: false,
            });
            Ok(res.map(|inner| Watched { inner, open }))
        })
    }
}

fn grpc_status(headers: &http::HeaderMap) -> Option<i32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

/// A call answered but not yet ended; counted `Cancelled` if dropped so.
struct Open {
    requests: Arc<Requests>,
    method: Method,
    ended: bool,
}

impl Open {
    fn end(mut self, code: i32) {
        self.requests.ended(&self.method, code);
        self.ended = true;
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if !self.ended {
            self.requests.ended(&self.method, Code::Cancelled as i32);
        }
    }
}

/// An answer's body, which counts its call when its trailers end it.
pub struct Watched<R> {
    inner: R,
    open: Option<Open>,
}

impl<R: Body + Unpin> Body for Watched<R> {
    type Data = R::Data;
    type Error = R::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<R::Data>, R::Error>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(f))) => {
                if let Some(code) = f.trailers_ref().map(grpc_status)
                    && let Some(open) = self.open.take()
                {
                    open.end(code.unwrap_or(Code::Unknown as i32));
                }
            }
            Poll::Ready(None) => {
                if let Some(open) = self.open.take() {
                    open.end(Code::Unknown as i32);
                }
            }
            _ => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_times_by_method_and_code() {
        let r = Arc::new(Requests::default());
        let ok = Code::Ok as i32;
        r.answered("/legion.v1.Objects/Get", Some(ok), Duration::from_millis(3));
        r.answered("/legion.v1.Objects/Get", Some(5), Duration::from_millis(30));
        r.answered("/legion.v1.Objects/Get", Some(ok), Duration::from_secs(60));
        // Unknown methods share a label.
        r.answered("/no.Such/Thing", Some(12), Duration::ZERO);
        r.answered("garbage", Some(12), Duration::ZERO);
        // Streams: timed when they start, counted when they end, or are
        // dropped before.
        for end in [Some(ok), None] {
            let method = r.answered("/legion.v1.Objects/Watch", None, Duration::from_millis(1));
            let open = Open {
                requests: r.clone(),
                method,
                ended: false,
            };
            if let Some(code) = end {
                open.end(code);
            }
        }
        // Ended or dropped, an open call lets go of what it held.
        assert_eq!(Arc::strong_count(&r), 1);
        let mut m = Metrics::default();
        r.collect(&mut m);
        let text = m.finish();
        for line in [
            "legion_api_requests_total{service=\"legion.v1.Objects\",method=\"Get\",code=\"Ok\"} 2",
            "legion_api_requests_total{service=\"legion.v1.Objects\",method=\"Get\",code=\"NotFound\"} 1",
            "legion_api_requests_total{service=\"unknown\",method=\"unknown\",code=\"Unimplemented\"} 2",
            "legion_api_requests_total{service=\"legion.v1.Objects\",method=\"Watch\",code=\"Ok\"} 1",
            "legion_api_requests_total{service=\"legion.v1.Objects\",method=\"Watch\",code=\"Cancelled\"} 1",
            "legion_api_request_duration_seconds_bucket{service=\"legion.v1.Objects\",method=\"Get\",le=\"0.005\"} 1",
            "legion_api_request_duration_seconds_bucket{service=\"legion.v1.Objects\",method=\"Get\",le=\"0.05\"} 2",
            "legion_api_request_duration_seconds_bucket{service=\"legion.v1.Objects\",method=\"Get\",le=\"10\"} 2",
            "legion_api_request_duration_seconds_bucket{service=\"legion.v1.Objects\",method=\"Get\",le=\"+Inf\"} 3",
            "legion_api_request_duration_seconds_count{service=\"legion.v1.Objects\",method=\"Watch\"} 2",
        ] {
            assert!(text.lines().any(|l| l == line), "{line} in\n{text}");
        }
    }
}
