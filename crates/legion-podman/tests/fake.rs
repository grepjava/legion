//! The client against a canned libpod server over TCP: request shapes,
//! status handling and stream decoding, with responses copied from Podman
//! 5.8 on FreeBSD.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use legion_podman::{ContainerSpec, Endpoint, Error, Output, Podman};

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    body: serde_json::Value,
}

type Reply = (u16, &'static [u8]);

/// Serves `routes` (method and path → status and body) and records requests.
async fn fake(routes: Vec<(&'static str, &'static str, Reply)>) -> (Podman, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let routes = Arc::new(routes);
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let (conn, _) = listener.accept().await.unwrap();
            let routes = routes.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let routes = routes.clone();
                    let log = log.clone();
                    async move {
                        let method = req.method().to_string();
                        let path = req.uri().path().to_owned();
                        let query = form_urlencoded_pairs(req.uri().query().unwrap_or(""));
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                        log.lock().unwrap().push(Seen {
                            method: method.clone(),
                            path: path.clone(),
                            query,
                            body,
                        });
                        let (status, body) = routes
                            .iter()
                            .find(|(m, p, _)| *m == method && path == format!("/v5.0.0/libpod{p}"))
                            .map(|(_, _, r)| *r)
                            .unwrap_or((404, br#"{"message":"no route"}"#));
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .header("Libpod-Api-Version", "5.8.9")
                                .body(Full::new(Bytes::from_static(body)))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(conn), svc)
                    .await;
            });
        }
    });
    (Podman::new(Endpoint::Tcp(addr.to_string())), seen)
}

fn form_urlencoded_pairs(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect()
}

fn decode(s: &str) -> String {
    let s = s.replace('+', " ");
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

#[tokio::test]
async fn ping_reports_version() {
    let (podman, _) = fake(vec![("GET", "/_ping", (200, b"OK"))]).await;
    assert_eq!(podman.ping().await.unwrap(), "5.8.9");
}

#[tokio::test]
async fn create_sends_the_spec() {
    let (podman, seen) = fake(vec![(
        "POST",
        "/containers/create",
        (201, br#"{"Id":"016c90","Warnings":null}"#),
    )])
    .await;
    let mut spec = ContainerSpec {
        name: "t1-infra".into(),
        image: "docker.io/freebsd/freebsd-runtime:15.1".into(),
        command: vec!["/bin/sleep".into(), "3600".into()],
        stop_timeout: Some(2),
        ..Default::default()
    };
    spec.annotations
        .insert("org.freebsd.jail.vnet".into(), "new".into());
    spec.labels.insert("legion.task.id".into(), "t1".into());
    assert_eq!(podman.create(&spec).await.unwrap(), "016c90");
    let body = &seen.lock().unwrap()[0].body;
    assert_eq!(
        *body,
        serde_json::json!({
            "name": "t1-infra",
            "image": "docker.io/freebsd/freebsd-runtime:15.1",
            "command": ["/bin/sleep", "3600"],
            "labels": { "legion.task.id": "t1" },
            "annotations": { "org.freebsd.jail.vnet": "new" },
            "netns": { "nsmode": "none" },
            "stop_timeout": 2,
        })
    );
}

#[tokio::test]
async fn statuses() {
    let (podman, seen) = fake(vec![
        ("POST", "/containers/up/start", (304, b"")),
        ("POST", "/containers/up/stop", (204, b"")),
        (
            "DELETE",
            "/containers/gone",
            (
                404,
                br#"{"cause":"no such container","message":"no container with ID or name \"gone\" found: no such container","response":404}"#,
            ),
        ),
    ])
    .await;
    podman.start("up").await.unwrap();
    podman.stop("up", Duration::from_secs(7)).await.unwrap();
    let err = podman.remove("gone").await.unwrap_err();
    assert!(err.is_not_found(), "{err}");
    assert!(
        err.to_string().contains("no container with ID or name"),
        "{err}"
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen[1].query, [("timeout".to_owned(), "7".to_owned())]);
    assert_eq!(seen[2].method, "DELETE");
    assert_eq!(seen[2].path, "/v5.0.0/libpod/containers/gone");
    assert_eq!(seen[2].query, [("force".to_owned(), "true".to_owned())]);
}

#[tokio::test]
async fn list_filters_by_label() {
    let (podman, seen) = fake(vec![(
        "GET",
        "/containers/json",
        (
            200,
            br#"[{"AutoRemove":false,"Command":["/bin/sleep","3600"],"ExitCode":0,"Exited":false,"Id":"016c90","Image":"docker.io/freebsd/freebsd-runtime:15.1","Labels":{"legion.task.id":"t1"},"Mounts":[],"Names":["lab-infra"],"Networks":[],"Pid":39706,"Ports":null,"State":"running"}]"#,
        ),
    )])
    .await;
    let list = podman.list(&[("legion.task.id", "t1")]).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name(), "lab-infra");
    assert_eq!(list[0].state, "running");
    let q = &seen.lock().unwrap()[0].query;
    assert!(q.contains(&("all".into(), "true".into())));
    let filters = q.iter().find(|(k, _)| k == "filters").unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&filters.1).unwrap(),
        serde_json::json!({ "label": ["legion.task.id=t1"] })
    );
}

#[tokio::test]
async fn inspect_reads_state() {
    let (podman, _) = fake(vec![(
        "GET",
        "/containers/016c90/json",
        (
            200,
            br#"{"Id":"016c90","Name":"lab-infra","State":{"OciVersion":"1.2.1","Status":"exited","Running":false,"Pid":0,"ExitCode":143,"OOMKilled":false,"StartedAt":"2026-10-26T22:04:42.135607566+08:00","FinishedAt":"2026-10-26T22:04:45Z"},"Config":{"Labels":{"legion.task.id":"t1"},"Annotations":{"org.freebsd.jail.vnet":"new"}}}"#,
        ),
    )])
    .await;
    let c = podman.inspect("016c90").await.unwrap();
    assert_eq!(
        (c.state.status.as_str(), c.state.exit_code),
        ("exited", 143)
    );
    assert_eq!(c.config.annotations["org.freebsd.jail.vnet"], "new");
}

#[tokio::test]
async fn exec_collects_output_and_exit_code() {
    let (podman, seen) = fake(vec![
        ("POST", "/containers/c1/exec", (201, br#"{"Id":"e1"}"#)),
        (
            "POST",
            "/exec/e1/start",
            (200, b"\x01\0\0\0\0\0\0\x04out\n\x02\0\0\0\0\0\0\x04err\n"),
        ),
        (
            "GET",
            "/exec/e1/json",
            (200, br#"{"ExitCode":3,"ID":"e1","Running":false,"Pid":0}"#),
        ),
    ])
    .await;
    let cmd = ["/bin/sh", "-c", "echo out; echo err >&2; exit 3"].map(String::from);
    let out = podman.exec("c1", &cmd).await.unwrap();
    assert_eq!(out.exit_code, 3);
    assert_eq!(out.stdout, b"out\n");
    assert_eq!(out.stderr, b"err\n");
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].body["Cmd"], serde_json::json!(cmd));
    assert_eq!(
        seen[1].body,
        serde_json::json!({ "Detach": false, "Tty": false })
    );
}

#[tokio::test]
async fn logs_are_demultiplexed() {
    let (podman, seen) = fake(vec![(
        "GET",
        "/containers/c1/logs",
        (
            200,
            b"\x01\0\0\0\0\0\0\x06hello\n\x02\0\0\0\0\0\0\x05oops\n",
        ),
    )])
    .await;
    let chunks: Vec<_> = podman
        .logs("c1", false, Some(10))
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(
        (chunks[0].stream, &chunks[0].data[..]),
        (Output::Stdout, &b"hello\n"[..])
    );
    assert_eq!(
        (chunks[1].stream, &chunks[1].data[..]),
        (Output::Stderr, &b"oops\n"[..])
    );
    assert!(
        seen.lock().unwrap()[0]
            .query
            .contains(&("tail".into(), "10".into()))
    );
}

#[tokio::test]
async fn events_stream() {
    let (podman, seen) = fake(vec![(
        "GET",
        "/events",
        (
            200,
            br#"{"status":"start","id":"67dd","Type":"container","Action":"start","Actor":{"ID":"67dd","Attributes":{"legion.task.id":"t2","name":"lab-ev","podId":""}},"scope":"local","time":1793023503,"timeNano":1793023503035733201}
{"status":"died","id":"67dd","Type":"container","Action":"died","Actor":{"ID":"67dd","Attributes":{"containerExitCode":"7","legion.task.id":"t2","name":"lab-ev"}},"scope":"local","time":1793023504,"timeNano":1793023504067641219}
"#,
        ),
    )])
    .await;
    let events: Vec<_> = podman
        .events(&[("legion.task.id", "t2")])
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].action, "start");
    assert_eq!(events[0].exit_code(), None);
    assert_eq!(
        (events[1].action.as_str(), events[1].exit_code()),
        ("died", Some(7))
    );
    let q = &seen.lock().unwrap()[0].query;
    let filters = q.iter().find(|(k, _)| k == "filters").unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&filters.1).unwrap(),
        serde_json::json!({ "type": ["container"], "label": ["legion.task.id=t2"] })
    );
}

#[tokio::test]
async fn pull() {
    let (podman, seen) = fake(vec![(
        "POST",
        "/images/pull",
        (
            200,
            br#"{"stream":"Copying blob sha256:78d6\n"}
{"stream":"Writing manifest to image destination\n"}
{"images":["90c493"],"id":"90c493"}
"#,
        ),
    )])
    .await;
    let auth = legion_podman::RegistryAuth {
        username: "u".into(),
        password: "p".into(),
    };
    assert_eq!(
        podman
            .pull("docker.io/freebsd/freebsd-runtime:15.1", Some(&auth))
            .await
            .unwrap(),
        "90c493"
    );
    assert_eq!(
        seen.lock().unwrap()[0].query,
        [(
            "reference".to_owned(),
            "docker.io/freebsd/freebsd-runtime:15.1".to_owned()
        )]
    );

    let (podman, _) = fake(vec![(
        "POST",
        "/images/pull",
        (
            200,
            br#"{"error":"initializing source docker://nope/nothing:1: requested access to the resource is denied"}
"#,
        ),
    )])
    .await;
    let err = podman.pull("nope/nothing:1", None).await.unwrap_err();
    assert!(matches!(err, Error::Pull { .. }), "{err}");
}

#[tokio::test]
async fn wait_returns_exit_code() {
    let (podman, _) = fake(vec![("POST", "/containers/c1/wait", (200, b"143\n"))]).await;
    assert_eq!(podman.wait("c1").await.unwrap(), 143);
}

#[tokio::test]
async fn image_exists() {
    let (podman, seen) = fake(vec![(
        "GET",
        "/images/docker.io/freebsd/freebsd-runtime:15.1/exists",
        (204, b""),
    )])
    .await;
    assert!(
        podman
            .image_exists("docker.io/freebsd/freebsd-runtime:15.1")
            .await
            .unwrap()
    );
    assert!(!podman.image_exists("missing:1").await.unwrap());
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn connect_error_names_the_endpoint() {
    let podman = Podman::new(Endpoint::Tcp("127.0.0.1:1".into()));
    let err = podman.ping().await.unwrap_err();
    assert!(matches!(err, Error::Connect { .. }), "{err}");
    assert!(err.to_string().contains("tcp://127.0.0.1:1"), "{err}");
}
