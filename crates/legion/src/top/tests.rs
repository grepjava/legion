use std::collections::BTreeSet;

use legion_api::task::{TaskState, TaskStatus};
use legion_api::{Kind, Object};
use legion_proto::v1 as pb;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::model::{LABEL_SERVICE, status};
use super::view::Tab;
use super::*;

fn object(
    kind: Kind,
    ns: &str,
    name: &str,
    version: u64,
    spec: serde_json::Value,
    status: serde_json::Value,
) -> Object {
    let mut o = Object::new(kind, ns, name, spec);
    o.status = status;
    o.metadata.version = version;
    o
}

fn node(name: &str, version: u64, state: &str, availability: &str) -> Object {
    object(
        Kind::Node,
        "",
        name,
        version,
        serde_json::json!({ "availability": availability }),
        serde_json::json!({ "state": state, "allocatable": { "cpu": "4", "memory": "8Gi" } }),
    )
}

fn task(name: &str, version: u64, node: &str, state: &str, ready: bool) -> Object {
    let mut t = object(
        Kind::Task,
        "shop",
        name,
        version,
        serde_json::json!({ "service": "web", "serviceUid": "1", "serviceGeneration": 1, "node": node,
            "pod": { "containers": [{ "name": "web", "image": "nginx:1" }],
                     "volumes": [{ "name": "d", "volume": { "name": "data" } }] } }),
        serde_json::json!({ "state": state, "ready": ready, "ip": "10.200.0.2" }),
    );
    t.metadata.labels.insert(LABEL_SERVICE.into(), "web".into());
    t
}

fn service(version: u64, ready: u32, desired: u32) -> Object {
    object(
        Kind::Service,
        "shop",
        "web",
        version,
        serde_json::json!({ "pod": { "containers": [{ "name": "web", "image": "nginx:1" }] } }),
        serde_json::json!({ "observedGeneration": 1, "desired": desired, "running": ready, "ready": ready }),
    )
}

fn cluster() -> Model {
    let mut m = Model::new(None);
    let mut sidecar = task("web-2-1", 5, "n2", "starting", false);
    sidecar.spec["pod"]["containers"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({ "name": "log", "image": "fluent:1" }));
    m.listed(
        vec![
            node("n1", 1, "ready", "active"),
            node("n2", 2, "ready", "active"),
            service(3, 1, 2),
            task("web-1-1", 4, "n1", "running", true),
            sidecar,
            object(
                Kind::Volume,
                "shop",
                "data",
                6,
                serde_json::json!({ "driver": "zfs", "size": "1Gi" }),
                serde_json::json!({ "node": "n1" }),
            ),
            object(
                Kind::CronJob,
                "shop",
                "report",
                7,
                serde_json::json!({ "schedule": "*/5 * * * *", "job": { "pod": { "containers": [{ "name": "r", "image": "tools:1" }] } } }),
                serde_json::json!({}),
            ),
            object(
                Kind::Secret,
                "shop",
                "key",
                8,
                serde_json::json!({ "data": "czNjcmV0" }),
                serde_json::Value::Null,
            ),
            object(
                Kind::Route,
                "shop",
                "www",
                9,
                serde_json::json!({ "host": "shop.example", "path": "/", "service": "web", "port": 80 }),
                serde_json::Value::Null,
            ),
        ],
        BTreeSet::new(),
    );
    m.usage(pb::UsageResponse {
        nodes: vec![pb::NodeUsage {
            node: "n1".into(),
            tasks: vec![pb::TaskUsage {
                namespace: "shop".into(),
                task: "web-1-1".into(),
                cpu_percent: 25.0,
                memory_bytes: 64 << 20,
            }],
        }],
    });
    m
}

fn screen_of(model: &Model, view: &View, width: u16) -> String {
    let mut t = Terminal::new(TestBackend::new(width, 20)).unwrap();
    t.draw(|f| draw::render(f, model, view)).unwrap();
    let buf = t.backend().buffer();
    (0..buf.area.height)
        .map(|y| {
            (0..buf.area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn screen(model: &Model, view: &View) -> String {
    screen_of(model, view, 130)
}

/// The line of the screen with `token`, without borders and bars, its
/// spaces collapsed.
fn line(screen: &str, token: &str) -> String {
    let l = screen
        .lines()
        .find(|l| l.contains(token))
        .unwrap_or_else(|| panic!("no {token}: {screen}"));
    l.chars()
        .filter(|c| !"│╭╮╰╯─▌█░·".contains(*c))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn typed(view: &mut View, model: &Model, keys: &str) -> Option<Request> {
    let mut last = None;
    for c in keys.chars() {
        let code = match c {
            '\n' => KeyCode::Enter,
            '\x1b' => KeyCode::Esc,
            '\x08' => KeyCode::Backspace,
            '↓' => KeyCode::Down,
            c => KeyCode::Char(c),
        };
        last = view.key(model, KeyEvent::new(code, KeyModifiers::NONE));
    }
    last
}

#[test]
fn shows_every_kind_with_its_use() {
    let m = cluster();
    let mut v = View::default();
    let s = screen(&m, &v);
    assert!(
        s.contains("2/2 nodes ready") && s.contains("1 running"),
        "{s}"
    );
    assert!(s.contains("0.2/8 CPU"), "{s}");
    assert_eq!(
        line(&s, "n1 "),
        "n1 ● ready active worker 1 0.25/4 64Mi/8Gi"
    );
    assert_eq!(line(&s, "n2 "), "n2 ● ready active worker 0 -/4 -/8Gi");

    typed(&mut v, &m, "2");
    let s = screen(&m, &v);
    assert_eq!(
        line(&s, "nginx"),
        "shop web replicated 1/2 25% 64Mi nginx:1"
    );
    assert_eq!(
        line(&s, "report"),
        "shop report cron job every */5 * * * * tools:1"
    );
    typed(&mut v, &m, "3");
    let s = screen(&m, &v);
    assert_eq!(
        line(&s, "web-1-1"),
        "shop web-1-1 n1 running ✓ 0 25% 64Mi 10.200.0.2"
    );
    typed(&mut v, &m, "4");
    assert_eq!(
        line(&screen(&m, &v), "data"),
        "shop data zfs 1Gi n1 web-1-1,web-2-1"
    );
    typed(&mut v, &m, "5");
    assert_eq!(
        line(&screen(&m, &v), "www"),
        "shop www route shop.example/ → web:80"
    );
    // A Secret's value is never kept, only its size.
    typed(&mut v, &m, "6");
    assert_eq!(line(&screen(&m, &v), "key"), "shop key secret 6B (hidden)");
    let secret = m.get(&(Kind::Secret, "shop".into(), "key".into())).unwrap();
    assert!(!secret.spec.to_string().contains("czNjcmV0"));
}

#[test]
fn details_and_yaml_of_the_selected_object() {
    let m = cluster();
    let mut v = View::default();
    // Wide enough, the details show beside the table.
    let s = screen_of(&m, &v, 170);
    assert!(s.contains("node n1") && s.contains("allocatable"), "{s}");
    typed(&mut v, &m, "i");
    assert!(!screen_of(&m, &v, 170).contains("allocatable"));
    typed(&mut v, &m, "y");
    let s = screen(&m, &v);
    assert!(
        s.contains("node n1 — YAML") && s.contains("availability: active"),
        "{s}"
    );
    typed(&mut v, &m, "j");
    assert!(v.yaml.is_some());
    typed(&mut v, &m, "x");
    assert!(v.yaml.is_none());
    typed(&mut v, &m, "?");
    assert!(screen(&m, &v).contains("Acting on the selected row"));
    typed(&mut v, &m, "x");
    assert!(!v.help);
}

#[test]
fn enter_shows_the_tasks_of_a_node_or_service() {
    let m = cluster();
    let mut v = View::default();
    typed(&mut v, &m, "↓\n");
    assert_eq!(v.tab, Tab::Tasks);
    let s = screen(&m, &v);
    assert!(s.contains("web-2-1") && !s.contains("web-1-1"), "{s}");
    assert!(s.contains("on node n2"), "{s}");
    // Esc shows all tasks again; q quits.
    typed(&mut v, &m, "\x1b");
    assert!(screen(&m, &v).contains("web-1-1"));
    assert!(!v.quit);
    typed(&mut v, &m, "q");
    assert!(v.quit);

    let mut v = View::default();
    typed(&mut v, &m, "2\n");
    let s = screen(&m, &v);
    assert!(s.contains("web-1-1") && s.contains("web-2-1"), "{s}");
    assert!(s.contains("of service shop/web"), "{s}");
}

#[test]
fn a_filter_narrows_the_rows_as_it_is_typed() {
    let m = cluster();
    let mut v = View::default();
    typed(&mut v, &m, "3/2-1");
    let s = screen(&m, &v);
    assert!(s.contains("web-2-1") && !s.contains("web-1-1"), "{s}");
    // Kept after Enter; Esc clears it.
    typed(&mut v, &m, "\n");
    assert!(screen(&m, &v).contains("/2-1"));
    typed(&mut v, &m, "\x1b");
    assert!(screen(&m, &v).contains("web-1-1"));
    // Esc while typing goes back to the filter before.
    typed(&mut v, &m, "/zzz");
    assert!(screen(&m, &v).contains("Nothing matches the filter."));
    typed(&mut v, &m, "\x1b");
    assert!(screen(&m, &v).contains("web-1-1"));
}

#[test]
fn ended_tasks_only_on_asking() {
    let mut m = cluster();
    m.changed(true, task("web-1-0", 19, "n1", "failed", false), 0);
    let mut v = View::default();
    typed(&mut v, &m, "3");
    assert!(!screen(&m, &v).contains("web-1-0"));
    typed(&mut v, &m, "h");
    let s = screen(&m, &v);
    let (live, ended) = (s.find("web-1-1").unwrap(), s.find("web-1-0").unwrap());
    assert!(live < ended, "ended last: {s}");
}

#[test]
fn keys_act_on_the_selected_row() {
    let m = cluster();
    let mut v = View::default();
    // A drain is asked first; anything but y cancels.
    assert_eq!(typed(&mut v, &m, "d"), None);
    assert!(screen(&m, &v).contains("Drain node n1? Its tasks move"));
    assert_eq!(typed(&mut v, &m, "n"), None);
    assert!(screen(&m, &v).contains("Cancelled."));
    let drain = Request::Availability {
        node: "n1".into(),
        to: "drain",
    };
    assert_eq!(typed(&mut v, &m, "dy"), Some(drain));
    assert_eq!(
        typed(&mut v, &m, "jp"),
        Some(Request::Availability {
            node: "n2".into(),
            to: "pause"
        })
    );
    assert_eq!(
        typed(&mut v, &m, "Ldisk=ssd\n"),
        Some(Request::Label {
            node: "n2".into(),
            key: "disk".into(),
            value: Some("ssd".into())
        })
    );
    assert_eq!(typed(&mut v, &m, "L=\n"), None);
    assert!(screen(&m, &v).contains("Cancelled: label"));

    // Scaling takes a number; letters are not typed in.
    let mut v = View::default();
    assert_eq!(typed(&mut v, &m, "2s1"), None);
    assert!(screen(&m, &v).contains("Replicas for shop/web: 1"));
    assert_eq!(
        typed(&mut v, &m, "x2\n"),
        Some(Request::Scale {
            namespace: "shop".into(),
            service: "web".into(),
            replicas: 12
        })
    );
    assert_eq!(
        typed(&mut v, &m, "ry"),
        Some(Request::Rollback {
            namespace: "shop".into(),
            service: "web".into()
        })
    );
    assert_eq!(
        typed(&mut v, &m, "jju"),
        Some(Request::Suspend {
            namespace: "shop".into(),
            cron_job: "report".into(),
            suspend: true
        })
    );

    // Deleting asks for the name.
    assert_eq!(typed(&mut v, &m, "Dreprt\n"), None);
    assert!(screen(&m, &v).contains("Not deleted: the name did not match."));
    assert_eq!(
        typed(&mut v, &m, "Dreport\n"),
        Some(Request::Delete {
            kind: Kind::CronJob,
            namespace: "shop".into(),
            name: "report".into()
        })
    );

    // A volume moves to the node typed.
    let mut v = View::default();
    assert_eq!(
        typed(&mut v, &m, "4mn2\n"),
        Some(Request::Move {
            namespace: "shop".into(),
            volume: "data".into(),
            node: "n2".into()
        })
    );
}

#[test]
fn logs_and_shells_ask_which_container_when_there_are_several() {
    let m = cluster();
    let mut v = View::default();
    assert_eq!(
        typed(&mut v, &m, "3l"),
        Some(Request::Logs {
            namespace: "shop".into(),
            name: "web-1-1".into(),
            container: None
        })
    );
    assert_eq!(typed(&mut v, &m, "jx"), None);
    assert_eq!(
        line(&screen(&m, &v), "which container"),
        "A shell in which container? 1 web 2 log Esc cancels"
    );
    let shell = typed(&mut v, &m, "2").unwrap();
    assert_eq!(
        shell,
        Request::Exec {
            namespace: "shop".into(),
            task: "web-2-1".into(),
            container: Some("log".into())
        }
    );
    let (banner, args) = command(&shell).unwrap();
    assert!(banner.starts_with("A shell in shop/web-2-1"));
    assert_eq!(args.join(" "), "exec -it -n shop web-2-1 -c log -- /bin/sh");
    // A task is deleted after a yes: its service starts another.
    assert_eq!(typed(&mut v, &m, "D"), None);
    assert!(screen(&m, &v).contains("Its service starts another"));
    assert!(matches!(
        typed(&mut v, &m, "y"),
        Some(Request::Delete {
            kind: Kind::Task,
            ..
        })
    ));
}

#[test]
fn global_and_autoscaled_services_are_not_scaled_here() {
    let mut m = cluster();
    let mut global = service(20, 1, 1);
    global.spec["mode"] = "global".into();
    m.changed(true, global, 0);
    let mut v = View::default();
    assert_eq!(typed(&mut v, &m, "2s"), None);
    assert!(screen(&m, &v).contains("global or autoscaled"));
    // The next key clears it.
    typed(&mut v, &m, "j");
    assert!(screen(&m, &v).contains("scale"));
}

#[test]
fn changes_are_logged_and_old_ones_ignored() {
    let mut m = cluster();
    let at = legion_api::clock::parse("2026-10-30T09:30:05Z").unwrap();
    m.changed(true, node("n2", 20, "suspect", "active"), at);
    m.changed(true, task("web-2-1", 21, "n2", "running", false), at);
    let mut failed = task("web-2-1", 22, "n2", "failed", false);
    failed.status["message"] = "container web exited with 1".into();
    m.changed(true, failed, at);
    m.changed(true, node("n1", 23, "ready", "drain"), at);
    let mut suspended = m
        .get(&(Kind::CronJob, "shop".into(), "report".into()))
        .unwrap()
        .clone();
    suspended.spec["suspend"] = true.into();
    suspended.metadata.version = 24;
    m.changed(true, suspended, at);
    // Replayed from before the listing: nothing changes.
    m.changed(true, task("web-1-1", 2, "n1", "failed", false), at);
    m.changed(false, service(1, 0, 0), at);
    let log: Vec<&str> = m.log.iter().rev().map(|(_, s)| s.as_str()).collect();
    assert_eq!(
        log,
        [
            "node n2: ready → suspect",
            "task shop/web-2-1 on n2: running",
            "task shop/web-2-1 on n2: failed: container web exited with 1",
            "node n1: active → drain",
            "cron job shop/report suspended",
        ]
    );
    assert_eq!(
        status::<TaskStatus>(
            m.get(&(Kind::Task, "shop".into(), "web-1-1".into()))
                .unwrap()
        )
        .state,
        TaskState::Running
    );
    let mut v = View::default();
    typed(&mut v, &m, "7");
    let s = screen(&m, &v);
    assert_eq!(line(&s, "drain"), "09:30:05 node n1: active → drain");
}

#[test]
fn what_the_caller_may_not_read_says_so() {
    let mut m = Model::new(Some("shop".into()));
    m.listed(
        vec![service(3, 1, 1), task("web-1-1", 4, "n1", "running", true)],
        BTreeSet::from([Kind::Node, Kind::Secret]),
    );
    // Another namespace's objects are not shown.
    let mut other = service(5, 1, 1);
    other.metadata.namespace = "b".into();
    m.changed(true, other, 0);
    let v = View::default();
    let s = screen(&m, &v);
    assert!(s.contains("do not let you read node"), "{s}");
    assert!(
        s.contains("namespace shop") && !s.contains("nodes ready"),
        "{s}"
    );
    assert_eq!(m.of(Kind::Service).count(), 1);
}

#[test]
fn a_problem_shows_until_listed_again() {
    let mut m = cluster();
    m.problem = Some("connection refused".into());
    let v = View::default();
    assert!(screen(&m, &v).contains("not current: connection refused"));
    m.listed(Vec::new(), BTreeSet::new());
    assert!(!screen(&m, &v).contains("not current"));
}

#[test]
fn once_prints_plain_tables() {
    let m = cluster();
    let v = View {
        tab: Tab::Nodes,
        ..View::default()
    };
    let text = draw::plain(&m, &v);
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines[0].starts_with("NAME  STATE"), "{text}");
    assert!(lines[1].starts_with("n1    ● ready"), "{text}");
    assert!(draw::plain_summary(&m).starts_with("● 2/2 nodes ready"));
}

#[test]
fn sizes() {
    assert_eq!(draw::bytes(512), "512B");
    assert_eq!(draw::bytes(64 << 20), "64Mi");
    assert_eq!(draw::bytes(3 << 29), "1.5Gi");
    assert_eq!(draw::trim(4.0), "4");
}
