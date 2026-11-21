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
        serde_json::json!({ "state": state, "ready": ready, "ip": "10.200.0.2", "jid": 494 }),
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
            ..Default::default()
        }],
    });
    m
}

fn screen_of(model: &Model, view: &View, width: u16) -> String {
    screen_sized(model, view, width, 20)
}

fn screen_sized(model: &Model, view: &View, width: u16, height: u16) -> String {
    let mut t = Terminal::new(TestBackend::new(width, height)).unwrap();
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

/// A view on the nodes' list, where most tests start.
fn listing() -> View {
    let mut v = View::default();
    v.jump(view::Tab::Nodes);
    v
}

fn typed(view: &mut View, model: &Model, keys: &str) -> Option<Request> {
    let mut last = None;
    for c in keys.chars() {
        let code = match c {
            '\n' => KeyCode::Enter,
            '\x1b' => KeyCode::Esc,
            '\x08' => KeyCode::Backspace,
            '↓' => KeyCode::Down,
            '↑' => KeyCode::Up,
            '«' => KeyCode::F(3),
            '§' => KeyCode::F(10),
            c => KeyCode::Char(c),
        };
        last = view.key(model, KeyEvent::new(code, KeyModifiers::NONE));
    }
    last
}

#[test]
fn shows_every_kind_with_its_use() {
    let m = cluster();
    let mut v = listing();
    let s = screen(&m, &v);
    assert!(
        s.contains("2/2 nodes ready") && s.contains("1 running"),
        "{s}"
    );
    assert!(s.contains("0.2 CPU of 8"), "{s}");
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
        "shop web-1-1 n1 running ✓ 0 25% 64Mi 10.200.0.2 494"
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

/// The YAML of an object deleted while open shows no more, and keys go to
/// the table again.
#[test]
fn yaml_of_a_deleted_object_lets_keys_through() {
    let mut m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "y");
    assert!(screen(&m, &v).contains("node n1 — YAML"));
    m.changed(false, node("n1", 1, "ready", "active"), 0);
    typed(&mut v, &m, "j");
    assert!(v.yaml.is_none());
    typed(&mut v, &m, "?");
    assert!(v.help, "? was taken by the YAML gone");
}

#[test]
fn details_and_yaml_of_the_selected_object() {
    let m = cluster();
    // The list shows no details; Enter opens them on a panel of their
    // own, F3 goes back.
    let mut v = listing();
    assert!(!screen_of(&m, &v, 170).contains("allocatable"));
    typed(&mut v, &m, "\n");
    let s = screen_of(&m, &v, 170);
    assert!(s.contains("node n1") && s.contains("allocatable"), "{s}");
    assert!(s.contains("Menu › Nodes › node n1"), "{s}");
    typed(&mut v, &m, "«");
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
    assert!(screen(&m, &v).contains("Acting on the row selected"));
    typed(&mut v, &m, "x");
    assert!(!v.help);
}

#[test]
fn a_services_details_show_the_connections_seen() {
    let mut m = cluster();
    assert_eq!(m.service_namespaces(), BTreeSet::from(["shop".to_owned()]));
    let end = |kind: &str, name: &str| {
        Some(pb::ConnectionEnd {
            kind: kind.into(),
            name: name.into(),
        })
    };
    let edge = |from, to, port, via: &str, connections| pb::ConnectionEdge {
        from,
        to,
        protocol: "tcp".into(),
        port,
        via: via.into(),
        connections,
        clients: 1,
    };
    let mut v = listing();
    typed(&mut v, &m, "2\n");
    // Not asked yet: no section.
    assert!(!screen_of(&m, &v, 170).contains("connections seen"));
    m.connections.insert("shop".into(), Vec::new());
    let s = screen_of(&m, &v, 170);
    assert!(
        s.contains("connections seen") && s.contains("none in the last minute"),
        "{s}"
    );
    m.connections.insert(
        "shop".into(),
        vec![
            edge(
                end("outside", ""),
                end("service", "web"),
                80,
                "published",
                3,
            ),
            edge(end("service", "web"), end("service", "db"), 5432, "", 1),
            edge(end("service", "api"), end("service", "db"), 5432, "", 4),
        ],
    );
    let s = screen_of(&m, &v, 170);
    assert!(
        s.contains("← (outside): 80/tcp via published, 3 connections"),
        "{s}"
    );
    assert!(s.contains("→ db: 5432/tcp, 1 connection"), "{s}");
    assert!(!s.contains("api"), "another service's: {s}");
}

#[test]
fn enter_shows_the_tasks_of_a_node_or_service() {
    let m = cluster();
    let mut v = listing();
    // Enter opens the node, Enter again its tasks.
    typed(&mut v, &m, "↓\n");
    assert!(screen(&m, &v).contains("Menu › Nodes › node n2"));
    typed(&mut v, &m, "\n");
    assert_eq!(v.tab, Tab::Tasks);
    let s = screen(&m, &v);
    assert!(s.contains("web-2-1") && !s.contains("web-1-1"), "{s}");
    assert!(s.contains("on node n2"), "{s}");
    // Esc goes back a panel at a time: the node, then the nodes.
    typed(&mut v, &m, "\x1b");
    assert!(matches!(v.panel(), view::Panel::Detail { .. }));
    typed(&mut v, &m, "\x1b");
    assert_eq!(v.tab, Tab::Nodes);
    assert_eq!(v.selected[0], 1);
    assert!(!v.quit);
    typed(&mut v, &m, "q");
    assert!(v.quit);

    let mut v = listing();
    typed(&mut v, &m, "2\n\n");
    let s = screen(&m, &v);
    assert!(s.contains("web-1-1") && s.contains("web-2-1"), "{s}");
    assert!(s.contains("of service shop/web"), "{s}");
}

#[test]
fn a_filter_narrows_the_rows_as_it_is_typed() {
    let m = cluster();
    let mut v = listing();
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
    let mut v = listing();
    typed(&mut v, &m, "3");
    assert!(!screen(&m, &v).contains("web-1-0"));
    typed(&mut v, &m, "h");
    let s = screen(&m, &v);
    let (live, ended) = (s.find("web-1-1").unwrap(), s.find("web-1-0").unwrap());
    assert!(live < ended, "ended last: {s}");
}

#[test]
fn letters_with_ctrl_or_alt_do_nothing() {
    let m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "2jj");
    assert_eq!(ctrl(&mut v, &m, 'u'), None);
    let alt = KeyEvent::new(KeyCode::Char('u'), KeyModifiers::ALT);
    assert_eq!(v.key(&m, alt), None);
    assert!(matches!(
        typed(&mut v, &m, "u"),
        Some(Request::Suspend { .. })
    ));
    // Nor do they answer a question.
    let mut v = listing();
    typed(&mut v, &m, "2r");
    assert_eq!(ctrl(&mut v, &m, 'y'), None);
    assert!(matches!(
        typed(&mut v, &m, "y"),
        Some(Request::Rollback { .. })
    ));
}

#[test]
fn keys_act_on_the_selected_row() {
    let m = cluster();
    let mut v = listing();
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
    let mut v = listing();
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
    let mut v = listing();
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
    let mut v = listing();
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
fn logs_follow_on_a_panel_of_their_own() {
    let m = cluster();
    let mut v = listing();
    let logs = typed(&mut v, &m, "3l").unwrap();
    v.show_logs(&logs);
    let s = screen_sized(&m, &v, 130, 30);
    assert!(s.contains("Logs of shop/web-1-1"), "{s}");
    assert!(s.contains("Waiting for lines"));
    assert!(s.contains("following"));
    // Over the list, F3 going back to it.
    assert!(!s.contains("web-2-1"), "{s}");
    assert!(s.contains("Menu › Tasks › Logs of shop/web-1-1"), "{s}");

    let l = v.logs.as_mut().unwrap();
    // Lines come in pieces, with colours, tabs and carriage returns.
    l.push(
        false,
        b"GET / 200
GET /a",
    );
    l.push(
        false,
        b"bout 404
[31mred[0m	x
",
    );
    l.push(
        true,
        b"warning: slow
",
    );
    for i in 0..30 {
        l.push(
            false,
            format!(
                "line {i}
"
            )
            .as_bytes(),
        );
    }
    let s = screen_sized(&m, &v, 130, 30);
    assert!(s.contains("line 29"), "{s}");
    assert!(!s.contains("GET / 200"), "the oldest are above the pane");
    assert_eq!(
        v.logs
            .as_ref()
            .unwrap()
            .lines
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>(),
        [
            (false, "GET / 200".to_owned()),
            (false, "GET /about 404".to_owned()),
            (false, "red     x".to_owned()),
        ]
    );
    assert_eq!(
        v.logs.as_ref().unwrap().lines[3],
        (true, "warning: slow".into())
    );

    // Back a page: the same lines stay as more come; forward follows again.
    typed_code(&mut v, &m, KeyCode::PageUp);
    let s = screen_sized(&m, &v, 130, 30);
    assert!(s.contains("20 lines back"), "{s}");
    assert!(!s.contains("line 29"));
    v.logs.as_mut().unwrap().push(
        false,
        b"line 30
",
    );
    assert!(screen_sized(&m, &v, 130, 30).contains("21 lines back"));
    typed_code(&mut v, &m, KeyCode::PageDown);
    typed_code(&mut v, &m, KeyCode::PageDown);
    let s = screen_sized(&m, &v, 130, 30);
    assert!(s.contains("line 30") && s.contains("following"), "{s}");

    // z puts them over the table, whose keys then wait: nothing acts on
    // rows out of sight.
    typed(&mut v, &m, "z");
    let s = screen_sized(&m, &v, 130, 30);
    assert!(!s.contains("web-2-1"), "{s}");
    assert_eq!(typed(&mut v, &m, "jD"), None);
    assert!(v.prompt.is_none());
    assert_eq!(v.selected, listing().selected);
    typed(&mut v, &m, "z");

    v.logs.as_mut().unwrap().push(false, b"no newline");
    v.logs
        .as_mut()
        .unwrap()
        .end("No more: the task ended.".into());
    let s = screen_sized(&m, &v, 130, 30);
    assert!(
        s.contains("no newline") && s.contains("No more: the task ended."),
        "{s}"
    );

    // Esc closes them first, and the tab stays.
    typed(&mut v, &m, "");
    assert!(v.logs.is_none());
    assert_eq!(v.tab, Tab::Tasks);
    assert!(!screen_sized(&m, &v, 130, 30).contains("Logs of"));
}

fn typed_code(view: &mut View, model: &Model, code: KeyCode) -> Option<Request> {
    view.key(model, KeyEvent::new(code, KeyModifiers::NONE))
}

fn ctrl(view: &mut View, model: &Model, c: char) -> Option<Request> {
    view.key(
        model,
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL),
    )
}

#[test]
fn a_shell_opens_over_the_list_and_takes_the_keys() {
    let m = cluster();
    let mut v = listing();
    let exec = typed(&mut v, &m, "3x").unwrap();
    v.show_shell(&exec);
    let area = ratatui::layout::Rect::new(0, 0, 130, 30);
    let (rows, cols) = draw::shell_size(&v, area).unwrap();
    let sh = v.shell.as_mut().unwrap();
    sh.set_size(rows, cols);
    // A prompt, a coloured word, and a line drawn over by a carriage
    // return, as a terminal shows them.
    sh.output(b"# ls\r\n\x1b[31mred\x1b[0m file\r\nold line\rnew\r\n# ");
    let s = screen_sized(&m, &v, 130, 30);
    assert!(s.contains("Shell in shop/web-1-1"), "{s}");
    assert!(s.contains("red file"), "{s}");
    assert!(s.contains("new line"), "{s}");
    assert!(!s.contains("web-2-1"), "over the list: {s}");
    let pane = s.lines().position(|l| l.contains("╭ Shell in")).unwrap();
    // The pane's inside is the shell's screen.
    let inside = s.lines().nth(pane + 1).unwrap();
    assert!(inside.starts_with("│# ls"), "{inside}");
    assert_eq!(cols, 128);

    // Every key goes to the shell, Ctrl-C and q too.
    assert_eq!(typed(&mut v, &m, "q"), Some(Request::Input(b"q".to_vec())));
    assert_eq!(ctrl(&mut v, &m, 'c'), Some(Request::Input(vec![3])));
    assert!(!v.quit);
    assert_eq!(
        typed(&mut v, &m, "\n"),
        Some(Request::Input(b"\r".to_vec()))
    );
    assert_eq!(
        typed_code(&mut v, &m, KeyCode::Up),
        Some(Request::Input(b"\x1b[A".to_vec()))
    );
    // A full-screen program asks for application cursor keys.
    v.shell.as_mut().unwrap().output(b"\x1b[?1h");
    assert_eq!(
        typed_code(&mut v, &m, KeyCode::Up),
        Some(Request::Input(b"\x1bOA".to_vec()))
    );
    assert!(screen_sized(&m, &v, 130, 30).contains("The shell has the keys"));

    // Ctrl-] gives the keys back to top: they scroll the shell, the
    // list under it stays.
    assert_eq!(ctrl(&mut v, &m, ']'), None);
    assert!(!v.typing());
    assert_eq!(typed(&mut v, &m, "j"), None);
    assert_eq!(v.selected[2], 0);
    assert_eq!(*v.panel(), view::Panel::Pane);
    // As crossterm reads Ctrl-] on a terminal: Ctrl-5.
    assert_eq!(ctrl(&mut v, &m, '5'), None);
    assert!(v.typing());
    ctrl(&mut v, &m, ']');

    // It ends: the keys stay with top, and Ctrl-] cannot go back.
    let sh = v.shell.as_mut().unwrap();
    sh.ended = Some("Ended with 0.".into());
    sh.focus = false;
    assert!(screen_sized(&m, &v, 130, 30).contains("Ended with 0."));
    ctrl(&mut v, &m, ']');
    assert!(!v.typing());
    typed(&mut v, &m, "\x1b");
    assert!(v.shell.is_none());
    assert_eq!(v.tab, Tab::Tasks);
    assert!(matches!(v.panel(), view::Panel::List { .. }));
    assert!(draw::shell_size(&v, area).is_none());

    // Logs and a shell share the pane: one closes the other.
    v.show_shell(&exec);
    let logs = Request::Logs {
        namespace: "shop".into(),
        name: "web-1-1".into(),
        container: None,
    };
    v.show_logs(&logs);
    assert!(v.shell.is_none() && v.logs.is_some());
    v.show_shell(&exec);
    assert!(v.logs.is_none() && v.shell.is_some());
}

#[test]
fn global_and_autoscaled_services_are_not_scaled_here() {
    let mut m = cluster();
    let mut global = service(20, 1, 1);
    global.spec["mode"] = "global".into();
    m.changed(true, global, 0);
    let mut v = listing();
    assert_eq!(typed(&mut v, &m, "2s"), None);
    assert!(screen(&m, &v).contains("global or autoscaled"));
    // The next key clears it, and the service's letters show again.
    typed(&mut v, &m, "k");
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
    // Gone before it was ever seen: no news either.
    let mut gone = service(25, 0, 0);
    gone.metadata.name = "never-seen".into();
    m.changed(false, gone, at);
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
    let mut v = listing();
    typed(&mut v, &m, "7");
    let s = screen(&m, &v);
    assert_eq!(line(&s, "drain"), "09:30:05 node n1: active → drain");
}

#[test]
fn a_host_lacking_what_it_requires_is_shown_and_logged() {
    let mut m = cluster();
    let at = legion_api::clock::parse("2026-10-30T09:30:05Z").unwrap();
    let mut lacking = node("n2", 20, "ready", "active");
    lacking.status["lacks"] = serde_json::json!([
        { "id": "pf.anchors", "need": "required", "state": "unmet", "detail": "no hooks" },
        { "id": "forwarding", "need": "advised", "state": "unmet", "detail": "off" }
    ]);
    m.changed(true, lacking.clone(), at);
    let v = listing();
    let s = screen(&m, &v);
    assert!(s.contains("ready, lacks pf.anchors"), "{s}");
    assert!(!s.contains("lacks pf.anchors forwarding"), "{s}");
    let mut put_right = node("n2", 21, "ready", "active");
    put_right.status["lacks"] = serde_json::json!([
        { "id": "forwarding", "need": "advised", "state": "unmet", "detail": "off" }
    ]);
    m.changed(true, put_right, at);
    let log: Vec<&str> = m.log.iter().rev().map(|(_, s)| s.as_str()).collect();
    assert_eq!(
        log,
        [
            "node n2: the host lacks pf.anchors: no new tasks",
            "node n2: the host has what it requires again",
        ]
    );
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
    let v = listing();
    let s = screen(&m, &v);
    assert!(s.contains("do not let you read node"), "{s}");
    assert!(
        s.contains("namespace shop") && !s.contains("nodes ready"),
        "{s}"
    );
    assert_eq!(m.of(Kind::Service).count(), 1);
}

/// A kind hidden at the listing shows once the watch brings one: the
/// caller's role bindings let it read that kind now.
#[test]
fn a_kind_shows_once_the_watch_brings_one() {
    let mut m = Model::new(Some("shop".into()));
    m.listed(vec![service(3, 1, 1)], BTreeSet::from([Kind::Node]));
    let v = listing();
    assert!(screen(&m, &v).contains("do not let you read node"));
    m.changed(true, node("n1", 4, "ready", "active"), 0);
    let s = screen(&m, &v);
    assert!(!s.contains("do not let you read node"), "{s}");
    assert!(!m.hidden.contains(&Kind::Node));
}

#[test]
fn a_problem_shows_until_listed_again() {
    let mut m = cluster();
    m.problem = Some("connection refused".into());
    let v = listing();
    assert!(screen(&m, &v).contains("not current: connection refused"));
    m.listed(Vec::new(), BTreeSet::new());
    assert!(!screen(&m, &v).contains("not current"));
}

#[test]
fn once_prints_plain_tables() {
    let m = cluster();
    let v = View {
        tab: Tab::Nodes,
        ..listing()
    };
    let text = draw::plain(&m, &v);
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines[0].starts_with("NAME  STATE"), "{text}");
    assert!(lines[1].starts_with("n1    ● ready"), "{text}");
    assert!(draw::plain_summary(&m).starts_with("● 2/2 nodes ready"));
}

#[test]
fn the_header_graphs_show_small_loads() {
    let mut m = cluster();
    // 8 CPUs and 16Gi in all; the tasks used 0.1, 0.5, then 0.25 CPU.
    m.history.clear();
    for cpu in [10.0, 50.0, 25.0] {
        m.history.push_back((cpu, 64 << 20));
    }
    let s = screen_of(&m, &listing(), 170);
    let cpu = s.lines().find(|l| l.contains("cpu ")).unwrap();
    let mem = s.lines().find(|l| l.contains("mem ")).unwrap();
    // The share of what the nodes hold, its bar, and the amounts.
    assert!(cpu.contains("cpu █░░░░░░░░░   3% 0.2 CPU of 8"), "{cpu}");
    assert!(mem.contains("mem █░░░░░░░░░  <1% 64Mi of 16Gi"), "{mem}");
    // The graph is scaled to its peak: a fifth, all, a half of it.
    assert!(cpu.contains("▁█▄"), "{cpu}");
    assert!(cpu.contains("peak 0.5"), "{cpu}");
    // Memory is held: drawn against all the nodes hold, steady use is a
    // low line, not a full one.
    assert!(mem.contains("▁▁▁") && mem.contains("peak 64Mi"), "{mem}");

    // An idle cluster's noise stays near the floor, not at full height.
    m.history.clear();
    for cpu in [1.0, 2.0, 1.0] {
        m.history.push_back((cpu, 0));
    }
    let s = screen_of(&m, &listing(), 170);
    let cpu = s.lines().find(|l| l.contains("cpu ")).unwrap();
    let graph = cpu.split("of 8").nth(1).unwrap();
    assert!(!graph.contains('█') && graph.contains('▁'), "{cpu}");
}

#[test]
fn sizes() {
    assert_eq!(draw::bytes(512), "512B");
    assert_eq!(draw::bytes(64 << 20), "64Mi");
    assert_eq!(draw::bytes(3 << 29), "1.5Gi");
    assert_eq!(draw::trim(4.0), "4");
}

#[test]
fn renders_max_scale_cluster_of_256_nodes_and_20000_tasks() {
    let mut objects = Vec::with_capacity(256 + 100 + 20_000);
    for i in 0..256 {
        objects.push(node(&format!("n-{i:03}"), 1, "ready", "active"));
    }
    for i in 0..100 {
        let mut s = service(1, 200, 200);
        s.metadata.name = format!("svc-{i:03}");
        objects.push(s);
    }
    let mut tasks_usage = Vec::with_capacity(20_000);
    for i in 0..20_000 {
        let node_name = format!("n-{:03}", i % 256);
        let svc_name = format!("svc-{:03}", i % 100);
        let task_name = format!("t-{i:05}");
        let mut t = object(
            Kind::Task,
            "shop",
            &task_name,
            1,
            serde_json::json!({
                "service": svc_name,
                "serviceUid": "1",
                "serviceGeneration": 1,
                "node": node_name,
                "pod": { "containers": [{ "name": "web", "image": "nginx:1" }] }
            }),
            serde_json::json!({ "state": "running", "ready": true, "ip": "10.200.0.2", "jid": i }),
        );
        t.metadata.labels.insert(LABEL_SERVICE.into(), svc_name);
        objects.push(t);
        tasks_usage.push(pb::TaskUsage {
            namespace: "shop".into(),
            task: task_name,
            cpu_percent: 1.0,
            memory_bytes: 1 << 20,
        });
    }
    let mut m = Model::new(None);
    m.listed(objects, BTreeSet::new());
    m.usage(pb::UsageResponse {
        nodes: vec![pb::NodeUsage {
            node: "n-000".into(),
            tasks: tasks_usage,
            ..Default::default()
        }],
    });

    let mut v = listing();
    let s = screen(&m, &v);
    assert!(
        s.contains("256/256 nodes ready") && s.contains("20000 running"),
        "{s}"
    );
    // Node n-000 gets 79 tasks (20000 = 256 * 78 + 32).
    assert!(line(&s, "n-000").starts_with("n-000 ● ready active worker 79 0.79/4 79Mi/8Gi"));

    typed(&mut v, &m, "2");
    let s = screen(&m, &v);
    assert!(line(&s, "svc-000").starts_with("shop svc-000 replicated 200/200 200% 200Mi nginx:1"));

    typed(&mut v, &m, "3G");
    assert!(screen_of(&m, &v, 170).contains("t-19999"));
    typed(&mut v, &m, "\n");
    let s = screen_of(&m, &v, 170);
    assert!(s.contains("› task shop/t-19999"), "{s}");
}

#[test]
fn failed_updates_are_highlighted() {
    for (state, warn) in [
        ("updating", false),
        ("completed", false),
        ("paused", true),
        ("rolling-back", true),
        ("rollback-paused", true),
        ("rolled-back", true),
    ] {
        let mut m = cluster();
        let mut s = service(10, 1, 2);
        s.status["update"] = serde_json::json!({
            "state": state, "generation": 1,
            "previous": { "pod": { "containers": [{ "name": "web", "image": "nginx:1" }] } },
        });
        m.changed(true, s, 0);
        let mut v = listing();
        typed(&mut v, &m, "2");
        let mut t = Terminal::new(TestBackend::new(130, 20)).unwrap();
        t.draw(|f| draw::render(f, &m, &v)).unwrap();
        let buf = t.backend().buffer();
        let at = (0..buf.area.height)
            .flat_map(|y| (0..buf.area.width).map(move |x| (x, y)))
            .find(|&(x, y)| {
                (x..(x + state.len() as u16).min(buf.area.width))
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
                    == state
            })
            .unwrap_or_else(|| panic!("no {state}"));
        assert_eq!(buf[at].fg == draw::tone(draw::WARN), warn, "{state}");
    }
}

#[test]
fn the_selection_follows_rows_that_went() {
    let mut m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "3↓");
    assert_eq!(v.selected[2], 1);
    let gone = m
        .of(Kind::Task)
        .find(|t| t.metadata.name == "web-2-1")
        .unwrap()
        .clone();
    m.changed(false, gone, 0);
    // The row left is selected, and a key acts on it at once.
    let mut opened = View { ..listing() };
    typed(&mut opened, &m, "3↓\n");
    let s = screen_of(&m, &opened, 170);
    assert!(s.contains("› task shop/web-1-1"), "{s}");
    typed(&mut v, &m, "↓");
    assert_eq!(v.selected[2], 0);
}

#[test]
fn panels_open_over_each_other_and_f3_goes_back() {
    let m = cluster();
    // It starts on the menu; F3 there leaves.
    let mut v = View::default();
    assert_eq!(*v.panel(), view::Panel::Menu);
    typed(&mut v, &m, "«");
    assert!(v.quit);

    let mut v = View::default();
    // Down and Enter open the second option.
    typed(&mut v, &m, "↓\n");
    assert_eq!(v.tab, Tab::Workloads);
    assert!(screen(&m, &v).contains("Menu › Workloads"));
    // The service, its tasks, a task: four panels deep.
    typed(&mut v, &m, "\n\n\n");
    assert_eq!(v.stack.len(), 5);
    let s = screen_of(&m, &v, 200);
    assert!(
        s.contains(
            "Menu › Workloads › service shop/web › Tasks of service shop/web › task shop/web-1-1"
        ),
        "{s}"
    );
    // F3 goes back one at a time, to where it was.
    typed(&mut v, &m, "««");
    assert_eq!(v.tab, Tab::Workloads);
    assert!(matches!(v.panel(), view::Panel::Detail { .. }));
    typed(&mut v, &m, "««");
    assert_eq!(*v.panel(), view::Panel::Menu);
    assert!(!v.quit);
    // A number goes straight to a list from anywhere, all else closed.
    typed(&mut v, &m, "2\n\n4");
    assert_eq!(v.stack.len(), 2);
    assert_eq!(v.tab, Tab::Storage);
    // The menu's options: 0 the graphs, then the lists.
    assert_eq!(v.option, 4);
    // Rows with nothing to open stay.
    typed(&mut v, &m, "7\n");
    assert_eq!(v.stack.len(), 2);
}

#[test]
fn details_scroll_but_not_past_their_end() {
    let m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "\n");
    // Three lines of details on the smallest screen.
    let small = |v: &View| screen_sized(&m, v, 100, 12);
    let top = small(&v);
    assert!(top.contains("version 1"), "{top}");
    typed(&mut v, &m, "jj");
    assert!(!small(&v).contains("version 1"));
    typed(&mut v, &m, "G");
    let end = small(&v);
    assert!(end.contains("availability"), "the last line shows: {end}");
    typed(&mut v, &m, "g");
    assert_eq!(small(&v), top);
}

#[test]
fn init_containers_have_logs_but_no_shell() {
    let mut m = cluster();
    let mut t = task("web-1-1", 30, "n1", "running", true);
    t.spec["pod"]["initContainers"] = serde_json::json!([{ "name": "seed", "image": "seed:1" }]);
    m.changed(true, t, 0);
    let mut v = listing();
    assert_eq!(typed(&mut v, &m, "3l"), None);
    assert_eq!(
        line(&screen(&m, &v), "which container"),
        "Logs of which container? 1 web 2 seed Esc cancels"
    );
    assert_eq!(
        typed(&mut v, &m, "2"),
        Some(Request::Logs {
            namespace: "shop".into(),
            name: "web-1-1".into(),
            container: Some("seed".into())
        })
    );
    assert_eq!(
        typed(&mut v, &m, "x"),
        Some(Request::Exec {
            namespace: "shop".into(),
            task: "web-1-1".into(),
            container: None
        })
    );
}

#[test]
fn a_wide_character_at_the_shells_edge_leaves_the_border() {
    let m = cluster();
    let mut v = listing();
    let exec = typed(&mut v, &m, "3x").unwrap();
    v.show_shell(&exec);
    let area = ratatui::layout::Rect::new(0, 0, 130, 30);
    let (rows, cols) = draw::shell_size(&v, area).unwrap();
    let sh = v.shell.as_mut().unwrap();
    // Wider than the pane, as just after the terminal shrank.
    sh.set_size(rows, cols + 2);
    sh.output(format!("\x1b[1;{cols}H\u{4e2d}x").as_bytes());
    let mut t = Terminal::new(TestBackend::new(130, 30)).unwrap();
    t.draw(|f| draw::render(f, &m, &v)).unwrap();
    let buf = t.backend().buffer();
    let pane = (0..30)
        .find(|&y| {
            (0..130)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
                .contains("╭ Shell in")
        })
        .unwrap();
    // The pane's last column, at x = cols, holds no half of it; the border
    // after it stays.
    assert_eq!(buf[(cols, pane + 1)].symbol(), " ");
    assert_eq!(buf[(129, pane + 1)].symbol(), "│");
}

#[test]
fn a_service_is_stopped_when_asked_and_started_at_once() {
    let mut m = cluster();
    let mut v = listing();
    // Stopping is asked first.
    assert_eq!(typed(&mut v, &m, "2o"), None);
    assert!(screen(&m, &v).contains("Stop service shop/web?"));
    assert_eq!(
        typed(&mut v, &m, "y"),
        Some(Request::Stop {
            namespace: "shop".into(),
            service: "web".into(),
            stop: true
        })
    );
    // Stopped, the row says so, and starting it again is not asked.
    let mut stopped = service(9, 0, 0);
    stopped.spec["stopped"] = true.into();
    m.changed(true, stopped, 0);
    assert!(screen(&m, &v).contains("stopped"));
    assert_eq!(
        typed(&mut v, &m, "o"),
        Some(Request::Stop {
            namespace: "shop".into(),
            service: "web".into(),
            stop: false
        })
    );
}

#[test]
fn a_service_is_paused_when_asked_and_resumed_at_once() {
    let mut m = cluster();
    let mut v = listing();
    assert_eq!(typed(&mut v, &m, "2P"), None);
    assert!(screen(&m, &v).contains("Pause service shop/web?"));
    assert_eq!(
        typed(&mut v, &m, "y"),
        Some(Request::Pause {
            namespace: "shop".into(),
            service: "web".into(),
            pause: true
        })
    );
    let mut paused = service(9, 3, 3);
    paused.spec["paused"] = true.into();
    m.changed(true, paused, 0);
    assert!(screen(&m, &v).contains("paused"));
    assert_eq!(
        typed(&mut v, &m, "P"),
        Some(Request::Pause {
            namespace: "shop".into(),
            service: "web".into(),
            pause: false
        })
    );
}

#[test]
fn restart_run_now_events_and_alerts() {
    let m = cluster();
    let mut v = listing();
    // A restart is asked first.
    assert_eq!(typed(&mut v, &m, "2R"), None);
    assert!(screen(&m, &v).contains("Restart service shop/web?"));
    assert_eq!(
        typed(&mut v, &m, "y"),
        Some(Request::Restart {
            namespace: "shop".into(),
            service: "web".into()
        })
    );
    let events = Request::Events {
        kind: Kind::Service,
        namespace: "shop".into(),
        name: "web".into(),
    };
    assert_eq!(typed(&mut v, &m, "e"), Some(events.clone()));
    assert_eq!(
        typed(&mut v, &m, "jn"),
        Some(Request::RunNow {
            namespace: "shop".into(),
            cron_job: "report".into()
        })
    );
    assert_eq!(typed(&mut v, &m, "A"), Some(Request::Alerts));
    // The pane says what it follows.
    v.show_logs(&events);
    assert!(screen(&m, &v).contains("Events of service shop/web"));
    v.show_logs(&Request::Alerts);
    assert!(screen(&m, &v).contains("Alerts"));
}

#[test]
fn builds_and_image_policies_are_listed() {
    let mut m = cluster();
    m.changed(
        true,
        object(
            Kind::Build,
            "shop",
            "web-k3",
            20,
            serde_json::json!({ "image": "registry.example/web:2", "source": { "containerfile": "FROM x" } }),
            serde_json::json!({ "phase": "Succeeded", "node": "n1" }),
        ),
        0,
    );
    m.changed(
        true,
        object(
            Kind::ImagePolicy,
            "",
            "signed",
            21,
            serde_json::json!({ "images": ["registry.example/*"], "keys": ["k1"] }),
            serde_json::Value::Null,
        ),
        0,
    );
    let mut v = listing();
    typed(&mut v, &m, "2");
    let s = screen(&m, &v);
    let build = line(&s, "web-k3");
    assert!(
        build.contains("build") && build.contains("succeeded") && build.contains("web:2"),
        "{build}"
    );
    typed(&mut v, &m, "6");
    let s = screen(&m, &v);
    assert!(
        line(&s, "signed").contains("enforce: 1 image patterns, 1 keys"),
        "{s}"
    );
}

#[test]
fn a_poll_that_fails_says_so_until_it_reads_again() {
    let mut m = cluster();
    let v = listing();
    m.polled = Some("the cluster's status: unavailable".into());
    assert!(screen(&m, &v).contains("not current: the cluster's status"));
    m.polled = None;
    assert!(!screen(&m, &v).contains("not current"));
}

#[test]
fn braille_graphs_and_rgb_heat_gradients() {
    let lines = draw::braille_lines(
        &[1.0, 2.0, 4.0, 8.0],
        8.0,
        4,
        2,
        &draw::HEAT,
        draw::Scale::Peak,
    );
    assert_eq!(lines.len(), 2);
    let rendered: String = lines
        .iter()
        .map(|l| l.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered
            .chars()
            .any(|c| ('\u{2801}'..='\u{28ff}').contains(&c)),
        "expected active Unicode braille dots: {rendered:?}"
    );
    // Green when idle, red when busy, as btop's meters.
    assert_eq!(
        draw::heat_rgb(0.0),
        ratatui::style::Color::Rgb(80, 200, 120)
    );
    assert_eq!(draw::heat_rgb(1.0), ratatui::style::Color::Rgb(240, 85, 85));
    // Shaded by height: the bottom row in the first colour, the top in
    // the last, however busy.
    let lines = draw::braille_lines(&[8.0; 8], 8.0, 4, 4, &draw::HEAT, draw::Scale::Peak);
    let colour = |row: usize| lines[row].spans[0].style.fg;
    assert_ne!(colour(0), colour(3));
}

#[test]
fn what_is_held_is_graphed_against_the_whole() {
    let blank = |l: &ratatui::text::Line| l.to_string().chars().all(|c| c == '\u{2800}');
    // A steady 11 % of memory: drawn to the top against its peak, a
    // tenth of the way up against the whole.
    let steady = [11.0; 8];
    let by_peak = draw::braille_lines(&steady, 100.0, 4, 4, &draw::HEAT, draw::Scale::Peak);
    assert!(!blank(&by_peak[0]), "the top row filled");
    let whole = draw::braille_lines(&steady, 100.0, 4, 4, &draw::HEAT, draw::Scale::Whole);
    assert!(blank(&whole[0]) && blank(&whole[1]) && blank(&whole[2]));
    assert!(!blank(&whole[3]), "the bottom row holds it");
}

#[test]
fn the_menu_graphs_zfs_and_network_and_storage_their_space() {
    let mut m = cluster();
    m.usage(pb::UsageResponse {
        nodes: vec![pb::NodeUsage {
            node: "n1".into(),
            zfs_used_bytes: 12 << 30,
            zfs_avail_bytes: 36 << 30,
            net_rx_bps: 2 << 20,
            net_tx_bps: 512 << 10,
            volumes: vec![pb::VolumeTelemetry {
                namespace: "shop".into(),
                volume: "data".into(),
                used_bytes: 256 << 20,
                avail_bytes: 768 << 20,
            }],
            tasks: vec![pb::TaskUsage {
                namespace: "shop".into(),
                task: "web-1-1".into(),
                cpu_percent: 25.0,
                memory_bytes: 64 << 20,
            }],
            ..Default::default()
        }],
    });
    let mut v = listing();
    // Storage shows each Volume's ZFS space used of its size when reported.
    typed(&mut v, &m, "4");
    assert_eq!(
        line(&screen(&m, &v), "data"),
        "shop data zfs 256Mi/1Gi n1 web-1-1,web-2-1"
    );

    // The menu, room allowing, graphs CPU, memory, ZFS and network.
    let v = View::default();
    let s = screen_sized(&m, &v, 150, 32);
    assert!(s.contains("12Gi/48Gi"), "{s}");
    assert!(s.contains("▼2Mi/s ▲512Ki/s"), "{s}");
    assert!(
        line(&s, "Storage").contains("4 Storage Volumes: their space, node and replica 1"),
        "{s}"
    );
    // Short, the two lines of graphs only.
    let s = screen_sized(&m, &v, 150, 20);
    assert!(!s.contains("12Gi/48Gi") && s.contains("CPU of 8"), "{s}");
}

#[test]
fn workload_tree_sorting_poll_stepping_and_service_logs() {
    let m = cluster();
    let mut v = listing();

    // Expand workload -> task -> jail tree (`t`) on Workloads tab.
    typed(&mut v, &m, "2t");
    assert!(v.tree);
    let s = screen(&m, &v);
    assert!(s.contains("├─ web-1-1") && s.contains("jid 494"), "{s}");
    assert!(s.contains("└─ web-2-1"), "{s}");

    // `L` on Service requests multiplexed logs across all running pods.
    typed(&mut v, &m, "t");
    assert!(!v.tree);
    let req = typed(&mut v, &m, "L");
    assert_eq!(
        req,
        Some(Request::ServiceLogs {
            namespace: "shop".into(),
            kind: Kind::Service,
            name: "web".into(),
        })
    );
    v.show_logs(req.as_ref().unwrap());
    v.logs
        .as_mut()
        .unwrap()
        .push_prefixed("web-1-1", false, b"ready on 80\n");
    let s = screen_sized(&m, &v, 130, 30);
    assert!(
        s.contains("all tasks of service shop/web") && s.contains("[web-1-1] ready on 80"),
        "{s}"
    );
    typed(&mut v, &m, "\x1b");

    // Sort tasks by CPU ascending (`>S`) so idle `web-2-1` precedes `web-1-1`.
    typed(&mut v, &m, "3>S");
    assert_eq!(v.sort, SortCol::Cpu);
    assert!(!v.sort_desc);
    let s = screen(&m, &v);
    assert!(
        s.find("web-2-1").unwrap() < s.find("web-1-1").unwrap(),
        "{s}"
    );

    // Poll interval steps faster (`+`) and slower (`-`).
    assert_eq!(v.poll_ms, 5000);
    typed(&mut v, &m, "++");
    assert_eq!(v.poll_ms, 1000);
    typed(&mut v, &m, "---");
    assert_eq!(v.poll_ms, 10000);
}

fn run(args: &[&str]) -> Option<Request> {
    Some(Request::Run {
        args: args.iter().map(|a| (*a).to_owned()).collect(),
    })
}

#[test]
fn a_command_line_runs_legion_with_its_output_below() {
    let m = cluster();
    let mut v = listing();
    assert_eq!(typed(&mut v, &m, ":diagnose\n"), run(&["diagnose"]));
    // `legion` before it is taken as said; quotes keep words together.
    assert_eq!(
        typed(
            &mut v,
            &m,
            ":legion volume snapshot data 'before upgrade' -n shop\n"
        ),
        run(&["volume", "snapshot", "data", "before upgrade", "-n", "shop"])
    );
    // Up recalls what ran, the latest first; Down comes back.
    typed(&mut v, &m, ":");
    typed_code(&mut v, &m, KeyCode::Up);
    typed_code(&mut v, &m, KeyCode::Up);
    typed_code(&mut v, &m, KeyCode::Down);
    assert_eq!(
        typed(&mut v, &m, "\n"),
        run(&["volume", "snapshot", "data", "before upgrade", "-n", "shop"])
    );
    // What needs the terminal, or a host, is not run.
    assert_eq!(typed(&mut v, &m, ":exec web-1-1 -- sh\n"), None);
    assert!(
        v.notice.as_deref().unwrap().contains("x on a task"),
        "{:?}",
        v.notice
    );
    assert_eq!(typed(&mut v, &m, ":daemon\n"), None);
    assert!(v.notice.as_deref().unwrap().contains("acts on the host"));
    assert_eq!(typed(&mut v, &m, ":get 'service\n"), None);
    assert!(v.notice.as_deref().unwrap().contains("not closed"));
    // The output shows below, titled by the command.
    v.show_logs(&run(&["diagnose"]).unwrap());
    assert!(screen(&m, &v).contains("legion diagnose"));
}

#[test]
fn command_lines_split_as_a_shell_splits_them() {
    let split = |s: &str| view::split_words(s);
    assert_eq!(split("  a  b\tc ").unwrap(), ["a", "b", "c"]);
    assert_eq!(
        split(r#"a "b c" 'd e' f\ g"#).unwrap(),
        ["a", "b c", "d e", "f g"]
    );
    assert_eq!(
        split(r#""say \"hi\"" x''y"#).unwrap(),
        [r#"say "hi""#, "xy"]
    );
    assert_eq!(split("''").unwrap(), [""]);
    assert!(split("'open").is_err());
    assert!(split("\"open").is_err());
    assert!(split("end\\").is_err());
    assert!(view::refused("top").is_some());
    assert!(view::refused("audit").is_none());
}

#[test]
fn keys_run_the_cluster_commands_for_the_namespace_shown() {
    let m = cluster();
    let mut v = listing();
    // On a node, cluster-scoped: the default namespace.
    assert_eq!(typed(&mut v, &m, "H"), run(&["diagnose"]));
    assert_eq!(typed(&mut v, &m, "W"), run(&["reach", "-n", "default"]));
    // On a workload of shop: shop's.
    typed(&mut v, &m, "2");
    assert_eq!(typed(&mut v, &m, "C"), run(&["connections", "-n", "shop"]));
    assert_eq!(typed(&mut v, &m, "T"), run(&["topology", "-n", "shop"]));
    assert_eq!(typed(&mut v, &m, "K"), run(&["usage", "-n", "shop"]));
    assert_eq!(typed(&mut v, &m, "U"), run(&["audit", "--since", "24h"]));
    // legion top -n gives the namespace when the row has none.
    let mut only = Model::new(Some("team-a".into()));
    only.listed(vec![node("n1", 1, "ready", "active")], BTreeSet::new());
    let mut v = listing();
    assert_eq!(typed(&mut v, &only, "W"), run(&["reach", "-n", "team-a"]));
}

#[test]
fn row_keys_edit_and_reach_what_the_console_s_pages_do() {
    let mut m = cluster();
    m.changed(
        true,
        object(
            Kind::Build,
            "shop",
            "web-k3",
            20,
            serde_json::json!({ "image": "registry.example/web:2" }),
            serde_json::json!({ "phase": "Running" }),
        ),
        0,
    );
    let mut v = listing();
    assert_eq!(
        typed(&mut v, &m, "I"),
        run(&["image", "ls", "--node", "n1"])
    );
    assert_eq!(
        typed(&mut v, &m, "E"),
        Some(Request::Edit {
            key: (Kind::Node, String::new(), "n1".into())
        })
    );
    typed(&mut v, &m, "4");
    assert_eq!(
        typed(&mut v, &m, "N"),
        run(&["volume", "snapshots", "data", "-n", "shop"])
    );
    assert_eq!(
        typed(&mut v, &m, "B/tmp/data.zst\n"),
        run(&[
            "volume",
            "backup",
            "data",
            "-o",
            "/tmp/data.zst",
            "-n",
            "shop"
        ])
    );
    assert_eq!(typed(&mut v, &m, "B\n"), None);
    assert!(v.notice.as_deref().unwrap().contains("no file"));
    typed(&mut v, &m, "3");
    assert_eq!(
        typed(&mut v, &m, "f"),
        run(&["files", "web-1-1", "/", "-n", "shop"])
    );
    typed(&mut v, &m, "2/web-k3\n");
    assert_eq!(
        typed(&mut v, &m, "l"),
        run(&["build-logs", "web-k3", "-n", "shop", "--follow"])
    );
}

#[test]
fn an_edit_applies_the_one_object_over_the_version_read() {
    let key = (Kind::Service, "shop".to_owned(), "web".to_owned());
    let manifest = |name: &str| {
        legion_api::manifest::yaml(&Object::new(
            Kind::Service,
            "shop",
            name,
            serde_json::json!({ "replicas": 3 }),
        ))
    };
    let r = super::edited(&manifest("web"), &key, 7).unwrap();
    assert_eq!(r.expected_version, Some(7));
    assert_eq!(r.object.unwrap().name, "web");
    let renamed = super::edited(&manifest("web2"), &key, 7).unwrap_err();
    assert!(renamed.contains("legion apply"), "{renamed}");
    let two = format!("{}---\n{}", manifest("web"), manifest("api"));
    assert!(
        super::edited(&two, &key, 7)
            .unwrap_err()
            .contains("2 objects")
    );
    assert!(super::edited("kind: [", &key, 7).is_err());
}

#[test]
fn the_keys_fit_a_short_wide_screen_in_two_columns() {
    let m = cluster();
    let v = View {
        help: true,
        ..listing()
    };
    let s = screen_sized(&m, &v, 160, 40);
    assert!(
        s.contains("Panels") && s.contains("diagnose every node"),
        "{s}"
    );
    assert!(s.contains("On Nodes now"), "{s}");
}

#[test]
fn the_activity_tab_lists_long_operations_newest_first() {
    use legion_api::activity::{Operation, OperationKind, OperationState, Stage};
    use legion_api::audit::Target;
    let mut m = cluster();
    let volume = Target {
        kind: "Volume".into(),
        namespace: "shop".into(),
        name: "data".into(),
    };
    let mut moving = Operation::new(
        OperationKind::Move,
        volume,
        "move Volume shop/data from n1 to n2".into(),
        2_000_000,
        Stage::Preparing,
    );
    moving.enter(2_001_000, Stage::FullCopy);
    moving.bytes = 3 * 1024 * 1024;
    let mut drain = Operation::new(
        OperationKind::Drain,
        Target {
            kind: "Node".into(),
            namespace: String::new(),
            name: "n1".into(),
        },
        "drain node n1".into(),
        1_000_000,
        Stage::Draining,
    );
    drain.end(
        3_000_000,
        OperationState::Cancelled,
        "the node was made active again",
    );
    for op in [drain, moving] {
        m.activity.insert(op.id.clone(), op);
    }
    let v = View {
        tab: Tab::Activity,
        ..listing()
    };
    let text = draw::plain(&m, &v);
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines[0].starts_with("BEGAN"), "{text}");
    assert!(
        lines[1].contains("move Volume shop/data from n1 to n2"),
        "{text}"
    );
    assert!(
        lines[1].contains("full-copy") && lines[1].contains("3.0 MiB"),
        "{text}"
    );
    assert!(lines[2].contains("cancelled"), "{text}");
    assert_eq!(
        super::view::tab_count(&m, Tab::Activity),
        1,
        "one under way"
    );
    // Its row leads to what it is done to.
    let rows = super::view::rows(&m, &v);
    assert_eq!(
        rows[0].key,
        Some((Kind::Volume, "shop".to_owned(), "data".to_owned()))
    );
}

/// What `legion top` checks first, as the CLI does: a drain, a move, a
/// node's removal and a Service's deletion; nothing else.
#[test]
fn disruptions_are_checked_first() {
    use legion_api::dryrun::Change;
    let checked = |r: Request| super::disruption(&r);
    assert_eq!(
        checked(Request::Availability {
            node: "n2".into(),
            to: "drain"
        }),
        Some(Change::Drain { node: "n2".into() })
    );
    assert_eq!(
        checked(Request::Availability {
            node: "n2".into(),
            to: "pause"
        }),
        None
    );
    assert_eq!(
        checked(Request::Move {
            namespace: String::new(),
            volume: "data".into(),
            node: "n3".into()
        }),
        Some(Change::MoveVolume {
            namespace: "default".into(),
            volume: "data".into(),
            node: "n3".into()
        })
    );
    assert_eq!(
        checked(Request::Delete {
            kind: Kind::Node,
            namespace: String::new(),
            name: "n2".into()
        }),
        Some(Change::RemoveNode { node: "n2".into() })
    );
    assert_eq!(
        checked(Request::Delete {
            kind: Kind::Service,
            namespace: "shop".into(),
            name: "web".into()
        }),
        Some(Change::DeleteService {
            namespace: "shop".into(),
            service: "web".into()
        })
    );
    assert_eq!(
        checked(Request::Delete {
            kind: Kind::Config,
            namespace: "shop".into(),
            name: "web".into()
        }),
        None
    );
}

/// A node with its host and two pools, one degraded with errors partly
/// taken as seen, and its agent's load.
fn host_and_pools() -> Model {
    let mut m = cluster();
    let mut n1 = node("n1", 30, "ready", "active");
    n1.spec["poolErrorsSeen"] = serde_json::json!({ "tank": 1 });
    n1.spec["scrubEveryDays"] = serde_json::json!(14);
    let now_ms = legion_api::clock::now() * 1000;
    n1.status["host"] = serde_json::json!({
        "release": "15.1-RELEASE-p2", "kernel": "15.1-RELEASE-p1", "boot": "UEFI",
        "bootedMs": now_ms - 3 * 86_400_000,
    });
    n1.status["pools"] = serde_json::json!([
        { "name": "zroot", "state": "ONLINE", "sizeBytes": 100u64 << 30, "allocBytes": 34u64 << 30,
          "scan": { "function": "scrub", "state": "finished", "endMs": now_ms - 86_400_000 } },
        { "name": "tank", "state": "DEGRADED", "sizeBytes": 10u64 << 30, "allocBytes": 5u64 << 30,
          "checksumErrors": 4, "scan": { "function": "resilver", "state": "scanning" } },
    ]);
    m.changed(true, n1, 0);
    m.usage(pb::UsageResponse {
        nodes: vec![pb::NodeUsage {
            node: "n1".into(),
            load1: 350,
            load5: 120,
            load15: 40,
            ..Default::default()
        }],
    });
    m
}

#[test]
fn nodes_show_their_load_and_pools_and_details_their_host() {
    let m = host_and_pools();
    let mut v = listing();
    let s = screen_of(&m, &v, 200);
    // 3.50 of 4 cores is past 70 %; tank is degraded, with 3 errors not
    // taken as seen and a resilver running.
    assert!(
        line(&s, "n1").contains("3.50 1.20 0.40 zroot 34% tank DEGRADED, 3 errors, resilver"),
        "{s}"
    );
    assert!(!line(&s, "n2").contains("zroot"), "{s}");
    typed(
        &mut v, &m, "
",
    );
    let s = screen_sized(&m, &v, 200, 60);
    let flat: String = s
        .lines()
        .map(|l| {
            l.chars()
                .filter(|c| !"│╭╮╰╯─".contains(*c))
                .collect::<String>()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n");
    for said in [
        "load 3.50 1.20 0.40 (1, 5, 15 min)",
        "release: 15.1-RELEASE-p2 (kernel 15.1-RELEASE-p1)",
        "boot: UEFI",
        "up: 3d",
        "zroot ONLINE 34% (34.0 of 100.0 GiB)",
        "errors: none",
        "scrubbed: 1d ago",
        "tank DEGRADED 50% (5.0 of 10.0 GiB)",
        "errors: 0 read, 0 write, 4 checksum",
        "scrubbed: resilver running",
        "scrubs: every 14 days",
    ] {
        assert!(flat.contains(said), "{said}: {flat}");
    }
    // Said once, not again as the status's fields.
    assert!(!s.contains("allocBytes"), "{s}");
}

#[test]
fn services_mark_linux_images_and_tags_that_moved() {
    let mut m = cluster();
    let mut s = service(40, 1, 1);
    s.spec["pod"]["containers"] = serde_json::json!([
        { "name": "web", "image": "nginx:1" },
        { "name": "cache", "image": "redis:7" },
        { "name": "app", "image": "examples/node:24" },
    ]);
    s.status["linuxImages"] = serde_json::json!({ "nginx:1": "static-site", "redis:7": "" });
    s.status["images"] = serde_json::json!({ "moved": { "examples/node:24": "sha256:ab" } });
    m.changed(true, s, 0);
    let mut v = listing();
    typed(&mut v, &m, "2");
    let screen = screen_of(&m, &v, 220);
    assert!(
        line(&screen, "replicated").contains(
            "nginx:1 (Linux; native: static-site),redis:7 (Linux),examples/node:24 (tag moved)"
        ),
        "{screen}"
    );
    // As legion list services says them.
    let web = m
        .get(&(Kind::Service, "shop".into(), "web".into()))
        .unwrap();
    assert_eq!(
        crate::cli::marked_image("nginx:1", &web.status),
        "nginx:1 (Linux; native: static-site)"
    );
}

#[test]
fn node_keys_schedule_scrubs_and_take_pool_errors_as_seen() {
    let m = host_and_pools();
    let mut v = listing();
    assert_eq!(typed(&mut v, &m, "c"), None);
    match &v.prompt {
        Some(view::Prompt::Input { ask, .. }) => assert!(ask.contains("now every 14"), "{ask}"),
        p => panic!("{p:?}"),
    }
    // Only digits are taken.
    assert_eq!(
        typed(&mut v, &m, "7x\n"),
        run(&["node", "scrub", "n1", "--every", "7"])
    );
    assert_eq!(
        typed(&mut v, &m, "c\n"),
        run(&["node", "scrub", "n1", "--never"])
    );
    assert_eq!(
        typed(&mut v, &m, "c0\n"),
        run(&["node", "scrub", "n1", "--never"])
    );
    // Asked first, then taken.
    assert_eq!(typed(&mut v, &m, "X"), None);
    match &v.prompt {
        Some(view::Prompt::Confirm(_, ask)) => assert!(ask.contains("4 pool errors"), "{ask}"),
        p => panic!("{p:?}"),
    }
    assert_eq!(typed(&mut v, &m, "y"), run(&["node", "errors-seen", "n1"]));
    // A node without errors has none to take.
    v.notice = None;
    assert_eq!(typed(&mut v, &m, "↓X"), None);
    assert!(v.prompt.is_none());
    assert!(
        v.notice.as_deref().unwrap().contains("no errors"),
        "{:?}",
        v.notice
    );
}

#[test]
fn a_volume_s_replica_is_promoted_once_its_name_is_typed() {
    let mut m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "4");
    // Without a replica, there is nothing to promote.
    assert_eq!(typed(&mut v, &m, "p"), None);
    assert!(v.notice.as_deref().unwrap().contains("no replica"));
    let mut data = m
        .get(&(Kind::Volume, "shop".into(), "data".into()))
        .unwrap()
        .clone();
    data.status["replica"] = serde_json::json!({ "node": "n2", "state": "ready" });
    data.metadata.version += 1;
    m.changed(true, data, 0);
    v.notice = None;
    assert_eq!(typed(&mut v, &m, "pdat\n"), None);
    assert!(v.notice.as_deref().unwrap().contains("did not match"));
    assert_eq!(
        typed(&mut v, &m, "pdata\n"),
        run(&["volume", "promote", "data", "-n", "shop", "--yes"])
    );
}

fn usage_history(step_ms: i64) -> pb::UsageHistoryResponse {
    let sample = |at_ms, cpu_percent, memory_bytes| pb::UsageSample {
        at_ms,
        cpu_percent,
        memory_bytes,
    };
    let node_sample =
        |at_ms, zfs_used_bytes, zfs_avail_bytes, net_rx_bps, net_tx_bps| pb::NodeSample {
            at_ms,
            zfs_used_bytes,
            zfs_avail_bytes,
            net_rx_bps,
            net_tx_bps,
        };
    pb::UsageHistoryResponse {
        step_ms,
        tasks: vec![
            pb::TaskSeries {
                namespace: "shop".into(),
                task: "web-1-1".into(),
                // Twice in the first step: its latest counts.
                samples: vec![
                    sample(0, 10.0, 100),
                    sample(5, 30.0, 300),
                    sample(10, 40.0, 400),
                ],
                ..Default::default()
            },
            pb::TaskSeries {
                namespace: "shop".into(),
                task: "web-2-1".into(),
                samples: vec![sample(12, 50.0, 500), sample(20, 60.0, 600)],
                ..Default::default()
            },
        ],
        nodes: vec![pb::NodeSeries {
            node: "n1".into(),
            samples: vec![node_sample(0, 1, 9, 5, 5), node_sample(10, 2, 8, 7, 3)],
        }],
    }
}

#[test]
fn what_the_managers_kept_is_summed_a_step_at_a_time() {
    let past = model::Past::of(7, &usage_history(10));
    assert_eq!(past.range_ms, 7);
    assert_eq!(past.totals, vec![(30.0, 300), (90.0, 900), (60.0, 600)]);
    assert_eq!(past.telemetry, vec![(1, 9, 5, 5), (2, 8, 7, 3)]);
}

fn series_len(m: &Model, v: &View) -> usize {
    m.series(v.range_ms()).0.len()
}

#[test]
fn the_graphs_start_with_the_last_hour_and_show_a_range_on_w() {
    let mut m = cluster();
    // Polls heard before the hour kept (the cluster's, then this): they
    // stay the latest.
    m.usage(pb::UsageResponse {
        nodes: vec![pb::NodeUsage {
            node: "n1".into(),
            tasks: vec![pb::TaskUsage {
                namespace: "shop".into(),
                task: "web-1-1".into(),
                cpu_percent: 5.0,
                memory_bytes: 50,
            }],
            ..Default::default()
        }],
    });
    m.history_answer(0, &usage_history(10));
    let polls: Vec<(f64, u64)> = m.history.iter().copied().collect();
    assert_eq!(
        polls,
        vec![
            (30.0, 300),
            (90.0, 900),
            (60.0, 600),
            (25.0, 64 << 20),
            (5.0, 50)
        ]
    );
    assert_eq!(m.telemetry_history.len(), 4);

    let mut v = listing();
    assert_eq!(series_len(&m, &v), 5);
    typed(&mut v, &m, "w");
    assert_eq!((v.range_ms(), v.range_name()), (3_600_000, "1h"));
    // Nothing until the managers answer for that range.
    assert_eq!(series_len(&m, &v), 0);
    m.history_answer(3_600_000, &usage_history(10));
    assert_eq!(series_len(&m, &v), 3);
    let s = screen(&m, &v);
    assert!(s.contains("cpu 1h") && s.contains("mem 1h"), "{s}");
    // An answer for another range is not shown for this one.
    typed(&mut v, &m, "w");
    assert_eq!(v.range_name(), "6h");
    assert_eq!(series_len(&m, &v), 0);
    typed(&mut v, &m, "wwww");
    assert_eq!(v.range_name(), "live");
    assert_eq!(series_len(&m, &v), 5);
}

#[test]
fn a_range_is_fitted_to_the_graph_keeping_its_peaks() {
    let samples: Vec<f64> = (0..100).map(|i| if i == 37 { 9.0 } else { 1.0 }).collect();
    let fitted = draw::fit(&samples, 10, true);
    assert_eq!(fitted.len(), 10);
    assert_eq!(fitted[3], 9.0);
    assert_eq!(fitted.iter().filter(|v| **v == 1.0).count(), 9);
    // The polls are shown as they are, the latest where they don't fit.
    assert_eq!(draw::fit(&samples, 10, false).len(), 100);
    assert_eq!(draw::fit(&samples[..5], 10, true).len(), 5);
}

#[test]
fn marked_rows_are_acted_on_together_once_asked() {
    let m = cluster();
    let mut v = listing();
    // Space marks a row and moves on.
    typed(&mut v, &m, "  ");
    assert_eq!(v.marked.len(), 2);
    let s = screen(&m, &v);
    assert!(s.contains("2 marked"), "{s}");
    assert!(line(&s, "n1 ").starts_with("✓ n1"), "{s}");
    // A letter for all, asked once.
    assert_eq!(typed(&mut v, &m, "p"), None);
    match &v.prompt {
        Some(view::Prompt::Confirm(Request::Batch(all), ask)) => {
            assert_eq!(all.len(), 2);
            assert!(ask.contains("Pause 2 marked: n1, n2?"), "{ask}");
        }
        p => panic!("{p:?}"),
    }
    let done = typed(&mut v, &m, "y").unwrap();
    assert!(v.marked.is_empty());
    assert_eq!(
        done.flatten(),
        vec![
            Request::Availability {
                node: "n1".into(),
                to: "pause"
            },
            Request::Availability {
                node: "n2".into(),
                to: "pause"
            },
        ]
    );
    // Drain, asked of one, is asked once for all.
    typed(&mut v, &m, "g  d");
    assert!(matches!(
        &v.prompt,
        Some(view::Prompt::Confirm(Request::Batch(all), _)) if all.len() == 2
    ));
    typed(&mut v, &m, "n");
    assert_eq!(v.marked.len(), 2, "kept when not done");
    // What asks for more than a yes is one row at a time.
    assert_eq!(typed(&mut v, &m, "L"), None);
    assert!(v.prompt.is_none());
    assert!(v.notice.as_deref().unwrap().contains("one row at a time"));
    // Logs open for one row.
    v.notice = None;
    typed(&mut v, &m, "\x1b3 ");
    assert!(v.marked.is_empty() || v.tab == Tab::Tasks);
    v.notice = None;
    assert_eq!(typed(&mut v, &m, "l"), None);
    assert!(v.notice.as_deref().unwrap().contains("one row at a time"));
    // Esc clears the marks before the filter, and goes back after.
    v.notice = None;
    typed(&mut v, &m, "\x1b");
    assert!(v.marked.is_empty());
    assert_eq!(v.tab, Tab::Tasks);
    // Letters that act on none of the rows marked do what they always do.
    typed(&mut v, &m, " ");
    typed(&mut v, &m, "h");
    assert!(v.history);
}

#[test]
fn the_command_line_takes_top_s_commands_and_legion_s() {
    let m = cluster();
    let mut v = View::default();
    // = starts it with =.
    typed(&mut v, &m, "=");
    let s = screen(&m, &v);
    assert!(s.contains("Command ===> =▏"), "{s}");
    typed(&mut v, &m, "4\n");
    assert_eq!(v.tab, Tab::Storage);
    typed(&mut v, &m, ":=0\n");
    assert_eq!(*v.panel(), view::Panel::Graphs);
    typed(&mut v, &m, ":=3\n:find 2-1\n");
    let s = screen(&m, &v);
    assert!(s.contains("web-2-1") && !s.contains("web-1-1"), "{s}");
    typed(&mut v, &m, ":sort cpu asc\n:range 7d\n");
    assert_eq!(v.sort, view::SortCol::Cpu);
    assert!(!v.sort_desc);
    assert_eq!(v.range_name(), "7d");
    typed(&mut v, &m, ":range 2w\n");
    assert!(v.notice.as_deref().unwrap().contains("range live"));
    typed(&mut v, &m, ":poll 2s\n");
    assert_eq!(v.poll_ms, 2000);
    typed(&mut v, &m, ":mark all\n");
    assert_eq!(v.marked.len(), 1);
    typed(&mut v, &m, ":unmark\n:back\n");
    assert!(v.marked.is_empty());
    assert_eq!(*v.panel(), view::Panel::Menu);
    // Anything else is legion's; "legion help" is too.
    assert_eq!(typed(&mut v, &m, ":list nodes\n"), run(&["list", "nodes"]));
    assert_eq!(typed(&mut v, &m, ":legion help\n"), run(&["help"]));
    typed(&mut v, &m, ":help\n");
    assert!(v.help);
    // ↑ recalls them, top's too.
    v.help = false;
    typed(&mut v, &m, ":↑↑↑");
    match &v.prompt {
        Some(view::Prompt::Input { input, .. }) => assert_eq!(input, "list nodes"),
        p => panic!("{p:?}"),
    }
    typed(&mut v, &m, "\x1b:=x\n");
    assert!(v.quit);
}

#[test]
fn f10_pulls_down_the_action_bar_s_menus() {
    let m = cluster();
    let mut v = listing();
    typed(&mut v, &m, "§");
    assert_eq!(v.bar, Some((0, 0)));
    let s = screen_of(&m, &v, 140);
    // The Actions of the node selected.
    assert!(
        s.contains("d      Drain") && s.contains("X      Pool errors seen"),
        "{s}"
    );
    // ↓ Enter does the second: pause.
    assert_eq!(
        typed(&mut v, &m, "↓\n"),
        Some(Request::Availability {
            node: "n1".into(),
            to: "pause"
        })
    );
    assert!(v.bar.is_none());
    // → to the Cluster menu, its first item: diagnose.
    typed(&mut v, &m, "§");
    for _ in 0..3 {
        typed_code(&mut v, &m, KeyCode::Right);
    }
    assert_eq!(v.bar, Some((3, 0)));
    assert!(screen_of(&m, &v, 140).contains("Diagnose every node"));
    assert_eq!(typed(&mut v, &m, "\n"), run(&["diagnose"]));
    // Esc closes it, doing nothing.
    typed(&mut v, &m, "§\x1b");
    assert!(v.bar.is_none());
    assert_eq!(
        *v.panel(),
        view::Panel::List {
            tab: Tab::Nodes,
            focus: None
        }
    );
    // On the menu, nothing is selected to act on.
    let mut v = View::default();
    typed(&mut v, &m, "§");
    assert!(screen_of(&m, &v, 140).contains("Nothing selected to act on"));
}

#[test]
fn a_click_opens_the_action_bar_and_does_an_item() {
    use ratatui::crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    let m = cluster();
    let mut v = listing();
    v.mouse = true;
    let area = ratatui::layout::Rect::new(0, 0, 140, 20);
    let click = |column, row| MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column,
        row,
        modifiers: KeyModifiers::NONE,
    };
    // View's name, then its second item: the workload tree.
    assert_eq!(v.mouse(&m, click(view::bar_x(1) + 1, 0), area), None);
    assert_eq!(v.bar, Some((1, 0)));
    v.mouse(&m, click(view::bar_x(1) + 2, 3), area);
    assert!(v.tree && v.bar.is_none());
    // Again on its name closes it; a click elsewhere too.
    v.mouse(&m, click(view::bar_x(0) + 1, 0), area);
    v.mouse(&m, click(view::bar_x(0) + 1, 0), area);
    assert!(v.bar.is_none());
    // Cluster's first: diagnose.
    v.mouse(&m, click(view::bar_x(3) + 1, 0), area);
    assert_eq!(
        v.mouse(&m, click(view::bar_x(3) + 2, 2), area),
        run(&["diagnose"])
    );
}

#[test]
fn the_graphs_panel_draws_the_four_large() {
    let m = cluster();
    let mut v = View::default();
    let s = screen(&m, &v);
    assert!(
        line(&s, "Graphs").starts_with("0 Graphs CPU, memory"),
        "{s}"
    );
    typed(&mut v, &m, "k\n");
    assert_eq!(*v.panel(), view::Panel::Graphs);
    let s = screen_sized(&m, &v, 120, 30);
    for card in ["cpu", "mem", "zfs", "net"] {
        assert!(s.contains(&format!("╭ {card} ")), "{card}: {s}");
    }
    // Two rows of two.
    let cpu = s.lines().position(|l| l.contains("╭ cpu ")).unwrap();
    let zfs = s.lines().position(|l| l.contains("╭ zfs ")).unwrap();
    assert!(zfs > cpu + 5, "{s}");
    typed(&mut v, &m, "w");
    assert!(screen_sized(&m, &v, 120, 30).contains("╭ cpu 1h"));
    typed(&mut v, &m, "«");
    assert_eq!(*v.panel(), view::Panel::Menu);
}
