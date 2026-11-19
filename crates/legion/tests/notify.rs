//! Alerts sent (CONSOLE.md, phase 9), through real daemons on three
//! machines, so ignored by default: this host is the manager n1, two VMs
//! the managers n2 and n3 (see `tests/cluster.rs` for the environment).
//! An AlertReceiver posts to a webhook this test serves. A node's daemon
//! killed, its `node-down` reaches the webhook within 60 s; back, the
//! alert's clearing does. Then the leader's daemon is killed: its own
//! `node-down` reaches the webhook within 60 s, sent by the next leader,
//! and nothing that was sent before is sent again. Every message comes
//! once. (Three managers can lose one: a node down and the leader killed
//! at once would leave no quorum, and nothing to send them.)

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{Lab, PORT, Role, text, wait};
use serde_json::Value;

/// Where the webhook listens on this host.
const HOOK_PORT: u16 = 7489;

/// A webhook of a few lines: every POST's JSON body kept, answered 200.
struct Hook {
    got: Arc<Mutex<Vec<Value>>>,
}

impl Hook {
    fn start() -> Hook {
        let listener = TcpListener::bind(("0.0.0.0", HOOK_PORT)).expect("the webhook's port");
        let got: Arc<Mutex<Vec<Value>>> = Arc::default();
        let kept = got.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { continue };
                let _ = conn.set_read_timeout(Some(Duration::from_secs(10)));
                let mut r = BufReader::new(conn.try_clone().unwrap());
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if r.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0; length];
                if r.read_exact(&mut body).is_ok()
                    && let Ok(v) = serde_json::from_slice::<Value>(&body)
                {
                    eprintln!("webhook: {} {}", v["state"], v["id"]);
                    kept.lock().unwrap().push(v);
                }
                let _ = conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            }
        });
        Hook { got }
    }

    /// How many of what came had `state` and `id`.
    fn count(&self, state: &str, id: &str) -> usize {
        self.got
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["state"] == state && v["id"] == id)
            .count()
    }

    fn all(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self
            .got
            .lock()
            .unwrap()
            .iter()
            .map(|v| {
                (
                    v["state"].as_str().unwrap_or_default().to_owned(),
                    v["id"].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        v.sort();
        v
    }
}

/// The node whose manager leads, as the manager at `server` says.
fn leader(lab: &Lab, server: &str) -> Option<String> {
    let out = lab.legion_at(server, &lab.admin(), &["status"]);
    let said = text(&out.stdout);
    let id = said
        .lines()
        .find_map(|l| l.strip_prefix("leader:"))?
        .trim()
        .to_owned();
    let addr = said.lines().find_map(|l| {
        let mut w = l.split_whitespace();
        (w.next() == Some(id.as_str())).then(|| w.next().unwrap_or_default().to_owned())
    })?;
    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
    if host == lab.host {
        return Some("n1".into());
    }
    let i = lab.vms.iter().position(|v| v.addr == host)?;
    Some(format!("n{}", i + 2))
}

#[test]
#[ignore = "needs root, ZFS and two more FreeBSD machines; see tests/cluster.rs"]
fn a_node_down_reaches_a_webhook_once_across_a_change_of_leader() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");
    let servers: Vec<String> = std::iter::once(lab.server())
        .chain(lab.vms.iter().map(|v| format!("{}:{PORT}", v.addr)))
        .collect();

    step("three managers, and a receiver posting node-down to a webhook here");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let manager = lab.ok(&["join-token", "manager"]).trim().to_owned();
    lab.join(0, &manager, Role::Manager);
    lab.join(1, &manager, Role::Manager);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"].iter().all(|n| lab.node_state(n) == "ready")
    });
    let hook = Hook::start();
    let receiver = lab.dir.join("receiver.yaml");
    std::fs::write(
        &receiver,
        format!(
            "apiVersion: legion/v1\nkind: AlertReceiver\nmetadata: {{ name: ops }}\nspec:\n  \
             webhook: {{ url: 'http://{}:{HOOK_PORT}/hook' }}\n  kinds: [node-down]\n",
            lab.host
        ),
    )
    .unwrap();
    lab.ok(&["apply", "-f", receiver.to_str().unwrap()]);

    step("a test reaches it");
    lab.ok(&["alerts", "test", "ops"]);
    wait("the test at the webhook", 30, || hook.count("test", "node-suspect//") == 1);

    let first = leader(&lab, &lab.server()).expect("a leader");
    let down = if first == "n2" { "n3" } else { "n2" };
    let i = common::vm(down);
    eprintln!("{first} leads; {down} goes down");

    step("a node's daemon killed: its node-down reaches the webhook within 60 s");
    let killed = Instant::now();
    let out = lab.ssh(&lab.vms[i], "pkill -KILL -f 'legion-lab daemon'");
    assert!(out.status.success(), "{}", text(&out.stderr));
    let id = format!("node-down//Node/{down}");
    wait("node-down at the webhook", 60, || hook.count("raised", &id) == 1);
    eprintln!("node-down of {down} at the webhook {} ms after its daemon was killed", killed.elapsed().as_millis());

    step("back: its clearing reaches the webhook");
    lab.restart_vm(i, Role::Manager);
    wait("the node ready again", 120, || lab.node_state(down) == "ready");
    wait("the clearing at the webhook", 90, || hook.count("cleared", &id) == 1);

    step("the leader's daemon killed: its node-down reaches the webhook within 60 s, from the next");
    let lead = leader(&lab, &lab.server()).expect("a leader");
    let killed = Instant::now();
    if lead == "n1" {
        let mut child = lab.daemon.take().expect("the host's daemon");
        child.kill().unwrap();
        let _ = child.wait();
    } else {
        let out = lab.ssh(&lab.vms[common::vm(&lead)], "pkill -KILL -f 'legion-lab daemon'");
        assert!(out.status.success(), "{}", text(&out.stderr));
    }
    let other = servers
        .iter()
        .zip(["n1", "n2", "n3"])
        .find(|(_, n)| *n != lead)
        .map(|(s, _)| s.clone())
        .unwrap();
    let lead_id = format!("node-down//Node/{lead}");
    wait("the leader's node-down at the webhook", 60, || hook.count("raised", &lead_id) == 1);
    eprintln!("node-down of {lead}, the leader, at the webhook {} ms after its daemon was killed", killed.elapsed().as_millis());
    let next = leader(&lab, &other).expect("a new leader");
    assert_ne!(next, lead, "a new leader sent it");

    step("every message once");
    // Long enough for a second sending, a pass and a retry, to show.
    std::thread::sleep(Duration::from_secs(20));
    assert_eq!(
        hook.all(),
        [
            ("cleared".to_owned(), id.clone()),
            ("raised".to_owned(), id.clone()),
            ("raised".to_owned(), lead_id.clone()),
            ("test".to_owned(), "node-suspect//".to_owned()),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>(),
        "each message once"
    );
    let status: Value = serde_json::from_str(&text(
        &lab.legion_at(&other, &lab.admin(), &["get", "alertreceiver", "ops", "-o", "json"])
            .stdout,
    ))
    .unwrap_or(Value::Null);
    assert_eq!(status["status"]["sent"], serde_json::json!([lead_id]), "{status}");
    assert!(status["status"]["error"].is_null(), "{status}");

    step("the old leader back, for the teardown");
    if lead == "n1" {
        lab.restart_host(&[]);
    } else {
        lab.restart_vm(common::vm(&lead), Role::Manager);
    }
}
