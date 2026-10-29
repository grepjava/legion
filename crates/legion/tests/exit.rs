//! The phase 1 exit test (PHASE.md): a multi-service app with probes and
//! volumes runs on one node and comes back from failures. It drives the
//! real daemon and CLI on FreeBSD as root, so it is ignored by default:
//!
//! ```sh
//! kldload -n zfs; truncate -s 1g /var/tmp/lexit.img
//! zpool create -m /var/tmp/lexit-pool lexit /var/tmp/lexit.img
//! sudo LEGION_EXIT_LAB=/var/tmp/lexit LEGION_EXIT_ZFS_ROOT=lexit/legion \
//!     target/debug/deps/exit-<hash> --ignored --nocapture
//! zpool destroy lexit; rm /var/tmp/lexit.img
//! ```
//!
//! It runs on the jail runtime; `LEGION_EXIT_RUNTIME=podman` runs it on
//! Podman and ocijail instead.
//!
//! The app is `tests/exit/app.yaml`: web (two replicas) counts requests in
//! db, whose data is on a ZFS volume, and in cache, whose data is on a
//! tmpfs volume. The lab directory is wiped first; remove it afterwards
//! with `chflags -R 0` and `rm -rf`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const LEGION: &str = env!("CARGO_BIN_EXE_legion");
const SERVER: &str = "127.0.0.1:7471";
const METRICS: &str = "127.0.0.1:7472";
const INGRESS: &str = "127.0.0.1:7473";
const INGRESS_TLS: &str = "127.0.0.1:7474";

struct Lab {
    dir: PathBuf,
    zfs_root: String,
    /// The jail runtime rather than Podman.
    jails: bool,
    daemon: Option<Child>,
}

impl Lab {
    fn start_daemon(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("daemon.log"))
            .unwrap();
        let child = Command::new(LEGION)
            .args(["daemon", "--listen", SERVER, "--zfs-root", &self.zfs_root])
            .args(["--metrics-listen", METRICS])
            .args(["--ingress-listen", INGRESS])
            .args(["--ingress-tls-listen", INGRESS_TLS])
            .args(["--runtime", if self.jails { "jail" } else { "podman" }])
            .arg("--data-dir")
            .arg(self.dir.join("data"))
            .arg("--run-dir")
            .arg(self.dir.join("run"))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.daemon = Some(child);
        wait("the daemon", 60, || legion(&["status"]).status.success());
    }

    /// Like a crash: no chance to clean up.
    fn kill_daemon(&mut self) {
        let mut child = self.daemon.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    fn stop_daemon(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = Command::new("kill")
                .args(["-INT", &child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(60);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }

    /// Legion's own Podman, through the daemon's socket.
    fn podman(&self, args: &[&str]) -> String {
        let socket = format!("unix://{}", self.dir.join("run/podman.sock").display());
        let out = Command::new("podman")
            .args(["--url", &socket])
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "podman {args:?}: {}",
            text(&out.stderr)
        );
        text(&out.stdout)
    }

    /// The jail runtime's records: (id, name, jail).
    fn records(&self) -> Vec<(String, String, String)> {
        let Ok(dirs) = std::fs::read_dir(self.dir.join("data/jails")) else {
            return Vec::new();
        };
        dirs.filter_map(|d| {
            let r: Value =
                serde_json::from_slice(&std::fs::read(d.ok()?.path().join("record.json")).ok()?)
                    .ok()?;
            let s = |k: &str| r[k].as_str().unwrap_or_default().to_owned();
            Some((s("id"), s("name"), s("jail")))
        })
        .collect()
    }

    fn jail_record(&self, name: &str) -> (String, String) {
        let (id, _, jail) = self
            .records()
            .into_iter()
            .find(|(_, n, _)| n == name)
            .unwrap_or_else(|| panic!("no container {name}"));
        (id, jail)
    }

    fn container_id(&self, name: &str) -> String {
        if self.jails {
            return self.jail_record(name).0;
        }
        self.podman(&["inspect", "--format", "{{.Id}}", name])
            .trim()
            .to_owned()
    }

    /// The container's processes die at once, as in a crash.
    fn kill_container(&self, name: &str) {
        if self.jails {
            let jid = sh(&format!("jls -j {} jid", self.jail_record(name).1));
            sh(&format!("pkill -KILL -j {}", jid.trim()));
        } else {
            self.podman(&["kill", name]);
        }
    }

    /// One process in the container dies.
    fn kill_process(&self, name: &str, process: &str) {
        if self.jails {
            let jid = sh(&format!("jls -j {} jid", self.jail_record(name).1));
            sh(&format!("pkill -j {} {process}", jid.trim()));
        } else {
            self.podman(&["exec", name, "pkill", process]);
        }
    }

    fn no_containers(&self) -> bool {
        if self.jails {
            self.records().is_empty()
        } else {
            self.podman(&["ps", "-aq"]).trim().is_empty()
        }
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn identity() -> PathBuf {
    PathBuf::from(std::env::var("LEGION_EXIT_LAB").expect("set LEGION_EXIT_LAB"))
        .join("data/admin.pem")
}

fn cli() -> Command {
    let mut c = Command::new(LEGION);
    c.args(["--server", SERVER])
        .arg("--identity")
        .arg(identity());
    c
}

fn legion(args: &[&str]) -> Output {
    cli().args(args).stdin(Stdio::null()).output().unwrap()
}

/// `legion` with `input` on its stdin.
fn legion_input(args: &[&str], input: &[u8]) -> Output {
    let mut child = cli()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

fn ok(args: &[&str]) -> String {
    let out = legion(args);
    assert!(
        out.status.success(),
        "legion {args:?}: {}",
        text(&out.stderr)
    );
    text(&out.stdout)
}

fn wait(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Objects of a kind in `shop`, as JSON.
fn list(kind: &str) -> Vec<Value> {
    let out = legion(&["list", kind, "-n", "shop", "-o", "json"]);
    if !out.status.success() {
        return Vec::new();
    }
    match serde_json::from_slice(&out.stdout) {
        Ok(Value::Array(v)) => v,
        Ok(one @ Value::Object(_)) => vec![one],
        _ => Vec::new(),
    }
}

/// Every service has all its tasks ready.
fn app_ready() -> bool {
    let services = list("services");
    services.len() == 3
        && services.iter().all(|s| {
            let st = &s["status"];
            st["desired"].as_u64().is_some_and(|d| d > 0)
                && st["ready"] == st["desired"]
                && st["observedGeneration"] == s["metadata"]["generation"]
        })
}

/// The running tasks of a service: (name, ip).
fn running(service: &str) -> Vec<(String, String)> {
    list("tasks")
        .into_iter()
        .filter(|t| {
            t["spec"]["service"] == service
                && t["status"]["state"] == "running"
                && t["spec"]["desiredState"] == "running"
        })
        .map(|t| {
            (
                t["metadata"]["name"].as_str().unwrap().to_owned(),
                t["status"]["ip"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

/// One HTTP/1.0 GET of a web task: (status, body).
fn get(ip: &str, path: &str) -> Option<(u16, String)> {
    get_at(&format!("{ip}:8080"), path)
}

fn get_at(addr: &str, path: &str) -> Option<(u16, String)> {
    get_host(addr, "", path)
}

/// With a `Host` header, unless `host` is empty.
fn get_host(addr: &str, host: &str, path: &str) -> Option<(u16, String)> {
    let addr: SocketAddr = addr.parse().ok()?;
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let host = if host.is_empty() {
        String::new()
    } else {
        format!("Host: {host}\r\n")
    };
    write!(s, "GET {path} HTTP/1.0\r\n{host}\r\n").ok()?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).ok()?;
    let status = resp.split_whitespace().nth(1)?.parse().ok()?;
    let body = resp.split_once("\r\n\r\n")?.1.trim().to_owned();
    Some((status, body))
}

/// A counter through some web task.
fn count(path: &str) -> u64 {
    for (_, ip) in running("web") {
        if let Some((200, body)) = get(&ip, path) {
            return body.parse().unwrap_or_else(|_| panic!("{path}: {body:?}"));
        }
    }
    panic!("no web task answered {path}");
}

fn sh(cmd: &str) -> String {
    text(
        &Command::new("/bin/sh")
            .args(["-c", cmd])
            .output()
            .unwrap()
            .stdout,
    )
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD; set LEGION_EXIT_LAB and LEGION_EXIT_ZFS_ROOT"]
fn app_survives_failures() {
    let dir = PathBuf::from(std::env::var("LEGION_EXIT_LAB").expect("set LEGION_EXIT_LAB"));
    let zfs_root = std::env::var("LEGION_EXIT_ZFS_ROOT").expect("set LEGION_EXIT_ZFS_ROOT");
    let on_jails = std::env::var("LEGION_EXIT_RUNTIME")
        .ok()
        .is_none_or(|r| r != "podman");
    // A failed run leaves its pods running; wiping the lab under them
    // would orphan them.
    let jails = sh("jls path");
    assert!(
        !jails
            .lines()
            .any(|p| p.starts_with(&*dir.to_string_lossy())),
        "jails from an earlier run still run under {}: remove them first",
        dir.display()
    );
    let mut lab = Lab {
        dir,
        zfs_root,
        jails: on_jails,
        daemon: None,
    };
    // The jail runtime's pod jails are on /var/empty: found by their ids.
    for (id, _, _) in lab.records() {
        assert!(
            !sh("jls name").lines().any(|j| j == id),
            "pod jail {id} from an earlier run still runs: remove it first"
        );
    }
    if lab.dir.exists() {
        sh(&format!(
            "chflags -R 0 {0} && rm -rf {0}",
            lab.dir.display()
        ));
    }
    std::fs::create_dir_all(&lab.dir).unwrap();
    lab.start_daemon();
    ok(&["init"]);
    let app = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/exit/app.yaml");
    ok(&["apply", "-f", app.to_str().unwrap()]);
    // The first run pulls the image.
    wait("the app to be ready", 300, app_ready);
    assert_eq!(running("web").len(), 2);

    // Prometheus sees the cluster and what each container uses. The kernel
    // updates a jail's resident memory once a second, so a container that
    // has only just started may show none yet.
    let scrape = || {
        let (code, metrics) = get_at(METRICS, "/metrics").expect("metrics");
        assert_eq!(code, 200, "{metrics}");
        metrics
    };
    let mut metrics = scrape();
    for line in [
        "legion_raft_leader 1",
        "legion_nodes{state=\"ready\"} 1",
        "legion_service_replicas{namespace=\"shop\",service=\"web\"} 2",
        "legion_service_ready_tasks{namespace=\"shop\",service=\"web\"} 2",
        "legion_tasks{namespace=\"shop\",state=\"running\"} 4",
        "legion_agent_pods 4",
        "legion_agent_usage_unread 0",
    ] {
        assert!(metrics.lines().any(|l| l == line), "{line} in:\n{metrics}");
    }
    let memory = |metrics: &str| -> Vec<f64> {
        metrics
            .lines()
            .filter(|l| l.starts_with("legion_container_memory_bytes{namespace=\"shop\","))
            .filter_map(|l| l.rsplit(' ').next()?.parse().ok())
            .collect()
    };
    assert_eq!(memory(&metrics).len(), 4, "{metrics}");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !memory(&metrics).iter().all(|m| *m > 0.0) {
        assert!(Instant::now() < deadline, "{metrics}");
        std::thread::sleep(Duration::from_millis(500));
        metrics = scrape();
    }

    // Each service has a VIP, and names resolve to it.
    for (svc, vip) in [
        ("web", "10.201.0.1"),
        ("db", "10.201.0.2"),
        ("cache", "10.201.0.3"),
    ] {
        let s = list("services")
            .into_iter()
            .find(|s| s["metadata"]["name"] == svc)
            .unwrap();
        assert_eq!(s["status"]["virtualIp"], vip, "{svc}");
        let answer = sh(&format!("drill @10.200.0.1 {svc}.shop.legion A"));
        assert!(answer.contains(&format!("IN	A	{vip}")), "{answer}");
    }

    // Both web tasks reach db and cache by name, through their VIPs.
    assert_eq!(count("/count"), 1);
    assert_eq!(count("/count"), 2);
    assert_eq!(count("/hits"), 1);
    for (name, ip) in running("web") {
        assert_eq!(get(&ip, "/healthz"), Some((200, "ok".into())), "{name}");
    }

    // A route sends HTTP for its host through the node's ingress proxy.
    let route = lab.dir.join("route.yaml");
    std::fs::write(
        &route,
        "apiVersion: legion/v1\nkind: Route\nmetadata: { name: www, namespace: shop }\n\
         spec: { host: shop.example.org, service: web }\n",
    )
    .unwrap();
    ok(&["apply", "-f", route.to_str().unwrap()]);
    wait("the route", 30, || {
        get_host(INGRESS, "shop.example.org", "/healthz") == Some((200, "ok".into()))
    });
    // The port in the header and the case of the name do not matter. (Not
    // /count: the steps below count on its value.)
    assert_eq!(
        get_host(INGRESS, "Shop.Example.org:80", "/healthz"),
        Some((200, "ok".into()))
    );
    assert_eq!(
        get_host(INGRESS, "other.example.org", "/healthz").map(|r| r.0),
        Some(404)
    );

    // With a certificate in a Secret, the route answers HTTPS too, checked
    // against that certificate.
    let (cert, key) = (lab.dir.join("www.crt"), lab.dir.join("www.key"));
    sh(&format!(
        "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 \
         -subj /CN=shop.example.org -addext subjectAltName=DNS:shop.example.org \
         -keyout {} -out {} 2>/dev/null",
        key.display(),
        cert.display()
    ));
    let pem = std::fs::read_to_string(&cert).unwrap() + &std::fs::read_to_string(&key).unwrap();
    let indented: String = pem.lines().map(|l| format!("    {l}\n")).collect();
    std::fs::write(
        &route,
        format!(
            "apiVersion: legion/v1\nkind: Secret\nmetadata: {{ name: www-cert, namespace: shop }}\n\
             spec:\n  text: |\n{indented}---\n\
             apiVersion: legion/v1\nkind: Route\nmetadata: {{ name: www, namespace: shop }}\n\
             spec: {{ host: shop.example.org, service: web, tls: {{ secret: www-cert }} }}\n"
        ),
    )
    .unwrap();
    ok(&["apply", "-f", route.to_str().unwrap()]);
    let https = || {
        sh(&format!(
            "printf 'GET /healthz HTTP/1.0\\r\\nHost: shop.example.org\\r\\n\\r\\n' | \
             openssl s_client -quiet -connect {INGRESS_TLS} -servername shop.example.org \
             -verify_return_error -CAfile {} 2>/dev/null",
            cert.display()
        ))
    };
    wait("HTTPS on the route", 30, || https().contains("\r\n\r\nok"));

    // Sessions through the manager. Exec passes output, exit code and,
    // with -i, stdin; with -t the command gets a terminal (script(1) gives
    // the CLI one).
    let out = legion(&[
        "exec",
        "web",
        "-n",
        "shop",
        "--",
        "/bin/sh",
        "-c",
        "echo out; echo err >&2; exit 3",
    ]);
    assert_eq!(
        (out.status.code(), text(&out.stdout), text(&out.stderr)),
        (Some(3), "out\n".into(), "err\n".into())
    );
    let out = legion_input(
        &["exec", "-i", "db", "-n", "shop", "--", "/bin/cat"],
        b"piped\n",
    );
    assert_eq!(text(&out.stdout), "piped\n", "{}", text(&out.stderr));
    let tty = sh(&format!(
        "script -q /dev/null {LEGION} --server {SERVER} --identity {} \
         exec -t web -n shop -- /bin/sh -c 'test -t 0 && echo terminal' </dev/null",
        identity().display()
    ));
    assert!(tty.contains("terminal"), "{tty:?}");
    // Logs: inetd -d lists its services as it starts.
    let out = legion(&["logs", "db", "-n", "shop", "--tail", "100"]);
    let logs = text(&out.stdout) + &text(&out.stderr);
    assert!(logs.contains("legion-store"), "{logs}");
    // Port-forward: a local port reaches a web task.
    let mut forward = cli()
        .args(["port-forward", "web", "-n", "shop", "18080:8080"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    wait("the forwarded port", 30, || {
        get_at("127.0.0.1:18080", "/healthz") == Some((200, "ok".into()))
    });
    forward.kill().unwrap();
    forward.wait().unwrap();

    // A database crash: the container restarts in its pod, the data stays.
    let db = running("db")[0].0.clone();
    lab.kill_container(&format!("shop_{db}_db"));
    wait("db to restart", 60, || {
        list("tasks")
            .iter()
            .any(|t| t["metadata"]["name"] == db.as_str() && t["status"]["restarts"] == 1)
    });
    wait("the app after the db restart", 60, app_ready);
    assert_eq!(count("/count"), 3);

    // A lost database pod: a new task mounts the same dataset.
    ok(&["delete", "task", &db, "-n", "shop"]);
    wait("a new db task", 120, || {
        running("db").first().is_some_and(|(n, _)| *n != db) && app_ready()
    });
    assert_eq!(count("/count"), 4);

    // The cache keeps its data across a container restart, not across pods.
    let cache = running("cache")[0].0.clone();
    lab.kill_container(&format!("shop_{cache}_cache"));
    wait("cache to restart", 60, || {
        list("tasks")
            .iter()
            .any(|t| t["metadata"]["name"] == cache.as_str() && t["status"]["restarts"] == 1)
    });
    wait("the app after the cache restart", 60, app_ready);
    assert_eq!(count("/hits"), 2);
    ok(&["delete", "task", &cache, "-n", "shop"]);
    wait("a new cache task", 120, || {
        running("cache").first().is_some_and(|(n, _)| *n != cache) && app_ready()
    });
    assert_eq!(count("/hits"), 1, "a new tmpfs");

    // web's process dies: the container restarts under the policy.
    let (web, web_ip) = running("web")[0].clone();
    lab.kill_process(&format!("shop_{web}_web"), "inetd");
    wait("web to be restarted", 90, || {
        list("tasks")
            .iter()
            .any(|t| t["metadata"]["name"] == web.as_str() && t["status"]["restarts"] == 1)
    });
    wait("the app after the web restart", 60, app_ready);
    assert_eq!(get(&web_ip, "/healthz"), Some((200, "ok".into())));

    // The daemon crashes: pods keep running, and the new daemon adopts them.
    let before: Vec<String> = running("web")
        .iter()
        .map(|(n, _)| lab.container_id(&format!("shop_{n}_web")))
        .collect();
    lab.kill_daemon();
    assert_eq!(
        get(&web_ip, "/healthz"),
        Some((200, "ok".into())),
        "pods outlive the daemon"
    );
    lab.start_daemon();
    wait("the app after the daemon restart", 120, app_ready);
    let after: Vec<String> = running("web")
        .iter()
        .map(|(n, _)| lab.container_id(&format!("shop_{n}_web")))
        .collect();
    assert_eq!(before, after, "the same containers, adopted");
    assert_eq!(count("/count"), 5);

    // Clean up: the pods go, the volume's dataset stays.
    for s in ["web", "db", "cache"] {
        ok(&["delete", "service", s, "-n", "shop"]);
    }
    wait("pods removed", 120, || lab.no_containers());
    lab.stop_daemon();
    let _ = Command::new("ifconfig")
        .args(["legion0", "destroy"])
        .status();
    let ds = format!("{}/volumes/shop/db-data", lab.zfs_root);
    assert!(sh(&format!("zfs list -H -o name {ds}")).contains(&ds));
}
