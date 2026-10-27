//! Runs the real binary: a daemon plus CLI commands against it.

use std::net::TcpListener;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const LEGION: &str = env!("CARGO_BIN_EXE_legion");

struct Daemon {
    child: Child,
    server: String,
    _dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_daemon() -> Daemon {
    let dir = tempfile::tempdir().unwrap();
    // Pick a free port; the race with other processes is acceptable in tests.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = format!("127.0.0.1:{port}");
    let child = Command::new(LEGION)
        .args(["daemon", "--no-agent", "--listen", &server, "--data-dir"])
        .arg(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let daemon = Daemon {
        child,
        server,
        _dir: dir,
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while !legion(&daemon, &["status"]).status.success() {
        assert!(Instant::now() < deadline, "daemon did not come up");
        std::thread::sleep(Duration::from_millis(100));
    }
    daemon
}

fn legion(d: &Daemon, args: &[&str]) -> Output {
    Command::new(LEGION)
        .arg("--server")
        .arg(&d.server)
        .arg("--identity")
        .arg(d._dir.path().join("admin.pem"))
        .args(args)
        .output()
        .unwrap()
}

/// Runs a command that must succeed and returns its stdout.
fn ok(d: &Daemon, args: &[&str]) -> String {
    let out = legion(d, args);
    assert!(
        out.status.success(),
        "legion {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn daemon_and_cli() {
    let d = start_daemon();
    assert!(ok(&d, &["status"]).contains("none (run `legion init`)"));
    assert!(ok(&d, &["init"]).contains("cluster initialized"));
    assert!(ok(&d, &["status"]).contains("Leader"));

    let manifest = d._dir.path().join("app.yaml");
    std::fs::write(
        &manifest,
        r#"
apiVersion: legion/v1
kind: Service
metadata:
  name: web
  namespace: shop
  labels: { app: web, tier: front }
spec:
  replicas: 3
  pod:
    containers: [{ name: web, image: web:1 }]
---
apiVersion: legion/v1
kind: Service
metadata: { name: db, namespace: shop, labels: { app: db } }
spec: { pod: { containers: [{ name: db, image: postgres:17 }] } }
---
"#,
    )
    .unwrap();
    let applied = ok(&d, &["apply", "-f", manifest.to_str().unwrap()]);
    assert!(applied.contains("Service/shop/web created"), "{applied}");
    assert!(applied.contains("Service/shop/db created"), "{applied}");
    let again = ok(&d, &["apply", "-f", manifest.to_str().unwrap()]);
    assert_eq!(again.matches("unchanged").count(), 2, "{again}");

    // `diff` is silent and succeeds when nothing would change...
    assert_eq!(ok(&d, &["diff", "-f", manifest.to_str().unwrap()]), "");
    // ...and shows the change and exits 1 otherwise.
    let text = std::fs::read_to_string(&manifest).unwrap();
    std::fs::write(&manifest, text.replace("replicas: 3", "replicas: 4")).unwrap();
    let diff = legion(&d, &["diff", "-f", manifest.to_str().unwrap()]);
    assert_eq!(diff.status.code(), Some(1));
    let diff = String::from_utf8_lossy(&diff.stdout);
    assert!(diff.contains("--- live Service/shop/web"), "{diff}");
    assert!(
        diff.contains(
            "-  replicas: 3
+  replicas: 4"
        ),
        "{diff}"
    );
    assert!(!diff.contains("Service/shop/db"), "{diff}");
    std::fs::write(&manifest, text).unwrap();

    let table = ok(&d, &["list", "services", "-n", "shop"]);
    assert!(table.starts_with("NAMESPACE"), "{table}");
    assert_eq!(table.lines().count(), 3, "{table}");
    let selected = ok(&d, &["list", "svc", "-A", "-l", "tier=front"]);
    assert!(
        selected.contains("web") && !selected.contains("db"),
        "{selected}"
    );

    let yaml = ok(&d, &["get", "service", "web", "-n", "shop"]);
    assert!(yaml.contains("replicas: 3"), "{yaml}");
    assert!(yaml.contains("generation: 1"), "{yaml}");

    ok(&d, &["delete", "service", "web", "-n", "shop"]);
    let missing = legion(&d, &["get", "service", "web", "-n", "shop"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("not found"));

    let bad = legion(&d, &["list", "pods"]);
    assert!(String::from_utf8_lossy(&bad.stderr).contains("unknown kind"));
}
