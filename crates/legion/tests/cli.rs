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

/// A free port; the race with other processes is acceptable in tests.
fn free_addr() -> String {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    format!("127.0.0.1:{port}")
}

fn start_daemon() -> Daemon {
    start_daemon_in(tempfile::tempdir().unwrap(), free_addr())
}

fn start_daemon_in(dir: tempfile::TempDir, server: String) -> Daemon {
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
    std::fs::write(&manifest, &text).unwrap();

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

    // A new image starts an update (no agent: it never finishes), which
    // `rollback` undoes.
    let rolled = legion(&d, &["rollback", "web", "-n", "shop"]);
    assert!(
        String::from_utf8_lossy(&rolled.stderr).contains("no update to roll back"),
        "{rolled:?}"
    );
    std::fs::write(&manifest, text.replace("web:1", "web:2")).unwrap();
    ok(&d, &["apply", "-f", manifest.to_str().unwrap()]);
    let updating = || ok(&d, &["list", "services", "-n", "shop"]).contains("updating");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !updating() {
        assert!(std::time::Instant::now() < deadline, "no update");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(ok(&d, &["rollback", "web", "-n", "shop"]).contains("rolling back"));
    let yaml = ok(&d, &["get", "service", "web", "-n", "shop"]);
    assert!(yaml.contains("image: web:1"), "{yaml}");
    let rolling = || ok(&d, &["list", "services", "-n", "shop"]).contains("rolling-back");
    while !rolling() {
        assert!(std::time::Instant::now() < deadline, "no rollback");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    std::fs::write(&manifest, &text).unwrap();

    ok(&d, &["delete", "service", "web", "-n", "shop"]);
    let missing = legion(&d, &["get", "service", "web", "-n", "shop"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("not found"));

    let bad = legion(&d, &["list", "pods"]);
    assert!(String::from_utf8_lossy(&bad.stderr).contains("unknown kind"));
}

#[test]
fn compose_stacks() {
    let d = start_daemon();
    ok(&d, &["init"]);
    let dir = d._dir.path();
    std::fs::write(dir.join("pw.txt"), "s3cret").unwrap();
    let compose = dir.join("compose.yaml");
    let app = r#"
services:
  web:
    image: nginx:1.27
    ports: ["8080:80"]
    depends_on: [db]
    deploy: { replicas: 2 }
  db:
    image: postgres:17
    secrets: [db_pw]
    volumes: ["data:/var/lib/postgresql/data"]
volumes:
  data: {}
secrets:
  db_pw: { file: pw.txt }
"#;
    std::fs::write(&compose, app).unwrap();
    let file = compose.to_str().unwrap();

    // Something Legion cannot honour: a warning, or a refusal with --strict.
    let strict = legion(&d, &["stack", "deploy", "shop", "-c", file, "--strict"]);
    assert!(!strict.status.success());
    let stderr = String::from_utf8_lossy(&strict.stderr);
    assert!(
        stderr.contains("warning: services.web.depends_on"),
        "{stderr}"
    );
    assert!(
        !ok(&d, &["list", "services", "-n", "shop"]).contains("web"),
        "nothing deployed"
    );

    let deployed = ok(&d, &["stack", "deploy", "shop", "-c", file]);
    for line in [
        "Namespace/shop created",
        "Volume/shop/data created",
        "Secret/shop/db-pw created",
        "Service/shop/db created",
        "Service/shop/web created",
    ] {
        assert!(deployed.contains(line), "{line}\n{deployed}");
    }
    let yaml = ok(&d, &["get", "service", "web", "-n", "shop"]);
    assert!(yaml.contains("legion.stack: shop"), "{yaml}");
    assert!(yaml.contains("publishedPort: 8080"), "{yaml}");
    let again = ok(&d, &["stack", "deploy", "shop", "-c", file]);
    assert_eq!(again.matches("unchanged").count(), 5, "{again}");

    let converted = ok(&d, &["stack", "convert", "shop", "-c", file, "-n", "other"]);
    assert_eq!(converted.matches("kind: Service").count(), 2, "{converted}");
    assert!(converted.contains("namespace: other"), "{converted}");

    // Without db, --prune removes it.
    let web_only = app.replace("    depends_on: [db]\n", "");
    let web_only = &web_only[..web_only.find("  db:").unwrap()];
    std::fs::write(&compose, web_only).unwrap();
    let pruned = ok(&d, &["stack", "deploy", "shop", "-c", file, "--prune"]);
    assert!(pruned.contains("Service/shop/db deleted"), "{pruned}");
    let services = ok(&d, &["list", "services", "-n", "shop"]);
    assert!(
        services.contains("web") && !services.contains(" db "),
        "{services}"
    );

    // rm takes the services and secrets; volumes keep their data.
    let removed = ok(&d, &["stack", "rm", "shop"]);
    assert!(removed.contains("Service/shop/web deleted"), "{removed}");
    assert!(removed.contains("Secret/shop/db-pw deleted"), "{removed}");
    assert!(ok(&d, &["list", "volumes", "-n", "shop"]).contains("data"));
    assert!(!legion(&d, &["stack", "rm", "shop"]).status.success());
}

#[test]
fn a_second_manager_joins() {
    let first = start_daemon();
    ok(&first, &["init"]);
    let token = ok(&first, &["join-token", "manager"]).trim().to_owned();
    assert!(token.starts_with("LGN-1-"), "{token}");

    // The new node joins from an empty data directory, then starts.
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let out = Command::new(LEGION)
        .args([
            "join",
            &token,
            "--manager",
            &first.server,
            "--advertise",
            &addr,
        ])
        .args(["--node-name", "m2", "--data-dir"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("joined as manager m2"));
    // Joining again into the same directory is refused.
    let again = Command::new(LEGION)
        .args(["join", &token, "--manager", &first.server, "--data-dir"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!again.status.success());

    let second = start_daemon_in(dir, addr);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = ok(&second, &["status"]);
        if status.matches("voter").count() >= 2 && !status.contains("learner") {
            break;
        }
        assert!(Instant::now() < deadline, "{status}");
        std::thread::sleep(Duration::from_millis(200));
    }
    // The second manager's own admin credentials work against the first.
    let manifest = second._dir.path().join("ns.yaml");
    std::fs::write(
        &manifest,
        "apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: shop }\n",
    )
    .unwrap();
    ok(&second, &["apply", "-f", manifest.to_str().unwrap()]);
    assert!(ok(&first, &["get", "namespace", "shop"]).contains("name: shop"));
}
