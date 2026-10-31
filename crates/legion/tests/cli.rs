//! Runs the real binary: a daemon plus CLI commands against it.

use std::net::TcpListener;
use std::process::{Child, Command, Output};
use std::time::{Duration, Instant};

const LEGION: &str = env!("CARGO_BIN_EXE_legion");

struct Daemon {
    child: Child,
    server: String,
    _dir: tempfile::TempDir,
}

impl Daemon {
    /// Kills the daemon, as a crash would, and hands back its data
    /// directory.
    fn kill(mut self) -> tempfile::TempDir {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::mem::replace(&mut self._dir, tempfile::tempdir().unwrap())
    }
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
    // Another test may take the free port before the daemon listens on it;
    // the daemon then exits, and another port is tried.
    for _ in 0..5 {
        let mut daemon = spawn_daemon(tempfile::tempdir().unwrap(), free_addr());
        match up(&mut daemon) {
            Ok(()) => return daemon,
            Err((true, _)) => continue,
            Err((false, log)) => panic!("daemon did not come up:\n{log}"),
        }
    }
    panic!("daemon exited on five ports");
}

fn start_daemon_in(dir: tempfile::TempDir, server: String) -> Daemon {
    let mut daemon = spawn_daemon(dir, server);
    wait_up(&mut daemon);
    daemon
}

/// Starts a daemon without waiting for it to answer.
fn spawn_daemon(dir: tempfile::TempDir, server: String) -> Daemon {
    // Appended to across restarts, and shown if the daemon does not come up.
    let log_path = dir.path().join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let child = Command::new(LEGION)
        .args(["daemon", "--no-agent", "--listen", &server, "--data-dir"])
        .arg(dir.path())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    Daemon {
        child,
        server,
        _dir: dir,
    }
}

/// Waits for the daemon to answer; shows its log if it does not.
fn wait_up(daemon: &mut Daemon) {
    if let Err((_, log)) = up(daemon) {
        panic!("daemon did not come up:\n{log}");
    }
}

/// Waits for the daemon to answer. Otherwise the tail of its log, and
/// whether it exited rather than staying silent.
fn up(daemon: &mut Daemon) -> Result<(), (bool, String)> {
    let tail = |daemon: &Daemon| {
        let log = std::fs::read_to_string(daemon._dir.path().join("daemon.log"));
        let log = log.unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(30).collect();
        tail.into_iter().rev().collect::<Vec<_>>().join("\n")
    };
    // Generous: the first start of a freshly built binary can be slow (on
    // Windows, while it is scanned).
    let deadline = Instant::now() + Duration::from_secs(60);
    while !legion(daemon, &["status"]).status.success() {
        if daemon.child.try_wait().unwrap().is_some() {
            return Err((true, tail(daemon)));
        }
        if Instant::now() >= deadline {
            return Err((false, tail(daemon)));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
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
    // The namespace is left alone once it exists.
    assert_eq!(again.matches("unchanged").count(), 4, "{again}");

    let converted = ok(&d, &["stack", "convert", "shop", "-c", file, "-n", "other"]);
    assert_eq!(converted.matches("kind: Service").count(), 2, "{converted}");
    assert!(converted.contains("namespace: other"), "{converted}");

    // Without db, --prune removes it, and its secret, which the file no
    // longer has; the volume keeps its data.
    let web_only = app.replace("    depends_on: [db]\n", "");
    let web_only = &web_only[..web_only.find("  db:").unwrap()];
    std::fs::write(&compose, web_only).unwrap();
    let pruned = ok(&d, &["stack", "deploy", "shop", "-c", file, "--prune"]);
    assert!(pruned.contains("Service/shop/db deleted"), "{pruned}");
    assert!(pruned.contains("Secret/shop/db-pw deleted"), "{pruned}");
    assert!(!pruned.contains("Volume/shop/data deleted"), "{pruned}");
    let services = ok(&d, &["list", "services", "-n", "shop"]);
    assert!(
        services.contains("web") && !services.contains(" db "),
        "{services}"
    );

    // rm takes the services and secrets; volumes keep their data.
    let removed = ok(&d, &["stack", "rm", "shop"]);
    assert!(removed.contains("Service/shop/web deleted"), "{removed}");
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

/// Runs a command with another identity.
fn legion_as(d: &Daemon, identity: &std::path::Path, args: &[&str]) -> Output {
    Command::new(LEGION)
        .arg("--server")
        .arg(&d.server)
        .arg("--identity")
        .arg(identity)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn users_get_their_roles() {
    let d = start_daemon();
    ok(&d, &["init"]);
    let dir = d._dir.path();
    let pem = dir.join("alice.pem");
    let pem_arg = pem.to_str().unwrap();
    let made = ok(
        &d,
        &[
            "user", "create", "alice", "--role", "edit", "-n", "shop", "-o", pem_arg,
        ],
    );
    assert!(made.contains("user alice's credentials"), "{made}");
    assert!(made.contains("RoleBinding/shop/alice created"), "{made}");
    // Never overwritten.
    let again = legion(&d, &["user", "create", "alice", "-o", pem_arg]);
    assert!(!again.status.success());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&pem).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let manifest = dir.join("web.yaml");
    std::fs::write(
        &manifest,
        "apiVersion: legion/v1\nkind: Service\nmetadata: { name: web, namespace: shop }\n\
         spec: { pod: { containers: [{ name: web, image: \"web:1\" }] } }\n",
    )
    .unwrap();
    let applied = legion_as(&d, &pem, &["apply", "-f", manifest.to_str().unwrap()]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let listed = legion_as(&d, &pem, &["list", "services", "-n", "shop"]);
    assert!(String::from_utf8_lossy(&listed.stdout).contains("web"));
    // A stack too, though the user may not create its namespace.
    let compose = dir.join("compose.yaml");
    std::fs::write(&compose, "services:\n  api:\n    image: api:1\n").unwrap();
    let deployed = legion_as(
        &d,
        &pem,
        &["stack", "deploy", "shop", "-c", compose.to_str().unwrap()],
    );
    let stdout = String::from_utf8_lossy(&deployed.stdout);
    assert!(
        deployed.status.success(),
        "{}",
        String::from_utf8_lossy(&deployed.stderr)
    );
    assert!(stdout.contains("Service/shop/api created"), "{stdout}");
    // Elsewhere, and on the cluster itself, nothing.
    for args in [
        &["list", "services", "-n", "default"][..],
        &["list", "nodes"],
        &["join-token", "worker"],
        &["user", "create", "mallory", "-o", "unused.pem"],
    ] {
        let out = legion_as(&d, &pem, args);
        assert!(!out.status.success(), "{args:?} was allowed");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("user alice"), "{args:?}: {stderr}");
    }
}

/// `legion restore` in `dir`.
fn restore(dir: &std::path::Path, file: Option<&std::path::Path>) -> Output {
    let mut cmd = Command::new(LEGION);
    cmd.arg("restore").arg("--data-dir").arg(dir);
    if let Some(file) = file {
        cmd.arg(file);
    }
    cmd.output().unwrap()
}

fn namespace(d: &Daemon, name: &str) {
    let manifest = d._dir.path().join(format!("{name}.yaml"));
    std::fs::write(
        &manifest,
        format!("apiVersion: legion/v1\nkind: Namespace\nmetadata: {{ name: {name} }}\n"),
    )
    .unwrap();
    ok(d, &["apply", "-f", manifest.to_str().unwrap()]);
}

fn voters(d: &Daemon) -> usize {
    ok(d, &["status"]).matches("voter").count()
}

#[test]
fn a_manager_is_restored_from_a_backup_and_from_its_own_store() {
    let first = start_daemon();
    ok(&first, &["init"]);
    let token = ok(&first, &["join-token", "manager"]).trim().to_owned();
    namespace(&first, "kept");
    let backups = tempfile::tempdir().unwrap();
    let file = backups.path().join("backup.json");
    let out = ok(&first, &["backup", "-o", file.to_str().unwrap()]);
    assert!(out.contains("backed up"), "{out}");
    namespace(&first, "lost");

    let busy = restore(first._dir.path(), Some(&file));
    assert!(!busy.status.success(), "restored under a running daemon");
    assert!(
        String::from_utf8_lossy(&busy.stderr).contains("stop the daemon"),
        "{}",
        String::from_utf8_lossy(&busy.stderr)
    );
    let server = first.server.clone();
    let dir = first.kill();
    let out = restore(dir.path(), Some(&file));
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let first = start_daemon_in(dir, server);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !legion(&first, &["get", "namespace", "kept"])
        .status
        .success()
    {
        assert!(
            Instant::now() < deadline,
            "the restored manager does not serve"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        !legion(&first, &["get", "namespace", "lost"])
            .status
            .success()
    );
    assert_eq!(voters(&first), 1);

    // The backed-up token lets a manager join the restored cluster.
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
    let second = start_daemon_in(dir, addr);
    let deadline = Instant::now() + Duration::from_secs(30);
    while voters(&first) < 2 {
        assert!(Instant::now() < deadline, "{}", ok(&first, &["status"]));
        std::thread::sleep(Duration::from_millis(200));
    }
    namespace(&second, "joined");

    // The second manager is lost: the first restarts the cluster from its
    // own store, with everything written so far.
    drop(second);
    let server = first.server.clone();
    let dir = first.kill();
    let out = restore(dir.path(), None);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let first = start_daemon_in(dir, server);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !legion(&first, &["get", "namespace", "joined"])
        .status
        .success()
    {
        assert!(
            Instant::now() < deadline,
            "the restarted manager does not serve"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(voters(&first), 1);
    let kept = std::fs::read_dir(first._dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("store.redb.")
        })
        .count();
    assert_eq!(kept, 2, "both old stores kept");
}

#[test]
fn a_volume_is_asked_to_move() {
    let d = start_daemon();
    ok(&d, &["init"]);
    let manifest = d._dir.path().join("vol.yaml");
    std::fs::write(
        &manifest,
        "apiVersion: legion/v1\nkind: Volume\nmetadata: { name: cache }\nspec: { size: 1Gi }\n",
    )
    .unwrap();
    ok(&d, &["apply", "-f", manifest.to_str().unwrap()]);
    let out = ok(&d, &["volume", "move", "cache", "n2"]);
    assert!(out.contains("moves to n2"), "{out}");
    let out = ok(&d, &["volume", "move", "cache", "n2"]);
    assert!(out.contains("already"), "{out}");
    let got = ok(&d, &["get", "volume", "cache"]);
    assert!(
        got.contains("node: n2") && got.contains("size: 1Gi"),
        "{got}"
    );
    let bad = legion(&d, &["volume", "move", "cache", "Not_A_Node"]);
    assert!(!bad.status.success());
}

#[test]
fn the_daemon_seals_its_store() {
    let d = start_daemon();
    ok(&d, &["init"]);
    let manifest = d._dir.path().join("secret.yaml");
    std::fs::write(
        &manifest,
        "apiVersion: legion/v1\nkind: Secret\nmetadata: { name: db }\nspec: { text: correct-horse-battery }\n",
    )
    .unwrap();
    ok(&d, &["apply", "-f", manifest.to_str().unwrap()]);
    assert!(d._dir.path().join("pki/store.key").exists());
    let dir = d.kill();
    let store = std::fs::read(dir.path().join("store.redb")).unwrap();
    // The text, and its base64 as the Secret's spec holds it.
    for needle in ["correct-horse-battery", "Y29ycmVjdC1ob3JzZS1iYXR0ZXJ5"] {
        assert!(
            !store.windows(needle.len()).any(|w| w == needle.as_bytes()),
            "{needle} in the store"
        );
    }
}

/// `legion unlock` against `d` with `key` on stdin.
fn unlock(d: &Daemon, key: &str) -> Output {
    use std::io::Write;
    let mut child = Command::new(LEGION)
        .args(["--server", &d.server, "unlock"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(child.stdin.take().unwrap(), "{key}").unwrap();
    child.wait_with_output().unwrap()
}

/// Waits until the daemon's private keys are sealed, or all in the clear.
fn wait_sealed(d: &Daemon, sealed: bool) {
    let pki = d._dir.path().join("pki");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = ["ca.key", "node.key", "store.key"].iter().all(|f| {
            pki.join(format!("{f}.sealed")).exists() == sealed && pki.join(f).exists() != sealed
        });
        if done {
            return;
        }
        assert!(Instant::now() < deadline, "keys not sealed={sealed}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Waits for a locked daemon to listen, and checks a wrong key fails.
fn refuses(d: &Daemon, key: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let out = unlock(d, key);
        let err = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(!out.status.success(), "unlocked with {key}");
        if err.contains("does not unlock") {
            return;
        }
        assert!(Instant::now() < deadline, "{err}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn autolock_seals_the_keys_and_a_restart_waits_for_the_unlock_key() {
    let d = start_daemon();
    ok(&d, &["init"]);
    namespace(&d, "kept");
    assert!(ok(&d, &["autolock", "show"]).contains("autolock is off"));
    let key = ok(&d, &["autolock", "enable"]).trim().to_owned();
    assert!(key.starts_with("LGN-UNLOCK-"), "{key}");
    assert_eq!(ok(&d, &["autolock", "enable"]).trim(), key, "kept");
    assert_eq!(ok(&d, &["autolock", "show"]).trim(), key);
    wait_sealed(&d, true);

    // Restarted, it serves nothing but Unlock until given the key.
    let server = d.server.clone();
    let dir = d.kill();
    let mut d = spawn_daemon(dir, server);
    let wrong = format!("LGN-UNLOCK-{}", "A".repeat(43));
    refuses(&d, &wrong);
    assert!(!legion(&d, &["status"]).status.success(), "locked");
    let out = unlock(&d, &key);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    wait_up(&mut d);
    assert!(ok(&d, &["get", "namespace", "kept"]).contains("name: kept"));

    // Rotated, the old key no longer opens it.
    let rotated = ok(&d, &["autolock", "rotate"]).trim().to_owned();
    assert_ne!(rotated, key);
    let sealed_at = std::fs::read(d._dir.path().join("pki/store.key.sealed")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::read(d._dir.path().join("pki/store.key.sealed")).unwrap() == sealed_at {
        assert!(Instant::now() < deadline, "not resealed");
        std::thread::sleep(Duration::from_millis(200));
    }
    let server = d.server.clone();
    let dir = d.kill();
    let mut d = spawn_daemon(dir, server);
    refuses(&d, &key);
    assert!(unlock(&d, &rotated).status.success());
    wait_up(&mut d);

    // Disabled, the keys are in the clear again and a restart needs none.
    assert!(ok(&d, &["autolock", "disable"]).contains("autolock is off"));
    wait_sealed(&d, false);
    let server = d.server.clone();
    let dir = d.kill();
    let d = start_daemon_in(dir, server);
    assert!(ok(&d, &["get", "namespace", "kept"]).contains("name: kept"));
}

#[test]
fn every_manager_seals_its_keys_once_autolock_is_on() {
    let first = start_daemon();
    ok(&first, &["init"]);
    let token = ok(&first, &["join-token", "manager"]).trim().to_owned();
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
    let second = start_daemon_in(dir, addr);
    let deadline = Instant::now() + Duration::from_secs(30);
    while voters(&first) < 2 {
        assert!(Instant::now() < deadline, "{}", ok(&first, &["status"]));
        std::thread::sleep(Duration::from_millis(200));
    }
    // Enabled through the second, which forwards to the leader.
    let key = ok(&second, &["autolock", "enable"]).trim().to_owned();
    wait_sealed(&first, true);
    wait_sealed(&second, true);
    assert_eq!(ok(&first, &["autolock", "show"]).trim(), key);
}
