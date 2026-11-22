//! Ready-to-deploy applications (TODO.MD item 5): three apps of the
//! daemonless catalog, each deployed with `legion app deploy` and its
//! defaults, reached where they publish. A web app (Vaultwarden), a
//! database that needs SysV IPC (PostgreSQL, its namespace made allowing
//! it) and a store (Redis), so what an app's manifest asks for is honoured.
//!
//! It drives a real daemon, so it is ignored by default and run by hand on
//! a FreeBSD host as root, with internet access (the catalog, and the
//! images from ghcr.io), and `LEGION_CLUSTER_LAB` set as `tests/common`
//! says.

mod common;

use common::{Lab, text, wait};

/// `legion app deploy APP --set …`, its output.
fn deploy(lab: &Lab, app: &str, set: &[&str]) -> String {
    let mut args = vec!["app", "deploy", app];
    for kv in set {
        args.extend(["--set", kv]);
    }
    lab.ok(&args)
}

fn running(lab: &Lab, namespace: &str) -> bool {
    lab.list("tasks", namespace)
        .iter()
        .any(|t| t["status"]["state"] == "running")
}

/// Whether something answers on `addr` (`ip:port`).
fn open(addr: &str) -> bool {
    addr.parse().is_ok_and(|a| {
        std::net::TcpStream::connect_timeout(&a, std::time::Duration::from_secs(3)).is_ok()
    })
}

/// Where an app answers from this host: its running task's address and
/// the container port behind `published`, which its Service must publish.
/// Not the published port itself: pf's `rdr` does not take what a host
/// sends itself (DESIGN.md §5.4), so a node's own published ports are
/// reached from another, and this test has one host.
fn reached(lab: &Lab, namespace: &str, published: u64) -> Option<String> {
    let service = lab.list("services", namespace).into_iter().next()?;
    let port = service["spec"]["endpoint"]["ports"]
        .as_array()?
        .iter()
        .find(|p| p["publishedPort"].as_u64() == Some(published))?
        .clone();
    // The container port: a number, or a container's port by its name.
    let target = match &port["targetPort"] {
        serde_json::Value::Number(n) => n.as_u64()?,
        serde_json::Value::String(name) => service["spec"]["pod"]["containers"]
            .as_array()?
            .iter()
            .flat_map(|c| c["ports"].as_array().cloned().unwrap_or_default())
            .find(|p| p["name"] == name.as_str())?["containerPort"]
            .as_u64()?,
        _ => port["port"].as_u64()?,
    };
    let ip = lab
        .list("tasks", namespace)
        .into_iter()
        .find(|t| t["status"]["state"] == "running")?["status"]["ip"]
        .as_str()?
        .to_owned();
    Some(format!("{ip}:{target}"))
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD and the internet; see the module docs"]
fn apps_of_the_catalog_deploy_and_answer() {
    let mut lab = Lab::local();
    lab.start_host(&[]);
    lab.ok(&["init"]);

    let listed = lab.ok(&["app", "list"]);
    for app in ["vaultwarden", "postgres", "redis"] {
        assert!(
            listed.lines().any(|l| l.starts_with(app)),
            "{app} listed:\n{listed}"
        );
    }
    let shown = lab.ok(&["app", "show", "postgres"]);
    assert!(
        shown.contains("POSTGRES_PASSWORD") && shown.contains("(made up)"),
        "{shown}"
    );

    let pg = deploy(&lab, "postgres", &["WEB_PORT=25432"]);
    let password = pg
        .lines()
        .find_map(|l| l.strip_prefix("POSTGRES_PASSWORD="))
        .and_then(|l| l.split_whitespace().next())
        .expect("the password made up, said")
        .to_owned();
    let ns = lab.ok(&["get", "namespace", "postgres", "-o", "yaml"]);
    assert!(
        ns.contains("sysvipc"),
        "its namespace allows what it asks for: {ns}"
    );
    deploy(&lab, "redis", &["WEB_PORT=26379"]);
    deploy(&lab, "vaultwarden", &["WEB_PORT=28080"]);

    for (namespace, port) in [
        ("postgres", 25432),
        ("redis", 26379),
        ("vaultwarden", 28080),
    ] {
        lab.wait_or_report(
            &format!("{namespace} running, published on {port} and answering"),
            900,
            || running(&lab, namespace) && reached(&lab, namespace, port).is_some_and(|a| open(&a)),
        );
    }
    // Vaultwarden's web page (a 200); PostgreSQL taking its password.
    let web = reached(&lab, "vaultwarden", 28080).unwrap();
    let (ip, port) = web.rsplit_once(':').unwrap();
    wait("Vaultwarden's page", 120, || {
        common::http_get(ip, port.parse().unwrap()).is_ok()
    });
    let task = lab
        .list("tasks", "postgres")
        .into_iter()
        .find(|t| t["status"]["state"] == "running")
        .and_then(|t| t["metadata"]["name"].as_str().map(str::to_owned))
        .unwrap();
    let said = lab.legion(
        &[
            "exec",
            &task,
            "-n",
            "postgres",
            "--",
            "env",
            &format!("PGPASSWORD={password}"),
            "psql",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
            "-Atc",
            "select 41 + 1",
        ],
        &[],
    );
    assert_eq!(text(&said.stdout).trim(), "42", "{}", text(&said.stderr));
}
