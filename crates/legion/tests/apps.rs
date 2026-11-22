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

/// Whether something answers on `port` of this host, at its address: a
/// published port is taken where the node is reached, not on its own
/// loopback.
fn open(host: &str, port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("{host}:{port}").parse().unwrap(),
        std::time::Duration::from_secs(3),
    )
    .is_ok()
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
            &format!("{namespace} running and answering on {port}"),
            900,
            || running(&lab, namespace) && open(&lab.host, port),
        );
    }
    // Vaultwarden's web page; PostgreSQL taking its password.
    wait("Vaultwarden's page", 120, || {
        common::http_get(&lab.host, 28080)
            .is_ok_and(|r| r.starts_with("HTTP/1.1 200") || r.starts_with("HTTP/1.0 200"))
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
