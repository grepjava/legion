//! Memory limits on a database (DESIGN.md §6.3). Postgres's backends map
//! its shared buffers, which rctl counts once for each process; Legion
//! counts every page once. Under a 256 MiB limit, with 128 MiB of shared
//! buffers:
//!
//! - sixteen sessions reading a table held in the buffers leave Postgres
//!   alone, while rctl, for the same jail, counts far past the limit;
//! - eight sessions each holding 50 MiB take it past the limit together,
//!   with every process under it, and the agent kills the largest
//!   backend; Postgres recovers;
//! - one session taking 300 MiB alone is killed (by the agent or by
//!   rctl's rule for each process, whichever reads it first).
//!
//! It drives the real daemon on one node, so it is ignored by default and
//! run by hand on a FreeBSD host as root, with the lab of `tests/common`
//! ([`Lab::local`]) and the Postgres image of `tests/stateful.rs`:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lmem \
//!     LEGION_PG_IMAGE=/var/tmp/legion-pg.oci.tar \
//!     target/debug/deps/memory-<hash> --ignored --nocapture
//! ```

mod common;

use std::process::{Command, Output};

use common::{Lab, ZFS_ROOT, text, wait};
use serde_json::Value;

const APP: &str = r#"
apiVersion: legion/v1
kind: Namespace
metadata: { name: data }
spec: { security: { allow: [sysvipc] } }
---
apiVersion: legion/v1
kind: Config
metadata: { name: pg-sh, namespace: data }
spec:
  text: |
    D=/var/db/postgres/data
    if [ ! -s $D/PG_VERSION ]; then
      initdb -D $D -A trust -U postgres >/dev/null || exit 1
    fi
    exec postgres -D $D -c listen_addresses='*' -c shared_buffers=128MB \
      -c max_connections=40 -c log_destination=stderr
---
apiVersion: legion/v1
kind: Service
metadata: { name: db, namespace: data }
spec:
  replicas: 1
  pod:
    containers:
      - name: pg
        image: localhost/legion-pg:17
        command: [/bin/sh, /etc/legion/pg.sh]
        configs: [{ name: pg-sh, target: /etc/legion/pg.sh }]
        security: { allow: [sysvipc] }
        resources: { limits: { memory: 256Mi } }
        probes:
          readiness: { tcp: { port: 5432 }, period: 2s }
"#;

const MIB: u64 = 1 << 20;

/// The running task of `db`.
fn task(lab: &Lab) -> Option<Value> {
    lab.list("tasks", "data")
        .into_iter()
        .find(|t| t["spec"]["service"] == "db" && t["status"]["state"] == "running")
}

/// `query` through `psql` in the task.
fn sql(lab: &Lab, task: &str, query: &str) -> Output {
    lab.legion(
        &[
            "exec",
            task,
            "-n",
            "data",
            "--",
            "psql",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
            "-Atc",
            query,
        ],
        &[],
    )
}

/// `query` in `n` sessions at once; how many failed.
fn sessions(lab: &Lab, task: &str, n: usize, query: &str) -> usize {
    std::thread::scope(|s| {
        let runs: Vec<_> = (0..n)
            .map(|_| s.spawn(|| sql(lab, task, query).status.success()))
            .collect();
        runs.into_iter()
            .map(|r| r.join().unwrap_or(false))
            .filter(|ok| !ok)
            .count()
    })
}

/// The jail of the container named `name`, from the daemon's records.
fn jail_of(lab: &Lab, name: &str) -> String {
    for d in std::fs::read_dir(lab.dir.join("data/jails")).unwrap() {
        let path = d.unwrap().path().join("record.json");
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let r: Value = serde_json::from_slice(&bytes).unwrap();
        if r["name"] == name {
            return r["jail"].as_str().unwrap().to_owned();
        }
    }
    panic!("no container {name}");
}

/// rctl's `memoryuse` for a jail.
fn rctl_memory(jail: &str) -> u64 {
    let out = Command::new("rctl")
        .args(["-u", &format!("jail:{jail}")])
        .output()
        .unwrap();
    text(&out.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("memoryuse=")?.trim().parse().ok())
        .unwrap_or(0)
}

/// How often the daemon's log says the agent killed for memory.
fn agent_kills(lab: &Lab) -> usize {
    std::fs::read_to_string(lab.dir.join("daemon.log"))
        .unwrap_or_default()
        .matches("killing its largest process")
        .count()
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, and the image; see the module docs"]
fn postgres_under_a_memory_limit() {
    let image = std::env::var("LEGION_PG_IMAGE").expect("set LEGION_PG_IMAGE");
    let mut lab = Lab::local();
    let step = |what: &str| eprintln!("--- {what}");

    step("one node, Postgres with 128 MiB of buffers under a 256 MiB limit");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let out = Command::new(common::LEGION)
        .args([
            "image",
            "load",
            &image,
            "--zfs-root",
            ZFS_ROOT,
            "--data-dir",
        ])
        .arg(lab.dir.join("data"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "loading the image: {}",
        text(&out.stderr)
    );
    let app = lab.dir.join("app.yaml");
    std::fs::write(&app, APP).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    let ready = || {
        task(&lab).is_some_and(|t| {
            t["status"]["ready"] == true
                && sql(&lab, t["metadata"]["name"].as_str().unwrap(), "select 1")
                    .status
                    .success()
        })
    };
    wait("Postgres ready", 300, ready);
    let t = task(&lab).unwrap();
    let name = t["metadata"]["name"].as_str().unwrap().to_owned();
    let jail = jail_of(&lab, &format!("data_{name}_pg"));

    step("sixteen sessions reading 30 MiB of buffers");
    // Under a quarter of the buffers, so scans use the buffers themselves
    // rather than a small ring of them.
    let out = sql(
        &lab,
        &name,
        "create table t as select g, repeat('x', 200) as pad from generate_series(1, 120000) g; \
         select count(*) from t",
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let rctl = std::cell::Cell::new(0);
    let failed = std::thread::scope(|s| {
        let reading = s.spawn(|| {
            sessions(
                &lab,
                &name,
                16,
                "select count(*) from t; select count(*) from t; select pg_sleep(6)",
            )
        });
        // While they sleep, holding what they read.
        std::thread::sleep(std::time::Duration::from_secs(4));
        rctl.set(rctl_memory(&jail));
        reading.join().unwrap()
    });
    eprintln!("rctl's memoryuse while reading: {} MiB", rctl.get() / MIB);
    assert_eq!(failed, 0, "sessions failed");
    assert!(rctl.get() > 256 * MIB, "rctl {} MiB", rctl.get() / MIB);
    assert_eq!(agent_kills(&lab), 0);
    let now = task(&lab).unwrap();
    assert_eq!(now["metadata"]["name"], name.as_str());
    assert_eq!(now["status"]["restarts"], 0);

    step("eight sessions holding 50 MiB each");
    let failed = sessions(
        &lab,
        &name,
        8,
        "select pg_sleep(5) from (select repeat('x', 50000000) r offset 0) s \
         where length(r) > 0",
    );
    eprintln!("{failed} of 8 sessions failed");
    assert!(failed > 0, "no session failed");
    assert!(agent_kills(&lab) > 0, "the agent killed nothing");
    wait("Postgres back", 120, ready);

    step("one session taking 300 MiB");
    let kills = agent_kills(&lab);
    let failed = sessions(&lab, &name, 1, "select length(repeat('x', 300000000))");
    assert_eq!(failed, 1, "the session ran");
    wait("Postgres back", 120, ready);
    eprintln!(
        "agent kills during it: {} (rctl's are not logged)",
        agent_kills(&lab) - kills
    );
    // Postgres lost backends, never itself: the container never restarted.
    let now = task(&lab).unwrap();
    assert_eq!(now["metadata"]["name"], name.as_str());
    assert_eq!(now["status"]["restarts"], 0);
}
