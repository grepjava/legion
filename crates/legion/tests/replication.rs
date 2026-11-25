//! A replicated database (DESIGN.md §6.9) on real jails, PostgreSQL 17
//! and Legion's PostgreSQL plugin: two members on the two VMs (`n1`
//! paused), one synchronous. The first primary is made, the replica copied
//! from it, rows written through the Service's address reach the replica,
//! and `<service>-read` answers from it. Then the primary's node is cut off,
//! its daemon running: once its member has been unhealthy for
//! `failoverAfter` and its task surely stopped, the replica is promoted
//! with every row, the Service's address follows, and writes go on. Mended,
//! the old primary comes back as a replica of the new one (pg_rewind) and
//! has the rows written meanwhile. A switchover hands the primary back,
//! losing nothing. A rolling update then replaces the replica first and
//! switches over before replacing the primary. Last, the daemon of the
//! primary's node is killed: the container's supervisor stops PostgreSQL
//! once the node's lease runs out (self-fencing, the watchdog), before
//! the replica is promoted; the daemon back, the old primary rejoins.
//! All along, the log is archived to a bucket (MinIO on n3): its segments
//! and each new timeline's history are there at the end. Then `legion
//! replication check` runs the plugin through the contract with three
//! members in a namespace of its own.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root, with the Postgres image of
//! `tests/stateful/Containerfile` in `LEGION_PG_IMAGE`, as `stateful.rs`
//! is, and the plugin (`legion-pg`) built in `LEGION_PG_PLUGIN`, which it
//! installs on every node.

mod common;

use common::{Lab, Role, text, vm, wait};
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PLUGIN_DIR: &str = "/usr/local/libexec/legion/replication";
/// MinIO on n3, the bucket the database's log is archived to.
const S3_DIR: &str = "/var/tmp/legion-lab-wal";
const S3_USER: &str = "legionlab";
const S3_PASSWORD: &str = "legionlab-secret";
const S3_BUCKET: &str = "legion-wal";

const APP: &str = r#"
apiVersion: legion/v1
kind: Namespace
metadata: { name: data }
spec: { security: { allow: [sysvipc] } }
---
apiVersion: legion/v1
kind: Secret
metadata: { name: pg-replication, namespace: data }
spec: { text: lab-replication-secret }
---
apiVersion: legion/v1
kind: Secret
metadata: { name: pg-password, namespace: data }
spec: { text: lab-superuser-secret }
---
apiVersion: legion/v1
kind: Service
metadata: { name: pg, namespace: data }
spec:
  mode: stateful
  replicas: 2
  replication:
    plugin: postgres
    failoverAfter: 10s
    synchronous: 1
    read: true
    archive: { target: pg-wal }
    options:
      pgdata: /var/db/pg/data
      replicationPasswordFile: /run/secrets/pg-replication
      superuserPasswordFile: /run/secrets/pg-password
  endpoint: { ports: [{ port: 5432 }] }
  pod:
    onPartition: stop
    stopGracePeriod: 5s
    initContainers:
      - name: own
        image: localhost/legion-pg:17
        user: root
        command: [/usr/bin/install, -d, -o, postgres, -m, "700", /var/db/pg/data]
        volumeMounts: [{ name: pg, path: /var/db/pg }]
    containers:
      - name: pg
        image: localhost/legion-pg:17
        command: [postgres, -D, /var/db/pg/data, -c, shared_buffers=16MB,
                  -c, max_connections=30, -c, log_destination=stderr]
        ports: [{ containerPort: 5432 }]
        secrets: [{ name: pg-replication }, { name: pg-password }]
        volumeMounts: [{ name: pg, path: /var/db/pg }]
        security: { allow: [sysvipc] }
        probes:
          readiness: { tcp: { port: 5432 }, period: 2s }
    volumes: [{ name: pg, claim: { size: 512Mi } }]
"#;

/// The plugin, built for this host: `legion-pg` of the tree under test,
/// as the lab's setup builds it.
fn plugin() -> String {
    let path = std::env::var("LEGION_PG_PLUGIN").expect("set LEGION_PG_PLUGIN");
    assert!(std::path::Path::new(&path).is_file(), "no plugin at {path}");
    path
}

impl Lab {
    /// Installs the plugin on this host and both VMs.
    fn install_plugin(&self, built: &str) {
        let here = std::process::Command::new("install")
            .args(["-d", PLUGIN_DIR])
            .status()
            .unwrap();
        assert!(here.success());
        let status = std::process::Command::new("install")
            .args(["-m", "755", built, &format!("{PLUGIN_DIR}/postgres")])
            .status()
            .unwrap();
        assert!(status.success(), "installing the plugin here");
        for v in &self.vms {
            self.copy(v, built, "/tmp/legion-replication-postgres");
            let out = self.ssh(
                v,
                &format!(
                    "install -d {PLUGIN_DIR} && install -m 755 /tmp/legion-replication-postgres \
                     {PLUGIN_DIR}/postgres && rm /tmp/legion-replication-postgres"
                ),
            );
            assert!(out.status.success(), "{}", text(&out.stderr));
        }
    }

    fn pg(&self) -> Value {
        self.list("services", "data")
            .into_iter()
            .find(|s| s["metadata"]["name"] == "pg")
            .unwrap_or_default()
    }

    fn repl(&self) -> Value {
        self.pg()["status"]["replication"].clone()
    }

    /// The running task of member `n`.
    fn member(&self, n: u64) -> Option<Value> {
        self.list("tasks", "data").into_iter().find(|t| {
            t["spec"]["service"] == "pg"
                && t["spec"]["slot"] == n
                && t["spec"]["desiredState"] == "running"
                && t["status"]["state"] == "running"
        })
    }

    /// `query` on member `n`, over its own socket, as the superuser.
    fn psql(&self, n: u64, query: &str) -> Option<String> {
        let t = self.member(n)?;
        let task = t["metadata"]["name"].as_str()?.to_owned();
        let out = self.legion(
            &[
                "exec", &task, "-n", "data", "--", "psql", "-h", "/tmp", "-U", "postgres", "-d",
                "postgres", "-Atc", query,
            ],
            &[],
        );
        out.status
            .success()
            .then(|| text(&out.stdout).trim().to_owned())
    }

    /// `query` through an address of the Service, from member `from`'s pod:
    /// `pg` for writes, `pg-read` for reads.
    fn through(&self, from: u64, name: &str, query: &str) -> Option<String> {
        let t = self.member(from)?;
        let task = t["metadata"]["name"].as_str()?.to_owned();
        let out = self.legion(
            &[
                "exec",
                &task,
                "-n",
                "data",
                "--",
                "env",
                "PGPASSWORD=lab-superuser-secret",
                "psql",
                "-h",
                &format!("{name}.data"),
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-Atc",
                query,
            ],
            &[],
        );
        out.status
            .success()
            .then(|| text(&out.stdout).trim().to_owned())
    }

    fn rows_on(&self, n: u64) -> Option<u64> {
        self.psql(n, "select count(*) from orders")?.parse().ok()
    }

    fn dump_pg(&self) {
        self.daemon_logs(&["replication", "prepare", "diverged"], 40);
        for v in &self.vms {
            let out = self.ssh(
                v,
                "ps -axww -o pid,jid,etime,command | grep -E 'postgres|legion-replication' | grep -v grep",
            );
            eprintln!(
                "=== processes on {}
{}",
                v.name,
                text(&out.stdout)
            );
        }
        eprintln!("replication: {}", self.repl());
        eprintln!(
            "{}",
            text(
                &self
                    .legion(&["replication", "status", "pg", "-n", "data"], &[])
                    .stdout
            )
        );
        for t in self.list("tasks", "data") {
            let name = t["metadata"]["name"].as_str().unwrap_or_default();
            eprintln!(
                "task {name} on {} {} {} {}",
                t["spec"]["node"],
                t["spec"]["desiredState"],
                t["status"]["state"],
                t["status"]["message"]
            );
            if t["status"]["state"] == "running" {
                let out = self.legion(&["logs", name, "-n", "data", "--tail", "30"], &[]);
                eprintln!("{}{}", text(&out.stdout), text(&out.stderr));
            }
        }
    }

    fn waited_pg(&self, what: &str, secs: u64, f: impl FnMut() -> bool) {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wait(what, secs, f)));
        if let Err(e) = r {
            self.dump_pg();
            std::panic::resume_unwind(e);
        }
    }
}

/// Stops MinIO on n3 and removes its bucket, however the test ends.
struct Minio<'a>(&'a Lab);

impl Drop for Minio<'_> {
    fn drop(&mut self) {
        let _ = self.0.on(
            "n3",
            &format!("pkill -f 'minio server {S3_DIR}'; sleep 1; rm -rf {S3_DIR} /root/.mc; true"),
        );
    }
}

fn cut_off(host: &str, other: &str) -> String {
    format!(
        "pfctl -si | grep -q 'Status: Enabled' || {{ echo 'pf is not enabled' >&2; exit 1; }}
pfctl -a legion/labcut -f - << 'RULES'
pass in quick proto tcp from {host} to any port 22 no state
pass out quick proto tcp from any port 22 to {host} no state
block drop quick from {host} to any
block drop quick from any to {host}
block drop quick from {other} to any
block drop quick from any to {other}
RULES
pfctl -q -F states
"
    )
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and the image; see the module docs"]
fn a_replicated_database_fails_over_and_its_old_primary_rejoins() {
    let image = std::env::var("LEGION_PG_IMAGE").expect("set LEGION_PG_IMAGE");
    let built = plugin();
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("a manager and two workers, the plugin on each; the manager paused");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(0, &worker, Role::Worker);
    lab.join(1, &worker, Role::Worker);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });
    lab.install_plugin(&built);
    lab.load_image(&image);
    lab.ok(&["node", "pause", "n1"]);

    step("MinIO on n3, the bucket the log is archived to");
    let _minio = Minio(&lab);
    let mc = "$(command -v minio-client || command -v mc)";
    let out = lab.on(
        "n3",
        &format!(
            "set -e
            command -v minio >/dev/null || ASSUME_ALWAYS_YES=yes pkg install -q minio minio-client >/dev/null
            mkdir -p {S3_DIR}
            MINIO_ROOT_USER={S3_USER} MINIO_ROOT_PASSWORD={S3_PASSWORD} \
                daemon -f minio server {S3_DIR} --address :9000 --quiet
            i=0
            until {mc} alias set wal http://127.0.0.1:9000 {S3_USER} {S3_PASSWORD} >/dev/null 2>&1; do
                i=$((i + 1)); [ $i -lt 60 ] || {{ echo 'MinIO did not start' >&2; exit 3; }}
                sleep 1
            done
            {mc} mb -p wal/{S3_BUCKET} >/dev/null"
        ),
    );
    assert!(out.status.success(), "MinIO on n3: {}", text(&out.stderr));
    let target = lab.dir.join("wal.yaml");
    std::fs::write(
        &target,
        format!(
            "apiVersion: legion/v1
kind: Secret
metadata: {{ name: s3-key, namespace: default }}
spec: {{ text: {S3_PASSWORD} }}
---
apiVersion: legion/v1
kind: BackupTarget
metadata: {{ name: pg-wal }}
spec:
  s3: {{ endpoint: \"http://{}:9000\", bucket: {S3_BUCKET}, prefix: lab, accessKeyId: {S3_USER}, secretKeySecret: {{ namespace: default, name: s3-key }} }}
",
            lab.vms[vm("n3")].addr
        ),
    )
    .unwrap();
    lab.ok(&["apply", "-f", target.to_str().unwrap()]);

    step("two members: the primary made, the replica copied from it");
    let app = lab.dir.join("pg.yaml");
    std::fs::write(&app, APP).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    lab.waited_pg("a healthy pair", 900, || {
        let r = lab.repl();
        r["phase"] == "healthy" && r["synchronous"] == serde_json::json!(["pg-1"])
    });
    let r = lab.repl();
    assert_eq!(
        (r["primary"].as_str(), r["epoch"].as_u64()),
        (Some("pg-0"), Some(1))
    );
    assert_eq!(
        lab.psql(1, "select pg_is_in_recovery()").as_deref(),
        Some("t")
    );

    step("rows written through the Service's address reach the replica");
    lab.through(
        1,
        "pg",
        "create table orders (id serial primary key, item text); \
         insert into orders (item) select 'o' || g from generate_series(1, 500) g",
    )
    .expect("writing through pg");
    assert_eq!(lab.rows_on(0), Some(500));
    // Synchronous: the replica has them once the insert returned.
    assert_eq!(lab.rows_on(1), Some(500));
    lab.waited_pg("reads through pg-read", 60, || {
        lab.through(0, "pg-read", "select pg_is_in_recovery()")
            .as_deref()
            == Some("t")
    });

    step("the primary's node cut off, its daemon running");
    let from = lab.member(0).unwrap()["spec"]["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let other = if from == "n2" { "n3" } else { "n2" };
    let host = lab.host.clone();
    let other_addr = lab.vms[vm(other)].addr.clone();
    let out = lab.on(&from, &cut_off(&host, &other_addr));
    assert!(
        out.status.success(),
        "cutting {from} off: {}",
        text(&out.stderr)
    );
    let cut = std::time::Instant::now();
    lab.waited_pg("pg-1 promoted", 300, || {
        let r = lab.repl();
        r["primary"] == "pg-1" && r["change"].is_null() && r["lastChange"]["to"] == "pg-1"
    });
    eprintln!("promoted {:?} after the cut", cut.elapsed());
    assert_eq!(lab.repl()["epoch"].as_u64(), Some(2));
    assert_eq!(
        lab.psql(1, "select pg_is_in_recovery()").as_deref(),
        Some("f")
    );
    assert_eq!(
        lab.rows_on(1),
        Some(500),
        "every row the old primary confirmed"
    );

    step("writes go on through the Service's address, to the new primary");
    lab.waited_pg("writing through pg again", 120, || {
        lab.through(1, "pg", "insert into orders (item) values ('after')")
            .is_some()
    });
    assert_eq!(lab.rows_on(1), Some(501));

    step("mended: the old primary comes back as a replica of the new one");
    let out = lab.on(&from, "pfctl -a legion/labcut -F rules");
    assert!(
        out.status.success(),
        "mending {from}: {}",
        text(&out.stderr)
    );
    lab.waited_pg("the mended node ready", 180, || {
        lab.node_state(&from) == "ready"
    });
    lab.waited_pg("pg-0 following pg-1", 600, || {
        lab.psql(0, "select pg_is_in_recovery()").as_deref() == Some("t")
            && lab.rows_on(0) == Some(501)
    });
    lab.waited_pg("healthy again", 180, || lab.repl()["phase"] == "healthy");

    step("a switchover hands the primary back, losing nothing");
    lab.ok(&[
        "replication",
        "switchover",
        "pg",
        "-n",
        "data",
        "--to",
        "pg-0",
    ]);
    lab.waited_pg("pg-0 primary again", 300, || {
        let r = lab.repl();
        r["primary"] == "pg-0" && r["change"].is_null() && r["lastChange"]["kind"] == "switchover"
    });
    lab.waited_pg("writing to pg-0", 120, || {
        lab.through(1, "pg", "insert into orders (item) values ('back')")
            .is_some()
    });
    lab.waited_pg("every row on both", 300, || {
        lab.rows_on(0) == Some(502) && lab.rows_on(1) == Some(502)
    });

    step("a rolling update replaces the replica, switches over, then the old primary");
    let updated = APP.replace(
        "-c, log_destination=stderr]",
        "-c, log_destination=stderr, -c, work_mem=8MB]",
    );
    std::fs::write(&app, &updated).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    let on_new_pod = |n: u64| {
        lab.member(n).is_some_and(|t| {
            t["spec"]["pod"]["containers"][0]["command"]
                .to_string()
                .contains("work_mem=8MB")
        })
    };
    lab.waited_pg("both members updated, the primary handed over", 600, || {
        let r = lab.repl();
        on_new_pod(0)
            && on_new_pod(1)
            && r["change"].is_null()
            && r["phase"] == "healthy"
            && r["lastChange"]["reason"] == "a rolling update replaces the primary"
    });
    let r = lab.repl();
    assert_eq!(r["lastChange"]["kind"], "switchover", "{r}");
    assert_eq!(
        r["primary"], "pg-1",
        "pg-0 was primary, so it went last: {r}"
    );
    lab.waited_pg("writing after the update", 120, || {
        lab.through(0, "pg", "insert into orders (item) values ('updated')")
            .is_some()
    });
    lab.waited_pg("every row on both after the update", 300, || {
        lab.rows_on(0) == Some(503) && lab.rows_on(1) == Some(503)
    });

    step("the primary's daemon killed: its PostgreSQL stops before the replica is promoted");
    let primary_node = lab.member(1).unwrap()["spec"]["node"]
        .as_str()
        .unwrap()
        .to_owned();
    let pv = &lab.vms[vm(&primary_node)];
    let out = lab.ssh(pv, "pkill -KILL -f 'legion-lab daemon'");
    assert!(out.status.success(), "killing {primary_node}'s daemon");
    let killed = std::time::Instant::now();
    let wall = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    // Its supervisor stops it once the lease runs out, no agent there.
    let mut stopped_at = None;
    while killed.elapsed() < Duration::from_secs(180) {
        let out = lab.ssh(pv, "pgrep -f 'postgres -D /var/db/pg/data' || true");
        if text(&out.stdout).trim().is_empty() {
            stopped_at = Some(wall());
            break;
        }
        assert!(
            lab.repl()["primary"] == "pg-1",
            "pg-0 promoted while pg-1's PostgreSQL still ran: {}",
            lab.repl()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    let Some(stopped_at) = stopped_at else {
        lab.dump_pg();
        panic!("PostgreSQL on {primary_node} still runs 180 s after its daemon was killed");
    };
    eprintln!(
        "stopped by its supervisor {:?} after the kill",
        killed.elapsed()
    );
    lab.waited_pg("pg-0 promoted", 300, || {
        let r = lab.repl();
        r["primary"] == "pg-0" && r["change"].is_null() && r["lastChange"]["to"] == "pg-0"
    });
    let r = lab.repl();
    assert_eq!(r["lastChange"]["kind"], "failover", "{r}");
    let promoted_at = r["lastChange"]["at"].as_i64().unwrap();
    assert!(
        stopped_at < promoted_at,
        "stopped at {stopped_at}, promotion recorded at {promoted_at}"
    );
    assert_eq!(lab.rows_on(0), Some(503), "every row pg-1 confirmed");

    step("the daemon back: the old primary rejoins as a replica");
    lab.restart_vm(vm(&primary_node), Role::Worker);
    lab.waited_pg("pg-1 following pg-0", 600, || {
        lab.psql(1, "select pg_is_in_recovery()").as_deref() == Some("t")
            && lab.rows_on(1) == Some(503)
    });
    lab.waited_pg("healthy at the end", 180, || {
        lab.repl()["phase"] == "healthy"
    });

    step("the log archived to the bucket across the failovers");
    let objects = |what: &str| {
        let out = lab.on(
            "n3",
            &format!("{mc} ls wal/{S3_BUCKET}/lab/data.pg/wal/ 2>/dev/null"),
        );
        text(&out.stdout)
            .lines()
            .filter(|l| l.trim_end().ends_with(what))
            .count()
    };
    lab.waited_pg(
        "WAL segments and the timelines' history in the bucket",
        300,
        || {
            let a = &lab.pg()["status"]["archive"];
            a["target"] == "pg-wal"
                && a["failures"].as_u64().unwrap_or(0) == 0
                && a["shippedMs"].as_i64().is_some()
                && objects(".history") >= 2
                && objects("") > 3
        },
    );
    eprintln!(
        "archived: {} files, {} timeline histories, status {}",
        objects(""),
        objects(".history"),
        lab.pg()["status"]["archive"]
    );
    let r = lab.legion(&["replication", "status", "pg", "-n", "data"], &[]);
    assert!(
        text(&r.stdout).contains("archive to pg-wal: last "),
        "{}",
        text(&r.stdout)
    );

    step("clean up");
    lab.ok(&["delete", "service", "pg", "-n", "data"]);
    lab.waited_pg("every task gone", 180, || {
        lab.list("tasks", "data")
            .iter()
            .all(|t| t["status"]["state"] != "running")
    });
    step("the conformance check on the PostgreSQL plugin, in a namespace of its own");
    let check = lab.dir.join("check.yaml");
    std::fs::write(
        &check,
        APP.replace("name: data }", "name: check }")
            .replace("namespace: data }", "namespace: check }")
            .replace("replicas: 2", "replicas: 3"),
    )
    .unwrap();
    let out = lab.legion(
        &["replication", "check", "-f", check.to_str().unwrap()],
        &[],
    );
    eprintln!("{}{}", text(&out.stdout), text(&out.stderr));
    assert!(out.status.success(), "the check failed");
    let said = text(&out.stdout);
    for step in [
        "ok    describe: postgresql",
        "ok    probe: a marker written on the primary is read on both replicas",
        "ok    the marker written just before the stop is on the new primary",
        "passed",
    ] {
        assert!(said.contains(step), "{step} missing");
    }

    step("done");
}
