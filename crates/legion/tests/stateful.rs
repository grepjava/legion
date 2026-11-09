//! The phase 4 exit test (PHASE.md): Postgres with replicas runs as a
//! stateful service and migrates between nodes. Ordinal 0 is the primary
//! and ordinal 1 a streaming replica of it, found by its stable name
//! `db-0.db.data.legion`; each keeps its data on a Volume of its own.
//! Draining the primary's node moves its data and the primary to another
//! node, where it carries on, and the replica follows it; draining the
//! replica's node then moves the replica.
//!
//! Phase 6's test of interrupted moves (PLAN.md) is here too, on the same
//! image: the primary's move, held by a full disk, then cut short as each
//! part is received, loses nothing. So is its restore drill: the cluster
//! rebuilt on replacement hosts from its backups, each stage timed.
//!
//! It drives real daemons, so it is ignored by default and run by hand on
//! a FreeBSD host as root, with the lab of `tests/common` (see there), as
//! the phase 3 test is, and with the Postgres image of
//! `tests/stateful/Containerfile` saved to `LEGION_PG_IMAGE` as an OCI
//! archive (`podman save --format oci-archive`), which it loads into every
//! node with `legion image load`:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lpg \
//!     LEGION_CLUSTER_HOST=192.168.253.254 \
//!     LEGION_CLUSTER_VMS="vultr=grepjava@192.168.254.2 lvm3=192.168.253.3" \
//!     LEGION_CLUSTER_KEY=/var/tmp/lvm/id_lab \
//!     LEGION_PG_IMAGE=/var/tmp/legion-pg.oci.tar \
//!     target/debug/deps/stateful-<hash> --ignored --nocapture
//! ```
//!
//! Every node gets a throwaway ZFS pool, `llab`, backed by a file in
//! `/var/tmp`, which goes again when the test ends.

mod common;

use common::{Lab, Role, ZFS_ROOT, kill_receiving, text, vm, wait};
use serde_json::Value;

/// The namespace, allowed the SysV shared memory Postgres needs, the
/// script its tasks run, and the service.
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
    # Ordinal 0 is the primary; the others stream from it.
    D=/var/db/pg/data
    if [ ! -s $D/PG_VERSION ]; then
      case $(hostname) in
      *-0)
        initdb -D $D -A trust -U postgres >/dev/null || exit 1
        echo "host all all 0.0.0.0/0 trust" >> $D/pg_hba.conf
        echo "host replication all 0.0.0.0/0 trust" >> $D/pg_hba.conf
        ;;
      *)
        until pg_basebackup -h $PRIMARY -U postgres -D $D -R -X stream; do
          rm -rf $D/*
          sleep 2
        done
        ;;
      esac
    fi
    postgres -D $D -c listen_addresses='*' -c shared_buffers=16MB \
      -c max_connections=20 -c wal_retrieve_retry_interval=1s \
      -c log_destination=stderr &
    pid=$!
    # A fast shutdown, which does not wait for the replica to let go.
    trap 'kill -INT $pid' TERM INT
    wait $pid
    wait $pid
---
apiVersion: legion/v1
kind: Service
metadata: { name: db, namespace: data }
spec:
  mode: stateful
  replicas: 2
  pod:
    initContainers:
      - name: own
        image: localhost/legion-pg:17
        user: root
        command: [/usr/bin/install, -d, -o, postgres, -m, "700", /var/db/pg/data]
        volumeMounts: [{ name: pg, path: /var/db/pg }]
    containers:
      - name: pg
        image: localhost/legion-pg:17
        command: [/bin/sh, /etc/legion/pg.sh]
        env: [{ name: PRIMARY, value: db-0.db.data.legion }]
        ports: [{ containerPort: 5432 }]
        configs: [{ name: pg-sh, target: /etc/legion/pg.sh }]
        volumeMounts: [{ name: pg, path: /var/db/pg }]
        security: { allow: [sysvipc] }
        probes:
          readiness: { tcp: { port: 5432 }, period: 2s }
        # No memory limit: tests/memory.rs runs Postgres under one.
    volumes: [{ name: pg, claim: { size: 512Mi } }]
"#;

impl Lab {
    /// The running task of an ordinal of `db`.
    fn ordinal(&self, n: u64) -> Option<Value> {
        self.list("tasks", "data").into_iter().find(|t| {
            t["spec"]["service"] == "db"
                && t["spec"]["slot"] == n
                && t["spec"]["desiredState"] == "running"
                && t["status"]["state"] == "running"
        })
    }

    fn node_of(&self, n: u64) -> String {
        let t = self.ordinal(n).unwrap();
        t["spec"]["node"].as_str().unwrap().to_owned()
    }

    /// `query`'s output on an ordinal, if it runs and the query works.
    fn sql(&self, n: u64, query: &str) -> Option<String> {
        let t = self.ordinal(n)?;
        let task = t["metadata"]["name"].as_str()?;
        let out = self.legion(
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
        );
        out.status
            .success()
            .then(|| text(&out.stdout).trim().to_owned())
    }

    fn rows(&self, n: u64) -> Option<u64> {
        self.sql(n, "select count(*) from orders")?.parse().ok()
    }

    /// Drains a node and waits for ordinal `n` to run elsewhere with its
    /// Volume, which has left the node; then makes the node active again.
    fn move_off(&self, n: u64) -> (String, String) {
        let from = self.node_of(n);
        let volume = format!("pg-db-{n}");
        // Waited for (PLAN.md, phase 5): completed only once nothing runs
        // on the node and none of its data is moving.
        let out = self.legion(
            &[
                "node",
                "drain",
                &from,
                "--wait",
                "--timeout",
                "900",
                "-o",
                "json",
            ],
            &[],
        );
        assert!(
            out.status.success(),
            "drain --wait: {}{}",
            text(&out.stdout),
            text(&out.stderr)
        );
        let done: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(done["phase"], "completed", "{done}");
        let ended = ["complete", "failed", "shutdown", "rejected"];
        for t in self.list("tasks", "data") {
            let state = t["status"]["state"].as_str().unwrap_or_default();
            assert!(
                t["spec"]["node"] != from.as_str() || ended.contains(&state),
                "completed, yet on {from}: {t}"
            );
        }
        for v in self.list("volumes", "data") {
            if v["metadata"]["name"] == volume.as_str() {
                assert!(
                    v["status"]["node"] != from.as_str() && v["status"].get("migration").is_none(),
                    "completed, yet its data is on {from}: {v}"
                );
            }
        }
        let to = std::cell::RefCell::new(String::new());
        wait(&format!("db-{n} running off {from}"), 600, || {
            let Some(t) = self.ordinal(n) else {
                return false;
            };
            let node = t["spec"]["node"].as_str().unwrap();
            let moved = self.list("volumes", "data").iter().any(|v| {
                v["metadata"]["name"] == volume.as_str()
                    && v["status"]["node"] == node
                    && v["status"].get("migration").is_none()
            });
            *to.borrow_mut() = node.to_owned();
            node != from && moved
        });
        self.ok(&["node", "activate", &from]);
        (from, to.into_inner())
    }
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and the image; see the module docs"]
fn postgres_moves_between_nodes() {
    let image = std::env::var("LEGION_PG_IMAGE").expect("set LEGION_PG_IMAGE");
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("two managers and a worker, each with a pool for volumes");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let manager = lab.ok(&["join-token", "manager"]).trim().to_owned();
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(0, &worker, Role::Worker);
    lab.join(1, &manager, Role::Manager);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });
    lab.load_image(&image);

    step("Postgres: a primary and a replica");
    let app = lab.dir.join("app.yaml");
    std::fs::write(&app, APP).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    wait("both ordinals ready", 600, || {
        lab.list("services", "data")
            .iter()
            .any(|s| s["metadata"]["name"] == "db" && s["status"]["ready"] == 2)
    });
    let (primary, replica) = (lab.node_of(0), lab.node_of(1));
    assert_ne!(primary, replica, "spread over nodes");
    assert_eq!(
        lab.sql(1, "select pg_is_in_recovery()").as_deref(),
        Some("t")
    );
    lab.sql(
        0,
        "create table orders (id serial primary key, item text); \
         insert into orders (item) select 'item ' || i from generate_series(1, 1000) i",
    )
    .expect("writing to the primary");
    wait("the replica has the rows", 60, || lab.rows(1) == Some(1000));

    step("draining the primary's node moves the primary with its data");
    let (from, to) = lab.move_off(0);
    eprintln!("db-0 moved from {from} to {to}");
    if to == replica {
        // What the scheduler saw: why not the node without a db task.
        for n in lab.list("nodes", "default") {
            eprintln!("node {} {}", n["metadata"]["name"], n["status"]);
        }
        for v in lab.list("volumes", "data") {
            eprintln!("volume {} {}", v["metadata"]["name"], v["status"]);
        }
        for t in lab.list("tasks", "data") {
            eprintln!(
                "task {}/{} on {} {} {} {}",
                t["metadata"]["namespace"],
                t["metadata"]["name"],
                t["spec"]["node"],
                t["spec"]["desiredState"],
                t["status"]["state"],
                t["status"]["message"]
            );
        }
    }
    assert_ne!(to, replica, "to the node without a db task");
    assert_eq!(lab.rows(0), Some(1000), "the rows came along");
    assert_eq!(
        lab.sql(0, "select pg_is_in_recovery()").as_deref(),
        Some("f")
    );
    lab.sql(
        0,
        "insert into orders (item) select 'more ' || i from generate_series(1, 1000) i",
    )
    .expect("writing to the moved primary");
    wait("the replica follows the primary", 120, || {
        lab.rows(1) == Some(2000)
    });

    step("draining the replica's node moves the replica");
    let (from, to) = lab.move_off(1);
    eprintln!("db-1 moved from {from} to {to}");
    assert_eq!(lab.rows(1), Some(2000), "the replica's data came along");
    lab.sql(0, "insert into orders (item) values ('last')")
        .expect("writing after the move");
    wait("the moved replica streams again", 120, || {
        lab.rows(1) == Some(2001)
    });

    step("clean up");
    lab.ok(&["delete", "service", "db", "-n", "data"]);
    wait("every task gone", 180, || {
        lab.list("tasks", "data")
            .iter()
            .all(|t| t["status"]["state"] != "running")
    });
    step("done");
}

impl Lab {
    /// The Volume of ordinal 0.
    fn pg_volume(&self) -> Value {
        self.list("volumes", "data")
            .into_iter()
            .find(|v| v["metadata"]["name"] == "pg-db-0")
            .unwrap_or_default()
    }

    /// What the move's failure leaves to read: the Volume, and on each VM
    /// its ZFS transfers, its copies and what its daemon said of moving.
    fn dump(&self) {
        eprintln!("volume: {}", self.pg_volume()["status"]);
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
                let out = self.legion(&["logs", name, "-n", "data", "--tail", "20"], &[]);
                eprintln!("{}{}", text(&out.stdout), text(&out.stderr));
            }
        }
        eprintln!(
            "replication: {:?}",
            self.sql(
                0,
                "select client_addr, state, sent_lsn, replay_lsn from pg_stat_replication"
            )
        );
        for n in self.list("nodes", "default") {
            eprintln!(
                "node {}: {} {}",
                n["metadata"]["name"], n["status"]["state"], n["status"]["drain"]
            );
        }
        for node in ["n2", "n3"] {
            let out = self.on(
                node,
                &format!(
                    "pgrep -lf 'zfs (send|receive)'
                     zfs list -H -o name -t all -r {ZFS_ROOT}/volumes
                     grep -iE 'volume|session|transfer' /var/log/legion-lab.log | tail -25
"
                ),
            );
            eprintln!(
                "=== {node}
{}{}",
                text(&out.stdout),
                text(&out.stderr)
            );
        }
        let host = std::fs::read_to_string(self.dir.join("daemon.log")).unwrap_or_default();
        let moving: Vec<&str> = host
            .lines()
            .filter(|l| l.contains("volume") || l.contains("migrat"))
            .collect();
        eprintln!(
            "=== n1
{}",
            moving[moving.len().saturating_sub(30)..].join(
                "
"
            )
        );
    }

    /// [`wait`], dumping the move's state if it times out.
    fn waited(&self, what: &str, secs: u64, f: impl FnMut() -> bool) {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wait(what, secs, f)));
        if let Err(e) = r {
            self.dump();
            std::panic::resume_unwind(e);
        }
    }

    /// What a node holds of ordinal 0's Volume: its datasets and snapshots.
    fn held(&self, node: &str) -> String {
        self.holds(node, "pg-db-0")
    }
}

/// Phase 6's interrupted moves on real ZFS (PLAN.md): the Postgres
/// primary's move off a draining node, first held by a full disk, then cut
/// short as each part is received, ends with every row, nothing left of
/// what was cut. The full copy is cut by killing the receiving node's
/// daemon (the move starts over once the node is back), the last part by
/// killing its `zfs receive` alone (the agent answers, and the part is sent
/// again onto the full copy). Three managers, so a node's daemon can go;
/// `n1` is paused, so the primary and its move are on the two VMs.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and the image; see the module docs"]
fn a_move_cut_short_or_held_by_a_full_disk_loses_nothing() {
    let image = std::env::var("LEGION_PG_IMAGE").expect("set LEGION_PG_IMAGE");
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("three managers, each with a pool for volumes; n1 takes no Postgres");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let manager = lab.ok(&["join-token", "manager"]).trim().to_owned();
    lab.join(0, &manager, Role::Manager);
    lab.join(1, &manager, Role::Manager);
    wait("three ready nodes", 120, || {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    });
    lab.load_image(&image);
    lab.ok(&["node", "pause", "n1"]);

    step(
        "a primary whose task may not stop yet, with rows and a ballast that takes a while to send",
    );
    let held = APP.replace("replicas: 2", "replicas: 1\n  maxUnavailable: 0");
    let app = lab.dir.join("app.yaml");
    std::fs::write(&app, &held).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    wait("the primary ready", 600, || {
        lab.list("services", "data")
            .iter()
            .any(|s| s["metadata"]["name"] == "db" && s["status"]["ready"] == 1)
    });
    let from = lab.node_of(0);
    let to = if from == "n2" { "n3" } else { "n2" };
    eprintln!("db-0 on {from}, to move to {to}");
    lab.sql(
        0,
        "create table orders (id serial primary key, item text); \
         insert into orders (item) select 'before ' || i from generate_series(1, 1000) i; \
         create table ballast1 as select md5(i::text) as v from generate_series(1, 1000000) i; \
         checkpoint",
    )
    .expect("writing to the primary");

    step("a full disk where it goes holds the move, which says why, and leaves nothing there");
    let ds = format!("{ZFS_ROOT}/volumes/data/pg-db-0");
    let out = lab.on(
        to,
        &format!(
            "set -e
             zfs create -p {ZFS_ROOT}/volumes/data
             zfs set quota=32M {ZFS_ROOT}/volumes
"
        ),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    lab.ok(&["node", "drain", &from]);
    let says_why = || {
        lab.list("nodes", "default")
            .iter()
            .filter(|n| n["metadata"]["name"] == from.as_str())
            .filter_map(|n| n["status"]["drain"]["remaining"].as_array())
            .flatten()
            .find(|i| i["kind"] == "Volume" && i["name"] == "pg-db-0")
            .and_then(|i| i["message"].as_str().map(str::to_owned))
            .filter(|m| m.contains("quota") || m.contains("space"))
    };
    lab.waited("the full disk in the drain status", 300, || {
        says_why().is_some()
    });
    eprintln!("drain status: {}", says_why().unwrap());
    assert_eq!(lab.node_of(0), from, "the primary runs on");
    assert_eq!(lab.rows(0), Some(1000));
    assert_eq!(lab.held(to), "", "nothing of the copy out of space");
    lab.sql(
        0,
        "insert into orders (item) select 'during ' || i from generate_series(1, 1000) i",
    )
    .expect("writing as the move waits");

    step("room made, the full copy is cut short: the receiving node's daemon killed");
    let out = lab.on(
        to,
        &format!(
            "zfs set quota=none {ZFS_ROOT}/volumes || exit 4
             [ \"$(zfs get -H -o value quota {ZFS_ROOT}/volumes)\" = none ] || exit 4
             {}",
            kill_receiving(&ds, "legion-lab daemon")
        ),
    );
    assert!(
        out.status.success(),
        "killing the daemon as it received: {}",
        text(&out.stderr)
    );
    let v = lab.pg_volume();
    assert_eq!(v["status"]["node"], from.as_str(), "{v}");
    assert!(
        v["status"]["migration"].is_null() || v["status"]["migration"]["phase"] == "copying",
        "cut short, yet copied: {v}"
    );
    assert_eq!(lab.held(to), "", "nothing of the copy cut short");
    lab.restart_vm(vm(to), Role::Manager);
    lab.waited("copied once the node is back", 600, || {
        lab.pg_volume()["status"]["migration"]["phase"] == "copied"
    });
    assert_eq!(lab.pg_volume()["status"]["migration"]["to"], to);

    step("what is written after the full copy goes in the last part, cut short too");
    lab.sql(
        0,
        "insert into orders (item) select 'after ' || i from generate_series(1, 1000) i; \
         create table ballast2 as select md5(i::text) as v from generate_series(1, 700000) i; \
         checkpoint",
    )
    .expect("writing after the full copy");
    std::fs::write(
        &app,
        APP.replace("replicas: 2", "replicas: 1\n  maxUnavailable: 1"),
    )
    .unwrap();
    let out = std::thread::scope(|s| {
        let killer =
            s.spawn(|| lab.on(to, &kill_receiving(&ds, &format!("zfs receive -F -u {ds}"))));
        lab.ok(&["apply", "-f", app.to_str().unwrap()]);
        killer.join().unwrap()
    });
    assert!(
        out.status.success(),
        "killing the last part's receive: {}",
        text(&out.stderr)
    );
    let v = lab.pg_volume();
    assert_eq!(v["status"]["node"], from.as_str(), "{v}");
    assert_eq!(v["status"]["migration"]["phase"], "finishing", "{v}");
    eprintln!("on {to} after the cut: {}", lab.held(to));

    step("the part sent again, the move ends: every row on the new node, nothing left over");
    let out = lab.legion(
        &[
            "node",
            "drain",
            &from,
            "--wait",
            "--timeout",
            "900",
            "-o",
            "json",
        ],
        &[],
    );
    assert!(
        out.status.success(),
        "drain --wait: {}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    lab.waited("db-0 running on its new node", 600, || {
        lab.ordinal(0).is_some_and(|t| t["spec"]["node"] == to)
    });
    assert_eq!(lab.pg_volume()["status"]["node"], to);
    lab.waited("the rows readable", 120, || lab.rows(0) == Some(3000));
    assert_eq!(
        lab.sql(0, "select count(*) from orders where item like 'after %'")
            .as_deref(),
        Some("1000"),
        "the last part's rows"
    );
    assert_eq!(
        lab.sql(
            0,
            "select (select count(*) from ballast1) || ' ' || (select count(*) from ballast2)"
        )
        .as_deref(),
        Some("1000000 700000")
    );
    assert_eq!(lab.held(to), ds, "the move's snapshots gone");
    lab.waited("the old copy dropped", 300, || lab.held(&from).is_empty());

    step("clean up");
    lab.ok(&["delete", "service", "db", "-n", "data"]);
    lab.waited("every task gone", 180, || {
        lab.list("tasks", "data")
            .iter()
            .all(|t| t["status"]["state"] != "running")
    });
    step("done");
}

/// Copies `from` to `to` keeping modes, as an operator's `cp -Rp` does.
fn kept(from: &std::path::Path, to: &std::path::Path) {
    let out = std::process::Command::new("cp")
        .arg("-Rp")
        .arg(from)
        .arg(to)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "copying {}: {}",
        from.display(),
        text(&out.stderr)
    );
}

/// Phase 6's restore drill (PLAN.md, item 4): Postgres, a primary and a
/// replica on two workers, is restored onto replacement hosts after every
/// host is lost, from the three backups kept apart (docs/DESIGN.md, What
/// to back up): the control state, the manager's keys, and the primary's
/// Volume. The hosts are the same machines, each with every disk Legion
/// used replaced: data directories and pools made anew. Each stage of the
/// recovery is timed, and the times printed for the record. The primary
/// comes back with what was written up to its backup, recovering as from
/// a crash; the replica, given an empty Volume, copies the primary anew.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and the image; see the module docs"]
fn postgres_is_restored_onto_replacement_hosts() {
    use std::time::Instant;
    let image = std::env::var("LEGION_PG_IMAGE").expect("set LEGION_PG_IMAGE");
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");

    step("a manager and two workers, each with a pool for volumes; the manager runs no Postgres");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(0, &worker, Role::Worker);
    lab.join(1, &worker, Role::Worker);
    let ready = |lab: &Lab| {
        ["n1", "n2", "n3"]
            .iter()
            .all(|n| lab.node_state(n) == "ready")
    };
    wait("three ready nodes", 120, || ready(&lab));
    lab.load_image(&image);
    lab.ok(&["node", "pause", "n1"]);

    step("Postgres, a primary and a replica, with rows and a ballast");
    let app = lab.dir.join("app.yaml");
    std::fs::write(&app, APP).unwrap();
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    let both = |lab: &Lab| {
        lab.list("services", "data")
            .iter()
            .any(|s| s["metadata"]["name"] == "db" && s["status"]["ready"] == 2)
    };
    lab.waited("both ordinals ready", 600, || both(&lab));
    lab.sql(
        0,
        "create table orders (id serial primary key, item text); \
         insert into orders (item) select 'kept ' || i from generate_series(1, 1000) i; \
         create table ballast as select md5(i::text) as v from generate_series(1, 1000000) i",
    )
    .expect("writing to the primary");
    // No checkpoint: one at once would recycle WAL the replica, behind
    // with the ballast, has yet to stream, and it would never catch up.
    lab.waited("the replica has the rows", 300, || {
        lab.rows(1) == Some(1000)
    });

    step("the three backups, kept apart: control state, keys, the primary's data");
    let safe = lab.dir.join("kept");
    std::fs::create_dir_all(&safe).unwrap();
    let control = safe.join("control.json");
    lab.ok(&["backup", "-o", control.to_str().unwrap()]);
    let data = lab.dir.join("data");
    for k in ["pki", "node-id", "admin.pem"] {
        kept(&data.join(k), &safe.join(k));
    }
    let volume = safe.join("pg-db-0.backup");
    let started = Instant::now();
    lab.ok(&[
        "volume",
        "backup",
        "pg-db-0",
        "-n",
        "data",
        "-o",
        volume.to_str().unwrap(),
    ]);
    let backup_took = started.elapsed();
    let size = std::fs::metadata(&volume).unwrap().len();
    lab.sql(
        0,
        "insert into orders (item) select 'late ' || i from generate_series(1, 100) i",
    )
    .expect("writing after the backup");

    step("every host lost: daemons killed, data directories and pools gone");
    for vm in &lab.vms {
        let out = lab.ssh(
            vm,
            &format!(
                "pkill -KILL -f 'legion-lab daemon'\n{}{}",
                common::clean(common::VM_DATA, common::VM_RUN),
                common::NO_POOL
            ),
        );
        assert!(
            out.status.success(),
            "losing {}: {}",
            vm.name,
            text(&out.stderr)
        );
    }
    if let Some(mut daemon) = lab.daemon.take() {
        daemon.kill().unwrap();
        daemon.wait().unwrap();
    }
    let d = lab.dir.display();
    let out = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!(
            "{}{}",
            common::clean(&format!("{d}/data"), &format!("{d}/run")),
            common::NO_POOL
        ))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "losing this host: {}",
        text(&out.stderr)
    );
    assert!(!data.exists(), "the manager's disk is gone");

    step("recovery, timed: the manager on a new disk, from its backup and its keys");
    let t0 = Instant::now();
    std::fs::create_dir_all(&data).unwrap();
    for k in ["pki", "node-id", "admin.pem"] {
        kept(&safe.join(k), &data.join(k));
    }
    let out = std::process::Command::new(common::LEGION)
        .arg("restore")
        .arg("--data-dir")
        .arg(&data)
        .arg(&control)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "legion restore: {}",
        text(&out.stderr)
    );
    // A new pool, then the daemon.
    lab.start_host(&[]);
    wait("the restored objects served", 120, || {
        lab.legion(&["get", "service", "db", "-n", "data"], &[])
            .status
            .success()
    });
    let manager_at = t0.elapsed();

    step("what the lost hosts held is cleared: the service, the old nodes, their Volumes");
    lab.ok(&["delete", "service", "db", "-n", "data"]);
    lab.ok(&["delete", "node", "n2"]);
    lab.ok(&["delete", "node", "n3"]);
    for v in ["pg-db-0", "pg-db-1"] {
        wait(&format!("{v} deleted"), 120, || {
            let out = lab.legion(&["delete", "volume", v, "-n", "data"], &[]);
            out.status.success() || text(&out.stderr).contains("not found")
        });
    }

    step("new hosts join as the old nodes did, and get the image");
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(0, &worker, Role::Worker);
    lab.join(1, &worker, Role::Worker);
    wait("three ready nodes again", 120, || ready(&lab));
    lab.load_image(&image);
    let nodes_at = t0.elapsed();

    step("the primary's data restored onto a node");
    lab.ok(&[
        "volume",
        "restore",
        volume.to_str().unwrap(),
        "--node",
        "n2",
    ]);
    let volume_at = t0.elapsed();

    step("Postgres again: the primary recovers as from a crash, the replica copies it anew");
    lab.ok(&["apply", "-f", app.to_str().unwrap()]);
    lab.waited("the primary serving", 600, || lab.rows(0).is_some());
    let primary_at = t0.elapsed();
    assert_eq!(lab.node_of(0), "n2", "where its data was restored");
    assert_eq!(
        lab.rows(0),
        Some(1000),
        "every row up to the backup, none after"
    );
    assert_eq!(
        lab.sql(0, "select count(*) from ballast").as_deref(),
        Some("1000000")
    );
    lab.waited("both ordinals ready", 600, || both(&lab));
    lab.waited("the replica in step", 600, || lab.rows(1) == Some(1000));
    let replica_at = t0.elapsed();
    assert_ne!(lab.node_of(1), "n2", "spread over the workers");
    lab.sql(0, "insert into orders (item) values ('after')")
        .expect("writing to the restored primary");
    lab.waited("the replica streams", 120, || lab.rows(1) == Some(1001));

    let s = |d: std::time::Duration| d.as_secs_f64();
    eprintln!(
        "recovery times: manager {:.1} s; nodes and image {:.1} s; volume ({} MiB) {:.1} s; \
         primary serving {:.1} s; replica in step {:.1} s; total {:.1} s \
         (the volume's backup took {:.1} s)",
        s(manager_at),
        s(nodes_at - manager_at),
        size >> 20,
        s(volume_at - nodes_at),
        s(primary_at - volume_at),
        s(replica_at - primary_at),
        s(replica_at),
        s(backup_took),
    );

    step("clean up");
    lab.ok(&["delete", "service", "db", "-n", "data"]);
    wait("every task gone", 180, || {
        lab.list("tasks", "data")
            .iter()
            .all(|t| t["status"]["state"] != "running")
    });
    step("done");
}
