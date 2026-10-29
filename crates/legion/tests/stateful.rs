//! The phase 4 exit test (PHASE.md): Postgres with replicas runs as a
//! stateful service and migrates between nodes. Ordinal 0 is the primary
//! and ordinal 1 a streaming replica of it, found by its stable name
//! `db-0.db.data.legion`; each keeps its data on a Volume of its own.
//! Draining the primary's node moves its data and the primary to another
//! node, where it carries on, and the replica follows it; draining the
//! replica's node then moves the replica.
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
//! With `LEGION_CLUSTER_RUNTIME=podman` it is any archive `podman load`
//! reads, loaded into every node's Podman.
//!
//! Every node gets a throwaway ZFS pool, `llab`, backed by a file in
//! `/var/tmp`, which goes again when the test ends.

mod common;

use std::process::Command;

use common::{Lab, Role, VM_DATA, VM_RUN, ZFS_ROOT, text, wait};
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
        # No memory limit: rctl's memoryuse counts the shared buffers once
        # per Postgres process.
    volumes: [{ name: pg, claim: { size: 512Mi } }]
"#;

impl Lab {
    /// Loads the image saved at `tar` into every node: its image store with
    /// the jail runtime (`tar` an OCI archive), or its Podman, through the
    /// socket of the lab's own Podman service.
    fn load_image(&self, tar: &str) {
        if self.jails {
            let out = Command::new(common::LEGION)
                .args(["image", "load", tar, "--zfs-root", ZFS_ROOT, "--data-dir"])
                .arg(self.dir.join("data"))
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "loading the image here: {}",
                text(&out.stderr)
            );
            for vm in &self.vms {
                self.copy(vm, tar, "/var/tmp/legion-pg.oci.tar");
                let out = self.ssh(
                    vm,
                    &format!(
                        "{} image load /var/tmp/legion-pg.oci.tar --zfs-root {ZFS_ROOT} \
                             --data-dir {VM_DATA} && rm /var/tmp/legion-pg.oci.tar",
                        common::REMOTE
                    ),
                );
                assert!(
                    out.status.success(),
                    "loading the image on {}: {}",
                    vm.name,
                    text(&out.stderr)
                );
            }
            return;
        }
        let socket = format!("unix://{}/run/podman.sock", self.dir.display());
        wait("the image loaded here", 120, || {
            Command::new("podman")
                .args(["--remote", "--url", &socket, "load", "-i", tar])
                .output()
                .unwrap()
                .status
                .success()
        });
        for vm in &self.vms {
            self.copy(vm, tar, "/var/tmp/legion-pg.tar.gz");
            let out = self.ssh(
                vm,
                &format!(
                    "i=0
                     until podman --remote --url unix://{VM_RUN}/podman.sock \
                         load -i /var/tmp/legion-pg.tar.gz >/dev/null; do
                         i=$((i + 1)); [ $i -lt 60 ] || exit 1; sleep 2
                     done
                     rm /var/tmp/legion-pg.tar.gz
"
                ),
            );
            assert!(
                out.status.success(),
                "loading the image on {}: {}",
                vm.name,
                text(&out.stderr)
            );
        }
    }

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
        self.ok(&["node", "drain", &from]);
        let volume = format!("pg-db-{n}");
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
    lab.use_pool();
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
