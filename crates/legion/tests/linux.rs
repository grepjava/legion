//! Linux images on Legion (DESIGN.md §6.6; PLAN.md phase 8, the Linux
//! images matrix): application images from Docker Hub that offer no
//! FreeBSD build, each a Service on a node started with `--linux`, each
//! doing real work through FreeBSD's Linux ABI. docs/LINUX.md is the table
//! this test keeps true.
//!
//! Every image is tried and the matrix printed before anything is asserted,
//! so one image failing still shows how the others did.
//!
//! It drives the real daemon on one node, so it is ignored by default and
//! run by hand on a FreeBSD host as root, with the lab of `tests/common`
//! ([`Lab::local`]); it pulls from Docker Hub:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/llinux \
//!     target/debug/deps/linux-<hash> --ignored --nocapture
//! ```

mod common;

use std::time::{Duration, Instant};

use common::{Lab, text, wait};
use serde_json::Value;

/// One row of the matrix: a Service running `image` with `command` (and
/// `more` of its container's fields), and a check run in its task, whose
/// output must hold `expect`.
struct Row {
    name: &'static str,
    image: &'static str,
    /// What the image is, for the table.
    what: &'static str,
    command: &'static [&'static str],
    more: &'static str,
    check: &'static [&'static str],
    expect: &'static str,
}

const SLEEP: &[&str] = &["/bin/sleep", "86400"];

const ROWS: &[Row] = &[
    Row {
        name: "alpine",
        image: "docker.io/library/alpine:3.20",
        what: "musl, BusyBox",
        command: SLEEP,
        more: "",
        check: &["/bin/sh", "-c", "uname -s; cat /etc/alpine-release"],
        expect: "Linux\n3.20",
    },
    Row {
        name: "ubuntu",
        image: "docker.io/library/ubuntu:24.04",
        what: "glibc, Perl",
        command: SLEEP,
        more: "",
        check: &[
            "/bin/sh",
            "-c",
            "perl -MDigest::MD5=md5_hex -e 'print md5_hex(\"legion\"), qq(\\n)'",
        ],
        // md5("legion")
        expect: "23d6e2be089bed49550f30814e08ad60",
    },
    Row {
        name: "nginx",
        image: "docker.io/library/nginx:1.27-alpine",
        what: "an HTTP server, worker processes",
        command: &["nginx", "-g", "daemon off;"],
        more: "",
        check: &["wget", "-q", "-O-", "http://127.0.0.1/"],
        expect: "Welcome to nginx!",
    },
    Row {
        name: "redis",
        image: "docker.io/library/redis:7-alpine",
        what: "an event loop (epoll)",
        command: &["redis-server", "--save", "", "--appendonly", "no"],
        more: "",
        check: &[
            "/bin/sh",
            "-c",
            "redis-cli set k legion >/dev/null && redis-cli get k",
        ],
        expect: "legion",
    },
    Row {
        name: "python",
        image: "docker.io/library/python:3.12-slim",
        what: "glibc, OpenSSL, SQLite, processes and POSIX semaphores",
        command: SLEEP,
        more: "",
        check: &[
            "python3",
            "-c",
            "import ssl, sqlite3, multiprocessing as m\n\
             db = sqlite3.connect(':memory:')\n\
             print('py', ssl.OPENSSL_VERSION.split()[0], db.execute('select 6*7').fetchone()[0], \
             m.Pool(2).map(abs, [-1, -2]))",
        ],
        expect: "py OpenSSL 42 [1, 2]",
    },
    Row {
        name: "node",
        image: "docker.io/library/node:22-alpine",
        what: "V8's JIT, libuv's threads",
        command: SLEEP,
        more: "",
        check: &[
            "node",
            "-e",
            "const c = require('crypto'); \
             require('fs').promises.readFile('/etc/hostname').then(() => \
             console.log('node', c.createHash('sha256').update('x').digest('hex').length))",
        ],
        expect: "node 64",
    },
    Row {
        name: "postgres",
        image: "docker.io/library/postgres:17-alpine",
        what: "its entrypoint, su-exec, shared memory",
        command: &["docker-entrypoint.sh", "postgres"],
        more: "        env: [{ name: POSTGRES_HOST_AUTH_METHOD, value: trust }]
        security: { allow: [sysvipc] }
",
        check: &[
            "psql",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
            "-tAc",
            "select 6*7",
        ],
        expect: "42",
    },
];

fn service(row: &Row) -> String {
    let list = |v: &[&str]| {
        v.iter()
            .map(|s| serde_json::to_string(s).unwrap())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "apiVersion: legion/v1
kind: Service
metadata: {{ name: {name}, namespace: lx }}
spec:
  replicas: 1
  pod:
    containers:
      - name: app
        image: {image}
        command: [{command}]
{more}",
        name = row.name,
        image = row.image,
        command = list(row.command),
        more = row.more,
    )
}

/// The running task of `service`.
fn task(lab: &Lab, service: &str) -> Option<Value> {
    lab.list("tasks", "lx").into_iter().find(|t| {
        t["spec"]["service"] == service
            && t["spec"]["desiredState"] == "running"
            && t["status"]["state"] == "running"
    })
}

/// How a row went: its check's output once it held what was expected, or
/// what went wrong last.
fn try_row(lab: &Lab, row: &Row, until: Instant) -> Result<String, String> {
    let mut last = String::from("no running task");
    while Instant::now() < until {
        if let Some(t) = task(lab, row.name) {
            let name = t["metadata"]["name"].as_str().unwrap();
            let mut args = vec!["exec", name, "-n", "lx", "--"];
            args.extend_from_slice(row.check);
            let out = lab.legion(&args, &[]);
            let said = text(&out.stdout);
            if out.status.success() && said.contains(row.expect) {
                return Ok(said.trim().to_owned());
            }
            last = format!(
                "{}{}{}",
                said.trim(),
                if said.trim().is_empty() { "" } else { "; " },
                text(&out.stderr).trim()
            );
        } else {
            let tasks: Vec<String> = lab
                .list("tasks", "lx")
                .into_iter()
                .filter(|t| t["spec"]["service"] == row.name)
                .map(|t| {
                    format!(
                        "{} {}",
                        t["status"]["state"].as_str().unwrap_or("?"),
                        t["status"]["message"].as_str().unwrap_or("")
                    )
                })
                .collect();
            if !tasks.is_empty() {
                last = format!("no running task: {}", tasks.join("; "));
            }
        }
        std::thread::sleep(Duration::from_secs(3));
    }
    Err(last)
}

#[test]
#[ignore = "needs root, ZFS and linux64 on FreeBSD, and Docker Hub; see the module docs"]
fn linux_application_images() {
    let mut lab = Lab::local();
    let step = |what: &str| eprintln!("--- {what}");

    step("one node running Linux images");
    lab.start_host(&["--linux"]);
    lab.ok(&["init"]);
    wait("the node says it runs Linux images", 60, || {
        lab.list("nodes", "default")
            .first()
            .is_some_and(|n| n["status"]["platform"]["linux"] == true)
    });
    let ns = lab.dir.join("namespace.yaml");
    std::fs::write(
        &ns,
        "apiVersion: legion/v1
kind: Namespace
metadata: { name: lx }
spec: { security: { allow: [sysvipc] } }
",
    )
    .unwrap();
    lab.ok(&["apply", "-f", ns.to_str().unwrap()]);

    step("a Service of each image");
    for row in ROWS {
        let path = lab.dir.join(format!("{}.yaml", row.name));
        std::fs::write(&path, service(row)).unwrap();
        lab.ok(&["apply", "-f", path.to_str().unwrap()]);
    }

    step("each one's check, in its task");
    // Pulls come first, one image after another: time enough for them all.
    let until = Instant::now() + Duration::from_secs(1500);
    let results: Vec<(&Row, Result<String, String>)> =
        ROWS.iter().map(|r| (r, try_row(&lab, r, until))).collect();
    eprintln!("Linux images matrix:");
    for (row, result) in &results {
        match result {
            Ok(out) => eprintln!(
                "  works   {} ({}): {}",
                row.image,
                row.what,
                out.replace('\n', " | ")
            ),
            Err(e) => eprintln!("  FAILS   {} ({}): {e}", row.image, row.what),
        }
    }
    let failed: Vec<&str> = results
        .iter()
        .filter(|(_, r)| r.is_err())
        .map(|(row, _)| row.image)
        .collect();
    assert!(failed.is_empty(), "failed: {failed:?}");

    step("no task restarted");
    for row in ROWS {
        let t = task(&lab, row.name).unwrap();
        assert_eq!(t["status"]["restarts"], 0, "{}: {t}", row.name);
    }
    step("done");
}
