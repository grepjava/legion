//! Linux images on Legion (DESIGN.md §6.6; PLAN.md phase 8, the Linux
//! images matrix): application images from Docker Hub that offer no
//! FreeBSD build, each a Service on a node started with `--linux`, each
//! doing real work through FreeBSD's Linux ABI. docs/LINUX.md is the table
//! this test keeps true.
//!
//! Every image is tried, each in time of its own and each check given
//! [`CHECK_SECS`], and the matrix printed before anything is asserted, so
//! one image failing still shows how the others did. Static Linux programs
//! (`gosu`, BusyBox) run as the daemon sets `kern.elf64.fallback_brand` for
//! them (DESIGN.md §6.6). What the ABI is known not to run stays in the
//! table as such (Node 22, Postgres), and is checked to fail as it says.
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
/// `more` of its container's fields), a check run in its task, and what
/// the row is to show.
struct Row {
    name: &'static str,
    image: &'static str,
    /// What the image is, for the table.
    what: &'static str,
    command: &'static [&'static str],
    more: &'static str,
    check: &'static [&'static str],
    expect: Expect,
}

/// What a row shows. A failure FreeBSD's Linux ABI is known for is kept
/// as one, so the table stays true when the ABI changes.
#[derive(Clone, Copy)]
enum Expect {
    /// The check's output holds this.
    Works(&'static str),
    /// The check runs and never answers; why, for the table.
    Hangs(&'static str),
    /// The container stops, its log saying the first; why, the second.
    Stops(&'static str, &'static str),
}

/// How long a check may take.
const CHECK_SECS: u64 = 60;

const SLEEP: &[&str] = &["/bin/sleep", "86400"];

const ROWS: &[Row] = &[
    Row {
        name: "alpine",
        image: "docker.io/library/alpine:3.20",
        what: "musl, BusyBox",
        command: SLEEP,
        more: "",
        check: &["/bin/sh", "-c", "uname -s; cat /etc/alpine-release"],
        expect: Expect::Works("Linux\n3.20"),
    },
    Row {
        name: "ubuntu",
        image: "docker.io/library/ubuntu:24.04",
        what: "glibc, Perl",
        command: SLEEP,
        more: "",
        check: &[
            "perl",
            "-e",
            "print join(q(,), map { $_ * 2 } grep { $_ % 2 } 1..9), qq(\n)",
        ],
        expect: Expect::Works("2,6,10,14,18"),
    },
    Row {
        name: "nginx",
        image: "docker.io/library/nginx:1.27-alpine",
        what: "an HTTP server, worker processes, accept_mutex on",
        // The ABI's epoll refuses EPOLLEXCLUSIVE (EINVAL), which nginx asks
        // for with several workers unless accept_mutex is on: its workers
        // would exit, the master accepting what none answer.
        command: &[
            "/bin/sh",
            "-c",
            "sed -i 's/^events {/events { accept_mutex on;/' /etc/nginx/nginx.conf && \
             exec nginx -g 'daemon off;'",
        ],
        more: "",
        check: &["wget", "-q", "-O-", "http://127.0.0.1/"],
        expect: Expect::Works("Welcome to nginx!"),
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
        expect: Expect::Works("legion"),
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
        expect: Expect::Works("py OpenSSL 42 [1, 2]"),
    },
    Row {
        name: "busybox",
        // The musl build: the default busybox:1.37 is glibc, dynamic and
        // noted as Linux's, so it runs whatever the fallback brand says.
        image: "docker.io/library/busybox:1.37-musl",
        what: "one static program (static PIE, no ABI note)",
        command: SLEEP,
        more: "",
        check: &["/bin/busybox", "echo", "static"],
        expect: Expect::Works("static"),
    },
    Row {
        name: "node",
        image: "docker.io/library/node:22-alpine",
        what: "V8, libuv",
        command: SLEEP,
        more: "",
        check: &["node", "-e", "console.log(6 * 7)"],
        expect: Expect::Hangs("its main thread spins from the start, running no script"),
    },
    Row {
        name: "postgres",
        image: "docker.io/library/postgres:17-alpine",
        what: "its entrypoint, gosu (static Go)",
        command: &["docker-entrypoint.sh", "postgres"],
        more: "        env: [{ name: POSTGRES_HOST_AUTH_METHOD, value: trust }]
        security: { allow: [sysvipc] }
",
        check: &[],
        // After gosu, a static program, ran it. Postgres 15 the same.
        expect: Expect::Stops("signalfd() failed", "the ABI has no signalfd(2)"),
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

/// A task of `service`, running or not.
fn any_task(lab: &Lab, service: &str) -> Option<Value> {
    lab.list("tasks", "lx")
        .into_iter()
        .find(|t| t["spec"]["service"] == service)
}

/// How a row went: what showed it as expected, or what happened instead.
fn try_row(lab: &Lab, row: &Row, until: Instant) -> Result<String, String> {
    let mut last = String::from("no running task");
    while Instant::now() < until {
        if let Expect::Stops(says, _) = row.expect {
            if let Some(t) = any_task(lab, row.name) {
                let name = t["metadata"]["name"].as_str().unwrap();
                let log = lab
                    .legion_within(CHECK_SECS, &["logs", name, "-n", "lx"])
                    .map(|o| text(&o.stdout) + &text(&o.stderr))
                    .unwrap_or_default();
                if let Some(line) = log.lines().find(|l| l.contains(says)) {
                    return Ok(line.trim().to_owned());
                }
                last = log.lines().last().unwrap_or("").to_owned();
            }
        } else if let Some(t) = task(lab, row.name) {
            let name = t["metadata"]["name"].as_str().unwrap();
            let mut args = vec!["exec", name, "-n", "lx", "--"];
            args.extend_from_slice(row.check);
            match (lab.legion_within(CHECK_SECS, &args), row.expect) {
                (None, Expect::Hangs(_)) => return Ok(format!("no answer in {CHECK_SECS} s")),
                (None, _) => last = format!("no answer in {CHECK_SECS} s"),
                (Some(out), expect) => {
                    let said = text(&out.stdout);
                    if let Expect::Works(holds) = expect
                        && out.status.success()
                        && said.contains(holds)
                    {
                        return Ok(said.trim().to_owned());
                    }
                    last = format!("{} {}", said.trim(), text(&out.stderr).trim());
                    if let Expect::Hangs(_) = expect {
                        return Err(format!("it answered: {last}"));
                    }
                }
            }
        } else if let Some(t) = any_task(lab, row.name) {
            last = format!(
                "no running task: {} {}",
                t["status"]["state"].as_str().unwrap_or("?"),
                t["status"]["message"].as_str().unwrap_or("")
            );
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
    // Each row its own time: the pulls run at once, and a row that fails
    // leaves the next its own.
    let results: Vec<(&Row, Result<String, String>)> = ROWS
        .iter()
        .map(|r| {
            (
                r,
                try_row(&lab, r, Instant::now() + Duration::from_secs(600)),
            )
        })
        .collect();
    eprintln!("Linux images matrix:");
    for (row, result) in &results {
        let shown = match (result, row.expect) {
            (Ok(_), Expect::Works(_)) => "works",
            (Ok(_), Expect::Hangs(why) | Expect::Stops(_, why)) => why,
            (Err(_), _) => "NOT AS THE MATRIX SAYS",
        };
        let detail = match result {
            Ok(s) | Err(s) => s.replace('\n', " | "),
        };
        eprintln!("  {} ({}): {shown}: {detail}", row.image, row.what);
    }
    let unexpected: Vec<&str> = results
        .iter()
        .filter(|(_, r)| r.is_err())
        .map(|(row, _)| row.image)
        .collect();
    assert!(
        unexpected.is_empty(),
        "not as the matrix says: {unexpected:?}"
    );

    step("no task that works restarted");
    for row in ROWS.iter().filter(|r| matches!(r.expect, Expect::Works(_))) {
        let t = task(&lab, row.name).unwrap();
        assert_eq!(t["status"]["restarts"], 0, "{}: {t}", row.name);
    }
    step("done");
}
