//! Resource limits measured (DESIGN.md §6.3; PLAN.md phase 8): what a
//! container's `cpu`, `maxproc` and `openfiles` limits do to work that
//! wants more, measured on one node, rctl's own counts read from the host.
//! Memory is measured on Postgres in `tests/memory.rs`.
//!
//! - `cpu: 500m` on a busy loop: rctl throttles it by pausing it, so it
//!   gets about half a CPU over 30 s, where the same loop unlimited beside
//!   it gets a whole one.
//! - `maxproc: 20`: forks past the limit are refused, and the container's
//!   main process runs on.
//! - `openfiles: 256`: the limit is the jail's, every process's open files
//!   counted, the supervisor's too; one process opening 30 more opens them,
//!   one opening 400 is refused each past the limit.
//!
//! The measurements are printed. It drives the real daemon on one node, so
//! it is ignored by default and run by hand on a FreeBSD host as root, with
//! `kern.racct.enable=1` and the lab of `tests/common` ([`Lab::local`]):
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/llimits \
//!     target/debug/deps/limits-<hash> --ignored --nocapture
//! ```

mod common;

use std::time::{Duration, Instant};

use common::{Lab, rctl_usage, text, wait};
use serde_json::Value;

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";

/// A Service of one task running `script`, with `limits` (YAML, as the
/// container's `resources.limits`).
fn service(name: &str, script: &str, limits: &str) -> String {
    format!(
        "apiVersion: legion/v1
kind: Service
metadata: {{ name: {name}, namespace: lim }}
spec:
  replicas: 1
  pod:
    containers:
      - name: app
        image: {IMAGE}
        command: [/bin/sh, -c, {script:?}]
        resources: {{ limits: {limits} }}
"
    )
}

impl Lab {
    /// The running task of `service`, once there is one.
    fn running_task(&self, service: &str) -> Value {
        let mut found = None;
        wait(&format!("{service} running"), 600, || {
            found = self.list("tasks", "lim").into_iter().find(|t| {
                t["spec"]["service"] == service
                    && t["spec"]["desiredState"] == "running"
                    && t["status"]["state"] == "running"
            });
            found.is_some()
        });
        found.unwrap()
    }

    /// The jail of `service`'s container.
    fn jail(&self, service: &str) -> (String, String) {
        let t = self.running_task(service);
        let task = t["metadata"]["name"].as_str().unwrap().to_owned();
        let jail = self.jail_of(&format!("lim_{task}_app"));
        (task, jail)
    }

    /// `script` run by `sh` in `task`: its status, stdout and stderr.
    fn sh_in(&self, task: &str, script: &str) -> (bool, String, String) {
        let out = self.legion(
            &["exec", task, "-n", "lim", "--", "/bin/sh", "-c", script],
            &[],
        );
        (out.status.success(), text(&out.stdout), text(&out.stderr))
    }
}

/// CPU seconds `jail` used over `secs`, by rctl, per second.
fn cpu_share(jails: &[&str], secs: u64) -> Vec<f64> {
    let before: Vec<u64> = jails.iter().map(|j| rctl_usage(j, "cputime")).collect();
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    let took = t0.elapsed().as_secs_f64();
    jails
        .iter()
        .zip(before)
        .map(|(j, b)| (rctl_usage(j, "cputime") - b) as f64 / took)
        .collect()
}

#[test]
#[ignore = "needs root, ZFS and kern.racct.enable on FreeBSD; see the module docs"]
fn limits_hold_what_wants_more() {
    let mut lab = Lab::local();
    let step = |what: &str| eprintln!("--- {what}");

    step("one node, and a busy loop with half a CPU beside one unlimited");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    let ns = lab.dir.join("namespace.yaml");
    std::fs::write(
        &ns,
        "apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: lim }\n",
    )
    .unwrap();
    lab.ok(&["apply", "-f", ns.to_str().unwrap()]);
    let busy = "while :; do :; done";
    let apps = [
        ("half", service("half", busy, "{ cpu: 500m }")),
        ("whole", service("whole", busy, "{ memory: 64Mi }")),
        (
            "procs",
            service("procs", "exec sleep 86400", "{ maxproc: 20 }"),
        ),
        (
            "files",
            service("files", "exec sleep 86400", "{ openfiles: 256 }"),
        ),
    ];
    for (name, yaml) in &apps {
        let path = lab.dir.join(format!("{name}.yaml"));
        std::fs::write(&path, yaml).unwrap();
        lab.ok(&["apply", "-f", path.to_str().unwrap()]);
    }
    let (_, half) = lab.jail("half");
    let (_, whole) = lab.jail("whole");
    // Past the start.
    std::thread::sleep(Duration::from_secs(5));
    let shares = cpu_share(&[&half, &whole], 30);
    eprintln!(
        "cpu: 500m got {:.2} CPU over 30 s; unlimited, {:.2}",
        shares[0], shares[1]
    );
    assert!(
        (0.35..=0.65).contains(&shares[0]),
        "cpu: 500m got {:.2}",
        shares[0]
    );
    assert!(shares[1] >= 0.85, "unlimited got {:.2}", shares[1]);
    lab.ok(&["delete", "service", "half", "-n", "lim"]);
    lab.ok(&["delete", "service", "whole", "-n", "lim"]);

    step("maxproc 20: forty sleeps asked for");
    let (task, jail) = lab.jail("procs");
    // Counted from the host: past the limit the container can't fork a
    // counter either.
    let sleeps = || {
        let out = std::process::Command::new("pgrep")
            .args(["-j", &jail, "-x", "sleep"])
            .output()
            .unwrap();
        text(&out.stdout).lines().count()
    };
    let (started, err) = std::thread::scope(|s| {
        let asking = s.spawn(|| {
            lab.sh_in(&task, "for i in $(seq 40); do sleep 20 & done; wait")
                .2
        });
        std::thread::sleep(Duration::from_secs(5));
        let started = sleeps();
        (started, asking.join().unwrap())
    });
    eprintln!(
        "maxproc 20: {started} sleeps at once, the main one with them; refused: {}",
        err.lines().next().unwrap_or("")
    );
    assert!((10..=20).contains(&started), "{started} running: {err}");
    assert!(
        err.contains("fork") || err.contains("Resource temporarily unavailable"),
        "the forks past the limit are refused: {err}"
    );
    wait("the forty gone", 60, || sleeps() == 1);
    let t = lab.running_task("procs");
    assert_eq!(t["metadata"]["name"], task.as_str(), "the task runs on");
    assert_eq!(t["status"]["restarts"], 0);

    step("openfiles 256: one process opening 30 files, then 400");
    let (task, jail) = lab.jail("files");
    // The jail's own, before: its processes' tables, the supervisor's too.
    let resting = rctl_usage(&jail, "openfiles");
    eprintln!("openfiles 256: the jail holds {resting} at rest");
    assert!(resting < 200, "room left under the limit: {resting}");
    let open = |n: u32| {
        lab.sh_in(
            &task,
            &format!(
                "mkdir -p /tmp/f && cd /tmp/f && touch $(seq -f f%g {n}) &&                  timeout 3 tail -q -f $(seq -f f%g {n}); echo status $?"
            ),
        )
    };
    let (_, out, err) = open(30);
    eprintln!("openfiles 256, 30 files: {} {}", out.trim(), err.trim());
    assert!(
        out.contains("status 124") && !err.contains("Too many open files"),
        "30 open until the timeout: {out} {err}"
    );
    let (_, out, err) = open(400);
    let refused = err.matches("Too many open files").count();
    eprintln!(
        "openfiles 256, 400 files: {refused} refused, {} opened; {}",
        400 - refused,
        out.trim()
    );
    assert!(
        refused >= 150 && 400 - refused >= 30,
        "past the limit refused, the rest open: {refused}
{err}"
    );
    step("done");
}
