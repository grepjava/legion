//! The examples in `examples/` (PLAN.md phase 8), each as its README says:
//! `legion local` builds its image from FreeBSD's packages, every node loads it,
//! and `legion stack deploy` runs its compose file on three nodes.
//!
//! - `base`: Legion's native base image, which the recipes build on.
//! - `static-site`: nginx answers on port 30081 of every node.
//! - `counter`: the web tier counts in Redis on port 30082 of every node,
//!   and the count stays when Redis's Volume moves to another node.
//! - `redis`: Redis keeps what it is told.
//! - `postgres`: PostgreSQL answers with its password, over the network,
//!   and keeps its data across a restart.
//! - `node`, `python`: a small HTTP server on ports 30083 and 30084.
//!
//! It drives real daemons, so it is ignored by default and run by hand on a
//! FreeBSD host as root, with the lab of `tests/common`; `legion local`
//! fetches FreeBSD's packages:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lexamples \
//!     LEGION_CLUSTER_HOST=10.78.0.1 \
//!     LEGION_CLUSTER_VMS="lab2=lab@10.78.0.2 lab3=lab@10.78.0.3" \
//!     LEGION_CLUSTER_KEY=/home/lab/.ssh/id_ed25519 \
//!     target/debug/deps/examples-<hash> --ignored --nocapture
//! ```

mod common;

use std::path::{Path, PathBuf};

use common::local::Local;
use common::{Lab, http_get, text, wait};
use serde_json::Value;

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(name)
}

impl Lab {
    /// Builds an example's image with `legion local` as `tag` and loads it on every
    /// node, as `localhost/<tag>`.
    fn image(&self, builder: &Local, name: &str, tag: &str) {
        let rec = self.dir.join(format!("{name}.json"));
        builder.build_dir(&example(name), tag, &rec);
        let tar = self.dir.join(format!("{name}.oci.tar"));
        builder.run(&[
            "export",
            "--record",
            rec.to_str().unwrap(),
            tag,
            tar.to_str().unwrap(),
        ]);
        self.load_image(tar.to_str().unwrap());
    }

    fn deploy(&self, stack: &str, name: &str) {
        self.deploy_with(stack, name, &[]);
    }

    /// As `deploy`, with variables for the compose file.
    fn deploy_with(&self, stack: &str, name: &str, env: &[(&str, &str)]) {
        let file = example(name).join("compose.yaml");
        let out = self.legion(
            &["stack", "deploy", stack, "-c", file.to_str().unwrap()],
            env,
        );
        assert!(
            out.status.success(),
            "deploying {name}: {}{}",
            text(&out.stdout),
            text(&out.stderr)
        );
        // Each example deploys as it is: nothing it asks for is ignored.
        assert!(
            !text(&out.stderr).contains("warning"),
            "{name}: {}",
            text(&out.stderr)
        );
    }

    /// Every service of `stack` has its tasks ready.
    fn stack_ready(&self, stack: &str) -> bool {
        let services = self.list("services", stack);
        !services.is_empty()
            && services.iter().all(|s| {
                let st = &s["status"];
                st["desired"].as_u64().is_some_and(|d| d > 0)
                    && st["ready"] == st["desired"]
                    && st["running"] == st["desired"]
            })
    }

    /// GET / on `port` of every node: the bodies, n1's asked from a VM.
    fn on_every_node(&self, port: u16) -> Vec<Result<String, String>> {
        let mut bodies = vec![self.http_get_from(&self.vms[0], &self.host, port)];
        bodies.extend(self.vms.iter().map(|vm| http_get(&vm.addr, port)));
        bodies
    }

    fn redis_task(&self) -> Value {
        self.list("tasks", "counter")
            .into_iter()
            .find(|t| {
                t["spec"]["service"] == "redis"
                    && t["spec"]["desiredState"] == "running"
                    && t["status"]["state"] == "running"
            })
            .expect("a running redis task")
    }
}

/// The count a body says, `hits <n>`.
fn hits(body: &str) -> u64 {
    body.strip_prefix("hits ")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("not a count: {body:?}"))
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD and two more nodes; see the module docs"]
fn the_examples_run_as_their_readmes_say() {
    let mut lab = Lab::from_env();
    let builder = Local::new(&lab, &[]);
    let step = |what: &str| eprintln!("--- {what}");

    step("three nodes, and each example's image built with `legion local` and loaded");
    lab.three_nodes(&[]);
    // The base first: the recipes are built FROM it, in the builder's store.
    builder.build_dir(
        &example("base"),
        "examples/base:15.1",
        &lab.dir.join("base.json"),
    );
    lab.image(&builder, "static-site", "examples/nginx:1");
    lab.image(&builder, "counter", "examples/counter:1");
    lab.image(&builder, "redis", "examples/redis:1");
    lab.image(&builder, "postgres", "examples/postgres:17");
    lab.image(&builder, "node", "examples/node:24");
    lab.image(&builder, "python", "examples/python:3");

    step("static-site: nginx on port 30081 of every node");
    lab.deploy("site", "static-site");
    wait("the site ready", 600, || lab.stack_ready("site"));
    wait("the page on every node", 120, || {
        lab.on_every_node(30081).iter().all(|b| {
            b.as_ref()
                .is_ok_and(|b| b.contains("Served by nginx on FreeBSD"))
        })
    });

    step("counter: every node counts, in one Redis");
    lab.deploy("counter", "counter");
    wait("the counter ready", 600, || lab.stack_ready("counter"));
    let mut last = 0;
    wait("a count on every node", 120, || {
        lab.on_every_node(30082).iter().all(|b| b.is_ok())
    });
    for _ in 0..3 {
        for body in lab.on_every_node(30082) {
            let n = hits(&body.unwrap());
            assert!(n > last, "the count went from {last} to {n}");
            last = n;
        }
    }

    step("Redis's Volume moved to another node: the count goes with it");
    let before = lab.redis_task();
    let from = before["spec"]["node"].as_str().unwrap().to_owned();
    let to = if from == "n2" { "n3" } else { "n2" };
    lab.ok(&["volume", "move", "counts", to, "-n", "counter"]);
    wait(&format!("Redis on {to}"), 900, || {
        lab.list("tasks", "counter").iter().any(|t| {
            t["spec"]["service"] == "redis"
                && t["spec"]["desiredState"] == "running"
                && t["status"]["state"] == "running"
                && t["spec"]["node"] == to
        })
    });
    let volume = lab
        .list("volumes", "counter")
        .into_iter()
        .find(|v| v["metadata"]["name"] == "counts")
        .unwrap();
    assert_eq!(volume["status"]["node"], to, "{volume}");
    wait("the counter ready again", 300, || {
        lab.stack_ready("counter")
    });
    let mut next = None;
    wait("a count again", 120, || {
        next = lab
            .on_every_node(30082)
            .into_iter()
            .find_map(Result::ok)
            .map(|b| hits(&b));
        next.is_some()
    });
    let next = next.unwrap();
    assert!(
        next > last,
        "the count carried on from {last}, at {next}: appended before the move"
    );

    step("redis: keeps what it is told");
    lab.deploy("cache", "redis");
    wait("redis ready", 600, || lab.stack_ready("cache"));
    let cli = |args: &[&str]| {
        let mut a = vec!["exec", "redis", "-n", "cache", "--", "/usr/local/bin/redis-cli"];
        a.extend_from_slice(args);
        lab.ok(&a)
    };
    cli(&["set", "native", "yes"]);
    assert_eq!(cli(&["get", "native"]).trim(), "yes");

    step("postgres: its password asked over the network, its data kept across a restart");
    let ns = lab.dir.join("db.yaml");
    std::fs::write(
        &ns,
        "apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: db }\nspec: { security: { allow: [sysvipc] } }\n",
    )
    .unwrap();
    lab.ok(&["apply", "-f", ns.to_str().unwrap()]);
    lab.deploy_with("db", "postgres", &[("POSTGRES_PASSWORD", "native-42")]);
    wait("postgres ready", 600, || lab.stack_ready("db"));
    let psql = |password: &str, sql: &str| {
        lab.legion(
            &[
                "exec", "postgres", "-n", "db", "--", "/usr/bin/env",
                &format!("PGPASSWORD={password}"),
                "/usr/local/bin/psql", "-h", "127.0.0.1", "-U", "postgres", "-tAc", sql,
            ],
            &[],
        )
    };
    let out = psql("native-42", "create table kept (n int); insert into kept values (42)");
    assert!(out.status.success(), "{}", text(&out.stderr));
    let wrong = psql("not-it", "select 1");
    assert!(
        !wrong.status.success() && text(&wrong.stderr).contains("password authentication failed"),
        "a wrong password is refused: {}",
        text(&wrong.stderr)
    );
    let task = |lab: &Lab| {
        lab.list("tasks", "db")
            .into_iter()
            .find(|t| t["spec"]["desiredState"] == "running" && t["status"]["state"] == "running")
            .map(|t| t["metadata"]["name"].as_str().unwrap().to_owned())
    };
    let first = task(&lab).unwrap();
    lab.ok(&["restart", "postgres", "-n", "db"]);
    wait("a new postgres task, ready", 600, || {
        task(&lab).is_some_and(|t| t != first) && lab.stack_ready("db")
    });
    let mut kept = String::new();
    wait("its data, read again", 120, || {
        let out = psql("native-42", "select n from kept");
        kept = text(&out.stdout);
        out.status.success()
    });
    assert_eq!(kept.trim(), "42", "the data outlived the task");

    step("node and python: a small server on every node");
    lab.deploy("hello-node", "node");
    lab.deploy("hello-python", "python");
    wait("both ready", 600, || {
        lab.stack_ready("hello-node") && lab.stack_ready("hello-python")
    });
    for (port, says) in [(30083, "hello from node v24"), (30084, "hello from python 3")] {
        wait(&format!("{says} on every node"), 120, || {
            lab.on_every_node(port)
                .iter()
                .all(|b| b.as_ref().is_ok_and(|b| b.contains(says)))
        });
    }
    for stack in ["cache", "db", "hello-node", "hello-python"] {
        lab.ok(&["stack", "rm", stack]);
    }

    step("both stacks removed; the counter's Volume kept");
    lab.ok(&["stack", "rm", "site"]);
    lab.ok(&["stack", "rm", "counter"]);
    wait("their tasks stopped", 180, || {
        ["site", "counter"].iter().all(|ns| {
            lab.list("tasks", ns)
                .iter()
                .all(|t| t["status"]["state"] != "running")
        })
    });
    assert!(
        lab.list("volumes", "counter")
            .iter()
            .any(|v| v["metadata"]["name"] == "counts"),
        "stack rm keeps Volumes"
    );
    step("done");
}
