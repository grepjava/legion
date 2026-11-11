//! The examples in `examples/` (PLAN.md phase 8), each as its README says:
//! jailer builds its image from FreeBSD's packages, every node loads it,
//! and `legion stack deploy` runs its compose file on three nodes.
//!
//! - `static-site`: nginx answers on port 30081 of every node.
//! - `counter`: the web tier counts in Redis on port 30082 of every node,
//!   and the count stays when Redis's Volume moves to another node.
//!
//! It drives real daemons, so it is ignored by default and run by hand on a
//! FreeBSD host as root, with the lab of `tests/common` and a jailer binary
//! in `LEGION_JAILER`; jailer fetches FreeBSD's packages:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lexamples \
//!     LEGION_CLUSTER_HOST=10.78.0.1 \
//!     LEGION_CLUSTER_VMS="lab2=lab@10.78.0.2 lab3=lab@10.78.0.3" \
//!     LEGION_CLUSTER_KEY=/home/lab/.ssh/id_ed25519 \
//!     LEGION_JAILER=/home/lab/jailer/target/debug/jailer \
//!     target/debug/deps/examples-<hash> --ignored --nocapture
//! ```

mod common;

use std::path::{Path, PathBuf};

use common::jailer::Jailer;
use common::{Lab, http_get, text, wait};
use serde_json::Value;

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(name)
}

impl Lab {
    /// Builds an example's image with jailer as `tag` and loads it on every
    /// node, as `localhost/<tag>`.
    fn image(&self, jailer: &Jailer, name: &str, tag: &str) {
        let rec = self.dir.join(format!("{name}.json"));
        jailer.build_dir(&example(name), tag, &rec);
        let tar = self.dir.join(format!("{name}.oci.tar"));
        jailer.run(&[
            "export",
            "--record",
            rec.to_str().unwrap(),
            tag,
            tar.to_str().unwrap(),
        ]);
        self.load_image(tar.to_str().unwrap());
    }

    fn deploy(&self, stack: &str, name: &str) {
        let file = example(name).join("compose.yaml");
        let out = self.legion(
            &["stack", "deploy", stack, "-c", file.to_str().unwrap()],
            &[],
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
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn the_examples_run_as_their_readmes_say() {
    let mut lab = Lab::from_env();
    let jailer = Jailer::new(&lab, &[]);
    let step = |what: &str| eprintln!("--- {what}");

    step("three nodes, and each example's image built with jailer and loaded");
    lab.three_nodes(&[]);
    lab.image(&jailer, "static-site", "examples/nginx:1");
    lab.image(&jailer, "counter", "examples/redis:1");

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
