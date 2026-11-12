//! The image handoff (jailer's ROADMAP, P1.4; PLAN.md phase 3): an image
//! jailer builds and records is the image Legion runs, by both routes.
//!
//! - From a registry: jailer pushes to the registry this test serves (see
//!   [`registry`]), and every task runs the digest jailer recorded, through
//!   a scale, a redeployment after the tag moved to a build in zstd, and a
//!   rollback.
//! - Offline: jailer exports an archive, every node loads it with
//!   `legion image load`, and every task runs the manifest jailer recorded.
//! - With data (jailer's ROADMAP, shared qualification; PLAN.md phases 4
//!   and 5): a stateful task of a jailer image writes to its Volume, its
//!   node is drained, and on the next node it runs the same manifest with
//!   the same data, its first writer having stopped before the data moved;
//!   and again with the move's last part cut short and sent again.
//! - Signed (PLAN.md phase 7): jailer signs what it pushes as cosign does,
//!   and an ImagePolicy wanting its key runs it, by tag or by the digest
//!   jailer recorded; it holds an unsigned image until jailer signs it, one
//!   signed by another key, a loaded one, and an update whose signature the
//!   registry cannot give, the old tasks running; under warn it runs an
//!   unsigned image and says so.
//! - From build to recovery (PLAN.md phase 8, jailer's shared qualification):
//!   one signed jailer image runs under a policy enforcing its key, through
//!   a drain, a move cut short and a restore onto another host, on the
//!   digest jailer recorded at every stage, its data the same.
//! - Across platforms (jailer's shared qualification): jailer pushes one
//!   signed index of an amd64 image built here and an arm64 one built on an
//!   arm64 host; every node pins the index and runs its own platform's
//!   manifest, a Service wanting arm64 waits for an arm64 node, and an
//!   index with no amd64 image is refused once on each node, not retried.
//!
//! It drives real daemons, so it is ignored by default and run by hand on a
//! FreeBSD host as root, with the lab of `tests/common` (see there), and with
//! a jailer binary in `LEGION_JAILER`; jailer pulls its base image from
//! Docker Hub:
//!
//! ```sh
//! sudo LEGION_CLUSTER_LAB=/var/tmp/lhandoff \
//!     LEGION_CLUSTER_HOST=10.78.0.1 \
//!     LEGION_CLUSTER_VMS="lab2=lab@10.78.0.2 lab3=lab@10.78.0.3" \
//!     LEGION_CLUSTER_KEY=/home/lab/.ssh/id_ed25519 \
//!     LEGION_JAILER=/home/lab/jailer/target/debug/jailer \
//!     target/debug/deps/handoff-<hash> --ignored --nocapture
//! ```

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use common::jailer::{Jailer, record, unique};
use common::{Lab, Role, ZFS_ROOT, kill_receiving, text, wait};
use serde_json::Value;

const NAMESPACE: &str = "apiVersion: legion/v1
kind: Namespace
metadata: { name: handoff }
";

/// A Service of `replicas` tasks running `image`, with `more` of its
/// container's fields, indented to it.
fn service(name: &str, image: &str, replicas: u32, more: &str) -> String {
    format!(
        "apiVersion: legion/v1
kind: Service
metadata: {{ name: {name}, namespace: handoff }}
spec:
  replicas: {replicas}
  pod:
    containers:
      - name: app
        image: {image}
        command: [/bin/sleep, \"86400\"]
{more}"
    )
}

impl Lab {
    fn apply(&self, name: &str, yaml: &str) {
        let path = self.dir.join(format!("{name}.yaml"));
        std::fs::write(&path, yaml).unwrap();
        self.ok(&["apply", "-f", path.to_str().unwrap()]);
    }

    fn service(&self, name: &str) -> Value {
        self.list("services", "handoff")
            .into_iter()
            .find(|s| s["metadata"]["name"] == name)
            .unwrap_or_else(|| panic!("no service {name}"))
    }

    /// The running tasks of a service, meant to run.
    fn running(&self, service: &str) -> Vec<Value> {
        self.list("tasks", "handoff")
            .into_iter()
            .filter(|t| {
                t["spec"]["service"] == service
                    && t["spec"]["desiredState"] == "running"
                    && t["status"]["state"] == "running"
            })
            .collect()
    }

    /// What `/id` holds in a task.
    fn id_in(&self, task: &Value) -> String {
        let name = task["metadata"]["name"].as_str().unwrap();
        let out = self.legion(&["exec", name, "-n", "handoff", "--", "cat", "/id"], &[]);
        assert!(
            out.status.success(),
            "exec in {name}: {}",
            text(&out.stderr)
        );
        text(&out.stdout).trim().to_owned()
    }

    /// Waits for `n` running tasks of `service`, each having run `manifest`
    /// for `image`, then checks they hold the build `id` and, if `pinned`,
    /// that the service and its tasks pin `image` to it.
    fn runs(
        &self,
        service: &str,
        image: &str,
        n: usize,
        manifest: &str,
        pinned: Option<&str>,
        id: &str,
    ) {
        wait(
            &format!("{n} tasks of {service} running {manifest}"),
            600,
            || {
                let tasks = self.running(service);
                tasks.len() == n
                    && tasks
                        .iter()
                        .all(|t| t["status"]["images"][image] == manifest)
            },
        );
        let images = &self.service(service)["status"]["images"];
        match pinned {
            Some(pinned) => {
                assert_eq!(images["resolved"][image], pinned, "{images}");
                assert!(images.get("message").is_none(), "{images}");
            }
            None => assert!(images["resolved"].get(image).is_none(), "{images}"),
        }
        for t in self.running(service) {
            match pinned {
                Some(pinned) => assert_eq!(t["spec"]["images"][image], pinned, "{t}"),
                None => assert!(t["spec"]["images"].get(image).is_none(), "{t}"),
            }
            assert_eq!(self.id_in(&t), id, "the build jailer recorded");
        }
    }

    fn delete(&self, service: &str) {
        self.ok(&["delete", "service", service, "-n", "handoff"]);
        wait(&format!("{service}'s tasks gone"), 180, || {
            self.running(service).is_empty()
        });
    }
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_archive_runs_as_exported() {
    let mut lab = Lab::from_env();
    let jailer = Jailer::new(&lab, &[]);
    let step = |what: &str| eprintln!("--- {what}");

    step("jailer builds an image and exports it under two names");
    let rec = lab.dir.join("offline.json");
    let id = unique("offline");
    jailer.build(&lab.dir, "handoff/app:1", &id, &rec);
    let local = lab.dir.join("local.oci.tar");
    let named = lab.dir.join("named.oci.tar");
    jailer.run(&[
        "export",
        "--record",
        rec.to_str().unwrap(),
        "handoff/app:1",
        local.to_str().unwrap(),
    ]);
    // A registry's name that resolves nowhere: asking it fails.
    let registry_named = "registry.invalid/handoff/app:1";
    jailer.run(&[
        "export",
        "--record",
        rec.to_str().unwrap(),
        "--name",
        registry_named,
        "handoff/app:1",
        named.to_str().unwrap(),
    ]);
    let r = record(&rec);
    assert_eq!(r.archives.len(), 2);
    for a in &r.archives {
        assert_eq!(
            a.manifest, r.image.manifest,
            "an archive keeps the manifest"
        );
    }

    step("every node loads both archives");
    lab.three_nodes(&[]);
    lab.load_image(local.to_str().unwrap());
    lab.load_image(named.to_str().unwrap());
    lab.apply("namespace", NAMESPACE);

    step("a localhost/ name runs as loaded, asking no registry");
    let local_name = "localhost/handoff/app:1";
    lab.apply("local", &service("local", local_name, 3, ""));
    lab.runs("local", local_name, 3, &r.image.manifest, None, &id);
    lab.delete("local");

    step("a registry's name runs as loaded under pullPolicy: never");
    let never = "        pullPolicy: never\n";
    lab.apply("never", &service("never", registry_named, 3, never));
    lab.runs("never", registry_named, 3, &r.image.manifest, None, &id);
    lab.delete("never");

    step("without it, the registry is asked, and nothing starts");
    lab.apply("asks", &service("asks", registry_named, 1, ""));
    wait("the resolver's message", 120, || {
        lab.service("asks")["status"]["images"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty())
    });
    assert!(lab.running("asks").is_empty(), "no task of it runs");
    lab.delete("asks");
    step("done");
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_push_is_what_legion_runs() {
    let mut lab = Lab::from_env();
    let registry = Registry::start(&lab);
    let addr = registry.addr.clone();
    let jailer = Jailer::new(&lab, &[&addr]);
    let image = format!("{addr}/handoff/app:1");
    let step = |what: &str| eprintln!("--- {what}");

    // Builds with `flags` and pushes a new image to `image`, as recorded in
    // `name`.json.
    let dir = lab.dir.clone();
    let push = |name: &str, flags: &[&str]| {
        let rec = dir.join(format!("{name}.json"));
        let id = unique(name);
        let tag = format!("handoff/{name}:1");
        jailer.build_with(&dir, &tag, &id, &rec, &[], flags);
        jailer.run(&["push", "--record", rec.to_str().unwrap(), &tag, &image]);
        let r = record(&rec);
        let p = r.published.last().expect("the push is recorded");
        assert_eq!(p.reference, image);
        assert_eq!(p.digest, r.image.manifest, "one platform: its manifest");
        assert_eq!(p.pinned, format!("{addr}/handoff/app@{}", p.digest));
        (p.pinned.clone(), r.image.manifest.clone(), id)
    };

    step("jailer builds an image and pushes it");
    let (pinned1, manifest1, id1) = push("first", &[]);

    step("the cluster resolves the tag to what jailer pushed");
    lab.flags = format!("{} --insecure-registry {addr}", lab.flags);
    lab.three_nodes(&["--insecure-registry", &addr]);
    lab.apply("namespace", NAMESPACE);
    lab.apply("web", &service("web", &image, 2, ""));
    lab.runs("web", &image, 2, &manifest1, Some(&pinned1), &id1);

    step("jailer pushes another build to the same tag, its layer in zstd");
    let (pinned2, manifest2, id2) = push("second", &["--compression", "zstd"]);
    assert_ne!(manifest2, manifest1);
    let held: Value =
        serde_json::from_str(&jailer.run(&["manifest", "inspect", &pinned2])).unwrap();
    let zstd = "application/vnd.oci.image.layer.v1.tar+zstd";
    assert_eq!(
        held["layers"].as_array().unwrap().last().unwrap()["mediaType"],
        zstd,
        "its own layer: {held}"
    );

    step("scaling is not a deployment: a new task runs the first build");
    lab.apply("web", &service("web", &image, 3, ""));
    lab.runs("web", &image, 3, &manifest1, Some(&pinned1), &id1);

    step("a deployment resolves the tag again: the second build");
    let env = "        env: [{ name: DEPLOY, value: \"2\" }]\n";
    lab.apply("web", &service("web", &image, 3, env));
    lab.runs("web", &image, 3, &manifest2, Some(&pinned2), &id2);

    step("a rollback runs the first build again, though the tag moved");
    lab.ok(&["rollback", "web", "-n", "handoff"]);
    lab.runs("web", &image, 3, &manifest1, Some(&pinned1), &id1);
    lab.delete("web");
    step("done");
}

/// The writer of [`a_jailer_image_keeps_its_data_through_a_drain`]: the
/// first start keeps the build it ran and some random data; every start
/// then logs lines of its own until it is stopped, and logs that last.
const WRITER: &str = r#"me=$(date +%s)-$$
if [ ! -f /data/born ]; then
  dd if=/dev/random of=/data/blob bs=64k count=4 2>/dev/null || exit 1
  cat /id > /data/born
fi
trap 'echo "$me stop" >> /data/log; exit 0' TERM INT
n=0
while :; do
  n=$((n + 1))
  echo "$me $n" >> /data/log
  sleep 0.2
done
"#;

/// A stateful Service of one task running the writer of `image` on a
/// Volume of its own, with `more` of its spec's fields, indented to them.
fn keeper(image: &str, more: &str) -> String {
    format!(
        "apiVersion: legion/v1
kind: Service
metadata: {{ name: keep, namespace: handoff }}
spec:
  mode: stateful
  replicas: 1
{more}  pod:
    containers:
      - name: app
        image: {image}
        command: [/bin/sh, /writer]
        volumeMounts: [{{ name: data, path: /data }}]
    volumes: [{{ name: data, claim: {{ size: 256Mi }} }}]
"
    )
}

impl Lab {
    /// What `path` holds in a task, as bytes.
    fn file_in(&self, task: &Value, path: &str) -> Vec<u8> {
        let name = task["metadata"]["name"].as_str().unwrap();
        let out = self.legion(&["exec", name, "-n", "handoff", "--", "cat", path], &[]);
        assert!(
            out.status.success(),
            "reading {path} in {name}: {}",
            text(&out.stderr)
        );
        out.stdout
    }

    /// The one running task of `service`, once it runs `manifest` for
    /// `image` and has logged a line.
    fn one(&self, service: &str, image: &str, manifest: &str) -> Value {
        let mut found = None;
        wait(&format!("{service} running {manifest}"), 600, || {
            let tasks = self.running(service);
            found = match tasks.as_slice() {
                [t] if t["status"]["images"][image] == manifest => Some(t.clone()),
                _ => None,
            };
            found.as_ref().is_some_and(|t| {
                let name = t["metadata"]["name"].as_str().unwrap();
                let out = self.legion(
                    &[
                        "exec",
                        name,
                        "-n",
                        "handoff",
                        "--",
                        "test",
                        "-s",
                        "/data/log",
                    ],
                    &[],
                );
                out.status.success()
            })
        });
        found.unwrap()
    }
}

/// The writers in a log, in the order they first wrote, each with its
/// lines; a writer that comes back after another wrote is an error.
fn writers(log: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for line in log.lines() {
        let (me, what) = line.split_once(' ').unwrap_or_else(|| panic!("{line:?}"));
        match out.last_mut() {
            Some((last, lines)) if last == me => lines.push(what.to_owned()),
            _ => {
                assert!(
                    out.iter().all(|(w, _)| w != me),
                    "{me} wrote again after another writer: two writers\n{log}"
                );
                out.push((me.to_owned(), vec![what.to_owned()]));
            }
        }
    }
    out
}

/// Checks the writer's log: the first task's lines, every one of them, up
/// to its last, which says it stopped, then the moved task's, and no line
/// of the first after that.
fn one_writer_at_a_time(log: &str) {
    let w = writers(log);
    assert_eq!(w.len(), 2, "the first task, then the moved one\n{log}");
    let (old, new) = (&w[0].1, &w[1].1);
    assert_eq!(
        old.last().map(String::as_str),
        Some("stop"),
        "the first task stopped before its data moved, and its last line came along\n{log}"
    );
    assert!(
        old.len() > 10 && new.len() > 10,
        "both wrote for a while\n{log}"
    );
    for (n, line) in old[..old.len() - 1].iter().enumerate() {
        assert_eq!(*line, (n + 1).to_string(), "no line of it lost\n{log}");
    }
}

#[test]
fn a_log_tells_writers_apart() {
    let w = writers("a 1\na 2\na stop\nb 1\nb 2\n");
    assert_eq!(w.len(), 2);
    assert_eq!(
        w[0],
        ("a".into(), vec!["1".into(), "2".into(), "stop".into()])
    );
    assert_eq!(w[1].1.len(), 2);
}

#[test]
#[should_panic(expected = "two writers")]
fn a_writer_back_after_another_is_two_writers() {
    writers("a 1\nb 1\na 2\n");
}

#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_image_keeps_its_data_through_a_drain() {
    let mut lab = Lab::from_env();
    let jailer = Jailer::new(&lab, &[]);
    let step = |what: &str| eprintln!("--- {what}");

    step("jailer builds a writer, its layers in zstd, and exports it");
    let rec = lab.dir.join("keep.json");
    let id = unique("keep");
    jailer.build_with(
        &lab.dir,
        "handoff/keep:1",
        &id,
        &rec,
        &[("writer", WRITER)],
        &["--compression", "zstd"],
    );
    let tar = lab.dir.join("keep.oci.tar");
    jailer.run(&[
        "export",
        "--record",
        rec.to_str().unwrap(),
        "handoff/keep:1",
        tar.to_str().unwrap(),
    ]);
    let r = record(&rec);
    let manifest = r.image.manifest.clone();
    assert_eq!(r.archives.last().unwrap().manifest, manifest);

    step("a stateful service runs it, writing to its Volume");
    lab.three_nodes(&[]);
    lab.load_image(tar.to_str().unwrap());
    lab.apply("namespace", NAMESPACE);
    let image = "localhost/handoff/keep:1";
    lab.apply("keep", &keeper(image, ""));
    let before = lab.one("keep", image, &manifest);
    assert_eq!(lab.id_in(&before), id, "the build jailer recorded");
    assert_eq!(text(&lab.file_in(&before, "/data/born")), id);
    let blob = lab.file_in(&before, "/data/blob");
    assert_eq!(blob.len(), 256 * 1024);
    let from = before["spec"]["node"].as_str().unwrap().to_owned();
    // A few seconds of lines before the move.
    std::thread::sleep(std::time::Duration::from_secs(3));

    step(&format!("draining {from} moves the task and its data"));
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
    let done: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(done["phase"], "completed", "{done}");
    let after = lab.one("keep", image, &manifest);
    let to = after["spec"]["node"].as_str().unwrap().to_owned();
    assert_ne!(to, from, "the task left {from}");
    assert_ne!(after["metadata"]["name"], before["metadata"]["name"]);
    lab.ok(&["node", "activate", &from]);

    step("the image and the data are the same; one writer at a time");
    assert_eq!(lab.id_in(&after), id, "the build jailer recorded");
    assert_eq!(text(&lab.file_in(&after, "/data/born")), id);
    assert!(
        lab.file_in(&after, "/data/blob") == blob,
        "the data came along"
    );
    // Lines from the moved task, then the whole log.
    std::thread::sleep(std::time::Duration::from_secs(3));
    one_writer_at_a_time(&text(&lab.file_in(&after, "/data/log")));
    assert!(
        lab.running("keep").len() == 1,
        "one task of the service runs"
    );
    lab.delete("keep");
    step("done");
}

/// The registry [`a_jailer_push_is_what_legion_runs`] runs, as this same
/// program, on the host's lab address.
struct Registry {
    addr: String,
    child: Child,
    /// The file that takes the registry down, as [`Registry::down`] says.
    down: PathBuf,
}

impl Registry {
    fn start(lab: &Lab) -> Registry {
        let file = lab.dir.join("registry.addr");
        let down = PathBuf::from(format!("{}.down", file.display()));
        let _ = std::fs::remove_file(&down);
        let log = std::fs::File::create(lab.dir.join("registry.log")).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "registry", "--test-threads=1"])
            .env("LEGION_HANDOFF_REGISTRY", &file)
            .env("LEGION_HANDOFF_REGISTRY_HOST", &lab.host)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut r = Registry {
            addr: String::new(),
            child,
            down,
        };
        wait("the registry", 30, || file.exists());
        r.addr = std::fs::read_to_string(&file).unwrap();
        r
    }

    /// With `Some("signatures")` the registry answers 503 for signature
    /// manifests, with `Some("all")` for every request, and with `None` as
    /// usual again; what it holds stays.
    fn down(&self, mode: Option<&str>) {
        match mode {
            Some(m) => std::fs::write(&self.down, m).unwrap(),
            None => {
                let _ = std::fs::remove_file(&self.down);
            }
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What the registry holds: blobs by digest, and manifests by repository
/// and tag or digest, with their media types.
#[derive(Default)]
struct Held {
    blobs: BTreeMap<String, Vec<u8>>,
    manifests: BTreeMap<(String, String), (String, Vec<u8>)>,
    uploads: u64,
}

/// The test's registry, run by [`a_jailer_push_is_what_legion_runs`]:
/// plain HTTP on `LEGION_HANDOFF_REGISTRY_HOST`, without authentication,
/// taking monolithic uploads into memory. It writes the address it took to
/// the file `LEGION_HANDOFF_REGISTRY`, and is down as that file's name with
/// `.down` says (see [`Registry::down`]).
#[test]
#[ignore = "the registry of a_jailer_push_is_what_legion_runs, run by it"]
fn registry() {
    let Ok(file) = std::env::var("LEGION_HANDOFF_REGISTRY") else {
        return;
    };
    DOWN.set(format!("{file}.down")).unwrap();
    let host = std::env::var("LEGION_HANDOFF_REGISTRY_HOST").unwrap();
    let listener = std::net::TcpListener::bind(format!("{host}:0")).unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    // Whole or not at all: the test reads it once it is there.
    std::fs::write(format!("{file}.new"), &addr).unwrap();
    std::fs::rename(format!("{file}.new"), &file).unwrap();
    let held = Arc::new(Mutex::new(Held::default()));
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let held = held.clone();
        std::thread::spawn(move || {
            let _ = serve(stream, &held);
        });
    }
}

/// Answers the requests of one connection until it closes.
fn serve(stream: std::net::TcpStream, held: &Mutex<Held>) -> std::io::Result<()> {
    let mut out = stream.try_clone()?;
    let mut read = BufReader::new(stream);
    loop {
        let mut first = String::new();
        if read.read_line(&mut first)? == 0 {
            return Ok(());
        }
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or("").to_owned();
        let target = parts.next().unwrap_or("").to_owned();
        let (mut length, mut media) = (0usize, String::new());
        loop {
            let mut line = String::new();
            if read.read_line(&mut line)? == 0 || line.trim().is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                match k.trim().to_ascii_lowercase().as_str() {
                    "content-length" => length = v.trim().parse().unwrap_or(0),
                    "content-type" => media = v.trim().to_owned(),
                    _ => {}
                }
            }
        }
        let mut body = vec![0; length];
        read.read_exact(&mut body)?;
        let (status, headers, reply) = answer(&method, &target, &media, body, held);
        let mut head = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", reply.len());
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("\r\n");
        out.write_all(head.as_bytes())?;
        if method != "HEAD" {
            out.write_all(&reply)?;
        }
        out.flush()?;
    }
}

/// The file whose text takes the registry down: `signatures` or `all`.
static DOWN: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The status, headers and body for one request. A HEAD gets a GET's
/// headers, its Content-Length included, and no body.
fn answer(
    method: &str,
    target: &str,
    media: &str,
    body: Vec<u8>,
    held: &Mutex<Held>,
) -> (&'static str, Vec<(&'static str, String)>, Vec<u8>) {
    const NOT_FOUND: &str = "404 Not Found";
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let last = path.rsplit('/').next().unwrap_or("").to_owned();
    let down = DOWN
        .get()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .unwrap_or_default();
    let signature = path.contains("/manifests/") && last.ends_with(".sig");
    if down == "all" || (down == "signatures" && signature) {
        return ("503 Service Unavailable", Vec::new(), Vec::new());
    }
    let mut h = held.lock().unwrap();
    if path == "/v2/" {
        return ("200 OK", Vec::new(), Vec::new());
    }
    if let Some(repo) = path
        .strip_prefix("/v2/")
        .and_then(|p| p.split_once("/manifests/"))
    {
        let key = (repo.0.to_owned(), repo.1.to_owned());
        return match method {
            "PUT" => {
                let digest = legion_image::sha256(&body);
                let at = format!("/v2/{}/manifests/{digest}", key.0);
                h.manifests.insert(
                    (key.0.clone(), digest.clone()),
                    (media.to_owned(), body.clone()),
                );
                h.manifests.insert(key, (media.to_owned(), body));
                let headers = vec![("Location", at), ("Docker-Content-Digest", digest)];
                ("201 Created", headers, Vec::new())
            }
            "GET" | "HEAD" => match h.manifests.get(&key) {
                Some((media, doc)) => {
                    let headers = vec![
                        ("Content-Type", media.clone()),
                        ("Docker-Content-Digest", legion_image::sha256(doc)),
                    ];
                    ("200 OK", headers, doc.clone())
                }
                None => (NOT_FOUND, Vec::new(), Vec::new()),
            },
            _ => (NOT_FOUND, Vec::new(), Vec::new()),
        };
    }
    if method == "POST" && path.ends_with("/blobs/uploads/") {
        h.uploads += 1;
        let at = format!("{path}{}", h.uploads);
        return ("202 Accepted", vec![("Location", at)], Vec::new());
    }
    if method == "PUT" && path.contains("/blobs/uploads/") {
        let digest = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("digest="))
            .unwrap_or("")
            .replace("%3A", ":")
            .replace("%3a", ":");
        if legion_image::sha256(&body) != digest {
            return ("400 Bad Request", Vec::new(), Vec::new());
        }
        let repo = path.trim_start_matches("/v2/");
        let repo = &repo[..repo.find("/blobs/uploads/").unwrap()];
        let at = format!("/v2/{repo}/blobs/{digest}");
        h.blobs.insert(digest.clone(), body);
        return (
            "201 Created",
            vec![("Location", at), ("Docker-Content-Digest", digest)],
            Vec::new(),
        );
    }
    if (method == "GET" || method == "HEAD") && path.contains("/blobs/") {
        return match h.blobs.get(&last) {
            Some(b) => (
                "200 OK",
                vec![
                    ("Content-Type", "application/octet-stream".to_owned()),
                    ("Docker-Content-Digest", last.clone()),
                ],
                b.clone(),
            ),
            None => (NOT_FOUND, Vec::new(), Vec::new()),
        };
    }
    (NOT_FOUND, Vec::new(), Vec::new())
}

impl Lab {
    /// The Volume of the writer's task.
    fn keep_volume(&self) -> Value {
        self.list("volumes", "handoff")
            .into_iter()
            .find(|v| v["metadata"]["name"] == "data-keep-0")
            .unwrap_or_default()
    }

    /// Runs `args` in a task, which must work.
    fn exec_in(&self, task: &Value, args: &[&str]) {
        let name = task["metadata"]["name"].as_str().unwrap();
        let mut all = vec!["exec", name, "-n", "handoff", "--"];
        all.extend_from_slice(args);
        let out = self.legion(&all, &[]);
        assert!(
            out.status.success(),
            "{args:?} in {name}: {}",
            text(&out.stderr)
        );
    }
}

/// As [`a_jailer_image_keeps_its_data_through_a_drain`], with the move cut
/// short (PLAN.md phase 6, as `stateful.rs` cuts Postgres's): the full copy
/// is made while the task runs, as its service lets none stop; then the
/// task may stop, and the last part's `zfs receive` is killed as it
/// receives. The part is sent again, and the moved task has every byte and
/// every line. `n1` is paused, so the task and its move are on the VMs.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_image_keeps_its_data_through_a_cut_move() {
    let mut lab = Lab::from_env();
    let jailer = Jailer::new(&lab, &[]);
    let step = |what: &str| eprintln!("--- {what}");

    step("jailer builds a writer and exports it");
    let rec = lab.dir.join("cut.json");
    let id = unique("cut");
    jailer.build_with(
        &lab.dir,
        "handoff/keep:1",
        &id,
        &rec,
        &[("writer", WRITER)],
        &[],
    );
    let tar = lab.dir.join("cut.oci.tar");
    jailer.run(&[
        "export",
        "--record",
        rec.to_str().unwrap(),
        "handoff/keep:1",
        tar.to_str().unwrap(),
    ]);
    let manifest = record(&rec).image.manifest;

    step("a stateful service runs it on a VM; its task may not stop yet");
    lab.three_nodes(&[]);
    lab.load_image(tar.to_str().unwrap());
    lab.apply("namespace", NAMESPACE);
    lab.ok(&["node", "pause", "n1"]);
    let image = "localhost/handoff/keep:1";
    lab.apply("keep", &keeper(image, "  maxUnavailable: 0\n"));
    let before = lab.one("keep", image, &manifest);
    assert_eq!(lab.id_in(&before), id, "the build jailer recorded");
    let blob = lab.file_in(&before, "/data/blob");
    let from = before["spec"]["node"].as_str().unwrap().to_owned();
    let to = if from == "n2" { "n3" } else { "n2" };
    eprintln!("keep on {from}, to move to {to}");
    let ds = format!("{ZFS_ROOT}/volumes/handoff/data-keep-0");

    step(&format!(
        "draining {from}: the full copy, made as the task runs"
    ));
    lab.ok(&["node", "drain", &from]);
    wait("the full copy", 600, || {
        lab.keep_volume()["status"]["migration"]["phase"] == "copied"
    });
    assert_eq!(lab.keep_volume()["status"]["migration"]["to"], to);
    assert_eq!(lab.running("keep").len(), 1, "the task runs on");

    step("data the last part must carry, then the task may stop: its receive is cut");
    // Enough that the last part takes a while to receive.
    lab.exec_in(
        &before,
        &[
            "dd",
            "if=/dev/random",
            "of=/data/ballast",
            "bs=1m",
            "count=64",
        ],
    );
    let ballast = lab.file_in(&before, "/data/ballast");
    assert_eq!(ballast.len(), 64 << 20);
    let out = std::thread::scope(|s| {
        let killer =
            s.spawn(|| lab.on(to, &kill_receiving(&ds, &format!("zfs receive -F -u {ds}"))));
        lab.apply("keep", &keeper(image, "  maxUnavailable: 1\n"));
        killer.join().unwrap()
    });
    assert!(
        out.status.success(),
        "killing the last part's receive: {}",
        text(&out.stderr)
    );
    let v = lab.keep_volume();
    assert_eq!(v["status"]["node"], from.as_str(), "{v}");
    assert_eq!(v["status"]["migration"]["phase"], "finishing", "{v}");

    step("the part sent again, the move ends on the VM it was going to");
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
    let done: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(done["phase"], "completed", "{done}");
    let after = lab.one("keep", image, &manifest);
    assert_eq!(after["spec"]["node"], to);
    assert_eq!(lab.keep_volume()["status"]["node"], to);

    step("the image and every byte the same; one writer at a time; nothing left over");
    assert_eq!(lab.id_in(&after), id, "the build jailer recorded");
    assert_eq!(text(&lab.file_in(&after, "/data/born")), id);
    assert!(lab.file_in(&after, "/data/blob") == blob, "the first data");
    assert!(
        lab.file_in(&after, "/data/ballast") == ballast,
        "the last part's data"
    );
    std::thread::sleep(std::time::Duration::from_secs(3));
    one_writer_at_a_time(&text(&lab.file_in(&after, "/data/log")));
    assert_eq!(
        lab.holds(to, "data-keep-0"),
        ds,
        "the move's snapshots gone"
    );
    wait("the old copy dropped", 300, || {
        lab.holds(&from, "data-keep-0").is_empty()
    });
    lab.delete("keep");
    step("done");
}

/// An ImagePolicy wanting one of `keys` (PEM) on the images `images` name.
fn policy(name: &str, images: &[&str], keys: &[&str], mode: &str) -> String {
    let images = images
        .iter()
        .map(|i| format!("\"{i}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut yaml = format!(
        "apiVersion: legion/v1
kind: ImagePolicy
metadata: {{ name: {name} }}
spec:
  images: [{images}]
  mode: {mode}
  keys:
"
    );
    for key in keys {
        yaml.push_str("    - |\n");
        for line in key.lines() {
            yaml.push_str(&format!("      {line}\n"));
        }
    }
    yaml
}

impl Lab {
    /// What a service's status says of its images.
    fn images(&self, service: &str) -> Value {
        self.service(service)["status"]["images"].clone()
    }

    /// Waits for `service`'s pod to be held with a message starting `says`.
    fn held(&self, service: &str, says: &str) {
        wait(&format!("{service} held: {says}"), 180, || {
            self.images(service)["message"]
                .as_str()
                .is_some_and(|m| m.starts_with(says))
        });
    }

    /// Checks that `image` of `service` was verified on `digest` by `key`.
    fn verified_by(&self, service: &str, image: &str, digest: &str, key: &str) {
        let v = &self.images(service)["verified"][image];
        assert_eq!(v["digest"], digest, "{v}");
        assert_eq!(v["key"], key, "{v}");
    }

    /// Applies `yaml`, which must be refused with a message holding `says`.
    fn refused(&self, name: &str, yaml: &str, says: &str) {
        let path = self.dir.join(format!("{name}.yaml"));
        std::fs::write(&path, yaml).unwrap();
        let out = self.legion(&["apply", "-f", path.to_str().unwrap()], &[]);
        let said = format!("{}{}", text(&out.stdout), text(&out.stderr));
        assert!(!out.status.success(), "{name} was taken: {said}");
        assert!(said.contains(says), "{name}: {said}");
    }
}

/// jailer's signature, as Legion checks it (PLAN.md phase 7): an
/// ImagePolicy wanting jailer's key runs what jailer signed, and holds what
/// it did not, or what it cannot check, the old tasks running.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_signature_is_what_legion_checks() {
    let mut lab = Lab::from_env();
    let registry = Registry::start(&lab);
    let addr = registry.addr.clone();
    let jailer = Jailer::new(&lab, &[&addr]);
    let step = |what: &str| eprintln!("--- {what}");

    step("jailer makes two key pairs");
    let path = |p: PathBuf| p.to_str().unwrap().to_owned();
    for name in ["good", "other"] {
        jailer.run(&["key", "generate", &path(lab.dir.join(name))]);
    }
    let good_key = path(lab.dir.join("good.key"));
    let good_pub = path(lab.dir.join("good.pub"));
    let other_key = path(lab.dir.join("other.key"));
    let pem = std::fs::read_to_string(&good_pub).unwrap();
    let key = legion_image::cosign::PublicKey::from_pem(&pem)
        .unwrap()
        .fingerprint()
        .to_owned();

    // Builds a new image and pushes it to `image`, signed with `sign` if
    // given: its pinned reference, digest and id, as recorded.
    let dir = lab.dir.clone();
    let push = |name: &str, image: &str, sign: Option<&str>| {
        let rec = dir.join(format!("{name}.json"));
        let id = unique(name);
        let tag = format!("handoff/{name}:1");
        jailer.build(&dir, &tag, &id, &rec);
        let mut args = vec!["push", "--record", rec.to_str().unwrap()];
        if let Some(k) = sign {
            args.extend(["--sign-key", k]);
        }
        args.extend([tag.as_str(), image]);
        jailer.run(&args);
        let r = record(&rec);
        let p = r.published.last().expect("the push is recorded");
        assert_eq!(p.reference, image);
        (p.pinned.clone(), p.digest.clone(), id)
    };

    step("jailer pushes a signed image and checks its signature");
    let image = format!("{addr}/handoff/app:1");
    let (pinned1, digest1, id1) = push("signed", &image, Some(&good_key));
    jailer.run(&["verify", "--key", &good_pub, &image]);

    step("a policy without a registry, or with a bad key, is refused");
    lab.flags = format!("{} --insecure-registry {addr}", lab.flags);
    lab.three_nodes(&["--insecure-registry", &addr]);
    lab.apply("namespace", NAMESPACE);
    lab.refused(
        "nohost",
        &policy("nohost", &["handoff/**"], &[&pem], "enforce"),
        "handoff/**",
    );
    let bad = "-----BEGIN PUBLIC KEY-----\nbm90IGEga2V5\n-----END PUBLIC KEY-----\n";
    lab.refused(
        "badkey",
        &policy("badkey", &[&format!("{addr}/**")], &[bad], "enforce"),
        "key",
    );

    step("policies: jailer's key enforced on the registry and loaded images, warned elsewhere");
    let enforced = [
        format!("{addr}/handoff/**"),
        "localhost/handoff/**".to_owned(),
    ];
    let enforced: Vec<&str> = enforced.iter().map(String::as_str).collect();
    lab.apply(
        "policy-signed",
        &policy("signed", &enforced, &[&pem], "enforce"),
    );
    lab.apply(
        "policy-warned",
        &policy("warned", &[&format!("{addr}/warned/**")], &[&pem], "warn"),
    );

    step("the signed image runs, checked on the digest jailer signed");
    lab.apply("web", &service("web", &image, 2, ""));
    lab.runs("web", &image, 2, &digest1, Some(&pinned1), &id1);
    lab.verified_by("web", &image, &digest1, &key);

    step("so does the record's reference by digest");
    lab.apply("pinned", &service("pinned", &pinned1, 1, ""));
    lab.runs("pinned", &pinned1, 1, &digest1, None, &id1);
    lab.verified_by("pinned", &pinned1, &digest1, &key);
    lab.delete("pinned");

    step("an unsigned push is held until jailer signs it");
    let plain = format!("{addr}/handoff/plain:1");
    let (pinned2, digest2, id2) = push("plain", &plain, None);
    lab.apply("plain", &service("plain", &plain, 1, ""));
    lab.held("plain", &format!("{plain} is unsigned"));
    assert!(lab.running("plain").is_empty(), "no task of it runs");
    jailer.run(&["sign", "--key", &good_key, &plain]);
    lab.runs("plain", &plain, 1, &digest2, Some(&pinned2), &id2);
    lab.verified_by("plain", &plain, &digest2, &key);
    lab.delete("plain");

    step("one signed by another key is refused, and stays refused");
    let rogue = format!("{addr}/handoff/rogue:1");
    push("rogue", &rogue, Some(&other_key));
    lab.apply("rogue", &service("rogue", &rogue, 1, ""));
    let refusal = format!("bad signature on {rogue}");
    lab.held("rogue", &refusal);
    // Past a retry.
    std::thread::sleep(std::time::Duration::from_secs(40));
    let images = lab.images("rogue");
    assert!(
        images["message"]
            .as_str()
            .is_some_and(|m| m.starts_with(&refusal)),
        "{images}"
    );
    assert!(lab.running("rogue").is_empty(), "no task of it runs");
    lab.delete("rogue");

    step("under warn, an unsigned image runs, and the status says so");
    let warned = format!("{addr}/warned/app:1");
    let (pinned3, digest3, id3) = push("warned", &warned, None);
    lab.apply("warned", &service("warned", &warned, 1, ""));
    wait("the warned task", 600, || {
        let tasks = lab.running("warned");
        tasks.len() == 1 && tasks[0]["status"]["images"][&warned] == digest3.as_str()
    });
    let images = lab.images("warned");
    assert_eq!(images["resolved"][&warned], pinned3.as_str(), "{images}");
    assert!(images.get("message").is_none(), "{images}");
    let unsigned = format!("{warned} is unsigned");
    assert!(
        images["warning"]
            .as_str()
            .is_some_and(|w| w.starts_with(&unsigned)),
        "{images}"
    );
    assert!(images["verified"].get(&warned).is_none(), "{images}");
    assert_eq!(lab.id_in(&lab.running("warned")[0]), id3);
    lab.delete("warned");

    step("an update whose signature the registry cannot give waits, the old tasks running");
    let (pinned4, digest4, id4) = push("next", &image, Some(&good_key));
    registry.down(Some("signatures"));
    let deploy = |n: u32| format!("        env: [{{ name: DEPLOY, value: \"{n}\" }}]\n");
    lab.apply("web", &service("web", &image, 2, &deploy(2)));
    lab.held("web", &format!("cannot check the signature of {image}"));
    let old = lab.running("web");
    assert_eq!(old.len(), 2, "the old tasks run on");
    for t in &old {
        assert_eq!(t["status"]["images"][&image], digest1.as_str(), "{t}");
    }

    step("the registry back, the update goes through");
    registry.down(None);
    lab.runs("web", &image, 2, &digest4, Some(&pinned4), &id4);
    lab.verified_by("web", &image, &digest4, &key);

    step("with the registry down altogether, the next update waits too");
    registry.down(Some("all"));
    lab.apply("web", &service("web", &image, 2, &deploy(3)));
    wait("web held", 180, || {
        lab.images("web")["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty())
    });
    assert_eq!(lab.running("web").len(), 2, "the old tasks run on");
    registry.down(None);
    // The update builds the same image: the old tasks run its digest
    // already, so first the hold has to go, at the next try.
    wait("web no longer held", 180, || {
        lab.images("web").get("message").is_none()
    });
    lab.runs("web", &image, 2, &digest4, Some(&pinned4), &id4);
    lab.delete("web");

    step("a loaded image the policy names has no signature to check");
    let tar = lab.dir.join("signed.oci.tar");
    jailer.run(&["export", "handoff/signed:1", tar.to_str().unwrap()]);
    lab.load_image(tar.to_str().unwrap());
    let local = "localhost/handoff/signed:1";
    lab.apply("local", &service("local", local, 1, ""));
    lab.held("local", &format!("{local} is local and has no signature"));
    assert!(lab.running("local").is_empty(), "no task of it runs");
    lab.delete("local");
    step("done");
}

/// Checks a writer's log through moves and a restore: `n` writers, each
/// starting after the one before it stopped, every line of each there,
/// except that the one at `backed_up` (counting from 0) has no last line
/// saying it stopped: the backup the next writer was restored from was
/// taken as it ran, so its lines end where the backup was taken.
fn writers_in_turn(log: &str, n: usize, backed_up: usize) {
    let w = writers(log);
    assert_eq!(w.len(), n, "{n} writers, one after another\n{log}");
    for (i, (me, lines)) in w.iter().enumerate() {
        let numbered = if i == backed_up || i == n - 1 {
            &lines[..]
        } else {
            assert_eq!(
                lines.last().map(String::as_str),
                Some("stop"),
                "writer {i} ({me}) stopped before the next began\n{log}"
            );
            &lines[..lines.len() - 1]
        };
        assert!(
            numbered.len() > 1,
            "writer {i} ({me}) wrote for a while\n{log}"
        );
        for (k, line) in numbered.iter().enumerate() {
            assert_eq!(
                *line,
                (k + 1).to_string(),
                "no line of writer {i} lost\n{log}"
            );
        }
    }
}

#[test]
fn a_backup_taken_as_a_writer_ran_ends_its_lines() {
    writers_in_turn("a 1\na 2\na stop\nb 1\nb 2\nb 3\nc 1\nc 2\n", 3, 1);
}

#[test]
#[should_panic(expected = "stopped before the next began")]
fn a_writer_cut_off_unbacked_is_refused() {
    writers_in_turn("a 1\na 2\nb 1\nb 2\n", 2, 5);
}

impl Lab {
    /// Checks that `service` is verified on `digest` by `key` for `image`,
    /// and that each of its running tasks runs that digest.
    fn carries(&self, service: &str, image: &str, digest: &str, key: &str, at: &str) {
        self.verified_by(service, image, digest, key);
        for t in self.running(service) {
            assert_eq!(
                t["status"]["images"][image], digest,
                "{at}: the digest jailer recorded\n{t}"
            );
        }
    }
}

/// Phase 8 of PLAN.md, jailer's shared qualification: one jailer image,
/// one identity, from its build to its recovery. jailer builds a writer,
/// signs it and pushes it; Legion runs it under an ImagePolicy enforcing
/// jailer's key, drains its node, moves it back with the move's last part
/// cut short, backs its Volume up, loses the host it is on and restores it
/// onto another. At every stage the task runs the digest jailer recorded,
/// the one the policy verified, and its data is the same, byte for byte.
///
/// A drain or a move keeps the verification the pod was given, for the
/// same digest; a restore applies the Service again, and the policy checks
/// the signature again before its task runs.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_image_goes_from_build_to_recovery() {
    let mut lab = Lab::from_env();
    let registry = Registry::start(&lab);
    let addr = registry.addr.clone();
    let jailer = Jailer::new(&lab, &[&addr]);
    let step = |what: &str| eprintln!("--- {what}");

    step("the versions: jailer, the Legion crates it was built with, and Legion");
    let version = |bin: &str| {
        let out = Command::new(bin).arg("--version").output().unwrap();
        assert!(
            out.status.success(),
            "{bin} --version: {}",
            text(&out.stderr)
        );
        text(&out.stdout).trim().to_owned()
    };
    let jailer_version = version(&jailer.bin);
    // jailer names the Legion crates it was built with, from its lock.
    assert!(
        jailer_version.contains(" (legion ") && !jailer_version.ends_with("(legion unknown)"),
        "jailer names the Legion crates it was built with: {jailer_version}"
    );
    let legion_version = version(common::LEGION);
    let tree = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.qualify-revision");
    let revision = std::fs::read_to_string(&tree)
        .map(|r| r.trim().to_owned())
        .unwrap_or_else(|_| "unrecorded".to_owned());
    eprintln!("versions: {jailer_version}; {legion_version} at {revision}");

    step("jailer makes a key pair, builds a writer, signs it and pushes it");
    let path = |p: PathBuf| p.to_str().unwrap().to_owned();
    jailer.run(&["key", "generate", &path(lab.dir.join("chain"))]);
    let sign_key = path(lab.dir.join("chain.key"));
    let public = path(lab.dir.join("chain.pub"));
    let pem = std::fs::read_to_string(&public).unwrap();
    let key = legion_image::cosign::PublicKey::from_pem(&pem)
        .unwrap()
        .fingerprint()
        .to_owned();
    let rec = lab.dir.join("chain.json");
    let id = unique("chain");
    jailer.build_with(
        &lab.dir,
        "handoff/chain:1",
        &id,
        &rec,
        &[("writer", WRITER)],
        &[],
    );
    let image = format!("{addr}/handoff/chain:1");
    jailer.run(&[
        "push",
        "--record",
        rec.to_str().unwrap(),
        "--sign-key",
        &sign_key,
        "handoff/chain:1",
        &image,
    ]);
    let r = record(&rec);
    let p = r.published.last().expect("the push is recorded");
    assert_eq!(p.reference, image);
    assert_eq!(
        p.digest, r.image.manifest,
        "what was built is what was pushed"
    );
    let (digest, pinned) = (p.digest.clone(), p.pinned.clone());
    let said = jailer.run(&["verify", "--key", &public, &image]);
    assert!(
        said.contains(&format!("is signed by the key {key}")),
        "jailer's check names the key Legion's policy will: {said}"
    );

    step("Legion, under a policy enforcing jailer's key, runs it on a VM");
    lab.flags = format!("{} --insecure-registry {addr}", lab.flags);
    lab.three_nodes(&["--insecure-registry", &addr]);
    lab.apply("namespace", NAMESPACE);
    lab.apply(
        "policy",
        &policy(
            "chain",
            &[&format!("{addr}/handoff/**")],
            &[&pem],
            "enforce",
        ),
    );
    // The task and its moves on the VMs, one of which the restore loses.
    lab.ok(&["node", "pause", "n1"]);
    let held = keeper(&image, "  maxUnavailable: 0\n");
    let free = keeper(&image, "  maxUnavailable: 1\n");
    lab.apply("keep", &free);
    let first = lab.one("keep", &image, &digest);
    assert_eq!(lab.images("keep")["resolved"][&image], pinned.as_str());
    lab.carries("keep", &image, &digest, &key, "deployed");
    assert_eq!(lab.id_in(&first), id, "the build jailer recorded");
    assert_eq!(text(&lab.file_in(&first, "/data/born")), id);
    let blob = lab.file_in(&first, "/data/blob");
    assert_eq!(blob.len(), 256 * 1024);
    let a = first["spec"]["node"].as_str().unwrap().to_owned();
    let b = if a == "n2" { "n3" } else { "n2" }.to_owned();
    eprintln!("keep on {a}; the other VM is {b}");
    std::thread::sleep(std::time::Duration::from_secs(3));

    let drain = |node: &str| {
        let out = lab.legion(
            &[
                "node",
                "drain",
                node,
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
    };

    step(&format!("drained: {a} to {b}, its data along"));
    drain(&a);
    let second = lab.one("keep", &image, &digest);
    assert_eq!(second["spec"]["node"], b.as_str());
    lab.ok(&["node", "activate", &a]);
    lab.carries("keep", &image, &digest, &key, "after the drain");
    assert_eq!(lab.id_in(&second), id, "the build jailer recorded");
    assert!(
        lab.file_in(&second, "/data/blob") == blob,
        "the data came along"
    );
    std::thread::sleep(std::time::Duration::from_secs(3));

    step(&format!(
        "moved back, {b} to {a}, the move's last part cut short"
    ));
    let ds = format!("{ZFS_ROOT}/volumes/handoff/data-keep-0");
    lab.apply("keep", &held);
    lab.ok(&["node", "drain", &b]);
    wait("the full copy", 600, || {
        lab.keep_volume()["status"]["migration"]["phase"] == "copied"
    });
    assert_eq!(lab.keep_volume()["status"]["migration"]["to"], a.as_str());
    assert_eq!(lab.running("keep").len(), 1, "the task runs on");
    // Enough that the last part takes a while to receive.
    lab.exec_in(
        &second,
        &[
            "dd",
            "if=/dev/random",
            "of=/data/ballast",
            "bs=1m",
            "count=64",
        ],
    );
    let ballast = lab.file_in(&second, "/data/ballast");
    assert_eq!(ballast.len(), 64 << 20);
    let out = std::thread::scope(|s| {
        let killer =
            s.spawn(|| lab.on(&a, &kill_receiving(&ds, &format!("zfs receive -F -u {ds}"))));
        lab.apply("keep", &free);
        killer.join().unwrap()
    });
    assert!(
        out.status.success(),
        "killing the last part's receive: {}",
        text(&out.stderr)
    );
    drain(&b);
    let third = lab.one("keep", &image, &digest);
    assert_eq!(third["spec"]["node"], a.as_str());
    assert_eq!(lab.keep_volume()["status"]["node"], a.as_str());
    lab.ok(&["node", "activate", &b]);
    lab.carries("keep", &image, &digest, &key, "after the cut move");
    assert_eq!(lab.id_in(&third), id, "the build jailer recorded");
    assert!(lab.file_in(&third, "/data/blob") == blob, "the first data");
    assert!(
        lab.file_in(&third, "/data/ballast") == ballast,
        "the last part's data"
    );
    std::thread::sleep(std::time::Duration::from_secs(3));

    step(&format!("the Volume backed up as its task runs on {a}"));
    let backup = lab.dir.join("data-keep-0.backup");
    lab.ok(&[
        "volume",
        "backup",
        "data-keep-0",
        "-n",
        "handoff",
        "-o",
        backup.to_str().unwrap(),
    ]);

    step(&format!(
        "{a} lost: its daemon killed, its data directory and pool gone"
    ));
    let vm = &lab.vms[common::vm(&a)];
    let out = lab.ssh(
        vm,
        &format!(
            "pkill -KILL -f 'legion-lab daemon'\n{}{}",
            common::clean(common::VM_DATA, common::VM_RUN),
            common::NO_POOL
        ),
    );
    assert!(out.status.success(), "losing {a}: {}", text(&out.stderr));

    step(&format!(
        "recovered: what {a} held cleared, the Volume restored onto {b}"
    ));
    lab.ok(&["delete", "service", "keep", "-n", "handoff"]);
    lab.ok(&["delete", "node", &a]);
    wait("data-keep-0 deleted", 120, || {
        let out = lab.legion(&["delete", "volume", "data-keep-0", "-n", "handoff"], &[]);
        out.status.success() || text(&out.stderr).contains("not found")
    });
    lab.ok(&["volume", "restore", backup.to_str().unwrap(), "--node", &b]);
    lab.apply("keep", &free);
    let fourth = lab.one("keep", &image, &digest);
    assert_eq!(
        fourth["spec"]["node"],
        b.as_str(),
        "where its data was restored"
    );
    lab.carries("keep", &image, &digest, &key, "after the restore");
    assert_eq!(lab.images("keep")["resolved"][&image], pinned.as_str());

    step("the image and every byte the same; each writer in turn");
    assert_eq!(lab.id_in(&fourth), id, "the build jailer recorded");
    assert_eq!(text(&lab.file_in(&fourth, "/data/born")), id);
    assert!(lab.file_in(&fourth, "/data/blob") == blob, "the first data");
    assert!(
        lab.file_in(&fourth, "/data/ballast") == ballast,
        "the cut move's data"
    );
    std::thread::sleep(std::time::Duration::from_secs(3));
    // The first task, the drained one, the moved one (backed up as it
    // ran), and the restored one.
    writers_in_turn(&text(&lab.file_in(&fourth, "/data/log")), 4, 2);

    step(&format!("{a} back as a new host"));
    let worker = lab.ok(&["join-token", "worker"]).trim().to_owned();
    lab.join(common::vm(&a), &worker, Role::Worker);
    wait(&format!("{a} ready again"), 120, || {
        lab.node_state(&a) == "ready"
    });
    lab.delete("keep");
    eprintln!("versions: {jailer_version}; {legion_version} at {revision}; image {digest}");
    step("done");
}

/// The arm64 half of [`a_jailer_index_runs_each_nodes_platform`]: built by
/// jailer on FreeBSD aarch64 from `FROM scratch` and an `/arch` saying
/// `arm64`, and exported; see `fixtures/README.md`. It never runs here.
const ARM64: &[u8] = include_bytes!("fixtures/jailer-arch-arm64.tar");

/// As [`service`], placed under `constraint` if given.
fn placed(name: &str, image: &str, replicas: u32, constraint: Option<&str>) -> String {
    let mut yaml = service(name, image, replicas, "");
    if let Some(c) = constraint {
        yaml.push_str(&format!("  placement:\n    constraints: [\"{c}\"]\n"));
    }
    yaml
}

impl Lab {
    /// Every task of `service`, whatever it is meant to do.
    fn tasks_of(&self, service: &str) -> Vec<Value> {
        self.list("tasks", "handoff")
            .into_iter()
            .filter(|t| t["spec"]["service"] == service)
            .collect()
    }

    /// Waits for `n` tasks of `service` meant to run, all pending on no
    /// node with `says`, and checks they stay so for 30 s, none rejected
    /// or failed meanwhile; returns how many were rejected in all.
    fn waits(&self, service: &str, n: usize, says: &str) -> usize {
        let pending = |lab: &Lab| {
            let live: Vec<Value> = lab
                .tasks_of(service)
                .into_iter()
                .filter(|t| t["spec"]["desiredState"] == "running")
                .collect();
            live.len() == n
                && live.iter().all(|t| {
                    t["status"]["state"] == "pending"
                        && t["spec"]["node"].as_str().unwrap_or("").is_empty()
                        && t["status"]["message"] == says
                })
        };
        wait(
            &format!("{n} tasks of {service} waiting: {says}"),
            300,
            || pending(self),
        );
        let ended = |lab: &Lab, state: &str| {
            lab.tasks_of(service)
                .iter()
                .filter(|t| t["status"]["state"] == state)
                .count()
        };
        let rejected = ended(self, "rejected");
        std::thread::sleep(std::time::Duration::from_secs(30));
        assert!(
            pending(self),
            "{service} stopped waiting: {:?}",
            self.tasks_of(service)
        );
        assert_eq!(ended(self, "rejected"), rejected, "{service} placed again");
        assert_eq!(ended(self, "failed"), 0, "{service}");
        rejected
    }

    /// The running tasks of `service`, by name, with what `/arch` says.
    fn arches(&self, service: &str) -> BTreeMap<String, String> {
        self.running(service)
            .iter()
            .map(|t| {
                let arch = String::from_utf8(self.file_in(t, "/arch")).unwrap();
                (t["metadata"]["name"].as_str().unwrap().to_owned(), arch)
            })
            .collect()
    }
}

/// One index across platforms (jailer's shared qualification): jailer
/// pushes an amd64 image built here and an arm64 one built on an arm64
/// host as one signed index. The service pins the index, the policy checks
/// its signature on that digest, and each node runs its own platform's
/// manifest. On these amd64 nodes a Service wanting arm64 waits, and an
/// index of arm64 alone is refused once on each node, then waits, until
/// the service runs other images.
#[test]
#[ignore = "needs root and ZFS on FreeBSD, two more nodes and jailer; see the module docs"]
fn a_jailer_index_runs_each_nodes_platform() {
    let mut lab = Lab::from_env();
    let registry = Registry::start(&lab);
    let addr = registry.addr.clone();
    let jailer = Jailer::new(&lab, &[&addr]);
    let step = |what: &str| eprintln!("--- {what}");
    let path = |p: PathBuf| p.to_str().unwrap().to_owned();

    step("jailer makes a key pair");
    jailer.run(&["key", "generate", &path(lab.dir.join("arch"))]);
    let sign_key = path(lab.dir.join("arch.key"));
    let pub_key = path(lab.dir.join("arch.pub"));
    let pem = std::fs::read_to_string(&pub_key).unwrap();
    let key = legion_image::cosign::PublicKey::from_pem(&pem)
        .unwrap()
        .fingerprint()
        .to_owned();

    step("jailer loads the arm64 image an arm64 host built, and builds the amd64 one");
    let archive = lab.dir.join("arch-arm64.tar");
    std::fs::write(&archive, ARM64).unwrap();
    let arm64 = "handoff/arch:arm64";
    jailer.run(&["load", "--name", arm64, &path(archive)]);
    let rec = lab.dir.join("arch.json");
    let id = unique("arch");
    let amd64 = "handoff/arch:amd64";
    jailer.build_with(&lab.dir, amd64, &id, &rec, &[("arch", "amd64\n")], &[]);

    // Pushes an index of `sources` to `image`, signed and recorded in
    // `rec`: its digest, its pinned reference, and its platforms.
    let push = |image: &str, sources: &[&str], rec: &Path| {
        let rec_path = path(rec.to_path_buf());
        let mut args = vec![
            "manifest",
            "push",
            "--sign-key",
            sign_key.as_str(),
            "--record",
            rec_path.as_str(),
            image,
        ];
        args.extend_from_slice(sources);
        jailer.run(&args);
        let r = record(rec);
        let p = r.published.last().expect("the push is recorded");
        assert_eq!(p.reference, image);
        let repo = image.rsplit_once(':').unwrap().0;
        assert_eq!(p.pinned, format!("{repo}@{}", p.digest));
        (p.digest.clone(), p.pinned.clone(), p.platforms.clone())
    };

    step("jailer pushes one signed index of both and checks its signature");
    let image = format!("{addr}/handoff/arch:1");
    let (index, pinned, platforms) = push(&image, &[amd64, arm64], &rec);
    let amd64_manifest = record(&rec).image.manifest;
    assert_eq!(platforms.len(), 2, "{platforms:?}");
    assert_eq!(platforms["freebsd/amd64"], amd64_manifest, "{platforms:?}");
    assert!(
        platforms.keys().any(|p| p.starts_with("freebsd/arm64")),
        "{platforms:?}"
    );
    assert_ne!(
        index, amd64_manifest,
        "the index, not a platform's manifest"
    );
    jailer.run(&["verify", "--key", &pub_key, &image]);

    step("a cluster of amd64 nodes, under a policy enforcing jailer's key");
    lab.flags = format!("{} --insecure-registry {addr}", lab.flags);
    lab.three_nodes(&["--insecure-registry", &addr]);
    lab.apply("namespace", NAMESPACE);
    let scope = format!("{addr}/handoff/**");
    lab.apply("policy", &policy("arch", &[&scope], &[&pem], "enforce"));

    step("the service pins the index, verified on it; every task runs amd64's manifest");
    lab.apply("multi", &placed("multi", &image, 3, None));
    lab.runs("multi", &image, 3, &amd64_manifest, Some(&pinned), &id);
    lab.verified_by("multi", &image, &index, &key);
    let before = lab.arches("multi");
    assert_eq!(before.len(), 3);
    assert!(before.values().all(|a| a == "amd64\n"), "{before:?}");

    step("a service wanting arm64 waits for an arm64 node, the other untouched");
    let arm = "node.platform.arch == arm64";
    lab.apply("armed", &placed("armed", &image, 2, Some(arm)));
    let rejected = lab.waits("armed", 2, "no node can run it: 3 constraints");
    assert_eq!(rejected, 0, "a constraint holds before any node tries");
    assert_eq!(lab.arches("multi"), before, "the first service as it was");
    lab.ok(&["delete", "service", "armed", "-n", "handoff"]);

    step("an index of arm64 alone is refused once on each node, then waits");
    let only = format!("{addr}/handoff/arch:arm64-only");
    let only_rec = lab.dir.join("arch-only.json");
    let (only_index, _, only_platforms) = push(&only, &[arm64], &only_rec);
    assert_eq!(only_platforms.len(), 1, "{only_platforms:?}");
    lab.apply("lone", &placed("lone", &only, 2, None));
    let rejected = lab.waits("lone", 2, "no node can run it: 3 no image for its platform");
    assert!((1..=3).contains(&rejected), "{rejected} rejected");
    for t in lab.tasks_of("lone") {
        if t["status"]["state"] == "rejected" {
            let wrong = &t["status"]["wrongPlatform"];
            assert_eq!(wrong["os"], "freebsd", "{t}");
            assert_eq!(wrong["arch"], "amd64", "{t}");
            let says = t["status"]["message"].as_str().unwrap_or("");
            assert!(says.contains("no image for freebsd/amd64"), "{t}");
        }
    }
    assert_eq!(
        lab.images("lone")["resolved"][&only],
        format!("{addr}/handoff/arch@{only_index}")
    );
    assert_eq!(lab.arches("multi"), before, "the first service as it was");

    step("given the index of both, the waiting service runs");
    lab.apply("lone", &placed("lone", &image, 2, None));
    lab.runs("lone", &image, 2, &amd64_manifest, Some(&pinned), &id);
    lab.delete("lone");
    lab.delete("multi");
    step("done");
}
