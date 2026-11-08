//! The image handoff (jailer's ROADMAP, P1.4; PLAN.md phase 3): an image
//! jailer builds and records is the image Legion runs, by both routes.
//!
//! - From a registry: jailer pushes to the registry this test serves (see
//!   [`registry`]), and every task runs the digest jailer recorded, through
//!   a scale, a redeployment after the tag moved, and a rollback.
//! - Offline: jailer exports an archive, every node loads it with
//!   `legion image load`, and every task runs the manifest jailer recorded.
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

use common::{Lab, Role, text, wait};
use serde::Deserialize;
use serde_json::Value;

/// What this test reads of a jailer release record, schema 1 (jailer's
/// docs/RELEASE-RECORD.md).
#[derive(Deserialize)]
struct Record {
    schema: u32,
    image: Built,
    #[serde(default)]
    published: Vec<Published>,
    #[serde(default)]
    archives: Vec<Archive>,
}

#[derive(Deserialize)]
struct Built {
    manifest: String,
}

#[derive(Deserialize)]
struct Published {
    reference: String,
    digest: String,
    pinned: String,
}

#[derive(Deserialize)]
struct Archive {
    manifest: String,
}

fn record(path: &Path) -> Record {
    let r: Record = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(r.schema, 1, "a record this test knows");
    r
}

/// The jailer in `LEGION_JAILER`, with a state directory of its own in the
/// lab directory, on plain directories.
struct Jailer {
    bin: String,
    state: PathBuf,
    /// Registries it talks to over plain HTTP.
    plain: Vec<String>,
}

impl Jailer {
    fn new(lab: &Lab, plain: &[&str]) -> Jailer {
        let bin = std::env::var("LEGION_JAILER").expect("set LEGION_JAILER to a jailer binary");
        assert!(Path::new(&bin).is_file(), "no jailer at {bin}");
        Jailer {
            bin,
            state: lab.dir.join("jailer"),
            plain: plain.iter().map(|p| p.to_string()).collect(),
        }
    }

    fn run(&self, args: &[&str]) -> String {
        let mut c = Command::new(&self.bin);
        c.arg("--state-dir").arg(&self.state).arg("--no-zfs");
        for p in &self.plain {
            c.args(["--plain-http", p]);
        }
        let out = c.args(args).stdin(Stdio::null()).output().unwrap();
        assert!(
            out.status.success(),
            "jailer {args:?}: {}{}",
            text(&out.stdout),
            text(&out.stderr)
        );
        text(&out.stdout)
    }

    /// Builds `tag`, whose `/id` holds `id`, into the record `rec`.
    fn build(&self, dir: &Path, tag: &str, id: &str, rec: &Path) {
        let ctx = dir.join(id);
        std::fs::create_dir_all(&ctx).unwrap();
        std::fs::write(
            ctx.join("Containerfile"),
            "FROM docker.io/freebsd/freebsd-runtime:15.1\nCOPY id /id\n",
        )
        .unwrap();
        std::fs::write(ctx.join("id"), id).unwrap();
        self.run(&[
            "build",
            "-t",
            tag,
            "--record",
            rec.to_str().unwrap(),
            ctx.to_str().unwrap(),
        ]);
    }
}

impl Drop for Jailer {
    /// Its store holds base layers with `schg` files.
    fn drop(&mut self) {
        let d = self.state.display();
        let _ = Command::new("/bin/sh")
            .args(["-c", &format!("chflags -R 0 {d} 2>/dev/null; rm -rf {d}")])
            .status();
    }
}

/// An id no other build of this test has.
fn unique(what: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    format!("{what}-{}-{}", std::process::id(), t.as_nanos())
}

/// Two managers and a worker, as the other cluster tests have them.
fn three_nodes(lab: &mut Lab, extra: &[&str]) {
    lab.start_host(extra);
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
}

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
    three_nodes(&mut lab, &[]);
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

    // Builds and pushes a new image to `image`, as recorded in `name`.json.
    let dir = lab.dir.clone();
    let push = |name: &str| {
        let rec = dir.join(format!("{name}.json"));
        let id = unique(name);
        let tag = format!("handoff/{name}:1");
        jailer.build(&dir, &tag, &id, &rec);
        jailer.run(&["push", "--record", rec.to_str().unwrap(), &tag, &image]);
        let r = record(&rec);
        let p = r.published.last().expect("the push is recorded");
        assert_eq!(p.reference, image);
        assert_eq!(p.digest, r.image.manifest, "one platform: its manifest");
        assert_eq!(p.pinned, format!("{addr}/handoff/app@{}", p.digest));
        (p.pinned.clone(), r.image.manifest.clone(), id)
    };

    step("jailer builds an image and pushes it");
    let (pinned1, manifest1, id1) = push("first");

    step("the cluster resolves the tag to what jailer pushed");
    lab.flags = format!("{} --insecure-registry {addr}", lab.flags);
    three_nodes(&mut lab, &["--insecure-registry", &addr]);
    lab.apply("namespace", NAMESPACE);
    lab.apply("web", &service("web", &image, 2, ""));
    lab.runs("web", &image, 2, &manifest1, Some(&pinned1), &id1);

    step("jailer pushes another build to the same tag");
    let (pinned2, manifest2, id2) = push("second");
    assert_ne!(manifest2, manifest1);

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

/// The registry [`a_jailer_push_is_what_legion_runs`] runs, as this same
/// program, on the host's lab address.
struct Registry {
    addr: String,
    child: Child,
}

impl Registry {
    fn start(lab: &Lab) -> Registry {
        let file = lab.dir.join("registry.addr");
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
        };
        wait("the registry", 30, || file.exists());
        r.addr = std::fs::read_to_string(&file).unwrap();
        r
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
/// the file `LEGION_HANDOFF_REGISTRY`.
#[test]
#[ignore = "the registry of a_jailer_push_is_what_legion_runs, run by it"]
fn registry() {
    let Ok(file) = std::env::var("LEGION_HANDOFF_REGISTRY") else {
        return;
    };
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
