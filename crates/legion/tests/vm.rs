//! A VM pod on FreeBSD itself (DESIGN.md §6.8): a container with `vm`,
//! its image holding the guest's disk, booted with bhyve on the pod's
//! network. The guest is the bhyve spike's (`legion-net/tests/
//! bhyve-guest.sh`): this host's kernel and `/rescue`, configured from
//! the loader's `legion.*` variables, answering on 8080.
//!
//! It drives a real daemon, so it is ignored by default and run on a
//! disposable host as root, with `vmm` loadable and `LEGION_CLUSTER_LAB`
//! naming a directory for the lab of `tests/common` (this host alone).

mod common;

use std::io::Write;

use common::{Lab, text, wait};
use sha2::{Digest, Sha256};

fn digest(b: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(b))
}

/// An OCI archive of one image, `name`, whose one layer holds `disk` as
/// `/disk.img`.
fn archive(disk: &[u8], name: &str, out: &std::path::Path) {
    let mut layer = tar::Builder::new(Vec::new());
    let mut h = tar::Header::new_ustar();
    h.set_mode(0o644);
    h.set_size(disk.len() as u64);
    layer.append_data(&mut h, "disk.img", disk).unwrap();
    let layer = layer.into_inner().unwrap();
    let config = serde_json::to_vec(&serde_json::json!({
        "architecture": "amd64", "os": "freebsd",
        "config": {},
        "rootfs": { "type": "layers", "diff_ids": [digest(&layer)] },
    }))
    .unwrap();
    let manifest = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": digest(&config), "size": config.len() },
        "layers": [{ "mediaType": "application/vnd.oci.image.layer.v1.tar",
                     "digest": digest(&layer), "size": layer.len() }],
    }))
    .unwrap();
    let index = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "manifests": [{ "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "digest": digest(&manifest), "size": manifest.len(),
                        "annotations": { "org.opencontainers.image.ref.name": name } }],
    }))
    .unwrap();
    let mut a = tar::Builder::new(std::fs::File::create(out).unwrap());
    let mut add = |path: &str, data: &[u8]| {
        let mut h = tar::Header::new_ustar();
        h.set_mode(0o644);
        h.set_size(data.len() as u64);
        a.append_data(&mut h, path, data).unwrap();
    };
    add("oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#);
    add("index.json", &index);
    for blob in [&layer, &config, &manifest] {
        let d = digest(blob);
        add(&format!("blobs/sha256/{}", &d["sha256:".len()..]), blob);
    }
    a.finish().unwrap();
}

const IMAGE: &str = "localhost/vm/guest:1";

const APP: &str = r#"
apiVersion: legion/v1
kind: Service
metadata: { name: guest, namespace: default }
spec:
  replicas: 1
  pod:
    containers:
      - name: guest
        image: localhost/vm/guest:1
        pullPolicy: never
        vm: { cpus: 1, memory: 256Mi }
        probes:
          readiness: { tcp: { port: 8080 }, period: 2s }
"#;

#[test]
#[ignore = "needs root, ZFS, bhyve (vmm) on FreeBSD; see the module docs"]
fn a_vm_pod_boots_and_serves() {
    let mut lab = Lab::local();
    let step = |what: &str| eprintln!("--- {what}");

    step("the guest's disk, as an image");
    let disk = lab.dir.join("guest.img");
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../legion-net/tests/bhyve-guest.sh"
    );
    let out = std::process::Command::new("/bin/sh")
        .args([script, disk.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", text(&out.stderr));
    let tar = lab.dir.join("guest.tar");
    archive(&std::fs::read(&disk).unwrap(), IMAGE, &tar);

    step("a manager on this host, the image loaded");
    lab.start_host(&[]);
    lab.ok(&["init"]);
    lab.load_image(tar.to_str().unwrap());
    let file = lab.dir.join("guest.yaml");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(APP.as_bytes()).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);

    step("the guest boots with the pod's address, and answers there");
    let task = || {
        lab.list("tasks", "default")
            .into_iter()
            .find(|t| t["status"]["state"] == "running" && t["status"]["ready"] == true)
    };
    let started = std::time::Instant::now();
    while task().is_none() {
        if started.elapsed().as_secs() > 300 {
            // What a failure leaves to read: the tasks, the VM, its
            // console and the daemon's log.
            eprintln!(
                "=== tasks\n{:#}",
                serde_json::Value::from(lab.list("tasks", "default"))
            );
            let sh = |c: &str| {
                let out = std::process::Command::new("sh")
                    .args(["-c", c])
                    .output()
                    .unwrap();
                eprintln!("=== {c}\n{}{}", text(&out.stdout), text(&out.stderr));
            };
            sh("ls /dev/vmm; ps axww | grep -E '[b]hyve|[t]ap'; ifconfig -l");
            for t in lab.list("tasks", "default") {
                if let Some(n) = t["metadata"]["name"].as_str()
                    && let Some(out) = lab.legion_within(20, &["logs", n, "-n", "default"])
                {
                    eprintln!("=== logs {n}\n{}{}", text(&out.stdout), text(&out.stderr));
                }
            }
            let log = std::fs::read_to_string(lab.dir.join("daemon.log")).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(80).collect();
            eprintln!(
                "=== daemon.log\n{}",
                tail.into_iter().rev().collect::<Vec<_>>().join("\n")
            );
            panic!("timed out waiting for the VM task running and ready");
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    eprintln!("ready in {:.1} s", started.elapsed().as_secs_f64());
    let t = task().unwrap();
    let ip = t["status"]["ip"].as_str().unwrap().to_owned();
    let name = t["metadata"]["name"].as_str().unwrap().to_owned();
    // The guest answers one connection at a time, its readiness probe's
    // among them: a call between two may find no listener.
    let mut answer = String::new();
    for _ in 0..10 {
        let out = std::process::Command::new("nc")
            .args(["-N", "-w", "5", &ip, "8080"])
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        answer = text(&out.stdout).trim().to_owned();
        if !answer.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    assert_eq!(answer, "from-the-guest");
    let logs = lab.ok(&["logs", &name, "-n", "default"]);
    assert!(
        logs.contains(&format!("legion-guest: up {ip}")),
        "its console, as its log: {logs}"
    );

    step("removed, the VM is destroyed");
    lab.ok(&["delete", "service", "guest", "-n", "default"]);
    wait("no VM left", 120, || {
        let out = std::process::Command::new("sh")
            .args(["-c", "ls /dev/vmm 2>/dev/null | grep -c '^legion-' || true"])
            .output()
            .unwrap();
        text(&out.stdout).trim() == "0"
    });
}
