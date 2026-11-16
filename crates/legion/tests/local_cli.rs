//! `legion local` as a command, on plain directories: what needs the
//! binary, not only legion-build's library (whose tests are its own).

#[cfg(unix)]
use std::path::Path;

#[path = "../../legion-build/tests/common/mod.rs"]
mod common;
use common::start_mock_registry;

/// The legion under test.
const LEGION: &str = env!("CARGO_BIN_EXE_legion");

/// `legion local`, to be given its arguments.
fn local() -> std::process::Command {
    let mut c = std::process::Command::new(LEGION);
    c.arg("local");
    c
}

/// `legion local check` checks an image's files: one whose files cannot be put
/// together fails, not passes on its config alone.
#[cfg(unix)]
#[test]
fn check_fails_on_an_image_whose_files_cannot_be_prepared() {
    let tmp = tempfile::tempdir().unwrap();
    let state = tmp.path().join("state");
    let ctx = tmp.path().join("ctx");
    std::fs::create_dir_all(&ctx).unwrap();
    std::fs::write(ctx.join("f"), "f\n").unwrap();
    std::fs::write(
        ctx.join("Containerfile"),
        "FROM scratch\nCOPY f /f\nCMD [\"/missing\"]\n",
    )
    .unwrap();
    let legion = |args: &[&str]| {
        local()
            .args(["--state-dir", state.to_str().unwrap(), "--no-zfs"])
            .args(args)
            .output()
            .unwrap()
    };
    let built = legion(&["build", "-t", "localhost/chk:1", ctx.to_str().unwrap()]);
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let check = || legion(&["check", "localhost/chk:1"]);
    let first = check();
    assert!(!first.status.success());
    assert!(
        String::from_utf8_lossy(&first.stderr).contains("LEGION009"),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );

    // Its layers gone, the store cannot put its files together.
    let mut stack = vec![state.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.file_name().is_some_and(|n| n == "layers") {
                for l in std::fs::read_dir(&p).unwrap() {
                    legion_build::layer::remove_tree(&l.unwrap().path()).unwrap();
                }
            } else if p.is_dir() && !p.is_symlink() {
                stack.push(p);
            }
        }
    }
    let second = check();
    assert!(
        !second.status.success(),
        "passed without its files:\n{}",
        String::from_utf8_lossy(&second.stdout)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("preparing the files"),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_release_record_follows_one_image_and_refuses_another() {
    let tmp = tempfile::tempdir().unwrap();
    let (addr, _registry) = start_mock_registry().await;
    let host = format!("127.0.0.1:{}", addr.port());
    let ctx = tmp.path().join("ctx");
    std::fs::create_dir_all(&ctx).unwrap();
    std::fs::write(ctx.join("f"), "one\n").unwrap();
    std::fs::write(
        ctx.join("Containerfile"),
        "FROM scratch\nARG TOKEN\nCOPY f /f\n",
    )
    .unwrap();
    let record = tmp.path().join("release.json");
    let rec = record.to_str().unwrap().to_owned();
    let legion = |args: &[&str]| {
        let host = host.clone();
        let args: Vec<String> = args.iter().map(|a| (*a).to_owned()).collect();
        let state = tmp.path().join("state");
        async move {
            tokio::task::spawn_blocking(move || {
                local()
                    .args(["--no-zfs", "--plain-http", &host, "--state-dir"])
                    .arg(state)
                    .args(args)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap()
        }
    };
    let ok = |out: std::process::Output| {
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    };
    let read = || -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(&record).unwrap()).unwrap()
    };
    let prefix = tmp.path().join("key");
    ok(legion(&["key", "generate", prefix.to_str().unwrap()]).await);
    let (private, public) = (
        prefix.with_extension("key").to_str().unwrap().to_owned(),
        prefix.with_extension("pub").to_str().unwrap().to_owned(),
    );
    let context = ctx.to_str().unwrap().to_owned();
    let target = format!("{host}/app:1");

    ok(legion(&[
        "build",
        "-t",
        "app:1",
        "--record",
        &rec,
        "--build-arg",
        "TOKEN=hunter2",
        &context,
    ])
    .await);
    assert!(
        !std::fs::read_to_string(&record)
            .unwrap()
            .contains("hunter2"),
        "a build argument's value stays out of the record"
    );
    ok(legion(&[
        "push",
        "--record",
        &rec,
        "--sign-key",
        &private,
        "app:1",
        &target,
    ])
    .await);
    ok(legion(&["verify", "--record", &rec, "--key", &public, &target]).await);
    let archive = tmp.path().join("app.tar");
    let archive = archive.to_str().unwrap();
    ok(legion(&["export", "--record", &rec, "app:1", archive]).await);
    let index = format!("{host}/app:index");
    ok(legion(&["manifest", "push", "--record", &rec, &index, "app:1"]).await);

    let r = read();
    let manifest = r["image"]["manifest"].as_str().unwrap().to_owned();
    assert_ne!(manifest, r["image"]["config"].as_str().unwrap());
    assert!(
        r["source"]["context"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    let published = &r["published"][0];
    assert_eq!(published["digest"], manifest.as_str());
    assert_eq!(published["pinned"], format!("{host}/app@{manifest}"));
    assert!(published["signature"].as_str().unwrap().contains(".sig"));
    assert_eq!(r["verified"][0]["digest"], manifest.as_str());
    assert_eq!(r["archives"][0]["manifest"], manifest.as_str());
    // The index has a digest of its own, and maps its platform to the image.
    let by_index = &r["published"][1];
    assert_ne!(by_index["digest"], manifest.as_str());
    let platforms = by_index["platforms"].as_object().unwrap();
    assert_eq!(platforms.len(), 1);
    assert_eq!(platforms.values().next().unwrap(), manifest.as_str());

    // The archive names the same manifest.
    let mut tar = tar::Archive::new(std::fs::File::open(archive).unwrap());
    let mut in_archive = None;
    for entry in tar.entries().unwrap() {
        let entry = entry.unwrap();
        if entry.path().unwrap().to_str() == Some("index.json") {
            let index: serde_json::Value = serde_json::from_reader(entry).unwrap();
            in_archive = index["manifests"][0]["digest"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(in_archive.as_deref(), Some(manifest.as_str()));

    // The tag moves to another image, signed by the same key.
    std::fs::write(ctx.join("f"), "two\n").unwrap();
    ok(legion(&["build", "-t", "app:2", &context]).await);
    let before = std::fs::read(&record).unwrap();
    let refused = legion(&["push", "--record", &rec, "app:2", &target]).await;
    assert!(!refused.status.success(), "another image, into this record");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("the record is of image"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    // Refused before anything was sent: the tag is where it was.
    ok(legion(&["verify", "--record", &rec, "--key", &public, &target]).await);
    ok(legion(&["push", "--sign-key", &private, "app:2", &target]).await);
    let moved = legion(&["verify", "--record", &rec, "--key", &public, &target]).await;
    assert!(!moved.status.success(), "a moved tag is not the record's");
    assert!(
        String::from_utf8_lossy(&moved.stderr).contains("has moved"),
        "{}",
        String::from_utf8_lossy(&moved.stderr)
    );
    assert_eq!(std::fs::read(&record).unwrap(), before);

    // What was recorded can still be had, whatever the tag says now.
    let pinned = r["published"][0]["pinned"].as_str().unwrap();
    ok(legion(&["verify", "--record", &rec, "--key", &public, pinned]).await);
    let pulled = ok(legion(&["pull", pinned]).await);
    let config = r["image"]["config"].as_str().unwrap();
    assert!(
        String::from_utf8_lossy(&pulled.stdout).contains(config),
        "{}",
        String::from_utf8_lossy(&pulled.stdout)
    );

    // Provenance and an SBOM, signed, about the recorded image alone.
    ok(legion(&[
        "attest",
        "--key",
        &private,
        "--type",
        "provenance",
        "--record",
        &rec,
        pinned,
    ])
    .await);
    ok(legion(&[
        "attest", "--key", &private, "--type", "sbom", "--image", "app:1", "--record", &rec, pinned,
    ])
    .await);
    let shown = ok(legion(&[
        "verify",
        "--key",
        &public,
        "--type",
        "provenance",
        "--record",
        &rec,
        pinned,
    ])
    .await);
    let provenance: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(
        provenance["buildDefinition"]["externalParameters"]["context"],
        r["source"]["context"]
    );
    let shown = ok(legion(&["verify", "--key", &public, "--type", "sbom", pinned]).await);
    let sbom: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(sbom["spdxVersion"], "SPDX-2.3");
    let comment = sbom["creationInfo"]["comment"].as_str().unwrap();
    assert!(comment.contains("NOT AN INVENTORY"), "{comment}");
    assert_eq!(read()["attestations"].as_array().unwrap().len(), 2);
    // The tag is another image's by now: nothing here is about it.
    let none = legion(&["verify", "--key", &public, "--type", "provenance", &target]).await;
    assert!(!none.status.success());
    let other = legion(&[
        "attest",
        "--key",
        &private,
        "--type",
        "provenance",
        "--record",
        &rec,
        &target,
    ])
    .await;
    assert!(!other.status.success(), "the record is not of this image");

    // A build names the images it took anything from, as they resolved.
    std::fs::write(
        ctx.join("Containerfile"),
        "FROM app:1 AS base\nFROM scratch\nCOPY --from=base /f /g\nCOPY --from=app:2 /f /h\n",
    )
    .unwrap();
    let derived = tmp.path().join("derived.json");
    let derived_path = derived.to_str().unwrap();
    ok(legion(&[
        "build",
        "-t",
        "derived:1",
        "--record",
        derived_path,
        &context,
    ])
    .await);
    let d: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&derived).unwrap()).unwrap();
    let bases = d["bases"].as_array().unwrap();
    assert_eq!(bases.len(), 2, "{bases:?}");
    assert!(bases.iter().any(|b| b["manifest"] == manifest.as_str()));
    assert!(
        bases
            .iter()
            .all(|b| b["config"].as_str().unwrap().starts_with("sha256:"))
    );
}

#[test]
fn check_can_answer_in_json() {
    let tmp = tempfile::tempdir().unwrap();
    let cf = tmp.path().join("Containerfile");
    std::fs::write(
        &cf,
        "FROM scratch\nHEALTHCHECK CMD /bin/true\nSTOPSIGNAL NOPE\n",
    )
    .unwrap();
    let out = local()
        .args(["--no-zfs", "--state-dir"])
        .arg(tmp.path().join("state"))
        .args(["check", "--json"])
        .arg(&cf)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(result["passed"], false);
    let codes: Vec<(&str, &str)> = result["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["code"].as_str().unwrap(), f["severity"].as_str().unwrap()))
        .collect();
    assert!(codes.contains(&("LEGION001", "warning")), "{codes:?}");
    assert!(codes.contains(&("LEGION003", "error")), "{codes:?}");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn publish_pushes_the_index_only_when_every_host_succeeds() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let (addr, registry) = start_mock_registry().await;
    let host = format!("127.0.0.1:{}", addr.port());
    let ctx = tmp.path().join("ctx");
    std::fs::create_dir_all(&ctx).unwrap();
    std::fs::write(ctx.join("f"), "one\n").unwrap();
    std::fs::write(ctx.join("x.log"), "left out\n").unwrap();
    std::fs::write(ctx.join(".containerignore"), "*.log\n").unwrap();
    std::fs::write(
        ctx.join("Containerfile"),
        "FROM scratch\nARG V\nLABEL v=$V\nCOPY f /f\n",
    )
    .unwrap();

    // Stands for ssh: the destination names a state directory here, and one
    // called `broken` fails. Each runs legion with no ssh and no second host.
    let bin = Path::new(LEGION);
    let ssh = tmp.path().join("ssh");
    std::fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nshift 2\ndest=$1\nshift\n\
             [ \"$dest\" != broken ] || {{ echo refused >&2; exit 7; }}\n\
             mkdir -p {bindir}\nln -sf {bin} {bindir}/legion\n\
             PATH={bindir}:$PATH LEGION_LOCAL_STATE_DIR={state}/$dest exec sh -c \"$*\"\n",
            bindir = tmp.path().join("bin").display(),
            bin = bin.display(),
            state = tmp.path().display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let wrapper = tmp.path().join("remote-legion");
    std::fs::write(
        &wrapper,
        // Given `local` and a command, as the hosts run it.
        format!("#!/bin/sh\nshift\nexec legion local --no-zfs --plain-http {host} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let platform = format!("freebsd/{}", legion_build::oci::arch());
    let target = format!("{host}/app:1");
    let record = tmp.path().join("release.json");
    let publish = |hosts: Vec<String>| {
        let mut c = local();
        c.args(["--no-zfs", "--plain-http", &host, "--state-dir"])
            .arg(tmp.path().join("local"))
            .arg("publish")
            .arg("--ssh")
            .arg(&ssh)
            .arg("--remote-legion")
            .arg(&wrapper)
            .arg("--record")
            .arg(&record)
            .args(["--build-arg", "V=a b"]);
        for h in hosts {
            c.args(["--host", &h]);
        }
        c.arg(&target).arg(&ctx);
        tokio::task::spawn_blocking(move || c.output().unwrap())
    };
    let tagged = |tag: &str| registry.manifests.lock().unwrap().contains_key(tag);

    // A host fails, the other does not: the target's tag is not touched.
    let out = publish(vec![
        format!("{platform}=one"),
        "freebsd/riscv64=broken".to_owned(),
    ])
    .await
    .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("nothing published"), "{err}");
    assert!(err.contains("freebsd/riscv64"), "{err}");
    assert!(!tagged("1"), "a partial release under the final tag");
    assert!(!record.exists());

    // A host of another platform than it was named for.
    let out = publish(vec!["freebsd/riscv64=one".to_owned()])
        .await
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("the host built"), "{err}");
    assert!(!tagged("1"));

    // All well: the index is there, and the record says what is in it.
    let out = publish(vec![format!("{platform}=one")]).await.unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(tagged("1"));
    let r: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&record).unwrap()).unwrap();
    let manifest = r["builds"][&platform]["image"]["manifest"]
        .as_str()
        .unwrap();
    let published = &r["published"][0];
    assert_eq!(published["platforms"][&platform], manifest);
    assert_ne!(published["digest"], manifest);
    assert!(
        r["source"]["context"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(!std::fs::read_to_string(&record).unwrap().contains("a b"));
}
