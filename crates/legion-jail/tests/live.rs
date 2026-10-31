//! The jail runtime on FreeBSD itself. Ignored by default: needs root,
//! the network (it pulls `freebsd-runtime` from Docker Hub) and
//! `LEGION_JAIL_ZFS_ROOT` naming a dataset it may create children under (a
//! throwaway pool). It only touches the jails and datasets it creates.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use legion_image::{Client, Store};
use legion_jail::{Config, JailRuntime};
use legion_runtime::{ContainerSpec, Mount, Output, Runtime};
use legion_sys::{Shell, Sys};

const IMAGE: &str = "docker.io/freebsd/freebsd-runtime:15.1";

fn runtime(sys: &Arc<Shell>, images: &Arc<Store>, dir: &Path, base: &str) -> JailRuntime {
    JailRuntime::new(
        sys.clone() as Arc<dyn Sys>,
        images.clone(),
        Config {
            state_dir: dir.join("jails"),
            dataset: Some(format!("{base}/jails")),
            shim: vec![env!("CARGO_BIN_EXE_legion-jail-shim").into()],
            prune_images: false,
        },
    )
    .unwrap()
}

fn labels(task: &str, role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("legion.node.id".to_owned(), "n1".to_owned()),
        ("legion.task.id".to_owned(), task.to_owned()),
        ("legion.role".to_owned(), role.to_owned()),
    ])
}

fn member(name: &str, pod: &str, script: &str) -> ContainerSpec {
    ContainerSpec {
        name: name.to_owned(),
        image: IMAGE.to_owned(),
        entrypoint: vec!["/bin/sh".into(), "-c".into(), script.into()],
        hostname: Some("web-0".into()),
        labels: labels("t1", "app"),
        annotations: BTreeMap::from([
            ("org.freebsd.parentJail".to_owned(), pod.to_owned()),
            (
                "legion.jail".to_owned(),
                "allow.raw_sockets=false allow.chflags=false enforce_statfs=2 securelevel=2"
                    .to_owned(),
            ),
        ]),
        ..Default::default()
    }
}

async fn output(rt: &JailRuntime, id: &str) -> String {
    let chunks: Vec<_> = rt.logs(id, false, None).await.unwrap().collect().await;
    chunks
        .into_iter()
        .map(|c| {
            let c = c.unwrap();
            let tag = if c.stream == Output::Stderr { "!" } else { "" };
            format!("{tag}{}", String::from_utf8_lossy(&c.data))
        })
        .collect()
}

async fn until(what: &str, mut f: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !f().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root, LEGION_JAIL_ZFS_ROOT and the network"]
async fn pods_of_jails() {
    let Ok(root) = std::env::var("LEGION_JAIL_ZFS_ROOT") else {
        eprintln!("LEGION_JAIL_ZFS_ROOT not set; skipping");
        return;
    };
    let base = format!("{root}/ljail{}", std::process::id());
    // Kept until the end: a test that fails leaves jails and mounts under
    // it, which must not be deleted from under them.
    let dir = tempfile::Builder::new()
        .prefix("ljail")
        .tempdir_in("/var/tmp")
        .unwrap()
        .keep();
    let sys = Arc::new(Shell::new());
    let images = Arc::new(
        Store::open(
            sys.clone() as Arc<dyn Sys>,
            &format!("{base}/images"),
            &dir.as_path().join("images"),
            Client::new().unwrap(),
        )
        .unwrap(),
    );
    let rt = runtime(&sys, &images, dir.as_path(), &base);
    assert!(!rt.image_usable(IMAGE, "shop").await.unwrap());
    rt.pull(IMAGE, "shop", &[]).await.unwrap();
    assert!(rt.image_usable(IMAGE, "shop").await.unwrap());

    // The pod: a VNET jail running nothing.
    let pod = rt
        .create(&ContainerSpec {
            name: "shop_web".into(),
            image: IMAGE.into(),
            hostname: Some("web-0".into()),
            labels: labels("t1", "infra"),
            annotations: BTreeMap::from([("org.freebsd.jail.vnet".to_owned(), "new".to_owned())]),
            ..Default::default()
        })
        .await
        .unwrap();
    rt.start(&pod).await.unwrap();
    let jail = sys
        .jails()
        .unwrap()
        .into_iter()
        .find(|j| j.name == pod)
        .unwrap();
    assert_eq!(jail.vnet, "new");
    sys.jail_set(&pod, &[("children.max", "4")]).unwrap();
    assert!(rt.inspect(&pod).await.unwrap().state.running);

    let mut events = rt.events(&[("legion.task.id", "t1")]).await.unwrap();

    // A member with a file mounted in, its output kept by stream.
    let secret = dir.as_path().join("secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let mut spec = member(
        "shop_web_app",
        &pod,
        "echo out; echo err >&2; cat /run/secrets/token; echo FOO=$FOO; exec sleep 60",
    );
    spec.env.insert("FOO".into(), "bar".into());
    spec.mounts.push(Mount::nullfs(
        &secret.to_string_lossy(),
        "/run/secrets/token",
        true,
    ));
    spec.stop_timeout = Some(5);
    let app = rt.create(&spec).await.unwrap();
    let started = Instant::now();
    rt.start(&app).await.unwrap();
    eprintln!("member started in {:?}", started.elapsed());
    assert!(rt.inspect(&app).await.unwrap().state.running);
    until("the app's output", async || {
        output(&rt, &app).await.contains("FOO=bar")
    })
    .await;
    assert_eq!(output(&rt, &app).await, "out\n!err\ns3cret\nFOO=bar\n");
    let child = format!("{pod}.{app}");
    let params = sys
        .jail_get(
            &child,
            &["allow.raw_sockets", "securelevel", "enforce_statfs", "vnet"],
        )
        .unwrap();
    assert_eq!(params["securelevel"], "2", "{params:?}");
    assert_eq!(params["vnet"], "inherit", "{params:?}");

    // Exec, as the container: its environment, hostname, network.
    let out = rt
        .exec(
            &app,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "echo $FOO; id -u; hostname; ifconfig -l".into(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(out.exit_code, 0, "{out:?}");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "bar\n0\nweb-0\nlo0\n");
    let out = rt.exec(&app, &["/nonexistent".into()]).await.unwrap();
    assert_ne!(out.exit_code, 0);
    // With a terminal.
    let mut attached = rt
        .exec_attach(
            &app,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "test -t 0 && test -t 1 && echo isatty; echo hi".into(),
            ],
            false,
            true,
        )
        .await
        .unwrap();
    rt.exec_resize(&attached.id, 100, 40).await.unwrap();
    let mut seen = Vec::new();
    while let Some(c) = attached.output.next().await {
        seen.extend_from_slice(&c.unwrap().data);
    }
    let seen = String::from_utf8_lossy(&seen);
    assert!(seen.contains("isatty") && seen.contains("hi"), "{seen}");
    assert_eq!(rt.exec_exit_code(&attached.id).await.unwrap(), 0);
    let running = |what: &'static str| {
        let child = child.clone();
        move || {
            let out = std::process::Command::new("ps")
                .args(["-J", &child, "-o", "command="])
                .output()
                .unwrap();
            // The command itself, not a shell whose script names it: that
            // runs before the script has done anything.
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .any(|l| l.trim() == what)
        }
    };
    // An exec given up on (a probe timing out) ends, with what it started.
    let slow_cmd: [String; 3] = [
        "/bin/sh".into(),
        "-c".into(),
        "sleep 300 & exec sleep 301".into(),
    ];
    let slow = rt.exec(&app, &slow_cmd);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), slow)
            .await
            .is_err()
    );
    let (bg, fg) = (running("sleep 300"), running("sleep 301"));
    until("the abandoned exec ends", async || !bg() && !fg()).await;
    // A terminal session let go of is hung up, even while a read of its
    // output waits on a command that says nothing more.
    let attached = rt
        .exec_attach(
            &app,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "echo up; exec sleep 302".into(),
            ],
            true,
            true,
        )
        .await
        .unwrap();
    let session = attached.id.clone();
    let mut stream = attached.output;
    let reading = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(Ok(c)) = stream.next().await {
            seen.extend_from_slice(&c.data);
        }
        seen
    });
    let held = running("sleep 302");
    until("the silent command", async || held()).await;
    // Give the output's next read time to start waiting.
    tokio::time::sleep(Duration::from_millis(300)).await;
    reading.abort();
    drop(attached.stdin);
    rt.exec_release(&session).await;
    let held = running("sleep 302");
    until("the released terminal hangs up", async || !held()).await;
    // So does a command without a terminal, and one ignoring the hangup is
    // killed after a grace.
    // And when the command ends on the hangup but leaves behind a child
    // ignoring it, the child is killed all the same.
    for script in [
        "exec sleep 303",
        "trap '' HUP; sleep 304; sleep 304",
        "(trap '' HUP; exec sleep 305) & wait",
    ] {
        let attached = rt
            .exec_attach(
                &app,
                &["/bin/sh".into(), "-c".into(), script.into()],
                true,
                false,
            )
            .await
            .unwrap();
        let what = ["sleep 303", "sleep 304", "sleep 305"]
            .into_iter()
            .find(|w| script.contains(w))
            .unwrap();
        let alive = running(what);
        until("the command", async || alive()).await;
        let session = attached.id.clone();
        drop(attached);
        rt.exec_release(&session).await;
        until("the released command ends", async || !alive()).await;
    }

    // Another member, as another user, that ends.
    let mut spec = member("shop_web_job", &pod, "id -un; exit 3");
    spec.user = Some("nobody".into());
    let job = rt.create(&spec).await.unwrap();
    rt.start(&job).await.unwrap();
    assert_eq!(rt.wait(&job).await.unwrap(), 3);
    assert_eq!(output(&rt, &job).await, "nobody\n");
    let died = tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        (died.action.as_str(), died.actor.id.as_str()),
        ("died", job.as_str())
    );
    assert_eq!(died.exit_code(), Some(3));
    // Its jail went with its process.
    assert!(!sys.jails().unwrap().iter().any(|j| j.name.ends_with(&job)));
    // A command that is not there fails the start.
    let bad = rt
        .create(&member("shop_web_bad", &pod, "true"))
        .await
        .unwrap();
    let mut bad_spec = member("shop_web_bad2", &pod, "true");
    bad_spec.entrypoint = vec!["/no/such".into()];
    let bad2 = rt.create(&bad_spec).await.unwrap();
    let e = rt.start(&bad2).await.unwrap_err();
    assert!(e.to_string().contains("/no/such"), "{e}");

    // A new runtime (an agent restart) finds them.
    drop(rt);
    let rt = runtime(&sys, &images, dir.as_path(), &base);
    let found = rt.list(&[("legion.task.id", "t1")]).await.unwrap();
    assert_eq!(found.len(), 5);
    let state = |id: &str| found.iter().find(|c| c.id == id).unwrap().state.clone();
    assert_eq!(state(&pod), "running");
    assert_eq!(state(&app), "running");
    assert_eq!(state(&job), "exited");
    assert_eq!(state(&bad), "created");

    // Stopping: the stop signal ends the shell.
    let t = Instant::now();
    rt.stop(&app, Duration::from_secs(5)).await.unwrap();
    assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
    let i = rt.inspect(&app).await.unwrap();
    assert!(!i.state.running);
    assert_eq!(i.state.exit_code, 143);
    // Started again, it runs again.
    rt.start(&app).await.unwrap();
    assert!(rt.inspect(&app).await.unwrap().state.running);
    // One that ignores it is killed after the grace period.
    let stubborn = rt
        .create(&member(
            "shop_web_stubborn",
            &pod,
            "trap '' TERM; echo ready; while :; do sleep 1; done",
        ))
        .await
        .unwrap();
    rt.start(&stubborn).await.unwrap();
    until("the trap", async || {
        output(&rt, &stubborn).await.contains("ready")
    })
    .await;
    let t = Instant::now();
    rt.stop(&stubborn, Duration::from_secs(1)).await.unwrap();
    assert!(t.elapsed() >= Duration::from_secs(1));
    assert_eq!(rt.inspect(&stubborn).await.unwrap().state.exit_code, 137);

    // Removal leaves nothing behind.
    for id in [&app, &job, &bad, &bad2, &stubborn, &pod] {
        rt.remove(id).await.unwrap();
    }
    assert!(rt.list(&[]).await.unwrap().is_empty());
    assert!(
        !sys.jails()
            .unwrap()
            .iter()
            .any(|j| j.name.starts_with(&pod))
    );
    assert!(
        sys.mounts(&dir.as_path().to_string_lossy())
            .unwrap()
            .is_empty()
    );
    assert_eq!(sys.zfs_list(&format!("{base}/jails")).unwrap().len(), 1);
    images.remove(IMAGE).await.unwrap();
    sys.zfs_destroy(&base, true).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Linux images (musl and glibc), and the `ID` their os-release gives.
const LINUX_IMAGES: [(&str, &str); 2] = [
    ("docker.io/library/alpine:3.20", "alpine"),
    ("docker.io/library/debian:bookworm-slim", "debian"),
];

/// Also needs the Linux ABI loaded (`kldload linux64 linprocfs linsysfs`)
/// and `LEGION_JAIL_LINUX` set.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root, LEGION_JAIL_ZFS_ROOT, LEGION_JAIL_LINUX, linux64 and the network"]
async fn linux_images() {
    let (Ok(root), Ok(_)) = (
        std::env::var("LEGION_JAIL_ZFS_ROOT"),
        std::env::var("LEGION_JAIL_LINUX"),
    ) else {
        eprintln!("LEGION_JAIL_ZFS_ROOT or LEGION_JAIL_LINUX not set; skipping");
        return;
    };
    let base = format!("{root}/llinux{}", std::process::id());
    let dir = tempfile::Builder::new()
        .prefix("llinux")
        .tempdir_in("/var/tmp")
        .unwrap()
        .keep();
    let sys = Arc::new(Shell::new());
    let open = |linux| {
        Store::open(
            sys.clone() as Arc<dyn Sys>,
            &format!("{base}/images"),
            &dir.as_path().join("images"),
            Client::new().unwrap(),
        )
        .unwrap()
        .with_linux(linux)
    };
    // Without the Linux ABI, Alpine (which has no FreeBSD image) is refused.
    let e = open(false)
        .pull(LINUX_IMAGES[0].0, None, "t")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no image for freebsd"), "{e}");
    let images = Arc::new(open(true));
    let rt = runtime(&sys, &images, dir.as_path(), &base);
    rt.pull(IMAGE, "t", &[]).await.unwrap();
    for (image, _) in LINUX_IMAGES {
        rt.pull(image, "t", &[]).await.unwrap();
    }

    // The pod is FreeBSD's, its member Alpine.
    let pod = rt
        .create(&ContainerSpec {
            name: "lx".into(),
            image: IMAGE.into(),
            labels: labels("t2", "infra"),
            annotations: BTreeMap::from([("org.freebsd.jail.vnet".to_owned(), "new".to_owned())]),
            ..Default::default()
        })
        .await
        .unwrap();
    rt.start(&pod).await.unwrap();
    sys.jail_set(&pod, &[("children.max", "4")]).unwrap();
    let mut members = Vec::new();
    for (i, (image, id)) in LINUX_IMAGES.into_iter().enumerate() {
        let mut spec = member(
            &format!("lx_{i}"),
            &pod,
            "uname -s; id -u; id -G; grep ^ID= /etc/os-release;              head -1 /proc/meminfo; ls /sys | head -1; ls -l /proc/self/fd/0;              echo shm > /dev/shm/x && cat /dev/shm/x;              touch /etc/passwd 2>&1 || echo refused",
        );
        spec.image = image.into();
        spec.labels = labels("t2", "app");
        spec.user = Some("nobody".into());
        let app = rt.create(&spec).await.unwrap();
        rt.start(&app).await.unwrap();
        assert_eq!(
            rt.wait(&app).await.unwrap(),
            0,
            "{}",
            output(&rt, &app).await
        );
        let out = output(&rt, &app).await;
        eprintln!(
            "{image}:
{out}"
        );
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "Linux");
        assert_eq!(lines[1], "65534", "nobody, from the image's /etc/passwd");
        assert_eq!(lines[3], format!("ID={id}"));
        assert!(lines[4].starts_with("MemTotal:"), "linprocfs: {out}");
        assert!(
            out.contains(
                "
shm
"
            ),
            "/dev/shm: {out}"
        );
        assert!(
            out.contains("refused"),
            "nobody cannot write /etc/passwd: {out}"
        );
        members.push(app);
    }

    for id in members.iter().chain([&pod]) {
        rt.remove(id).await.unwrap();
    }
    assert!(
        sys.mounts(&dir.as_path().to_string_lossy())
            .unwrap()
            .is_empty()
    );
    for (image, _) in LINUX_IMAGES {
        images.remove(image).await.unwrap();
    }
    images.remove(IMAGE).await.unwrap();
    sys.zfs_destroy(&base, true).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// What jailer calls: a container with no pod, run supervised and on the
/// caller's output, found by name, and a build's steps on a directory.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root, LEGION_JAIL_ZFS_ROOT and the network"]
async fn standalone_containers_and_build_steps() {
    use legion_jail::{Step, StepMount, StepNetwork};

    let Ok(root) = std::env::var("LEGION_JAIL_ZFS_ROOT") else {
        eprintln!("LEGION_JAIL_ZFS_ROOT not set; skipping");
        return;
    };
    let base = format!("{root}/lstand{}", std::process::id());
    let dir = tempfile::Builder::new()
        .prefix("lstand")
        .tempdir_in("/var/tmp")
        .unwrap()
        .keep();
    let sys = Arc::new(Shell::new());
    let images = Arc::new(
        Store::open(
            sys.clone() as Arc<dyn Sys>,
            &format!("{base}/images"),
            &dir.as_path().join("images"),
            Client::new().unwrap(),
        )
        .unwrap(),
    );
    let rt = runtime(&sys, &images, dir.as_path(), &base);
    rt.pull(IMAGE, "t", &[]).await.unwrap();
    let standalone = |name: &str, script: &str| ContainerSpec {
        name: name.to_owned(),
        image: IMAGE.to_owned(),
        entrypoint: vec!["/bin/sh".into(), "-c".into(), script.into()],
        annotations: BTreeMap::from([(
            "legion.jail".to_owned(),
            "allow.raw_sockets=false allow.chflags=false enforce_statfs=2 securelevel=2".to_owned(),
        )]),
        ..Default::default()
    };

    // Supervised: a jail of its own on the host's addresses, locked down
    // as a pod's member is.
    let app = rt
        .create(&standalone("app", "hostname; echo up; exec sleep 60"))
        .await
        .unwrap();
    assert_eq!(rt.find("app").unwrap(), app);
    assert_eq!(rt.find(&app[..12]).unwrap(), app);
    assert!(rt.find("nothing").is_err());
    rt.start(&app).await.unwrap();
    until("the app's output", async || {
        output(&rt, &app).await.contains("up")
    })
    .await;
    assert_eq!(output(&rt, &app).await, "app\nup\n");
    let params = sys
        .jail_get(
            &app,
            &[
                "ip4",
                "vnet",
                "securelevel",
                "devfs_ruleset",
                "allow.chflags",
            ],
        )
        .unwrap();
    assert_eq!(params["ip4"], "inherit", "{params:?}");
    assert_eq!(params["vnet"], "inherit", "{params:?}");
    assert_eq!(params["securelevel"], "2", "{params:?}");
    assert_eq!(params["devfs_ruleset"], "4", "{params:?}");
    assert_eq!(params["allow.chflags"], "false", "{params:?}");
    // An exec on this process's output gives its exit code.
    let code = rt
        .exec_inherit(
            &app,
            &["/bin/sh".into(), "-c".into(), "exit 5".into()],
            false,
            false,
        )
        .await
        .unwrap();
    assert_eq!(code, 5);
    rt.stop(&app, Duration::from_secs(5)).await.unwrap();
    assert!(!rt.inspect(&app).await.unwrap().state.running);

    // On this process's output: its exit code, recorded too, and its jail
    // gone after.
    let once = rt.create(&standalone("once", "exit 7")).await.unwrap();
    assert_eq!(rt.run_attached(&once, false, false).await.unwrap(), 7);
    let i = rt.inspect(&once).await.unwrap();
    assert_eq!((i.state.running, i.state.exit_code), (false, 7));
    assert!(!sys.jails().unwrap().iter().any(|j| j.name == once));
    // And again.
    assert_eq!(rt.run_attached(&once, false, false).await.unwrap(), 7);
    // Its future dropped while it runs: the same, and recorded as ended.
    let long = rt.create(&standalone("long", "sleep 60")).await.unwrap();
    let attached = rt.run_attached(&long, false, false);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), attached)
            .await
            .is_err()
    );
    assert!(!rt.inspect(&long).await.unwrap().state.running);
    assert!(!sys.jails().unwrap().iter().any(|j| j.name == long));
    let left: Vec<String> = sys.mounts(&dir.as_path().to_string_lossy()).unwrap();
    assert!(
        !left
            .iter()
            .any(|m| m.contains(&long) && m.ends_with("/dev")),
        "{left:?}"
    );

    // Removed right after its last connection closed: its jail is dying a
    // while yet (the socket's TIME_WAIT holds it), its root still busy.
    // The host's nc (and the libraries it needs beyond the image's) serves
    // one connection and closes it first, so the TIME_WAIT is the jail's.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut spec = standalone("server", &format!("echo hi | nc -N -l 127.0.0.1 {port}"));
    for f in [
        "/usr/bin/nc",
        "/lib/libipsec.so.4",
        "/lib/libsbuf.so.6",
        "/lib/libstats.so.0",
    ] {
        spec.mounts.push(Mount {
            destination: f.to_owned(),
            kind: "nullfs".to_owned(),
            source: f.to_owned(),
            options: vec!["ro".to_owned()],
        });
    }
    let server = rt.create(&spec).await.unwrap();
    rt.start(&server).await.unwrap();
    let mut said = String::new();
    until("the server's answer", async || {
        use std::io::Read;
        match std::net::TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut c) => {
                c.read_to_string(&mut said).unwrap();
                true
            }
            Err(_) => false,
        }
    })
    .await;
    assert_eq!(said, "hi\n");
    assert_eq!(rt.wait(&server).await.unwrap(), 0);
    rt.remove(&server).await.unwrap();
    assert!(rt.find("server").is_err());

    for id in [&app, &once, &long] {
        rt.remove(id).await.unwrap();
    }
    assert!(rt.list(&[]).await.unwrap().is_empty());

    // Without a dataset for them, roots are copies of the image.
    let copies = JailRuntime::new(
        sys.clone() as Arc<dyn Sys>,
        images.clone(),
        Config {
            state_dir: dir.as_path().join("copies"),
            dataset: None,
            shim: vec![env!("CARGO_BIN_EXE_legion-jail-shim").into()],
            prune_images: false,
        },
    )
    .unwrap();
    let copy = copies
        .create(&standalone("copy", "test -x /bin/sh && exit 6"))
        .await
        .unwrap();
    let copy_root = dir.as_path().join("copies").join(&copy).join("root");
    assert!(copy_root.join("bin/sh").is_file());
    assert!(
        !sys.mounts(&dir.as_path().join("copies").to_string_lossy())
            .unwrap()
            .iter()
            .any(|m| std::path::Path::new(m) == copy_root),
        "a plain directory, not a clone"
    );
    assert_eq!(copies.run_attached(&copy, false, false).await.unwrap(), 6);
    copies.remove(&copy).await.unwrap();
    assert!(!copy_root.exists());

    // Build steps, on a plain directory (here a clone of the image).
    let image = images.get(IMAGE).unwrap().unwrap();
    let build = dir.as_path().join("build");
    std::fs::create_dir(&build).unwrap();
    sys.zfs_clone(
        &images.snapshot(&image).unwrap(),
        &format!("{base}/build"),
        &[("mountpoint", &build.to_string_lossy())],
    )
    .unwrap();
    let resolv_before = std::fs::read(build.join("etc/resolv.conf")).ok();
    let out = dir.as_path().join("out");
    std::fs::create_dir(&out).unwrap();
    // Written by nobody too.
    std::fs::set_permissions(&out, std::os::unix::fs::PermissionsExt::from_mode(0o777)).unwrap();
    let mounts = [
        StepMount::Nullfs {
            source: out.clone(),
            target: "/legion-out".into(),
            read_only: false,
        },
        StepMount::Tmpfs {
            target: "/legion-scratch".into(),
            size: Some(1 << 20),
        },
    ];
    let argv = |script: &str| vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()];
    let script = "id -un > /legion-out/user; pwd > /legion-out/pwd; \
        test -s /etc/resolv.conf && echo yes > /legion-out/resolver; \
        : > /legion-scratch/x; : > /flagged && chflags schg /flagged && echo ok > /legion-out/chflags; \
        echo $GREETING > /legion-out/env";
    let step = Step {
        name: "lstand_step",
        root: &build,
        argv: &argv(script),
        env: &["GREETING=hello".to_owned()],
        user: "",
        work_dir: "/srv/app",
        mounts: &mounts,
        network: StepNetwork::Host,
        params: &[],
    };
    assert_eq!(rt.run_step(&step).await.unwrap(), 0);
    let read = |f: &str| std::fs::read_to_string(out.join(f)).unwrap_or_default();
    assert_eq!(read("user"), "root\n");
    assert_eq!(read("pwd"), "/srv/app\n");
    assert_eq!(read("chflags"), "ok\n", "a step may set file flags");
    assert_eq!(read("env"), "hello\n");
    if std::path::Path::new("/etc/resolv.conf").exists() {
        assert_eq!(read("resolver"), "yes\n", "the host's resolver is lent");
    }
    // As another user, from the root's /etc/passwd.
    let as_nobody = Step {
        user: "nobody",
        argv: &argv("id -un > /legion-out/who"),
        ..step
    };
    assert_eq!(rt.run_step(&as_nobody).await.unwrap(), 0);
    assert_eq!(read("who"), "nobody\n");
    // What the step made for its mounts and its resolver went; what it
    // wrote stayed.
    assert!(!build.join("legion-out").exists());
    assert!(!build.join("legion-scratch").exists());
    assert_eq!(
        std::fs::read(build.join("etc/resolv.conf")).ok(),
        resolv_before
    );
    assert!(build.join("srv/app").is_dir() && build.join("flagged").exists());
    // Without a network: only lo0; and its exit code.
    let alone = Step {
        argv: &argv("ifconfig -l > /legion-out/ifs; exit 3"),
        network: StepNetwork::None,
        ..step
    };
    assert_eq!(rt.run_step(&alone).await.unwrap(), 3);
    assert_eq!(read("ifs"), "lo0\n");
    // Its future dropped while it runs: all of that goes all the same.
    let slow = Step {
        argv: &argv("sleep 60"),
        ..step
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(2), rt.run_step(&slow))
            .await
            .is_err()
    );
    assert!(!build.join("legion-out").exists());
    assert!(!build.join("legion-scratch").exists());
    assert_eq!(
        std::fs::read(build.join("etc/resolv.conf")).ok(),
        resolv_before
    );
    let steps = dir.as_path().join("jails/.steps");
    assert!(!steps.exists() || std::fs::read_dir(&steps).unwrap().next().is_none());
    assert!(!sys.jails().unwrap().iter().any(|j| j.name == "lstand_step"));
    assert!(
        sys.mounts(&build.to_string_lossy())
            .unwrap()
            .iter()
            .all(|m| std::path::Path::new(m) == build),
        "only the root's own dataset is mounted"
    );
    let _ = std::process::Command::new("chflags")
        .args(["noschg", &build.join("flagged").to_string_lossy()])
        .status();

    sys.zfs_destroy(&format!("{base}/build"), true).unwrap();
    images.remove(IMAGE).await.unwrap();
    sys.zfs_destroy(&base, true).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
