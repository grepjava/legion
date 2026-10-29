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
use legion_agent::Runtime;
use legion_agent::runtime::{ContainerSpec, Mount, Output};
use legion_image::{Client, Store};
use legion_jail::{Config, JailRuntime};
use legion_sys::{Shell, Sys};

const IMAGE: &str = "docker.io/freebsd/freebsd-runtime:15.1";

fn runtime(sys: &Arc<Shell>, images: &Arc<Store>, dir: &Path, base: &str) -> JailRuntime {
    JailRuntime::new(
        sys.clone() as Arc<dyn Sys>,
        images.clone(),
        Config {
            state_dir: dir.join("jails"),
            dataset: format!("{base}/jails"),
            shim: vec![env!("CARGO_BIN_EXE_legion-jail-shim").into()],
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
    assert!(!rt.image_exists(IMAGE).await.unwrap());
    rt.pull(IMAGE).await.unwrap();
    assert!(rt.image_exists(IMAGE).await.unwrap());

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
