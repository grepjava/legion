//! What a build host (jailer) does with a store besides pulling: images
//! built on it, kept without ZFS, pinned against removal, exported and
//! pushed, against a registry served in the test that takes uploads.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode};
use legion_image::layer::{self, LayerArtifact};
use legion_image::oci::{self, Descriptor, ImageConfig};
use legion_image::{Client, Image, Pin, Reference, Store, chain_ids, hex_of, sha256};
use legion_sys::{FakeSys, Sys};

/// Blobs and manifests by digest or tag, as pushed.
#[derive(Default)]
struct Registry {
    blobs: BTreeMap<String, Vec<u8>>,
    manifests: BTreeMap<String, Vec<u8>>,
    /// Told when a push next asks whether a blob is here; the question
    /// then waits to be resumed.
    hook: Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
}

/// Serves `registry` without authentication, taking monolithic uploads.
async fn serve(registry: Arc<Mutex<Registry>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let registry = registry.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let registry = registry.clone();
                    async move { Ok::<_, Infallible>(answer(&registry, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    addr
}

async fn answer(
    registry: &Mutex<Registry>,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let reply = |status: StatusCode, body: Vec<u8>| {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    };
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let body = req.into_body().collect().await.unwrap().to_bytes().to_vec();
    // Before a push reads the blob it asks about.
    if method == Method::HEAD && path.contains("/blobs/") {
        let hook = registry.lock().unwrap().hook.take();
        if let Some((told, resume)) = hook {
            let _ = told.send(());
            let _ = resume.await;
        }
    }
    let last = path.rsplit('/').next().unwrap_or("").to_owned();
    let mut r = registry.lock().unwrap();
    match method {
        Method::POST if path.ends_with("/blobs/uploads/") => Response::builder()
            .status(StatusCode::ACCEPTED)
            .header("location", format!("{path}u1"))
            .body(Full::new(Bytes::new()))
            .unwrap(),
        Method::PUT if path.contains("/blobs/uploads/") => {
            let d = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("digest="))
                .unwrap_or("")
                .to_owned();
            assert_eq!(sha256(&body), d, "an upload matches its digest");
            r.blobs.insert(d, body);
            reply(StatusCode::CREATED, Vec::new())
        }
        Method::PUT if path.contains("/manifests/") => {
            r.manifests.insert(sha256(&body), body.clone());
            r.manifests.insert(last, body);
            reply(StatusCode::CREATED, Vec::new())
        }
        Method::HEAD | Method::GET if path.contains("/blobs/") => match r.blobs.get(&last) {
            Some(b) if method == Method::GET => reply(StatusCode::OK, b.clone()),
            Some(_) => reply(StatusCode::OK, Vec::new()),
            None => reply(StatusCode::NOT_FOUND, Vec::new()),
        },
        Method::GET if path.contains("/manifests/") => match r.manifests.get(&last) {
            Some(b) => reply(StatusCode::OK, b.clone()),
            None => reply(StatusCode::NOT_FOUND, Vec::new()),
        },
        _ => reply(StatusCode::NOT_FOUND, Vec::new()),
    }
}

/// Builds a one-layer image of these files in `s` as `name`, for `arch`.
async fn build(s: &Store, pin: &Pin, name: &str, arch: &str, files: &[(&str, &str)]) -> Image {
    let work = tempfile::tempdir_in(s.dir()).unwrap();
    let root = work.path().join("root");
    for (p, data) in files {
        let p = root.join(p);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, data).unwrap();
    }
    let (tar, gz) = (work.path().join("l.tar"), work.path().join("l.tar.gz"));
    let art: LayerArtifact = layer::pack(&root, &tar, &gz).unwrap();
    let chain = chain_ids(std::slice::from_ref(&art.diff_id));
    let hex = hex_of(&chain[0]).unwrap().to_owned();
    let desc = s
        .commit_built_layer(pin, None, &hex, &tar, &gz, &art)
        .await
        .unwrap();
    let mut config = ImageConfig {
        architecture: arch.to_owned(),
        os: "freebsd".to_owned(),
        ..Default::default()
    };
    config.config.cmd = vec!["/bin/app".to_owned()];
    config.rootfs.diff_ids = vec![art.diff_id];
    config
        .extra
        .insert("author".to_owned(), serde_json::json!("test"));
    s.register_built_image(
        pin,
        &Reference::parse_local_tag(name).unwrap(),
        config,
        vec![hex],
        vec![desc],
    )
    .await
    .unwrap()
}

fn directory_store(dir: &Path, client: Client) -> Store {
    let sys: Arc<dyn Sys> = Arc::new(FakeSys::new());
    Store::open_directory(sys, dir, client)
        .unwrap()
        .keep_blobs()
}

fn blobs(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir.join("blobs/sha256"))
        .map(|d| {
            d.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn built_images_kept_without_zfs_pinned_and_exported() {
    let t = tempfile::tempdir().unwrap();
    let roots_file = t.path().join("store/roots");
    let roots_path = roots_file.clone();
    let s = directory_store(&t.path().join("store"), Client::new().unwrap()).with_gc_roots(
        Arc::new(move |_: &Path| {
            Ok(std::fs::read_to_string(&roots_path)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect())
        }),
    );
    let pin = s.pin().unwrap();
    let image = build(&s, &pin, "app:1", oci::arch(), &[("bin/app", "v1")]).await;
    drop(pin);
    let hex = image.layers[0].clone();
    assert_eq!(image.references, ["localhost/app:1"]);
    assert!(s.layer_dir(&hex).join("bin/app").exists());
    assert!(s.has_layer(&hex).unwrap());
    assert_eq!(s.snapshot(&image), None);
    // `get` reads names as a registry does, `find` as a build host does.
    assert!(s.get("app:1").unwrap().is_none());
    assert_eq!(s.find("app:1").unwrap().unwrap().id, image.id);
    assert_eq!(s.find(&image.id[7..19]).unwrap().unwrap().id, image.id);
    // Config keys the store does not name survive in the image's config.
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(s.blob_path(&image.id).unwrap()).unwrap()).unwrap();
    assert_eq!(config["author"], "test");

    // A writable copy.
    let root = t.path().join("rootfs");
    s.prepare_rootfs(&image, &root, None).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("bin/app")).unwrap(), "v1");
    s.cleanup_rootfs(&root, None).unwrap();
    assert!(!root.exists());

    // An export loads into another store as the same image, and holds
    // Docker's manifest too.
    let archive = t.path().join("app.tar");
    s.export("app:1", &archive, None).await.unwrap();
    let other = directory_store(&t.path().join("other"), Client::new().unwrap());
    let loaded = other.load(&archive, None).await.unwrap();
    assert_eq!(
        (loaded.id.as_str(), loaded.digest.as_str()),
        (image.id.as_str(), image.digest.as_str())
    );
    assert_eq!(loaded.layer_descriptors, image.layer_descriptors);
    let mut names = Vec::new();
    let mut a = tar::Archive::new(std::fs::File::open(&archive).unwrap());
    for e in a.entries().unwrap() {
        names.push(e.unwrap().path().unwrap().to_string_lossy().into_owned());
    }
    assert!(
        names.iter().any(|n| n.ends_with("manifest.json")),
        "{names:?}"
    );

    // A pin keeps a layer and its blobs through a removal until dropped.
    let kept = blobs(s.dir());
    assert_eq!(kept.len(), 3, "layer, config and manifest");
    let pin = s.pin().unwrap();
    assert!(s.pin_image(&pin, "app:1").await.unwrap().is_some());
    assert!(s.remove("localhost/app:1").await.unwrap());
    assert!(s.has_layer(&hex).unwrap());
    assert_eq!(blobs(s.dir()), kept);
    drop(pin);
    // A GC root keeps a layer too.
    std::fs::write(&roots_file, format!("{hex}\n")).unwrap();
    assert!(s.prune().await.unwrap().is_empty());
    assert!(s.has_layer(&hex).unwrap());
    assert!(blobs(s.dir()).is_empty(), "blobs no image or root names go");
    std::fs::remove_file(&roots_file).unwrap();
    assert_eq!(s.prune().await.unwrap(), std::slice::from_ref(&hex));
    assert!(!s.has_layer(&hex).unwrap());
}

#[tokio::test]
async fn pushes_images_and_indexes_and_pulls_them_back() {
    let registry = Arc::new(Mutex::new(Registry::default()));
    let addr = serve(registry.clone()).await;
    let client = || Client::new().unwrap().plain_http(&addr);
    let t = tempfile::tempdir().unwrap();
    let s = directory_store(&t.path().join("store"), client());
    let pin = s.pin().unwrap();
    let native = build(&s, &pin, "app:native", oci::arch(), &[("bin/app", "n")]).await;
    let other_arch = if oci::arch() == "amd64" {
        "arm64"
    } else {
        "amd64"
    };
    let other = build(&s, &pin, "app:other", other_arch, &[("bin/app", "o")]).await;
    drop(pin);

    let target = Reference::parse(&format!("{addr}/me/app:1")).unwrap();
    assert_eq!(
        s.push("app:native", &target, None).await.unwrap(),
        native.digest
    );
    let e = s
        .push(
            "app:native",
            &Reference::parse("localhost/x:1").unwrap(),
            None,
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("localhost reference"), "{e}");

    // An index of two local images, then of one local and one pushed.
    let index = Reference::parse(&format!("{addr}/me/app:multi")).unwrap();
    let sources = ["app:native".to_owned(), "app:other".to_owned()];
    let digest = s.push_index(&index, &sources, None).await.unwrap();
    let (got, doc) = s.client().document(&index, "multi", None).await.unwrap();
    assert_eq!(got, digest);
    let platforms: Vec<_> = doc
        .manifests
        .unwrap()
        .into_iter()
        .map(|d: Descriptor| d.platform.unwrap().architecture)
        .collect();
    assert_eq!(platforms, [oci::arch(), other_arch]);
    let pushed_other = Reference::parse(&format!("{addr}/me/app:other")).unwrap();
    s.push("app:other", &pushed_other, None).await.unwrap();
    let mixed = Reference::parse(&format!("{addr}/me/app:mixed")).unwrap();
    s.push_index(
        &mixed,
        &["app:native".to_owned(), pushed_other.to_string()],
        None,
    )
    .await
    .unwrap();
    let e = s
        .push_index(
            &mixed,
            &["app:native".to_owned(), "app:native".to_owned()],
            None,
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("two images for"), "{e}");
    assert_eq!(other.config.architecture, other_arch);

    // A node pulls its own platform's image from either, on ZFS, without
    // keeping blobs: as Legion's daemon does.
    let sys = Arc::new(FakeSys::new());
    let node = Store::open(
        sys.clone() as Arc<dyn Sys>,
        "tank/legion/images",
        &t.path().join("node"),
        client(),
    )
    .unwrap();
    for r in [&target, &index, &mixed] {
        let pulled = node.pull(&r.to_string(), None, "t").await.unwrap();
        assert_eq!(pulled.id, native.id);
        assert!(pulled.layer_descriptors.is_empty());
    }
    assert!(
        !node.dir().join("blobs").exists(),
        "no blobs kept by default"
    );

    // A store that kept no blobs pushes too, its layers packed again (from
    // directories here: the fake ZFS keeps no files in clones).
    let plain = Store::open_directory(
        sys.clone() as Arc<dyn Sys>,
        &t.path().join("plain"),
        client(),
    )
    .unwrap();
    plain.pull(&target.to_string(), None, "t").await.unwrap();
    assert!(!plain.dir().join("blobs").exists());
    let again = Reference::parse(&format!("{addr}/me/app:again")).unwrap();
    plain.push(&target.to_string(), &again, None).await.unwrap();
    let fresh = directory_store(&t.path().join("fresh"), client());
    let repacked = fresh.pull(&again.to_string(), None, "t").await.unwrap();
    assert_ne!(repacked.id, native.id);
    let root = t.path().join("repacked");
    fresh.prepare_rootfs(&repacked, &root, None).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("bin/app")).unwrap(), "n");
}

#[tokio::test]
async fn what_a_push_reads_stays_while_it_runs() {
    let registry = Arc::new(Mutex::new(Registry::default()));
    let addr = serve(registry.clone()).await;
    let client = || Client::new().unwrap().plain_http(&addr);
    let t = tempfile::tempdir().unwrap();
    // Pauses the next upload; while it waits, `meddle` runs.
    async fn paused<F: Future<Output = ()>>(
        registry: &Mutex<Registry>,
        push: impl Future<Output = Result<String, legion_image::Error>>,
        meddle: impl FnOnce() -> F,
    ) -> Result<String, legion_image::Error> {
        let (told, arrived) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        registry.lock().unwrap().hook = Some((told, resumed));
        let meddling = async {
            arrived.await.unwrap();
            meddle().await;
            resume.send(()).unwrap();
        };
        tokio::join!(push, meddling).0
    }

    // The image removed while it is pushed: its blobs stay until the push
    // is done.
    let s = directory_store(&t.path().join("store"), client());
    let pin = s.pin().unwrap();
    let image = build(&s, &pin, "app:gone", oci::arch(), &[("bin/app", "g")]).await;
    drop(pin);
    let target = Reference::parse(&format!("{addr}/me/app:gone")).unwrap();
    let pushed = paused(&registry, s.push("app:gone", &target, None), || async {
        assert!(s.remove("localhost/app:gone").await.unwrap());
    })
    .await;
    assert_eq!(pushed.unwrap(), image.digest);
    assert!(s.find("app:gone").unwrap().is_none());
    s.prune().await.unwrap();
    assert!(blobs(s.dir()).is_empty(), "and go after it");

    // A store that keeps no blobs packs the layer again for a push; a prune
    // meanwhile leaves what it packed.
    let plain = Store::open_directory(
        Arc::new(FakeSys::new()) as Arc<dyn Sys>,
        &t.path().join("plain"),
        client(),
    )
    .unwrap();
    plain.pull(&target.to_string(), None, "t").await.unwrap();
    let again = Reference::parse(&format!("{addr}/me/app:again")).unwrap();
    let pushed = paused(
        &registry,
        plain.push(&target.to_string(), &again, None),
        || async {
            plain.prune().await.unwrap();
        },
    )
    .await;
    pushed.unwrap();

    // An export, too, holds what it reads.
    let archive = t.path().join("plain.tar");
    plain
        .export(&target.to_string(), &archive, None)
        .await
        .unwrap();
    assert!(archive.exists());

    // What it packs again, it does not write over where it is already
    // (another push may be reading it): made read-only, it is left alone.
    let packed = blobs(plain.dir());
    assert!(!packed.is_empty());
    for b in &packed {
        let path = plain.dir().join("blobs/sha256").join(b);
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&path, perms).unwrap();
    }
    plain
        .export(&target.to_string(), &archive, None)
        .await
        .unwrap();
    assert_eq!(blobs(plain.dir()), packed);
    assert_eq!(
        std::fs::read_dir(plain.dir().join("tmp")).unwrap().count(),
        0,
        "nothing left in the store's tmp"
    );
}
