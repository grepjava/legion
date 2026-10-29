//! The image store against a registry served in the test, on a fake ZFS.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode};
use legion_image::{Client, Credentials, Error, Store, chain_ids, oci};
use legion_sys::{FakeSys, Sys};
use serde_json::json;
use sha2::{Digest, Sha256};

fn digest(b: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(b))
}

/// A gzipped layer of these files, and its diff_id.
fn layer(files: &[(&str, &str)]) -> (Vec<u8>, String) {
    let mut b = tar::Builder::new(Vec::new());
    for (p, data) in files {
        let mut h = tar::Header::new_ustar();
        h.set_mode(0o644);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, p, data.as_bytes()).unwrap();
    }
    let tar = b.into_inner().unwrap();
    let diff_id = digest(&tar);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut gz, &tar).unwrap();
    (gz.finish().unwrap(), diff_id)
}

#[derive(Default)]
struct Registry {
    /// Path (after `/v2/`) to body and content type.
    files: BTreeMap<String, (Vec<u8>, String)>,
    /// Blob requests served, by digest.
    fetched: Vec<String>,
    /// Repositories only this `user:password` may pull.
    private: BTreeMap<String, String>,
}

impl Registry {
    fn blob(&mut self, repo: &str, data: Vec<u8>) -> String {
        let d = digest(&data);
        self.files
            .insert(format!("{repo}/blobs/{d}"), (data, String::new()));
        d
    }

    fn doc(&mut self, repo: &str, tag: Option<&str>, v: serde_json::Value, kind: &str) -> String {
        let data = serde_json::to_vec(&v).unwrap();
        let d = digest(&data);
        self.files.insert(
            format!("{repo}/manifests/{d}"),
            (data.clone(), kind.to_owned()),
        );
        if let Some(t) = tag {
            self.files
                .insert(format!("{repo}/manifests/{t}"), (data, kind.to_owned()));
        }
        d
    }

    /// An image of these layers for `os`, under an index that also holds a
    /// Linux one.
    fn image(&mut self, repo: &str, tag: &str, os: &str, layers: &[(Vec<u8>, String)]) {
        let (_, arch) = oci::platform();
        let config = json!({
            "architecture": arch, "os": os,
            "config": {"cmd": ["/bin/sh"], "Env": ["PATH=/bin"]},
            "rootfs": {"type": "layers", "diff_ids": layers.iter().map(|l| l.1.clone()).collect::<Vec<_>>()},
        });
        let config = serde_json::to_vec(&config).unwrap();
        let config_size = config.len();
        let cfg = self.blob(repo, config);
        let descs: Vec<_> = layers
            .iter()
            .map(|(data, _)| {
                json!({"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                       "digest": self.blob(repo, data.clone()), "size": data.len()})
            })
            .collect();
        let manifest = self.doc(
            repo,
            None,
            json!({"schemaVersion": 2, "mediaType": oci::OCI_MANIFEST,
                   "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": cfg, "size": config_size},
                   "layers": descs}),
            oci::OCI_MANIFEST,
        );
        let linux = self.doc(
            repo,
            None,
            json!({"schemaVersion": 2, "config": {"digest": cfg}, "layers": []}),
            oci::OCI_MANIFEST,
        );
        self.doc(
            repo,
            Some(tag),
            json!({"schemaVersion": 2, "mediaType": oci::OCI_INDEX, "manifests": [
                {"digest": linux, "platform": {"os": "linux", "architecture": arch}},
                {"digest": manifest, "platform": {"os": os, "architecture": arch}},
            ]}),
            oci::OCI_INDEX,
        );
    }
}

const TOKEN: &str = "t0ken";

/// Serves the registry, asking for a token from its own `/token`, and
/// redirecting blobs to `/cdn/...` (which needs no token).
async fn serve(registry: Arc<Mutex<Registry>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let realm = format!("http://{addr}/token");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let registry = registry.clone();
            let realm = realm.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                    let registry = registry.clone();
                    let realm = realm.clone();
                    async move { Ok::<_, Infallible>(answer(&registry, &realm, &req)) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    addr
}

fn answer(
    registry: &Mutex<Registry>,
    realm: &str,
    req: &Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_owned();
    let reply = |status: StatusCode, body: Vec<u8>| {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    };
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let private = |repo: &str| registry.lock().unwrap().private.get(repo).cloned();
    if path == "/token" {
        let query = req.uri().query().unwrap_or("");
        let query = query.replace("%3A", ":").replace("%2F", "/");
        assert!(query.contains("scope=repository"), "{query}");
        let repo = query
            .split("scope=repository:")
            .nth(1)
            .and_then(|r| r.split(':').next())
            .unwrap_or("");
        // As Docker Hub does, a token without access for the wrong account.
        let token = match private(repo) {
            None => TOKEN.to_owned(),
            Some(account) if auth == format!("Basic {}", STANDARD.encode(&account)) => {
                format!("{TOKEN}-{repo}")
            }
            Some(_) => "no-access".to_owned(),
        };
        return reply(
            StatusCode::OK,
            json!({"token": token}).to_string().into_bytes(),
        );
    }
    if let Some(rest) = path.strip_prefix("/cdn/") {
        let mut r = registry.lock().unwrap();
        let (data, _) = r.files.get(rest).cloned().unwrap();
        r.fetched.push(rest.rsplit('/').next().unwrap().to_owned());
        return reply(StatusCode::OK, data);
    }
    let rest = path.strip_prefix("/v2/").unwrap_or("");
    let repo = rest
        .split_once("/manifests/")
        .or_else(|| rest.split_once("/blobs/"))
        .map_or("", |(r, _)| r);
    let authorized = match private(repo) {
        None => auth == format!("Bearer {TOKEN}"),
        Some(_) => auth == format!("Bearer {TOKEN}-{repo}"),
    };
    if !authorized {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(
                "www-authenticate",
                format!(r#"Bearer realm="{realm}",service="test""#),
            )
            .body(Full::new(Bytes::new()))
            .unwrap();
    }
    if rest.contains("/blobs/") {
        return Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header("location", format!("/cdn/{rest}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
    }
    match registry.lock().unwrap().files.get(rest) {
        Some((data, kind)) => Response::builder()
            .header("content-type", kind.as_str())
            .body(Full::new(Bytes::from(data.clone())))
            .unwrap(),
        None => reply(StatusCode::NOT_FOUND, Vec::new()),
    }
}

fn store(sys: &Arc<FakeSys>, dir: &Path, addr: &str) -> Store {
    let client = Client::new().unwrap().plain_http(addr);
    Store::open(
        sys.clone() as Arc<dyn Sys>,
        "tank/legion/images",
        dir,
        client,
    )
    .unwrap()
}

#[tokio::test]
async fn pulls_share_layers_and_removal_waits_for_containers() {
    let base = layer(&[("etc/base", "b")]);
    let app = layer(&[("srv/app", "a")]);
    let other = layer(&[("srv/other", "o")]);
    let registry = Arc::new(Mutex::new(Registry::default()));
    {
        let mut r = registry.lock().unwrap();
        r.image("team/app", "1", "freebsd", &[base.clone(), app.clone()]);
        r.image("team/other", "1", "freebsd", &[base.clone(), other.clone()]);
        r.image("team/linux", "1", "linux", std::slice::from_ref(&base));
    }
    let addr = serve(registry.clone()).await;
    let t = tempfile::tempdir().unwrap();
    let sys = Arc::new(FakeSys::new());
    let s = store(&sys, t.path(), &addr);

    let image = s
        .pull(&format!("{addr}/team/app:1"), None, "t")
        .await
        .unwrap();
    let chains = chain_ids(&[base.1.clone(), app.1.clone()]);
    let hex: Vec<String> = chains.iter().map(|c| c[7..].to_owned()).collect();
    assert_eq!(image.layers, hex);
    assert_eq!(image.config.config.cmd, ["/bin/sh"]);
    assert_eq!(image.references, [format!("{addr}/team/app:1")]);
    let top = s.snapshot(&image).unwrap();
    assert_eq!(top, format!("tank/legion/images/{}@layer", hex[1]));
    assert_eq!(
        sys.zfs_snapshots(&format!("tank/legion/images/{}", hex[0]))
            .unwrap(),
        ["layer"]
    );
    assert_eq!(
        sys.zfs_list("tank/legion/images").unwrap()[1].mountpoint,
        None,
        "layers are left unmounted"
    );
    assert_eq!(
        registry.lock().unwrap().fetched.len(),
        3,
        "config and two layers"
    );

    // The other image shares the base layer: only its own is fetched.
    let before = registry.lock().unwrap().fetched.len();
    let second = s
        .pull(&format!("{addr}/team/other:1"), None, "t")
        .await
        .unwrap();
    assert_eq!(second.layers[0], hex[0]);
    assert_eq!(registry.lock().unwrap().fetched.len(), before + 2);
    // Pulling again fetches only the config.
    s.pull(&format!("{addr}/team/app:1"), None, "t")
        .await
        .unwrap();
    assert_eq!(registry.lock().unwrap().fetched.len(), before + 3);
    assert_eq!(s.images().unwrap().len(), 2);
    assert!(s.get(&format!("{addr}/team/app:1")).unwrap().is_some());
    assert!(s.get(&format!("{addr}/team/app:2")).unwrap().is_none());

    // A container cloned from the app keeps its layers past removal.
    sys.zfs_create("tank/legion/ctr", &[]).unwrap();
    sys.zfs_clone(&top, "tank/legion/ctr/c1", &[]).unwrap();
    assert!(s.remove(&format!("{addr}/team/app:1")).await.unwrap());
    assert!(!s.remove(&format!("{addr}/team/app:1")).await.unwrap());
    assert!(s.get(&format!("{addr}/team/app:1")).unwrap().is_none());
    let layers = |sys: &FakeSys| sys.zfs_list("tank/legion/images").unwrap().len() - 1;
    assert_eq!(layers(&sys), 3, "base, app (in use), other");
    sys.zfs_destroy("tank/legion/ctr/c1", false).unwrap();
    assert_eq!(s.prune().await.unwrap(), [hex[1].clone()]);
    assert_eq!(layers(&sys), 2);
    s.remove(&format!("{addr}/team/other:1")).await.unwrap();
    assert_eq!(layers(&sys), 0);

    // Only images for this node's OS.
    let e = s
        .pull(&format!("{addr}/team/linux:1"), None, "t")
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no image for freebsd"), "{e}");
    let e = s
        .pull(&format!("{addr}/team/none:1"), None, "t")
        .await
        .unwrap_err();
    assert!(matches!(e, Error::NotFound(_)), "{e}");
    let e = s.pull("localhost/built:1", None, "t").await.unwrap_err();
    assert!(e.to_string().contains("local image"), "{e}");
}

#[tokio::test]
async fn private_images_are_for_those_who_pulled_them() {
    let base = layer(&[("etc/base", "b")]);
    let registry = Arc::new(Mutex::new(Registry::default()));
    {
        let mut r = registry.lock().unwrap();
        r.image("team/secret", "1", "freebsd", std::slice::from_ref(&base));
        r.image("team/open", "1", "freebsd", std::slice::from_ref(&base));
        r.private.insert("team/secret".into(), "bot:pw".into());
    }
    let addr = serve(registry.clone()).await;
    let t = tempfile::tempdir().unwrap();
    let sys = Arc::new(FakeSys::new());
    let s = store(&sys, t.path(), &addr);
    let secret = format!("{addr}/team/secret:1");
    let creds = Credentials {
        username: "bot".into(),
        password: "pw".into(),
    };
    let wrong = Credentials {
        username: "bot".into(),
        password: "nope".into(),
    };

    let e = s.pull(&secret, None, "a").await.unwrap_err();
    assert!(e.to_string().contains("access denied"), "{e}");
    s.pull(&secret, Some(&wrong), "a").await.unwrap_err();
    s.pull(&secret, Some(&creds), "a").await.unwrap();
    assert!(s.usable(&secret, "a").unwrap().is_some());
    assert!(s.usable(&secret, "b").unwrap().is_none());
    assert!(s.get(&secret).unwrap().is_some(), "the node has it");

    // The token the first pull got does not serve another's.
    let e = s.pull(&secret, None, "b").await.unwrap_err();
    assert!(e.to_string().contains("access denied"), "{e}");
    s.pull(&secret, Some(&wrong), "b").await.unwrap_err();
    assert!(s.usable(&secret, "b").unwrap().is_none());
    // With its own login, only the manifest and config are fetched.
    let before = registry.lock().unwrap().fetched.len();
    s.pull(&secret, Some(&creds), "b").await.unwrap();
    assert_eq!(registry.lock().unwrap().fetched.len(), before + 1);
    assert!(s.usable(&secret, "a").unwrap().is_some());
    assert!(s.usable(&secret, "b").unwrap().is_some());

    // Public images are for anyone, whoever pulled them and how.
    let open = format!("{addr}/team/open:1");
    s.pull(&open, None, "a").await.unwrap();
    assert!(s.usable(&open, "c").unwrap().is_some());
    // The same image under a public name stays private under its own.
    assert_eq!(s.images().unwrap().len(), 1);
    assert!(s.usable(&secret, "c").unwrap().is_none());
    // An image found public later is anyone's.
    registry.lock().unwrap().private.clear();
    s.pull(&secret, None, "c").await.unwrap();
    assert!(s.usable(&secret, "d").unwrap().is_some());

    // Removing a name forgets who could use it.
    s.remove(&secret).await.unwrap();
    assert!(s.images().unwrap()[0].private.is_empty());
}

#[tokio::test]
async fn tampered_layers_are_refused() {
    let base = layer(&[("etc/base", "b")]);
    let registry = Arc::new(Mutex::new(Registry::default()));
    let addr = serve(registry.clone()).await;
    {
        let mut r = registry.lock().unwrap();
        // The layer's bytes changed but not its digest: the blob check.
        r.image("team/blob", "1", "freebsd", std::slice::from_ref(&base));
        let blob = digest(&base.0);
        let tampered = layer(&[("etc/base", "evil")]).0;
        r.files
            .insert(format!("team/blob/blobs/{blob}"), (tampered, String::new()));
        // A layer whose contents are not its diff_id (another layer's,
        // claimed to share it): the diff_id check.
        let claimed = (layer(&[("etc/base", "evil")]).0, base.1.clone());
        r.image("team/diff", "1", "freebsd", &[claimed]);
    }
    let t = tempfile::tempdir().unwrap();
    let sys = Arc::new(FakeSys::new());
    let s = store(&sys, t.path(), &addr);
    for repo in ["blob", "diff"] {
        let e = s
            .pull(&format!("{addr}/team/{repo}:1"), None, "t")
            .await
            .unwrap_err();
        assert!(matches!(e, Error::Digest { .. }), "{repo}: {e}");
    }
    assert_eq!(
        sys.zfs_list("tank/legion/images").unwrap().len(),
        1,
        "no layers"
    );
    assert!(s.images().unwrap().is_empty());
}

#[tokio::test]
async fn imports_an_oci_layout() {
    let base = layer(&[("etc/base", "b")]);
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("blobs/sha256");
    std::fs::create_dir_all(&blobs).unwrap();
    let put = |data: &[u8]| {
        let d = digest(data);
        std::fs::write(blobs.join(&d[7..]), data).unwrap();
        d
    };
    let config = serde_json::to_vec(&json!({"os": "freebsd",
        "config": {"Cmd": ["/bin/sh"]}, "rootfs": {"diff_ids": [base.1]}}))
    .unwrap();
    let cfg = put(&config);
    let layer_digest = put(&base.0);
    let manifest = serde_json::to_vec(&json!({"schemaVersion": 2,
        "config": {"digest": cfg}, "layers": [{"digest": layer_digest}]}))
    .unwrap();
    let m = put(&manifest);
    std::fs::write(
        dir.path().join("index.json"),
        serde_json::to_vec(&json!({"schemaVersion": 2, "manifests": [{"digest": m,
            "annotations": {"org.opencontainers.image.ref.name": "localhost/built:1"}}]}))
        .unwrap(),
    )
    .unwrap();
    let t = tempfile::tempdir().unwrap();
    let sys = Arc::new(FakeSys::new());
    let s = store(&sys, t.path(), "127.0.0.1:1");
    let image = s.import(dir.path(), "localhost/built:1").await.unwrap();
    assert_eq!(image.digest, m);
    assert_eq!(image.layers.len(), 1);
    assert!(s.get("localhost/built:1").unwrap().is_some());

    // The same layout as an archive, named by its index.
    let archive = t.path().join("built.tar");
    let mut b = tar::Builder::new(std::fs::File::create(&archive).unwrap());
    b.append_dir_all(".", dir.path()).unwrap();
    b.finish().unwrap();
    drop(b);
    assert!(s.remove("localhost/built:1").await.unwrap());
    let loaded = s.load(&archive, None).await.unwrap();
    assert_eq!(loaded.references, ["localhost/built:1"]);
    let renamed = s.load(&archive, Some("localhost/other:2")).await.unwrap();
    assert_eq!(renamed.id, loaded.id);
    assert_eq!(
        renamed.references,
        ["localhost/built:1", "localhost/other:2"]
    );
    let not_oci = t.path().join("plain.tar");
    let mut b = tar::Builder::new(std::fs::File::create(&not_oci).unwrap());
    b.append_path_with_name(&archive, "manifest.json").unwrap();
    b.finish().unwrap();
    drop(b);
    let e = s.load(&not_oci, None).await.unwrap_err();
    assert!(e.to_string().contains("oci-archive"), "{e}");
}
