//! A registry for the lab's tests: plain HTTP on the host's lab address,
//! without authentication, holding what is pushed in memory. A test binary
//! that uses it has an ignored test `registry` calling [`run`], which
//! [`Registry::start`] runs as a process of its own.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use super::{Lab, wait};

/// A registry the test runs, as this same program (its `registry` test,
/// which calls [`run`]), on the host's lab address.
pub struct Registry {
    pub addr: String,
    child: Child,
    /// The file that takes the registry down, as [`Registry::down`] says.
    down: PathBuf,
}

impl Registry {
    pub fn start(lab: &Lab) -> Registry {
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
    pub fn down(&self, mode: Option<&str>) {
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
pub fn run() {
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
