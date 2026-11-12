//! jailer as the tests drive it: `LEGION_JAILER`, with a state directory
//! of its own in the lab directory, and what they read of its release
//! records.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

use super::{Lab, text};

/// What the tests read of a jailer release record, schema 1 (jailer's
/// docs/RELEASE-RECORD.md).
#[derive(Deserialize)]
pub struct Record {
    pub schema: u32,
    pub image: Built,
    #[serde(default)]
    pub published: Vec<Published>,
    #[serde(default)]
    pub archives: Vec<Archive>,
}

#[derive(Deserialize)]
pub struct Built {
    pub manifest: String,
}

#[derive(Deserialize)]
pub struct Published {
    pub reference: String,
    pub digest: String,
    pub pinned: String,
    /// For an index: each platform, as `os/arch`, and its manifest.
    #[serde(default)]
    pub platforms: std::collections::BTreeMap<String, String>,
}

#[derive(Deserialize)]
pub struct Archive {
    pub manifest: String,
}

pub fn record(path: &Path) -> Record {
    let r: Record = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(r.schema, 1, "a record this test knows");
    r
}

/// The jailer in `LEGION_JAILER`, with a state directory of its own in the
/// lab directory, on plain directories.
pub struct Jailer {
    pub bin: String,
    pub state: PathBuf,
    /// Registries it talks to over plain HTTP.
    pub plain: Vec<String>,
}

impl Jailer {
    pub fn new(lab: &Lab, plain: &[&str]) -> Jailer {
        let bin = std::env::var("LEGION_JAILER").expect("set LEGION_JAILER to a jailer binary");
        assert!(Path::new(&bin).is_file(), "no jailer at {bin}");
        Jailer {
            bin,
            state: lab.dir.join("jailer"),
            plain: plain.iter().map(|p| p.to_string()).collect(),
        }
    }

    pub fn run(&self, args: &[&str]) -> String {
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
    pub fn build(&self, dir: &Path, tag: &str, id: &str, rec: &Path) {
        self.build_with(dir, tag, id, rec, &[], &[]);
    }

    /// As [`Self::build`], with each of `files`, a name and its text, put
    /// at `/` and the name, and `flags` for the build.
    pub fn build_with(
        &self,
        dir: &Path,
        tag: &str,
        id: &str,
        rec: &Path,
        files: &[(&str, &str)],
        flags: &[&str],
    ) {
        let ctx = dir.join(id);
        std::fs::create_dir_all(&ctx).unwrap();
        let mut containerfile =
            String::from("FROM docker.io/freebsd/freebsd-runtime:15.1\nCOPY id /id\n");
        for (name, body) in files {
            std::fs::write(ctx.join(name), body).unwrap();
            containerfile.push_str(&format!("COPY {name} /{name}\n"));
        }
        std::fs::write(ctx.join("Containerfile"), containerfile).unwrap();
        std::fs::write(ctx.join("id"), id).unwrap();
        let mut args = vec!["build", "-t", tag, "--record", rec.to_str().unwrap()];
        args.extend_from_slice(flags);
        args.push(ctx.to_str().unwrap());
        self.run(&args);
    }
}

impl Jailer {
    /// Builds the Containerfile in `ctx` as it is, into `tag` and the
    /// record `rec`.
    pub fn build_dir(&self, ctx: &Path, tag: &str, rec: &Path) {
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
pub fn unique(what: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    format!("{what}-{}-{}", std::process::id(), t.as_nanos())
}
