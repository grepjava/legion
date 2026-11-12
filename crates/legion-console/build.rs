//! Embeds the console's build (`dist/`, made by `npm run build` in `web/`)
//! as a table of paths, bytes and media types. Without one, the table is
//! empty and the gateway says the console was built without its pages.

use std::fmt::Write;
use std::path::{Path, PathBuf};

fn main() {
    let dist = Path::new(env!("CARGO_MANIFEST_DIR")).join("dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let mut files = Vec::new();
    if dist.is_dir() {
        walk(&dist, &mut files);
    }
    files.sort();
    let mut out = String::from("pub static ASSETS: &[(&str, &[u8], &str)] = &[\n");
    for path in &files {
        println!("cargo:rerun-if-changed={}", path.display());
        let rel = path
            .strip_prefix(&dist)
            .expect("under dist")
            .to_string_lossy()
            .replace('\\', "/");
        writeln!(
            out,
            "    ({rel:?}, include_bytes!({:?}), {:?}),",
            path.display().to_string(),
            media_type(&rel)
        )
        .expect("writes");
    }
    out.push_str("];\n");
    let target = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("assets.rs");
    std::fs::write(target, out).expect("writes assets.rs");
}

fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("reads dist") {
        let path = entry.expect("reads dist").path();
        if path.is_dir() {
            walk(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn media_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "webmanifest" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
