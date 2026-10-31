//! Applying a layer to a directory: the blob decompressed and checked
//! against its `diff_id`, whiteouts applied, then the rest extracted by
//! bsdtar, which keeps file flags and refuses paths leaving the root. And
//! the other way: a directory packed into a layer and compressed.

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};

use crate::Error;

const WHITEOUT: &str = ".wh.";
const OPAQUE: &str = ".wh..wh..opq";

/// A layer packed and compressed for a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerArtifact {
    /// The uncompressed tar's digest, for the config's `rootfs.diff_ids`.
    pub diff_id: String,
    /// The gzip blob's digest, for the manifest.
    pub blob_digest: String,
    /// The gzip blob's size in bytes.
    pub blob_size: u64,
}

/// Decompresses `blob` (gzip, zstd or plain tar, told by its first bytes)
/// into `tar`, checking the result against `diff_id`.
pub fn decompress(blob: &Path, diff_id: &str, tar: &Path) -> Result<(), Error> {
    let mut input = BufReader::new(File::open(blob)?);
    let mut magic = [0u8; 4];
    let n = read_full(&mut input, &mut magic)?;
    let head = std::io::Cursor::new(magic[..n].to_vec()).chain(input);
    let mut reader: Box<dyn Read> = match magic {
        [0x1f, 0x8b, ..] => Box::new(flate2::read::MultiGzDecoder::new(head)),
        [0x28, 0xb5, 0x2f, 0xfd] => Box::new(
            ruzstd::decoding::StreamingDecoder::new(head)
                .map_err(|e| Error::Unsupported(format!("a zstd layer: {e}")))?,
        ),
        _ => Box::new(head),
    };
    let mut out = std::io::BufWriter::new(File::create(tar)?);
    let mut hash = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
        out.write_all(&buf[..n])?;
    }
    out.flush()?;
    let got = format!("sha256:{:x}", hash.finalize());
    if got != diff_id {
        return Err(Error::Digest {
            what: "layer contents (diff_id)".to_owned(),
            expected: diff_id.to_owned(),
            got,
        });
    }
    Ok(())
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            m => n += m,
        }
    }
    Ok(n)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Whiteout {
    /// A path lower layers made, to remove.
    Remove(PathBuf),
    /// A directory whose lower contents are hidden: emptied.
    Opaque(PathBuf),
}

/// The whiteouts in an uncompressed layer.
pub fn whiteouts(tar: &Path) -> Result<Vec<Whiteout>, Error> {
    let mut archive = tar::Archive::new(BufReader::new(File::open(tar)?));
    let mut out = Vec::new();
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?.into_owned();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        if name == OPAQUE {
            out.push(Whiteout::Opaque(dir));
        } else if let Some(hidden) = name.strip_prefix(WHITEOUT) {
            out.push(Whiteout::Remove(dir.join(hidden)));
        }
    }
    Ok(out)
}

/// `rel` under `root`, or `None` if it reaches outside it or through a
/// symlink (which a lower layer's directory cannot be, so there is nothing
/// of it to remove).
fn inside(root: &Path, rel: &Path) -> Result<Option<PathBuf>, Error> {
    let mut path = root.to_path_buf();
    let mut parts = rel
        .components()
        .filter(|c| *c != Component::CurDir)
        .peekable();
    while let Some(c) = parts.next() {
        let Component::Normal(name) = c else {
            return Err(Error::Unsupported(format!(
                "a whiteout outside the image: {}",
                rel.display()
            )));
        };
        path.push(name);
        if parts.peek().is_some() {
            match std::fs::symlink_metadata(&path) {
                Ok(m) if m.is_dir() => {}
                _ => return Ok(None),
            }
        }
    }
    Ok(Some(path))
}

/// Applies whiteouts to `root`, a lower layer's clone.
pub fn apply_whiteouts(root: &Path, whiteouts: &[Whiteout]) -> Result<(), Error> {
    for w in whiteouts {
        match w {
            Whiteout::Remove(rel) => {
                if let Some(p) = inside(root, rel)? {
                    remove_tree(&p)?;
                }
            }
            Whiteout::Opaque(rel) => {
                let Some(dir) = inside(root, rel)? else {
                    continue;
                };
                match std::fs::symlink_metadata(&dir) {
                    Ok(m) if m.is_dir() => {
                        for e in std::fs::read_dir(&dir)? {
                            remove_tree(&e?.path())?;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// Removes a file, link or directory tree without following links,
/// clearing file flags (`schg` and the like) that would refuse it.
pub fn remove_tree(path: &Path) -> Result<(), Error> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    clear_flags(path)?;
    if meta.is_dir() {
        for e in std::fs::read_dir(path)? {
            remove_tree(&e?.path())?;
        }
        std::fs::remove_dir(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(target_os = "freebsd")]
fn clear_flags(path: &Path) -> Result<(), Error> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| Error::Unsupported(format!("a NUL in {}", path.display())))?;
    // SAFETY: `c` is a valid NUL-terminated path; lchflags does not follow
    // a final symlink.
    if unsafe { libc::lchflags(c.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(target_os = "freebsd"))]
fn clear_flags(_: &Path) -> Result<(), Error> {
    Ok(())
}

/// Extracts an uncompressed layer into `root`, leaving out whiteout files.
pub fn extract(tar: &Path, root: &Path) -> Result<(), Error> {
    let out = extract_command(tar, root).output()?;
    if !out.status.success() {
        return Err(Error::Extract(
            String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        ));
    }
    Ok(())
}

/// [`extract`]'s bsdtar; `tar` is `-` for its input.
fn extract_command(tar: &Path, root: &Path) -> Command {
    let mut cmd = Command::new("tar");
    cmd.arg("-x");
    if cfg!(target_os = "freebsd") {
        // Owners as numbers (the image's, not this host's names), modes,
        // file flags, extended attributes; and flags cleared on files a
        // layer replaces.
        cmd.args([
            "-p",
            "--numeric-owner",
            "--fflags",
            "--xattrs",
            "--clear-nochange-fflags",
        ]);
    }
    cmd.arg("-f")
        .arg(tar)
        .arg("-C")
        .arg(root)
        .args(["--exclude", ".wh.*", "--exclude", "*/.wh.*"]);
    cmd
}

/// Applies an uncompressed layer to `root`.
pub fn apply(tar: &Path, root: &Path) -> Result<(), Error> {
    apply_whiteouts(root, &whiteouts(tar)?)?;
    extract(tar, root)
}

/// Archives the tree at `root` into `tar` with bsdtar, keeping owners as
/// numbers, modes and, on FreeBSD, file flags and extended attributes.
pub fn archive(root: &Path, tar: &Path) -> Result<(), Error> {
    let out = archive_command(root, tar).output()?;
    if !out.status.success() {
        return Err(archiving(root, &out.stderr));
    }
    Ok(())
}

/// [`archive`]'s bsdtar; `tar` is `-` for its output.
fn archive_command(root: &Path, tar: &Path) -> Command {
    let mut cmd = Command::new("tar");
    if cfg!(target_os = "freebsd") {
        cmd.args(["--numeric-owner", "--fflags", "--xattrs"]);
    }
    cmd.arg("-cf").arg(tar).arg("-C").arg(root).arg(".");
    cmd
}

fn archiving(root: &Path, stderr: &[u8]) -> Error {
    Error::Extract(format!(
        "archiving {}: {}",
        root.display(),
        String::from_utf8_lossy(stderr).trim()
    ))
}

/// Copies the tree at `src` to `dst` as [`archive`] and [`extract`] keep it
/// (elsewhere than Unix, files, directories and links only). On Unix one
/// tar pipes into the other: nothing is written but the copy, however big
/// the tree.
pub fn copy_tree(src: &Path, dst: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(dst)?;
    if cfg!(unix) {
        use std::process::Stdio;
        let stdout = Path::new("-");
        let mut from = archive_command(src, stdout)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pipe = from.stdout.take().expect("piped");
        let mut errors = from.stderr.take().expect("piped");
        let errors = std::thread::spawn(move || {
            let mut e = Vec::new();
            let _ = errors.read_to_end(&mut e);
            e
        });
        let into = extract_command(stdout, dst).stdin(pipe).output();
        let archived = from.wait()?;
        let errors = errors.join().unwrap_or_default();
        if !archived.success() {
            return Err(archiving(src, &errors));
        }
        let into = into?;
        if !into.status.success() {
            return Err(Error::Extract(
                String::from_utf8_lossy(&into.stderr).trim().to_owned(),
            ));
        }
        return Ok(());
    }
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let target = dst.join(entry.file_name());
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            let link = std::fs::read_link(&path)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(link, &target)?;
            #[cfg(windows)]
            if path.is_dir() {
                std::os::windows::fs::symlink_dir(link, &target)?;
            } else {
                std::os::windows::fs::symlink_file(link, &target)?;
            }
        } else if meta.is_dir() {
            copy_tree(&path, &target)?;
        } else {
            std::fs::copy(&path, &target)?;
        }
    }
    Ok(())
}

/// Packs the tree at `root` into a layer: `tar_out`, and `gz_out` its
/// gzip blob.
pub fn pack(root: &Path, tar_out: &Path, gz_out: &Path) -> Result<LayerArtifact, Error> {
    archive(root, tar_out)?;
    compress(tar_out, gz_out)
}

/// Compresses the layer `tar` to `gz` and names both by digest.
pub fn compress(tar: &Path, gz: &Path) -> Result<LayerArtifact, Error> {
    let mut input = BufReader::new(File::open(tar)?);
    let mut enc = flate2::write::GzEncoder::new(
        std::io::BufWriter::new(File::create(gz)?),
        flate2::Compression::default(),
    );
    let mut diff = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        diff.update(&buf[..n]);
        enc.write_all(&buf[..n])?;
    }
    enc.finish()?.flush()?;
    let mut blob = Sha256::new();
    let mut size = 0u64;
    let mut input = BufReader::new(File::open(gz)?);
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        blob.update(&buf[..n]);
        size += n as u64;
    }
    Ok(LayerArtifact {
        diff_id: format!("sha256:{:x}", diff.finalize()),
        blob_digest: format!("sha256:{:x}", blob.finalize()),
        blob_size: size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tar with these files (a trailing `/` makes a directory).
    fn layer(dir: &Path, name: &str, entries: &[(&str, &str)]) -> PathBuf {
        let path = dir.join(name);
        let mut b = tar::Builder::new(File::create(&path).unwrap());
        for (p, data) in entries {
            let mut h = tar::Header::new_ustar();
            if p.ends_with('/') {
                h.set_entry_type(tar::EntryType::Directory);
                h.set_mode(0o755);
                h.set_size(0);
                b.append_data(&mut h, p, std::io::empty()).unwrap();
            } else {
                h.set_mode(0o644);
                h.set_size(data.len() as u64);
                b.append_data(&mut h, p, data.as_bytes()).unwrap();
            }
        }
        b.finish().unwrap();
        path
    }

    fn gzip(path: &Path) -> PathBuf {
        let out = path.with_extension("tgz");
        let mut gz =
            flate2::write::GzEncoder::new(File::create(&out).unwrap(), flate2::Compression::fast());
        std::io::copy(&mut File::open(path).unwrap(), &mut gz).unwrap();
        gz.finish().unwrap();
        out
    }

    fn digest(path: &Path) -> String {
        format!("sha256:{:x}", Sha256::digest(std::fs::read(path).unwrap()))
    }

    #[test]
    fn checks_the_uncompressed_digest() {
        let t = tempfile::tempdir().unwrap();
        let tar = layer(t.path(), "l.tar", &[("a", "1")]);
        let id = digest(&tar);
        let out = t.path().join("out.tar");
        decompress(&gzip(&tar), &id, &out).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&tar).unwrap());
        decompress(&tar, &id, &out).unwrap();
        let other = layer(t.path(), "m.tar", &[("a", "2")]);
        let e = decompress(&gzip(&other), &id, &out).unwrap_err();
        assert!(matches!(e, Error::Digest { .. }), "{e}");
    }

    #[test]
    fn layers_with_whiteouts() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let base = layer(
            t.path(),
            "base.tar",
            &[
                ("etc/", ""),
                ("etc/motd", "hi"),
                ("etc/keep", "k"),
                ("usr/", ""),
                ("usr/share/", ""),
                ("usr/share/doc/", ""),
                ("usr/share/doc/a", "a"),
                ("usr/share/misc/", ""),
                ("usr/share/misc/old", "o"),
            ],
        );
        apply(&base, &root).unwrap();
        let top = layer(
            t.path(),
            "top.tar",
            &[
                ("etc/.wh.motd", ""),
                ("usr/share/.wh.doc", ""),
                ("usr/share/misc/", ""),
                ("usr/share/misc/.wh..wh..opq", ""),
                ("usr/share/misc/new", "n"),
                ("./etc/added", "+"),
                ("nothing/.wh.here", ""),
            ],
        );
        assert_eq!(whiteouts(&top).unwrap().len(), 4);
        apply(&top, &root).unwrap();
        let has = |p: &str| root.join(p).exists();
        assert!(!has("etc/motd"));
        assert!(has("etc/keep"));
        assert!(!has("usr/share/doc"));
        assert!(!has("usr/share/misc/old"));
        assert_eq!(
            std::fs::read_to_string(root.join("usr/share/misc/new")).unwrap(),
            "n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("etc/added")).unwrap(),
            "+"
        );
        assert!(!has("nothing"));
        let names: Vec<_> = walk(&root);
        assert!(names.iter().all(|n| !n.contains(".wh.")), "{names:?}");
    }

    fn walk(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            out.push(p.display().to_string());
            if p.is_dir() {
                out.extend(walk(&p));
            }
        }
        out
    }

    /// Run again in a process of its own whose temporary directory is
    /// missing, between the two directories named in the environment.
    #[test]
    #[cfg(unix)]
    fn a_copy_takes_no_temporary_space() {
        const DIRS: &str = "LEGION_COPY_TREE_DIRS";
        if let Ok(dirs) = std::env::var(DIRS) {
            let (src, dst) = dirs.split_once('\n').unwrap();
            copy_tree(Path::new(src), Path::new(dst)).unwrap();
            return;
        }
        let t = tempfile::tempdir().unwrap();
        let (src, dst) = (t.path().join("src"), t.path().join("dst"));
        std::fs::create_dir_all(src.join("etc")).unwrap();
        std::fs::write(src.join("etc/motd"), "hi").unwrap();
        std::os::unix::fs::symlink("motd", src.join("etc/link")).unwrap();
        let out = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "layer::tests::a_copy_takes_no_temporary_space"])
            .env(DIRS, format!("{}\n{}", src.display(), dst.display()))
            .env("TMPDIR", t.path().join("missing"))
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(String::from_utf8_lossy(&out.stdout).contains("1 passed"));
        assert_eq!(std::fs::read_to_string(dst.join("etc/motd")).unwrap(), "hi");
        assert_eq!(
            std::fs::read_link(dst.join("etc/link")).unwrap(),
            Path::new("motd")
        );
        // A source that is not there is an error.
        assert!(copy_tree(&t.path().join("none"), &t.path().join("d2")).is_err());
    }

    #[test]
    fn whiteouts_stay_inside_the_root() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(t.path().join("outside"), "x").unwrap();
        let bad = [Whiteout::Remove(PathBuf::from("../outside"))];
        assert!(apply_whiteouts(&root, &bad).is_err());
        assert!(t.path().join("outside").exists());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(t.path(), root.join("link")).unwrap();
            let through = [Whiteout::Remove(PathBuf::from("link/outside"))];
            apply_whiteouts(&root, &through).unwrap();
            assert!(t.path().join("outside").exists());
            let opaque = [Whiteout::Opaque(PathBuf::from("link"))];
            apply_whiteouts(&root, &opaque).unwrap();
            assert!(t.path().join("outside").exists());
        }
    }
}
