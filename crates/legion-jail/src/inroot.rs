//! Paths inside a root, resolved as a process jailed there would: links are
//! followed inside it (an absolute one starts again from it, `..` stops at
//! it), so none leads out to the host. For what writes into an image from
//! outside its jail; jailer's builds use it too.

use std::ffi::OsString;
use std::fs::Metadata;
use std::io::{Error, ErrorKind, Result};
use std::path::{Component, Path, PathBuf};

/// Hops through links before giving up, as the kernel does.
const MAX_LINKS: usize = 40;

/// Where `path` is under `root`. What is missing is kept as named. With
/// `follow_last`, a last component that is a link is followed too. Too many
/// links is an [`ErrorKind::InvalidInput`].
pub fn resolve(root: &Path, path: &str, follow_last: bool) -> Result<PathBuf> {
    walk(root, path, follow_last, |_, _| Ok(()))
}

/// [`resolve`], calling `at` with each component that is not followed as a
/// link, on the host, before going past it: with its metadata, or `None` if
/// it is missing. `at` may make what is missing, or refuse what it finds.
pub fn walk(
    root: &Path,
    path: &str,
    follow_last: bool,
    mut at: impl FnMut(&Path, Option<&Metadata>) -> Result<()>,
) -> Result<PathBuf> {
    // Kept reversed: the next component is last.
    let push = |rest: &mut Vec<OsString>, p: &Path| {
        let parts = p.components().rev().filter_map(|c| match c {
            Component::Normal(n) => Some(n.to_owned()),
            Component::ParentDir => Some(OsString::from("..")),
            _ => None,
        });
        rest.extend(parts);
    };
    let mut rest = Vec::new();
    push(&mut rest, Path::new(path));
    let mut done: Vec<OsString> = Vec::new();
    let under = |done: &[OsString]| -> PathBuf {
        std::iter::once(root.as_os_str().to_owned())
            .chain(done.iter().cloned())
            .collect()
    };
    let mut links = 0;
    while let Some(name) = rest.pop() {
        if name == ".." {
            done.pop();
            continue;
        }
        let next = under(&done).join(&name);
        let found = match std::fs::symlink_metadata(&next) {
            Ok(m) => Some(m),
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => None,
            Err(e) => return Err(e),
        };
        let link = found.as_ref().is_some_and(|m| m.file_type().is_symlink());
        if link && (follow_last || !rest.is_empty()) {
            links += 1;
            if links > MAX_LINKS {
                return Err(Error::new(ErrorKind::InvalidInput, "too many links"));
            }
            let to = std::fs::read_link(&next)?;
            if to.has_root() {
                done.clear();
            }
            push(&mut rest, &to);
            continue;
        }
        at(&next, found.as_ref())?;
        done.push(name);
    }
    Ok(under(&done))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn paths_stay_inside_the_root() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        std::fs::create_dir_all(root.join("usr/home")).unwrap();
        symlink("usr/home", root.join("home")).unwrap();
        symlink("/usr/home", root.join("abs")).unwrap();
        symlink("../../..", root.join("usr/up")).unwrap();
        symlink("/etc/passwd", root.join("last")).unwrap();
        symlink("loop", root.join("loop")).unwrap();

        let home = root.join("usr/home");
        assert_eq!(resolve(root, "/home", true).unwrap(), home);
        assert_eq!(resolve(root, "/abs", true).unwrap(), home);
        assert_eq!(resolve(root, "/../../home", true).unwrap(), home);
        // Out through a link stops at the root.
        assert_eq!(
            resolve(root, "/usr/up/etc", true).unwrap(),
            root.join("etc")
        );
        // Followed to where the jail would find it, or left as the link.
        assert_eq!(
            resolve(root, "/last", true).unwrap(),
            root.join("etc/passwd")
        );
        assert_eq!(resolve(root, "/last", false).unwrap(), root.join("last"));
        // But one on the way is always followed.
        assert_eq!(resolve(root, "/home/x", false).unwrap(), home.join("x"));
        // What is missing is kept as named.
        assert_eq!(
            resolve(root, "/no/such", true).unwrap(),
            root.join("no/such")
        );
        let err = resolve(root, "/loop", true).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn each_component_is_seen_before_going_past_it() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        let mut seen = Vec::new();
        walk(root, "/a/b", true, |p, m| {
            seen.push((p.to_owned(), m.is_some()));
            std::fs::create_dir(p)
        })
        .unwrap();
        assert_eq!(seen, [(root.join("a"), false), (root.join("a/b"), false)]);
        assert!(root.join("a/b").is_dir());
    }
}
