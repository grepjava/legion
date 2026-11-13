//! Output of tasks whose pods are gone (DESIGN.md §3.8). Before a pod's
//! containers are removed, the agent saves what each wrote, its last
//! [`MAX`] bytes, to `<logs dir>/<task uid>/<container>`, so `legion logs`
//! still answers for a completed Job or a crashed task. The files go when
//! the task is no longer assigned here, which is when it is deleted.
//!
//! A file holds one record per line written, each the stream (1 for
//! stdout, 2 for stderr), the length as a big-endian u32, then the bytes.
//! A record whose stream byte has [`TIMED`] set has, after the length, when
//! the line was written: nanoseconds since the epoch, a big-endian u64.
//! Files written before have no times, and read as before.

use std::path::{Path, PathBuf};

use crate::runtime::{Chunk, Output};
use bytes::Bytes;
use futures::StreamExt;

use crate::runtime::Runtime;

/// What is kept of each container's output.
pub(crate) const MAX: usize = 1 << 20;

const HEADER: usize = 5;
/// Set on the stream byte of a record that carries its time.
const TIMED: u8 = 0x80;

fn file(root: &Path, uid: &str, container: &str) -> PathBuf {
    // Container names are DNS labels, and uids hex.
    root.join(uid).join(container)
}

/// Saves the output of each `(container name, container id)`. A container
/// whose output cannot be read is skipped.
pub(crate) async fn save<R: Runtime>(
    rt: &R,
    root: &Path,
    uid: &str,
    containers: &[(String, String)],
) {
    for (name, id) in containers {
        let chunks = match rt.logs(id, false, None).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(container = %name, "reading output to keep: {e}");
                continue;
            }
        };
        let lines = last(
            chunks
                .filter_map(|c| async { c.ok() })
                .collect::<Vec<_>>()
                .await,
        );
        let path = file(root, uid, name);
        let written = tokio::task::spawn_blocking(move || write(&path, &lines)).await;
        if let Ok(Err(e)) = written {
            tracing::warn!(container = %name, "keeping output: {e}");
        }
    }
}

/// The output split into lines, of which the last [`MAX`] bytes at most.
fn last(chunks: Vec<Chunk>) -> Vec<Chunk> {
    let mut lines = Vec::new();
    for c in chunks {
        let mut rest = c.data;
        while !rest.is_empty() {
            let at = rest
                .iter()
                .position(|b| *b == b'\n')
                .map_or(rest.len(), |i| i + 1);
            lines.push(Chunk {
                stream: c.stream,
                data: rest.split_to(at),
                at: c.at,
            });
        }
    }
    let mut size = 0;
    let keep = lines
        .iter()
        .rev()
        .take_while(|c| {
            size += c.data.len();
            size <= MAX
        })
        .count();
    lines.split_off(lines.len() - keep)
}

fn write(path: &Path, lines: &[Chunk]) -> std::io::Result<()> {
    let mut out = Vec::with_capacity(lines.iter().map(|c| HEADER + 8 + c.data.len()).sum());
    for c in lines {
        let stream = match c.stream {
            Output::Stderr => 2,
            _ => 1,
        };
        let nanos =
            c.at.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as u64);
        out.push(if nanos.is_some() {
            stream | TIMED
        } else {
            stream
        });
        out.extend_from_slice(&(c.data.len() as u32).to_be_bytes());
        if let Some(n) = nanos {
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&c.data);
    }
    std::fs::create_dir_all(path.parent().expect("a task directory"))?;
    let tmp = path.with_extension("new");
    std::fs::write(&tmp, out)?;
    std::fs::rename(&tmp, path)
}

/// The saved output of a container, its last `tail` lines if given; `None`
/// if none was saved.
pub(crate) fn read(
    root: &Path,
    uid: &str,
    container: &str,
    tail: Option<usize>,
) -> Option<Vec<Chunk>> {
    let data = Bytes::from(std::fs::read(file(root, uid, container)).ok()?);
    let mut lines = Vec::new();
    let mut at = 0;
    while at + HEADER <= data.len() {
        let len =
            u32::from_be_bytes(data[at + 1..at + HEADER].try_into().expect("4 bytes")) as usize;
        let timed = data[at] & TIMED != 0;
        let mut start = at + HEADER;
        let mut when = None;
        if timed {
            if start + 8 > data.len() {
                break;
            }
            let n = u64::from_be_bytes(data[start..start + 8].try_into().expect("8 bytes"));
            when = Some(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(n));
            start += 8;
        }
        let end = (start + len).min(data.len());
        lines.push(Chunk {
            stream: if data[at] & !TIMED == 2 {
                Output::Stderr
            } else {
                Output::Stdout
            },
            data: data.slice(start..end),
            at: when,
        });
        at = end;
    }
    if let Some(n) = tail {
        lines = lines.split_off(lines.len().saturating_sub(n));
    }
    Some(lines)
}

/// Task uids with saved output.
pub(crate) fn present(root: &Path) -> Vec<String> {
    crate::files::present(root)
}

pub(crate) fn remove(root: &Path, uid: &str) -> std::io::Result<()> {
    match std::fs::remove_dir_all(root.join(uid)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(stream: Output, s: &str) -> Chunk {
        Chunk {
            stream,
            data: Bytes::from(s.to_owned()),
            at: None,
        }
    }

    /// Lines kept with their times read back with them; one without a time
    /// (as every line in a file from before) reads as before.
    #[test]
    fn keeps_each_lines_time() {
        let dir = tempfile::tempdir().unwrap();
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(1_794_000_000_123_456_789);
        let lines = last(vec![
            Chunk {
                at: Some(t),
                ..chunk(Output::Stderr, "first\nsecond\n")
            },
            chunk(Output::Stdout, "untimed\n"),
        ]);
        write(&file(dir.path(), "u1", "app"), &lines).unwrap();
        let back = read(dir.path(), "u1", "app", None).unwrap();
        let seen: Vec<_> = back
            .iter()
            .map(|c| {
                (
                    c.stream,
                    String::from_utf8_lossy(&c.data).into_owned(),
                    c.at,
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                (Output::Stderr, "first\n".to_owned(), Some(t)),
                (Output::Stderr, "second\n".to_owned(), Some(t)),
                (Output::Stdout, "untimed\n".to_owned(), None),
            ]
        );
    }

    #[test]
    fn keeps_the_last_lines_with_their_streams() {
        let dir = tempfile::tempdir().unwrap();
        let lines = last(vec![
            chunk(Output::Stdout, "one\ntwo\n"),
            chunk(Output::Stderr, "oops\n"),
            chunk(Output::Stdout, "three"),
        ]);
        write(&file(dir.path(), "u1", "app"), &lines).unwrap();
        let back = read(dir.path(), "u1", "app", None).unwrap();
        let text: Vec<_> = back
            .iter()
            .map(|c| (c.stream, String::from_utf8_lossy(&c.data).into_owned()))
            .collect();
        assert_eq!(
            text,
            [
                (Output::Stdout, "one\n".to_owned()),
                (Output::Stdout, "two\n".to_owned()),
                (Output::Stderr, "oops\n".to_owned()),
                (Output::Stdout, "three".to_owned()),
            ]
        );
        let tail = read(dir.path(), "u1", "app", Some(2)).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(&tail[0].data[..], b"oops\n");
        assert_eq!(read(dir.path(), "u1", "other", None), None);
        assert_eq!(present(dir.path()), ["u1"]);
        remove(dir.path(), "u1").unwrap();
        remove(dir.path(), "u1").unwrap();
        assert!(present(dir.path()).is_empty());
    }

    #[test]
    fn bounded() {
        let line = "x".repeat(1023) + "\n";
        let lines = last(vec![chunk(Output::Stdout, &line.repeat(2000))]);
        assert_eq!(lines.len(), MAX / 1024);
    }
}
