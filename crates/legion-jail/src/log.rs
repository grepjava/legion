//! A container's output as the supervisor keeps it: one record per line,
//! `<seconds>.<nanos> <stdout|stderr> <F|P> <text>`, `P` for a piece of a
//! line too long to hold (or cut short when the stream ended). The file is
//! rotated to `<name>.1` past a size, so a container keeps at most about
//! twice that.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use legion_agent::runtime::{Chunk, Output};

/// The longest piece of a line held before it is written as `P`.
const PIECE: usize = 16 << 10;

pub struct Writer {
    path: PathBuf,
    file: File,
    size: u64,
    max: u64,
    /// What each stream wrote since its last newline.
    partial: [Vec<u8>; 2],
}

fn index(stream: Output) -> usize {
    match stream {
        Output::Stdout => 0,
        Output::Stderr => 1,
    }
}

fn name(stream: Output) -> &'static str {
    match stream {
        Output::Stdout => "stdout",
        Output::Stderr => "stderr",
    }
}

impl Writer {
    pub fn open(path: &Path, max: u64) -> std::io::Result<Writer> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(Writer {
            path: path.to_owned(),
            file,
            size,
            max,
            partial: [Vec::new(), Vec::new()],
        })
    }

    pub fn write(&mut self, stream: Output, mut data: &[u8]) -> std::io::Result<()> {
        while let Some(nl) = data.iter().position(|b| *b == b'\n') {
            let mut line = std::mem::take(&mut self.partial[index(stream)]);
            line.extend_from_slice(&data[..nl]);
            self.record(stream, true, &line)?;
            data = &data[nl + 1..];
        }
        let held = &mut self.partial[index(stream)];
        held.extend_from_slice(data);
        if held.len() >= PIECE {
            let piece = std::mem::take(held);
            self.record(stream, false, &piece)?;
        }
        Ok(())
    }

    /// Writes what is held of unfinished lines.
    pub fn flush(&mut self) -> std::io::Result<()> {
        for stream in [Output::Stdout, Output::Stderr] {
            let piece = std::mem::take(&mut self.partial[index(stream)]);
            if !piece.is_empty() {
                self.record(stream, false, &piece)?;
            }
        }
        self.file.flush()
    }

    fn record(&mut self, stream: Output, full: bool, text: &[u8]) -> std::io::Result<()> {
        if self.size >= self.max {
            self.rotate()?;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let mut line = format!(
            "{}.{:09} {} {} ",
            now.as_secs(),
            now.subsec_nanos(),
            name(stream),
            if full { 'F' } else { 'P' }
        )
        .into_bytes();
        line.extend_from_slice(text);
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.size += line.len() as u64;
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        std::fs::rename(&self.path, rotated(&self.path))?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

pub fn rotated(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".1");
    PathBuf::from(p)
}

/// A record back as output: a full line gets its newline again. Lines that
/// do not parse (a torn last line) are skipped.
pub fn parse(line: &[u8]) -> Option<Chunk> {
    let mut parts = line.splitn(4, |b| *b == b' ');
    let _time = parts.next()?;
    let stream = match parts.next()? {
        b"stdout" => Output::Stdout,
        b"stderr" => Output::Stderr,
        _ => return None,
    };
    let full = match parts.next()? {
        b"F" => true,
        b"P" => false,
        _ => return None,
    };
    let mut data = parts.next()?.to_vec();
    if full {
        data.push(b'\n');
    }
    Some(Chunk {
        stream,
        data: Bytes::from(data),
    })
}

/// Reads records from `path` starting at byte `offset`; returns them and
/// the offset after the last whole line.
pub fn read_from(path: &Path, offset: u64) -> std::io::Result<(Vec<Chunk>, u64)> {
    use std::io::{Seek, SeekFrom};
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), offset)),
        Err(e) => return Err(e),
    };
    file.seek(SeekFrom::Start(offset))?;
    let mut reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut at = offset;
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 || line.last() != Some(&b'\n') {
            break;
        }
        at += n as u64;
        line.pop();
        out.extend(parse(&line));
    }
    Ok((out, at))
}

/// Everything kept, oldest first, or the last `tail` lines of it.
pub fn read_all(path: &Path, tail: Option<usize>) -> std::io::Result<Vec<Chunk>> {
    let (mut chunks, _) = read_from(&rotated(path), 0)?;
    chunks.extend(read_from(path, 0)?.0);
    if let Some(n) = tail {
        // Lines end with a full record: keep the pieces of the last n.
        let mut ends = 0;
        let mut start = chunks.len();
        for (i, c) in chunks.iter().enumerate().rev() {
            if c.data.ends_with(b"\n") {
                ends += 1;
                if ends > n {
                    break;
                }
            }
            start = i;
        }
        chunks.drain(..start);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(chunks: &[Chunk]) -> String {
        chunks
            .iter()
            .map(|c| {
                let tag = if c.stream == Output::Stderr { "!" } else { "" };
                format!("{tag}{}", String::from_utf8_lossy(&c.data))
            })
            .collect()
    }

    #[test]
    fn lines_pieces_and_streams() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("log");
        let mut w = Writer::open(&p, 1 << 20).unwrap();
        w.write(Output::Stdout, b"one\ntw").unwrap();
        w.write(Output::Stderr, b"err\n").unwrap();
        w.write(Output::Stdout, b"o\nthree").unwrap();
        w.flush().unwrap();
        assert_eq!(text(&read_all(&p, None).unwrap()), "one\n!err\ntwo\nthree");
        assert_eq!(text(&read_all(&p, Some(1)).unwrap()), "two\nthree");
        assert_eq!(text(&read_all(&p, Some(2)).unwrap()), "!err\ntwo\nthree");
        let long = vec![b'x'; PIECE + 10];
        w.write(Output::Stdout, &long).unwrap();
        w.write(Output::Stdout, b"\n").unwrap();
        // The last line: "three" (flushed unfinished), the long piece, and
        // the newline that ended them.
        let all = read_all(&p, Some(1)).unwrap();
        assert_eq!(
            all.iter().map(|c| c.data.len()).sum::<usize>(),
            5 + PIECE + 10 + 1
        );
    }

    #[test]
    fn rotation_keeps_the_last_file() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("log");
        let mut w = Writer::open(&p, 100).unwrap();
        for i in 0..30 {
            w.write(Output::Stdout, format!("line {i}\n").as_bytes())
                .unwrap();
        }
        let all = text(&read_all(&p, None).unwrap());
        assert!(all.ends_with("line 29\n"), "{all}");
        assert!(!all.contains("line 0\n"), "{all}");
        let size = |p: &Path| std::fs::metadata(p).unwrap().len();
        assert!(size(&p) <= 140 && size(&rotated(&p)) <= 140);
    }

    #[test]
    fn reading_on_from_an_offset() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("log");
        let mut w = Writer::open(&p, 1 << 20).unwrap();
        w.write(Output::Stdout, b"a\n").unwrap();
        let (first, at) = read_from(&p, 0).unwrap();
        assert_eq!(text(&first), "a\n");
        w.write(Output::Stdout, b"b\n").unwrap();
        let (next, _) = read_from(&p, at).unwrap();
        assert_eq!(text(&next), "b\n");
        // A torn line is left for later.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(b"123.0 stdout F half")
            .unwrap();
        let (later, end) = read_from(&p, at).unwrap();
        assert_eq!(text(&later), "b\n");
        assert!(end < std::fs::metadata(&p).unwrap().len());
    }
}
