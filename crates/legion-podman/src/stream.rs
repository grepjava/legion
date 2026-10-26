//! Decoding of streamed response bodies: newline-delimited JSON (events,
//! pull progress) and Docker's multiplexed stdout/stderr framing (logs,
//! exec).

use bytes::{Buf, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use serde::de::DeserializeOwned;

use crate::Error;

/// The data frames of a body.
pub(crate) fn chunks(body: Incoming) -> impl Stream<Item = Result<Bytes, Error>> + Send {
    http_body_util::BodyStream::new(body).filter_map(|frame| async move {
        match frame {
            Ok(frame) => frame.into_data().ok().map(Ok),
            Err(e) => Some(Err(Error::Http(e))),
        }
    })
}

/// Splits a byte stream into lines and parses each as `T`. Blank lines are
/// skipped; a final line without a newline still counts.
pub(crate) fn json_lines<T, S>(input: S) -> impl Stream<Item = Result<T, Error>> + Send
where
    T: DeserializeOwned + Send + 'static,
    S: Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'static,
{
    futures::stream::unfold(
        (input, BytesMut::new(), false),
        |(mut input, mut buf, mut done)| async move {
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line = buf.split_to(pos + 1);
                    let line = line[..pos].trim_ascii();
                    if line.is_empty() {
                        continue;
                    }
                    let item = serde_json::from_slice(line).map_err(Error::Decode);
                    return Some((item, (input, buf, done)));
                }
                if done {
                    let rest = buf.split().freeze();
                    let rest = rest.trim_ascii();
                    if rest.is_empty() {
                        return None;
                    }
                    let item = serde_json::from_slice(rest).map_err(Error::Decode);
                    return Some((item, (input, buf, done)));
                }
                match input.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => return Some((Err(e), (input, buf, true))),
                    None => done = true,
                }
            }
        },
    )
}

/// Which stream a piece of output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Stdout,
    Stderr,
}

/// A piece of container output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub stream: Output,
    pub data: Bytes,
}

/// Header of a multiplexed frame: stream type, three zero bytes, then the
/// payload length as a big-endian u32.
const HEADER: usize = 8;

/// Decodes the multiplexed framing into chunks. Stdin echoes (type 0) are
/// reported as stdout.
pub(crate) fn demux<S>(input: S) -> impl Stream<Item = Result<Chunk, Error>> + Send
where
    S: Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'static,
{
    futures::stream::unfold(
        (input, BytesMut::new(), false),
        |(mut input, mut buf, mut done)| async move {
            loop {
                if buf.len() >= HEADER {
                    let len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
                    if buf.len() >= HEADER + len {
                        let stream = if buf[0] == 2 {
                            Output::Stderr
                        } else {
                            Output::Stdout
                        };
                        buf.advance(HEADER);
                        let data = buf.split_to(len).freeze();
                        return Some((Ok(Chunk { stream, data }), (input, buf, done)));
                    }
                }
                if done {
                    if buf.is_empty() {
                        return None;
                    }
                    buf.clear();
                    let err = Error::Protocol("output stream ended inside a frame".into());
                    return Some((Err(err), (input, buf, done)));
                }
                match input.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => return Some((Err(e), (input, buf, true))),
                    None => done = true,
                }
            }
        },
    )
}

/// Reads a whole body.
pub(crate) async fn collect(body: Incoming) -> Result<Bytes, Error> {
    Ok(body.collect().await.map_err(Error::Http)?.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(parts: &[&[u8]]) -> impl Stream<Item = Result<Bytes, Error>> + Send + Unpin + 'static {
        let parts: Vec<_> = parts
            .iter()
            .map(|p| Ok(Bytes::copy_from_slice(p)))
            .collect();
        futures::stream::iter(parts)
    }

    #[tokio::test]
    async fn json_lines_across_chunks() {
        let lines: Vec<serde_json::Value> =
            json_lines(input(&[b"{\"a\":1}\n{\"a\"", b":2}\n\n", b"{\"a\":3}"]))
                .map(Result::unwrap)
                .collect()
                .await;
        assert_eq!(lines, [1, 2, 3].map(|a| serde_json::json!({ "a": a })));
    }

    #[tokio::test]
    async fn demux_across_chunks() {
        // As captured from libpod: "out\n" on stdout, "err\n" on stderr.
        let raw: &[u8] = b"\x01\0\0\0\0\0\0\x04out\n\x02\0\0\0\0\0\0\x04err\n";
        let chunks: Vec<Chunk> = demux(input(&[&raw[..3], &raw[3..10], &raw[10..]]))
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(
            chunks,
            [
                Chunk {
                    stream: Output::Stdout,
                    data: Bytes::from_static(b"out\n")
                },
                Chunk {
                    stream: Output::Stderr,
                    data: Bytes::from_static(b"err\n")
                },
            ]
        );
    }

    #[tokio::test]
    async fn demux_truncated_frame() {
        let mut s = Box::pin(demux(input(&[b"\x01\0\0\0\0\0\0\x09short"])));
        assert!(matches!(s.next().await, Some(Err(Error::Protocol(_)))));
        assert!(s.next().await.is_none());
    }
}
