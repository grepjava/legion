//! An S3-compatible bucket as a backup target (DESIGN.md §4.3, Volume
//! backups): each backup is an object, the whole stream `zfs send`
//! writes, at `<prefix>/<namespace>.<volume>/<snapshot>.zfs`.
//!
//! Requests are signed with AWS Signature Version 4, which every
//! S3-compatible service takes. A backup goes up as a multipart upload,
//! a part at a time (16 MiB, larger once there are many, as S3 allows at
//! most 10,000), so a stream of any length is sent holding one part in
//! memory, and the object appears only once all of it is there. An upload
//! that fails is aborted; one left by an agent that stopped is aborted by
//! the next run. A part that fails on the way (a 5xx, a dropped
//! connection) is sent again, twice at most.

use std::io::{Read, Write};
use std::time::Duration;

use legion_api::backup::S3Target;
use ring::{digest, hmac};

use crate::backups::{Remote, snapshot_of};

/// The SHA-256 of nothing, as S3 wants it for a request without a body.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// How many times a request that may be repeated is tried.
const TRIES: u32 = 3;

pub(crate) struct S3 {
    target: S3Target,
    secret: String,
    /// `<prefix>/<namespace>.<volume>`: its backups' keys are under it.
    base: String,
    /// `https`, and the host (with a port other than the scheme's) the
    /// requests go to and are signed for.
    scheme: String,
    host: String,
    /// The first parts' size; later ones are larger.
    part: usize,
    http: reqwest::Client,
    rt: tokio::runtime::Handle,
}

impl S3 {
    /// The bucket `t` names, with the secret key `secret`, for the Volume
    /// `of` (`<namespace>.<volume>`); its requests run on `rt`.
    pub(crate) fn new(
        t: &S3Target,
        secret: &[u8],
        of: &str,
        rt: tokio::runtime::Handle,
    ) -> Result<S3, String> {
        let secret = String::from_utf8(secret.to_vec())
            .map_err(|_| "the target's Secret is no secret key: not text".to_owned())?
            .trim()
            .to_owned();
        if secret.is_empty() {
            return Err("the target's Secret holds no secret key".to_owned());
        }
        let url = reqwest::Url::parse(&t.endpoint)
            .map_err(|e| format!("the endpoint {}: {e}", t.endpoint))?;
        let mut host = url
            .host_str()
            .ok_or_else(|| format!("the endpoint {} names no host", t.endpoint))?
            .to_owned();
        if let Some(port) = url.port() {
            host = format!("{host}:{port}");
        }
        if t.virtual_hosted {
            host = format!("{}.{host}", t.bucket);
        }
        // reqwest uses the process's rustls provider: ring, as elsewhere.
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| format!("an HTTP client: {e}"))?;
        let base = if t.prefix.is_empty() {
            of.to_owned()
        } else {
            format!("{}/{of}", t.prefix)
        };
        Ok(S3 {
            target: t.clone(),
            secret,
            base,
            scheme: url.scheme().to_owned(),
            host,
            part: 16 << 20,
            http,
            rt,
        })
    }

    /// With first parts of `bytes` (tests send small ones).
    #[cfg(test)]
    fn with_part(mut self, bytes: usize) -> S3 {
        self.part = bytes;
        self
    }

    /// The size of part `n` (from 1): 16 MiB for the first thousand, 64
    /// MiB to the four thousandth, 256 MiB after; about 1.7 TB in all.
    fn part_size(&self, n: u32) -> usize {
        match n {
            0..=1000 => self.part,
            1001..=4000 => self.part * 4,
            _ => self.part * 16,
        }
    }

    /// Whether `held` (an object's key) is one of this Volume's backups.
    fn ours(&self, held: &str) -> bool {
        held.strip_prefix(&self.base)
            .and_then(|r| r.strip_prefix('/'))
            .is_some_and(|name| !name.contains('/') && name.ends_with(".zfs"))
    }

    /// The path a request for `key` (empty: the bucket) goes to.
    fn path(&self, key: &str) -> String {
        match (self.target.virtual_hosted, key.is_empty()) {
            (true, _) => format!("/{}", uri_encode(key, false)),
            (false, true) => format!("/{}", self.target.bucket),
            (false, false) => format!("/{}/{}", self.target.bucket, uri_encode(key, false)),
        }
    }

    /// Sends one request, signed; its status and body. Tried again on a
    /// failure on the way or a 5xx, when `again` says it may be.
    async fn call(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
        again: bool,
    ) -> Result<reqwest::Response, String> {
        let path = self.path(key);
        let query = canonical_query(query);
        let payload = if body.is_empty() {
            EMPTY_SHA256.to_owned()
        } else {
            hex(digest::digest(&digest::SHA256, &body).as_ref())
        };
        let url = if query.is_empty() {
            format!("{}://{}{path}", self.scheme, self.host)
        } else {
            format!("{}://{}{path}?{query}", self.scheme, self.host)
        };
        let what = format!("{method} s3://{}/{key}", self.target.bucket);
        let mut tried = 0;
        loop {
            tried += 1;
            let date = amz_date(std::time::SystemTime::now());
            let auth = authorization(
                &Signer {
                    access_key: &self.target.access_key_id,
                    secret: &self.secret,
                    region: &self.target.region,
                },
                method.as_str(),
                &path,
                &query,
                &[
                    ("host", &self.host),
                    ("x-amz-content-sha256", &payload),
                    ("x-amz-date", &date),
                ],
                &payload,
                &date,
            );
            let sent = self
                .http
                .request(method.clone(), &url)
                .header("x-amz-date", &date)
                .header("x-amz-content-sha256", &payload)
                .header("authorization", auth)
                .body(body.clone())
                .send()
                .await;
            let failed = match sent {
                Ok(r) if r.status().is_success() => return Ok(r),
                Ok(r) => {
                    let status = r.status();
                    let text = r.text().await.unwrap_or_default();
                    let why = format!("{what}: {status}{}", said(&text));
                    if !(status.is_server_error() || status.as_u16() == 429) {
                        return Err(why);
                    }
                    why
                }
                Err(e) => format!("{what}: {e}"),
            };
            if !again || tried >= TRIES {
                return Err(failed);
            }
            tracing::debug!("{failed}; trying again");
            tokio::time::sleep(Duration::from_secs(u64::from(tried))).await;
        }
    }

    /// The keys under `prefix`, every page of them.
    async fn keys(&self, prefix: &str) -> Result<Vec<String>, String> {
        let mut found = Vec::new();
        let mut next = String::new();
        loop {
            let mut q = vec![("list-type", "2"), ("prefix", prefix)];
            if !next.is_empty() {
                q.push(("continuation-token", &next));
            }
            let r = self
                .call(reqwest::Method::GET, "", &q, Vec::new(), true)
                .await?;
            let xml = r.text().await.map_err(|e| format!("listing: {e}"))?;
            found.extend(texts(&xml, "Key"));
            let truncated = texts(&xml, "IsTruncated")
                .first()
                .is_some_and(|t| t == "true");
            next = texts(&xml, "NextContinuationToken")
                .into_iter()
                .next()
                .unwrap_or_default();
            if !truncated || next.is_empty() {
                return Ok(found);
            }
        }
    }

    /// Uploads under `prefix` never finished: their keys and IDs.
    async fn unfinished(&self, prefix: &str) -> Result<Vec<(String, String)>, String> {
        let q = [("uploads", ""), ("prefix", prefix)];
        let r = self
            .call(reqwest::Method::GET, "", &q, Vec::new(), true)
            .await?;
        let xml = r
            .text()
            .await
            .map_err(|e| format!("listing uploads: {e}"))?;
        Ok(blocks(&xml, "Upload")
            .into_iter()
            .filter_map(|u| {
                let key = texts(u, "Key").into_iter().next()?;
                let id = texts(u, "UploadId").into_iter().next()?;
                Some((key, id))
            })
            .collect())
    }

    async fn abort(&self, key: &str, id: &str) -> Result<(), String> {
        self.call(
            reqwest::Method::DELETE,
            key,
            &[("uploadId", id)],
            Vec::new(),
            true,
        )
        .await
        .map(drop)
    }

    /// Sends what `input` gives as `key`, a part at a time.
    async fn upload(&self, key: &str, input: &mut dyn Read) -> Result<(), String> {
        // One left by an agent that stopped goes first: it costs space.
        for (k, id) in self.unfinished(&format!("{}/", self.base)).await? {
            if self.ours(&k) {
                self.abort(&k, &id).await?;
            }
        }
        let r = self
            .call(
                reqwest::Method::POST,
                key,
                &[("uploads", "")],
                Vec::new(),
                false,
            )
            .await?;
        let xml = r
            .text()
            .await
            .map_err(|e| format!("starting the upload: {e}"))?;
        let id = texts(&xml, "UploadId")
            .into_iter()
            .next()
            .ok_or_else(|| format!("starting the upload of {key}: no UploadId{}", said(&xml)))?;
        let sent = self.parts(key, &id, input).await;
        if sent.is_err() {
            let _ = self.abort(key, &id).await;
        }
        sent
    }

    async fn parts(&self, key: &str, id: &str, input: &mut dyn Read) -> Result<(), String> {
        let mut etags = Vec::new();
        let mut n = 1u32;
        loop {
            let size = self.part_size(n);
            let mut buf = vec![0u8; size];
            let filled = fill(input, &mut buf).map_err(|e| format!("reading the stream: {e}"))?;
            buf.truncate(filled);
            if filled == 0 && n > 1 {
                break;
            }
            if n > 10_000 {
                return Err("more than S3's 10,000 parts: the stream is too long".to_owned());
            }
            let number = n.to_string();
            let r = self
                .call(
                    reqwest::Method::PUT,
                    key,
                    &[("partNumber", &number), ("uploadId", id)],
                    buf,
                    true,
                )
                .await?;
            let etag = r
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| format!("part {n} of {key}: no ETag"))?
                .to_owned();
            etags.push(etag);
            n += 1;
            if filled < size {
                break;
            }
        }
        let mut done = String::from("<CompleteMultipartUpload>");
        for (i, etag) in etags.iter().enumerate() {
            done.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                i + 1,
                escape(etag)
            ));
        }
        done.push_str("</CompleteMultipartUpload>");
        let r = self
            .call(
                reqwest::Method::POST,
                key,
                &[("uploadId", id)],
                done.into_bytes(),
                true,
            )
            .await?;
        // S3 can say 200 and fail all the same, in the body.
        let xml = r
            .text()
            .await
            .map_err(|e| format!("finishing {key}: {e}"))?;
        if xml.contains("<Error>") {
            return Err(format!("finishing {key}:{}", said(&xml)));
        }
        Ok(())
    }

    fn block<T>(&self, f: impl std::future::Future<Output = T>) -> T {
        self.rt.block_on(f)
    }

    /// The size of the object `name` under this one's keys, if there.
    pub(crate) fn size_of(&self, name: &str) -> Result<Option<u64>, String> {
        let key = format!("{}/{name}", self.base);
        self.block(async {
            match self
                .call(reqwest::Method::HEAD, &key, &[], Vec::new(), true)
                .await
            {
                Ok(r) => Ok(r.content_length()),
                // A HEAD's 404 has no body to say more.
                Err(e) if e.contains(": 404 ") => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    /// Stores `data` as the object `name` under this one's keys, whole.
    pub(crate) fn put(&self, name: &str, data: Vec<u8>) -> Result<(), String> {
        let key = format!("{}/{name}", self.base);
        self.block(self.call(reqwest::Method::PUT, &key, &[], data, true))
            .map(drop)
    }

    /// The objects under this one's keys: each name, and when it was
    /// stored (Unix ms).
    pub(crate) fn dated(&self) -> Result<Vec<(String, i64)>, String> {
        let prefix = format!("{}/", self.base);
        self.block(async {
            let mut found = Vec::new();
            let mut next = String::new();
            loop {
                let mut q = vec![("list-type", "2"), ("prefix", prefix.as_str())];
                if !next.is_empty() {
                    q.push(("continuation-token", &next));
                }
                let r = self
                    .call(reqwest::Method::GET, "", &q, Vec::new(), true)
                    .await?;
                let xml = r.text().await.map_err(|e| format!("listing: {e}"))?;
                for c in blocks(&xml, "Contents") {
                    let key = texts(c, "Key").into_iter().next().unwrap_or_default();
                    let at = texts(c, "LastModified")
                        .first()
                        .and_then(|t| iso_ms(t))
                        .unwrap_or(0);
                    if let Some(name) = key.strip_prefix(&prefix)
                        && !name.contains('/')
                    {
                        found.push((name.to_owned(), at));
                    }
                }
                let truncated = texts(&xml, "IsTruncated")
                    .first()
                    .is_some_and(|t| t == "true");
                next = texts(&xml, "NextContinuationToken")
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                if !truncated || next.is_empty() {
                    return Ok(found);
                }
            }
        })
    }

    /// The object `name` under this one's keys, whole.
    pub(crate) fn get(&self, name: &str) -> Result<Vec<u8>, String> {
        let key = format!("{}/{name}", self.base);
        self.block(async {
            let r = self
                .call(reqwest::Method::GET, &key, &[], Vec::new(), true)
                .await?;
            r.bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| format!("taking {name}: {e}"))
        })
    }

    /// Removes the object `name` under this one's keys.
    pub(crate) fn remove(&self, name: &str) -> Result<(), String> {
        let key = format!("{}/{name}", self.base);
        self.block(self.call(reqwest::Method::DELETE, &key, &[], Vec::new(), true))
            .map(drop)
    }
}

/// `2026-11-25T04:05:06.000Z` (S3's `LastModified`) in Unix ms.
pub(crate) fn iso_ms(t: &str) -> Option<i64> {
    legion_api::volume::iso_ms(t)
}

impl Remote for S3 {
    fn list(&self) -> Result<Vec<String>, String> {
        let keys = self.block(self.keys(&format!("{}/", self.base)))?;
        let mut found: Vec<String> = keys
            .into_iter()
            .filter(|k| {
                self.ours(k)
                    && snapshot_of(k).starts_with(legion_api::backup::BackupSchedule::PREFIX)
            })
            .collect();
        found.sort_by(|a, b| snapshot_of(a).cmp(snapshot_of(b)));
        Ok(found)
    }
    fn incremental(&self) -> bool {
        false
    }
    fn receive(
        &self,
        snapshot: &str,
        _: Option<&str>,
        input: &mut dyn Read,
    ) -> Result<String, String> {
        let key = format!("{}/{snapshot}.zfs", self.base);
        self.block(self.upload(&key, input))?;
        Ok(key)
    }
    fn destroy(&self, held: &str) -> Result<(), String> {
        if !self.ours(held) {
            return Err(format!("{held} is not one of this Volume's backups"));
        }
        self.block(self.call(reqwest::Method::DELETE, held, &[], Vec::new(), true))
            .map(drop)
    }
    fn send(&self, held: &str, out: &mut dyn Write) -> Result<(), String> {
        if !self.ours(held) {
            return Err(format!("{held} is not one of this Volume's backups"));
        }
        self.block(async {
            let mut r = self
                .call(reqwest::Method::GET, held, &[], Vec::new(), true)
                .await?;
            while let Some(chunk) = r.chunk().await.map_err(|e| format!("taking {held}: {e}"))? {
                out.write_all(&chunk)
                    .map_err(|e| format!("taking {held}: {e}"))?;
            }
            Ok(())
        })
    }
}

/// Reads into `buf` until it is full or the input ends: how much.
fn fill(input: &mut dyn Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut at = 0;
    while at < buf.len() {
        match input.read(&mut buf[at..]) {
            Ok(0) => break,
            Ok(n) => at += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(at)
}

/// Who signs: the access key, its secret, the region.
pub(crate) struct Signer<'a> {
    pub access_key: &'a str,
    pub secret: &'a str,
    pub region: &'a str,
}

/// The `Authorization` header for a request to S3 (Signature Version 4):
/// `path` and `query` as sent (encoded), `headers` those signed (`host`,
/// `x-amz-*`, any other), `payload` the body's SHA-256 in hex, `date` as
/// `x-amz-date` says it.
pub(crate) fn authorization(
    s: &Signer,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
    payload: &str,
    date: &str,
) -> String {
    let mut headers: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    headers.sort();
    let signed = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let request = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed}\n{payload}");
    let day = &date[..8];
    let scope = format!("{day}/{}/s3/aws4_request", s.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, request.as_bytes()).as_ref())
    );
    let mac = |key: &[u8], data: &str| {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data.as_bytes())
    };
    let k = mac(format!("AWS4{}", s.secret).as_bytes(), day);
    let k = mac(k.as_ref(), s.region);
    let k = mac(k.as_ref(), "s3");
    let k = mac(k.as_ref(), "aws4_request");
    let signature = hex(mac(k.as_ref(), &to_sign).as_ref());
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
        s.access_key
    )
}

/// `20130524T000000Z` for `t`.
fn amz_date(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let (y, m, d) = legion_api::clock::civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        s / 3600,
        s / 60 % 60,
        s % 60
    )
}

/// The query as S3 signs and takes it: each name and value encoded,
/// sorted by name.
fn canonical_query(q: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = q
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encodes all but letters, digits and `-_.~` (and `/`, in a
/// path), as Signature Version 4 wants.
fn uri_encode(s: &str, slash_too: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || b == b'/' && !slash_too
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The texts of each `<tag>…</tag>` in `xml`, unescaped.
fn texts(xml: &str, tag: &str) -> Vec<String> {
    blocks(xml, tag).into_iter().map(unescape).collect()
}

/// What each `<tag>…</tag>` in `xml` holds, as it is.
fn blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        found.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    found
}

fn unescape(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// What an S3 error body says, as `: Code: Message`, if it says.
fn said(xml: &str) -> String {
    let code = texts(xml, "Code").into_iter().next();
    let message = texts(xml, "Message").into_iter().next();
    match (code, message) {
        (Some(c), Some(m)) => format!(": {c}: {m}"),
        (Some(c), None) => format!(": {c}"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's own example (Signature Version 4, "GET Object"): the same
    /// request signed the same.
    #[test]
    fn requests_are_signed_as_aws_documents() {
        let auth = authorization(
            &Signer {
                access_key: "AKIAIOSFODNN7EXAMPLE",
                secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                region: "us-east-1",
            },
            "GET",
            "/test.txt",
            "",
            &[
                ("host", "examplebucket.s3.amazonaws.com"),
                ("range", "bytes=0-9"),
                ("x-amz-content-sha256", EMPTY_SHA256),
                ("x-amz-date", "20130524T000000Z"),
            ],
            EMPTY_SHA256,
            "20130524T000000Z",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// A bucket in memory behind a real HTTP server, as S3 answers: it
    /// refuses a request whose signature is not right, lists a page of two
    /// keys at a time, and fails a part when told to.
    mod bucket {
        use std::collections::{BTreeMap, HashMap};
        use std::sync::{Arc, Mutex};

        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper::{Request, Response, StatusCode};

        use super::super::*;

        pub(super) const AKID: &str = "LEGIONTEST";
        pub(super) const SECRET: &str = "not-so-secret";

        #[derive(Default)]
        pub(super) struct State {
            pub objects: BTreeMap<String, Vec<u8>>,
            /// Upload ID: its key and parts by number.
            pub uploads: HashMap<String, (String, BTreeMap<u32, Vec<u8>>)>,
            next: u32,
            /// The part to fail, and whether only once.
            pub fail_part: Option<(u32, bool)>,
            pub requests: u32,
        }

        /// Serves `bucket` on 127.0.0.1 on `rt`: where.
        pub(super) fn serve(
            rt: &tokio::runtime::Runtime,
            bucket: &'static str,
            state: Arc<Mutex<State>>,
        ) -> String {
            let listener = rt
                .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
                .unwrap();
            let at = listener.local_addr().unwrap();
            rt.spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let state = state.clone();
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(move |req| {
                            let state = state.clone();
                            async move {
                                Ok::<_, std::convert::Infallible>(answer(bucket, &state, req).await)
                            }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
            format!("http://{at}")
        }

        fn reply(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
            let mut r = Response::new(Full::new(body.into()));
            *r.status_mut() = status;
            r
        }

        fn error(status: StatusCode, code: &str) -> Response<Full<Bytes>> {
            reply(
                status,
                format!("<Error><Code>{code}</Code><Message>as told</Message></Error>"),
            )
        }

        fn decode(s: &str) -> String {
            let b = s.as_bytes();
            let mut out = Vec::new();
            let mut i = 0;
            while i < b.len() {
                if b[i] == b'%' && i + 2 < b.len() {
                    out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                    i += 3;
                } else {
                    out.push(b[i]);
                    i += 1;
                }
            }
            String::from_utf8(out).unwrap()
        }

        async fn answer(
            bucket: &str,
            state: &Mutex<State>,
            req: Request<hyper::body::Incoming>,
        ) -> Response<Full<Bytes>> {
            let (parts, body) = req.into_parts();
            let body = body.collect().await.unwrap().to_bytes().to_vec();
            let header = |n: &str| {
                parts
                    .headers
                    .get(n)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            // Signed as S3 checks it: the body's hash and the signature.
            let payload = if body.is_empty() {
                EMPTY_SHA256.to_owned()
            } else {
                hex(digest::digest(&digest::SHA256, &body).as_ref())
            };
            let (path, query) = (parts.uri.path(), parts.uri.query().unwrap_or(""));
            let (host, date) = (header("host"), header("x-amz-date"));
            let wanted = authorization(
                &Signer {
                    access_key: AKID,
                    secret: SECRET,
                    region: "us-east-1",
                },
                parts.method.as_str(),
                path,
                query,
                &[
                    ("host", &host),
                    ("x-amz-content-sha256", &payload),
                    ("x-amz-date", &date),
                ],
                &payload,
                &date,
            );
            if header("x-amz-content-sha256") != payload || header("authorization") != wanted {
                return error(StatusCode::FORBIDDEN, "SignatureDoesNotMatch");
            }
            let q: HashMap<String, String> = query
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let (k, v) = p.split_once('=').unwrap_or((p, ""));
                    (decode(k), decode(v))
                })
                .collect();
            let Some(rest) = path.strip_prefix(&format!("/{bucket}")) else {
                return error(StatusCode::NOT_FOUND, "NoSuchBucket");
            };
            let key = decode(rest.trim_start_matches('/'));
            let mut s = state.lock().unwrap();
            s.requests += 1;
            let method = parts.method.as_str();
            match (method, key.is_empty()) {
                ("GET", true) if q.contains_key("uploads") => {
                    let prefix = q.get("prefix").cloned().unwrap_or_default();
                    let listed: String = s
                        .uploads
                        .iter()
                        .filter(|(_, (k, _))| k.starts_with(&prefix))
                        .map(|(id, (k, _))| {
                            format!("<Upload><Key>{k}</Key><UploadId>{id}</UploadId></Upload>")
                        })
                        .collect();
                    reply(
                        StatusCode::OK,
                        format!(
                            "<ListMultipartUploadsResult>{listed}</ListMultipartUploadsResult>"
                        ),
                    )
                }
                ("GET", true) => {
                    let prefix = q.get("prefix").cloned().unwrap_or_default();
                    let from: usize = q
                        .get("continuation-token")
                        .map_or(0, |t| t.parse().unwrap());
                    let keys: Vec<&String> = s
                        .objects
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .collect();
                    let page: String = keys
                        .iter()
                        .skip(from)
                        .take(2)
                        .map(|k| format!("<Contents><Key>{k}</Key></Contents>"))
                        .collect();
                    let more = from + 2 < keys.len();
                    let next = if more {
                        format!(
                            "<NextContinuationToken>{}</NextContinuationToken>",
                            from + 2
                        )
                    } else {
                        String::new()
                    };
                    reply(
                        StatusCode::OK,
                        format!(
                            "<ListBucketResult><IsTruncated>{more}</IsTruncated>{page}{next}</ListBucketResult>"
                        ),
                    )
                }
                ("POST", false) if q.contains_key("uploads") => {
                    s.next += 1;
                    let id = format!("up+{}/=", s.next);
                    s.uploads.insert(id.clone(), (key, BTreeMap::new()));
                    reply(
                        StatusCode::OK,
                        format!(
                            "<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                        ),
                    )
                }
                ("PUT", false) => {
                    let n: u32 = q["partNumber"].parse().unwrap();
                    if let Some((at, once)) = s.fail_part
                        && at == n
                    {
                        if once {
                            s.fail_part = None;
                        }
                        return error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
                    }
                    let Some((_, ps)) = s.uploads.get_mut(&q["uploadId"]) else {
                        return error(StatusCode::NOT_FOUND, "NoSuchUpload");
                    };
                    ps.insert(n, body);
                    let mut r = reply(StatusCode::OK, "");
                    r.headers_mut()
                        .insert("etag", format!("\"etag-{n}\"").parse().unwrap());
                    r
                }
                ("POST", false) => {
                    let Some((k, ps)) = s.uploads.remove(&q["uploadId"]) else {
                        return error(StatusCode::NOT_FOUND, "NoSuchUpload");
                    };
                    let xml = String::from_utf8(body).unwrap();
                    let named: Vec<String> = texts(&xml, "PartNumber");
                    assert_eq!(named.len(), ps.len(), "every part named");
                    let whole: Vec<u8> = ps.into_values().flatten().collect();
                    s.objects.insert(k, whole);
                    reply(StatusCode::OK, "<CompleteMultipartUploadResult/>")
                }
                ("DELETE", false) if q.contains_key("uploadId") => {
                    s.uploads.remove(&q["uploadId"]);
                    reply(StatusCode::NO_CONTENT, "")
                }
                ("DELETE", false) => {
                    s.objects.remove(&key);
                    reply(StatusCode::NO_CONTENT, "")
                }
                ("GET", false) => match s.objects.get(&key) {
                    Some(o) => reply(StatusCode::OK, o.clone()),
                    None => error(StatusCode::NOT_FOUND, "NoSuchKey"),
                },
                _ => error(StatusCode::BAD_REQUEST, "NotImplemented"),
            }
        }
    }

    const DS: &str = "tank/legion/volumes/shop/db";

    fn snap(n: u32) -> String {
        format!("legion-bk-cloud-20261124T{n:02}0000Z")
    }

    fn target(endpoint: &str, key: &str) -> S3Target {
        S3Target {
            endpoint: endpoint.to_owned(),
            region: "us-east-1".into(),
            bucket: "backups".into(),
            prefix: "legion/prod".into(),
            access_key_id: key.into(),
            secret_key_secret: Default::default(),
            virtual_hosted: false,
        }
    }

    /// Backups go up as objects, a part at a time, listed across pages,
    /// pruned to `keep`, and restore; a part that fails once is sent
    /// again; an upload that fails for good leaves nothing there and no
    /// snapshot here; one left unfinished is aborted by the next run.
    #[test]
    fn a_bucket_keeps_each_backup_whole_as_an_object() {
        use std::sync::{Arc, Mutex as StdMutex};

        use legion_sys::{FakeSys, Sys};
        use tokio::sync::Mutex;

        use crate::backups::{restore, run};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let state = Arc::new(StdMutex::new(bucket::State::default()));
        let endpoint = bucket::serve(&rt, "backups", state.clone());
        let s3 = S3::new(
            &target(&endpoint, bucket::AKID),
            format!("{}\n", bucket::SECRET).as_bytes(),
            "shop.db",
            rt.handle().clone(),
        )
        .unwrap()
        .with_part(64);
        let (sys, lock) = (FakeSys::new(), Mutex::new(()));
        sys.zfs_create(DS, &[]).unwrap();
        let data = |n: u8| vec![n; 300];

        for n in 1..=3 {
            sys.write_dataset(DS, &data(n));
            let r = run(&sys, &lock, &s3, DS, &snap(u32::from(n)), "", 2).unwrap();
            assert!(r.whole, "a bucket holds no increments");
        }
        let held = s3.list().unwrap();
        assert_eq!(
            held,
            [
                format!("legion/prod/shop.db/{}.zfs", snap(2)),
                format!("legion/prod/shop.db/{}.zfs", snap(3)),
            ],
            "pruned to keep, its own"
        );
        let other = FakeSys::new();
        restore(&other, &lock, &s3, DS, &held[0]).unwrap();
        assert_eq!(other.dataset_contents(DS).unwrap(), data(2));

        // Listed across pages: more than one page of them.
        for n in 4..=6 {
            sys.write_dataset(DS, &data(n));
            run(&sys, &lock, &s3, DS, &snap(u32::from(n)), "", 5).unwrap();
        }
        assert_eq!(s3.list().unwrap().len(), 5);

        // A part failing once is sent again.
        state.lock().unwrap().fail_part = Some((2, true));
        sys.write_dataset(DS, &data(7));
        run(&sys, &lock, &s3, DS, &snap(7), "", 5).unwrap();
        assert!(s3.list().unwrap().iter().any(|h| h.contains(&snap(7))));

        // Failing for good: no object, no upload, no snapshot here.
        state.lock().unwrap().fail_part = Some((2, false));
        sys.write_dataset(DS, &data(8));
        let e = run(&sys, &lock, &s3, DS, &snap(8), "", 5).unwrap_err();
        assert!(e.contains("500"), "{e}");
        assert!(!s3.list().unwrap().iter().any(|h| h.contains(&snap(8))));
        assert!(state.lock().unwrap().uploads.is_empty(), "aborted");
        assert!(!sys.zfs_snapshots(DS).unwrap().contains(&snap(8)));

        // One left unfinished (an agent that stopped) goes at the next run;
        // another Volume's is not touched.
        {
            let mut s = state.lock().unwrap();
            s.fail_part = None;
            for (id, key) in [
                ("left", format!("legion/prod/shop.db/{}.zfs", snap(8))),
                ("other", "legion/prod/shop.web/x.zfs".to_owned()),
            ] {
                s.uploads.insert(id.to_owned(), (key, Default::default()));
            }
        }
        sys.write_dataset(DS, &data(9));
        run(&sys, &lock, &s3, DS, &snap(9), "", 5).unwrap();
        let left: Vec<String> = state.lock().unwrap().uploads.keys().cloned().collect();
        assert_eq!(left, ["other"]);

        // Nothing of another Volume's is reached through this one.
        assert!(s3.destroy("legion/prod/shop.web/x.zfs").is_err());
        assert!(
            s3.send("legion/prod/shop.db/../shop.web/x.zfs", &mut Vec::new())
                .is_err()
        );

        // A wrong secret is refused, and says so.
        let wrong = S3::new(
            &target(&endpoint, bucket::AKID),
            b"wrong",
            "shop.db",
            rt.handle().clone(),
        )
        .unwrap();
        let e = wrong.list().unwrap_err();
        assert!(
            e.contains("403") && e.contains("SignatureDoesNotMatch"),
            "{e}"
        );
    }

    #[test]
    fn dates_queries_and_xml_are_as_s3_has_them() {
        let t = std::time::UNIX_EPOCH + Duration::from_secs(1_369_353_600 + 3661);
        assert_eq!(amz_date(t), "20130524T010101Z");
        assert_eq!(
            canonical_query(&[("uploads", ""), ("prefix", "legion/shop.db/")]),
            "prefix=legion%2Fshop.db%2F&uploads="
        );
        assert_eq!(uri_encode("a b/c+d~", false), "a%20b/c%2Bd~");
        let xml = "<R><Upload><Key>a&amp;b</Key><UploadId>1</UploadId></Upload>\
                   <Upload><Key>c</Key><UploadId>2</UploadId></Upload></R>";
        assert_eq!(texts(xml, "Key"), ["a&b", "c"]);
        assert_eq!(blocks(xml, "Upload").len(), 2);
        assert_eq!(
            said("<Error><Code>NoSuchBucket</Code><Message>gone</Message></Error>"),
            ": NoSuchBucket: gone"
        );
    }
}
