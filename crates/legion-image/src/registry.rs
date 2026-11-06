//! A client for registries speaking the OCI distribution API: pulling and
//! pushing manifests and blobs, anonymous or with a token from the
//! registry's auth service.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, LOCATION, WWW_AUTHENTICATE,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::oci::{self, Descriptor, Document, ImageConfig, Manifest, Platform};
use crate::{Error, Reference};

fn token_key(
    r: &Reference,
    creds: Option<&Credentials>,
    push: bool,
) -> (String, String, String, bool) {
    let account = creds.map_or_else(String::new, |c| {
        sha256(format!("{}\0{}", c.username, c.password).as_bytes())
    });
    (r.registry.clone(), r.repository.clone(), account, push)
}

/// A registry account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

pub struct Client {
    http: reqwest::Client,
    /// Registries reached over plain HTTP (tests, and registries an
    /// operator marks insecure).
    plain: BTreeSet<String>,
    /// Bearer tokens by registry, repository, account and push scope: a
    /// token one account got must not serve another, or a pull without
    /// credentials.
    tokens: Mutex<BTreeMap<(String, String, String, bool), String>>,
}

impl Client {
    pub fn new() -> Result<Client, Error> {
        // reqwest uses the process's rustls provider: ring, as elsewhere.
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(std::time::Duration::from_secs(30))
            // A registry gone quiet, mid-answer.
            .read_timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|e| Error::Registry(format!("making the HTTP client: {e}")))?;
        Ok(Client {
            http,
            plain: BTreeSet::new(),
            tokens: Mutex::new(BTreeMap::new()),
        })
    }

    /// Talks to `registry` (`host[:port]`) over plain HTTP.
    pub fn plain_http(mut self, registry: &str) -> Client {
        self.plain.insert(registry.to_ascii_lowercase());
        self
    }

    fn scheme(&self, r: &Reference) -> &'static str {
        if self.plain.contains(&r.registry) {
            "http"
        } else {
            "https"
        }
    }

    fn url(&self, r: &Reference, path: &str) -> String {
        format!(
            "{}://{}/v2/{}/{path}",
            self.scheme(r),
            r.host(),
            r.repository
        )
    }

    fn resolve_location(&self, r: &Reference, location: &str) -> String {
        if location.starts_with("http://") || location.starts_with("https://") {
            location.to_owned()
        } else if location.starts_with('/') {
            format!("{}://{}{location}", self.scheme(r), r.host())
        } else {
            self.url(r, location)
        }
    }

    /// Whether `url` is on the registry itself (same scheme, host and
    /// port), where its credentials may go.
    fn own_origin(&self, r: &Reference, url: &str) -> bool {
        let origin = format!("{}://{}", self.scheme(r), r.host());
        url.to_ascii_lowercase()
            .strip_prefix(&origin)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '?']))
    }

    /// GET, answering one authentication challenge.
    async fn get(
        &self,
        r: &Reference,
        path: &str,
        accept: &[&str],
        creds: Option<&Credentials>,
    ) -> Result<reqwest::Response, Error> {
        let url = self.url(r, path);
        let key = token_key(r, creds, false);
        let send = |auth: Option<String>| {
            let mut req = self.http.get(&url);
            for a in accept {
                req = req.header(ACCEPT, *a);
            }
            if let Some(auth) = auth {
                req = req.header(AUTHORIZATION, auth);
            }
            req.send()
        };
        let cached = self.tokens.lock().expect("tokens").get(&key).cloned();
        let mut resp = send(cached.map(|t| format!("Bearer {t}")))
            .await
            .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            let challenge = resp
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let auth = self.answer(r, &challenge, creds, false).await?;
            resp = send(Some(auth))
                .await
                .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        }
        match resp.status() {
            s if s.is_success() => Ok(resp),
            StatusCode::NOT_FOUND => Err(Error::NotFound(format!("{r}: {path} not found"))),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(denied(r, creds)),
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Registry(format!(
                    "{url}: {s}: {}",
                    body.chars().take(200).collect::<String>()
                )))
            }
        }
    }

    /// Checks whether blob `digest` already exists in `r`'s repository.
    pub async fn blob_exists(
        &self,
        r: &Reference,
        digest: &str,
        creds: Option<&Credentials>,
    ) -> Result<bool, Error> {
        let url = self.url(r, &format!("blobs/{digest}"));
        let key = token_key(r, creds, true);
        let send = |auth: Option<String>| {
            let mut req = self.http.head(&url);
            if let Some(auth) = auth {
                req = req.header(AUTHORIZATION, auth);
            }
            req.send()
        };
        let cached = self.tokens.lock().expect("tokens").get(&key).cloned();
        let mut resp = send(cached.map(|t| format!("Bearer {t}")))
            .await
            .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            let challenge = resp
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let auth = self.answer(r, &challenge, creds, true).await?;
            resp = send(Some(auth))
                .await
                .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        }
        match resp.status() {
            s if s.is_success() => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(denied(r, creds)),
            s => Err(Error::Registry(format!("{url}: {s}"))),
        }
    }

    /// Uploads a blob from `file_path` with expected `digest` to `r` if not
    /// already present. Returns `true` if uploaded, `false` if already present.
    pub async fn push_blob(
        &self,
        r: &Reference,
        digest: &str,
        file_path: &Path,
        creds: Option<&Credentials>,
    ) -> Result<bool, Error> {
        if self.blob_exists(r, digest, creds).await? {
            return Ok(false);
        }
        let bytes = tokio::fs::read(file_path).await?;
        let got = sha256(&bytes);
        if got != digest {
            return Err(Error::Digest {
                what: format!("local blob {}", file_path.display()),
                expected: digest.to_owned(),
                got,
            });
        }
        let start_url = self.url(r, "blobs/uploads/");
        let key = token_key(r, creds, true);
        let mut auth_header = self
            .tokens
            .lock()
            .expect("tokens")
            .get(&key)
            .cloned()
            .map(|t| format!("Bearer {t}"));

        let start_post = |auth: Option<String>| {
            let mut req = self.http.post(&start_url).header(CONTENT_LENGTH, 0);
            if let Some(a) = auth {
                req = req.header(AUTHORIZATION, a);
            }
            req.send()
        };

        let mut resp = start_post(auth_header.clone())
            .await
            .map_err(|e| Error::Registry(format!("{start_url}: {e}")))?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            let challenge = resp
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let auth = self.answer(r, &challenge, creds, true).await?;
            auth_header = Some(auth.clone());
            resp = start_post(Some(auth))
                .await
                .map_err(|e| Error::Registry(format!("{start_url}: {e}")))?;
        }
        if !resp.status().is_success() && resp.status() != StatusCode::ACCEPTED {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Registry(format!(
                "starting blob upload at {start_url}: {status}: {}",
                body.chars().take(200).collect::<String>()
            )));
        }
        let location = resp
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                Error::Registry(format!(
                    "{start_url}: upload response had no Location header"
                ))
            })?;
        let upload_base = self.resolve_location(r, location);
        let sep = if upload_base.contains('?') { '&' } else { '?' };
        let put_url = format!("{upload_base}{sep}digest={digest}");

        let mut put_req = self
            .http
            .put(&put_url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, bytes.len())
            .body(bytes);
        // An upload elsewhere (object storage, say) is authorised by its
        // URL: the registry's credentials stay with the registry.
        if let Some(a) = auth_header.filter(|_| self.own_origin(r, &put_url)) {
            put_req = put_req.header(AUTHORIZATION, a);
        }
        let put_resp = put_req
            .send()
            .await
            .map_err(|e| Error::Registry(format!("{put_url}: {e}")))?;
        if !put_resp.status().is_success() {
            let status = put_resp.status();
            let body = put_resp.text().await.unwrap_or_default();
            return Err(Error::Registry(format!(
                "uploading blob {digest} to {put_url}: {status}: {}",
                body.chars().take(200).collect::<String>()
            )));
        }
        Ok(true)
    }

    /// Pushes `manifest` to `r` (`PUT /v2/<repo>/manifests/<target>`) and
    /// returns the manifest's `sha256:...` digest.
    pub async fn push_manifest(
        &self,
        r: &Reference,
        manifest: &Manifest,
        creds: Option<&Credentials>,
    ) -> Result<String, Error> {
        let content_type = if manifest.media_type.is_empty() {
            oci::OCI_MANIFEST
        } else {
            &manifest.media_type
        };
        self.push_document(r, serde_json::to_vec_pretty(manifest)?, content_type, creds)
            .await
    }

    /// Pushes a manifest or index, already serialized, to `r` and returns
    /// its digest.
    pub async fn push_document(
        &self,
        r: &Reference,
        bytes: Vec<u8>,
        content_type: &str,
        creds: Option<&Credentials>,
    ) -> Result<String, Error> {
        let digest = sha256(&bytes);
        let target = r.target();
        let url = self.url(r, &format!("manifests/{target}"));
        let key = token_key(r, creds, true);
        let send = |auth: Option<String>| {
            let mut req = self
                .http
                .put(&url)
                .header(CONTENT_TYPE, content_type)
                .body(bytes.clone());
            if let Some(a) = auth {
                req = req.header(AUTHORIZATION, a);
            }
            req.send()
        };
        let cached = self.tokens.lock().expect("tokens").get(&key).cloned();
        let mut resp = send(cached.map(|t| format!("Bearer {t}")))
            .await
            .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        if resp.status() == StatusCode::UNAUTHORIZED {
            let challenge = resp
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let auth = self.answer(r, &challenge, creds, true).await?;
            resp = send(Some(auth))
                .await
                .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        }
        match resp.status() {
            s if s.is_success() => Ok(digest),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(denied(r, creds)),
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Registry(format!(
                    "{url}: {s}: {}",
                    body.chars().take(200).collect::<String>()
                )))
            }
        }
    }

    /// Refuses a realm `registry` sends logins to, unless it is https, or
    /// plain HTTP for a registry spoken to in the clear.
    pub fn check_realm(&self, registry: &str, realm: &str) -> Result<(), Error> {
        let plain = realm.starts_with("http://") && self.plain.contains(registry);
        if realm.starts_with("https://") || plain {
            return Ok(());
        }
        Err(Error::Registry(format!(
            "{registry}: the registry sends logins to {realm}, which is not https"
        )))
    }

    /// Checks `creds` with `registry` (`docker.io`, `host:port`, or an
    /// image reference's registry) as `docker login` does, without a
    /// repository. A registry that asks for no login takes any.
    pub async fn check_login(&self, registry: &str, creds: &Credentials) -> Result<(), Error> {
        let name = crate::registry_of(registry);
        let r = Reference {
            registry: name.clone(),
            ..Reference::parse(&format!("{name}/login"))?
        };
        let fail = |e: String| Error::Registry(format!("{name}: {e}"));
        let root = format!("{}://{}/v2/", self.scheme(&r), r.host());
        let resp = self
            .http
            .get(&root)
            .send()
            .await
            .map_err(|e| fail(e.to_string()))?;
        if resp.status().is_success() {
            return Ok(());
        }
        if resp.status() != StatusCode::UNAUTHORIZED {
            return Err(fail(format!("HTTP {} from {root}", resp.status())));
        }
        let challenge = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let (kind, rest) = challenge.split_once(' ').unwrap_or((&challenge, ""));
        let req = if kind.eq_ignore_ascii_case("basic") {
            self.http.get(&root)
        } else if kind.eq_ignore_ascii_case("bearer") {
            let params = parse_challenge(rest);
            let realm = params
                .get("realm")
                .ok_or_else(|| fail(format!("auth challenge without realm: {challenge}")))?;
            self.check_realm(&name, realm)?;
            let mut query = vec![("account", creds.username.as_str())];
            if let Some(service) = params.get("service") {
                query.push(("service", service));
            }
            self.http.get(realm).query(&query)
        } else {
            return Err(fail(format!("unknown auth challenge: {challenge}")));
        };
        let resp = req
            .basic_auth(&creds.username, Some(&creds.password))
            .send()
            .await
            .map_err(|e| fail(e.to_string()))?;
        match resp.status() {
            s if s.is_success() => Ok(()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(fail(format!(
                "login refused for {}: wrong username, password or token",
                creds.username
            ))),
            s => Err(fail(format!("HTTP {s} logging in"))),
        }
    }

    /// The `Authorization` value answering a `WWW-Authenticate` challenge.
    async fn answer(
        &self,
        r: &Reference,
        challenge: &str,
        creds: Option<&Credentials>,
        push: bool,
    ) -> Result<String, Error> {
        let basic = creds.map(|c| {
            let pair = format!("{}:{}", c.username, c.password);
            format!("Basic {}", STANDARD.encode(pair))
        });
        let rest = match challenge
            .strip_prefix("Bearer ")
            .or_else(|| challenge.strip_prefix("bearer "))
        {
            Some(rest) => rest,
            None => {
                return basic.ok_or_else(|| denied(r, None));
            }
        };
        let params = parse_challenge(rest);
        let Some(realm) = params.get("realm") else {
            return Err(Error::Registry(format!(
                "{r}: auth challenge without realm: {challenge}"
            )));
        };
        self.check_realm(&r.registry, realm)?;
        let scope = scope(r, params.get("scope").map(String::as_str), push);
        let mut req = self.http.get(realm).query(&[("scope", scope.as_str())]);
        if let Some(service) = params.get("service") {
            req = req.query(&[("service", service.as_str())]);
        }
        if let Some(basic) = basic {
            req = req.header(AUTHORIZATION, basic);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Registry(format!("{realm}: {e}")))?;
        if !resp.status().is_success() {
            return Err(Error::Registry(format!(
                "{r}: the registry's auth service answered {}",
                resp.status()
            )));
        }
        #[derive(serde::Deserialize)]
        struct Token {
            #[serde(default)]
            token: String,
            #[serde(default)]
            access_token: String,
        }
        let t: Token = resp
            .json()
            .await
            .map_err(|e| Error::Registry(format!("{realm}: {e}")))?;
        let token = if t.token.is_empty() {
            t.access_token
        } else {
            t.token
        };
        self.tokens
            .lock()
            .expect("tokens")
            .insert(token_key(r, creds, push), token.clone());
        Ok(format!("Bearer {token}"))
    }

    /// The manifest for this node's architecture and the first of `oses`
    /// the image is offered for, and its digest.
    pub async fn manifest(
        &self,
        r: &Reference,
        creds: Option<&Credentials>,
        oses: &[&str],
    ) -> Result<(String, Manifest), Error> {
        let (mut digest, mut doc) = self.document(r, r.target(), creds).await?;
        // An index, possibly of indexes, down to a manifest.
        for _ in 0..oci::INDEX_DEPTH {
            let Some(manifests) = &doc.manifests else {
                return Ok((digest, doc.into_manifest()?));
            };
            let chosen = oci::choose(manifests, oses)?.digest.clone();
            (digest, doc) = self.document(r, &chosen, creds).await?;
        }
        match doc.manifests {
            None => Ok((digest, doc.into_manifest()?)),
            Some(_) => Err(Error::Unsupported("indexes nested too deep".to_owned())),
        }
    }

    /// The manifest or index `target` names in `r`'s repository, and its
    /// digest.
    pub async fn document(
        &self,
        r: &Reference,
        target: &str,
        creds: Option<&Credentials>,
    ) -> Result<(String, Document), Error> {
        let (digest, bytes) = self.document_bytes(r, target, creds).await?;
        Ok((digest, Document::parse(&bytes)?))
    }

    /// Index entries for `r`: an index's own entries, or one entry for a
    /// single manifest, its platform read from the image config.
    pub async fn describe(
        &self,
        r: &Reference,
        creds: Option<&Credentials>,
    ) -> Result<Vec<Descriptor>, Error> {
        let (digest, bytes) = self.document_bytes(r, r.target(), creds).await?;
        let doc = Document::parse(&bytes)?;
        if let Some(manifests) = doc.manifests {
            return Ok(manifests);
        }
        let media_type = if doc.media_type.is_empty() {
            oci::OCI_MANIFEST.to_owned()
        } else {
            doc.media_type.clone()
        };
        let manifest = doc.into_manifest()?;
        let tmp = tempfile::NamedTempFile::new()?;
        self.blob(
            r,
            &manifest.config.digest,
            manifest.config.size,
            tmp.path(),
            creds,
        )
        .await?;
        let config: ImageConfig = serde_json::from_slice(&std::fs::read(tmp.path())?)
            .map_err(|e| Error::Unsupported(format!("{r}: the image config: {e}")))?;
        Ok(vec![Descriptor {
            media_type,
            digest,
            size: bytes.len() as u64,
            platform: Some(Platform {
                architecture: config.architecture,
                os: config.os,
            }),
            annotations: BTreeMap::new(),
        }])
    }

    async fn document_bytes(
        &self,
        r: &Reference,
        target: &str,
        creds: Option<&Credentials>,
    ) -> Result<(String, Vec<u8>), Error> {
        let resp = self
            .get(
                r,
                &format!("manifests/{target}"),
                &oci::MANIFEST_TYPES,
                creds,
            )
            .await?;
        let mut bytes = Vec::new();
        let mut body = resp.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Registry(format!("{r}: {e}")))?;
            if bytes.len() + chunk.len() > MAX_MANIFEST {
                return Err(Error::Registry(format!(
                    "{r}: a manifest over {} MiB",
                    MAX_MANIFEST >> 20
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let digest = sha256(&bytes);
        if target.starts_with("sha256:") && digest != target {
            return Err(Error::Digest {
                what: format!("{r} manifest"),
                expected: target.to_owned(),
                got: digest,
            });
        }
        Ok((digest, bytes))
    }

    /// Writes the blob `digest` to `dest`, checking it. A blob longer than
    /// `size`, its descriptor's, is cut off (0: not known).
    pub async fn blob(
        &self,
        r: &Reference,
        digest: &str,
        size: u64,
        dest: &Path,
        creds: Option<&Credentials>,
    ) -> Result<(), Error> {
        let resp = self.get(r, &format!("blobs/{digest}"), &[], creds).await?;
        let mut file = tokio::fs::File::create(dest).await?;
        let mut hash = Sha256::new();
        let mut body = resp.bytes_stream();
        let mut written = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Registry(format!("{r}: {digest}: {e}")))?;
            written += chunk.len() as u64;
            if size > 0 && written > size {
                return Err(Error::Registry(format!(
                    "{r}: {digest}: longer than its {size} bytes"
                )));
            }
            hash.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        let got = format!("sha256:{:x}", hash.finalize());
        if got != digest {
            return Err(Error::Digest {
                what: format!("{r} blob"),
                expected: digest.to_owned(),
                got,
            });
        }
        Ok(())
    }
}

/// The largest manifest or index taken, as most registries limit them.
const MAX_MANIFEST: usize = 4 << 20;

pub fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// The error for a 401 or 403, which Docker Hub also gives for a
/// repository that does not exist.
fn denied(r: &Reference, creds: Option<&Credentials>) -> Error {
    Error::Registry(match creds {
        None => format!("{r}: not found, or access denied (no credentials given)"),
        Some(c) => format!("{r}: not found, or access denied to {}", c.username),
    })
}

/// The scope a token is asked for: the challenge's for a pull, but always
/// `pull,push` for a push, since its token is cached for the uploads after
/// it and a `HEAD` on a blob is challenged for `pull` only.
fn scope(r: &Reference, challenged: Option<&str>, push: bool) -> String {
    match challenged {
        _ if push => format!("repository:{}:pull,push", r.repository),
        Some(scope) => scope.to_owned(),
        None => format!("repository:{}:pull", r.repository),
    }
}

/// `realm="...",service="...",scope="..."`.
fn parse_challenge(s: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = s.trim();
    while let Some((key, after)) = rest.split_once('=') {
        let key = key
            .trim()
            .trim_start_matches(',')
            .trim()
            .to_ascii_lowercase();
        let (value, next) = if let Some(quoted) = after.strip_prefix('"') {
            match quoted.find('"') {
                Some(end) => (&quoted[..end], &quoted[end + 1..]),
                None => (quoted, ""),
            }
        } else {
            match after.find(',') {
                Some(end) => (&after[..end], &after[end..]),
                None => (after, ""),
            }
        };
        out.insert(key, value.to_owned());
        rest = next.trim_start_matches(',').trim();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenges() {
        let p = parse_challenge(
            r#"realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:freebsd/x:pull""#,
        );
        assert_eq!(p["realm"], "https://auth.docker.io/token");
        assert_eq!(p["service"], "registry.docker.io");
        assert_eq!(p["scope"], "repository:freebsd/x:pull");
        let p = parse_challenge(r#"realm=https://r/t, service="a,b""#);
        assert_eq!(p["realm"], "https://r/t");
        assert_eq!(p["service"], "a,b");
    }

    #[test]
    fn a_push_asks_to_push_whatever_the_challenge() {
        let r = Reference::parse("r.example/me/app:1").unwrap();
        let pull = Some("repository:me/app:pull");
        assert_eq!(scope(&r, pull, true), "repository:me/app:pull,push");
        assert_eq!(scope(&r, None, true), "repository:me/app:pull,push");
        assert_eq!(scope(&r, pull, false), "repository:me/app:pull");
        assert_eq!(scope(&r, None, false), "repository:me/app:pull");
    }

    #[test]
    fn credentials_stay_with_the_registry() {
        let c = Client::new().unwrap();
        let r = Reference::parse("r.example:5000/me/app:1").unwrap();
        for own in [
            "https://r.example:5000/v2/me/app/blobs/uploads/1?x=y",
            "HTTPS://R.example:5000/v2/",
            "https://r.example:5000",
        ] {
            assert!(c.own_origin(&r, own), "{own}");
        }
        for other in [
            "http://r.example:5000/v2/me/app/blobs/uploads/1",
            "https://r.example/v2/",
            "https://r.example:50001/v2/",
            "https://r.example:5000.evil/v2/",
            "https://bucket.s3.example/upload?sig=1",
        ] {
            assert!(!c.own_origin(&r, other), "{other}");
        }
        let c = c.plain_http("r.example:5000");
        assert!(c.own_origin(&r, "http://r.example:5000/v2/"));
        assert!(!c.own_origin(&r, "https://r.example:5000/v2/"));
    }
}
