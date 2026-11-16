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

impl Credentials {
    /// The first of `logins` for `registry`, as a [`Reference`] names it.
    pub fn for_registry(logins: &[legion_api::login::Login], registry: &str) -> Option<Self> {
        logins
            .iter()
            .find(|l| crate::registry_of(&l.registry) == registry)
            .map(|l| Credentials {
                username: l.username.clone(),
                password: l.password.clone(),
            })
    }
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
    /// Where searches go.
    search_at: SearchEndpoints,
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
            search_at: SearchEndpoints::default(),
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

    /// The manifest [`Client::manifest`] chooses, its digest, and the image
    /// config it names.
    pub async fn config(
        &self,
        r: &Reference,
        creds: Option<&Credentials>,
        oses: &[&str],
    ) -> Result<(String, Manifest, ImageConfig), Error> {
        let (digest, manifest) = self.manifest(r, creds, oses).await?;
        if manifest.config.size > MAX_MANIFEST as u64 {
            return Err(Error::Registry(format!(
                "{r}: an image config over {} MiB",
                MAX_MANIFEST >> 20
            )));
        }
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
        Ok((digest, manifest, config))
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

    /// Lists tags in `r`'s repository (`GET /v2/<repo>/tags/list`), sorted.
    pub async fn list_tags(
        &self,
        r: &Reference,
        creds: Option<&Credentials>,
    ) -> Result<Vec<String>, Error> {
        #[derive(serde::Deserialize)]
        struct TagsList {
            #[serde(default)]
            tags: Option<Vec<String>>,
        }
        let resp = self
            .get(r, "tags/list?n=100", &["application/json"], creds)
            .await?;
        let parsed: TagsList = resp
            .json()
            .await
            .map_err(|e| Error::Registry(format!("{r}: tags/list: {e}")))?;
        let mut tags = parsed.tags.unwrap_or_default();
        tags.sort();
        Ok(tags)
    }

    /// Searches the curated FreeBSD jail image catalog and Docker Hub for
    /// `query`, returning up to `limit` hits. If Docker Hub cannot be reached
    /// (for example in an air-gapped cluster), returns the matching curated
    /// FreeBSD catalog hits.
    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<RegistryHit>, Error> {
        self.search_with_ghcr(query, limit, None).await
    }

    /// [`Client::search`], and GitHub Container Registry:
    ///
    /// - `ghcr.io/owner/repo` (the prefix in any case) looks the repository
    ///   up in the registry itself, anonymously as a pull is, so a public
    ///   image is found without a token; with `ghcr_token` (a GitHub token
    ///   with `read:packages`) the owner's packages matching `repo` are
    ///   listed too.
    /// - `ghcr.io/owner` lists the owner's packages, and `ghcr.io/` the
    ///   token's own: GitHub's packages API answers only with a token.
    /// - Any other query, with `ghcr_token`, also lists the token's own
    ///   packages matching it. Without one it asks ghcr.io nothing, so a
    ///   cluster that cannot reach it waits no longer than before.
    pub async fn search_with_ghcr(
        &self,
        query: &str,
        limit: usize,
        ghcr_token: Option<&str>,
    ) -> Result<Vec<RegistryHit>, Error> {
        let limit = limit.clamp(1, 50);
        let trimmed = query.trim();
        let q = trimmed.to_ascii_lowercase();
        let token = ghcr_token.filter(|t| !t.is_empty());

        if let Some(g) = GhcrQuery::parse(trimmed) {
            return self.search_ghcr(&g, limit, token).await;
        }

        let mut out: Vec<RegistryHit> = FREEBSD_CATALOG
            .iter()
            .filter(|c| {
                q.is_empty()
                    || c.name.to_ascii_lowercase().contains(&q)
                    || c.description.to_ascii_lowercase().contains(&q)
            })
            .map(|c| RegistryHit {
                name: c.name.to_owned(),
                description: c.description.to_owned(),
                stars: 0,
                official: c.official,
                source: "freebsd-catalog".to_owned(),
                suggested_tag: c.tag.to_owned(),
                os: c.os.to_owned(),
            })
            .collect();

        if let Some(token) = token
            && out.len() < limit
            && let Ok(hits) = self.ghcr_packages(None, &q, limit - out.len(), token).await
        {
            for hit in hits {
                if !out.iter().any(|h| h.name == hit.name) {
                    out.push(hit);
                }
            }
        }

        if !q.is_empty() && out.len() < limit {
            let page_size = (limit - out.len()).clamp(1, 25);
            let req = self
                .http
                .get(format!("{}/v2/search/repositories/", self.search_at.hub))
                .query(&[("query", trimmed), ("page_size", &page_size.to_string())])
                .timeout(std::time::Duration::from_secs(6));
            if let Ok(resp) = req.send().await
                && resp.status().is_success()
                && let Ok(hub) = resp.json::<HubSearchResponse>().await
            {
                for item in hub.results {
                    if out.len() >= limit {
                        break;
                    }
                    if out.iter().any(|h| h.name == item.repo_name) {
                        continue;
                    }
                    out.push(RegistryHit {
                        name: item.repo_name,
                        description: item.short_description,
                        stars: item.star_count,
                        official: item.is_official,
                        source: "docker-hub".to_owned(),
                        suggested_tag: "latest".to_owned(),
                        // Docker's own images are Linux's; anyone else's
                        // may be either.
                        os: if item.is_official {
                            "linux".to_owned()
                        } else {
                            String::new()
                        },
                    });
                }
            }
        }
        out.truncate(limit);
        Ok(out)
    }

    /// A `ghcr.io/...` query (see [`Client::search_with_ghcr`]).
    async fn search_ghcr(
        &self,
        g: &GhcrQuery,
        limit: usize,
        token: Option<&str>,
    ) -> Result<Vec<RegistryHit>, Error> {
        let Some(repo) = &g.repo else {
            let token = token.ok_or_else(|| {
                Error::Registry(
                    "listing ghcr.io packages needs a GitHub token (a Secret with \
                     read:packages); name the repository, ghcr.io/owner/repo, to look \
                     a public one up without"
                        .to_owned(),
                )
            })?;
            let owner = (!g.owner.is_empty()).then_some(g.owner.as_str());
            return self.ghcr_packages(owner, "", limit, token).await;
        };
        let mut out = Vec::new();
        let mut why = None;
        match self.ghcr_repository(&g.owner, repo).await {
            Ok(hit) => out.push(hit),
            Err(e) => why = Some(e),
        }
        if let Some(token) = token {
            match self.ghcr_packages(Some(&g.owner), repo, limit, token).await {
                Ok(hits) => {
                    for hit in hits {
                        if !out.iter().any(|h| h.name == hit.name) {
                            out.push(hit);
                        }
                    }
                }
                Err(e) if out.is_empty() => why = Some(e),
                Err(_) => {}
            }
        }
        match why {
            Some(e) if out.is_empty() => Err(e),
            _ => {
                out.truncate(limit);
                Ok(out)
            }
        }
    }

    /// `ghcr.io/owner/repo` as the registry has it, read as a pull is:
    /// anonymously, with the token its auth service gives anyone for a
    /// public image. Its tags are the first page the registry gives
    /// ([`Client::list_tags`]: 100), so the tag suggested is of those.
    async fn ghcr_repository(&self, owner: &str, repo: &str) -> Result<RegistryHit, Error> {
        let r = Reference::parse(&format!("{}/{owner}/{repo}", self.search_at.ghcr))?;
        let tags =
            tokio::time::timeout(std::time::Duration::from_secs(8), self.list_tags(&r, None))
                .await
                .map_err(|_| {
                    Error::Registry(format!("ghcr.io/{owner}/{repo}: no answer in 8 s"))
                })??;
        let suggested = suggest_tag(&tags)
            .ok_or_else(|| Error::NotFound(format!("ghcr.io/{owner}/{repo}: no tags")))?;
        let more = if tags.len() >= 100 { "+" } else { "" };
        Ok(RegistryHit {
            name: format!("ghcr.io/{owner}/{repo}"),
            description: format!("Container image on ghcr.io ({}{more} tags)", tags.len()),
            stars: 0,
            official: false,
            source: "ghcr.io".to_owned(),
            suggested_tag: suggested,
            os: String::new(),
        })
    }

    /// The container packages of `owner` (an organization, else a user), or
    /// of the token's own account, whose names hold `filter`. GitHub's
    /// packages API pages its answers: the first page only, 100 packages,
    /// is read. Their tags are not, so no tag is suggested.
    async fn ghcr_packages(
        &self,
        owner: Option<&str>,
        filter: &str,
        limit: usize,
        token: &str,
    ) -> Result<Vec<RegistryHit>, Error> {
        let get = async |url: reqwest::Url| {
            self.http
                .get(url)
                .header(ACCEPT, "application/vnd.github+json")
                .bearer_auth(token)
                .timeout(std::time::Duration::from_secs(8))
                .send()
                .await
                .map_err(|e| Error::Registry(format!("ghcr.io search: {e}")))
        };
        let api = &self.search_at.github_api;
        let resp = match owner {
            None => get(packages_url(api, &["user"])?).await?,
            Some(owner) => {
                let org = get(packages_url(api, &["orgs", owner])?).await?;
                if org.status() == StatusCode::NOT_FOUND {
                    get(packages_url(api, &["users", owner])?).await?
                } else {
                    org
                }
            }
        };
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Registry(format!(
                "ghcr.io: GitHub's packages API answered {status}"
            )));
        }
        let packages: Vec<GhcrPackage> = resp
            .json()
            .await
            .map_err(|e| Error::Registry(format!("ghcr.io: GitHub's packages API: {e}")))?;
        Ok(ghcr_hits(packages, filter, limit))
    }
}

/// Where searches go, apart from the tests'.
struct SearchEndpoints {
    /// Docker Hub's API.
    hub: String,
    /// GitHub's API, for its packages.
    github_api: String,
    /// GitHub Container Registry, as a registry name.
    ghcr: String,
}

impl Default for SearchEndpoints {
    fn default() -> Self {
        SearchEndpoints {
            hub: "https://hub.docker.com".to_owned(),
            github_api: "https://api.github.com".to_owned(),
            ghcr: "ghcr.io".to_owned(),
        }
    }
}

/// A search for `ghcr.io/<owner>[/<repo>]`: `ghcr.io/` in any case, the
/// rest lowercased, as ghcr.io's names are; a tag or digest given is not
/// what is searched for, and is dropped.
#[derive(Debug, PartialEq, Eq)]
struct GhcrQuery {
    /// Empty: the token's own account.
    owner: String,
    repo: Option<String>,
}

impl GhcrQuery {
    fn parse(query: &str) -> Option<GhcrQuery> {
        let prefix = query.get(..8)?;
        if !prefix.eq_ignore_ascii_case("ghcr.io/") {
            return None;
        }
        let rest = query[8..].trim().to_ascii_lowercase();
        let rest = rest.split('@').next().unwrap_or_default();
        let (owner, repo) = match rest.split_once('/') {
            Some((o, r)) => (o.trim(), r.trim().trim_matches('/')),
            None => (rest.trim(), ""),
        };
        let slash = repo.rfind('/').map_or(0, |i| i + 1);
        let repo = match repo[slash..].find(':') {
            Some(i) => &repo[..slash + i],
            None => repo,
        };
        Some(GhcrQuery {
            owner: owner.to_owned(),
            repo: (!repo.is_empty()).then(|| repo.to_owned()),
        })
    }
}

/// `<api>/<segments...>/packages?package_type=container&per_page=100`,
/// each segment escaped.
fn packages_url(api: &str, segments: &[&str]) -> Result<reqwest::Url, Error> {
    let mut url = reqwest::Url::parse(api)
        .map_err(|e| Error::Registry(format!("GitHub's API at {api}: {e}")))?;
    url.path_segments_mut()
        .map_err(|()| Error::Registry(format!("GitHub's API at {api}: not a base")))?
        .pop_if_empty()
        .extend(segments)
        .push("packages");
    url.query_pairs_mut()
        .append_pair("package_type", "container")
        .append_pair("per_page", "100");
    Ok(url)
}

/// Search hits of GitHub's container packages whose lowercased names hold
/// `filter`, at most `limit`, named as ghcr.io names them (lowercase).
fn ghcr_hits(packages: Vec<GhcrPackage>, filter: &str, limit: usize) -> Vec<RegistryHit> {
    let filter = filter.to_ascii_lowercase();
    packages
        .into_iter()
        .map(|p| {
            let owner = p.owner.map(|o| o.login).unwrap_or_default();
            let name = if owner.is_empty() {
                format!("ghcr.io/{}", p.name)
            } else {
                format!("ghcr.io/{owner}/{}", p.name)
            };
            (name.to_ascii_lowercase(), p.visibility)
        })
        .filter(|(name, _)| filter.is_empty() || name.contains(&filter))
        .take(limit)
        .map(|(name, visibility)| RegistryHit {
            name,
            description: if visibility.is_empty() {
                "Container package on ghcr.io".to_owned()
            } else {
                format!("{visibility} container package on ghcr.io")
            },
            stars: 0,
            official: false,
            source: "ghcr.io".to_owned(),
            suggested_tag: String::new(),
            os: String::new(),
        })
        .collect()
}

/// The tag to suggest of `tags`: `latest` when there is one, else the
/// highest version-like tag (`1.2.3`, `v15.1`), else the last that is not
/// a signature's or attestation's (`sha256-...`), else the last.
fn suggest_tag(tags: &[String]) -> Option<String> {
    if tags.iter().any(|t| t == "latest") {
        return Some("latest".to_owned());
    }
    let version = |t: &str| -> Option<Vec<u64>> {
        let digits = t.strip_prefix(['v', 'V']).unwrap_or(t);
        digits.split('.').map(|p| p.parse().ok()).collect()
    };
    tags.iter()
        .filter_map(|t| Some((version(t)?, t)))
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, t)| t)
        .or_else(|| tags.iter().rev().find(|t| !t.starts_with("sha256-")))
        .or(tags.last())
        .cloned()
}

/// A search result from the FreeBSD jail catalog, Docker Hub or ghcr.io.
/// `suggested_tag` is empty when no tag is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryHit {
    pub name: String,
    pub description: String,
    pub stars: u64,
    pub official: bool,
    pub source: String,
    pub suggested_tag: String,
    /// The OS its images are for, where the search knows it without
    /// fetching them: `freebsd`, `linux`, or empty for not known (inspect
    /// the image to tell).
    pub os: String,
}

#[derive(serde::Deserialize)]
struct GhcrPackage {
    name: String,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    owner: Option<GhcrOwner>,
}

#[derive(serde::Deserialize)]
struct GhcrOwner {
    login: String,
}

#[derive(serde::Deserialize)]
struct HubSearchResponse {
    #[serde(default)]
    results: Vec<HubRepo>,
}

#[derive(serde::Deserialize)]
struct HubRepo {
    #[serde(default)]
    repo_name: String,
    #[serde(default)]
    short_description: String,
    #[serde(default)]
    star_count: u64,
    #[serde(default)]
    is_official: bool,
}

/// Built-in FreeBSD OCI jail base images & common runtimes.
/// An image the search offers before asking any registry.
struct CatalogEntry {
    name: &'static str,
    description: &'static str,
    /// A tag the image has (checked when the entry was written).
    tag: &'static str,
    os: &'static str,
    /// Published by the FreeBSD project (or Docker), not a third party.
    official: bool,
}

const fn freebsd(name: &'static str, description: &'static str, tag: &'static str) -> CatalogEntry {
    CatalogEntry {
        name,
        description,
        tag,
        os: "freebsd",
        official: false,
    }
}

const fn project(name: &'static str, description: &'static str) -> CatalogEntry {
    CatalogEntry {
        name,
        description,
        tag: "15.1",
        os: "freebsd",
        official: true,
    }
}

/// FreeBSD images (and one Linux one, for `--linux` nodes): the FreeBSD
/// project's base images, and application images from the daemonless
/// project. No registry lists FreeBSD images, so they are
/// listed here.
const FREEBSD_CATALOG: &[CatalogEntry] = &[
    project(
        "ghcr.io/freebsd/freebsd-runtime",
        "FreeBSD's base image: the runtime userland, the smallest base for a jail",
    ),
    project(
        "ghcr.io/freebsd/freebsd-static",
        "FreeBSD's base image for statically linked programs",
    ),
    project(
        "ghcr.io/freebsd/freebsd-dynamic",
        "FreeBSD's base image for dynamically linked programs",
    ),
    project(
        "ghcr.io/freebsd/freebsd-notoolchain",
        "FreeBSD's base system without the compilers: a base to pkg install into",
    ),
    project(
        "ghcr.io/freebsd/freebsd-toolchain",
        "FreeBSD's base system with the compilers, for building",
    ),
    freebsd(
        "ghcr.io/daemonless/base",
        "FreeBSD base image with pkg, from the daemonless project",
        "latest",
    ),
    freebsd(
        "ghcr.io/daemonless/nginx",
        "nginx HTTP server and reverse proxy on FreeBSD, from the daemonless project",
        "latest",
    ),
    freebsd(
        "ghcr.io/daemonless/caddy",
        "Caddy web server with automatic HTTPS on FreeBSD, from the daemonless project",
        "latest",
    ),
    freebsd(
        "ghcr.io/daemonless/postgres",
        "PostgreSQL database server on FreeBSD, from the daemonless project",
        "17",
    ),
    freebsd(
        "ghcr.io/daemonless/mariadb",
        "MariaDB (MySQL) database server on FreeBSD, from the daemonless project",
        "latest",
    ),
    freebsd(
        "ghcr.io/daemonless/redis",
        "Redis in-memory data store on FreeBSD, from the daemonless project",
        "latest",
    ),
    freebsd(
        "ghcr.io/daemonless/jellyfin",
        "Jellyfin media server on FreeBSD, from the daemonless project",
        "latest",
    ),
    CatalogEntry {
        name: "alpine",
        description: "Alpine Linux: a Linux image, run through the Linux ABI on nodes started with --linux",
        tag: "3.20",
        os: "linux",
        official: true,
    },
];

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

    #[test]
    fn ghcr_queries() {
        let q = GhcrQuery::parse;
        let g = |owner: &str, repo: Option<&str>| {
            Some(GhcrQuery {
                owner: owner.to_owned(),
                repo: repo.map(str::to_owned),
            })
        };
        assert_eq!(q("ghcr.io/Owner/Repo"), g("owner", Some("repo")));
        assert_eq!(q("GHCR.IO/owner/repo"), g("owner", Some("repo")));
        assert_eq!(q("Ghcr.Io/owner"), g("owner", None));
        assert_eq!(q("ghcr.io/owner/"), g("owner", None));
        assert_eq!(q("ghcr.io/"), g("", None));
        assert_eq!(q("ghcr.io/o/team/app:1.2"), g("o", Some("team/app")));
        assert_eq!(
            q(&format!("ghcr.io/o/app@sha256:{}", "0".repeat(64))),
            g("o", Some("app"))
        );
        for plain in [
            "nginx",
            "freebsd/runtime",
            "ghcr.io",
            "docker.io/library/x",
            "",
        ] {
            assert_eq!(q(plain), None, "{plain}");
        }
    }

    #[test]
    fn the_tag_suggested() {
        let s =
            |tags: &[&str]| suggest_tag(&tags.iter().map(|t| (*t).to_owned()).collect::<Vec<_>>());
        assert_eq!(s(&["1.0", "latest", "2.0"]).as_deref(), Some("latest"));
        assert_eq!(s(&["1.10.0", "1.2.0", "1.9.9"]).as_deref(), Some("1.10.0"));
        assert_eq!(s(&["v14.2", "15.1", "edge"]).as_deref(), Some("15.1"));
        assert_eq!(
            s(&["edge", "main", "sha256-ab.sig"]).as_deref(),
            Some("main"),
            "a signature's tag is not suggested while another is there"
        );
        assert_eq!(s(&["sha256-ab.sig"]).as_deref(), Some("sha256-ab.sig"));
        assert_eq!(s(&[]), None);
    }

    #[test]
    fn package_urls_escape_the_owner() {
        let u = packages_url("https://api.github.com", &["orgs", "a b/c?d"]).unwrap();
        assert_eq!(
            u.as_str(),
            "https://api.github.com/orgs/a%20b%2Fc%3Fd/packages?package_type=container&per_page=100"
        );
        let u = packages_url("http://127.0.0.1:1/", &["user"]).unwrap();
        assert_eq!(
            u.as_str(),
            "http://127.0.0.1:1/user/packages?package_type=container&per_page=100"
        );
    }

    #[test]
    fn packages_named_as_ghcr_names_them() {
        let packages: Vec<GhcrPackage> = serde_json::from_str(
            r#"[{"name": "Web", "visibility": "public", "owner": {"login": "Team"}},
                {"name": "db", "visibility": "private", "owner": {"login": "Team"}},
                {"name": "solo"}]"#,
        )
        .unwrap();
        let hits = ghcr_hits(packages, "WEB", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "ghcr.io/team/web");
        assert_eq!(
            hits[0].suggested_tag, "",
            "no tag claimed when none is known"
        );
        let packages: Vec<GhcrPackage> =
            serde_json::from_str(r#"[{"name": "a"}, {"name": "b"}, {"name": "c"}]"#).unwrap();
        let hits = ghcr_hits(packages, "", 2);
        assert_eq!(
            hits.iter().map(|h| h.name.as_str()).collect::<Vec<_>>(),
            ["ghcr.io/a", "ghcr.io/b"]
        );
    }

    /// Serves `answer` over plain HTTP, keeping each request's path and
    /// query and its `Authorization`; returns the address.
    async fn stub(
        answer: fn(&str) -> (u16, String),
    ) -> (String, std::sync::Arc<Mutex<Vec<(String, String)>>>) {
        use http_body_util::Full;
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let kept = seen.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let kept = kept.clone();
                tokio::spawn(async move {
                    let svc = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let kept = kept.clone();
                            async move {
                                let path = req.uri().to_string();
                                let auth = req
                                    .headers()
                                    .get("authorization")
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("")
                                    .to_owned();
                                kept.lock().unwrap().push((path.clone(), auth));
                                let (status, body) = answer(&path);
                                Ok::<_, std::convert::Infallible>(
                                    hyper::Response::builder()
                                        .status(status)
                                        .header("content-type", "application/json")
                                        .body(Full::new(bytes::Bytes::from(body)))
                                        .unwrap(),
                                )
                            }
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        (addr, seen)
    }

    fn stubbed(addr: &str) -> Client {
        let mut c = Client::new().unwrap().plain_http(addr);
        c.search_at = SearchEndpoints {
            hub: format!("http://{addr}"),
            github_api: format!("http://{addr}/api"),
            ghcr: addr.to_owned(),
        };
        c
    }

    fn github(path: &str) -> (u16, String) {
        match path.split('?').next().unwrap() {
            "/v2/owner/app/tags/list" => (
                200,
                r#"{"tags": ["1.2.0", "1.10.0", "sha256-x.sig"]}"#.into(),
            ),
            "/v2/search/repositories/" => (
                200,
                r#"{"results": [
                    {"repo_name": "postgres", "short_description": "", "star_count": 9, "is_official": true},
                    {"repo_name": "someone/postgres", "short_description": "", "star_count": 1, "is_official": false}
                ]}"#
                .into(),
            ),
            "/api/orgs/team/packages" => (404, "{}".into()),
            "/api/users/team/packages" | "/api/user/packages" => (
                200,
                r#"[{"name": "App", "visibility": "public", "owner": {"login": "Team"}}]"#.into(),
            ),
            _ => (404, "{}".into()),
        }
    }

    #[tokio::test]
    async fn a_public_ghcr_repository_is_found_without_a_token() {
        let (addr, seen) = stub(github).await;
        let hits = stubbed(&addr)
            .search_with_ghcr("GHCR.IO/Owner/App:9", 5, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].name, "ghcr.io/owner/app");
        assert_eq!(hits[0].suggested_tag, "1.10.0");
        assert_eq!(hits[0].source, "ghcr.io");
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter()
                .all(|(p, auth)| !p.starts_with("/api") && auth.is_empty()),
            "{seen:?}"
        );
    }

    #[tokio::test]
    async fn listing_an_owner_needs_a_token() {
        let (addr, seen) = stub(github).await;
        let c = stubbed(&addr);
        let e = c
            .search_with_ghcr("ghcr.io/team", 5, None)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("GitHub token"), "{e}");
        assert!(
            seen.lock().unwrap().is_empty(),
            "nothing asked without a token"
        );

        let hits = c
            .search_with_ghcr("ghcr.io/Team", 5, Some("t0ken"))
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(
            hits[0].name, "ghcr.io/team/app",
            "lowercased as ghcr.io names it"
        );
        assert_eq!(hits[0].suggested_tag, "");
        let seen = seen.lock().unwrap();
        let paths: Vec<&str> = seen
            .iter()
            .map(|(p, _)| p.split('?').next().unwrap())
            .collect();
        assert_eq!(
            paths,
            ["/api/orgs/team/packages", "/api/users/team/packages"]
        );
        assert!(
            seen.iter().all(|(_, auth)| auth == "Bearer t0ken"),
            "{seen:?}"
        );
    }

    #[tokio::test]
    async fn a_plain_search_asks_ghcr_nothing_without_a_token() {
        let (addr, seen) = stub(github).await;
        let c = stubbed(&addr);
        let hits = c.search_with_ghcr("postgres", 10, None).await.unwrap();
        assert!(hits.iter().all(|h| h.source != "ghcr.io"), "{hits:?}");
        // Docker's own images are Linux's; another's OS is not known
        // until the image is read.
        let os = |name: &str| hits.iter().find(|h| h.name == name).map(|h| h.os.clone());
        assert_eq!(os("postgres").as_deref(), Some("linux"), "{hits:?}");
        assert_eq!(os("someone/postgres").as_deref(), Some(""), "{hits:?}");
        assert_eq!(
            os("ghcr.io/daemonless/postgres").as_deref(),
            Some("freebsd"),
            "{hits:?}"
        );
        let paths: Vec<String> = seen
            .lock()
            .unwrap()
            .iter()
            .map(|(p, _)| p.clone())
            .collect();
        assert!(
            paths
                .iter()
                .all(|p| p.starts_with("/v2/search/repositories/")),
            "only Docker Hub's search: {paths:?}"
        );
        // With a token, the token's own packages too.
        let hits = c.search_with_ghcr("app", 10, Some("t0ken")).await.unwrap();
        assert!(
            hits.iter().any(|h| h.name == "ghcr.io/team/app"),
            "{hits:?}"
        );
    }

    #[tokio::test]
    async fn freebsd_catalog_matches_queries() {
        let c = Client::new().unwrap();
        let hits = c.search("postgres", 10).await.unwrap();
        assert!(
            hits.iter()
                .any(|h| h.name == "ghcr.io/daemonless/postgres" && h.source == "freebsd-catalog"),
            "{hits:?}"
        );
        let empty = c.search("", 5).await.unwrap();
        assert_eq!(empty.len(), 5);
        assert_eq!(empty[0].source, "freebsd-catalog");
    }

    #[test]
    fn the_catalog_says_each_image_s_os_and_only_the_projects_are_official() {
        for c in FREEBSD_CATALOG {
            assert!(c.os == "freebsd" || c.os == "linux", "{}", c.name);
            assert!(!c.tag.is_empty(), "{}", c.name);
            assert_eq!(
                c.official,
                c.name.starts_with("ghcr.io/freebsd/") || c.name == "alpine",
                "{}",
                c.name
            );
            if c.os == "linux" {
                assert!(c.description.contains("--linux"), "{}", c.name);
            }
        }
    }
}
