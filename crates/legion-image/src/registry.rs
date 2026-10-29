//! A client for registries speaking the OCI distribution API: manifests and
//! blobs, anonymous or with a token from the registry's auth service.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::oci::{self, Document, Manifest};
use crate::{Error, Reference};

/// A registry account.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

pub struct Client {
    http: reqwest::Client,
    /// Registries reached over plain HTTP (tests, and registries an
    /// operator marks insecure).
    plain: BTreeSet<String>,
    /// Bearer tokens by registry and repository.
    tokens: Mutex<BTreeMap<(String, String), String>>,
}

impl Client {
    pub fn new() -> Result<Client, Error> {
        // reqwest uses the process's rustls provider: ring, as elsewhere.
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(std::time::Duration::from_secs(30))
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
        self.plain.insert(registry.to_owned());
        self
    }

    fn url(&self, r: &Reference, path: &str) -> String {
        let scheme = if self.plain.contains(&r.registry) {
            "http"
        } else {
            "https"
        };
        format!("{scheme}://{}/v2/{}/{path}", r.host(), r.repository)
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
        let key = (r.registry.clone(), r.repository.clone());
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
            let auth = self.answer(r, &challenge, creds).await?;
            resp = send(Some(auth))
                .await
                .map_err(|e| Error::Registry(format!("{url}: {e}")))?;
        }
        match resp.status() {
            s if s.is_success() => Ok(resp),
            StatusCode::NOT_FOUND => Err(Error::NotFound(format!("{r}: {path} not found"))),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(Error::Registry(format!(
                "{r}: access denied{}",
                if creds.is_none() {
                    " (no credentials)"
                } else {
                    ""
                }
            ))),
            s => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Registry(format!(
                    "{url}: {s}: {}",
                    body.chars().take(200).collect::<String>()
                )))
            }
        }
    }

    /// The `Authorization` value answering a `WWW-Authenticate` challenge.
    async fn answer(
        &self,
        r: &Reference,
        challenge: &str,
        creds: Option<&Credentials>,
    ) -> Result<String, Error> {
        let basic = creds.map(|c| {
            let pair = format!("{}:{}", c.username, c.password);
            format!("Basic {}", STANDARD.encode(pair))
        });
        let Some(params) = challenge.strip_prefix("Bearer ") else {
            return basic
                .ok_or_else(|| Error::Registry(format!("{r}: access denied (no credentials)")));
        };
        let params = parse_challenge(params);
        let realm = params
            .get("realm")
            .ok_or_else(|| Error::Registry(format!("{r}: a token challenge without a realm")))?;
        let mut req = self.http.get(realm.as_str());
        let mut query = vec![(
            "scope",
            params
                .get("scope")
                .cloned()
                .unwrap_or_else(|| format!("repository:{}:pull", r.repository)),
        )];
        if let Some(service) = params.get("service") {
            query.push(("service", service.clone()));
        }
        req = req.query(&query);
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
            .insert((r.registry.clone(), r.repository.clone()), token.clone());
        Ok(format!("Bearer {token}"))
    }

    /// The manifest for this node's platform, and its digest.
    pub async fn manifest(
        &self,
        r: &Reference,
        creds: Option<&Credentials>,
    ) -> Result<(String, Manifest), Error> {
        let (digest, doc) = self.document(r, r.target(), creds).await?;
        if let Some(manifests) = &doc.manifests {
            let chosen = oci::select(manifests)?.digest.clone();
            let (digest, doc) = self.document(r, &chosen, creds).await?;
            return Ok((digest, doc.into_manifest()?));
        }
        Ok((digest, doc.into_manifest()?))
    }

    async fn document(
        &self,
        r: &Reference,
        target: &str,
        creds: Option<&Credentials>,
    ) -> Result<(String, Document), Error> {
        let bytes = self
            .get(
                r,
                &format!("manifests/{target}"),
                &oci::MANIFEST_TYPES,
                creds,
            )
            .await?
            .bytes()
            .await
            .map_err(|e| Error::Registry(format!("{r}: {e}")))?;
        let digest = sha256(&bytes);
        if target.starts_with("sha256:") && digest != target {
            return Err(Error::Digest {
                what: format!("{r} manifest"),
                expected: target.to_owned(),
                got: digest,
            });
        }
        Ok((digest, Document::parse(&bytes)?))
    }

    /// Writes the blob `digest` to `dest`, checking it.
    pub async fn blob(
        &self,
        r: &Reference,
        digest: &str,
        dest: &Path,
        creds: Option<&Credentials>,
    ) -> Result<(), Error> {
        let resp = self.get(r, &format!("blobs/{digest}"), &[], creds).await?;
        let mut file = tokio::fs::File::create(dest).await?;
        let mut hash = Sha256::new();
        let mut body = resp.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Registry(format!("{r}: {digest}: {e}")))?;
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

pub(crate) fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
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
}
