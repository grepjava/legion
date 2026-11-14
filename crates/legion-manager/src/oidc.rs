//! Single sign-on in the console gateway (CONSOLE.md phase 6): OpenID
//! Connect's authorization code flow with PKCE, state and nonce, and the ID
//! token checked here, against the keys the provider publishes, with ring.
//! What comes out is a name and groups; the session that follows is like
//! any other (`console::oidc_session`).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use legion_api::console::Oidc;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// How long what discovery found is used before it is asked again.
const DISCOVERY_AGE: Duration = Duration::from_secs(600);
/// Clocks may differ this much between the provider and here.
const SKEW_S: i64 = 60;

/// The provider's endpoints and keys, as discovery last found them.
#[derive(Clone, Debug)]
struct Discovered {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    keys: Vec<Jwk>,
    at: Instant,
}

#[derive(Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

#[derive(Clone, Debug, Deserialize)]
struct Jwk {
    kty: String,
    kid: Option<String>,
    #[serde(rename = "use")]
    usage: Option<String>,
    n: Option<String>,
    e: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// One sign-in under way: kept in the browser, in a cookie only this flow
/// reads, and checked when the provider sends the browser back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Flow {
    pub state: String,
    pub nonce: String,
    pub verifier: String,
}

impl Flow {
    fn new() -> Flow {
        // Hex: fit for a URL, a cookie and PKCE alike.
        Flow {
            state: legion_pki::random_secret(),
            nonce: legion_pki::random_secret(),
            verifier: legion_pki::random_secret(),
        }
    }

    pub fn cookie_value(&self) -> String {
        format!("{}.{}.{}", self.state, self.nonce, self.verifier)
    }

    pub fn from_cookie(v: &str) -> Option<Flow> {
        let mut parts = v.split('.');
        let flow = Flow {
            state: parts.next()?.to_owned(),
            nonce: parts.next()?.to_owned(),
            verifier: parts.next()?.to_owned(),
        };
        (parts.next().is_none() && !flow.state.is_empty()).then_some(flow)
    }
}

/// The gateway's single sign-on client: discovery and keys cached.
pub(crate) struct Provider {
    http: reqwest::Client,
    found: Mutex<Option<Discovered>>,
}

impl Provider {
    pub fn new() -> Provider {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("an HTTP client");
        Provider {
            http,
            found: Mutex::new(None),
        }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, String> {
        if !legion_api::console::secure_url(url) {
            return Err(format!("{url}: not an https:// address"));
        }
        let r = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("{url}: {}", r.status()));
        }
        r.json().await.map_err(|e| format!("{url}: {e}"))
    }

    /// What discovery says of `oidc`'s provider, from the cache while it is
    /// fresh and for the same issuer, unless `refresh`.
    async fn discover(&self, oidc: &Oidc, refresh: bool) -> Result<Discovered, String> {
        if !refresh
            && let Some(d) = self.found.lock().unwrap().as_ref()
            && d.issuer == oidc.issuer
            && d.at.elapsed() < DISCOVERY_AGE
        {
            return Ok(d.clone());
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            oidc.issuer.trim_end_matches('/')
        );
        let doc: Discovery = self.get_json(&url).await?;
        if doc.issuer != oidc.issuer {
            return Err(format!(
                "the provider calls itself {:?}, not {:?}",
                doc.issuer, oidc.issuer
            ));
        }
        let jwks: Jwks = self.get_json(&doc.jwks_uri).await?;
        let d = Discovered {
            issuer: doc.issuer,
            authorization_endpoint: doc.authorization_endpoint,
            token_endpoint: doc.token_endpoint,
            keys: jwks.keys,
            at: Instant::now(),
        };
        *self.found.lock().unwrap() = Some(d.clone());
        Ok(d)
    }

    /// Where to send the browser to sign in, and the flow to keep.
    pub async fn start(&self, oidc: &Oidc) -> Result<(String, Flow), String> {
        let d = self.discover(oidc, false).await?;
        if !legion_api::console::secure_url(&d.authorization_endpoint) {
            return Err("the provider's authorization endpoint is not https://".into());
        }
        let flow = Flow::new();
        let challenge = B64.encode(Sha256::digest(flow.verifier.as_bytes()));
        let url = reqwest::Url::parse_with_params(
            &d.authorization_endpoint,
            [
                ("response_type", "code"),
                ("client_id", oidc.client_id.as_str()),
                ("redirect_uri", oidc.redirect_uri().as_str()),
                ("scope", oidc.scopes.join(" ").as_str()),
                ("state", flow.state.as_str()),
                ("nonce", flow.nonce.as_str()),
                ("code_challenge", challenge.as_str()),
                ("code_challenge_method", "S256"),
            ],
        )
        .map_err(|e| format!("the provider's authorization endpoint: {e}"))?;
        Ok((url.into(), flow))
    }

    /// The user the provider vouches for with `code`: `oidc:` and their
    /// name, and their groups.
    pub async fn finish(
        &self,
        oidc: &Oidc,
        client_secret: &str,
        code: &str,
        flow: &Flow,
    ) -> Result<(String, Vec<String>), String> {
        let d = self.discover(oidc, false).await?;
        if !legion_api::console::secure_url(&d.token_endpoint) {
            return Err("the provider's token endpoint is not https://".into());
        }
        let r = self
            .http
            .post(&d.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", oidc.redirect_uri().as_str()),
                ("client_id", oidc.client_id.as_str()),
                ("client_secret", client_secret),
                ("code_verifier", flow.verifier.as_str()),
            ])
            .send()
            .await
            .map_err(|e| format!("the provider's token endpoint: {e}"))?;
        let status = r.status();
        let body: Value = r
            .json()
            .await
            .map_err(|e| format!("the provider's token endpoint: {e}"))?;
        if !status.is_success() {
            return Err(format!("the provider refused the code: {status} {body}"));
        }
        let token = body["id_token"]
            .as_str()
            .ok_or("the provider sent no ID token")?;
        let now = crate::console::now_ms() / 1000;
        let claims = match verify(token, &d.keys, oidc, &flow.nonce, now) {
            // Keys rotated since they were fetched: once more, fresh.
            Err(Unverified::NoKey) => {
                let d = self.discover(oidc, true).await?;
                verify(token, &d.keys, oidc, &flow.nonce, now)
            }
            other => other,
        }
        .map_err(|e| e.to_string())?;
        identity(oidc, &claims)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Unverified {
    NoKey,
    Bad(String),
}

impl std::fmt::Display for Unverified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unverified::NoKey => {
                f.write_str("the ID token is signed with a key the provider does not publish")
            }
            Unverified::Bad(why) => write!(f, "the ID token: {why}"),
        }
    }
}

fn bad(why: impl Into<String>) -> Unverified {
    Unverified::Bad(why.into())
}

fn b64(part: &str) -> Result<Vec<u8>, Unverified> {
    B64.decode(part.trim_end_matches('='))
        .map_err(|_| bad("not base64url"))
}

/// An ID token's claims, once its signature (RS256 or ES256, by a key the
/// provider publishes), issuer, audience, expiry and nonce are checked.
fn verify(
    token: &str,
    keys: &[Jwk],
    oidc: &Oidc,
    nonce: &str,
    now: i64,
) -> Result<Value, Unverified> {
    use ring::signature;
    let mut parts = token.split('.');
    let (Some(h), Some(p), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("not three parts"));
    };
    let header: Value =
        serde_json::from_slice(&b64(h)?).map_err(|_| bad("a header that is not JSON"))?;
    let alg = header["alg"].as_str().unwrap_or_default();
    let kty = match alg {
        "RS256" => "RSA",
        "ES256" => "EC",
        other => return Err(bad(format!("signed with {other:?}, not RS256 or ES256"))),
    };
    let kid = header["kid"].as_str();
    let candidates: Vec<&Jwk> = keys
        .iter()
        .filter(|k| k.kty == kty && k.usage.as_deref().is_none_or(|u| u == "sig"))
        .filter(|k| kid.is_none() || k.kid.as_deref() == kid)
        .collect();
    if candidates.is_empty() {
        return Err(Unverified::NoKey);
    }
    let signed = format!("{h}.{p}");
    let sig = b64(s)?;
    let good = candidates.iter().any(|k| match alg {
        "RS256" => {
            let (Some(n), Some(e)) = (&k.n, &k.e) else {
                return false;
            };
            let (Ok(n), Ok(e)) = (b64(n), b64(e)) else {
                return false;
            };
            signature::RsaPublicKeyComponents { n: &n, e: &e }
                .verify(
                    &signature::RSA_PKCS1_2048_8192_SHA256,
                    signed.as_bytes(),
                    &sig,
                )
                .is_ok()
        }
        _ => {
            let (Some("P-256"), Some(x), Some(y)) = (k.crv.as_deref(), &k.x, &k.y) else {
                return false;
            };
            let (Ok(x), Ok(y)) = (b64(x), b64(y)) else {
                return false;
            };
            let point: Vec<u8> = [&[4u8][..], &x, &y].concat();
            signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, point)
                .verify(signed.as_bytes(), &sig)
                .is_ok()
        }
    });
    if !good {
        return Err(bad("its signature does not verify"));
    }
    let claims: Value =
        serde_json::from_slice(&b64(p)?).map_err(|_| bad("claims that are not JSON"))?;
    if claims["iss"].as_str() != Some(oidc.issuer.as_str()) {
        return Err(bad(format!(
            "issued by {}, not {}",
            claims["iss"], oidc.issuer
        )));
    }
    let audience: Vec<&str> = match &claims["aud"] {
        Value::String(a) => vec![a.as_str()],
        Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    if !audience.contains(&oidc.client_id.as_str()) {
        return Err(bad("for another client"));
    }
    if audience.len() > 1 && claims["azp"].as_str() != Some(oidc.client_id.as_str()) {
        return Err(bad("for several clients, and not authorized for this one"));
    }
    match claims["exp"].as_i64() {
        Some(exp) if exp + SKEW_S > now => {}
        _ => return Err(bad("expired")),
    }
    if claims["nonce"].as_str() != Some(nonce) {
        return Err(bad("not from this sign-in (its nonce differs)"));
    }
    Ok(claims)
}

/// Who the claims say: `oidc:` and the username claim, and the groups.
fn identity(oidc: &Oidc, claims: &Value) -> Result<(String, Vec<String>), String> {
    let name = claims[&oidc.username_claim]
        .as_str()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| format!("the ID token has no {} claim", oidc.username_claim))?;
    if oidc.username_claim == "email" && claims["email_verified"] == Value::Bool(false) {
        return Err(format!("the provider has not verified {name}"));
    }
    let groups = match &claims[&oidc.groups_claim] {
        Value::String(g) => vec![g.clone()],
        Value::Array(a) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    };
    let name = format!("{}{name}", legion_api::rbac::OIDC_PREFIX);
    legion_api::rbac::check_user("name", &name).map_err(|e| e.to_string())?;
    Ok((name, groups))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};

    fn oidc() -> Oidc {
        serde_json::from_value(serde_json::json!({
            "issuer": "https://idp.example", "clientId": "legion",
            "consoleUrl": "https://console.example",
        }))
        .unwrap()
    }

    /// A key, published as a JWK, and a token it signs.
    fn key() -> (EcdsaKeyPair, Jwk) {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        let point = pair.public_key().as_ref();
        let jwk = Jwk {
            kty: "EC".into(),
            kid: Some("k1".into()),
            usage: Some("sig".into()),
            n: None,
            e: None,
            crv: Some("P-256".into()),
            x: Some(B64.encode(&point[1..33])),
            y: Some(B64.encode(&point[33..])),
        };
        (pair, jwk)
    }

    fn sign(pair: &EcdsaKeyPair, header: Value, claims: Value) -> String {
        let signed = format!(
            "{}.{}",
            B64.encode(header.to_string()),
            B64.encode(claims.to_string())
        );
        let sig = pair.sign(&SystemRandom::new(), signed.as_bytes()).unwrap();
        format!("{signed}.{}", B64.encode(sig.as_ref()))
    }

    fn claims() -> Value {
        serde_json::json!({
            "iss": "https://idp.example", "aud": "legion", "exp": 2000,
            "nonce": "n1", "email": "alice@example.com",
            "groups": ["shop", "ops"],
        })
    }

    #[test]
    fn an_id_token_is_checked_before_it_is_believed() {
        let (pair, jwk) = key();
        let header = serde_json::json!({ "alg": "ES256", "kid": "k1" });
        let o = oidc();
        let check = |claims: Value| {
            verify(
                &sign(&pair, header.clone(), claims),
                std::slice::from_ref(&jwk),
                &o,
                "n1",
                1000,
            )
        };

        let c = check(claims()).unwrap();
        assert_eq!(
            identity(&o, &c).unwrap(),
            (
                "oidc:alice@example.com".to_owned(),
                vec!["shop".to_owned(), "ops".to_owned()]
            )
        );
        let with = |k: &str, v: Value| {
            let mut c = claims();
            c[k] = v;
            check(c).unwrap_err()
        };
        assert!(
            with("iss", "https://evil.example".into())
                .to_string()
                .contains("issued by")
        );
        assert!(
            with("aud", "other".into())
                .to_string()
                .contains("another client")
        );
        assert!(
            with("aud", serde_json::json!(["legion", "other"]))
                .to_string()
                .contains("several")
        );
        assert!(with("exp", 900.into()).to_string().contains("expired"));
        assert!(with("nonce", "n2".into()).to_string().contains("nonce"));

        // Signed by another key, or with a key not published.
        let (other, _) = key();
        let forged = sign(&other, header.clone(), claims());
        assert_eq!(
            verify(&forged, std::slice::from_ref(&jwk), &o, "n1", 1000).unwrap_err(),
            bad("its signature does not verify")
        );
        let unknown = sign(
            &pair,
            serde_json::json!({ "alg": "ES256", "kid": "k9" }),
            claims(),
        );
        assert_eq!(
            verify(&unknown, std::slice::from_ref(&jwk), &o, "n1", 1000).unwrap_err(),
            Unverified::NoKey
        );
        // No algorithm but the two.
        let none = sign(&pair, serde_json::json!({ "alg": "none" }), claims());
        assert!(
            verify(&none, std::slice::from_ref(&jwk), &o, "n1", 1000)
                .unwrap_err()
                .to_string()
                .contains("\"none\"")
        );
        let hs = sign(
            &pair,
            serde_json::json!({ "alg": "HS256", "kid": "k1" }),
            claims(),
        );
        assert!(verify(&hs, &[jwk], &o, "n1", 1000).is_err());

        // An email the provider has not verified is no name.
        let mut c = claims();
        c["email_verified"] = false.into();
        assert!(identity(&o, &c).unwrap_err().contains("not verified"));
        let mut c = claims();
        c.as_object_mut().unwrap().remove("email");
        assert!(identity(&o, &c).unwrap_err().contains("no email claim"));
    }

    #[test]
    fn a_flow_goes_through_its_cookie() {
        let f = Flow::new();
        assert_eq!(Flow::from_cookie(&f.cookie_value()), Some(f.clone()));
        assert_eq!(Flow::from_cookie("a.b"), None);
        assert_eq!(Flow::from_cookie("a.b.c.d"), None);
        assert_ne!(Flow::new().state, f.state);
    }
}
