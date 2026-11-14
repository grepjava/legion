//! An OpenID Connect provider for the console's single sign-on test, as a
//! company's would be to a browser: discovery, its key, a sign-in page
//! with a session of its own (a second console's sign-in asks nothing), and
//! a token endpoint that checks the code, the client and PKCE. Users are
//! `<name>@example.com`, in the groups given; ID tokens are ES256.
//!
//! Plain HTTP on loopback, one thread, enough for one browser and the
//! managers that exchange its codes.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};

/// A code given to a browser, until a manager exchanges it.
struct Grant {
    user: String,
    client: String,
    redirect: String,
    nonce: String,
    challenge: String,
}

#[derive(Default)]
struct State {
    grants: HashMap<String, Grant>,
    sessions: HashMap<String, String>,
    next: u64,
    /// How many times the sign-in page was shown, and codes exchanged.
    asked: u32,
    exchanged: u32,
}

pub struct Idp {
    pub issuer: String,
    state: Arc<Mutex<State>>,
}

impl Idp {
    /// Serves on `127.0.0.1:port`, `clients` its client IDs and secrets,
    /// `groups` each user's groups.
    pub fn start(port: u16, clients: &[(&str, &str)], groups: &[(&str, &[&str])]) -> Idp {
        let issuer = format!("http://127.0.0.1:{port}");
        let listener = TcpListener::bind(("127.0.0.1", port)).expect("the provider's port");
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let serve = Server {
            issuer: issuer.clone(),
            key,
            clients: clients
                .iter()
                .map(|(c, s)| (c.to_string(), s.to_string()))
                .collect(),
            groups: groups
                .iter()
                .map(|(u, g)| (u.to_string(), g.iter().map(|g| g.to_string()).collect()))
                .collect(),
            state: state.clone(),
        };
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                if let Err(e) = serve.answer(stream) {
                    eprintln!("the provider: {e}");
                }
            }
        });
        Idp { issuer, state }
    }

    /// How many times the sign-in page was shown.
    pub fn asked(&self) -> u32 {
        self.state.lock().unwrap().asked
    }

    /// How many codes were exchanged for tokens.
    pub fn exchanged(&self) -> u32 {
        self.state.lock().unwrap().exchanged
    }
}

struct Server {
    issuer: String,
    key: EcdsaKeyPair,
    clients: HashMap<String, String>,
    groups: HashMap<String, Vec<String>>,
    state: Arc<Mutex<State>>,
}

struct Request {
    method: String,
    path: String,
    query: HashMap<String, String>,
    raw_query: String,
    cookie: Option<String>,
    form: HashMap<String, String>,
}

fn pairs(s: &str) -> HashMap<String, String> {
    url_pairs(s).into_iter().collect()
}

fn url_pairs(s: &str) -> Vec<(String, String)> {
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect()
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn read(stream: &TcpStream) -> std::io::Result<Request> {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let (path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p.to_owned(), q.to_owned()),
        None => (target, String::new()),
    };
    let mut length = 0;
    let mut cookie = None;
    loop {
        let mut h = String::new();
        r.read_line(&mut h)?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((name, value)) = h.split_once(':') {
            let value = value.trim();
            match name.to_ascii_lowercase().as_str() {
                "content-length" => length = value.parse().unwrap_or(0),
                "cookie" => {
                    cookie = value
                        .split(';')
                        .filter_map(|c| c.trim().strip_prefix("idp="))
                        .map(str::to_owned)
                        .next()
                }
                _ => {}
            }
        }
    }
    let mut body = vec![0; length];
    r.read_exact(&mut body)?;
    Ok(Request {
        method,
        path,
        query: pairs(&raw_query),
        raw_query,
        cookie,
        form: pairs(&String::from_utf8_lossy(&body)),
    })
}

fn respond(
    mut stream: TcpStream,
    status: &str,
    headers: &[(&str, String)],
    body: &str,
) -> std::io::Result<()> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    out.push_str(body);
    stream.write_all(out.as_bytes())
}

impl Server {
    fn answer(&self, stream: TcpStream) -> std::io::Result<()> {
        let req = read(&stream)?;
        let json_type = ("Content-Type", "application/json".to_owned());
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/.well-known/openid-configuration") => {
                let i = &self.issuer;
                let body = json!({
                    "issuer": i,
                    "authorization_endpoint": format!("{i}/authorize"),
                    "token_endpoint": format!("{i}/token"),
                    "jwks_uri": format!("{i}/jwks"),
                    "response_types_supported": ["code"],
                    "id_token_signing_alg_values_supported": ["ES256"],
                });
                respond(stream, "200 OK", &[json_type], &body.to_string())
            }
            ("GET", "/jwks") => {
                let point = self.key.public_key().as_ref();
                let body = json!({ "keys": [{
                    "kty": "EC", "crv": "P-256", "kid": "lab", "use": "sig", "alg": "ES256",
                    "x": B64.encode(&point[1..33]), "y": B64.encode(&point[33..]),
                }]});
                respond(stream, "200 OK", &[json_type], &body.to_string())
            }
            // Signed in here already: straight back with a code.
            ("GET", "/authorize") => {
                let user = req
                    .cookie
                    .as_ref()
                    .and_then(|c| self.state.lock().unwrap().sessions.get(c).cloned());
                match user {
                    Some(user) => self.back(stream, &req.query, &user, None),
                    None => {
                        self.state.lock().unwrap().asked += 1;
                        let page = format!(
                            "<!doctype html><html lang=\"en\"><title>Lab ID</title>\
                             <h1>Lab ID</h1>\
                             <form method=\"post\" action=\"/login\">\
                             <input type=\"hidden\" name=\"q\" value=\"{}\">\
                             <label>Username <input name=\"user\" aria-label=\"Username\"></label> \
                             <button>Sign in</button></form>",
                            html_escape(&req.raw_query)
                        );
                        respond(
                            stream,
                            "200 OK",
                            &[("Content-Type", "text/html; charset=utf-8".to_owned())],
                            &page,
                        )
                    }
                }
            }
            ("POST", "/login") => {
                let user = req.form.get("user").cloned().unwrap_or_default();
                if !self.groups.contains_key(&user) {
                    return respond(stream, "403 Forbidden", &[], "no such user");
                }
                let session = {
                    let mut s = self.state.lock().unwrap();
                    s.next += 1;
                    let id = format!("s{}", s.next);
                    s.sessions.insert(id.clone(), user.clone());
                    id
                };
                let query = pairs(req.form.get("q").map(String::as_str).unwrap_or(""));
                self.back(stream, &query, &user, Some(&session))
            }
            ("POST", "/token") => self.token(stream, &req.form),
            _ => respond(stream, "404 Not Found", &[], "not here"),
        }
    }

    /// Back to the client's redirect, with a code for `user`.
    fn back(
        &self,
        stream: TcpStream,
        q: &HashMap<String, String>,
        user: &str,
        session: Option<&str>,
    ) -> std::io::Result<()> {
        let get = |k: &str| q.get(k).cloned().unwrap_or_default();
        let (client, redirect) = (get("client_id"), get("redirect_uri"));
        if !self.clients.contains_key(&client) || get("code_challenge_method") != "S256" {
            return respond(stream, "400 Bad Request", &[], "unknown client or no PKCE");
        }
        let code = {
            let mut s = self.state.lock().unwrap();
            s.next += 1;
            let code = format!("code{}", s.next);
            s.grants.insert(
                code.clone(),
                Grant {
                    user: user.to_owned(),
                    client,
                    redirect: redirect.clone(),
                    nonce: get("nonce"),
                    challenge: get("code_challenge"),
                },
            );
            code
        };
        let to = format!("{redirect}?code={code}&state={}", encode(&get("state")));
        let mut headers = vec![("Location", to)];
        if let Some(s) = session {
            headers.push(("Set-Cookie", format!("idp={s}; Path=/; HttpOnly")));
        }
        respond(stream, "302 Found", &headers, "")
    }

    fn token(&self, stream: TcpStream, form: &HashMap<String, String>) -> std::io::Result<()> {
        let get = |k: &str| form.get(k).cloned().unwrap_or_default();
        let refuse = |stream| {
            respond(
                stream,
                "400 Bad Request",
                &[("Content-Type", "application/json".to_owned())],
                r#"{"error":"invalid_grant"}"#,
            )
        };
        let Some(grant) = self.state.lock().unwrap().grants.remove(&get("code")) else {
            return refuse(stream);
        };
        let verifier = get("code_verifier");
        let good = get("grant_type") == "authorization_code"
            && get("client_id") == grant.client
            && self.clients.get(&grant.client) == Some(&get("client_secret"))
            && get("redirect_uri") == grant.redirect
            && B64.encode(Sha256::digest(verifier.as_bytes())) == grant.challenge;
        if !good {
            return refuse(stream);
        }
        self.state.lock().unwrap().exchanged += 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = json!({ "alg": "ES256", "kid": "lab", "typ": "JWT" });
        let claims = json!({
            "iss": self.issuer, "aud": grant.client, "sub": grant.user,
            "exp": now + 300, "iat": now, "nonce": grant.nonce,
            "email": format!("{}@example.com", grant.user), "email_verified": true,
            "groups": self.groups.get(&grant.user).cloned().unwrap_or_default(),
        });
        let signed = format!(
            "{}.{}",
            B64.encode(header.to_string()),
            B64.encode(claims.to_string())
        );
        let sig = self
            .key
            .sign(&SystemRandom::new(), signed.as_bytes())
            .expect("signing");
        let body = json!({
            "id_token": format!("{signed}.{}", B64.encode(sig.as_ref())),
            "token_type": "Bearer",
            "access_token": "unused",
            "expires_in": 300,
        });
        respond(
            stream,
            "200 OK",
            &[("Content-Type", "application/json".to_owned())],
            &body.to_string(),
        )
    }
}
