//! `Route`: HTTP requests for a host and path, sent to a service's ready
//! tasks by every node's ingress proxy (DESIGN.md §5.8).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, check_name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteSpec {
    /// The request's host, without a port, in lower case; `*.example.org`
    /// matches any one label in front of `example.org`.
    pub host: String,
    /// The path prefix, whole segments: `/shop` matches `/shop` and
    /// `/shop/cart` but not `/shopping`.
    #[serde(default = "root")]
    pub path: String,
    /// A service of the route's namespace.
    pub service: String,
    /// The service's port, needed when it has more than one TCP port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// HTTPS for the host, with the certificate in a Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<RouteTls>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteTls {
    /// A Secret of the route's namespace holding, in PEM, the certificate
    /// chain for the host and its private key. Every node may read it.
    pub secret: String,
    /// Have the leader keep the Secret holding a certificate from an ACME
    /// authority, validated over HTTP on port 80 (HTTP-01).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acme: Option<RouteAcme>,
}

/// Let's Encrypt's directory.
pub const LETS_ENCRYPT: &str = "https://acme-v02.api.letsencrypt.org/directory";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteAcme {
    /// The authority's directory, Let's Encrypt's if not given.
    #[serde(default = "lets_encrypt")]
    pub directory: String,
    /// Where the authority may write about the account, such as expiry
    /// warnings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

fn lets_encrypt() -> String {
    LETS_ENCRYPT.into()
}

/// What a route's status says of its ACME certificate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acme: Option<AcmeStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcmeStatus {
    /// HTTP-01 challenges being validated, token to key authorization,
    /// which the ingress proxies answer.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub challenges: BTreeMap<String, String>,
    /// When the certificate in the Secret expires (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_after: Option<String>,
    /// Why the last attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Attempts failed in a row.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failures: u32,
    /// When the next attempt is, after a failure (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<String>,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

fn root() -> String {
    "/".into()
}

impl Spec for RouteSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        let name = self.host.strip_prefix("*.").unwrap_or(&self.host);
        let labels: Vec<&str> = name.split('.').collect();
        if self.host.len() > 253 || labels.iter().any(|l| crate::validate_name(l).is_err()) {
            return Err(Invalid::new(
                "host",
                format!(
                    "{:?} is not a host name: lower-case labels of letters, digits and hyphens, \
                     optionally after `*.`",
                    self.host
                ),
            ));
        }
        // A request's path is printable ASCII, other bytes percent-encoded:
        // a route with anything else would match nothing.
        if !self.path.starts_with('/')
            || self.path.contains(['?', '#'])
            || !self.path.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(Invalid::new(
                "path",
                "a path starting with /, of printable ASCII without spaces, ? or #",
            ));
        }
        // `/shop/` is `/shop`, and `//` is `/`.
        let trimmed = self.path.trim_end_matches('/');
        self.path = if trimmed.is_empty() { "/" } else { trimmed }.to_owned();
        check_name("service", &self.service)?;
        if self.port == Some(0) {
            return Err(Invalid::new("port", "a port above 0"));
        }
        if let Some(tls) = &self.tls {
            check_name("tls.secret", &tls.secret)?;
            if let Some(acme) = &tls.acme {
                if self.host.starts_with("*.") {
                    return Err(Invalid::new(
                        "tls.acme",
                        "a `*.` host needs a DNS challenge, which Legion does not answer; \
                         give the certificate in the Secret",
                    ));
                }
                if !directory_allowed(&acme.directory) {
                    return Err(Invalid::new(
                        "tls.acme.directory",
                        "an https:// URL (http:// only to this host's loopback)",
                    ));
                }
                if let Some(email) = &acme.email
                    && (email
                        .split_once('@')
                        .is_none_or(|(u, d)| u.is_empty() || d.is_empty())
                        || email.contains([' ', ',', '<', '>']))
                {
                    return Err(Invalid::new("tls.acme.email", "an email address"));
                }
            }
        }
        Ok(())
    }
}

/// An ACME directory is fetched over HTTPS; plain HTTP only from the
/// manager's own loopback, which managers reach only when testing.
fn directory_allowed(url: &str) -> bool {
    if url
        .strip_prefix("https://")
        .is_some_and(|rest| !rest.is_empty())
    {
        return true;
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let host = match authority.rsplit_once(':') {
        Some((h, port)) if !port.is_empty() && port.bytes().all(|c| c.is_ascii_digit()) => h,
        _ => authority,
    };
    matches!(host, "127.0.0.1" | "[::1]" | "localhost")
}

/// Whether routes for hosts `a` and `b` may take the same requests: the
/// same host, or a `*.` host and one it covers. A host, and a `*.` host
/// with those under it, belong to the routes of one namespace.
pub fn hosts_overlap(a: &str, b: &str) -> bool {
    let covers = |wild: &str, host: &str| {
        wild.strip_prefix("*.")
            .is_some_and(|domain| host.split_once('.').is_some_and(|(_, rest)| rest == domain))
    };
    a == b || covers(a, b) || covers(b, a)
}

/// The hosts that routes of more than one namespace claim, of routes given
/// as (namespace, host). Admission refuses such a route, but routes from
/// before it may be left; no namespace's route is served for them.
pub fn contested<'a>(routes: impl IntoIterator<Item = (&'a str, &'a str)>) -> BTreeSet<String> {
    let mut by_host: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (namespace, host) in routes {
        by_host.entry(host).or_default().insert(namespace);
    }
    let mut contested = BTreeSet::new();
    for (host, namespaces) in &by_host {
        if namespaces.len() > 1 {
            contested.insert(host.to_string());
        }
        let Some((_, domain)) = host.split_once('.').filter(|_| !host.starts_with("*.")) else {
            continue;
        };
        let wild = format!("*.{domain}");
        if let Some(others) = by_host.get(wild.as_str())
            && namespaces.union(others).nth(1).is_some()
        {
            contested.insert(host.to_string());
            contested.insert(wild);
        }
    }
    contested
}

impl RouteSpec {
    /// Whether the route takes a request for `host` (lower case, without a
    /// port) and `path`, and how well: longer matches win, an exact host
    /// over a wildcard.
    pub fn matches(&self, host: &str, path: &str) -> Option<(bool, usize)> {
        let exact = match self.host.strip_prefix("*.") {
            Some(domain) => {
                let (first, rest) = host.split_once('.')?;
                if first.is_empty() || rest != domain {
                    return None;
                }
                false
            }
            None if self.host == host => true,
            None => return None,
        };
        let prefix = self.path.as_str();
        let under = prefix == "/"
            || path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/') || rest.starts_with('?'));
        under.then_some((exact, prefix.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(host: &str, path: &str) -> RouteSpec {
        let mut r = RouteSpec {
            host: host.into(),
            path: path.into(),
            service: "web".into(),
            port: None,
            tls: None,
        };
        r.admit().unwrap();
        r
    }

    #[test]
    fn hosts_and_paths() {
        let shop = route("shop.example.org", "/shop/");
        assert_eq!(shop.path, "/shop");
        assert_eq!(shop.matches("shop.example.org", "/shop"), Some((true, 5)));
        assert!(shop.matches("shop.example.org", "/shop/cart").is_some());
        assert!(shop.matches("shop.example.org", "/shop?x=1").is_some());
        assert!(shop.matches("shop.example.org", "/shopping").is_none());
        assert!(shop.matches("www.example.org", "/shop").is_none());

        // Only slashes is the root, and admitting it again changes nothing.
        let mut root = route("shop.example.org", "//");
        assert_eq!(root.path, "/");
        root.admit().unwrap();
        assert_eq!(root.path, "/");
        assert!(root.matches("shop.example.org", "/x").is_some());

        let any = route("*.example.org", "/");
        assert_eq!(any.matches("www.example.org", "/x"), Some((false, 1)));
        assert!(any.matches("example.org", "/").is_none());
        assert!(any.matches("a.b.example.org", "/").is_none());
    }

    #[test]
    fn hosts_of_one_namespace() {
        assert!(hosts_overlap("a.example.org", "a.example.org"));
        assert!(hosts_overlap("*.example.org", "a.example.org"));
        assert!(hosts_overlap("a.example.org", "*.example.org"));
        assert!(!hosts_overlap("*.example.org", "a.b.example.org"));
        assert!(!hosts_overlap("*.example.org", "example.org"));
        assert!(!hosts_overlap("a.example.org", "b.example.org"));

        let routes = [
            ("shop", "shop.example.org"),
            ("shop", "*.example.org"),
            ("blog", "blog.example.org"),
            ("blog", "blog.example.net"),
            ("evil", "blog.example.net"),
            ("api", "api.example.com"),
            ("api", "*.example.com"),
        ];
        let contested: Vec<String> = contested(routes).into_iter().collect();
        assert_eq!(
            contested,
            ["*.example.org", "blog.example.net", "blog.example.org"]
        );
    }

    #[test]
    fn admission() {
        for (host, path) in [
            ("Shop.example.org", "/"),
            ("shop_1.example.org", "/"),
            ("*.", "/"),
            ("shop.example.org", "shop"),
            ("shop.example.org", "/a?b"),
            ("shop.example.org", "/a b"),
            ("shop.example.org", "/a\tb"),
            ("shop.example.org", "/caf\u{e9}"),
            // 253 characters after the `*.`, 255 with it.
            (
                format!("*.{}.{}", vec!["a".repeat(63); 3].join("."), "a".repeat(61)).as_str(),
                "/",
            ),
        ] {
            let mut r = RouteSpec {
                host: host.into(),
                path: path.into(),
                service: "web".into(),
                port: None,
                tls: None,
            };
            assert!(r.admit().is_err(), "{host} {path}");
        }
    }

    #[test]
    fn acme() {
        let with = |host: &str, acme: &str| -> Result<RouteSpec, Invalid> {
            let mut r: RouteSpec = serde_json::from_value(serde_json::json!({
                "host": host,
                "service": "web",
                "tls": { "secret": "cert", "acme": serde_json::from_str::<serde_json::Value>(acme).unwrap() },
            }))
            .unwrap();
            r.admit().map(|()| r)
        };
        let r = with("shop.example.org", "{}").unwrap();
        let acme = r.tls.unwrap().acme.unwrap();
        assert_eq!((acme.directory.as_str(), acme.email), (LETS_ENCRYPT, None));
        with("shop.example.org", r#"{"email": "ops@example.org"}"#).unwrap();
        with(
            "shop.example.org",
            r#"{"directory": "http://127.0.0.1:14000/dir"}"#,
        )
        .unwrap();
        with(
            "shop.example.org",
            r#"{"directory": "http://localhost/dir"}"#,
        )
        .unwrap();
        for (host, acme) in [
            ("*.example.org", "{}"),
            (
                "shop.example.org",
                r#"{"directory": "http://acme.example.org/dir"}"#,
            ),
            (
                "shop.example.org",
                r#"{"directory": "http://127.0.0.1.example.org/dir"}"#,
            ),
            (
                "shop.example.org",
                r#"{"directory": "ftp://acme.example.org/"}"#,
            ),
            ("shop.example.org", r#"{"email": "ops"}"#),
            ("shop.example.org", r#"{"email": "a@b, c@d"}"#),
        ] {
            assert!(with(host, acme).is_err(), "{host} {acme}");
        }
    }
}
