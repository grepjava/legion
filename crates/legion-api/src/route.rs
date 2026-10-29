//! `Route`: HTTP requests for a host and path, sent to a service's ready
//! tasks by every node's ingress proxy (DESIGN.md §5.8).

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
}

fn root() -> String {
    "/".into()
}

impl Spec for RouteSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        let name = self.host.strip_prefix("*.").unwrap_or(&self.host);
        let labels: Vec<&str> = name.split('.').collect();
        if name.len() > 253 || labels.iter().any(|l| crate::validate_name(l).is_err()) {
            return Err(Invalid::new(
                "host",
                format!(
                    "{:?} is not a host name: lower-case labels of letters, digits and hyphens, \
                     optionally after `*.`",
                    self.host
                ),
            ));
        }
        if !self.path.starts_with('/') || self.path.contains(['?', '#']) {
            return Err(Invalid::new(
                "path",
                "a path starting with / and without ? or #",
            ));
        }
        if self.path.len() > 1 {
            self.path = self.path.trim_end_matches('/').to_owned();
        }
        check_name("service", &self.service)?;
        if self.port == Some(0) {
            return Err(Invalid::new("port", "a port above 0"));
        }
        Ok(())
    }
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

        let any = route("*.example.org", "/");
        assert_eq!(any.matches("www.example.org", "/x"), Some((false, 1)));
        assert!(any.matches("example.org", "/").is_none());
        assert!(any.matches("a.b.example.org", "/").is_none());
    }

    #[test]
    fn admission() {
        for (host, path) in [
            ("Shop.example.org", "/"),
            ("shop_1.example.org", "/"),
            ("*.", "/"),
            ("shop.example.org", "shop"),
            ("shop.example.org", "/a?b"),
        ] {
            let mut r = RouteSpec {
                host: host.into(),
                path: path.into(),
                service: "web".into(),
                port: None,
            };
            assert!(r.admit().is_err(), "{host} {path}");
        }
    }
}
