//! The web console's settings for a cluster (CONSOLE.md phase 6): its name
//! and a banner every page shows, the sign-in page too; links in the side
//! bar; the other clusters' consoles; the namespace a user starts in; how
//! long a session lasts. Set by the cluster admin (`legion console
//! configure`), read by anyone signed in.

use serde::{Deserialize, Serialize};

use crate::admit::Invalid;
use crate::quantity::Duration;

/// Sessions last this long unless the settings say otherwise.
pub const DEFAULT_SESSION: Duration = Duration::from_secs(12 * 3600);
/// The shortest and longest session the settings may ask for.
pub const SHORTEST_SESSION: Duration = Duration::from_secs(5 * 60);
pub const LONGEST_SESSION: Duration = Duration::from_secs(7 * 24 * 3600);
/// Most links in the side bar, and most other clusters.
pub const MAX_LINKS: usize = 20;
const MAX_TEXT: usize = 500;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Settings {
    /// This cluster's name as the console shows it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub banner: Option<Banner>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
    /// The other clusters' consoles, each signed into on its own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<Peer>,
    /// The namespace the console opens in; every one the user may read
    /// when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_namespace: Option<String>,
    /// How long a session lasts at most; it never outlasts the
    /// certificate it was signed in with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Banner {
    pub text: String,
    #[serde(default)]
    pub level: Level,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    #[default]
    Info,
    Warning,
    Danger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub label: String,
    /// `https://` or `http://` only.
    pub url: String,
}

/// Another cluster's console.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub name: String,
    /// Its address as browsers reach it, `https://`.
    pub url: String,
}

impl Peer {
    /// The origin its pages are served from, as a browser's `Origin`
    /// header names it: scheme, host and a port other than the default.
    pub fn origin(&self) -> Option<String> {
        let rest = self
            .url
            .strip_prefix("https://")
            .or_else(|| self.url.strip_prefix("http://"))?;
        let scheme = &self.url[..self.url.len() - rest.len()];
        let host = rest.split(['/', '?', '#']).next()?.to_ascii_lowercase();
        if host.is_empty() || host.contains('@') {
            return None;
        }
        let host = match scheme {
            "https://" => host.strip_suffix(":443").unwrap_or(&host).to_owned(),
            _ => host.strip_suffix(":80").unwrap_or(&host).to_owned(),
        };
        Some(format!("{scheme}{host}"))
    }
}

impl Settings {
    /// What a browser not yet signed in may see: the name and banner.
    pub fn public(&self) -> Settings {
        Settings {
            name: self.name.clone(),
            banner: self.banner.clone(),
            ..Default::default()
        }
    }

    /// How long a session signed in now lasts at most.
    pub fn session(&self) -> Duration {
        self.session.unwrap_or(DEFAULT_SESSION)
    }

    /// Whether a page from `origin` is another listed cluster's console.
    pub fn lists(&self, origin: &str) -> bool {
        self.clusters
            .iter()
            .any(|p| p.origin().is_some_and(|o| o.eq_ignore_ascii_case(origin)))
    }

    pub fn check(&self) -> Result<(), Invalid> {
        if let Some(n) = &self.name {
            text("name", n)?;
        }
        if let Some(b) = &self.banner {
            text("banner.text", &b.text)?;
        }
        if self.links.len() > MAX_LINKS {
            return Err(Invalid::new("links", format!("at most {MAX_LINKS} links")));
        }
        for (i, l) in self.links.iter().enumerate() {
            text(&format!("links[{i}].label"), &l.label)?;
            let url = l.url.trim();
            let scheme_ok = url.starts_with("https://") || url.starts_with("http://");
            if !scheme_ok || url.len() > 2000 || url.chars().any(char::is_whitespace) {
                return Err(Invalid::new(
                    format!("links[{i}].url"),
                    "must be an http:// or https:// address",
                ));
            }
        }
        if self.clusters.len() > MAX_LINKS {
            return Err(Invalid::new(
                "clusters",
                format!("at most {MAX_LINKS} clusters"),
            ));
        }
        for (i, p) in self.clusters.iter().enumerate() {
            text(&format!("clusters[{i}].name"), &p.name)?;
            if !secure_url(&p.url) || p.origin().is_none() {
                return Err(Invalid::new(
                    format!("clusters[{i}].url"),
                    "must be an https:// address (http:// only on this host, for tests)",
                ));
            }
        }
        if let Some(ns) = &self.default_namespace {
            crate::validate_name(ns)
                .map_err(|e| Invalid::new("defaultNamespace", e.to_string()))?;
        }
        if let Some(s) = self.session
            && !(SHORTEST_SESSION..=LONGEST_SESSION).contains(&s)
        {
            return Err(Invalid::new("session", "must be between 5m and 168h"));
        }
        Ok(())
    }
}

/// Single sign-on (CONSOLE.md phase 6): an OpenID Connect provider the
/// console signs users in with. Its client secret is kept apart and never
/// shown again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Oidc {
    /// The provider's issuer URL, as its ID tokens say it.
    pub issuer: String,
    pub client_id: String,
    /// The console's address as browsers reach it, e.g.
    /// `https://console.example:7480`; the provider sends them back to
    /// `<url>/api/oidc/callback`, which it must allow.
    pub console_url: String,
    /// The claim naming the user, who is then `oidc:<it>`.
    #[serde(default = "email")]
    pub username_claim: String,
    /// The claim listing the user's groups.
    #[serde(default = "groups")]
    pub groups_claim: String,
    #[serde(default = "scopes")]
    pub scopes: Vec<String>,
    /// What the sign-in button says.
    #[serde(default = "label")]
    pub label: String,
}

fn email() -> String {
    "email".into()
}
fn groups() -> String {
    "groups".into()
}
fn scopes() -> Vec<String> {
    ["openid", "email", "profile"].map(String::from).to_vec()
}
fn label() -> String {
    "Sign in with single sign-on".into()
}

impl Oidc {
    /// Where the provider sends the browser back.
    pub fn redirect_uri(&self) -> String {
        format!(
            "{}/api/oidc/callback",
            self.console_url.trim_end_matches('/')
        )
    }

    pub fn check(&self) -> Result<(), Invalid> {
        for (path, url) in [("issuer", &self.issuer), ("consoleUrl", &self.console_url)] {
            if !secure_url(url) {
                return Err(Invalid::new(
                    path,
                    "must be an https:// address (http:// only on this host, for tests)",
                ));
            }
        }
        text("clientId", &self.client_id)?;
        text("usernameClaim", &self.username_claim)?;
        text("groupsClaim", &self.groups_claim)?;
        text("label", &self.label)?;
        if !self.scopes.iter().any(|s| s == "openid") {
            return Err(Invalid::new("scopes", "must include openid"));
        }
        for (i, s) in self.scopes.iter().enumerate() {
            if s.is_empty() || !s.chars().all(|c| c.is_ascii_graphic()) {
                return Err(Invalid::new(format!("scopes[{i}]"), "must be one word"));
            }
        }
        Ok(())
    }
}

/// `https://`, or `http://` to this host alone (a provider played by a
/// test).
pub fn secure_url(url: &str) -> bool {
    let plain_local = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
        .iter()
        .any(|p| {
            url.strip_prefix(p)
                .is_some_and(|r| r.is_empty() || r.starts_with([':', '/']))
        });
    (url.starts_with("https://") || plain_local)
        && url.len() <= 2000
        && !url.chars().any(|c| c.is_whitespace() || c.is_control())
}

fn text(path: &str, t: &str) -> Result<(), Invalid> {
    if t.trim().is_empty() {
        return Err(Invalid::new(path, "must not be empty"));
    }
    if t.chars().count() > MAX_TEXT || t.chars().any(char::is_control) {
        return Err(Invalid::new(
            path,
            format!("at most {MAX_TEXT} characters on one line"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(yaml: &str) -> Settings {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn settings_read_and_checked() {
        let s = read(
            "banner: { text: Production, level: danger }\n\
             links: [{ label: Runbook, url: \"https://wiki.example/run\" }]\n\
             defaultNamespace: shop\nsession: 8h\n",
        );
        s.check().unwrap();
        assert_eq!(s.session(), Duration::from_secs(8 * 3600));
        assert_eq!(s.banner.as_ref().unwrap().level, Level::Danger);
        assert_eq!(
            s.public(),
            Settings {
                banner: s.banner.clone(),
                ..Default::default()
            }
        );
        assert_eq!(Settings::default().session(), DEFAULT_SESSION);
        assert_eq!(
            read("banner: { text: hi }").banner.unwrap().level,
            Level::Info
        );

        for (yaml, path) in [
            ("banner: { text: \" \" }", "banner.text"),
            ("banner: { text: \"a\\nb\" }", "banner.text"),
            (
                "links: [{ label: x, url: \"javascript:alert(1)\" }]",
                "links[0].url",
            ),
            (
                "links: [{ label: x, url: \"https://a b\" }]",
                "links[0].url",
            ),
            ("defaultNamespace: Shop", "defaultNamespace"),
            ("session: 1m", "session"),
            ("session: 200h", "session"),
        ] {
            assert_eq!(read(yaml).check().unwrap_err().path, path, "{yaml}");
        }
        assert!(serde_yaml_ng::from_str::<Settings>("theme: dark").is_err());
    }

    #[test]
    fn other_clusters_are_listed_by_origin() {
        let s = read(
            "name: prod-east\n\
             clusters:\n\
             - { name: prod-west, url: \"https://West.console.example:7480/\" }\n\
             - { name: lab, url: \"https://lab.console.example:443/k/Service\" }\n",
        );
        s.check().unwrap();
        assert_eq!(s.public().name.as_deref(), Some("prod-east"));
        assert!(s.public().clusters.is_empty());
        assert!(s.lists("https://west.console.example:7480"));
        assert!(s.lists("https://lab.console.example"));
        for other in [
            "https://west.console.example",
            "http://west.console.example:7480",
            "https://evil.example",
            "null",
        ] {
            assert!(!s.lists(other), "{other}");
        }
        for (yaml, path) in [
            (
                "clusters: [{ name: x, url: \"http://x.example\" }]",
                "clusters[0].url",
            ),
            (
                "clusters: [{ name: x, url: \"https://\" }]",
                "clusters[0].url",
            ),
            (
                "clusters: [{ name: \"\", url: \"https://x\" }]",
                "clusters[0].name",
            ),
            ("name: \" \"", "name"),
        ] {
            assert_eq!(read(yaml).check().unwrap_err().path, path, "{yaml}");
        }
    }

    #[test]
    fn oidc_is_read_with_defaults_and_checked() {
        let o: Oidc = serde_yaml_ng::from_str(
            "issuer: https://idp.example\nclientId: legion\nconsoleUrl: https://console.example:7480/\n",
        )
        .unwrap();
        o.check().unwrap();
        assert_eq!(o.username_claim, "email");
        assert_eq!(o.groups_claim, "groups");
        assert_eq!(o.scopes, ["openid", "email", "profile"]);
        assert_eq!(
            o.redirect_uri(),
            "https://console.example:7480/api/oidc/callback"
        );
        let bad = |f: &dyn Fn(&mut Oidc)| {
            let mut b = o.clone();
            f(&mut b);
            b.check().unwrap_err().path
        };
        assert_eq!(bad(&|b| b.issuer = "http://idp.example".into()), "issuer");
        assert_eq!(
            bad(&|b| b.issuer = "http://localhost.evil".into()),
            "issuer"
        );
        assert_eq!(bad(&|b| b.scopes = vec!["email".into()]), "scopes");
        assert_eq!(bad(&|b| b.client_id = String::new()), "clientId");
        assert!(secure_url("http://127.0.0.1:5556/dex"));
        assert!(secure_url("http://localhost"));
    }
}
