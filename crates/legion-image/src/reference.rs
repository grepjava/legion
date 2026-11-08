//! Image references: `[registry/]repository[:tag][@digest]`, as Docker and
//! Podman read them.

use std::fmt;

use crate::Error;

const DOCKER_HUB: &str = "docker.io";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Reference {
    pub registry: String,
    pub repository: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl Reference {
    pub fn parse(s: &str) -> Result<Reference, Error> {
        let bad = |why: &str| Error::Reference(format!("{s:?}: {why}"));
        let (name, digest) = match s.split_once('@') {
            Some((n, d)) => {
                let hex = d
                    .strip_prefix("sha256:")
                    .ok_or_else(|| bad("only sha256 digests"))?;
                if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(bad("bad digest"));
                }
                (n, Some(d.to_ascii_lowercase()))
            }
            None => (s, None),
        };
        // A tag follows the last `:` after the last `/`; a `:` before it is
        // a registry's port.
        let slash = name.rfind('/').map_or(0, |i| i + 1);
        let (name, tag) = match name[slash..].rfind(':') {
            Some(i) => (&name[..slash + i], Some(name[slash + i + 1..].to_owned())),
            None => (name, None),
        };
        if let Some(t) = &tag {
            let ok = !t.is_empty()
                && t.len() <= 128
                && t.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b));
            if !ok {
                return Err(bad("bad tag"));
            }
        }
        let (registry, repository) = match name.split_once('/') {
            Some((first, rest)) if first.contains(['.', ':']) || first == "localhost" => {
                if !is_host(first) {
                    return Err(bad("bad registry"));
                }
                // Host names are case-blind; logins and plain-HTTP
                // registries are matched against the lowercase name.
                (first.to_ascii_lowercase(), rest.to_owned())
            }
            _ => (DOCKER_HUB.to_owned(), name.to_owned()),
        };
        let repository = if registry == DOCKER_HUB && !repository.contains('/') {
            format!("library/{repository}")
        } else {
            repository
        };
        let ok = !repository.is_empty()
            && repository.split('/').all(|c| {
                !c.is_empty()
                    && c.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b)
                    })
            });
        if !ok {
            return Err(bad("bad repository name"));
        }
        let tag = match (&tag, &digest) {
            (None, None) => Some("latest".to_owned()),
            _ => tag,
        };
        Ok(Reference {
            registry,
            repository,
            tag,
            digest,
        })
    }

    /// The host to talk to: Docker Hub's API is not at `docker.io`.
    pub fn host(&self) -> &str {
        if self.registry == DOCKER_HUB {
            "registry-1.docker.io"
        } else {
            &self.registry
        }
    }

    /// What to ask the registry for: the digest if there is one.
    pub fn target(&self) -> &str {
        self.digest
            .as_deref()
            .or(self.tag.as_deref())
            .unwrap_or("latest")
    }

    /// The same image by `digest` (`sha256:<hex>`), with no tag: what a
    /// deployment pins and a release record names.
    pub fn by_digest(&self, digest: &str) -> Reference {
        Reference {
            tag: None,
            digest: Some(digest.to_owned()),
            ..self.clone()
        }
    }

    /// `localhost/...` names images built or loaded on the node, which no
    /// registry serves (Podman's convention).
    pub fn is_local(&self) -> bool {
        self.registry == "localhost"
    }

    /// Parses a name for an image built or loaded here: without a registry
    /// (`app:1`, `me/app`) it is a `localhost/` name, as Podman's are;
    /// with one (`ghcr.io/me/app:1`, `localhost/app`) it is taken as is.
    pub fn parse_local_tag(s: &str) -> Result<Reference, Error> {
        let slash = s.rfind('/').map_or(0, |i| i + 1);
        let before_tag = match s[slash..].rfind(':') {
            Some(i) => &s[..slash + i],
            None => s,
        };
        let has_registry = match before_tag.split_once('/') {
            Some((first, _)) => first.contains(['.', ':']) || first == "localhost",
            None => false,
        };
        if has_registry {
            Self::parse(s)
        } else {
            Self::parse(&format!("localhost/{s}"))
        }
    }
}

/// `host[:port]`: dot-separated labels of letters, digits and inner `-`,
/// or a bracketed IPv6 address.
fn is_host(s: &str) -> bool {
    let (host, port) = match s.strip_prefix('[') {
        Some(v6) => match v6.split_once(']') {
            Some((addr, rest)) if addr.parse::<std::net::Ipv6Addr>().is_ok() => {
                ("", rest.strip_prefix(':').or(rest.is_empty().then_some("")))
            }
            _ => return false,
        },
        None => match s.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (s, Some("")),
        },
    };
    let Some(port) = port else {
        return false;
    };
    // Digits only: `u16`'s parse takes a leading `+` too.
    let port_ok = port.is_empty()
        || (port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok_and(|p| p > 0));
    let host_ok = s.starts_with('[')
        || (!host.is_empty()
            && host.split('.').all(|l| {
                !l.is_empty()
                    && !l.starts_with('-')
                    && !l.ends_with('-')
                    && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }));
    port_ok && host_ok && !s.ends_with(':')
}

/// The registry a Docker config's `auths` key names, as a [`Reference`]
/// names it: `https://index.docker.io/v1/` is `docker.io`,
/// `https://ghcr.io` is `ghcr.io`.
pub fn registry_of(key: &str) -> String {
    let key = key
        .strip_prefix("https://")
        .or_else(|| key.strip_prefix("http://"))
        .unwrap_or(key);
    let host = key.split('/').next().unwrap_or("").to_ascii_lowercase();
    match host.as_str() {
        "index.docker.io" | "registry-1.docker.io" | "registry.hub.docker.com" => {
            DOCKER_HUB.to_owned()
        }
        _ => host,
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.registry, self.repository)?;
        if let Some(t) = &self.tag {
            write!(f, ":{t}")?;
        }
        if let Some(d) = &self.digest {
            write!(f, "@{d}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_image_by_digest_drops_its_tag() {
        let d = format!("sha256:{}", "a".repeat(64));
        let r = Reference::parse("ghcr.io/me/app:1").unwrap();
        assert_eq!(r.by_digest(&d).to_string(), format!("ghcr.io/me/app@{d}"));
        let hub = Reference::parse("nginx").unwrap();
        assert_eq!(
            hub.by_digest(&d).to_string(),
            format!("docker.io/library/nginx@{d}")
        );
        assert_eq!(
            Reference::parse(&r.by_digest(&d).to_string()).unwrap(),
            r.by_digest(&d)
        );
    }

    fn canonical(s: &str) -> String {
        Reference::parse(s).unwrap().to_string()
    }

    #[test]
    fn registries_of_docker_config_keys() {
        for (key, registry) in [
            ("https://index.docker.io/v1/", "docker.io"),
            ("registry-1.docker.io", "docker.io"),
            ("docker.io", "docker.io"),
            ("https://ghcr.io", "ghcr.io"),
            ("Registry.Example.com:5000/v2/", "registry.example.com:5000"),
            ("http://localhost:5000", "localhost:5000"),
        ] {
            assert_eq!(registry_of(key), registry, "{key}");
        }
        assert_eq!(
            Reference::parse("nginx").unwrap().registry,
            registry_of("https://index.docker.io/v1/")
        );
    }

    #[test]
    fn local_tags() {
        let local = |s: &str| Reference::parse_local_tag(s).unwrap().to_string();
        assert_eq!(local("myapp"), "localhost/myapp:latest");
        assert_eq!(local("myapp:1.0"), "localhost/myapp:1.0");
        assert_eq!(local("me/app"), "localhost/me/app:latest");
        assert_eq!(local("localhost/app:2"), "localhost/app:2");
        assert_eq!(local("ghcr.io/me/app:v1"), "ghcr.io/me/app:v1");
        assert_eq!(local("registry:5000/app"), "registry:5000/app:latest");
    }

    #[test]
    fn references_as_podman_reads_them() {
        assert_eq!(canonical("nginx"), "docker.io/library/nginx:latest");
        assert_eq!(
            canonical("freebsd/freebsd-runtime:15.1"),
            "docker.io/freebsd/freebsd-runtime:15.1"
        );
        assert_eq!(
            canonical("docker.io/freebsd/freebsd-runtime:15.1"),
            "docker.io/freebsd/freebsd-runtime:15.1"
        );
        assert_eq!(
            canonical("localhost/legion-pg:17"),
            "localhost/legion-pg:17"
        );
        assert_eq!(
            canonical("registry.example:5000/team/app"),
            "registry.example:5000/team/app:latest"
        );
        let d = format!("sha256:{}", "a".repeat(64));
        let r = Reference::parse(&format!("ghcr.io/x/y:1@{d}")).unwrap();
        assert_eq!(r.target(), d);
        assert_eq!(r.tag.as_deref(), Some("1"));
        let r = Reference::parse(&format!("ghcr.io/x/y@{d}")).unwrap();
        assert_eq!(r.tag, None);
        assert_eq!(r.host(), "ghcr.io");
        assert_eq!(
            Reference::parse("alpine").unwrap().host(),
            "registry-1.docker.io"
        );
        assert!(Reference::parse("localhost/x").unwrap().is_local());
        assert!(!Reference::parse("localhost:5000/x").unwrap().is_local());
        for bad in [
            "",
            "Upper/case",
            "x:",
            "x:bad tag",
            "x@sha256:abc",
            "x@md5:aa",
            "a//b",
            "./app",
            "../app",
            ":/app",
            "a..b/app",
            "-x.io/app",
            "x.io:/app",
            "x.io:http/app",
            "x.io:0/app",
            "x.io:65536/app",
            "x.io:99999/app",
            "x.io:+5000/app",
            "[::1/app",
            "[nope]:5000/app",
        ] {
            assert!(Reference::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn registries_are_hosts_in_lowercase() {
        assert_eq!(canonical("GHCR.IO/org/app:1"), "ghcr.io/org/app:1");
        assert_eq!(
            Reference::parse("Registry.Example.com:5000/v2/app")
                .unwrap()
                .registry,
            registry_of("Registry.Example.com:5000/v2/")
        );
        assert_eq!(canonical("[::1]:5000/app"), "[::1]:5000/app:latest");
        assert_eq!(canonical("[fd00::1]/app"), "[fd00::1]/app:latest");
        assert_eq!(canonical("127.0.0.1:5000/app"), "127.0.0.1:5000/app:latest");
    }
}
