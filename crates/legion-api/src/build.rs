//! `Build`: an image built with jailer on a builder node, pushed to a
//! registry and, if asked, rolled out to a Service (CONSOLE.md phase 5).
//! A Build runs once; building again is another Build.

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec, check_name};

/// Nodes the operator labels so may run builds.
pub const BUILDER_LABEL: &str = "legion.builder";
/// How much of a build's log its status keeps, the end of it.
pub const LOG_KEPT: usize = 32 * 1024;
/// The most a written-out Containerfile may hold.
pub const MAX_CONTAINERFILE: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildSpec {
    pub source: BuildSource,
    /// Where to push: `registry/repository:tag`.
    pub image: String,
    /// A Secret of the namespace holding the registry's login, a Docker
    /// config, as `imagePullSecrets` hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_secret: Option<String>,
    /// Once pushed, this Service's container runs the image, pinned to
    /// the digest pushed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<BuildRollout>,
}

/// One of `git` or `containerfile`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitSource>,
    /// A Containerfile written out, built with nothing else beside it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub containerfile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitSource {
    /// `https://`, `http://` or `git://`; the repository is read as it is
    /// published, with no login.
    pub url: String,
    /// A branch or tag; the default branch when not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,
    /// The directory in the repository built, its Containerfile in it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildRollout {
    pub service: String,
    /// The container whose image changes; may be left out when the pod
    /// has only one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildPhase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl BuildPhase {
    pub fn done(self) -> bool {
        matches!(self, BuildPhase::Succeeded | BuildPhase::Failed)
    }
}

/// Written by the leader's build controller.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
    #[serde(default)]
    pub phase: BuildPhase,
    /// The node building it, once one is.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    /// Why it waits, or failed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// The manifest pushed (`sha256:…`), and the image pinned to it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub digest: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pinned: String,
    /// When it started and ended, in milliseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_ms: Option<i64>,
    /// The end of its log, once it ended ([`LOG_KEPT`] bytes at most).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub log: String,
}

impl Spec for BuildSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        match (&mut self.source.git, &self.source.containerfile) {
            (Some(git), None) => {
                let url = git.url.trim();
                let scheme = ["https://", "http://", "git://"]
                    .iter()
                    .any(|s| url.starts_with(s));
                if !scheme || url.len() > 2000 || url.chars().any(char::is_whitespace) {
                    return Err(Invalid::new(
                        "source.git.url",
                        "must be an https://, http:// or git:// address",
                    ));
                }
                git.url = url.to_owned();
                if let Some(r) = &git.r#ref
                    && (r.is_empty()
                        || r.starts_with('-')
                        || r.len() > 255
                        || !r.chars().all(|c| c.is_ascii_graphic()))
                {
                    return Err(Invalid::new("source.git.ref", "must be a branch or tag"));
                }
                if let Some(p) = &git.path
                    && (p.is_empty()
                        || p.starts_with('/')
                        || p.split('/').any(|s| s == ".." || s.is_empty())
                        || p.chars().any(|c| c.is_control()))
                {
                    return Err(Invalid::new(
                        "source.git.path",
                        "must be a directory in the repository, with no `..`",
                    ));
                }
            }
            (None, Some(c)) => {
                if c.trim().is_empty() || c.len() > MAX_CONTAINERFILE {
                    return Err(Invalid::new(
                        "source.containerfile",
                        format!("must hold a Containerfile of at most {MAX_CONTAINERFILE} bytes"),
                    ));
                }
            }
            _ => {
                return Err(Invalid::new(
                    "source",
                    "must give one of `git` or `containerfile`",
                ));
            }
        }
        if !pushable(&self.image) {
            return Err(Invalid::new(
                "image",
                "must name a registry, a repository and a tag: registry.example/team/app:2",
            ));
        }
        if let Some(s) = &self.push_secret {
            check_name("pushSecret", s)?;
        }
        if let Some(r) = &self.rollout {
            check_name("rollout.service", &r.service)?;
            if let Some(c) = &r.container {
                check_name("rollout.container", c)?;
            }
        }
        Ok(())
    }
}

/// `registry/repository:tag`: a registry host (with a dot, a port or
/// `localhost`), a repository and a tag, no digest.
pub fn pushable(image: &str) -> bool {
    if image.is_empty() || image.len() > 512 || image.contains(char::is_whitespace) {
        return false;
    }
    if image.contains('@') {
        return false;
    }
    let Some((registry, rest)) = image.split_once('/') else {
        return false;
    };
    let host_like = registry.contains('.') || registry.contains(':') || registry == "localhost";
    let Some((repo, tag)) = rest.rsplit_once(':') else {
        return false;
    };
    host_like && !repo.is_empty() && !tag.is_empty() && !tag.contains('/')
}

/// The end of `log`, at most [`LOG_KEPT`] bytes, starting on a whole line.
pub fn kept_log(log: &str) -> String {
    if log.len() <= LOG_KEPT {
        return log.to_owned();
    }
    let mut start = log.len() - LOG_KEPT;
    while !log.is_char_boundary(start) {
        start += 1;
    }
    let tail = &log[start..];
    match tail.find('\n') {
        Some(i) if i + 1 < tail.len() => tail[i + 1..].to_owned(),
        _ => tail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admit(yaml: &str) -> Result<BuildSpec, Invalid> {
        let mut s: BuildSpec = serde_yaml_ng::from_str(yaml).unwrap();
        s.admit().map(|()| s)
    }

    #[test]
    fn a_build_is_read_and_checked() {
        let s = admit(
            "source: { git: { url: \" https://git.example/shop/web \", ref: v2, path: app } }\n\
             image: registry.example:5000/shop/web:2\npushSecret: registry\n\
             rollout: { service: web }\n",
        )
        .unwrap();
        assert_eq!(s.source.git.unwrap().url, "https://git.example/shop/web");
        admit("source: { containerfile: \"FROM x\\n\" }\nimage: localhost/web:1\n").unwrap();

        for (yaml, path) in [
            ("source: {}\nimage: r.example/a:1", "source"),
            (
                "source: { containerfile: x, git: { url: \"https://g\" } }\nimage: r.example/a:1",
                "source",
            ),
            (
                "source: { git: { url: \"ssh://g/x\" } }\nimage: r.example/a:1",
                "source.git.url",
            ),
            (
                "source: { git: { url: \"https://g\", ref: \"--upload-pack=x\" } }\nimage: r.example/a:1",
                "source.git.ref",
            ),
            (
                "source: { git: { url: \"https://g\", path: \"../etc\" } }\nimage: r.example/a:1",
                "source.git.path",
            ),
            (
                "source: { containerfile: \" \" }\nimage: r.example/a:1",
                "source.containerfile",
            ),
            ("source: { containerfile: x }\nimage: web:1", "image"),
            (
                "source: { containerfile: x }\nimage: r.example/web",
                "image",
            ),
            (
                "source: { containerfile: x }\nimage: \"r.example/web@sha256:00\"",
                "image",
            ),
            (
                "source: { containerfile: x }\nimage: r.example/a:1\nrollout: { service: Web }",
                "rollout.service",
            ),
        ] {
            assert_eq!(admit(yaml).unwrap_err().path, path, "{yaml}");
        }
        assert!(pushable("r.example:5000/a/b:1"));
        assert!(!pushable("r.example/a:1/b"));
    }

    #[test]
    fn the_end_of_a_log_is_kept_from_a_whole_line() {
        assert_eq!(kept_log("a\nb\n"), "a\nb\n");
        let long = format!("{}\nlast line\n", "x".repeat(LOG_KEPT));
        assert_eq!(kept_log(&long), "last line\n");
    }
}
