//! A registry login as Docker configs hold it, in `~/.docker/config.json`
//! and Kubernetes' `dockerconfigjson` Secrets: for each registry an `auth`,
//! base64 of `user:password`, or `username` and `password`. For the agent's
//! pull secrets, the manager's resolving of tags and jailer's logins alike.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;

/// A registry account, from a pod's `imagePullSecrets`.
#[derive(Clone, PartialEq, Eq)]
pub struct Login {
    /// As a Docker config's `auths` names it: `ghcr.io`,
    /// `https://index.docker.io/v1/`.
    pub registry: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("registry", &self.registry)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Entry {
    #[serde(default)]
    auth: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

/// Every login in a Docker config, `{"auths": {"<registry>": {...}}}`, or
/// the older `.dockercfg` form, the `auths` map alone. One entry it can't
/// read refuses the whole config.
pub fn docker_config(value: &[u8]) -> Result<Vec<Login>, String> {
    let not = |e: serde_json::Error| format!("not a Docker config: {e}");
    let mut v: serde_json::Value = serde_json::from_slice(value).map_err(not)?;
    let auths = match v.get_mut("auths") {
        Some(a) => a.take(),
        None => v,
    };
    let auths: BTreeMap<String, Entry> = serde_json::from_value(auths).map_err(not)?;
    auths
        .into_iter()
        .map(|(registry, e)| {
            let (username, password) = decode(
                e.auth.as_deref(),
                e.username.as_deref(),
                e.password.as_deref(),
            )
            .map_err(|why| format!("{registry}: {why}"))?;
            Ok(Login {
                registry,
                username,
                password,
            })
        })
        .collect()
}

/// The username and password of one entry. `auth` wins, as it does for
/// Docker, when both forms are there. A caller reading a whole config may
/// pass over an entry this refuses, or refuse the config.
pub fn decode(
    auth: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(String, String), String> {
    if let Some(auth) = auth.filter(|a| !a.is_empty()) {
        let bad = || "auth is not base64 of user:password".to_owned();
        let pair = STANDARD
            .decode(auth.trim())
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(bad)?;
        let (u, p) = pair.split_once(':').ok_or_else(bad)?;
        return Ok((u.to_owned(), p.to_owned()));
    }
    match (username, password) {
        (Some(u), Some(p)) => Ok((u.to_owned(), p.to_owned())),
        _ => Err(
            "no auth, or username and password (credential helpers and identity tokens \
             are not supported)"
                .to_owned(),
        ),
    }
}

/// Every login in a Docker config that can be read, as a person's
/// `~/.docker/config.json` holds them: an entry with nothing usable (Docker
/// Desktop's with `credsStore`, or an identity token alone) is passed over,
/// and a lone username or password is taken with the other empty, as
/// configs written by hand have it. Only a file that isn't a Docker config
/// is refused. For logins a person keeps; a pod's pull secrets are read
/// with [`docker_config`], which refuses what it can't use.
pub fn docker_config_lenient(value: &[u8]) -> Result<Vec<Login>, String> {
    let not = |e: serde_json::Error| format!("not a Docker config: {e}");
    let mut v: serde_json::Value = serde_json::from_slice(value).map_err(not)?;
    let auths = match v.get_mut("auths") {
        Some(a) => a.take(),
        None => v,
    };
    let auths: BTreeMap<String, Entry> = serde_json::from_value(auths).map_err(not)?;
    Ok(auths
        .into_iter()
        .filter_map(|(registry, e)| {
            let lone = |one: &Option<String>, other: &Option<String>| {
                one.clone()
                    .or_else(|| other.as_ref().map(|_| String::new()))
            };
            let (username, password) = decode(
                e.auth.as_deref(),
                lone(&e.username, &e.password).as_deref(),
                lone(&e.password, &e.username).as_deref(),
            )
            .ok()?;
            Some(Login {
                registry,
                username,
                password,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_configs() {
        let auth = STANDARD.encode("bot:s3cr:et");
        let config = format!(
            r#"{{"auths": {{"ghcr.io": {{"auth": "{auth}", "email": "x@y"}},
                "https://index.docker.io/v1/": {{"username": "me", "password": "pw"}}}},
                "credsStore": "desktop"}}"#
        );
        let logins = docker_config(config.as_bytes()).unwrap();
        assert_eq!(
            logins
                .iter()
                .map(|l| (&*l.registry, &*l.username, &*l.password))
                .collect::<Vec<_>>(),
            [
                ("ghcr.io", "bot", "s3cr:et"),
                ("https://index.docker.io/v1/", "me", "pw")
            ]
        );
        let old = format!(r#"{{"quay.io": {{"auth": "{auth}"}}}}"#);
        assert_eq!(
            docker_config(old.as_bytes()).unwrap()[0].registry,
            "quay.io"
        );

        for bad in [
            "not json",
            r#"{"auths": {"ghcr.io": {}}}"#,
            r#"{"auths": {"ghcr.io": {"auth": "bm9jb2xvbg=="}}}"#,
            r#"{"auths": {"ghcr.io": {"auth": "%%%"}}}"#,
        ] {
            assert!(docker_config(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_persons_config_keeps_what_it_can_use() {
        let auth = STANDARD.encode("bot:pw");
        let config = format!(
            r#"{{"auths": {{
                "https://index.docker.io/v1/": {{}},
                "quay.io": {{"identitytoken": "t"}},
                "ghcr.io": {{"auth": "{auth}"}},
                "me.example": {{"username": "me"}},
                "pw.example": {{"password": "secret"}},
                "bad.example": {{"auth": "%%%"}}}},
                "credsStore": "desktop"}}"#
        );
        // A pod's secret refuses it whole.
        assert!(docker_config(config.as_bytes()).is_err());
        let logins = docker_config_lenient(config.as_bytes()).unwrap();
        assert_eq!(
            logins
                .iter()
                .map(|l| (&*l.registry, &*l.username, &*l.password))
                .collect::<Vec<_>>(),
            [
                ("ghcr.io", "bot", "pw"),
                ("me.example", "me", ""),
                ("pw.example", "", "secret"),
            ]
        );
        assert!(docker_config_lenient(b"not json").is_err());
    }

    #[test]
    fn logins_hide_passwords() {
        let l = Login {
            registry: "ghcr.io".into(),
            username: "bot".into(),
            password: "hunter2".into(),
        };
        assert!(!format!("{l:?}").contains("hunter2"));
    }

    #[test]
    fn entries() {
        let pair = |u: &str, p: &str| Ok((u.to_owned(), p.to_owned()));
        // "bob:s3:cret": only the first colon separates.
        let auth = "Ym9iOnMzOmNyZXQ=";
        assert_eq!(decode(Some(auth), None, None), pair("bob", "s3:cret"));
        assert_eq!(
            decode(Some(auth), Some("x"), Some("y")),
            pair("bob", "s3:cret")
        );
        assert_eq!(decode(Some(""), Some("x"), Some("y")), pair("x", "y"));
        assert_eq!(decode(None, Some("x"), Some("y")), pair("x", "y"));
        assert!(decode(Some("not base64!"), Some("x"), Some("y")).is_err());
        assert!(decode(Some("bm9jb2xvbg=="), None, None).is_err());
        assert!(decode(None, Some("x"), None).is_err());
        assert!(decode(None, None, None).is_err());
    }
}
