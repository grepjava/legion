//! Registry logins from a pod's `imagePullSecrets` (DESIGN.md §6.1). Each
//! Secret is a Docker config, as `docker login` writes
//! `~/.docker/config.json` and Kubernetes' `dockerconfigjson` Secrets hold:
//! `{"auths": {"<registry>": {"auth": "<base64 of user:password>"}}}`, or
//! `username` and `password` in place of `auth`.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use legion_api::Kind;
use serde::Deserialize;

use crate::control::{Assignment, Control};
use crate::runtime::Login;

/// The logins in the pod's pull secrets, in their order.
pub(crate) async fn read<C: Control>(control: &C, a: &Assignment) -> Result<Vec<Login>, String> {
    let mut logins = Vec::new();
    for name in &a.spec.pod.image_pull_secrets {
        let value = match control.data(&a.key.namespace, Kind::Secret, name).await {
            Ok(Some(v)) => v,
            Ok(None) => return Err(format!("secret {name} not found")),
            Err(e) => return Err(format!("reading secret {name}: {e}")),
        };
        logins.extend(parse(&value).map_err(|e| format!("secret {name}: {e}"))?);
    }
    Ok(logins)
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

fn parse(value: &[u8]) -> Result<Vec<Login>, String> {
    let not = |e: serde_json::Error| format!("not a Docker config: {e}");
    let mut v: serde_json::Value = serde_json::from_slice(value).map_err(not)?;
    // The older `.dockercfg` form is the `auths` map alone.
    let auths = match v.get_mut("auths") {
        Some(a) => a.take(),
        None => v,
    };
    let auths: BTreeMap<String, Entry> = serde_json::from_value(auths).map_err(not)?;
    auths
        .into_iter()
        .map(|(registry, e)| {
            let (username, password) = match (e.auth, e.username, e.password) {
                (Some(auth), _, _) if !auth.is_empty() => {
                    let pair = STANDARD
                        .decode(auth.trim())
                        .ok()
                        .and_then(|b| String::from_utf8(b).ok())
                        .ok_or_else(|| {
                            format!("{registry}: auth is not base64 of user:password")
                        })?;
                    let (u, p) = pair.split_once(':').ok_or_else(|| {
                        format!("{registry}: auth is not base64 of user:password")
                    })?;
                    (u.to_owned(), p.to_owned())
                }
                (_, Some(u), Some(p)) => (u, p),
                _ => {
                    return Err(format!(
                        "{registry}: no auth, or username and password (credential helpers \
                         and identity tokens are not supported)"
                    ));
                }
            };
            Ok(Login {
                registry,
                username,
                password,
            })
        })
        .collect()
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
        let logins = parse(config.as_bytes()).unwrap();
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
        assert_eq!(parse(old.as_bytes()).unwrap()[0].registry, "quay.io");

        for bad in [
            "not json",
            r#"{"auths": {"ghcr.io": {}}}"#,
            r#"{"auths": {"ghcr.io": {"auth": "bm9jb2xvbg=="}}}"#,
            r#"{"auths": {"ghcr.io": {"auth": "%%%"}}}"#,
        ] {
            assert!(parse(bad.as_bytes()).is_err(), "{bad}");
        }
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
}
