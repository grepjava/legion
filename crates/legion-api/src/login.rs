//! A registry login as Docker configs hold it, in `~/.docker/config.json`
//! and Kubernetes' `dockerconfigjson` Secrets: for each registry an `auth`,
//! base64 of `user:password`, or `username` and `password`. For the agent's
//! pull secrets and jailer's logins alike.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

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

#[cfg(test)]
mod tests {
    use super::decode;

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
