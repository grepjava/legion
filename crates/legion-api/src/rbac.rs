//! Who may do what in a namespace (DESIGN.md §4.2, RBAC): a `RoleBinding`
//! gives one user one of three built-in roles there.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};

/// A role in one namespace. Each includes the ones before it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceRole {
    /// Reads every object but Secrets.
    #[default]
    View,
    /// Also reads Secrets, and creates, changes and deletes objects.
    Edit,
    /// Also manages the namespace's RoleBindings.
    Admin,
}

impl NamespaceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            NamespaceRole::View => "view",
            NamespaceRole::Edit => "edit",
            NamespaceRole::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<NamespaceRole> {
        [
            NamespaceRole::View,
            NamespaceRole::Edit,
            NamespaceRole::Admin,
        ]
        .into_iter()
        .find(|r| r.as_str() == s)
    }
}

impl fmt::Display for NamespaceRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What an OIDC user's name starts with (CONSOLE.md phase 6): no name a
/// provider gives can then be a certificate user's, which are DNS labels.
pub const OIDC_PREFIX: &str = "oidc:";
/// Longest group, or OIDC user's name, after its prefix.
const MAX_NAME: usize = 256;

/// Spec of a `RoleBinding`: a `user` (the name in their certificate, or
/// `oidc:` and what their identity provider calls them) or a `group` (as an
/// identity provider names it) has `role` in the binding's namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoleBindingSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub role: NamespaceRole,
}

impl RoleBindingSpec {
    /// Whether it binds `user`, who is in `groups`.
    pub fn binds(&self, user: &str, groups: &[String]) -> bool {
        self.user.as_deref() == Some(user)
            || self.group.as_ref().is_some_and(|g| groups.contains(g))
    }
}

/// A name a provider gives, a group's or after `oidc:`: some text on one
/// line.
fn check_text(path: &str, t: &str) -> Result<(), Invalid> {
    if t.trim().is_empty() || t.chars().count() > MAX_NAME || t.chars().any(char::is_control) {
        return Err(Invalid::new(
            path,
            format!("must be 1 to {MAX_NAME} characters on one line"),
        ));
    }
    Ok(())
}

/// A user a binding may name: a certificate's DNS label, or `oidc:` and
/// a provider's name for them.
pub fn check_user(path: &str, user: &str) -> Result<(), Invalid> {
    match user.strip_prefix(OIDC_PREFIX) {
        Some(rest) => check_text(path, rest),
        None => crate::validate_name(user).map_err(|e| Invalid::new(path, e.to_string())),
    }
}

impl Spec for RoleBindingSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        match (&self.user, &self.group) {
            (Some(u), None) => check_user("user", u),
            (None, Some(g)) => check_text("group", g),
            _ => Err(Invalid::new("", "name exactly one of `user` and `group`")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_include_the_ones_before() {
        assert!(NamespaceRole::View < NamespaceRole::Edit);
        assert!(NamespaceRole::Edit < NamespaceRole::Admin);
        assert_eq!(NamespaceRole::parse("edit"), Some(NamespaceRole::Edit));
        assert_eq!(NamespaceRole::parse("owner"), None);
    }

    #[test]
    fn bindings_name_a_user() {
        let mut ok: RoleBindingSpec =
            serde_json::from_value(serde_json::json!({"user": "alice", "role": "view"})).unwrap();
        ok.admit().unwrap();
        let mut bad = RoleBindingSpec {
            user: Some("Alice!".into()),
            group: None,
            role: NamespaceRole::Edit,
        };
        assert_eq!(bad.admit().unwrap_err().path, "user");
        assert!(
            serde_json::from_value::<RoleBindingSpec>(serde_json::json!({"user": "a"})).is_err(),
            "the role is required"
        );
    }

    #[test]
    fn bindings_name_an_oidc_user_or_a_group() {
        let spec = |v| serde_json::from_value::<RoleBindingSpec>(v).unwrap();
        let mut b = spec(serde_json::json!({"user": "oidc:alice@example.com", "role": "edit"}));
        b.admit().unwrap();
        assert!(b.binds("oidc:alice@example.com", &[]));
        assert!(!b.binds("alice", &[]));
        let mut g = spec(serde_json::json!({"group": "Shop Team", "role": "view"}));
        g.admit().unwrap();
        assert!(g.binds("oidc:bob@example.com", &["x".into(), "Shop Team".into()]));
        assert!(!g.binds("oidc:bob@example.com", &["shop team".into()]));
        assert_eq!(
            serde_json::to_value(&g).unwrap(),
            serde_json::json!({"group": "Shop Team", "role": "view"})
        );

        for (v, path) in [
            (serde_json::json!({"role": "view"}), ""),
            (
                serde_json::json!({"user": "a", "group": "g", "role": "view"}),
                "",
            ),
            (serde_json::json!({"user": "oidc:", "role": "view"}), "user"),
            (
                serde_json::json!({"user": "oidc:a\nb", "role": "view"}),
                "user",
            ),
            (serde_json::json!({"group": " ", "role": "view"}), "group"),
        ] {
            assert_eq!(spec(v.clone()).admit().unwrap_err().path, path, "{v}");
        }
    }
}
