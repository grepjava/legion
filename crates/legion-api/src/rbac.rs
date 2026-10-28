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

/// Spec of a `RoleBinding`: `user` (the name in their certificate) has
/// `role` in the binding's namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoleBindingSpec {
    pub user: String,
    pub role: NamespaceRole,
}

impl Spec for RoleBindingSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        crate::validate_name(&self.user).map_err(|e| Invalid::new("user", e.to_string()))
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
            user: "Alice!".into(),
            role: NamespaceRole::Edit,
        };
        assert_eq!(bad.admit().unwrap_err().path, "user");
        assert!(
            serde_json::from_value::<RoleBindingSpec>(serde_json::json!({"user": "a"})).is_err(),
            "the role is required"
        );
    }
}
