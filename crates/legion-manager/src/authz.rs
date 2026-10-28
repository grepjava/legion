//! RBAC (DESIGN.md §4.2): what an API caller may do. The manager a request
//! reaches first decides, before forwarding it; the leader then sees the
//! forwarding manager, which may do everything.
//!
//! The cluster admin and managers may do everything. A user may act only
//! in namespaces where a RoleBinding names them, as far as its role goes,
//! and never on cluster-scoped objects. Rights live in the bindings, not
//! the certificate, so deleting a binding takes them away at once.

use std::fmt;

use legion_api::Kind;
use legion_api::rbac::{NamespaceRole, RoleBindingSpec};
use legion_pki::{Identity, Role};
use legion_store::Store;
use tonic::{Request, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verb {
    /// get, list, watch
    Read,
    /// apply, delete
    Write,
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Verb::Read => "read",
            Verb::Write => "change",
        })
    }
}

/// An API caller, as far as rights go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    /// The cluster admin, a manager, or anyone without TLS (tests only).
    All,
    /// A user, by the name in their certificate.
    User(String),
}

impl Caller {
    pub fn of<T>(req: &Request<T>, tls: bool) -> Result<Caller, Status> {
        match crate::tls::caller(req, tls)? {
            None => Ok(Caller::All),
            Some(Identity {
                role: Role::Admin | Role::Manager,
                ..
            }) => Ok(Caller::All),
            Some(Identity {
                role: Role::User,
                name,
            }) => Ok(Caller::User(name)),
            Some(id) => Err(Status::permission_denied(format!(
                "{id} may not call the API"
            ))),
        }
    }

    /// Only the cluster admin and managers.
    pub fn require_all(&self, what: &str) -> Result<(), Status> {
        match self {
            Caller::All => Ok(()),
            Caller::User(name) => Err(Status::permission_denied(format!(
                "user {name} may not {what}: only the cluster admin may"
            ))),
        }
    }

    /// Whether the caller may `verb` objects of `kind` in `namespace`.
    pub fn check(
        &self,
        store: &Store,
        verb: Verb,
        kind: Kind,
        namespace: &str,
    ) -> Result<(), Status> {
        let Caller::User(name) = self else {
            return Ok(());
        };
        if kind.is_cluster_scoped() {
            return Err(Status::permission_denied(format!(
                "user {name} may not {verb} {kind}s: they are cluster-scoped"
            )));
        }
        let need = needed(verb, kind);
        match role_in(store, namespace, name)? {
            Some(role) if role >= need => Ok(()),
            Some(role) => Err(Status::permission_denied(format!(
                "user {name} has role {role} in namespace {namespace}; \
                 to {verb} {kind}s takes {need}"
            ))),
            None => Err(Status::permission_denied(format!(
                "user {name} has no role in namespace {namespace}"
            ))),
        }
    }

    /// [`Caller::check`] as a yes or no, for filtering lists and watches.
    pub fn may(&self, store: &Store, verb: Verb, kind: Kind, namespace: &str) -> bool {
        self.check(store, verb, kind, namespace).is_ok()
    }
}

/// The role it takes to `verb` objects of `kind`.
fn needed(verb: Verb, kind: Kind) -> NamespaceRole {
    match (verb, kind) {
        (Verb::Read, Kind::Secret) => NamespaceRole::Edit,
        (Verb::Read, _) => NamespaceRole::View,
        (Verb::Write, Kind::RoleBinding) => NamespaceRole::Admin,
        (Verb::Write, _) => NamespaceRole::Edit,
    }
}

/// The highest role RoleBindings give `user` in `namespace`.
fn role_in(store: &Store, namespace: &str, user: &str) -> Result<Option<NamespaceRole>, Status> {
    let bindings = store
        .list(Kind::RoleBinding, Some(namespace))
        .map_err(|e| Status::internal(e.to_string()))?;
    Ok(bindings
        .objects
        .iter()
        .filter_map(|o| legion_api::parse::<RoleBindingSpec>(&o.spec).ok())
        .filter(|b| b.user == user)
        .map(|b| b.role)
        .max())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_needed() {
        assert_eq!(needed(Verb::Read, Kind::Service), NamespaceRole::View);
        assert_eq!(needed(Verb::Read, Kind::Secret), NamespaceRole::Edit);
        assert_eq!(needed(Verb::Write, Kind::Service), NamespaceRole::Edit);
        assert_eq!(needed(Verb::Write, Kind::RoleBinding), NamespaceRole::Admin);
    }
}
