//! RBAC (DESIGN.md §4.2): what an API caller may do. A follower forwards a
//! user's request with the user's name and the leader decides, on its own
//! state, which a follower's may trail; what a follower serves itself
//! (watches, sessions) it serves once it has caught up with the leader.
//!
//! The cluster admin and managers may do everything. A user may act only
//! in namespaces where a RoleBinding names them or a group of theirs (an
//! OIDC user's, as of their last sign-in), as far as its role goes,
//! and never on cluster-scoped objects. Rights live in the bindings, not
//! the certificate, so deleting a binding takes them away at once: a
//! request after it is refused, and a stream open before it ends within
//! [`REVOCATION_CHECK`].

use std::fmt;
use std::time::Duration;

/// How often a stream checks again that its caller may still have it: a
/// user's session or watch, a worker's streams.
pub(crate) const REVOCATION_CHECK: Duration = Duration::from_secs(5);

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

/// Metadata naming the user a manager forwards a request for, so the
/// leader checks the user's rights again on its own, newer, state.
pub(crate) const FORWARDED_FOR: &str = "legion-forwarded-for";

impl Caller {
    pub fn of<T>(req: &Request<T>, tls: bool) -> Result<Caller, Status> {
        match crate::tls::caller(req, tls)? {
            None => Ok(Caller::All),
            Some(Identity {
                role: Role::Manager,
                ..
            }) => match req.metadata().get(FORWARDED_FOR) {
                Some(user) => {
                    let user = user.to_str().map_err(|_| {
                        Status::invalid_argument("a forwarded user name is not text")
                    })?;
                    Ok(Caller::User(user.to_owned()))
                }
                None => Ok(Caller::All),
            },
            Some(Identity {
                role: Role::Admin, ..
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

    /// The role RoleBindings give a user in each namespace; `None` for a
    /// caller who may do everything.
    pub fn roles(&self, store: &Store) -> Result<Option<Roles>, Status> {
        let Caller::User(name) = self else {
            return Ok(None);
        };
        let bindings = store
            .list(Kind::RoleBinding, None)
            .map_err(|e| Status::internal(e.to_string()))?;
        let groups = crate::console::groups(store, name)?;
        let mut roles = Roles::new();
        for o in &bindings.objects {
            let Ok(b) = legion_api::parse::<RoleBindingSpec>(&o.spec) else {
                continue;
            };
            if b.binds(name, &groups) {
                let role = roles.entry(o.metadata.namespace.clone()).or_insert(b.role);
                *role = (*role).max(b.role);
            }
        }
        Ok(Some(roles))
    }
}

/// A user's role in each namespace they have one in.
pub(crate) type Roles = std::collections::BTreeMap<String, NamespaceRole>;

/// Why a stream opened with rights `before` must end, now that they are
/// `now`: the first namespace where the user's role was lowered or taken.
pub(crate) fn lowered(user: &str, before: &Roles, now: &Roles) -> Option<Status> {
    before.iter().find_map(|(namespace, was)| {
        let is = now.get(namespace);
        (is < Some(was)).then(|| {
            Status::permission_denied(match is {
                Some(role) => format!(
                    "user {user}'s role in namespace {namespace} is now {role}, not {was}: watch again"
                ),
                None => format!("user {user} has no role in namespace {namespace} now"),
            })
        })
    })
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

/// The highest role RoleBindings give `user`, or their groups, in
/// `namespace`.
fn role_in(store: &Store, namespace: &str, user: &str) -> Result<Option<NamespaceRole>, Status> {
    let bindings = store
        .list(Kind::RoleBinding, Some(namespace))
        .map_err(|e| Status::internal(e.to_string()))?;
    let groups = crate::console::groups(store, user)?;
    Ok(bindings
        .objects
        .iter()
        .filter_map(|o| legion_api::parse::<RoleBindingSpec>(&o.spec).ok())
        .filter(|b| b.binds(user, &groups))
        .map(|b| b.role)
        .max())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_role_lowered_or_taken_ends_a_stream_and_a_raised_one_does_not() {
        let roles = |r: &[(&str, NamespaceRole)]| -> Roles {
            r.iter().map(|(n, r)| ((*n).to_owned(), *r)).collect()
        };
        use NamespaceRole::{Edit, View};
        let before = roles(&[("a", Edit), ("b", View)]);
        assert!(lowered("alice", &before, &before).is_none());
        assert!(
            lowered(
                "alice",
                &before,
                &roles(&[("a", Edit), ("b", Edit), ("c", View)])
            )
            .is_none()
        );
        let why = lowered("alice", &before, &roles(&[("a", View), ("b", View)])).unwrap();
        assert_eq!(why.code(), tonic::Code::PermissionDenied);
        assert!(why.message().contains("namespace a is now view"), "{why}");
        let why = lowered("alice", &before, &roles(&[("a", Edit)])).unwrap();
        assert!(why.message().contains("no role in namespace b"), "{why}");
    }

    #[test]
    fn roles_needed() {
        assert_eq!(needed(Verb::Read, Kind::Service), NamespaceRole::View);
        assert_eq!(needed(Verb::Read, Kind::Secret), NamespaceRole::Edit);
        assert_eq!(needed(Verb::Write, Kind::Service), NamespaceRole::Edit);
        assert_eq!(needed(Verb::Write, Kind::RoleBinding), NamespaceRole::Admin);
    }
}
