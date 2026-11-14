//! The web console's sign-in records (DESIGN.md §3.9): one-time codes and
//! the sessions they are exchanged for, kept as Secrets in the reserved
//! namespace, which no caller sees. Each is named by a hash of its secret,
//! so the store (and a backup of it) holds nothing a browser could use.
//! Written on the leader; a session is read on any manager.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use legion_api::console::Settings;

use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, Outcome, Precondition, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::api::ApiService;
use crate::join::SYSTEM_NAMESPACE;

/// How long a code from `legion console login` may be exchanged.
pub(crate) const CODE_LIFETIME: Duration = Duration::from_secs(60);

const CODE_PREFIX: &str = "console-code-";
const SESSION_PREFIX: &str = "console-session-";
/// The console's settings (`legion_api::console::Settings`), one record.
const SETTINGS: &str = "console-settings";

/// Who a code or session is for, and until when (Unix ms).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signed {
    pub name: String,
    /// "admin" or "user".
    pub role: String,
    pub expires_ms: i64,
}

impl Signed {
    fn expired(&self, now: i64) -> bool {
        self.expires_ms <= now
    }
}

/// A code: who it is for, when it stops being good (`expires_ms`), and
/// when the certificate it was asked with expires (`until_ms`), which a
/// session it becomes never outlasts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Code {
    name: String,
    role: String,
    expires_ms: i64,
    until_ms: i64,
}

pub(crate) fn now_ms() -> i64 {
    ms(SystemTime::now())
}

pub(crate) fn ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn key(prefix: &str, secret: &str) -> ObjectKey {
    let hash = format!("{:x}", Sha256::digest(secret.as_bytes()));
    ObjectKey::new(
        Kind::Secret,
        SYSTEM_NAMESPACE,
        &format!("{prefix}{}", &hash[..40]),
    )
}

fn read<T: serde::de::DeserializeOwned>(
    store: &Store,
    key: &ObjectKey,
) -> Result<Option<(T, u64)>, Status> {
    let Some(o) = store
        .get(key)
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return Ok(None);
    };
    let record = serde_json::from_value(o.spec.clone())
        .map_err(|e| Status::internal(format!("{key}: {e}")))?;
    Ok(Some((record, o.metadata.version)))
}

async fn put(api: &ApiService, key: &ObjectKey, record: &impl Serialize) -> Result<(), Status> {
    let object = Object::new(
        Kind::Secret,
        SYSTEM_NAMESPACE,
        &key.name,
        serde_json::to_value(record).expect("serialises"),
    );
    match api
        .write(Command::Put {
            object,
            precondition: Precondition::MustNotExist,
        })
        .await?
    {
        Outcome::Written(_) => Ok(()),
        other => Err(Status::internal(format!("unexpected {other:?}"))),
    }
}

/// A one-time code for `name` as `role`, on the leader: good for
/// [`CODE_LIFETIME`], or until `until_ms` if sooner (the end of the
/// certificate it was asked with).
pub(crate) async fn new_code(
    api: &ApiService,
    name: &str,
    role: &str,
    until_ms: i64,
) -> Result<(String, i64), Status> {
    sweep(api).await;
    let code = legion_pki::random_secret();
    let record = Code {
        name: name.to_owned(),
        role: role.to_owned(),
        expires_ms: (now_ms() + CODE_LIFETIME.as_millis() as i64).min(until_ms),
        until_ms,
    };
    put(api, &key(CODE_PREFIX, &code), &record).await?;
    Ok((code, record.expires_ms))
}

/// Exchanges `code`, once, for a session, on the leader. The session lasts
/// as long as the console's settings say, or until the certificate the code
/// was asked with expires if sooner.
pub(crate) async fn redeem(api: &ApiService, code: &str) -> Result<(String, Signed), Status> {
    let refused = || Status::permission_denied("the code is wrong, used or expired");
    let code_key = key(CODE_PREFIX, code);
    let Some((found, version)) = read::<Code>(&api.store, &code_key)? else {
        return Err(refused());
    };
    // Used up whether or not it is still good; a second use finds nothing.
    match api
        .write(Command::Delete {
            key: code_key,
            precondition: Precondition::Version(version),
        })
        .await?
    {
        Outcome::Deleted { .. } => {}
        _ => return Err(refused()),
    }
    let now = now_ms();
    if found.expires_ms <= now {
        return Err(refused());
    }
    let session = legion_pki::random_secret();
    let record = Signed {
        name: found.name,
        role: found.role,
        expires_ms: (now + settings(&api.store)?.session().millis() as i64).min(found.until_ms),
    };
    put(api, &key(SESSION_PREFIX, &session), &record).await?;
    Ok((session, record))
}

fn settings_key() -> ObjectKey {
    ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, SETTINGS)
}

/// The console's settings, on any manager from its replica; the defaults
/// when none were set.
pub(crate) fn settings(store: &Store) -> Result<Settings, Status> {
    Ok(read::<Settings>(store, &settings_key())?
        .map(|(s, _)| s)
        .unwrap_or_default())
}

/// Sets the console's settings, on the leader, once checked.
pub(crate) async fn set_settings(api: &ApiService, s: &Settings) -> Result<(), Status> {
    s.check()
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    let object = Object::new(
        Kind::Secret,
        SYSTEM_NAMESPACE,
        SETTINGS,
        serde_json::to_value(s).expect("serialises"),
    );
    api.write(Command::Put {
        object,
        precondition: Precondition::None,
    })
    .await?;
    Ok(())
}

/// The session `session` names, if it is current: on any manager, from its
/// replica.
pub fn session(store: &Store, session: &str) -> Result<Option<Signed>, Status> {
    Ok(read::<Signed>(store, &key(SESSION_PREFIX, session))?
        .map(|(s, _)| s)
        .filter(|s| !s.expired(now_ms())))
}

/// Ends `session`, on the leader; one already gone is no error.
pub(crate) async fn end(api: &ApiService, session: &str) -> Result<(), Status> {
    api.write(Command::Delete {
        key: key(SESSION_PREFIX, session),
        precondition: Precondition::None,
    })
    .await?;
    Ok(())
}

/// Forgets expired codes and sessions, on the leader. Best effort: what is
/// left is refused when read, and goes on the next sweep.
async fn sweep(api: &ApiService) {
    let Ok(list) = api.store.list(Kind::Secret, Some(SYSTEM_NAMESPACE)) else {
        return;
    };
    let now = now_ms();
    for o in list.objects {
        let name = &o.metadata.name;
        if !(name.starts_with(CODE_PREFIX) || name.starts_with(SESSION_PREFIX)) {
            continue;
        }
        // A code's and a session's `expires_ms` alike.
        let expired = o
            .spec
            .get("expires_ms")
            .and_then(|v| v.as_i64())
            .is_none_or(|at| at <= now);
        if expired {
            let _ = api
                .write(Command::Delete {
                    key: o.key(),
                    precondition: Precondition::Version(o.metadata.version),
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_named_by_a_hash_not_their_secret() {
        let k = key(SESSION_PREFIX, "abc");
        assert!(k.name.starts_with(SESSION_PREFIX));
        assert!(!k.name.contains("abc"));
        assert_eq!(k.name.len(), SESSION_PREFIX.len() + 40);
        assert_ne!(key(SESSION_PREFIX, "abd"), k);
    }
}
