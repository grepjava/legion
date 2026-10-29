//! Autolock (DESIGN.md §9): the unlock key every manager seals its private
//! keys with, kept in the store (sealed there by each manager's data key)
//! so that managers learn it, and shown to the operator as
//! `LGN-UNLOCK-<base64>`.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Command, Outcome, Precondition, Store, StoreKey};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::join::SYSTEM_NAMESPACE;

const PREFIX: &str = "LGN-UNLOCK-";
const NAME: &str = "autolock";

#[derive(Serialize, Deserialize)]
struct Spec {
    key: String,
}

/// The key as the operator keeps it.
pub fn encode(key: &StoreKey) -> String {
    format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(key.as_bytes()))
}

pub fn parse(text: &str) -> Result<StoreKey, String> {
    let bad = || "not an unlock key (LGN-UNLOCK-...)".to_owned();
    let b64 = text.trim().strip_prefix(PREFIX).ok_or_else(bad)?;
    let bytes = URL_SAFE_NO_PAD.decode(b64).map_err(|_| bad())?;
    StoreKey::from_bytes(&bytes).map_err(|_| bad())
}

fn object_key() -> ObjectKey {
    ObjectKey::new(Kind::Secret, SYSTEM_NAMESPACE, NAME)
}

/// The unlock key, if autolock is on, and the version it was read at.
pub fn current(store: &Store) -> Result<Option<(StoreKey, u64)>, String> {
    let Some(o) = store.get(&object_key()).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let spec: Spec = serde_json::from_value(o.spec).map_err(|e| e.to_string())?;
    Ok(Some((parse(&spec.key)?, o.metadata.version)))
}

/// Carries out `action` on the leader; the key afterwards.
pub(crate) async fn apply(
    api: &crate::api::ApiService,
    action: &str,
) -> Result<Option<StoreKey>, Status> {
    let now = current(&api.store).map_err(Status::internal)?;
    let put = |key: &StoreKey| Command::Put {
        object: Object::new(
            Kind::Secret,
            SYSTEM_NAMESPACE,
            NAME,
            serde_json::to_value(Spec { key: encode(key) }).expect("serialises"),
        ),
        precondition: now.as_ref().map_or(Precondition::MustNotExist, |(_, v)| {
            Precondition::Version(*v)
        }),
    };
    let (command, key) = match (action, &now) {
        ("show", _) | ("enable", Some(_)) => return Ok(now.map(|(k, _)| k)),
        ("enable" | "rotate", _) => {
            let key = StoreKey::generate();
            (put(&key), Some(key))
        }
        ("disable", None) => return Ok(None),
        ("disable", Some((_, version))) => (
            Command::Delete {
                key: object_key(),
                precondition: Precondition::Version(*version),
            },
            None,
        ),
        _ => {
            return Err(Status::invalid_argument(
                "the action is enable, disable, rotate or show",
            ));
        }
    };
    match api.write(command).await? {
        Outcome::Written(_) | Outcome::Deleted { .. } => {
            tracing::info!(action, "autolock");
            Ok(key)
        }
        Outcome::Conflict { .. } => Err(Status::aborted("autolock changed meanwhile; retry")),
        other => Err(Status::internal(format!("unexpected {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_as_operators_keep_them() {
        let key = StoreKey::generate();
        let text = encode(&key);
        assert!(text.starts_with("LGN-UNLOCK-"));
        assert_eq!(parse(&format!(" {text}\n")).unwrap(), key);
        assert!(parse("LGN-UNLOCK-abc").is_err());
        assert!(parse(&text.replace("LGN-UNLOCK-", "LGN-1-")).is_err());
    }
}
