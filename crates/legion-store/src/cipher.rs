//! Encryption at rest (DESIGN.md §9): every value the store writes is
//! sealed with ChaCha20-Poly1305 under the manager's own data key, with a
//! random nonce and, as associated data, the table and key it is stored
//! under, so a value cannot be passed off as another. Values written
//! before encryption, plain JSON, are still read, and sealed when next
//! written. A store opened without a key writes plain JSON (tests).

use ring::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Error;

/// Marks a sealed value; plain JSON never starts with a zero byte.
const SEALED: [u8; 3] = [0, b'L', 1];

/// A manager's data key: 32 random bytes, kept beside the store, never
/// sent anywhere.
#[derive(Clone, PartialEq, Eq)]
pub struct StoreKey([u8; 32]);

impl std::fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreKey(..)")
    }
}

impl StoreKey {
    pub fn generate() -> StoreKey {
        let mut key = [0; 32];
        SystemRandom::new()
            .fill(&mut key)
            .expect("the system's random source");
        StoreKey(key)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<StoreKey, Error> {
        let key = bytes
            .try_into()
            .map_err(|_| Error::Sealed(format!("a store key is 32 bytes, not {}", bytes.len())))?;
        Ok(StoreKey(key))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Seals and opens the store's values.
pub(crate) struct Codec {
    key: Option<LessSafeKey>,
    random: SystemRandom,
}

impl Codec {
    pub(crate) fn new(key: Option<&StoreKey>) -> Codec {
        Codec {
            key: key.map(|k| {
                LessSafeKey::new(UnboundKey::new(&CHACHA20_POLY1305, &k.0).expect("a 32-byte key"))
            }),
            random: SystemRandom::new(),
        }
    }

    /// `value`, as JSON, sealed for `table` and `key` when the store has a
    /// key.
    pub(crate) fn encode<T: Serialize>(
        &self,
        table: &str,
        key: &str,
        value: &T,
    ) -> Result<Vec<u8>, Error> {
        self.seal(table, key, serde_json::to_vec(value)?)
    }

    pub(crate) fn decode<T: DeserializeOwned>(
        &self,
        table: &str,
        key: &str,
        stored: &[u8],
    ) -> Result<T, Error> {
        Ok(serde_json::from_slice(&self.open(table, key, stored)?)?)
    }

    pub(crate) fn seal(
        &self,
        table: &str,
        key: &str,
        mut plain: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let Some(k) = &self.key else {
            return Ok(plain);
        };
        let mut nonce = [0; NONCE_LEN];
        self.random
            .fill(&mut nonce)
            .map_err(|_| Error::Sealed("the system's random source failed".into()))?;
        k.seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad(table, key)),
            &mut plain,
        )
        .map_err(|_| Error::Sealed("sealing failed".into()))?;
        let mut out = Vec::with_capacity(SEALED.len() + NONCE_LEN + plain.len());
        out.extend_from_slice(&SEALED);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&plain);
        Ok(out)
    }

    pub(crate) fn open(&self, table: &str, key: &str, stored: &[u8]) -> Result<Vec<u8>, Error> {
        let Some(sealed) = stored.strip_prefix(&SEALED) else {
            // Written before the store was encrypted.
            return Ok(stored.to_vec());
        };
        let k = self.key.as_ref().ok_or_else(|| {
            Error::Sealed(format!(
                "{table}/{key} is encrypted and the store has no key"
            ))
        })?;
        if sealed.len() < NONCE_LEN {
            return Err(Error::Sealed(format!("{table}/{key} is cut short")));
        }
        let (nonce, body) = sealed.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce).expect("NONCE_LEN bytes");
        let mut body = body.to_vec();
        let plain = k
            .open_in_place(nonce, Aad::from(aad(table, key)), &mut body)
            .map_err(|_| {
                Error::Sealed(format!(
                    "{table}/{key} does not open with this key: another key, or damaged"
                ))
            })?;
        Ok(plain.to_vec())
    }
}

/// Seals `plain` with `key`, bound to `label`: files autolock protects.
pub fn seal(key: &StoreKey, label: &str, plain: &[u8]) -> Result<Vec<u8>, Error> {
    Codec::new(Some(key)).seal("file", label, plain.to_vec())
}

/// Opens what [`seal`] sealed with the same key and label; anything else,
/// plain bytes included, is refused.
pub fn open(key: &StoreKey, label: &str, sealed: &[u8]) -> Result<Vec<u8>, Error> {
    if !sealed.starts_with(&SEALED) {
        return Err(Error::Sealed(format!("{label} is not sealed")));
    }
    Codec::new(Some(key)).open("file", label, sealed)
}

fn aad(table: &str, key: &str) -> Vec<u8> {
    format!("{table}/{key}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_values_open_only_where_they_were_stored() {
        let key = StoreKey::generate();
        let c = Codec::new(Some(&key));
        let sealed = c.encode("objects", "Service/shop/web", &"spec").unwrap();
        assert!(sealed.starts_with(&SEALED));
        assert!(!String::from_utf8_lossy(&sealed).contains("spec"));
        let back: String = c.decode("objects", "Service/shop/web", &sealed).unwrap();
        assert_eq!(back, "spec");
        // Another place, another key, or no key: refused.
        assert!(
            c.decode::<String>("objects", "Service/shop/db", &sealed)
                .is_err()
        );
        let other = Codec::new(Some(&StoreKey::generate()));
        assert!(
            other
                .decode::<String>("objects", "Service/shop/web", &sealed)
                .is_err()
        );
        assert!(
            Codec::new(None)
                .decode::<String>("objects", "Service/shop/web", &sealed)
                .is_err()
        );
        assert!(c.open("objects", "x", &SEALED).is_err(), "cut short");
        // Plain JSON from before is read as it is.
        let plain = serde_json::to_vec(&"old").unwrap();
        assert_eq!(c.decode::<String>("objects", "x", &plain).unwrap(), "old");
        // Two seals of one value differ (fresh nonces).
        assert_ne!(
            sealed,
            c.encode("objects", "Service/shop/web", &"spec").unwrap()
        );
        assert!(StoreKey::from_bytes(&[0; 31]).is_err());
        assert_eq!(StoreKey::from_bytes(key.as_bytes()).unwrap(), key);
    }
}
