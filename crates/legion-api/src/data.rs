//! `Secret` and `Config`: one opaque value each, as in Swarm. Containers get
//! it as a read-only file or an environment variable.
//!
//! Manifests give either `data` (base64) or `text`; the stored spec always
//! holds `data`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use crate::admit::{Invalid, Spec};

/// Largest value accepted, as in Swarm.
pub const MAX_DATA: usize = 500 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

pub type SecretSpec = DataSpec;
pub type ConfigSpec = DataSpec;

impl DataSpec {
    /// The decoded value of an admitted spec.
    pub fn bytes(&self) -> Result<Vec<u8>, Invalid> {
        match (&self.data, &self.text) {
            (Some(data), None) => STANDARD
                .decode(data)
                .map_err(|e| Invalid::new("data", format!("not valid base64: {e}"))),
            (None, Some(text)) => Ok(text.as_bytes().to_vec()),
            _ => Err(Invalid::new("", "set exactly one of `data` and `text`")),
        }
    }
}

impl Spec for DataSpec {
    fn admit(&mut self) -> Result<(), Invalid> {
        let bytes = self.bytes()?;
        if bytes.len() > MAX_DATA {
            return Err(Invalid::new(
                "",
                format!("{} bytes is above the {MAX_DATA}-byte limit", bytes.len()),
            ));
        }
        *self = DataSpec {
            data: Some(STANDARD.encode(bytes)),
            text: None,
        };
        Ok(())
    }
}
