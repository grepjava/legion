//! `legion-local`: standalone FreeBSD jail runtime, local OCI image store,
//! bridge networking, registry auth, and host checks.

pub mod auth;
pub mod doctor;
pub mod migrate;
pub mod net;
pub mod runtime;
pub mod store;

pub use legion_image::oci;
pub use legion_image::{Client, Credentials, Reference, registry_of};
pub use legion_jail::{log, shim, state, users};
pub use oci::{Config, ImageConfig};
pub use store::{Image, StorageMode, Store, chain_ids};

pub mod registry {
    pub use legion_image::{Client, Credentials, sha256};
}

/// An HTTP client builder for local/builder requests using ring.
pub fn http_client() -> reqwest::ClientBuilder {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder().user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
}

/// A size such as `512`, `64k`, `100m` or `10g` (a trailing `b` is allowed).
pub fn parse_size(v: &str) -> Option<u64> {
    let lower = v.to_ascii_lowercase();
    let s = lower.strip_suffix('b').unwrap_or(&lower);
    let (num, mult) = match s.chars().last()? {
        'k' => (&s[..s.len() - 1], 1u64 << 10),
        'm' => (&s[..s.len() - 1], 1 << 20),
        'g' => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    num.parse::<u64>().ok()?.checked_mul(mult)
}

pub mod sys {
    pub use legion_sys::*;

    /// Whether this host has a ZFS pool to keep images in.
    pub fn zfs_available() -> bool {
        std::process::Command::new("zfs")
            .args(["list", "-H", "-d", "0"])
            .output()
            .is_ok_and(|o| o.status.success() && !o.stdout.is_empty())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("image reference {0}")]
    Reference(String),
    #[error("{0}")]
    Registry(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{what}: digest {got}, expected {expected}")]
    Digest {
        what: String,
        expected: String,
        got: String,
    },
    #[error("{0}")]
    Unsupported(String),
    #[error("extracting a layer: {0}")]
    Extract(String),
    #[error("{0}")]
    Build(String),
    #[error("{0}")]
    Runtime(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sys(#[from] sys::Error),
}

impl From<legion_runtime::Error> for Error {
    fn from(e: legion_runtime::Error) -> Self {
        use legion_runtime::Error as E;
        match e {
            E::NotFound(s) => Error::NotFound(s),
            E::Io(e) => Error::Io(e),
            e => Error::Runtime(e.to_string()),
        }
    }
}

impl From<legion_image::Error> for Error {
    fn from(e: legion_image::Error) -> Self {
        use legion_image::Error as E;
        match e {
            E::Reference(s) => Error::Reference(s),
            E::Registry(s) => Error::Registry(s),
            E::NotFound(s) => Error::NotFound(s),
            E::Digest {
                what,
                expected,
                got,
            } => Error::Digest {
                what,
                expected,
                got,
            },
            E::Unsupported(s) | E::Platform(s) => Error::Unsupported(s),
            E::Extract(s) => Error::Extract(s),
            E::Io(e) => Error::Io(e),
            E::Json(e) => Error::Json(e),
            E::Sys(e) => Error::Sys(e),
        }
    }
}
