//! OCI images for Legion's jail runtime (DESIGN.md §6.5): references, a
//! registry client, and a node's store of images as ZFS layers.

pub mod layer;
pub mod oci;
mod reference;
mod registry;
mod store;

pub use oci::{Config, ImageConfig};
pub use reference::Reference;
pub use registry::{Client, Credentials};
pub use store::{Image, Store, chain_ids};

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
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sys(#[from] legion_sys::Error),
}
