//! `legion-build`: FreeBSD OCI image builder, Containerfile parser,
//! compatibility checks, signing, and attestations.

pub mod attest;
pub mod build;
pub mod cache;
pub mod check;
pub mod containerfile;
pub mod init;
pub mod layer;
pub mod record;
pub mod remote;
pub mod sign;
pub mod vm;

pub use legion_local::{
    Client, Config, Credentials, Error, Image, ImageConfig, Reference, StorageMode, Store, auth,
    chain_ids, http_client, log, net, oci, parse_size, registry, registry_of, runtime, shim, state,
    store, sys, users,
};
