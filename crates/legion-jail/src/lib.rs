//! Legion's jail runtime (DESIGN.md §6.5): containers run as jails on ZFS
//! clones of their images, each process under a small supervisor, with no
//! Podman or OCI runtime.

pub mod log;
#[cfg(unix)]
mod runtime;
pub mod shim;
pub mod state;

#[cfg(unix)]
pub use runtime::{Config, JailRuntime};
