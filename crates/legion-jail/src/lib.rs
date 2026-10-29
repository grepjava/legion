//! Legion's jail runtime (DESIGN.md §6.5): containers run as jails on ZFS
//! clones of their images, each process under a small supervisor, with no
//! Podman or OCI runtime.

pub mod log;
#[cfg(unix)]
mod runtime;
pub mod shim;
pub mod state;
pub mod users;

#[cfg(unix)]
pub use runtime::{Config, JailRuntime};

/// Kernel modules Linux images need: the ABI and the file systems mounted
/// for them (`/proc`, `/sys`, `/dev/fd`, `/dev/shm`).
pub const LINUX_MODULES: [&str; 5] = ["linux64", "linprocfs", "linsysfs", "fdescfs", "tmpfs"];
