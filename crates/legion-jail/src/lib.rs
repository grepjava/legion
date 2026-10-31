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
pub use runtime::{Config, JailRuntime, Step, StepMount, StepNetwork};

/// A container that is a pod's jail: `persist`, VNET, running nothing.
pub const ANNOTATION_VNET: &str = "org.freebsd.jail.vnet";
/// The pod (its container id) a member runs in.
pub const ANNOTATION_PARENT: &str = "org.freebsd.parentJail";
/// More jail parameters, `name=value` separated by spaces, which win over
/// the runtime's own: Legion's agent gives a container's security here
/// (`legion_api::security::Security::jail_params`).
pub const ANNOTATION_JAIL: &str = "legion.jail";
/// `true` allows mlock(2).
pub const ANNOTATION_MLOCK: &str = "org.freebsd.jail.allow.mlock";
/// SysV IPC: annotation, and the jail parameter it sets to its value.
pub const ANNOTATION_SYSV: [(&str, &str); 3] = [
    ("org.freebsd.jail.sysvmsg", "sysvmsg"),
    ("org.freebsd.jail.sysvsem", "sysvsem"),
    ("org.freebsd.jail.sysvshm", "sysvshm"),
];
/// devfs(8)'s ruleset for jails, a member's `/dev`.
pub const DEVFS_RULESET: u32 = 4;

/// Kernel modules Linux images need: the ABI and the file systems mounted
/// for them (`/proc`, `/sys`, `/dev/fd`, `/dev/shm`).
pub const LINUX_MODULES: [&str; 5] = ["linux64", "linprocfs", "linsysfs", "fdescfs", "tmpfs"];
