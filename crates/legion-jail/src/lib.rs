//! Legion's jail runtime (DESIGN.md §6.5): containers run as jails on ZFS
//! clones of their images, each process under a small supervisor, with no
//! Podman or OCI runtime.

pub mod inroot;
pub mod log;
#[cfg(unix)]
mod runtime;
pub mod shim;
pub mod state;
pub mod users;

#[cfg(unix)]
pub use runtime::{Config, JailRuntime, Starting, Step, StepMount, StepNetwork};

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
/// The CPUs a member's processes may run on, a list as cpuset(1)'s `-l`
/// takes it (`0-3,6`): set on its jail as soon as it is made, before
/// anything runs in it.
pub const ANNOTATION_CPUSET: &str = "org.freebsd.jail.cpuset";
/// devfs(8)'s ruleset for jails, a member's `/dev`.
pub const DEVFS_RULESET: u32 = 4;

pub use legion_sys::check::LINUX_MODULES;

/// Readies the host for Linux images: loads [`LINUX_MODULES`] and, where
/// the host left `kern.elf64.fallback_brand` as FreeBSD does (none), sets
/// it to Linux's, so static Linux programs run (`gosu`, Go's programs; see
/// [`legion_sys::check::FALLBACK_BRAND`]). A host that chose another brand
/// keeps it: the note returned says what won't run.
pub fn enable_linux(sys: &dyn legion_sys::Sys) -> legion_sys::Result<Option<String>> {
    use legion_sys::check::{FALLBACK_BRAND, LINUX_BRAND, NO_BRAND};
    for m in LINUX_MODULES {
        sys.kld_load(m)?;
    }
    let brand = sys.sysctl_get(FALLBACK_BRAND)?;
    if brand == NO_BRAND {
        sys.sysctl_set(FALLBACK_BRAND, LINUX_BRAND)?;
        return Ok(None);
    }
    Ok((brand != LINUX_BRAND).then(|| {
        format!(
            "{FALLBACK_BRAND} is {brand}, set on this host: static Linux programs will not run              (set it to {LINUX_BRAND} for them)"
        )
    }))
}

#[cfg(test)]
mod tests {
    use legion_sys::check::{FALLBACK_BRAND, LINUX_BRAND};
    use legion_sys::{FakeSys, Sys};

    #[test]
    fn linux_is_enabled_unless_the_host_chose_another_brand() {
        let sys = FakeSys::new();
        assert_eq!(super::enable_linux(&sys).unwrap(), None);
        assert_eq!(sys.sysctl_get(FALLBACK_BRAND).unwrap(), LINUX_BRAND);
        // Again, as at the next start: nothing to say.
        assert_eq!(super::enable_linux(&sys).unwrap(), None);

        let sys = FakeSys::new();
        sys.sysctl_set(FALLBACK_BRAND, "9").unwrap();
        let note = super::enable_linux(&sys).unwrap().unwrap();
        assert!(
            note.contains("static Linux programs will not run"),
            "{note}"
        );
        assert_eq!(
            sys.sysctl_get(FALLBACK_BRAND).unwrap(),
            "9",
            "the host's choice kept"
        );
    }
}
