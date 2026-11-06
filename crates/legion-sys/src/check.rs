//! Read-only checks of what a host offers, shared by `legion doctor` and
//! jailer's. Each stands alone, changes nothing and needs no daemon. It says
//! what it found; what that blocks is the caller's to decide, as a host with
//! no pf suits some operations and not others. Ids are published in JSON
//! output, so they stay as they are, and remedies name no tool's own paths.

use serde::{Deserialize, Serialize};

use crate::{Error, KldState, Sys};

/// Kernel modules Linux images need: the ABI and the file systems mounted
/// for them (`/proc`, `/sys`, `/dev/fd`, `/dev/shm`).
pub const LINUX_MODULES: [&str; 5] = ["linux64", "linprocfs", "linsysfs", "fdescfs", "tmpfs"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    /// What was asked about holds.
    Met,
    /// It doesn't; the remedy says how to change that.
    Unmet,
    /// It couldn't be read, which says nothing about the host: an ordinary
    /// user can't read pf's rules, and mustn't be told they are wrong.
    Unknown,
    /// This host can't do it as it stands.
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// Stable: `racct`, `forwarding`, `module.pf` and so on.
    pub id: String,
    pub state: State,
    /// What was found.
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub remedy: Option<String>,
}

impl Finding {
    fn new(id: &str, state: State, detail: impl Into<String>, remedy: Option<String>) -> Self {
        Finding {
            id: id.to_owned(),
            state,
            detail: detail.into(),
            remedy,
        }
    }

    fn met(id: &str, detail: impl Into<String>) -> Self {
        Finding::new(id, State::Met, detail, None)
    }

    fn unmet(id: &str, detail: impl Into<String>, remedy: impl Into<String>) -> Self {
        Finding::new(id, State::Unmet, detail, Some(remedy.into()))
    }

    /// What couldn't be read, naming root if that is what it took.
    fn unknown(id: &str, what: &str, e: &Error) -> Self {
        let text = e.to_string();
        let detail = if denied(&text) {
            format!("reading {what} needs root: {text}")
        } else {
            format!("reading {what}: {text}")
        };
        Finding::new(id, State::Unknown, detail, None)
    }
}

fn denied(text: &str) -> bool {
    text.contains("Permission denied") || text.contains("Operation not permitted")
}

fn unknown_oid(e: &Error) -> bool {
    e.to_string().contains("unknown oid")
}

/// Whether rctl can hold limits: `kern.racct.enable`, a loader tunable.
pub fn racct(sys: &dyn Sys) -> Finding {
    const ID: &str = "racct";
    const REMEDY: &str = "set kern.racct.enable=1 in /boot/loader.conf and reboot";
    match sys.sysctl_get("kern.racct.enable") {
        Ok(v) if v == "1" => Finding::met(ID, "kern.racct.enable is 1"),
        Ok(v) => Finding::unmet(ID, format!("kern.racct.enable is {v}"), REMEDY),
        Err(e) if unknown_oid(&e) => Finding::new(
            ID,
            State::Unsupported,
            "this kernel has no RACCT",
            Some("use a kernel built with options RACCT and RCTL, as GENERIC is".to_owned()),
        ),
        Err(e) => Finding::unknown(ID, "kern.racct.enable", &e),
    }
}

/// Whether the host forwards IPv4. Only reports: a tool that needs it may
/// set it itself.
pub fn forwarding(sys: &dyn Sys) -> Finding {
    const ID: &str = "forwarding";
    match sys.sysctl_get("net.inet.ip.forwarding") {
        Ok(v) if v == "1" => Finding::met(ID, "net.inet.ip.forwarding is 1"),
        Ok(v) => Finding::unmet(
            ID,
            format!("net.inet.ip.forwarding is {v}"),
            "set gateway_enable=\"YES\" in /etc/rc.conf, and sysctl net.inet.ip.forwarding=1 for now",
        ),
        Err(e) => Finding::unknown(ID, "net.inet.ip.forwarding", &e),
    }
}

/// Whether pf sees traffic on a bridge's members and its local address,
/// which pods' policy needs. The sysctls come with `if_bridge`.
pub fn bridge_filtering(sys: &dyn Sys) -> Finding {
    const ID: &str = "bridge.filtering";
    const SYSCTLS: [&str; 2] = [
        "net.link.bridge.pfil_member",
        "net.link.bridge.pfil_local_phys",
    ];
    let mut off = Vec::new();
    for name in SYSCTLS {
        match sys.sysctl_get(name) {
            Ok(v) if v == "1" => {}
            Ok(_) => off.push(name),
            Err(e) if unknown_oid(&e) => {
                return Finding::new(
                    ID,
                    State::Unknown,
                    format!("{name} is not there, as if_bridge is not loaded"),
                    None,
                );
            }
            Err(e) => return Finding::unknown(ID, name, &e),
        }
    }
    if off.is_empty() {
        Finding::met(ID, "pf filters bridge members and local traffic")
    } else {
        let set = off
            .iter()
            .map(|n| format!("{n}=1"))
            .collect::<Vec<_>>()
            .join(" ");
        Finding::unmet(
            ID,
            format!("off: {}", off.join(", ")),
            format!("add to /etc/sysctl.conf, and for now run: sysctl {set}"),
        )
    }
}

/// Those of `hooks`, the lines for `nat-anchor`, `rdr-anchor` and `anchor`
/// of `anchor`, that the host's main ruleset lacks.
pub fn missing_hooks_of<'a>(main_rules: &str, anchor: &str, hooks: [&'a str; 3]) -> Vec<&'a str> {
    let quoted = format!("\"{anchor}\"");
    let called = |kind: &str| {
        main_rules.lines().any(|l| {
            let l = l.trim();
            l.starts_with(kind) && l.contains(&quoted)
        })
    };
    ["nat-anchor", "rdr-anchor", "anchor"]
        .into_iter()
        .zip(hooks)
        .filter(|(kind, _)| !called(kind))
        .map(|(_, hook)| hook)
        .collect()
}

/// Whether pf's main ruleset calls `anchor`, given the lines that do.
pub fn pf_anchors(sys: &dyn Sys, anchor: &str, hooks: [&str; 3]) -> Finding {
    const ID: &str = "pf.anchors";
    match sys.pf_main_rules() {
        Ok(rules) => {
            let missing = missing_hooks_of(&rules, anchor, hooks);
            if missing.is_empty() {
                Finding::met(ID, format!("pf's main ruleset calls {anchor}"))
            } else {
                Finding::unmet(
                    ID,
                    format!("pf's main ruleset lacks: {}", missing.join("; ")),
                    format!(
                        "add to /etc/pf.conf, translation lines before filter rules, and run pfctl -f /etc/pf.conf: {}",
                        hooks.join("; ")
                    ),
                )
            }
        }
        Err(e) => Finding::unknown(ID, "pf's main ruleset", &e),
    }
}

/// Whether a kernel module is there to use: loaded, compiled in, or able to
/// be loaded. A tool that loads what it needs is content with the last;
/// [`Sys::kld_state`] tells them apart for one that isn't.
pub fn module(sys: &dyn Sys, name: &str) -> Finding {
    let id = format!("module.{name}");
    match sys.kld_state(name) {
        Ok(KldState::Loaded) => Finding::met(&id, format!("{name} is loaded")),
        Ok(KldState::Available) => Finding::met(&id, format!("{name} is not loaded, and can be")),
        Ok(KldState::Missing) => Finding::new(
            &id,
            State::Unsupported,
            format!("{name} is neither loaded nor in kern.module_path"),
            Some(format!("install a kernel with {name}.ko")),
        ),
        Err(e) => Finding::unknown(&id, &format!("{name}'s state"), &e),
    }
}

/// [`module`] for each of [`LINUX_MODULES`].
pub fn linux_abi(sys: &dyn Sys) -> Vec<Finding> {
    LINUX_MODULES.iter().map(|m| module(sys, m)).collect()
}

/// Whether `dataset` is there for a tool to work under.
pub fn zfs_root(sys: &dyn Sys, dataset: &str) -> Finding {
    const ID: &str = "zfs.root";
    match sys.zfs_list(dataset) {
        Ok(found) if found.iter().any(|d| d.name == dataset) => {
            Finding::met(ID, format!("{dataset} is there"))
        }
        Ok(_) => missing_dataset(dataset),
        Err(e) if e.to_string().contains("does not exist") => missing_dataset(dataset),
        Err(e) => Finding::unknown(ID, dataset, &e),
    }
}

fn missing_dataset(dataset: &str) -> Finding {
    Finding::unmet(
        "zfs.root",
        format!("{dataset} does not exist"),
        format!("zfs create -p {dataset}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FakeSys;

    const HOOKS: [&str; 3] = ["nat-anchor \"t/*\"", "rdr-anchor \"t/*\"", "anchor \"t/*\""];

    #[test]
    fn racct_says_what_it_found() {
        let sys = FakeSys::new();
        assert_eq!(racct(&sys).state, State::Met);
        sys.sysctl_set("kern.racct.enable", "0").unwrap();
        let f = racct(&sys);
        assert_eq!(f.state, State::Unmet);
        assert!(f.remedy.unwrap().contains("/boot/loader.conf"));
    }

    #[test]
    fn a_kernel_without_racct_is_not_a_setting_to_change() {
        let sys = FakeSys::new();
        sys.remove_sysctl("kern.racct.enable");
        assert_eq!(racct(&sys).state, State::Unsupported);
    }

    #[test]
    fn forwarding_reports_and_changes_nothing() {
        let sys = FakeSys::new();
        sys.sysctl_set("net.inet.ip.forwarding", "0").unwrap();
        assert_eq!(forwarding(&sys).state, State::Unmet);
        assert_eq!(sys.sysctl_get("net.inet.ip.forwarding").unwrap(), "0");
        sys.sysctl_set("net.inet.ip.forwarding", "1").unwrap();
        assert_eq!(forwarding(&sys).state, State::Met);
    }

    #[test]
    fn bridge_filtering_needs_both_and_if_bridge() {
        let sys = FakeSys::new();
        assert_eq!(bridge_filtering(&sys).state, State::Unknown);
        sys.sysctl_set("net.link.bridge.pfil_member", "1").unwrap();
        sys.sysctl_set("net.link.bridge.pfil_local_phys", "0")
            .unwrap();
        let f = bridge_filtering(&sys);
        assert_eq!(f.state, State::Unmet);
        assert!(f.detail.contains("pfil_local_phys"));
        assert!(!f.detail.contains("pfil_member"));
        sys.sysctl_set("net.link.bridge.pfil_local_phys", "1")
            .unwrap();
        assert_eq!(bridge_filtering(&sys).state, State::Met);
    }

    #[test]
    fn anchors_are_the_callers() {
        let sys = FakeSys::new();
        sys.set_main_rules("nat-anchor \"t/*\" all\nanchor \"other/*\" all\n");
        let f = pf_anchors(&sys, "t/*", HOOKS);
        assert_eq!(f.state, State::Unmet);
        assert_eq!(
            f.detail,
            "pf's main ruleset lacks: rdr-anchor \"t/*\"; anchor \"t/*\""
        );
        sys.set_main_rules("nat-anchor \"t/*\" all\nrdr-anchor \"t/*\" all\nanchor \"t/*\" all\n");
        assert_eq!(pf_anchors(&sys, "t/*", HOOKS).state, State::Met);
    }

    #[test]
    fn rules_an_ordinary_user_cannot_read_are_not_called_wrong() {
        let sys = FakeSys::new();
        sys.fail_next("pfctl -sn");
        let f = pf_anchors(&sys, "t/*", HOOKS);
        assert_eq!(f.state, State::Unknown);
        assert!(f.detail.contains("needs root"), "{}", f.detail);
        assert_eq!(f.remedy, None);
    }

    #[test]
    fn a_module_that_can_be_loaded_is_usable_and_one_not_there_is_not() {
        let sys = FakeSys::new();
        assert_eq!(module(&sys, "pf").state, State::Met);
        assert_eq!(sys.kld_state("pf").unwrap(), KldState::Available);
        sys.kld_load("pf").unwrap();
        assert_eq!(sys.kld_state("pf").unwrap(), KldState::Loaded);
        sys.remove_module("nope");
        let f = module(&sys, "nope");
        assert_eq!(
            (f.id.as_str(), f.state),
            ("module.nope", State::Unsupported)
        );
    }

    #[test]
    fn linux_abi_is_what_the_runtime_loads() {
        let sys = FakeSys::new();
        sys.remove_module("linprocfs");
        let found = linux_abi(&sys);
        assert_eq!(found.len(), LINUX_MODULES.len());
        let bad: Vec<_> = found
            .iter()
            .filter(|f| f.state != State::Met)
            .map(|f| f.id.as_str())
            .collect();
        assert_eq!(bad, ["module.linprocfs"]);
    }

    #[test]
    fn a_root_dataset_is_there_or_not() {
        let sys = FakeSys::new();
        let f = zfs_root(&sys, "tank/t");
        assert_eq!(f.state, State::Unmet);
        assert_eq!(f.remedy.as_deref(), Some("zfs create -p tank/t"));
        sys.zfs_create("tank/t", &[]).unwrap();
        assert_eq!(zfs_root(&sys, "tank/t").state, State::Met);
    }

    #[test]
    fn findings_publish_as_plain_data() {
        let f = Finding::unmet("racct", "kern.racct.enable is 0", "set it");
        assert_eq!(
            serde_json::to_string(&f).unwrap(),
            r#"{"id":"racct","state":"unmet","detail":"kern.racct.enable is 0","remedy":"set it"}"#
        );
        let met = serde_json::to_string(&Finding::met("x", "y")).unwrap();
        assert_eq!(met, r#"{"id":"x","state":"met","detail":"y"}"#);
    }
}
