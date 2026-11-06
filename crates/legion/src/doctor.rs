//! `legion doctor`: whether this host can run Legion, and how to fix it.
//! What it checks is shared with jailer in [`legion_sys::check`]; what those
//! findings mean for Legion is here. It changes nothing on the host.

use anyhow::Result;
use legion_sys::Sys;
use legion_sys::check::{self, Finding, State};
use serde::Serialize;

/// What a finding that isn't met means for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Need {
    /// Legion can't do its work here.
    Required,
    /// Something is lost or Legion puts it right itself: the warning says so.
    Advised,
}

#[derive(Debug, Serialize)]
struct Entry {
    need: Need,
    #[serde(flatten)]
    finding: Finding,
}

fn entry(need: Need, finding: Finding) -> Entry {
    Entry { need, finding }
}

fn examine(sys: &dyn Sys, zfs_root: Option<&str>) -> Vec<Entry> {
    use Need::{Advised, Required};
    let mut found: Vec<_> = legion_agent::MODULES
        .iter()
        .map(|m| entry(Required, check::module(sys, m)))
        .collect();
    found.push(entry(
        Required,
        check::pf_anchors(sys, "legion/*", legion_net::lb::HOOKS),
    ));
    if let Some(root) = zfs_root {
        found.push(entry(Required, check::zfs_root(sys, root)));
    }
    // Pods with limits go to other nodes; the agent sets the rest at start.
    found.push(entry(Advised, check::racct(sys)));
    found.push(entry(Advised, check::forwarding(sys)));
    found.push(entry(Advised, check::bridge_filtering(sys)));
    // Only for `--linux`.
    found.extend(check::linux_abi(sys).into_iter().map(|f| entry(Advised, f)));
    found
}

/// What can't be vouched for counts: a host is not ready because it went
/// unread, so a run without root says to run it as root.
fn ready(found: &[Entry]) -> bool {
    found
        .iter()
        .all(|e| e.need != Need::Required || e.finding.state == State::Met)
}

fn text(found: &[Entry]) -> String {
    let mut out = String::new();
    for e in found {
        let f = &e.finding;
        let label = match (f.state, e.need) {
            (State::Met, _) => "ok",
            (State::Unknown, _) => "unknown",
            (_, Need::Required) => "FAIL",
            (State::Unsupported, Need::Advised) => "unsupported",
            (State::Unmet, Need::Advised) => "warn",
        };
        out.push_str(&format!("{label:<12}{:<20}{}\n", f.id, f.detail));
        if let Some(remedy) = &f.remedy {
            out.push_str(&format!("{:<32}fix: {remedy}\n", ""));
        }
    }
    out
}

pub fn run(sys: &dyn Sys, zfs_root: Option<&str>, json: bool) -> Result<()> {
    let found = examine(sys, zfs_root);
    if json {
        println!("{}", serde_json::to_string_pretty(&found)?);
    } else {
        print!("{}", text(&found));
    }
    match verdict(&found) {
        Some(why) => anyhow::bail!(why),
        None => Ok(()),
    }
}

/// Why this run can't call the host ready: a requirement that went unread
/// says nothing against the host, and mustn't read as if it did.
fn verdict(found: &[Entry]) -> Option<&'static str> {
    let required = || found.iter().filter(|e| e.need == Need::Required);
    if required().any(|e| matches!(e.finding.state, State::Unmet | State::Unsupported)) {
        Some("this host lacks what Legion requires")
    } else if !ready(found) {
        Some("not everything could be checked: run it as root")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    /// A host with everything Legion wants.
    fn good() -> FakeSys {
        let sys = FakeSys::new();
        for (name, value) in [
            ("net.inet.ip.forwarding", "1"),
            ("net.link.bridge.pfil_member", "1"),
            ("net.link.bridge.pfil_local_phys", "1"),
        ] {
            sys.sysctl_set(name, value).unwrap();
        }
        sys.set_main_rules(&format!("{}\n", legion_net::lb::HOOKS.join("\n")));
        sys.zfs_create("tank/legion", &[]).unwrap();
        sys
    }

    fn state(found: &[Entry], id: &str) -> State {
        found
            .iter()
            .find(|e| e.finding.id == id)
            .unwrap()
            .finding
            .state
    }

    #[test]
    fn a_good_host_is_ready() {
        let found = examine(&good(), Some("tank/legion"));
        assert!(ready(&found), "{}", text(&found));
        assert_eq!(verdict(&found), None);
        assert!(found.iter().all(|e| e.finding.state == State::Met));
    }

    #[test]
    fn each_requirement_fails_alone() {
        type Break = fn(&FakeSys);
        let cases: [(&str, Break); 6] = [
            ("module.pf", |s| s.remove_module("pf")),
            ("module.if_bridge", |s| s.remove_module("if_bridge")),
            ("module.if_epair", |s| s.remove_module("if_epair")),
            ("module.if_wg", |s| s.remove_module("if_wg")),
            ("pf.anchors", |s| s.set_main_rules("")),
            ("zfs.root", |s| s.zfs_destroy("tank/legion", true).unwrap()),
        ];
        for (id, break_it) in cases {
            let sys = good();
            break_it(&sys);
            let found = examine(&sys, Some("tank/legion"));
            assert!(!ready(&found), "{id}: {}", text(&found));
            assert_ne!(state(&found, id), State::Met, "{id}");
            let bad = found
                .iter()
                .filter(|e| e.need == Need::Required && e.finding.state != State::Met)
                .count();
            assert_eq!(bad, 1, "{id}: {}", text(&found));
            assert!(text(&found).contains("FAIL"), "{id}");
            assert_eq!(
                verdict(&found),
                Some("this host lacks what Legion requires")
            );
        }
    }

    #[test]
    fn what_legion_copes_with_only_warns() {
        let sys = good();
        sys.sysctl_set("kern.racct.enable", "0").unwrap();
        sys.sysctl_set("net.inet.ip.forwarding", "0").unwrap();
        sys.sysctl_set("net.link.bridge.pfil_member", "0").unwrap();
        sys.remove_module("linux64");
        let found = examine(&sys, Some("tank/legion"));
        assert!(ready(&found), "{}", text(&found));
        let out = text(&found);
        assert_eq!(out.matches("warn").count(), 3, "{out}");
        assert_eq!(out.matches("unsupported").count(), 1, "{out}");
        assert!(out.contains("fix: set kern.racct.enable=1"), "{out}");
    }

    #[test]
    fn rules_it_could_not_read_are_not_a_pass_or_a_failure() {
        let sys = good();
        sys.fail_next("pfctl -sn");
        let found = examine(&sys, None);
        assert_eq!(state(&found, "pf.anchors"), State::Unknown);
        assert!(!ready(&found));
        let out = text(&found);
        assert!(out.contains("unknown"), "{out}");
        assert!(!out.contains("FAIL"), "{out}");
        assert_eq!(
            verdict(&found),
            Some("not everything could be checked: run it as root")
        );
    }

    #[test]
    fn no_zfs_root_is_not_checked() {
        let found = examine(&good(), None);
        assert!(found.iter().all(|e| e.finding.id != "zfs.root"));
    }

    #[test]
    fn it_changes_nothing() {
        let sys = good();
        sys.sysctl_set("net.inet.ip.forwarding", "0").unwrap();
        let _ = examine(&sys, Some("tank/legion"));
        assert_eq!(sys.sysctl_get("net.inet.ip.forwarding").unwrap(), "0");
        assert!(sys.modules().is_empty());
    }

    #[test]
    fn json_carries_the_stable_fields() {
        let found = examine(&good(), None);
        let v = serde_json::to_value(&found).unwrap();
        assert_eq!(v[0]["id"], "module.if_bridge");
        assert_eq!(v[0]["need"], "required");
        assert_eq!(v[0]["state"], "met");
    }
}
