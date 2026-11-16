//! `legion doctor`: whether this host can run Legion, and how to fix it.
//! What it checks is shared with `legion local` in [`legion_sys::check`]; what those
//! findings mean for a node is in [`legion_agent::doctor`]; how they are
//! said is here. It changes nothing on the host.

use anyhow::Result;
#[cfg(test)]
use legion_agent::doctor::ready;
use legion_agent::doctor::{Entry, Need, examine, verdict};
use legion_sys::Sys;
use legion_sys::check::State;

fn text(found: &[Entry]) -> String {
    let mut out = String::new();
    for e in found {
        let f = &e.finding;
        out += &line(
            label(f.state, e.need),
            &f.id,
            &f.detail,
            f.remedy.as_deref().unwrap_or(""),
        );
    }
    out
}

fn label(state: State, need: Need) -> &'static str {
    match (state, need) {
        (State::Met, _) => "ok",
        (State::Unknown, _) => "unknown",
        (_, Need::Required) => "FAIL",
        (State::Unsupported, Need::Advised) => "unsupported",
        (State::Unmet, Need::Advised) => "warn",
    }
}

/// A finding's line, and its remedy's below it.
fn line(label: &str, id: &str, detail: &str, remedy: &str) -> String {
    let mut out = format!("{label:<12}{id:<20}{detail}\n");
    if !remedy.is_empty() {
        out += &format!("{:<32}fix: {remedy}\n", "");
    }
    out
}

/// `legion diagnose`: each node's findings, as `legion doctor` says them
/// on the node itself.
pub fn diagnosis(nodes: &[legion_proto::v1::NodeDiagnosis]) -> String {
    let mut out = String::new();
    for n in nodes {
        let verdict = if !n.error.is_empty() {
            n.error.as_str()
        } else if n.ready {
            "ready"
        } else {
            "NOT READY"
        };
        out += &format!("{}: {verdict}\n", n.node);
        for f in &n.findings {
            // Labelled as `text` labels them, from the words the agent sent.
            let word = |w: &str| serde_json::Value::String(w.to_owned());
            let state = serde_json::from_value(word(&f.state));
            let need = serde_json::from_value(word(&f.need));
            let label = match (state, need) {
                (Ok(state), Ok(need)) => label(state, need),
                _ => "?",
            };
            out += "  ";
            out += &line(label, &f.id, &f.detail, &f.remedy).replace('\n', "\n  ");
            out.truncate(out.trim_end_matches(' ').len());
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
    fn each_nodes_findings_are_said_as_on_the_node() {
        use legion_proto::v1::{DoctorFinding, NodeDiagnosis};
        let f = |id: &str, state: &str, need: &str, remedy: &str| DoctorFinding {
            id: id.into(),
            state: state.into(),
            need: need.into(),
            detail: format!("{id} detail"),
            remedy: remedy.into(),
        };
        let nodes = [
            NodeDiagnosis {
                node: "n1".into(),
                ready: false,
                findings: vec![
                    f("module.pf", "unmet", "required", "kldload pf"),
                    f("racct", "unmet", "advised", ""),
                    f("zfs.root", "met", "required", ""),
                ],
                ..Default::default()
            },
            NodeDiagnosis {
                node: "n2".into(),
                error: "not asked: the node is down".into(),
                ..Default::default()
            },
        ];
        let text = diagnosis(&nodes);
        assert!(text.starts_with("n1: NOT READY\n"), "{text}");
        assert!(text.contains("  FAIL        module.pf"), "{text}");
        assert!(text.contains("fix: kldload pf"), "{text}");
        assert!(text.contains("  warn        racct"), "{text}");
        assert!(text.contains("  ok          zfs.root"), "{text}");
        assert!(text.contains("n2: not asked: the node is down"), "{text}");
        assert!(!text.contains("  \n"), "no blank indented lines: {text:?}");
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
