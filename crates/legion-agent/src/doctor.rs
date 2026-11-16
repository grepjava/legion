//! What `legion doctor` checks: whether this host can run Legion. The checks
//! are shared with jailer in [`legion_sys::check`]; what their findings mean
//! for a node is here, where both the CLI and each agent (for the cluster's
//! diagnostics) read it. It changes nothing on the host.

use legion_sys::Sys;
use legion_sys::check::{self, Finding, State};
use serde::{Deserialize, Serialize};

/// What a finding that isn't met means for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Need {
    /// Legion can't do its work here.
    Required,
    /// Something is lost or Legion puts it right itself: the warning says so.
    Advised,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub need: Need,
    #[serde(flatten)]
    pub finding: Finding,
}

fn entry(need: Need, finding: Finding) -> Entry {
    Entry { need, finding }
}

pub fn examine(sys: &dyn Sys, zfs_root: Option<&str>) -> Vec<Entry> {
    use Need::{Advised, Required};
    let mut found: Vec<_> = crate::MODULES
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

/// What a node's status says of `found`: the findings not met.
pub fn lacks(found: &[Entry]) -> Vec<legion_api::node::HostLack> {
    found
        .iter()
        .filter(|e| e.finding.state != State::Met)
        .map(|e| legion_api::node::HostLack {
            id: e.finding.id.clone(),
            need: word(&e.need),
            state: word(&e.finding.state),
            detail: e.finding.detail.clone(),
            remedy: e.finding.remedy.clone(),
        })
        .collect()
}

/// A unit enum's name as serde writes it.
fn word<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// What can't be vouched for counts: a host is not ready because it went
/// unread, so a run without root says to run it as root.
pub fn ready(found: &[Entry]) -> bool {
    found
        .iter()
        .all(|e| e.need != Need::Required || e.finding.state == State::Met)
}

/// Why this run can't call the host ready: a requirement that went unread
/// says nothing against the host, and mustn't read as if it did.
pub fn verdict(found: &[Entry]) -> Option<&'static str> {
    let required = || found.iter().filter(|e| e.need == Need::Required);
    if required().any(|e| matches!(e.finding.state, State::Unmet | State::Unsupported)) {
        Some("this host lacks what Legion requires")
    } else if !ready(found) {
        Some("not everything could be checked: run it as root")
    } else {
        None
    }
}

/// A manager's session asking what this node's doctor finds:
/// `{"ready": …, "findings": […]}` as JSON on `stdout`, each finding as
/// `legion doctor --json` writes it, then `exit_code` 0.
pub(crate) async fn send(
    volumes: &crate::transfer::Volumes,
    tx: &tokio::sync::mpsc::Sender<legion_proto::v1::SessionFrame>,
) -> Result<(), tonic::Status> {
    use legion_proto::v1::SessionFrame;
    use legion_proto::v1::session_frame::Frame;
    let root = volumes.zfs_root.clone();
    let found = crate::task::blocking(&volumes.sys, move |sys| examine(sys, root.as_deref())).await;
    let said = serde_json::json!({ "ready": ready(&found), "findings": found });
    let json = serde_json::to_vec(&said)
        .map_err(|e| tonic::Status::internal(format!("the findings: {e}")))?;
    for frame in [Frame::Stdout(json), Frame::ExitCode(0)] {
        let _ = tx.send(SessionFrame { frame: Some(frame) }).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    #[test]
    fn a_nodes_status_carries_what_is_not_met() {
        let sys = FakeSys::new();
        sys.sysctl_set("net.inet.ip.forwarding", "1").unwrap();
        sys.sysctl_set("net.link.bridge.pfil_member", "1").unwrap();
        sys.sysctl_set("net.link.bridge.pfil_local_phys", "1")
            .unwrap();
        sys.set_main_rules(&format!("{}\n", legion_net::lb::HOOKS.join("\n")));
        sys.zfs_create("tank/legion", &[]).unwrap();
        assert_eq!(lacks(&examine(&sys, Some("tank/legion"))), []);

        sys.set_main_rules("");
        sys.remove_module("linux64");
        sys.sysctl_set("net.inet.ip.forwarding", "0").unwrap();
        let lacks = lacks(&examine(&sys, Some("tank/legion")));
        let said: Vec<_> = lacks
            .iter()
            .map(|l| (l.id.as_str(), l.need.as_str(), l.state.as_str()))
            .collect();
        assert_eq!(
            said,
            [
                ("pf.anchors", "required", "unmet"),
                ("forwarding", "advised", "unmet"),
                ("module.linux64", "advised", "unsupported"),
            ]
        );
        assert!(lacks[0].remedy.is_some());
    }
}
