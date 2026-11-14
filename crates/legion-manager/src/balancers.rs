//! Which node holds each load balancer address (CONSOLE.md phase 3): CARP
//! decides that among the nodes, not the managers, so each node's
//! heartbeats report the CARP state of the addresses it took up, and the
//! leader keeps the latest of each for `Cluster.LoadBalancers`.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A report older than this says nothing: its node may be gone.
const FRESH: Duration = Duration::from_secs(30);

/// A node's report: when it came, and the state of each address.
type Report = (Instant, BTreeMap<Ipv4Addr, String>);

/// The CARP states each node reported last, by node.
#[derive(Default)]
pub(crate) struct Held(Mutex<BTreeMap<String, Report>>);

impl Held {
    /// `node`'s report: the state of each load balancer address it took up.
    pub(crate) fn record(&self, node: &str, states: impl IntoIterator<Item = (Ipv4Addr, String)>) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).insert(
            node.to_owned(),
            (Instant::now(), states.into_iter().collect()),
        );
    }

    /// The reports still fresh, by node.
    pub(crate) fn fresh(&self) -> BTreeMap<String, BTreeMap<Ipv4Addr, String>> {
        let mut all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        all.retain(|_, (at, _)| at.elapsed() < FRESH);
        all.iter()
            .map(|(node, (_, states))| (node.clone(), states.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_latest_report_of_each_node_counts() {
        let held = Held::default();
        let a: Ipv4Addr = "192.0.2.10".parse().unwrap();
        held.record("n1", [(a, "backup".to_owned())]);
        held.record("n2", [(a, "master".to_owned())]);
        held.record("n1", [(a, "master".to_owned())]);
        let fresh = held.fresh();
        assert_eq!(fresh["n1"][&a], "master");
        assert_eq!(fresh["n2"][&a], "master");
        held.record("n2", []);
        assert!(held.fresh()["n2"].is_empty());
    }
}
