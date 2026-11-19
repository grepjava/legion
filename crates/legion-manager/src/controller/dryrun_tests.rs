//! [`dry_run`] over a cluster simulated as the planner's tests simulate it.

use legion_api::dryrun::{Change, DryRun, Severity};
use legion_api::node::{Availability, NodeState};

use super::super::plan::dryrun::{Cluster, Facts, Refused, dry_run};
use super::{STATEFUL, World};

impl World {
    fn cluster(&self) -> Cluster {
        Cluster {
            services: self.services.clone(),
            jobs: self.jobs.clone(),
            tasks: self.tasks.clone(),
            nodes: self.nodes.clone(),
            volumes: self.volumes.clone(),
            namespaces: self.namespaces.clone(),
        }
    }

    fn check(&self, change: Change, facts: &Facts) -> DryRun {
        let before = format!("{:?}{:?}", self.tasks, self.volumes);
        let d = dry_run(self.cluster(), &change, facts, self.now).unwrap();
        // Nothing is changed by looking.
        assert_eq!(before, format!("{:?}{:?}", self.tasks, self.volumes));
        d
    }
}

/// Each finding as `severity reason object: message`.
fn said(d: &DryRun) -> Vec<String> {
    d.findings
        .iter()
        .map(|f| {
            let s = match f.severity {
                Severity::Blocking => "blocking",
                Severity::Warning => "warning",
            };
            format!("{s} {} {}: {}", f.reason, f.object, f.message)
        })
        .collect()
}

fn drain(node: &str) -> Change {
    Change::Drain { node: node.into() }
}

fn on_ssd(replicas: u32) -> String {
    format!(
        "{}\nplacement: {{ constraints: [\"node.labels.disk == ssd\"] }}",
        STATEFUL.replace("replicas: 3", &format!("replicas: {replicas}"))
    )
}

/// The phase's done-when: a drain that would leave a Volume behind blocks,
/// saying why, and nothing changes meanwhile.
#[test]
fn a_drain_that_would_strand_a_volume_blocks() {
    let mut w = World::new();
    w.node("n1", &[("disk", "ssd")]);
    w.node("n2", &[]);
    w.service("db", &on_ssd(1));
    w.settle();
    assert_eq!(w.placed("db"), ["n1"]);
    let d = w.check(drain("n1"), &Facts::default());
    assert!(d.blocks());
    assert_eq!(
        said(&d),
        [
            "blocking stays Task shop/db-0-1: keeps running on n1 (data)",
            "blocking stranded Volume shop/data-db-0: stays on n1: no node takes it: 1 constraints",
        ]
    );
    assert!(d.moves.is_empty());
    assert_eq!(d.stopped, 0);
    assert_eq!(w.node_mut("n1").spec.availability, Availability::Active);
}

#[test]
fn a_drain_says_what_moves_and_whether_it_fits() {
    let mut w = World::new();
    for n in ["n1", "n2", "n3"] {
        w.node(n, &[]);
    }
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 2"));
    w.settle();
    let from = w.placed_task("db-0-1").spec.node.clone();
    let busy = w.placed_task("db-1-1").spec.node.clone();
    let free = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != from && *n != busy)
        .unwrap();
    let mut facts = Facts::default();
    facts.used.insert("shop/data-db-0".into(), 3 << 30);
    let d = w.check(drain(&from), &facts);
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!(d.moves.len(), 1);
    let m = &d.moves[0];
    assert_eq!(
        (m.volume.as_str(), m.from.as_str(), m.to.as_str(), m.bytes),
        ("data-db-0", from.as_str(), free, Some(3 << 30))
    );
    assert_eq!(d.stopped, 1);

    // Less free there than moves: it blocks.
    facts.free.insert(free.into(), 1 << 30);
    let d = w.check(drain(&from), &facts);
    assert_eq!(
        said(&d),
        [format!(
            "blocking no-room Node {free}: 3.0 GiB would move there; its pools have 1.0 GiB free"
        )]
    );
}

#[test]
fn a_drain_says_what_no_node_would_take() {
    let mut w = World::new();
    w.node("n1", &[("zone", "a")]);
    w.node("n2", &[("zone", "b")]);
    w.service(
        "web",
        "replicas: 2\nplacement: { constraints: [\"node.labels.zone == a\"] }\npod: { containers: [{ name: web, image: \"web:1\" }] }",
    );
    w.settle();
    assert_eq!(w.placed("web"), ["n1", "n1"]);
    let d = w.check(drain("n1"), &Facts::default());
    assert!(d.blocks());
    assert_eq!(
        said(&d),
        [
            "blocking unplaced Service shop/web: 0 of its 2 running task(s) would run: no node can run it: 1 constraints, 1 not active"
        ]
    );
    assert_eq!(d.stopped, 2);
    // Nothing runs on n2: it drains freely.
    let d = w.check(drain("n2"), &Facts::default());
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!((d.stopped, d.moves.len()), (0, 0));
}

#[test]
fn a_drain_says_what_the_budget_holds_back() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service(
        "db",
        &format!(
            "{}\nmaxUnavailable: 1",
            STATEFUL.replace("replicas: 3", "replicas: 2")
        ),
    );
    w.settle();
    assert_eq!(w.placed("db"), ["n1", "n1"]);
    w.node("n2", &[]);
    w.node("n3", &[]);
    let d = w.check(drain("n1"), &Facts::default());
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!(
        said(&d),
        [
            "warning budget Service shop/db: its disruption budget (maxUnavailable 1) holds 1 of its task(s) back until the others run again: they move a few at a time"
        ]
    );
    assert_eq!((d.stopped, d.moves.len()), (2, 2));
}

#[test]
fn removing_a_node_says_what_data_goes_with_it() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let home = w.placed("db")[0].clone();
    let remove = Change::RemoveNode { node: home.clone() };
    let d = w.check(remove.clone(), &Facts::default());
    assert!(d.blocks());
    assert!(
        said(&d).contains(&format!(
            "blocking lost Volume shop/data-db-0: its data goes with node {home}, which is up: drain the node first to move it"
        )),
        "{:?}",
        said(&d)
    );
    // Already down, it is told, not refused.
    w.state(&home, NodeState::Down);
    let d = w.check(remove, &Facts::default());
    assert!(
        said(&d).contains(&format!(
            "warning lost Volume shop/data-db-0: its data goes with node {home}"
        )),
        "{:?}",
        said(&d)
    );
    assert_eq!(
        dry_run(
            w.cluster(),
            &Change::RemoveNode { node: "n9".into() },
            &Facts::default(),
            w.now
        )
        .unwrap_err(),
        Refused::NotFound("Node n9 not found".into())
    );
}

#[test]
fn deleting_a_service_says_what_volumes_it_leaves() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let mut facts = Facts::default();
    facts.used.insert("shop/data-db-0".into(), 5 << 20);
    let d = w.check(
        Change::DeleteService {
            namespace: "shop".into(),
            service: "db".into(),
        },
        &facts,
    );
    assert!(!d.blocks());
    assert_eq!(
        said(&d),
        [
            "warning kept Volume shop/data-db-0: kept (5.0 MiB): deleting the Volume frees it, or the Service made again uses it"
        ]
    );
    assert_eq!(d.stopped, 1);
}

#[test]
fn a_volume_asked_where_its_task_cannot_run_blocks() {
    let mut w = World::new();
    w.node("n1", &[("disk", "ssd")]);
    w.node("n2", &[]);
    w.node("n3", &[("disk", "ssd")]);
    w.service("db", &on_ssd(1));
    w.settle();
    let home = w.placed("db")[0].clone();
    let to = |node: &str| Change::MoveVolume {
        namespace: "shop".into(),
        volume: "data-db-0".into(),
        node: node.into(),
    };
    let d = w.check(to("n2"), &Facts::default());
    assert_eq!(
        said(&d),
        [format!(
            "blocking stranded Volume shop/data-db-0: stays on {home}: node n2 does not take its tasks: constraints"
        )]
    );
    let other = if home == "n1" { "n3" } else { "n1" };
    let d = w.check(to(other), &Facts::default());
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!(d.moves.len(), 1);
    assert_eq!(d.moves[0].to, other);
    assert_eq!(d.stopped, 1);
    assert_eq!(
        said(&d),
        [
            "warning stops : its tasks stop while what changed since the full copy is sent, then start on the new node"
        ]
    );
}

/// A move past its switch-over goes through: asked elsewhere, it is
/// refused as the API refuses it.
#[test]
fn a_move_asked_back_past_its_switch_over_blocks() {
    use legion_api::volume::MigrationPhase::*;
    let mut w = World::new();
    for n in ["n1", "n2"] {
        w.node(n, &[]);
    }
    w.service("db", &STATEFUL.replace("replicas: 3", "replicas: 1"));
    w.settle();
    let from = w.placed("db")[0].clone();
    let to = if from == "n1" { "n2" } else { "n1" };
    w.ask("data-db-0", Some(to));
    w.settle();
    w.migrate();
    w.settle();
    assert_eq!(w.migration("data-db-0"), Some((to.to_owned(), Finishing)));
    let d = w.check(
        Change::MoveVolume {
            namespace: "shop".into(),
            volume: "data-db-0".into(),
            node: from.clone(),
        },
        &Facts::default(),
    );
    assert_eq!(
        said(&d),
        [format!(
            "blocking switch-over Volume shop/data-db-0: Volume shop/data-db-0 is past its switch-over from {from} to {to}: its tasks have stopped for the last part, and the move goes through; ask again once it is on {to}"
        )]
    );
}

/// A node not ready is drained all the same: its drain waits for it.
#[test]
fn a_node_not_ready_drains_with_a_warning() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.state("n2", NodeState::Down);
    let d = w.check(drain("n2"), &Facts::default());
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!(
        said(&d),
        [
            "warning unreachable Node n2: not ready: what it runs is not known, so its drain completes once it is back"
        ]
    );
}

/// An unused Volume may be asked onto a node yet to join; one holding
/// data stays where it is.
#[test]
fn a_volume_asked_onto_a_node_not_there() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.volume_of("cache", serde_json::json!({}), "");
    let to = |volume: &str| Change::MoveVolume {
        namespace: "shop".into(),
        volume: volume.into(),
        node: "n9".into(),
    };
    let d = w.check(to("cache"), &Facts::default());
    assert!(!d.blocks(), "{:?}", said(&d));
    assert_eq!(
        said(&d),
        [
            "warning absent Volume shop/cache: node n9 is not in the cluster: its first task waits for it"
        ]
    );
    w.volume("held", "n1");
    let d = w.check(to("held"), &Facts::default());
    assert_eq!(
        said(&d),
        ["blocking stranded Volume shop/held: stays on n1: node n9 is not in the cluster"]
    );
}

/// A budget that lets no task stop holds the drain: it blocks, saying so,
/// and says where the data would go and how much there is.
#[test]
fn a_budget_that_lets_nothing_stop_blocks_the_drain() {
    let mut w = World::new();
    w.node("n1", &[]);
    w.node("n2", &[]);
    w.service(
        "db",
        &format!(
            "{}\nmaxUnavailable: 0",
            STATEFUL.replace("replicas: 3", "replicas: 1")
        ),
    );
    w.settle();
    let from = w.placed("db")[0].clone();
    let to = if from == "n1" { "n2" } else { "n1" };
    let mut facts = Facts::default();
    facts.used.insert("shop/data-db-0".into(), 200 << 20);
    let d = w.check(drain(&from), &facts);
    assert!(d.blocks());
    assert_eq!(
        said(&d),
        [format!("blocking held Volume shop/data-db-0: its move to {to} waits for its tasks to stop"), format!(
            "blocking stays Task shop/db-0-1: keeps running on {from} (budget)"
        ), "warning budget Service shop/db: its disruption budget (maxUnavailable 0) holds 1 of its task(s) back until the others run again: they move a few at a time".to_owned()]
    );
    assert_eq!(d.moves.len(), 1);
    assert_eq!(
        (d.moves[0].to.as_str(), d.moves[0].bytes),
        (to, Some(200 << 20))
    );
}
