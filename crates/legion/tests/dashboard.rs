//! The Grafana dashboard in packaging/grafana reads only metrics the
//! daemons export, and takes the cluster's once though every manager
//! reports it.

#[test]
fn the_dashboard_reads_metrics_the_daemons_export() {
    let dashboard = include_str!("../../../packaging/grafana/legion.json");
    let parsed: serde_json::Value = serde_json::from_str(dashboard).expect("valid JSON");
    assert_eq!(parsed["uid"], "legion-cluster");
    let exported = [
        include_str!("../../legion-manager/src/metrics.rs"),
        include_str!("../../legion-agent/src/metrics.rs"),
    ]
    .concat();
    let mut names: Vec<&str> = Vec::new();
    let mut rest = dashboard;
    while let Some(at) = rest.find("legion_") {
        let tail = &rest[at..];
        let end = tail
            .find(|c: char| !(c.is_ascii_lowercase() || c == '_'))
            .unwrap_or(tail.len());
        names.push(&tail[..end]);
        rest = &tail[end..];
    }
    assert!(names.len() > 10, "{names:?}");
    for name in names {
        assert!(
            exported.contains(&format!("\"{name}\"")),
            "the dashboard reads {name}, which no daemon exports"
        );
    }
}

/// Every manager reports the cluster (its objects, tasks, nodes, quotas
/// and storage), so each such series comes once per manager scraped: the
/// dashboard takes it once (`max without (instance)`) before summing or
/// showing it, or a three-manager cluster would show three times its
/// tasks.
#[test]
fn the_dashboard_takes_the_cluster_once_across_managers() {
    let dashboard = include_str!("../../../packaging/grafana/legion.json");
    let manager = include_str!("../../legion-manager/src/metrics.rs");
    // The manager's own metrics (API requests, Raft) are per manager; the
    // rest are the cluster's.
    let cluster: Vec<&str> = manager
        .split('"')
        .filter(|s| {
            s.starts_with("legion_")
                && s.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && !s.starts_with("legion_raft_")
                && !s.starts_with("legion_api_")
        })
        .collect();
    assert!(cluster.contains(&"legion_tasks"), "{cluster:?}");
    let parsed: serde_json::Value = serde_json::from_str(dashboard).expect("valid JSON");
    let mut exprs = Vec::new();
    fn walk<'a>(v: &'a serde_json::Value, out: &mut Vec<&'a str>) {
        match v {
            serde_json::Value::Object(o) => {
                if let Some(e) = o.get("expr").and_then(|e| e.as_str()) {
                    out.push(e);
                }
                o.values().for_each(|v| walk(v, out));
            }
            serde_json::Value::Array(a) => a.iter().for_each(|v| walk(v, out)),
            _ => {}
        }
    }
    walk(&parsed, &mut exprs);
    let mut read = 0;
    for expr in exprs {
        let mut rest = expr;
        while let Some(at) = rest.find("legion_") {
            let tail = &rest[at..];
            let end = tail
                .find(|c: char| !(c.is_ascii_lowercase() || c == '_'))
                .unwrap_or(tail.len());
            let name = &tail[..end];
            if cluster.contains(&name) {
                read += 1;
                let before = &expr[..expr.len() - rest.len() + at];
                assert!(
                    before.ends_with("max without (instance) ("),
                    "{name} is read once per manager in: {expr}"
                );
            }
            rest = &tail[end..];
        }
    }
    assert!(read > 10, "{read}");
}
