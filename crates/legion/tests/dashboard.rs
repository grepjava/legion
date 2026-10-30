//! The Grafana dashboard in packaging/grafana reads only metrics the
//! daemons export.

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
