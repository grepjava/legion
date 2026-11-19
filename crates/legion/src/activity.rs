//! `legion activity`: long operations as the leader follows them
//! (`Cluster.Activity`, legion_api::activity), as a table, one
//! operation's full output, or followed a line per change. `legion top`'s
//! Activity tab shows the same rows.

use std::collections::BTreeMap;

use legion_api::activity::{Operation, OperationState};
use legion_proto::v1 as pb;
use tonic::transport::Channel;

use crate::cli::Format;

/// How long, as `1h02m`, `3m05s` or `12s`.
pub(crate) fn took(ms: i64) -> String {
    let s = (ms / 1000).max(0);
    match (s / 3600, (s % 3600) / 60, s % 60) {
        (0, 0, s) => format!("{s}s"),
        (0, m, s) => format!("{m}m{s:02}s"),
        (h, m, _) => format!("{h}h{m:02}m"),
    }
}

/// Its state with how long it ran or has been running: `running 1m05s`.
pub(crate) fn state_cell(op: &Operation, now_ms: i64) -> String {
    let until = op.ended_ms.unwrap_or(now_ms);
    format!("{} {}", op.state.as_str(), took(until - op.began_ms))
}

/// What it has sent in its stage, empty for none.
pub(crate) fn sent_cell(op: &Operation) -> String {
    match op.bytes {
        0 => String::new(),
        n => legion_api::activity::bytes(n),
    }
}

/// A table's row: began, what, stage, sent, state, by, message.
pub(crate) fn row(op: &Operation, now_ms: i64) -> [String; 7] {
    [
        legion_api::clock::format(op.began_ms.div_euclid(1000)),
        op.what.clone(),
        op.stage.to_string(),
        sent_cell(op),
        state_cell(op, now_ms),
        op.by.clone(),
        op.message.clone(),
    ]
}

pub(crate) const HEADER: [&str; 7] = ["BEGAN", "WHAT", "STAGE", "SENT", "STATE", "BY", "MESSAGE"];

/// A followed change in a line: when, what, where it is now.
fn line(op: &Operation, now_ms: i64) -> String {
    let mut s = format!(
        "{}  {}  {}",
        legion_api::clock::format(now_ms.div_euclid(1000)),
        op.what,
        op.stage
    );
    let sent = sent_cell(op);
    if !sent.is_empty() {
        s.push_str(&format!(" ({sent})"));
    }
    if op.state != OperationState::Running {
        s.push_str(&format!("  {}", op.state.as_str()));
    }
    if !op.message.is_empty() {
        s.push_str(&format!(": {}", op.message));
    }
    s
}

/// One operation in full: what it is, each stage with when, and all it
/// said.
pub(crate) fn detail(op: &Operation, now_ms: i64) -> String {
    let mut out = format!(
        "{}\n  id       {}\n  state    {}\n  began    {}\n",
        op.what,
        op.id,
        state_cell(op, now_ms),
        legion_api::clock::format(op.began_ms.div_euclid(1000))
    );
    if let Some(e) = op.ended_ms {
        out.push_str(&format!(
            "  ended    {}\n",
            legion_api::clock::format(e.div_euclid(1000))
        ));
    }
    if !op.by.is_empty() {
        out.push_str(&format!("  by       {}\n", op.by));
    }
    if !op.part_of.is_empty() {
        out.push_str(&format!("  part of  {}\n", op.part_of));
    }
    let stages: Vec<String> = op
        .stages
        .iter()
        .map(|m| format!("{} {}", m.stage, took(m.at_ms - op.began_ms)))
        .collect();
    out.push_str(&format!("  stages   {}\n", stages.join(", ")));
    out.push_str("output:\n");
    for l in &op.output {
        out.push_str(&format!(
            "  {}  {}\n",
            legion_api::clock::format(l.at_ms.div_euclid(1000)),
            l.text
        ));
    }
    out
}

fn parse(json: &[u8]) -> anyhow::Result<Operation> {
    Ok(serde_json::from_slice(json)?)
}

pub(crate) async fn run(
    channel: Channel,
    id: Option<String>,
    watch: bool,
    output: Format,
) -> anyhow::Result<()> {
    let mut cluster = crate::cli::cluster_client(channel);
    let mut stream = cluster
        .activity(pb::ActivityRequest {
            follow: watch && id.is_none(),
        })
        .await?
        .into_inner();
    let now = || legion_api::clock::now() * 1000;
    let Some(first) = stream.message().await? else {
        return Ok(());
    };
    let ops: Vec<Operation> = first
        .operations_json
        .iter()
        .map(|j| parse(j))
        .collect::<anyhow::Result<_>>()?;
    if let Some(id) = id {
        // The one whose id is that or starts so, or whose target is named
        // so: the newest.
        let found = ops
            .iter()
            .rev()
            .find(|o| o.id == id || o.id.starts_with(&id) || o.target.name == id)
            .ok_or_else(|| anyhow::anyhow!("no operation {id} kept"))?;
        match output {
            Format::Json => println!("{}", serde_json::to_string_pretty(found)?),
            Format::Yaml => print!("{}", serde_yaml_ng::to_string(found)?),
            Format::Table => print!("{}", detail(found, now())),
        }
        return Ok(());
    }
    match output {
        Format::Json => {
            println!("{}", serde_json::to_string_pretty(&ops)?);
            if !watch {
                return Ok(());
            }
        }
        Format::Yaml => {
            print!("{}", serde_yaml_ng::to_string(&ops)?);
            if !watch {
                return Ok(());
            }
        }
        Format::Table if ops.is_empty() && !watch => {
            println!("no operation under way or ended in the last hour");
            return Ok(());
        }
        Format::Table => {
            let rows: Vec<[String; 7]> = ops.iter().map(|o| row(o, now())).collect();
            crate::cli::print_table(HEADER, &rows);
        }
    }
    // Followed: a line per change, as it says something new.
    let mut said: BTreeMap<String, (String, OperationState, String, String)> = ops
        .iter()
        .map(|o| {
            (
                o.id.clone(),
                (
                    o.stage.to_string(),
                    o.state,
                    o.message.clone(),
                    sent_cell(o),
                ),
            )
        })
        .collect();
    while let Some(r) = stream.message().await? {
        for json in r.operations_json {
            let op = parse(&json)?;
            if matches!(output, Format::Json) {
                println!("{}", serde_json::to_string(&op)?);
                continue;
            }
            let now_said = (
                op.stage.to_string(),
                op.state,
                op.message.clone(),
                sent_cell(&op),
            );
            // Bytes alone are said at most every so often: each change of
            // stage, state or message at once.
            let before = said.get(&op.id);
            let news =
                before.is_none_or(|b| b.0 != now_said.0 || b.1 != now_said.1 || b.2 != now_said.2);
            if news || before.is_some_and(|b| sent_step(&b.3, &now_said.3)) {
                println!("{}", line(&op, now()));
                said.insert(op.id.clone(), now_said);
            }
        }
    }
    Ok(())
}

/// Whether what was sent moved on enough to say again: a new unit or
/// ten more of it.
fn sent_step(before: &str, now: &str) -> bool {
    let num = |s: &str| {
        let mut p = s.split(' ');
        let n: f64 = p.next().and_then(|n| n.parse().ok()).unwrap_or(0.0);
        (n, p.next().unwrap_or("").to_owned())
    };
    let ((a, ua), (b, ub)) = (num(before), num(now));
    ua != ub || (b - a).abs() >= 10.0
}

#[cfg(test)]
mod tests {
    use legion_api::activity::{OperationKind, Stage};
    use legion_api::audit::Target;

    use super::*;

    fn op() -> Operation {
        let mut op = Operation::new(
            OperationKind::Move,
            Target {
                kind: "Volume".into(),
                namespace: "data".into(),
                name: "pg-db-0".into(),
            },
            "move Volume data/pg-db-0 from n2 to n3".into(),
            1_700_000_000_000,
            Stage::Preparing,
        );
        op.enter(1_700_000_002_000, Stage::FullCopy);
        op.bytes = 5 * 1024 * 1024;
        op
    }

    #[test]
    fn a_row_says_the_stage_what_was_sent_and_for_how_long() {
        let r = row(&op(), 1_700_000_065_000);
        assert_eq!(r[1], "move Volume data/pg-db-0 from n2 to n3");
        assert_eq!(r[2], "full-copy");
        assert_eq!(r[3], "5.0 MiB");
        assert_eq!(r[4], "running 1m05s");
    }

    #[test]
    fn the_detail_lists_each_stage_and_all_it_said() {
        let mut o = op();
        o.end(1_700_000_010_000, OperationState::Succeeded, "on n3");
        let d = detail(&o, 0);
        assert!(d.contains("stages   preparing 0s, full-copy 2s"), "{d}");
        assert!(d.contains("full-copy: 5.0 MiB sent"), "{d}");
        assert!(d.contains("succeeded: on n3"), "{d}");
        assert!(d.contains("state    succeeded 10s"), "{d}");
    }

    #[test]
    fn durations_read_short() {
        assert_eq!(took(12_000), "12s");
        assert_eq!(took(185_000), "3m05s");
        assert_eq!(took(3_720_000), "1h02m");
    }

    #[test]
    fn bytes_are_said_again_on_a_new_unit_or_ten_more() {
        assert!(!sent_step("5.0 MiB", "9.0 MiB"));
        assert!(sent_step("5.0 MiB", "15.0 MiB"));
        assert!(sent_step("900.0 KiB", "1.0 MiB"));
    }
}
