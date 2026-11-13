//! Searching a service's logs across its tasks for a time range (CONSOLE.md
//! phase 4), as `legion logs-search` and the console both do: a logs
//! session per task and container, asked for the range with each line's
//! time, the lines holding the text kept, merged in time order. Only what
//! the API offers, as the caller: a user reads the logs they may read.

use std::collections::BTreeSet;

use futures::StreamExt;
use legion_proto::v1 as pb;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::pods_client::PodsClient;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use tonic::Status;
use tonic::transport::Channel;

/// Sessions open at once.
const AT_ONCE: usize = 8;

/// What to look for.
#[derive(Clone, Debug, Default)]
pub struct Search {
    pub namespace: String,
    /// A Service or Job.
    pub service: String,
    /// From and before, in milliseconds since the epoch.
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// Lines holding this, without regard to case; every line when empty.
    pub text: String,
    /// At most this many lines, the newest; 0 for 1000.
    pub limit: usize,
}

/// A line found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub at_ms: i64,
    pub task: String,
    pub container: String,
    pub stderr: bool,
    pub text: String,
}

/// What a search found, oldest first, and the tasks it could not read with
/// why: a node whose agent does not keep times, one unreachable.
#[derive(Clone, Debug, Default)]
pub struct Results {
    pub lines: Vec<Found>,
    /// More matched than the limit: the oldest were left out.
    pub truncated: bool,
    pub unread: Vec<String>,
}

/// Searches `s.service`'s tasks, those there are now (a task deleted has
/// no logs left to read).
pub async fn search(channel: Channel, s: &Search) -> Result<Results, Status> {
    let listed = ObjectsClient::new(channel.clone())
        .list(pb::ListRequest {
            kind: "Task".into(),
            namespace: s.namespace.clone(),
            match_labels: Default::default(),
        })
        .await?
        .into_inner();
    let mut targets = Vec::new();
    for t in listed.objects {
        let Ok(spec) = serde_json::from_slice::<serde_json::Value>(&t.spec_json) else {
            continue;
        };
        if spec["service"] != s.service.as_str() {
            continue;
        }
        let containers: BTreeSet<String> = spec["pod"]["containers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| c["name"].as_str().map(str::to_owned))
            .collect();
        for c in containers {
            targets.push((t.name.clone(), c));
        }
    }
    if targets.is_empty() {
        return Err(Status::not_found(format!(
            "{} has no tasks in {}",
            s.service, s.namespace
        )));
    }
    let needle = s.text.to_lowercase();
    let read = futures::stream::iter(targets.into_iter().map(|(task, container)| {
        let (channel, s, needle) = (channel.clone(), s.clone(), needle.clone());
        async move {
            let r = read(channel, &s, &task, &container, &needle).await;
            (task, container, r)
        }
    }))
    .buffer_unordered(AT_ONCE)
    .collect::<Vec<_>>()
    .await;
    let mut out = Results::default();
    for (task, container, r) in read {
        match r {
            Ok(mut lines) => out.lines.append(&mut lines),
            Err(why) => out.unread.push(format!("{task} ({container}): {why}")),
        }
    }
    out.unread.sort();
    let limit = if s.limit == 0 { 1000 } else { s.limit };
    out.lines
        .sort_by(|a, b| (a.at_ms, &a.task).cmp(&(b.at_ms, &b.task)));
    if out.lines.len() > limit {
        out.truncated = true;
        out.lines.drain(..out.lines.len() - limit);
    }
    Ok(out)
}

/// One container's lines in the range holding `needle` (lower case).
async fn read(
    channel: Channel,
    s: &Search,
    task: &str,
    container: &str,
    needle: &str,
) -> Result<Vec<Found>, String> {
    let open = pb::SessionFrame {
        frame: Some(Frame::Open(pb::SessionOpen {
            namespace: s.namespace.clone(),
            task: task.to_owned(),
            container: container.to_owned(),
            action: Some(Action::Logs(pb::LogsOptions {
                follow: false,
                tail: None,
                since_ms: s.since_ms,
                until_ms: s.until_ms,
                times: true,
            })),
        })),
    };
    let mut frames = PodsClient::new(channel)
        .session(futures::stream::iter([open]).chain(futures::stream::pending()))
        .await
        .map_err(|e| e.message().to_owned())?
        .into_inner();
    let mut found = Vec::new();
    loop {
        match frames.message().await {
            Ok(Some(pb::SessionFrame {
                frame: Some(Frame::Line(l)),
            })) => {
                let text = String::from_utf8_lossy(&l.data);
                let text = text.strip_suffix('\n').unwrap_or(&text);
                if needle.is_empty() || text.to_lowercase().contains(needle) {
                    found.push(Found {
                        at_ms: l.at_ms,
                        task: task.to_owned(),
                        container: container.to_owned(),
                        stderr: l.stderr,
                        text: text.to_owned(),
                    });
                }
            }
            // An agent from before times: it cannot say when, nor keep
            // to the range.
            Ok(Some(pb::SessionFrame {
                frame: Some(Frame::Stdout(_) | Frame::Stderr(_)),
            })) => return Err("its node's agent gives no times; upgrade it".into()),
            Ok(Some(_)) => {}
            Ok(None) => return Ok(found),
            // A task never started, or whose output went with its node.
            Err(e) if e.code() == tonic::Code::NotFound => return Ok(found),
            Err(e) => return Err(e.message().to_owned()),
        }
    }
}
