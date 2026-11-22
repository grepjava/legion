//! What tasks and nodes used, kept 30 days (CONSOLE.md, phase 12): a
//! sample a minute for the last day, then one every 15 minutes. The
//! leader makes a minute's sample of each series from what agents report
//! ([`crate::usage`]); every manager keeps them in a file of its own,
//! outside Raft, asking each other manager each minute for what it took
//! since last asked (`Cluster.UsageArchive`), so a new leader has them
//! too, and a manager that led alone hands on what only it had.
//!
//! The file is a line of JSON for each sample, appended as they come and
//! written again, compacted, once an hour. A line that does not read (a
//! write cut short) is skipped.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Between samples for the last [`FINE_FOR`].
pub const FINE: Duration = Duration::from_secs(60);
/// How long samples are kept a minute apart.
pub const FINE_FOR: Duration = Duration::from_secs(24 * 3600);
/// Between samples past [`FINE_FOR`].
pub const COARSE: Duration = Duration::from_secs(15 * 60);
/// How long samples are kept at all.
pub const KEEP: Duration = Duration::from_secs(30 * 24 * 3600);
/// How often the file is written again, compacted.
const COMPACT: Duration = Duration::from_secs(3600);
/// The most samples one answer to another manager carries; it asks again
/// for the rest.
pub const BATCH: usize = 20_000;

/// The file each manager keeps, in its data directory.
pub const FILE: &str = "usage-history.jsonl";

/// A series' sample at a moment, in milliseconds since the epoch.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Entry {
    /// `t/<namespace>/<task>` or `n/<node>`.
    #[serde(rename = "k")]
    pub key: String,
    /// A task's node.
    #[serde(rename = "n", default, skip_serializing_if = "String::is_empty")]
    pub node: String,
    /// A task's Service or Job.
    #[serde(rename = "s", default, skip_serializing_if = "String::is_empty")]
    pub service: String,
    #[serde(rename = "t")]
    pub at_ms: i64,
    /// A task's CPU (percent of a core) and memory (bytes); a node's ZFS
    /// used and free (bytes), received and sent (bytes a second), then its
    /// pool's bytes read and written and read and write operations (each a
    /// second). A node's entries written before the pool's I/O was kept
    /// have only the first four.
    pub v: Vec<f64>,
}

/// The key of a task's series.
pub fn task_key(namespace: &str, task: &str) -> String {
    format!("t/{namespace}/{task}")
}

/// The key of a node's series.
pub fn node_key(node: &str) -> String {
    format!("n/{node}")
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct Series {
    pub node: String,
    pub service: String,
    /// Oldest first.
    pub points: VecDeque<(i64, Vec<f64>)>,
}

#[derive(Default)]
pub struct Archive {
    series: BTreeMap<String, Series>,
    path: Option<PathBuf>,
    file: Option<std::fs::File>,
    compacted_ms: i64,
}

pub type Shared = Arc<Mutex<Archive>>;

fn ms(d: Duration) -> i64 {
    d.as_millis() as i64
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl Archive {
    /// The archive kept in `path`, read back: none if it is not there.
    pub fn open(path: &Path, now_ms: i64) -> Archive {
        let mut a = Archive {
            path: Some(path.to_owned()),
            ..Default::default()
        };
        match std::fs::File::open(path) {
            Ok(f) => {
                for line in std::io::BufReader::new(f).lines() {
                    let Ok(line) = line else { break };
                    if let Ok(e) = serde_json::from_str::<Entry>(&line) {
                        a.insert(e);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!("usage history {}: {e}", path.display()),
        }
        a.compact(now_ms);
        a
    }

    /// Takes `e` if it is its series' latest, or, from before, if the
    /// series has nothing in its 15 minutes: a gap filled from another
    /// manager, never a sample beside one already made of them.
    fn insert(&mut self, e: Entry) -> bool {
        if e.v.iter().any(|v| !v.is_finite()) {
            return false;
        }
        let s = self.series.entry(e.key).or_default();
        if s.points.back().is_none_or(|(at, _)| *at < e.at_ms) {
            if !e.node.is_empty() {
                s.node = e.node;
            }
            if !e.service.is_empty() {
                s.service = e.service;
            }
            s.points.push_back((e.at_ms, e.v));
            return true;
        }
        let coarse = ms(COARSE);
        let start = e.at_ms.div_euclid(coarse) * coarse;
        let i = s.points.partition_point(|(at, _)| *at < start);
        if s.points.get(i).is_some_and(|(at, _)| *at < start + coarse) {
            return false;
        }
        if s.node.is_empty() {
            s.node = e.node;
        }
        if s.service.is_empty() {
            s.service = e.service;
        }
        s.points.insert(i, (e.at_ms, e.v));
        true
    }

    /// Adds samples, those taken appended to the file; compacts it once
    /// an hour.
    pub fn add(&mut self, entries: Vec<Entry>, now_ms: i64) {
        let mut lines = Vec::new();
        for e in entries {
            let line = serde_json::to_vec(&e).expect("serializes");
            if self.insert(e) {
                lines.extend(line);
                lines.push(b'\n');
            }
        }
        if now_ms - self.compacted_ms >= ms(COMPACT) {
            self.compact(now_ms);
        } else if !lines.is_empty() {
            self.append(&lines);
        }
    }

    fn append(&mut self, lines: &[u8]) {
        let Some(path) = &self.path else { return };
        if self.file.is_none() {
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(f) => self.file = Some(f),
                Err(e) => {
                    tracing::warn!("usage history {}: {e}", path.display());
                    return;
                }
            }
        }
        if let Some(f) = &mut self.file
            && let Err(e) = f.write_all(lines)
        {
            tracing::warn!("usage history {}: {e}", path.display());
            self.file = None;
        }
    }

    /// Drops what is older than [`KEEP`], makes each whole 15 minutes
    /// older than [`FINE_FOR`] one sample (their mean), and writes the
    /// file again.
    pub fn compact(&mut self, now_ms: i64) {
        self.compacted_ms = now_ms;
        let oldest = now_ms - ms(KEEP);
        let coarse = ms(COARSE);
        // Only whole buckets: one already made stays as it is.
        let before = (now_ms - ms(FINE_FOR)).div_euclid(coarse) * coarse;
        self.series.retain(|_, s| {
            while s.points.front().is_some_and(|(at, _)| *at < oldest) {
                s.points.pop_front();
            }
            let old = s.points.iter().take_while(|(at, _)| *at < before).count();
            if old > 0 {
                let mut merged: VecDeque<(i64, Vec<f64>)> = VecDeque::new();
                let mut bucket: Option<(i64, Vec<f64>, usize)> = None;
                for (at, v) in s.points.drain(..old) {
                    let b = at.div_euclid(coarse) * coarse;
                    match &mut bucket {
                        Some((start, sum, n)) if *start == b && sum.len() == v.len() => {
                            sum.iter_mut().zip(&v).for_each(|(s, v)| *s += v);
                            *n += 1;
                        }
                        _ => {
                            if let Some(done) = bucket.take() {
                                merged.push_back(mean(done));
                            }
                            bucket = Some((b, v, 1));
                        }
                    }
                }
                if let Some(done) = bucket {
                    merged.push_back(mean(done));
                }
                merged.extend(s.points.drain(..));
                s.points = merged;
            }
            !s.points.is_empty()
        });
        self.rewrite();
    }

    fn rewrite(&mut self) {
        let Some(path) = self.path.clone() else {
            return;
        };
        self.file = None;
        let tmp = path.with_extension("jsonl.new");
        let written = (|| -> std::io::Result<()> {
            let mut w = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
            for e in self.entries_since(i64::MIN, usize::MAX) {
                serde_json::to_writer(&mut w, &e)?;
                w.write_all(b"\n")?;
            }
            w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            std::fs::rename(&tmp, &path)
        })();
        if let Err(e) = written {
            tracing::warn!("usage history {}: {e}", path.display());
            let _ = std::fs::remove_file(&tmp);
        }
    }

    /// The samples after `since_ms`, oldest first: about `limit` of them,
    /// never splitting the samples of one moment, so asking again from the
    /// last one's time misses none.
    pub fn entries_since(&self, since_ms: i64, limit: usize) -> Vec<Entry> {
        let mut all: Vec<Entry> = self
            .series
            .iter()
            .flat_map(|(key, s)| {
                let first = s.points.partition_point(|(at, _)| *at <= since_ms);
                s.points.range(first..).map(|(at, v)| Entry {
                    key: key.clone(),
                    node: s.node.clone(),
                    service: s.service.clone(),
                    at_ms: *at,
                    v: v.clone(),
                })
            })
            .collect();
        all.sort_by_key(|e| e.at_ms);
        if all.len() > limit {
            let last = all[limit - 1].at_ms;
            let end = all.partition_point(|e| e.at_ms <= last);
            all.truncate(end);
        }
        all
    }

    /// Each series' samples from `from_ms` on, one per `step_ms` (the mean
    /// of those in it, at its start).
    pub fn range(&self, from_ms: i64, step_ms: i64) -> BTreeMap<String, Series> {
        self.series
            .iter()
            .filter_map(|(key, s)| {
                let first = s.points.partition_point(|(at, _)| *at < from_ms);
                let mut points = VecDeque::new();
                let mut bucket: Option<(i64, Vec<f64>, usize)> = None;
                for (at, v) in s.points.range(first..) {
                    let b = at.div_euclid(step_ms) * step_ms;
                    match &mut bucket {
                        Some((start, sum, n)) if *start == b && sum.len() == v.len() => {
                            sum.iter_mut().zip(v).for_each(|(s, v)| *s += v);
                            *n += 1;
                        }
                        _ => {
                            if let Some(done) = bucket.take() {
                                points.push_back(mean(done));
                            }
                            bucket = Some((b, v.clone(), 1));
                        }
                    }
                }
                if let Some(done) = bucket {
                    points.push_back(mean(done));
                }
                (!points.is_empty()).then(|| {
                    (
                        key.clone(),
                        Series {
                            node: s.node.clone(),
                            service: s.service.clone(),
                            points,
                        },
                    )
                })
            })
            .collect()
    }
}

fn mean((at, sum, n): (i64, Vec<f64>, usize)) -> (i64, Vec<f64>) {
    (at, sum.into_iter().map(|s| s / n as f64).collect())
}

/// A node's sample from its series' values at `at_ms` (see [`Entry::v`]);
/// `None` with fewer than the four every entry has. What an entry written
/// before the pool's I/O was kept lacks is 0.
pub fn node_sample(at_ms: i64, v: &[f64]) -> Option<crate::usage::NodeSample> {
    if v.len() < 4 {
        return None;
    }
    let at = |i: usize| v.get(i).copied().unwrap_or(0.0) as u64;
    Some(crate::usage::NodeSample {
        at_ms,
        zfs_used_bytes: at(0),
        zfs_avail_bytes: at(1),
        net_rx_bps: at(2),
        net_tx_bps: at(3),
        zfs_read_bps: at(4),
        zfs_write_bps: at(5),
        zfs_read_iops: at(6),
        zfs_write_iops: at(7),
    })
}

/// The step a chart of the last `range` is drawn with: a minute within a
/// day, 15 minutes past it.
pub fn step_for(range: Duration) -> Duration {
    if range <= FINE_FOR { FINE } else { COARSE }
}

/// A minute's sample of each series the leader heard of, from its own
/// samples after `since_ms` up to `at_ms`: their mean, at `at_ms`. A task's
/// service is as `service_of` says.
pub fn minute(
    usage: &crate::usage::Usage,
    since_ms: i64,
    at_ms: i64,
    service_of: impl Fn(&str) -> Option<String>,
) -> Vec<Entry> {
    let within = |t: i64| t > since_ms && t <= at_ms;
    let mut out = Vec::new();
    for (task, (node, samples)) in usage.history() {
        let s: Vec<_> = samples.iter().filter(|s| within(s.at_ms)).collect();
        if s.is_empty() {
            continue;
        }
        let n = s.len() as f64;
        let Some((ns, name)) = task.split_once('/') else {
            continue;
        };
        out.push(Entry {
            key: task_key(ns, name),
            service: service_of(&task).unwrap_or_default(),
            node,
            at_ms,
            v: vec![
                s.iter().map(|s| f64::from(s.cpu)).sum::<f64>() / n,
                s.iter().map(|s| s.memory as f64).sum::<f64>() / n,
            ],
        });
    }
    for (node, samples) in usage.node_history() {
        let s: Vec<_> = samples.iter().filter(|s| within(s.at_ms)).collect();
        if s.is_empty() {
            continue;
        }
        let n = s.len() as f64;
        let avg = |f: fn(&crate::usage::NodeSample) -> u64| {
            s.iter().map(|s| f(s) as f64).sum::<f64>() / n
        };
        out.push(Entry {
            key: node_key(&node),
            node: String::new(),
            service: String::new(),
            at_ms,
            v: vec![
                avg(|s| s.zfs_used_bytes),
                avg(|s| s.zfs_avail_bytes),
                avg(|s| s.net_rx_bps),
                avg(|s| s.net_tx_bps),
                avg(|s| s.zfs_read_bps),
                avg(|s| s.zfs_write_bps),
                avg(|s| s.zfs_read_iops),
                avg(|s| s.zfs_write_iops),
            ],
        });
    }
    out
}

/// Each minute: the leader samples what it heard, and every manager
/// asks the others for what they took since it last asked. All keep it.
pub(crate) async fn run(api: crate::api::ApiService) {
    let mut closing = api.closing.clone();
    let mut last_ms = now_ms();
    // By manager id: the latest sample it handed over.
    let mut since: HashMap<u64, i64> = HashMap::new();
    loop {
        // On the minute.
        let now = now_ms();
        let next = (now.div_euclid(ms(FINE)) + 1) * ms(FINE);
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis((next - now).max(0) as u64)) => {}
            _ = closing.wait_for(|c| *c) => return,
        }
        let at = next;
        let metrics = api.store.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(api.id) {
            let services: HashMap<String, String> = api
                .store
                .list(legion_api::Kind::Task, None)
                .map(|l| l.objects)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|t| {
                    let spec: legion_api::task::TaskSpec = legion_api::parse(&t.spec).ok()?;
                    Some((
                        format!("{}/{}", t.metadata.namespace, t.metadata.name),
                        spec.service,
                    ))
                })
                .collect();
            let entries = minute(&api.usage, last_ms, at, |t| services.get(t).cloned());
            let archive = api.history.clone();
            let _ = tokio::task::spawn_blocking(move || {
                archive.lock().unwrap().add(entries, at);
            })
            .await;
        }
        last_ms = at;
        let others: Vec<(u64, String)> = metrics
            .membership_config
            .membership()
            .nodes()
            .filter(|(id, _)| **id != api.id)
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect();
        since.retain(|id, _| others.iter().any(|(o, _)| o == id));
        for (id, addr) in others {
            let from = since.entry(id).or_insert(i64::MIN);
            match gather(&api, &addr, *from).await {
                Ok(latest) => *from = latest,
                Err(e) => tracing::debug!(manager = id, "usage history: {e}"),
            }
        }
    }
}

/// Takes what the manager at `addr` has after `since_ms`, until it has
/// it all; the latest time it handed over.
async fn gather(
    api: &crate::api::ApiService,
    addr: &str,
    mut since_ms: i64,
) -> Result<i64, tonic::Status> {
    let mut c = legion_proto::v1::cluster_client::ClusterClient::new(
        crate::raft_net::connect_lazy(addr, api.peer_tls.as_ref()),
    )
    .max_decoding_message_size(crate::raft_net::MAX_MESSAGE);
    loop {
        let asked = c.usage_archive(legion_proto::v1::UsageArchiveRequest { since_ms });
        let r = tokio::time::timeout(Duration::from_secs(30), asked)
            .await
            .map_err(|_| tonic::Status::unavailable("no answer"))??
            .into_inner();
        let entries: Vec<Entry> = serde_json::from_slice(&r.entries_json)
            .map_err(|e| tonic::Status::internal(format!("its samples: {e}")))?;
        let more = entries.len() >= BATCH;
        if let Some(last) = entries.last() {
            since_ms = last.at_ms;
        }
        let archive = api.history.clone();
        tokio::task::spawn_blocking(move || {
            archive.lock().unwrap().add(entries, now_ms());
        })
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        if !more {
            return Ok(since_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;
    const DAY: i64 = 24 * 60 * MIN;

    fn e(key: &str, at_ms: i64, v: f64) -> Entry {
        Entry {
            key: key.into(),
            node: "n1".into(),
            service: "web".into(),
            at_ms,
            v: vec![v, 2.0 * v],
        }
    }

    #[test]
    fn a_day_is_kept_a_minute_apart_then_fifteen_minutes_for_thirty_days() {
        let start = 1_000 * DAY;
        let mut a = Archive::default();
        // Two days and a bit, a sample a minute.
        let end = start + 2 * DAY + 37 * MIN;
        let mut at = start;
        while at <= end {
            a.add(vec![e("t/a/web-1-1", at, (at / MIN % 15) as f64)], at);
            at += MIN;
        }
        a.compact(end);
        let s = &a.series["t/a/web-1-1"];
        let fine_from = (end - DAY).div_euclid(15 * MIN) * 15 * MIN;
        for w in s.points.iter().collect::<Vec<_>>().windows(2) {
            let gap = w[1].0 - w[0].0;
            if w[1].0 <= fine_from {
                assert_eq!(gap, 15 * MIN, "{w:?}");
            } else if w[0].0 >= fine_from {
                assert_eq!(gap, MIN, "{w:?}");
            }
        }
        // Each 15-minute sample is the mean of its minutes: 0..15 → 7.
        let (_, v) = &s.points[0];
        assert_eq!(v, &vec![7.0, 14.0]);
        // Compacting again changes nothing.
        let before = a.series.clone();
        a.compact(end);
        assert_eq!(a.series, before);
        // Thirty days on, the first days are gone.
        a.compact(start + 31 * DAY);
        let s = &a.series["t/a/web-1-1"];
        assert!(s.points[0].0 >= start + DAY, "{:?}", s.points[0]);
        // And past them, the series with them.
        a.compact(end + 31 * DAY);
        assert!(a.series.is_empty());
    }

    #[test]
    fn the_file_outlives_the_manager() {
        let dir = std::env::temp_dir().join(format!("legion-history-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(FILE);
        let _ = std::fs::remove_file(&path);
        let start = 1_000 * DAY;
        let mut a = Archive::open(&path, start);
        for i in 0..90 {
            a.add(
                vec![
                    e("t/a/web-1-1", start + i * MIN, 1.0),
                    e("n/n1", start + i * MIN, 3.0),
                ],
                start + i * MIN,
            );
        }
        // A line cut short, as by a crash, and one the same again.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"{\"k\":\"t/a/web-1-1\",\"t\":12").unwrap();
        f.write_all(b"\n").unwrap();
        drop(f);
        a.append(
            &[
                serde_json::to_vec(&e("n/n1", start, 3.0)).unwrap(),
                b"\n".to_vec(),
            ]
            .concat(),
        );
        let b = Archive::open(&path, start + 90 * MIN);
        assert_eq!(b.series, a.series);
        assert_eq!(b.series["t/a/web-1-1"].points.len(), 90);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_follower_asking_again_from_the_last_misses_none() {
        let mut a = Archive::default();
        for i in 0..10 {
            let v = (0..5)
                .map(|k| e(&format!("t/a/w-{k}"), i * MIN, 1.0))
                .collect();
            a.add(v, i * MIN);
        }
        let mut b = Archive::default();
        let mut since = i64::MIN;
        loop {
            let got = a.entries_since(since, 7);
            let more = got.len() >= 7;
            // Never a moment split between two answers.
            assert_eq!(got.len() % 5, 0);
            since = got.last().map_or(since, |e| e.at_ms);
            b.add(got, 0);
            if !more {
                break;
            }
        }
        assert_eq!(b.series, a.series);
        // Asked again, nothing new.
        assert!(a.entries_since(since, 7).is_empty());
    }

    #[test]
    fn a_gap_is_filled_from_another_manager_but_nothing_is_doubled() {
        let mut a = Archive::default();
        // This minute, then a week every 15 minutes from another manager:
        // what a new leader has after the one that kept the week lost the
        // lead.
        let now = 1_000 * DAY;
        a.add(vec![e("t/a/web-1-1", now, 1.0)], now);
        let week: Vec<_> = (1..=7 * 96)
            .rev()
            .map(|i| e("t/a/web-1-1", now - i * 15 * MIN, 2.0))
            .collect();
        a.add(week.clone(), now);
        let s = &a.series["t/a/web-1-1"];
        assert_eq!(s.points.len(), 7 * 96 + 1);
        assert!(
            s.points
                .iter()
                .zip(s.points.iter().skip(1))
                .all(|(x, y)| x.0 < y.0)
        );
        // The same again, or a minute within those 15 minutes: nothing.
        a.add(week, now);
        a.add(vec![e("t/a/web-1-1", now - 15 * MIN + MIN, 3.0)], now);
        assert_eq!(a.series["t/a/web-1-1"].points.len(), 7 * 96 + 1);
    }

    #[test]
    fn a_minute_is_the_mean_of_what_the_leader_heard_in_it() {
        use legion_api::dispatch::{NodeTelemetry, TaskUsage};
        let u = crate::usage::Usage::default();
        let start = tokio::time::Instant::now();
        let used = |cpu, memory| [("a/web-1-1".to_owned(), TaskUsage { cpu, memory })];
        // Four samples 15 s apart: two in the minute before, two in it.
        for (i, (cpu, mem)) in [(1.0, 10), (3.0, 30), (10.0, 100), (20.0, 300)]
            .into_iter()
            .enumerate()
        {
            let at = start + crate::usage::STEP * i as u32;
            u.record_at("w1", used(cpu, mem), |_| true, at, i as i64 * 15_000);
        }
        u.record_node(
            "w1",
            NodeTelemetry {
                zfs_used_bytes: 8,
                zfs_avail_bytes: 2,
                net_rx_bps: 4,
                net_tx_bps: 6,
                zfs_read_bps: 5000,
                zfs_write_bps: 7000,
                zfs_read_iops: 50,
                zfs_write_iops: 70,
                ..Default::default()
            },
            |_, _| true,
        );
        let now = now_ms();
        let got = minute(&u, 15_000, 60_000, |t| {
            (t == "a/web-1-1").then(|| "web".into())
        });
        assert_eq!(
            got,
            vec![Entry {
                key: "t/a/web-1-1".into(),
                node: "w1".into(),
                service: "web".into(),
                at_ms: 60_000,
                v: vec![15.0, 200.0],
            }],
            "the node's sample is not in that minute"
        );
        let got = minute(&u, now - 60_000, now + 60_000, |_| None);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].key, "n/w1");
        assert_eq!(
            got[0].v,
            vec![8.0, 2.0, 4.0, 6.0, 5000.0, 7000.0, 50.0, 70.0]
        );
        let s = node_sample(now, &got[0].v).unwrap();
        assert_eq!(
            (
                s.net_rx_bps,
                s.zfs_read_bps,
                s.zfs_write_bps,
                s.zfs_read_iops,
                s.zfs_write_iops
            ),
            (4, 5000, 7000, 50, 70)
        );
    }

    #[test]
    fn a_node_entry_from_before_the_pool_io_still_loads() {
        // A line as a manager wrote it before the pool's I/O was kept.
        let old: Entry =
            serde_json::from_str(r#"{"k":"n/w1","t":60000,"v":[8.0,2.0,4.0,6.0]}"#).unwrap();
        let mut a = Archive::default();
        a.add(vec![old], 0);
        let r = a.range(0, MIN);
        let (at, v) = &r["n/w1"].points[0];
        assert_eq!(
            node_sample(*at, v),
            Some(crate::usage::NodeSample {
                at_ms: 60_000,
                zfs_used_bytes: 8,
                zfs_avail_bytes: 2,
                net_rx_bps: 4,
                net_tx_bps: 6,
                ..Default::default()
            })
        );
        // Fewer than the four every node entry has is no sample.
        assert_eq!(node_sample(0, &[1.0, 2.0]), None);
    }

    #[test]
    fn a_range_is_drawn_a_step_apart() {
        let mut a = Archive::default();
        for i in 0..60 {
            a.add(vec![e("n/n1", i * MIN, i as f64)], 0);
        }
        let r = a.range(30 * MIN, 15 * MIN);
        let p: Vec<_> = r["n/n1"]
            .points
            .iter()
            .map(|(at, v)| (*at / MIN, v[0]))
            .collect();
        assert_eq!(p, vec![(30, 37.0), (45, 52.0)]);
        // Samples that are no numbers are not taken.
        a.add(vec![e("n/n2", 0, f64::NAN)], 0);
        assert!(!a.series.contains_key("n/n2") || a.series["n/n2"].points.is_empty());
    }
}
