//! `legion top`: the cluster, live, in the terminal. It lists nodes,
//! services, Jobs and tasks, then follows a watch to keep them current;
//! asks the leader every few seconds what tasks use; and keeps a log of
//! what changed while it ran. It changes nothing in the cluster.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use legion_api::job::{JobState, JobStatus, LABEL_JOB};
use legion_api::node::{NodeSpec, NodeState, NodeStatus};
use legion_api::service::ServiceStatus;
use legion_api::task::{TaskState, TaskStatus};
use legion_api::{Kind, Object};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{self as pb, ListRequest, StatusRequest, WatchRequest, watch_event};
use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState, Tabs};
use serde::Serialize;
use tokio::sync::mpsc;
use tonic::transport::Channel;

/// What it shows.
const KINDS: [Kind; 4] = [Kind::Node, Kind::Service, Kind::Job, Kind::Task];
/// How often it asks for usage and the managers' state.
const POLL: Duration = Duration::from_secs(5);
/// Changes kept in the log.
const LOG: usize = 500;
/// The label tasks of a service carry.
const LABEL_SERVICE: &str = "legion.service";

/// The cluster as last heard.
#[derive(Default)]
pub struct Model {
    /// Only this namespace's objects, if given.
    namespace: Option<String>,
    objects: BTreeMap<(Kind, String, String), Object>,
    /// What tasks use, by namespace and task: percent of a CPU, bytes.
    usage: BTreeMap<(String, String), (f64, u64)>,
    /// What changed, newest first, with when (Unix seconds).
    log: VecDeque<(i64, String)>,
    cluster: Option<pb::StatusResponse>,
    /// Kinds the caller may not list.
    hidden: BTreeSet<Kind>,
    /// Why it is not current, while it is not.
    problem: Option<String>,
}

impl Model {
    pub fn new(namespace: Option<String>) -> Self {
        Model {
            namespace,
            ..Default::default()
        }
    }

    fn shown(&self, o: &Object) -> bool {
        KINDS.contains(&o.kind)
            && (o.kind.is_cluster_scoped()
                || self
                    .namespace
                    .as_ref()
                    .is_none_or(|ns| *ns == o.metadata.namespace))
    }

    /// Everything listed again, after (re)connecting.
    fn listed(&mut self, objects: Vec<Object>, hidden: BTreeSet<Kind>) {
        self.objects = objects
            .into_iter()
            .filter(|o| self.shown(o))
            .map(|o| (key(&o), o))
            .collect();
        self.hidden = hidden;
        self.problem = None;
    }

    /// A change from the watch, at `at`. Older than what it has (the watch
    /// starts from the oldest listing) changes nothing.
    fn changed(&mut self, put: bool, o: Object, at: i64) {
        if !self.shown(&o) {
            return;
        }
        let k = key(&o);
        let before = self.objects.get(&k);
        if before.is_some_and(|b| b.metadata.version > o.metadata.version) {
            return;
        }
        if put && before.is_some_and(|b| b.metadata.version == o.metadata.version) {
            return;
        }
        let what = if put {
            describe(before, &o)
        } else {
            (o.kind != Kind::Task).then(|| format!("{} deleted", name(&o)))
        };
        if let Some(what) = what {
            self.log.push_front((at, what));
            self.log.truncate(LOG);
        }
        if put {
            self.objects.insert(k, o);
        } else {
            self.objects.remove(&k);
        }
    }

    fn usage(&mut self, u: pb::UsageResponse) {
        self.usage = u
            .nodes
            .into_iter()
            .flat_map(|n| n.tasks)
            .map(|t| ((t.namespace, t.task), (t.cpu_percent, t.memory_bytes)))
            .collect();
    }

    fn of(&self, kind: Kind) -> impl Iterator<Item = &Object> {
        self.objects
            .range((kind, String::new(), String::new())..)
            .take_while(move |((k, _, _), _)| *k == kind)
            .map(|(_, o)| o)
    }

    /// What a task uses, if reported.
    fn task_usage(&self, t: &Object) -> Option<(f64, u64)> {
        self.usage
            .get(&(t.metadata.namespace.clone(), t.metadata.name.clone()))
            .copied()
    }

    /// What tasks matching `f` use together, if any reported.
    fn sum_usage(&self, f: impl Fn(&Object) -> bool) -> Option<(f64, u64)> {
        self.of(Kind::Task)
            .filter(|t| f(t))
            .filter_map(|t| self.task_usage(t))
            .reduce(|(c1, m1), (c2, m2)| (c1 + c2, m1 + m2))
    }
}

fn key(o: &Object) -> (Kind, String, String) {
    (
        o.kind,
        o.metadata.namespace.clone(),
        o.metadata.name.clone(),
    )
}

/// `node n1`, `service shop/web`.
fn name(o: &Object) -> String {
    let kind = o.kind.as_str().to_ascii_lowercase();
    if o.metadata.namespace.is_empty() {
        format!("{kind} {}", o.metadata.name)
    } else {
        format!("{kind} {}/{}", o.metadata.namespace, o.metadata.name)
    }
}

fn status<T: serde::de::DeserializeOwned + Default>(o: &Object) -> T {
    serde_json::from_value(o.status.clone()).unwrap_or_default()
}

fn spec<T: serde::de::DeserializeOwned + Default>(o: &Object) -> T {
    serde_json::from_value(o.spec.clone()).unwrap_or_default()
}

/// An enum as it is written in the API: `ready`, `drain`.
fn word(v: &impl Serialize) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        _ => String::new(),
    }
}

/// What changed in `o`, worth a line in the log.
fn describe(before: Option<&Object>, o: &Object) -> Option<String> {
    let who = name(o);
    match o.kind {
        Kind::Node => {
            let (spec_now, now): (NodeSpec, NodeStatus) = (spec(o), status(o));
            let Some(before) = before else {
                return Some(format!("{who} joined"));
            };
            let (spec_then, then): (NodeSpec, NodeStatus) = (spec(before), status(before));
            let mut changes = Vec::new();
            if then.state != now.state {
                changes.push(format!("{} → {}", word(&then.state), word(&now.state)));
            }
            if spec_then.availability != spec_now.availability {
                changes.push(format!(
                    "{} → {}",
                    word(&spec_then.availability),
                    word(&spec_now.availability)
                ));
            }
            (!changes.is_empty()).then(|| format!("{who}: {}", changes.join(", ")))
        }
        Kind::Service => {
            let Some(before) = before else {
                return Some(format!("{who} created"));
            };
            if before.metadata.generation != o.metadata.generation {
                return Some(format!("{who} changed"));
            }
            let (then, now): (ServiceStatus, ServiceStatus) = (status(before), status(o));
            let state = |s: &ServiceStatus| s.update.as_ref().map(|u| (u.state, u.message.clone()));
            match (state(&then), state(&now)) {
                (a, Some((b, why))) if a.as_ref().map(|a| a.0) != Some(b) => {
                    let why = if why.is_empty() {
                        why
                    } else {
                        format!(": {why}")
                    };
                    Some(format!("{who} update {}{why}", word(&b)))
                }
                _ => None,
            }
        }
        Kind::Job => {
            let now: JobStatus = status(o);
            let Some(before) = before else {
                return Some(format!("{who} created"));
            };
            let then: JobStatus = status(before);
            (then.state != now.state).then(|| {
                let why = if now.message.is_empty() {
                    String::new()
                } else {
                    format!(": {}", now.message)
                };
                format!("{who} {}{why}", word(&now.state))
            })
        }
        Kind::Task => {
            let now: TaskStatus = status(o);
            let then: TaskStatus = before.map(status).unwrap_or_default();
            let node = o.spec["node"].as_str().unwrap_or_default();
            let at = if node.is_empty() {
                String::new()
            } else {
                format!(" on {node}")
            };
            let why = if now.message.is_empty() {
                String::new()
            } else {
                format!(": {}", now.message)
            };
            if before.is_none() || then.state != now.state {
                // The steps on the way to running are not news.
                if !(now.state == TaskState::Running || now.state.is_terminal()) {
                    return None;
                }
                let failed = matches!(now.state, TaskState::Failed | TaskState::Rejected);
                let why = if failed || now.state == TaskState::Running {
                    why
                } else {
                    String::new()
                };
                return Some(format!("{who}{at}: {}{why}", word(&now.state)));
            }
            if now.restarts > then.restarts {
                return Some(format!("{who}{at}: restarted{why}"));
            }
            if now.state == TaskState::Running && now.ready != then.ready {
                let ready = if now.ready { "ready" } else { "not ready" };
                return Some(format!("{who}{at}: {ready}"));
            }
            None
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tab {
    Nodes,
    Workloads,
    Tasks,
    Log,
}

const TABS: [Tab; 4] = [Tab::Nodes, Tab::Workloads, Tab::Tasks, Tab::Log];

impl Tab {
    fn title(self) -> &'static str {
        match self {
            Tab::Nodes => "1 Nodes",
            Tab::Workloads => "2 Services & Jobs",
            Tab::Tasks => "3 Tasks",
            Tab::Log => "4 Changes",
        }
    }
}

/// Which tasks the task list shows.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Focus {
    Node(String),
    /// A service or Job: its kind, namespace and name.
    Owner(Kind, String, String),
}

/// What the person looks at.
pub struct View {
    tab: Tab,
    /// The selected row of each tab.
    selected: [usize; 4],
    focus: Option<Focus>,
    /// Tasks that ended are listed too.
    history: bool,
    quit: bool,
}

impl Default for View {
    fn default() -> Self {
        View {
            tab: Tab::Nodes,
            selected: [0; 4],
            focus: None,
            history: false,
            quit: false,
        }
    }
}

impl View {
    fn index(&self) -> usize {
        TABS.iter().position(|t| *t == self.tab).expect("a tab")
    }

    fn go(&mut self, tab: Tab) {
        self.tab = tab;
    }

    pub fn key(&mut self, model: &Model, k: KeyEvent) {
        let count = rows(model, self).len();
        let i = self.index();
        let sel = &mut self.selected[i];
        match k.code {
            KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => self.quit = true,
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Esc => {
                if self.focus.take().is_none() {
                    self.quit = true;
                }
            }
            KeyCode::Char(c @ '1'..='4') => self.go(TABS[c as usize - '1' as usize]),
            KeyCode::Tab | KeyCode::Right => self.go(TABS[(i + 1) % TABS.len()]),
            KeyCode::BackTab | KeyCode::Left => self.go(TABS[(i + TABS.len() - 1) % TABS.len()]),
            KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(count.saturating_sub(1)),
            KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
            KeyCode::PageDown => *sel = (*sel + 20).min(count.saturating_sub(1)),
            KeyCode::PageUp => *sel = sel.saturating_sub(20),
            KeyCode::Home | KeyCode::Char('g') => *sel = 0,
            KeyCode::End | KeyCode::Char('G') => *sel = count.saturating_sub(1),
            KeyCode::Char('h') => self.history = !self.history,
            KeyCode::Enter => {
                let at = *sel;
                let focus = match (self.tab, rows(model, self).get(at)) {
                    (Tab::Nodes | Tab::Workloads, Some(r)) => r.focus.clone(),
                    _ => None,
                };
                if let Some(focus) = focus {
                    self.focus = Some(focus);
                    self.selected[TABS.iter().position(|t| *t == Tab::Tasks).unwrap()] = 0;
                    self.tab = Tab::Tasks;
                }
            }
            _ => {}
        }
    }
}

/// A row of a table, and the tasks it stands for.
struct TableRow {
    cells: Vec<String>,
    style: Style,
    focus: Option<Focus>,
}

fn rows(model: &Model, view: &View) -> Vec<TableRow> {
    match view.tab {
        Tab::Nodes => node_rows(model),
        Tab::Workloads => workload_rows(model),
        Tab::Tasks => task_rows(model, view),
        Tab::Log => model
            .log
            .iter()
            .map(|(at, what)| TableRow {
                cells: vec![time_of_day(*at), what.clone()],
                style: Style::default(),
                focus: None,
            })
            .collect(),
    }
}

fn header(tab: Tab) -> &'static [&'static str] {
    match tab {
        Tab::Nodes => &[
            "NAME", "STATE", "AVAIL", "ROLE", "TASKS", "CPU", "MEMORY", "LABELS",
        ],
        Tab::Workloads => &[
            "NAMESPACE",
            "NAME",
            "KIND",
            "TASKS",
            "STATE",
            "CPU",
            "MEMORY",
            "IMAGES",
        ],
        Tab::Tasks => &[
            "NAMESPACE",
            "NAME",
            "NODE",
            "STATE",
            "READY",
            "RESTARTS",
            "CPU",
            "MEMORY",
            "IP",
            "MESSAGE",
        ],
        Tab::Log => &["TIME (UTC)", "CHANGE"],
    }
}

fn node_rows(model: &Model) -> Vec<TableRow> {
    model
        .of(Kind::Node)
        .map(|n| {
            let (spec, st): (NodeSpec, NodeStatus) = (spec(n), status(n));
            let name = n.metadata.name.clone();
            let on = |t: &Object| t.spec["node"].as_str() == Some(name.as_str());
            let running = model
                .of(Kind::Task)
                .filter(|t| on(t) && status::<TaskStatus>(t).state == TaskState::Running)
                .count();
            let used = model.sum_usage(|t| on(t));
            let cpu = format!(
                "{}/{}",
                used.map_or("-".into(), |(c, _)| format!("{:.2}", c / 100.0)),
                trim(st.allocatable.cpu.millis() as f64 / 1000.0)
            );
            let memory = format!(
                "{}/{}",
                used.map_or("-".into(), |(_, m)| bytes(m)),
                bytes(st.allocatable.memory.0)
            );
            let labels: Vec<String> = n
                .metadata
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            let style = match st.state {
                NodeState::Ready => Style::default(),
                NodeState::Suspect => Style::default().fg(Color::Yellow),
                NodeState::Down | NodeState::Unknown => Style::default().fg(Color::Red),
            };
            TableRow {
                cells: vec![
                    name.clone(),
                    word(&st.state),
                    word(&spec.availability),
                    word(&spec.role),
                    running.to_string(),
                    cpu,
                    memory,
                    labels.join(","),
                ],
                style,
                focus: Some(Focus::Node(name)),
            }
        })
        .collect()
}

fn workload_rows(model: &Model) -> Vec<TableRow> {
    let owned_by = |t: &Object, kind: Kind, o: &Object| {
        t.metadata.namespace == o.metadata.namespace && owner(t) == Some((kind, &o.metadata.name))
    };
    let images = |o: &Object| -> String {
        o.spec["pod"]["containers"]
            .as_array()
            .map(|cs| {
                cs.iter()
                    .filter_map(|c| c["image"].as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default()
    };
    let services = model.of(Kind::Service).map(|s| {
        let st: ServiceStatus = status(s);
        let used = model.sum_usage(|t| owned_by(t, Kind::Service, s));
        let state = st
            .update
            .as_ref()
            .map(|u| word(&u.state))
            .unwrap_or_default();
        let style = if st.ready < st.desired {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        TableRow {
            cells: vec![
                s.metadata.namespace.clone(),
                s.metadata.name.clone(),
                s.spec["mode"].as_str().unwrap_or("replicated").to_owned(),
                format!("{}/{}", st.ready, st.desired),
                state,
                used.map_or(String::new(), |(c, _)| format!("{c:.0}%")),
                used.map_or(String::new(), |(_, m)| bytes(m)),
                images(s),
            ],
            style,
            focus: Some(Focus::Owner(
                Kind::Service,
                s.metadata.namespace.clone(),
                s.metadata.name.clone(),
            )),
        }
    });
    let jobs = model.of(Kind::Job).map(|j| {
        let st: JobStatus = status(j);
        let used = model.sum_usage(|t| owned_by(t, Kind::Job, j));
        let done = match j.spec["completions"].as_u64() {
            Some(n) => format!("{}/{n}", st.succeeded),
            None => st.succeeded.to_string(),
        };
        let style = match st.state {
            JobState::Failed => Style::default().fg(Color::Red),
            JobState::Complete => Style::default().fg(Color::DarkGray),
            _ => Style::default(),
        };
        TableRow {
            cells: vec![
                j.metadata.namespace.clone(),
                j.metadata.name.clone(),
                "job".into(),
                done,
                word(&st.state),
                used.map_or(String::new(), |(c, _)| format!("{c:.0}%")),
                used.map_or(String::new(), |(_, m)| bytes(m)),
                images(j),
            ],
            style,
            focus: Some(Focus::Owner(
                Kind::Job,
                j.metadata.namespace.clone(),
                j.metadata.name.clone(),
            )),
        }
    });
    services.chain(jobs).collect()
}

/// The service or Job a task is of.
fn owner(t: &Object) -> Option<(Kind, &str)> {
    if let Some(job) = t.metadata.labels.get(LABEL_JOB) {
        return Some((Kind::Job, job));
    }
    t.metadata
        .labels
        .get(LABEL_SERVICE)
        .map(String::as_str)
        .or_else(|| t.spec["service"].as_str())
        .map(|s| (Kind::Service, s))
}

fn task_rows(model: &Model, view: &View) -> Vec<TableRow> {
    let mut rows: Vec<(bool, TableRow)> = model
        .of(Kind::Task)
        .filter(|t| match &view.focus {
            None => true,
            Some(Focus::Node(n)) => t.spec["node"].as_str() == Some(n.as_str()),
            Some(Focus::Owner(kind, ns, name)) => {
                t.metadata.namespace == *ns && owner(t) == Some((*kind, name.as_str()))
            }
        })
        .filter_map(|t| {
            let st: TaskStatus = status(t);
            let ended = st.state.is_terminal();
            if ended && !view.history {
                return None;
            }
            let used = model.task_usage(t);
            let style = match st.state {
                TaskState::Failed | TaskState::Rejected => Style::default().fg(Color::Red),
                _ if ended => Style::default().fg(Color::DarkGray),
                TaskState::Running if st.ready => Style::default(),
                _ => Style::default().fg(Color::Yellow),
            };
            Some((
                ended,
                TableRow {
                    cells: vec![
                        t.metadata.namespace.clone(),
                        t.metadata.name.clone(),
                        t.spec["node"].as_str().unwrap_or_default().to_owned(),
                        word(&st.state),
                        if st.ready { "yes" } else { "" }.to_owned(),
                        st.restarts.to_string(),
                        used.map_or(String::new(), |(c, _)| format!("{c:.0}%")),
                        used.map_or(String::new(), |(_, m)| bytes(m)),
                        st.ip.unwrap_or_default(),
                        st.message,
                    ],
                    style,
                    focus: None,
                },
            ))
        })
        .collect();
    // Those still there first.
    rows.sort_by_key(|(ended, _)| *ended);
    rows.into_iter().map(|(_, r)| r).collect()
}

/// `1.5`, `4`.
fn trim(n: f64) -> String {
    let s = format!("{n:.1}");
    s.strip_suffix(".0").map_or(s.clone(), str::to_owned)
}

/// `512Mi`, `1.2Gi`.
fn bytes(n: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("Ti", 1 << 40),
        ("Gi", 1 << 30),
        ("Mi", 1 << 20),
        ("Ki", 1 << 10),
    ];
    for (unit, size) in UNITS {
        if n >= size {
            return format!("{}{unit}", trim(n as f64 / size as f64));
        }
    }
    n.to_string()
}

/// `09:30:05`, of the day in UTC.
fn time_of_day(unix: i64) -> String {
    legion_api::clock::format(unix)[11..19].to_owned()
}

/// Draws the whole screen.
pub fn render(f: &mut Frame, model: &Model, view: &View) {
    let [top, tabs, body, bottom] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(f.area());
    f.render_widget(Paragraph::new(summary(model)), top);
    f.render_widget(
        Tabs::new(TABS.map(Tab::title))
            .select(view.index())
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
        tabs,
    );

    let hidden = match view.tab {
        Tab::Nodes => model.hidden.contains(&Kind::Node).then_some("nodes"),
        Tab::Tasks => model.hidden.contains(&Kind::Task).then_some("tasks"),
        _ => None,
    };
    if let Some(what) = hidden {
        f.render_widget(
            Paragraph::new(format!("You may not read {what}.")).dark_gray(),
            body,
        );
    } else {
        let rows = rows(model, view);
        let titles = header(view.tab);
        let mut widths: Vec<usize> = titles.iter().map(|t| t.len()).collect();
        for r in &rows {
            for (w, c) in widths.iter_mut().zip(&r.cells) {
                *w = (*w).max(c.chars().count());
            }
        }
        let last = widths.len() - 1;
        let constraints: Vec<Constraint> = widths
            .iter()
            .enumerate()
            .map(|(i, w)| {
                if i == last {
                    Constraint::Fill(1)
                } else {
                    Constraint::Length(*w as u16)
                }
            })
            .collect();
        let selected = view.selected[view.index()];
        let selected = (selected != usize::MAX).then(|| selected.min(rows.len().saturating_sub(1)));
        let table = Table::new(
            rows.into_iter()
                .map(|r| Row::new(r.cells.into_iter().map(Cell::from)).style(r.style)),
            constraints,
        )
        .header(Row::new(titles.iter().copied()).bold())
        .column_spacing(2)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        let mut state = TableState::default().with_selected(selected);
        f.render_stateful_widget(table, body, &mut state);
    }

    let mut help = vec![Span::raw(
        "q quit  ←→ tabs  ↑↓ select  Enter tasks of  h ended tasks",
    )];
    if let Some(focus) = &view.focus {
        let of = match focus {
            Focus::Node(n) => format!("node {n}"),
            Focus::Owner(kind, ns, name) => {
                format!("{} {ns}/{name}", kind.as_str().to_ascii_lowercase())
            }
        };
        help.push(Span::raw(format!("  Esc all tasks  — tasks of {of}")).bold());
    }
    f.render_widget(Paragraph::new(Line::from(help)), bottom);
}

/// The top line: the managers, nodes and tasks at a glance, or why it is
/// not current.
fn summary(model: &Model) -> Line<'static> {
    if let Some(problem) = &model.problem {
        return Line::from(format!("legion top — not current: {problem}")).red();
    }
    let mut parts = vec!["legion top".to_owned()];
    if let Some(ns) = &model.namespace {
        parts.push(format!("namespace {ns}"));
    }
    if let Some(c) = &model.cluster {
        let leader = c
            .leader_id
            .map_or("no leader".into(), |l| format!("leader {l}"));
        parts.push(format!("{leader}, {} managers", c.members.len()));
    }
    if !model.hidden.contains(&Kind::Node) {
        let nodes: Vec<NodeStatus> = model.of(Kind::Node).map(status).collect();
        let ready = nodes.iter().filter(|n| n.state == NodeState::Ready).count();
        parts.push(format!("{ready}/{} nodes ready", nodes.len()));
    }
    let tasks: Vec<TaskStatus> = model.of(Kind::Task).map(status).collect();
    let running = tasks
        .iter()
        .filter(|t| t.state == TaskState::Running)
        .count();
    let failed = tasks
        .iter()
        .filter(|t| matches!(t.state, TaskState::Failed | TaskState::Rejected))
        .count();
    parts.push(format!("{running} tasks running, {failed} failed"));
    Line::from(parts.join(" — "))
}

/// What the screen hears of.
enum Msg {
    Listed(Vec<Object>, BTreeSet<Kind>),
    Changed(bool, Object),
    Usage(pb::UsageResponse),
    Status(pb::StatusResponse),
    Problem(String),
    Key(KeyEvent),
    Redraw,
}

/// Runs until the person quits.
pub async fn run(channel: Channel, namespace: Option<String>) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let follower = tokio::spawn(follow(channel.clone(), namespace.clone(), tx.clone()));
    let poller = tokio::spawn(poll(channel, namespace.clone(), tx.clone()));
    // Reading keys blocks; the thread ends with the process.
    std::thread::spawn(move || {
        loop {
            let msg = match event::read() {
                Ok(Event::Key(k)) if k.kind == KeyEventKind::Press => Msg::Key(k),
                Ok(Event::Resize(..)) => Msg::Redraw,
                Ok(_) => continue,
                Err(_) => return,
            };
            if tx.send(msg).is_err() {
                return;
            }
        }
    });

    let mut model = Model::new(namespace);
    let mut view = View::default();
    let mut terminal = ratatui::init();
    let result = loop {
        if let Err(e) = terminal.draw(|f| render(f, &model, &view)) {
            break Err(e.into());
        }
        let Some(msg) = rx.recv().await else {
            break Ok(());
        };
        let mut next = Some(msg);
        while let Some(msg) = next {
            match msg {
                Msg::Listed(objects, hidden) => model.listed(objects, hidden),
                Msg::Changed(put, o) => model.changed(put, o, legion_api::clock::now()),
                Msg::Usage(u) => model.usage(u),
                Msg::Status(s) => model.cluster = Some(s),
                Msg::Problem(p) => model.problem = Some(p),
                Msg::Key(k) => view.key(&model, k),
                Msg::Redraw => {}
            }
            next = rx.try_recv().ok();
        }
        if view.quit {
            break Ok(());
        }
    };
    ratatui::restore();
    follower.abort();
    poller.abort();
    result
}

/// Lists, then follows the watch; again from the start when it breaks.
async fn follow(channel: Channel, namespace: Option<String>, tx: mpsc::UnboundedSender<Msg>) {
    let mut objects = ObjectsClient::new(channel);
    loop {
        let why = match follow_once(&mut objects, &namespace, &tx).await {
            Ok(()) => return,
            Err(why) => why,
        };
        if tx.send(Msg::Problem(why)).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn follow_once(
    objects: &mut ObjectsClient<Channel>,
    namespace: &Option<String>,
    tx: &mpsc::UnboundedSender<Msg>,
) -> Result<(), String> {
    let (listed, hidden, from) = list(objects, namespace).await?;
    if tx.send(Msg::Listed(listed, hidden)).is_err() {
        return Ok(());
    }
    let mut stream = objects
        .watch(WatchRequest {
            kind: String::new(),
            from_version: from,
        })
        .await
        .map_err(|s| s.message().to_owned())?
        .into_inner();
    loop {
        let ev = match stream.message().await {
            Ok(Some(ev)) => ev,
            Ok(None) => return Err("the watch ended".into()),
            Err(s) => return Err(s.message().to_owned()),
        };
        let put = ev.r#type() == watch_event::Type::Put;
        let Some(o) = ev.object else { continue };
        // Kinds it does not show are not even read.
        if !KINDS.iter().any(|k| k.as_str() == o.kind) {
            continue;
        }
        let o = Object::try_from(o).map_err(|e| e.to_string())?;
        if tx.send(Msg::Changed(put, o)).is_err() {
            return Ok(());
        }
    }
}

/// The objects shown, the kinds the caller may not list, and the version
/// to watch from: the oldest listing's.
async fn list(
    objects: &mut ObjectsClient<Channel>,
    namespace: &Option<String>,
) -> Result<(Vec<Object>, BTreeSet<Kind>, u64), String> {
    let mut listed = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut from = None::<u64>;
    for kind in KINDS {
        let namespace = if kind.is_cluster_scoped() {
            String::new()
        } else {
            namespace.clone().unwrap_or_default()
        };
        let req = ListRequest {
            kind: kind.to_string(),
            namespace,
            ..Default::default()
        };
        match objects.list(req).await {
            Ok(r) => {
                let r = r.into_inner();
                from = Some(from.map_or(r.version, |v| v.min(r.version)));
                for o in r.objects {
                    listed.push(Object::try_from(o).map_err(|e| e.to_string())?);
                }
            }
            Err(s) if s.code() == tonic::Code::PermissionDenied => {
                hidden.insert(kind);
            }
            Err(s) => return Err(s.message().to_owned()),
        }
    }
    let Some(from) = from else {
        return Err("you may read none of nodes, services, Jobs and tasks".into());
    };
    Ok((listed, hidden, from))
}

/// `--once`: the nodes, services and Jobs, and tasks as they are now,
/// printed as text, as `top -b` would.
pub async fn once(channel: Channel, namespace: Option<String>) -> anyhow::Result<()> {
    let mut objects = ObjectsClient::new(channel.clone());
    let (listed, hidden, _) = list(&mut objects, &namespace)
        .await
        .map_err(anyhow::Error::msg)?;
    let mut model = Model::new(namespace.clone());
    model.listed(listed, hidden);
    let mut cluster = ClusterClient::new(channel);
    model.cluster = cluster
        .status(StatusRequest {})
        .await
        .ok()
        .map(|s| s.into_inner());
    let req = pb::UsageRequest {
        namespace: namespace.unwrap_or_default(),
    };
    if let Ok(u) = cluster.usage(req).await {
        model.usage(u.into_inner());
    }
    let mut out = String::new();
    for (i, tab) in [Tab::Nodes, Tab::Workloads, Tab::Tasks]
        .into_iter()
        .enumerate()
    {
        let view = View {
            tab,
            ..View::default()
        };
        // The summary once, on top.
        let screen = text(&model, &view, 160);
        out.extend(screen.split_inclusive('\n').skip(usize::from(i > 0)));
        out.push('\n');
    }
    print!("{out}");
    Ok(())
}

/// The screen as text, `width` wide and as tall as its rows, without the
/// selection and the keys.
fn text(model: &Model, view: &View, width: u16) -> String {
    let height = rows(model, view).len() as u16 + 4;
    let mut t = ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, height))
        .expect("a test backend");
    let plain = View {
        selected: [usize::MAX; 4],
        focus: view.focus.clone(),
        ..*view
    };
    t.draw(|f| render(f, model, &plain)).expect("drawn");
    let buf = t.backend().buffer();
    // The tab bar and the keys are for the screen.
    (0..buf.area.height)
        .filter(|y| *y != 1 && *y != buf.area.height - 1)
        .map(|y| {
            let line: String = (0..buf.area.width).map(|x| buf[(x, y)].symbol()).collect();
            format!("{}\n", line.trim_end())
        })
        .collect()
}

/// Asks for usage and the managers' state every [`POLL`].
async fn poll(channel: Channel, namespace: Option<String>, tx: mpsc::UnboundedSender<Msg>) {
    let mut cluster = ClusterClient::new(channel);
    let mut tick = tokio::time::interval(POLL);
    loop {
        tick.tick().await;
        if let Ok(s) = cluster.status(StatusRequest {}).await
            && tx.send(Msg::Status(s.into_inner())).is_err()
        {
            return;
        }
        // A manager too old to answer, or a leader just elected: none yet.
        let req = pb::UsageRequest {
            namespace: namespace.clone().unwrap_or_default(),
        };
        if let Ok(u) = cluster.usage(req).await
            && tx.send(Msg::Usage(u.into_inner())).is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn object(
        kind: Kind,
        ns: &str,
        name: &str,
        version: u64,
        spec: serde_json::Value,
        status: serde_json::Value,
    ) -> Object {
        let mut o = Object::new(kind, ns, name, spec);
        o.status = status;
        o.metadata.version = version;
        o
    }

    fn node(name: &str, version: u64, state: &str, availability: &str) -> Object {
        object(
            Kind::Node,
            "",
            name,
            version,
            serde_json::json!({ "availability": availability }),
            serde_json::json!({ "state": state, "allocatable": { "cpu": "4", "memory": "8Gi" } }),
        )
    }

    fn task(name: &str, version: u64, node: &str, state: &str, ready: bool) -> Object {
        let mut t = object(
            Kind::Task,
            "shop",
            name,
            version,
            serde_json::json!({ "service": "web", "node": node }),
            serde_json::json!({ "state": state, "ready": ready, "ip": "10.200.0.2" }),
        );
        t.metadata.labels.insert(LABEL_SERVICE.into(), "web".into());
        t
    }

    fn service(version: u64, ready: u32, desired: u32) -> Object {
        object(
            Kind::Service,
            "shop",
            "web",
            version,
            serde_json::json!({ "pod": { "containers": [{ "name": "web", "image": "nginx:1" }] } }),
            serde_json::json!({ "observedGeneration": 1, "desired": desired, "running": ready, "ready": ready }),
        )
    }

    fn screen(model: &Model, view: &View) -> String {
        let mut t = Terminal::new(TestBackend::new(120, 12)).unwrap();
        t.draw(|f| render(f, model, view)).unwrap();
        let buf = t.backend().buffer();
        (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The line of the screen with `token`, its spaces collapsed.
    fn line(screen: &str, token: &str) -> String {
        let l = screen
            .lines()
            .find(|l| l.contains(token))
            .unwrap_or_else(|| panic!("no {token}: {screen}"));
        l.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn press(view: &mut View, model: &Model, code: KeyCode) {
        view.key(model, KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn cluster() -> Model {
        let mut m = Model::new(None);
        m.listed(
            vec![
                node("n1", 1, "ready", "active"),
                node("n2", 2, "ready", "active"),
                service(3, 1, 2),
                task("web-1-1", 4, "n1", "running", true),
                task("web-2-1", 5, "n2", "starting", false),
            ],
            BTreeSet::new(),
        );
        m.usage(pb::UsageResponse {
            nodes: vec![pb::NodeUsage {
                node: "n1".into(),
                tasks: vec![pb::TaskUsage {
                    namespace: "shop".into(),
                    task: "web-1-1".into(),
                    cpu_percent: 25.0,
                    memory_bytes: 64 << 20,
                }],
            }],
        });
        m
    }

    #[test]
    fn shows_nodes_workloads_and_tasks_with_their_use() {
        let m = cluster();
        let mut v = View::default();
        let s = screen(&m, &v);
        assert!(
            s.contains("2/2 nodes ready — 1 tasks running, 0 failed"),
            "{s}"
        );
        assert_eq!(line(&s, "n1 "), "n1 ready active worker 1 0.25/4 64Mi/8Gi");
        assert_eq!(line(&s, "n2 "), "n2 ready active worker 0 -/4 -/8Gi");

        press(&mut v, &m, KeyCode::Char('2'));
        let s = screen(&m, &v);
        assert_eq!(
            line(&s, "nginx"),
            "shop web replicated 1/2 25% 64Mi nginx:1"
        );

        press(&mut v, &m, KeyCode::Char('3'));
        let s = screen(&m, &v);
        assert_eq!(
            line(&s, "web-1-1"),
            "shop web-1-1 n1 running yes 0 25% 64Mi 10.200.0.2"
        );
        assert_eq!(line(&s, "web-2-1"), "shop web-2-1 n2 starting 0 10.200.0.2");
    }

    #[test]
    fn enter_shows_the_tasks_of_a_node_or_service() {
        let m = cluster();
        let mut v = View::default();
        press(&mut v, &m, KeyCode::Down);
        press(&mut v, &m, KeyCode::Enter);
        assert_eq!(v.tab, Tab::Tasks);
        let s = screen(&m, &v);
        assert!(s.contains("web-2-1") && !s.contains("web-1-1"), "{s}");
        assert!(s.contains("tasks of node n2"), "{s}");
        // Esc shows all tasks again, then quits.
        press(&mut v, &m, KeyCode::Esc);
        assert!(screen(&m, &v).contains("web-1-1"));
        assert!(!v.quit);
        press(&mut v, &m, KeyCode::Esc);
        assert!(v.quit);

        let mut v = View::default();
        press(&mut v, &m, KeyCode::Char('2'));
        press(&mut v, &m, KeyCode::Enter);
        let s = screen(&m, &v);
        assert!(s.contains("web-1-1") && s.contains("web-2-1"), "{s}");
        assert!(s.contains("tasks of service shop/web"), "{s}");
    }

    #[test]
    fn ended_tasks_only_on_asking() {
        let mut m = cluster();
        m.changed(true, task("web-1-0", 9, "n1", "failed", false), 0);
        let mut v = View::default();
        press(&mut v, &m, KeyCode::Char('3'));
        assert!(!screen(&m, &v).contains("web-1-0"));
        press(&mut v, &m, KeyCode::Char('h'));
        let s = screen(&m, &v);
        let (live, ended) = (s.find("web-1-1").unwrap(), s.find("web-1-0").unwrap());
        assert!(live < ended, "ended last: {s}");
    }

    #[test]
    fn changes_are_logged_and_old_ones_ignored() {
        let mut m = cluster();
        let at = legion_api::clock::parse("2026-10-30T09:30:05Z").unwrap();
        m.changed(true, node("n2", 10, "suspect", "active"), at);
        m.changed(true, task("web-2-1", 11, "n2", "running", false), at);
        let mut failed = task("web-2-1", 12, "n2", "failed", false);
        failed.status["message"] = "container web exited with 1".into();
        m.changed(true, failed, at);
        m.changed(true, node("n1", 13, "ready", "drain"), at);
        // Replayed from before the listing: nothing changes.
        m.changed(true, task("web-1-1", 2, "n1", "failed", false), at);
        m.changed(false, service(1, 0, 0), at);
        let log: Vec<&str> = m.log.iter().rev().map(|(_, s)| s.as_str()).collect();
        assert_eq!(
            log,
            [
                "node n2: ready → suspect",
                "task shop/web-2-1 on n2: running",
                "task shop/web-2-1 on n2: failed: container web exited with 1",
                "node n1: active → drain",
            ]
        );
        assert_eq!(
            status::<TaskStatus>(&m.objects[&(Kind::Task, "shop".into(), "web-1-1".into())]).state,
            TaskState::Running
        );
        assert!(
            m.objects
                .contains_key(&(Kind::Service, "shop".into(), "web".into()))
        );
        let mut v = View::default();
        press(&mut v, &m, KeyCode::Char('4'));
        let s = screen(&m, &v);
        assert_eq!(line(&s, "drain"), "09:30:05 node n1: active → drain");
    }

    #[test]
    fn what_the_caller_may_not_read_says_so() {
        let mut m = Model::new(Some("shop".into()));
        m.listed(
            vec![service(3, 1, 1), task("web-1-1", 4, "n1", "running", true)],
            BTreeSet::from([Kind::Node]),
        );
        // Another namespace's objects are not shown.
        let mut other = service(5, 1, 1);
        other.metadata.namespace = "b".into();
        m.changed(true, other, 0);
        let v = View::default();
        let s = screen(&m, &v);
        assert!(s.contains("You may not read nodes."), "{s}");
        assert!(
            s.contains("namespace shop") && !s.contains("nodes ready"),
            "{s}"
        );
        assert_eq!(m.of(Kind::Service).count(), 1);
    }

    #[test]
    fn a_problem_shows_until_listed_again() {
        let mut m = cluster();
        m.problem = Some("connection refused".into());
        let v = View::default();
        assert!(screen(&m, &v).contains("not current: connection refused"));
        m.listed(Vec::new(), BTreeSet::new());
        assert!(!screen(&m, &v).contains("not current"));
    }

    #[test]
    fn sizes() {
        assert_eq!(bytes(512), "512");
        assert_eq!(bytes(64 << 20), "64Mi");
        assert_eq!(bytes(3 << 29), "1.5Gi");
        assert_eq!(trim(4.0), "4");
    }
}
