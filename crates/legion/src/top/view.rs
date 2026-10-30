//! What the person looks at and what their keys do: the tabs, the rows of
//! each, and the requests keys make.

use legion_api::data::DataSpec;
use legion_api::job::{CronJobStatus, JobState, JobStatus};
use legion_api::node::{NodeSpec, NodeState, NodeStatus};
use legion_api::route::RouteSpec;
use legion_api::service::ServiceStatus;
use legion_api::task::{TaskSpec, TaskState, TaskStatus};
use legion_api::volume::{VolumeSpec, VolumeStatus};
use legion_api::{Kind, Object};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use super::draw::{BAD, OK, WARN, bytes, dimmed, fg, meter, trim};
use super::logs::Logs;
use super::model::{Key, Model, containers, key, kind_word, owner, spec, status, word};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    Nodes,
    Workloads,
    Tasks,
    Storage,
    Network,
    Config,
    Log,
}

pub(super) const TABS: [Tab; 7] = [
    Tab::Nodes,
    Tab::Workloads,
    Tab::Tasks,
    Tab::Storage,
    Tab::Network,
    Tab::Config,
    Tab::Log,
];

impl Tab {
    pub(super) fn title(self) -> &'static str {
        match self {
            Tab::Nodes => "Nodes",
            Tab::Workloads => "Workloads",
            Tab::Tasks => "Tasks",
            Tab::Storage => "Storage",
            Tab::Network => "Network",
            Tab::Config => "Config",
            Tab::Log => "Changes",
        }
    }

    /// The kinds it lists.
    pub(super) fn kinds(self) -> &'static [Kind] {
        match self {
            Tab::Nodes => &[Kind::Node],
            Tab::Workloads => &[Kind::Service, Kind::Job, Kind::CronJob],
            Tab::Tasks => &[Kind::Task],
            Tab::Storage => &[Kind::Volume],
            Tab::Network => &[Kind::Route, Kind::NetworkPolicy],
            Tab::Config => &[
                Kind::Namespace,
                Kind::Secret,
                Kind::Config,
                Kind::RoleBinding,
            ],
            Tab::Log => &[],
        }
    }

    pub(super) fn header(self) -> &'static [&'static str] {
        match self {
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
            Tab::Storage => &[
                "NAMESPACE",
                "NAME",
                "DRIVER",
                "SIZE",
                "NODE",
                "USED BY",
                "MIGRATION",
            ],
            Tab::Network => &["NAMESPACE", "NAME", "KIND", "WHAT"],
            Tab::Config => &["NAMESPACE", "NAME", "KIND", "WHAT"],
            Tab::Log => &["TIME (UTC)", "CHANGE"],
        }
    }
}

/// Which tasks the task list shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Focus {
    Node(String),
    /// A service or Job: its kind, namespace and name.
    Owner(Kind, String, String),
}

/// What the person asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// `drain`, `pause` or `active`.
    Availability {
        node: String,
        to: &'static str,
    },
    /// Sets a label, or removes it (`None`).
    Label {
        node: String,
        key: String,
        value: Option<String>,
    },
    Scale {
        namespace: String,
        service: String,
        replicas: u32,
    },
    Rollback {
        namespace: String,
        service: String,
    },
    Suspend {
        namespace: String,
        cron_job: String,
        suspend: bool,
    },
    Move {
        namespace: String,
        volume: String,
        node: String,
    },
    Delete {
        kind: Kind,
        namespace: String,
        name: String,
    },
    /// Of a task, or a service's or Job's (the CLI picks a task).
    Logs {
        namespace: String,
        name: String,
        container: Option<String>,
    },
    Exec {
        namespace: String,
        task: String,
        container: Option<String>,
    },
}

/// What typed text is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Then {
    Scale {
        namespace: String,
        service: String,
    },
    Move {
        namespace: String,
        volume: String,
    },
    Label {
        node: String,
    },
    /// The name must be typed again.
    Delete {
        kind: Kind,
        namespace: String,
        name: String,
    },
    /// Filters as it is typed; Esc goes back to this.
    Filter {
        before: String,
    },
}

/// What a chosen container is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pick {
    Logs { namespace: String, name: String },
    Exec { namespace: String, task: String },
}

/// A question on the bottom line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Prompt {
    /// Asked first: `y` does it.
    Confirm(Request, String),
    Input {
        ask: String,
        input: String,
        then: Then,
    },
    Choose {
        ask: String,
        options: Vec<String>,
        pick: Pick,
    },
}

/// What the person looks at.
pub struct View {
    pub(super) tab: Tab,
    /// The selected row of each tab.
    pub(super) selected: [usize; 7],
    /// Each tab's filter: rows holding the text, in any case.
    pub(super) filter: [String; 7],
    pub(super) focus: Option<Focus>,
    /// Tasks that ended are listed too.
    pub(super) history: bool,
    pub(super) prompt: Option<Prompt>,
    /// What came of the last request, or why it was not made.
    pub(super) notice: Option<String>,
    /// The detail panel: shown or not, or as the width allows.
    pub(super) detail: Option<bool>,
    /// An object's YAML over everything, scrolled this far.
    pub(super) yaml: Option<(Key, u16)>,
    pub(super) help: bool,
    /// Logs followed under the table.
    pub(super) logs: Option<Logs>,
    pub(super) quit: bool,
}

impl Default for View {
    fn default() -> Self {
        View {
            tab: Tab::Nodes,
            selected: [0; 7],
            filter: Default::default(),
            focus: None,
            history: false,
            prompt: None,
            notice: None,
            detail: None,
            yaml: None,
            help: false,
            logs: None,
            quit: false,
        }
    }
}

impl View {
    /// Starts showing the logs a request asks for, in place of any shown.
    pub(super) fn show_logs(&mut self, request: &Request) {
        if let Request::Logs {
            namespace,
            name,
            container,
        } = request
        {
            let mut of = format!("{namespace}/{name}");
            if let Some(c) = container {
                of = format!("{of}/{c}");
            }
            self.logs = Some(Logs::new(of));
        }
    }

    pub(super) fn index(&self) -> usize {
        TABS.iter().position(|t| *t == self.tab).expect("a tab")
    }

    /// The object of the selected row.
    pub(super) fn selected_key(&self, model: &Model) -> Option<Key> {
        rows(model, self)
            .into_iter()
            .nth(self.selected[self.index()])
            .and_then(|r| r.key)
    }

    /// What a key does, and the request it makes, if any.
    pub fn key(&mut self, model: &Model, k: KeyEvent) -> Option<Request> {
        if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
            self.quit = true;
            return None;
        }
        if let Some(prompt) = self.prompt.take() {
            return self.answer(prompt, k);
        }
        if self.help {
            self.help = false;
            return None;
        }
        if let Some((key, scroll)) = self.yaml.take() {
            let scroll = match k.code {
                KeyCode::Down | KeyCode::Char('j') => scroll.saturating_add(1),
                KeyCode::Up | KeyCode::Char('k') => scroll.saturating_sub(1),
                KeyCode::PageDown | KeyCode::Char(' ') => scroll.saturating_add(20),
                KeyCode::PageUp => scroll.saturating_sub(20),
                KeyCode::Home | KeyCode::Char('g') => 0,
                _ => return None,
            };
            self.yaml = Some((key, scroll));
            return None;
        }
        self.notice = None;
        if let Some(logs) = &mut self.logs {
            let page = 20;
            match k.code {
                KeyCode::Esc => {
                    self.logs = None;
                    return None;
                }
                KeyCode::PageUp => {
                    logs.scroll(page);
                    return None;
                }
                KeyCode::PageDown => {
                    logs.scroll(-page);
                    return None;
                }
                KeyCode::Char('z') => {
                    logs.zoom = !logs.zoom;
                    return None;
                }
                _ => {}
            }
        }
        let selected = self.selected_key(model);
        if let KeyCode::Char(c) = k.code
            && let Some(key) = &selected
            && let Some(request) = self.act(c, model, key)
        {
            return Some(request);
        }
        if self.prompt.is_some() || self.notice.is_some() {
            return None;
        }
        let count = rows(model, self).len();
        let i = self.index();
        let sel = &mut self.selected[i];
        let last = count.saturating_sub(1);
        match k.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Esc => {
                if !self.filter[i].is_empty() {
                    self.filter[i].clear();
                } else {
                    self.focus = None;
                }
            }
            KeyCode::Char(c @ '1'..='7') => self.tab = TABS[c as usize - '1' as usize],
            KeyCode::Tab | KeyCode::Right => self.tab = TABS[(i + 1) % TABS.len()],
            KeyCode::BackTab | KeyCode::Left => self.tab = TABS[(i + TABS.len() - 1) % TABS.len()],
            KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(last),
            KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1).min(last),
            KeyCode::PageDown => *sel = (*sel + 20).min(last),
            KeyCode::PageUp => *sel = sel.saturating_sub(20),
            KeyCode::Home | KeyCode::Char('g') => *sel = 0,
            KeyCode::End | KeyCode::Char('G') => *sel = last,
            KeyCode::Char('h') => self.history = !self.history,
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('i') => self.detail = Some(!self.detail.unwrap_or(true)),
            KeyCode::Char('y') => self.yaml = selected.map(|k| (k, 0)),
            KeyCode::Char('/') => {
                let before = self.filter[i].clone();
                self.prompt = Some(Prompt::Input {
                    ask: "Filter".into(),
                    input: before.clone(),
                    then: Then::Filter { before },
                });
            }
            KeyCode::Enter => match selected {
                Some((Kind::Node, _, name)) => self.show_tasks(Focus::Node(name)),
                Some((kind @ (Kind::Service | Kind::Job), ns, name)) => {
                    self.show_tasks(Focus::Owner(kind, ns, name))
                }
                Some(k) => self.yaml = Some((k, 0)),
                None => {}
            },
            _ => {}
        }
        None
    }

    fn show_tasks(&mut self, focus: Focus) {
        self.focus = Some(focus);
        let tasks = TABS.iter().position(|t| *t == Tab::Tasks).expect("a tab");
        self.selected[tasks] = 0;
        self.tab = Tab::Tasks;
    }

    /// A letter acting on the selected object: a request, a question
    /// first, or why not.
    fn act(&mut self, c: char, model: &Model, k: &Key) -> Option<Request> {
        let (kind, namespace, name) = k.clone();
        let o = model.get(k)?;
        let (ns, n) = (namespace.clone(), name.clone());
        let request = match (c, kind) {
            ('d', Kind::Node) => {
                let ask = format!("Drain node {name}? Its tasks move to other nodes.");
                self.confirm(
                    Request::Availability {
                        node: name,
                        to: "drain",
                    },
                    ask,
                );
                return None;
            }
            ('p', Kind::Node) => Request::Availability {
                node: name,
                to: "pause",
            },
            ('a', Kind::Node) => Request::Availability {
                node: name,
                to: "active",
            },
            ('L', Kind::Node) => {
                self.input(
                    format!("Label node {name} (key=value, or key- to remove)"),
                    Then::Label { node: name },
                );
                return None;
            }
            ('s', Kind::Service) => {
                let global = o.spec["mode"].as_str() == Some("global");
                if global || !o.spec["autoscale"].is_null() {
                    self.notice = Some(format!(
                        "{ns}/{n} is global or autoscaled: its spec decides its tasks."
                    ));
                } else {
                    self.input(
                        format!("Replicas for {ns}/{n}"),
                        Then::Scale {
                            namespace,
                            service: name,
                        },
                    );
                }
                return None;
            }
            ('r', Kind::Service) => {
                let ask =
                    format!("Roll service {ns}/{n} back to the spec before its latest update?");
                self.confirm(
                    Request::Rollback {
                        namespace,
                        service: name,
                    },
                    ask,
                );
                return None;
            }
            ('u', Kind::CronJob) => Request::Suspend {
                namespace,
                cron_job: name,
                suspend: !o.spec["suspend"].as_bool().unwrap_or(false),
            },
            ('m', Kind::Volume) => {
                self.input(
                    format!("Move volume {ns}/{n} to node"),
                    Then::Move {
                        namespace,
                        volume: name,
                    },
                );
                return None;
            }
            ('l', Kind::Task | Kind::Service | Kind::Job) => {
                let pick = Pick::Logs { namespace, name };
                return self.container(o, pick, "Logs of which container?");
            }
            ('x', Kind::Task) => {
                let pick = Pick::Exec {
                    namespace,
                    task: name,
                };
                return self.container(o, pick, "A shell in which container?");
            }
            ('D', Kind::Task) => {
                let ask = format!("Delete task {ns}/{n}? Its service starts another.");
                self.confirm(
                    Request::Delete {
                        kind,
                        namespace,
                        name,
                    },
                    ask,
                );
                return None;
            }
            ('D', _) => {
                let what = super::model::name(o);
                self.input(
                    format!("Type {n} to delete {what}"),
                    Then::Delete {
                        kind,
                        namespace,
                        name,
                    },
                );
                return None;
            }
            _ => return None,
        };
        Some(request)
    }

    fn confirm(&mut self, request: Request, ask: String) {
        self.prompt = Some(Prompt::Confirm(request, format!("{ask} y/n")));
    }

    fn input(&mut self, ask: String, then: Then) {
        self.prompt = Some(Prompt::Input {
            ask,
            input: String::new(),
            then,
        });
    }

    /// Logs or a shell in the one container, or asks which.
    fn container(&mut self, o: &Object, pick: Pick, ask: &str) -> Option<Request> {
        let options = containers(o);
        if options.len() > 1 {
            self.prompt = Some(Prompt::Choose {
                ask: ask.into(),
                options,
                pick,
            });
            return None;
        }
        Some(picked(pick, None))
    }

    fn answer(&mut self, prompt: Prompt, k: KeyEvent) -> Option<Request> {
        match prompt {
            Prompt::Confirm(request, _) if k.code == KeyCode::Char('y') => Some(request),
            Prompt::Confirm(..) => {
                self.notice = Some("Cancelled.".into());
                None
            }
            Prompt::Choose { options, pick, .. } if k.code != KeyCode::Esc => {
                let chosen = match k.code {
                    KeyCode::Char(c) => c
                        .to_digit(10)
                        .and_then(|d| options.get((d as usize).checked_sub(1)?)),
                    _ => None,
                };
                match chosen {
                    Some(c) => Some(picked(pick, Some(c.clone()))),
                    None => {
                        self.notice = Some("Cancelled.".into());
                        None
                    }
                }
            }
            Prompt::Choose { .. } => {
                self.notice = Some("Cancelled.".into());
                None
            }
            Prompt::Input {
                ask,
                mut input,
                then,
            } => {
                let i = self.index();
                match k.code {
                    KeyCode::Char(c) if input.chars().count() < 120 => {
                        let digits_only = matches!(then, Then::Scale { .. });
                        if !digits_only || c.is_ascii_digit() {
                            input.push(c);
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                    }
                    KeyCode::Esc => {
                        if let Then::Filter { before } = then {
                            self.filter[i] = before;
                        } else {
                            self.notice = Some("Cancelled.".into());
                        }
                        return None;
                    }
                    KeyCode::Enter => return self.entered(input, then),
                    _ => {}
                }
                if matches!(then, Then::Filter { .. }) {
                    self.filter[i] = input.clone();
                    self.selected[i] = 0;
                }
                self.prompt = Some(Prompt::Input { ask, input, then });
                None
            }
        }
    }

    fn entered(&mut self, input: String, then: Then) -> Option<Request> {
        let input = input.trim().to_owned();
        let refuse = |view: &mut View, why: &str| {
            view.notice = Some(why.to_owned());
            None
        };
        match then {
            Then::Filter { .. } => {
                let i = self.index();
                self.filter[i] = input;
                None
            }
            Then::Scale { namespace, service } => match input.parse() {
                Ok(replicas) => Some(Request::Scale {
                    namespace,
                    service,
                    replicas,
                }),
                Err(_) => refuse(self, "Cancelled: no number given."),
            },
            Then::Move { namespace, volume } if !input.is_empty() => Some(Request::Move {
                namespace,
                volume,
                node: input,
            }),
            Then::Move { .. } => refuse(self, "Cancelled: no node given."),
            Then::Label { node } => match crate::cli::label_change(&input) {
                Ok((key, value)) => Some(Request::Label { node, key, value }),
                Err(e) => refuse(self, &format!("Cancelled: {e}.")),
            },
            Then::Delete {
                kind,
                namespace,
                name,
            } if input == name => Some(Request::Delete {
                kind,
                namespace,
                name,
            }),
            Then::Delete { .. } => refuse(self, "Not deleted: the name did not match."),
        }
    }
}

fn picked(pick: Pick, container: Option<String>) -> Request {
    match pick {
        Pick::Logs { namespace, name } => Request::Logs {
            namespace,
            name,
            container,
        },
        Pick::Exec { namespace, task } => Request::Exec {
            namespace,
            task,
            container,
        },
    }
}

/// A row of a table, and the object it stands for.
pub(super) struct TableRow {
    pub(super) cells: Vec<Line<'static>>,
    pub(super) style: Style,
    pub(super) key: Option<Key>,
}

impl TableRow {
    fn new(cells: Vec<Line<'static>>, key: Option<Key>) -> Self {
        TableRow {
            cells,
            style: Style::default(),
            key,
        }
    }

    fn style(mut self, style: Style) -> Self {
        self.style = style;
        self
    }

    pub(super) fn text(&self) -> String {
        self.cells
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The rows of the view's tab, filtered.
pub(super) fn rows(model: &Model, view: &View) -> Vec<TableRow> {
    let all = match view.tab {
        Tab::Nodes => node_rows(model),
        Tab::Workloads => workload_rows(model),
        Tab::Tasks => task_rows(model, view),
        Tab::Storage => volume_rows(model),
        Tab::Network => network_rows(model),
        Tab::Config => config_rows(model),
        Tab::Log => model
            .log
            .iter()
            .map(|(at, what)| {
                TableRow::new(vec![dimmed(time_of_day(*at)), what.clone().into()], None)
            })
            .collect(),
    };
    let filter = view.filter[view.index()].to_lowercase();
    if filter.is_empty() {
        return all;
    }
    all.into_iter()
        .filter(|r| r.text().to_lowercase().contains(&filter))
        .collect()
}

fn text(s: impl Into<String>) -> Line<'static> {
    Line::from(s.into())
}

fn colored(s: impl Into<String>, style: Style) -> Line<'static> {
    Line::from(Span::styled(s.into(), style))
}

/// Use against capacity: a small bar, then the numbers.
fn gauge(used: Option<f64>, capacity: f64, label: String) -> Line<'static> {
    let mut spans = match used {
        Some(u) if capacity > 0.0 => meter(u / capacity, 8),
        _ => vec![Span::styled("········", dim())],
    };
    spans.push(Span::raw(format!(" {label}")));
    Line::from(spans)
}

fn dim() -> Style {
    Style::default().add_modifier(Modifier::DIM)
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
            let used = model.sum_usage(on);
            let cores = st.allocatable.cpu.millis() as f64 / 1000.0;
            let cpu = gauge(
                used.map(|(c, _)| c / 100.0),
                cores,
                format!(
                    "{}/{}",
                    used.map_or("-".into(), |(c, _)| format!("{:.2}", c / 100.0)),
                    trim(cores)
                ),
            );
            let memory = gauge(
                used.map(|(_, m)| m as f64),
                st.allocatable.memory.0 as f64,
                format!(
                    "{}/{}",
                    used.map_or("-".into(), |(_, m)| bytes(m)),
                    bytes(st.allocatable.memory.0)
                ),
            );
            let labels: Vec<String> = n
                .metadata
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            let state = match st.state {
                NodeState::Ready => fg(OK),
                NodeState::Suspect => fg(WARN),
                NodeState::Down | NodeState::Unknown => fg(BAD),
            };
            let avail = match word(&spec.availability).as_str() {
                "active" => Style::default(),
                _ => fg(WARN),
            };
            TableRow::new(
                vec![
                    text(name),
                    colored(format!("● {}", word(&st.state)), state),
                    colored(word(&spec.availability), avail),
                    text(word(&spec.role)),
                    text(running.to_string()),
                    cpu,
                    memory,
                    dimmed(labels.join(",")),
                ],
                Some(key(n)),
            )
        })
        .collect()
}

fn images(pod: &serde_json::Value) -> String {
    pod["containers"]
        .as_array()
        .map(|cs| {
            cs.iter()
                .filter_map(|c| c["image"].as_str())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn usage_cells(used: Option<(f64, u64)>) -> [Line<'static>; 2] {
    [
        text(used.map_or(String::new(), |(c, _)| format!("{c:.0}%"))),
        text(used.map_or(String::new(), |(_, m)| bytes(m))),
    ]
}

fn workload_rows(model: &Model) -> Vec<TableRow> {
    let owned_by = |t: &Object, kind: Kind, o: &Object| {
        t.metadata.namespace == o.metadata.namespace && owner(t) == Some((kind, &o.metadata.name))
    };
    let services = model.of(Kind::Service).map(|s| {
        let st: ServiceStatus = status(s);
        let [cpu, memory] = usage_cells(model.sum_usage(|t| owned_by(t, Kind::Service, s)));
        let update = st
            .update
            .as_ref()
            .map(|u| word(&u.state))
            .unwrap_or_default();
        let short = st.ready < st.desired;
        let tasks = colored(
            format!("{}/{}", st.ready, st.desired),
            if short { fg(WARN) } else { fg(OK) },
        );
        let state = match update.as_str() {
            "rollingback" | "rolledback" | "paused" | "rollbackpaused" => colored(update, fg(WARN)),
            _ => text(update),
        };
        TableRow::new(
            vec![
                dimmed(s.metadata.namespace.clone()),
                text(s.metadata.name.clone()),
                text(s.spec["mode"].as_str().unwrap_or("replicated").to_owned()),
                tasks,
                state,
                cpu,
                memory,
                dimmed(images(&s.spec["pod"])),
            ],
            Some(key(s)),
        )
    });
    let jobs = model.of(Kind::Job).map(|j| {
        let st: JobStatus = status(j);
        let [cpu, memory] = usage_cells(model.sum_usage(|t| owned_by(t, Kind::Job, j)));
        let done = match j.spec["completions"].as_u64() {
            Some(n) => format!("{}/{n}", st.succeeded),
            None => st.succeeded.to_string(),
        };
        let state = match st.state {
            JobState::Failed => fg(BAD),
            JobState::Complete => fg(OK),
            _ => Style::default(),
        };
        TableRow::new(
            vec![
                dimmed(j.metadata.namespace.clone()),
                text(j.metadata.name.clone()),
                text("job"),
                text(done),
                colored(word(&st.state), state),
                cpu,
                memory,
                dimmed(images(&j.spec["pod"])),
            ],
            Some(key(j)),
        )
    });
    let cron_jobs = model.of(Kind::CronJob).map(|c| {
        let st: CronJobStatus = status(c);
        let state = if c.spec["suspend"].as_bool().unwrap_or(false) {
            colored("suspended", fg(WARN))
        } else {
            text(format!(
                "every {}",
                c.spec["schedule"].as_str().unwrap_or("?")
            ))
        };
        TableRow::new(
            vec![
                dimmed(c.metadata.namespace.clone()),
                text(c.metadata.name.clone()),
                text("cron job"),
                dimmed(st.last_job),
                state,
                text(""),
                text(""),
                dimmed(images(&c.spec["job"]["pod"])),
            ],
            Some(key(c)),
        )
    });
    services.chain(jobs).chain(cron_jobs).collect()
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
            let [cpu, memory] = usage_cells(model.task_usage(t));
            let state = match st.state {
                TaskState::Failed | TaskState::Rejected => fg(BAD),
                _ if ended => dim(),
                TaskState::Running if st.ready => fg(OK),
                _ => fg(WARN),
            };
            let row = TableRow::new(
                vec![
                    dimmed(t.metadata.namespace.clone()),
                    text(t.metadata.name.clone()),
                    text(t.spec["node"].as_str().unwrap_or_default().to_owned()),
                    colored(word(&st.state), state),
                    colored(if st.ready { "✓" } else { "" }, fg(OK)),
                    text(st.restarts.to_string()),
                    cpu,
                    memory,
                    dimmed(st.ip.unwrap_or_default()),
                    text(st.message),
                ],
                Some(key(t)),
            );
            Some((ended, if ended { row.style(dim()) } else { row }))
        })
        .collect();
    // Those still there first.
    rows.sort_by_key(|(ended, _)| *ended);
    rows.into_iter().map(|(_, r)| r).collect()
}

fn volume_rows(model: &Model) -> Vec<TableRow> {
    // What running tasks mount, by namespace and Volume.
    let mut users: std::collections::BTreeMap<(String, String), Vec<String>> = Default::default();
    for t in model.of(Kind::Task) {
        if status::<TaskStatus>(t).state.is_terminal() {
            continue;
        }
        let Ok(task) = serde_json::from_value::<TaskSpec>(t.spec.clone()) else {
            continue;
        };
        for v in task.volumes() {
            users
                .entry((t.metadata.namespace.clone(), v))
                .or_default()
                .push(t.metadata.name.clone());
        }
    }
    model
        .of(Kind::Volume)
        .map(|v| {
            let (sp, st): (VolumeSpec, VolumeStatus) = (spec(v), status(v));
            let node = match (st.node.as_str(), sp.node.as_deref()) {
                ("", Some(asked)) => format!("({asked})"),
                (node, _) => node.to_owned(),
            };
            let migration = match &st.migration {
                Some(m) => colored(format!("{} → {}", word(&m.phase), m.to), fg(WARN)),
                None => text(""),
            };
            let used_by = users
                .get(&(v.metadata.namespace.clone(), v.metadata.name.clone()))
                .map(|u| u.join(","))
                .unwrap_or_default();
            TableRow::new(
                vec![
                    dimmed(v.metadata.namespace.clone()),
                    text(v.metadata.name.clone()),
                    text(word(&sp.driver)),
                    text(sp.size.map(|s| bytes(s.0)).unwrap_or_default()),
                    text(node),
                    text(used_by),
                    migration,
                ],
                Some(key(v)),
            )
        })
        .collect()
}

fn network_rows(model: &Model) -> Vec<TableRow> {
    let routes = model.of(Kind::Route).map(|r| {
        let sp: RouteSpec = serde_json::from_value(r.spec.clone()).unwrap_or(RouteSpec {
            host: String::new(),
            path: "/".into(),
            service: String::new(),
            port: None,
            tls: None,
        });
        let port = sp.port.map(|p| format!(":{p}")).unwrap_or_default();
        let tls = match &sp.tls {
            Some(t) => format!("  https, {}", t.secret),
            None => String::new(),
        };
        TableRow::new(
            vec![
                dimmed(r.metadata.namespace.clone()),
                text(r.metadata.name.clone()),
                text("route"),
                text(format!(
                    "{}{} → {}{port}{tls}",
                    sp.host, sp.path, sp.service
                )),
            ],
            Some(key(r)),
        )
    });
    let policies = model.of(Kind::NetworkPolicy).map(|p| {
        let services = match p.spec["services"].as_object() {
            Some(m) if !m.is_empty() => m
                .iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(","),
            _ => "every service".into(),
        };
        let rules =
            |n: usize, what: &str| format!("{n} {what} rule{}", if n == 1 { "" } else { "s" });
        let ingress = rules(p.spec["ingress"].as_array().map_or(0, Vec::len), "ingress");
        let egress = match p.spec["egress"].as_array() {
            None => "egress open".to_owned(),
            Some(r) => rules(r.len(), "egress"),
        };
        TableRow::new(
            vec![
                dimmed(p.metadata.namespace.clone()),
                text(p.metadata.name.clone()),
                text("network policy"),
                text(format!("{services}: {ingress}, {egress}")),
            ],
            Some(key(p)),
        )
    });
    routes.chain(policies).collect()
}

fn config_rows(model: &Model) -> Vec<TableRow> {
    let namespaces = model.of(Kind::Namespace).map(|n| {
        let quota = &n.spec["quota"];
        let limits: Vec<String> = ["cpu", "memory", "tasks", "volumes", "storage"]
            .iter()
            .filter_map(|k| {
                let v = &quota[*k];
                (!v.is_null())
                    .then(|| format!("{k} {}", v.as_str().map_or(v.to_string(), str::to_owned)))
            })
            .collect();
        let isolation = n.spec["isolation"].as_str().unwrap_or("none").to_owned();
        let quota = if limits.is_empty() {
            "no quota".into()
        } else {
            format!("quota: {}", limits.join(", "))
        };
        TableRow::new(
            vec![
                dimmed(""),
                text(n.metadata.name.clone()),
                text("namespace"),
                text(format!("isolation {isolation}; {quota}")),
            ],
            Some(key(n)),
        )
    });
    let data = model
        .of(Kind::Secret)
        .chain(model.of(Kind::Config))
        .map(|d| {
            let what = if d.kind == Kind::Secret {
                match d.spec["bytes"].as_u64() {
                    Some(n) => format!("{} (hidden)", bytes(n)),
                    None => "(hidden)".into(),
                }
            } else {
                let size = serde_json::from_value::<DataSpec>(d.spec.clone())
                    .ok()
                    .and_then(|s| s.bytes().ok())
                    .map_or(0, |b| b.len() as u64);
                bytes(size)
            };
            TableRow::new(
                vec![
                    dimmed(d.metadata.namespace.clone()),
                    text(d.metadata.name.clone()),
                    text(kind_word(d.kind)),
                    text(what),
                ],
                Some(key(d)),
            )
        });
    let bindings = model.of(Kind::RoleBinding).map(|b| {
        TableRow::new(
            vec![
                dimmed(b.metadata.namespace.clone()),
                text(b.metadata.name.clone()),
                text("role binding"),
                text(format!(
                    "{} is {}",
                    b.spec["user"].as_str().unwrap_or_default(),
                    b.spec["role"].as_str().unwrap_or_default()
                )),
            ],
            Some(key(b)),
        )
    });
    namespaces.chain(data).chain(bindings).collect()
}

/// `09:30:05`, of the day in UTC.
pub(super) fn time_of_day(unix: i64) -> String {
    legion_api::clock::format(unix)[11..19].to_owned()
}
