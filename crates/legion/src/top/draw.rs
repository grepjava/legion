//! Drawing `legion top`: a header of the cluster at a glance with graphs
//! of what its tasks use, the tabs, the selected tab's table beside the
//! selected object's details, logs followed under them, and the bottom line of keys, questions and
//! answers; over them, an object's YAML or the keys' help. The colours
//! and bars follow jmon's.

use std::sync::OnceLock;

use legion_api::Kind;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::{TaskState, TaskStatus};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Cell, Clear, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Sparkline, Table, TableState, Tabs, Wrap,
};

use super::logs::Logs;
use super::model::{Model, kind_word, status};
use super::view::{Focus, Prompt, TABS, Tab, View, rows};

pub(super) const ACCENT: Color = Color::Cyan;
pub(super) const OK: Color = Color::Green;
pub(super) const WARN: Color = Color::Yellow;
pub(super) const BAD: Color = Color::Red;

/// No colours (https://no-color.org).
static MONO: OnceLock<bool> = OnceLock::new();

pub(super) fn set_mono(mono: bool) {
    let _ = MONO.set(mono);
}

fn mono() -> bool {
    MONO.get().copied().unwrap_or(false)
}

/// Text in `color`, unless colours are off.
pub(super) fn fg(color: Color) -> Style {
    if mono() {
        Style::default()
    } else {
        Style::default().fg(color)
    }
}

fn dim() -> Style {
    Style::default().add_modifier(Modifier::DIM)
}

pub(super) fn dimmed(s: impl Into<String>) -> Line<'static> {
    Line::from(Span::styled(s.into(), dim()))
}

/// Green, yellow from 70 %, red from 90 %, as in jmon.
fn busy(frac: f64) -> Color {
    if frac >= 0.9 {
        BAD
    } else if frac >= 0.7 {
        WARN
    } else {
        OK
    }
}

/// A bar `width` cells wide, filled to `frac` in its busy colour.
pub(super) fn meter(frac: f64, width: usize) -> Vec<Span<'static>> {
    let filled = ((frac.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    let filled = if frac > 0.0 { filled.max(1) } else { 0 };
    vec![
        Span::styled("█".repeat(filled), fg(busy(frac))),
        Span::styled("░".repeat(width - filled), dim()),
    ]
}

/// `1.5`, `4`.
pub(super) fn trim(n: f64) -> String {
    let s = format!("{n:.1}");
    s.strip_suffix(".0").map_or(s.clone(), str::to_owned)
}

/// `512B`, `512Mi`, `1.2Gi`.
pub(super) fn bytes(n: u64) -> String {
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
    format!("{n}B")
}

fn panel(title: Line<'static>) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(fg(Color::DarkGray))
        .title(title)
}

fn title(s: impl Into<String>) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {} ", s.into()),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    ))
}

/// Whether the detail panel shows: as asked, else when there is room.
fn shows_detail(view: &View, width: u16) -> bool {
    view.tab != Tab::Log && view.detail.unwrap_or(width >= 150)
}

/// Draws the whole screen.
pub fn render(f: &mut Frame, model: &Model, view: &View) {
    let area = f.area();
    if area.width < 60 || area.height < 12 {
        f.render_widget(
            Paragraph::new("legion top needs at least 60×12.").alignment(Alignment::Center),
            area,
        );
        return;
    }
    let [top, tabs, body, bottom] = Layout::vertical([
        Constraint::Length(4),
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(area);
    header(f, model, top);
    f.render_widget(tab_bar(model, view), tabs);
    let body = match &view.logs {
        Some(l) if l.zoom => {
            logs(f, l, body);
            None
        }
        Some(l) => {
            // The table as tall as its rows, up to half.
            let rows = rows(model, view).len().min(1000) as u16 + 3;
            let upper = rows.clamp(5, (body.height / 2).max(5));
            let [upper, lower] =
                Layout::vertical([Constraint::Length(upper), Constraint::Fill(1)]).areas(body);
            logs(f, l, lower);
            Some(upper)
        }
        None => Some(body),
    };
    if let Some(body) = body {
        upper_body(f, model, view, body);
    }
    f.render_widget(Paragraph::new(bottom_line(view)), bottom);
    if let Some((key, scroll)) = &view.yaml {
        yaml(f, model, key, *scroll);
    }
    if view.help {
        help(f, view);
    }
}

/// The table, with the details beside it when shown.
fn upper_body(f: &mut Frame, model: &Model, view: &View, body: Rect) {
    let width = f.area().width;
    if shows_detail(view, width) {
        let side = (width / 3).clamp(40, 70);
        let [table, detail_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(side)]).areas(body);
        draw_table(f, model, view, table);
        detail(f, model, view, detail_area);
    } else {
        draw_table(f, model, view, body);
    }
}

/// The logs followed: the newest lines, or those scrolled back to.
fn logs(f: &mut Frame, logs: &Logs, area: Rect) {
    let state = match (&logs.ended, logs.back) {
        (_, back @ 1..) => Span::styled(format!(" {back} lines back · PgDn returns "), fg(WARN)),
        (Some(why), _) if why.starts_with("Failed") => Span::styled(format!(" {why} "), fg(BAD)),
        (Some(why), _) => Span::styled(format!(" {why} "), dim()),
        (None, _) => Span::styled(" following ", fg(OK)),
    };
    let block = panel(title(format!("Logs of {}", logs.of)))
        .title(Line::from(state).right_aligned())
        .title_bottom(
            Line::from(Span::styled(
                " PgUp PgDn scroll · z enlarges · Esc closes ",
                dim(),
            ))
            .right_aligned(),
        );
    let height = block.inner(area).height as usize;
    let end = logs.lines.len() - logs.back.min(logs.lines.len());
    let lines: Vec<Line> = if logs.lines.is_empty() && logs.ended.is_none() {
        vec![dimmed("Waiting for lines…")]
    } else {
        logs.lines
            .range(end.saturating_sub(height)..end)
            .map(|(stderr, l)| {
                let style = if *stderr { fg(WARN) } else { Style::default() };
                Line::from(Span::styled(l.clone(), style))
            })
            .collect()
    };
    f.render_widget(Paragraph::new(lines).block(block), area);
}

/// The managers, nodes and tasks at a glance, and graphs of use.
fn header(f: &mut Frame, model: &Model, area: Rect) {
    let mut top = Line::from(vec![Span::styled(
        " ◆ legion ",
        fg(ACCENT).add_modifier(Modifier::BOLD),
    )]);
    if let Some(ns) = &model.namespace {
        top.push_span(Span::styled(format!("namespace {ns} "), fg(WARN)));
    }
    let block = panel(top);
    let block = match &model.problem {
        Some(problem) => block.title(
            Line::from(Span::styled(
                format!(" not current: {problem} "),
                fg(BAD).add_modifier(Modifier::BOLD),
            ))
            .right_aligned(),
        ),
        None => block,
    };
    let inner = block.inner(area);
    f.render_widget(block, area);
    let [left, right] =
        Layout::horizontal([Constraint::Fill(1), Constraint::Percentage(45)]).areas(inner);
    f.render_widget(Paragraph::new(summary(model)), left);

    let (millicores, memory) = model.capacity();
    let rows = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).split(right);
    let now = model.history.back().copied();
    let cpu: Vec<u64> = model.history.iter().map(|(c, _)| *c as u64).collect();
    let mem: Vec<u64> = model.history.iter().map(|(_, m)| *m >> 20).collect();
    let cpu_label = format!(
        "{}/{} CPU",
        now.map_or("-".into(), |(c, _)| format!("{:.1}", c / 100.0)),
        trim(millicores as f64 / 1000.0)
    );
    let mem_label = format!(
        "{}/{}",
        now.map_or("-".into(), |(_, m)| bytes(m)),
        bytes(memory)
    );
    graph(f, rows[0], "cpu", &cpu, millicores / 10, &cpu_label, OK);
    graph(f, rows[1], "mem", &mem, memory >> 20, &mem_label, ACCENT);
}

/// One line: a name, a graph of the latest samples against `max`, and
/// the value now.
fn graph(f: &mut Frame, area: Rect, name: &str, data: &[u64], max: u64, now: &str, color: Color) {
    let [label, spark, value] = Layout::horizontal([
        Constraint::Length(4),
        Constraint::Fill(1),
        Constraint::Length(now.chars().count() as u16 + 1),
    ])
    .areas(area);
    f.render_widget(Paragraph::new(Span::styled(name.to_owned(), dim())), label);
    let skip = data.len().saturating_sub(spark.width as usize);
    let mut spark_widget = Sparkline::default()
        .data(&data[skip..])
        .style(fg(color))
        .bar_set(symbols::bar::NINE_LEVELS);
    if max > 0 {
        spark_widget = spark_widget.max(max);
    }
    f.render_widget(spark_widget, spark);
    f.render_widget(
        Paragraph::new(Span::raw(now.to_owned())).alignment(Alignment::Right),
        value,
    );
}

/// Two lines: nodes and tasks by state, then the managers.
fn summary(model: &Model) -> Vec<Line<'static>> {
    let mut first = Vec::new();
    if !model.hidden.contains(&Kind::Node) {
        let nodes: Vec<NodeStatus> = model.of(Kind::Node).map(status).collect();
        let ready = nodes.iter().filter(|n| n.state == NodeState::Ready).count();
        let color = if ready == nodes.len() { OK } else { WARN };
        first.push(Span::styled("● ", fg(color)));
        first.push(Span::raw(format!("{ready}/{} nodes ready   ", nodes.len())));
    }
    let tasks: Vec<TaskStatus> = model.of(Kind::Task).map(status).collect();
    let count = |f: &dyn Fn(TaskState) -> bool| tasks.iter().filter(|t| f(t.state)).count();
    let running = count(&|s| s == TaskState::Running);
    let failed = count(&|s| matches!(s, TaskState::Failed | TaskState::Rejected));
    let waiting = count(&|s| s != TaskState::Running && !s.is_terminal());
    first.extend([
        Span::styled("▲ ", fg(OK)),
        Span::raw(format!("{running} running   ")),
        Span::styled("◌ ", fg(WARN)),
        Span::raw(format!("{waiting} starting   ")),
        Span::styled("✗ ", fg(if failed > 0 { BAD } else { Color::DarkGray })),
        Span::raw(format!("{failed} failed")),
    ]);
    let second = match &model.cluster {
        Some(c) => {
            let leader = c
                .leader_id
                .and_then(|l| c.members.iter().find(|m| m.id == l).map(|m| m.addr.clone()));
            let leader = match (c.leader_id, leader) {
                (None, _) => Span::styled("no leader", fg(BAD)),
                (Some(_), Some(addr)) => Span::raw(format!("leader {addr}")),
                (Some(id), None) => Span::raw(format!("leader {id}")),
            };
            Line::from(vec![
                Span::styled("  managers ", dim()),
                Span::raw(c.members.len().to_string()),
                Span::styled(" · ", dim()),
                leader,
                Span::styled(" · term ", dim()),
                Span::raw(c.term.to_string()),
                Span::styled(" · version ", dim()),
                Span::raw(c.applied_version.to_string()),
            ])
        }
        None => dimmed("  asking the managers…"),
    };
    vec![Line::from(first), second]
}

fn tab_bar(model: &Model, view: &View) -> Tabs<'static> {
    let titles = TABS.iter().enumerate().map(|(i, t)| {
        let count: usize = t.kinds().iter().map(|k| model.of(*k).count()).sum();
        let mut spans = vec![
            Span::styled(format!("{}", i + 1), fg(ACCENT)),
            Span::raw(format!(" {}", t.title())),
        ];
        if *t != Tab::Log && count > 0 {
            spans.push(Span::styled(format!(" ({count})"), dim()));
        }
        Line::from(spans)
    });
    let selected = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(Color::DarkGray)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD)
    };
    Tabs::new(titles.collect::<Vec<_>>())
        .select(view.index())
        .highlight_style(selected)
        .divider(Span::styled("│", dim()))
        .padding(" ", " ")
}

fn draw_table(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let mut heading = vec![Span::styled(
        format!(" {} ", view.tab.title()),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    )];
    if view.tab == Tab::Tasks
        && let Some(focus) = &view.focus
    {
        let of = match focus {
            Focus::Node(n) => format!("on node {n}"),
            Focus::Owner(kind, ns, name) => format!("of {} {ns}/{name}", kind_word(*kind)),
        };
        heading.push(Span::styled(format!("{of} "), fg(WARN)));
    }
    let filter = &view.filter[view.index()];
    if !filter.is_empty() {
        heading.push(Span::styled(format!("/{filter} "), fg(WARN)));
    }
    let block = panel(Line::from(heading));

    let kinds = view.tab.kinds();
    if !kinds.is_empty() && kinds.iter().all(|k| model.hidden.contains(k)) {
        let what: Vec<&str> = kinds.iter().map(|k| kind_word(*k)).collect();
        f.render_widget(
            Paragraph::new(dimmed(format!(
                "Your role bindings do not let you read {}.",
                what.join(", ")
            )))
            .block(block),
            area,
        );
        return;
    }
    let rows = rows(model, view);
    let count = rows.len();
    let block = block.title(Line::from(Span::styled(format!(" {count} "), dim())).right_aligned());
    if rows.is_empty() {
        let empty = if filter.is_empty() {
            "Nothing here yet."
        } else {
            "Nothing matches the filter."
        };
        f.render_widget(Paragraph::new(dimmed(empty)).block(block), area);
        return;
    }
    let titles = view.tab.header();
    let mut widths: Vec<usize> = titles.iter().map(|t| t.len()).collect();
    for r in &rows {
        for (w, c) in widths.iter_mut().zip(&r.cells) {
            *w = (*w).max(c.width());
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
    let selected = view.selected[view.index()].min(count - 1);
    let highlight = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(Color::Indexed(237))
            .add_modifier(Modifier::BOLD)
    };
    let table = Table::new(
        rows.into_iter()
            .map(|r| Row::new(r.cells.into_iter().map(Cell::from)).style(r.style)),
        constraints,
    )
    .header(
        Row::new(titles.iter().map(|t| Cell::from(*t)))
            .style(dim().add_modifier(Modifier::BOLD))
            .bottom_margin(0),
    )
    .column_spacing(2)
    .row_highlight_style(highlight)
    .highlight_symbol(Span::styled("▌", fg(ACCENT)));
    let mut state = TableState::default().with_selected(Some(selected));
    f.render_stateful_widget(table.block(block), area, &mut state);
    if count + 3 > area.height as usize {
        let mut bar = ScrollbarState::new(count).position(selected);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_style(fg(ACCENT)),
            area.inner(ratatui::layout::Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut bar,
        );
    }
}

/// The selected object, field by field.
fn detail(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let Some(key) = view.selected_key(model) else {
        f.render_widget(
            Paragraph::new(dimmed("Nothing selected.")).block(panel(title("Details"))),
            area,
        );
        return;
    };
    let Some(o) = model.get(&key) else { return };
    let mut lines = vec![Line::from(vec![
        Span::styled("version ", dim()),
        Span::raw(o.metadata.version.to_string()),
        Span::styled("  generation ", dim()),
        Span::raw(o.metadata.generation.to_string()),
        Span::styled("  uid ", dim()),
        Span::raw(o.metadata.uid.clone()),
    ])];
    if o.kind == Kind::Task
        && let Some((cpu, memory)) = model.task_usage(o)
    {
        lines.push(Line::from(vec![
            Span::styled("uses ", dim()),
            Span::raw(format!("{cpu:.0}% of a CPU, {}", bytes(memory))),
        ]));
    }
    if !o.metadata.labels.is_empty() {
        lines.push(Line::default());
        lines.push(section("labels"));
        for (k, v) in &o.metadata.labels {
            lines.push(field(1, k, v));
        }
    }
    for (name, value) in [("status", &o.status), ("spec", &o.spec)] {
        if value.is_null() || value.as_object().is_some_and(|m| m.is_empty()) {
            continue;
        }
        lines.push(Line::default());
        lines.push(section(name));
        flatten(value, 1, &mut lines);
    }
    lines.push(Line::default());
    lines.push(dimmed("y: the whole YAML"));
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(panel(title(super::model::name(o)))),
        area,
    );
}

fn section(name: &str) -> Line<'static> {
    Line::from(Span::styled(
        name.to_owned(),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    ))
}

fn field(depth: usize, k: &str, v: &str) -> Line<'static> {
    Line::from(vec![
        Span::raw("  ".repeat(depth)),
        Span::styled(format!("{k}: "), dim()),
        Span::raw(v.to_owned()),
    ])
}

/// Fields of `v`, nested by indent: scalars inline, lists of scalars
/// joined, a list of objects by item.
fn flatten(v: &serde_json::Value, depth: usize, lines: &mut Vec<Line<'static>>) {
    let Some(map) = v.as_object() else {
        lines.push(field(depth, "", &scalar(v)));
        return;
    };
    for (k, v) in map {
        match v {
            serde_json::Value::Object(m) if m.is_empty() => {}
            serde_json::Value::Object(_) => {
                lines.push(field(depth, k, ""));
                flatten(v, depth + 1, lines);
            }
            serde_json::Value::Array(items) if items.iter().all(|i| !i.is_object()) => {
                let joined: Vec<String> = items.iter().map(scalar).collect();
                lines.push(field(depth, k, &joined.join(", ")));
            }
            serde_json::Value::Array(items) => {
                lines.push(field(depth, k, ""));
                for (i, item) in items.iter().enumerate() {
                    lines.push(field(depth + 1, &format!("- {}", i + 1), ""));
                    flatten(item, depth + 2, lines);
                }
            }
            v => lines.push(field(depth, k, &scalar(v))),
        }
    }
}

fn scalar(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "-".into(),
        other => other.to_string(),
    }
}

/// The YAML of an object over everything.
fn yaml(f: &mut Frame, model: &Model, key: &super::model::Key, scroll: u16) {
    let area = centered(f.area(), 90, 90);
    let Some(o) = model.get(key) else { return };
    let text = serde_yaml_ng::to_string(o).unwrap_or_else(|e| format!("{e}"));
    let lines: Vec<Line> = text
        .lines()
        .map(|l| match l.split_once(':') {
            Some((k, v)) if !k.trim_start().starts_with('-') || k.contains(' ') => {
                Line::from(vec![
                    Span::styled(format!("{k}:"), fg(ACCENT)),
                    Span::raw(v.to_owned()),
                ])
            }
            _ => Line::from(l.to_owned()),
        })
        .collect();
    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).scroll((scroll, 0)).block(
            panel(title(format!("{} — YAML", super::model::name(o)))).title_bottom(
                Line::from(Span::styled(
                    " ↑↓ PgUp PgDn scroll · any other key closes ",
                    dim(),
                ))
                .right_aligned(),
            ),
        ),
        area,
    );
}

const HELP: &[(&str, &[(&str, &str)])] = &[
    (
        "Moving around",
        &[
            ("1–7 ←→ Tab", "tabs"),
            ("↑↓ j k PgUp PgDn g G", "select"),
            ("Enter", "a node's or workload's tasks; else the YAML"),
            ("/", "filter rows; Esc clears"),
            ("i", "details panel on or off"),
            ("y", "the selected object's YAML"),
            ("h", "ended tasks too"),
            ("q Ctrl-C", "quit"),
        ],
    ),
    (
        "Acting on the selected row",
        &[
            ("d p a", "node: drain (asked), pause, activate"),
            ("L", "node: set or remove a label"),
            ("s r", "service: scale, roll back (asked)"),
            ("u", "cron job: suspend or resume"),
            ("m", "volume: move to a node"),
            ("l", "follow logs of a task, service or Job below"),
            ("x", "a shell in a task"),
            ("D", "delete (type the name; a task: asked)"),
        ],
    ),
    (
        "While logs show",
        &[
            ("PgUp PgDn", "scroll them"),
            ("z", "logs over the table, or not"),
            ("Esc", "close them"),
        ],
    ),
];

fn help(f: &mut Frame, view: &View) {
    let mut lines = Vec::new();
    for (heading, keys) in HELP {
        lines.push(section(heading));
        for (k, what) in *keys {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {k:<22}"),
                    fg(ACCENT).add_modifier(Modifier::BOLD),
                ),
                Span::raw((*what).to_owned()),
            ]));
        }
        lines.push(Line::default());
    }
    lines.push(dimmed(format!(
        "On the {} tab now. What you may do follows your role bindings.",
        view.tab.title()
    )));
    let [area] = Layout::vertical([Constraint::Length(lines.len() as u16 + 2)])
        .flex(Flex::Center)
        .areas(centered(f.area(), 70, 100));
    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).block(
            panel(title("Keys"))
                .title_bottom(Line::from(Span::styled(" any key closes ", dim())).right_aligned()),
        ),
        area,
    );
}

fn centered(area: Rect, width_percent: u16, height_percent: u16) -> Rect {
    let [area] = Layout::horizontal([Constraint::Percentage(width_percent)])
        .flex(Flex::Center)
        .areas(area);
    let [area] = Layout::vertical([Constraint::Percentage(height_percent)])
        .flex(Flex::Center)
        .areas(area);
    area
}

/// A question, else what came of the last request, else the keys.
fn bottom_line(view: &View) -> Line<'static> {
    let ask = |s: String| Line::from(Span::styled(s, fg(WARN).add_modifier(Modifier::BOLD)));
    match &view.prompt {
        Some(Prompt::Confirm(_, q)) => return ask(format!(" {q}")),
        Some(Prompt::Input { ask: q, input, .. }) => {
            return Line::from(vec![
                Span::styled(format!(" {q}: "), fg(WARN).add_modifier(Modifier::BOLD)),
                Span::raw(input.clone()),
                Span::styled("▏", fg(ACCENT)),
                Span::styled("  Enter · Esc cancels", dim()),
            ]);
        }
        Some(Prompt::Choose {
            ask: q, options, ..
        }) => {
            let mut spans = vec![Span::styled(
                format!(" {q} "),
                fg(WARN).add_modifier(Modifier::BOLD),
            )];
            for (i, o) in options.iter().enumerate().take(9) {
                spans.push(Span::styled(format!(" {} ", i + 1), fg(ACCENT)));
                spans.push(Span::raw(o.clone()));
            }
            spans.push(Span::styled("  Esc cancels", dim()));
            return Line::from(spans);
        }
        None => {}
    }
    if let Some(notice) = &view.notice {
        let style = if notice.starts_with("Failed") || notice.starts_with("Not ") {
            fg(BAD)
        } else {
            fg(OK)
        };
        return Line::from(Span::styled(
            format!(" {notice}"),
            style.add_modifier(Modifier::BOLD),
        ));
    }
    let keys: &[(&str, &str)] = match view.tab {
        Tab::Nodes => &[
            ("Enter", "tasks"),
            ("d", "drain"),
            ("p", "pause"),
            ("a", "activate"),
            ("L", "label"),
        ],
        Tab::Workloads => &[
            ("Enter", "tasks"),
            ("s", "scale"),
            ("r", "roll back"),
            ("u", "suspend"),
            ("l", "logs"),
        ],
        Tab::Tasks => &[
            ("l", "logs"),
            ("x", "shell"),
            ("h", "ended"),
            ("D", "delete"),
        ],
        Tab::Storage => &[("m", "move"), ("D", "delete")],
        Tab::Network | Tab::Config => &[("y", "YAML"), ("D", "delete")],
        Tab::Log => &[],
    };
    let logs: &[(&str, &str)] = if view.logs.is_some() {
        &[("Esc", "close logs")]
    } else {
        &[]
    };
    let mut spans = Vec::new();
    for (k, what) in logs
        .iter()
        .chain(keys)
        .chain(&[("/", "filter"), ("?", "help"), ("q", "quit")])
    {
        spans.push(Span::styled(
            format!(" {k} "),
            if mono() {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            },
        ));
        spans.push(Span::styled(format!(" {what} "), dim()));
    }
    Line::from(spans)
}

/// `--once`: a tab's table as plain text, columns padded.
pub(super) fn plain(model: &Model, view: &View) -> String {
    let rows = rows(model, view);
    let titles = view.tab.header();
    let cells: Vec<Vec<String>> = std::iter::once(titles.iter().map(|t| (*t).to_owned()).collect())
        .chain(
            rows.iter()
                .map(|r| r.cells.iter().map(|c| c.to_string()).collect()),
        )
        .collect();
    let mut widths = vec![0; titles.len()];
    for row in &cells {
        for (w, c) in widths.iter_mut().zip(row) {
            *w = (*w).max(c.chars().count());
        }
    }
    let mut out = String::new();
    for row in &cells {
        let line: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        out.push_str(line.join("  ").trim_end());
        out.push('\n');
    }
    out
}

/// `--once`: the header's first line as plain text.
pub(super) fn plain_summary(model: &Model) -> String {
    summary(model)
        .iter()
        .map(|l| l.to_string().trim().to_owned())
        .collect::<Vec<_>>()
        .join("\n")
}
