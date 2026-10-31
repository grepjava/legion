//! Drawing `legion top`: a header of the cluster at a glance with graphs
//! of what its tasks use, the tabs, the selected tab's table beside the
//! selected object's details, logs followed under them, and the bottom line of keys, questions and
//! answers; over them, an object's YAML or the keys' help. The colours
//! and bars follow jmon's.

use std::sync::OnceLock;

use legion_api::Kind;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::TaskState;
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
use super::model::{Key, Model, kind_word, status, task_state};
use super::shell::Shell;
use super::view::{Focus, Prompt, TABS, Tab, TableRow, View, rows};

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
    let rows = if view.zoom && (view.logs.is_some() || view.shell.is_some()) {
        Vec::new()
    } else {
        rows(model, view)
    };
    let (upper, lower) = split(view, rows.len(), body);
    if let Some(upper) = upper {
        upper_body(f, model, view, rows, upper);
    }
    if let Some(lower) = lower {
        if let Some(l) = &view.logs {
            logs(f, l, lower);
        }
        if let Some(s) = &view.shell {
            shell(f, s, lower);
        }
    }
    f.render_widget(Paragraph::new(bottom_line(view)), bottom);
    if let Some((key, scroll)) = &view.yaml {
        yaml(f, model, key, *scroll);
    }
    if view.help {
        help(f, view);
    }
}

/// The table's area and the pane's under it, if they show.
fn split(view: &View, row_count: usize, body: Rect) -> (Option<Rect>, Option<Rect>) {
    if view.logs.is_none() && view.shell.is_none() {
        return (Some(body), None);
    }
    if view.zoom {
        return (None, Some(body));
    }
    // The table as tall as its rows, up to half.
    let rows = row_count.min(1000) as u16 + 3;
    let upper = rows.clamp(5, (body.height / 2).max(5));
    let [upper, lower] =
        Layout::vertical([Constraint::Length(upper), Constraint::Fill(1)]).areas(body);
    (Some(upper), Some(lower))
}

/// The rows and columns of the shell's screen on a terminal of `area`.
pub(super) fn shell_size(model: &Model, view: &View, area: Rect) -> Option<(u16, u16)> {
    if area.width < 60 || area.height < 12 {
        return None;
    }
    let body = Rect {
        y: area.y + 5,
        height: area.height - 6,
        ..area
    };
    let row_count = if view.zoom { 0 } else { rows(model, view).len() };
    let lower = split(view, row_count, body).1?;
    let inner = panel(Line::default()).inner(lower);
    Some((inner.height.max(1), inner.width.max(1)))
}

/// The shell's screen, as the terminal emulated shows it.
fn shell(f: &mut Frame, shell: &Shell, area: Rect) {
    let screen = shell.screen.screen();
    let state = match (&shell.ended, shell.focus, screen.scrollback()) {
        (Some(why), ..) if why.starts_with("Failed") => Span::styled(format!(" {why} "), fg(BAD)),
        (Some(why), ..) => Span::styled(format!(" {why} "), dim()),
        (None, _, back @ 1..) => {
            Span::styled(format!(" {back} lines back · PgDn returns "), fg(WARN))
        }
        (None, true, _) => Span::styled(" has the keys · Ctrl-] gives them back ", fg(OK)),
        (None, false, _) => Span::styled(" Ctrl-] types in it again ", fg(WARN)),
    };
    let hint = if shell.focus {
        " keys go to the shell "
    } else {
        " PgUp PgDn scroll · z enlarges · Esc closes "
    };
    let border = if shell.focus { ACCENT } else { Color::DarkGray };
    let block = panel(title(format!("Shell in {}", shell.of)))
        .border_style(fg(border))
        .title(Line::from(state).right_aligned())
        .title_bottom(Line::from(Span::styled(hint, dim())).right_aligned());
    let inner = block.inner(area);
    f.render_widget(block, area);
    let buf = f.buffer_mut();
    let (rows, cols) = shell.size();
    for row in 0..rows.min(inner.height) {
        for col in 0..cols.min(inner.width) {
            let Some(cell) = screen.cell(row, col) else {
                continue;
            };
            if cell.is_wide_continuation() {
                continue;
            }
            let at = &mut buf[(inner.x + col, inner.y + row)];
            let text = cell.contents();
            at.set_symbol(if text.is_empty() { " " } else { text });
            at.set_style(cell_style(cell));
        }
    }
    let (row, col) = screen.cursor_position();
    if shell.focus
        && shell.ended.is_none()
        && screen.scrollback() == 0
        && !screen.hide_cursor()
        && row < inner.height
        && col < inner.width
    {
        f.set_cursor_position((inner.x + col, inner.y + row));
    }
}

fn cell_style(cell: &vt100::Cell) -> Style {
    let mut style = Style::default();
    if !mono() {
        if let Some(c) = color(cell.fgcolor()) {
            style = style.fg(c);
        }
        if let Some(c) = color(cell.bgcolor()) {
            style = style.bg(c);
        }
    }
    for (on, m) in [
        (cell.bold(), Modifier::BOLD),
        (cell.dim(), Modifier::DIM),
        (cell.italic(), Modifier::ITALIC),
        (cell.underline(), Modifier::UNDERLINED),
        (cell.inverse(), Modifier::REVERSED),
    ] {
        if on {
            style = style.add_modifier(m);
        }
    }
    style
}

fn color(c: vt100::Color) -> Option<Color> {
    match c {
        vt100::Color::Default => None,
        vt100::Color::Idx(i) => Some(Color::Indexed(i)),
        vt100::Color::Rgb(r, g, b) => Some(Color::Rgb(r, g, b)),
    }
}

/// The table, with the details beside it when shown.
fn upper_body(f: &mut Frame, model: &Model, view: &View, rows: Vec<TableRow>, body: Rect) {
    let width = f.area().width;
    if shows_detail(view, width) {
        let selected = rows.get(view.selected[view.index()]).and_then(|r| r.key.clone());
        let side = (width / 3).clamp(40, 70);
        let [table, detail_area] =
            Layout::horizontal([Constraint::Fill(1), Constraint::Length(side)]).areas(body);
        draw_table(f, model, view, rows, table);
        detail(f, model, selected, detail_area);
    } else {
        draw_table(f, model, view, rows, body);
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
    let cpu: Vec<f64> = model.history.iter().map(|(c, _)| c / 100.0).collect();
    let mem: Vec<f64> = model.history.iter().map(|(_, m)| *m as f64).collect();
    let cores = |c: f64| format!("{c:.1}");
    let cpu_total = format!(" CPU of {}", trim(millicores as f64 / 1000.0));
    let mem_total = format!(" of {}", bytes(memory));
    let mib = |m: f64| bytes(m as u64);
    graph(
        f,
        rows[0],
        "cpu",
        &cpu,
        millicores as f64 / 1000.0,
        &cores,
        &cpu_total,
        OK,
    );
    graph(
        f,
        rows[1],
        "mem",
        &mem,
        memory as f64,
        &mib,
        &mem_total,
        ACCENT,
    );
}

/// One line of what all tasks use: a bar of the share of what the nodes
/// hold, in jmon's colours; that share and the amounts; then the latest
/// samples, scaled to their peak so that small loads show, and the peak.
#[allow(clippy::too_many_arguments)]
fn graph(
    f: &mut Frame,
    area: Rect,
    name: &str,
    samples: &[f64],
    capacity: f64,
    show: &dyn Fn(f64) -> String,
    total: &str,
    color: Color,
) {
    let now = samples.last().copied();
    let frac = match now {
        Some(n) if capacity > 0.0 => n / capacity,
        _ => 0.0,
    };
    let share = match now {
        None => "  -".to_owned(),
        Some(n) if n > 0.0 && frac < 0.01 => " <1%".to_owned(),
        Some(_) => format!("{:>3.0}%", frac * 100.0),
    };
    let mut head = vec![Span::styled(format!("{name} "), dim())];
    head.extend(meter(frac, 10));
    head.push(Span::styled(
        format!(" {share} "),
        fg(busy(frac)).add_modifier(Modifier::BOLD),
    ));
    head.push(Span::raw(now.map_or("-".into(), show)));
    head.push(Span::styled(total.to_owned(), dim()));
    let head = Line::from(head);
    let [head_area, spark, peak_area] = Layout::horizontal([
        Constraint::Length(head.width() as u16 + 2),
        Constraint::Fill(1),
        Constraint::Length(14),
    ])
    .areas(area);
    f.render_widget(Paragraph::new(head), head_area);
    if spark.width < 8 {
        return;
    }
    let shown = &samples[samples.len().saturating_sub(spark.width as usize)..];
    let peak = shown.iter().copied().fold(0.0, f64::max);
    // Scaled to the peak, but not below 2 % of what the nodes hold:
    // noise on an idle cluster stays flat.
    let scale = peak.max(capacity * 0.02).max(f64::MIN_POSITIVE);
    let data: Vec<u64> = shown
        .iter()
        .map(|v| (v / scale * 64.0).round() as u64)
        .collect();
    f.render_widget(
        Sparkline::default()
            .data(&data)
            .max(64)
            .style(fg(color))
            .bar_set(symbols::bar::NINE_LEVELS),
        spark,
    );
    if !shown.is_empty() {
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(" peak ", dim()),
                Span::raw(show(peak)),
            ])),
            peak_area,
        );
    }
}

/// Two lines: nodes and tasks by state, then the managers.
fn summary(model: &Model) -> Vec<Line<'static>> {
    let mut first = Vec::new();
    if !model.hidden.contains(&Kind::Node) {
        let (mut total, mut ready) = (0usize, 0usize);
        for n in model.of(Kind::Node) {
            total += 1;
            if status::<NodeStatus>(n).state == NodeState::Ready {
                ready += 1;
            }
        }
        let color = if ready == total { OK } else { WARN };
        first.push(Span::styled("● ", fg(color)));
        first.push(Span::raw(format!("{ready}/{total} nodes ready   ")));
    }
    let (mut running, mut waiting, mut failed) = (0usize, 0usize, 0usize);
    for t in model.of(Kind::Task) {
        match task_state(t) {
            TaskState::Running => running += 1,
            TaskState::Failed | TaskState::Rejected => failed += 1,
            s if !s.is_terminal() => waiting += 1,
            _ => {}
        }
    }
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

fn draw_table(f: &mut Frame, model: &Model, view: &View, rows: Vec<TableRow>, area: Rect) {
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
    let page = area.height.saturating_sub(3) as usize;
    let start = if page > 0 {
        selected.saturating_sub(page - 1)
    } else {
        0
    };
    let end = (start + page).min(count);
    let highlight = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(Color::Indexed(237))
            .add_modifier(Modifier::BOLD)
    };
    let table = Table::new(
        rows.into_iter()
            .skip(start)
            .take(end - start)
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
    let mut state = TableState::default().with_selected(Some(selected - start));
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
fn detail(f: &mut Frame, model: &Model, selected: Option<Key>, area: Rect) {
    let Some(key) = selected else {
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
            (
                "x",
                "a shell in a task, below; Ctrl-] leaves it and returns",
            ),
            ("D", "delete (type the name; a task: asked)"),
        ],
    ),
    (
        "While logs or a shell show",
        &[
            ("PgUp PgDn", "scroll back"),
            ("z", "over the table, or not"),
            ("Esc", "close (a shell ends)"),
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
    if view.typing() {
        return Line::from(vec![
            Span::styled(
                " The shell has the keys. ",
                fg(OK).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" Ctrl-] ", fg(ACCENT).add_modifier(Modifier::BOLD)),
            Span::styled(" gives them back to legion top ", dim()),
        ]);
    }
    let logs: &[(&str, &str)] = if view.logs.is_some() {
        &[("Esc", "close logs")]
    } else if view.shell.as_ref().is_some_and(|s| s.ended.is_none()) {
        &[("Ctrl-]", "to the shell"), ("Esc", "end it")]
    } else if view.shell.is_some() {
        &[("Esc", "close")]
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
