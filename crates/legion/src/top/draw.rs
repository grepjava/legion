//! Drawing `legion top`: a header of the cluster at a glance with graphs
//! of what its tasks use, the tabs, the selected tab's table beside the
//! selected object's details, logs followed under them, and the bottom line of keys, questions and
//! answers; over them, an object's YAML or the keys' help. The colours
//! and bars follow jmon's.

use std::sync::OnceLock;

use legion_api::Kind;
use legion_api::node::{NodeState, NodeStatus};
use legion_api::task::TaskState;
use legion_proto::v1 as pb;
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
use super::view::{Focus, Preset, Prompt, SortCol, TABS, Tab, TableRow, View, rows, rows_for_tab};

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

/// Smooth `btop`-style Truecolor RGB heat gradient (cyan → emerald → amber → crimson).
pub(super) fn heat_rgb(frac: f64) -> Color {
    if mono() {
        return Color::Reset;
    }
    let f = frac.clamp(0.0, 1.0);
    let lerp = |a: u8, b: u8, t: f64| (a as f64 + (b as f64 - a as f64) * t).round() as u8;
    if f < 0.45 {
        let t = f / 0.45;
        Color::Rgb(lerp(56, 52, t), lerp(189, 211, t), lerp(248, 153, t))
    } else if f < 0.75 {
        let t = (f - 0.45) / 0.30;
        Color::Rgb(lerp(52, 251, t), lerp(211, 191, t), lerp(153, 36, t))
    } else {
        let t = (f - 0.75) / 0.25;
        Color::Rgb(lerp(251, 248, t), lerp(191, 113, t), lerp(36, 113, t))
    }
}

/// Unicode `2×4` Braille area graph (`U+2800`..`U+28FF`) as in `btop`:
/// `2 * width` horizontal data points × `4 * height` vertical sub-dots.
pub(super) fn braille_lines(
    samples: &[f64],
    capacity: f64,
    width: usize,
    height: usize,
) -> Vec<Line<'static>> {
    if width == 0 || height == 0 {
        return Vec::new();
    }
    const LEFT_DOTS: [u32; 4] = [0x40, 0x04, 0x02, 0x01];
    const RIGHT_DOTS: [u32; 4] = [0x80, 0x20, 0x10, 0x08];
    let pts_needed = width * 2;
    let tail = &samples[samples.len().saturating_sub(pts_needed)..];
    let peak = tail.iter().copied().fold(0.0, f64::max);
    let scale = peak.max(capacity * 0.02).max(f64::MIN_POSITIVE);
    let color_denom = if capacity > 0.0 { capacity } else { scale };
    let total_dots = height * 4;
    let pad = pts_needed.saturating_sub(tail.len());
    let sample_at = |idx: usize| -> f64 {
        if idx < pad {
            0.0
        } else {
            tail[idx - pad].max(0.0)
        }
    };
    let dots_for = |val: f64| -> usize {
        if val <= 0.0 {
            0
        } else {
            (((val / scale).clamp(0.0, 1.0) * total_dots as f64).round() as usize)
                .max(1)
                .min(total_dots)
        }
    };
    let mut out = Vec::with_capacity(height);
    for row in 0..height {
        // row 0 is the top row, row (height - 1) is the bottom row.
        let base_dot = (height - 1 - row) * 4;
        let mut spans = Vec::with_capacity(width);
        for col in 0..width {
            let lv = sample_at(col * 2);
            let rv = sample_at(col * 2 + 1);
            let ld = dots_for(lv);
            let rd = dots_for(rv);
            let mut mask = 0u32;
            for sub in 0..4 {
                if ld > base_dot + sub {
                    mask |= LEFT_DOTS[sub];
                }
                if rd > base_dot + sub {
                    mask |= RIGHT_DOTS[sub];
                }
            }
            let ch = char::from_u32(0x2800 + mask).unwrap_or('⠀');
            let style = if mask == 0 {
                dim()
            } else {
                fg(heat_rgb(lv.max(rv) / color_denom))
            };
            spans.push(Span::styled(ch.to_string(), style));
        }
        out.push(Line::from(spans));
    }
    out
}

/// `128Ki/s`, `4.2Mi/s`.
pub(super) fn rate(bps: u64) -> String {
    format!("{}/s", bytes(bps))
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
pub(crate) fn bytes(n: u64) -> String {
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

/// Whether the terminal has room for the detail panel unless asked.
pub(super) fn wide(width: u16) -> bool {
    width >= 150
}

/// Whether the detail panel shows: as asked, else when there is room.
fn shows_detail(view: &View, width: u16) -> bool {
    view.tab != Tab::Log && view.detail.unwrap_or(wide(width))
}

fn top_height(view: &View) -> u16 {
    match view.preset {
        Preset::Classic => 5,
        Preset::Telemetry => 8,
        Preset::Bento => {
            if view.boxes[0] {
                7
            } else {
                0
            }
        }
    }
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
    match view.preset {
        Preset::Classic => {
            let [top, tabs, body, bottom] = Layout::vertical([
                Constraint::Length(4),
                Constraint::Length(1),
                Constraint::Fill(1),
                Constraint::Length(1),
            ])
            .areas(area);
            header(f, model, top);
            f.render_widget(tab_bar(model, view), tabs);
            render_body_and_pane(f, model, view, body);
            f.render_widget(Paragraph::new(bottom_line(view)), bottom);
        }
        Preset::Telemetry => {
            let [top, tabs, body, bottom] = Layout::vertical([
                Constraint::Length(7),
                Constraint::Length(1),
                Constraint::Fill(1),
                Constraint::Length(1),
            ])
            .areas(area);
            bento_telemetry_deck(f, model, view, top);
            f.render_widget(tab_bar(model, view), tabs);
            render_body_and_pane(f, model, view, body);
            f.render_widget(Paragraph::new(bottom_line(view)), bottom);
        }
        Preset::Bento => {
            let deck_h = if view.boxes[0] && area.height >= 18 {
                7
            } else {
                0
            };
            let [top, body, bottom] = Layout::vertical([
                Constraint::Length(deck_h),
                Constraint::Fill(1),
                Constraint::Length(1),
            ])
            .areas(area);
            if deck_h > 0 {
                bento_telemetry_deck(f, model, view, top);
            }
            let row_count = if view.zoom && (view.logs.is_some() || view.shell.is_some()) {
                0
            } else {
                rows(model, view).len()
            };
            let (upper, lower) = split(view, row_count, body);
            if let Some(upper) = upper {
                bento_body(f, model, view, upper);
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
        }
    }
    if let Some((key, scroll)) = &view.yaml {
        yaml(f, model, key, *scroll);
    }
    if view.help {
        help(f, view);
    }
}

fn render_body_and_pane(f: &mut Frame, model: &Model, view: &View, body: Rect) {
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
    let used_top = top_height(view);
    let body = Rect {
        y: area.y + used_top,
        height: area.height.saturating_sub(used_top + 1),
        ..area
    };
    let row_count = if view.zoom {
        0
    } else {
        rows(model, view).len()
    };
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
            // A wide character with no room for its second half would be
            // drawn over the border.
            let clipped = cell.is_wide() && col + 1 >= inner.width;
            at.set_symbol(if text.is_empty() || clipped {
                " "
            } else {
                text
            });
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
        let at = view.selected[view.index()].min(rows.len().saturating_sub(1));
        let selected = rows.get(at).and_then(|r| r.key.clone());
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
    let heading = if logs.titled {
        logs.of.clone()
    } else {
        format!("Logs of {}", logs.of)
    };
    let block = panel(title(heading))
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
    let block = match model.problem.as_ref().or(model.polled.as_ref()) {
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

/// `btop`-style 4-subsystem Unicode Braille telemetry deck (`CPU`, `MEM`, `ZFS`, `NET`).
fn bento_telemetry_deck(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let mut top = vec![
        Span::styled(" ◆ legion ", fg(ACCENT).add_modifier(Modifier::BOLD)),
        Span::styled(format!("[{}] ", view.preset.label()), dim()),
    ];
    if let Some(ns) = &model.namespace {
        top.push(Span::styled(format!("ns:{ns} "), fg(WARN)));
    }
    for s in summary(model).into_iter().take(1) {
        top.extend(s.spans);
    }
    let mut right = vec![Span::styled(
        format!(" poll {}s ", view.poll_ms as f64 / 1000.0),
        dim(),
    )];
    if view.sort != SortCol::Default {
        let arrow = if view.sort_desc { "↓" } else { "↑" };
        right.push(Span::styled(
            format!("sort:{}{arrow} ", view.sort.label()),
            fg(ACCENT),
        ));
    }
    if view.tree {
        right.push(Span::styled("tree ", fg(OK)));
    }
    if let Some(problem) = &model.problem {
        right.push(Span::styled(
            format!("not current: {problem} "),
            fg(BAD).add_modifier(Modifier::BOLD),
        ));
    }
    let block = panel(Line::from(top)).title(Line::from(right).right_aligned());
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.height == 0 || inner.width < 20 {
        return;
    }

    let (millicores, memory) = model.capacity();
    let cpu_cap = millicores as f64 / 1000.0;
    let mem_cap = memory as f64;
    let cpu_samples: Vec<f64> = model.history.iter().map(|(c, _)| c / 100.0).collect();
    let mem_samples: Vec<f64> = model.history.iter().map(|(_, m)| *m as f64).collect();
    let (zfs_used, zfs_avail) = model.zfs_totals();
    let zfs_cap = (zfs_used + zfs_avail) as f64;
    let zfs_samples: Vec<f64> = model
        .telemetry_history
        .iter()
        .map(|(u, _, _, _)| *u as f64)
        .collect();
    let (net_rx, net_tx) = model.net_totals();
    let net_samples: Vec<f64> = model
        .telemetry_history
        .iter()
        .map(|(_, _, rx, tx)| (*rx + *tx) as f64)
        .collect();
    let net_peak = net_samples
        .iter()
        .copied()
        .fold(0.0, f64::max)
        .max((1 << 20) as f64);

    let cols = Layout::horizontal([
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
    ])
    .split(inner);

    let cpu_now = cpu_samples.last().copied().unwrap_or(0.0);
    let cpu_label = format!(
        "{:.1}/{} cores",
        cpu_now,
        if cpu_cap > 0.0 {
            trim(cpu_cap)
        } else {
            "-".into()
        }
    );
    braille_card(f, cols[0], "¹cpu", &cpu_samples, cpu_cap, &cpu_label);

    let mem_now = mem_samples.last().copied().unwrap_or(0.0) as u64;
    let mem_label = format!("{}/{}", bytes(mem_now), bytes(memory));
    braille_card(f, cols[1], "mem", &mem_samples, mem_cap, &mem_label);

    let zfs_label = if zfs_used + zfs_avail > 0 {
        format!("{}/{}", bytes(zfs_used), bytes(zfs_used + zfs_avail))
    } else {
        "no zfs telemetry".into()
    };
    braille_card(f, cols[2], "zfs", &zfs_samples, zfs_cap, &zfs_label);

    let net_label = format!("▼{} ▲{}", rate(net_rx), rate(net_tx));
    braille_card(f, cols[3], "net", &net_samples, net_peak, &net_label);
}

fn braille_card(
    f: &mut Frame,
    area: Rect,
    name: &str,
    samples: &[f64],
    capacity: f64,
    readout: &str,
) {
    if area.width < 8 || area.height < 2 {
        return;
    }
    let now = samples.last().copied().unwrap_or(0.0);
    let frac = if capacity > 0.0 {
        (now / capacity).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let pct = if capacity > 0.0 {
        format!("{:>3.0}%", frac * 100.0)
    } else {
        String::new()
    };
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(fg(Color::DarkGray))
        .title(Line::from(vec![
            Span::styled(format!(" {name} "), fg(ACCENT).add_modifier(Modifier::BOLD)),
            Span::styled(pct, fg(heat_rgb(frac)).add_modifier(Modifier::BOLD)),
            Span::raw(" "),
        ]))
        .title(Line::from(Span::styled(format!(" {readout} "), dim())).right_aligned());
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let lines = braille_lines(
        samples,
        capacity,
        inner.width as usize,
        inner.height as usize,
    );
    f.render_widget(Paragraph::new(lines), inner);
}

/// Multi-pane `btop` dashboard layout: `Nodes`, `Workloads`, and `Tasks`/`Storage`
/// (or whichever tab is currently selected) side by side.
fn bento_body(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let show_nodes = view.boxes[1];
    let show_workloads = view.boxes[2];
    let show_lower = view.boxes[3];

    // Determine which auxiliary tab occupies the lower-right box so pressing 1-7
    // always brings the selected tab on screen with an accented border!
    let lower_tab = match view.tab {
        Tab::Nodes | Tab::Workloads | Tab::Tasks => Tab::Tasks,
        other => other,
    };

    if !show_nodes && !show_workloads && !show_lower {
        let rows = rows_for_tab(model, view, view.tab);
        upper_body(f, model, view, rows, area);
        return;
    }

    match (show_nodes, show_workloads || show_lower) {
        (true, true) => {
            let [left, right] =
                Layout::horizontal([Constraint::Percentage(38), Constraint::Fill(1)]).areas(area);
            if left.height >= 14 && view.tab != Tab::Storage {
                let [nodes_area, storage_area] =
                    Layout::vertical([Constraint::Percentage(58), Constraint::Fill(1)]).areas(left);
                draw_pane_table(
                    f,
                    model,
                    view,
                    Tab::Nodes,
                    rows_for_tab(model, view, Tab::Nodes),
                    nodes_area,
                    view.tab == Tab::Nodes,
                    "²",
                );
                draw_pane_table(
                    f,
                    model,
                    view,
                    Tab::Storage,
                    rows_for_tab(model, view, Tab::Storage),
                    storage_area,
                    false,
                    "⁴",
                );
            } else {
                draw_pane_table(
                    f,
                    model,
                    view,
                    Tab::Nodes,
                    rows_for_tab(model, view, Tab::Nodes),
                    left,
                    view.tab == Tab::Nodes,
                    "²",
                );
            }

            match (show_workloads, show_lower) {
                (true, true) => {
                    let [top_r, bot_r] =
                        Layout::vertical([Constraint::Percentage(50), Constraint::Fill(1)])
                            .areas(right);
                    draw_pane_table(
                        f,
                        model,
                        view,
                        Tab::Workloads,
                        rows_for_tab(model, view, Tab::Workloads),
                        top_r,
                        view.tab == Tab::Workloads,
                        "³",
                    );
                    draw_pane_table(
                        f,
                        model,
                        view,
                        lower_tab,
                        rows_for_tab(model, view, lower_tab),
                        bot_r,
                        view.tab == lower_tab,
                        "⁴",
                    );
                }
                (true, false) => {
                    draw_pane_table(
                        f,
                        model,
                        view,
                        Tab::Workloads,
                        rows_for_tab(model, view, Tab::Workloads),
                        right,
                        view.tab == Tab::Workloads,
                        "³",
                    );
                }
                (false, true) => {
                    draw_pane_table(
                        f,
                        model,
                        view,
                        lower_tab,
                        rows_for_tab(model, view, lower_tab),
                        right,
                        view.tab == lower_tab,
                        "⁴",
                    );
                }
                (false, false) => {}
            }
        }
        (true, false) => {
            draw_pane_table(
                f,
                model,
                view,
                Tab::Nodes,
                rows_for_tab(model, view, Tab::Nodes),
                area,
                true,
                "²",
            );
        }
        (false, true) => {
            let tab = if show_workloads && !show_lower {
                Tab::Workloads
            } else if !show_workloads && show_lower {
                lower_tab
            } else {
                let [top_r, bot_r] =
                    Layout::vertical([Constraint::Percentage(50), Constraint::Fill(1)]).areas(area);
                draw_pane_table(
                    f,
                    model,
                    view,
                    Tab::Workloads,
                    rows_for_tab(model, view, Tab::Workloads),
                    top_r,
                    view.tab == Tab::Workloads,
                    "³",
                );
                draw_pane_table(
                    f,
                    model,
                    view,
                    lower_tab,
                    rows_for_tab(model, view, lower_tab),
                    bot_r,
                    view.tab == lower_tab,
                    "⁴",
                );
                return;
            };
            draw_pane_table(
                f,
                model,
                view,
                tab,
                rows_for_tab(model, view, tab),
                area,
                true,
                "",
            );
        }
        (false, false) => {}
    }
}

fn draw_table(f: &mut Frame, model: &Model, view: &View, rows: Vec<TableRow>, area: Rect) {
    draw_pane_table(f, model, view, view.tab, rows, area, false, "");
}

#[allow(clippy::too_many_arguments)]
fn draw_pane_table(
    f: &mut Frame,
    model: &Model,
    view: &View,
    tab: Tab,
    rows: Vec<TableRow>,
    area: Rect,
    active_border: bool,
    badge: &str,
) {
    let tab_idx = TABS.iter().position(|t| *t == tab).unwrap_or(0);
    let mut heading = vec![Span::styled(
        format!(" {badge}{} ", tab.title()),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    )];
    if tab == Tab::Tasks
        && let Some(focus) = &view.focus
    {
        let of = match focus {
            Focus::Node(n) => format!("on node {n}"),
            Focus::Owner(kind, ns, name) => format!("of {} {ns}/{name}", kind_word(*kind)),
        };
        heading.push(Span::styled(format!("{of} "), fg(WARN)));
    }
    let filter = &view.filter[tab_idx];
    if !filter.is_empty() {
        heading.push(Span::styled(format!("/{filter} "), fg(WARN)));
    }
    let border_color = if active_border {
        ACCENT
    } else {
        Color::DarkGray
    };
    let block = panel(Line::from(heading)).border_style(fg(border_color));

    let kinds = tab.kinds();
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
    let titles = tab.header();
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
    let selected = view.selected[tab_idx].min(count - 1);
    let page = area.height.saturating_sub(3) as usize;
    let start = if page > 0 {
        selected.saturating_sub(page - 1)
    } else {
        0
    };
    let end = (start + page).min(count);
    let is_focused_tab = view.tab == tab;
    let highlight = if !is_focused_tab && view.preset == Preset::Bento {
        Style::default()
    } else if mono() {
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
    .highlight_symbol(if is_focused_tab || view.preset != Preset::Bento {
        Span::styled("▌", fg(ACCENT))
    } else {
        Span::raw(" ")
    });
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
    if o.kind == Kind::Node
        && let Some(nm) = model.node_metrics(&o.metadata.name)
    {
        if nm.zfs_used + nm.zfs_avail > 0 {
            lines.push(Line::from(vec![
                Span::styled("zfs pool ", dim()),
                Span::raw(format!(
                    "{} used of {}",
                    bytes(nm.zfs_used),
                    bytes(nm.zfs_used + nm.zfs_avail)
                )),
            ]));
        }
        if nm.net_rx_bps + nm.net_tx_bps > 0 {
            lines.push(Line::from(vec![
                Span::styled("network ", dim()),
                Span::raw(format!(
                    "▼ {}  ▲ {}",
                    rate(nm.net_rx_bps),
                    rate(nm.net_tx_bps)
                )),
            ]));
        }
    }
    if o.kind == Kind::Service
        && let Some(edges) = model.connections.get(&o.metadata.namespace)
    {
        lines.push(Line::default());
        lines.push(section("connections seen"));
        lines.extend(connection_lines(&o.metadata.name, edges));
    }
    if o.kind == Kind::Volume
        && let Some((used, avail)) = model.volume_space(&o.metadata.namespace, &o.metadata.name)
    {
        lines.push(Line::from(vec![
            Span::styled("zfs dataset ", dim()),
            Span::raw(format!("{} used, {} free", bytes(used), bytes(avail))),
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

/// The connections `service` answered (←) and opened (→), open or closed
/// in about the last minute and a half, as `legion connections` names
/// their other ends.
fn connection_lines(service: &str, edges: &[pb::ConnectionEdge]) -> Vec<Line<'static>> {
    let is_it = |e: &Option<pb::ConnectionEnd>| {
        e.as_ref()
            .is_some_and(|e| e.kind == "service" && e.name == service)
    };
    let mut lines = Vec::new();
    for e in edges {
        let (arrow, other) = match (is_it(&e.from), is_it(&e.to)) {
            (_, true) => ("←", e.from.as_ref()),
            (true, false) => ("→", e.to.as_ref()),
            (false, false) => continue,
        };
        let via = match e.via.as_str() {
            "" => String::new(),
            via => format!(" via {via}"),
        };
        let n = match e.connections {
            1 => "1 connection".to_owned(),
            n => format!("{n} connections"),
        };
        lines.push(field(
            1,
            &format!("{arrow} {}", crate::cli::connection_end(other)),
            &format!("{}/{}{via}, {n}", e.port, e.protocol),
        ));
    }
    if lines.is_empty() {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("none in the last minute and a half", dim()),
        ]));
    }
    lines
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
            ("A", "follow the alerts below"),
            ("q Ctrl-C", "quit"),
        ],
    ),
    (
        "Acting on the selected row",
        &[
            ("d p a", "node: drain (asked), pause, activate"),
            ("L", "node: set or remove a label"),
            (
                "s r R o",
                "service: scale, roll back, restart, stop or start (asked)",
            ),
            ("P", "service: pause (asked) or resume"),
            ("u n", "cron job: suspend or resume, run now"),
            ("m", "volume: move to a node"),
            ("l e", "follow logs, or events, of the row below"),
            ("L", "service or Job: follow the logs of all its tasks"),
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
    (
        "Layout and telemetry",
        &[
            ("b / 0", "cycle preset: classic, bento, telemetry"),
            ("F1–F4 / ! @ # $", "toggle boxes in bento mode"),
            ("t", "expand workload → task → jail tree"),
            ("< > S", "sort column (cpu, mem, state, name) and direction"),
            ("+ -", "step live poll interval (1s, 2s, 5s, 10s)"),
            ("M", "toggle mouse click & wheel support"),
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
            ("R", "restart"),
            ("o", "stop/start"),
            ("P", "pause/resume"),
            ("u", "suspend"),
            ("n", "run now"),
            ("l", "logs"),
            ("e", "events"),
        ],
        Tab::Tasks => &[
            ("l", "logs"),
            ("e", "events"),
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
