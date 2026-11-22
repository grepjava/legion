//! Drawing `legion top`, one panel at a time as ISPF shows them: the
//! title line with the panels opened, the cluster at a glance with graphs
//! of what its tasks use, the panel (the menu, a list, an object's
//! details, or logs, output or a shell), the message line of questions
//! and answers, and the function keys; over them, an object's YAML or
//! the keys' help. The colours and bars follow jmon's.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};

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
    ScrollbarState, Sparkline, Table, TableState, Wrap,
};

use super::logs::Logs;
use super::model::{Key, Model, kind_word, status, task_state};
use super::shell::Shell;
use super::view::{
    BAR, COMMAND, Focus, OPTIONS, PULLDOWN_EMPTY, Panel, Prompt, SortCol, Tab, TableRow, Then,
    View, actions, option_tab, rows,
};

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

/// Colours the terminal shows: 24 (any), 8 (256), 4 (16); 0 until
/// asked or read from the environment.
static DEPTH: AtomicU8 = AtomicU8::new(0);

/// Sets the colours shown, as `colors` on the command line does.
pub(super) fn set_depth(bits: u8) {
    DEPTH.store(bits, Ordering::Relaxed);
}

/// The colours shown: as set, else as `COLORTERM` and `TERM` say.
pub(super) fn depth() -> u8 {
    match DEPTH.load(Ordering::Relaxed) {
        0 => {
            let colorterm = std::env::var("COLORTERM").unwrap_or_default();
            let term = std::env::var("TERM").unwrap_or_default();
            let bits = if matches!(colorterm.as_str(), "truecolor" | "24bit")
                || term.ends_with("-direct")
            {
                24
            } else if term.contains("256color") {
                8
            } else {
                4
            };
            DEPTH.store(bits, Ordering::Relaxed);
            bits
        }
        bits => bits,
    }
}

/// The theme: each of the named colours as the terminal can show it,
/// and any colour brought to what it can.
pub(super) fn tone(color: Color) -> Color {
    let bits = depth();
    if bits == 4 {
        return match color {
            Color::Rgb(r, g, b) => {
                if r > 200 && g < 150 {
                    BAD
                } else if r > 200 {
                    WARN
                } else if g > b {
                    OK
                } else {
                    ACCENT
                }
            }
            Color::Indexed(236 | 237) => Color::DarkGray,
            other => other,
        };
    }
    let (rgb, index) = match color {
        Color::Cyan => ((86, 182, 247), 75),
        Color::Green => ((52, 211, 153), 79),
        Color::Yellow => ((251, 191, 36), 214),
        Color::Red => ((248, 113, 113), 203),
        Color::DarkGray => ((75, 85, 99), 240),
        Color::White => ((229, 231, 235), 254),
        Color::Black => ((17, 24, 39), 234),
        Color::Indexed(236) => ((31, 36, 46), 236),
        Color::Indexed(237) => ((45, 55, 72), 238),
        Color::Rgb(r, g, b) if bits == 8 => {
            let six = |c: u8| (u16::from(c) * 5 + 127) / 255;
            let at = 16 + 36 * six(r) + 6 * six(g) + six(b);
            return Color::Indexed(at as u8);
        }
        other => return other,
    };
    if bits == 24 {
        Color::Rgb(rgb.0, rgb.1, rgb.2)
    } else {
        Color::Indexed(index)
    }
}

/// Text in `color`, unless colours are off.
pub(super) fn fg(color: Color) -> Style {
    if mono() {
        Style::default()
    } else {
        Style::default().fg(tone(color))
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

/// A gradient of three colours, as btop's themes give each box.
pub(super) struct Palette {
    start: (u8, u8, u8),
    mid: (u8, u8, u8),
    end: (u8, u8, u8),
}

/// Busy: green, yellow, red, as btop's meters.
pub(super) const HEAT: Palette = Palette {
    start: (80, 200, 120),
    mid: (230, 200, 60),
    end: (240, 85, 85),
};
const CPU: Palette = HEAT;
const MEM: Palette = Palette {
    start: (86, 182, 247),
    mid: (167, 139, 250),
    end: (236, 72, 153),
};
const ZFS: Palette = Palette {
    start: (251, 191, 36),
    mid: (249, 115, 22),
    end: (239, 68, 68),
};
const NET: Palette = Palette {
    start: (45, 212, 191),
    mid: (96, 165, 250),
    end: (192, 132, 252),
};

impl Palette {
    /// The colour `frac` of the way along.
    pub(super) fn at(&self, frac: f64) -> Color {
        let f = frac.clamp(0.0, 1.0);
        let lerp = |a: u8, b: u8, t: f64| (a as f64 + (b as f64 - a as f64) * t).round() as u8;
        let mix = |a: (u8, u8, u8), b: (u8, u8, u8), t: f64| {
            Color::Rgb(lerp(a.0, b.0, t), lerp(a.1, b.1, t), lerp(a.2, b.2, t))
        };
        if f < 0.5 {
            mix(self.start, self.mid, f * 2.0)
        } else {
            mix(self.mid, self.end, (f - 0.5) * 2.0)
        }
    }

    /// The box's own colour: its frame and name.
    fn own(&self) -> Color {
        let (r, g, b) = self.start;
        Color::Rgb(r, g, b)
    }
}

/// How busy, coloured green to red; none without colours.
pub(super) fn heat_rgb(frac: f64) -> Color {
    if mono() {
        return Color::Reset;
    }
    HEAT.at(frac)
}

/// What a graph's top stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Scale {
    /// The highest point shown, not below 2 % of the whole: what moves
    /// (CPU, network) shows its shape even when small, and noise on an
    /// idle cluster stays flat.
    Peak,
    /// All there is: what is held (memory, ZFS space) shows how full,
    /// not a steady line drawn to the top.
    Whole,
}

impl Scale {
    /// The value the graph's top stands for.
    fn top(self, peak: f64, capacity: f64) -> f64 {
        match self {
            Scale::Whole if capacity > 0.0 => capacity,
            _ => peak.max(capacity * 0.02),
        }
        .max(f64::MIN_POSITIVE)
    }
}

/// Unicode `2×4` Braille area graph (`U+2800`..`U+28FF`) as in `btop`:
/// `2 * width` horizontal data points × `4 * height` vertical sub-dots.
pub(super) fn braille_lines(
    samples: &[f64],
    capacity: f64,
    width: usize,
    height: usize,
    palette: &Palette,
    scale: Scale,
) -> Vec<Line<'static>> {
    if width == 0 || height == 0 {
        return Vec::new();
    }
    const LEFT_DOTS: [u32; 4] = [0x40, 0x04, 0x02, 0x01];
    const RIGHT_DOTS: [u32; 4] = [0x80, 0x20, 0x10, 0x08];
    let pts_needed = width * 2;
    let tail = &samples[samples.len().saturating_sub(pts_needed)..];
    let peak = tail.iter().copied().fold(0.0, f64::max);
    let scale = scale.top(peak, capacity);
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
        // Shaded by height, as btop's: the box's first colour at the
        // bottom, its last at the top.
        let shade = fg(palette.at((height - row) as f64 / height as f64 - 0.5 / height as f64));
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
            let style = if mask == 0 { dim() } else { shade };
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

/// A bar `width` cells wide, filled to `frac`: in its busy colour on 16
/// colours, else each cell shaded along the heat gradient, as btop's.
pub(super) fn meter(frac: f64, width: usize) -> Vec<Span<'static>> {
    let filled = ((frac.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    let filled = if frac > 0.0 { filled.max(1) } else { 0 };
    let mut spans = if depth() == 4 || mono() {
        vec![Span::styled("█".repeat(filled), fg(busy(frac)))]
    } else {
        (0..filled)
            .map(|i| Span::styled("█", fg(heat_rgb((i as f64 + 0.5) / width as f64))))
            .collect()
    };
    spans.push(Span::styled("░".repeat(width - filled), dim()));
    spans
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
        .border_style(frame())
        .title(title)
}

/// Frames: slate blue, or grey on 16 colours.
fn frame() -> Style {
    if depth() == 4 {
        fg(Color::DarkGray)
    } else {
        fg(Color::Rgb(70, 92, 132))
    }
}

fn title(s: impl Into<String>) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {} ", s.into()),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    ))
}

/// Where each part of the screen goes.
struct Areas {
    /// The action bar, the panels opened, how lists sort and graphs draw.
    bar: Rect,
    /// The command line.
    command: Rect,
    /// The cluster at a glance: none over the pane or the graphs, more
    /// on the menu.
    top: Rect,
    /// The panel shown.
    body: Rect,
    /// A question, what came of a request, or what letters do.
    message: Rect,
    /// The function keys.
    keys: Rect,
}

/// The cluster at a glance as the menu shows it: the four graphs, room
/// allowing.
fn deck(view: &View, area: Rect) -> bool {
    *view.panel() == Panel::Menu && area.height >= 28
}

fn areas(view: &View, area: Rect) -> Areas {
    let top = match view.panel() {
        Panel::Pane | Panel::Graphs => 0,
        _ if deck(view, area) => (area.height / 3).max(7),
        _ if area.height >= 18 => 4,
        _ => 0,
    };
    let [bar, command, top, body, message, keys] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(top),
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(area);
    Areas {
        bar,
        command,
        top,
        body,
        message,
        keys,
    }
}

/// Where the panel shown is drawn on a terminal of `area`.
pub(super) fn body(view: &View, area: Rect) -> Rect {
    areas(view, area).body
}

/// The rows and columns of the shell's screen on a terminal of `area`.
pub(super) fn shell_size(view: &View, area: Rect) -> Option<(u16, u16)> {
    if area.width < 60 || area.height < 12 || *view.panel() != Panel::Pane {
        return None;
    }
    let inner = panel(Line::default()).inner(body(view, area));
    Some((inner.height.max(1), inner.width.max(1)))
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
    let a = areas(view, area);
    action_bar(f, model, view, a.bar);
    command_line(f, model, view, a.command);
    if deck(view, area) {
        telemetry_deck(f, model, view, a.top);
    } else if a.top.height > 0 {
        header(f, model, view, a.top);
    }
    match view.panel() {
        Panel::Menu => menu(f, model, view, a.body),
        Panel::Graphs => graphs(f, model, view, a.body),
        Panel::List { .. } => draw_table(f, model, view, rows(model, view), a.body),
        Panel::Detail { key, scroll } => detail(f, model, key, *scroll, a.body),
        Panel::Pane => {
            if let Some(l) = &view.logs {
                logs(f, l, a.body);
            }
            if let Some(s) = &view.shell {
                shell(f, s, a.body);
            }
        }
    }
    f.render_widget(Paragraph::new(bottom_line(model, view)), a.message);
    f.render_widget(Paragraph::new(function_keys(view)), a.keys);
    if let Some((menu, at)) = view.bar {
        pulldown(f, model, view, menu, at, area);
    }
    if let Some((key, scroll)) = &view.yaml {
        yaml(f, model, key, *scroll);
    }
    if view.help {
        help(f, view);
    }
}

/// What a panel is called on the title line.
fn panel_name(model: &Model, view: &View, panel: &Panel) -> String {
    match panel {
        Panel::Menu => "Menu".to_owned(),
        Panel::Graphs => "Graphs".to_owned(),
        Panel::List { tab, focus } => match focus {
            Some(Focus::Node(n)) => format!("{} on node {n}", tab.title()),
            Some(Focus::Owner(kind, ns, name)) => {
                format!("{} of {} {ns}/{name}", tab.title(), kind_word(*kind))
            }
            None => tab.title().to_owned(),
        },
        Panel::Detail { key, .. } => match model.get(key) {
            Some(o) => super::model::name(o),
            None => format!("{} {} (gone)", kind_word(key.0), key.2),
        },
        Panel::Pane => match (&view.logs, &view.shell) {
            (Some(l), _) if l.titled => l.of.clone(),
            (Some(l), _) => format!("Logs of {}", l.of),
            (_, Some(s)) => format!("Shell in {}", s.of),
            _ => "Output".to_owned(),
        },
    }
}

/// The bars' background.
fn bar_style() -> Style {
    if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default().bg(tone(Color::Indexed(236)))
    }
}

/// The first line: the action bar's menus (F10), the panels opened as a
/// trail back to the menu, and how lists sort and graphs draw.
fn action_bar(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let mut left = vec![Span::raw(" ")];
    for (m, name) in BAR.iter().enumerate() {
        let open = view.bar.is_some_and(|(o, _)| o == m);
        left.push(if open {
            Span::styled(
                format!(" {name} "),
                if mono() {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                        .bg(tone(ACCENT))
                        .fg(tone(Color::Black))
                        .add_modifier(Modifier::BOLD)
                },
            )
        } else {
            Span::raw(format!(" {name} "))
        });
    }
    left.push(Span::styled(" │ ", dim()));
    let last = view.stack.len() - 1;
    for (i, panel) in view.stack.iter().enumerate() {
        if i > 0 {
            left.push(Span::styled(" › ", dim()));
        }
        let name = panel_name(model, view, panel);
        left.push(if i == last {
            Span::styled(name, fg(ACCENT).add_modifier(Modifier::BOLD))
        } else {
            Span::styled(name, dim())
        });
    }
    let mut right = Vec::new();
    if view.sort != SortCol::Default {
        let arrow = if view.sort_desc { "↓" } else { "↑" };
        right.push(Span::styled(
            format!("sort {}{arrow} ", view.sort.label()),
            fg(ACCENT),
        ));
    }
    if view.tree {
        right.push(Span::styled("tree ", fg(OK)));
    }
    if view.history {
        right.push(Span::styled("ended too ", fg(WARN)));
    }
    right.push(Span::styled(
        format!(
            "graphs {} · poll {}s ",
            view.range_name(),
            trim(view.poll_ms as f64 / 1000.0)
        ),
        dim(),
    ));
    right.push(Span::styled(
        " LEGION ",
        if mono() {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .bg(tone(ACCENT))
                .fg(tone(Color::Black))
                .add_modifier(Modifier::BOLD)
        },
    ));
    let right = Line::from(right);
    let [l, r] = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(right.width() as u16),
    ])
    .areas(area);
    f.render_widget(Paragraph::new(Line::from(left)).style(bar_style()), l);
    f.render_widget(Paragraph::new(right).style(bar_style()), r);
}

/// The second line: the command line, as ISPF's, and where the panel is
/// scrolled to.
fn command_line(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let mut left = vec![Span::styled(
        format!(" {COMMAND} "),
        fg(ACCENT).add_modifier(Modifier::BOLD),
    )];
    match &view.prompt {
        Some(Prompt::Input {
            input,
            then: Then::Command,
            ..
        }) => {
            left.push(Span::raw(input.clone()));
            left.push(Span::styled("▏", fg(ACCENT)));
            left.push(Span::styled("  Enter · ↑↓ recall · Esc cancels", dim()));
        }
        _ => left.push(Span::styled(
            ": or = types here: =N, find, sort, range, back, or a legion command",
            dim(),
        )),
    }
    let mut right = Vec::new();
    if !view.marked.is_empty() {
        right.push(Span::styled(
            format!("{} marked ", view.marked.len()),
            fg(WARN).add_modifier(Modifier::BOLD),
        ));
    }
    match view.panel() {
        Panel::List { .. } => {
            let count = rows(model, view).len();
            let at = view.selected[view.index()].min(count.saturating_sub(1));
            right.push(Span::styled(
                match count {
                    0 => "No rows ".to_owned(),
                    n => format!("Row {} of {n} ", at + 1),
                },
                dim(),
            ));
        }
        Panel::Detail { scroll, .. } => {
            right.push(Span::styled(format!("Line {} ", scroll + 1), dim()));
        }
        _ => {}
    }
    let right = Line::from(right);
    let [l, r] = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(right.width() as u16),
    ])
    .areas(area);
    f.render_widget(Paragraph::new(Line::from(left)), l);
    f.render_widget(Paragraph::new(right), r);
}

/// An action bar menu pulled down under its name.
fn pulldown(f: &mut Frame, model: &Model, view: &View, menu: usize, at: usize, area: Rect) {
    let drop = view.pulldown_area(model, menu, area);
    let items = &view.menus(model)[menu];
    let selected = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(tone(Color::Indexed(237)))
            .add_modifier(Modifier::BOLD)
    };
    let lines: Vec<Line> = if items.is_empty() {
        vec![dimmed(format!(" {PULLDOWN_EMPTY}"))]
    } else {
        items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let line = Line::from(vec![
                    Span::styled(
                        format!(" {:<6} ", item.key),
                        fg(ACCENT).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(item.label.clone()),
                ]);
                if i == at { line.style(selected) } else { line }
            })
            .collect()
    };
    f.render_widget(Clear, drop);
    f.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(fg(ACCENT)),
        ),
        drop,
    );
}

/// What each list holds, as the menu says it.
fn about(tab: Tab) -> &'static str {
    match tab {
        Tab::Nodes => "machines: their load, pools and tasks",
        Tab::Workloads => "Services, Jobs, cron jobs and builds",
        Tab::Tasks => "every task, where it runs and what it uses",
        Tab::Storage => "Volumes: their space, node and replica",
        Tab::Network => "Routes and network policies",
        Tab::Config => "namespaces, Secrets, configs, roles, policies, alerts",
        Tab::Log => "the changes heard, as they happen",
        Tab::Activity => "operations running and done",
    }
}

/// The menu: the graphs and the lists, by number, as ISPF's primary
/// option menu.
fn menu(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let selected = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(tone(Color::Indexed(237)))
            .add_modifier(Modifier::BOLD)
    };
    let mut lines: Vec<Line> = (0..OPTIONS)
        .map(|i| {
            let (title, what, count) = match option_tab(i) {
                Some(tab) => {
                    let count = super::view::tab_count(model, tab);
                    let count = (tab != Tab::Log && count > 0).then_some(count);
                    (tab.title(), about(tab), count)
                }
                None => ("Graphs", "CPU, memory, ZFS and network, large", None),
            };
            let mut spans = vec![
                if i == view.option {
                    Span::styled("▌", fg(ACCENT))
                } else {
                    Span::raw(" ")
                },
                Span::styled(format!(" {i} "), fg(ACCENT).add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!(" {title:<11}"),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("{what:<56}"), dim()),
            ];
            if let Some(count) = count {
                spans.push(Span::raw(format!("{count:>5}")));
            }
            let line = Line::from(spans);
            if i == view.option {
                line.style(selected)
            } else {
                line
            }
        })
        .collect();
    lines.push(Line::default());
    lines.push(dimmed(
        "  Enter or a number opens an option; Enter on a row opens it, F3 goes back.",
    ));
    lines.push(dimmed(
        "  0–8 go to an option from any panel; F10 opens the action bar; : or = the command line.",
    ));
    f.render_widget(Paragraph::new(lines).block(panel(title("Options"))), area);
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
        " PgUp PgDn scroll · F3 back (the shell ends) "
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
            Line::from(Span::styled(" PgUp PgDn scroll · F3 back ", dim())).right_aligned(),
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
fn header(f: &mut Frame, model: &Model, view: &View, area: Rect) {
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
    let (totals, _) = model.series(view.range_ms());
    let whole = view.range_ms() != 0;
    let cpu: Vec<f64> = totals.iter().map(|(c, _)| c / 100.0).collect();
    let mem: Vec<f64> = totals.iter().map(|(_, m)| *m as f64).collect();
    let cores = |c: f64| format!("{c:.1}");
    let cpu_total = format!(" CPU of {}", trim(millicores as f64 / 1000.0));
    let mem_total = format!(" of {}", bytes(memory));
    let mib = |m: f64| bytes(m as u64);
    let (cpu_name, mem_name) = match whole {
        true => (
            format!("cpu {}", view.range_name()),
            format!("mem {}", view.range_name()),
        ),
        false => ("cpu".to_owned(), "mem".to_owned()),
    };
    graph(
        f,
        rows[0],
        &cpu_name,
        &cpu,
        millicores as f64 / 1000.0,
        &cores,
        &cpu_total,
        OK,
        whole,
        Scale::Peak,
    );
    graph(
        f,
        rows[1],
        &mem_name,
        &mem,
        memory as f64,
        &mib,
        &mem_total,
        ACCENT,
        whole,
        Scale::Whole,
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
    whole: bool,
    scale: Scale,
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
    let fitted = fit(samples, spark.width as usize, whole);
    let shown = &fitted[fitted.len().saturating_sub(spark.width as usize)..];
    let peak = shown.iter().copied().fold(0.0, f64::max);
    let scale = scale.top(peak, capacity);
    let data: Vec<u64> = shown
        .iter()
        // Any use at all shows, if only as the lowest bar.
        .map(|v| match (v / scale * 64.0).round() as u64 {
            // An eighth of the height: the lowest bar drawn.
            n if n < 8 && *v > 0.0 => 8,
            n => n,
        })
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

/// A graph of the cluster: its name, samples, what they are out of, and
/// what it reads now.
struct Card {
    palette: Palette,
    scale: Scale,
    /// Whether what it is out of is real, for a share of it: not the
    /// network's, whose links' speeds are not known.
    share: bool,
    name: String,
    samples: Vec<f64>,
    capacity: f64,
    readout: String,
}

/// CPU, memory, ZFS and network, over the range shown.
fn cards(model: &Model, view: &View) -> [Card; 4] {
    let (millicores, memory) = model.capacity();
    let (totals, telemetry) = model.series(view.range_ms());
    let named = |n: &str| match view.range_ms() != 0 {
        true => format!("{n} {}", view.range_name()),
        false => n.to_owned(),
    };
    let cpu: Vec<f64> = totals.iter().map(|(c, _)| c / 100.0).collect();
    let mem: Vec<f64> = totals.iter().map(|(_, m)| *m as f64).collect();
    let (zfs_used, zfs_avail) = model.zfs_totals();
    let zfs: Vec<f64> = telemetry.iter().map(|(u, _, _, _)| *u as f64).collect();
    let (net_rx, net_tx) = model.net_totals();
    let net: Vec<f64> = telemetry
        .iter()
        .map(|(_, _, rx, tx)| (*rx + *tx) as f64)
        .collect();
    // The links' speeds are not known: the graph's top is the busiest
    // seen, or 1 MiB/s, so a trickle stays a trickle.
    let net_peak = net
        .iter()
        .copied()
        .fold(0.0, f64::max)
        .max((1 << 20) as f64);
    let cpu_cap = millicores as f64 / 1000.0;
    let cpu_now = cpu.last().copied().unwrap_or(0.0);
    let mem_now = mem.last().copied().unwrap_or(0.0) as u64;
    [
        Card {
            palette: CPU,
            scale: Scale::Peak,
            share: true,
            name: named("cpu"),
            readout: format!(
                "{cpu_now:.1}/{} cores",
                if cpu_cap > 0.0 {
                    trim(cpu_cap)
                } else {
                    "-".into()
                }
            ),
            samples: cpu,
            capacity: cpu_cap,
        },
        Card {
            palette: MEM,
            scale: Scale::Whole,
            share: true,
            name: named("mem"),
            readout: format!("{}/{}", bytes(mem_now), bytes(memory)),
            samples: mem,
            capacity: memory as f64,
        },
        Card {
            palette: ZFS,
            scale: Scale::Whole,
            share: true,
            name: named("zfs"),
            readout: if zfs_used + zfs_avail > 0 {
                format!("{}/{}", bytes(zfs_used), bytes(zfs_used + zfs_avail))
            } else {
                "no zfs telemetry".into()
            },
            samples: zfs,
            capacity: (zfs_used + zfs_avail) as f64,
        },
        Card {
            palette: NET,
            scale: Scale::Whole,
            share: false,
            name: named("net"),
            readout: format!("▼{} ▲{}", rate(net_rx), rate(net_tx)),
            samples: net,
            capacity: net_peak,
        },
    ]
}

/// The cluster at a glance on the menu: CPU, memory, ZFS and network,
/// each a braille graph.
fn telemetry_deck(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let mut top = vec![Span::styled(
        " ◆ legion ",
        fg(ACCENT).add_modifier(Modifier::BOLD),
    )];
    if let Some(ns) = &model.namespace {
        top.push(Span::styled(format!("ns:{ns} "), fg(WARN)));
    }
    for s in summary(model).into_iter().take(1) {
        top.extend(s.spans);
    }
    let mut right = Vec::new();
    if let Some(problem) = model.problem.as_ref().or(model.polled.as_ref()) {
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
    let whole = view.range_ms() != 0;
    let cols = Layout::horizontal([Constraint::Ratio(1, 4); 4]).split(inner);
    for (card, at) in cards(model, view).iter().zip(cols.iter()) {
        braille_card(f, *at, card, whole);
    }
}

/// The graphs panel (`0`): the four graphs as large as the screen.
fn graphs(f: &mut Frame, model: &Model, view: &View, area: Rect) {
    let whole = view.range_ms() != 0;
    let cards = cards(model, view);
    let places: Vec<Rect> = if area.height >= 12 {
        let [upper, lower] = Layout::vertical([Constraint::Ratio(1, 2); 2]).areas(area);
        let [a, b] = Layout::horizontal([Constraint::Ratio(1, 2); 2]).areas(upper);
        let [c, d] = Layout::horizontal([Constraint::Ratio(1, 2); 2]).areas(lower);
        vec![a, b, c, d]
    } else {
        Layout::horizontal([Constraint::Ratio(1, 4); 4])
            .split(area)
            .to_vec()
    };
    for (card, at) in cards.iter().zip(places) {
        braille_card(f, at, card, whole);
    }
}

/// The samples for a graph `points` wide: as they are, the latest
/// shown; or, `whole`, the range in that space, each point the highest of
/// the samples it stands for, so a peak is not lost.
pub(super) fn fit(samples: &[f64], points: usize, whole: bool) -> Vec<f64> {
    if !whole || points == 0 || samples.len() <= points {
        return samples.to_vec();
    }
    (0..points)
        .map(|i| {
            let (from, to) = (i * samples.len() / points, (i + 1) * samples.len() / points);
            samples[from..to.max(from + 1)]
                .iter()
                .copied()
                .fold(0.0, f64::max)
        })
        .collect()
}

fn braille_card(f: &mut Frame, area: Rect, card: &Card, whole: bool) {
    let Card {
        palette,
        scale,
        share,
        name,
        samples,
        capacity,
        readout,
    } = card;
    let capacity = *capacity;
    if area.width < 8 || area.height < 2 {
        return;
    }
    let now = samples.last().copied().unwrap_or(0.0);
    let frac = if capacity > 0.0 {
        (now / capacity).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let pct = if *share && capacity > 0.0 {
        format!("{:>3.0}%", frac * 100.0)
    } else {
        String::new()
    };
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(fg(palette.own()))
        .title(Line::from(vec![
            Span::styled(
                format!(" {name} "),
                fg(palette.own()).add_modifier(Modifier::BOLD),
            ),
            Span::styled(pct, fg(heat_rgb(frac)).add_modifier(Modifier::BOLD)),
            Span::raw(" "),
        ]))
        .title(Line::from(Span::styled(format!(" {readout} "), dim())).right_aligned());
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let samples = fit(samples, inner.width as usize * 2, whole);
    let lines = braille_lines(
        &samples,
        capacity,
        inner.width as usize,
        inner.height as usize,
        palette,
        *scale,
    );
    f.render_widget(Paragraph::new(lines), inner);
}

/// Multi-pane `btop` dashboard layout: `Nodes`, `Workloads`, and `Tasks`/`Storage`
/// (or whichever tab is currently selected) side by side.
/// The list shown: its rows, the selected one marked.
fn draw_table(f: &mut Frame, model: &Model, view: &View, rows: Vec<TableRow>, area: Rect) {
    let tab = view.tab;
    let tab_idx = view.index();
    let mut heading = vec![Span::styled(
        format!(" {} ", tab.title()),
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
    let block = panel(Line::from(heading));

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
    // A column for the marks first, while rows are marked.
    let marking = !view.marked.is_empty();
    let titles: Vec<&str> = marking
        .then_some("")
        .into_iter()
        .chain(tab.header().iter().copied())
        .collect();
    let rows: Vec<TableRow> = rows
        .into_iter()
        .map(|mut r| {
            if !marking {
                return r;
            }
            let marked = r.key.as_ref().is_some_and(|k| view.marked.contains(k));
            r.cells.insert(
                0,
                if marked {
                    Line::from(Span::styled("✓", fg(WARN).add_modifier(Modifier::BOLD)))
                } else {
                    Line::from(" ")
                },
            );
            r
        })
        .collect();
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
    let highlight = if mono() {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
            .bg(tone(Color::Indexed(237)))
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
fn detail(f: &mut Frame, model: &Model, key: &Key, scroll: u16, area: Rect) {
    let Some(o) = model.get(key) else {
        f.render_widget(
            Paragraph::new(dimmed("It is gone. F3 goes back.")).block(panel(title("Details"))),
            area,
        );
        return;
    };
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
        if nm.zfs_read_bps + nm.zfs_write_bps > 0 {
            lines.push(Line::from(vec![
                Span::styled("pool i/o ", dim()),
                Span::raw(format!(
                    "read {}  written {}",
                    rate(nm.zfs_read_bps),
                    rate(nm.zfs_write_bps)
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
    if o.kind == Kind::Node {
        lines.extend(node_lines(
            o,
            model.node_metrics(&o.metadata.name),
            legion_api::clock::now() * 1000,
        ));
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
    // A node's host and pools are said above.
    let mut node_status = o.status.clone();
    if o.kind == Kind::Node
        && let Some(m) = node_status.as_object_mut()
    {
        m.remove("host");
        m.remove("pools");
    }
    for (name, value) in [("status", &node_status), ("spec", &o.spec)] {
        if value.is_null() || value.as_object().is_some_and(|m| m.is_empty()) {
            continue;
        }
        lines.push(Line::default());
        lines.push(section(name));
        flatten(value, 1, &mut lines);
    }
    let hint = match o.kind {
        Kind::Node | Kind::Service | Kind::Job => " Enter its tasks · y the whole YAML ",
        _ => " y the whole YAML ",
    };
    let block = panel(title(super::model::name(o)))
        .title_bottom(Line::from(Span::styled(hint, dim())).right_aligned());
    // Not past the last line, each as many as it wraps to.
    let inner = block.inner(area);
    let width = usize::from(inner.width.max(1));
    let wrapped: usize = lines.iter().map(|l| l.width().div_ceil(width).max(1)).sum();
    let most = wrapped.saturating_sub(usize::from(inner.height)) as u16;
    f.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((scroll.min(most), 0))
            .block(block),
        area,
    );
}

/// A node's load, its host and its pools, as `legion node pools` says
/// them, for its details.
pub(super) fn node_lines(
    o: &legion_api::Object,
    metrics: Option<super::model::NodeMetrics>,
    now_ms: i64,
) -> Vec<Line<'static>> {
    let st: NodeStatus = status(o);
    let spec: legion_api::node::NodeSpec = super::model::spec(o);
    let mut lines = Vec::new();
    if let Some(load) = metrics.map(|m| m.load).filter(|l| *l != [0; 3]) {
        let [a, b, c] = load.map(|l| f64::from(l) / 100.0);
        lines.push(Line::from(vec![
            Span::styled("load ", dim()),
            Span::raw(format!("{a:.2} {b:.2} {c:.2}")),
            Span::styled(" (1, 5, 15 min)", dim()),
        ]));
    }
    if let Some(h) = &st.host {
        lines.push(Line::default());
        lines.push(section("host"));
        let mut release = h.release.clone();
        if !h.kernel.is_empty() && h.kernel != h.release {
            release = format!("{release} (kernel {})", h.kernel);
        }
        lines.push(field(1, "release", &release));
        if !h.boot.is_empty() {
            lines.push(field(1, "boot", &h.boot));
        }
        if h.booted_ms > 0 {
            let up = std::time::Duration::from_millis((now_ms - h.booted_ms).max(0) as u64);
            lines.push(field(1, "up", &legion_api::clock::span(up)));
        }
    }
    if !st.pools.is_empty() {
        lines.push(Line::default());
        lines.push(section("pools"));
        for p in &st.pools {
            let [_, name, state, used, errors, scrubbed] =
                crate::cli::pool_row(&o.metadata.name, p, now_ms);
            let seen = spec.pool_errors_seen.get(&p.name).copied().unwrap_or(0);
            let errors = match (p.errors(), seen) {
                (0, _) => errors,
                (n, seen) if n <= seen => format!("{errors} (taken as seen)"),
                _ => errors,
            };
            let style = if !p.healthy() {
                fg(BAD)
            } else if p.errors() > seen {
                fg(WARN)
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    format!("{name} {state}"),
                    style.add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(" {used}")),
            ]));
            lines.push(field(2, "errors", &errors));
            lines.push(field(2, "scrubbed", &scrubbed));
        }
        let every = spec
            .scrub_every_days
            .map_or("none scheduled".to_owned(), |d| format!("every {d} days"));
        lines.push(field(1, "scrubs", &every));
    }
    lines
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
        "Panels",
        &[
            ("Enter", "open: a list from the menu, a row's details"),
            ("", "and a node's or workload's tasks from its details"),
            ("F3 Esc F12", "back one panel; F3 on the menu quits"),
            ("0–8", "go to the graphs or a list from any panel"),
            ("F10", "the action bar: ←→ menus, ↑↓ items, Enter does one"),
            ("↑↓ j k PgUp PgDn g G", "select, or scroll"),
            ("/", "filter the list; Esc clears"),
            ("y", "the YAML of the row or object"),
            ("h", "ended tasks too"),
            ("A", "follow the alerts"),
            ("F1 ?", "these keys"),
            ("q Ctrl-C", "quit"),
        ],
    ),
    (
        "Acting on the row selected, or the object shown",
        &[
            (
                "Space",
                "mark the row; a letter then acts on all marked (asked once)",
            ),
            ("d p a", "node: drain (asked), pause, activate"),
            ("L", "node: set or remove a label"),
            (
                "c X",
                "node: scrub its pools every N days; take its pool errors as seen (asked)",
            ),
            (
                "s r R o",
                "service: scale, roll back, restart, stop or start (asked)",
            ),
            ("P", "service: pause (asked) or resume"),
            ("u n", "cron job: suspend or resume, run now"),
            ("m", "volume: move to a node"),
            ("l e", "follow logs, or events, of the row below"),
            ("L", "service or Job: follow the logs of all its tasks"),
            ("x", "a shell in a task; Ctrl-] leaves it and returns"),
            ("D", "delete (type the name; a task: asked)"),
            ("E", "edit in $EDITOR, then apply over the version read"),
            ("I", "node: its images"),
            ("N B", "volume: its snapshots; back it up to a file"),
            ("p", "volume: promote its replica (type the name)"),
            ("f", "task: its files (more with :files TASK PATH)"),
            ("l", "build: follow its log"),
        ],
    ),
    (
        "The cluster, on a panel of its own",
        &[
            (
                ": =",
                "the command line: a legion command (join-token worker); ↑↓ recall",
            ),
            (
                "",
                "or top's: =N, =x, find TEXT, sort cpu|mem|state|name [asc|desc],",
            ),
            (
                "",
                "range 1h…30d, poll 2s, tree, ended, back, mark all, unmark,",
            ),
            ("", "colors 24bit|256|16"),
            ("H", "diagnose every node"),
            ("W C", "the namespace's reach, and connections seen"),
            ("T", "the namespace's topology"),
            ("U K", "the audit log (a day), and what tasks use"),
        ],
    ),
    (
        "While logs, output or a shell show",
        &[
            ("PgUp PgDn ↑↓", "scroll back"),
            ("F3 Esc", "back (a shell ends)"),
        ],
    ),
    (
        "Lists and graphs",
        &[
            ("t", "expand workload → task → jail tree"),
            ("< > S", "sort column (cpu, mem, state, name) and direction"),
            ("+ -", "step live poll interval (1s, 2s, 5s, 10s)"),
            ("w", "graphs over live, 1h, 6h, 24h, 7d or 30d"),
            ("M", "toggle mouse click & wheel support"),
        ],
    ),
];

fn help(f: &mut Frame, view: &View) {
    let block = |heading: &str, keys: &[(&str, &str)]| {
        let mut lines = vec![section(heading)];
        for (k, what) in keys {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {k:<22}"),
                    fg(ACCENT).add_modifier(Modifier::BOLD),
                ),
                Span::raw((*what).to_owned()),
            ]));
        }
        lines.push(Line::default());
        lines
    };
    let blocks: Vec<Vec<Line>> = HELP.iter().map(|(h, k)| block(h, k)).collect();
    let footer = dimmed(format!(
        "On {} now. What you may do follows your role bindings.",
        match view.panel() {
            Panel::Menu => "the menu",
            Panel::Graphs => "the graphs",
            Panel::List { .. } => view.tab.title(),
            Panel::Detail { .. } => "an object's details",
            Panel::Pane => "logs or output",
        }
    ));
    let total: usize = blocks.iter().map(Vec::len).sum::<usize>() + 1;
    let screen = f.area();
    // Too tall for the screen but wide enough: two columns, the sections
    // split where their lines are about halved.
    let columns = if total + 2 > usize::from(screen.height) && screen.width >= 110 {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for b in blocks {
            if left.len() + b.len() / 2 < total / 2 {
                left.extend(b);
            } else {
                right.extend(b);
            }
        }
        right.push(footer);
        vec![left, right]
    } else {
        let mut lines: Vec<Line> = blocks.into_iter().flatten().collect();
        lines.push(footer);
        vec![lines]
    };
    let tallest = columns.iter().map(Vec::len).max().unwrap_or(0);
    let width = if columns.len() > 1 { 96 } else { 70 };
    let [area] = Layout::vertical([Constraint::Length(tallest as u16 + 2)])
        .flex(Flex::Center)
        .areas(centered(screen, width, 100));
    f.render_widget(Clear, area);
    let outer = panel(title("Keys"))
        .title_bottom(Line::from(Span::styled(" any key closes ", dim())).right_aligned());
    let inner = outer.inner(area);
    f.render_widget(outer, area);
    let parts = Layout::horizontal(vec![Constraint::Fill(1); columns.len()]).split(inner);
    for (lines, part) in columns.into_iter().zip(parts.iter()) {
        f.render_widget(Paragraph::new(lines), *part);
    }
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
fn bottom_line(model: &Model, view: &View) -> Line<'static> {
    let ask = |s: String| Line::from(Span::styled(s, fg(WARN).add_modifier(Modifier::BOLD)));
    match &view.prompt {
        Some(Prompt::Input {
            then: Then::Command,
            ..
        }) => {}
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
    // What letters do to the row selected, or the object shown.
    let Some(key) = view.selected_key(model) else {
        return Line::default();
    };
    let mut spans = Vec::new();
    if !view.marked.is_empty() {
        spans.push(Span::styled(
            format!(
                " {} marked: a letter acts on all, Esc clears ",
                view.marked.len()
            ),
            fg(WARN).add_modifier(Modifier::BOLD),
        ));
    }
    for (k, what) in actions(key.0).iter().take(6) {
        spans.push(Span::styled(
            format!(" {k} "),
            if mono() {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
                    .bg(tone(Color::DarkGray))
                    .fg(tone(Color::White))
            },
        ));
        spans.push(Span::styled(format!(" {} ", what.to_lowercase()), dim()));
    }
    Line::from(spans)
}

/// The function keys, as ISPF's last line: what moves between panels.
fn function_keys(view: &View) -> Line<'static> {
    let mut keys: Vec<(&str, &str)> = Vec::new();
    match view.panel() {
        Panel::Menu => keys.extend([("Enter", "Open"), ("0-8", "Go"), (":", "Command")]),
        Panel::Graphs => keys.extend([("w", "Range"), ("F3", "Back"), ("0-8", "Go")]),
        Panel::List { .. } => keys.extend([
            ("Enter", "Open"),
            ("Space", "Mark"),
            ("F3", "Back"),
            ("/", "Filter"),
        ]),
        Panel::Detail { key, .. } => {
            if matches!(key.0, Kind::Node | Kind::Service | Kind::Job) {
                keys.push(("Enter", "Tasks"));
            }
            keys.extend([("↑↓", "Scroll"), ("y", "YAML"), ("F3", "Back")]);
        }
        Panel::Pane => {
            if view
                .shell
                .as_ref()
                .is_some_and(|s| s.ended.is_none() && !s.focus)
            {
                keys.push(("Ctrl-]", "Type"));
            }
            keys.extend([("PgUp PgDn", "Scroll"), ("F3", "Back")]);
        }
    }
    keys.push(("F10", "Actions"));
    keys.push(("F1", "Help"));
    keys.push(if view.stack.len() == 1 {
        ("F3", "Exit")
    } else {
        ("q", "Quit")
    });
    let mut spans = Vec::new();
    for (k, what) in keys {
        spans.push(Span::styled(
            format!(" {k}"),
            fg(ACCENT).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(format!("={what} "), dim()));
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
