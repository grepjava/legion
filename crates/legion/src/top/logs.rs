//! Logs followed in a pane under the table: the lines that came, cleaned
//! of what would upset the screen, and how far back the person looks.

use std::collections::VecDeque;

/// The most lines kept; older ones go.
const KEPT: usize = 5000;

pub(super) struct Logs {
    /// Whose: `shop/web-1-1`, with `/container` when one was chosen.
    pub(super) of: String,
    /// Each line, and whether it came on stderr.
    pub(super) lines: VecDeque<(bool, String)>,
    /// What came after the last newline, of stdout and of stderr.
    partial: [String; 2],
    /// Lines scrolled back from the newest; 0 follows them.
    pub(super) back: usize,
    /// Why no more come.
    pub(super) ended: Option<String>,
    /// Over the table too.
    pub(super) zoom: bool,
}

impl Logs {
    pub(super) fn new(of: String) -> Self {
        Logs {
            of,
            lines: VecDeque::new(),
            partial: Default::default(),
            back: 0,
            ended: None,
            zoom: false,
        }
    }

    /// What came on stdout or stderr.
    pub(super) fn push(&mut self, stderr: bool, data: &[u8]) {
        let partial = &mut self.partial[usize::from(stderr)];
        partial.push_str(&String::from_utf8_lossy(data));
        let Some(end) = partial.rfind('\n') else {
            return;
        };
        let done: String = partial.drain(..=end).collect();
        let lines: Vec<String> = done.lines().map(clean).collect();
        self.add(stderr, lines);
    }

    /// No more come, and why.
    pub(super) fn end(&mut self, why: String) {
        for stderr in [false, true] {
            let rest = std::mem::take(&mut self.partial[usize::from(stderr)]);
            if !rest.is_empty() {
                self.add(stderr, vec![clean(&rest)]);
            }
        }
        self.ended = Some(why);
    }

    fn add(&mut self, stderr: bool, lines: Vec<String>) {
        // Looking back, the same lines stay in view.
        if self.back > 0 {
            self.back += lines.len();
        }
        self.lines.extend(lines.into_iter().map(|l| (stderr, l)));
        while self.lines.len() > KEPT {
            self.lines.pop_front();
        }
        self.back = self.back.min(self.lines.len());
    }

    /// Back (or forward, when negative) by `n` lines; at the newest it
    /// follows again.
    pub(super) fn scroll(&mut self, n: isize) {
        self.back = self
            .back
            .saturating_add_signed(n)
            .min(self.lines.len().saturating_sub(1));
    }
}

/// A line as the screen can show it: tabs to spaces, without colour
/// codes and other control characters.
fn clean(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '\t' => {
                let n = 8 - out.chars().count() % 8;
                out.extend(std::iter::repeat_n(' ', n));
            }
            // ESC [ … a letter, or ESC and one more.
            '\u{1b}' => {
                if chars.next() == Some('[') {
                    for c in chars.by_ref() {
                        if c.is_ascii_alphabetic() || c == '~' {
                            break;
                        }
                    }
                }
            }
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}
