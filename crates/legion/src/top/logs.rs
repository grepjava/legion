//! Logs followed in a pane under the table: the lines that came, cleaned
//! of what would upset the screen, and how far back the person looks.

use std::collections::VecDeque;

/// The most lines kept; older ones go.
const KEPT: usize = 5000;
/// The longest line kept whole: output without newlines comes as lines of
/// this many bytes rather than growing without end.
const LONGEST: usize = 64 * 1024;

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
}

impl Logs {
    pub(super) fn new(of: String) -> Self {
        Logs {
            of,
            lines: VecDeque::new(),
            partial: Default::default(),
            back: 0,
            ended: None,
        }
    }

    /// What came on stdout or stderr.
    pub(super) fn push(&mut self, stderr: bool, data: &[u8]) {
        let partial = &mut self.partial[usize::from(stderr)];
        partial.push_str(&String::from_utf8_lossy(data));
        let mut lines = Vec::new();
        if let Some(end) = partial.rfind('\n') {
            let done: String = partial.drain(..=end).collect();
            lines.extend(done.lines().map(clean));
        }
        while partial.len() > LONGEST {
            let mut cut = LONGEST;
            while !partial.is_char_boundary(cut) {
                cut -= 1;
            }
            let head: String = partial.drain(..cut).collect();
            lines.push(clean(&head));
        }
        if !lines.is_empty() {
            self.add(stderr, lines);
        }
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
        self.back = self.back.min(self.lines.len().saturating_sub(1));
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
            // ESC [ … its final byte (`@` to `~`, as ECMA-48 has it); ESC ]
            // (a title, a link) and the other strings to BEL or ESC \; or
            // ESC and one more.
            '\u{1b}' => match chars.next() {
                Some('[') => {
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
                Some(']' | 'P' | 'X' | '^' | '_') => {
                    while let Some(c) = chars.next() {
                        if c == '\u{7}' || (c == '\u{1b}' && chars.next().is_some()) {
                            break;
                        }
                    }
                }
                _ => {}
            },
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_end_at_their_final_byte() {
        assert_eq!(clean("\u{1b}[31mred\u{1b}[0m"), "red");
        // Final bytes that are not letters: `@` (insert), `` ` ``, `{`, `|`.
        for seq in [
            "\u{1b}[2@",
            "\u{1b}[5`",
            "\u{1b}[1{",
            "\u{1b}[3|",
            "\u{1b}[2~",
        ] {
            assert_eq!(clean(&format!("{seq}kept text")), "kept text", "{seq:?}");
        }
        assert_eq!(clean("a\tb"), "a       b");
        // Strings: a title ended by BEL, a link ended by ESC \.
        assert_eq!(clean("\u{1b}]0;title\u{7}after"), "after");
        assert_eq!(
            clean("\u{1b}]8;;http://x\u{1b}\\link\u{1b}]8;;\u{1b}\\"),
            "link"
        );
    }

    #[test]
    fn scrolled_back_at_the_cap_keeps_lines_in_view() {
        let mut logs = Logs::new("shop/web".into());
        logs.push(false, "a\n".repeat(KEPT).as_bytes());
        logs.back = KEPT - 1;
        logs.push(false, b"more\n");
        assert_eq!(logs.back, KEPT - 1);
    }

    #[test]
    fn lines_without_newlines_are_cut() {
        let mut logs = Logs::new("shop/web".into());
        logs.push(false, &vec![b'x'; LONGEST * 2 + 10]);
        assert_eq!(logs.lines.len(), 2);
        logs.end("done".into());
        assert_eq!(logs.lines.len(), 3);
        assert_eq!(logs.lines[2].1.len(), 10);
    }
}
