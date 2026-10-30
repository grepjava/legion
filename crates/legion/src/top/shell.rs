//! A shell in a task, in the pane under the table: a terminal emulated
//! here (vt100), fed what the shell writes, and the keys it is sent
//! while it has them.

use std::num::NonZeroU16;

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Lines kept above the screen, to scroll back to.
const SCROLLBACK: usize = 2000;

pub(super) struct Shell {
    /// Whose: `shop/web-1-1`, with `/container` when one was chosen.
    pub(super) of: String,
    pub(super) screen: vt100::Parser,
    /// Keys go to the shell, not to `top`.
    pub(super) focus: bool,
    /// Why it ended.
    pub(super) ended: Option<String>,
}

impl Shell {
    pub(super) fn new(of: String) -> Self {
        Shell {
            of,
            screen: vt100::Parser::new(nonzero(24), nonzero(80), SCROLLBACK),
            focus: true,
            ended: None,
        }
    }

    /// Rows and columns.
    pub(super) fn size(&self) -> (u16, u16) {
        let (rows, cols) = self.screen.screen().size();
        (rows.get(), cols.get())
    }

    pub(super) fn set_size(&mut self, rows: u16, cols: u16) {
        self.screen.set_size(nonzero(rows), nonzero(cols));
    }

    pub(super) fn output(&mut self, data: &[u8]) {
        self.screen.process(data);
    }

    /// Back (or forward, when negative) by `n` lines of what scrolled off.
    pub(super) fn scroll(&mut self, n: isize) {
        let screen = self.screen.screen_mut();
        let back = screen.scrollback().saturating_add_signed(n);
        screen.set_scrollback(back);
    }

    /// What a key sends: the bytes a terminal would.
    pub(super) fn bytes(&self, k: KeyEvent) -> Option<Vec<u8>> {
        let app = self.screen.screen().application_cursor();
        let arrow = |c: char| {
            if app {
                format!("\x1bO{c}").into_bytes()
            } else {
                format!("\x1b[{c}").into_bytes()
            }
        };
        let ctrl = k.modifiers.contains(KeyModifiers::CONTROL);
        let alt = k.modifiers.contains(KeyModifiers::ALT);
        let mut out = match k.code {
            KeyCode::Char(c) if ctrl => {
                let b = match c.to_ascii_lowercase() {
                    c @ 'a'..='z' => c as u8 - b'a' + 1,
                    '@' | ' ' | '2' => 0,
                    '[' | '3' => 0x1b,
                    '\\' | '4' => 0x1c,
                    ']' | '5' => 0x1d,
                    '^' | '6' => 0x1e,
                    '_' | '7' | '/' => 0x1f,
                    _ => return None,
                };
                vec![b]
            }
            KeyCode::Char(c) => c.to_string().into_bytes(),
            KeyCode::Enter => b"\r".to_vec(),
            KeyCode::Backspace => vec![0x7f],
            KeyCode::Tab => b"\t".to_vec(),
            KeyCode::BackTab => b"\x1b[Z".to_vec(),
            KeyCode::Esc => vec![0x1b],
            KeyCode::Up => arrow('A'),
            KeyCode::Down => arrow('B'),
            KeyCode::Right => arrow('C'),
            KeyCode::Left => arrow('D'),
            KeyCode::Home => arrow('H'),
            KeyCode::End => arrow('F'),
            KeyCode::Insert => b"\x1b[2~".to_vec(),
            KeyCode::Delete => b"\x1b[3~".to_vec(),
            KeyCode::PageUp => b"\x1b[5~".to_vec(),
            KeyCode::PageDown => b"\x1b[6~".to_vec(),
            KeyCode::F(n @ 1..=4) => format!("\x1bO{}", (b'P' + n - 1) as char).into_bytes(),
            KeyCode::F(n @ 5..=12) => {
                let code = [15, 17, 18, 19, 20, 21, 23, 24][usize::from(n - 5)];
                format!("\x1b[{code}~").into_bytes()
            }
            _ => return None,
        };
        if alt {
            out.insert(0, 0x1b);
        }
        Some(out)
    }
}

/// Whether the key gives the keys back to `top`: Ctrl-], as telnet's.
pub(super) fn is_leave(k: KeyEvent) -> bool {
    k.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(k.code, KeyCode::Char(']') | KeyCode::Char('5'))
}

fn nonzero(n: u16) -> NonZeroU16 {
    NonZeroU16::new(n).unwrap_or(NonZeroU16::MIN)
}
