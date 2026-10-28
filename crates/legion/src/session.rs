//! `logs`, `exec` and `port-forward`: sessions with a task's containers,
//! opened at the manager, which reaches the task's node (DESIGN.md §4.2).

use std::io::IsTerminal;

use anyhow::{Context, bail};
use legion_proto::v1::pods_client::PodsClient;
use legion_proto::v1::session_frame::Frame;
use legion_proto::v1::session_open::Action;
use legion_proto::v1::{
    ExecOptions, InputClosed, LogsOptions, SessionFrame, SessionOpen, TerminalSize,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tonic::Streaming;
use tonic::transport::Channel;

/// What to open a session with.
pub struct Target {
    pub namespace: String,
    pub task: String,
    pub container: String,
}

impl Target {
    fn open(&self, action: Action) -> SessionOpen {
        SessionOpen {
            namespace: self.namespace.clone(),
            task: self.task.clone(),
            container: self.container.clone(),
            action: Some(action),
        }
    }
}

struct Session {
    tx: mpsc::Sender<SessionFrame>,
    rx: Streaming<SessionFrame>,
}

impl Session {
    async fn open(channel: &Channel, open: SessionOpen) -> anyhow::Result<Session> {
        let (tx, rx) = mpsc::channel(16);
        tx.send(frame(Frame::Open(open))).await?;
        let rx = PodsClient::new(channel.clone())
            .session(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(failed)?
            .into_inner();
        Ok(Session { tx, rx })
    }

    /// The next frame, or `None` at the end.
    async fn next(&mut self) -> anyhow::Result<Option<Frame>> {
        Ok(self
            .rx
            .message()
            .await
            .map_err(failed)?
            .and_then(|f| f.frame))
    }
}

fn frame(f: Frame) -> SessionFrame {
    SessionFrame { frame: Some(f) }
}

fn failed(s: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("{}", s.message())
}

pub async fn logs(
    channel: &Channel,
    target: &Target,
    follow: bool,
    tail: Option<u32>,
) -> anyhow::Result<()> {
    let action = Action::Logs(LogsOptions { follow, tail });
    let mut s = Session::open(channel, target.open(action)).await?;
    let (mut stdout, mut stderr) = (tokio::io::stdout(), tokio::io::stderr());
    while let Some(f) = s.next().await? {
        match f {
            Frame::Stdout(data) => {
                stdout.write_all(&data).await?;
                stdout.flush().await?;
            }
            Frame::Stderr(data) => {
                stderr.write_all(&data).await?;
                stderr.flush().await?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Runs `command` and returns its exit code.
pub async fn exec(
    channel: &Channel,
    target: &Target,
    command: Vec<String>,
    stdin: bool,
    tty: bool,
) -> anyhow::Result<i32> {
    if tty && !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        bail!("--tty needs a terminal; leave it out to pipe input or output");
    }
    let size = if tty { terminal::size() } else { None };
    let action = Action::Exec(ExecOptions {
        command,
        stdin,
        tty,
        size: size.map(|(width, height)| TerminalSize { width, height }),
    });
    let mut s = Session::open(channel, target.open(action)).await?;
    // Restores the terminal when dropped, however this returns.
    let _raw = if tty {
        Some(terminal::Raw::enter()?)
    } else {
        None
    };
    if stdin {
        tokio::spawn(send_stdin(s.tx.clone()));
    }
    if tty {
        tokio::spawn(terminal::resizes(s.tx.clone()));
    }
    let (mut stdout, mut stderr) = (tokio::io::stdout(), tokio::io::stderr());
    loop {
        match s.next().await? {
            Some(Frame::Stdout(data)) => {
                stdout.write_all(&data).await?;
                stdout.flush().await?;
            }
            Some(Frame::Stderr(data)) => {
                stderr.write_all(&data).await?;
                stderr.flush().await?;
            }
            Some(Frame::ExitCode(code)) => return Ok(code),
            Some(_) => {}
            None => bail!("the session ended without the command's exit code"),
        }
    }
}

/// Sends stdin as input until it ends, then closes the input.
async fn send_stdin(tx: mpsc::Sender<SessionFrame>) {
    let mut stdin = tokio::io::stdin();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if tx
                    .send(frame(Frame::Input(buf[..n].to_vec())))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
    let _ = tx.send(frame(Frame::InputClosed(InputClosed {}))).await;
}

/// `LOCAL:REMOTE`, or `PORT` for both.
pub fn parse_ports(spec: &str) -> anyhow::Result<(u16, u16)> {
    let port = |s: &str| {
        s.parse::<u16>()
            .ok()
            .filter(|p| *p != 0)
            .with_context(|| format!("bad port {s:?} in {spec:?}"))
    };
    match spec.split_once(':') {
        Some((local, remote)) => Ok((port(local)?, port(remote)?)),
        None => Ok((port(spec)?, port(spec)?)),
    }
}

/// Listens on `address` for each of `ports` and forwards every connection
/// to the task's pod, one session each, until interrupted.
pub async fn port_forward(
    channel: &Channel,
    target: Target,
    address: &str,
    ports: &[(u16, u16)],
) -> anyhow::Result<()> {
    let target = std::sync::Arc::new(target);
    let mut listeners = Vec::new();
    for &(local, remote) in ports {
        let listener = tokio::net::TcpListener::bind((address, local))
            .await
            .with_context(|| format!("listening on {address}:{local}"))?;
        println!(
            "forwarding {} to {}:{remote}",
            listener.local_addr()?,
            target.task
        );
        listeners.push((listener, remote));
    }
    for (listener, remote) in listeners {
        let channel = channel.clone();
        let target = target.clone();
        tokio::spawn(async move {
            loop {
                let Ok((conn, peer)) = listener.accept().await else {
                    continue;
                };
                let channel = channel.clone();
                let target = target.clone();
                tokio::spawn(async move {
                    if let Err(e) = forward(&channel, &target, remote, conn).await {
                        eprintln!("{peer} to port {remote}: {e:#}");
                    }
                });
            }
        });
    }
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn forward(
    channel: &Channel,
    target: &Target,
    port: u16,
    conn: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    let mut s = Session::open(channel, target.open(Action::Port(port.into()))).await?;
    let (mut read, mut write) = conn.into_split();
    let tx = s.tx.clone();
    let sending = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            match read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx
                        .send(frame(Frame::Input(buf[..n].to_vec())))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
        let _ = tx.send(frame(Frame::InputClosed(InputClosed {}))).await;
    });
    let result = async {
        while let Some(f) = s.next().await? {
            if let Frame::Stdout(data) = f {
                write.write_all(&data).await?;
            }
        }
        anyhow::Ok(())
    }
    .await;
    sending.abort();
    let _ = write.shutdown().await;
    result
}

#[cfg(unix)]
mod terminal {
    use legion_proto::v1::session_frame::Frame;
    use legion_proto::v1::{SessionFrame, TerminalSize};
    use tokio::sync::mpsc;

    /// The terminal in raw mode, until dropped.
    pub struct Raw {
        saved: libc::termios,
    }

    impl Raw {
        pub fn enter() -> std::io::Result<Raw> {
            // SAFETY: termios is plain data, filled in by tcgetattr.
            let mut t: libc::termios = unsafe { std::mem::zeroed() };
            if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut t) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let saved = t;
            unsafe { libc::cfmakeraw(&mut t) };
            if unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Raw { saved })
        }
    }

    impl Drop for Raw {
        fn drop(&mut self) {
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.saved) };
        }
    }

    /// Columns and rows of the terminal on stdout.
    pub fn size() -> Option<(u32, u32)> {
        // SAFETY: winsize is plain data, filled in by the ioctl.
        let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
        let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0;
        (ok && ws.ws_col > 0).then(|| (ws.ws_col.into(), ws.ws_row.into()))
    }

    /// Sends the terminal's new size whenever it changes.
    pub async fn resizes(tx: mpsc::Sender<SessionFrame>) {
        use tokio::signal::unix::{SignalKind, signal};
        let Ok(mut winch) = signal(SignalKind::window_change()) else {
            return;
        };
        while winch.recv().await.is_some() {
            if let Some((width, height)) = size() {
                let resize = Frame::Resize(TerminalSize { width, height });
                if tx
                    .send(SessionFrame {
                        frame: Some(resize),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

#[cfg(not(unix))]
mod terminal {
    use legion_proto::v1::SessionFrame;
    use tokio::sync::mpsc;

    pub struct Raw;

    impl Raw {
        pub fn enter() -> anyhow::Result<Raw> {
            anyhow::bail!("--tty needs a Unix terminal on this side")
        }
    }

    pub fn size() -> Option<(u32, u32)> {
        None
    }

    pub async fn resizes(_tx: mpsc::Sender<SessionFrame>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports() {
        assert_eq!(parse_ports("8080:80").unwrap(), (8080, 80));
        assert_eq!(parse_ports("5432").unwrap(), (5432, 5432));
        assert!(parse_ports("0").is_err());
        assert!(parse_ports("80:http").is_err());
    }
}
