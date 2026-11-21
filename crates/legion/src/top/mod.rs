//! `legion top`: the cluster, live, in the terminal. It lists every kind
//! of object the caller may read, then follows a watch to keep them
//! current; asks the leader every few seconds what tasks use; and keeps a
//! log of what changed while it ran. Keys act on the selected row: drain,
//! pause, label or delete a node; scale or roll back a service; suspend a
//! cron job; move a volume; delete anything; follow logs or open a shell
//! in a pane under the table.

mod draw;
mod logs;
mod model;
mod shell;
#[cfg(test)]
mod tests;
mod view;

use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use legion_api::{Kind, Object};
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    self as pb, ListRequest, ObjectRef, StatusRequest, WatchRequest, watch_event,
};
use ratatui::backend::Backend;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyEvent, KeyEventKind, MouseEvent,
};
use ratatui::layout::Rect;
use ratatui::{Terminal, crossterm};
use tokio::sync::{mpsc, watch};
use tonic::transport::Channel;

use crate::session::{ExecEvent, ExecStream};

pub(crate) use draw::bytes;
pub use model::Model;
#[cfg(test)]
pub use view::SortCol;
pub use view::{Request, View};

/// How often it asks for usage and the managers' state.
const POLL: Duration = Duration::from_secs(5);
/// How often every cell is written again, not only those that changed:
/// what else wrote to the terminal (console messages) goes.
const REPAINT: Duration = Duration::from_secs(5);
/// How often the connections of the namespaces shown are asked: each ask
/// has every node list its pf states.
const SEEN: Duration = Duration::from_secs(10);

/// What the screen hears of.
enum Msg {
    Listed(Vec<Object>, BTreeSet<Kind>),
    Changed(bool, Object),
    Usage(pb::UsageResponse),
    Status(pb::StatusResponse),
    Problem(String),
    /// Why the cluster's status or use could not be read, or `None` once
    /// it could again.
    Polled(Option<String>),
    Key(KeyEvent),
    Mouse(MouseEvent),
    /// What came of a request.
    Done(String),
    /// Logs of the stream numbered so, on stderr or not.
    Log(u64, bool, Vec<u8>),
    /// Multiplexed log chunk from a named pod in stream `n`.
    PodLog(u64, String, bool, Vec<u8>),
    /// That stream ended, and why.
    LogEnded(u64, String),
    /// What the shell numbered so wrote.
    ShellOutput(u64, Vec<u8>),
    /// That shell ended, and why.
    ShellEnded(u64, String),
    Redraw,
    /// The connections the nodes saw of a namespace's tasks.
    Connections(String, Vec<pb::ConnectionEdge>),
    /// Long operations as they change (`Cluster.Activity`); `true`: all
    /// of them, what was known before replaced.
    Activity(bool, Vec<legion_api::activity::Operation>),
    /// What the managers kept over a range (`0`: the hour before the
    /// first poll).
    History(i64, pb::UsageHistoryResponse),
    /// The terminal is gone.
    HungUp,
}

/// Runs until the person quits or the terminal goes.
/// The manager and identity `legion top` was started with, which the
/// commands it runs (`:`) use too.
#[derive(Clone, Debug)]
pub struct Connection {
    pub server: String,
    pub identity: PathBuf,
}

pub async fn run(
    channel: Channel,
    namespace: Option<String>,
    connection: Connection,
) -> anyhow::Result<()> {
    draw::set_mono(std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (poll_tx, poll_rx) = watch::channel(POLL);
    let follower = tokio::spawn(follow(channel.clone(), namespace.clone(), tx.clone()));
    let poller = tokio::spawn(poll(
        channel.clone(),
        namespace.clone(),
        poll_rx,
        tx.clone(),
    ));
    let (seen_tx, seen_rx) = watch::channel(BTreeSet::new());
    let seer = tokio::spawn(poll_connections(channel.clone(), seen_rx, tx.clone()));
    let activity = tokio::spawn(follow_activity(channel.clone(), tx.clone()));
    let (range_tx, range_rx) = watch::channel(0i64);
    let past = tokio::spawn(follow_history(
        channel.clone(),
        namespace.clone(),
        range_rx,
        tx.clone(),
    ));
    let keys = Keys::default();
    read_keys(tx.clone(), keys.clone());

    // The logs shown, numbered: what an earlier stream sent is dropped.
    let mut logs: Option<(u64, tokio::task::JoinHandle<()>)> = None;
    // The shell shown, numbered too, and where its typing goes.
    let mut shell: Option<(
        u64,
        tokio::task::JoinHandle<()>,
        mpsc::Sender<pb::SessionFrame>,
    )> = None;
    let mut streams = 0u64;
    let objects = crate::cli::Objects::new(channel.clone());
    let mut model = Model::new(namespace);
    let mut view = View::default();
    let mut terminal = ratatui::init();
    if let Ok(size) = terminal.size()
        && size.width >= 110
        && size.height >= 26
    {
        view.mouse = true;
    }
    let mut mouse_captured = false;
    let mut last_poll_ms = view.poll_ms;
    let mut painted = Instant::now();
    let result = loop {
        if view.mouse != mouse_captured {
            if view.mouse {
                let _ = crossterm::execute!(terminal.backend_mut(), EnableMouseCapture);
            } else {
                let _ = crossterm::execute!(terminal.backend_mut(), DisableMouseCapture);
            }
            mouse_captured = view.mouse;
        }
        range_tx.send_if_modified(|r| {
            (*r != view.range_ms())
                .then(|| *r = view.range_ms())
                .is_some()
        });
        if view.poll_ms != last_poll_ms {
            last_poll_ms = view.poll_ms;
            let _ = poll_tx.send(Duration::from_millis(last_poll_ms));
        }
        if painted.elapsed() >= REPAINT {
            invalidate(&mut terminal);
            painted = Instant::now();
        }
        // The shell's screen as large as its pane.
        if let Some((_, _, input)) = &shell
            && let Ok(size) = terminal.size()
            && let Some((rows, cols)) = draw::shell_size(&view, size.into())
            && let Some(s) = &mut view.shell
            && s.size() != (rows, cols)
        {
            s.set_size(rows, cols);
            let _ = input.try_send(crate::session::resize(cols, rows));
        }
        let term_area = terminal.size().map(Rect::from).unwrap_or_default();
        if let Err(e) = terminal.draw(|f| draw::render(f, &model, &view)) {
            break Err(e.into());
        }
        let Some(msg) = rx.recv().await else {
            break Ok(());
        };
        let mut next = Some(msg);
        let mut requests = Vec::new();
        while let Some(msg) = next {
            match msg {
                Msg::Listed(objects, hidden) => model.listed(objects, hidden),
                Msg::Changed(put, o) => model.changed(put, o, legion_api::clock::now()),
                Msg::Usage(u) => model.usage(u),
                Msg::Status(s) => model.cluster = Some(s),
                Msg::Problem(p) => model.problem = Some(p),
                Msg::Polled(p) => model.polled = p,
                Msg::Key(k) => {
                    requests.extend(view.key(&model, k).into_iter().flat_map(Request::flatten))
                }
                Msg::Mouse(m) => requests.extend(
                    view.mouse(&model, m, term_area)
                        .into_iter()
                        .flat_map(Request::flatten),
                ),
                Msg::Done(what) => view.notice = Some(what),
                Msg::Log(n, stderr, data) => {
                    if let (Some(l), Some((shown, _))) = (&mut view.logs, &logs)
                        && n == *shown
                    {
                        l.push(stderr, &data);
                    }
                }
                Msg::PodLog(n, pod, stderr, data) => {
                    if let (Some(l), Some((shown, _))) = (&mut view.logs, &logs)
                        && n == *shown
                    {
                        l.push_prefixed(&pod, stderr, &data);
                    }
                }
                Msg::ShellOutput(n, data) => {
                    if let (Some(s), Some((shown, ..))) = (&mut view.shell, &shell)
                        && n == *shown
                    {
                        s.output(&data);
                    }
                }
                Msg::ShellEnded(n, why) => {
                    if let (Some(s), Some((shown, ..))) = (&mut view.shell, &shell)
                        && n == *shown
                    {
                        s.ended = Some(why);
                        s.focus = false;
                    }
                }
                Msg::LogEnded(n, why) => {
                    if let (Some(l), Some((shown, _))) = (&mut view.logs, &logs)
                        && n == *shown
                    {
                        l.end(why);
                    }
                }
                Msg::Redraw => {}
                Msg::Activity(all, ops) => {
                    if all {
                        model.activity.clear();
                    }
                    for op in ops {
                        model.activity.insert(op.id.clone(), op);
                    }
                }
                Msg::History(range_ms, r) => model.history_answer(range_ms, &r),
                Msg::Connections(namespace, edges) => {
                    model.connections.insert(namespace, edges);
                }
                Msg::HungUp => view.quit = true,
            }
            next = rx.try_recv().ok();
        }
        seen_tx.send_if_modified(|asked| {
            let now = model.service_namespaces();
            model.connections.retain(|ns, _| now.contains(ns));
            (*asked != now).then(|| *asked = now).is_some()
        });
        for request in requests {
            match request {
                Request::Logs { .. } | Request::ServiceLogs { .. } | Request::Exec { .. } => {
                    if let Some((_, stream)) = logs.take() {
                        stream.abort();
                    }
                    if let Some((_, stream, _)) = shell.take() {
                        stream.abort();
                    }
                    streams += 1;
                    match &request {
                        Request::Logs { .. } => {
                            view.show_logs(&request);
                            let stream = tokio::spawn(stream_logs(
                                channel.clone(),
                                request,
                                streams,
                                tx.clone(),
                            ));
                            logs = Some((streams, stream));
                        }
                        Request::ServiceLogs {
                            namespace,
                            kind,
                            name,
                        } => {
                            let pods: Vec<String> = model
                                .of(Kind::Task)
                                .filter(|t| {
                                    t.metadata.namespace == *namespace
                                        && model::owner(t) == Some((*kind, name.as_str()))
                                        && !model::task_state(t).is_terminal()
                                })
                                .map(|t| t.metadata.name.clone())
                                .collect();
                            view.show_logs(&request);
                            let stream = tokio::spawn(stream_service_logs(
                                channel.clone(),
                                namespace.clone(),
                                pods,
                                streams,
                                tx.clone(),
                            ));
                            logs = Some((streams, stream));
                        }
                        Request::Exec { .. } => {
                            view.show_shell(&request);
                            let area = terminal.size().map(Rect::from).unwrap_or_default();
                            let size = draw::shell_size(&view, area).unwrap_or((24, 80));
                            if let Some(s) = &mut view.shell {
                                s.set_size(size.0, size.1);
                            }
                            let (input, typed) = mpsc::channel(256);
                            let stream = tokio::spawn(run_shell(
                                channel.clone(),
                                request,
                                streams,
                                size,
                                typed,
                                tx.clone(),
                            ));
                            shell = Some((streams, stream, input));
                        }
                        _ => unreachable!(),
                    }
                }
                Request::Events { .. } | Request::Alerts | Request::Run { .. } => {
                    if let Some((_, stream)) = logs.take() {
                        stream.abort();
                    }
                    if let Some((_, stream, _)) = shell.take() {
                        stream.abort();
                    }
                    streams += 1;
                    view.show_logs(&request);
                    let stream = match request {
                        Request::Run { args } => {
                            tokio::spawn(run_command(connection.clone(), args, streams, tx.clone()))
                        }
                        request => tokio::spawn(stream_happenings(
                            channel.clone(),
                            request,
                            streams,
                            tx.clone(),
                        )),
                    };
                    logs = Some((streams, stream));
                }
                Request::Edit { key } => {
                    let done = edit(&mut terminal, &objects, &key, &keys, mouse_captured).await;
                    view.notice = Some(done);
                    invalidate(&mut terminal);
                }
                Request::Input(data) => {
                    if let Some((_, _, input)) = &shell {
                        let _ = input.try_send(crate::session::input(data));
                    }
                }
                request => {
                    view.notice = Some("Asking…".into());
                    let (objects, tx) = (objects.clone(), tx.clone());
                    let cluster = crate::cli::cluster_client(channel.clone());
                    tokio::spawn(async move {
                        let _ = tx.send(Msg::Done(act(objects, cluster, request).await));
                    });
                }
            }
        }
        if view.shell.is_none()
            && let Some((_, stream, _)) = shell.take()
        {
            stream.abort();
        }
        if view.logs.is_none()
            && let Some((_, stream)) = logs.take()
        {
            stream.abort();
        }
        if view.quit {
            break Ok(());
        }
    };
    if mouse_captured {
        let _ = crossterm::execute!(terminal.backend_mut(), DisableMouseCapture);
    }
    // The FreeBSD console has no alternate screen: blank it, so the last
    // frame does not stay behind the prompt.
    let _ = crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
        crossterm::cursor::MoveTo(0, 0)
    );
    ratatui::restore();
    follower.abort();
    poller.abort();
    seer.abort();
    activity.abort();
    past.abort();
    result
}

/// Runs `legion ARGS` with the connection `legion top` has, its output
/// as stream `n` in the pane, then how it ended. Closing the pane drops the
/// task, which kills the command.
async fn run_command(
    connection: Connection,
    args: Vec<String>,
    n: u64,
    tx: mpsc::UnboundedSender<Msg>,
) {
    use tokio::io::AsyncReadExt;
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            let _ = tx.send(Msg::LogEnded(
                n,
                format!("Not run: this program's path: {e}"),
            ));
            return;
        }
    };
    let child = tokio::process::Command::new(exe)
        .args(&args)
        .env("LEGION_SERVER", &connection.server)
        .env("LEGION_IDENTITY", &connection.identity)
        .env("NO_COLOR", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(Msg::LogEnded(n, format!("Not run: {e}")));
            return;
        }
    };
    let pipe = |stream: Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>, stderr: bool| {
        let tx = tx.clone();
        async move {
            let Some(mut stream) = stream else { return };
            let mut buf = vec![0; 8192];
            while let Ok(read @ 1..) = stream.read(&mut buf).await {
                if tx.send(Msg::Log(n, stderr, buf[..read].to_vec())).is_err() {
                    return;
                }
            }
        }
    };
    let out = child
        .stdout
        .take()
        .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Send + Unpin>);
    let err = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Send + Unpin>);
    tokio::join!(pipe(out, false), pipe(err, true));
    let why = match child.wait().await {
        Ok(s) if s.success() => "Done.".to_owned(),
        Ok(s) => match s.code() {
            Some(code) => format!("Failed: exit {code}."),
            None => "Killed.".to_owned(),
        },
        Err(e) => format!("Failed: {e}"),
    };
    let _ = tx.send(Msg::LogEnded(n, why));
}

/// What an edit of `key`, read at `version`, applies: the one object, over
/// that version, so an edit of what someone changed meanwhile is refused.
pub(super) fn edited(
    text: &str,
    key: &model::Key,
    version: u64,
) -> Result<pb::ApplyRequest, String> {
    let objects = legion_api::manifest::parse(text)?;
    let [object] = objects.as_slice() else {
        return Err(format!(
            "the file holds {} objects: an edit is of the one",
            objects.len()
        ));
    };
    let (kind, namespace, name) = key;
    if object.kind != *kind
        || object.metadata.namespace != *namespace
        || object.metadata.name != *name
    {
        return Err(format!(
            "it names {} now: apply another object with legion apply",
            object.key()
        ));
    }
    Ok(pb::ApplyRequest {
        object: Some(object.into()),
        expected_version: Some(version),
        ..Default::default()
    })
}

/// The editor to run: `$VISUAL`, `$EDITOR`, else `vi`, with its words.
fn editor() -> Vec<String> {
    ["VISUAL", "EDITOR"]
        .iter()
        .filter_map(|v| std::env::var(v).ok())
        .find_map(|e| view::split_words(&e).ok().filter(|w| !w.is_empty()))
        .unwrap_or_else(|| vec!["vi".to_owned()])
}

/// Edits `key` in the editor, the terminal given over to it meanwhile, and
/// applies what was saved: what came of it, for the bottom line. A failed
/// apply keeps the edit in its file, named.
async fn edit(
    terminal: &mut ratatui::DefaultTerminal,
    objects: &crate::cli::Objects,
    key: &model::Key,
    keys: &Keys,
    mouse: bool,
) -> String {
    let (kind, namespace, name) = key.clone();
    let got = objects
        .clone()
        .get(ObjectRef {
            kind: kind.to_string(),
            namespace: namespace.clone(),
            name: name.clone(),
        })
        .await;
    let object = match got.map(|r| Object::try_from(r.into_inner())) {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return format!("Not edited: {e}"),
        Err(e) => return format!("Not edited: {}", e.message()),
    };
    let version = object.metadata.version;
    let before = legion_api::manifest::yaml(&object);
    let path = std::env::temp_dir().join(format!(
        "legion-edit-{}-{name}-{}.yaml",
        kind.to_string().to_lowercase(),
        std::process::id()
    ));
    if let Err(e) = std::fs::write(&path, &before) {
        return format!("Not edited: writing {}: {e}", path.display());
    }
    keys.hold().await;
    if mouse {
        let _ = crossterm::execute!(terminal.backend_mut(), DisableMouseCapture);
    }
    ratatui::restore();
    let words = editor();
    let ran = tokio::process::Command::new(&words[0])
        .args(&words[1..])
        .arg(&path)
        .status()
        .await;
    *terminal = ratatui::init();
    if mouse {
        let _ = crossterm::execute!(terminal.backend_mut(), EnableMouseCapture);
    }
    keys.release();
    let what = format!("{} {}", kind_word_of(kind), display(&namespace, &name));
    let kept = |why: String| format!("{why}. Your edit is kept in {}.", path.display());
    match ran {
        Ok(s) if s.success() => {}
        Ok(s) => return kept(format!("Not applied: {} exited {s}", words[0])),
        Err(e) => return format!("Not edited: running {}: {e}", words[0]),
    }
    let after = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => return format!("Not applied: reading {}: {e}", path.display()),
    };
    if after == before {
        let _ = std::fs::remove_file(&path);
        return format!("No change to {what}.");
    }
    let request = match edited(&after, key, version) {
        Ok(r) => r,
        Err(e) => return kept(format!("Not applied: {e}")),
    };
    match objects.clone().apply(request).await {
        Ok(r) => {
            let _ = std::fs::remove_file(&path);
            let version = r.into_inner().object.map_or(0, |o| o.version);
            format!("Applied {what} (version {version}).")
        }
        Err(e) => kept(format!("Not applied: {}", e.message())),
    }
}

fn kind_word_of(kind: Kind) -> &'static str {
    model::kind_word(kind)
}

fn display(namespace: &str, name: &str) -> String {
    if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{namespace}/{name}")
    }
}

/// Asks for the connections of each namespace in `namespaces` every
/// [`SEEN`], and at once when they change. A manager too old to say, or a
/// caller who may not read a namespace's services, gets none shown.
async fn poll_connections(
    channel: Channel,
    mut namespaces: watch::Receiver<BTreeSet<String>>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    let mut cluster = crate::cli::cluster_client(channel);
    let mut tick = tokio::time::interval(SEEN);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            changed = namespaces.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
        let asked: Vec<String> = namespaces.borrow_and_update().iter().cloned().collect();
        for namespace in asked {
            let req = pb::ConnectionsRequest {
                namespace: namespace.clone(),
            };
            if let Ok(r) = cluster.connections(req).await
                && tx
                    .send(Msg::Connections(namespace, r.into_inner().edges))
                    .is_err()
            {
                return;
            }
        }
    }
}

/// How often a range the graphs show is asked again: the managers keep
/// a sample a minute.
const PAST: Duration = Duration::from_secs(60);

/// Asks what the managers kept: the hour before the first poll once, for
/// the graphs to start full, then the range the graphs show, as it
/// changes and every minute while it is shown. Not for a manager too old
/// to have it, nor a caller who may not read it.
async fn follow_history(
    channel: Channel,
    namespace: Option<String>,
    mut range: watch::Receiver<i64>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    let cluster = crate::cli::cluster_client(channel);
    let ask = |range_ms: i64| {
        let req = pb::UsageHistoryRequest {
            namespace: namespace.clone().unwrap_or_default(),
            task: String::new(),
            range_ms: if range_ms == 0 { 3_600_000 } else { range_ms },
        };
        let mut cluster = cluster.clone();
        async move { cluster.usage_history(req).await.map(|r| r.into_inner()) }
    };
    match ask(0).await {
        Ok(r) => {
            if tx.send(Msg::History(0, r)).is_err() {
                return;
            }
        }
        Err(e) if e.code() == tonic::Code::Unimplemented => return,
        Err(_) => {}
    }
    loop {
        let shown = *range.borrow_and_update();
        if shown != 0
            && let Ok(r) = ask(shown).await
            && tx.send(Msg::History(shown, r)).is_err()
        {
            return;
        }
        // The polls need nothing more: wait for a range to be chosen.
        let wait = if shown == 0 {
            Duration::from_secs(86_400)
        } else {
            PAST
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            changed = range.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Follows `Cluster.Activity`, again a few seconds after it ends; not
/// for a manager too old to have it.
async fn follow_activity(channel: Channel, tx: mpsc::UnboundedSender<Msg>) {
    let mut cluster = crate::cli::cluster_client(channel);
    loop {
        match cluster.activity(pb::ActivityRequest { follow: true }).await {
            Ok(r) => {
                let mut stream = r.into_inner();
                let mut first = true;
                while let Ok(Some(r)) = stream.message().await {
                    let ops = r
                        .operations_json
                        .iter()
                        .filter_map(|j| serde_json::from_slice(j).ok())
                        .collect();
                    if tx.send(Msg::Activity(first, ops)).is_err() {
                        return;
                    }
                    first = false;
                }
            }
            Err(e) if e.code() == tonic::Code::Unimplemented => return,
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Reads keys on a thread of its own. crossterm reads a hung-up terminal
/// forever (jmon found it), so it stops once stdin is no longer a
/// terminal.
/// Whether the key reader is to leave the terminal alone (`held`), and
/// whether it does (`parked`): an editor run meanwhile reads the keys.
#[derive(Clone, Default)]
struct Keys {
    held: Arc<AtomicBool>,
    parked: Arc<AtomicBool>,
}

impl Keys {
    /// Stops the reader, once it is not reading.
    async fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
        for _ in 0..50 {
            if self.parked.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
    }
}

fn read_keys(tx: mpsc::UnboundedSender<Msg>, keys: Keys) {
    let watch_tty = std::io::stdin().is_terminal();
    std::thread::spawn(move || {
        loop {
            if keys.held.load(Ordering::SeqCst) {
                keys.parked.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            keys.parked.store(false, Ordering::SeqCst);
            if watch_tty && !std::io::stdin().is_terminal() {
                let _ = tx.send(Msg::HungUp);
                return;
            }
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => {
                    let _ = tx.send(Msg::HungUp);
                    return;
                }
            }
            let msg = match event::read() {
                Ok(Event::Key(k)) if k.kind == KeyEventKind::Press => Msg::Key(k),
                Ok(Event::Mouse(m)) => Msg::Mouse(m),
                Ok(Event::Resize(..)) => Msg::Redraw,
                Ok(_) => continue,
                Err(_) => Msg::HungUp,
            };
            if tx.send(msg).is_err() {
                return;
            }
        }
    });
}

/// Makes the next draw write every cell: ratatui writes only those that
/// changed, so anything else written to the terminal would stay.
fn invalidate<B: Backend>(terminal: &mut Terminal<B>) {
    for c in &mut terminal.current_buffer_mut().content {
        c.set_symbol("\u{fffe}");
    }
    terminal.swap_buffers();
}

/// Multiplexes live logs across all running tasks `pods` of a Service or Job.
async fn stream_service_logs(
    channel: Channel,
    namespace: String,
    pods: Vec<String>,
    n: u64,
    tx: mpsc::UnboundedSender<Msg>,
) {
    if pods.is_empty() {
        let _ = tx.send(Msg::LogEnded(n, "No running tasks yet.".into()));
        return;
    }
    let mut join = tokio::task::JoinSet::new();
    for pod in pods {
        let ch = channel.clone();
        let ns = namespace.clone();
        let tx_pod = tx.clone();
        join.spawn(async move {
            let target = crate::session::Target {
                namespace: ns,
                task: pod.clone(),
                container: String::new(),
            };
            if let Ok(mut s) = crate::session::LogStream::open(&ch, &target, true, Some(50)).await {
                while let Ok(Some((stderr, data))) = s.next().await {
                    if tx_pod
                        .send(Msg::PodLog(n, pod.clone(), stderr, data))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });
    }
    while join.join_next().await.is_some() {}
    let _ = tx.send(Msg::LogEnded(n, "All followed tasks ended.".into()));
}

/// Follows the logs a request asks for, numbered `n`.
async fn stream_logs(channel: Channel, request: Request, n: u64, tx: mpsc::UnboundedSender<Msg>) {
    let Request::Logs {
        namespace,
        name,
        container,
    } = request
    else {
        return;
    };
    let target = crate::session::Target {
        namespace,
        task: name,
        container: container.unwrap_or_default(),
    };
    let why = match crate::session::LogStream::open(&channel, &target, true, Some(200)).await {
        Ok(mut s) => loop {
            match s.next().await {
                Ok(Some((stderr, data))) => {
                    if tx.send(Msg::Log(n, stderr, data)).is_err() {
                        return;
                    }
                }
                Ok(None) => break "No more: the task ended.".to_owned(),
                Err(e) => break format!("Failed: {e:#}"),
            }
        },
        Err(e) => format!("Failed: {e:#}"),
    };
    let _ = tx.send(Msg::LogEnded(n, why));
}

/// Follows the events or alerts a request asks for, numbered `n`, as
/// lines in the pane: an event a line, the alerts each time they change.
async fn stream_happenings(
    channel: Channel,
    request: Request,
    n: u64,
    tx: mpsc::UnboundedSender<Msg>,
) {
    let mut cluster = crate::cli::cluster_client(channel);
    let line = |text: String| Msg::Log(n, false, format!("{text}\n").into_bytes());
    let why = match request {
        Request::Events {
            kind,
            namespace,
            name,
        } => {
            let req = pb::EventsRequest {
                namespace,
                kind: kind.to_string(),
                name,
                follow: true,
            };
            match cluster.events(req).await {
                Ok(s) => {
                    let mut s = s.into_inner();
                    let mut first = true;
                    loop {
                        match s.message().await {
                            Ok(Some(r)) => {
                                if first && r.events.is_empty() {
                                    let _ = tx.send(line(
                                        "Nothing yet that the manager saw; new ones come as they happen."
                                            .into(),
                                    ));
                                }
                                first = false;
                                for e in r.events {
                                    let what = if e.message.is_empty() {
                                        e.reason
                                    } else {
                                        format!("{}: {}", e.reason, e.message)
                                    };
                                    let text = format!(
                                        "{}  {} {}  {what}",
                                        view::time_of_day(e.at_ms.div_euclid(1000)),
                                        e.kind.to_lowercase(),
                                        e.name
                                    );
                                    if tx.send(line(text)).is_err() {
                                        return;
                                    }
                                }
                            }
                            Ok(None) => break "No more.".to_owned(),
                            Err(e) => break format!("Failed: {}", e.message()),
                        }
                    }
                }
                Err(e) => format!("Failed: {}", e.message()),
            }
        }
        Request::Alerts => match cluster.watch_alerts(pb::AlertsRequest::default()).await {
            Ok(s) => {
                let mut s = s.into_inner();
                loop {
                    match s.message().await {
                        Ok(Some(r)) => {
                            let now = view::time_of_day(legion_api::clock::now());
                            let mut text = match r.alerts.len() {
                                0 => format!("{now}  no alerts"),
                                1 => format!("{now}  1 alert:"),
                                k => format!("{now}  {k} alerts:"),
                            };
                            for a in r.alerts {
                                let at = if a.object.is_empty() {
                                    "the cluster".to_owned()
                                } else if a.namespace.is_empty() {
                                    a.object
                                } else {
                                    format!("{}/{}", a.namespace, a.object)
                                };
                                text.push_str(&format!("\n  {} {at}: {}", a.severity, a.message));
                            }
                            if tx.send(line(text)).is_err() {
                                return;
                            }
                        }
                        Ok(None) => break "No more.".to_owned(),
                        Err(e) => break format!("Failed: {}", e.message()),
                    }
                }
            }
            Err(e) => format!("Failed: {}", e.message()),
        },
        _ => return,
    };
    let _ = tx.send(Msg::LogEnded(n, why));
}

/// Runs the shell a request asks for, numbered `n`, on a screen of
/// `size` (rows, columns), sending it what `input` brings.
async fn run_shell(
    channel: Channel,
    request: Request,
    n: u64,
    (rows, cols): (u16, u16),
    mut input: mpsc::Receiver<pb::SessionFrame>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    let Request::Exec {
        namespace,
        task,
        container,
    } = request
    else {
        return;
    };
    let target = crate::session::Target {
        namespace,
        task,
        container: container.unwrap_or_default(),
    };
    let command = vec!["/bin/sh".to_owned()];
    let why = match ExecStream::open(&channel, &target, command, (cols, rows)).await {
        Ok(mut s) => {
            let send = s.sender();
            loop {
                tokio::select! {
                    next = s.next() => match next {
                        Ok(Some(ExecEvent::Output(data))) => {
                            if tx.send(Msg::ShellOutput(n, data)).is_err() {
                                return;
                            }
                        }
                        Ok(Some(ExecEvent::Exit(code))) => break format!("Ended with {code}."),
                        Ok(None) => break "Ended.".to_owned(),
                        Err(e) => break format!("Failed: {e:#}"),
                    },
                    Some(frame) = input.recv() => {
                        if send.send(frame).await.is_err() {
                            break "Ended.".to_owned();
                        }
                    }
                }
            }
        }
        Err(e) => format!("Failed: {e:#}"),
    };
    let _ = tx.send(Msg::ShellEnded(n, why));
}

/// The change a request makes that is checked first.
fn disruption(request: &Request) -> Option<legion_api::dryrun::Change> {
    use legion_api::dryrun::Change;
    let namespace = |n: &str| {
        if n.is_empty() {
            "default".to_owned()
        } else {
            n.to_owned()
        }
    };
    match request {
        Request::Availability { node, to } if *to == "drain" => {
            Some(Change::Drain { node: node.clone() })
        }
        Request::Move {
            namespace: ns,
            volume,
            node,
        } => Some(Change::MoveVolume {
            namespace: namespace(ns),
            volume: volume.clone(),
            node: node.clone(),
        }),
        Request::Delete {
            kind: Kind::Node,
            name,
            ..
        } => Some(Change::RemoveNode { node: name.clone() }),
        Request::Delete {
            kind: Kind::Service,
            namespace: ns,
            name,
        } => Some(Change::DeleteService {
            namespace: namespace(ns),
            service: name.clone(),
        }),
        _ => None,
    }
}

fn object_ref(kind: Kind, namespace: String, name: String) -> ObjectRef {
    ObjectRef {
        kind: kind.to_string(),
        namespace,
        name,
    }
}

/// Makes a request over the API; what came of it, for people. A drain, a
/// Volume's move and a node's removal or a Service's deletion are checked
/// first, as the CLI checks them: one that would not complete is refused,
/// saying why (the CLI's --force goes ahead all the same).
async fn act(
    mut objects: crate::cli::Objects,
    mut cluster: legion_proto::v1::cluster_client::ClusterClient<Channel>,
    request: Request,
) -> String {
    if let Some(change) = disruption(&request) {
        match crate::cli::dry_run_of(&mut cluster, &change).await {
            Ok(Some(d)) if d.blocks() => {
                let why: Vec<String> = d
                    .findings
                    .iter()
                    .filter(|f| f.severity == legion_api::dryrun::Severity::Blocking)
                    .map(|f| match f.object.is_empty() {
                        true => f.message.clone(),
                        false => format!("{}: {}", f.object, f.message),
                    })
                    .collect();
                return format!(
                    "Refused: {} would not complete: {} (the CLI's --force goes ahead anyway)",
                    change.describe(),
                    why.join("; ")
                );
            }
            Ok(_) => {}
            Err(e) => return format!("Failed: {e:#}"),
        }
    }
    let said = |changed: bool, done: String, already: String| {
        if changed { done } else { already }
    };
    let result = match request {
        Request::Availability { node, to } => objects
            .set_spec(
                object_ref(Kind::Node, String::new(), node),
                "availability",
                to.into(),
            )
            .await
            .map(|(key, changed)| {
                said(
                    changed,
                    format!("{key} {to}"),
                    format!("{key} is already {to}"),
                )
            }),
        Request::Label { node, key, value } => {
            let what = match &value {
                Some(v) => format!("labelled {key}={v}"),
                None => format!("lost label {key}"),
            };
            objects
                .set_labels(object_ref(Kind::Node, String::new(), node), &[(key, value)])
                .await
                .map(|(k, changed)| said(changed, format!("{k} {what}"), format!("{k} unchanged")))
        }
        Request::Scale {
            namespace,
            service,
            replicas,
        } => objects
            .set_spec(
                object_ref(Kind::Service, namespace, service),
                "replicas",
                replicas.into(),
            )
            .await
            .map(|(key, changed)| {
                said(
                    changed,
                    format!("{key} scaled to {replicas}"),
                    format!("{key} already has {replicas}"),
                )
            }),
        Request::Rollback { namespace, service } => {
            crate::cli::rollback(&mut objects, namespace, service).await
        }
        Request::Restart { namespace, service } => {
            legion_manager::actions::restart(&mut objects, namespace, service)
                .await
                .map_err(|s| anyhow::anyhow!("{}", s.message()))
        }
        Request::RunNow {
            namespace,
            cron_job,
        } => legion_manager::actions::run_now(&mut objects, namespace, cron_job)
            .await
            .map_err(|s| anyhow::anyhow!("{}", s.message())),
        Request::Stop {
            namespace,
            service,
            stop,
        } => {
            let what = if stop { "stopped" } else { "started" };
            objects
                .set_spec(
                    object_ref(Kind::Service, namespace, service),
                    "stopped",
                    stop.into(),
                )
                .await
                .map(|(key, changed)| {
                    said(
                        changed,
                        format!("{key} {what}"),
                        format!("{key} already {what}"),
                    )
                })
        }
        Request::Pause {
            namespace,
            service,
            pause,
        } => {
            let what = if pause { "paused" } else { "resumed" };
            objects
                .set_spec(
                    object_ref(Kind::Service, namespace, service),
                    "paused",
                    pause.into(),
                )
                .await
                .map(|(key, changed)| {
                    said(
                        changed,
                        format!("{key} {what}"),
                        format!("{key} already {what}"),
                    )
                })
        }
        Request::Suspend {
            namespace,
            cron_job,
            suspend,
        } => {
            let what = if suspend { "suspended" } else { "resumed" };
            objects
                .set_spec(
                    object_ref(Kind::CronJob, namespace, cron_job),
                    "suspend",
                    suspend.into(),
                )
                .await
                .map(|(key, changed)| {
                    said(
                        changed,
                        format!("{key} {what}"),
                        format!("{key} already {what}"),
                    )
                })
        }
        Request::Move {
            namespace,
            volume,
            node,
        } => objects
            .set_spec(
                object_ref(Kind::Volume, namespace, volume),
                "node",
                node.clone().into(),
            )
            .await
            .map(|(key, changed)| {
                said(
                    changed,
                    format!("{key} moves to {node}"),
                    format!("{key} is already asked to be on {node}"),
                )
            }),
        Request::Delete {
            kind,
            namespace,
            name,
        } => {
            let r#ref = object_ref(kind, namespace, name);
            let key = format!("{}/{}/{}", r#ref.kind, r#ref.namespace, r#ref.name);
            objects
                .delete(pb::DeleteRequest {
                    r#ref: Some(r#ref),
                    expected_version: None,
                })
                .await
                .map(|_| format!("{} deleted", key.replace("//", "/")))
                .map_err(Into::into)
        }
        Request::Logs { .. }
        | Request::ServiceLogs { .. }
        | Request::Exec { .. }
        | Request::Input(_)
        | Request::Events { .. }
        | Request::Alerts
        | Request::Run { .. }
        | Request::Edit { .. }
        | Request::Batch(_) => {
            unreachable!("not asked of the API")
        }
    };
    result.unwrap_or_else(|e| format!("Failed: {e:#}"))
}

/// Lists, then follows the watch; again from the start when it breaks.
async fn follow(channel: Channel, namespace: Option<String>, tx: mpsc::UnboundedSender<Msg>) {
    let mut objects = crate::cli::objects_client(channel);
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
    let (listed, hidden, versions) = list(objects, namespace).await?;
    let from = versions.values().copied().min().unwrap_or_default();
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
        let o = Object::try_from(o).map_err(|e| e.to_string())?;
        // The watch starts at the oldest listing: what a newer one of its
        // kind already holds (or held and lost) is no news.
        if versions
            .get(&o.kind)
            .is_some_and(|&v| o.metadata.version <= v)
        {
            continue;
        }
        if tx.send(Msg::Changed(put, o)).is_err() {
            return Ok(());
        }
    }
}

/// Every object the caller may list, the kinds they may not, and the
/// version each kind was listed at.
async fn list(
    objects: &mut ObjectsClient<Channel>,
    namespace: &Option<String>,
) -> Result<(Vec<Object>, BTreeSet<Kind>, BTreeMap<Kind, u64>), String> {
    let mut listed = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut versions = BTreeMap::new();
    for kind in Kind::ALL {
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
                versions.insert(kind, r.version);
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
    if versions.is_empty() {
        return Err("your role bindings let you read nothing".into());
    }
    Ok((listed, hidden, versions))
}

/// `--once`: every tab as text, as `top -b` would.
pub async fn once(channel: Channel, namespace: Option<String>) -> anyhow::Result<()> {
    let mut objects = crate::cli::objects_client(channel.clone());
    let (listed, hidden, _) = list(&mut objects, &namespace)
        .await
        .map_err(anyhow::Error::msg)?;
    let mut model = Model::new(namespace.clone());
    model.listed(listed, hidden);
    let mut cluster = crate::cli::cluster_client(channel);
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
    if let Ok(r) = cluster
        .activity(pb::ActivityRequest { follow: false })
        .await
        && let Ok(Some(r)) = r.into_inner().message().await
    {
        for op in r
            .operations_json
            .iter()
            .filter_map(|j| serde_json::from_slice::<legion_api::activity::Operation>(j).ok())
        {
            model.activity.insert(op.id.clone(), op);
        }
    }
    let mut out = draw::plain_summary(&model);
    out.push('\n');
    for tab in view::TABS.into_iter().filter(|t| *t != view::Tab::Log) {
        let view = View {
            tab,
            ..View::default()
        };
        if tab == view::Tab::Activity && model.activity.is_empty() {
            continue;
        }
        if tab != view::Tab::Activity && tab.kinds().iter().all(|k| model.hidden.contains(k)) {
            continue;
        }
        out.push_str(&format!("\n{}\n", tab.title()));
        out.push_str(&draw::plain(&model, &view));
    }
    print!("{out}");
    Ok(())
}

/// Asks for usage and the managers' state every configured poll interval.
async fn poll(
    channel: Channel,
    namespace: Option<String>,
    mut interval_rx: watch::Receiver<Duration>,
    tx: mpsc::UnboundedSender<Msg>,
) {
    let mut cluster = crate::cli::cluster_client(channel);
    let mut dur = *interval_rx.borrow_and_update();
    let mut tick = tokio::time::interval(dur);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            changed = interval_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let next = *interval_rx.borrow_and_update();
                if next != dur {
                    dur = next;
                    tick = tokio::time::interval(dur);
                    tick.tick().await;
                }
            }
        }
        // What failed is said in the header until it reads again, so the
        // figures shown are not taken as current.
        let mut failed = None;
        match cluster.status(StatusRequest {}).await {
            Ok(s) => {
                if tx.send(Msg::Status(s.into_inner())).is_err() {
                    return;
                }
            }
            Err(e) => failed = Some(format!("the cluster's status: {}", e.message())),
        }
        let req = pb::UsageRequest {
            namespace: namespace.clone().unwrap_or_default(),
        };
        match cluster.usage(req).await {
            Ok(u) => {
                if tx.send(Msg::Usage(u.into_inner())).is_err() {
                    return;
                }
            }
            // A manager too old to have usage says nothing of the rest.
            Err(e) if e.code() == tonic::Code::Unimplemented => {}
            Err(e) => {
                failed.get_or_insert_with(|| format!("what tasks use: {}", e.message()));
            }
        }
        if tx.send(Msg::Polled(failed)).is_err() {
            return;
        }
    }
}
