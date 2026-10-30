//! `legion top`: the cluster, live, in the terminal. It lists every kind
//! of object the caller may read, then follows a watch to keep them
//! current; asks the leader every few seconds what tasks use; and keeps a
//! log of what changed while it ran. Keys act on the selected row: drain,
//! pause, label or delete a node; scale or roll back a service; suspend a
//! cron job; move a volume; delete anything; follow logs in a pane under
//! the table; open a shell, by running the CLI itself, which has the
//! terminal until it ends.

mod draw;
mod logs;
mod model;
#[cfg(test)]
mod tests;
mod view;

use std::collections::BTreeSet;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use legion_api::{Kind, Object};
use legion_proto::v1::cluster_client::ClusterClient;
use legion_proto::v1::objects_client::ObjectsClient;
use legion_proto::v1::{
    self as pb, ListRequest, ObjectRef, StatusRequest, WatchRequest, watch_event,
};
use ratatui::backend::Backend;
use ratatui::crossterm::event::{self, Event, KeyEvent, KeyEventKind};
use ratatui::{Terminal, crossterm};
use tokio::sync::mpsc;
use tonic::transport::Channel;

pub use model::Model;
pub use view::{Request, View};

/// How often it asks for usage and the managers' state.
const POLL: Duration = Duration::from_secs(5);
/// How often every cell is written again, not only those that changed:
/// what else wrote to the terminal (console messages) goes.
const REPAINT: Duration = Duration::from_secs(5);

/// What the screen hears of.
enum Msg {
    Listed(Vec<Object>, BTreeSet<Kind>),
    Changed(bool, Object),
    Usage(pb::UsageResponse),
    Status(pb::StatusResponse),
    Problem(String),
    Key(KeyEvent),
    /// What came of a request.
    Done(String),
    /// Logs of the stream numbered so, on stderr or not.
    Log(u64, bool, Vec<u8>),
    /// That stream ended, and why.
    LogEnded(u64, String),
    Redraw,
    /// The terminal is gone.
    HungUp,
}

/// How to run the CLI again, for logs and shells.
pub struct Cli {
    pub server: String,
    pub identity: PathBuf,
}

/// Runs until the person quits or the terminal goes.
pub async fn run(channel: Channel, cli: Cli, namespace: Option<String>) -> anyhow::Result<()> {
    draw::set_mono(std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let follower = tokio::spawn(follow(channel.clone(), namespace.clone(), tx.clone()));
    let poller = tokio::spawn(poll(channel.clone(), namespace.clone(), tx.clone()));
    // Ctrl-C reaches a shell run from here as well as this process,
    // which stays.
    tokio::spawn(async { while tokio::signal::ctrl_c().await.is_ok() {} });
    let paused = Arc::new(AtomicBool::new(false));
    read_keys(tx.clone(), paused.clone());

    // The logs shown, numbered: what an earlier stream sent is dropped.
    let mut logs: Option<(u64, tokio::task::JoinHandle<()>)> = None;
    let mut streams = 0u64;
    let objects = crate::cli::Objects::new(channel.clone());
    let mut model = Model::new(namespace);
    let mut view = View::default();
    let mut terminal = ratatui::init();
    let mut painted = Instant::now();
    let result = loop {
        if painted.elapsed() >= REPAINT {
            invalidate(&mut terminal);
            painted = Instant::now();
        }
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
                Msg::Key(k) => requests.extend(view.key(&model, k)),
                Msg::Done(what) => view.notice = Some(what),
                Msg::Log(n, stderr, data) => {
                    if let (Some(l), Some((shown, _))) = (&mut view.logs, &logs)
                        && n == *shown
                    {
                        l.push(stderr, &data);
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
                Msg::HungUp => view.quit = true,
            }
            next = rx.try_recv().ok();
        }
        for request in requests {
            if let Request::Logs { .. } = &request {
                if let Some((_, stream)) = logs.take() {
                    stream.abort();
                }
                streams += 1;
                view.show_logs(&request);
                let stream =
                    tokio::spawn(stream_logs(channel.clone(), request, streams, tx.clone()));
                logs = Some((streams, stream));
            } else if let Some(args) = command(&request) {
                paused.store(true, Ordering::SeqCst);
                // A poll under way ends first.
                tokio::time::sleep(Duration::from_millis(150)).await;
                ratatui::restore();
                view.notice = Some(hand_over(&cli, args).await);
                terminal = ratatui::init();
                paused.store(false, Ordering::SeqCst);
            } else {
                view.notice = Some("Asking…".into());
                let (objects, tx) = (objects.clone(), tx.clone());
                tokio::spawn(async move {
                    let _ = tx.send(Msg::Done(act(objects, request).await));
                });
            }
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
    result
}

/// Reads keys on a thread of its own, except while a command run from
/// here has the terminal. crossterm reads a hung-up terminal forever
/// (jmon found it), so it stops once stdin is no longer a terminal.
fn read_keys(tx: mpsc::UnboundedSender<Msg>, paused: Arc<AtomicBool>) {
    let watch_tty = std::io::stdin().is_terminal();
    std::thread::spawn(move || {
        loop {
            if watch_tty && !std::io::stdin().is_terminal() {
                let _ = tx.send(Msg::HungUp);
                return;
            }
            if paused.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
                continue;
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

/// The CLI's arguments for a request it runs itself, with what to tell
/// the person first.
fn command(request: &Request) -> Option<(String, Vec<String>)> {
    let args = |a: &[&str], container: &Option<String>| {
        let mut v: Vec<String> = a.iter().map(|s| (*s).to_owned()).collect();
        if let Some(c) = container {
            v.extend(["-c".to_owned(), c.clone()]);
        }
        v
    };
    match request {
        Request::Exec {
            namespace,
            task,
            container,
        } => {
            let mut a = args(&["exec", "-it", "-n", namespace, task], container);
            a.extend(["--".to_owned(), "/bin/sh".to_owned()]);
            Some((
                format!("A shell in {namespace}/{task}; exit returns to legion top."),
                a,
            ))
        }
        _ => None,
    }
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

/// Runs the CLI with the terminal until it ends; what came of it.
async fn hand_over(cli: &Cli, (banner, args): (String, Vec<String>)) -> String {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return format!("Failed: cannot run legion again: {e}"),
    };
    let mut command = std::process::Command::new(exe);
    command
        .arg("--server")
        .arg(&cli.server)
        .arg("--identity")
        .arg(&cli.identity)
        .args(&args);
    println!("{banner}");
    let done = tokio::task::spawn_blocking(move || {
        let status = command.status();
        // An error stays on the screen until read.
        if status
            .as_ref()
            .is_ok_and(|s| s.code().is_some_and(|c| c != 0))
        {
            println!("[Enter returns to legion top]");
            let _ = std::io::stdin().read_line(&mut String::new());
        }
        status
    })
    .await;
    match done {
        Ok(Ok(s)) if s.success() => "Back.".into(),
        Ok(Ok(s)) => match s.code() {
            Some(code) => format!("Not done: it ended with {code}."),
            None => "Back.".into(),
        },
        Ok(Err(e)) => format!("Failed: cannot run legion again: {e}"),
        Err(e) => format!("Failed: cannot run legion again: {e}"),
    }
}

fn object_ref(kind: Kind, namespace: String, name: String) -> ObjectRef {
    ObjectRef {
        kind: kind.to_string(),
        namespace,
        name,
    }
}

/// Makes a request over the API; what came of it, for people.
async fn act(mut objects: crate::cli::Objects, request: Request) -> String {
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
        Request::Logs { .. } | Request::Exec { .. } => unreachable!("not asked of the API"),
    };
    result.unwrap_or_else(|e| format!("Failed: {e:#}"))
}

/// Lists, then follows the watch; again from the start when it breaks.
async fn follow(channel: Channel, namespace: Option<String>, tx: mpsc::UnboundedSender<Msg>) {
    let mut objects = ObjectsClient::new(channel);
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
    let (listed, hidden, from) = list(objects, namespace).await?;
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
        if tx.send(Msg::Changed(put, o)).is_err() {
            return Ok(());
        }
    }
}

/// Every object the caller may list, the kinds they may not, and the
/// version to watch from: the oldest listing's.
async fn list(
    objects: &mut ObjectsClient<Channel>,
    namespace: &Option<String>,
) -> Result<(Vec<Object>, BTreeSet<Kind>, u64), String> {
    let mut listed = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut from = None::<u64>;
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
                from = Some(from.map_or(r.version, |v| v.min(r.version)));
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
    let Some(from) = from else {
        return Err("your role bindings let you read nothing".into());
    };
    Ok((listed, hidden, from))
}

/// `--once`: every tab as text, as `top -b` would.
pub async fn once(channel: Channel, namespace: Option<String>) -> anyhow::Result<()> {
    let mut objects = ObjectsClient::new(channel.clone());
    let (listed, hidden, _) = list(&mut objects, &namespace)
        .await
        .map_err(anyhow::Error::msg)?;
    let mut model = Model::new(namespace.clone());
    model.listed(listed, hidden);
    let mut cluster = ClusterClient::new(channel);
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
    let mut out = draw::plain_summary(&model);
    out.push('\n');
    for tab in view::TABS.into_iter().filter(|t| *t != view::Tab::Log) {
        let view = View {
            tab,
            ..View::default()
        };
        if tab.kinds().iter().all(|k| model.hidden.contains(k)) {
            continue;
        }
        out.push_str(&format!("\n{}\n", tab.title()));
        out.push_str(&draw::plain(&model, &view));
    }
    print!("{out}");
    Ok(())
}

/// Asks for usage and the managers' state every [`POLL`].
async fn poll(channel: Channel, namespace: Option<String>, tx: mpsc::UnboundedSender<Msg>) {
    let mut cluster = ClusterClient::new(channel);
    let mut tick = tokio::time::interval(POLL);
    loop {
        tick.tick().await;
        if let Ok(s) = cluster.status(StatusRequest {}).await
            && tx.send(Msg::Status(s.into_inner())).is_err()
        {
            return;
        }
        // A manager too old to answer, or a leader just elected: none yet.
        let req = pb::UsageRequest {
            namespace: namespace.clone().unwrap_or_default(),
        };
        if let Ok(u) = cluster.usage(req).await
            && tx.send(Msg::Usage(u.into_inner())).is_err()
        {
            return;
        }
    }
}
