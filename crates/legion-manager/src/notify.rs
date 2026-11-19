//! Alerts sent (CONSOLE.md, phase 9): the leader tells each `AlertReceiver`
//! of every alert it chooses once as it is raised and once as it clears,
//! by a webhook, ntfy or mail. What each was told is kept in its status,
//! so a new leader carries on from there: nothing sent twice that was
//! written as sent, nothing that clears left untold. A receiver that fails
//! is tried again after 5 s, then twice as long each time up to 5 minutes,
//! and says why in its status meanwhile.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use legion_api::alert::{Alert, ReceiverSpec, ReceiverStatus, SecretRef, SmtpTls};
use legion_api::{Kind, ObjectKey};
use legion_store::{Command, Outcome, Precondition, Store};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::api::ApiService;

/// How often the leader looks for what to send.
const PASS: Duration = Duration::from_secs(5);
/// How long an alert must be gone before it is said to clear: a new
/// leader, its counts of restarts starting over, would otherwise clear
/// and raise again what goes on.
const CLEARED_AFTER: Duration = Duration::from_secs(15);
/// The wait after a receiver's first failure, and the longest.
const RETRY: Duration = Duration::from_secs(5);
const RETRY_MAX: Duration = Duration::from_secs(300);
/// How long one delivery may take.
const TIMEOUT: Duration = Duration::from_secs(20);

/// What a receiver is told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Change {
    Raised,
    Cleared,
    /// `legion alerts test`: nothing is raised.
    Test,
}

impl Change {
    fn as_str(self) -> &'static str {
        match self {
            Change::Raised => "raised",
            Change::Cleared => "cleared",
            Change::Test => "test",
        }
    }
}

/// One message: an alert and what became of it.
#[derive(Debug, Clone)]
pub(crate) struct Note {
    pub cluster: String,
    pub change: Change,
    pub alert: Alert,
    pub since_ms: i64,
}

impl Note {
    /// A line for a title or a subject.
    fn title(&self) -> String {
        let what = if self.alert.object.is_empty() {
            self.alert.kind.as_str().to_owned()
        } else if self.alert.namespace.is_empty() {
            format!("{} {}", self.alert.kind.as_str(), self.alert.object)
        } else {
            format!(
                "{} {}/{}",
                self.alert.kind.as_str(),
                self.alert.namespace,
                self.alert.object
            )
        };
        match self.change {
            Change::Raised => format!("[{}] {}: {what}", self.cluster, self.alert.severity.as_str()),
            Change::Cleared => format!("[{}] cleared: {what}", self.cluster),
            Change::Test => format!("[{}] a test from Legion", self.cluster),
        }
    }

    fn body(&self) -> String {
        match self.change {
            Change::Raised => self.alert.message.clone(),
            Change::Cleared => format!("No longer so: {}", self.alert.message),
            Change::Test => "This receiver works: Legion's alerts will come here.".to_owned(),
        }
    }

    /// The webhook's JSON.
    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "cluster": self.cluster,
            "state": self.change.as_str(),
            "id": self.alert.key(),
            "kind": self.alert.kind.as_str(),
            "severity": self.alert.severity.as_str(),
            "namespace": self.alert.namespace,
            "object": self.alert.object,
            "message": self.alert.message,
            "sinceMs": self.since_ms,
        })
    }
}

/// What a header line may hold: no line ends, nothing beyond ASCII.
fn header_safe(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii() && !c.is_ascii_control() { c } else { ' ' })
        .collect()
}

/// An alert as `key` (`kind/namespace/object`) names it, for one that
/// cleared and is no longer worked out.
fn from_key(key: &str) -> Option<Alert> {
    let mut parts = key.splitn(3, '/');
    let kind = parts.next()?;
    let namespace = parts.next()?.to_owned();
    let object = parts.next().unwrap_or_default().to_owned();
    let kind = legion_api::alert::KINDS.into_iter().find(|k| k.as_str() == kind)?;
    Some(Alert {
        kind,
        severity: legion_api::alert::Severity::Warning,
        namespace,
        message: format!("{}{}", kind.as_str(), if object.is_empty() { String::new() } else { format!(" on {object}") }),
        object,
    })
}

fn secret(store: &Store, r: Option<&SecretRef>) -> Result<Option<String>, String> {
    let Some(r) = r else { return Ok(None) };
    let key = ObjectKey::new(Kind::Secret, &r.namespace, &r.name);
    let data = crate::acme::read_data(store, &key)?
        .ok_or_else(|| format!("its Secret {}/{} is not there", r.namespace, r.name))?;
    let text = String::from_utf8(data)
        .map_err(|_| format!("its Secret {}/{} is not text", r.namespace, r.name))?;
    Ok(Some(text.trim().to_owned()))
}

fn http() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        reqwest::Client::builder()
            .user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("an HTTP client")
    })
}

/// Sends `note` as `spec` says; why not, if it was not taken.
pub(crate) async fn send(store: &Store, spec: &ReceiverSpec, note: &Note) -> Result<(), String> {
    let token = secret(store, spec.secret())?;
    if let Some(w) = &spec.webhook {
        let mut req = http().post(&w.url).json(&note.json());
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        return answered(req.send().await);
    }
    if let Some(n) = &spec.ntfy {
        let critical = note.alert.severity == legion_api::alert::Severity::Critical;
        let (priority, tag) = match note.change {
            Change::Raised if critical => ("high", "rotating_light"),
            Change::Raised => ("default", "warning"),
            Change::Cleared => ("low", "white_check_mark"),
            Change::Test => ("low", "wave"),
        };
        let mut req = http()
            .post(&n.url)
            .header("Title", header_safe(&note.title()))
            .header("Priority", priority)
            .header("Tags", tag)
            .body(note.body());
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        return answered(req.send().await);
    }
    if let Some(s) = &spec.smtp {
        return tokio::time::timeout(TIMEOUT, mail(s, token.as_deref(), note))
            .await
            .map_err(|_| format!("the mail server took more than {}s", TIMEOUT.as_secs()))?;
    }
    Err("it says nowhere to send".to_owned())
}

fn answered(r: reqwest::Result<reqwest::Response>) -> Result<(), String> {
    let r = r.map_err(|e| format!("sending: {e}"))?;
    if r.status().is_success() {
        Ok(())
    } else {
        Err(format!("answered {}", r.status()))
    }
}

/// A mail of `note` through `s` (RFC 5321): EHLO, STARTTLS when asked,
/// AUTH PLAIN with a login, then the envelope and the message.
async fn mail(s: &legion_api::alert::Smtp, password: Option<&str>, note: &Note) -> Result<(), String> {
    let (host, _) = s
        .server
        .rsplit_once(':')
        .ok_or("the server is host:port")?;
    let tcp = tokio::net::TcpStream::connect(&s.server)
        .await
        .map_err(|e| format!("connecting to {}: {e}", s.server))?;
    match s.tls {
        SmtpTls::Tls => {
            let tls = tls(host, tcp).await?;
            let mut c = Conn::new(tls);
            c.expect(220).await?;
            converse(&mut c, s, password, note).await
        }
        SmtpTls::None => {
            let mut c = Conn::new(tcp);
            c.expect(220).await?;
            converse(&mut c, s, password, note).await
        }
        SmtpTls::Starttls => {
            let mut c = Conn::new(tcp);
            c.expect(220).await?;
            c.command("EHLO legion", 250).await?;
            c.command("STARTTLS", 220).await?;
            let tcp = c.into_inner();
            let mut c = Conn::new(tls(host, tcp).await?);
            converse(&mut c, s, password, note).await
        }
    }
}

async fn tls(
    host: &str,
    tcp: tokio::net::TcpStream,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, String> {
    use rustls_platform_verifier::ConfigVerifierExt;
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let config = tokio_rustls::rustls::ClientConfig::with_platform_verifier()
        .map_err(|e| format!("TLS: {e}"))?;
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| format!("TLS to {host}: {e}"))?;
    tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect(name, tcp)
        .await
        .map_err(|e| format!("TLS to {host}: {e}"))
}

async fn converse<S: AsyncRead + AsyncWrite + Unpin>(
    c: &mut Conn<S>,
    s: &legion_api::alert::Smtp,
    password: Option<&str>,
    note: &Note,
) -> Result<(), String> {
    use base64::Engine as _;
    c.command("EHLO legion", 250).await?;
    if let Some(password) = password {
        let plain = base64::engine::general_purpose::STANDARD
            .encode(format!("\0{}\0{password}", s.username));
        c.command(&format!("AUTH PLAIN {plain}"), 235).await?;
    }
    c.command(&format!("MAIL FROM:<{}>", s.from), 250).await?;
    for to in &s.to {
        c.command(&format!("RCPT TO:<{to}>"), 250).await?;
    }
    c.command("DATA", 354).await?;
    let date = httpdate(std::time::SystemTime::now());
    let mut message = format!(
        "From: Legion <{}>\r\nTo: {}\r\nSubject: {}\r\nDate: {date}\r\nMIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n",
        s.from,
        s.to.join(", "),
        header_safe(&note.title()),
    );
    for line in note.body().lines().chain(["", &format!("({}: {})", note.alert.key(), note.change.as_str())]) {
        // A line starting with a dot is sent with another (RFC 5321 4.5.2).
        if line.starts_with('.') {
            message.push('.');
        }
        message.push_str(line);
        message.push_str("\r\n");
    }
    message.push_str(".\r\n");
    c.write(&message).await?;
    c.expect(250).await?;
    let _ = c.command("QUIT", 221).await;
    Ok(())
}

/// A mail's `Date:` (RFC 5322), in UTC.
fn httpdate(at: std::time::SystemTime) -> String {
    let secs = at
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as i64;
    let (days, rest) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let weekday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"][days.rem_euclid(7) as usize];
    // Civil date from days since the epoch (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    format!(
        "{weekday}, {day:02} {} {year} {:02}:{:02}:{:02} +0000",
        names[(month - 1) as usize],
        rest / 3600,
        rest / 60 % 60,
        rest % 60
    )
}

/// A conversation with a mail server: commands out, replies in.
struct Conn<S> {
    io: BufReader<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Conn<S> {
    fn new(io: S) -> Self {
        Conn { io: BufReader::new(io) }
    }

    fn into_inner(self) -> S {
        self.io.into_inner()
    }

    async fn write(&mut self, s: &str) -> Result<(), String> {
        self.io
            .get_mut()
            .write_all(s.as_bytes())
            .await
            .map_err(|e| format!("to the mail server: {e}"))?;
        self.io.get_mut().flush().await.map_err(|e| format!("to the mail server: {e}"))
    }

    /// A reply, its lines joined; an error unless its code is `want`.
    async fn expect(&mut self, want: u16) -> Result<(), String> {
        let mut said = Vec::new();
        loop {
            let mut line = String::new();
            let n = self
                .io
                .read_line(&mut line)
                .await
                .map_err(|e| format!("from the mail server: {e}"))?;
            if n == 0 {
                return Err("the mail server closed the connection".to_owned());
            }
            let line = line.trim_end().to_owned();
            let last = line.as_bytes().get(3) != Some(&b'-');
            said.push(line);
            if last {
                break;
            }
        }
        let code: u16 = said[0].get(..3).and_then(|c| c.parse().ok()).unwrap_or(0);
        // 251: a recipient taken on to be passed on.
        if code == want || (want == 250 && code == 251) {
            Ok(())
        } else {
            Err(format!("the mail server said: {}", said.join(" / ")))
        }
    }

    async fn command(&mut self, line: &str, want: u16) -> Result<(), String> {
        self.write(&format!("{line}\r\n")).await?;
        self.expect(want).await
    }
}

/// Sends a test through the receiver `name` now; why not, if it failed.
pub(crate) async fn test(store: &Store, name: &str) -> Result<(), String> {
    let key = ObjectKey::new(Kind::AlertReceiver, "", name);
    let o = store
        .get(&key)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no AlertReceiver {name}"))?;
    let spec: ReceiverSpec = legion_api::parse(&o.spec).map_err(|e| e.to_string())?;
    let cluster = crate::console::settings(store)
        .ok()
        .and_then(|s| s.name)
        .unwrap_or_else(|| "legion".to_owned());
    let note = Note {
        cluster,
        change: Change::Test,
        alert: Alert {
            kind: legion_api::alert::AlertKind::NodeSuspect,
            severity: legion_api::alert::Severity::Warning,
            namespace: String::new(),
            object: String::new(),
            message: "a test from Legion".to_owned(),
        },
        since_ms: crate::console::now_ms(),
    };
    send(store, &spec, &note).await
}

/// A receiver's failures in a row, and when it may be tried again.
#[derive(Default)]
struct Failing {
    count: u32,
    next: Option<Instant>,
}

/// Runs while this manager leads: each [`PASS`], every receiver is told
/// what was raised and what cleared since it was last told.
pub(crate) async fn run(api: ApiService) {
    let mut closing = api.closing.clone();
    let mut metrics = api.store.raft.metrics();
    let mut failing: HashMap<String, Failing> = HashMap::new();
    // Since when each alert told of is gone, by receiver and key.
    let mut gone: HashMap<(String, String), Instant> = HashMap::new();
    loop {
        if *closing.borrow() {
            return;
        }
        if metrics.borrow().current_leader != Some(api.id) {
            // Another's to send; what it does is in the store.
            failing.clear();
            gone.clear();
            tokio::select! {
                changed = metrics.changed() => if changed.is_err() { return },
                _ = closing.changed() => {}
            }
            continue;
        }
        pass(&api, &mut failing, &mut gone).await;
        tokio::select! {
            () = tokio::time::sleep(PASS) => {}
            _ = closing.wait_for(|c| *c) => return,
        }
    }
}

async fn pass(
    api: &ApiService,
    failing: &mut HashMap<String, Failing>,
    gone: &mut HashMap<(String, String), Instant>,
) {
    let receivers = match api.store.list(Kind::AlertReceiver, None) {
        Ok(l) => l.objects,
        Err(e) => {
            tracing::debug!("alert receivers: {e}");
            return;
        }
    };
    if receivers.is_empty() {
        return;
    }
    let alerts = crate::alerts::current(api);
    let cluster = crate::console::settings(&api.store)
        .ok()
        .and_then(|s| s.name)
        .unwrap_or_else(|| "legion".to_owned());
    let now = Instant::now();
    for r in receivers {
        let name = r.metadata.name.clone();
        if failing.get(&name).and_then(|f| f.next).is_some_and(|t| t > now) {
            continue;
        }
        let Ok(spec) = legion_api::parse::<ReceiverSpec>(&r.spec) else {
            continue;
        };
        let status: ReceiverStatus = serde_json::from_value(r.status.clone()).unwrap_or_default();
        let wanted: Vec<&(Alert, i64)> = alerts.iter().filter(|(a, _)| spec.wants(a)).collect();
        let keys: HashSet<String> = wanted.iter().map(|(a, _)| a.key()).collect();
        let told: HashSet<&String> = status.sent.iter().collect();
        let mut notes: Vec<(Note, String)> = wanted
            .iter()
            .filter(|(a, _)| !told.contains(&a.key()))
            .map(|(a, since)| {
                (
                    Note {
                        cluster: cluster.clone(),
                        change: Change::Raised,
                        alert: a.clone(),
                        since_ms: *since,
                    },
                    a.key(),
                )
            })
            .collect();
        for key in &status.sent {
            let at = (name.clone(), key.clone());
            if keys.contains(key) {
                gone.remove(&at);
                continue;
            }
            let since = *gone.entry(at).or_insert(now);
            if now.duration_since(since) < CLEARED_AFTER {
                continue;
            }
            if let Some(alert) = from_key(key) {
                notes.push((
                    Note {
                        cluster: cluster.clone(),
                        change: Change::Cleared,
                        alert,
                        since_ms: crate::console::now_ms(),
                    },
                    key.clone(),
                ));
            }
        }
        if notes.is_empty() {
            continue;
        }
        let mut sent: Vec<String> = status.sent.clone();
        let mut delivered = false;
        let mut error = None;
        for (note, key) in &notes {
            match send(&api.store, &spec, note).await {
                Ok(()) => {
                    delivered = true;
                    match note.change {
                        Change::Raised => sent.push(key.clone()),
                        _ => {
                            sent.retain(|k| k != key);
                            gone.remove(&(name.clone(), key.clone()));
                        }
                    }
                    tracing::info!(receiver = %name, alert = %key, "alert {}", note.change.as_str());
                }
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        let f = failing.entry(name.clone()).or_default();
        if let Some(e) = &error {
            f.count += 1;
            let wait = RETRY
                .saturating_mul(2u32.saturating_pow(f.count - 1))
                .min(RETRY_MAX);
            f.next = Some(now + wait);
            tracing::warn!(receiver = %name, attempt = f.count, "sending alerts: {e} (again in {}s)", wait.as_secs());
        } else {
            *f = Failing::default();
        }
        sent.sort();
        sent.dedup();
        let at = crate::console::now_ms();
        record(&api.store, &name, |s| {
            s.sent.clone_from(&sent);
            if delivered {
                s.delivered_ms = Some(at);
            }
            match &error {
                Some(e) => {
                    s.error.clone_from(e);
                    s.error_ms = Some(at);
                }
                None => {
                    s.error.clear();
                    s.error_ms = None;
                }
            }
        })
        .await;
    }
}

/// Writes a receiver's status as `change` makes it, over what is there.
pub(crate) async fn record(store: &Store, name: &str, change: impl Fn(&mut ReceiverStatus)) {
    let key = ObjectKey::new(Kind::AlertReceiver, "", name);
    for _ in 0..5 {
        let Ok(Some(mut o)) = store.get(&key) else {
            return;
        };
        let mut s: ReceiverStatus = serde_json::from_value(o.status.clone()).unwrap_or_default();
        let before = s.clone();
        change(&mut s);
        if s == before {
            return;
        }
        o.status = serde_json::to_value(&s).expect("serializes");
        let version = o.metadata.version;
        let command = Command::Put {
            object: o,
            precondition: Precondition::Version(version),
        };
        match tokio::time::timeout(crate::COMMIT_TIMEOUT, store.raft.client_write(command)).await {
            Ok(Ok(out)) => match out.data {
                Outcome::Written(_) => return,
                Outcome::Conflict { .. } => continue,
                other => {
                    tracing::warn!(receiver = %name, "its status: {other:?}");
                    return;
                }
            },
            Ok(Err(e)) => {
                tracing::warn!(receiver = %name, "its status: {e}");
                return;
            }
            Err(_) => {
                tracing::warn!(receiver = %name, "its status: no quorum to write it");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_api::alert::{AlertKind, Severity};

    fn note(change: Change) -> Note {
        Note {
            cluster: "lab".into(),
            change,
            alert: Alert {
                kind: AlertKind::NodeDown,
                severity: Severity::Critical,
                namespace: String::new(),
                object: "Node/n2".into(),
                message: "node n2 is down\r\nBcc: x".into(),
            },
            since_ms: 1000,
        }
    }

    #[test]
    fn a_note_reads_as_its_change() {
        assert_eq!(note(Change::Raised).title(), "[lab] critical: node-down Node/n2");
        assert_eq!(note(Change::Cleared).title(), "[lab] cleared: node-down Node/n2");
        let j = note(Change::Raised).json();
        assert_eq!(j["state"], "raised");
        assert_eq!(j["id"], "node-down//Node/n2");
        assert_eq!(j["sinceMs"], 1000);
        // A header carries no line end however the alert reads.
        assert!(!header_safe(&note(Change::Raised).body()).contains(['\r', '\n']));
    }

    #[test]
    fn an_alert_gone_is_known_again_from_its_key() {
        let a = from_key("node-down//Node/n2").unwrap();
        assert_eq!((a.kind, a.namespace.as_str(), a.object.as_str()), (AlertKind::NodeDown, "", "Node/n2"));
        assert_eq!(a.key(), "node-down//Node/n2");
        let a = from_key("crash-loop/shop/Task/web-1").unwrap();
        assert_eq!((a.namespace.as_str(), a.object.as_str()), ("shop", "Task/web-1"));
        assert!(from_key("no-such-kind//x").is_none());
    }

    #[test]
    fn a_mails_date_is_rfc_5322() {
        let at = std::time::UNIX_EPOCH + Duration::from_secs(1_795_035_876);
        assert_eq!(httpdate(at), "Wed, 18 Nov 2026 21:04:36 +0000");
        assert_eq!(httpdate(std::time::UNIX_EPOCH), "Thu, 01 Jan 1970 00:00:00 +0000");
    }

    /// A mail server of a few lines: answers each command, keeps what it
    /// was sent.
    #[tokio::test]
    async fn a_mail_goes_through_a_server_without_tls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut io = BufReader::new(sock);
            let mut got = String::new();
            io.get_mut().write_all(b"220 test ESMTP\r\n").await.unwrap();
            let mut in_data = false;
            loop {
                let mut line = String::new();
                if io.read_line(&mut line).await.unwrap() == 0 {
                    break;
                }
                got.push_str(&line);
                let reply: &[u8] = if in_data {
                    if line == ".\r\n" {
                        in_data = false;
                        b"250 queued\r\n"
                    } else {
                        continue;
                    }
                } else if line.starts_with("EHLO") {
                    b"250-test\r\n250 AUTH PLAIN\r\n"
                } else if line.starts_with("AUTH PLAIN") {
                    b"235 ok\r\n"
                } else if line.starts_with("DATA") {
                    in_data = true;
                    b"354 go on\r\n"
                } else if line.starts_with("QUIT") {
                    io.get_mut().write_all(b"221 bye\r\n").await.unwrap();
                    break;
                } else {
                    b"250 ok\r\n"
                };
                io.get_mut().write_all(reply).await.unwrap();
            }
            got
        });
        let smtp = legion_api::alert::Smtp {
            server: addr.to_string(),
            tls: SmtpTls::None,
            from: "legion@example.com".into(),
            to: vec!["ops@example.com".into()],
            username: "legion".into(),
            secret: None,
        };
        let mut n = note(Change::Raised);
        n.alert.message = "line one\n.dot line".into();
        mail(&smtp, Some("pw"), &n).await.unwrap();
        let got = server.await.unwrap();
        assert!(got.contains("MAIL FROM:<legion@example.com>\r\n"), "{got}");
        assert!(got.contains("RCPT TO:<ops@example.com>\r\n"), "{got}");
        assert!(got.contains("Subject: [lab] critical: node-down Node/n2\r\n"), "{got}");
        assert!(got.contains("\r\n..dot line\r\n"), "a dot line is doubled: {got}");
        assert!(got.contains("AUTH PLAIN AGxlZ2lvbgBwdw==\r\n"), "{got}");
    }
}
