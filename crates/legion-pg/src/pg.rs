//! The operations, over a [`Host`] that runs PostgreSQL's tools: the
//! real one ([`System`]) or a test's.

use std::path::{Path, PathBuf};

use legion_api::replication::spi::{
    self, Capabilities, Describe, Lag, Member, Position, Request, Role, Status,
};
use serde::Deserialize;
use serde_json::{Value, json};

/// The configuration file this plugin owns, included from
/// `postgresql.conf`.
pub const CONF: &str = "legion-replication.conf";
const INCLUDE: &str = "include_if_exists = 'legion-replication.conf'";
/// The replication password, as libpq's password file, for a replica's
/// `primary_conninfo`.
pub const PASSFILE: &str = "legion-replication.pgpass";
/// The spool, in the data directory, that `archive_command` leaves each
/// finished WAL file in for Legion to ship (`replication.archive`).
pub const SPOOL: &str = "legion_wal";
/// Files the spool holds at most: past them `archive-wal` fails, so
/// PostgreSQL keeps its WAL (and says why) rather than the spool filling
/// the Volume while Legion cannot ship.
pub const SPOOL_MAX: usize = 1024;
const HBA_BEGIN: &str = "# legion-replication begin";
const HBA_END: &str = "# legion-replication end";

/// A failed operation, with the code Legion acts on.
#[derive(Debug)]
pub struct Fail {
    pub code: &'static str,
    pub message: String,
}

impl Fail {
    pub fn new(message: impl Into<String>) -> Self {
        Fail {
            code: "failed",
            message: message.into(),
        }
    }

    fn code(code: &'static str, message: impl Into<String>) -> Self {
        Fail {
            code,
            message: message.into(),
        }
    }
}

type R<T> = Result<T, Fail>;

/// What a command did.
#[derive(Debug, Clone, Default)]
pub struct Output {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs PostgreSQL's tools.
pub trait Host {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        env: &[(&str, String)],
        stdin: Option<&str>,
        deadline: i64,
    ) -> R<Output>;

    fn env(&self, key: &str) -> Option<String>;
}

/// The jail's own processes.
pub struct System;

impl Host for System {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        env: &[(&str, String)],
        stdin: Option<&str>,
        deadline: i64,
    ) -> R<Output> {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| Fail::new(format!("running {program}: {e}")))?;
        if let (Some(text), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(text.as_bytes())
                .map_err(|e| Fail::new(format!("writing to {program}: {e}")))?;
        }
        // Read both pipes aside, so a full one cannot stall the child.
        let read = |p: Option<Box<dyn std::io::Read + Send>>| {
            std::thread::spawn(move || {
                let mut s = String::new();
                if let Some(mut p) = p {
                    let _ = p.read_to_string(&mut s);
                }
                s
            })
        };
        let out = read(child.stdout.take().map(|p| Box::new(p) as _));
        let err = read(child.stderr.take().map(|p| Box::new(p) as _));
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) if now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Fail::new(format!("{program} did not finish in time")));
                }
                Ok(None) => std::thread::sleep(std::time::Duration::from_millis(50)),
                Err(e) => return Err(Fail::new(format!("waiting for {program}: {e}"))),
            }
        };
        Ok(Output {
            ok: status.success(),
            stdout: out.join().unwrap_or_default(),
            stderr: err.join().unwrap_or_default(),
        })
    }

    fn env(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// The Service's `replication.options`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct Options {
    /// Unset, `$PGDATA`, else `/var/db/postgres/data`.
    pgdata: Option<String>,
    port: u16,
    /// Where the server's Unix socket is, for the plugin's own queries.
    socket_dir: String,
    /// The user the plugin queries the local server as, over the socket.
    superuser: String,
    /// For `initdb`: the superuser's password.
    superuser_password_file: Option<String>,
    replication_user: String,
    replication_password_file: Option<String>,
    max_slot_wal_keep_size: String,
    /// WAL every member keeps, whatever its replicas: a primary cut off and
    /// back is rewound (`pg_rewind`) from the last checkpoint before the
    /// timelines parted, which it must still hold.
    wal_keep_size: String,
    initdb_args: Vec<String>,
    /// Where replicas (and, with `allowClients`, clients) connect from.
    cidrs: Vec<String>,
    /// Let every user in from `cidrs` with a password, as a new database
    /// would otherwise refuse everyone from the network.
    allow_clients: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            pgdata: None,
            port: 5432,
            socket_dir: "/tmp".into(),
            superuser: "postgres".into(),
            superuser_password_file: None,
            replication_user: "replicator".into(),
            replication_password_file: None,
            max_slot_wal_keep_size: "4GB".into(),
            wal_keep_size: "512MB".into(),
            initdb_args: Vec::new(),
            cidrs: vec!["0.0.0.0/0".into(), "::/0".into()],
            allow_clients: true,
        }
    }
}

/// Characters safe inside the quoted values this plugin writes.
fn plain(what: &str, v: &str) -> R<()> {
    let ok = !v.is_empty()
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(Fail::new(format!(
            "{what} {v:?}: letters, digits and ._:/- only"
        )))
    }
}

fn identifier(what: &str, v: &str) -> R<()> {
    let ok = v.len() < 64
        && v.bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && v.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(Fail::new(format!(
            "{what} {v:?}: a lower-case SQL name (letters, digits, _)"
        )))
    }
}

/// A member's replication slot on the primary.
pub fn slot(member: &str) -> String {
    let s: String = member
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let mut s = format!("legion_{s}");
    s.truncate(63);
    s
}

/// A SQL string literal.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// `synchronous_standby_names` for the topology: a quorum of the members
/// named.
pub fn standby_names(t: &spi::Topology) -> String {
    match &t.synchronous {
        Some(s) if s.count > 0 && !s.members.is_empty() => {
            let names: Vec<String> = s
                .members
                .iter()
                .map(|m| format!("\"{}\"", m.replace('"', "")))
                .collect();
            format!("ANY {} ({})", s.count, names.join(", "))
        }
        _ => String::new(),
    }
}

/// What `pg_controldata` says of a data directory.
#[derive(Debug, PartialEq, Eq)]
pub struct Control {
    pub system: String,
    pub timeline: u64,
    pub state: String,
}

pub fn parse_controldata(text: &str) -> R<Control> {
    let get = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .map(|v| v.trim_start_matches(':').trim().to_owned())
    };
    let system = get("Database system identifier")
        .ok_or_else(|| Fail::new("pg_controldata: no system identifier"))?;
    let timeline = get("Latest checkpoint's TimeLineID")
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| Fail::new("pg_controldata: no timeline"))?;
    let state = get("Database cluster state").unwrap_or_default();
    Ok(Control {
        system,
        timeline,
        state,
    })
}

/// The data of a server that ran as a primary: its history may have gone
/// past where another member took over.
fn ran_as_primary(state: &str) -> bool {
    state == "in production" || state == "shut down"
}

/// The status query: one line, fields separated by `|`.
const STATUS_SQL: &str = "SELECT pg_is_in_recovery(), \
 CASE WHEN pg_is_in_recovery() \
  THEN COALESCE((SELECT received_tli FROM pg_stat_wal_receiver), (SELECT timeline_id FROM pg_control_checkpoint())) \
  ELSE ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int END, \
 pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() \
  THEN COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) ELSE pg_current_wal_lsn() END, '0/0'), \
 COALESCE((SELECT status = 'streaming' FROM pg_stat_wal_receiver), false), \
 COALESCE((SELECT sender_host FROM pg_stat_wal_receiver), ''), \
 CASE WHEN pg_is_in_recovery() THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) END, \
 CASE WHEN pg_is_in_recovery() THEN EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()) END, \
 COALESCE((SELECT string_agg(application_name || ':' || sync_state || ':' || state, ',') FROM pg_stat_replication), '')";

/// An integer column that PostgreSQL may print as a numeric (`123` or
/// `123.000`).
fn whole(v: &str) -> Option<u64> {
    v.split('.').next()?.parse().ok()
}

/// Reads the status query's line.
pub fn parse_status(line: &str, members: &[Member]) -> R<Status> {
    let f: Vec<&str> = line.trim().split('|').collect();
    if f.len() != 8 {
        return Err(Fail::new(format!("unexpected status line {line:?}")));
    }
    let recovery = f[0] == "t";
    let position = match (f[1].parse::<u64>(), whole(f[2])) {
        (Ok(term), Some(offset)) => Some(Position { term, offset }),
        _ => None,
    };
    let upstream = (!f[4].is_empty()).then(|| {
        members
            .iter()
            .find(|m| m.address == f[4] || m.dns == f[4])
            .map_or_else(|| f[4].to_owned(), |m| m.member.clone())
    });
    let lag = recovery.then(|| Lag {
        bytes: whole(f[5]),
        seconds: f[6].parse().ok(),
    });
    let mut detail = serde_json::Map::new();
    if !f[7].is_empty() {
        let replicas: Vec<Value> = f[7]
            .split(',')
            .filter_map(|r| {
                let mut p = r.splitn(3, ':');
                Some(json!({ "name": p.next()?, "sync": p.next()?, "state": p.next()? }))
            })
            .collect();
        detail.insert("replicas".into(), Value::Array(replicas));
    }
    Ok(Status {
        role: if recovery {
            Role::Replica
        } else {
            Role::Primary
        },
        healthy: true,
        position,
        upstream,
        streaming: f[3] == "t",
        synchronous: false,
        lag,
        detail,
        spool: None,
    })
}

/// The settings file for a role: what replication needs, and for a
/// replica where its primary is.
/// `archive`: the plugin's own path, which `archive_command` runs as
/// `<plugin> archive-wal %p %f <spool>`. Every member archives
/// (`always`): a replica promoted then has what the old primary wrote
/// before, and the target takes each file once, by name.
pub fn conf(
    o: &Options,
    data: &Path,
    me: &Member,
    primary: Option<&Member>,
    archive: Option<&str>,
) -> String {
    let mut c = format!(
        "# Written by Legion's replication plugin at each prepare and configure.\n\
         listen_addresses = '*'\n\
         port = {port}\n\
         wal_level = replica\n\
         hot_standby = on\n\
         wal_log_hints = on\n\
         max_wal_senders = 16\n\
         max_replication_slots = 16\n\
         max_slot_wal_keep_size = '{keep}'\n\
         wal_keep_size = '{wal}'\n",
        port = o.port,
        keep = o.max_slot_wal_keep_size,
        wal = o.wal_keep_size,
    );
    match archive {
        Some(exe) => c.push_str(&format!(
            "archive_mode = always\n\
             archive_command = '\"{exe}\" archive-wal \"%p\" \"%f\" \"{spool}\"'\n\
             archive_timeout = 60\n",
            spool = spool_of(data),
        )),
        None => c.push_str("archive_mode = off\n"),
    }
    if let Some(p) = primary {
        let host = if p.address.is_empty() {
            &p.dns
        } else {
            &p.address
        };
        c.push_str(&format!(
            "primary_conninfo = 'host={host} port={port} user={user} passfile={pass} application_name={name} connect_timeout=5'\n\
             primary_slot_name = '{slot}'\n\
             recovery_target_timeline = 'latest'\n",
            port = o.port,
            user = o.replication_user,
            pass = data.join(PASSFILE).display(),
            name = me.member,
            slot = slot(&me.member),
        ));
    }
    c
}

/// `pg_hba.conf` with this plugin's lines (between its markers) as they
/// should be.
pub fn hba(current: &str, o: &Options) -> String {
    let mut kept = String::new();
    let mut inside = false;
    for line in current.lines() {
        if line == HBA_BEGIN {
            inside = true;
        } else if line == HBA_END {
            inside = false;
        } else if !inside {
            kept.push_str(line);
            kept.push('\n');
        }
    }
    kept.push_str(HBA_BEGIN);
    kept.push('\n');
    for cidr in &o.cidrs {
        kept.push_str(&format!(
            "host replication {u} {cidr} scram-sha-256\nhost postgres {u} {cidr} scram-sha-256\n",
            u = o.replication_user
        ));
        if o.allow_clients {
            kept.push_str(&format!("host all all {cidr} scram-sha-256\n"));
        }
    }
    kept.push_str(HBA_END);
    kept.push('\n');
    kept
}

/// libpq's password file line for any server, as the replication user.
pub fn passfile(user: &str, password: &str) -> String {
    let esc = |s: &str| s.replace('\\', "\\\\").replace(':', "\\:");
    format!("*:*:*:{}:{}\n", esc(user), esc(password))
}

/// What the primary's single-user session runs: the replication role,
/// with what `pg_rewind` reads through it.
pub fn role_sql(user: &str, password: &str) -> String {
    let u = user;
    format!(
        "DO $legion$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = {lit}) THEN CREATE ROLE {u}; END IF; END $legion$;\n\n\
         ALTER ROLE {u} WITH REPLICATION LOGIN PASSWORD {pw};\n\n\
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text, boolean, boolean) TO {u};\n\n\
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text, boolean) TO {u};\n\n\
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO {u};\n\n\
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO {u};\n\n",
        lit = literal(user),
        pw = literal(password),
    )
}

/// Whether a connection failure is one to wait out.
fn unreachable(text: &str) -> bool {
    [
        "could not connect",
        "Connection refused",
        "timeout expired",
        "the database system is starting up",
        "the database system is shutting down",
        "No route to host",
    ]
    .iter()
    .any(|s| text.contains(s))
}

fn tail(text: &str) -> String {
    let lines: Vec<&str> = text.trim().lines().collect();
    lines[lines.len().saturating_sub(3)..].join(" / ")
}

/// One call.
struct Pg<'h, H: Host> {
    h: &'h mut H,
    req: Request,
    o: Options,
    data: PathBuf,
}

/// Runs `operation` on `input`, the request, returning the response.
pub fn handle(operation: &str, input: &str, h: &mut impl Host) -> R<String> {
    if operation == "describe" {
        return Ok(serde_json::to_string(&describe()).expect("serialises"));
    }
    let req: Request =
        serde_json::from_str(input).map_err(|e| Fail::new(format!("the request: {e}")))?;
    let o: Options = serde_json::from_value(Value::Object(req.options.clone()))
        .map_err(|e| Fail::new(format!("replication.options: {e}")))?;
    identifier("replicationUser", &o.replication_user)?;
    identifier("superuser", &o.superuser)?;
    plain("maxSlotWalKeepSize", &o.max_slot_wal_keep_size)?;
    plain("walKeepSize", &o.wal_keep_size)?;
    for c in &o.cidrs {
        plain("cidrs", c)?;
    }
    let data = PathBuf::from(
        o.pgdata
            .clone()
            .or_else(|| h.env("PGDATA"))
            .unwrap_or_else(|| "/var/db/postgres/data".into()),
    );
    plain("pgdata", &data.display().to_string())?;
    let mut pg = Pg { h, req, o, data };
    let v = match operation {
        "prepare" => pg.prepare()?,
        "status" => serde_json::to_value(pg.status()?).expect("serialises"),
        "promote" => pg.promote()?,
        "configure" => pg.configure()?,
        "probe" => pg.probe()?,
        other => return Err(Fail::new(format!("no operation {other:?}"))),
    };
    Ok(v.to_string())
}

/// The spool of the data directory `data`, as the container sees it.
fn spool_of(data: &Path) -> String {
    format!("{}/{SPOOL}", data.display())
}

/// This plugin's path, as the agent ran it (in the container, where
/// `archive_command` runs it too).
fn own_path() -> String {
    std::env::args().next().unwrap_or_default()
}

/// `archive_command`: `<plugin> archive-wal <path> <name> <spool>` keeps
/// the finished WAL file `path` (relative to the data directory, where
/// PostgreSQL runs it) as `<spool>/<name>`, whole and on disk before it
/// says so: written aside, synced, renamed, the directory synced. Kept
/// already (a retry after a crash), it succeeds. A spool of
/// [`SPOOL_MAX`] files fails it: PostgreSQL keeps the WAL and tries again.
pub fn archive_wal(path: &Path, name: &str, spool: &Path) -> Result<(), String> {
    if name.is_empty() || name.contains('/') || name.starts_with('.') {
        return Err(format!("archive-wal: {name:?} is no WAL file's name"));
    }
    let size = std::fs::metadata(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .len();
    let kept = spool.join(name);
    if std::fs::metadata(&kept).is_ok_and(|m| m.len() == size) {
        return Ok(());
    }
    let held = std::fs::read_dir(spool)
        .map_err(|e| format!("{}: {e}", spool.display()))?
        .count();
    if held >= SPOOL_MAX {
        return Err(format!(
            "{} holds {held} files Legion has not shipped yet: see the Service's status.archive",
            spool.display()
        ));
    }
    let aside = spool.join(format!("{name}.part"));
    let copied = (|| {
        std::fs::copy(path, &aside)?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&aside)?
            .sync_all()?;
        std::fs::rename(&aside, &kept)?;
        #[cfg(unix)]
        std::fs::File::open(spool)?.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    copied.map_err(|e| {
        let _ = std::fs::remove_file(&aside);
        format!("archive-wal {name}: {e}")
    })
}

pub fn describe() -> Describe {
    Describe {
        spi_versions: vec![spi::VERSION.into()],
        database: "postgresql".into(),
        capabilities: Capabilities {
            synchronous: true,
            readable_replicas: true,
            rewind: true,
            seed_from_snapshot: false,
            probe: true,
            archive: true,
        },
    }
}

impl<H: Host> Pg<'_, H> {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        env: &[(&str, String)],
        stdin: Option<&str>,
    ) -> R<Output> {
        self.h.run(program, args, env, stdin, self.req.deadline)
    }

    fn is_me(&self, m: Option<&Member>) -> bool {
        m.is_some_and(|m| m.member == self.req.me.member)
    }

    fn password(&self) -> R<String> {
        let f = self.o.replication_password_file.as_deref().ok_or_else(|| {
            Fail::new("replication.options.replicationPasswordFile: set it to a Secret mounted in the pod")
        })?;
        let p = std::fs::read_to_string(f).map_err(|e| Fail::new(format!("{f}: {e}")))?;
        let p = p.trim_end_matches(['\r', '\n']).to_owned();
        if p.is_empty() {
            return Err(Fail::new(format!("{f} is empty")));
        }
        Ok(p)
    }

    fn write(&self, name: &str, text: &str, private: bool) -> R<()> {
        let path = self.data.join(name);
        let aside = self.data.join(format!(".{name}.new"));
        std::fs::write(&aside, text).map_err(|e| Fail::new(format!("{}: {e}", aside.display())))?;
        #[cfg(unix)]
        if private {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&aside, std::fs::Permissions::from_mode(0o600));
        }
        #[cfg(not(unix))]
        let _ = private;
        std::fs::rename(&aside, &path).map_err(|e| Fail::new(format!("{}: {e}", path.display())))
    }

    /// Removes what the data directory holds, keeping the directory (its
    /// owner and mode are the image's).
    fn empty(&self) -> R<()> {
        let entries = std::fs::read_dir(&self.data)
            .map_err(|e| Fail::new(format!("{}: {e}", self.data.display())))?;
        for e in entries.flatten() {
            let path = e.path();
            let gone = if e.file_type().is_ok_and(|t| t.is_dir()) {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            gone.map_err(|e| Fail::new(format!("{}: {e}", path.display())))?;
        }
        Ok(())
    }

    fn has_database(&self) -> bool {
        self.data.join("PG_VERSION").exists()
    }

    /// The configuration for this member's role: the settings file, the
    /// include, the access lines and, for a replica, its password file.
    fn configure_files(&mut self, primary: Option<&Member>) -> R<()> {
        let exe = self.req.archive.then(own_path);
        if exe.is_some() {
            let spool = self.data.join(SPOOL);
            std::fs::create_dir_all(&spool)
                .map_err(|e| Fail::new(format!("{}: {e}", spool.display())))?;
        }
        let text = conf(&self.o, &self.data, &self.req.me, primary, exe.as_deref());
        self.write(CONF, &text, false)?;
        let main = self.data.join("postgresql.conf");
        let current = std::fs::read_to_string(&main).unwrap_or_default();
        if !current.lines().any(|l| l.trim() == INCLUDE) {
            let mut c = current;
            if !c.is_empty() && !c.ends_with('\n') {
                c.push('\n');
            }
            c.push_str(INCLUDE);
            c.push('\n');
            self.write("postgresql.conf", &c, false)?;
        }
        let file = self.data.join("pg_hba.conf");
        let current = std::fs::read_to_string(&file).unwrap_or_default();
        let wanted = hba(&current, &self.o);
        if wanted != current {
            self.write("pg_hba.conf", &wanted, false)?;
        }
        if primary.is_some() {
            let pw = self.password()?;
            self.write(PASSFILE, &passfile(&self.o.replication_user, &pw), true)?;
        }
        Ok(())
    }

    fn prepare(&mut self) -> R<Value> {
        match self.req.role {
            Some(Role::Primary) => self.prepare_primary(),
            Some(Role::Replica) => self.prepare_replica(),
            _ => Err(Fail::new("prepare needs a role")),
        }
    }

    fn prepare_primary(&mut self) -> R<Value> {
        let mut action = "none";
        if !self.has_database() {
            let empty = std::fs::read_dir(&self.data).map_or(true, |mut d| d.next().is_none());
            if !empty {
                return Err(Fail::code(
                    spi::DIVERGED,
                    format!("{} holds files but no database", self.data.display()),
                ));
            }
            let mut args = vec![
                "-D".to_owned(),
                self.data.display().to_string(),
                "-U".into(),
                self.o.superuser.clone(),
                "-E".into(),
                "UTF8".into(),
                "--no-locale".into(),
                "--data-checksums".into(),
                "--auth-local=trust".into(),
                "--auth-host=scram-sha-256".into(),
            ];
            if let Some(f) = &self.o.superuser_password_file {
                args.push(format!("--pwfile={f}"));
            }
            args.extend(self.o.initdb_args.iter().cloned());
            let out = self.run("initdb", &args, &[], None)?;
            if !out.ok {
                return Err(Fail::new(format!("initdb: {}", tail(&out.stderr))));
            }
            action = "initialized";
        }
        // Named primary: a promotion cut short is finished by starting
        // without the standby's signal.
        let _ = std::fs::remove_file(self.data.join("standby.signal"));
        self.configure_files(None)?;
        let pw = self.password()?;
        let args = vec![
            "--single".to_owned(),
            "-j".into(),
            "-D".into(),
            self.data.display().to_string(),
            "postgres".into(),
        ];
        let out = self.run(
            "postgres",
            &args,
            &[],
            Some(&role_sql(&self.o.replication_user, &pw)),
        )?;
        let said = format!("{}\n{}", out.stdout, out.stderr);
        if !out.ok || said.contains("ERROR:") || said.contains("FATAL:") {
            return Err(Fail::new(format!(
                "making the replication role: {}",
                tail(&said)
            )));
        }
        Ok(json!({ "action": action }))
    }

    /// `IDENTIFY_SYSTEM` on the primary: its system identifier and
    /// timeline.
    fn identify(&mut self, p: &Member, pw: &str) -> R<(String, u64)> {
        let host = if p.address.is_empty() {
            &p.dns
        } else {
            &p.address
        };
        let args = vec![
            "-X".to_owned(),
            "-A".into(),
            "-t".into(),
            "-F".into(),
            "|".into(),
            format!(
                "host={host} port={} user={} dbname=postgres replication=database connect_timeout=5",
                self.o.port, self.o.replication_user
            ),
            "-c".into(),
            "IDENTIFY_SYSTEM".into(),
        ];
        let out = self.run("psql", &args, &[("PGPASSWORD", pw.to_owned())], None)?;
        if !out.ok {
            let code = if unreachable(&out.stderr) {
                spi::RETRY
            } else {
                "failed"
            };
            return Err(Fail::code(
                code,
                format!("the primary {}: {}", p.member, tail(&out.stderr)),
            ));
        }
        let f: Vec<&str> = out.stdout.trim().split('|').collect();
        match (f.first(), f.get(1).and_then(|t| t.parse().ok())) {
            (Some(s), Some(t)) if !s.is_empty() => Ok((s.to_string(), t)),
            _ => Err(Fail::new(format!(
                "IDENTIFY_SYSTEM said {:?}",
                out.stdout.trim()
            ))),
        }
    }

    fn controldata(&mut self) -> R<Control> {
        let out = self.run(
            "pg_controldata",
            &["-D".into(), self.data.display().to_string()],
            &[("LC_ALL", "C".into())],
            None,
        )?;
        if !out.ok {
            return Err(Fail::new(format!("pg_controldata: {}", tail(&out.stderr))));
        }
        parse_controldata(&out.stdout)
    }

    fn prepare_replica(&mut self) -> R<Value> {
        let Some(p) = self.req.topology.primary.clone() else {
            return Err(Fail::code(spi::RETRY, "no primary named yet"));
        };
        if self.is_me(Some(&p)) {
            return Err(Fail::new("this member is the named primary, not a replica"));
        }
        let pw = self.password()?;
        let host = if p.address.is_empty() {
            p.dns.clone()
        } else {
            p.address.clone()
        };
        let env = [("PGPASSWORD", pw.clone())];
        if self.req.reclone {
            self.empty()?;
        }
        let action;
        if !self.has_database() {
            let empty = std::fs::read_dir(&self.data).map_or(true, |mut d| d.next().is_none());
            if !empty {
                return Err(Fail::code(
                    spi::DIVERGED,
                    format!("{} holds files but no database", self.data.display()),
                ));
            }
            self.identify(&p, &pw)?;
            let args = vec![
                "-D".to_owned(),
                self.data.display().to_string(),
                "-h".into(),
                host,
                "-p".into(),
                self.o.port.to_string(),
                "-U".into(),
                self.o.replication_user.clone(),
                "-X".into(),
                "stream".into(),
                "-c".into(),
                "fast".into(),
                "-w".into(),
            ];
            let out = self.run("pg_basebackup", &args, &env, None)?;
            if !out.ok {
                let code = if unreachable(&out.stderr) {
                    spi::RETRY
                } else {
                    "failed"
                };
                return Err(Fail::code(
                    code,
                    format!("pg_basebackup: {}", tail(&out.stderr)),
                ));
            }
            action = "cloned";
        } else {
            let local = self.controldata()?;
            let (system, timeline) = self.identify(&p, &pw)?;
            if local.system != system {
                return Err(Fail::code(
                    spi::DIVERGED,
                    format!(
                        "this data is of database {}, the primary's of {system}",
                        local.system
                    ),
                ));
            }
            if ran_as_primary(&local.state) || local.timeline != timeline {
                // pg_rewind brings a crashed target to a clean stop itself,
                // which a standby's signal would refuse.
                let _ = std::fs::remove_file(self.data.join("standby.signal"));
                let args = vec![
                    format!("--target-pgdata={}", self.data.display()),
                    format!(
                        "--source-server=host={host} port={} user={} dbname=postgres connect_timeout=5",
                        self.o.port, self.o.replication_user
                    ),
                ];
                let out = self.run("pg_rewind", &args, &env, None)?;
                let said = format!("{}\n{}", out.stdout, out.stderr);
                if !out.ok {
                    let code = if unreachable(&said) {
                        spi::RETRY
                    } else {
                        spi::DIVERGED
                    };
                    return Err(Fail::code(code, format!("pg_rewind: {}", tail(&said))));
                }
                action = if said.contains("no rewind required") {
                    "none"
                } else {
                    "rewound"
                };
            } else {
                action = "none";
            }
        }
        self.configure_files(Some(&p))?;
        self.write("standby.signal", "", false)?;
        Ok(json!({ "action": action }))
    }

    fn psql(&mut self, sql: &str) -> R<String> {
        let args = vec![
            "-X".to_owned(),
            "-A".into(),
            "-t".into(),
            "-q".into(),
            "-F".into(),
            "|".into(),
            "-v".into(),
            "ON_ERROR_STOP=1".into(),
            "-h".into(),
            self.o.socket_dir.clone(),
            "-p".into(),
            self.o.port.to_string(),
            "-U".into(),
            self.o.superuser.clone(),
            "-d".into(),
            "postgres".into(),
            "-c".into(),
            sql.into(),
        ];
        let out = self.run("psql", &args, &[], None)?;
        if !out.ok {
            let code = if unreachable(&out.stderr) {
                spi::RETRY
            } else {
                "failed"
            };
            return Err(Fail::code(code, format!("psql: {}", tail(&out.stderr))));
        }
        Ok(out.stdout.trim().to_owned())
    }

    /// The conformance check's marker: a row of `legion_probe` in the
    /// `postgres` database, written on the primary as any commit is (so
    /// synchronous replicas confirm it), looked for on any member.
    fn probe(&mut self) -> R<Value> {
        let Some(p) = self.req.probe.clone() else {
            return Err(Fail::new("probe: say what to write or read"));
        };
        let key = p.key();
        if key.is_empty()
            || key.len() > 64
            || !key
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Fail::new(format!("probe: bad key {key:?}")));
        }
        let found = match &p {
            spi::Probe::Write(k) => {
                self.psql(&format!(
                    "CREATE TABLE IF NOT EXISTS legion_probe (key text PRIMARY KEY, \
                     at timestamptz NOT NULL DEFAULT now()); \
                     INSERT INTO legion_probe (key) VALUES ('{k}') ON CONFLICT DO NOTHING"
                ))?;
                true
            }
            spi::Probe::Read(k) => {
                self.psql("SELECT to_regclass('public.legion_probe') IS NOT NULL")? == "t"
                    && self.psql(&format!(
                        "SELECT count(*) FROM legion_probe WHERE key = '{k}'"
                    ))? != "0"
            }
        };
        Ok(json!({ "found": found }))
    }

    fn status(&mut self) -> R<Status> {
        let line = self.psql(STATUS_SQL)?;
        let mut s = parse_status(&line, &self.req.topology.members)?;
        if self.req.archive {
            s.spool = Some(spool_of(&self.data));
        }
        Ok(s)
    }

    fn promote(&mut self) -> R<Value> {
        if !self.is_me(self.req.topology.primary.as_ref()) {
            return Err(Fail::new("promote: the topology names another primary"));
        }
        if self.psql("SELECT pg_is_in_recovery()")? == "t" {
            let r = self.psql("SELECT pg_promote(true, 50)")?;
            if r != "t" {
                return Err(Fail::code(spi::RETRY, "not promoted within 50 s"));
            }
        }
        self.configure_files(None)?;
        self.configure_primary()?;
        let s = self.status()?;
        Ok(json!({ "position": s.position }))
    }

    fn configure(&mut self) -> R<Value> {
        let recovery = self.psql("SELECT pg_is_in_recovery()")? == "t";
        let primary = self.req.topology.primary.clone();
        if self.is_me(primary.as_ref()) {
            if recovery {
                return Err(Fail::new(
                    "named primary but not promoted: promote it first",
                ));
            }
            self.configure_primary()?;
        } else if !recovery {
            return Err(Fail::code(
                spi::RESTART,
                "this member runs as a primary; it must start again as a replica",
            ));
        } else if let Some(p) = primary {
            self.configure_files(Some(&p))?;
            self.psql("SELECT pg_reload_conf()")?;
        }
        Ok(json!({}))
    }

    /// A slot for each other member, none for those gone, and the
    /// synchronous quorum.
    fn configure_primary(&mut self) -> R<()> {
        let wanted: Vec<String> = self
            .req
            .topology
            .members
            .iter()
            .filter(|m| m.member != self.req.me.member)
            .map(|m| slot(&m.member))
            .collect();
        let have = self.psql(
            "SELECT slot_name || '|' || active FROM pg_replication_slots WHERE slot_name LIKE 'legion\\_%'",
        )?;
        let have: Vec<(String, bool)> = have
            .lines()
            .filter_map(|l| {
                let (n, a) = l.split_once('|')?;
                Some((n.to_owned(), a == "t"))
            })
            .collect();
        for s in &wanted {
            if !have.iter().any(|(n, _)| n == s) {
                self.psql(&format!(
                    "SELECT pg_create_physical_replication_slot({}, true)",
                    literal(s)
                ))?;
            }
        }
        for (n, active) in &have {
            if !wanted.contains(n) && !active {
                self.psql(&format!("SELECT pg_drop_replication_slot({})", literal(n)))?;
            }
        }
        let names = standby_names(&self.req.topology);
        let current = self.psql("SHOW synchronous_standby_names")?;
        if current != names {
            self.psql(&format!(
                "ALTER SYSTEM SET synchronous_standby_names = {}",
                literal(&names)
            ))?;
            self.psql("SELECT pg_reload_conf()")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
