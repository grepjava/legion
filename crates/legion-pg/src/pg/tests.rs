use super::*;
use legion_api::replication::spi::{ServiceRef, Synchronous, Topology};

/// A host whose commands answer from a script, recording what ran.
#[derive(Default)]
struct Fake {
    ran: Vec<(String, Vec<String>, Option<String>)>,
    answers: Vec<(String, Output)>,
    env: Vec<(String, String)>,
}

impl Fake {
    /// Commands whose program and arguments joined contain `key` answer
    /// `out`, the first match first.
    fn on(&mut self, key: &str, ok: bool, stdout: &str, stderr: &str) {
        self.answers.push((
            key.into(),
            Output {
                ok,
                stdout: stdout.into(),
                stderr: stderr.into(),
            },
        ));
    }

    fn line(p: &str, args: &[String]) -> String {
        format!("{p} {}", args.join(" "))
    }

    fn ran(&self, key: &str) -> bool {
        self.ran
            .iter()
            .any(|(p, a, _)| Self::line(p, a).contains(key))
    }
}

impl Host for Fake {
    fn run(
        &mut self,
        program: &str,
        args: &[String],
        _env: &[(&str, String)],
        stdin: Option<&str>,
        _deadline: i64,
    ) -> R<Output> {
        self.ran
            .push((program.into(), args.to_vec(), stdin.map(str::to_owned)));
        let line = Self::line(program, args);
        Ok(self
            .answers
            .iter()
            .find(|(k, _)| line.contains(k.as_str()))
            .map(|(_, o)| o.clone())
            .unwrap_or(Output {
                ok: true,
                ..Default::default()
            }))
    }

    fn env(&self, key: &str) -> Option<String> {
        self.env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }
}

fn member(n: u32) -> Member {
    Member {
        member: format!("pg-{n}"),
        ordinal: n,
        address: format!("10.200.{n}.9"),
        dns: format!("pg-{n}.pg.data"),
    }
}

struct Setup {
    _dir: tempfile::TempDir,
    data: PathBuf,
    secret: PathBuf,
}

fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let secret = dir.path().join("pw");
    std::fs::write(&secret, "s3cr:t\n").unwrap();
    Setup {
        data,
        secret,
        _dir: dir,
    }
}

fn request(s: &Setup, me: u32, primary: Option<u32>, role: Option<Role>) -> String {
    let mut options = serde_json::Map::new();
    options.insert(
        "pgdata".into(),
        json!(s.data.display().to_string().replace('\\', "/")),
    );
    options.insert(
        "replicationPasswordFile".into(),
        json!(s.secret.display().to_string()),
    );
    let r = Request {
        spi_version: "1".into(),
        operation: String::new(),
        deadline: i64::MAX,
        service: ServiceRef {
            namespace: "data".into(),
            name: "pg".into(),
        },
        epoch: 2,
        me: member(me),
        topology: Topology {
            primary: primary.map(member),
            members: (0..3).map(member).collect(),
            synchronous: Some(Synchronous {
                count: 1,
                members: vec!["pg-1".into(), "pg-2".into()],
            }),
        },
        options,
        role,
        reclone: false,
        probe: None,
        archive: false,
        restore: None,
    };
    serde_json::to_string(&r).unwrap()
}

/// The temporary directory's path may hold characters the plugin refuses
/// (Windows' `\` and `C:`); these tests use its own forward-slash form.
fn usable(s: &Setup) -> bool {
    plain("pgdata", &s.data.display().to_string().replace('\\', "/")).is_ok()
}

#[test]
fn describe_offers_spi_1_with_synchronous_and_readable_replicas() {
    let v: Value =
        serde_json::from_str(&handle("describe", "", &mut Fake::default()).unwrap()).unwrap();
    assert_eq!(v["spiVersions"], json!(["1"]));
    assert_eq!(v["capabilities"]["synchronous"], true);
    assert_eq!(v["capabilities"]["readableReplicas"], true);
}

#[test]
fn a_new_primary_is_made_configured_and_given_its_replication_role() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let mut h = Fake::default();
    // initdb leaves a database behind.
    let data = s.data.clone();
    h.on("initdb", true, "", "");
    let req = request(&s, 0, Some(0), Some(Role::Primary));
    // The fake initdb makes nothing: make PG_VERSION after it is asked.
    let out = {
        let r = handle("prepare", &req, &mut h);
        r.unwrap()
    };
    assert!(out.contains("initialized"), "{out}");
    assert!(h.ran("initdb -D"));
    assert!(h.ran("--data-checksums"));
    let (_, _, stdin) = h.ran.iter().find(|(p, _, _)| p == "postgres").unwrap();
    let sql = stdin.as_deref().unwrap();
    assert!(sql.contains("ALTER ROLE replicator WITH REPLICATION LOGIN PASSWORD 's3cr:t'"));
    assert!(
        sql.contains(";\n\n"),
        "single-user statements end with ;\\n\\n"
    );
    let conf = std::fs::read_to_string(data.join(CONF)).unwrap();
    assert!(conf.contains("wal_level = replica"));
    assert!(conf.contains("wal_keep_size = '512MB'"), "{conf}");
    assert!(!conf.contains("primary_conninfo"));
    let main = std::fs::read_to_string(data.join("postgresql.conf")).unwrap();
    assert_eq!(main.matches(INCLUDE).count(), 1);
    let hba = std::fs::read_to_string(data.join("pg_hba.conf")).unwrap();
    assert!(hba.contains("host replication replicator 0.0.0.0/0 scram-sha-256"));

    // Again, with a database: nothing is made, the files stay as they are.
    std::fs::write(data.join("PG_VERSION"), "17\n").unwrap();
    std::fs::write(data.join("standby.signal"), "").unwrap();
    let mut h = Fake::default();
    let out = handle("prepare", &req, &mut h).unwrap();
    assert!(out.contains("\"none\""), "{out}");
    assert!(!h.ran("initdb"));
    assert!(
        !data.join("standby.signal").exists(),
        "a named primary starts as one"
    );
    let main = std::fs::read_to_string(data.join("postgresql.conf")).unwrap();
    assert_eq!(main.matches(INCLUDE).count(), 1);
    let again = std::fs::read_to_string(data.join("pg_hba.conf")).unwrap();
    assert_eq!(again, hba);
}

#[test]
fn a_primary_refuses_a_directory_of_something_else() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    std::fs::write(s.data.join("stray"), "").unwrap();
    let e = handle(
        "prepare",
        &request(&s, 0, Some(0), Some(Role::Primary)),
        &mut Fake::default(),
    )
    .unwrap_err();
    assert_eq!(e.code, spi::DIVERGED);
}

const CONTROL: &str = "pg_control version number:            1700\n\
Database system identifier:           7300000000000000001\n\
Database cluster state:               shut down\n\
Latest checkpoint's TimeLineID:       1\n";

#[test]
fn a_crashed_primary_finishes_recovery_with_the_settings_it_ran_with_before_pg_rewind() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    std::fs::write(s.data.join("PG_VERSION"), "17").unwrap();
    let crashed = "Database system identifier:           7300000000000000001\n\
Database cluster state:               in production\n\
Latest checkpoint's TimeLineID:       1\n\
max_connections setting:              30\n\
max_worker_processes setting:         8\n\
max_wal_senders setting:              16\n\
max_prepared_xacts setting:           0\n\
max_locks_per_xact setting:           64\n\
track_commit_timestamp setting:       off\n";
    let mut h = Fake::default();
    h.on("pg_controldata", true, crashed, "");
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/3000060|\n",
        "",
    );
    handle(
        "prepare",
        &request(&s, 1, Some(0), Some(Role::Replica)),
        &mut h,
    )
    .unwrap();
    let single = h
        .ran
        .iter()
        .position(|(p, a, _)| p == "postgres" && a.contains(&"--single".to_owned()))
        .expect("crash recovery in single-user mode");
    let (_, args, _) = &h.ran[single];
    for set in [
        "max_connections=30",
        "max_locks_per_transaction=64",
        "track_commit_timestamp=off",
    ] {
        assert!(args.contains(&set.to_owned()), "{set} in {args:?}");
    }
    let rewind = h
        .ran
        .iter()
        .position(|(p, _, _)| p == "pg_rewind")
        .expect("rewound");
    assert!(single < rewind, "recovered before the rewind");

    // A primary named on data that ran: no single-user mode at all.
    let mut h = Fake::default();
    h.on("pg_controldata", true, crashed, "");
    handle(
        "prepare",
        &request(&s, 0, Some(0), Some(Role::Primary)),
        &mut h,
    )
    .unwrap();
    assert!(!h.ran("--single"), "{:?}", h.ran);
}

#[test]
fn a_replica_is_copied_when_empty_rewound_when_it_ran_as_primary_and_refused_another_database() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let req = request(&s, 1, Some(0), Some(Role::Replica));

    // No primary yet: wait.
    let e = handle(
        "prepare",
        &request(&s, 1, None, Some(Role::Replica)),
        &mut Fake::default(),
    )
    .unwrap_err();
    assert_eq!(e.code, spi::RETRY);

    // Empty: a base backup from the primary.
    let mut h = Fake::default();
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|1|0/3000060|\n",
        "",
    );
    let out = handle("prepare", &req, &mut h).unwrap();
    assert!(out.contains("cloned"), "{out}");
    assert!(h.ran("pg_basebackup -D"));
    assert!(h.ran("-h 10.200.0.9"));
    let conf = std::fs::read_to_string(s.data.join(CONF)).unwrap();
    assert!(conf.contains("host=10.200.0.9 port=5432 user=replicator"));
    assert!(conf.contains("application_name=pg-1"));
    assert!(conf.contains("primary_slot_name = 'legion_pg_1'"));
    assert!(s.data.join("standby.signal").exists());
    let pass = std::fs::read_to_string(s.data.join(PASSFILE)).unwrap();
    assert_eq!(pass, "*:*:*:replicator:s3cr\\:t\n");

    // The primary unreachable: wait.
    let mut h = Fake::default();
    std::fs::remove_dir_all(&s.data).unwrap();
    std::fs::create_dir(&s.data).unwrap();
    h.on(
        "IDENTIFY_SYSTEM",
        false,
        "",
        "psql: error: could not connect to server: Connection refused",
    );
    assert_eq!(
        handle("prepare", &req, &mut h).unwrap_err().code,
        spi::RETRY
    );

    // An old primary: rewound.
    std::fs::write(s.data.join("PG_VERSION"), "17\n").unwrap();
    let mut h = Fake::default();
    h.on("pg_controldata", true, CONTROL, "");
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/5000000|\n",
        "",
    );
    h.on("pg_rewind", true, "", "pg_rewind: Done!");
    let out = handle("prepare", &req, &mut h).unwrap();
    assert!(out.contains("rewound"), "{out}");

    // A replica already following on the primary's timeline: nothing.
    let mut h = Fake::default();
    h.on(
        "pg_controldata",
        true,
        &CONTROL
            .replace("shut down", "shut down in recovery")
            .replace("TimeLineID:       1", "TimeLineID:       2"),
        "",
    );
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/5000000|\n",
        "",
    );
    let out = handle("prepare", &req, &mut h).unwrap();
    assert!(out.contains("\"none\""), "{out}");
    assert!(!h.ran("pg_rewind"));

    // A failed rewind, or another database: diverged.
    let mut h = Fake::default();
    h.on("pg_controldata", true, CONTROL, "");
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/5000000|\n",
        "",
    );
    h.on(
        "pg_rewind",
        false,
        "",
        "pg_rewind: fatal: could not find common ancestor",
    );
    assert_eq!(
        handle("prepare", &req, &mut h).unwrap_err().code,
        spi::DIVERGED
    );
    let mut h = Fake::default();
    h.on("pg_controldata", true, CONTROL, "");
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7399999999999999999|1|0/5000000|\n",
        "",
    );
    assert_eq!(
        handle("prepare", &req, &mut h).unwrap_err().code,
        spi::DIVERGED
    );
}

#[test]
fn status_reads_role_position_lag_and_upstream() {
    let members: Vec<Member> = (0..3).map(member).collect();
    let r = parse_status("t|2|50331744|t|10.200.0.9|8192|0.4|", &members).unwrap();
    assert_eq!(r.role, Role::Replica);
    assert_eq!(
        r.position,
        Some(Position {
            term: 2,
            offset: 50331744
        })
    );
    assert!(r.streaming);
    assert_eq!(r.upstream.as_deref(), Some("pg-0"));
    assert_eq!(r.lag.as_ref().unwrap().bytes, Some(8192));
    let p = parse_status(
        "f|3|67108864.000|f||||pg-1:quorum:streaming,pg-2:quorum:catchup",
        &members,
    )
    .unwrap();
    assert_eq!(p.role, Role::Primary);
    assert_eq!(
        p.position,
        Some(Position {
            term: 3,
            offset: 67108864
        })
    );
    assert!(p.lag.is_none() && p.upstream.is_none());
    assert_eq!(p.detail["replicas"][1]["state"], "catchup");
    assert!(parse_status("garbage", &members).is_err());
}

#[test]
fn configure_keeps_slots_for_the_members_and_sets_the_quorum() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let mut h = Fake::default();
    h.on("pg_is_in_recovery", true, "f\n", "");
    h.on(
        "pg_replication_slots",
        true,
        "legion_pg_1|t\nlegion_pg_9|f\nlegion_pg_8|t\n",
        "",
    );
    h.on("SHOW synchronous_standby_names", true, "\n", "");
    handle("configure", &request(&s, 0, Some(0), None), &mut h).unwrap();
    assert!(h.ran("pg_create_physical_replication_slot('legion_pg_2', true)"));
    assert!(!h.ran("pg_create_physical_replication_slot('legion_pg_1'"));
    assert!(h.ran("pg_drop_replication_slot('legion_pg_9')"));
    assert!(
        !h.ran("pg_drop_replication_slot('legion_pg_8')"),
        "an active slot stays"
    );
    assert!(h.ran("ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (\"pg-1\", \"pg-2\")'"));

    // A primary told another one leads: start again as a replica.
    let mut h = Fake::default();
    h.on("pg_is_in_recovery", true, "f\n", "");
    let e = handle("configure", &request(&s, 0, Some(1), None), &mut h).unwrap_err();
    assert_eq!(e.code, spi::RESTART);

    // A replica re-pointed: its settings rewritten and reloaded.
    let mut h = Fake::default();
    h.on("pg_is_in_recovery", true, "t\n", "");
    handle("configure", &request(&s, 2, Some(1), None), &mut h).unwrap();
    assert!(h.ran("pg_reload_conf"));
    let conf = std::fs::read_to_string(s.data.join(CONF)).unwrap();
    assert!(conf.contains("host=10.200.1.9"));
}

#[test]
fn promote_is_asked_once_and_succeeds_on_a_primary() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let mut h = Fake::default();
    // The status query also asks pg_is_in_recovery(): it answers first.
    h.on("pg_walfile_name", true, "f|2|100|f||||\n", "");
    h.on("SELECT pg_is_in_recovery()", true, "t\n", "");
    h.on("pg_promote", true, "t\n", "");
    h.on("pg_replication_slots", true, "", "");
    h.on("SHOW synchronous", true, "\n", "");
    let out = handle("promote", &request(&s, 1, Some(1), None), &mut h).unwrap();
    assert!(h.ran("pg_promote(true, 50)"));
    assert!(out.contains("\"offset\":\"100\""), "{out}");
    // Already primary: no promotion asked.
    let mut h = Fake::default();
    h.on("pg_walfile_name", true, "f|2|100|f||||\n", "");
    h.on("SELECT pg_is_in_recovery()", true, "f\n", "");
    handle("promote", &request(&s, 1, Some(1), None), &mut h).unwrap();
    assert!(!h.ran("pg_promote"));
    // Not the named primary: refused.
    assert!(
        handle(
            "promote",
            &request(&s, 1, Some(0), None),
            &mut Fake::default()
        )
        .is_err()
    );
}

#[test]
fn helpers_quote_and_name_safely() {
    assert_eq!(slot("pg-10"), "legion_pg_10");
    assert_eq!(literal("it's"), "'it''s'");
    assert_eq!(passfile("r", "a\\b:c"), "*:*:*:r:a\\\\b\\:c\n");
    assert!(identifier("u", "replicator").is_ok());
    assert!(identifier("u", "Robert'); DROP").is_err());
    assert!(plain("x", "10.0.0.0/8").is_ok());
    assert!(plain("x", "a b").is_err());
    let c = parse_controldata(CONTROL).unwrap();
    assert_eq!(c.timeline, 1);
    assert_eq!(c.state, "shut down");
    assert!(ran_as_primary(&c.state));
    assert!(!ran_as_primary("shut down in recovery"));
    assert_eq!(
        standby_names(&Topology {
            synchronous: Some(Synchronous {
                count: 2,
                members: vec!["a".into(), "b\"".into()]
            }),
            ..Default::default()
        }),
        "ANY 2 (\"a\", \"b\")"
    );
    let replaced = hba(
        "local all all trust\n# legion-replication begin\nold\n# legion-replication end\n",
        &Options::default(),
    );
    assert!(replaced.starts_with("local all all trust\n"));
    assert!(!replaced.contains("old"));
    assert_eq!(replaced.matches(HBA_BEGIN).count(), 1);
}

#[test]
fn a_replica_allowed_to_reclone_discards_its_data_and_copies_the_primary() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    std::fs::write(s.data.join("PG_VERSION"), "17\n").unwrap();
    std::fs::create_dir(s.data.join("base")).unwrap();
    std::fs::write(s.data.join("base").join("1"), "old").unwrap();
    let mut req: Request =
        serde_json::from_str(&request(&s, 1, Some(0), Some(Role::Replica))).unwrap();
    req.reclone = true;
    let mut h = Fake::default();
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/5000000|\n",
        "",
    );
    let out = handle("prepare", &serde_json::to_string(&req).unwrap(), &mut h).unwrap();
    assert!(out.contains("cloned"), "{out}");
    assert!(!s.data.join("base").exists(), "the old data went");
    assert!(s.data.exists(), "the directory stays");
    assert!(h.ran("pg_basebackup -D"));
    assert!(!h.ran("pg_controldata"));
}

#[test]
fn probe_writes_a_marker_and_reads_it_back() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let with = |probe: spi::Probe| {
        let mut r: Request = serde_json::from_str(&request(&s, 0, Some(0), None)).unwrap();
        r.probe = Some(probe);
        serde_json::to_string(&r).unwrap()
    };
    let mut h = Fake::default();
    let out = handle("probe", &with(spi::Probe::Write("k-1".into())), &mut h).unwrap();
    assert_eq!(out, r#"{"found":true}"#);
    assert!(h.ran("INSERT INTO legion_probe (key) VALUES ('k-1') ON CONFLICT DO NOTHING"));

    // No table yet: not found, nothing else asked.
    let mut h = Fake::default();
    h.on("to_regclass", true, "f\n", "");
    let out = handle("probe", &with(spi::Probe::Read("k-1".into())), &mut h).unwrap();
    assert_eq!(out, r#"{"found":false}"#);
    assert!(!h.ran("FROM legion_probe"));
    let mut h = Fake::default();
    h.on("to_regclass", true, "t\n", "");
    h.on("FROM legion_probe WHERE key = 'k-1'", true, "1\n", "");
    let out = handle("probe", &with(spi::Probe::Read("k-1".into())), &mut h).unwrap();
    assert_eq!(out, r#"{"found":true}"#);

    // A key that could say more than a key is refused.
    let e = handle(
        "probe",
        &with(spi::Probe::Write("x'); DROP".into())),
        &mut Fake::default(),
    )
    .unwrap_err();
    assert!(e.message.contains("bad key"), "{}", e.message);
    assert!(describe().capabilities.probe);
}

#[test]
fn asked_to_archive_every_member_spools_its_wal_through_the_plugin() {
    let o: Options = serde_json::from_value(json!({})).unwrap();
    let c = conf(
        &o,
        Path::new("/var/db/pg/data"),
        &member(1),
        None,
        Some("/.legion/replication/postgres"),
        None,
    );
    assert!(c.contains("archive_mode = always\n"), "{c}");
    assert!(
        c.contains(
            "archive_command = '\"/.legion/replication/postgres\" archive-wal \"%p\" \"%f\" \"/var/db/pg/data/legion_wal\"'\n"
        ),
        "{c}"
    );
    assert!(c.contains("archive_timeout = 60\n"), "{c}");
    let c = conf(
        &o,
        Path::new("/var/db/pg/data"),
        &member(1),
        None,
        None,
        None,
    );
    assert!(c.contains("archive_mode = off\n"), "{c}");
}

#[test]
fn archive_wal_keeps_each_file_whole_once_and_refuses_past_a_full_spool() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    let name = "000000010000000000000003";
    let wal = dir.path().join(name);
    std::fs::write(&wal, b"sixteen bytes!!!").unwrap();
    archive_wal(&wal, name, &spool).unwrap();
    assert_eq!(
        std::fs::read(spool.join(name)).unwrap(),
        b"sixteen bytes!!!"
    );
    assert!(!spool.join(format!("{name}.part")).exists());
    // Again, after a crash before PostgreSQL heard: kept already.
    archive_wal(&wal, name, &spool).unwrap();
    assert!(archive_wal(&wal, "../escape", &spool).is_err());
    // Full: PostgreSQL keeps its WAL and tries again.
    for i in 0..SPOOL_MAX {
        std::fs::write(spool.join(format!("f{i}")), b"").unwrap();
    }
    let e = archive_wal(&wal, "000000010000000000000004", &spool).unwrap_err();
    assert!(e.contains("Legion has not shipped"), "{e}");
}

#[test]
fn a_restored_backup_replays_the_log_to_the_time_asked_once() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let data = s.data.clone();
    // A backup of a replica, taken as it ran.
    for f in ["PG_VERSION", "standby.signal", "postmaster.pid"] {
        std::fs::write(data.join(f), "x\n").unwrap();
    }
    let restore = spi::Restore {
        id: "pg@2026-11-25T10:00:00Z".into(),
        log: "/var/db/pg/legion_restore".into(),
        to: Some("2026-11-25T10:00:00Z".into()),
    };
    let mut req: Request =
        serde_json::from_str(&request(&s, 0, Some(0), Some(Role::Primary))).unwrap();
    req.restore = Some(restore.clone());
    let req = serde_json::to_string(&req).unwrap();
    let out = handle("prepare", &req, &mut Fake::default()).unwrap();
    assert!(out.contains("restoring"), "{out}");
    assert!(data.join("recovery.signal").exists());
    assert!(!data.join("standby.signal").exists());
    assert!(!data.join("postmaster.pid").exists());
    let conf = std::fs::read_to_string(data.join(CONF)).unwrap();
    assert!(
        conf.contains("restore_command = 'cp \"/var/db/pg/legion_restore/%f\" \"%p\""),
        "{conf}"
    );
    // As PostgreSQL takes it: `Z` is refused as a time zone abbreviation
    // (found in the lab).
    assert!(
        conf.contains("recovery_target_time = '2026-11-25 10:00:00+00'"),
        "{conf}"
    );
    assert_eq!(
        pg_time("2026-11-25T10:00:00.250Z"),
        "2026-11-25 10:00:00.250+00"
    );
    assert!(
        conf.contains("recovery_target_action = 'promote'"),
        "{conf}"
    );

    // Replaying: status says the primary to be, not healthy; configure and
    // promote wait.
    let mut h = Fake::default();
    h.on("pg_is_in_recovery(), ", true, "t|1|100|f||0||", "");
    let st: Status = serde_json::from_str(&handle("status", &req, &mut h).unwrap()).unwrap();
    assert_eq!((st.role, st.healthy), (Role::Primary, false));
    assert_eq!(st.detail["restoring"], json!(true));
    let mut h = Fake::default();
    h.on("SELECT pg_is_in_recovery()", true, "t", "");
    let e = handle("configure", &req, &mut h).unwrap_err();
    assert_eq!(e.code, spi::RETRY);
    let e = handle("promote", &req, &mut Fake::default()).unwrap_err();
    assert_eq!(e.code, spi::RETRY);

    // A restart while replaying: carried on, not set up anew.
    std::fs::write(data.join("recovery.signal"), "kept").unwrap();
    let out = handle("prepare", &req, &mut Fake::default()).unwrap();
    assert!(out.contains("replaying"), "{out}");
    assert_eq!(
        std::fs::read_to_string(data.join("recovery.signal")).unwrap(),
        "kept"
    );

    // Done (PostgreSQL removed the signal): told again, nothing is replayed.
    std::fs::remove_file(data.join("recovery.signal")).unwrap();
    let out = handle("prepare", &req, &mut Fake::default()).unwrap();
    assert!(out.contains("\"none\""), "{out}");
    assert!(!data.join("recovery.signal").exists());
    let conf = std::fs::read_to_string(data.join(CONF)).unwrap();
    assert!(!conf.contains("restore_command"), "{conf}");
    assert!(describe().capabilities.restore);
}

#[test]
fn a_restore_onto_no_database_is_refused() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let mut req: Request =
        serde_json::from_str(&request(&s, 0, Some(0), Some(Role::Primary))).unwrap();
    req.restore = Some(spi::Restore {
        id: "pg@end".into(),
        log: "/var/db/pg/legion_restore".into(),
        to: None,
    });
    let e = handle(
        "prepare",
        &serde_json::to_string(&req).unwrap(),
        &mut Fake::default(),
    )
    .unwrap_err();
    assert!(e.message.contains("holds no database"), "{}", e.message);
    assert!(
        !s.data.join("PG_VERSION").exists(),
        "no database made in its place"
    );
}

/// Seen 2026-11-25: an old primary rewound (`pg_rewind` removes what the
/// new primary lacks) lost a WAL segment it had archived but Legion had
/// not shipped, and a restore across it stopped short. The spool is
/// beside the data directory now, what PostgreSQL finished but had not
/// archived goes to it before the rewind, and a spool from before moves.
#[cfg(unix)]
#[test]
fn a_rewind_keeps_what_was_archived_but_not_shipped() {
    let s = setup();
    if !usable(&s) {
        return;
    }
    let spool = PathBuf::from(format!(
        "{}/data.legion_wal",
        s.data.parent().unwrap().display()
    ));
    assert_eq!(spool_dir(&s.data), spool);
    // An old primary: an old spool inside, a finished segment not archived.
    std::fs::write(
        s.data.join("PG_VERSION"),
        "18
",
    )
    .unwrap();
    std::fs::create_dir(s.data.join(SPOOL)).unwrap();
    std::fs::write(s.data.join(SPOOL).join("000000040000000000000004"), "four").unwrap();
    let status = s.data.join("pg_wal/archive_status");
    std::fs::create_dir_all(&status).unwrap();
    std::fs::write(s.data.join("pg_wal/000000040000000000000005"), "five").unwrap();
    std::fs::write(status.join("000000040000000000000005.ready"), "").unwrap();
    let mut req: Request =
        serde_json::from_str(&request(&s, 1, Some(0), Some(Role::Replica))).unwrap();
    req.archive = true;
    let mut h = Fake::default();
    h.on("pg_controldata", true, CONTROL, "");
    h.on(
        "IDENTIFY_SYSTEM",
        true,
        "7300000000000000001|2|0/5000000|
",
        "",
    );
    h.on("pg_rewind", true, "", "pg_rewind: Done!");
    let out = handle("prepare", &serde_json::to_string(&req).unwrap(), &mut h).unwrap();
    assert!(out.contains("rewound"), "{out}");
    let read = |n: &str| std::fs::read_to_string(spool.join(n)).unwrap();
    assert_eq!(
        read("000000040000000000000004"),
        "four",
        "moved from the old spool"
    );
    assert_eq!(
        read("000000040000000000000005"),
        "five",
        "archived before the rewind"
    );
    assert!(status.join("000000040000000000000005.done").exists());
    assert!(!s.data.join(SPOOL).exists(), "the old spool went");
    let conf = std::fs::read_to_string(s.data.join(CONF)).unwrap();
    assert!(
        conf.contains(&format!("\"{}\"'", spool.display())),
        "archive_command spools beside: {conf}"
    );
}

/// A spool that must stay in the data directory (its top a filesystem's)
/// is shipped before anything replaces the data directory; one beside it
/// never holds that up.
#[test]
fn a_spool_inside_the_data_directory_is_shipped_before_it_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let inside = data.join(SPOOL);
    let beside = dir.path().join("data.legion_wal");
    std::fs::create_dir_all(&inside).unwrap();
    std::fs::create_dir_all(&beside).unwrap();
    assert_eq!(unshipped_inside(&data, &inside), None, "empty");
    std::fs::write(inside.join("000000010000000000000001.part"), "").unwrap();
    assert_eq!(
        unshipped_inside(&data, &inside),
        None,
        "only one being written"
    );
    std::fs::write(inside.join("000000010000000000000001"), "").unwrap();
    assert_eq!(unshipped_inside(&data, &inside), Some(1));
    std::fs::write(beside.join("000000010000000000000002"), "").unwrap();
    assert_eq!(unshipped_inside(&data, &beside), None);
    // Where it is: beside on the data directory's filesystem; inside when
    // that cannot be told (no data directory yet).
    let none = dir.path().join("none");
    assert_eq!(
        spool_dir(&none),
        PathBuf::from(format!("{}/{SPOOL}", none.display()))
    );
}
