//! Scheduled snapshots and backups (DESIGN.md §4.3) on three nodes: a
//! Volume written to on n2 keeps its scheduled snapshots pruned to `keep`;
//! its backups go to n3 over SSH, whole at first and then what changed,
//! pruned there too; one is restored onto n3 as a new Volume with the data
//! as it was; then to a directory on n2, a file each; then to an S3 bucket
//! (MinIO on n3), an object each, one restored from there. Then back to
//! n3 over SSH through a failover of the leader and a drain of n2, which
//! moves the Volume to n3: both schedules go on, pruned to `keep`, and a
//! backup taken after the move restores on n1. No `snapshot-late` or
//! `backup-late` alert stands at the end.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root: this host is the manager n1,
//! the first VM the worker n2, the second the manager n3, which is also
//! the backup host, reached over SSH by a user of the test's own.

mod common;

use common::{Lab, text, vm};
use serde_json::Value;

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";
/// The user and dataset on n3 that backups go to.
const BK_USER: &str = "legionbk";
const BK_DATASET: &str = "llab/backups";
/// MinIO on n3: its data, user, password and bucket.
const S3_DIR: &str = "/var/tmp/legion-lab-s3";
const S3_USER: &str = "legionlab";
const S3_PASSWORD: &str = "legionlab-secret";
const S3_BUCKET: &str = "legion-lab";

/// The Volume, a writer on n2 appending a line every second, and its
/// snapshot schedule and backups.
fn app(target: &str) -> String {
    app_placed(target, "node.name == n2")
}

/// [`app`], the writer placed by `constraint`.
fn app_placed(target: &str, constraint: &str) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Volume
metadata: {{ name: data, namespace: default }}
spec:
  snapshots: [{{ every: 1m, keep: 2 }}]
  backup: {{ target: {target}, every: 1m, keep: 2 }}
---
apiVersion: legion/v1
kind: Service
metadata: {{ name: writer, namespace: default }}
spec:
  replicas: 1
  placement: {{ constraints: ["{constraint}"] }}
  pod:
    volumes: [{{ name: data, volume: {{ name: data }} }}]
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sh, -c, "while :; do date >> /data/log; sleep 1; done"]
        volumeMounts: [{{ name: data, path: /data }}]
"#
    )
}

/// The Volume `name` in the default namespace, as JSON.
fn volume_named(lab: &Lab, name: &str) -> Value {
    lab.list("volumes", "default")
        .into_iter()
        .find(|v| v["metadata"]["name"] == name)
        .unwrap_or_default()
}

fn volume(lab: &Lab) -> Value {
    volume_named(lab, "data")
}

/// Applies `yaml`, as a file, as `legion apply -f` does.
fn apply(lab: &Lab, yaml: &str) {
    let file = lab.dir.join("applied.yaml");
    std::fs::write(&file, yaml).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
}

/// The datasets and snapshots `node` holds under `prefix`.
fn zfs(lab: &Lab, node: &str, prefix: &str) -> Vec<String> {
    let out = lab.on(
        node,
        &format!("zfs list -H -o name -t all -r {prefix} 2>/dev/null"),
    );
    text(&out.stdout).lines().map(str::to_owned).collect()
}

/// The newest backup run, if it went well.
fn last_ok(v: &Value) -> Option<Value> {
    v["status"]["backup"]["runs"]
        .as_array()?
        .last()
        .filter(|r| r["error"].as_str().unwrap_or("").is_empty())
        .cloned()
}

/// The scheduled snapshots `node` holds of the Volume, by name.
fn auto_held(lab: &Lab, node: &str) -> Vec<String> {
    let dataset = format!("{}/volumes/default/data", common::ZFS_ROOT);
    zfs(lab, node, &dataset)
        .into_iter()
        .filter_map(|s| s.split_once("@legion-auto-1m-").map(|(_, t)| t.to_owned()))
        .collect()
}

/// Waits, after `what`, for two snapshots taken since on `node`, the older
/// pruned, and two backups run well since. Asked through n1, which may not
/// lead.
fn check_schedules_go_on(lab: &Lab, node: &str, what: &str) {
    let v = volume(lab);
    let since = v["status"]["snapshots"][0]["last"]
        .as_str()
        .unwrap_or("")
        .to_owned();
    let since = since
        .rsplit("legion-auto-1m-")
        .next()
        .unwrap_or("")
        .to_owned();
    let since_run = last_ok(&v).and_then(|r| r["atMs"].as_i64()).unwrap_or(0);
    let mut polls = 0;
    lab.wait_or_report(&format!("{what}: snapshots and backups go on"), 420, || {
        let held = auto_held(lab, node);
        let v = volume(lab);
        polls += 1;
        if polls % 30 == 0 {
            eprintln!(
                "{what}: since {since} / {since_run}, {node} holds {held:?}; snapshots {}; backup {}",
                v["status"]["snapshots"], v["status"]["backup"]
            );
        }
        let runs = v["status"]["backup"]["runs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let good = runs
            .iter()
            .filter(|r| {
                r["atMs"].as_i64().unwrap_or(0) > since_run
                    && r["error"].as_str().unwrap_or("").is_empty()
            })
            .count();
        held.len() == 2
            && held.iter().all(|t| t.as_str() > since.as_str())
            && good >= 2
            && last_ok(&v).is_some()
    });
    let v = volume(lab);
    for r in v["status"]["backup"]["runs"]
        .as_array()
        .into_iter()
        .flatten()
    {
        if !r["error"].as_str().unwrap_or("").is_empty() {
            eprintln!("{what}: a run failed: {r}");
        }
    }
}

/// Puts n3 back as it was: no backup user, no backups dataset.
struct Cleanup<'a>(&'a Lab);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let _ = self.0.on(
            "n3",
            &format!(
                "zfs destroy -r {BK_DATASET} 2>/dev/null; pw userdel {BK_USER} -r 2>/dev/null; true"
            ),
        );
        let _ = self.0.on("n2", "rm -rf /var/tmp/legion-lab-bk");
        let _ = self.0.on(
            "n3",
            &format!("pkill -f 'minio server {S3_DIR}'; sleep 1; rm -rf {S3_DIR} /root/.mc; true"),
        );
    }
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD and two more nodes; see tests/common/mod.rs"]
fn volumes_keep_scheduled_snapshots_and_backups_that_restore() {
    let mut lab = Lab::from_env();
    lab.three_nodes(&[]);
    let _cleanup = Cleanup(&lab);
    let n3 = &lab.vms[vm("n3")];

    // n3 takes backups as a user of their own, allowed only what a
    // receive and its pruning need under one dataset.
    let key = lab.dir.join("bk_key");
    let _ = std::fs::remove_file(&key);
    let made = std::process::Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f"])
        .arg(&key)
        .status()
        .unwrap();
    assert!(made.success(), "ssh-keygen");
    let public = std::fs::read_to_string(key.with_extension("pub")).unwrap();
    let out = lab.on(
        "n3",
        &format!(
            "set -e
            pw useradd {BK_USER} -m -s /bin/sh 2>/dev/null || true
            mkdir -p /home/{BK_USER}/.ssh
            echo '{}' > /home/{BK_USER}/.ssh/authorized_keys
            chown -R {BK_USER} /home/{BK_USER}/.ssh
            chmod 700 /home/{BK_USER}/.ssh; chmod 600 /home/{BK_USER}/.ssh/authorized_keys
            zfs create -o mountpoint=none {BK_DATASET}
            zfs allow -u {BK_USER} create,receive,mount,destroy,snapshot,send,hold {BK_DATASET}",
            public.trim()
        ),
    );
    assert!(out.status.success(), "{}", text(&out.stderr));
    let scanned = std::process::Command::new("ssh-keyscan")
        .args(["-t", "ed25519", &n3.addr])
        .output()
        .unwrap();
    let host_key = text(&scanned.stdout)
        .lines()
        .find(|l| !l.starts_with('#'))
        .and_then(|l| l.split_once(' ').map(|(_, k)| k.trim().to_owned()))
        .expect("n3's host key");
    let secret = format!(
        "apiVersion: legion/v1\nkind: Secret\nmetadata: {{ name: bk-key, namespace: default }}\nspec:\n  text: |\n{}",
        std::fs::read_to_string(&key)
            .unwrap()
            .lines()
            .map(|l| format!("    {l}\n"))
            .collect::<String>()
    );
    apply(&lab, &secret);
    apply(&lab, &format!(
        "apiVersion: legion/v1
kind: BackupTarget
metadata: {{ name: offsite }}
spec:
  ssh: {{ host: \"{}\", user: {BK_USER}, dataset: {BK_DATASET}, keySecret: {{ namespace: default, name: bk-key }}, hostKey: \"{host_key}\" }}
---
apiVersion: legion/v1
kind: BackupTarget
metadata: {{ name: local }}
spec:
  directory: {{ path: /var/tmp/legion-lab-bk }}
",
        n3.addr
    ));
    apply(&lab, &app("offsite"));
    let dataset = format!("{}/volumes/default/data", common::ZFS_ROOT);

    // Snapshots: one a minute, the last two kept.
    lab.wait_or_report("two scheduled snapshots, the oldest pruned", 300, || {
        let v = volume(&lab);
        let taken = v["status"]["snapshots"][0]["last"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        let held: Vec<_> = zfs(&lab, "n2", &dataset)
            .into_iter()
            .filter(|s| s.contains("@legion-auto-1m-"))
            .collect();
        held.len() == 2 && !taken.is_empty() && held.iter().any(|h| h.ends_with(&taken)) && {
            // A third taken means one was pruned.
            v["status"]["snapshots"][0]["kept"] == 2
        }
    });
    let listed = lab.ok(&["volume", "snapshots", "data"]);
    assert_eq!(listed.matches("legion-auto-1m-").count(), 2, "{listed}");
    assert!(
        !listed.contains("legion-bk-"),
        "a backup's base is not listed: {listed}"
    );

    // Backups: whole, then what changed, the last two kept on n3.
    lab.wait_or_report("three backups to n3, the last incremental", 420, || {
        let runs = volume(&lab)["status"]["backup"]["runs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        runs.len() >= 3
            && runs
                .iter()
                .all(|r| r["error"].as_str().unwrap_or("").is_empty())
            && runs[0]["whole"] == true
            && runs.last().unwrap()["whole"] != true
    });
    let held = lab.ok(&["volume", "backups", "data"]);
    let names: Vec<String> = held
        .lines()
        .skip(1)
        .filter_map(|l| l.split_whitespace().next().map(str::to_owned))
        .collect();
    assert_eq!(names.len(), 2, "pruned to keep there: {held}");
    let on_n3 = zfs(&lab, "n3", BK_DATASET);
    assert_eq!(
        on_n3
            .iter()
            .filter(|s| s.contains("@legion-bk-offsite-"))
            .count(),
        2,
        "{on_n3:?}"
    );
    let here: Vec<_> = zfs(&lab, "n2", &dataset)
        .into_iter()
        .filter(|s| s.contains("@legion-bk-"))
        .collect();
    assert_eq!(
        here.len(),
        1,
        "the holder keeps only the next base: {here:?}"
    );

    // One restored onto n3, as a new Volume, its lines there.
    lab.ok(&[
        "volume",
        "restore",
        "--from",
        "offsite",
        &format!("data@{}", names[0]),
        "--node",
        "n3",
        "--name",
        "data-back",
    ]);
    let restored = format!("{}/volumes/default/data-back", common::ZFS_ROOT);
    let lines = lab.on(
        "n3",
        &format!("cat \"$(zfs get -H -o value mountpoint {restored})/log\" | wc -l"),
    );
    let n: u64 = text(&lines.stdout).trim().parse().unwrap_or(0);
    assert!(n > 30, "the restored data has the writer's lines: {n}");
    let back = volume_named(&lab, "data-back");
    assert_eq!(back["status"]["node"], "n3", "{back}");

    // A backup cut short (its SSH connection killed as it sends): n3 keeps
    // what came, its resume token says so, and a later run goes on from
    // there; none is left once it has.
    let tokens = || {
        let out = lab.on(
            "n3",
            &format!("zfs get -H -o name,value -r -t filesystem receive_resume_token {BK_DATASET}"),
        );
        text(&out.stdout)
            .lines()
            .filter(|l| l.split('\t').nth(1).is_some_and(|t| t.starts_with("1-")))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    // The dataset as the agent words it, quoted for the remote shell.
    let out = lab.on(
        "n2",
        &format!(
            "set -e
            dd if=/dev/urandom of=\"$(zfs get -H -o value mountpoint {dataset})/ballast\" bs=1m count=300 2>/dev/null
            i=0
            until pgrep -qf 'ssh .*zfs receive -s -u .?{BK_DATASET}'; do
                i=$((i + 1)); [ $i -lt 3000 ] || {{ echo 'no backup came' >&2; exit 3; }}
                sleep 0.1
            done
            sleep 1
            pkill -KILL -f 'ssh .*zfs receive -s -u .?{BK_DATASET}'"
        ),
    );
    assert!(
        out.status.success(),
        "cutting a backup short: {}",
        text(&out.stderr)
    );
    lab.wait_or_report("what came of the backup cut short kept on n3", 60, || {
        !tokens().is_empty()
    });
    eprintln!("kept on n3: {:?}", tokens());
    let cut_runs = volume(&lab)["status"]["backup"]["runs"]
        .as_array()
        .map_or(0, Vec::len);
    lab.wait_or_report("the backup cut short gone on with", 420, || {
        let runs = volume(&lab)["status"]["backup"]["runs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        tokens().is_empty()
            && runs.len() > cut_runs
            && runs
                .last()
                .is_some_and(|r| r["error"].as_str().unwrap_or("").is_empty())
    });

    // To a directory on n2: a file a backup, whole.
    apply(&lab, &app("local"));
    lab.wait_or_report("two backups as files on n2", 300, || {
        let out = lab.on("n2", "ls /var/tmp/legion-lab-bk/default.data/ 2>/dev/null");
        text(&out.stdout)
            .lines()
            .filter(|l| l.ends_with(".zfs"))
            .count()
            == 2
    });

    // To an S3-compatible bucket: MinIO on n3, reached from n2 over HTTP,
    // signed. MinIO's own client says what the bucket holds.
    let mc = "$(command -v minio-client || command -v mc)";
    let out = lab.on(
        "n3",
        &format!(
            "set -e
            command -v minio >/dev/null || ASSUME_ALWAYS_YES=yes pkg install -q minio minio-client >/dev/null
            mkdir -p {S3_DIR}
            MINIO_ROOT_USER={S3_USER} MINIO_ROOT_PASSWORD={S3_PASSWORD} \
                daemon -f minio server {S3_DIR} --address :9000 --quiet
            i=0
            until {mc} alias set lab http://127.0.0.1:9000 {S3_USER} {S3_PASSWORD} >/dev/null 2>&1; do
                i=$((i + 1)); [ $i -lt 60 ] || {{ echo 'MinIO did not start' >&2; exit 3; }}
                sleep 1
            done
            {mc} mb -p lab/{S3_BUCKET} >/dev/null"
        ),
    );
    assert!(out.status.success(), "MinIO on n3: {}", text(&out.stderr));
    apply(
        &lab,
        &format!(
            "apiVersion: legion/v1
kind: Secret
metadata: {{ name: s3-key, namespace: default }}
spec:
  text: {S3_PASSWORD}
---
apiVersion: legion/v1
kind: BackupTarget
metadata: {{ name: cloud }}
spec:
  s3: {{ endpoint: \"http://{}:9000\", bucket: {S3_BUCKET}, prefix: lab/run, accessKeyId: {S3_USER}, secretKeySecret: {{ namespace: default, name: s3-key }} }}
",
            n3.addr
        ),
    );
    apply(&lab, &app("cloud"));
    let objects = || {
        let out = lab.on(
            "n3",
            &format!("{mc} ls --recursive lab/{S3_BUCKET}/lab/run/default.data/ 2>/dev/null"),
        );
        text(&out.stdout)
            .lines()
            .filter(|l| l.ends_with(".zfs"))
            .count()
    };
    lab.wait_or_report(
        "two backups as objects in the bucket, pruned to keep",
        360,
        || {
            let v = volume(&lab);
            let b = &v["status"]["backup"];
            let runs = b["runs"].as_array().cloned().unwrap_or_default();
            // A new target's status starts afresh: a third run there means one
            // was pruned.
            b["target"] == "cloud"
                && b["failures"].as_u64().unwrap_or(0) == 0
                && runs.len() >= 3
                && runs
                    .last()
                    .is_some_and(|r| r["error"].as_str().unwrap_or("").is_empty())
                && objects() == 2
        },
    );
    let held = lab.ok(&["volume", "backups", "data", "--from", "cloud"]);
    let first = held
        .lines()
        .skip(1)
        .find_map(|l| l.split_whitespace().next())
        .expect("a backup in the bucket")
        .to_owned();
    lab.ok(&[
        "volume",
        "restore",
        "--from",
        "cloud",
        &format!("data@{first}"),
        "--node",
        "n3",
        "--name",
        "data-s3",
    ]);
    let restored = format!("{}/volumes/default/data-s3", common::ZFS_ROOT);
    let lines = lab.on(
        "n3",
        &format!("cat \"$(zfs get -H -o value mountpoint {restored})/log\" | wc -l"),
    );
    let n: u64 = text(&lines.stdout).trim().parse().unwrap_or(0);
    assert!(
        n > 30,
        "the data restored from the bucket has the writer's lines: {n}"
    );

    // Back to n3 over SSH, through a failover and a drain (TODO.MD 1's
    // "done when"): the writer may now run anywhere but n1, so a drain of
    // n2 can only take it, and its Volume, to n3.
    apply(&lab, &app_placed("offsite", "node.name != n1"));
    lab.wait_or_report("a backup to n3 again", 300, || {
        let v = volume(&lab);
        v["status"]["backup"]["target"] == "offsite" && last_ok(&v).is_some()
    });
    check_schedules_go_on(&lab, "n2", "back to n3 over SSH");

    // A real failover: n2 promoted, so three managers vote; n3 made the
    // leader and its daemon stopped. The two left elect one of them, which
    // takes over the schedules from their status in the Volume.
    lab.ok(&["node", "promote", "n2"]);
    lab.wait_or_report("three voters", 300, || {
        lab.ok(&["status"])
            .lines()
            .filter(|l| l.trim_end().ends_with("voter"))
            .count()
            == 3
    });
    lab.ok(&["node", "lead", "n3"]);
    let out = lab.on(
        "n3",
        &format!(
            "{}
true
",
            common::STOP
        ),
    );
    assert!(out.status.success(), "stopping n3: {}", text(&out.stderr));
    check_schedules_go_on(&lab, "n2", "n3, the leader, stopped");
    let status = lab.ok(&["status"]);
    eprintln!(
        "after the failover:
{status}"
    );
    lab.restart_vm(vm("n3"), common::Role::Manager);
    lab.wait_or_report("n3 back", 120, || lab.node_state("n3") == "ready");

    // A drain: the Volume moves to n3 with its writer. The base it backed
    // up from stays on n2, so the first backup from n3 is whole and the
    // next what changed since; both schedules carry on pruned to `keep`.
    let before = last_ok(&volume(&lab)).unwrap();
    let out = lab.legion(
        &[
            "node",
            "drain",
            "n2",
            "--wait",
            "--timeout",
            "900",
            "-o",
            "json",
        ],
        &[],
    );
    assert!(
        out.status.success(),
        "drain --wait: {}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let done: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(done["phase"], "completed", "{done}");
    assert_eq!(
        volume(&lab)["status"]["node"],
        "n3",
        "the Volume moved with its writer"
    );
    check_schedules_go_on(&lab, "n3", "n2 drained");
    let v = volume(&lab);
    let after: Vec<Value> = v["status"]["backup"]["runs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            r["atMs"].as_i64() > before["atMs"].as_i64()
                && r["error"].as_str().unwrap_or("").is_empty()
        })
        .collect();
    assert_eq!(
        after.first().map(|r| r["whole"] == true),
        Some(true),
        "the first from n3 is whole: {v}"
    );
    assert_eq!(
        after.last().map(|r| r["whole"] != true),
        Some(true),
        "then what changed: {v}"
    );
    assert_eq!(v["status"]["backup"]["from"], "n3", "{v}");
    let on_n3 = zfs(&lab, "n3", BK_DATASET);
    assert_eq!(
        on_n3
            .iter()
            .filter(|s| s.contains("@legion-bk-offsite-"))
            .count(),
        2,
        "pruned there: {on_n3:?}"
    );
    let here: Vec<_> = zfs(&lab, "n3", &dataset)
        .into_iter()
        .filter(|s| s.contains("@legion-bk-"))
        .collect();
    assert_eq!(here.len(), 1, "n3 keeps only the next base: {here:?}");

    // The newest restored onto n1, a node that never held the data: it has
    // the lines written on n3.
    let held = lab.ok(&["volume", "backups", "data"]);
    let newest = held
        .lines()
        .skip(1)
        .filter_map(|l| l.split_whitespace().next())
        .max()
        .expect("a backup on n3")
        .to_owned();
    lab.ok(&[
        "volume",
        "restore",
        "--from",
        "offsite",
        &format!("data@{newest}"),
        "--node",
        "n1",
        "--name",
        "data-moved",
    ]);
    let restored = format!("{}/volumes/default/data-moved", common::ZFS_ROOT);
    let lines = std::process::Command::new("/bin/sh")
        .args([
            "-c",
            &format!("cat \"$(zfs get -H -o value mountpoint {restored})/log\" | wc -l"),
        ])
        .output()
        .unwrap();
    let moved: u64 = text(&lines.stdout).trim().parse().unwrap_or(0);
    assert!(
        moved > n,
        "restored after the drain, it has the later lines: {moved} against {n}"
    );

    let alerts = lab.ok(&["alerts"]);
    assert!(
        !alerts.contains("snapshot-late") && !alerts.contains("backup-late"),
        "{alerts}"
    );
}
