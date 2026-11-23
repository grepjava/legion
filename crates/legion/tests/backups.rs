//! Scheduled snapshots and backups (DESIGN.md §4.3) on three nodes: a
//! Volume written to on n2 keeps its scheduled snapshots pruned to `keep`;
//! its backups go to n3 over SSH, whole at first and then what changed,
//! pruned there too; one is restored onto n3 as a new Volume with the data
//! as it was; then to a directory on n2, a file each. No `snapshot-late`
//! or `backup-late` alert stands meanwhile.
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

/// The Volume, a writer on n2 appending a line every second, and its
/// snapshot schedule and backups.
fn app(target: &str) -> String {
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
  placement: {{ constraints: ["node.name == n2"] }}
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
    let out = lab.on(node, &format!("zfs list -H -o name -t all -r {prefix} 2>/dev/null"));
    text(&out.stdout).lines().map(str::to_owned).collect()
}

/// Puts n3 back as it was: no backup user, no backups dataset.
struct Cleanup<'a>(&'a Lab);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let _ = self.0.on(
            "n3",
            &format!("zfs destroy -r {BK_DATASET} 2>/dev/null; pw userdel {BK_USER} -r 2>/dev/null; true"),
        );
        let _ = self.0.on("n2", "rm -rf /var/tmp/legion-lab-bk");
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
        let taken = v["status"]["snapshots"][0]["last"].as_str().unwrap_or("").to_owned();
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
    assert!(!listed.contains("legion-bk-"), "a backup's base is not listed: {listed}");

    // Backups: whole, then what changed, the last two kept on n3.
    lab.wait_or_report("three backups to n3, the last incremental", 420, || {
        let runs = volume(&lab)["status"]["backup"]["runs"].as_array().cloned().unwrap_or_default();
        runs.len() >= 3
            && runs.iter().all(|r| r["error"].as_str().unwrap_or("").is_empty())
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
    assert_eq!(on_n3.iter().filter(|s| s.contains("@legion-bk-offsite-")).count(), 2, "{on_n3:?}");
    let here: Vec<_> = zfs(&lab, "n2", &dataset)
        .into_iter()
        .filter(|s| s.contains("@legion-bk-"))
        .collect();
    assert_eq!(here.len(), 1, "the holder keeps only the next base: {here:?}");

    // One restored onto n3, as a new Volume, its lines there.
    lab.ok(&[
        "volume", "restore", "--from", "offsite", &format!("data@{}", names[0]), "--node", "n3", "--name",
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
    let out = lab.on(
        "n2",
        &format!(
            "set -e
            dd if=/dev/urandom of=\"$(zfs get -H -o value mountpoint {dataset})/ballast\" bs=1m count=300 2>/dev/null
            i=0
            until pgrep -qf 'ssh .*zfs receive -s -u {BK_DATASET}'; do
                i=$((i + 1)); [ $i -lt 3000 ] || {{ echo 'no backup came' >&2; exit 3; }}
                sleep 0.1
            done
            sleep 1
            pkill -KILL -f 'ssh .*zfs receive -s -u {BK_DATASET}'"
        ),
    );
    assert!(out.status.success(), "cutting a backup short: {}", text(&out.stderr));
    lab.wait_or_report("what came of the backup cut short kept on n3", 60, || {
        !tokens().is_empty()
    });
    eprintln!("kept on n3: {:?}", tokens());
    let cut_runs = volume(&lab)["status"]["backup"]["runs"]
        .as_array()
        .map_or(0, Vec::len);
    lab.wait_or_report("the backup cut short gone on with", 420, || {
        let runs = volume(&lab)["status"]["backup"]["runs"].as_array().cloned().unwrap_or_default();
        tokens().is_empty()
            && runs.len() > cut_runs
            && runs.last().is_some_and(|r| r["error"].as_str().unwrap_or("").is_empty())
    });

    // To a directory on n2: a file a backup, whole.
    apply(&lab, &app("local"));
    lab.wait_or_report("two backups as files on n2", 300, || {
        let out = lab.on("n2", "ls /var/tmp/legion-lab-bk/default.data/ 2>/dev/null");
        text(&out.stdout).lines().filter(|l| l.ends_with(".zfs")).count() == 2
    });

    let alerts = lab.ok(&["alerts"]);
    assert!(
        !alerts.contains("snapshot-late") && !alerts.contains("backup-late"),
        "{alerts}"
    );
}
