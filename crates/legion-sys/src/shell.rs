//! [`Sys`] by running the FreeBSD tools. Command forms and outputs were
//! checked on FreeBSD 15.1 (see the tests at the bottom for the captured
//! outputs).

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::process::{Command, Stdio};

use serde::Deserialize;

use crate::{Dataset, Destination, Epair, Error, IfConfig, Jail, RctlRule, Result, Sys};

/// Runs one program to completion.
pub trait Runner: Send + Sync {
    fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> std::io::Result<CommandOutput>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs programs on this host.
#[derive(Debug, Clone, Copy, Default)]
pub struct Host;

impl Runner for Host {
    fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> std::io::Result<CommandOutput> {
        use std::io::Write;
        let mut child = Command::new(program)
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(input) = stdin {
            child
                .stdin
                .take()
                .expect("piped stdin")
                .write_all(input.as_bytes())?;
        }
        let out = child.wait_with_output()?;
        Ok(CommandOutput {
            success: out.status.success(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

/// [`Sys`] over the FreeBSD command-line tools.
#[derive(Debug, Clone, Default)]
pub struct Shell<R = Host> {
    runner: R,
}

impl Shell<Host> {
    pub fn new() -> Self {
        Shell { runner: Host }
    }
}

impl<R: Runner> Shell<R> {
    pub fn with_runner(runner: R) -> Self {
        Shell { runner }
    }

    fn output(&self, program: &str, args: &[String], stdin: Option<&str>) -> Result<CommandOutput> {
        let command = describe(program, args);
        tracing::debug!(%command, "run");
        self.runner
            .run(program, args, stdin)
            .map_err(|source| Error::Spawn { command, source })
    }

    /// Runs a command that must succeed; returns its stdout.
    fn run(&self, program: &str, args: &[String]) -> Result<String> {
        self.run_input(program, args, None)
    }

    fn run_input(&self, program: &str, args: &[String], stdin: Option<&str>) -> Result<String> {
        let out = self.output(program, args, stdin)?;
        if out.success {
            Ok(out.stdout)
        } else {
            Err(failed(program, args, &out))
        }
    }
}

fn describe(program: &str, args: &[String]) -> String {
    std::iter::once(program)
        .chain(args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

fn failed(program: &str, args: &[String], out: &CommandOutput) -> Error {
    let message = out.stderr.trim();
    Error::Failed {
        command: describe(program, args),
        message: if message.is_empty() {
            "failed with no message".to_owned()
        } else {
            message.to_owned()
        },
    }
}

fn args<const N: usize>(a: [&str; N]) -> Vec<String> {
    a.iter().map(|s| (*s).to_owned()).collect()
}

/// `ifconfig -j <jail>` / `route -j <jail>` prefix.
fn in_jail(jail: Option<&str>) -> Vec<String> {
    match jail {
        Some(j) => args(["-j", j]),
        None => Vec::new(),
    }
}

#[derive(Deserialize)]
struct JlsOutput {
    #[serde(rename = "jail-information")]
    info: JlsInfo,
}

#[derive(Deserialize)]
struct JlsInfo {
    jail: Vec<JlsJail>,
}

/// `jls --libxo=json` writes every value as a string.
#[derive(Deserialize)]
struct JlsJail {
    jid: String,
    name: String,
    vnet: String,
    #[serde(rename = "children.max")]
    children_max: String,
    #[serde(rename = "children.cur")]
    children_cur: String,
    parent: String,
}

impl<R: Runner> Sys for Shell<R> {
    fn kld_load(&self, module: &str) -> Result<()> {
        self.run("kldload", &args(["-n", module])).map(drop)
    }

    fn jails(&self) -> Result<Vec<Jail>> {
        let a = args([
            "--libxo=json",
            "-n",
            "jid",
            "name",
            "vnet",
            "children.max",
            "children.cur",
            "parent",
        ]);
        let out = self.run("jls", &a)?;
        parse_jls(&out).map_err(|message| Error::Parse {
            command: describe("jls", &a),
            message,
        })
    }

    fn jail_set(&self, jail: &str, params: &[(&str, &str)]) -> Result<()> {
        let mut a = vec!["-m".to_owned(), format!("name={jail}")];
        a.extend(params.iter().map(|(k, v)| format!("{k}={v}")));
        self.run("jail", &a).map(drop)
    }

    fn interfaces(&self, jail: Option<&str>) -> Result<Vec<String>> {
        let mut a = in_jail(jail);
        a.push("-l".into());
        Ok(self
            .run("ifconfig", &a)?
            .split_whitespace()
            .map(str::to_owned)
            .collect())
    }

    fn bridge_create(&self, name: &str) -> Result<()> {
        let created = self.run("ifconfig", &args(["bridge", "create"]))?;
        let created = created.trim();
        if let Err(e) = self.run("ifconfig", &args([created, "name", name])) {
            let _ = self.run("ifconfig", &args([created, "destroy"]));
            return Err(e);
        }
        Ok(())
    }

    fn epair_create(&self) -> Result<Epair> {
        let a = args(["epair", "create"]);
        let out = self.run("ifconfig", &a)?;
        let name = out.trim();
        match name.strip_suffix('a') {
            Some(base) if !base.is_empty() => Ok(Epair {
                a: name.to_owned(),
                b: format!("{base}b"),
            }),
            _ => Err(Error::Parse {
                command: describe("ifconfig", &a),
                message: format!("expected an `a` interface name, got {name:?}"),
            }),
        }
    }

    fn interface_destroy(&self, name: &str) -> Result<()> {
        self.run("ifconfig", &args([name, "destroy"])).map(drop)
    }

    fn interface_rename(&self, name: &str, new_name: &str) -> Result<()> {
        self.run("ifconfig", &args([name, "name", new_name]))
            .map(drop)
    }

    fn bridge_add_member(&self, bridge: &str, member: &str) -> Result<()> {
        self.run("ifconfig", &args([bridge, "addm", member]))
            .map(drop)
    }

    fn interface_move(&self, name: &str, jail: &str) -> Result<()> {
        self.run("ifconfig", &args([name, "vnet", jail])).map(drop)
    }

    fn interface_configure(&self, jail: Option<&str>, name: &str, config: &IfConfig) -> Result<()> {
        let mut a = in_jail(jail);
        a.push(name.to_owned());
        if let Some(addr) = config.addr {
            a.extend(["inet".to_owned(), addr.to_string()]);
        }
        if let Some(mtu) = config.mtu {
            a.extend(["mtu".to_owned(), mtu.to_string()]);
        }
        if config.up {
            a.push("up".into());
        }
        self.run("ifconfig", &a).map(drop)
    }

    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: Ipv4Addr) -> Result<()> {
        let mut a = in_jail(jail);
        a.extend(["-q".to_owned(), "add".to_owned()]);
        a.extend(route_dest(dest));
        a.push(gateway.to_string());
        self.run("route", &a).map(drop)
    }

    fn route_delete(&self, jail: Option<&str>, dest: Destination) -> Result<()> {
        let mut a = in_jail(jail);
        a.extend(["-q".to_owned(), "delete".to_owned()]);
        a.extend(route_dest(dest));
        self.run("route", &a).map(drop)
    }

    fn rctl_add(&self, rule: &RctlRule) -> Result<()> {
        self.run("rctl", &["-a".to_owned(), rule.to_string()])
            .map(drop)
    }

    fn rctl_remove_jail(&self, jail: &str) -> Result<()> {
        let a = vec!["-r".to_owned(), format!("jail:{jail}")];
        let out = self.output("rctl", &a, None)?;
        // ESRCH when the jail has no rules.
        if out.success || out.stderr.contains("No such process") {
            Ok(())
        } else {
            Err(failed("rctl", &a, &out))
        }
    }

    fn rctl_usage(&self, jail: &str) -> Result<BTreeMap<String, u64>> {
        let a = vec!["-u".to_owned(), format!("jail:{jail}")];
        let out = self.run("rctl", &a)?;
        out.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                line.split_once('=')
                    .and_then(|(k, v)| Some((k.to_owned(), v.trim().parse().ok()?)))
                    .ok_or_else(|| Error::Parse {
                        command: describe("rctl", &a),
                        message: format!("bad usage line {line:?}"),
                    })
            })
            .collect()
    }

    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()> {
        self.run_input("pfctl", &args(["-a", anchor, "-f", "-"]), Some(rules))
            .map(drop)
    }

    fn pf_flush_anchor(&self, anchor: &str) -> Result<()> {
        self.run("pfctl", &args(["-q", "-a", anchor, "-F", "all"]))
            .map(drop)
    }

    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[Ipv4Addr]) -> Result<()> {
        let mut a = args(["-q", "-a", anchor, "-t", table, "-T", "replace"]);
        a.extend(addrs.iter().map(Ipv4Addr::to_string));
        self.run("pfctl", &a).map(drop)
    }

    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<Ipv4Addr>> {
        let a = args(["-a", anchor, "-t", table, "-T", "show"]);
        let out = self.output("pfctl", &a, None)?;
        if !out.success {
            if out.stderr.contains("Table does not exist") {
                return Ok(Vec::new());
            }
            return Err(failed("pfctl", &a, &out));
        }
        out.stdout
            .split_whitespace()
            .map(|s| {
                s.parse().map_err(|_| Error::Parse {
                    command: describe("pfctl", &a),
                    message: format!("bad table entry {s:?}"),
                })
            })
            .collect()
    }

    fn zfs_list(&self, root: &str) -> Result<Vec<Dataset>> {
        let a = args([
            "list",
            "-H",
            "-p",
            "-r",
            "-t",
            "filesystem",
            "-o",
            "name,mountpoint,quota",
            root,
        ]);
        let out = self.output("zfs", &a, None)?;
        if !out.success {
            if out.stderr.contains("dataset does not exist") {
                return Ok(Vec::new());
            }
            return Err(failed("zfs", &a, &out));
        }
        out.stdout
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| {
                parse_dataset(line).ok_or_else(|| Error::Parse {
                    command: describe("zfs", &a),
                    message: format!("bad line {line:?}"),
                })
            })
            .collect()
    }

    fn zfs_create(&self, name: &str, props: &[(&str, &str)]) -> Result<()> {
        let mut a = args(["create", "-p"]);
        for (k, v) in props {
            a.extend(["-o".to_owned(), format!("{k}={v}")]);
        }
        a.push(name.to_owned());
        self.run("zfs", &a).map(drop)
    }

    fn zfs_set(&self, name: &str, prop: &str, value: &str) -> Result<()> {
        self.run(
            "zfs",
            &["set".to_owned(), format!("{prop}={value}"), name.to_owned()],
        )
        .map(drop)
    }

    fn zfs_snapshot(&self, snapshot: &str) -> Result<()> {
        self.run("zfs", &args(["snapshot", snapshot])).map(drop)
    }

    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()> {
        let a = if recursive {
            args(["destroy", "-r", name])
        } else {
            args(["destroy", name])
        };
        self.run("zfs", &a).map(drop)
    }

    fn mount_tmpfs(&self, path: &str, size: Option<u64>, mode: u32) -> Result<()> {
        let mut opts = format!("mode={mode:04o}");
        if let Some(size) = size {
            opts += &format!(",size={size}");
        }
        self.run("mount", &args(["-t", "tmpfs", "-o", &opts, "tmpfs", path]))
            .map(drop)
    }

    fn unmount(&self, path: &str) -> Result<()> {
        self.run("umount", &args(["-f", path])).map(drop)
    }

    fn mounts(&self, prefix: &str) -> Result<Vec<String>> {
        let out = self.run("mount", &args(["-p"]))?;
        Ok(parse_mounts(&out, prefix))
    }
}

/// Mount points from `mount -p` (fstab lines: source, mount point, type,
/// options, dump, pass) at or below `prefix`. fstab escapes spaces in
/// paths as `\040`.
fn parse_mounts(out: &str, prefix: &str) -> Vec<String> {
    let prefix = prefix.trim_end_matches('/');
    out.lines()
        .filter_map(|l| l.split_whitespace().nth(1))
        .map(|p| p.replace("\\040", " "))
        .filter(|p| {
            p == prefix
                || p.strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/'))
        })
        .collect()
}

fn route_dest(dest: Destination) -> Vec<String> {
    match dest {
        Destination::Default => vec!["default".to_owned()],
        Destination::Net(n) => vec!["-net".to_owned(), format!("{}/{}", n.network(), n.prefix)],
    }
}

fn parse_jls(out: &str) -> Result<Vec<Jail>, String> {
    let parsed: JlsOutput = serde_json::from_str(out).map_err(|e| e.to_string())?;
    parsed
        .info
        .jail
        .into_iter()
        .map(|j| {
            let num = |field: &str, v: &str| {
                v.parse::<u32>()
                    .map_err(|_| format!("{field} {v:?} of jail {:?} is not a number", j.name))
            };
            Ok(Jail {
                jid: num("jid", &j.jid)?,
                children_max: num("children.max", &j.children_max)?,
                children_cur: num("children.cur", &j.children_cur)?,
                parent: num("parent", &j.parent)?,
                name: j.name,
                vnet: j.vnet,
            })
        })
        .collect()
}

fn parse_dataset(line: &str) -> Option<Dataset> {
    let mut cols = line.split('\t');
    let name = cols.next()?.to_owned();
    let mountpoint = match cols.next()? {
        "-" | "none" | "legacy" => None,
        m => Some(m.to_owned()),
    };
    let quota = match cols.next()? {
        "0" | "-" | "none" => None,
        q => Some(q.parse().ok()?),
    };
    Some(Dataset {
        name,
        mountpoint,
        quota,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{Ipv4Net, Resource};

    /// Replies to each expected command line in order.
    struct Script {
        steps: Mutex<Vec<(String, Option<String>, CommandOutput)>>,
    }

    fn ok(stdout: &str) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    fn err(stderr: &str) -> CommandOutput {
        CommandOutput {
            success: false,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }

    impl Script {
        fn shell(steps: Vec<(&str, CommandOutput)>) -> Shell<Script> {
            Shell::with_runner(Script {
                steps: Mutex::new(
                    steps
                        .into_iter()
                        .rev()
                        .map(|(cmd, out)| (cmd.to_owned(), None, out))
                        .collect(),
                ),
            })
        }
    }

    impl Runner for Script {
        fn run(
            &self,
            program: &str,
            args: &[String],
            stdin: Option<&str>,
        ) -> std::io::Result<CommandOutput> {
            let got = describe(program, args);
            let (want, _, out) = self
                .steps
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("unexpected command {got:?}"));
            assert_eq!(got, want);
            if program == "pfctl" && args.contains(&"-f".to_owned()) {
                assert!(stdin.is_some(), "rules go on stdin");
            }
            Ok(out)
        }
    }

    impl Drop for Script {
        fn drop(&mut self) {
            let left = self.steps.lock().unwrap();
            if !std::thread::panicking() {
                assert!(left.is_empty(), "commands not run: {left:?}");
            }
        }
    }

    #[test]
    fn mounts() {
        let sh = Script::shell(vec![
            (
                "mount -t tmpfs -o mode=0700,size=1048576 tmpfs /var/run/legion/tasks/t1",
                ok(""),
            ),
            (
                "mount -t tmpfs -o mode=0777 tmpfs /var/run/legion/tasks/t1/volumes/cache",
                ok(""),
            ),
            (
                "mount -p",
                // Captured on FreeBSD 15.1, with the task mounts added.
                ok("/dev/ada0p2\t\t/\t\t\tufs\trw\t\t1 1\n\
                    devfs\t\t\t/dev\t\t\tdevfs\trw\t\t0 0\n\
                    tmpfs\t\t\t/var/run/legion/tasks/t1\ttmpfs\trw,size=1048576,mode=0700\t0 0\n\
                    tmpfs\t\t\t/var/run/legion/tasks-other\ttmpfs\trw\t0 0\n\
                    /var/run/legion/tasks/t1/web/s\t/var/run/legion/tasks/t1/web/x\\040y\tnullfs\tro\t0 0\n"),
            ),
            ("umount -f /var/run/legion/tasks/t1", ok("")),
        ]);
        sh.mount_tmpfs("/var/run/legion/tasks/t1", Some(1 << 20), 0o700)
            .unwrap();
        sh.mount_tmpfs("/var/run/legion/tasks/t1/volumes/cache", None, 0o777)
            .unwrap();
        assert_eq!(
            sh.mounts("/var/run/legion/tasks/").unwrap(),
            [
                "/var/run/legion/tasks/t1",
                "/var/run/legion/tasks/t1/web/x y"
            ]
        );
        sh.unmount("/var/run/legion/tasks/t1").unwrap();
    }

    #[test]
    fn jails_from_libxo() {
        let sh = Script::shell(vec![(
            "jls --libxo=json -n jid name vnet children.max children.cur parent",
            // Captured on FreeBSD 15.1 (with children.cur added).
            ok(
                r#"{"__version": "2", "jail-information": {"jail": [{"jid":"7","name":"lrctl","vnet":"inherit","children.max":"4","children.cur":"1","parent":"0"},{"jid":"8","name":"lrctl.c1","vnet":"new","children.max":"0","children.cur":"0","parent":"7"}]}}"#,
            ),
        )]);
        let jails = sh.jails().unwrap();
        assert_eq!(jails.len(), 2);
        assert_eq!(
            jails[1],
            Jail {
                jid: 8,
                name: "lrctl.c1".into(),
                vnet: "new".into(),
                children_max: 0,
                children_cur: 0,
                parent: 7,
            }
        );
    }

    #[test]
    fn pod_network() {
        let sh = Script::shell(vec![
            ("ifconfig bridge create", ok("bridge0\n")),
            ("ifconfig bridge0 name legion0", ok("legion0\n")),
            ("ifconfig legion0 inet 10.200.3.1/24 mtu 1420 up", ok("")),
            ("ifconfig epair create", ok("epair0a\n")),
            ("ifconfig legion0 addm epair0a", ok("")),
            ("ifconfig epair0b vnet pod1", ok("")),
            (
                "ifconfig -j pod1 epair0b inet 10.200.3.17/24 mtu 1420 up",
                ok(""),
            ),
            ("route -j pod1 -q add default 10.200.3.1", ok("")),
            ("route -q add -net 10.200.2.0/24 100.64.0.2", ok("")),
            ("ifconfig -j pod1 -l", ok("lo0 epair0b\n")),
        ]);
        sh.bridge_create("legion0").unwrap();
        sh.interface_configure(
            None,
            "legion0",
            &IfConfig {
                addr: Some("10.200.3.1/24".parse().unwrap()),
                mtu: Some(1420),
                up: true,
            },
        )
        .unwrap();
        let pair = sh.epair_create().unwrap();
        assert_eq!((pair.a.as_str(), pair.b.as_str()), ("epair0a", "epair0b"));
        sh.bridge_add_member("legion0", &pair.a).unwrap();
        sh.interface_move(&pair.b, "pod1").unwrap();
        sh.interface_configure(
            Some("pod1"),
            &pair.b,
            &IfConfig {
                addr: Some("10.200.3.17/24".parse().unwrap()),
                mtu: Some(1420),
                up: true,
            },
        )
        .unwrap();
        sh.route_add(
            Some("pod1"),
            Destination::Default,
            Ipv4Addr::new(10, 200, 3, 1),
        )
        .unwrap();
        sh.route_add(
            None,
            Destination::Net("10.200.2.9/24".parse::<Ipv4Net>().unwrap()),
            Ipv4Addr::new(100, 64, 0, 2),
        )
        .unwrap();
        assert_eq!(sh.interfaces(Some("pod1")).unwrap(), ["lo0", "epair0b"]);
    }

    #[test]
    fn bridge_rename_failure_cleans_up() {
        let sh = Script::shell(vec![
            ("ifconfig bridge create", ok("bridge3\n")),
            (
                "ifconfig bridge3 name legion0",
                err("ifconfig: ioctl SIOCSIFNAME (set name): File exists"),
            ),
            ("ifconfig bridge3 destroy", ok("")),
        ]);
        let e = sh.bridge_create("legion0").unwrap_err();
        assert!(e.to_string().contains("File exists"), "{e}");
    }

    #[test]
    fn rctl() {
        let sh = Script::shell(vec![
            ("rctl -a jail:i.c:vmemoryuse:deny=536870912", ok("")),
            (
                "rctl -r jail:gone",
                err("rctl: failed to remove rule 'jail:gone': No such process"),
            ),
            (
                "rctl -r jail:i.c",
                err("rctl: failed to remove rule 'jail:i.c': Operation not permitted"),
            ),
            (
                "rctl -u jail:i.c",
                ok("cputime=0\nmemoryuse=1048576\nmaxproc=2\npcpu=0\n"),
            ),
        ]);
        sh.rctl_add(&RctlRule {
            jail: "i.c".into(),
            resource: Resource::VMemoryUse,
            amount: 512 << 20,
        })
        .unwrap();
        sh.rctl_remove_jail("gone").unwrap();
        assert!(sh.rctl_remove_jail("i.c").is_err());
        let usage = sh.rctl_usage("i.c").unwrap();
        assert_eq!((usage["memoryuse"], usage["maxproc"]), (1 << 20, 2));
    }

    #[test]
    fn pf() {
        let sh = Script::shell(vec![
            ("pfctl -a legion/svc -f -", ok("")),
            (
                "pfctl -q -a legion/svc -t svc_web -T replace 10.200.3.17 10.200.3.18",
                ok(""),
            ),
            (
                "pfctl -a legion/svc -t svc_web -T show",
                ok("   10.200.3.17\n   10.200.3.18\n"),
            ),
            (
                "pfctl -a legion/svc -t nope -T show",
                err("pfctl: Table does not exist."),
            ),
            ("pfctl -q -a legion/svc -t svc_web -T replace", ok("")),
            ("pfctl -q -a legion/svc -F all", ok("")),
        ]);
        sh.pf_load_anchor("legion/svc", "pass in quick on legion0 from <svc_web>\n")
            .unwrap();
        let addrs = [Ipv4Addr::new(10, 200, 3, 17), Ipv4Addr::new(10, 200, 3, 18)];
        sh.pf_table_replace("legion/svc", "svc_web", &addrs)
            .unwrap();
        assert_eq!(sh.pf_table_show("legion/svc", "svc_web").unwrap(), addrs);
        assert!(sh.pf_table_show("legion/svc", "nope").unwrap().is_empty());
        sh.pf_table_replace("legion/svc", "svc_web", &[]).unwrap();
        sh.pf_flush_anchor("legion/svc").unwrap();
    }

    #[test]
    fn zfs() {
        let sh = Script::shell(vec![
            (
                "zfs create -p -o quota=10737418240 tank/legion/volumes/shop/data",
                ok(""),
            ),
            (
                "zfs list -H -p -r -t filesystem -o name,mountpoint,quota tank/legion",
                ok(
                    "tank/legion\t/var/db/legion/zfs\t0\ntank/legion/volumes/shop/data\t/var/db/legion/zfs/volumes/shop/data\t10737418240\ntank/legion/podman\tnone\t0\n",
                ),
            ),
            (
                "zfs list -H -p -r -t filesystem -o name,mountpoint,quota tank/nothing",
                err("cannot open 'tank/nothing': dataset does not exist"),
            ),
            ("zfs snapshot tank/legion/volumes/shop/data@t1", ok("")),
            ("zfs destroy -r tank/legion/volumes/shop/data", ok("")),
        ]);
        sh.zfs_create("tank/legion/volumes/shop/data", &[("quota", "10737418240")])
            .unwrap();
        let ds = sh.zfs_list("tank/legion").unwrap();
        assert_eq!(ds.len(), 3);
        assert_eq!(ds[0].quota, None);
        assert_eq!(ds[1].quota, Some(10 << 30));
        assert_eq!(ds[2].mountpoint, None);
        assert!(sh.zfs_list("tank/nothing").unwrap().is_empty());
        sh.zfs_snapshot("tank/legion/volumes/shop/data@t1").unwrap();
        sh.zfs_destroy("tank/legion/volumes/shop/data", true)
            .unwrap();
    }

    #[test]
    fn errors_carry_command_and_message() {
        let sh = Script::shell(vec![(
            "ifconfig epair9a destroy",
            err("ifconfig: interface epair9a does not exist\n"),
        )]);
        let e = sh.interface_destroy("epair9a").unwrap_err();
        assert_eq!(
            e.to_string(),
            "ifconfig epair9a destroy: ifconfig: interface epair9a does not exist"
        );
    }
}
