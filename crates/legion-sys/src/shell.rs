//! [`Sys`] by running the FreeBSD tools. Command forms and outputs were
//! checked on FreeBSD 15.1 (see the tests at the bottom for the captured
//! outputs).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::process::{Command, Stdio};

use serde::Deserialize;

use crate::{
    Carp, CarpState, Dataset, Destination, Epair, Error, IfConfig, IpNet, IscsiSession, Jail,
    JailMemory, PfState, RctlRule, Result, Sys, WgPeer,
};

/// Counts the memory of each jail's processes from the kernel's maps.
#[cfg(target_os = "freebsd")]
fn jail_memory(pids: BTreeMap<String, Vec<u32>>) -> Result<BTreeMap<String, JailMemory>> {
    use crate::memory::{count, freebsd};
    let read = |what: &str, e: std::io::Error| Error::Failed {
        command: format!("sysctl {what}"),
        message: e.to_string(),
    };
    if pids.values().all(Vec::is_empty) {
        return Ok(pids
            .into_keys()
            .map(|j| (j, JailMemory::default()))
            .collect());
    }
    let objects = freebsd::objects().map_err(|e| read("vm.swap_objects", e))?;
    let page = freebsd::page_size();
    pids.into_iter()
        .map(|(jail, pids)| {
            let mut processes = Vec::new();
            for pid in pids {
                match freebsd::mappings(pid) {
                    Ok(map) => processes.push((pid, map)),
                    // Exited since ps listed it.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(read(&format!("kern.proc.vmmap.{pid}"), e)),
                }
            }
            Ok((jail, count(&processes, &objects, page)))
        })
        .collect()
}

#[cfg(not(target_os = "freebsd"))]
fn jail_memory(pids: BTreeMap<String, Vec<u32>>) -> Result<BTreeMap<String, JailMemory>> {
    if pids.values().all(Vec::is_empty) {
        return Ok(pids
            .into_keys()
            .map(|j| (j, JailMemory::default()))
            .collect());
    }
    Err(Error::Failed {
        command: "sysctl kern.proc.vmmap".into(),
        message: "only on FreeBSD".into(),
    })
}

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

/// Runs a command whose input or output is a stream: `pump` moves it
/// while the command runs, then the command must succeed. A pump that
/// fails stops the command.
fn stream(
    program: &str,
    args: &[String],
    pump: impl FnOnce(&mut std::process::Child) -> std::io::Result<()>,
) -> Result<()> {
    use std::io::Read;
    let command = describe(program, args);
    tracing::debug!(%command, "run");
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| Error::Spawn {
            command: command.clone(),
            source,
        })?;
    // Read stderr meanwhile, so the command cannot block on it.
    let mut stderr = child.stderr.take().expect("piped stderr");
    let errors = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    let pumped = pump(&mut child);
    if pumped.is_err() {
        let _ = child.kill();
    }
    // The command sees the end of its input.
    drop(child.stdin.take());
    let status = child.wait().map_err(|source| Error::Spawn {
        command: command.clone(),
        source,
    })?;
    let stderr = errors.join().unwrap_or_default();
    if !status.success() {
        let message = stderr.trim();
        return Err(Error::Failed {
            command,
            message: if message.is_empty() {
                format!("failed ({status})")
            } else {
                message.to_owned()
            },
        });
    }
    pumped.map_err(|e| Error::Failed {
        command,
        message: format!("streaming: {e}"),
    })
}

/// Whether jls says the jail is gone: not there, or dying, which is a
/// jail already removed whose last references (sockets timing out, its
/// root's vnode) have yet to go.
fn gone(stderr: &str) -> bool {
    stderr.contains("not found") || stderr.contains("is dying")
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

/// The vhid of `addr` in `ifconfig <interface>` output, if the interface
/// has the address as a CARP address.
fn carp_addr(shown: &str, addr: Ipv4Addr) -> Option<u8> {
    let addr = addr.to_string();
    shown.lines().find_map(|line| {
        let words: Vec<&str> = line.split_whitespace().collect();
        if words.first() != Some(&"inet") || words.get(1) != Some(&addr.as_str()) {
            return None;
        }
        let at = words.iter().position(|w| *w == "vhid")?;
        words.get(at + 1)?.parse().ok()
    })
}

/// The state in a `carp: MASTER vhid 7 advbase 1 advskew 0` line.
fn parse_carp_state(shown: &str, vhid: u8) -> Option<CarpState> {
    shown.lines().find_map(|line| {
        let words: Vec<&str> = line.split_whitespace().collect();
        if words.first() != Some(&"carp:") || words.get(3) != Some(&vhid.to_string().as_str()) {
            return None;
        }
        match *words.get(1)? {
            "MASTER" => Some(CarpState::Master),
            "BACKUP" => Some(CarpState::Backup),
            _ => Some(CarpState::Init),
        }
    })
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
        // `name=<parent>.<child>` would ask to move the jail under that
        // parent; a child jail is named by its jid instead.
        let target = if jail.contains('.') {
            let jid = self.run("jls", &args(["-j", jail, "jid"]))?;
            format!("jid={}", jid.trim())
        } else {
            format!("name={jail}")
        };
        let mut a = vec!["-m".to_owned(), target];
        a.extend(params.iter().map(|(k, v)| format!("{k}={v}")));
        self.run("jail", &a).map(drop)
    }

    fn jail_create(&self, params: &[(&str, &str)]) -> Result<u32> {
        let mut a = args(["-i", "-c"]);
        a.extend(params.iter().map(|(k, v)| {
            if v.is_empty() {
                (*k).to_owned()
            } else {
                format!("{k}={v}")
            }
        }));
        let out = self.run("jail", &a)?;
        out.trim().parse().map_err(|_| Error::Parse {
            command: describe("jail", &a),
            message: format!("not a jid: {out:?}"),
        })
    }

    fn jail_cpuset(&self, jid: u32, cpus: &str) -> Result<()> {
        self.run("cpuset", &args(["-l", cpus, "-j", &jid.to_string()]))
            .map(drop)
    }

    fn jail_remove(&self, jail: &str) -> Result<()> {
        let a = args(["-j", jail, "jid"]);
        let out = self.output("jls", &a, None)?;
        if !out.success {
            if gone(&out.stderr) {
                return Err(Error::NotFound(format!("jail {jail}")));
            }
            return Err(failed("jls", &a, &out));
        }
        self.run("jail", &args(["-r", out.stdout.trim()])).map(drop)
    }

    fn jail_get(&self, jail: &str, params: &[&str]) -> Result<BTreeMap<String, String>> {
        let mut a = args(["-j", jail, "--libxo=json", "-n"]);
        a.extend(params.iter().map(|p| (*p).to_owned()));
        let out = self.output("jls", &a, None)?;
        if !out.success {
            if gone(&out.stderr) {
                return Err(Error::NotFound(format!("jail {jail}")));
            }
            return Err(failed("jls", &a, &out));
        }
        parse_jail_params(&out.stdout).map_err(|message| Error::Parse {
            command: describe("jls", &a),
            message,
        })
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

    fn interface_ether(&self, name: &str) -> Result<String> {
        let a = args([name]);
        self.run("ifconfig", &a)?
            .lines()
            .find_map(|l| l.trim().strip_prefix("ether "))
            .map(|e| e.trim().to_owned())
            .ok_or_else(|| Error::Parse {
                command: describe("ifconfig", &a),
                message: format!("{name} has no Ethernet address"),
            })
    }

    fn interface_set_ether(&self, name: &str, ether: &str) -> Result<()> {
        self.run("ifconfig", &args([name, "ether", ether]))
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
        self.run("ifconfig", &a)?;
        if let Some(addr) = config.addr6 {
            // devd may have disabled IPv6 on the interface as it appeared
            // (rc.conf's `ipv6_activate_all_interfaces`); and an address
            // waiting for duplicate address detection cannot be bound.
            let mut flags = in_jail(jail);
            flags.extend(args([name, "inet6", "-ifdisabled", "no_dad"]));
            self.run("ifconfig", &flags)?;
            let mut a = in_jail(jail);
            a.extend(args([name, "inet6", &addr.to_string()]));
            self.run("ifconfig", &a)?;
        }
        Ok(())
    }

    fn carp_set(&self, interface: &str, carp: &Carp, state: Option<CarpState>) -> Result<()> {
        let shown = self.run("ifconfig", &args([interface]))?;
        let mut a = args([interface, "vhid"]);
        a.extend([
            carp.vhid.to_string(),
            "advskew".into(),
            carp.advskew.to_string(),
        ]);
        if carp_addr(&shown, carp.addr).is_none() {
            // In a jail even `kldload -n` of a loaded module is refused.
            if self.run("kldstat", &args(["-q", "-m", "carp"])).is_err() {
                self.kld_load("carp")?;
            }
            a.extend(["pass".into(), carp.password.clone(), "alias".into()]);
            a.push(format!("{}/32", carp.addr));
        }
        if let Some(state) = state.filter(|s| *s != CarpState::Init) {
            a.extend(["state".into(), state.as_str().into()]);
        }
        self.run("ifconfig", &a).map(drop)
    }

    fn carp_state(&self, interface: &str, vhid: u8) -> Result<Option<CarpState>> {
        let shown = self.run("ifconfig", &args([interface]))?;
        Ok(parse_carp_state(&shown, vhid))
    }

    fn carp_remove(&self, interface: &str, addr: Ipv4Addr) -> Result<()> {
        let shown = self.run("ifconfig", &args([interface]))?;
        if carp_addr(&shown, addr).is_none() {
            return Ok(());
        }
        let a = args([interface, "inet", &addr.to_string(), "-alias"]);
        self.run("ifconfig", &a).map(drop)
    }

    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: IpAddr) -> Result<()> {
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

    fn route_to_interface(
        &self,
        jail: Option<&str>,
        dest: Destination,
        interface: &str,
    ) -> Result<()> {
        let route = |verb: &str| {
            let mut a = in_jail(jail);
            a.extend(["-q".to_owned(), verb.to_owned()]);
            a.extend(route_dest(dest));
            a.extend(["-interface".to_owned(), interface.to_owned()]);
            a
        };
        let a = route("add");
        let out = self.output("route", &a, None)?;
        if out.success {
            return Ok(());
        }
        if !out.stderr.contains("File exists") {
            return Err(failed("route", &a, &out));
        }
        self.run("route", &route("change")).map(drop)
    }

    fn wg_genkey(&self) -> Result<String> {
        Ok(self.run("wg", &args(["genkey"]))?.trim().to_owned())
    }

    fn wg_pubkey(&self, private_key: &str) -> Result<String> {
        let out = self.run_input("wg", &args(["pubkey"]), Some(&format!("{private_key}\n")))?;
        Ok(out.trim().to_owned())
    }

    fn wg_create(&self, name: &str) -> Result<()> {
        self.run("ifconfig", &args(["wg", "create", "name", name]))
            .map(drop)
    }

    fn wg_configure(&self, name: &str, private_key: &str, port: u16) -> Result<()> {
        // The key goes on stdin, never on a command line.
        let port = port.to_string();
        let a = args([
            "set",
            name,
            "private-key",
            "/dev/stdin",
            "listen-port",
            &port,
        ]);
        self.run_input("wg", &a, Some(&format!("{private_key}\n")))
            .map(drop)
    }

    fn wg_peers(&self, name: &str) -> Result<Vec<WgPeer>> {
        let a = args(["show", name, "dump"]);
        let out = self.run("wg", &a)?;
        parse_wg_dump(&out).map_err(|message| Error::Parse {
            command: describe("wg", &a),
            message,
        })
    }

    fn wg_set_peer(&self, name: &str, peer: &WgPeer) -> Result<()> {
        let mut a = args(["set", name, "peer", &peer.public_key]);
        if let Some(endpoint) = &peer.endpoint {
            a.extend(["endpoint".to_owned(), endpoint.clone()]);
        }
        let allowed: Vec<String> = peer.allowed_ips.iter().map(|n| n.to_string()).collect();
        a.extend(["allowed-ips".to_owned(), allowed.join(",")]);
        a.extend([
            "persistent-keepalive".to_owned(),
            peer.keepalive.map_or("off".to_owned(), |k| k.to_string()),
        ]);
        self.run("wg", &a).map(drop)
    }

    fn wg_remove_peer(&self, name: &str, public_key: &str) -> Result<()> {
        self.run("wg", &args(["set", name, "peer", public_key, "remove"]))
            .map(drop)
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

    fn jail_memory(&self, jails: &[String]) -> Result<BTreeMap<String, JailMemory>> {
        let mut pids: BTreeMap<String, Vec<u32>> =
            jails.iter().map(|j| (j.clone(), Vec::new())).collect();
        if jails.is_empty() {
            return Ok(BTreeMap::new());
        }
        let a = args(["-ax", "-o", "pid=", "-o", "jail="]);
        for line in self.run("ps", &a)?.lines() {
            let mut fields = line.split_whitespace();
            let (Some(pid), Some(jail)) = (fields.next(), fields.next()) else {
                continue;
            };
            if let (Some(list), Ok(pid)) = (pids.get_mut(jail), pid.parse()) {
                list.push(pid);
            }
        }
        jail_memory(pids)
    }

    fn kill(&self, pid: u32) -> Result<()> {
        let a = vec!["-KILL".to_owned(), pid.to_string()];
        let out = self.output("kill", &a, None)?;
        if out.success || out.stderr.contains("No such process") {
            Ok(())
        } else {
            Err(failed("kill", &a, &out))
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

    fn default_interface(&self) -> Result<Option<String>> {
        let a = args(["-n", "get", "default"]);
        let out = self.output("route", &a, None)?;
        if !out.success {
            if out.stderr.contains("not in table") {
                return Ok(None);
            }
            return Err(failed("route", &a, &out));
        }
        Ok(out
            .stdout
            .lines()
            .find_map(|l| l.trim().strip_prefix("interface:"))
            .map(|i| i.trim().to_owned()))
    }

    fn sysctl_set(&self, name: &str, value: &str) -> Result<()> {
        self.run("sysctl", &[format!("{name}={value}")]).map(drop)
    }

    fn sysctl_get(&self, name: &str) -> Result<String> {
        self.run("sysctl", &["-n".to_owned(), name.to_owned()])
            .map(|v| v.trim().to_owned())
    }

    fn pf_main_rules(&self) -> Result<String> {
        let nat = self.run("pfctl", &args(["-sn"]))?;
        let filter = self.run("pfctl", &args(["-sr"]))?;
        Ok(nat + &filter)
    }

    fn pf_states(&self) -> Result<Vec<PfState>> {
        let out = self.run("pfctl", &args(["-ss", "-vv"]))?;
        Ok(parse_states(&out))
    }

    fn pf_kill_state(&self, id: &str) -> Result<()> {
        self.run("pfctl", &args(["-q", "-k", "id", "-k", id]))
            .map(drop)
    }

    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()> {
        self.run_input("pfctl", &args(["-a", anchor, "-f", "-"]), Some(rules))
            .map(drop)
    }

    fn pf_flush_anchor(&self, anchor: &str) -> Result<()> {
        // Each part on its own: `-F all` on an anchor fails on FreeBSD 15
        // ("Operation not supported by device") after flushing it. An
        // anchor that is not there (or no longer, emptied) has nothing
        // left to flush: pfctl says so, or, for tables, that same
        // "Operation not supported by device".
        for what in ["Tables", "nat", "rules"] {
            let a = args(["-q", "-a", anchor, "-F", what]);
            let out = self.output("pfctl", &a, None)?;
            let absent = out.stderr.contains("Anchor does not exist")
                || (what == "Tables" && out.stderr.contains("Operation not supported by device"));
            if !out.success && !absent {
                return Err(failed("pfctl", &a, &out));
            }
        }
        Ok(())
    }

    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[IpAddr]) -> Result<()> {
        let mut a = args(["-q", "-a", anchor, "-t", table, "-T", "replace"]);
        a.extend(addrs.iter().map(IpAddr::to_string));
        self.run("pfctl", &a).map(drop)
    }

    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<IpAddr>> {
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

    fn zfs_snapshots(&self, dataset: &str) -> Result<Vec<String>> {
        let a = args([
            "list",
            "-H",
            "-t",
            "snapshot",
            "-o",
            "name",
            "-s",
            "createtxg",
            "-d",
            "1",
            dataset,
        ]);
        let out = self.output("zfs", &a, None)?;
        if !out.success {
            if out.stderr.contains("dataset does not exist") {
                return Ok(Vec::new());
            }
            return Err(failed("zfs", &a, &out));
        }
        Ok(out
            .stdout
            .lines()
            .filter_map(|l| l.split_once('@'))
            .map(|(_, name)| name.to_owned())
            .collect())
    }

    fn zfs_rollback(&self, snapshot: &str) -> Result<()> {
        self.run("zfs", &args(["rollback", "-r", snapshot]))
            .map(drop)
    }

    fn zfs_clone(&self, snapshot: &str, dataset: &str, props: &[(&str, &str)]) -> Result<()> {
        let mut a = args(["clone"]);
        for (k, v) in props {
            a.extend(["-o".to_owned(), format!("{k}={v}")]);
        }
        a.extend([snapshot.to_owned(), dataset.to_owned()]);
        self.run("zfs", &a).map(drop)
    }

    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()> {
        let a = if recursive {
            args(["destroy", "-r", name])
        } else {
            args(["destroy", name])
        };
        self.run("zfs", &a).map(drop)
    }

    fn zfs_send(
        &self,
        snapshot: &str,
        since: Option<&str>,
        out: &mut dyn std::io::Write,
    ) -> Result<()> {
        let mut a = args(["send"]);
        if let Some(since) = since {
            a.extend(["-i".to_owned(), format!("@{since}")]);
        }
        a.push(snapshot.to_owned());
        stream("zfs", &a, |child| {
            let mut from = child.stdout.take().expect("piped stdout");
            std::io::copy(&mut from, out).map(drop)
        })
    }

    fn zfs_receive(&self, dataset: &str, input: &mut dyn std::io::Read) -> Result<()> {
        stream("zfs", &args(["receive", "-F", "-u", dataset]), |child| {
            let mut to = child.stdin.take().expect("piped stdin");
            std::io::copy(input, &mut to).map(drop)
        })
    }

    fn zfs_mount(&self, dataset: &str) -> Result<()> {
        let mounted = self.run(
            "zfs",
            &args(["get", "-H", "-o", "value", "mounted", dataset]),
        )?;
        if mounted.trim() == "yes" {
            return Ok(());
        }
        self.run("zfs", &args(["mount", dataset])).map(drop)
    }

    fn mount_tmpfs(&self, path: &str, size: Option<u64>, mode: u32) -> Result<()> {
        let mut opts = format!("mode={mode:04o}");
        if let Some(size) = size {
            opts += &format!(",size={size}");
        }
        self.run("mount", &args(["-t", "tmpfs", "-o", &opts, "tmpfs", path]))
            .map(drop)
    }

    fn mount_nullfs(&self, source: &str, target: &str, read_only: bool) -> Result<()> {
        let opts = if read_only { "ro" } else { "rw" };
        self.run("mount", &args(["-t", "nullfs", "-o", opts, source, target]))
            .map(drop)
    }

    fn mount_devfs(&self, target: &str, ruleset: u32) -> Result<()> {
        self.run("mount", &args(["-t", "devfs", "devfs", target]))?;
        let set = ruleset.to_string();
        let applied = self
            .run("devfs", &args(["-m", target, "ruleset", &set]))
            .and_then(|_| self.run("devfs", &args(["-m", target, "rule", "applyset"])));
        if let Err(e) = applied {
            // Not left showing every device.
            let _ = self.run("umount", &args(["-f", target]));
            return Err(e);
        }
        Ok(())
    }

    fn devfs_link(&self, dev: &str, name: &str, target: &str) -> Result<()> {
        let link = format!("{}/{name}", dev.trim_end_matches('/'));
        self.run("ln", &args(["-s", target, &link]))?;
        self.run(
            "devfs",
            &args(["-m", dev, "rule", "apply", "path", name, "unhide"]),
        )
        .map(drop)
    }

    fn mount_pseudo(&self, fstype: &str, target: &str, options: &str) -> Result<()> {
        let mut a = vec!["-t", fstype];
        if !options.is_empty() {
            a.extend(["-o", options]);
        }
        a.extend([fstype, target]);
        let a: Vec<String> = a.iter().map(|s| (*s).to_owned()).collect();
        self.run("mount", &a).map(drop)
    }

    fn mount_fs(&self, fstype: &str, source: &str, target: &str, options: &str) -> Result<()> {
        let mut a = args(["-t", fstype]);
        if !options.is_empty() {
            a.extend(args(["-o", options]));
        }
        a.extend(args([source, target]));
        self.run("mount", &a).map(drop)
    }

    fn fs_type(&self, device: &str) -> Result<Option<String>> {
        let a = args([device]);
        let out = self.output("fstyp", &a, None)?;
        if out.success {
            return Ok(Some(out.stdout.trim().to_owned()));
        }
        if out.stderr.contains("filesystem not recognized") {
            return Ok(None);
        }
        Err(failed("fstyp", &a, &out))
    }

    fn fsck_preen(&self, device: &str) -> Result<()> {
        self.run("fsck", &args(["-t", "ufs", "-p", device]))
            .map(drop)
    }

    fn iscsi_sessions(&self) -> Result<Vec<IscsiSession>> {
        let a = args(["--libxo", "json", "-L", "-v"]);
        let out = self.run("iscsictl", &a)?;
        parse_iscsi(&out).map_err(|message| Error::Parse {
            command: describe("iscsictl", &a),
            message,
        })
    }

    fn iscsi_attach(&self, portal: &str, target: &str) -> Result<()> {
        self.run("iscsictl", &args(["-A", "-p", portal, "-t", target]))
            .map(drop)
    }

    fn iscsi_detach(&self, target: &str) -> Result<()> {
        if !self.iscsi_sessions()?.iter().any(|s| s.target == target) {
            return Ok(());
        }
        self.run("iscsictl", &args(["-R", "-t", target])).map(drop)
    }

    fn run_program(&self, program: &str, args: &[&str], input: &str) -> Result<String> {
        let a: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        match self.output(program, &a, Some(input)) {
            Err(Error::Spawn { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(program.to_owned()))
            }
            Err(e) => Err(e),
            Ok(out) if out.success => Ok(out.stdout),
            Ok(out) => Err(failed(program, &a, &out)),
        }
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

/// States from `pfctl -ss -vv`: a line per state, then indented detail
/// lines, one of them `id: <id> creatorid: <creator>`. Addresses carry
/// their ports as `10.200.9.3:8080` and `fd00:10:200:9::3[8080]`.
fn parse_states(out: &str) -> Vec<PfState> {
    let mut states = Vec::new();
    let mut addrs: Option<Vec<IpAddr>> = None;
    for line in out.lines() {
        if !line.starts_with(' ') {
            addrs = Some(
                line.split_whitespace()
                    .filter_map(|w| {
                        let w = w.trim_start_matches('(').trim_end_matches(')');
                        let w = w.split_once('[').map_or(w, |(a, _)| a);
                        w.parse::<IpAddr>().ok().or_else(|| {
                            let (a, _) = w.rsplit_once(':')?;
                            a.parse::<Ipv4Addr>().ok().map(IpAddr::V4)
                        })
                    })
                    .collect(),
            );
            continue;
        }
        let mut words = line.split_whitespace();
        if words.next() == Some("id:")
            && let (Some(id), Some("creatorid:"), Some(creator)) =
                (words.next(), words.next(), words.next())
            && let Some(addrs) = addrs.take()
        {
            states.push(PfState {
                id: format!("{id}/{creator}"),
                addrs,
            });
        }
    }
    states
}

/// Peers from `wg show <name> dump`: a line for the interface (private key,
/// public key, port, fwmark), then one per peer, tab-separated: public key,
/// preshared key, endpoint, allowed IPs, latest handshake, bytes received
/// and sent, keepalive.
fn parse_wg_dump(out: &str) -> Result<Vec<WgPeer>, String> {
    out.lines()
        .skip(1)
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.len() < 8 {
                return Err(format!("bad peer line {line:?}"));
            }
            let mut allowed_ips: Vec<IpNet> =
                cols[3].split(',').filter_map(|n| n.parse().ok()).collect();
            allowed_ips.sort();
            Ok(WgPeer {
                public_key: cols[0].to_owned(),
                endpoint: (cols[2] != "(none)").then(|| cols[2].to_owned()),
                allowed_ips,
                keepalive: cols[7].parse().ok(),
            })
        })
        .collect()
}

/// Sessions from `iscsictl --libxo json -L -v`.
fn parse_iscsi(out: &str) -> Result<Vec<IscsiSession>, String> {
    #[derive(Deserialize)]
    struct Out {
        iscsictl: List,
    }
    #[derive(Deserialize)]
    struct List {
        #[serde(default)]
        session: Vec<Session>,
    }
    #[derive(Deserialize)]
    struct Session {
        target: Target,
        state: String,
        #[serde(default, rename = "failureReason")]
        failure: String,
        #[serde(default)]
        devices: Option<Devices>,
    }
    #[derive(Deserialize)]
    struct Target {
        name: String,
        #[serde(default)]
        portal: String,
    }
    #[derive(Deserialize)]
    struct Devices {
        #[serde(default)]
        lun: Vec<Lun>,
    }
    #[derive(Deserialize)]
    struct Lun {
        lun: u32,
        device: String,
    }
    let parsed: Out = serde_json::from_str(out).map_err(|e| e.to_string())?;
    Ok(parsed
        .iscsictl
        .session
        .into_iter()
        .map(|s| IscsiSession {
            target: s.target.name,
            portal: s.target.portal,
            state: s.state,
            failure: s.failure,
            luns: s
                .devices
                .map(|d| d.lun.into_iter().map(|l| (l.lun, l.device)).collect())
                .unwrap_or_default(),
        })
        .collect())
}

fn route_dest(dest: Destination) -> Vec<String> {
    match dest {
        Destination::Default => vec!["default".to_owned()],
        Destination::Net(n) => vec!["-net".to_owned(), format!("{}/{}", n.network(), n.prefix)],
        Destination::Default6 => args(["-inet6", "default"]),
        Destination::Net6(n) => args(["-inet6", "-net", &format!("{}/{}", n.network(), n.prefix)]),
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

/// One jail's parameters from `jls --libxo=json -n`, which writes booleans
/// as JSON booleans and everything else as strings.
fn parse_jail_params(out: &str) -> Result<BTreeMap<String, String>, String> {
    let parsed: serde_json::Value = serde_json::from_str(out).map_err(|e| e.to_string())?;
    let jail = parsed
        .pointer("/jail-information/jail/0")
        .and_then(serde_json::Value::as_object)
        .ok_or("no jail")?;
    jail.iter()
        .map(|(k, v)| {
            let v = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Number(n) => n.to_string(),
                other => return Err(format!("{k}: unexpected {other}")),
            };
            Ok((k.clone(), v))
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
    use crate::{Ipv4Net, Ipv6Net, Resource};

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

    /// `ifconfig epair0a` on FreeBSD 15.1 with a CARP address.
    const CARP_SHOWN: &str = "epair0a: flags=1008943<UP,BROADCAST,RUNNING,PROMISC,SIMPLEX,MULTICAST,LOWER_UP> metric 0 mtu 1500
	ether c2:c4:69:1c:c3:ed
	inet 10.77.0.1 netmask 0xffffff00 broadcast 10.77.0.255
	inet 10.77.0.100 netmask 0xffffffff broadcast 10.77.0.100 vhid 7
	groups: epair
	carp: BACKUP vhid 7 advbase 1 advskew 100
	      peer 224.0.0.18 peer6 ff02::12
	status: active
";

    #[test]
    fn ether() {
        let sh = Script::shell(vec![
            ("ifconfig epair0a", ok(CARP_SHOWN)),
            (
                "ifconfig lo0",
                ok("lo0: flags=8049<UP,LOOPBACK> mtu 16384\n"),
            ),
            ("ifconfig lp0123 ether c2:c4:69:1c:c3:ed", ok("")),
        ]);
        assert_eq!(sh.interface_ether("epair0a").unwrap(), "c2:c4:69:1c:c3:ed");
        assert!(matches!(
            sh.interface_ether("lo0"),
            Err(Error::Parse { .. })
        ));
        sh.interface_set_ether("lp0123", "c2:c4:69:1c:c3:ed")
            .unwrap();
    }

    #[test]
    fn carp() {
        let plain = "epair0a: flags=8843<UP> mtu 1500
	inet 10.77.0.1 netmask 0xffffff00 broadcast 10.77.0.255
";
        let master = CARP_SHOWN.replace(
            "BACKUP vhid 7 advbase 1 advskew 100",
            "MASTER vhid 7 advbase 1 advskew 0",
        );
        let sh = Script::shell(vec![
            ("ifconfig epair0a", ok(plain)),
            ("kldstat -q -m carp", err("")),
            ("kldload -n carp", ok("")),
            (
                "ifconfig epair0a vhid 7 advskew 100 pass s3cret alias 10.77.0.100/32",
                ok(""),
            ),
            ("ifconfig epair0a", ok(CARP_SHOWN)),
            ("ifconfig epair0a vhid 7 advskew 0 state master", ok("")),
            ("ifconfig epair0a", ok(&master)),
            ("ifconfig epair0a", ok(CARP_SHOWN)),
            ("ifconfig epair0a", ok(CARP_SHOWN)),
            ("ifconfig epair0a inet 10.77.0.100 -alias", ok("")),
            ("ifconfig epair0a", ok(plain)),
        ]);
        let mut carp = Carp {
            vhid: 7,
            addr: Ipv4Addr::new(10, 77, 0, 100),
            password: "s3cret".into(),
            advskew: 100,
        };
        sh.carp_set("epair0a", &carp, None).unwrap();
        carp.advskew = 0;
        sh.carp_set("epair0a", &carp, Some(CarpState::Master))
            .unwrap();
        assert_eq!(
            sh.carp_state("epair0a", 7).unwrap(),
            Some(CarpState::Master)
        );
        assert_eq!(sh.carp_state("epair0a", 8).unwrap(), None);
        sh.carp_remove("epair0a", carp.addr).unwrap();
        sh.carp_remove("epair0a", carp.addr).unwrap();
    }

    #[test]
    fn storage() {
        // Captured on FreeBSD 15.1: a session with two LUNs, connected, and
        // one that failed.
        let sessions = r#"{"__version": "1", "iscsictl": {"session": [{"sessionId":2, "initiator": {"name":"iqn.1994-09.org.freebsd:freebsd","portal":"","alias":""}, "target": {"name":"iqn.2026-10.lan.legion:spike","portal":"127.0.0.1","alias":""}, "auth": {"user":"","secret":"","mutualUser":"","mutualSecret":""},"type":"Normal","enable":"Yes","state":"Connected","failureReason":"","headerDigest":"None","dataDigest":"None","recvDataSegmentLen":262144,"sendDataSegmentLen":262144,"maxBurstLen":1048576,"firstBurstLen":1048576,"immediateData":"Yes","iSER":"No","offloadDriver":"None", "devices": {"lun": [{"lun":0,"device":"da0"}, {"lun":1,"device":"da1"}],"scbus":2,"bus":0,"target":0}}, {"sessionId":1, "initiator": {"name":"iqn.1994-09.org.freebsd:freebsd","portal":"","alias":""}, "target": {"name":"iqn.2026-10.lan.legion:nope","portal":"127.0.0.1","alias":""}, "auth": {"user":"","secret":"","mutualUser":"","mutualSecret":""},"type":"Normal","enable":"Yes","state":"Disconnected","failureReason":"Not found","headerDigest":"None","dataDigest":"None","recvDataSegmentLen":8192,"sendDataSegmentLen":8192,"maxBurstLen":262144,"firstBurstLen":65536,"immediateData":"No","iSER":"No","offloadDriver":"None", "devices": {"lun": [],"scbus":-1,"bus":0,"target":-1}}]}}"#;
        let spike = "iqn.2026-10.lan.legion:spike";
        let sh = Script::shell(vec![
            ("iscsictl --libxo json -L -v", ok(sessions)),
            (&format!("iscsictl -A -p 127.0.0.1:3260 -t {spike}"), ok("")),
            ("iscsictl --libxo json -L -v", ok(sessions)),
            (&format!("iscsictl -R -t {spike}"), ok("")),
            (
                "iscsictl --libxo json -L -v",
                ok(r#"{"__version": "1", "iscsictl": {}}"#),
            ),
            ("fstyp /dev/da0", ok("ufs\n")),
            (
                "fsck -t ufs -p /dev/da0",
                ok("/dev/da0: 2 files, 2 used, 30000 free\n"),
            ),
            (
                "fstyp /dev/da1",
                err("fstyp: /dev/da1: filesystem not recognized"),
            ),
            ("mount -t ufs /dev/da0 /var/run/legion/volumes/x", ok("")),
            (
                "mount -t nfs -o nfsv4,soft files:/export /var/run/legion/tasks/t/volumes/v",
                ok(""),
            ),
        ]);
        let s = sh.iscsi_sessions().unwrap();
        assert_eq!(s.len(), 2);
        assert!(s[0].connected() && !s[1].connected());
        assert_eq!(
            (s[0].target.as_str(), s[0].portal.as_str()),
            (spike, "127.0.0.1")
        );
        assert_eq!(
            s[0].luns,
            BTreeMap::from([(0, "da0".into()), (1, "da1".into())])
        );
        assert_eq!((s[1].failure.as_str(), s[1].luns.len()), ("Not found", 0));
        assert_eq!(s[0].device(1), Some("da1"));
        let probing = IscsiSession {
            luns: BTreeMap::from([(0, "probe0".into())]),
            ..s[0].clone()
        };
        assert_eq!(probing.device(0), None);
        sh.iscsi_attach("127.0.0.1:3260", spike).unwrap();
        sh.iscsi_detach(spike).unwrap();
        sh.iscsi_detach(spike).unwrap(); // no session: nothing to do
        assert_eq!(sh.fs_type("/dev/da0").unwrap().as_deref(), Some("ufs"));
        sh.fsck_preen("/dev/da0").unwrap();
        assert_eq!(sh.fs_type("/dev/da1").unwrap(), None);
        sh.mount_fs("ufs", "/dev/da0", "/var/run/legion/volumes/x", "")
            .unwrap();
        sh.mount_fs(
            "nfs",
            "files:/export",
            "/var/run/legion/tasks/t/volumes/v",
            "nfsv4,soft",
        )
        .unwrap();
    }

    #[test]
    fn states_and_routes() {
        let sh = Script::shell(vec![
            (
                "pfctl -ss -vv",
                // Captured on FreeBSD 15.1: a connection to a VIP, through
                // the hairpin NAT.
                ok(
                    "all tcp 10.200.9.3:8080 (10.201.0.1:80) <- 10.200.9.2:10226       ESTABLISHED:ESTABLISHED\n   \
                    [311516909 + 65792] wscale 8  [2002298317 + 65792] wscale 8\n   \
                    age 00:02:36, expires in 23:57:25, 3:2 pkts, 170:112 bytes, anchor 0\n   \
                    id: 52f5df6a00000000 creatorid: b38009b1\n   \
                    origif: lb0\n\
                    all tcp 10.200.9.1:63804 (10.200.9.2:10226) -> 10.200.9.3:8080       ESTABLISHED:ESTABLISHED\n   \
                    [2002298317 + 65792] wscale 8  [311516909 + 65792] wscale 8\n   \
                    id: 53f5df6a00000000 creatorid: b38009b1\n\
                    all tcp fd00:10:200:9::3[8080] (fd00:10:201::1[80]) <- fd00:10:200:9::2[10226]       ESTABLISHED:ESTABLISHED\n   \
                    id: 54f5df6a00000000 creatorid: b38009b1\n",
                ),
            ),
            ("pfctl -q -k id -k 52f5df6a00000000/b38009b1", ok("")),
            (
                "route -n get default",
                ok(
                    "   route to: 0.0.0.0\ndestination: 0.0.0.0\n    gateway: 192.168.100.1\n  interface: wlan0\n",
                ),
            ),
            (
                "route -n get default",
                err("route: route has not been found\nnot in table\n"),
            ),
            (
                "sysctl net.inet.ip.forwarding=1",
                ok("net.inet.ip.forwarding: 0 -> 1\n"),
            ),
            ("sysctl -n kern.racct.enable", ok("1\n")),
        ]);
        let states = sh.pf_states().unwrap();
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        assert_eq!(
            states,
            [
                PfState {
                    id: "52f5df6a00000000/b38009b1".into(),
                    addrs: vec![ip("10.200.9.3"), ip("10.201.0.1"), ip("10.200.9.2")],
                },
                PfState {
                    id: "53f5df6a00000000/b38009b1".into(),
                    addrs: vec![ip("10.200.9.1"), ip("10.200.9.2"), ip("10.200.9.3")],
                },
                PfState {
                    id: "54f5df6a00000000/b38009b1".into(),
                    addrs: vec![
                        ip("fd00:10:200:9::3"),
                        ip("fd00:10:201::1"),
                        ip("fd00:10:200:9::2")
                    ],
                },
            ]
        );
        sh.pf_kill_state(&states[0].id).unwrap();
        assert_eq!(sh.default_interface().unwrap().as_deref(), Some("wlan0"));
        assert_eq!(sh.default_interface().unwrap(), None);
        sh.sysctl_set("net.inet.ip.forwarding", "1").unwrap();
        assert_eq!(sh.sysctl_get("kern.racct.enable").unwrap(), "1");
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
        let sh = Script::shell(vec![
            (
                "jls -j lgq.c --libxo=json -n allow.raw_sockets enforce_statfs securelevel",
                // Captured on FreeBSD 15.1.
                ok(
                    r#"{"__version": "2", "jail-information": {"jail": [{"allow.raw_sockets":false,"enforce_statfs":"2","securelevel":"2"}]}}"#,
                ),
            ),
            (
                "jls -j gone --libxo=json -n securelevel",
                err("jls: jail \"gone\" not found\n"),
            ),
            ("jls -j lgq.c jid", ok("12\n")),
            ("jail -m jid=12 securelevel=2", ok("")),
            ("jail -m name=lgq children.max=2", ok("")),
        ]);
        let params = sh
            .jail_get(
                "lgq.c",
                &["allow.raw_sockets", "enforce_statfs", "securelevel"],
            )
            .unwrap();
        assert_eq!(
            params.into_iter().collect::<Vec<_>>(),
            [
                ("allow.raw_sockets".to_owned(), "false".to_owned()),
                ("enforce_statfs".to_owned(), "2".to_owned()),
                ("securelevel".to_owned(), "2".to_owned()),
            ]
        );
        assert!(matches!(
            sh.jail_get("gone", &["securelevel"]),
            Err(Error::NotFound(_))
        ));
        sh.jail_set("lgq.c", &[("securelevel", "2")]).unwrap();
        sh.jail_set("lgq", &[("children.max", "2")]).unwrap();
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
            ("ifconfig -j pod1 epair0b inet6 -ifdisabled no_dad", ok("")),
            (
                "ifconfig -j pod1 epair0b inet6 fd00:10:200:3::11/64",
                ok(""),
            ),
            ("route -j pod1 -q add default 10.200.3.1", ok("")),
            (
                "route -j pod1 -q add -inet6 default fd00:10:200:3::1",
                ok(""),
            ),
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
                ..Default::default()
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
                addr6: Some("fd00:10:200:3::11/64".parse::<Ipv6Net>().unwrap()),
                mtu: Some(1420),
                up: true,
            },
        )
        .unwrap();
        sh.route_add(
            Some("pod1"),
            Destination::Default,
            Ipv4Addr::new(10, 200, 3, 1).into(),
        )
        .unwrap();
        sh.route_add(
            Some("pod1"),
            Destination::Default6,
            "fd00:10:200:3::1".parse().unwrap(),
        )
        .unwrap();
        sh.route_add(
            None,
            Destination::Net("10.200.2.9/24".parse::<Ipv4Net>().unwrap()),
            Ipv4Addr::new(100, 64, 0, 2).into(),
        )
        .unwrap();
        assert_eq!(sh.interfaces(Some("pod1")).unwrap(), ["lo0", "epair0b"]);
    }

    #[test]
    fn wireguard() {
        let key = "IKZKpQMHH2P54bdfaIAH2z5gZ5z/zMKXR3aVlYeff1U=";
        let public = "bHl/X2scPeKxboeUJP6EVXRQT6n3N0uWVAj/pVg5rj0=";
        let peer = "5nbajeT02rOuwRCUy6ojUtx5QYfcxNWsyvKG+zkJDlY=";
        let dump = format!(
            // Captured on FreeBSD 15.1, with a peer that has no endpoint
            // yet added.
            "{key}\t{public}\t51820\toff\n\
             {peer}\t(none)\t192.168.251.2:51820\t10.200.2.0/24,100.64.2.1/32,fd00:10:200:2::/64\t1793078934\t644\t784\t25\n\
             x6TAvtkCJ4t2nKnVPZvb4CFNTsbtUH0cDNzEhsEyd0A=\t(none)\t(none)\t(none)\t0\t0\t0\toff\n"
        );
        let set = format!(
            "wg set wg0 peer {peer} endpoint 192.168.251.2:51820 \
             allowed-ips 100.64.2.1/32,10.200.2.0/24,fd00:10:200:2::/64 persistent-keepalive 25"
        );
        let remove = format!("wg set wg0 peer {peer} remove");
        let sh = Script::shell(vec![
            ("wg genkey", ok(&format!("{key}\n"))),
            ("wg pubkey", ok(&format!("{public}\n"))),
            ("ifconfig wg create name wg0", ok("wg0\n")),
            (
                "wg set wg0 private-key /dev/stdin listen-port 51820",
                ok(""),
            ),
            ("wg show wg0 dump", ok(&dump)),
            (&set, ok("")),
            (&remove, ok("")),
            (
                "route -q add -net 10.200.0.0/16 -interface wg0",
                err("route: message indicates error: File exists"),
            ),
            ("route -q change -net 10.200.0.0/16 -interface wg0", ok("")),
            (
                "route -q add -inet6 -net fd00:10:200::/48 -interface wg0",
                ok(""),
            ),
        ]);
        assert_eq!(sh.wg_genkey().unwrap(), key);
        assert_eq!(sh.wg_pubkey(key).unwrap(), public);
        sh.wg_create("wg0").unwrap();
        sh.wg_configure("wg0", key, 51820).unwrap();
        let net = |s: &str| s.parse::<IpNet>().unwrap();
        let peers = sh.wg_peers("wg0").unwrap();
        assert_eq!(
            peers[0],
            WgPeer {
                public_key: peer.into(),
                endpoint: Some("192.168.251.2:51820".into()),
                allowed_ips: vec![
                    net("10.200.2.0/24"),
                    net("100.64.2.1/32"),
                    net("fd00:10:200:2::/64")
                ],
                keepalive: Some(25),
            }
        );
        assert_eq!((&peers[1].endpoint, peers[1].keepalive), (&None, None));
        assert!(peers[1].allowed_ips.is_empty());
        sh.wg_set_peer(
            "wg0",
            &WgPeer {
                allowed_ips: vec![
                    net("100.64.2.1/32"),
                    net("10.200.2.0/24"),
                    net("fd00:10:200:2::/64"),
                ],
                ..peers[0].clone()
            },
        )
        .unwrap();
        sh.wg_remove_peer("wg0", peer).unwrap();
        let v4 = "10.200.0.0/16".parse().unwrap();
        sh.route_to_interface(None, Destination::Net(v4), "wg0")
            .unwrap();
        let v6 = "fd00:10:200::/48".parse().unwrap();
        sh.route_to_interface(None, Destination::Net6(v6), "wg0")
            .unwrap();
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
            (
                "rctl -a jail:i.c:memoryuse:sigkill=536870912/process",
                ok(""),
            ),
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
            resource: Resource::MemoryUse,
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
                "pfctl -q -a legion/svc -t svc_web -T replace 10.200.3.17 fd00:10:200:3::11",
                ok(""),
            ),
            (
                "pfctl -a legion/svc -t svc_web -T show",
                ok("   10.200.3.17\n   fd00:10:200:3::11\n"),
            ),
            (
                "pfctl -a legion/svc -t nope -T show",
                err("pfctl: Table does not exist."),
            ),
            ("pfctl -q -a legion/svc -t svc_web -T replace", ok("")),
            ("pfctl -q -a legion/svc -F Tables", ok("")),
            ("pfctl -q -a legion/svc -F nat", ok("")),
            // Emptied, the anchor is gone.
            (
                "pfctl -q -a legion/svc -F rules",
                err("pfctl: Anchor does not exist."),
            ),
            // An anchor never loaded.
            (
                "pfctl -q -a legion/none -F Tables",
                err("pfctl: Operation not supported by device"),
            ),
            ("pfctl -q -a legion/none -F nat", ok("")),
            ("pfctl -q -a legion/none -F rules", ok("")),
            // Anything else is an error.
            (
                "pfctl -q -a legion/bad -F Tables",
                err("pfctl: Permission denied"),
            ),
        ]);
        sh.pf_load_anchor("legion/svc", "pass in quick on legion0 from <svc_web>\n")
            .unwrap();
        let addrs: [IpAddr; 2] = [
            Ipv4Addr::new(10, 200, 3, 17).into(),
            "fd00:10:200:3::11".parse().unwrap(),
        ];
        sh.pf_table_replace("legion/svc", "svc_web", &addrs)
            .unwrap();
        assert_eq!(sh.pf_table_show("legion/svc", "svc_web").unwrap(), addrs);
        assert!(sh.pf_table_show("legion/svc", "nope").unwrap().is_empty());
        sh.pf_table_replace("legion/svc", "svc_web", &[]).unwrap();
        sh.pf_flush_anchor("legion/svc").unwrap();
        sh.pf_flush_anchor("legion/none").unwrap();
        assert!(sh.pf_flush_anchor("legion/bad").is_err());
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
            (
                "zfs clone -o mountpoint=/c tank/legion/volumes/shop/data@t1 tank/c",
                ok(""),
            ),
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
        sh.zfs_clone(
            "tank/legion/volumes/shop/data@t1",
            "tank/c",
            &[("mountpoint", "/c")],
        )
        .unwrap();
        sh.zfs_destroy("tank/legion/volumes/shop/data", true)
            .unwrap();
    }

    #[test]
    fn jails_and_mounts() {
        let sh = Script::shell(vec![
            (
                "jail -i -c name=p path=/var/empty vnet persist",
                ok("7
"),
            ),
            (
                "jls -j p.c jid",
                ok("8
"),
            ),
            ("jail -r 8", ok("")),
            ("jls -j gone jid", err("jls: jail \"gone\" not found")),
            // Captured on FreeBSD 15.1, just after the jail's removal.
            ("jls -j dying jid", err("jls: jail \"dying\" is dying")),
            ("mount -t nullfs -o ro /src /dst", ok("")),
            ("mount -t devfs devfs /r/dev", ok("")),
            ("devfs -m /r/dev ruleset 4", ok("")),
            ("devfs -m /r/dev rule applyset", ok("")),
        ]);
        let jid = sh
            .jail_create(&[
                ("name", "p"),
                ("path", "/var/empty"),
                ("vnet", ""),
                ("persist", ""),
            ])
            .unwrap();
        assert_eq!(jid, 7);
        sh.jail_remove("p.c").unwrap();
        assert!(matches!(sh.jail_remove("gone"), Err(Error::NotFound(_))));
        assert!(matches!(sh.jail_remove("dying"), Err(Error::NotFound(_))));
        sh.mount_nullfs("/src", "/dst", true).unwrap();
        sh.mount_devfs("/r/dev", 4).unwrap();
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
