//! [`Sys`] in memory, for tests of the agent. It models what the agent
//! relies on: interfaces live in one network stack at a time and return to
//! the host when their jail goes away; WireGuard interfaces, rctl rules, pf
//! anchors and tables, and datasets and mounts are kept as given. Keys are
//! made up: a public key is `pub:` and the private one. Failures mirror the real tools'
//! (missing interface, missing jail, existing dataset, unmounting what is
//! not mounted).
//!
//! Tests that fake a runtime add jails with
//! [`FakeSys::add_jail`] and remove them with [`FakeSys::remove_jail`], as
//! a runtime does through [`Sys::jail_create`] and [`Sys::jail_remove`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Mutex, MutexGuard};

use crate::{
    Carp, CarpState, Dataset, Destination, Epair, Error, IfConfig, Ipv6Net, IscsiSession, Jail,
    JailMemory, PfState, RctlRule, Result, Sys, WgPeer,
};

#[derive(Debug, Default)]
pub struct FakeSys {
    state: Mutex<State>,
}

/// One interface as the fake sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FakeInterface {
    /// The jail whose network stack holds it; `None` for the host.
    pub jail: Option<String>,
    pub config: IfConfig,
    /// Bridge members, for a bridge.
    pub members: Vec<String>,
    /// The other end, for an epair.
    pub peer: Option<String>,
    /// Its Ethernet address, for a bridge or an epair end.
    pub ether: Option<String>,
}

#[derive(Debug, Default)]
struct State {
    modules: BTreeSet<String>,
    next_jid: u32,
    next_epair: u32,
    next_ether: u32,
    jails: BTreeMap<String, Jail>,
    /// Parameters set with `jail -m`, by jail name.
    jail_params: BTreeMap<String, BTreeMap<String, String>>,
    interfaces: BTreeMap<String, FakeInterface>,
    routes: BTreeMap<Option<String>, BTreeMap<Destination, IpAddr>>,
    /// Routes out of an interface, by stack and destination.
    interface_routes: BTreeMap<Option<String>, BTreeMap<Destination, String>>,
    next_key: u32,
    wireguard: BTreeMap<String, FakeWireGuard>,
    rctl: Vec<RctlRule>,
    usage: BTreeMap<String, BTreeMap<String, u64>>,
    anchors: BTreeMap<String, String>,
    tables: BTreeMap<(String, String), Vec<IpAddr>>,
    datasets: BTreeMap<String, Dataset>,
    /// Snapshots, with the order they were taken in.
    snapshots: BTreeMap<String, u64>,
    next_snapshot: u64,
    /// What tests wrote into datasets, and what their snapshots hold.
    contents: BTreeMap<String, Vec<u8>>,
    /// Clones, with the snapshot each was made from.
    origins: BTreeMap<String, String>,
    /// tmpfs mount points, with their size.
    mounts: BTreeMap<String, Option<u64>>,
    /// What `mount_fs` mounted where: `<fstype> <source>`.
    sources: BTreeMap<String, String>,
    /// iSCSI targets that may be logged in to, with their LUNs' devices.
    targets: BTreeMap<String, BTreeMap<u32, String>>,
    /// Targets logged in to, with their portals.
    sessions: BTreeMap<String, String>,
    /// File systems on devices, by device path.
    filesystems: BTreeMap<String, String>,
    /// Devices whose file system is unclean: mounted read-write only after
    /// a check.
    unclean: BTreeSet<String>,
    /// Programs that run as volume plugins: `mount <dir>` mounts something
    /// there, `unmount <dir>` unmounts it.
    plugins: BTreeSet<String>,
    /// Every program run, with its arguments.
    ran: Vec<String>,
    /// devfs links: path, target. They go with their devfs's unmount.
    links: BTreeMap<String, String>,
    default_interface: Option<String>,
    sysctls: BTreeMap<String, String>,
    main_rules: String,
    states: Vec<PfState>,
    /// CARP virtual hosts by interface and vhid, with their state.
    carp: BTreeMap<(String, u8), (Carp, CarpState)>,
    /// Commands that fail once, the next time they run.
    failing: BTreeSet<String>,
    /// Mount points something still holds (a dying jail its root): a
    /// dataset mounted there is busy until the path is unmounted, by force.
    held: BTreeSet<String>,
    /// Address selection policies added to jails' VNETs.
    addr_policies: BTreeMap<String, Vec<(Ipv6Net, u32, u32)>>,
    /// Anchors whose loads fail until told otherwise.
    broken_anchors: BTreeSet<String>,
    /// What jails' processes use, as set.
    memory: BTreeMap<String, JailMemory>,
    kills: Vec<u32>,
}

impl State {
    fn take_snapshot(&mut self, snapshot: &str) {
        self.next_snapshot += 1;
        self.snapshots
            .insert(snapshot.to_owned(), self.next_snapshot);
    }
}

/// A WireGuard interface's settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FakeWireGuard {
    pub private_key: Option<String>,
    pub port: Option<u16>,
    /// By public key.
    pub peers: BTreeMap<String, WgPeer>,
}

fn fail(command: &str, message: impl Into<String>) -> Error {
    Error::Failed {
        command: command.to_owned(),
        message: message.into(),
    }
}

impl FakeSys {
    /// A host as Legion wants it: racct on, so rctl limits hold.
    pub fn new() -> Self {
        let sys = Self::default();
        sys.state()
            .sysctls
            .insert("kern.racct.enable".into(), "1".into());
        sys
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Creates a jail, as a runtime does for a container. A child's `name` is
    /// `<parent>.<child>`.
    pub fn add_jail(&self, name: &str, vnet: bool) -> u32 {
        let mut s = self.state();
        s.next_jid += 1;
        let jid = s.next_jid;
        let parent = match name.rsplit_once('.') {
            Some((p, _)) => {
                let parent = s.jails.get_mut(p).expect("parent jail exists");
                parent.children_cur += 1;
                parent.jid
            }
            None => 0,
        };
        s.jails.insert(
            name.to_owned(),
            Jail {
                jid,
                name: name.to_owned(),
                vnet: if vnet { "new" } else { "inherit" }.to_owned(),
                children_max: 0,
                children_cur: 0,
                parent,
            },
        );
        jid
    }

    /// Removes a jail and its children. Interfaces in their network stacks
    /// return to the host, unconfigured, as the kernel does.
    pub fn remove_jail(&self, name: &str) {
        let mut s = self.state();
        let prefix = format!("{name}.");
        let gone: Vec<String> = s
            .jails
            .keys()
            .filter(|j| *j == name || j.starts_with(&prefix))
            .cloned()
            .collect();
        if let Some((p, _)) = name.rsplit_once('.')
            && let Some(parent) = s.jails.get_mut(p)
        {
            parent.children_cur = parent.children_cur.saturating_sub(1);
        }
        for j in &gone {
            s.jails.remove(j);
            s.jail_params.remove(j);
            s.routes.remove(&Some(j.clone()));
        }
        for i in s.interfaces.values_mut() {
            if i.jail.as_ref().is_some_and(|j| gone.contains(j)) {
                i.jail = None;
                i.config = IfConfig::default();
            }
        }
    }

    /// The CARP virtual host `vhid` on `interface`, with its state.
    pub fn carp(&self, interface: &str, vhid: u8) -> Option<(Carp, CarpState)> {
        self.state()
            .carp
            .get(&(interface.to_owned(), vhid))
            .cloned()
    }

    /// Changes a virtual host's state, as CARP advertisements on the link
    /// would.
    pub fn set_carp_state(&self, interface: &str, vhid: u8, state: CarpState) {
        if let Some(c) = self.state().carp.get_mut(&(interface.to_owned(), vhid)) {
            c.1 = state;
        }
    }

    /// Sets what [`Sys::jail_memory`] reports for a jail; without it,
    /// what [`FakeSys::set_usage`] gave as `memoryuse`.
    pub fn set_memory(&self, jail: &str, memory: JailMemory) {
        self.state().memory.insert(jail.to_owned(), memory);
    }

    /// The processes killed so far, in order.
    pub fn kills(&self) -> Vec<u32> {
        self.state().kills.clone()
    }

    /// Sets what [`Sys::rctl_usage`] reports for a jail.
    pub fn set_usage(&self, jail: &str, usage: &[(&str, u64)]) {
        self.state().usage.insert(
            jail.to_owned(),
            usage.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
        );
    }

    pub fn interface(&self, name: &str) -> Option<FakeInterface> {
        self.state().interfaces.get(name).cloned()
    }

    pub fn routes(&self, jail: Option<&str>) -> BTreeMap<Destination, IpAddr> {
        self.state()
            .routes
            .get(&jail.map(str::to_owned))
            .cloned()
            .unwrap_or_default()
    }

    pub fn interface_routes(&self, jail: Option<&str>) -> BTreeMap<Destination, String> {
        self.state()
            .interface_routes
            .get(&jail.map(str::to_owned))
            .cloned()
            .unwrap_or_default()
    }

    pub fn wireguard(&self, name: &str) -> Option<FakeWireGuard> {
        self.state().wireguard.get(name).cloned()
    }

    /// A peer WireGuard knows of without being told, as when it learns one
    /// from a handshake.
    pub fn add_wg_peer(&self, name: &str, peer: WgPeer) {
        if let Some(wg) = self.state().wireguard.get_mut(name) {
            wg.peers.insert(peer.public_key.clone(), peer);
        }
    }

    pub fn rctl_rules(&self) -> Vec<RctlRule> {
        self.state().rctl.clone()
    }

    pub fn anchor(&self, anchor: &str) -> Option<String> {
        self.state().anchors.get(anchor).cloned()
    }

    pub fn modules(&self) -> BTreeSet<String> {
        self.state().modules.clone()
    }

    pub fn snapshots(&self) -> BTreeSet<String> {
        self.state().snapshots.keys().cloned().collect()
    }

    /// Replaces what a dataset holds, as a task writing to it would.
    pub fn write_dataset(&self, dataset: &str, data: &[u8]) {
        self.state()
            .contents
            .insert(dataset.to_owned(), data.to_vec());
    }

    /// What a dataset holds.
    pub fn dataset_contents(&self, dataset: &str) -> Option<Vec<u8>> {
        self.state().contents.get(dataset).cloned()
    }

    /// The host's interface with the default route; it is created if
    /// missing, as a host's would exist.
    pub fn set_default_interface(&self, name: &str) {
        let mut s = self.state();
        s.default_interface = Some(name.to_owned());
        s.interfaces
            .entry(name.to_owned())
            .or_insert_with(|| FakeInterface {
                jail: None,
                config: IfConfig::default(),
                members: Vec::new(),
                peer: None,
                ether: None,
            });
    }

    pub fn sysctl(&self, name: &str) -> Option<String> {
        self.state().sysctls.get(name).cloned()
    }

    /// What `pfctl -sn` and `-sr` would print for the main ruleset.
    pub fn set_main_rules(&self, rules: &str) {
        self.state().main_rules = rules.to_owned();
    }

    pub fn add_state(&self, state: PfState) {
        self.state().states.push(state);
    }

    pub fn states(&self) -> Vec<PfState> {
        self.state().states.clone()
    }

    /// Makes `command` fail the next time it runs. Only some honour it:
    /// `pfctl -ss`, `pfctl -k`, `rctl -a` and `zfs destroy -r`.
    pub fn fail_next(&self, command: &str) {
        self.state().failing.insert(command.to_owned());
    }

    /// Holds the mount point `path`, as a dying jail holds its root: the
    /// dataset mounted there cannot be destroyed until [`Sys::unmount`]
    /// (a forced unmount) lets it go.
    pub fn hold(&self, path: &str) {
        self.state().held.insert(path.to_owned());
    }

    /// The address selection policies [`Sys::addr_policy_add`] gave `jail`.
    pub fn addr_policies(&self, jail: &str) -> Vec<(Ipv6Net, u32, u32)> {
        self.state()
            .addr_policies
            .get(jail)
            .cloned()
            .unwrap_or_default()
    }

    /// Makes loading `anchor` fail from now on, or work again.
    pub fn break_anchor(&self, anchor: &str, broken: bool) {
        let mut s = self.state();
        if broken {
            s.broken_anchors.insert(anchor.to_owned());
        } else {
            s.broken_anchors.remove(anchor);
        }
    }

    /// tmpfs mount points and their sizes.
    pub fn tmpfs_mounts(&self) -> BTreeMap<String, Option<u64>> {
        self.state().mounts.clone()
    }

    pub fn devfs_links(&self) -> BTreeMap<String, String> {
        self.state().links.clone()
    }

    /// What [`Sys::mount_fs`] mounted at `path`: `<fstype> <source>`.
    pub fn mount_source(&self, path: &str) -> Option<String> {
        let s = self.state();
        s.mounts
            .contains_key(path)
            .then(|| s.sources.get(path).cloned())?
    }

    /// An iSCSI target this host may log in to, its LUNs as devices
    /// (`da0`) holding file systems (`ufs`, or none).
    pub fn add_iscsi_target(&self, target: &str, luns: &[(u32, &str, Option<&str>)]) {
        let mut s = self.state();
        let devices = luns.iter().map(|(l, d, _)| (*l, (*d).to_owned())).collect();
        s.targets.insert(target.to_owned(), devices);
        for (_, device, fs) in luns {
            if let Some(fs) = fs {
                s.filesystems
                    .insert(format!("/dev/{device}"), (*fs).to_owned());
            }
        }
    }

    /// Marks the file system on `device` (`/dev/da0`) unclean, as a crash
    /// leaves it.
    pub fn make_unclean(&self, device: &str) {
        self.state().unclean.insert(device.to_owned());
    }

    /// The targets logged in to.
    pub fn iscsi_targets(&self) -> Vec<String> {
        self.state().sessions.keys().cloned().collect()
    }

    /// Makes `program` a volume plugin.
    pub fn add_plugin(&self, program: &str) {
        self.state().plugins.insert(program.to_owned());
    }

    /// The programs run so far, each with its arguments.
    pub fn programs_run(&self) -> Vec<String> {
        self.state().ran.clone()
    }
}

fn wg_mut<'a>(s: &'a mut State, name: &str) -> Result<&'a mut FakeWireGuard> {
    s.wireguard
        .get_mut(name)
        .ok_or_else(|| fail("wg", format!("Unable to access interface {name}")))
}

impl State {
    fn check_jail(&self, command: &str, jail: Option<&str>) -> Result<()> {
        match jail {
            Some(j) if !self.jails.contains_key(j) => {
                Err(fail(command, format!("jail \"{j}\" not found")))
            }
            _ => Ok(()),
        }
    }

    /// A new, locally administered Ethernet address.
    fn new_ether(&mut self) -> String {
        self.next_ether += 1;
        let n = self.next_ether;
        format!("02:00:00:00:{:02x}:{:02x}", (n >> 8) & 0xff, n & 0xff)
    }

    /// An interface in the given network stack.
    fn interface_mut(
        &mut self,
        command: &str,
        jail: Option<&str>,
        name: &str,
    ) -> Result<&mut FakeInterface> {
        self.interfaces
            .get_mut(name)
            .filter(|i| i.jail.as_deref() == jail)
            .ok_or_else(|| fail(command, format!("interface {name} does not exist")))
    }
}

impl Sys for FakeSys {
    fn kld_load(&self, module: &str) -> Result<()> {
        self.state().modules.insert(module.to_owned());
        Ok(())
    }

    fn jails(&self) -> Result<Vec<Jail>> {
        Ok(self.state().jails.values().cloned().collect())
    }

    fn jail_set(&self, jail: &str, params: &[(&str, &str)]) -> Result<()> {
        let mut s = self.state();
        let j = s
            .jails
            .get_mut(jail)
            .ok_or_else(|| fail("jail -m", format!("jail \"{jail}\" not found")))?;
        for (k, v) in params {
            if *k == "children.max" {
                j.children_max = v
                    .parse()
                    .map_err(|_| fail("jail -m", format!("children.max: invalid {v:?}")))?;
            }
        }
        let kept = s.jail_params.entry(jail.to_owned()).or_default();
        for (k, v) in params {
            kept.insert((*k).to_owned(), (*v).to_owned());
        }
        Ok(())
    }

    fn jail_create(&self, params: &[(&str, &str)]) -> Result<u32> {
        let get = |k: &str| params.iter().find(|(n, _)| *n == k).map(|(_, v)| *v);
        let name = get("name").ok_or_else(|| fail("jail", "no name"))?;
        {
            let s = self.state();
            if s.jails.contains_key(name) {
                return Err(fail("jail", format!("jail \"{name}\" already exists")));
            }
            if let Some((p, _)) = name.rsplit_once('.')
                && !s.jails.contains_key(p)
            {
                return Err(fail("jail", format!("jail \"{p}\" not found")));
            }
        }
        let vnet = matches!(get("vnet"), Some("" | "new"));
        let jid = self.add_jail(name, vnet);
        let mut s = self.state();
        let recorded = s.jail_params.entry(name.to_owned()).or_default();
        for (k, v) in params {
            recorded.insert((*k).to_owned(), (*v).to_owned());
        }
        if let Some(max) = get("children.max").and_then(|m| m.parse().ok()) {
            s.jails.get_mut(name).expect("added").children_max = max;
        }
        Ok(jid)
    }

    /// Kept as the jail's parameter `cpuset`, which `jail_get` reads.
    fn jail_cpuset(&self, jid: u32, cpus: &str) -> Result<()> {
        let mut s = self.state();
        let name = s
            .jails
            .values()
            .find(|j| j.jid == jid)
            .map(|j| j.name.clone())
            .ok_or_else(|| fail("cpuset", format!("jail {jid}: not found")))?;
        s.jail_params
            .entry(name)
            .or_default()
            .insert("cpuset".to_owned(), cpus.to_owned());
        Ok(())
    }

    fn addr_policy_add(&self, jail: &str, policies: &[(Ipv6Net, u32, u32)]) -> Result<()> {
        let mut s = self.state();
        if !s.jails.get(jail).is_some_and(|j| j.vnet == "new") {
            return Err(fail("ip6addrctl", format!("{jail}: not a VNET jail")));
        }
        s.addr_policies
            .entry(jail.to_owned())
            .or_default()
            .extend_from_slice(policies);
        Ok(())
    }

    fn jail_remove(&self, jail: &str) -> Result<()> {
        if !self.state().jails.contains_key(jail) {
            return Err(Error::NotFound(format!("jail {jail}")));
        }
        self.remove_jail(jail);
        Ok(())
    }

    fn jail_get(&self, jail: &str, params: &[&str]) -> Result<BTreeMap<String, String>> {
        let s = self.state();
        if !s.jails.contains_key(jail) {
            return Err(Error::NotFound(format!("jail {jail}")));
        }
        let kept = s.jail_params.get(jail);
        params
            .iter()
            .map(|p| {
                let v = kept
                    .and_then(|k| k.get(*p))
                    .ok_or_else(|| fail("jls", format!("{p} was never set")))?;
                Ok(((*p).to_owned(), v.clone()))
            })
            .collect()
    }

    fn interfaces(&self, jail: Option<&str>) -> Result<Vec<String>> {
        let s = self.state();
        s.check_jail("ifconfig -l", jail)?;
        Ok(std::iter::once("lo0".to_owned())
            .chain(
                s.interfaces
                    .iter()
                    .filter(|(_, i)| i.jail.as_deref() == jail)
                    .map(|(n, _)| n.clone()),
            )
            .collect())
    }

    fn bridge_create(&self, name: &str) -> Result<()> {
        let mut s = self.state();
        if s.interfaces.contains_key(name) {
            return Err(fail(
                "ifconfig name",
                "ioctl SIOCSIFNAME (set name): File exists",
            ));
        }
        let ether = Some(s.new_ether());
        s.interfaces.insert(
            name.to_owned(),
            FakeInterface {
                ether,
                ..Default::default()
            },
        );
        Ok(())
    }

    fn epair_create(&self) -> Result<Epair> {
        let mut s = self.state();
        let n = s.next_epair;
        s.next_epair += 1;
        let pair = Epair {
            a: format!("epair{n}a"),
            b: format!("epair{n}b"),
        };
        for (me, peer) in [(&pair.a, &pair.b), (&pair.b, &pair.a)] {
            let ether = Some(s.new_ether());
            s.interfaces.insert(
                me.clone(),
                FakeInterface {
                    peer: Some(peer.clone()),
                    ether,
                    ..Default::default()
                },
            );
        }
        Ok(pair)
    }

    fn interface_destroy(&self, name: &str) -> Result<()> {
        let mut s = self.state();
        let peer = s
            .interface_mut("ifconfig destroy", None, name)?
            .peer
            .clone();
        s.interfaces.remove(name);
        s.wireguard.remove(name);
        for table in s.interface_routes.values_mut() {
            table.retain(|_, i| i != name);
        }
        // Destroying either end of an epair destroys both.
        if let Some(peer) = peer {
            s.interfaces.remove(&peer);
        }
        for i in s.interfaces.values_mut() {
            i.members.retain(|m| m != name);
        }
        Ok(())
    }

    fn interface_rename(&self, name: &str, new_name: &str) -> Result<()> {
        let mut s = self.state();
        if s.interfaces.contains_key(new_name) {
            return Err(fail(
                "ifconfig name",
                "ioctl SIOCSIFNAME (set name): File exists",
            ));
        }
        if new_name.len() > 15 {
            return Err(fail(
                "ifconfig name",
                "ioctl SIOCSIFNAME (set name): Invalid argument",
            ));
        }
        s.interface_mut("ifconfig name", None, name)?;
        let i = s.interfaces.remove(name).expect("checked");
        if let Some(peer) = &i.peer
            && let Some(p) = s.interfaces.get_mut(peer)
        {
            p.peer = Some(new_name.to_owned());
        }
        for other in s.interfaces.values_mut() {
            for m in other.members.iter_mut().filter(|m| *m == name) {
                *m = new_name.to_owned();
            }
        }
        s.interfaces.insert(new_name.to_owned(), i);
        Ok(())
    }

    fn bridge_add_member(&self, bridge: &str, member: &str) -> Result<()> {
        let mut s = self.state();
        s.interface_mut("ifconfig addm", None, member)?;
        let b = s.interface_mut("ifconfig addm", None, bridge)?;
        if !b.members.iter().any(|m| m == member) {
            b.members.push(member.to_owned());
        }
        Ok(())
    }

    fn interface_ether(&self, name: &str) -> Result<String> {
        let mut s = self.state();
        let i = s.interface_mut("ifconfig", None, name)?;
        i.ether
            .clone()
            .ok_or_else(|| fail("ifconfig", format!("{name} has no Ethernet address")))
    }

    fn interface_set_ether(&self, name: &str, ether: &str) -> Result<()> {
        let mut s = self.state();
        s.interface_mut("ifconfig ether", None, name)?.ether = Some(ether.to_owned());
        Ok(())
    }

    fn interface_move(&self, name: &str, jail: &str) -> Result<()> {
        let mut s = self.state();
        match s.jails.get(jail) {
            None => return Err(fail("ifconfig vnet", format!("jail \"{jail}\" not found"))),
            Some(j) if j.vnet != "new" => {
                return Err(fail("ifconfig vnet", "Invalid argument"));
            }
            Some(_) => {}
        }
        let i = s.interface_mut("ifconfig vnet", None, name)?;
        i.jail = Some(jail.to_owned());
        i.config = IfConfig::default();
        Ok(())
    }

    fn interface_configure(&self, jail: Option<&str>, name: &str, config: &IfConfig) -> Result<()> {
        let mut s = self.state();
        s.check_jail("ifconfig", jail)?;
        if name == "lo0" {
            return Ok(());
        }
        let i = s.interface_mut("ifconfig", jail, name)?;
        if config.addr.is_some() {
            i.config.addr = config.addr;
        }
        if config.addr6.is_some() {
            i.config.addr6 = config.addr6;
        }
        if config.mtu.is_some() {
            i.config.mtu = config.mtu;
        }
        i.config.up |= config.up;
        Ok(())
    }

    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: IpAddr) -> Result<()> {
        let mut s = self.state();
        s.check_jail("route add", jail)?;
        // The gateway must be on a configured, attached network.
        let reachable = s.interfaces.values().any(|i| {
            i.jail.as_deref() == jail
                && match gateway {
                    IpAddr::V4(g) => i.config.addr.is_some_and(|a| a.contains(g)),
                    IpAddr::V6(g) => i.config.addr6.is_some_and(|a| a.contains(g)),
                }
        });
        if !reachable {
            return Err(fail("route add", "Network is unreachable"));
        }
        let table = s.routes.entry(jail.map(str::to_owned)).or_default();
        if table.contains_key(&dest) {
            return Err(fail("route add", "File exists"));
        }
        table.insert(dest, gateway);
        Ok(())
    }

    fn route_delete(&self, jail: Option<&str>, dest: Destination) -> Result<()> {
        let mut s = self.state();
        s.check_jail("route delete", jail)?;
        s.routes
            .get_mut(&jail.map(str::to_owned))
            .and_then(|t| t.remove(&dest))
            .map(drop)
            .ok_or_else(|| fail("route delete", "not in table"))
    }

    fn route_to_interface(
        &self,
        jail: Option<&str>,
        dest: Destination,
        interface: &str,
    ) -> Result<()> {
        let mut s = self.state();
        s.check_jail("route add", jail)?;
        s.interface_mut("route add", jail, interface)?;
        if let Some(t) = s.routes.get_mut(&jail.map(str::to_owned)) {
            t.remove(&dest);
        }
        s.interface_routes
            .entry(jail.map(str::to_owned))
            .or_default()
            .insert(dest, interface.to_owned());
        Ok(())
    }

    fn carp_set(&self, interface: &str, carp: &Carp, state: Option<CarpState>) -> Result<()> {
        let mut s = self.state();
        s.modules.insert("carp".into());
        let entry = s
            .carp
            .entry((interface.to_owned(), carp.vhid))
            .or_insert_with(|| (carp.clone(), CarpState::Backup));
        entry.0.advskew = carp.advskew;
        if let Some(state) = state.filter(|s| *s != CarpState::Init) {
            entry.1 = state;
        }
        Ok(())
    }

    fn carp_state(&self, interface: &str, vhid: u8) -> Result<Option<CarpState>> {
        Ok(self.carp(interface, vhid).map(|(_, state)| state))
    }

    fn carp_remove(&self, interface: &str, addr: Ipv4Addr) -> Result<()> {
        self.state()
            .carp
            .retain(|(i, _), (c, _)| !(i == interface && c.addr == addr));
        Ok(())
    }

    fn wg_genkey(&self) -> Result<String> {
        let mut s = self.state();
        s.next_key += 1;
        Ok(format!("key{}", s.next_key))
    }

    fn wg_pubkey(&self, private_key: &str) -> Result<String> {
        Ok(format!("pub:{private_key}"))
    }

    fn wg_create(&self, name: &str) -> Result<()> {
        let mut s = self.state();
        if s.interfaces.contains_key(name) {
            return Err(fail("ifconfig wg create", "File exists"));
        }
        s.interfaces
            .insert(name.to_owned(), FakeInterface::default());
        s.wireguard
            .insert(name.to_owned(), FakeWireGuard::default());
        Ok(())
    }

    fn wg_configure(&self, name: &str, private_key: &str, port: u16) -> Result<()> {
        let mut s = self.state();
        let wg = wg_mut(&mut s, name)?;
        wg.private_key = Some(private_key.to_owned());
        wg.port = Some(port);
        Ok(())
    }

    fn wg_peers(&self, name: &str) -> Result<Vec<WgPeer>> {
        let mut s = self.state();
        Ok(wg_mut(&mut s, name)?.peers.values().cloned().collect())
    }

    fn wg_set_peer(&self, name: &str, peer: &WgPeer) -> Result<()> {
        let mut s = self.state();
        let wg = wg_mut(&mut s, name)?;
        let mut peer = peer.clone();
        peer.allowed_ips.sort();
        let entry = wg
            .peers
            .entry(peer.public_key.clone())
            .or_insert_with(|| WgPeer {
                endpoint: None,
                ..peer.clone()
            });
        if peer.endpoint.is_some() {
            entry.endpoint = peer.endpoint;
        }
        entry.allowed_ips = peer.allowed_ips;
        entry.keepalive = peer.keepalive;
        Ok(())
    }

    fn wg_remove_peer(&self, name: &str, public_key: &str) -> Result<()> {
        let mut s = self.state();
        wg_mut(&mut s, name)?.peers.remove(public_key);
        Ok(())
    }

    fn rctl_add(&self, rule: &RctlRule) -> Result<()> {
        let mut s = self.state();
        if s.failing.remove("rctl -a") {
            return Err(fail("rctl -a", "Operation not permitted"));
        }
        if !s.rctl.contains(rule) {
            s.rctl.push(rule.clone());
        }
        Ok(())
    }

    fn rctl_remove_jail(&self, jail: &str) -> Result<()> {
        self.state().rctl.retain(|r| r.jail != jail);
        Ok(())
    }

    fn rctl_usage(&self, jail: &str) -> Result<BTreeMap<String, u64>> {
        Ok(self.state().usage.get(jail).cloned().unwrap_or_default())
    }

    fn jail_memory(&self, jails: &[String]) -> Result<BTreeMap<String, JailMemory>> {
        let s = self.state();
        let rctl = |j: &str| JailMemory {
            bytes: s
                .usage
                .get(j)
                .and_then(|u| u.get("memoryuse"))
                .copied()
                .unwrap_or(0),
            largest: None,
        };
        Ok(jails
            .iter()
            .map(|j| {
                (
                    j.clone(),
                    s.memory.get(j).copied().unwrap_or_else(|| rctl(j)),
                )
            })
            .collect())
    }

    fn kill(&self, pid: u32) -> Result<()> {
        self.state().kills.push(pid);
        Ok(())
    }

    fn default_interface(&self) -> Result<Option<String>> {
        Ok(self.state().default_interface.clone())
    }

    fn sysctl_get(&self, name: &str) -> Result<String> {
        self.state()
            .sysctls
            .get(name)
            .cloned()
            .ok_or_else(|| fail("sysctl", format!("unknown oid '{name}'")))
    }

    fn sysctl_set(&self, name: &str, value: &str) -> Result<()> {
        self.state()
            .sysctls
            .insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    fn pf_main_rules(&self) -> Result<String> {
        Ok(self.state().main_rules.clone())
    }

    fn pf_states(&self) -> Result<Vec<PfState>> {
        let mut s = self.state();
        if s.failing.remove("pfctl -ss") {
            return Err(fail("pfctl -ss", "Device busy"));
        }
        Ok(s.states.clone())
    }

    fn pf_kill_state(&self, id: &str) -> Result<()> {
        let mut s = self.state();
        if s.failing.remove("pfctl -k") {
            return Err(fail("pfctl -k", "Device busy"));
        }
        s.states.retain(|s| s.id != id);
        Ok(())
    }

    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()> {
        let mut s = self.state();
        if s.broken_anchors.contains(anchor) {
            return Err(fail("pfctl -f", "Device busy"));
        }
        s.anchors.insert(anchor.to_owned(), rules.to_owned());
        Ok(())
    }

    fn pf_flush_anchor(&self, anchor: &str) -> Result<()> {
        let mut s = self.state();
        s.anchors.remove(anchor);
        s.tables.retain(|(a, _), _| a != anchor);
        Ok(())
    }

    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[IpAddr]) -> Result<()> {
        let mut addrs = addrs.to_vec();
        addrs.sort();
        addrs.dedup();
        self.state()
            .tables
            .insert((anchor.to_owned(), table.to_owned()), addrs);
        Ok(())
    }

    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<IpAddr>> {
        Ok(self
            .state()
            .tables
            .get(&(anchor.to_owned(), table.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    fn zfs_list(&self, root: &str) -> Result<Vec<Dataset>> {
        let prefix = format!("{root}/");
        Ok(self
            .state()
            .datasets
            .values()
            .filter(|d| d.name == root || d.name.starts_with(&prefix))
            .cloned()
            .collect())
    }

    fn zfs_create(&self, name: &str, props: &[(&str, &str)]) -> Result<()> {
        let mut s = self.state();
        // Like `zfs create -p`: an existing dataset is fine and left alone.
        if s.datasets.contains_key(name) {
            return Ok(());
        }
        let parts: Vec<&str> = name.split('/').collect();
        for n in 1..parts.len() {
            let parent = parts[..n].join("/");
            s.datasets.entry(parent.clone()).or_insert(Dataset {
                name: parent,
                mountpoint: None,
                quota: None,
            });
        }
        let mut ds = Dataset {
            name: name.to_owned(),
            mountpoint: Some(format!("/{name}")),
            quota: None,
        };
        for (k, v) in props {
            apply_prop(&mut ds, k, v)?;
        }
        s.datasets.insert(name.to_owned(), ds);
        Ok(())
    }

    fn zfs_set(&self, name: &str, prop: &str, value: &str) -> Result<()> {
        let mut s = self.state();
        let ds = s.datasets.get_mut(name).ok_or_else(|| {
            fail(
                "zfs set",
                format!("cannot open '{name}': dataset does not exist"),
            )
        })?;
        apply_prop(ds, prop, value)
    }

    fn zfs_snapshot(&self, snapshot: &str) -> Result<()> {
        let mut s = self.state();
        let (ds, _) = snapshot.split_once('@').ok_or_else(|| {
            fail(
                "zfs snapshot",
                format!("invalid snapshot name {snapshot:?}"),
            )
        })?;
        if !s.datasets.contains_key(ds) {
            return Err(fail(
                "zfs snapshot",
                format!("cannot open '{ds}': dataset does not exist"),
            ));
        }
        if s.snapshots.contains_key(snapshot) {
            return Err(fail(
                "zfs snapshot",
                format!("cannot create snapshot '{snapshot}': dataset already exists"),
            ));
        }
        s.take_snapshot(snapshot);
        let data = s.contents.get(ds).cloned().unwrap_or_default();
        s.contents.insert(snapshot.to_owned(), data);
        Ok(())
    }

    fn zfs_snapshots(&self, dataset: &str) -> Result<Vec<String>> {
        let s = self.state();
        let prefix = format!("{dataset}@");
        let mut found: Vec<(u64, String)> = s
            .snapshots
            .iter()
            .filter_map(|(d, n)| Some((*n, d.strip_prefix(&prefix)?.to_owned())))
            .collect();
        found.sort();
        Ok(found.into_iter().map(|(_, name)| name).collect())
    }

    fn zfs_rollback(&self, snapshot: &str) -> Result<()> {
        let mut s = self.state();
        let (ds, _) = snapshot.split_once('@').ok_or_else(|| {
            fail(
                "zfs rollback",
                format!("invalid snapshot name {snapshot:?}"),
            )
        })?;
        let Some(&taken) = s.snapshots.get(snapshot) else {
            return Err(fail(
                "zfs rollback",
                format!("cannot open '{snapshot}': dataset does not exist"),
            ));
        };
        let prefix = format!("{ds}@");
        let later: Vec<String> = s
            .snapshots
            .iter()
            .filter(|(d, n)| d.starts_with(&prefix) && **n > taken)
            .map(|(d, _)| d.clone())
            .collect();
        for d in later {
            s.snapshots.remove(&d);
            s.contents.remove(&d);
        }
        let data = s.contents.get(snapshot).cloned().unwrap_or_default();
        s.contents.insert(ds.to_owned(), data);
        Ok(())
    }

    fn zfs_clone(&self, snapshot: &str, dataset: &str, props: &[(&str, &str)]) -> Result<()> {
        let mut s = self.state();
        if !s.snapshots.contains_key(snapshot) {
            return Err(fail(
                "zfs clone",
                format!("cannot open '{snapshot}': dataset does not exist"),
            ));
        }
        if s.datasets.contains_key(dataset) {
            return Err(fail(
                "zfs clone",
                format!("cannot create '{dataset}': dataset already exists"),
            ));
        }
        let parent = dataset.rsplit_once('/').map_or("", |(p, _)| p);
        if !s.datasets.contains_key(parent) {
            return Err(fail(
                "zfs clone",
                format!("cannot create '{dataset}': parent does not exist"),
            ));
        }
        let mut ds = Dataset {
            name: dataset.to_owned(),
            mountpoint: Some(format!("/{dataset}")),
            quota: None,
        };
        for (k, v) in props {
            apply_prop(&mut ds, k, v)?;
        }
        s.datasets.insert(dataset.to_owned(), ds);
        let data = s.contents.get(snapshot).cloned().unwrap_or_default();
        s.contents.insert(dataset.to_owned(), data);
        s.origins.insert(dataset.to_owned(), snapshot.to_owned());
        Ok(())
    }

    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()> {
        let mut s = self.state();
        // Clones of the snapshot, or of any of the dataset's, outside what
        // is destroyed.
        let gone = |d: &str| {
            d == name || d.starts_with(&format!("{name}@")) || d.starts_with(&format!("{name}/"))
        };
        if let Some((clone, _)) = s
            .origins
            .iter()
            .find(|(c, o)| gone(o) && !(recursive && gone(c)))
        {
            return Err(fail(
                "zfs destroy",
                format!("cannot destroy '{name}': snapshot has dependent clones ({clone})"),
            ));
        }
        if recursive && s.failing.remove("zfs destroy -r") {
            return Err(fail(
                "zfs destroy",
                format!("cannot destroy '{name}': dataset is busy"),
            ));
        }
        if let Some(at) = s
            .datasets
            .iter()
            .filter(|(d, _)| gone(d))
            .find_map(|(_, ds)| ds.mountpoint.as_ref().filter(|m| s.held.contains(*m)))
        {
            return Err(fail(
                "zfs destroy",
                format!("cannot unmount '{at}': pool or dataset is busy"),
            ));
        }
        if name.contains('@') {
            if s.snapshots.remove(name).is_none() {
                return Err(fail(
                    "zfs destroy",
                    "could not find any snapshots to destroy; check snapshot names.".to_owned(),
                ));
            }
            s.contents.remove(name);
            return Ok(());
        }
        if !s.datasets.contains_key(name) {
            return Err(fail(
                "zfs destroy",
                format!("cannot open '{name}': dataset does not exist"),
            ));
        }
        let child = format!("{name}/");
        let snap = format!("{name}@");
        let has_children = s.datasets.keys().any(|d| d.starts_with(&child))
            || s.snapshots.keys().any(|d| d.starts_with(&snap));
        if has_children && !recursive {
            return Err(fail(
                "zfs destroy",
                format!("cannot destroy '{name}': filesystem has children"),
            ));
        }
        s.datasets
            .retain(|d, _| d != name && !d.starts_with(&child));
        s.snapshots
            .retain(|d, _| !d.starts_with(&snap) && !d.starts_with(&child));
        s.contents
            .retain(|d, _| d != name && !d.starts_with(&snap) && !d.starts_with(&child));
        s.origins.retain(|d, _| d != name && !d.starts_with(&child));
        Ok(())
    }

    /// The stream is `fakezfs <snapshot name> <since or ->` on one line,
    /// then what the snapshot holds.
    fn zfs_send(
        &self,
        snapshot: &str,
        since: Option<&str>,
        out: &mut dyn std::io::Write,
    ) -> Result<()> {
        let (header, data) = {
            let s = self.state();
            let fail_send = |m: String| fail("zfs send", m);
            let (ds, name) = snapshot
                .split_once('@')
                .ok_or_else(|| fail_send(format!("invalid snapshot name {snapshot:?}")))?;
            if !s.snapshots.contains_key(snapshot) {
                return Err(fail_send(format!(
                    "cannot open '{snapshot}': dataset does not exist"
                )));
            }
            if let Some(since) = since
                && !s.snapshots.contains_key(&format!("{ds}@{since}"))
            {
                return Err(fail_send(format!(
                    "cannot open '{ds}@{since}': dataset does not exist"
                )));
            }
            (
                format!("fakezfs {name} {}\n", since.unwrap_or("-")),
                s.contents.get(snapshot).cloned().unwrap_or_default(),
            )
        };
        out.write_all(header.as_bytes())
            .and_then(|()| out.write_all(&data))
            .map_err(|e| fail("zfs send", e.to_string()))
    }

    fn zfs_receive(&self, dataset: &str, input: &mut dyn std::io::Read) -> Result<()> {
        let mut stream = Vec::new();
        input
            .read_to_end(&mut stream)
            .map_err(|e| fail("zfs receive", e.to_string()))?;
        let bad = || fail("zfs receive", "invalid backup stream".to_owned());
        let end = stream.iter().position(|b| *b == b'\n').ok_or_else(bad)?;
        let header = String::from_utf8_lossy(&stream[..end]).into_owned();
        let data = stream[end + 1..].to_vec();
        let mut parts = header.split(' ');
        let (Some("fakezfs"), Some(name), Some(since)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(bad());
        };
        let mut s = self.state();
        let parent = dataset.rsplit_once('/').map(|(p, _)| p);
        if let Some(parent) = parent
            && !s.datasets.contains_key(parent)
        {
            return Err(fail(
                "zfs receive",
                format!("cannot receive new filesystem stream: parent '{parent}' does not exist"),
            ));
        }
        let snap = format!("{dataset}@");
        if since == "-" {
            if let Some(old) = s.snapshots.keys().find(|d| d.starts_with(&snap)) {
                return Err(fail(
                    "zfs receive",
                    format!(
                        "cannot receive new filesystem stream: destination has snapshots (eg. {old})
must destroy them to overwrite it"
                    ),
                ));
            }
            s.datasets.insert(
                dataset.to_owned(),
                Dataset {
                    name: dataset.to_owned(),
                    mountpoint: None,
                    quota: None,
                },
            );
        } else if !s.snapshots.contains_key(&format!("{dataset}@{since}")) {
            return Err(fail(
                "zfs receive",
                format!(
                    "cannot receive incremental stream: most recent snapshot of {dataset} does not match incremental source"
                ),
            ));
        }
        s.take_snapshot(&format!("{dataset}@{name}"));
        s.contents.insert(format!("{dataset}@{name}"), data.clone());
        s.contents.insert(dataset.to_owned(), data);
        Ok(())
    }

    fn zfs_mount(&self, dataset: &str) -> Result<()> {
        let mut s = self.state();
        let ds = s.datasets.get_mut(dataset).ok_or_else(|| {
            fail(
                "zfs mount",
                format!("cannot open '{dataset}': dataset does not exist"),
            )
        })?;
        ds.mountpoint.get_or_insert_with(|| format!("/{dataset}"));
        Ok(())
    }

    fn mount_tmpfs(&self, path: &str, size: Option<u64>, _mode: u32) -> Result<()> {
        let mut s = self.state();
        if s.mounts.contains_key(path) {
            // The real mount would stack a second file system on the first.
            return Err(fail("mount", format!("{path}: already mounted (fake)")));
        }
        s.mounts.insert(path.to_owned(), size);
        Ok(())
    }

    fn mount_nullfs(&self, _source: &str, target: &str, _read_only: bool) -> Result<()> {
        self.mount_tmpfs(target, None, 0)
    }

    fn mount_devfs(&self, target: &str, _ruleset: u32) -> Result<()> {
        self.mount_tmpfs(target, None, 0)
    }

    fn devfs_link(&self, dev: &str, name: &str, target: &str) -> Result<()> {
        let link = format!("{}/{name}", dev.trim_end_matches('/'));
        let mut s = self.state();
        if s.links.contains_key(&link) {
            return Err(fail("ln", format!("{link}: File exists")));
        }
        s.links.insert(link, target.to_owned());
        Ok(())
    }

    fn mount_pseudo(&self, _fstype: &str, target: &str, _options: &str) -> Result<()> {
        self.mount_tmpfs(target, None, 0)
    }

    fn mount_fs(&self, fstype: &str, source: &str, target: &str, _options: &str) -> Result<()> {
        if fstype == "ufs" && !self.state().filesystems.contains_key(source) {
            return Err(fail("mount", format!("{source}: Invalid argument")));
        }
        if fstype == "ufs" && self.state().unclean.contains(source) {
            return Err(fail(
                "mount",
                format!("{source}: R/W mount denied. Filesystem is not clean - run fsck."),
            ));
        }
        self.mount_tmpfs(target, None, 0)?;
        self.state()
            .sources
            .insert(target.to_owned(), format!("{fstype} {source}"));
        Ok(())
    }

    fn fs_type(&self, device: &str) -> Result<Option<String>> {
        let s = self.state();
        let attached = s
            .sessions
            .keys()
            .filter_map(|t| s.targets.get(t))
            .any(|luns| luns.values().any(|d| format!("/dev/{d}") == device));
        if !attached {
            return Err(fail(
                "fstyp",
                format!("{device}: No such file or directory"),
            ));
        }
        Ok(s.filesystems.get(device).cloned())
    }

    fn fsck_preen(&self, device: &str) -> Result<()> {
        self.state().unclean.remove(device);
        Ok(())
    }

    fn iscsi_sessions(&self) -> Result<Vec<IscsiSession>> {
        let s = self.state();
        Ok(s.sessions
            .iter()
            .map(|(target, portal)| IscsiSession {
                target: target.clone(),
                portal: portal.clone(),
                state: "Connected".into(),
                failure: String::new(),
                luns: s.targets[target].clone(),
            })
            .collect())
    }

    fn iscsi_attach(&self, portal: &str, target: &str) -> Result<()> {
        let mut s = self.state();
        if s.sessions.contains_key(target) {
            return Err(fail("iscsictl", "iscsictl: session already exists"));
        }
        if !s.targets.contains_key(target) {
            // The real one adds a session that never connects.
            return Err(fail("iscsictl", format!("{target}: Not found (fake)")));
        }
        s.sessions.insert(target.to_owned(), portal.to_owned());
        Ok(())
    }

    fn iscsi_detach(&self, target: &str) -> Result<()> {
        let mut s = self.state();
        if let Some(luns) = s.targets.get(target) {
            let devices: Vec<String> = luns.values().map(|d| format!("/dev/{d}")).collect();
            if s.sources.iter().any(|(path, src)| {
                s.mounts.contains_key(path)
                    && devices.iter().any(|d| src.ends_with(&format!(" {d}")))
            }) {
                return Err(fail("iscsictl", "device busy: still mounted (fake)"));
            }
        }
        s.sessions.remove(target);
        Ok(())
    }

    fn run_program(&self, program: &str, args: &[&str], _input: &str) -> Result<String> {
        let mut s = self.state();
        s.ran.push(
            std::iter::once(program)
                .chain(args.iter().copied())
                .collect::<Vec<_>>()
                .join(" "),
        );
        if !s.plugins.contains(program) {
            return Err(Error::NotFound(program.to_owned()));
        }
        drop(s);
        match args {
            ["mount", dir] => self.mount_tmpfs(dir, None, 0).map(|()| String::new()),
            ["unmount", dir] => {
                if self.state().mounts.contains_key(*dir) {
                    self.unmount(dir)?;
                }
                Ok(String::new())
            }
            _ => Err(fail(program, "usage: mount|unmount DIR")),
        }
    }

    fn unmount(&self, path: &str) -> Result<()> {
        let mut s = self.state();
        // Forced: what held it lets go.
        if s.held.remove(path) {
            return Ok(());
        }
        let inside = format!("{path}/");
        s.links.retain(|l, _| !l.starts_with(&inside));
        s.sources.remove(path);
        match s.mounts.remove(path) {
            Some(_) => Ok(()),
            None => Err(fail(
                "umount",
                format!("{path}: not a file system root directory"),
            )),
        }
    }

    fn mounts(&self, prefix: &str) -> Result<Vec<String>> {
        // By path components, so tests on any platform see what FreeBSD does.
        let prefix = std::path::Path::new(prefix);
        Ok(self
            .state()
            .mounts
            .keys()
            .filter(|p| std::path::Path::new(p).starts_with(prefix))
            .cloned()
            .collect())
    }
}

fn apply_prop(ds: &mut Dataset, prop: &str, value: &str) -> Result<()> {
    match prop {
        "quota" => {
            ds.quota = match value {
                "none" | "0" => None,
                v => Some(
                    v.parse()
                        .map_err(|_| fail("zfs set", format!("bad numeric value '{v}'")))?,
                ),
            }
        }
        "mountpoint" => {
            ds.mountpoint = match value {
                "none" | "legacy" => None,
                v => Some(v.to_owned()),
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Ipv4Net, Resource};

    fn up(addr: &str) -> IfConfig {
        IfConfig {
            addr: Some(addr.parse::<Ipv4Net>().unwrap()),
            mtu: Some(1420),
            up: true,
            ..Default::default()
        }
    }

    #[test]
    fn pod_network_and_teardown() {
        let sys = FakeSys::new();
        sys.bridge_create("legion0").unwrap();
        assert!(sys.bridge_create("legion0").is_err());
        sys.interface_configure(None, "legion0", &up("10.200.3.1/24"))
            .unwrap();

        sys.add_jail("infra1", true);
        sys.add_jail("plain", false);
        let pair = sys.epair_create().unwrap();
        sys.bridge_add_member("legion0", &pair.a).unwrap();
        assert!(
            sys.interface_move(&pair.b, "plain").is_err(),
            "not a VNET jail"
        );
        sys.interface_move(&pair.b, "infra1").unwrap();
        // The b end is no longer in the host stack.
        assert!(
            sys.interface_configure(None, &pair.b, &up("10.200.3.17/24"))
                .is_err()
        );
        sys.interface_configure(Some("infra1"), &pair.b, &up("10.200.3.17/24"))
            .unwrap();
        assert!(
            sys.route_add(
                Some("infra1"),
                Destination::Default,
                Ipv4Addr::new(10, 9, 9, 9).into()
            )
            .is_err(),
            "gateway off-link"
        );
        sys.route_add(
            Some("infra1"),
            Destination::Default,
            Ipv4Addr::new(10, 200, 3, 1).into(),
        )
        .unwrap();
        // An IPv6 gateway needs an IPv6 address on its link.
        let gw6: IpAddr = "fd00:10:200:3::1".parse().unwrap();
        assert!(
            sys.route_add(Some("infra1"), Destination::Default6, gw6)
                .is_err()
        );
        let addr6 = IfConfig {
            addr6: Some("fd00:10:200:3::11/64".parse().unwrap()),
            ..Default::default()
        };
        sys.interface_configure(Some("infra1"), &pair.b, &addr6)
            .unwrap();
        sys.route_add(Some("infra1"), Destination::Default6, gw6)
            .unwrap();
        assert_eq!(sys.routes(Some("infra1")).len(), 2);
        assert_eq!(sys.interfaces(Some("infra1")).unwrap(), ["lo0", "epair0b"]);
        assert_eq!(sys.interface("legion0").unwrap().members, ["epair0a"]);

        // Removing the jail returns b to the host; destroying a removes both.
        sys.remove_jail("infra1");
        let b = sys.interface(&pair.b).unwrap();
        assert_eq!((b.jail, b.config), (None, IfConfig::default()));
        assert!(sys.routes(Some("infra1")).is_empty());
        sys.interface_destroy(&pair.a).unwrap();
        assert!(sys.interface(&pair.b).is_none());
        assert!(sys.interface("legion0").unwrap().members.is_empty());
    }

    #[test]
    fn child_jails() {
        let sys = FakeSys::new();
        let parent = sys.add_jail("infra1", true);
        sys.jail_set("infra1", &[("children.max", "4")]).unwrap();
        sys.add_jail("infra1.web", false);
        let jails = sys.jails().unwrap();
        assert_eq!(jails[0].children_max, 4);
        assert_eq!(jails[0].children_cur, 1);
        assert_eq!(jails[1].parent, parent);
        sys.remove_jail("infra1");
        assert!(sys.jails().unwrap().is_empty());
    }

    #[test]
    fn zfs_streams() {
        let (a, b) = (FakeSys::new(), FakeSys::new());
        let src = "tank/legion/volumes/shop/data";
        let dst = "lvol/legion/volumes/shop/data";
        a.zfs_create(src, &[]).unwrap();
        a.write_dataset(src, b"one");
        a.zfs_snapshot(&format!("{src}@m1")).unwrap();
        a.write_dataset(src, b"two");
        let send = |snap: &str, since: Option<&str>| {
            let mut out = Vec::new();
            a.zfs_send(&format!("{src}@{snap}"), since, &mut out)
                .unwrap();
            out
        };
        // The parent must be there.
        let full = send("m1", None);
        assert!(b.zfs_receive(dst, &mut full.as_slice()).is_err());
        b.zfs_create("lvol/legion/volumes/shop", &[]).unwrap();
        b.zfs_receive(dst, &mut full.as_slice()).unwrap();
        assert_eq!(b.dataset_contents(dst).unwrap(), b"one");
        // Received unmounted.
        assert_eq!(b.zfs_list(dst).unwrap()[0].mountpoint, None);

        a.zfs_snapshot(&format!("{src}@m2")).unwrap();
        let incremental = send("m2", Some("m1"));
        b.zfs_receive(dst, &mut incremental.as_slice()).unwrap();
        assert_eq!(b.dataset_contents(dst).unwrap(), b"two");
        // A full stream does not replace a dataset with snapshots.
        assert!(b.zfs_receive(dst, &mut full.as_slice()).is_err());
        // Not again: the target is past m1's successor already, so an
        // increment from a snapshot it lacks fails.
        a.zfs_snapshot(&format!("{src}@m3")).unwrap();
        let skip = {
            let mut out = Vec::new();
            a.zfs_send(&format!("{src}@m3"), Some("m2"), &mut out)
                .unwrap();
            out
        };
        b.zfs_destroy(&format!("{dst}@m2"), false).unwrap();
        assert!(b.zfs_receive(dst, &mut skip.as_slice()).is_err());
        assert!(
            a.zfs_send(&format!("{src}@m9"), None, &mut Vec::new())
                .is_err()
        );

        b.zfs_mount(dst).unwrap();
        assert_eq!(
            b.zfs_list(dst).unwrap()[0].mountpoint.as_deref(),
            Some("/lvol/legion/volumes/shop/data")
        );
    }

    #[test]
    fn rctl_pf_zfs() {
        let sys = FakeSys::new();
        let rule = RctlRule {
            jail: "infra1.web".into(),
            resource: Resource::MaxProc,
            amount: 64,
        };
        sys.rctl_add(&rule).unwrap();
        sys.rctl_add(&rule).unwrap();
        assert_eq!(sys.rctl_rules(), [rule]);
        sys.rctl_remove_jail("infra1.web").unwrap();
        sys.rctl_remove_jail("infra1.web").unwrap();
        assert!(sys.rctl_rules().is_empty());
        sys.set_usage("infra1.web", &[("memoryuse", 42)]);
        assert_eq!(sys.rctl_usage("infra1.web").unwrap()["memoryuse"], 42);

        let ip = |n| IpAddr::V4(Ipv4Addr::new(10, 200, 3, n));
        sys.pf_table_replace("legion/svc", "svc_web", &[ip(9), ip(3), ip(9)])
            .unwrap();
        assert_eq!(
            sys.pf_table_show("legion/svc", "svc_web").unwrap(),
            [ip(3), ip(9)]
        );
        sys.pf_load_anchor("legion/svc", "pass all\n").unwrap();
        sys.pf_flush_anchor("legion/svc").unwrap();
        assert!(sys.anchor("legion/svc").is_none());
        assert!(
            sys.pf_table_show("legion/svc", "svc_web")
                .unwrap()
                .is_empty()
        );

        sys.zfs_create("tank/legion/volumes/shop/data", &[("quota", "1024")])
            .unwrap();
        sys.zfs_create("tank/legion/volumes/shop/data", &[])
            .unwrap();
        assert_eq!(sys.zfs_list("tank/legion").unwrap().len(), 4);
        sys.zfs_snapshot("tank/legion/volumes/shop/data@t1")
            .unwrap();
        assert!(
            sys.zfs_destroy("tank/legion/volumes/shop/data", false)
                .is_err()
        );
        sys.zfs_destroy("tank/legion/volumes", true).unwrap();
        assert_eq!(sys.zfs_list("tank/legion").unwrap().len(), 1);
        assert!(sys.snapshots().is_empty());
        assert!(sys.zfs_list("tank/nothing").unwrap().is_empty());
    }

    #[test]
    fn zfs_clones_keep_their_origin() {
        let sys = FakeSys::new();
        sys.zfs_create("tank/img/a", &[("mountpoint", "none")])
            .unwrap();
        sys.zfs_snapshot("tank/img/a@layer").unwrap();
        sys.zfs_create("tank/ctr", &[]).unwrap();
        sys.zfs_clone("tank/img/a@layer", "tank/ctr/c1", &[("mountpoint", "/c1")])
            .unwrap();
        assert!(
            sys.zfs_clone("tank/img/a@layer", "tank/ctr/c1", &[])
                .is_err()
        );
        assert!(
            sys.zfs_clone("tank/img/b@layer", "tank/ctr/c2", &[])
                .is_err()
        );
        assert_eq!(
            sys.zfs_list("tank/ctr/c1").unwrap()[0]
                .mountpoint
                .as_deref(),
            Some("/c1")
        );
        let busy = sys.zfs_destroy("tank/img/a@layer", false).unwrap_err();
        assert!(busy.to_string().contains("dependent clones"), "{busy}");
        assert!(sys.zfs_destroy("tank/img/a", true).is_err());
        sys.zfs_destroy("tank/ctr/c1", false).unwrap();
        sys.zfs_destroy("tank/img/a", true).unwrap();
    }

    #[test]
    fn zfs_rollback() {
        let sys = FakeSys::new();
        let ds = "tank/legion/volumes/shop/data";
        sys.zfs_create(ds, &[]).unwrap();
        assert!(sys.zfs_snapshots(ds).unwrap().is_empty());
        for (snap, data) in [("b", b"one"), ("a", b"two"), ("c", b"thr")] {
            sys.write_dataset(ds, data);
            sys.zfs_snapshot(&format!("{ds}@{snap}")).unwrap();
        }
        sys.write_dataset(ds, b"four");
        assert_eq!(sys.zfs_snapshots(ds).unwrap(), ["b", "a", "c"]);
        sys.zfs_rollback(&format!("{ds}@a")).unwrap();
        assert_eq!(sys.dataset_contents(ds).unwrap(), b"two");
        assert_eq!(sys.zfs_snapshots(ds).unwrap(), ["b", "a"]);
        assert!(sys.zfs_rollback(&format!("{ds}@c")).is_err());
        assert!(sys.zfs_snapshots("tank/nothing").unwrap().is_empty());
    }
}
