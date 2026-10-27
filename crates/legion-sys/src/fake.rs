//! [`Sys`] in memory, for tests of the agent. It models what the agent
//! relies on: interfaces live in one network stack at a time and return to
//! the host when their jail goes away; WireGuard interfaces, rctl rules, pf
//! anchors and tables, and datasets and mounts are kept as given. Keys are
//! made up: a public key is `pub:` and the private one. Failures mirror the real tools'
//! (missing interface, missing jail, existing dataset, unmounting what is
//! not mounted).
//!
//! Jails are created by Podman in production, so tests add them with
//! [`FakeSys::add_jail`] and remove them with [`FakeSys::remove_jail`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::{Mutex, MutexGuard};

use crate::{
    Dataset, Destination, Epair, Error, IfConfig, Jail, PfState, RctlRule, Result, Sys, WgPeer,
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
}

#[derive(Debug, Default)]
struct State {
    modules: BTreeSet<String>,
    next_jid: u32,
    next_epair: u32,
    jails: BTreeMap<String, Jail>,
    interfaces: BTreeMap<String, FakeInterface>,
    routes: BTreeMap<Option<String>, BTreeMap<Destination, Ipv4Addr>>,
    /// Routes out of an interface, by stack and destination.
    interface_routes: BTreeMap<Option<String>, BTreeMap<Destination, String>>,
    next_key: u32,
    wireguard: BTreeMap<String, FakeWireGuard>,
    rctl: Vec<RctlRule>,
    usage: BTreeMap<String, BTreeMap<String, u64>>,
    anchors: BTreeMap<String, String>,
    tables: BTreeMap<(String, String), Vec<Ipv4Addr>>,
    datasets: BTreeMap<String, Dataset>,
    snapshots: BTreeSet<String>,
    /// tmpfs mount points, with their size.
    mounts: BTreeMap<String, Option<u64>>,
    default_interface: Option<String>,
    sysctls: BTreeMap<String, String>,
    main_rules: String,
    states: Vec<PfState>,
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
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Creates a jail, as Podman does for a container. A child's `name` is
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
            s.routes.remove(&Some(j.clone()));
        }
        for i in s.interfaces.values_mut() {
            if i.jail.as_ref().is_some_and(|j| gone.contains(j)) {
                i.jail = None;
                i.config = IfConfig::default();
            }
        }
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

    pub fn routes(&self, jail: Option<&str>) -> BTreeMap<Destination, Ipv4Addr> {
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
        self.state().snapshots.clone()
    }

    pub fn set_default_interface(&self, name: &str) {
        self.state().default_interface = Some(name.to_owned());
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

    /// tmpfs mount points and their sizes.
    pub fn tmpfs_mounts(&self) -> BTreeMap<String, Option<u64>> {
        self.state().mounts.clone()
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
        Ok(())
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
        s.interfaces
            .insert(name.to_owned(), FakeInterface::default());
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
            s.interfaces.insert(
                me.clone(),
                FakeInterface {
                    peer: Some(peer.clone()),
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
        if config.mtu.is_some() {
            i.config.mtu = config.mtu;
        }
        i.config.up |= config.up;
        Ok(())
    }

    fn route_add(&self, jail: Option<&str>, dest: Destination, gateway: Ipv4Addr) -> Result<()> {
        let mut s = self.state();
        s.check_jail("route add", jail)?;
        // The gateway must be on a configured, attached network.
        let reachable = s.interfaces.values().any(|i| {
            i.jail.as_deref() == jail && i.config.addr.is_some_and(|a| a.contains(gateway))
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

    fn default_interface(&self) -> Result<Option<String>> {
        Ok(self.state().default_interface.clone())
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
        Ok(self.state().states.clone())
    }

    fn pf_kill_state(&self, id: &str) -> Result<()> {
        self.state().states.retain(|s| s.id != id);
        Ok(())
    }

    fn pf_load_anchor(&self, anchor: &str, rules: &str) -> Result<()> {
        self.state()
            .anchors
            .insert(anchor.to_owned(), rules.to_owned());
        Ok(())
    }

    fn pf_flush_anchor(&self, anchor: &str) -> Result<()> {
        let mut s = self.state();
        s.anchors.remove(anchor);
        s.tables.retain(|(a, _), _| a != anchor);
        Ok(())
    }

    fn pf_table_replace(&self, anchor: &str, table: &str, addrs: &[Ipv4Addr]) -> Result<()> {
        let mut addrs = addrs.to_vec();
        addrs.sort();
        addrs.dedup();
        self.state()
            .tables
            .insert((anchor.to_owned(), table.to_owned()), addrs);
        Ok(())
    }

    fn pf_table_show(&self, anchor: &str, table: &str) -> Result<Vec<Ipv4Addr>> {
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
        if !s.snapshots.insert(snapshot.to_owned()) {
            return Err(fail(
                "zfs snapshot",
                format!("cannot create snapshot '{snapshot}': dataset already exists"),
            ));
        }
        Ok(())
    }

    fn zfs_destroy(&self, name: &str, recursive: bool) -> Result<()> {
        let mut s = self.state();
        if !s.datasets.contains_key(name) {
            return Err(fail(
                "zfs destroy",
                format!("cannot open '{name}': dataset does not exist"),
            ));
        }
        let child = format!("{name}/");
        let snap = format!("{name}@");
        let has_children = s.datasets.keys().any(|d| d.starts_with(&child))
            || s.snapshots.iter().any(|d| d.starts_with(&snap));
        if has_children && !recursive {
            return Err(fail(
                "zfs destroy",
                format!("cannot destroy '{name}': filesystem has children"),
            ));
        }
        s.datasets
            .retain(|d, _| d != name && !d.starts_with(&child));
        s.snapshots
            .retain(|d| !d.starts_with(&snap) && !d.starts_with(&child));
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

    fn unmount(&self, path: &str) -> Result<()> {
        match self.state().mounts.remove(path) {
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
                Ipv4Addr::new(10, 9, 9, 9)
            )
            .is_err(),
            "gateway off-link"
        );
        sys.route_add(
            Some("infra1"),
            Destination::Default,
            Ipv4Addr::new(10, 200, 3, 1),
        )
        .unwrap();
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

        let ip = |n| Ipv4Addr::new(10, 200, 3, n);
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
}
