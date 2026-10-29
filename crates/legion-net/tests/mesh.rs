//! The mesh and services across nodes, on FreeBSD itself. Three VNET jails
//! are the nodes and run Legion's network code through `jexec`; their pods
//! are child jails. A fourth jail joins the nodes' links like a switch and
//! is a client outside the cluster. Node 3 is behind NAT: its WireGuard
//! traffic leaves from another port, and nothing reaches the one it
//! advertises. Run as root on the host (it touches
//! nothing outside its jails, named `lmesh-*`, which it removes):
//!
//! ```sh
//! kldload -n if_bridge if_epair if_wg pf
//! target/debug/deps/mesh-<hash> --ignored --nocapture
//! ```

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::process::Command;
use std::time::Duration;

use legion_net::PodNetwork;
use legion_net::lb::{self, Exits, LoadBalancer, Port, Proto, Service};
use legion_net::mesh::{self, Mesh, Peer};
use legion_sys::{CommandOutput, Host, Ipv4Net, Runner, Shell, Sys};

const SWITCH: &str = "lmesh-sw";
const NODES: [&str; 3] = ["lmesh-n1", "lmesh-n2", "lmesh-n3"];
const VIP: Ipv4Addr = Ipv4Addr::new(10, 201, 0, 1);
const VIP6: &str = "fd00:10:201::1";
/// The nodes' LAN, for IPv6 (`192.168.252.0/24` for IPv4).
const LAN6: &str = "fd00:192:168:252";
const PUBLISHED: u16 = 30080;

fn sh(cmd: &str) -> (bool, String) {
    let out = Command::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    )
}

fn must(cmd: &str) -> String {
    let (ok, out) = sh(cmd);
    assert!(ok, "{cmd}: {out}");
    out
}

/// Runs programs inside a jail.
struct Jexec(String);

impl Runner for Jexec {
    fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> std::io::Result<CommandOutput> {
        let mut a = vec![self.0.clone(), program.to_owned()];
        a.extend_from_slice(args);
        Host.run("jexec", &a, stdin)
    }
}

/// Removes the lab's jails and links, however the test ends.
struct Lab {
    /// Each node's link to the switch, by the node's end.
    links: Vec<(String, String)>,
}

impl Drop for Lab {
    fn drop(&mut self) {
        // Interfaces moved into a jail come back to the host only once the
        // jail is gone, which can be a minute after its removal (until its
        // connections time out), so the links go first, from where they
        // are. Interfaces made inside a jail go with it.
        for (jail, link) in &self.links {
            if !sh(&format!("jexec {jail} ifconfig {link} destroy")).0 {
                sh(&format!("ifconfig {link} destroy"));
            }
        }
        // Removing a node removes its pods.
        for j in NODES.iter().chain([&SWITCH]) {
            sh(&format!("jail -r {j}"));
        }
    }
}

/// A node: its jail, Legion's view of its system, its pod network.
struct Node {
    jail: String,
    sys: Shell<Jexec>,
    net: PodNetwork,
    peer: Peer,
    mesh: Mesh,
}

impl Node {
    /// Runs `cmd` in pod `pod` of this node.
    fn pod(&self, pod: &str, cmd: &str) -> (bool, String) {
        sh(&format!("jexec {} jexec {pod} {cmd}", self.jail))
    }

    fn balance(&self, lb: &mut LoadBalancer, services: &[Service], mesh: bool) {
        let egress = self.sys.default_interface().unwrap();
        let published: Vec<String> = egress.iter().cloned().collect();
        let exits = Exits {
            egress: egress.as_deref(),
            mesh: mesh.then_some(mesh::INTERFACE),
            published: &published,
        };
        lb.apply(&self.sys, services, self.net.subnet, exits)
            .unwrap();
    }
}

/// What a few connections to `addr port` get back, from `client` (a
/// command prefix): the server names, or `None` for a failure.
fn connect(client: &str, addr: &str, port: u16, times: usize) -> Vec<Option<String>> {
    (0..times)
        .map(|_| {
            let (ok, out) = sh(&format!("{client} nc -N -w 3 {addr} {port} </dev/null"));
            let out = out.trim().to_owned();
            (ok && !out.is_empty()).then_some(out)
        })
        .collect()
}

fn names(got: &[Option<String>]) -> BTreeSet<&str> {
    got.iter().flatten().map(String::as_str).collect()
}

#[test]
#[ignore = "needs root on a FreeBSD host"]
fn pods_across_nodes() {
    assert_eq!(must("id -u").trim(), "0", "run as root");
    assert_eq!(
        must("sysctl -n security.jail.jailed").trim(),
        "0",
        "run on the host: the nodes are jails"
    );
    for j in NODES.iter().chain([&SWITCH]) {
        assert!(!sh(&format!("jls -j {j}")).0, "{j} exists already");
    }
    let mut lab = Lab { links: Vec::new() };

    // The switch: a bridge, and the outside client's address on it.
    must(&format!(
        "jail -c name={SWITCH} path=/ vnet persist allow.raw_sockets"
    ));
    let bridge = must(&format!("jexec {SWITCH} ifconfig bridge create"));
    let bridge = bridge.trim();
    must(&format!(
        "jexec {SWITCH} ifconfig {bridge} inet 192.168.252.254/24 up"
    ));
    must(&format!(
        "jexec {SWITCH} ifconfig {bridge} inet6 -ifdisabled no_dad"
    ));
    must(&format!(
        "jexec {SWITCH} ifconfig {bridge} inet6 {LAN6}::fe/64"
    ));

    // The nodes, each on a link to the switch, as on a LAN.
    let mut nodes = Vec::new();
    for (i, jail) in NODES.iter().enumerate() {
        let n = i + 1;
        must(&format!(
            "jail -c name={jail} path=/ vnet persist children.max=8 allow.raw_sockets"
        ));
        // Renamed: epair units are numbered per network stack, so the node's
        // own epairs (for its pods) would collide with an `epair<n>a` moved
        // in.
        let created = must("ifconfig epair create");
        let created = created.trim();
        let (a, b) = (format!("lmul{n}"), format!("lmsw{n}"));
        must(&format!("ifconfig {created} name {a}"));
        lab.links.push((jail.to_string(), a.clone()));
        must(&format!(
            "ifconfig {}b name {b}",
            created.strip_suffix('a').unwrap()
        ));
        must(&format!("ifconfig {a} vnet {jail}"));
        must(&format!("ifconfig {b} vnet {SWITCH}"));
        must(&format!("jexec {SWITCH} ifconfig {b} up"));
        must(&format!("jexec {SWITCH} ifconfig {bridge} addm {b}"));
        must(&format!("jexec {jail} ifconfig lo0 up"));
        must(&format!(
            "jexec {jail} ifconfig {a} inet 192.168.252.{n}/24 up"
        ));
        must(&format!(
            "jexec {jail} ifconfig {a} inet6 -ifdisabled no_dad"
        ));
        must(&format!("jexec {jail} ifconfig {a} inet6 {LAN6}::{n}/64"));
        must(&format!(
            "jexec {jail} route -q add default 192.168.252.254"
        ));
        must(&format!("jexec {jail} sysctl net.inet.ip.forwarding=1"));
        must(&format!("jexec {jail} sysctl net.inet6.ip6.forwarding=1"));
        // The node's own pf, with Legion's anchors hooked in. Node 3's
        // also stands for a NAT in front of it: WireGuard leaves from port
        // 61000, and only answers come back to it.
        let mut hooks = lb::HOOKS.join("\n") + "\npass all\n";
        if n == 3 {
            hooks = format!(
                "nat on {a} inet proto udp from ({a}) port {port} to any -> ({a}) port 61000\n\
                 {hooks}block in quick on {a} proto udp to port {{ {port} 61000 }}\n",
                port = mesh::PORT
            );
        }
        let (ok, out) = sh(&format!(
            "printf '{}' | jexec {jail} pfctl -f -",
            hooks.replace('"', "\\\"")
        ));
        assert!(ok, "{out}");
        sh(&format!("jexec {jail} pfctl -e"));

        let sys = Shell::with_runner(Jexec(jail.to_string()));
        let subnet: Ipv4Net = format!("10.200.{n}.0/24").parse().unwrap();
        let net = PodNetwork::new(subnet);
        net.ensure_bridge(&sys).unwrap();
        let key = sys.wg_genkey().unwrap();
        let mesh = Mesh {
            subnet,
            private_key: key.clone(),
            port: mesh::PORT,
        };
        mesh.ensure(&sys).unwrap();
        mesh.ensure(&sys).unwrap(); // again: fine
        let peer = Peer {
            public_key: sys.wg_pubkey(&key).unwrap(),
            endpoint: Some(format!("192.168.252.{n}:{}", mesh::PORT)),
            subnet,
        };
        nodes.push(Node {
            jail: jail.to_string(),
            sys,
            net,
            peer,
            mesh,
        });
    }
    let peers: Vec<Peer> = nodes.iter().map(|n| n.peer.clone()).collect();
    for n in &nodes {
        n.mesh.apply(&n.sys, &peers, &[]).unwrap();
        n.mesh.apply(&n.sys, &peers, &peers).unwrap(); // nothing changes
        assert_eq!(n.sys.wg_peers(mesh::INTERFACE).unwrap().len(), 2);
    }
    // Node 3 behind NAT is reached once it has reached the others, from
    // its NAT's port.
    let n3 = "100.64.3.1";
    for n in &nodes[..2] {
        let reached = (0..30).any(|_| sh(&format!("jexec {} ping -c1 -t1 {n3}", n.jail)).0);
        assert!(reached, "{} reaches node 3", n.jail);
        let wg = must(&format!(
            "jexec {} wg show {} endpoints",
            n.jail,
            mesh::INTERFACE
        ));
        assert!(wg.contains("192.168.252.3:61000"), "{wg}");
    }

    // Pods: a client and a server on node 1, a server on node 2, none on
    // node 3. Servers answer with their name.
    let mut ips = std::collections::BTreeMap::new();
    for (node, pod, id) in [
        (0, "c", "00000000000000c1"),
        (0, "w1", "00000000000000a1"),
        (1, "w2", "00000000000000a2"),
    ] {
        let n = &nodes[node];
        must(&format!(
            "jexec {} jail -c name={pod} path=/ vnet persist allow.raw_sockets",
            n.jail
        ));
        let used = ips.values().copied().collect();
        let ip = n.net.allocate(&used).unwrap();
        n.net.attach(&n.sys, id, pod, ip).unwrap();
        ips.insert(pod, ip);
        if pod != "c" {
            // Background processes must not hold on to sh()'s pipes.
            for family in ["-4", "-6"] {
                n.pod(
                    pod,
                    &format!(
                        "sh -c 'while :; do echo {pod} | nc {family} -N -l 8080; done >/dev/null 2>&1 </dev/null &'"
                    ),
                );
            }
        }
    }
    std::thread::sleep(Duration::from_millis(500));
    let (c, w2) = (ips["c"], ips["w2"]);

    // Pod to pod across nodes, and node to node over the underlay.
    let (ok, out) = nodes[0].pod("c", &format!("ping -c2 -t3 {w2}"));
    assert!(ok, "c to w2: {out}");
    let (ok, out) = sh(&format!("jexec {} ping -c1 -t3 100.64.3.1", nodes[1].jail));
    assert!(ok, "node 2 to node 3: {out}");
    let (ok, out) = sh(&format!("jexec {} ping -c1 -t3 {c}", nodes[2].jail));
    assert!(ok, "node 3 to c: {out}");
    // The same over IPv6.
    let (c6, w2_6) = (nodes[0].net.twin(c), nodes[1].net.twin(w2));
    let (ok, out) = nodes[0].pod("c", &format!("ping -6 -c2 -t3 {w2_6}"));
    assert!(ok, "c to w2 over IPv6: {out}");
    let (ok, out) = sh(&format!(
        "jexec {} ping -6 -c1 -t3 fd00:100:64::3:1",
        nodes[1].jail
    ));
    assert!(ok, "node 2 to node 3 over IPv6: {out}");
    let (ok, out) = sh(&format!("jexec {} ping -6 -c1 -t3 {c6}", nodes[2].jail));
    assert!(ok, "node 3 to c over IPv6: {out}");

    // A service with a task on each of nodes 1 and 2, balanced on every
    // node.
    let web = Service {
        uid: "00000000000000aa".into(),
        vip: Some(VIP),
        ports: vec![Port {
            proto: Proto::Tcp,
            port: 80,
            target: 8080,
            published: Some(PUBLISHED),
        }],
        ready: vec![ips["w1"], w2],
        alive: vec![ips["w1"], w2],
    };
    let mut lbs: Vec<LoadBalancer> = nodes.iter().map(|_| LoadBalancer::default()).collect();
    for (n, lb) in nodes.iter().zip(&mut lbs) {
        n.balance(lb, std::slice::from_ref(&web), true);
    }

    // Through the VIP, the client reaches the local and the remote task.
    let client = format!("jexec {} jexec c", nodes[0].jail);
    let got = connect(&client, &VIP.to_string(), 80, 6);
    assert_eq!(names(&got), ["w1", "w2"].into(), "{got:?}");
    assert!(got.iter().all(Option::is_some), "{got:?}");
    let got = connect(&client, VIP6, 80, 6);
    assert_eq!(names(&got), ["w1", "w2"].into(), "over IPv6: {got:?}");
    assert!(got.iter().all(Option::is_some), "over IPv6: {got:?}");

    // From outside, the published port on node 3, which runs no task,
    // reaches both: the replies come back through node 3.
    let outside = format!("jexec {SWITCH}");
    let got = connect(&outside, "192.168.252.3", PUBLISHED, 6);
    assert_eq!(names(&got), ["w1", "w2"].into(), "{got:?}");
    assert!(got.iter().all(Option::is_some), "{got:?}");
    let got = connect(&outside, &format!("{LAN6}::3"), PUBLISHED, 6);
    assert_eq!(names(&got), ["w1", "w2"].into(), "over IPv6: {got:?}");
    assert!(got.iter().all(Option::is_some), "over IPv6: {got:?}");
    // Without the NAT on wg0 the remote tasks answer the client directly,
    // from the wrong address, and nothing connects.
    let mut bare = LoadBalancer::default();
    nodes[2].balance(&mut bare, std::slice::from_ref(&web), false);
    let got = connect(&outside, "192.168.252.3", PUBLISHED, 2);
    assert!(got.iter().all(Option::is_none), "{got:?}");
    // (A new balancer: the old one would trust the rules it loaded.)
    nodes[2].balance(
        &mut LoadBalancer::default(),
        std::slice::from_ref(&web),
        true,
    );
    let got = connect(&outside, "192.168.252.3", PUBLISHED, 2);
    assert!(got.iter().all(Option::is_some), "{got:?}");

    // Node 2 leaves the mesh: its pods are out of reach; back, they are
    // reachable again.
    let without: Vec<Peer> = peers
        .iter()
        .filter(|p| p.subnet != nodes[1].peer.subnet)
        .cloned()
        .collect();
    nodes[0]
        .mesh
        .apply(&nodes[0].sys, &without, &peers)
        .unwrap();
    assert_eq!(nodes[0].sys.wg_peers(mesh::INTERFACE).unwrap().len(), 1);
    let (ok, _) = nodes[0].pod("c", &format!("ping -c1 -t2 {w2}"));
    assert!(!ok, "w2 is still reachable without its node as a peer");
    // The list changed, and node 3 behind NAT is still reached: the
    // endpoint learned stands over the one it advertises.
    let (ok, out) = sh(&format!("jexec {} ping -c1 -t2 {n3}", nodes[0].jail));
    assert!(ok, "node 3 after the peers changed: {out}");
    nodes[0]
        .mesh
        .apply(&nodes[0].sys, &peers, &without)
        .unwrap();
    let (ok, out) = nodes[0].pod("c", &format!("ping -c2 -t3 {w2}"));
    assert!(ok, "c to w2 again: {out}");
    let (ok, out) = sh(&format!("jexec {} ping -c1 -t2 {n3}", nodes[0].jail));
    assert!(ok, "node 3 again: {out}");

    drop(lab);
    let left = must("ifconfig -l");
    assert!(
        !left
            .split_whitespace()
            .any(|i| i.starts_with("epair") || i.starts_with("lp") || i.starts_with("wg")),
        "{left}"
    );
}
