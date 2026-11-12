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

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::process::Command;
use std::time::Duration;

use legion_net::lb::{self, Exits, LoadBalancer, Port, Proto, Service};
use legion_net::mesh::{self, Mesh, Peer};
use legion_net::policy::{self, Allow, Guard, Policy};
use legion_net::{PodNetwork, host_interface};
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
            balancer: None,
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

/// The pods' server, which the test runs in each server pod as this same
/// program: it answers every connection to port 8080, on both families,
/// with its name. Its listening sockets stay open throughout. (`nc -l` in
/// a loop does not do: it keeps its listening socket while it serves, and
/// a connection queued on it meanwhile is dropped when it exits.)
#[test]
#[ignore = "the pods' server, run by pods_across_nodes"]
fn serve() {
    use std::io::Write;
    let Ok(name) = std::env::var("LEGION_MESH_SERVE") else {
        return;
    };
    let listeners: Vec<_> = ["0.0.0.0:8080", "[::]:8080"]
        .into_iter()
        .map(|a| std::net::TcpListener::bind(a).unwrap())
        .collect();
    let threads: Vec<_> = listeners
        .into_iter()
        .map(|l| {
            let name = name.clone();
            std::thread::spawn(move || {
                for c in l.incoming() {
                    let Ok(mut c) = c else { continue };
                    let name = name.clone();
                    std::thread::spawn(move || {
                        let _ = writeln!(c, "{name}");
                        let _ = c.shutdown(std::net::Shutdown::Write);
                        let _ = std::io::copy(&mut c, &mut std::io::sink());
                    });
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
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
    // A node of a real cluster may run here too: its own interfaces stay.
    let before = must("ifconfig -l");
    let before: BTreeSet<&str> = before.split_whitespace().collect();
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
            nat: false,
        };
        mesh.ensure(&sys).unwrap();
        mesh.ensure(&sys).unwrap(); // again: fine
        let peer = Peer {
            public_key: sys.wg_pubkey(&key).unwrap(),
            endpoint: Some(format!("192.168.252.{n}:{}", mesh::PORT)),
            subnet,
            nat: false,
            relay: false,
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
    let mut ips = BTreeMap::new();
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
            // This program again, as the pod's server (see `serve`). The
            // pods share the host's filesystem, so its path holds there.
            // Background processes must not hold on to sh()'s pipes.
            let me = std::env::current_exe().unwrap();
            n.pod(
                pod,
                &format!(
                    "sh -c 'LEGION_MESH_SERVE={pod} {} --ignored --exact serve --test-threads=1 >/dev/null 2>&1 </dev/null &'",
                    me.display()
                ),
            );
            let listening = (0..100).any(|_| {
                let (_, out) = n.pod(pod, "sockstat -46l -P tcp -p 8080");
                if out.lines().filter(|l| l.contains(":8080")).count() == 2 {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(100));
                false
            });
            assert!(listening, "{pod} serves");
        }
    }
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
        address: None,
        carp: None,
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

    // w2's egress limited, its namespace open and then letting c in: it
    // still answers what reaches it over the mesh, from another node's pod
    // and from another node's own address, as published ports and ingress
    // proxies elsewhere do.
    let n2 = &nodes[1];
    for knob in ["pfil_member", "pfil_local_phys"] {
        must(&format!(
            "jexec {} sysctl net.link.bridge.{knob}=1",
            n2.jail
        ));
    }
    let w2_guard = |open: bool| Guard {
        interface: host_interface("00000000000000a2"),
        ip: w2,
        open,
        allowed: if open {
            vec![]
        } else {
            vec![Allow {
                table: "tc".into(),
                ports: vec![(Proto::Tcp, 8080)],
            }]
        },
        vips: vec![],
        egress: Some(vec![]),
    };
    let tables = BTreeMap::from([("tc".to_owned(), vec![c])]);
    let mut policy = Policy::default();
    let w2s = w2.to_string();
    for open in [true, false] {
        policy
            .apply(
                &n2.sys,
                &[w2_guard(open)],
                &tables,
                n2.net.gateway(),
                "10.201.0.0/16".parse().unwrap(),
            )
            .unwrap();
        let rules = || {
            sh(&format!(
                "jexec {} pfctl -a {} -vsr; jexec {} pfctl -ss",
                n2.jail,
                policy::ANCHOR,
                n2.jail
            ))
            .1
        };
        // Node 3 connects from its underlay address, as an ingress proxy
        // there does; its published port reaches w2 NATed to that address.
        for (family, to, published) in [
            ("", w2s.clone(), "192.168.252.3".to_owned()),
            (" over IPv6", w2_6.to_string(), format!("{LAN6}::3")),
        ] {
            let got = connect(&client, &to, 8080, 2);
            assert!(
                got.iter().all(|g| g.as_deref() == Some("w2")),
                "open {open}, from c{family}: {got:?}\n{}",
                rules()
            );
            let got = connect(&format!("jexec {}", nodes[2].jail), &to, 8080, 2);
            assert!(
                got.iter().all(|g| g.as_deref() == Some("w2")),
                "open {open}, from node 3{family}: {got:?}\n{}",
                rules()
            );
            let got = connect(&outside, &published, PUBLISHED, 6);
            assert!(
                got.iter().all(Option::is_some) && names(&got).contains("w2"),
                "open {open}, published on node 3{family}: {got:?}\n{}",
                rules()
            );
        }
        // Egress itself stays limited.
        let (ok, _) = n2.pod("w2", &format!("nc -z -w 2 {} 8080", ips["w1"]));
        assert!(!ok, "open {open}: w2 reached w1");
    }
    n2.sys.pf_flush_anchor(policy::ANCHOR).unwrap();

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
            .filter(|i| !before.contains(i))
            .any(|i| i.starts_with("epair") || i.starts_with("lp") || i.starts_with("wg")),
        "{left}"
    );
}

/// The jails and links of [`mesh_growth_by_nodes`], removed however it ends.
struct Growth {
    jails: Vec<String>,
    links: Vec<(String, String)>,
}

impl Drop for Growth {
    fn drop(&mut self) {
        for (jail, link) in &self.links {
            if !sh(&format!("jexec {jail} ifconfig {link} destroy")).0 {
                sh(&format!("ifconfig {link} destroy"));
            }
        }
        for j in self.jails.iter().rev() {
            sh(&format!("jail -r {j}"));
        }
    }
}

/// Measurement (PLAN.md phase 9): nodes join a mesh one at a time, VNET
/// jails on one switch as in [`pods_across_nodes`]. For each join: the new
/// node setting every peer, every other node adding it, and the time until
/// the new node has reached every other node over WireGuard.
#[test]
#[ignore = "needs root on a FreeBSD host"]
fn mesh_growth_by_nodes() {
    use std::time::Instant;
    const MOST: usize = 24;
    assert_eq!(must("id -u").trim(), "0", "run as root");
    assert_eq!(
        must("sysctl -n security.jail.jailed").trim(),
        "0",
        "run on the host: the nodes are jails"
    );
    let switch = "lmeshg-sw".to_owned();
    assert!(
        !sh(&format!("jls -j {switch}")).0,
        "{switch} exists already"
    );
    let mut lab = Growth {
        jails: vec![switch.clone()],
        links: Vec::new(),
    };
    must(&format!("jail -c name={switch} path=/ vnet persist"));
    let bridge = must(&format!("jexec {switch} ifconfig bridge create"));
    let bridge = bridge.trim().to_owned();
    must(&format!(
        "jexec {switch} ifconfig {bridge} inet 192.168.253.254/24 up"
    ));

    let ms = |d: Duration| format!("{:.0} ms", d.as_secs_f64() * 1000.0);
    println!(
        "| Nodes | New node's peers set | Others add it (each, slowest) | Every node reached |"
    );
    println!("|---|---|---|---|");
    let mut nodes: Vec<Node> = Vec::new();
    let mut peers: Vec<Peer> = Vec::new();
    for n in 1..=MOST {
        let jail = format!("lmeshg-n{n}");
        assert!(!sh(&format!("jls -j {jail}")).0, "{jail} exists already");
        must(&format!(
            "jail -c name={jail} path=/ vnet persist allow.raw_sockets"
        ));
        lab.jails.push(jail.clone());
        let created = must("ifconfig epair create");
        let created = created.trim();
        let (a, b) = (format!("lmgu{n}"), format!("lmgs{n}"));
        must(&format!("ifconfig {created} name {a}"));
        lab.links.push((jail.clone(), a.clone()));
        must(&format!(
            "ifconfig {}b name {b}",
            created.strip_suffix('a').unwrap()
        ));
        must(&format!("ifconfig {a} vnet {jail}"));
        must(&format!("ifconfig {b} vnet {switch}"));
        must(&format!("jexec {switch} ifconfig {b} up"));
        must(&format!("jexec {switch} ifconfig {bridge} addm {b}"));
        must(&format!("jexec {jail} ifconfig lo0 up"));
        must(&format!(
            "jexec {jail} ifconfig {a} inet 192.168.253.{n}/24 up"
        ));

        let sys = Shell::with_runner(Jexec(jail.clone()));
        let subnet: Ipv4Net = format!("10.200.{n}.0/24").parse().unwrap();
        let net = PodNetwork::new(subnet);
        net.ensure_bridge(&sys).unwrap();
        let key = sys.wg_genkey().unwrap();
        let mesh = Mesh {
            subnet,
            private_key: key.clone(),
            port: mesh::PORT,
            nat: false,
        };
        mesh.ensure(&sys).unwrap();
        let peer = Peer {
            public_key: sys.wg_pubkey(&key).unwrap(),
            endpoint: Some(format!("192.168.253.{n}:{}", mesh::PORT)),
            subnet,
            nat: false,
            relay: false,
        };
        let before = peers.clone();
        peers.push(peer.clone());

        // The new node, with every peer.
        let start = Instant::now();
        mesh.apply(&sys, &peers, &[]).unwrap();
        let own = start.elapsed();
        // Every other node, adding it.
        let mut slowest = Duration::ZERO;
        let start = Instant::now();
        for other in &nodes {
            let one = Instant::now();
            other.mesh.apply(&other.sys, &peers, &before).unwrap();
            slowest = slowest.max(one.elapsed());
        }
        let others = start.elapsed();
        // The new node reaches each other node's mesh address.
        let start = Instant::now();
        for other in &nodes {
            let to = mesh::underlay_addr(other.net.subnet).unwrap();
            let reached = (0..30).any(|_| sh(&format!("jexec {jail} ping -c1 -t1 {to}")).0);
            assert!(reached, "node {n} reaches {}", other.jail);
        }
        let reached = start.elapsed();
        assert_eq!(sys.wg_peers(mesh::INTERFACE).unwrap().len(), n - 1);
        if [2, 4, 8, 12, 16, 20, 24].contains(&n) {
            let each = others.checked_div(n as u32 - 1).unwrap_or_default();
            println!(
                "| {n} | {} | {}, {} | {} |",
                ms(own),
                ms(each),
                ms(slowest),
                ms(reached)
            );
        }
        nodes.push(Node {
            jail,
            sys,
            net,
            peer,
            mesh,
        });
    }
    drop(lab);
}

/// Two nodes each behind a NAT that lets no handshake in reach each other
/// only through a relay (`--mesh-nat`, `--mesh-relay`): VNET jails on one
/// switch, node 1 the relay, nodes 2 and 3 each behind a NAT as in
/// [`pods_across_nodes`]. Without the relay they never reach each other.
#[test]
#[ignore = "needs root on a FreeBSD host"]
fn nodes_behind_nat_reach_each_other_through_a_relay() {
    assert_eq!(must("id -u").trim(), "0", "run as root");
    assert_eq!(
        must("sysctl -n security.jail.jailed").trim(),
        "0",
        "run on the host: the nodes are jails"
    );
    let switch = "lmeshn-sw".to_owned();
    assert!(
        !sh(&format!("jls -j {switch}")).0,
        "{switch} exists already"
    );
    let mut lab = Growth {
        jails: vec![switch.clone()],
        links: Vec::new(),
    };
    must(&format!("jail -c name={switch} path=/ vnet persist"));
    let bridge = must(&format!("jexec {switch} ifconfig bridge create"));
    let bridge = bridge.trim().to_owned();
    must(&format!(
        "jexec {switch} ifconfig {bridge} inet 192.168.251.254/24 up"
    ));

    let mut nodes: Vec<Node> = Vec::new();
    for n in 1..=3u8 {
        let jail = format!("lmeshn-n{n}");
        must(&format!(
            "jail -c name={jail} path=/ vnet persist allow.raw_sockets"
        ));
        lab.jails.push(jail.clone());
        let created = must("ifconfig epair create");
        let created = created.trim();
        let (a, b) = (format!("lmnu{n}"), format!("lmns{n}"));
        must(&format!("ifconfig {created} name {a}"));
        lab.links.push((jail.clone(), a.clone()));
        must(&format!(
            "ifconfig {}b name {b}",
            created.strip_suffix('a').unwrap()
        ));
        must(&format!("ifconfig {a} vnet {jail}"));
        must(&format!("ifconfig {b} vnet {switch}"));
        must(&format!("jexec {switch} ifconfig {b} up"));
        must(&format!("jexec {switch} ifconfig {bridge} addm {b}"));
        must(&format!("jexec {jail} ifconfig lo0 up"));
        must(&format!(
            "jexec {jail} ifconfig {a} inet 192.168.251.{n}/24 up"
        ));
        // The relay forwards between its peers.
        must(&format!("jexec {jail} sysctl net.inet.ip.forwarding=1"));
        must(&format!("jexec {jail} sysctl net.inet6.ip6.forwarding=1"));
        // Nodes 2 and 3 each behind a NAT: WireGuard leaves from port
        // 61000, and nothing but answers comes back.
        let mut rules = "pass all\n".to_owned();
        if n > 1 {
            rules = format!(
                "nat on {a} inet proto udp from ({a}) port {port} to any -> ({a}) port 61000\n\
                 {rules}block in quick on {a} proto udp to port {{ {port} 61000 }}\n",
                port = mesh::PORT
            );
        }
        let (ok, out) = sh(&format!("printf '{rules}' | jexec {jail} pfctl -f -"));
        assert!(ok, "{out}");
        sh(&format!("jexec {jail} pfctl -e"));

        let sys = Shell::with_runner(Jexec(jail.clone()));
        let subnet: Ipv4Net = format!("10.200.{n}.0/24").parse().unwrap();
        let net = PodNetwork::new(subnet);
        net.ensure_bridge(&sys).unwrap();
        let key = sys.wg_genkey().unwrap();
        let mesh = Mesh {
            subnet,
            private_key: key.clone(),
            port: mesh::PORT,
            nat: false,
        };
        mesh.ensure(&sys).unwrap();
        let peer = Peer {
            public_key: sys.wg_pubkey(&key).unwrap(),
            endpoint: Some(format!("192.168.251.{n}:{}", mesh::PORT)),
            subnet,
            nat: false,
            relay: false,
        };
        nodes.push(Node {
            jail,
            sys,
            net,
            peer,
            mesh,
        });
    }
    let jails: Vec<String> = nodes.iter().map(|n| n.jail.clone()).collect();
    let addrs: Vec<Ipv4Addr> = nodes
        .iter()
        .map(|n| mesh::underlay_addr(n.net.subnet).unwrap())
        .collect();
    let reaches = |from: usize, to: usize, tries: usize| {
        (0..tries).any(|_| sh(&format!("jexec {} ping -c1 -t1 {}", jails[from], addrs[to])).0)
    };

    // Each node reaches the relay; without it, 2 and 3 never meet.
    let plain: Vec<Peer> = nodes.iter().map(|n| n.peer.clone()).collect();
    for n in &nodes {
        n.mesh.apply(&n.sys, &plain, &[]).unwrap();
    }
    assert!(reaches(1, 0, 20), "node 2 reaches the relay");
    assert!(reaches(2, 0, 20), "node 3 reaches the relay");
    assert!(!reaches(1, 2, 5), "nodes behind NAT do not meet directly");

    // Said behind NAT, with node 1 relaying: 2 and 3 meet through it.
    let flagged: Vec<Peer> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| Peer {
            nat: i > 0,
            relay: i == 0,
            ..n.peer.clone()
        })
        .collect();
    for (i, n) in nodes.iter_mut().enumerate() {
        n.mesh.nat = i > 0;
        n.mesh.apply(&n.sys, &flagged, &plain).unwrap();
    }
    assert!(reaches(1, 2, 20), "node 2 reaches node 3 through the relay");
    assert!(reaches(2, 1, 20), "and back");
    // Neither has the other as a peer: only the relay.
    for i in [1, 2] {
        let peers = nodes[i].sys.wg_peers(mesh::INTERFACE).unwrap();
        assert_eq!(peers.len(), 1, "node {}: {peers:?}", i + 1);
        assert_eq!(peers[0].public_key, nodes[0].peer.public_key);
    }
    drop(lab);
}
