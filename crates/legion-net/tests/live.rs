//! The pod network on FreeBSD itself. It changes interfaces and routes, so
//! it refuses to run outside a VNET jail; run it in a throwaway one, a
//! test at a time (they share the bridge):
//!
//! ```sh
//! jail -c name=lnet path=/ vnet persist children.max=4 allow.raw_sockets
//! jexec lnet target/debug/deps/live-<hash> --ignored --test-threads=1
//! jail -r lnet
//! ```

use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;

use legion_net::lb::{ANCHOR, Exits, LoadBalancer, Port, Proto, Service};
use legion_net::{BRIDGE, MTU, PodNetwork, host_interface};
use legion_sys::{Shell, Sys};

fn sh(cmd: &str) -> (bool, String) {
    let out = Command::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    )
}

#[test]
#[ignore = "needs root inside a throwaway VNET jail"]
fn pods_on_the_bridge() {
    assert_eq!(
        sh("sysctl -n security.jail.jailed").1.trim(),
        "1",
        "run inside a VNET jail"
    );
    let sys = Shell::new();
    let net = PodNetwork::new("10.200.7.0/24".parse().unwrap());
    net.ensure_bridge(&sys).unwrap();
    net.ensure_bridge(&sys).unwrap();
    let (_, bridge) = sh(&format!("ifconfig {BRIDGE}"));
    assert!(
        bridge.contains("inet 10.200.7.1 netmask 0xffffff00"),
        "{bridge}"
    );
    assert!(bridge.contains(&format!("mtu {MTU}")), "{bridge}");
    assert!(
        bridge.contains("inet6 fd00:10:200:7::1 prefixlen 64"),
        "{bridge}"
    );

    // Two pods, as infra jails with their own VNET.
    let pid = std::process::id();
    let pods = [
        (format!("lnetpod{pid}a"), "00000000000000a1"),
        (format!("lnetpod{pid}b"), "00000000000000b2"),
    ];
    let mut ips = Vec::new();
    for (jail, id) in &pods {
        assert!(sh(&format!("jail -c name={jail} path=/ vnet persist")).0);
        let used = ips.iter().copied().collect();
        let ip = net.allocate(&used).unwrap();
        net.attach(&sys, id, jail, ip).unwrap();
        net.attach(&sys, id, jail, ip).unwrap(); // already attached: nothing to do
        ips.push(ip);
    }
    assert_eq!(
        ips,
        [Ipv4Addr::new(10, 200, 7, 2), Ipv4Addr::new(10, 200, 7, 3)]
    );

    // Host to pod, pod to its loopback, pod to gateway, pod to pod across
    // the bridge.
    let (a, b) = (&pods[0].0, &pods[1].0);
    for cmd in [
        "ping -c1 -t2 10.200.7.2".to_owned(),
        format!("jexec {a} ping -c1 -t2 127.0.0.1"),
        format!("jexec {a} ping -c1 -t2 10.200.7.1"),
        format!("jexec {a} ping -c1 -t2 10.200.7.3"),
        format!("jexec {b} ping -c1 -t2 10.200.7.2"),
        // The same over IPv6, at once (no duplicate address detection).
        "ping -6 -c1 -t2 fd00:10:200:7::2".to_owned(),
        format!("jexec {a} ping -6 -c1 -t2 ::1"),
        format!("jexec {a} ping -6 -c1 -t2 fd00:10:200:7::1"),
        format!("jexec {a} ping -6 -c1 -t2 fd00:10:200:7::3"),
        format!("jexec {b} ping -6 -c1 -t2 fd00:10:200:7::2"),
    ] {
        let (ok, out) = sh(&cmd);
        assert!(ok, "{cmd}: {out}");
    }
    // However often a pod asks, the gateway is the bridge's address, not
    // its host end's (which the VIPs' rules would never see).
    let bridge_ether = sys.interface_ether(BRIDGE).unwrap();
    for _ in 0..5 {
        sh(&format!("jexec {a} arp -d 10.200.7.1"));
        assert!(sh(&format!("jexec {a} ping -c1 -t2 10.200.7.1")).0);
        let (_, arp) = sh(&format!("jexec {a} arp -n 10.200.7.1"));
        assert!(arp.contains(&bridge_ether), "{arp}");
    }
    let (_, route) = sh(&format!("route -j {a} -n get default"));
    assert!(route.contains("gateway: 10.200.7.1"), "{route}");
    let (_, route) = sh(&format!("route -j {a} -n get -inet6 default"));
    assert!(route.contains("gateway: fd00:10:200:7::1"), "{route}");
    let (_, mtu) = sh(&format!("ifconfig -j {a} -l"));
    assert!(mtu.contains("epair"), "{mtu}");

    // A service VIP on this jail's pf, with Legion's anchors hooked in.
    assert!(sh("sysctl net.inet.ip.forwarding=1").0);
    assert!(sh("sysctl net.inet6.ip6.forwarding=1").0);
    sh("pfctl -e");
    let hooks = legion_net::lb::HOOKS.join("\n") + "\n";
    let (ok, out) = sh(&format!(
        "printf '{}' | pfctl -f -",
        hooks.replace('"', "\\\"")
    ));
    assert!(ok, "{out}");
    assert!(legion_net::lb::missing_hooks(&sys.pf_main_rules().unwrap()).is_empty());
    let svc = |ready: &[Ipv4Addr], alive: &[Ipv4Addr]| Service {
        uid: "00000000000000aa".into(),
        vip: Some(Ipv4Addr::new(10, 201, 0, 1)),
        ports: vec![Port {
            proto: Proto::Tcp,
            port: 80,
            target: 8080,
            published: None,
        }],
        ready: ready.to_vec(),
        alive: alive.to_vec(),
    };
    let server = ips[1];
    let mut lb = LoadBalancer::default();
    lb.apply(
        &sys,
        &[svc(&[server], &[server])],
        net.subnet,
        Exits::default(),
    )
    .unwrap();
    // Pod a talks to pod b on the same bridge through the VIP: the
    // hairpin NAT brings the reply back through the host.
    // Background processes must not hold on to sh()'s output pipes.
    sh(&format!(
        "jexec {b} sh -c 'nc -l 8080 > /tmp/lnet-got 2>/dev/null </dev/null &'"
    ));
    std::thread::sleep(std::time::Duration::from_millis(500));
    sh(&format!(
        "jexec {a} sh -c '((echo through-the-vip; sleep 30) | nc 10.201.0.1 80) >/dev/null 2>&1 </dev/null &'"
    ));
    std::thread::sleep(std::time::Duration::from_secs(1));
    let (_, got) = sh(&format!("jexec {b} cat /tmp/lnet-got"));
    assert_eq!(got.trim(), "through-the-vip");
    // And through the VIP's IPv6 twin, to the server's.
    sh(&format!(
        "jexec {b} sh -c 'nc -6 -l 8080 > /tmp/lnet-got6 2>/dev/null </dev/null &'"
    ));
    std::thread::sleep(std::time::Duration::from_millis(500));
    sh(&format!(
        "jexec {a} sh -c '((echo through-the-vip6; sleep 30) | nc fd00:10:201::1 80) >/dev/null 2>&1 </dev/null &'"
    ));
    std::thread::sleep(std::time::Duration::from_secs(1));
    let (_, got) = sh(&format!("jexec {b} cat /tmp/lnet-got6"));
    assert_eq!(
        got.trim(),
        "through-the-vip6",
        "{}{}",
        sh("pfctl -a legion/svc -sn").1,
        sh("pfctl -ss").1
    );
    let server6 = IpAddr::V6(net.twin(server));
    let to_server = |sys: &Shell| {
        sys.pf_states()
            .unwrap()
            .into_iter()
            .filter(|s| s.addrs.contains(&server.into()) || s.addrs.contains(&server6))
            .count()
    };
    assert!(to_server(&sys) >= 4, "{:?}", sys.pf_states());
    // Not ready but running: out of the table, the connection stays.
    lb.apply(&sys, &[svc(&[], &[server])], net.subnet, Exits::default())
        .unwrap();
    assert!(
        sys.pf_table_show(ANCHOR, "s00000000000000aa")
            .unwrap()
            .is_empty()
    );
    assert!(to_server(&sys) >= 2);
    // Gone: its connections are killed.
    lb.apply(&sys, &[svc(&[], &[])], net.subnet, Exits::default())
        .unwrap();
    assert_eq!(to_server(&sys), 0, "{:?}", sys.pf_states());
    sys.pf_flush_anchor(ANCHOR).unwrap();

    // Detaching removes both ends, even with the pod's end still in its jail.
    net.detach(&sys, pods[0].1).unwrap();
    assert!(
        !sys.interfaces(None)
            .unwrap()
            .contains(&host_interface(pods[0].1))
    );
    assert_eq!(sys.interfaces(Some(a)).unwrap(), ["lo0"]);
    // A jail that goes away first returns its end to the host; detaching
    // then removes the pair.
    assert!(sh(&format!("jail -r {b}")).0);
    net.detach(&sys, pods[1].1).unwrap();
    assert!(sh(&format!("jail -r {a}")).0);
    let left = sys.interfaces(None).unwrap();
    assert!(
        !left
            .iter()
            .any(|i| i.starts_with("epair") || i.starts_with("lp")),
        "{left:?}"
    );
    sys.interface_destroy(BRIDGE).unwrap();
}

#[test]
#[ignore = "needs root inside a throwaway VNET jail"]
fn policy_on_the_bridge() {
    use std::collections::BTreeMap;

    use legion_net::policy::{self, Allow, Egress, Guard, Policy, Target};

    assert_eq!(
        sh("sysctl -n security.jail.jailed").1.trim(),
        "1",
        "run inside a VNET jail"
    );
    let sys = Shell::new();
    let net = PodNetwork::new("10.200.8.0/24".parse().unwrap());
    net.ensure_bridge(&sys).unwrap();
    let pid = std::process::id();
    let pods = [
        (format!("lpolpod{pid}a"), "00000000000000c1"),
        (format!("lpolpod{pid}b"), "00000000000000d2"),
    ];
    let mut ips = Vec::new();
    for (jail, id) in &pods {
        assert!(sh(&format!("jail -c name={jail} path=/ vnet persist")).0);
        let ip = net.allocate(&ips.iter().copied().collect()).unwrap();
        net.attach(&sys, id, jail, ip).unwrap();
        ips.push(ip);
    }
    let (a, b) = (&pods[0].0, &pods[1].0);
    let (a_ip, b_ip) = (ips[0], ips[1]);
    assert!(sh("sysctl net.inet.ip.forwarding=1").0);
    assert!(sh("sysctl net.inet6.ip6.forwarding=1").0);
    // pf must see bridged traffic on the members (FreeBSD 15 defaults to
    // not); this jail's VNET only.
    assert!(sh("sysctl net.link.bridge.pfil_member=1").0);
    // And on the member a frame for the host came in on, not only on the
    // bridge: before the VIP's translation there.
    assert!(sh("sysctl net.link.bridge.pfil_local_phys=1").0);
    sh("pfctl -e");
    let hooks = legion_net::lb::HOOKS.join("\n") + "\n";
    assert!(
        sh(&format!(
            "printf '{}' | pfctl -f -",
            hooks.replace('"', "\\\"")
        ))
        .0
    );
    // b serves on 8080, also behind a VIP.
    let vip = Ipv4Addr::new(10, 201, 0, 9);
    let mut lb = LoadBalancer::default();
    let svc = Service {
        uid: "00000000000000dd".into(),
        vip: Some(vip),
        ports: vec![Port {
            proto: Proto::Tcp,
            port: 80,
            target: 8080,
            published: None,
        }],
        ready: vec![b_ip],
        alive: vec![b_ip],
    };
    lb.apply(&sys, &[svc], net.subnet, Exits::default())
        .unwrap();
    for family in ["-4", "-6"] {
        sh(&format!(
            "jexec {b} sh -c 'while :; do nc {family} -l 8080 </dev/null >/dev/null 2>&1; done &' </dev/null >/dev/null 2>&1"
        ));
    }
    // And a on 9090, for replies while its egress is limited.
    sh(&format!(
        "jexec {a} sh -c 'while :; do nc -4 -l 9090 </dev/null >/dev/null 2>&1; done &' </dev/null >/dev/null 2>&1"
    ));
    std::thread::sleep(std::time::Duration::from_millis(500));
    let reach = |from: Option<&str>, to: &str, port: u16| {
        let cmd = match from {
            Some(j) => format!("jexec {j} nc -z -w 2 {to} {port}"),
            None => format!("nc -z -w 2 {to} {port}"),
        };
        sh(&cmd).0
    };
    let (b6, vip6) = (net.twin(b_ip).to_string(), "fd00:10:201::9");
    assert!(
        reach(Some(a), &b_ip.to_string(), 8080) && reach(Some(a), &b6, 8080),
        "open before any policy"
    );

    let services = "10.201.0.0/16".parse().unwrap();
    let guard = |allowed: &[Ipv4Addr], vips: bool, egress: Option<Vec<Egress>>| {
        let guards = vec![
            Guard {
                interface: host_interface(pods[0].1),
                ip: a_ip,
                open: false,
                allowed: vec![],
                vips: if vips {
                    vec![(vip, Proto::Tcp, 80)]
                } else {
                    vec![]
                },
                egress,
            },
            Guard {
                interface: host_interface(pods[1].1),
                ip: b_ip,
                open: false,
                allowed: vec![Allow {
                    table: "tfriends".into(),
                    ports: vec![(Proto::Tcp, 8080)],
                }],
                vips: vec![],
                egress: None,
            },
        ];
        let tables = BTreeMap::from([
            ("tfriends".to_owned(), allowed.to_vec()),
            ("tb".to_owned(), vec![b_ip]),
        ]);
        (guards, tables)
    };
    let mut p = Policy::default();
    let mut set = |allowed: &[Ipv4Addr], vips: bool, egress: Option<Vec<Egress>>| {
        let (guards, tables) = guard(allowed, vips, egress);
        p.apply(&sys, &guards, &tables, net.gateway(), services)
            .unwrap();
    };

    // Nobody allowed: a cannot reach b, directly or through the VIP; the
    // host (probes) still can.
    set(&[], false, None);
    assert!(
        !reach(Some(a), &b_ip.to_string(), 8080),
        "direct, refused: {}{}{}",
        sh("pfctl -a legion/policy -vsr").1,
        sh("pfctl -ss").1,
        sh("sysctl net.link.bridge").1
    );
    assert!(
        !reach(Some(a), &vip.to_string(), 80),
        "VIP, refused: {}{}",
        sh("pfctl -a legion/policy -vsr").1,
        sh("pfctl -ss").1
    );
    assert!(reach(None, &b_ip.to_string(), 8080), "the host");
    assert!(!reach(Some(a), &b6, 8080), "direct over IPv6, refused");
    assert!(!reach(Some(a), vip6, 80), "VIP over IPv6, refused");
    assert!(reach(None, &b6, 8080), "the host over IPv6");
    // a allowed on 8080: directly, yes (the reply passes a's own guard as
    // state of what a started); the VIP still closed at a.
    set(&[a_ip], false, None);
    assert!(reach(Some(a), &b_ip.to_string(), 8080), "direct, allowed");
    assert!(reach(Some(a), &b6, 8080), "direct over IPv6, allowed");
    assert!(!reach(Some(a), &vip.to_string(), 80), "VIP, refused at a");
    assert!(!reach(Some(a), vip6, 80), "VIP over IPv6, refused at a");
    // And the VIP opened at a: through it, with the hairpin NAT.
    set(&[a_ip], true, None);
    assert!(reach(Some(a), &vip.to_string(), 80), "VIP, allowed");
    assert!(
        reach(Some(a), vip6, 80),
        "VIP over IPv6, allowed: {}{}",
        sh("pfctl -a legion/policy -vsr").1,
        sh("pfctl -ss").1
    );
    // An address that is not a's own is refused, though allowed at b.
    let fake = Ipv4Addr::new(10, 200, 8, 50);
    set(&[a_ip, fake], true, None);
    let epair = sh(&format!("ifconfig -j {a} -l")).1;
    let epair = epair
        .split_whitespace()
        .find(|i| i.starts_with("epair"))
        .unwrap()
        .to_owned();
    assert!(sh(&format!("ifconfig -j {a} {epair} inet {fake}/32 alias")).0);
    assert!(
        !sh(&format!("jexec {a} nc -z -w 2 -s {fake} {b_ip} 8080")).0,
        "spoofed source"
    );
    let fake6 = net.twin(fake);
    assert!(
        sh(&format!(
            "ifconfig -j {a} {epair} inet6 {fake6}/128 alias no_dad"
        ))
        .0
    );
    assert!(
        !sh(&format!("jexec {a} nc -z -w 2 -s {fake6} {b6} 8080")).0,
        "spoofed IPv6 source"
    );
    assert!(reach(Some(a), &b6, 8080), "a's own IPv6 address still");

    // Egress limited at a, with b letting a in throughout: nothing but
    // DNS at first, while what reaches a still gets its replies.
    set(&[a_ip], false, Some(vec![]));
    assert!(!reach(Some(a), &b_ip.to_string(), 8080), "egress: none");
    assert!(!reach(Some(a), &b6, 8080), "egress over IPv6: none");
    assert!(reach(None, &a_ip.to_string(), 9090), "replies from a");
    // b's tasks on 8080, as a table: both families.
    let to_b = Egress {
        to: Target::Table("tb".into()),
        ports: vec![(Proto::Tcp, 8080)],
    };
    set(&[a_ip], false, Some(vec![to_b]));
    assert!(
        reach(Some(a), &b_ip.to_string(), 8080),
        "egress to b's tasks: {}",
        sh("pfctl -a legion/policy -vsr").1
    );
    assert!(reach(Some(a), &b6, 8080), "egress to b's tasks over IPv6");
    // b's address as a network: IPv4 only.
    let to_net = Egress {
        to: Target::Net(format!("{b_ip}/32").parse().unwrap()),
        ports: vec![],
    };
    set(&[a_ip], false, Some(vec![to_net]));
    assert!(
        reach(Some(a), &b_ip.to_string(), 8080),
        "egress to a network"
    );
    assert!(!reach(Some(a), &b6, 8080), "networks are IPv4 only");
    sys.pf_flush_anchor(policy::ANCHOR).unwrap();
    sys.pf_flush_anchor(ANCHOR).unwrap();
    for (jail, id) in &pods {
        net.detach(&sys, id).unwrap();
        assert!(sh(&format!("jail -r {jail}")).0);
    }
    sys.interface_destroy(BRIDGE).unwrap();
}
