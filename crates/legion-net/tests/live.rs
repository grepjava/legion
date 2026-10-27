//! The pod network on FreeBSD itself. It changes interfaces and routes, so
//! it refuses to run outside a VNET jail; run it in a throwaway one:
//!
//! ```sh
//! jail -c name=lnet path=/ vnet persist children.max=4 allow.raw_sockets
//! jexec lnet target/debug/deps/live-<hash> --ignored
//! jail -r lnet
//! ```

use std::net::Ipv4Addr;
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
    ] {
        let (ok, out) = sh(&cmd);
        assert!(ok, "{cmd}: {out}");
    }
    let (_, route) = sh(&format!("route -j {a} -n get default"));
    assert!(route.contains("gateway: 10.200.7.1"), "{route}");
    let (_, mtu) = sh(&format!("ifconfig -j {a} -l"));
    assert!(mtu.contains("epair"), "{mtu}");

    // A service VIP on this jail's pf, with Legion's anchors hooked in.
    assert!(sh("sysctl net.inet.ip.forwarding=1").0);
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
    let to_server = |sys: &Shell| {
        sys.pf_states()
            .unwrap()
            .into_iter()
            .filter(|s| s.addrs.contains(&server))
            .count()
    };
    assert!(to_server(&sys) >= 2, "{:?}", sys.pf_states());
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
