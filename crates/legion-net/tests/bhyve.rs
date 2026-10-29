//! A bhyve guest as a pod, the spike for bhyve tasks: a small FreeBSD
//! guest boots from a ZFS volume, joins the pod bridge on a tap, and
//! Legion's service and policy rules are checked against it as against the
//! jail pod beside it. It changes interfaces and pf, so it refuses to run
//! outside a VNET jail, which must also allow vmm:
//!
//! ```sh
//! sh crates/legion-net/tests/bhyve-guest.sh /var/tmp/guest.img
//! zfs create -V 96m -o volmode=dev <pool>/guest
//! dd if=/var/tmp/guest.img of=/dev/zvol/<pool>/guest bs=1m
//! jail -c name=lbhyve path=/ vnet persist children.max=4 allow.raw_sockets allow.vmm
//! jexec lbhyve env LEGION_BHYVE_DISK=/dev/zvol/<pool>/guest LEGION_BHYVE_LAB=<dir> \
//!     <absolute path>/target/debug/deps/bhyve-<hash> --ignored --nocapture
//! jail -r lbhyve
//! ```
//!
//! The guest takes its addresses from loader variables (`bhyveload -e`),
//! serves on 8080 and probes a VIP every second, logging to its console,
//! `<lab>/console.log`.

use std::collections::BTreeMap;
use std::fs::File;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use legion_net::lb::{self, Exits, LoadBalancer, Port, Proto, Service};
use legion_net::policy::{self, Allow, Guard, Policy};
use legion_net::{BRIDGE, MTU, PodNetwork, host_interface};
use legion_sys::{Shell, Sys};

fn sh(cmd: &str) -> (bool, String) {
    let out = Command::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    )
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

/// The guest, destroyed when dropped.
struct Guest {
    name: String,
    bhyve: Option<Child>,
    console: PathBuf,
}

impl Drop for Guest {
    fn drop(&mut self) {
        sh(&format!("bhyvectl --vm={} --destroy", self.name));
        if let Some(mut bhyve) = self.bhyve.take() {
            let _ = bhyve.kill();
            let _ = bhyve.wait();
        }
    }
}

impl Guest {
    fn console(&self) -> String {
        std::fs::read_to_string(&self.console).unwrap_or_default()
    }

    /// Waits for the console to say `what`.
    fn says(&self, what: &str, secs: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while !self.console().contains(what) {
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        true
    }

    /// Waits until the guest's last three `probe` results logged after
    /// `from` (a length of the console) are all `want`.
    fn probes(&self, probe: &str, from: usize, want: &str) -> bool {
        let tag = format!("legion-guest: {probe} ");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let console = self.console();
            let got: Vec<&str> = console
                .get(from..)
                .unwrap_or_default()
                .lines()
                .filter_map(|l| l.strip_prefix(&*tag))
                .map(|l| l.split_once(' ').map_or("", |(_, got)| got))
                .collect();
            if got.len() >= 3 && got[got.len() - 3..].iter().all(|g| *g == want) {
                return true;
            }
            if Instant::now() > deadline {
                eprintln!("{}", console.get(from..).unwrap_or_default());
                return false;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

#[test]
#[ignore = "needs root and bhyve inside a throwaway VNET jail"]
fn guest_on_the_bridge() {
    assert_eq!(
        sh("sysctl -n security.jail.jailed").1.trim(),
        "1",
        "run inside a VNET jail"
    );
    let disk = env("LEGION_BHYVE_DISK");
    let lab = PathBuf::from(env("LEGION_BHYVE_LAB"));
    std::fs::create_dir_all(&lab).unwrap();
    let sys = Shell::new();
    let net = PodNetwork::new("10.200.9.0/24".parse().unwrap());
    net.ensure_bridge(&sys).unwrap();
    assert!(sh("sysctl net.inet.ip.forwarding=1").0);
    assert!(sh("sysctl net.inet6.ip6.forwarding=1").0);
    assert!(sh("sysctl net.link.bridge.pfil_member=1").0);
    assert!(sh("sysctl net.link.bridge.pfil_local_phys=1").0);
    sh("pfctl -e");
    let hooks = lb::HOOKS.join("\n") + "\n";
    assert!(
        sh(&format!(
            "printf '{}' | pfctl -f -",
            hooks.replace('"', "\\\"")
        ))
        .0
    );

    // A jail pod, serving on 8080.
    let pid = std::process::id();
    let (pod, pod_id) = (format!("lbhyvepod{pid}"), "00000000000000e1");
    assert!(sh(&format!("jail -c name={pod} path=/ vnet persist")).0);
    let pod_ip = net.allocate(&Default::default()).unwrap();
    net.attach(&sys, pod_id, &pod, pod_ip).unwrap();
    for family in ["-4", "-6"] {
        sh(&format!(
            "jexec {pod} sh -c 'while :; do echo from-the-pod | nc {family} -N -l 8080 >/dev/null 2>&1; done &' </dev/null >/dev/null 2>&1"
        ));
    }

    // The guest, on a tap in the bridge; its host end takes the place of
    // a pod's epair.
    let guest_ip = net.allocate(&[pod_ip].into()).unwrap();
    let (ok, tap) = sh("ifconfig tap create");
    assert!(ok, "{tap}");
    let tap = tap.trim().to_owned();
    net.join(&sys, &tap).unwrap();
    let (to_guest, to_pod) = (Ipv4Addr::new(10, 201, 0, 11), Ipv4Addr::new(10, 201, 0, 12));
    let vars = [
        ("ip", guest_ip.to_string()),
        ("prefix", net.subnet.prefix.to_string()),
        ("gateway", net.gateway().to_string()),
        ("ip6", net.twin(guest_ip).to_string()),
        ("gateway6", net.gateway6().to_string()),
        ("mtu", MTU.to_string()),
        ("probe", format!("{to_pod} 80")),
        ("probe6", format!("{} 80", legion_net::v6(to_pod).unwrap())),
    ];
    let console = lab.join("console.log");
    let started = Instant::now();
    let mut load = Command::new("bhyveload");
    load.args(["-m", "256M", "-d", &disk]);
    for (k, v) in &vars {
        load.arg("-e").arg(format!("legion.{k}={v}"));
    }
    let name = format!("lbhyve{pid}");
    let log = File::create(&console).unwrap();
    let status = load
        .arg(&name)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log.try_clone().unwrap())
        .status()
        .unwrap();
    let mut guest = Guest {
        name: name.clone(),
        bhyve: None,
        console: console.clone(),
    };
    assert!(status.success(), "bhyveload: {}", guest.console());
    guest.bhyve = Some(
        Command::new("bhyve")
            .args(["-c", "1", "-m", "256M", "-H", "-P"])
            .args(["-s", "0,hostbridge", "-s", "31,lpc", "-l", "com1,stdio"])
            .args(["-s", &format!("2,virtio-net,{tap}")])
            .args(["-s", &format!("3,virtio-blk,{disk}")])
            .arg(&name)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    assert!(
        guest.says("legion-guest: up", 120),
        "the guest did not come up: {}",
        guest.console()
    );
    eprintln!("the guest was up {:?} after bhyveload", started.elapsed());

    let services = [
        Service {
            uid: "00000000000000e2".into(),
            vip: Some(to_guest),
            ports: vec![Port {
                proto: Proto::Tcp,
                port: 80,
                target: 8080,
                published: None,
            }],
            ready: vec![guest_ip],
            alive: vec![guest_ip],
        },
        Service {
            uid: "00000000000000e3".into(),
            vip: Some(to_pod),
            ports: vec![Port {
                proto: Proto::Tcp,
                port: 80,
                target: 8080,
                published: None,
            }],
            ready: vec![pod_ip],
            alive: vec![pod_ip],
        },
    ];
    let mut lb = LoadBalancer::default();
    lb.apply(&sys, &services, net.subnet, Exits::default())
        .unwrap();

    let guest6 = net.twin(guest_ip).to_string();
    let to_guest6 = legion_net::v6(to_guest).unwrap().to_string();
    let reach = |from: Option<&str>, to: &str, port: u16| {
        let cmd = match from {
            Some(j) => format!("jexec {j} nc -z -w 2 {to} {port}"),
            None => format!("nc -z -w 2 {to} {port}"),
        };
        sh(&cmd).0
    };
    let fetch = |to: &str| sh(&format!("jexec {pod} nc -w 2 -N {to} 80 </dev/null")).1;

    // Open: the host and the pod reach the guest, directly and through
    // its VIP; the guest reaches the pod through the pod's.
    for cmd in [
        format!("ping -c1 -t2 {guest_ip}"),
        format!("ping -6 -c1 -t2 {guest6}"),
        format!("jexec {pod} ping -c1 -t2 {guest_ip}"),
    ] {
        let (ok, out) = sh(&cmd);
        assert!(ok, "{cmd}: {out}");
    }
    assert!(reach(Some(&pod), &guest_ip.to_string(), 8080));
    assert!(reach(Some(&pod), &guest6, 8080));
    assert_eq!(fetch(&to_guest.to_string()).trim(), "from-the-guest");
    assert_eq!(fetch(&to_guest6).trim(), "from-the-guest");
    assert!(
        guest.probes("probe", 0, "from-the-pod"),
        "the guest to a VIP"
    );
    assert!(
        guest.probes("probe6", 0, "from-the-pod"),
        "the guest to a VIP6"
    );

    // Policy, with the tap as the guest's host end.
    let ips = |allowed: &[Ipv4Addr]| BTreeMap::from([("tfriends".to_owned(), allowed.to_vec())]);
    let guards = |vips: bool| {
        vec![
            Guard {
                interface: host_interface(pod_id),
                ip: pod_ip,
                open: false,
                allowed: vec![],
                vips: if vips {
                    vec![(to_guest, Proto::Tcp, 80)]
                } else {
                    vec![]
                },
                egress: None,
            },
            Guard {
                interface: tap.clone(),
                ip: guest_ip,
                open: false,
                allowed: vec![Allow {
                    table: "tfriends".into(),
                    ports: vec![(Proto::Tcp, 8080)],
                }],
                vips: if vips {
                    vec![(to_pod, Proto::Tcp, 80)]
                } else {
                    vec![]
                },
                egress: None,
            },
        ]
    };
    let mut p = Policy::default();
    let mut set = |allowed: &[Ipv4Addr], vips: bool| {
        p.apply(
            &sys,
            &guards(vips),
            &ips(allowed),
            net.gateway(),
            legion_net::SERVICE_CIDR,
        )
        .unwrap();
    };

    // Closed: the pod is refused at the guest, the guest at the VIP; the
    // host still reaches it.
    let mark = guest.console().len();
    set(&[], false);
    assert!(
        !reach(Some(&pod), &guest_ip.to_string(), 8080),
        "pod to guest"
    );
    assert!(!reach(Some(&pod), &guest6, 8080), "pod to guest over IPv6");
    assert!(
        !reach(Some(&pod), &to_guest.to_string(), 80),
        "pod to its VIP"
    );
    assert!(reach(None, &guest_ip.to_string(), 8080), "the host");
    assert!(
        guest.probes("probe", mark, "refused"),
        "the guest to a VIP, refused: {}",
        sh("pfctl -a legion/policy -vsr").1
    );
    assert!(guest.probes("probe6", mark, "refused"));
    // Allowed: the pod on 8080, each the other's VIP.
    let mark = guest.console().len();
    set(&[pod_ip], true);
    assert!(
        reach(Some(&pod), &guest_ip.to_string(), 8080),
        "pod to guest"
    );
    assert!(reach(Some(&pod), &guest6, 8080), "pod to guest over IPv6");
    assert_eq!(fetch(&to_guest.to_string()).trim(), "from-the-guest");
    assert_eq!(fetch(&to_guest6).trim(), "from-the-guest");
    assert!(guest.probes("probe", mark, "from-the-pod"));
    assert!(guest.probes("probe6", mark, "from-the-pod"));

    drop(guest);
    sys.pf_flush_anchor(policy::ANCHOR).unwrap();
    sys.pf_flush_anchor(lb::ANCHOR).unwrap();
    sys.interface_destroy(&tap).unwrap();
    net.detach(&sys, pod_id).unwrap();
    assert!(sh(&format!("jail -r {pod}")).0);
    sys.interface_destroy(BRIDGE).unwrap();
}
