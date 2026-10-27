//! [`Shell`] against FreeBSD itself. Ignored by default; all need root.
//!
//! `network_and_pf` changes interfaces, routes and pf, so it refuses to run
//! outside a VNET jail. Run it in a throwaway one:
//!
//! ```sh
//! jail -c name=lsys path=/ vnet persist children.max=4 allow.raw_sockets
//! jexec lsys target/debug/deps/live-<hash> --ignored network_and_pf
//! jail -r lsys
//! ```
//!
//! `rctl_on_child_jail` runs on the host (rctl is not available in jails);
//! it only touches jails it creates. `zfs_datasets` runs when
//! `LEGION_SYS_ZFS_ROOT` names a dataset it may create children under.

use std::net::Ipv4Addr;
use std::process::Command;

use legion_sys::{Destination, IfConfig, Ipv4Net, RctlRule, Resource, Shell, Sys};

fn sh(cmd: &str) -> String {
    let out = Command::new("/bin/sh").args(["-c", cmd]).output().unwrap();
    assert!(
        out.status.success(),
        "{cmd}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn jailed() -> bool {
    sh("sysctl -n security.jail.jailed").trim() == "1"
}

#[test]
#[ignore = "needs root inside a throwaway VNET jail"]
fn network_and_pf() {
    assert!(
        jailed(),
        "refusing to change the host's network; run inside a VNET jail"
    );
    let sys = Shell::new();
    let pod = format!("lsyspod{}", std::process::id());

    sys.bridge_create("lsys0").unwrap();
    assert!(
        sys.bridge_create("lsys0").is_err(),
        "a second bridge with the same name"
    );
    assert!(
        !sys.interfaces(None)
            .unwrap()
            .iter()
            .any(|i| i.starts_with("bridge")),
        "the failed attempt leaves no bridge behind"
    );
    sys.interface_configure(
        None,
        "lsys0",
        &IfConfig {
            addr: Some("10.200.3.1/24".parse().unwrap()),
            mtu: Some(1420),
            up: true,
        },
    )
    .unwrap();

    sh(&format!("jail -c name={pod} path=/ vnet persist"));
    let jail = sys
        .jails()
        .unwrap()
        .into_iter()
        .find(|j| j.name == pod)
        .expect("pod jail listed");
    assert_eq!(jail.vnet, "new");
    sys.jail_set(&pod, &[("children.max", "2")]).unwrap();
    let jail = sys
        .jails()
        .unwrap()
        .into_iter()
        .find(|j| j.name == pod)
        .unwrap();
    assert_eq!(jail.children_max, 2);

    let pair = sys.epair_create().unwrap();
    sys.interface_configure(
        None,
        &pair.a,
        &IfConfig {
            mtu: Some(1420),
            up: true,
            ..Default::default()
        },
    )
    .unwrap();
    sys.bridge_add_member("lsys0", &pair.a).unwrap();
    sys.interface_move(&pair.b, &pod).unwrap();
    assert!(!sys.interfaces(None).unwrap().contains(&pair.b));
    assert!(sys.interfaces(Some(&pod)).unwrap().contains(&pair.b));
    sys.interface_configure(
        Some(&pod),
        "lo0",
        &IfConfig {
            up: true,
            ..Default::default()
        },
    )
    .unwrap();
    sys.interface_configure(
        Some(&pod),
        &pair.b,
        &IfConfig {
            addr: Some("10.200.3.17/24".parse().unwrap()),
            mtu: Some(1420),
            up: true,
        },
    )
    .unwrap();
    sys.route_add(
        Some(&pod),
        Destination::Default,
        Ipv4Addr::new(10, 200, 3, 1),
    )
    .unwrap();
    sh("ping -c1 -t2 10.200.3.17");
    sh(&format!("jexec {pod} ping -c1 -t2 10.200.3.1"));

    // A network route on the host side, through the pod.
    let net = Destination::Net("10.250.0.0/16".parse::<Ipv4Net>().unwrap());
    sys.route_add(None, net, Ipv4Addr::new(10, 200, 3, 17))
        .unwrap();
    assert!(sh("route -n get 10.250.1.1").contains("gateway: 10.200.3.17"));
    sys.route_delete(None, net).unwrap();

    // pf: an anchor with a table, replaced and emptied.
    sh("pfctl -e 2>/dev/null || true");
    sh("echo 'anchor \"legion/*\"' | pfctl -f -");
    sys.pf_load_anchor("legion/svc", "pass in quick on lsys0 from <svc_test>\n")
        .unwrap();
    let ips = [Ipv4Addr::new(10, 200, 3, 17), Ipv4Addr::new(10, 200, 3, 18)];
    sys.pf_table_replace("legion/svc", "svc_test", &ips)
        .unwrap();
    assert_eq!(sys.pf_table_show("legion/svc", "svc_test").unwrap(), ips);
    sys.pf_table_replace("legion/svc", "svc_test", &ips[..1])
        .unwrap();
    assert_eq!(
        sys.pf_table_show("legion/svc", "svc_test").unwrap(),
        ips[..1]
    );
    assert!(
        sys.pf_table_show("legion/svc", "missing")
            .unwrap()
            .is_empty()
    );
    assert!(
        sys.pf_load_anchor("legion/svc", "this is not pf\n")
            .is_err()
    );
    sys.pf_flush_anchor("legion/svc").unwrap();
    assert!(
        sys.pf_table_show("legion/svc", "svc_test")
            .unwrap()
            .is_empty()
    );

    // Removing the jail returns the b end; destroying a removes both.
    sh(&format!("jail -r {pod}"));
    assert!(sys.interfaces(None).unwrap().contains(&pair.b));
    sys.interface_destroy(&pair.a).unwrap();
    let left = sys.interfaces(None).unwrap();
    assert!(
        !left.contains(&pair.a) && !left.contains(&pair.b),
        "{left:?}"
    );
    sys.interface_destroy("lsys0").unwrap();
    assert!(sys.interface_destroy("lsys0").is_err());
}

#[test]
#[ignore = "needs root on the host"]
fn rctl_on_child_jail() {
    assert!(!jailed(), "rctl is only available on the host");
    let sys = Shell::new();
    let parent = format!("lsysrctl{}", std::process::id());
    let child = format!("{parent}.c1");
    sh(&format!(
        "jail -c name={parent} path=/ persist children.max=1"
    ));
    sh(&format!("jexec {parent} jail -c name=c1 path=/ persist"));

    for (resource, amount) in [
        (Resource::MemoryUse, 512 << 20),
        (Resource::MaxProc, 50),
        (Resource::Pcpu, 50),
    ] {
        sys.rctl_add(&RctlRule {
            jail: child.clone(),
            resource,
            amount,
        })
        .unwrap();
    }
    let rules = sh(&format!("rctl jail:{child}"));
    assert!(
        rules.contains(&format!("jail:{child}:memoryuse:sigkill=536870912")),
        "{rules}"
    );
    assert!(
        rules.contains(&format!("jail:{child}:maxproc:deny=50")),
        "{rules}"
    );
    assert!(
        rules.contains(&format!("jail:{child}:pcpu:deny=50")),
        "{rules}"
    );

    let usage = sys.rctl_usage(&child).unwrap();
    assert!(
        usage.contains_key("memoryuse") && usage.contains_key("maxproc"),
        "{usage:?}"
    );

    sys.rctl_remove_jail(&child).unwrap();
    assert!(sh(&format!("rctl jail:{child}")).trim().is_empty());
    sys.rctl_remove_jail(&child).unwrap(); // nothing left: still fine

    sh(&format!("jexec {parent} jail -r c1"));
    sh(&format!("jail -r {parent}"));
}

#[test]
#[ignore = "needs root and LEGION_SYS_ZFS_ROOT"]
fn zfs_datasets() {
    let Ok(root) = std::env::var("LEGION_SYS_ZFS_ROOT") else {
        eprintln!("LEGION_SYS_ZFS_ROOT not set; skipping");
        return;
    };
    let sys = Shell::new();
    let base = format!("{root}/lsys{}", std::process::id());
    let vol = format!("{base}/volumes/shop/data");
    sys.zfs_create(&vol, &[("quota", "10737418240")]).unwrap();
    sys.zfs_create(&vol, &[]).unwrap(); // already there: fine
    let list = sys.zfs_list(&base).unwrap();
    assert_eq!(list.len(), 4, "{list:?}");
    let data = list.iter().find(|d| d.name == vol).unwrap();
    assert_eq!(data.quota, Some(10 << 30));
    assert!(data.mountpoint.is_some());
    sys.zfs_set(&vol, "quota", "none").unwrap();
    assert_eq!(sys.zfs_list(&vol).unwrap()[0].quota, None, "quota cleared");
    sys.zfs_snapshot(&format!("{vol}@t1")).unwrap();
    assert!(sys.zfs_destroy(&vol, false).is_err(), "has a snapshot");
    sys.zfs_destroy(&base, true).unwrap();
    assert!(sys.zfs_list(&base).unwrap().is_empty());
}
