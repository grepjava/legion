//! What [`FakeSys`] stands in for, held to what FreeBSD does. Every other
//! crate's tests trust the fake: a rule it doesn't have is a mistake only a
//! node finds, and a rule it has wrongly is one a test is written around.
//! So each rule here is one function over [`Sys`], run against the fake
//! always, and against [`Shell`] when qualifying.
//!
//! The `real_` tests are ignored by default and need root. `real_jails` makes
//! throwaway jails named `lcontract*`; `real_zfs` needs `LEGION_SYS_ZFS_ROOT`
//! naming a dataset it may create children under; `real_pf` loads rules, so
//! it refuses to run outside a VNET jail (see `live.rs`).

use std::net::IpAddr;

use legion_sys::{Error, FakeSys, KldState, Shell, Sys};

fn jail(sys: &dyn Sys, name: &str, more: &[(&str, &str)]) -> legion_sys::Result<u32> {
    let mut params = vec![("name", name), ("path", "/"), ("persist", "")];
    params.extend_from_slice(more);
    sys.jail_create(&params)
}

fn exists(sys: &dyn Sys, name: &str) -> bool {
    sys.jails().unwrap().iter().any(|j| j.name == name)
}

fn jails(sys: &dyn Sys) {
    let name = format!("lcontract{}", std::process::id());
    let child = format!("{name}.kid");
    jail(sys, &name, &[]).unwrap();
    let rules = || -> legion_sys::Result<()> {
        assert!(jail(sys, &name, &[]).is_err(), "a name is taken once");

        // A parameter the kernel doesn't know is refused, jail and all.
        let other = format!("{name}x");
        assert!(jail(sys, &other, &[("allow.nonsense", "true")]).is_err());
        assert!(!exists(sys, &other), "nothing made");
        assert!(sys.jail_set(&name, &[("allow.nonsense", "true")]).is_err());
        assert!(sys.jail_set(&name, &[("children.max", "many")]).is_err());

        // Children: under a parent that is there. children.max limits what
        // is made from inside a jail, not from the host.
        assert!(jail(sys, &format!("{name}nope.kid"), &[]).is_err());
        jail(sys, &child, &[]).unwrap();
        sys.jail_set(&name, &[("children.max", "2")]).unwrap();
        let got = sys.jail_get(&name, &["children.max"]).unwrap();
        assert_eq!(got["children.max"], "2");

        // A parent takes its children with it.
        sys.jail_remove(&name)?;
        assert!(!exists(sys, &child), "went with its parent");
        assert!(matches!(sys.jail_remove(&name), Err(Error::NotFound(_))));
        assert!(matches!(
            sys.jail_get(&name, &["children.max"]),
            Err(Error::NotFound(_))
        ));
        Ok(())
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(rules));
    let _ = sys.jail_remove(&name);
    match result {
        Ok(r) => r.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// What the fake takes must be what the kernel takes, or a test passes with
/// a parameter that fails on a node.
fn jail_parameters(sys: &dyn Sys) {
    let name = format!("lcontractp{}", std::process::id());
    for (param, value) in FakeSys::JAIL_PARAMETERS {
        // These the helper sets; the others one at a time.
        if ["name", "path", "persist"].contains(param) {
            continue;
        }
        let made = jail(sys, &name, &[(param, value)]);
        assert!(made.is_ok(), "{param}={value}: {made:?}");
        sys.jail_remove(&name).unwrap();
    }
}

fn zfs(sys: &dyn Sys, root: &str) {
    let base = format!("{root}/lcontract{}", std::process::id());
    let ds = format!("{base}/a/b");
    let snap = format!("{ds}@one");
    let clone = format!("{base}/clone");

    // Made with its parents; made again, left as it is.
    sys.zfs_create(&ds, &[("quota", "1073741824"), ("mountpoint", "none")])
        .unwrap();
    sys.zfs_create(&ds, &[("quota", "2147483648")]).unwrap();
    let got = &sys.zfs_list(&ds).unwrap()[0];
    assert_eq!(got.quota, Some(1 << 30));
    assert_eq!(got.mountpoint, None);
    assert_eq!(sys.zfs_list(&base).unwrap().len(), 3, "with its parents");

    // No mountpoint is inherited, and can't be mounted until it has one.
    let under = format!("{ds}/under");
    sys.zfs_create(&under, &[]).unwrap();
    assert_eq!(sys.zfs_list(&under).unwrap()[0].mountpoint, None);
    let e = sys.zfs_mount(&under).unwrap_err().to_string();
    assert!(e.contains("no mountpoint set"), "{e}");
    let place = format!("/var/tmp/lcontract{}", std::process::id());
    sys.zfs_set(&under, "mountpoint", &place).unwrap();
    sys.zfs_mount(&under).unwrap();
    assert_eq!(
        sys.zfs_list(&under).unwrap()[0].mountpoint.as_deref(),
        Some(place.as_str())
    );
    sys.zfs_destroy(&under, false).unwrap();
    let _ = std::fs::remove_dir(&place);

    // Properties: ones it doesn't know, and values that make no sense.
    assert!(sys.zfs_set(&ds, "nonsense", "1").is_err());
    assert!(sys.zfs_set(&ds, "quota", "lots").is_err());
    assert!(
        sys.zfs_create(&format!("{base}/x"), &[("nonsense", "1")])
            .is_err()
    );
    assert!(sys.zfs_list(&format!("{base}/x")).unwrap().is_empty());
    sys.zfs_set(&ds, "quota", "none").unwrap();
    assert_eq!(sys.zfs_list(&ds).unwrap()[0].quota, None);

    // What isn't there: listed as nothing, otherwise an error.
    let missing = format!("{base}/missing");
    assert!(sys.zfs_list(&missing).unwrap().is_empty());
    assert!(sys.zfs_snapshots(&missing).unwrap().is_empty());
    assert!(sys.zfs_set(&missing, "quota", "none").is_err());
    assert!(sys.zfs_destroy(&missing, false).is_err());
    assert!(sys.zfs_snapshot(&format!("{missing}@one")).is_err());

    // Snapshots and clones hold what they came from.
    sys.zfs_snapshot(&snap).unwrap();
    assert!(sys.zfs_snapshot(&snap).is_err(), "a name is taken once");
    assert!(
        sys.zfs_clone(&format!("{ds}@nope"), &clone, &[("mountpoint", "none")])
            .is_err()
    );
    assert!(
        sys.zfs_clone(
            &snap,
            &format!("{base}/no/parent"),
            &[("mountpoint", "none")]
        )
        .is_err(),
        "a clone's parent must be there"
    );
    sys.zfs_clone(&snap, &clone, &[("mountpoint", "none")])
        .unwrap();
    assert!(
        sys.zfs_clone(&snap, &clone, &[("mountpoint", "none")])
            .is_err()
    );
    assert!(sys.zfs_destroy(&snap, false).is_err(), "it has a clone");
    assert!(sys.zfs_destroy(&ds, false).is_err(), "it has a snapshot");
    assert!(sys.zfs_destroy(&ds, true).is_err(), "its clone is outside");
    assert!(sys.zfs_destroy(&format!("{base}/a"), false).is_err());
    sys.zfs_destroy(&clone, false).unwrap();
    sys.zfs_destroy(&ds, true).unwrap();
    assert!(sys.zfs_list(&ds).unwrap().is_empty());
    assert!(sys.zfs_rollback(&snap).is_err(), "gone with its dataset");
    sys.zfs_destroy(&base, true).unwrap();
}

fn pf(sys: &dyn Sys) {
    let anchor = "legion/contract";
    let ip = |s: &str| -> IpAddr { s.parse().unwrap() };
    let good = "table <t> { 10.99.0.1 }\n\
                nat on lo0 inet from 10.99.0.0/24 to any -> 127.0.0.1\n\
                rdr on lo0 inet proto tcp to 10.99.0.2 port 80 -> <t> port 8080\n\
                pass quick on lo0 all\n";
    sys.pf_load_anchor(anchor, good).unwrap();
    assert_eq!(sys.pf_table_show(anchor, "t").unwrap(), [ip("10.99.0.1")]);

    // Refused whole, with what was loaded still there.
    assert!(sys.pf_load_anchor(anchor, "this is not pf\n").is_err());
    let late = "pass all\nnat on lo0 inet from any to any -> 127.0.0.1\n";
    assert!(sys.pf_load_anchor(anchor, late).is_err(), "out of order");
    assert_eq!(sys.pf_table_show(anchor, "t").unwrap(), [ip("10.99.0.1")]);

    sys.pf_table_replace(anchor, "t", &[ip("10.99.0.3"), ip("10.99.0.4")])
        .unwrap();
    let mut now = sys.pf_table_show(anchor, "t").unwrap();
    now.sort();
    assert_eq!(now, [ip("10.99.0.3"), ip("10.99.0.4")]);
    assert!(sys.pf_table_show(anchor, "nope").unwrap().is_empty());

    // A persistent table is the agent's to fill: loading the rules again,
    // as a change of services does, leaves its addresses.
    let kept = "table <p> persist\npass quick on lo0 from <p> to any\n";
    sys.pf_load_anchor(anchor, kept).unwrap();
    sys.pf_table_replace(anchor, "p", &[ip("10.99.0.5")])
        .unwrap();
    sys.pf_load_anchor(anchor, kept).unwrap();
    assert_eq!(sys.pf_table_show(anchor, "p").unwrap(), [ip("10.99.0.5")]);
    // Loaded with its addresses, it holds those, whatever it held before:
    // the load balancer sets every table so when the rules change.
    let listed = "table <p> persist { 10.99.0.6 fd00:99::6 }\n\
                  pass quick on lo0 from <p> to any\n";
    sys.pf_load_anchor(anchor, listed).unwrap();
    let mut now = sys.pf_table_show(anchor, "p").unwrap();
    now.sort();
    assert_eq!(now, [ip("10.99.0.6"), ip("fd00:99::6")]);

    sys.pf_flush_anchor(anchor).unwrap();
    assert!(sys.pf_table_show(anchor, "t").unwrap().is_empty());
    assert!(sys.pf_table_show(anchor, "p").unwrap().is_empty());
}

#[test]
fn fake_jails() {
    jails(&FakeSys::new());
    jail_parameters(&FakeSys::new());
}

#[test]
fn fake_zfs() {
    let sys = FakeSys::new();
    sys.zfs_create("tank", &[]).unwrap();
    zfs(&sys, "tank");
}

#[test]
fn fake_pf() {
    pf(&FakeSys::new());
}

#[test]
#[ignore = "needs root"]
fn real_jails() {
    jails(&Shell::new());
    jail_parameters(&Shell::new());
}

#[test]
#[ignore = "needs root and LEGION_SYS_ZFS_ROOT"]
fn real_zfs() {
    let Ok(root) = std::env::var("LEGION_SYS_ZFS_ROOT") else {
        legion_sys::live::skip("LEGION_SYS_ZFS_ROOT not set");
        return;
    };
    zfs(&Shell::new(), &root);
}

#[test]
#[ignore = "needs root inside a throwaway VNET jail"]
fn real_pf() {
    let jailed = std::process::Command::new("sysctl")
        .args(["-n", "security.jail.jailed"])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&jailed.stdout).trim(),
        "1",
        "refusing to change the host's pf; run inside a VNET jail"
    );
    pf(&Shell::new());
}

/// A module's state follows it into the kernel, and asking loads nothing.
fn modules(sys: &dyn Sys, name: &str) {
    assert_eq!(sys.kld_state(name).unwrap(), KldState::Available, "{name}");
    assert_eq!(sys.kld_state(name).unwrap(), KldState::Available, "{name}");
    sys.kld_load(name).unwrap();
    assert_eq!(sys.kld_state(name).unwrap(), KldState::Loaded, "{name}");
}

#[test]
fn fake_modules() {
    modules(&FakeSys::new(), "pf");
}

/// Each module the checks ask about, loaded and unloaded, on a lab VM: it
/// unloads pf, which nothing on a shared host could stand. One that can't be
/// unloaded is compiled in or in use, and must then read as loaded.
#[test]
#[ignore = "needs root on a disposable lab VM"]
fn real_modules() {
    if !std::path::Path::new("/etc/lab-base").exists() {
        return legion_sys::live::skip("needs a lab VM: it unloads kernel modules");
    }
    let sys = Shell::new();
    let unload = |m: &str| {
        std::process::Command::new("kldunload")
            .arg(m)
            .output()
            .unwrap()
            .status
            .success()
    };
    let mut cycled = Vec::new();
    for m in [
        "pf",
        "if_bridge",
        "if_epair",
        "linux64",
        "linprocfs",
        "linsysfs",
        "fdescfs",
        "tmpfs",
        "nullfs",
    ] {
        sys.kld_load(m).unwrap();
        assert_eq!(sys.kld_state(m).unwrap(), KldState::Loaded, "{m}");
        if unload(m) {
            modules(&sys, m);
            cycled.push(m);
        } else {
            println!("{m} stays in the kernel: compiled in or in use");
        }
    }
    println!("cycled: {cycled:?}");
    assert!(cycled.contains(&"linux64"), "{cycled:?}");
    assert_eq!(sys.kld_state("no_such_module").unwrap(), KldState::Missing);
    let pf = std::process::Command::new("pfctl")
        .args(["-e", "-f", "/etc/pf.conf"])
        .output()
        .unwrap();
    println!("pf back: {}", String::from_utf8_lossy(&pf.stderr));
}
