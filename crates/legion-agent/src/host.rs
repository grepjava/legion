//! The node's host as its agent reads it for the managers: FreeBSD's
//! release, the kernel, how and when it booted, and its ZFS pools' health
//! (`zpool status -j`, OpenZFS 2.3 and later), sent with heartbeats for the
//! leader to write into the Node when it changes (CONSOLE.md, phase 8).
//! The pools' scrubs are started here too, when the Node schedules them.

use legion_api::node::{HostFacts, HostReport, Pool, PoolScan};
use legion_sys::Sys;
use serde_json::Value;

/// What the host is and its pools, as read now; what can't be read is
/// left out rather than failing the rest.
pub fn read(sys: &dyn Sys) -> HostReport {
    let run = |program: &str, args: &[&str]| {
        sys.run_program(program, args, "")
            .ok()
            .map(|s| s.trim().to_owned())
    };
    let host = run("freebsd-version", &["-u"]).map(|release| HostFacts {
        release,
        kernel: run("freebsd-version", &["-k"]).unwrap_or_default(),
        boot: sys.sysctl_get("machdep.bootmethod").unwrap_or_default().trim().to_owned(),
        booted_ms: sys
            .sysctl_get("kern.boottime")
            .ok()
            .and_then(|b| boottime_ms(&b))
            .unwrap_or_default(),
    });
    let pools = run("zpool", &["status", "-j", "-p", "--json-int"])
        .map(|j| pools(&j))
        .unwrap_or_default();
    HostReport { host, pools }
}

/// `kern.boottime` (`{ sec = 1795035876, usec = 50383 } Thu Nov 19 …`) in
/// milliseconds since the epoch.
pub fn boottime_ms(raw: &str) -> Option<i64> {
    let field = |name: &str| -> Option<i64> {
        let at = raw.find(&format!("{name} = "))? + name.len() + 3;
        raw[at..]
            .split(|c: char| !c.is_ascii_digit())
            .next()?
            .parse()
            .ok()
    };
    Some(field("sec")? * 1000 + field("usec").unwrap_or(0) / 1000)
}

/// A number from `zpool status -j`, given as one or, without
/// `--json-int`, as a string.
fn num(v: &Value) -> u64 {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}

/// Read, write and checksum errors of a vdev and every vdev under it.
fn errors(v: &Value, out: &mut [u64; 3]) {
    out[0] += num(&v["read_errors"]);
    out[1] += num(&v["write_errors"]);
    out[2] += num(&v["checksum_errors"]);
    if let Some(children) = v["vdevs"].as_object() {
        for c in children.values() {
            errors(c, out);
        }
    }
}

/// The pools in `zpool status -j -p --json-int`'s output. A pool's space
/// moves in steps of a hundredth of its size (see [`Pool`]).
pub fn pools(json: &str) -> Vec<Pool> {
    let Ok(all) = serde_json::from_str::<Value>(json) else {
        return Vec::new();
    };
    let Some(pools) = all["pools"].as_object() else {
        return Vec::new();
    };
    let mut out: Vec<Pool> = pools
        .iter()
        .map(|(name, p)| {
            // The pool's own vdev, named as it is, holds its space.
            let root = &p["vdevs"][name];
            let size = num(&root["total_space"]);
            let step = (size / 100).max(1);
            let mut errs = [0; 3];
            if let Some(vdevs) = p["vdevs"].as_object() {
                for v in vdevs.values() {
                    errors(v, &mut errs);
                }
            }
            let s = &p["scan_stats"];
            let scan = s["function"].as_str().filter(|f| !f.is_empty() && *f != "NONE").map(|f| {
                let at = |k: &str| Some(num(&s[k]) as i64 * 1000).filter(|t| *t > 0);
                PoolScan {
                    function: f.to_lowercase(),
                    state: s["state"].as_str().unwrap_or_default().to_lowercase(),
                    start_ms: at("start_time"),
                    end_ms: at("end_time"),
                    errors: num(&s["errors"]),
                }
            });
            Pool {
                name: name.clone(),
                state: p["state"].as_str().unwrap_or("UNKNOWN").to_owned(),
                size_bytes: size,
                alloc_bytes: num(&root["alloc_space"]) / step * step,
                read_errors: errs[0],
                write_errors: errs[1],
                checksum_errors: errs[2],
                scan,
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The pools due a scrub at `now_ms` on a node scrubbing every `days`:
/// none running, and the last ended longer ago than that, or never.
pub fn due(pools: &[Pool], days: u32, now_ms: i64) -> Vec<String> {
    let every = i64::from(days) * 86_400_000;
    pools
        .iter()
        .filter(|p| p.healthy() && !p.scanning())
        .filter(|p| p.scrubbed_ms().is_none_or(|t| now_ms - t >= every))
        .map(|p| p.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As the Ryzen's OpenZFS 2.4 wrote it, a mirror's side offline.
    const STATUS: &str = r#"{"output_version":{"command":"zpool status","vers_major":0,"vers_minor":1},
      "pools":{
        "zroot":{"name":"zroot","state":"ONLINE",
          "scan_stats":{"function":"SCRUB","state":"FINISHED","start_time":1794226684,"end_time":1794226751,"errors":0},
          "vdevs":{"zroot":{"name":"zroot","vdev_type":"root","state":"ONLINE",
            "alloc_space":54541230080,"total_space":253403070464,
            "read_errors":0,"write_errors":0,"checksum_errors":0,
            "vdevs":{"ada0p4":{"name":"ada0p4","vdev_type":"disk","state":"ONLINE",
              "read_errors":0,"write_errors":0,"checksum_errors":0}}}},
          "error_count":0},
        "tank":{"name":"tank","state":"DEGRADED",
          "scan_stats":{"function":"NONE"},
          "vdevs":{"tank":{"name":"tank","vdev_type":"root","state":"DEGRADED",
            "alloc_space":"1000","total_space":"100000",
            "read_errors":0,"write_errors":0,"checksum_errors":0,
            "vdevs":{"mirror-0":{"name":"mirror-0","vdev_type":"mirror","state":"DEGRADED",
              "read_errors":0,"write_errors":0,"checksum_errors":0,
              "vdevs":{"/tmp/a":{"state":"ONLINE","read_errors":2,"write_errors":0,"checksum_errors":5},
                       "/tmp/b":{"state":"OFFLINE","read_errors":0,"write_errors":1,"checksum_errors":0}}}}}}}}}"#;

    #[test]
    fn pools_are_read_with_their_state_errors_space_and_scrub() {
        let p = pools(STATUS);
        assert_eq!(p.len(), 2);
        let (tank, zroot) = (&p[0], &p[1]);
        assert_eq!((tank.name.as_str(), tank.state.as_str()), ("tank", "DEGRADED"));
        assert_eq!(
            (tank.read_errors, tank.write_errors, tank.checksum_errors),
            (2, 1, 5),
            "every device's errors, however deep"
        );
        assert_eq!(tank.scan, None, "never scanned");
        assert_eq!((tank.size_bytes, tank.alloc_bytes), (100_000, 1000), "numbers as strings too");
        assert!(zroot.healthy());
        assert_eq!(zroot.scrubbed_ms(), Some(1_794_226_751_000));
        assert_eq!(zroot.percent_used(), 21);
        // In steps of a hundredth of the pool: a write within one changes
        // nothing.
        assert_eq!(zroot.alloc_bytes % (253_403_070_464 / 100), 0);
    }

    #[test]
    fn unreadable_status_is_no_pools() {
        assert_eq!(pools("zpool: unrecognized option"), []);
        assert_eq!(pools(r#"{"pools":{}}"#), []);
    }

    #[test]
    fn the_boot_time_is_read_to_the_millisecond() {
        assert_eq!(
            boottime_ms("{ sec = 1795035876, usec = 50383 } Thu Nov 19 05:04:36 2026"),
            Some(1_795_035_876_050)
        );
        assert_eq!(boottime_ms("nonsense"), None);
    }

    #[test]
    fn a_scrub_is_due_when_the_last_is_older_than_the_schedule() {
        let p = pools(STATUS);
        let day = 86_400_000i64;
        let scrubbed = 1_794_226_751_000;
        // tank is degraded: a scrub would only slow it while short a side.
        assert_eq!(due(&p, 30, scrubbed + 29 * day), Vec::<String>::new());
        assert_eq!(due(&p, 30, scrubbed + 30 * day), ["zroot"]);
        let mut running = p.clone();
        running[1].scan.as_mut().unwrap().state = "scanning".into();
        assert_eq!(due(&running, 30, scrubbed + 31 * day), Vec::<String>::new());
    }
}
