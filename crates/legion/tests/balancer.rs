//! A service's load balancer address on three nodes (DESIGN.md §5.4): its
//! ports answer at one address of the nodes' network, held by one node as a
//! CARP virtual host, and another takes it over once that node is drained.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root: this host is the manager n1, the
//! first VM the worker n2, the second the manager n3. The address is the
//! .250 of the nodes' network, on the interface holding this host's
//! address (`LEGION_CLUSTER_HOST`), which the VMs share.

mod common;

use common::{Lab, text, wait};

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";
const VHID: u8 = 77;

/// `web`, answering on 8080 with its task's name, at `address` port 80.
fn app(address: &str) -> String {
    format!(
        r#"
apiVersion: legion/v1
kind: Config
metadata: {{ name: services, namespace: default }}
spec:
  text: "legion-name 8080/tcp\n"
---
apiVersion: legion/v1
kind: Config
metadata: {{ name: inetd, namespace: default }}
spec:
  text: "legion-name stream tcp nowait root /bin/hostname hostname\n"
---
apiVersion: legion/v1
kind: Service
metadata: {{ name: web, namespace: default }}
spec:
  replicas: 3
  pod:
    containers:
      - name: web
        image: {IMAGE}
        command: [/usr/sbin/inetd, -d, /etc/inetd.conf]
        ports: [{{ containerPort: 8080 }}]
        configs:
          - {{ name: services, target: /etc/services }}
          - {{ name: inetd, target: /etc/inetd.conf }}
        probes:
          readiness: {{ tcp: {{ port: 8080 }}, period: 2s }}
  endpoint:
    ports: [{{ port: 80, targetPort: 8080 }}]
    loadBalancer: {{ address: {address}, vhid: {VHID} }}
"#
    )
}

/// The interface of this host with `addr`, and the network's .250.
fn interface_and_address(addr: &str) -> (String, String) {
    let list = std::process::Command::new("ifconfig")
        .arg("-l")
        .output()
        .unwrap();
    for i in text(&list.stdout).split_whitespace() {
        let out = std::process::Command::new("ifconfig")
            .arg(i)
            .output()
            .unwrap();
        if text(&out.stdout).contains(&format!("inet {addr} ")) {
            let (net, _) = addr.rsplit_once('.').unwrap();
            return (i.to_owned(), format!("{net}.250"));
        }
    }
    panic!("no interface of this host has {addr}");
}

/// The node whose interface holds the address as CARP master, if one does.
fn master(lab: &Lab, iface: &str) -> Option<String> {
    let mut found = None;
    for node in ["n1", "n2", "n3"] {
        let out = if node == "n1" {
            std::process::Command::new("ifconfig")
                .arg(iface)
                .output()
                .unwrap()
        } else {
            lab.on(node, &format!("ifconfig {iface}"))
        };
        if text(&out.stdout).contains(&format!("carp: MASTER vhid {VHID} ")) {
            assert!(found.is_none(), "two masters");
            found = Some(node.to_owned());
        }
    }
    found
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD and two more nodes; see the module docs"]
fn a_load_balancer_address_answers_and_moves() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");
    let (iface, address) = interface_and_address(&lab.host);
    let flags = ["--load-balancer-interface", iface.as_str()];
    lab.flags = format!("{} {}", lab.flags, flags.join(" "));

    step("three nodes, and web at a load balancer address");
    lab.three_nodes(&flags);
    let file = lab.dir.join("web.yaml");
    std::fs::write(&file, app(&address)).unwrap();
    lab.ok(&["apply", "-f", file.to_str().unwrap()]);
    let answer = |lab: &Lab| {
        let out = lab.on("n2", &format!("nc -N -w 5 {address} 80 </dev/null"));
        text(&out.stdout).trim().to_owned()
    };
    wait("web answering at the address", 600, || {
        !answer(&lab).is_empty()
    });

    step("one node holds it");
    let first = master(&lab, &iface).expect("a master");
    eprintln!("held by {first}");

    step("that node drained: another takes the address over, and web answers on");
    lab.ok(&["node", "drain", &first]);
    wait("another master", 120, || {
        master(&lab, &iface).is_some_and(|m| m != first)
    });
    wait("web answering again", 120, || !answer(&lab).is_empty());
    lab.ok(&["node", "activate", &first]);

    step("removed, the address is given up everywhere");
    lab.ok(&["delete", "service", "web", "-n", "default"]);
    wait("no master", 120, || master(&lab, &iface).is_none());
}
