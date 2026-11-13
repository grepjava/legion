//! The web console on three nodes (CONSOLE.md, phases 1 to 5), in a real
//! browser: a headless Chromium signs in as a user who may only look, a
//! user who may change a namespace and the cluster admin, and does through
//! the console's pages what each phase's "done when" asks.
//!
//! - Phase 1: the viewer sees only their namespace, a change made with the
//!   CLI shows in an open page within a second, and no page offers them an
//!   action: every button is disabled, saying why.
//! - Phase 2: the editor deploys from pasted YAML (shown as changes
//!   first), scales, rolls out a new image and rolls back, reads a task's
//!   logs and types in its terminal; each of these is in `legion audit`.
//! - Phase 3: the admin takes a node out for maintenance and back, loses a
//!   node and restores its Volume from a backup downloaded in the console
//!   onto another, and rotates the CA, watching every node move to it.
//! - Phase 4: a crash-looping task and a node going down each raise an
//!   alert in the open console within 30 s, leading to the cause; a lost
//!   Volume raises its own; a service's logs are searched across its tasks.
//! - Phase 5: the editor deploys from an image with the form, saves a
//!   compose file as a template and deploys it from the catalog with its
//!   variable set, and sees both healthy in the topology.
//!
//! It drives real daemons, so it is ignored by default and run on the lab
//! of `tests/common` (see there), as root, with `LEGION_BROWSER` naming
//! Chromium: this host is the manager n1 and serves the console, the first
//! VM the worker n2, the second the manager n3.

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::browser::Browser;
use common::{Lab, STOP, text, wait};
use serde_json::Value;

const IMAGE: &str = "docker.io/freebsd/freebsd-notoolchain:15.1";
const NEXT: &str = "docker.io/freebsd/freebsd-runtime:15.1";

/// What the editor deploys: a service that writes to its log, and one task
/// keeping a Volume on the node labelled `disk=kept`.
fn app() -> String {
    format!(
        r#"apiVersion: legion/v1
kind: Service
metadata: {{ name: hello, namespace: team-a }}
spec:
  replicas: 2
  pod:
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sh, -c, "while :; do echo tick; sleep 1; done"]
---
apiVersion: legion/v1
kind: Service
metadata: {{ name: keeper, namespace: team-a }}
spec:
  mode: stateful
  replicas: 1
  placement: {{ constraints: ["node.labels.disk == kept"] }}
  pod:
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sleep, "86400"]
        volumeMounts: [{{ name: data, path: /data }}]
    volumes: [{{ name: data, claim: {{ size: 64Mi }} }}]
"#
    )
}

struct Console {
    lab: Lab,
    url: String,
}

impl Console {
    /// A browser signed in with the credentials in `identity`, by the link
    /// `legion console login` prints, its profile in the lab directory.
    fn sign_in(&self, who: &str, identity: &std::path::Path) -> Browser {
        let mut b = Browser::start(&self.lab.dir.join(format!("browser-{who}")));
        let out = self
            .lab
            .ok_as(identity, &["console", "login", "--url", &self.url]);
        let link = out
            .split_whitespace()
            .find(|w| w.starts_with("https://"))
            .unwrap_or_else(|| panic!("a sign-in link in {out:?}"))
            .to_owned();
        b.goto(&link);
        b.shows("Sign out", 30);
        b
    }

    fn page(&self, b: &mut Browser, path: &str) {
        b.goto(&format!("{}{path}", self.url));
    }

    /// One object as the admin sees it.
    fn get(&self, kind: &str, ns: &str, name: &str) -> Value {
        let mut args = vec!["get", kind, name, "-o", "json"];
        if !ns.is_empty() {
            args.extend(["-n", ns]);
        }
        serde_json::from_str(&self.lab.ok(&args)).unwrap()
    }

    /// `hello` has as many running tasks as it asks for, all ready, and with
    /// `current` all of its current spec. A scale makes a new generation
    /// without replacing the tasks running, so only a rollout asks that.
    fn hello_settled(&self, current: bool) -> bool {
        let s = self.get("service", "team-a", "hello");
        let generation = s["metadata"]["generation"].as_u64().unwrap_or(0);
        let replicas = s["spec"]["replicas"].as_u64().unwrap_or(0);
        let tasks: Vec<Value> = self
            .lab
            .list("tasks", "team-a")
            .into_iter()
            .filter(|t| {
                t["spec"]["service"] == "hello"
                    && t["spec"]["desiredState"] == "running"
                    && t["status"]["state"] == "running"
            })
            .collect();
        tasks.len() as u64 == replicas
            && (!current
                || tasks
                    .iter()
                    .all(|t| t["spec"]["serviceGeneration"].as_u64() == Some(generation)))
            && s["status"]["ready"].as_u64() == Some(replicas)
    }

    fn image(&self) -> String {
        self.get("service", "team-a", "hello")["spec"]["pod"]["containers"][0]["image"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    /// The running task of a service, by name.
    fn task_of(&self, service: &str) -> Option<(String, String)> {
        self.lab
            .list("tasks", "team-a")
            .into_iter()
            .find(|t| {
                t["spec"]["service"] == service
                    && t["spec"]["desiredState"] == "running"
                    && t["status"]["state"] == "running"
            })
            .map(|t| {
                (
                    t["metadata"]["name"].as_str().unwrap().to_owned(),
                    t["spec"]["node"].as_str().unwrap().to_owned(),
                )
            })
    }
}

/// Every button on the page that does something (not tabs or the
/// sign-out) is disabled, with a title saying why; there is at least one.
fn all_disabled(b: &mut Browser) -> Vec<(String, bool, String)> {
    let v = b.eval(
        "[...document.querySelectorAll('main button')].filter(H.shown)
           .filter((e) => e.getAttribute('role') !== 'tab')
           .map((e) => [e.innerText.trim(), !e.disabled, e.title ?? ''])",
    );
    v.as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a[0].as_str().unwrap_or("").to_owned(),
                a[1] == Value::Bool(true),
                a[2].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect()
}

fn assert_read_only(b: &mut Browser, page: &str) {
    let buttons = all_disabled(b);
    assert!(!buttons.is_empty(), "{page}: buttons to check");
    for (t, enabled, title) in &buttons {
        assert!(!enabled, "{page}: {t:?} may be pressed by a viewer");
        assert!(!title.is_empty(), "{page}: {t:?} says why it is disabled");
    }
    assert!(
        b.text().contains("Read only:"),
        "{page}: says it is read only"
    );
}

#[test]
#[ignore = "needs root, ZFS on FreeBSD, two more nodes and Chromium; see the module docs"]
fn the_console_runs_the_cluster() {
    let mut lab = Lab::from_env();
    let step = |what: &str| eprintln!("--- {what}");
    let listen = format!("{}:7480", lab.host);
    let url = format!("https://{listen}");

    step("three nodes, the console on n1; two namespaces, a viewer and an editor of one");
    lab.three_nodes(&["--console-listen", &listen]);
    let setup = lab.dir.join("setup.yaml");
    std::fs::write(
        &setup,
        "apiVersion: legion/v1
kind: Namespace
metadata: { name: team-a }
---
apiVersion: legion/v1
kind: Namespace
metadata: { name: team-b }
---
apiVersion: legion/v1
kind: Config
metadata: { name: hidden, namespace: team-b }
spec: { text: team-b's }
",
    )
    .unwrap();
    lab.ok(&["apply", "-f", setup.to_str().unwrap()]);
    lab.ok(&["node", "label", "n2", "disk=kept"]);
    let pem = |who: &str| -> PathBuf { lab.dir.join(format!("{who}.pem")) };
    let (vera, ed) = (pem("vera"), pem("ed"));
    lab.ok(&[
        "user",
        "create",
        "vera",
        "--role",
        "view",
        "-n",
        "team-a",
        "-o",
        vera.to_str().unwrap(),
    ]);
    lab.ok(&[
        "user",
        "create",
        "ed",
        "--role",
        "edit",
        "-n",
        "team-a",
        "-o",
        ed.to_str().unwrap(),
    ]);
    let c = Console { lab, url };

    // --- Phase 1 -------------------------------------------------------
    step("phase 1: the viewer signs in and sees only team-a");
    let mut v = c.sign_in("vera", &vera);
    let choices = v.eval("[...document.querySelectorAll('label.ns option')].map((o) => o.value)");
    assert_eq!(
        choices,
        serde_json::json!(["", "team-a"]),
        "the namespaces offered"
    );
    c.page(&mut v, "/k/Config");
    v.shows("Configs", 30);
    std::thread::sleep(Duration::from_secs(2));
    assert!(!v.text().contains("hidden"), "team-b's Config is not shown");

    step("phase 1: a change made with the CLI shows in the open page within a second");
    let greeting = c.lab.dir.join("greeting.yaml");
    std::fs::write(
        &greeting,
        "apiVersion: legion/v1\nkind: Config\nmetadata: { name: greeting, namespace: team-a }\nspec: { text: hello }\n",
    )
    .unwrap();
    c.lab.ok(&["apply", "-f", greeting.to_str().unwrap()]);
    let applied = Instant::now();
    v.until("the new Config listed", 5, |b| {
        b.text().contains("greeting")
    });
    let shown = applied.elapsed();
    eprintln!("shown {} ms after the CLI returned", shown.as_millis());
    assert!(
        shown <= Duration::from_secs(1),
        "shown within a second: {shown:?}"
    );

    step("phase 1: no page offers the viewer an action");
    c.page(&mut v, "/k/Config/team-a/greeting");
    v.shows("greeting", 30);
    assert_read_only(&mut v, "a Config");
    c.page(&mut v, "/apply");
    v.fill("YAML", "kind: Config");
    assert_read_only(&mut v, "Apply YAML");

    // --- Phase 2 -------------------------------------------------------
    step("phase 2: the editor deploys from pasted YAML, its changes shown first");
    let mut e = c.sign_in("ed", &ed);
    c.page(&mut e, "/apply");
    e.fill("YAML", &app());
    e.click("Show changes");
    e.shows("Service/team-a/hello: would be created", 30);
    e.shows("Service/team-a/keeper: would be created", 30);
    assert!(
        c.lab.list("services", "team-a").is_empty(),
        "a dry run writes nothing"
    );
    e.click("Apply");
    e.shows("Service/team-a/hello: created, version", 30);
    e.shows("Service/team-a/keeper: created, version", 30);
    wait("hello's two tasks", 300, || c.hello_settled(true));
    wait("keeper's task on n2", 300, || {
        c.task_of("keeper").is_some_and(|(_, n)| n == "n2")
    });

    step("phase 2: scaled to three");
    c.page(&mut e, "/k/Service/team-a/hello");
    e.shows("Scale", 30);
    e.fill("Replicas", "3");
    e.click("Scale");
    e.shows("hello scaled to 3", 30);
    wait("three tasks", 300, || {
        c.get("service", "team-a", "hello")["spec"]["replicas"] == 3 && c.hello_settled(false)
    });

    step("phase 2: a new image rolled out, and the rollout followed");
    e.click("Rollout");
    e.fill("New image for c", NEXT);
    e.click("Roll out");
    wait("the new image on every task", 600, || {
        c.image() == NEXT && c.hello_settled(true)
    });
    e.shows("completed", 60);

    step("phase 2: rolled back");
    e.click("Details");
    e.click("Roll back");
    wait("the image before", 600, || {
        c.image() == IMAGE && c.hello_settled(true)
    });

    step("phase 2: a task's logs");
    let (task, _) = c.task_of("hello").expect("a hello task");
    c.page(&mut e, &format!("/k/Task/team-a/{task}"));
    e.click("Logs");
    e.shows("tick", 60);

    step("phase 2: a terminal in keeper's task, writing to its Volume");
    let (keeper, _) = c.task_of("keeper").expect("keeper's task");
    c.page(&mut e, &format!("/k/Task/team-a/{keeper}"));
    e.click("Terminal");
    e.fill("Command", "/bin/sh");
    e.click("Open");
    e.until("the terminal", 30, |b| {
        b.eval("!!document.querySelector('.xterm-helper-textarea')") == Value::Bool(true)
    });
    std::thread::sleep(Duration::from_secs(2));
    e.eval("document.querySelector('.xterm-helper-textarea').focus()");
    e.type_line("echo console-$((6*7)) > /data/kept; cat /data/kept");
    e.until("the command's output", 30, |b| {
        b.eval("document.querySelector('.xterm-rows')?.innerText ?? ''")
            .as_str()
            .is_some_and(|t| t.contains("console-42"))
    });
    let kept = c
        .lab
        .ok(&["exec", "keeper", "-n", "team-a", "--", "cat", "/data/kept"]);
    assert_eq!(kept.trim(), "console-42", "written through the terminal");

    step("phase 2: every action is in legion audit, as the editor");
    wait("the editor's actions audited", 60, || {
        let out = c.lab.ok(&["audit", "--actor", "ed", "-o", "json"]);
        let events: Vec<Value> = serde_json::from_str(&out).unwrap_or_default();
        let n = |a: &str| events.iter().filter(|e| e["action"] == a).count();
        // Two deployed, scaled, rolled out, rolled back; a terminal.
        n("apply") >= 5 && n("exec") >= 1 && n("logs") >= 1
    });

    step("phase 2: the viewer sees the service's buttons disabled, saying why");
    c.page(&mut v, "/k/Service/team-a/hello");
    v.shows("Scale", 30);
    assert_read_only(&mut v, "a Service");
    let (enabled, why) = v.button("Scale").expect("a Scale button");
    assert!(!enabled && why.contains("view"), "Scale: {why}");
    drop(v);
    drop(e);

    // --- Phase 3 -------------------------------------------------------
    step("phase 3: the admin takes n3 out for maintenance and back");
    let mut a = c.sign_in("admin", &c.lab.admin());
    c.page(&mut a, "/k/Node/_/n3");
    a.click("Drain");
    wait("n3 drained", 300, || {
        c.get("node", "", "n3")["spec"]["availability"] == "drain"
            && !c.lab.placed("team-a", "hello").iter().any(|n| n == "n3")
            && c.hello_settled(false)
    });
    a.click("Activate");
    wait("n3 active", 60, || {
        c.get("node", "", "n3")["spec"]["availability"] == "active"
    });

    step("phase 3: keeper's Volume backed up from the console");
    let volume = "data-keeper-0";
    c.page(&mut a, &format!("/k/Volume/team-a/{volume}"));
    a.click("Data");
    a.eval("H.button('Download a backup').click()");
    let backup = a.downloaded(300);
    eprintln!(
        "backup {} ({} bytes)",
        backup.display(),
        backup.metadata().unwrap().len()
    );

    // --- Phase 4 -------------------------------------------------------
    step("phase 4: a crash-looping task raises an alert in the open console, leading to it");
    c.page(&mut a, "/alerts");
    a.shows("Alerts", 30);
    let crasher = c.lab.dir.join("crasher.yaml");
    std::fs::write(
        &crasher,
        format!(
            "apiVersion: legion/v1
kind: Service
metadata: {{ name: crasher, namespace: team-a }}
spec:
  replicas: 1
  placement: {{ constraints: [\"node.name != n2\"] }}
  pod:
    restartPolicy: {{ delay: 1s }}
    containers:
      - name: c
        image: {IMAGE}
        command: [/bin/sh, -c, \"echo crashing; exit 1\"]
"
        ),
    )
    .unwrap();
    c.lab.ok(&["apply", "-f", crasher.to_str().unwrap()]);
    let restarted = |n: u64| {
        c.lab.list("tasks", "team-a").iter().any(|t| {
            t["spec"]["service"] == "crasher" && t["status"]["restarts"].as_u64() >= Some(n)
        })
    };
    wait("crasher restarting", 300, || restarted(3));
    let looping = Instant::now();
    a.until("the crash loop shown", 30, |b| {
        b.text().contains("crash-loop")
    });
    eprintln!(
        "crash loop shown {} ms after its third restart",
        looping.elapsed().as_millis()
    );
    a.eval(
        "[...document.querySelectorAll('tr.alert a')]
           .find((l) => l.innerText.includes('Task/crasher')).click()",
    );
    a.shows("exited with 1", 30);
    c.lab.ok(&["delete", "service", "crasher", "-n", "team-a"]);
    c.page(&mut a, "/alerts");
    a.until("the crash loop gone with its task", 120, |b| {
        !b.text().contains("crash-loop")
    });

    step("phase 4: hello's logs searched across its tasks over the last hour");
    c.page(&mut a, "/k/Service/team-a/hello");
    a.click("Logs");
    a.fill("Search text", "TICK");
    a.click("Search");
    a.until("lines from more than one task", 120, |b| {
        b.eval(
            "new Set([...document.querySelectorAll('table.found tbody tr td:nth-child(2)')]
               .map((td) => td.innerText)).size",
        )
        .as_u64()
            >= Some(2)
    });
    assert!(!a.text().contains("Not searched"), "{}", a.text());

    // --- Phase 5 -------------------------------------------------------
    step("phase 5: the editor deploys from an image with the form, its YAML shown first");
    let mut e = c.sign_in("ed-5", &ed);
    c.page(&mut e, "/deploy");
    e.fill("Name", "formed");
    e.fill("Namespace", "team-a");
    e.fill("Image", IMAGE);
    e.fill("Command", "/bin/sh\n-c\nwhile :; do echo formed; sleep 1; done");
    e.fill("Container port", "8080");
    e.fill("Memory", "64Mi");
    e.until("the YAML written", 30, |b| {
        let t = b.text();
        t.contains("kind: Service") && t.contains("memory: 64Mi") && t.contains("containerPort: 8080")
    });
    e.click("Show changes");
    e.shows("Service/team-a/formed: would be created", 30);
    e.click("Deploy");
    e.shows("Service/team-a/formed: created, version", 30);
    wait("formed's task ready", 300, || {
        c.get("service", "team-a", "formed")["status"]["ready"] == 1
    });

    step("phase 5: a compose file saved as a template, deployed from the catalog with its variable set");
    c.page(&mut e, "/import");
    e.fill("Stack", "draft");
    e.fill(
        "Compose file",
        "# Sleeps.\nservices:\n  sleeper:\n    image: \"${IMAGE:-localhost/none:1}\"\n    command: [/bin/sleep, \"86400\"]\n",
    );
    e.shows("Variable IMAGE", 30);
    e.fill("Template name", "sleeper");
    e.fill("Template namespace", "team-a");
    e.click("Save");
    e.shows("Saved as Config team-a/sleeper.", 30);
    c.page(&mut e, "/templates");
    e.shows("saved in team-a", 30);
    assert!(e.text().contains("counter"), "the examples are in the catalog");
    e.eval("document.querySelector('[aria-label=\"Use sleeper\"]').click()");
    e.until("the template opened", 30, |b| {
        b.eval("H.field('Stack')?.value ?? ''") == serde_json::json!("sleeper")
    });
    e.fill("Namespace", "team-a");
    e.fill("Variable IMAGE", IMAGE);
    e.until("the objects with the image given", 30, |b| {
        let t = b.text();
        t.contains(IMAGE) && t.contains("legion.stack: sleeper")
    });
    e.click("Deploy");
    e.shows("Service/team-a/sleeper", 30);
    wait("sleeper's task ready", 300, || {
        c.get("service", "team-a", "sleeper")["status"]["ready"] == 1
    });

    step("phase 5: the topology shows them, healthy, with keeper's Volume");
    c.page(&mut e, "/topology");
    e.until("every service green and keeper's Volume", 60, |b| {
        b.eval(
            "['formed', 'sleeper', 'hello'].every((s) =>
               document.querySelector(`[aria-label=\"Service ${s}\"] rect`)?.classList.contains('good'))
             && !!document.querySelector('[aria-label^=\"Volume \"]')",
        ) == serde_json::json!(true)
    });
    for s in ["formed", "sleeper"] {
        c.lab.ok(&["delete", "service", s, "-n", "team-a"]);
    }
    drop(e);

    step("phases 3 and 4: n2 lost, alerted within 30 s, removed from the console; its Volume lost");
    // Watched where an operator would: the alerts page, open before n2 goes.
    c.page(&mut a, "/alerts");
    let out = c.lab.ssh(&c.lab.vms[0], &format!("{STOP}\ntrue\n"));
    assert!(out.status.success(), "stopping n2: {}", text(&out.stderr));
    let stopped = Instant::now();
    a.until("n2's alert", 30, |b| b.text().contains("Node/n2"));
    eprintln!(
        "n2's alert shown {} ms after its daemon stopped",
        stopped.elapsed().as_millis()
    );
    wait("n2 down", 120, || c.lab.node_state("n2") == "down");
    c.page(&mut a, "/k/Node/_/n2");
    a.click("Delete");
    wait("the Volume lost", 120, || {
        c.get("volume", "team-a", volume)["status"]["lost"]
            .as_str()
            .is_some_and(|l| !l.is_empty())
    });
    c.page(&mut a, "/alerts");
    a.shows(&format!("team-a/Volume/{volume}"), 30);

    step("phase 3: n1 labelled for keeper, the Volume restored onto it from the backup");
    c.page(&mut a, "/k/Node/_/n1");
    a.fill("New label", "disk=kept");
    a.eval("H.field('New label').form.requestSubmit()");
    wait("n1 labelled", 30, || {
        c.get("node", "", "n1")["metadata"]["labels"]["disk"] == "kept"
    });
    c.page(&mut a, &format!("/k/Volume/team-a/{volume}"));
    a.click("Data");
    a.shows("Lost with its node", 30);
    a.choose_file("Backup file", &backup);
    a.fill("Node", "n1");
    a.click("Restore into it");
    wait("keeper running on n1", 600, || {
        c.task_of("keeper").is_some_and(|(_, n)| n == "n1")
    });
    let kept = c
        .lab
        .ok(&["exec", "keeper", "-n", "team-a", "--", "cat", "/data/kept"]);
    assert_eq!(kept.trim(), "console-42", "the data restored");

    step("phase 3: the audit log, filtered, in the console");
    c.page(&mut a, "/audit");
    a.fill("Actor", "ed");
    a.fill("Action", "exec");
    a.click("Show");
    a.shows("exec", 30);

    step("phase 3: the CA rotated from the console, every node moving to it");
    let before = c.lab.ok(&["ca", "status"]);
    c.page(&mut a, "/cluster");
    a.shows("Issuing:", 30);
    let issuing = "document.querySelector('main p code')?.innerText ?? ''";
    let old = a.eval(issuing);
    a.click("Rotate the CA");
    // The first table is the CA's: a row per holder, good once on the new CA.
    let mut seen = Vec::new();
    a.until("every holder on the new CA and the rotation ended", 900, |b| {
        let t = b.text();
        if let Some(m) = t.lines().find(|l| l.contains(" moved"))
            && seen.last().is_none_or(|l: &String| l != m)
        {
            eprintln!("{m}");
            seen.push(m.to_owned());
        }
        b.eval(issuing) != old
            && b.eval(
                "(() => { const rows = [...document.querySelector('main table').querySelectorAll('tbody tr')];
                   return !H.text().includes('rotating') && rows.length >= 2 &&
                     rows.every((r) => r.querySelector('.state.good')); })()",
            ) == Value::Bool(true)
    });
    assert!(!seen.is_empty(), "the rotation's progress was shown");
    let after = c.lab.ok(&["ca", "status"]);
    let digest = |s: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix("CA: "))
            .unwrap_or_default()
            .trim()
            .to_owned()
    };
    assert_ne!(digest(&before), digest(&after), "a new CA issues: {after}");
    assert!(after.contains("no rotation under way"), "{after}");
    if !a.errors.is_empty() {
        eprintln!("the admin's pages reported: {:?}", a.errors);
    }
}
