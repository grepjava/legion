//! A headless Chromium as the console's lab tests drive it: the program in
//! `LEGION_BROWSER`, spoken to over the DevTools protocol on a pipe
//! (`--remote-debugging-pipe`: commands to its descriptor 3, answers and
//! events from its descriptor 4, each message ending in a NUL), with one
//! page. Each [`Browser`] has a profile of its own, so its own cookies:
//! one per user signed in.
//!
//! Pages are found as a user finds them: buttons by their text, fields by
//! their label. Dialogs (`confirm`) are answered yes.

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// What every expression the tests evaluate may use: `H.button(text)` the
/// visible button, link styled as one, or tab whose text is `text`;
/// `H.field(label)` the control labelled `label`; `H.text()` the page's text.
const HELPERS: &str = r#"
const H = {
  shown: (e) => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length),
  button: (t) =>
    [...document.querySelectorAll('button, a.button, [role=tab]')]
      .filter(H.shown)
      .find((e) => e.innerText.trim() === t),
  field: (l) => [...document.querySelectorAll(`[aria-label="${l}"]`)].find(H.shown),
  text: () => document.body.innerText,
};
"#;

pub struct Browser {
    child: Child,
    to: File,
    from: Receiver<Value>,
    next: u64,
    session: String,
    /// Where downloads land.
    pub downloads: PathBuf,
    /// Errors the page reported (exceptions, `console.error`), for a failure's message.
    pub errors: Vec<String>,
}

fn pipe() -> (RawFd, RawFd) {
    let mut fds = [0; 2];
    assert_eq!(
        unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) },
        0,
        "pipe"
    );
    (fds[0], fds[1])
}

impl Browser {
    /// Starts the browser with its profile, downloads and log in `dir`.
    pub fn start(dir: &Path) -> Browser {
        let program = std::env::var("LEGION_BROWSER").expect("set LEGION_BROWSER");
        std::fs::create_dir_all(dir).unwrap();
        let downloads = dir.join("downloads");
        std::fs::create_dir_all(&downloads).unwrap();
        let log = File::create(dir.join("browser.log")).unwrap();
        let (cmd_r, cmd_w) = pipe();
        let (out_r, out_w) = pipe();
        let mut command = Command::new(program);
        command
            .args([
                "--headless=new",
                // Run as root on the lab, where Chromium's sandbox refuses.
                "--no-sandbox",
                "--disable-gpu",
                "--no-first-run",
                "--remote-debugging-pipe",
                // The console's certificate is of the cluster's own CA.
                "--ignore-certificate-errors",
                "--window-size=1400,1000",
            ])
            .arg(format!("--user-data-dir={}", dir.join("profile").display()))
            .arg("about:blank")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        // Its descriptors 3 and 4, copied above the others first so that a
        // pipe already on 3 or 4 is not closed by the other's dup2.
        unsafe {
            command.pre_exec(move || {
                let r = libc::fcntl(cmd_r, libc::F_DUPFD, 10);
                let w = libc::fcntl(out_w, libc::F_DUPFD, 10);
                if r < 0 || w < 0 || libc::dup2(r, 3) < 0 || libc::dup2(w, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("starting the browser");
        unsafe {
            libc::close(cmd_r);
            libc::close(out_w);
        }
        let to = unsafe { File::from_raw_fd(cmd_w) };
        let from = unsafe { File::from_raw_fd(out_r) };
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(from);
            loop {
                let mut buf = Vec::new();
                match reader.read_until(0, &mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
                        buf.pop();
                        if let Ok(v) = serde_json::from_slice::<Value>(&buf)
                            && tx.send(v).is_err()
                        {
                            return;
                        }
                    }
                }
            }
        });
        let mut b = Browser {
            child,
            to,
            from: rx,
            next: 0,
            session: String::new(),
            downloads,
            errors: Vec::new(),
        };
        let target = b.call("Target.createTarget", json!({ "url": "about:blank" }));
        let attached = b.call(
            "Target.attachToTarget",
            json!({ "targetId": target["targetId"], "flatten": true }),
        );
        b.session = attached["sessionId"]
            .as_str()
            .expect("a session")
            .to_owned();
        b.call(
            "Browser.setDownloadBehavior",
            json!({ "behavior": "allow", "downloadPath": b.downloads }),
        );
        b.page("Page.enable", json!({}));
        b.page("Runtime.enable", json!({}));
        b.page(
            "Page.addScriptToEvaluateOnNewDocument",
            json!({ "source": "window.confirm = () => true;" }),
        );
        b
    }

    fn send(&mut self, method: &str, params: Value, session: bool) -> Value {
        self.next += 1;
        let id = self.next;
        let mut msg = json!({ "id": id, "method": method, "params": params });
        if session {
            msg["sessionId"] = json!(self.session);
        }
        let mut bytes = serde_json::to_vec(&msg).unwrap();
        bytes.push(0);
        self.to.write_all(&bytes).expect("writing to the browser");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let v = match self.from.recv_timeout(left) {
                Ok(v) => v,
                Err(RecvTimeoutError::Timeout) => panic!("the browser did not answer {method}"),
                Err(RecvTimeoutError::Disconnected) => panic!("the browser ended during {method}"),
            };
            if v["id"] == id {
                if let Some(e) = v.get("error") {
                    panic!("{method}: {e}");
                }
                return v["result"].clone();
            }
            self.event(&v);
        }
    }

    /// Keeps what the page reports going wrong.
    fn event(&mut self, v: &Value) {
        match v["method"].as_str() {
            Some("Runtime.exceptionThrown") => self.errors.push(
                v["params"]["exceptionDetails"]["exception"]["description"]
                    .as_str()
                    .or(v["params"]["exceptionDetails"]["text"].as_str())
                    .unwrap_or("an exception")
                    .to_owned(),
            ),
            Some("Runtime.consoleAPICalled") if v["params"]["type"] == "error" => {
                let args: Vec<String> = v["params"]["args"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|a| {
                        a["value"]
                            .as_str()
                            .or(a["description"].as_str())
                            .unwrap_or("")
                            .to_owned()
                    })
                    .collect();
                self.errors.push(args.join(" "));
            }
            _ => {}
        }
    }

    /// A browser-wide command.
    pub fn call(&mut self, method: &str, params: Value) -> Value {
        self.send(method, params, false)
    }

    /// A command to the page.
    pub fn page(&mut self, method: &str, params: Value) -> Value {
        self.send(method, params, true)
    }

    /// Opens `url` and waits for its document to load.
    pub fn goto(&mut self, url: &str) {
        let r = self.page("Page.navigate", json!({ "url": url }));
        if let Some(e) = r.get("errorText").and_then(Value::as_str) {
            panic!("opening {url}: {e}");
        }
        self.until(&format!("{url} loaded"), 30, |b| {
            b.eval("document.readyState === 'complete'") == json!(true)
        });
    }

    /// The value of `expr`, awaited, with [`HELPERS`] in scope.
    pub fn eval(&mut self, expr: &str) -> Value {
        let source = format!("(async () => {{ {HELPERS} return ({expr}); }})()");
        let r = self.page(
            "Runtime.evaluate",
            json!({ "expression": source, "awaitPromise": true, "returnByValue": true }),
        );
        if let Some(e) = r.get("exceptionDetails") {
            panic!("evaluating {expr}: {e}");
        }
        r["result"]["value"].clone()
    }

    /// The page's text, as a user reads it.
    pub fn text(&mut self) -> String {
        self.eval("H.text()")
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    /// Waits up to `secs` for `f`, failing with what the page shows.
    pub fn until(&mut self, what: &str, secs: u64, mut f: impl FnMut(&mut Browser) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while !f(self) {
            if Instant::now() > deadline {
                let text = self.text();
                panic!(
                    "{what} within {secs} s; the page shows:\n{text}\npage errors: {:?}",
                    self.errors
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Waits for the page to show `what`.
    pub fn shows(&mut self, what: &str, secs: u64) {
        self.until(&format!("the page shows {what:?}"), secs, |b| {
            b.text().contains(what)
        });
    }

    /// The button with the text `t`: whether it may be pressed, and its title.
    pub fn button(&mut self, t: &str) -> Option<(bool, String)> {
        let v = self.eval(&format!(
            "(() => {{ const e = H.button({t:?}); return e ? [!e.disabled, e.title ?? ''] : null; }})()"
        ));
        v.as_array()
            .map(|a| (a[0] == json!(true), a[1].as_str().unwrap_or("").to_owned()))
    }

    /// Presses the button with the text `t`, waiting for it to be enabled.
    pub fn click(&mut self, t: &str) {
        self.until(&format!("a button {t:?} to press"), 30, |b| {
            matches!(b.button(t), Some((true, _)))
        });
        self.eval(&format!("H.button({t:?}).click()"));
    }

    /// Types `value` into the field labelled `label`, as a user does.
    pub fn fill(&mut self, label: &str, value: &str) {
        self.until(&format!("a field {label:?}"), 30, |b| {
            b.eval(&format!("!!H.field({label:?})")) == json!(true)
        });
        let done = self.eval(&format!(
            "(() => {{ const e = H.field({label:?}); e.focus(); e.value = {value:?};
               e.dispatchEvent(new Event('input', {{ bubbles: true }}));
               e.dispatchEvent(new Event('change', {{ bubbles: true }}));
               return e.value; }})()"
        ));
        assert_eq!(done, json!(value), "{label} holds what was typed");
    }

    /// Chooses `path` in the file field labelled `label`.
    pub fn choose_file(&mut self, label: &str, path: &Path) {
        let source = format!("(() => {{ {HELPERS} return H.field({label:?}); }})()");
        let r = self.page("Runtime.evaluate", json!({ "expression": source }));
        let object = r["result"]["objectId"]
            .as_str()
            .unwrap_or_else(|| panic!("no file field {label:?}"))
            .to_owned();
        self.page(
            "DOM.setFileInputFiles",
            json!({ "files": [path], "objectId": object }),
        );
    }

    /// Types `text` where the focus is, then presses Enter.
    pub fn type_line(&mut self, text: &str) {
        self.page("Input.insertText", json!({ "text": text }));
        for kind in ["keyDown", "keyUp"] {
            self.page(
                "Input.dispatchKeyEvent",
                json!({ "type": kind, "key": "Enter", "code": "Enter",
                        "windowsVirtualKeyCode": 13, "text": if kind == "keyDown" { "\r" } else { "" } }),
            );
        }
    }

    /// A file downloaded whole, waited for up to `secs`.
    pub fn downloaded(&mut self, secs: u64) -> PathBuf {
        let dir = self.downloads.clone();
        let mut found = None;
        self.until("a download", secs, |_| {
            found = std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(Result::ok)
                .map(|e| e.path())
                .find(|p| {
                    p.extension().is_none_or(|e| e != "crdownload")
                        && p.metadata().is_ok_and(|m| m.len() > 0)
                });
            found.is_some()
        });
        found.unwrap()
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
