//! The supervisor (`legion jail-shim`), one per running container, and the
//! entry into a container's jail that exec sessions use.
//!
//! `supervise <dir> <jid>` starts the container's process in jail `jid`
//! (as its user, in its working directory, with its environment), answers
//! the runtime on stdout with `ok <pid>` or `err <why>`, then writes the
//! process's output to the container's log. When the process ends it
//! removes the jail, so nothing the process left behind keeps running, and
//! writes how it ended to `exit.json`. SIGTERM asks it to send the
//! container's stop signal. It is single-threaded, so forking is safe.
//!
//! `exec <dir> <jid> <tty 0|1> <command...>` enters the jail the same way
//! and becomes the command, on the stdio the runtime gave it.

use std::path::Path;

pub fn main(args: &[String]) -> i32 {
    match args {
        [cmd, dir, jid] if cmd == "supervise" => match jid.parse() {
            Ok(jid) => imp::supervise(Path::new(dir), jid),
            Err(_) => usage(),
        },
        [cmd, dir, jid, tty, argv @ ..] if cmd == "exec" && !argv.is_empty() => match jid.parse() {
            Ok(jid) => imp::exec(Path::new(dir), jid, tty == "1", argv),
            Err(_) => usage(),
        },
        _ => usage(),
    }
}

fn usage() -> i32 {
    eprintln!("usage: jail-shim supervise <dir> <jid> | exec <dir> <jid> <tty> <command...>");
    2
}

#[cfg(not(target_os = "freebsd"))]
mod imp {
    use std::path::Path;

    pub fn supervise(_: &Path, _: i32) -> i32 {
        println!("err jails need FreeBSD");
        1
    }

    pub fn exec(_: &Path, _: i32, _: bool, _: &[String]) -> i32 {
        eprintln!("jails need FreeBSD");
        1
    }
}

#[cfg(target_os = "freebsd")]
mod imp {
    use std::ffi::{CStr, CString};
    use std::io::Write;
    use std::os::fd::RawFd;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use legion_agent::runtime::Output;

    use crate::log::Writer;
    use crate::state::{self, EXIT, Exit, LOG, RECORD, RUN, Record, Run};

    /// How much output a container keeps, per file (two are kept).
    const LOG_MAX: u64 = 8 << 20;
    const PATH: &str = "/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin";

    static STOP: AtomicBool = AtomicBool::new(false);

    extern "C" fn on_term(_: libc::c_int) {
        STOP.store(true, Ordering::Relaxed);
    }

    fn errno() -> String {
        std::io::Error::last_os_error().to_string()
    }

    fn pipe() -> Result<(RawFd, RawFd), String> {
        let mut fds = [0; 2];
        // SAFETY: fds has room for the two descriptors.
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(format!("pipe: {}", errno()));
        }
        Ok((fds[0], fds[1]))
    }

    fn answer(line: &str) {
        let mut out = std::io::stdout();
        let _ = writeln!(out, "{line}");
        let _ = out.flush();
    }

    pub fn supervise(dir: &Path, jid: i32) -> i32 {
        let rec: Record = match state::read(dir, RECORD) {
            Ok(Some(r)) => r,
            Ok(None) => {
                answer("err no container record");
                return 1;
            }
            Err(e) => {
                answer(&format!("err reading the container record: {e}"));
                return 1;
            }
        };
        match run(dir, jid, &rec) {
            Ok(()) => 0,
            Err(why) => {
                let _ = state::write(
                    dir,
                    EXIT,
                    &Exit {
                        code: 127,
                        finished: state::now(),
                        error: Some(why.clone()),
                    },
                );
                answer(&format!("err {why}"));
                1
            }
        }
    }

    fn run(dir: &Path, jid: i32, rec: &Record) -> Result<(), String> {
        let mut log = Writer::open(&dir.join(LOG), LOG_MAX).map_err(|e| format!("the log: {e}"))?;
        // SAFETY: installing handlers; on_term only stores to an atomic.
        unsafe {
            libc::signal(libc::SIGINT, libc::SIG_IGN);
            libc::signal(libc::SIGHUP, libc::SIG_IGN);
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
            libc::signal(libc::SIGTERM, on_term as *const () as libc::sighandler_t);
        }
        let (out_r, out_w) = pipe()?;
        let (err_r, err_w) = pipe()?;
        let (st_r, st_w) = pipe()?;
        // SAFETY: this process has one thread, so the child may do anything
        // before exec.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(format!("fork: {}", errno()));
        }
        if pid == 0 {
            // SAFETY: in the child, which never returns.
            unsafe { child(rec, jid, &[out_w, err_w], st_w) };
        }
        // SAFETY: closing our copies of the child's ends.
        unsafe {
            libc::close(out_w);
            libc::close(err_w);
            libc::close(st_w);
        }
        let failure = read_to_end(st_r);
        if !failure.is_empty() {
            let mut status = 0;
            // SAFETY: reaping the child that failed to exec.
            unsafe { libc::waitpid(pid, &mut status, 0) };
            return Err(String::from_utf8_lossy(&failure).into_owned());
        }
        let run = Run {
            shim: std::process::id(),
            pid: pid as u32,
            jid: jid as u32,
            started: state::now(),
            boot: state::boot_time(),
        };
        state::write(dir, RUN, &run).map_err(|e| format!("writing run.json: {e}"))?;
        answer(&format!("ok {pid}"));
        // Nothing more for the runtime, which may go away.
        if let Ok(null) = std::fs::OpenOptions::new().write(true).open("/dev/null") {
            use std::os::fd::AsRawFd;
            // SAFETY: replacing stdout with /dev/null.
            unsafe { libc::dup2(null.as_raw_fd(), 1) };
        }

        let mut open = [Some(out_r), Some(err_r)];
        let mut ended: Option<(i32, Instant)> = None;
        let mut buf = vec![0u8; 64 << 10];
        loop {
            if STOP.swap(false, Ordering::Relaxed) && ended.is_none() {
                // SAFETY: the child is ours and not yet reaped.
                unsafe { libc::kill(pid, rec.stop_signal) };
            }
            let mut fds: Vec<libc::pollfd> = open
                .iter()
                .flatten()
                .map(|fd| libc::pollfd {
                    fd: *fd,
                    events: libc::POLLIN,
                    revents: 0,
                })
                .collect();
            if !fds.is_empty() {
                // SAFETY: fds is a valid array of pollfd.
                unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, 200) };
            } else {
                std::thread::sleep(Duration::from_millis(200));
            }
            for p in &fds {
                if p.revents == 0 {
                    continue;
                }
                // SAFETY: reading into buf, at most its length.
                let n = unsafe { libc::read(p.fd, buf.as_mut_ptr().cast(), buf.len()) };
                let stream = if Some(p.fd) == open[0] {
                    Output::Stdout
                } else {
                    Output::Stderr
                };
                if n > 0 {
                    let _ = log.write(stream, &buf[..n as usize]);
                } else if n == 0
                    || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                {
                    let i = usize::from(stream == Output::Stderr);
                    // SAFETY: closing a descriptor we own.
                    unsafe { libc::close(p.fd) };
                    open[i] = None;
                }
            }
            if ended.is_none() {
                let mut status = 0;
                // SAFETY: polling our child's status.
                if unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) } == pid {
                    let code = if libc::WIFEXITED(status) {
                        libc::WEXITSTATUS(status)
                    } else if libc::WIFSIGNALED(status) {
                        128 + libc::WTERMSIG(status)
                    } else {
                        255
                    };
                    // What it left behind goes with the jail; then the
                    // pipes close.
                    // SAFETY: removing the container's jail by jid.
                    unsafe { libc::jail_remove(jid) };
                    ended = Some((code, Instant::now()));
                }
            }
            if let Some((code, at)) = ended
                && (open.iter().all(Option::is_none) || at.elapsed() > Duration::from_secs(5))
            {
                let _ = log.flush();
                state::write(
                    dir,
                    EXIT,
                    &Exit {
                        code,
                        finished: state::now(),
                        error: None,
                    },
                )
                .map_err(|e| format!("writing exit.json: {e}"))?;
                return Ok(());
            }
        }
    }

    fn read_to_end(fd: RawFd) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            // SAFETY: reading into buf.
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                out.extend_from_slice(&buf[..n as usize]);
            } else if n == 0
                || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
            {
                break;
            }
        }
        // SAFETY: closing our descriptor.
        unsafe { libc::close(fd) };
        out
    }

    /// The forked child: into the jail, then the container's command.
    ///
    /// # Safety
    /// Only in a child just forked from a single-threaded process.
    unsafe fn child(rec: &Record, jid: i32, out: &[RawFd; 2], status: RawFd) -> ! {
        let fail = |msg: String| -> ! {
            // SAFETY: writing the reason to the status pipe, then leaving.
            unsafe {
                libc::write(status, msg.as_ptr().cast(), msg.len());
                libc::_exit(127)
            }
        };
        // SAFETY: back to default dispositions for the command.
        unsafe {
            for s in [libc::SIGINT, libc::SIGHUP, libc::SIGPIPE, libc::SIGTERM] {
                libc::signal(s, libc::SIG_DFL);
            }
        }
        let prepared = match enter(rec, jid, false) {
            Ok(p) => p,
            Err(e) => fail(e),
        };
        // SAFETY: the command's stdio: no input, output to the pipes.
        unsafe {
            let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
            if null >= 0 {
                libc::dup2(null, 0);
            }
            libc::dup2(out[0], 1);
            libc::dup2(out[1], 2);
        }
        prepared.exec();
        fail(format!("{}: {}", rec.argv[0], errno()))
    }

    pub fn exec(dir: &Path, jid: i32, tty: bool, argv: &[String]) -> i32 {
        let mut rec: Record = match state::read(dir, RECORD) {
            Ok(Some(r)) => r,
            _ => {
                eprintln!("no container record in {}", dir.display());
                return 1;
            }
        };
        rec.argv = argv.to_vec();
        if tty && !rec.env.iter().any(|e| e.starts_with("TERM=")) {
            rec.env.push("TERM=xterm".to_owned());
        }
        let prepared = match enter(&rec, jid, tty) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("{e}");
                return 126;
            }
        };
        prepared.exec();
        eprintln!("{}: {}", argv[0], errno());
        127
    }

    struct Prepared {
        path: CString,
        argv: Vec<CString>,
        env: Vec<CString>,
    }

    impl Prepared {
        /// Returns only if exec fails.
        fn exec(&self) {
            let mut argv: Vec<*const libc::c_char> = self.argv.iter().map(|a| a.as_ptr()).collect();
            argv.push(std::ptr::null());
            let mut env: Vec<*const libc::c_char> = self.env.iter().map(|a| a.as_ptr()).collect();
            env.push(std::ptr::null());
            // SAFETY: NUL-terminated arrays of valid C strings.
            unsafe { libc::execve(self.path.as_ptr(), argv.as_ptr(), env.as_ptr()) };
        }
    }

    fn cstring(s: &str) -> Result<CString, String> {
        CString::new(s).map_err(|_| format!("a NUL in {s:?}"))
    }

    /// Into jail `jid` as the container's user, in its working directory;
    /// the command to exec and its environment. With `tty`, the process
    /// leads a new session with stdin as its terminal.
    fn enter(rec: &Record, jid: i32, tty: bool) -> Result<Prepared, String> {
        // SAFETY: plain system calls.
        unsafe {
            if libc::jail_attach(jid) != 0 {
                return Err(format!("entering jail {jid}: {}", errno()));
            }
            if tty {
                libc::setsid();
                libc::ioctl(0, libc::TIOCSCTTY, 0);
            }
        }
        let user = resolve_user(&rec.user)?;
        // SAFETY: dropping privileges: groups, then gid, then uid.
        unsafe {
            let groups_set = match &user.name {
                Some(n) => libc::initgroups(n.as_ptr(), user.gid as _) == 0,
                None => libc::setgroups(1, &user.gid) == 0,
            };
            if !groups_set {
                return Err(format!("setting the groups of {}: {}", rec.user, errno()));
            }
            if libc::setgid(user.gid) != 0 || libc::setuid(user.uid) != 0 {
                return Err(format!("becoming {}: {}", rec.user, errno()));
            }
        }
        let wd = cstring(&rec.work_dir)?;
        // SAFETY: wd is a valid C string.
        if unsafe { libc::chdir(wd.as_ptr()) } != 0 {
            return Err(format!("working directory {}: {}", rec.work_dir, errno()));
        }
        let mut env = rec.env.clone();
        let has = |env: &[String], k: &str| env.iter().any(|e| e.split('=').next() == Some(k));
        if !has(&env, "PATH") {
            env.push(format!("PATH={PATH}"));
        }
        if !has(&env, "HOME") {
            env.push(format!("HOME={}", user.home));
        }
        let path_var = env
            .iter()
            .find_map(|e| e.strip_prefix("PATH="))
            .unwrap_or(PATH)
            .to_owned();
        let program = rec.argv.first().ok_or("no command")?;
        let path = if program.contains('/') {
            program.clone()
        } else {
            path_var
                .split(':')
                .map(|d| format!("{}/{program}", if d.is_empty() { "." } else { d }))
                .find(|p| executable(p))
                .ok_or_else(|| format!("{program}: not found in {path_var}"))?
        };
        Ok(Prepared {
            path: cstring(&path)?,
            argv: rec
                .argv
                .iter()
                .map(|a| cstring(a))
                .collect::<Result<_, _>>()?,
            env: env.iter().map(|e| cstring(e)).collect::<Result<_, _>>()?,
        })
    }

    fn executable(path: &str) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }

    struct User {
        uid: libc::uid_t,
        gid: libc::gid_t,
        /// For the supplementary groups, when the jail knows the user.
        name: Option<CString>,
        home: String,
    }

    /// `user[:group]` by name or number, from the jail's own databases.
    fn resolve_user(spec: &str) -> Result<User, String> {
        let (user, group) = match spec.split_once(':') {
            Some((u, g)) => (u, Some(g)),
            None => (spec, None),
        };
        let user = if user.is_empty() { "0" } else { user };
        // SAFETY: getpwnam/getpwuid return a pointer to static storage or
        // null; it is read at once.
        let pw = unsafe {
            match user.parse::<libc::uid_t>() {
                Ok(uid) => libc::getpwuid(uid),
                Err(_) => libc::getpwnam(cstring(user)?.as_ptr()),
            }
        };
        let (uid, mut gid, name, home) = if pw.is_null() {
            let uid = user
                .parse()
                .map_err(|_| format!("no user {user:?} in the image"))?;
            (uid, 0, None, "/".to_owned())
        } else {
            // SAFETY: pw is a valid passwd entry.
            unsafe {
                let p = &*pw;
                (
                    p.pw_uid,
                    p.pw_gid,
                    Some(CStr::from_ptr(p.pw_name).to_owned()),
                    CStr::from_ptr(p.pw_dir).to_string_lossy().into_owned(),
                )
            }
        };
        if let Some(g) = group {
            gid = match g.parse() {
                Ok(n) => n,
                Err(_) => {
                    // SAFETY: as above, for the group database.
                    let gr = unsafe { libc::getgrnam(cstring(g)?.as_ptr()) };
                    if gr.is_null() {
                        return Err(format!("no group {g:?} in the image"));
                    }
                    // SAFETY: gr is a valid group entry.
                    unsafe { (*gr).gr_gid }
                }
            };
        }
        Ok(User {
            uid,
            gid,
            name,
            home,
        })
    }
}
