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
        [cmd, dir, jid, tty, argv @ ..] if cmd == "exec" && !argv.is_empty() => {
            match (jid.parse(), Terminal::parse(tty)) {
                (Ok(jid), Some(tty)) => imp::exec(Path::new(dir), jid, tty, argv),
                _ => usage(),
            }
        }
        _ => usage(),
    }
}

/// The terminal an `exec` runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Terminal {
    /// None.
    None,
    /// A new one, on stdin: the command leads a session with it as its
    /// controlling terminal.
    Own,
    /// The caller's, inherited: the command stays in the caller's session
    /// and group, as a shell's foreground child does.
    Caller,
}

impl Terminal {
    pub fn arg(self) -> &'static str {
        match self {
            Terminal::None => "0",
            Terminal::Own => "1",
            Terminal::Caller => "2",
        }
    }

    pub fn parse(arg: &str) -> Option<Self> {
        match arg {
            "0" => Some(Terminal::None),
            "1" => Some(Terminal::Own),
            "2" => Some(Terminal::Caller),
            _ => None,
        }
    }
}

fn usage() -> i32 {
    eprintln!("usage: jail-shim supervise <dir> <jid> | exec <dir> <jid> <0|1|2> <command...>");
    2
}

/// An environment entry as an error may name it: by its name alone.
#[cfg_attr(not(target_os = "freebsd"), allow(dead_code))]
fn env_entry(entry: &str) -> String {
    match entry.split_once('=') {
        Some((name, _)) if !name.contains('\0') => format!("environment variable {name}"),
        _ => "an environment variable".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_environment_entry_is_named_without_its_value() {
        let named = super::env_entry("TOKEN=s3cr\0et");
        assert_eq!(named, "environment variable TOKEN");
        for entry in ["s3cr\0et", "s3cr\0et=more"] {
            assert_eq!(super::env_entry(entry), "an environment variable");
        }
    }
}

#[cfg(not(target_os = "freebsd"))]
mod imp {
    use std::path::Path;

    pub fn supervise(_: &Path, _: i32) -> i32 {
        println!("err jails need FreeBSD");
        1
    }

    pub fn exec(_: &Path, _: i32, _: super::Terminal, _: &[String]) -> i32 {
        eprintln!("jails need FreeBSD");
        1
    }
}

#[cfg(target_os = "freebsd")]
mod imp {
    use std::ffi::CString;
    use std::io::Write;
    use std::os::fd::RawFd;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use legion_runtime::Output;

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
        // One started with stdio closed may have been given 0, 1 or 2 for
        // any of its descriptors: each is moved above 2 before the
        // command's stdio is set up, as `dup2` onto itself would leave it
        // close-on-exec and one onto another's number would close that.
        let above = |fd: libc::c_int| match fd {
            // SAFETY: duplicating an open descriptor.
            0..=2 => unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) },
            fd => fd,
        };
        // The status pipe too: the command's stdio would replace it, and a
        // failed exec would then report to the command's output.
        let status = above(status);
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
            let null = above(libc::open(
                c"/dev/null".as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            ));
            let (stdout, stderr) = (above(out[0]), above(out[1]));
            if null >= 0 {
                libc::dup2(null, 0);
                libc::close(null);
            }
            libc::dup2(stdout, 1);
            libc::dup2(stderr, 2);
        }
        prepared.exec();
        fail(format!("{}: {}", rec.argv[0], errno()))
    }

    pub fn exec(dir: &Path, jid: i32, tty: super::Terminal, argv: &[String]) -> i32 {
        let mut rec: Record = match state::read(dir, RECORD) {
            Ok(Some(r)) => r,
            _ => {
                eprintln!("no container record in {}", dir.display());
                return 1;
            }
        };
        rec.argv = argv.to_vec();
        if tty != super::Terminal::None && !rec.env.iter().any(|e| e.starts_with("TERM=")) {
            rec.env.push("TERM=xterm".to_owned());
        }
        let prepared = match enter(&rec, jid, tty == super::Terminal::Own) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("{e}");
                return 126;
            }
        };
        if tty == super::Terminal::Own {
            return run_and_drain(&prepared, &argv[0]);
        }
        prepared.exec();
        eprintln!("{}: {}", argv[0], errno());
        127
    }

    /// With a terminal of its own, the command runs in a child while this
    /// stays its session's leader: a leader's exit revokes the terminal,
    /// and output not yet read from it with it, so a command that wrote and
    /// ended at once could lose all it wrote. Waits for the command, lets
    /// the reader take what it wrote (`tcdrain`, for at most a few seconds:
    /// a reader gone takes nothing), and exits as the command did, a signal
    /// as a shell gives it (128 and the signal).
    fn run_and_drain(prepared: &Prepared, name: &str) -> i32 {
        extern "C" fn wake(_: libc::c_int) {}
        const TERMINAL: [libc::c_int; 3] = [libc::SIGINT, libc::SIGQUIT, libc::SIGHUP];
        // SAFETY: plain system calls; this process is single-threaded, so
        // the child may run anything before its exec.
        unsafe {
            // The terminal's signals are the command's to take: they reach
            // its group, this process included.
            for s in TERMINAL {
                libc::signal(s, libc::SIG_IGN);
            }
            let child = libc::fork();
            if child < 0 {
                eprintln!("fork: {}", errno());
                return 126;
            }
            if child == 0 {
                for s in TERMINAL {
                    libc::signal(s, libc::SIG_DFL);
                }
                prepared.exec();
                eprintln!("{name}: {}", errno());
                libc::_exit(127);
            }
            let mut status = 0;
            while libc::waitpid(child, &mut status, 0) < 0 {
                if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                    return 1;
                }
            }
            libc::signal(libc::SIGALRM, wake as *const () as libc::sighandler_t);
            libc::alarm(5);
            libc::tcdrain(1);
            libc::alarm(0);
            if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else if libc::WIFSIGNALED(status) {
                128 + libc::WTERMSIG(status)
            } else {
                1
            }
        }
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

    /// The text of `path` if it is a plain file of at most 1 MiB.
    fn image_file(path: &str) -> Option<String> {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        const MAX: u64 = 1 << 20;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .ok()?;
        let meta = f.metadata().ok()?;
        if !meta.is_file() || meta.len() > MAX {
            return None;
        }
        let mut text = String::new();
        f.take(MAX).read_to_string(&mut text).ok()?;
        Some(text)
    }

    /// `s`, called `what` if it cannot be: never shown, as it may hold a
    /// Secret, and the error reaches the task's status.
    fn cstring(s: &str, what: &str) -> Result<CString, String> {
        CString::new(s).map_err(|_| format!("a NUL in {what}"))
    }

    /// Into jail `jid` as the container's user, in its working directory;
    /// the command to exec and its environment. With `session`, the
    /// process leads a new session with stdin, a terminal of its own, as
    /// its controlling terminal.
    fn enter(rec: &Record, jid: i32, session: bool) -> Result<Prepared, String> {
        // SAFETY: plain system calls.
        unsafe {
            if libc::jail_attach(jid) != 0 {
                return Err(format!("entering jail {jid}: {}", errno()));
            }
            if session {
                libc::setsid();
                libc::ioctl(0, libc::TIOCSCTTY, 0);
            }
        }
        // In the jail, so these are the image's own, and only plain files
        // of a sane size: a FIFO would hang the start, `/dev/zero` fill
        // memory.
        let read = |p: &str| image_file(p).unwrap_or_default();
        let user = crate::users::resolve(&rec.user, &read("/etc/passwd"), &read("/etc/group"))?;
        let groups: Vec<libc::gid_t> = user.groups.iter().map(|&g| g as _).collect();
        // SAFETY: dropping privileges: groups, then gid, then uid.
        unsafe {
            if libc::setgroups(groups.len() as _, groups.as_ptr()) != 0 {
                return Err(format!("setting the groups of {}: {}", rec.user, errno()));
            }
            if libc::setgid(user.gid as _) != 0 || libc::setuid(user.uid as _) != 0 {
                return Err(format!("becoming {}: {}", rec.user, errno()));
            }
        }
        let wd = cstring(&rec.work_dir, "the working directory")?;
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
                .ok_or_else(|| format!("{program}: not found in the PATH"))?
        };
        Ok(Prepared {
            path: cstring(&path, "the command")?,
            argv: rec
                .argv
                .iter()
                .map(|a| cstring(a, "the command's arguments"))
                .collect::<Result<_, _>>()?,
            env: env
                .iter()
                .map(|e| cstring(e, &super::env_entry(e)))
                .collect::<Result<_, _>>()?,
        })
    }

    fn executable(path: &str) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
}
