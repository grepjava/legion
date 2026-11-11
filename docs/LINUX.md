# Linux images matrix

Which Linux application images run on Legion, through FreeBSD's Linux ABI
(linux(4)), and which don't, with why (PLAN.md phase 8). How Linux images
run is in DESIGN.md section 6.6; this page is the table.

Each row is a Service on a node started with `legion daemon --linux`,
doing real work in its task: `linux_application_images` in
`crates/legion/tests/linux.rs`, run by the qualification on FreeBSD 15.1
(amd64, in KVM). The test also keeps the failures true: a row that stops
failing as described fails the test, so this page is changed with it.

| Image | What it exercises | Result |
|---|---|---|
| `alpine:3.20` | musl, BusyBox | works |
| `ubuntu:24.04` | glibc, Perl | works |
| `python:3.12-slim` | glibc, OpenSSL, SQLite, `multiprocessing` (processes, POSIX semaphores) | works |
| `redis:7-alpine` | an event loop on epoll | works |
| `busybox:1.37` | a static program | works, as `--linux` sets `kern.elf64.fallback_brand=3` |
| `nginx:1.27-alpine` | an HTTP server with worker processes | works with `events { accept_mutex on; }` or `worker_processes 1`; see below |
| `node:22-alpine` | V8, libuv | does not run: its main thread spins from the start, running no script |
| `postgres:17-alpine` (and 15) | the image's entrypoint, `gosu` | does not run: `signalfd() failed`, as the ABI has no signalfd(2) |

## What the failures come from

- **Static programs** (Go's, BusyBox, `gosu`) name no interpreter, so the
  kernel can't tell they are Linux's and refuses them ("Exec format
  error") unless `kern.elf64.fallback_brand` is 3. `--linux` sets it where
  the host left it unset; a host that chose another value keeps it, the
  daemon warns, and `legion doctor` reports `linux.static`.
- **nginx**: the ABI's `epoll_ctl` refuses `EPOLLEXCLUSIVE` with EINVAL.
  nginx asks for it on its listening sockets when it has more than one
  worker and `accept_mutex` is off, its defaults on a host with several
  CPUs: every worker exits (`epoll_ctl(1, 6) failed (22: Invalid
  argument)`), and the master goes on accepting connections nothing
  answers. With `accept_mutex on`, or one worker, it serves. It also logs
  `io_setup() failed (38: Function not implemented)`: the ABI has no Linux
  AIO, and nginx falls back from it.
- **Postgres**: Postgres 15 and later wait on `signalfd(2)`, which the ABI
  lacks. Run the FreeBSD Postgres package instead; Legion's stateful tests
  do (`crates/legion/tests/stateful/Containerfile`).
- **Node 22** starts (`node --version` answers) but spins in its main
  thread before running anything, in user space; why is not known yet.

The ABI has no namespaces, cgroups, seccomp or io_uring either (DESIGN.md
6.6), so images that run containers or sandboxes of their own don't work.

## jailer

jailer's `run --linux` uses the same runtime (`legion_jail`) and the same
Linux ABI, so an image's ABI failures are the same there; its networking
and lifecycle are its own (see jailer's docs/LOCAL-AND-LEGION.md). From
the jailer release that pins Legion 0.1.18 it calls
`legion_jail::enable_linux`; before it, static Linux programs need
`kern.elf64.fallback_brand=3` set by hand. Only nginx was checked with
jailer: it fails as above, and serves with `accept_mutex on` or
`worker_processes 1`. jailer keeps no matrix of its own (decided 10
November 2026); its one Linux test runs static BusyBox, from that release.
