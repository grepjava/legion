# sysutils/legion

The FreeBSD port, ready to copy into a ports tree as `sysutils/legion`.
It builds the `legion` binary with the ports' cargo framework and installs
it with the rc.d script and newsyslog entry from `packaging/freebsd`, the
volume plugin directory, and README.md and DESIGN.md as docs.

`distinfo` is for the latest release, v0.1.5: it holds the checksum of
the release's GitHub tarball, which exists only once the release is
tagged, so between releases it lags `Cargo.lock`.

## For a release

1. Tag the release (`vX.Y.Z`) and push the tag; set `DISTVERSION` here.
2. If `Cargo.lock` changed since `Makefile.crates` was made, make it
   again: `make makesum extract && make cargo-crates > Makefile.crates`
   (or list the lock file's registry packages in its order, which is
   what `cargo-crates` prints).
3. `make makesum` fetches the tarball and every crate and writes
   `distinfo`.
4. Test it: `make stage check-plist stage-qa package` with
   `DEVELOPER=yes`, `portlint -AC`, then in poudriere (`poudriere testport
   -o sysutils/legion`) on FreeBSD 15 for amd64 and aarch64.
5. Submit it to the ports tree as a new port at https://bugs.freebsd.org.

Without root, point the ports framework at directories of your own:
`PORTSDIR`, `DISTDIR`, `WRKDIRPREFIX` and `PORT_DBDIR`, with `BATCH=yes`.
A ports tree newer than the installed packages asks for a newer `rust`
than `pkg` has and starts building it; `NO_DEPENDS=yes` builds with the
installed one instead.

`portlint` warns that `IGNORE` should come earlier: it needs `OSVERSION`,
so it follows `bsd.port.pre.mk`.
