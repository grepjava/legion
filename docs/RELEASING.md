# Releasing Legion

The steps of a release, in order, and which of them a machine does. A step
marked **manual** depends on someone doing it, so check it off for each
release rather than assume it. Replace `X.Y.Z` throughout.

| Step | | Automated by |
|---|---|---|
| 1. Bump the version | required | manual |
| 2. Qualify | required | `scripts/qualify` or `scripts/lab`, started by hand |
| 3. Tag | required | manual |
| 4. Update the port's `distinfo` | required | manual |
| 5. `poudriere testport` | required | manual |
| 6. Build the tarballs | required | `packaging/freebsd/release.sh`, started by hand on each architecture |
| 7. Write the notes | required | manual |
| 8. Publish | required | manual |
| 9. Tell jailer | required | manual |

CI runs the unit tests on Linux, and on FreeBSD 15.1 amd64 and emulated arm64,
for every push. It runs none of the privileged tests, so a green CI is not a
qualification.

## 1. Bump the version

This comes before the run that qualifies the release, as the record has to name
the tagged commit: bump, push, run, and tag if the record is clean.

Set `X.Y.Z` in:

- `Cargo.toml` (`[workspace.package]`), then `cargo check` to update `Cargo.lock`;
- `packaging/freebsd/port/Makefile` (`DISTVERSION`);
- `PHASE.md`: a line on the release in its paragraph of tagged versions.

Commit as `Version X.Y.Z, the port with it`, and push.

## 2. Qualify

On a FreeBSD host of the release's architecture, from a tree of the exact
commit to be released:

```sh
echo "$(git rev-parse --short HEAD)" > .qualify-revision
scripts/qualify --manifest scripts/legion.qualify --profile shared \
    --pool POOL --out ~/qualify/X.Y.Z
```

That `shared` profile is safe on a host that does other work; `isolated` and
`cluster` publish ports or need several hosts, and each profile includes those
before it. `scripts/lab` gives disposable FreeBSD VMs for them, shared with
jailer, and one run covers everything:

```sh
scripts/lab run --manifest scripts/legion.qualify --profile cluster \
    --vm lab1 --also lab2 --also lab3 --setup scripts/legion.labsetup \
    --copy IMAGE:/var/tmp/legion-pg.oci.tar
```

It resets the VMs, sets up the servers the tests need, runs the commit at HEAD
and brings the record back. IMAGE is `crates/legion/tests/stateful/Containerfile`
built and exported by jailer. A record's `vm` field says the host was a VM, and
`image` which base.

Keep each run's `result.json`. A release is ready when every test in the
manifest has a recorded result on each supported platform: passed, or not run
for a reason stated in the notes. Stop for any of these:

- a test failed, or was skipped;
- `invalid` is true: the host ran out of memory, and the run says nothing;
- `leftovers` is not empty: a test left something behind.

A change after the run needs a new run. So does a run on a tree whose
`.qualify-revision` is not the tagged commit.

## 3. Tag

```sh
git tag -a vX.Y.Z -m "Legion X.Y.Z"
git push origin vX.Y.Z
```

jailer depends on Legion's crates by tag, so a tag is a release to it even
before the tarballs exist.

## 4. Update the port's `distinfo`

Fetch the tag's tarball from GitHub, as the port does. Put its SHA-256 and size
in `packaging/freebsd/port/distinfo`, with a new `TIMESTAMP`, and in the port's
`README.md`. If `Cargo.lock` changed, regenerate `Makefile.crates` and their
`distinfo` lines with `make cargo-crates` and `make makesum`. Commit as
`Port: distinfo for vX.Y.Z`, and push.

## 5. `poudriere testport`

```sh
poudriere testport -j JAIL -p PORTS_TREE -o sysutils/legion
```

Run it on 15.1 amd64 with the port from this tree overlaid. Once arm64 is
qualified, build it on 15.1 arm64 too. A release that skips this says so in its
notes, along with the last version that passed.

## 6. Build the tarballs

On a FreeBSD 15 host of each architecture, from `git archive` of the tag:

```sh
packaging/freebsd/release.sh ~/dist
```

It writes `legion-X.Y.Z-freebsd15-ARCH.tar.xz` and its `.sha256`. Unpack one
and run `bin/legion --version` before publishing it. amd64 is published today;
arm64 is built from the same script when a host is available, and the notes say
when it is not included.

## 7. Write the notes

State, for people upgrading:

- what changed, and anything they must do;
- what was qualified: the platforms, the profiles, and the commit;
- what was not: tests not run, `testport` or arm64 if skipped.

## 8. Publish

```sh
gh release create vX.Y.Z --repo grepjava/legion --verify-tag --latest \
    --title "Legion X.Y.Z" --notes-file NOTES.md \
    legion-X.Y.Z-freebsd15-amd64.tar.xz \
    legion-X.Y.Z-freebsd15-amd64.tar.xz.sha256
```

Download the published tarball and check it against its `.sha256`.

## 9. Tell jailer

jailer pins Legion's crates by tag. Tell its maintainers what changed in the
crates it uses, above all a change of behaviour, so that it moves to the tag
knowingly.
