# Test fixtures

## jailer-arch-arm64.tar

The arm64 half of `handoff.rs`'s `a_jailer_index_runs_each_nodes_platform`,
built by jailer on an arm64 host because the lab is amd64 only. It never runs
here: the test loads it into jailer, pushes it in an index with an amd64 build,
and checks that amd64 nodes run the other half.

- Built by jailer 33ee0d7 (Legion v0.1.18) on FreeBSD 15.1-RELEASE-p6 aarch64,
  from `FROM scratch` and `COPY arch /arch`, `/arch` holding `arm64`, and
  exported with `jailer export` (an OCI archive).
- Archive sha256: `e32d277135d8d5bbb33562d6857ec61ee0defc728c90a6f7dcdab822af718a6b`
- Manifest: `sha256:807336f1458d4bc925f89ef1a21c7a011ffb36f87ccf72a4098d71c034aafc4f`
- Config: `sha256:b8473d0e081a6789fdb854660bad3362f32999c282e4fb0622abc56e76be4cc0`,
  `freebsd`/`arm64`

To make it again, on a FreeBSD arm64 host:

```sh
printf 'arm64\n' > arch
printf 'FROM scratch\nCOPY arch /arch\n' > Containerfile
jailer build -t localhost/jailer-arch:arm64 .
jailer export -o jailer-arch-arm64.tar localhost/jailer-arch:arm64
```
