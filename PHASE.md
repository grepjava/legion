# Phase tracker

Where Legion stands against the roadmap in [docs/DESIGN.md](docs/DESIGN.md)
(section 11). The design says what to build and why; this file says what is
done and what comes next. Update it in the same commit as the work.

Current phase: **1 — Single node**.

## Phase 0 — Spikes

| Item | State |
|------|-------|
| S1–S6 spikes on FreeBSD 15.1 | Done (DESIGN.md §10) |
| Cargo workspace | Done |
| CI: Linux fmt, clippy, test | Done |
| CI: tests on FreeBSD 15.1 amd64 | Done |
| CI: tests on FreeBSD 15.1 arm64 (emulated) | Added; first run pending |

## Phase 1 — Single node

Exit criterion: a multi-service app with probes and volumes runs and
self-heals on one node.

### Done

- **Store** (`legion-store`): openraft on redb, one file per manager;
  versions, preconditions, watches with resume and compaction; snapshots.
  Tested as a 3-node cluster, although phase 1 needs only one member.
- **API** (`legion-proto`, `legion-manager`): gRPC `Objects` (apply, get,
  list, delete, watch) and `Cluster` (init, status) on port 7470; server-side
  apply; followers forward to the leader.
- **CLI and daemon** (`legion`): `daemon`, `init`, `status`, `apply -f`,
  `get`, `list` (namespaces, label selectors, table/YAML/JSON), `delete`,
  `watch`.
- **Labels**: on every object; `list -l` selects by them.
- **Typed specs and admission** (`legion-api`): Service (with its pod),
  Task, Node, Secret, Config, Volume and Namespace as Rust types; every apply
  is parsed strictly, validated across fields and stored with defaults
  written out (DESIGN.md §7, Admission). `legion diff -f` over a dry-run
  apply.
- **libpod client** (`legion-podman`): pull, create (labels, annotations,
  mounts), start, stop, kill, remove, inspect, list by label, wait, exec,
  logs and events; plus `PodmanService`, which runs and restarts the
  Legion-owned `podman system service` with its own root, runroot and tmpdir.
  Unit tests run against a canned server in CI; `tests/live.rs` runs the pod
  pattern (§6.2) against real Podman and ocijail and is run by hand on a
  FreeBSD host.

### To do, in order

Each step ends with tests that run in CI; steps that need jails, pf or ZFS
also get an integration test run by hand on a FreeBSD host until CI can run
them.

1. **System layer** (`legion-sys`): trait over jail, rctl, zfs, ifconfig,
   route and pfctl; shell-out implementation and a fake for tests.
2. **Agent and executor** (`legion-agent`): the daemon registers its `Node`;
   runs assigned Tasks as an infra jail plus child container jails (§6.2);
   per-container rctl limits (§6.3); reconciles Podman against assignments by
   label on start, on events and periodically; reports Task state.
3. **Service controller and scheduler**: replicated and global services
   create and remove Tasks; single-node placement (resource requests fit);
   restart policy; Task state machine (§7).
4. **Node-local networking** (`legion-net`): `legion0` bridge, per-pod
   epair, IP allocation from the node subnet, pf anchors with the default-block
   ruleset (§5.3).
5. **Service VIPs and endpoints**: allocator for VIPs, Endpoints controller,
   pf tables for load balancing with soft drain and kill-by-key (§5.4).
6. **DNS**: resolver on the bridge gateway for cluster names, forwarding the
   rest (§5.5).
7. **Probes**: liveness and readiness (TCP, HTTP, gRPC from the host; exec in
   the container); readiness feeds Endpoints.
8. **Secrets and configs**: per-task tmpfs, read-only nullfs mounts.
9. **ZFS volumes**: create, mount into containers, keep across task
   restarts.
10. **Exit test**: a multi-service app (for example web + database + cache)
    with probes and a volume; kill containers and the agent and check that it
    comes back.

### Deferred to phase 2

mTLS and PKI (the API is plaintext on loopback until then), `join`, the
dispatcher and agent streams across nodes, WireGuard, and multi-node
scheduling.

Decided: in phase 1 the agent runs in the same process as the manager and
reads its assignments from the store directly, through a narrow interface
that the phase 2 dispatcher stream replaces. This keeps phase 1 to one node
and builds the manager–agent protocol once, when it is needed.

## Later phases

See DESIGN.md §11: 2 Cluster (MVP), 3 K8s-lite, 4 Stateful and operations,
5 Extensions.
