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
| CI: tests on FreeBSD 15.1 arm64 (emulated) | Done (about 55 minutes per run) |

## Phase 1 — Single node

Exit criterion: a multi-service app with probes and volumes runs and
self-heals on one node. **Met** on FreeBSD 15.1 (see the exit test below),
with service traffic through pf VIPs. Phase 1 is done.

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
- **System layer** (`legion-sys`): the `Sys` trait over jails, interfaces,
  routes, rctl, pf anchors and tables, and ZFS; `Shell` runs the FreeBSD
  tools, `FakeSys` models them in memory for tests. `tests/live.rs` checks
  `Shell` on FreeBSD (DESIGN.md §3.6).
- **Agent and executor** (`legion-agent`): `legion daemon` starts Legion's
  Podman service and the agent, which loads `if_bridge`, `if_epair` and
  `pf`, registers its `Node`, and follows its assigned Tasks. Each task gets
  a worker that pulls images, starts the infra jail (refusing it without a
  VNET), runs init containers, starts the containers as child jails with
  rctl limits, applies the restart policy, reports Task state and tears the
  pod down when the task ends or stops. Pods survive agent restarts and are
  adopted; containers no task accounts for are removed. Tested in CI with
  fakes of Podman and the system; `tests/live.rs` runs a real pod on
  FreeBSD.
- **Service controller and scheduler** (`legion-manager`, DESIGN.md §3.3):
  replicated and global services, scaling, replacement of failed tasks with
  backoff, task history, rolling updates (`parallelism`, `order`), drain and
  pause, placement by constraints and requests. `legion list` shows tasks,
  services and nodes with their state. `tests/e2e.rs` (in `legion-agent`)
  runs apply to running pods in one process; on FreeBSD, a Service applied
  through the CLI runs as real pods.
- **Node-local networking** (`legion-net`, DESIGN.md §5.3): each node gets a
  /24 of `10.200.0.0/16` on registration; the agent creates `legion0`, gives
  each pod an epair, the lowest free address and a default route, keeps the
  address across agent restarts and removes the pair at teardown. Pods reach
  each other and the host. `tests/live.rs` in `legion-net` (in a VNET jail)
  and `legion-agent` check it on FreeBSD. Not yet: outbound NAT and pf.
- **Probes** (`legion-agent`): startup, liveness and readiness probes per
  container. HTTP, TCP and gRPC (`grpc.health.v1`) checks run from the host
  against the pod's address; exec checks run in the container. A failing
  startup or liveness probe stops the container, which then restarts under
  the restart policy; readiness sets the Task's `ready`, which the Service
  status counts. Probes start over after a restart and on adoption. Not
  yet: HTTPS probes (rejected); readiness feeding Endpoints comes with
  step 1.
- **Secrets and configs** (`legion-agent`, `legion-sys`): when a pod starts,
  the agent reads the Secrets and Configs its containers name, writes them
  to a tmpfs of the task's own and bind-mounts each file read-only (nullfs)
  into the container that asked for it, at `/run/secrets/<name>`,
  `/run/configs/<name>` or its `target`; values for environment variables
  are passed to Podman. A missing one fails the task, so the controller
  retries with backoff. The tmpfs goes with the pod, and the agent removes
  any it finds for a task it does not run. Values are read at pod start: a
  changed Secret reaches a task when its pod next starts.
- **Volumes** (`legion-agent`): a `Volume` is the ZFS dataset
  `<zfs root>/volumes/<ns>/<name>` (`legion daemon --zfs-root`), created on
  first use with its `size` as quota and bound to that node in the Volume's
  status; tasks elsewhere are rejected. Its mountpoint is bind-mounted into
  each container that mounts it, read-only where asked, and the data
  outlives tasks. Pod `tmpfs` volumes are shared by the pod's containers and
  go with the pod. Not yet: removing datasets of deleted Volumes (they are
  left in place), and scheduling tasks to the node holding their volume
  (one node in phase 1).
- **DNS** (`legion-net::dns`, `legion-agent`): the agent runs a resolver on
  the pod gateway (`10.200.N.1:53`, UDP and TCP). It answers
  `<svc>.<ns>.legion`, `tasks.<svc>.<ns>.legion` and `<svc>.<ns>` with the
  ready tasks' addresses (the VIP once services have one), NXDOMAIN for
  other names under `legion.`, and relays everything else to the host's
  resolvers, so pods resolve outside names without a route out. Each pod
  gets a `resolv.conf` (search `<ns>.legion legion`, `ndots:2`) from its
  tmpfs. The agent follows service endpoints through `Control`, which the
  load balancing step reuses. Not yet: stateful `<svc>-<n>` names.
- **Service VIPs, published ports and outbound NAT** (`legion-manager`,
  `legion-net::lb`, `legion-agent`): the Service controller gives each
  service with ports and `mode: vip` the lowest free address in
  `10.201.0.0/16`, and picks ports from 30000-32767 for `publish: ingress`
  without `publishedPort`; both go in the Service status and stay put. The
  agent enables IP forwarding and keeps the pf anchor `legion/svc` in line
  with the endpoints: an `rdr` per VIP port and per published port (on the
  egress interface) round-robin over a table of the ready tasks, NAT for
  hairpin traffic on `legion0`, and NAT for pods going out. Readiness
  changes only touch tables, so connections drain; connections of tasks
  that are gone are killed by state id. Legion never touches the host's
  main ruleset: pf.conf calls its anchors (`nat-anchor`, `rdr-anchor`,
  `anchor "legion/*"`), and the agent warns when it does not. Tested with
  fakes in CI and against real pf in a VNET jail (`legion-net`'s
  `tests/live.rs`: VIP with hairpin, soft drain, kill), then on weft-bsd's
  own pf: the exit test through VIPs, the published port from another
  machine, and pods reaching the internet over TCP and UDP.
- **Exit test** (`crates/legion/tests/exit.rs`, run by hand on FreeBSD with
  a throwaway ZFS pool): `tests/exit/app.yaml` runs web (two replicas, HTTP
  readiness and TCP liveness probes), db (ZFS volume) and cache (tmpfs
  volume), built from FreeBSD base tools and wired by DNS names that
  resolve to their VIPs, so every call between services goes through pf;
  web is published on port 30080. The test
  kills the db container (it restarts, data kept), deletes the db task (a
  new pod mounts the same dataset), restarts and replaces the cache (its
  tmpfs survives the first, not the second), kills web's process, and
  SIGKILLs the daemon: the pods keep serving, the new daemon adopts the
  same containers, and the count goes on. Passes in about 140 s.

### To do, in order

Each step ends with tests that run in CI; steps that need jails, pf or ZFS
also get an integration test run by hand on a FreeBSD host until CI can run
them.

Nothing left in phase 1.

Decided: in phase 1 the agent runs in the same process as the manager and
reads its assignments from the store directly, through a narrow interface
that the phase 2 dispatcher stream replaces. This keeps phase 1 to one node
and builds the manager–agent protocol once, when it is needed. The
dispatcher replaced it in phase 2.

## Phase 2 — Cluster (MVP)

Exit criterion (DESIGN.md §11): a compose app deploys on 3 nodes, survives
losing any one node, and updates with zero downtime.

There is one FreeBSD host (weft-bsd). Extra nodes are jails on it: VNET
jails for the network (WireGuard, routing), and agents with the fake
runtime for scheduling and failure handling; a second real node is for the
end.

### Done

- **mTLS and PKI** (`legion-pki`, `legion-manager`, `legion`): the first
  `legion daemon` start creates a cluster CA, the manager's certificate and
  `<data-dir>/admin.pem` (certificate, key and CA in one file, mode 0600)
  under `<data-dir>/pki`; later starts reuse them and reissue the
  manager's certificate after two thirds of its 90 days. Certificates name
  an identity (common name and role: manager, worker, admin) and the DNS
  name `legion`, which clients verify whatever address they dial. Every
  connection is mutual TLS: the API takes admins and managers (managers
  forward writes to the leader as themselves), the Raft transport only
  managers. The CLI reads `--identity` (`LEGION_IDENTITY`, default
  `/var/db/legion/admin.pem`). Tested: a 3-manager cluster over mTLS, and
  refusals of no certificate, plaintext, another CA, a worker on the API
  and an admin on Raft; the exit test passes on FreeBSD over mTLS.
- **Join** (`legion-manager::join`, `legion join`, `legion join-token`):
  tokens are `LGN-1-<ca-digest>-<secret>`, one secret per role, kept in the
  store in the reserved namespace `legion-system`, which the API refuses
  and leaves out of lists and watches; `join-token --rotate` replaces one.
  `legion join <token> --manager <addr>` runs on the new node before its
  daemon starts: it reads the CA from the chain the manager presents, keeps
  it only if it matches the token's digest, connects again verifying with
  it, and sends the token with a certificate request for a key that never
  leaves the node. The manager (the leader; others forward) signs it for the
  token's role. Managers also get the CA key and join Raft as learners; the
  leader promotes learners to voters once they have caught up. Tested: two
  managers join and vote, a worker gets a worker certificate without the CA
  key, bad, rotated and foreign tokens fail, the tokens stay out of the
  API; and through the CLI, a second daemon joins and serves.
- **Dispatcher** (`legion-manager::dispatcher`, `legion_agent::GrpcControl`,
  `proto/legion/v1/dispatcher.proto`): the agent's `Control` is a gRPC
  client of any manager, which streams the node's assignments and every
  service's endpoints from its replica (complete sets, sent again after
  each change) and serves the secrets and configs the node's tasks use;
  registration, status reports, volume claims and heartbeats go to the
  leader, forwarded by followers. A worker acts only as itself (its
  certificate's name); a manager names the node it acts for. The leader
  marks a node `down` after three silent heartbeat periods (5 s) and
  `ready` when it hears from it again; a new leader allows three periods
  from taking over. The agent fails over to the next manager when one
  fails and learns the managers from heartbeat replies. `legion daemon` on
  a node that joined as a worker runs only the agent, calling the managers
  recorded at join (or `--manager`), and saves the list it learned on exit;
  a manager node's own agent calls its manager over mTLS too. Task names
  are never reused now (a deleted task's number stays taken, in
  `ServiceStatus.lastTasks`), which the exit test caught once the old
  pod's name came back. Tested: through a follower, a worker registers,
  receives its task and the endpoints, reads only its tasks' secrets,
  reports only on its tasks, goes `down` when silent and `ready` again;
  the agent tests and the exit test (FreeBSD) run through the dispatcher.

### To do, in order

1. **WireGuard mesh and routed pod network**: per-node keys, peers from
   the store, routes to other nodes' pod subnets; pf rules for remote
   backends (§5.4).
2. **Scheduling and failure handling across nodes**: taints, tasks of a
   lost node rescheduled, the routing mesh on every node.
3. **Rollback and compose `stack deploy`**.
4. **Exit test**.

Smaller follow-ups: workers renew their certificates through managers;
`--listen` still defaults to loopback.

## Later phases

See DESIGN.md §11: 2 Cluster (MVP), 3 K8s-lite, 4 Stateful and operations,
5 Extensions.
