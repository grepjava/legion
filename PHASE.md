# Phase tracker

Where Legion stands against the roadmap in [docs/DESIGN.md](docs/DESIGN.md)
(section 11). The design says what to build and why; this file says what is
done and what comes next. Update it in the same commit as the work.

Current phase: **3 — K8s-lite** (phases 1 and 2 met their exit criteria).

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
  left in place). Scheduling tasks to the node holding their volume came
  in phase 2.
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
  egress interface; since phase 2 on every interface of the node but the
  bridge, pod links, the mesh and loopback, rechecked every 30 s) round-robin over a table of the ready tasks, NAT for
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

Most of it was built on one FreeBSD host (weft-bsd), with extra nodes as
jails on it: VNET jails for the network (WireGuard, routing), and agents
with the fake runtime for scheduling and failure handling. The exit test
runs real daemons on three machines: weft-bsd, a bhyve guest on it, and a
remote FreeBSD server reached through a WireGuard site tunnel.

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
- **WireGuard mesh** (`legion-net::mesh`, the dispatcher's `Peers` stream):
  each node makes its key on first start, registers the public key and its
  endpoint, and keeps `wg0` (address `100.64.<n>.1`, from its pod subnet)
  with every other node as a peer; one route sends the pod CIDR into `wg0`
  and WireGuard's allowed IPs pick the node. Traffic from outside the pod
  network going to another node's pod (a published port's) is NATed to the
  node's `wg0` address so replies return through it. Tested: the agent
  makes and keeps its key, sets up `wg0` and follows nodes appearing and
  going; and on FreeBSD, three VNET jails as nodes run the mesh and load
  balancing code: pods reach pods on other nodes, a VIP balances over a
  local and a remote task, a published port on a node without tasks
  reaches both (and nothing connects without the NAT rule), and a node
  removed from the peers is unreachable. The exit test passes with the mesh
  up on its single node.
- **Failure handling and taints** (`legion-manager` controller): the tasks
  of a node that goes `down` are orphaned and told to stop, so they leave
  the endpoints and their slots run elsewhere at once, whatever the restart
  policy; the node removes their pods when it is back. Nodes take taints
  (`NoSchedule`, `PreferNoSchedule`, `NoExecute`) that services tolerate
  through `placement.tolerations`. Tasks mounting a claimed Volume go only
  to the node holding it, and wait for it while it is down. Tested in the
  planner and in `legion-agent`'s `tests/cluster.rs`: one manager and three
  agents with fake runtimes, each through the dispatcher. Tasks spread one
  per node; every node balances the VIP and the published port over all
  of them and has the others as mesh peers (the routing mesh on every
  node). Then a node goes silent: its task is orphaned and replaced on
  the others, which stop balancing to it. It comes back and removes the
  lost pod, and a `NoExecute` taint empties a node.
- **Rolling update monitoring and rollback** (`legion-manager` controller,
  `legion rollback`): updates honour `delay`, wait for readiness, watch new
  tasks for `monitor`, and on a failure pause, continue or roll back as
  `failureAction` says; the service status shows the update's state and
  why it stopped (`legion list services` has an UPDATE column).
  `legion rollback` applies the spec the latest update came from. Tested in
  the planner (completion, delay, pause, automatic and manual rollback) and
  through the CLI.
- **Compose stacks** (`legion_api::compose`, `legion stack deploy`,
  `convert`, `rm`): compose files become Services, Volumes, Secrets and
  Configs labelled `legion.stack`, in a namespace named after the stack;
  DESIGN.md §8 lists what maps and what is warned about (`--strict`
  refuses). Tested: a compose file with most keys converted, interpolation,
  errors that name the place; and through the CLI, deploy (again
  unchanged), `--strict`, convert, `--prune` and `rm`.
- **Exit test** (`crates/legion/tests/cluster.rs`, ignored; run as root on
  a FreeBSD host, see its docs): passes on three real nodes, all managers:
  weft-bsd, a bhyve guest on it, and a remote server about 225 ms away
  through a WireGuard site tunnel. It starts this host's daemon, copies its
  own `legion` (stripped) to the others over ssh and joins them, deploys
  `tests/cluster/compose.yaml` (web published on 30080 asks api for its
  name through api's VIP), and checks that tasks spread one per node and
  that every node answers on the published port and reaches every task.
  It then updates web start-first under load (35 requests, none failed),
  powers the bhyve node off (3 of 23 requests failed in the 13 s before
  the node was marked down; the app was back on the other two after 17 s,
  with no failure after), and updates again on the two left.

  It found, and these were fixed: pods' `lo0` had no 127.0.0.1 (a new
  VNET's loopback gets only `::1`); a manager joining while another was
  being promoted was refused (it now waits for the membership change);
  openraft's replication timeout is the heartbeat interval, so at 250 ms
  managers 225 ms apart never kept a leader (now 1 s, elections after 5 to
  10 s); a write without a quorum hung forever (now 30 s, then an error);
  and memory limits denied on the virtual memory of all a container's
  processes, so `memory: 128M` failed to fork under light load (now
  `memoryuse:sigkill`, DESIGN.md §6.3). A review found, also fixed with
  tests: nodes registering at once got the same subnet (now reserved in
  one object), two services could claim the same published port (the
  older keeps it; the other reports it in `status.conflicts`), and a
  failed connection cleanup in the load balancer was not retried.

  Learned on the way:
  - Whole nodes cannot be VNET jails: inside a jail, ocijail fails
    creating containers ("removing rctl rules ... Operation not
    permitted"; FreeBSD has no jail parameter allowing rctl), and
    Legion's own rctl limits would fail the same way. Nodes are bhyve
    guests or real machines.
  - Nodes must reach each other's advertised addresses (DESIGN.md §5.3,
    Reachability); a node behind NAT joins through a site tunnel.
  - The Podman package's `storage.conf` defaults to the ZFS driver, so on
    a UFS host Legion's Podman exits at start (125) until the driver is
    set to `vfs`; rctl needs `kern.racct.enable=1` in `loader.conf`.
  - A host cannot reach its own published ports (DESIGN.md §5.4), so the
    test asks for this host's from another node.

### To do, in order

Nothing left in phase 2.

Smaller follow-ups: workers renew their certificates through managers;
`--listen` still defaults to loopback; a worker without `--advertise` has
no WireGuard endpoint (it could use the address it reaches managers from);
placement preferences (`spread`) are accepted but not used yet; the daemon takes Podman's storage driver from `storage.conf`
(it could choose `vfs` itself when its root is not on ZFS); a node's
advertised WireGuard endpoint replaces the one WireGuard learned, so a
node behind NAT cannot be reached by its peers' replies alone; node subnets
are reserved by name (`Config/legion-system/node-subnets`) and a deleted
Node's reservation is not released; on a host without
`kern.racct.enable=1` (a loader tunable) rctl limits fail and the agent
only warns, so the pod runs unlimited (it could refuse to start, or report
it in the Node's status); a lost node's tasks keep getting a share of
new connections until the node is marked down (three missed 5 s
heartbeats), which a faster down signal or per-connection retry could
shorten.

## Phase 3 — K8s-lite

Exit criterion (DESIGN.md §11): two teams share a cluster with isolation
enforced.

Already there from phase 1: namespaces scope names and DNS
(`<svc>.<ns>.legion`), and startup probes run.

### Done

- **RBAC and users** (`legion-manager::authz`, `legion_api::rbac`,
  `legion user create`): certificates for people carry the role `user`;
  `legion user create <name> [--role R -n NS] [-o FILE] [--days N]` makes
  the key where it runs, has a manager sign a request for it (`IssueUser`,
  cluster admin only) and writes a bundle (mode 0600, never overwritten),
  plus a RoleBinding named after the user. A `RoleBinding` gives `view`,
  `edit` or `admin` in its namespace (DESIGN.md §9); users never touch
  cluster-scoped objects, `init` or join tokens, and lists and watches
  across namespaces show only what they may read. The manager a request
  reaches first decides, before forwarding. `stack deploy` by a user
  skips creating the namespace. Tested: through the API, each role's
  reads and writes, Secrets, other namespaces, cluster-scoped kinds,
  filtered lists and watches, revocation by deleting the binding, and who
  may issue certificates; through the CLI, `user create` and a user
  applying, listing and deploying a stack in their namespace only.

### To do, in order

The first three are what the exit criterion rests on: who may do what,
what may talk to what, and how much each team may use; RBAC is done.

1. **Namespace isolation and network policy**: by default a task reaches
   tasks of its own namespace and the node's DNS only; `NetworkPolicy`
   allows more by namespace, labels and ports, compiled to the
   `legion/policy` anchor and enforced on the destination node
   (DESIGN.md §5.6).
2. **Namespace quotas**: limits on the requests, tasks and volumes a
   namespace may hold, checked at admission and by the scheduler.
3. **`logs`, `exec`, `port-forward`**: streamed from the agent through a
   manager, under RBAC (`exec` and `port-forward` need `edit`).
4. **Init containers**: run in order to completion before the main ones.
5. **Jobs and CronJobs**: run to completion, retries and deadlines; cron
   schedules, concurrency policy, history limits.
6. **Disruption budgets**: `maxUnavailable` honoured by drain and updates.
7. **Security profiles**: the jail defaults in DESIGN.md §9, loosened per
   service only explicitly.
8. **Metrics**: a Prometheus endpoint on each manager and agent.
9. **Exit test**: two teams, each with its own user and namespace, deploy
    on the same cluster; neither can read or change the other's objects,
    reach the other's tasks, or exceed its quota.

## Later phases

See DESIGN.md §11: 4 Stateful and operations, 5 Extensions.
