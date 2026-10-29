# Phase tracker

Where Legion stands against the roadmap in [docs/DESIGN.md](docs/DESIGN.md)
(section 11). The design says what to build and why; this file says what is
done and what comes next. Update it in the same commit as the work.

Current work: **hardening** for real use, after phase 5 (phases 1 to 4
met their exit criteria; phase 5's features are built, bhyve as a spike).

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

Smaller follow-ups: placement preferences (`spread`) are accepted but not used yet; a node's
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

- **Namespace isolation and network policy** (`legion_api::policy`,
  `legion-agent::policing`, `legion-net::policy`, DESIGN.md §5.6): a
  task is reached by its namespace's tasks, the nodes and published
  ports by default; `Namespace.spec.isolation` can be `open` or `locked`,
  and `NetworkPolicy` objects let in more by namespace, service labels and
  ports. The agent guards each pod's host end in the `legion/policy`
  anchor: only its own address out, no IPv6, VIPs checked at the client
  pod (the hairpin NAT hides the source at the server), replies passing as
  `if-bound` state. pf only sees bridged pod traffic with
  `net.link.bridge.pfil_member=1` and `pfil_local_phys=1`, which FreeBSD
  15 leaves off; the agent sets both, host-wide (decided with the user).
  `stack deploy` no longer re-applies an existing namespace, so its
  isolation stays. Tested: the decisions (namespace, open, locked,
  policies by namespace, labels and ports, VIPs), the rules and tables,
  the agent guarding its pods in the fake cluster; and on FreeBSD in a
  VNET jail, a pod refused and then allowed directly, a VIP closed and
  then opened at the client, the host still reaching a refused pod, and a
  spoofed source dropped. Egress policies are not built.

- **Namespace quotas** (`legion_api::quota`, `legion-manager::quota`,
  DESIGN.md §3.3): `Namespace.spec.quota` limits the CPU and memory its
  tasks request, its placed tasks, its volumes and their sizes. The
  scheduler holds tasks over it as `pending`, with the limits in the
  message; admission refuses a Service or Volume whose desired state
  would go over, and, under a CPU, memory or storage quota, one that does
  not state its needs. Only growth is refused. Quota-counted applies are
  serialised on the leader. Pending tasks are no longer sent to agents: a
  global task held for lack of room (or quota) used to run anyway. Tested:
  the arithmetic; the scheduler holding replicated and global tasks,
  releasing them when the quota grows and stopping nothing when it
  shrinks; through the API, refusals by CPU, task count, volume count and
  storage, missing requests and sizes, shrinking under a lowered quota,
  other namespaces unaffected.

- **`logs`, `exec`, `port-forward`** (`legion-manager::session`,
  `legion-agent::session`, `session.proto`, DESIGN.md §3.8): opened at any
  manager's `Pods` service, which checks RBAC (logs `view`, the others
  `edit`), resolves the task, or a running task of a service, and its
  container, and offers the session to the node's agent. Agents keep a
  `Sessions` stream with every manager and answer with `Attach`, so no
  manager forwards to another and nodes need no port. Exec runs attached
  through libpod's upgraded connection, with stdin, a terminal and resizes;
  the CLI exits with the command's code. Port-forward is a session per
  connection. Tested: the libpod upgrade against a canned server and real
  Podman; through a manager and the agent, logs (tail, follow to the end),
  exec with stdin, a terminal size and exit code, a user going away, the
  errors, and port-forward; RBAC over TLS; and in the phase 1 exit test on
  FreeBSD, the CLI's exec (`-i`, `-t` under script(1)), logs and
  port-forward against the real app.
- **Init containers** (`legion-agent::task`): run in order, each to
  completion, as child jails of the pod with its volumes, before the main
  containers start; they do not run again when a main container restarts.
  The task shows which one is running (`init container migrate (1/2)`); a
  stop does not wait for one that never ends. One that fails fails the task
  with its last lines of output in the message, since the pod goes with
  it. The scheduler and quotas count the larger of the init containers'
  and the main containers' requests. Tested with the fakes and, on
  FreeBSD, with real Podman and ocijail: an init container leaves a file in
  the pod's volume for the app, and a failing one's output reaches the
  task's message.
- **Jobs and CronJobs** (`legion-api::job`, `cron`, `clock`;
  `legion-manager::controller::plan::jobs`, DESIGN.md §3.3): Jobs run
  their completion slots (`completions`, `parallelism`, or one per node
  when global) through the same planner and scheduler as Services, retry
  failed tasks with backoff up to `backoffLimit`, and fail past
  `activeDeadline`; their pods' restart condition defaults to `none`.
  CronJobs make a Job per run of a five-field UTC schedule, owe only the
  latest missed run, skip runs while suspended, past `startingDeadline` or
  under `forbid`, replace under `replace`, and keep their history limits.
  `legion list jobs` and `cronjobs` show the progress; `logs`, `exec` and
  `port-forward` take a Job's name as they take a Service's. Tested: cron
  parsing and next and owed runs, admission, the planner (completions,
  parallelism, retries, the backoff limit, the deadline, global jobs,
  drains, deletion; schedules, history, the concurrency policies, suspend
  and the starting deadline), and a Job through a manager and an agent.
- **Output of finished tasks** (`legion-agent::output`, DESIGN.md §3.8):
  before removing a pod the agent saves the last 1 MiB of each
  container's output, init containers' too, under
  `<data-dir>/logs/<task uid>`, and removes it when the task is deleted.
  `logs` falls back to it when the pod is gone, and a Service or Job with
  no running task gives its last finished task. Tested: the file format
  and bound, a completed task's logs by its name and its owner's, tail,
  follow, exec refused, removal with the task; on weft-bsd, a failed init
  container's output kept from real Podman.
- **Disruption budgets** (`maxUnavailable` on the Service, DESIGN.md
  §3.3): updates and drains take a ready task away only while fewer than
  `maxUnavailable` slots lack one, and otherwise start the replacement
  first; start-first services also drain start-first. Moved from
  `update.maxUnavailable`, which nothing read. Tested in the planner: the
  fewest ready tasks during a drain and during an all-at-once update,
  unbounded, at 1 and at 0, and a drain while another slot has failed.
- **Security profiles** (`legion_api::security`, `legion-manager::security`,
  `legion-agent::hook`, DESIGN.md §9): containers' jails have no raw
  sockets, `chflags`, mlock, SysV IPC or mounting, with `enforce_statfs=2`
  and `securelevel=2`. A container's `security` loosens them, only as far
  as its Namespace's `spec.security.allow` (cluster admin only) lets it;
  admission refuses Services, Jobs and CronJobs that go further. The
  parameters are set by a `createRuntime` hook in Legion's Podman before
  the container's process starts, since ocijail cannot be told through
  Podman; the agent checks them after each start and refuses a pod whose
  jail lacks them, and tightens adopted pods from older agents in place.
  Compose's `cap_add: [NET_RAW]` maps to raw sockets. Tested: the profile
  and its bounds, admission through the API (no namespace, an empty one,
  allowed and refused loosenings for each kind), the annotations, the
  fake runtime with and without the hook and an adopted pod tightened;
  the hook script against a jail on FreeBSD; and on weft-bsd with real
  Podman and ocijail, a locked-down container refused `ping` at
  securelevel 2 while one allowed raw sockets pinged.
- **Metrics** (`legion_api::metrics`, `legion-manager::metrics`,
  `legion-agent::metrics`, DESIGN.md §3.8): `legion daemon
  --metrics-listen <addr>` serves `/metrics` in Prometheus's text format.
  Every manager reports Raft; the leader alone reports objects, tasks by
  state, task restarts, services' replicas and ready tasks, nodes and
  quota limits beside use; the agent reports each running container's
  rctl usage (CPU, memory, processes, threads, open files, I/O). Tested:
  the text format and its escaping, the HTTP endpoint, the leader's
  report through the API (and nothing from a manager that has stopped,
  with its store shut down while a handle is held), the agent's report
  from the fake runtime (the infra jail left out); and on weft-bsd, real
  rctl usage of both containers in the agent's live test, and the phase
  1 exit test's daemon scraped once its app was ready.
- **Flaky tests fixed.** One was a bug: a container's `died` event that
  came after a resync had already seen the exit and restarted the
  container counted as a new exit, restarting the new run (an extra
  restart, and an attempt used up); the agent now ignores the event while
  the container runs. Also `legion rollback` failed with ABORTED when the
  controllers wrote the Service's status between its read and its write;
  it now reads again while only the status changed. The rest were races
  in tests: an init container checked before it started, followed logs
  opened after the line they waited for, and a probe test's closed port
  taken by another test. Tested: a held-back event against a restarted
  container (fails without the fix), and 14 clean runs of the whole suite
  in a row.
- **Exec sessions no longer keep conmons for five minutes** (DESIGN.md
  §6.1): Legion's Podman gets `exit_command_delay = 5` through a
  containers.conf override. Found when the exit test left conmon pairs
  behind; on weft-bsd, with exec probes every second, the live test now
  peaks at about 40 conmons and is back to none seconds after it ends.
- **Exit test** (`crates/legion/tests/teams.rs`, ignored; run as root on
  a FreeBSD host, see its docs; the lab code it shares with the phase 2
  test is in `tests/common`): passes on weft-bsd and the bhyve guest as
  managers and the remote server, about 225 ms away, as a worker. The
  cluster admin makes namespaces `alpha` and `beta` with task quotas and
  the users alice and bob with `edit` in one each; each deploys, as
  itself, a Secret and `web` (one task per node, published). Then for
  each user against the other's namespace: listing, reading, the Secret,
  deleting, applying, `logs` and `exec` are refused, `-A` lists show only
  their own, and neither may change a Namespace, even their own quota.
  From inside each task, the team's own tasks on every node and its VIP
  answer while the other team's tasks on every node and its VIP do not.
  A Service of beta's asking for alpha's published port is refused it
  (`status.conflicts`) and alpha keeps answering there. Over the quota,
  alpha's six replicas are refused and a Job's third task waits as
  `pending`, while beta's next task runs; the leader's metrics show both
  quotas beside their use; raw sockets the namespace does not allow are
  refused.

  It found, and these were fixed: the guards on pods' host ends were
  never evaluated for traffic from other nodes (pf does not see what the
  host sends onto the bridge), so a task reached every other namespace's
  tasks on other nodes; that traffic is now checked as it comes in on
  `wg0` (DESIGN.md §5.6). And the CLI gave up at the first `UNAVAILABLE`,
  which a manager's moment without quorum causes; it now asks again for
  up to 30 s. A first layout with the remote server as the second of two
  managers lost its quorum whenever that server stalled for a few seconds:
  with two managers Raft needs both, so far nodes are better workers.
- **Draining stopping tasks** (`AgentConfig.drain`, DESIGN.md §5.4): a
  ready Service task told to stop runs on for 2 s first, while the other
  nodes take it out of their balancing; before, a node across the WAN
  could still send it a connection after its pod was gone, which hung.
  Found as an occasional request timing out during the phase 2 exit
  test's rolling update (1 run of 3). Tested: a ready task drains while a
  Job's task stops at once (fails with no drain); the phase 2 exit test
  twice more with no request failing during the update, and the phase 3
  exit test again.

### To do, in order

Nothing left in phase 3.

Smaller follow-ups: egress policies; lab runs on weft-bsd leave the bridge
sysctls at 1; API request counts and latencies in the metrics.

## Phase 4 — Stateful and operations

Exit criterion (DESIGN.md §11): Postgres with replicas runs as a stateful
service and migrates between nodes.

### Done

- **Stateful services** (`mode: stateful`, DESIGN.md §3.3): tasks have
  ordinals from 0 and are `<svc>-<n>` to their pod (its hostname) and to
  DNS (`<svc>-<n>.<svc>.<ns>.legion`, the ordinal's ready task). A pod
  volume `claim: { size }` mounts a Volume of the task's own,
  `<claim>-<svc>-<n>`, which the controller makes for each wanted ordinal,
  resizes with the claim, and keeps when the service shrinks or is deleted;
  the first task to use it binds it to its node, as any Volume. Ordinal `n`
  starts only once every lower ordinal has a ready task; the service
  shrinks from the top one task at a time and updates from the highest
  ordinal down, stopping each task before its replacement starts
  (`start-first` is refused). Quotas count the Volumes claims will make,
  made or not, once. Claims are refused outside stateful services, and
  names that would pass 63 characters at admission. Tested: the planner
  (ordered start, a failing ordinal holding back the others, shrinking,
  growing back onto the kept Volumes, resizing, update order, one task
  per slot throughout, replacements going where their Volume is),
  admission, quotas through the API, DNS names, and the agent mounting a
  task's own Volume with its hostname set.

- **Migration** (`legion-manager::migrate`, `controller/plan/migrate.rs`,
  `legion-agent::transfer`, DESIGN.md §4.3): draining a node moves each
  wanted stateful ordinal's Volumes to the eligible node with fewest of
  its service's tasks. The migration lives in the Volume's status
  (`copying`, `copied`, `finishing`, with the last failure's message);
  the leader's migrator offers the old node a `VolumeSend` session and
  the new node a `VolumeReceive` and joins them, so data goes through the
  leader and nodes still need no port. A full copy goes while the task
  runs; once copied the drain stops the task; with nothing placed using
  the Volume the increment since goes, the Volume becomes the new node's
  and the old node drops its copy (`VolumeDrop`). Meanwhile the slot's
  next task waits with `volume moving`. A migration is dropped when its
  new node is no longer ready and active, or, before its last part, when
  the old node stops draining; failures are noted and retried after 5 s.
  `Sys` gained `zfs_send`, `zfs_receive` (`-F -u`) and `zfs_mount`;
  volumes are mounted before use. `legion list volumes` shows sizes,
  nodes and migrations. Tested: the planner (start, copy, stop, finish,
  placement on the new node; dropped when the drain is called off or the
  new node goes); through one manager and three agents with fake ZFS,
  data written before and after the full copy arriving, mounted, with the
  snapshots gone and the old copy dropped, and moving back onto a node
  whose old copy could not be dropped (fails without replacing it); and
  on weft-bsd, the streams with real ZFS: full, incremental, into a new
  parent, unmounted then mounted, refused over a dataset with snapshots
  (which the fake missed), a cut stream leaving nothing.
- **Snapshots before updates** (`update.snapshot`, `legion-agent`
  volumes): a stateful service's update records its snapshot
  (`legion-update-<generation>`) in `status.update.snapshot`, and its
  tasks carry a step (`spec.snapshot`): an update's tasks take the
  snapshot of their claimed Volumes before they run unless taken,
  dropping older ones; a rollback's tasks roll back to it and drop it.
  Refused outside stateful services. `Sys` gained `zfs_snapshots` and
  `zfs_rollback` (`-r`). Also fixed: with `restartPolicy.condition:
  none`, a rollback left the slot of the task that failed empty; a task
  that failed running another pod is now replaced whatever the policy.
  Tested: the planner (take on update, restore on rollback, nothing after,
  nothing without `snapshot`; the restart-policy case); through a manager
  and an agent with fake ZFS, data changed by the failing update's task
  back as it was before the update; and on weft-bsd, snapshot listing
  and a rollback of a mounted dataset with real ZFS.

- **Exit test** (`crates/legion/tests/stateful.rs`, met 2026-10-29):
  Postgres 17 as the stateful service `db`, ordinal 0 the primary and
  ordinal 1 a streaming replica of `db-0.db.data.legion`, each on a
  claimed Volume, in a namespace allowing the SysV shared memory Postgres
  needs (`security.allow: [sysvipc]`). On weft-bsd (n1), the Vultr VPS
  (n2, worker) and the bhyve VM lvm3 (n3), each with a throwaway
  file-backed pool: 1000 rows on the primary reach the replica; draining
  the primary's node moved it (n1 to n3) with its rows, it took 1000 more
  and the replica followed it to its new address; draining the replica's
  node moved the replica (n2 to n1), which streamed again. Passed twice in a row (6 and 7 minutes).
  The image comes from `tests/stateful/Containerfile` (the server without
  its LLVM JIT, 363 MB) and is loaded into every node's Podman through
  its socket, as the lab has no registry; the lab hooks gained per-node
  setup, daemon flags and teardown. Added for it: `legion node
  drain|pause|activate <node>`. Found: rctl's `memoryuse` counts shared
  memory once per process, so a 256 MiB limit killed Postgres's WAL
  sender during the base backup; the test sets no memory limit.

- **Store backup and restore** (`legion_store::backup`, `legion backup`,
  `legion restore`, DESIGN.md §3.7): a backup is every object read at one
  linearizable version on the leader, with the managers, as JSON;
  restoring, offline, writes a new store in which they are applied at
  that version, the log purged up to it, and the manager the only voter,
  from a backup file or from the manager's own store (quorum lost). The
  old store is set aside. Raft calls from managers outside the membership
  are now refused, so those of the cluster a restore left cannot pull it
  back (without that, openraft took their log or, in debug builds,
  panicked). Tested: the store (versions and uids kept, writes continuing
  above the backup, a fourth manager joining through the restored
  snapshot, restarting from its own store, formats refused); through
  three managers over gRPC, a backup through a follower, restored at the
  same address while the other two keep electing and writing, staying
  alone and serving; and the binary: backup, refused under a running
  daemon, restore after a crash, a manager joining with the backed-up
  token, and a restore from the manager's own store once it is lost.

- **Autoscaling** (`autoscale` on replicated services,
  `controller/plan/autoscale.rs`, `legion-manager::usage`, DESIGN.md §3.3
  and §4.3): agents send each running task's use (rctl `pcpu` and
  `memoryuse` over its app containers) with a heartbeat every 15 s; the
  leader keeps the reports for a minute, from the task's own node only,
  and the Service controller's pass turns them into the replica count,
  in `status.autoscale` with the measured percentages. It grows at once
  and shrinks after `downDelay`; quotas count `max`. Tested: the planner
  (start at min, growth to the target, staying within the tolerance, the
  delay before shrinking, the cap at max, reports from the wrong node
  ignored, silent tasks counted as idle growing and on target shrinking,
  a new leader's wait, back to `replicas` without `autoscale`),
  admission, and through a manager and two agents with fake rctl, a busy
  service growing from one task to three and back.

- **Moving a Volume on request** (`spec.node` on Volumes, `legion volume
  move`, DESIGN.md §4.3): a Volume's `node` names where its data should
  be; set elsewhere than the data, the migrator moves it as for a drain
  and the tasks using it follow, and set before first use it places the
  first task. A drain still moves a Volume asked to be on the draining
  node, which returns once the node is active. New placements wait during
  any migration (`volume moving`), not only its last part. `legion node`
  and `legion volume move` share a read-then-write that retries over
  concurrent writes. Tested: the planner (no move to where it is, copy
  with the task running, stop, wait, run on the new node; called off;
  waiting for a paused node; drained and back; a first task placed by
  `node`), through a manager and two agents with fake ZFS (the data
  arriving, the old copy dropped), and the CLI.

- **CARP VIP** (`legion-manager::vip`, `legion daemon --vip`, DESIGN.md
  §4.3): managers on one link share a floating address for the API, a
  CARP virtual host the leader takes (skew 0, forced master) and
  followers stand by for (skew 100, keeping it only until a better
  advertiser takes it). Released on shutdown. `Sys` gained `carp_set`,
  `carp_state` and `carp_remove`. Tested: the forms against FreeBSD
  15.1's output, the loop with the fake, and with real CARP on weft-bsd
  (`crates/legion/tests/vip.rs`): three managers in VNET jails on a
  bridge, the leader holding the address alone and the API answering on
  it; a clean stop and a SIGKILL each moving it to the next leader.

- **Ingress routes** (`legion_api::route`, `legion-agent::ingress`,
  DESIGN.md §5.8): `Route` objects (host or `*.domain`, path prefix,
  service, port), streamed by the dispatcher to every agent and served by
  an HTTP proxy in the agent (`legion daemon --ingress-listen`) instead of
  the haproxy service earlier drafts planned: longest match, exact host
  before wildcard, ready tasks in turn at their own addresses,
  `X-Forwarded-*` headers, upgrades passed through. Tested: matching and
  admission; the proxy against real local backends (routing, 404, 503,
  502, routes and endpoints changing, a WebSocket-style upgrade); routes
  reaching a node through the dispatcher; and on weft-bsd in the phase 1
  exit test, the app's web service answering through a route with real
  pods.

- **HTTPS for routes** (`tls: { secret }`, `--ingress-tls-listen`,
  DESIGN.md §5.8): the certificate of each route's host from a PEM Secret,
  by SNI (wildcards after exact hosts), read again on route changes and
  every minute; any node may read a Secret a route names. Tested: the
  proxy with self-signed certificates (per host, wildcard, none for other
  hosts, renewed, a broken one keeping the last), the dispatcher letting a
  node without tasks read a route's Secret and no other, and in the phase
  1 exit test on weft-bsd with an `openssl` certificate and client. Also:
  the CLI tests give a starting daemon 60 s (the first start of a freshly
  built binary on Windows took over 20 s once) and print its log if it
  does not come up.

- **Encryption at rest** (`legion_store::cipher`, DESIGN.md §9): every
  value a manager's store writes is sealed with ChaCha20-Poly1305 under
  its own key, `pki/store.key`, bound to where it is stored; snapshots
  travel in the clear between managers and are sealed by each; plain
  stores from before still read and are sealed as written. Tested:
  sealing and opening (another place, another key, no key, cut short),
  openraft's storage conformance suite with encryption on, a secret
  absent from the file (log and snapshot) and read back only with the
  key, a plain store taking a key, and the daemon's own store.

- **Autolock** (`legion autolock enable|disable|rotate|show`, `legion
  unlock`, `legion_manager::autolock`, `legion::autolock`, DESIGN.md
  §9): an unlock key kept in the store, with which every manager seals
  its CA, node and store keys on disk (rechecked every 5 s, so rotations
  reseal and disabling unseals); a restarted manager with sealed keys
  serves only `Unlock`, from its own host, until given a key that opens
  them. `legion restore` takes `--unlock-key-file`. Tested: key format;
  through the binary, enable (and enable again keeping the key), keys
  sealed on disk, a restart serving only `Unlock`, a wrong key refused,
  the right one starting it with its data, a rotation refusing the old
  key, disabling and starting without a key; two managers, enabled
  through the follower, both sealing theirs.

### To do, in order

Nothing left in phase 4.

Smaller follow-ups: ingress HTTP/2, connection reuse and ACME certificates; sealing the
WireGuard key under autolock.

## Phase 5 — Extensions

DESIGN.md §11. First the native jail runtime (DESIGN.md §6.5), which
replaces Podman and ocijail.

### Done

- **Native runtime spike** (DESIGN.md §10, S7): an image pulled into ZFS
  layers and run as child jails of a `persist` VNET pod jail; about 55
  ms to start against Podman's 900, 1 KB of disk per container against
  a full copy.
- **Image store** (crate `legion-image`; `Sys::zfs_clone`): image
  references as Podman reads them; a registry client (token and basic
  auth, platform chosen from indexes, every blob's digest checked,
  redirects to CDNs, plain HTTP only for registries named so); layers
  decompressed (gzip, zstd) and checked against their `diff_id` before
  anything is extracted, whiteouts applied without following links and
  clearing file flags, then bsdtar; each layer a clone of the one below,
  snapshotted by chain id, so images share layers; import of OCI layout
  directories; removal and prune leave layers that containers were
  cloned from until they are gone. Tested: references, configs in either
  case, platform choice, whiteouts (and staying inside the root); against
  a registry served in the test: token auth, redirects, shared layers
  fetched once, tampered blobs and false `diff_id`s refused with nothing
  left behind, Linux-only images refused, removal held by a container's
  clone; on FreeBSD with real ZFS: `freebsd-runtime` from Docker Hub
  (12 s) with its `schg` and set-uid files and numeric owners, and a
  whiteout and a replacement of `schg` files.

- **Jail runtime** (crate `legion-jail`; `Sys::jail_create`,
  `jail_remove`, `mount_nullfs`, `mount_devfs`): the agent's `Runtime`
  over jails. A pod is a `persist` VNET jail on `/var/empty`; members are
  child jails on ZFS clones of their images, with devfs (ruleset 4),
  nullfs mounts (targets made without following the image's links) and
  the security profile's jail parameters set at creation. Each process
  runs under a supervisor (`legion-jail-shim`, to be `legion jail-shim`)
  that enters the jail as the container's user, keeps its output (lines
  by stream, rotated at 8 MiB) and its exit status, and removes the jail
  when the process ends; it outlives the agent, which finds its
  containers again from their state directories (a run from before a
  reboot counts as over). Stop asks the supervisor for the stop signal,
  then removes the jail after the grace period. Exec enters the jail the
  same way, with a terminal when asked. Tested: log records, rotation and
  tails, stop signals, mount targets; on FreeBSD, a pod with members (a
  member starts in about 30 ms), a mounted file, environment, user
  switching, exec with and without a terminal, exit codes and `died`
  events, a missing command failing the start, a new runtime finding
  everything, stop by signal and by grace period, restart, and removal
  leaving no jail, mount or dataset.

- **The jail runtime in the daemon** (`legion daemon --runtime
  jail|podman`, Podman still the default; `legion jail-shim`, run before
  the async runtime starts since it forks): images in
  `<zfs-root>/images` with their index in `<data-dir>/images`,
  containers in `<data-dir>/jails` and `<zfs-root>/jails`; images no
  container has used for an hour are removed (checked every 10 minutes),
  then the layers nothing uses. The phase 1 exit test takes
  `LEGION_EXIT_RUNTIME=jail` and **passes on the jail runtime** on
  weft-bsd (174 s): probes, ZFS and tmpfs volumes, crashes and restarts,
  DNS, VIPs, ingress over HTTP and HTTPS, metrics, a daemon restart
  adopting the running pods, all without Podman.

- **`legion image load`** (`Store::load`): an OCI archive (`podman save
  --format oci-archive`) into a node's image store, named as the archive
  names it or `--name`; the store holds a file lock while it changes, so
  this runs beside a daemon that pulls. Tested: loading by the archive's
  name and another, and a non-OCI archive refused with what to do.
- **Every exit test passes on the jail runtime** (2026-10-29, weft-bsd
  with the Vultr VPS and bhyve `lvm3`; `LEGION_CLUSTER_RUNTIME` and
  `LEGION_EXIT_RUNTIME` choose, each node of the multi-node lab getting a
  throwaway pool): phase 1 (174 s), phase 2 cluster (411 s: deploy, a
  zero-downtime update, a node lost), phase 3 teams (328 s: isolation,
  network policy across nodes, published ports, quotas) and phase 4
  stateful (1094 s: Postgres primary and replica, both drained and moved
  with their data, the image loaded with `legion image load`).
- **Podman and ocijail removed** (the same day, once every exit test had
  passed on jails): `legion-podman`, the ocijail hook, the agent's Podman
  live test, `--runtime` and the Podman paths of the lab tests. The
  `Runtime` trait has its own types (`legion_agent::runtime`), with
  errors of its own (not found, wrong state, failed, pull). The agent
  needs `--zfs-root`. Re-checked on FreeBSD: the runtime's live test and
  the phase 1 exit test (168 s).
- **Flaky tests fixed**: an agent started a task again whose worker had
  just reported its end, while the assignments still carried the older
  state: in tests the pod ran on (`startup_failures`,
  `finished_tasks_keep_their_output`, `restart_policy` failed most runs
  on FreeBSD, and had before this phase); in a cluster a failed task's
  pod could have run beside its replacement. The agent now remembers the
  tasks whose workers ended. The agent tests' harness logs with
  `RUST_LOG`, and its waits name the containers left. Two store backup tests
  failed now and then on Windows: `Store::shutdown` waited for the
  database's last handle, but an `Arc`'s count drops before its value's
  drop has closed the file, which may still run on another thread; it
  now waits until the file opens again.
- **Registry logins from Secrets**: `imagePullSecrets` on a pod names
  Secrets holding Docker configs; the node may read them (they count among
  the pod's `data_refs`) but they are not written to the task's files. The
  registry client keys its tokens by account, so a token one login got
  never serves a pull without it. The image store records who pulled an
  image with a login (`Image::private`, by reference); another namespace's
  task pulls it again, so the registry checks its access. Tested: against
  the test registry (a private repository; wrong, missing and right
  logins; a cached token not reused; layers not fetched twice; an image
  found public later), the Docker config forms, and the agent (a task
  without the login fails, another namespace needs its own, a missing
  Secret fails the task).
- **Pruning and starting tasks**: an image a task had just found could be
  pruned before the task made its containers, failing that start. The
  runtime now notes when a task finds or pulls an image, and pruning
  holds that note while it removes, so such an image stays another hour.
  Re-checked on FreeBSD: the runtime's live test.
- **Linux images** (DESIGN.md §6.6): `legion daemon --linux` loads the
  Linux ABI and its file systems, takes `linux/<arch>` where an image has
  no FreeBSD build and reports `platform.linux`, which placement selects
  with `node.platform.linux == true`; other nodes refuse Linux images.
  Linux containers get linprocfs, linsysfs, fdescfs and a `/dev/shm`
  (a devfs link to a tmpfs, as devfs takes no mounts). The supervisor now
  reads users and groups from the image's `/etc/passwd` and `/etc/group`
  as text, for every image. Tested: platform choice, the store with and
  without Linux, user and group resolution, placement; on FreeBSD 15.1,
  `alpine:3.20` (musl) and `debian:bookworm-slim` (glibc) as `nobody` in
  a FreeBSD pod, and the FreeBSD live test again.
- **IPv6 dual-stack** (DESIGN.md §5.7): every pod, VIP and node address
  has an IPv6 twin in a ULA range (`10.200.3.17` is
  `fd00:10:200:3::11`), derived on each node, so nothing new is
  allocated or sent. The bridge, pods (no duplicate address detection),
  `wg0`, routes and WireGuard peers carry both families; every load
  balancing and policy rule has an `inet6` twin, tables hold both, and
  neighbor discovery passes on pods' links. DNS answers AAAA. Pods reach
  the outside over IPv4 only. Tested: the address mapping, rules, tables
  and peers; on FreeBSD 15.1, the system layer's live test, pods and
  policy on one bridge, and the three-node mesh, all over IPv6 as well.

- **Volume drivers and plugins** (DESIGN.md §6.7): a Volume's `driver`
  is `zfs` (as before), `nfs` (mounted per task, shared by any nodes),
  `iscsi` (logged in to and mounted once per node; one node at a time)
  or `plugin` (an executable in the node's plugin directory, run as
  `mount|unmount <dir>` with the options as JSON; shared or one node at
  a time). A volume used one node at a time is bound to the node while
  its tasks use it; the agent lets go (unmounts, logs out, then
  `ClaimVolume` with `release`) after the last one, and a removed node's
  volumes are freed. LUNs must hold UFS: Legion never formats. Tested:
  admission, placement, the agent with all three drivers on the fake
  system; on FreeBSD 15.1, mounts of a real NFSv3 export and a ctld
  iSCSI LUN, which found that a LUN's device is a `probe…` placeholder
  for a moment after login; and end to end
  (`crates/legion/tests/storage.rs`): a service on both through the
  daemon, scaled to nothing and back with its data. That run found three
  more: an unclean LUN (left by a node going down) mounts read-only at
  best, so the agent preens it first (`fsck -p`); a jail still dying
  after its removal failed the removal instead of counting as gone; and
  every manager daemon's stop failed with "store still open" after 5 s,
  as the autolock follower held a store handle until after the store
  closed (since autolock, 8f37bb4). The test now checks the daemon stops
  cleanly. Open: two jails of the first run's first pod stayed dying
  (later runs' pods did not); not yet explained.

- **bhyve spike** (DESIGN.md §6.8, `legion-net/tests/bhyve.rs`): a
  small FreeBSD guest boots from a zvol in 2.9 s inside a VNET jail
  with `allow.vmm`, configured by loader variables, on a tap in
  `legion0`. Legion's service and policy rules work for it unchanged,
  both ways and over both families, with the tap as its host end.
- **Pods' VIPs failing after an ARP exchange** (DESIGN.md §5.3), found
  by the spike: the host answered a pod's ARP for its gateway twice,
  with the bridge's address and with the pod's host end's, the last
  answer winning; with the host end's, the pod's frames bypassed the
  VIPs' rules on the bridge until the host next asked for the pod's
  address. Host ends now take the bridge's Ethernet address
  (`PodNetwork::join`, `Sys::interface_ether`/`interface_set_ether`);
  the live test asks five times and checks the answer, and fails at the
  first ask without the fix.

### To do, in order

Nothing left in phase 5. Decided with the user (2026-10-29): bhyve stays
a spike until a workload needs its own kernel; DESIGN.md §6.8 lists what
a `runtime: bhyve` would take.

## Hardening

Follow-ups from the phases that matter before anyone runs Legion for
real, in the order agreed with the user.

### Done

- **Certificate renewal without restarts** (DESIGN.md §9). Managers
  renewed only at start, so one running 90 days served an expired
  certificate, and workers never did. Now a running manager issues
  itself a new certificate past two thirds of its life, and a worker
  asks a manager for one (`Dispatcher.Renew`, for the identity its
  current certificate names). TLS is ended by Legion
  (`legion_proto::tls::Credentials`), with certificates picked per
  handshake on both sides, so the new one is served and presented from
  the next connection on. A worker writes the new pair beside the old
  and renames it in place, and a manager whose key and certificate do
  not match (a crash between writes) issues itself a new pair. Tested:
  renewal through the dispatcher (a manager or admin refused), the
  server's certificate replaced while serving, the worker's write
  surviving a crash at each step; on FreeBSD 15.1, the phase 1 exit
  test and the three-manager VIP test over the new transport.
- **New pods guarded before they run** (DESIGN.md §5.6). The policy pass
  built guards from the assignments, which carry a pod's address only
  after the agent reports it, so a new pod's containers ran unguarded
  until then. Now the worker hands the pod to the pass once its network
  is up and starts nothing in it until the guard is loaded at its
  address; a pod not guarded within a minute fails. The pass also no
  longer spins while the policies are still unknown. Tested: in the fake
  cluster, with pf refusing the policy anchor, a task stays `starting`
  with nothing run, then starts once pf takes it, its guard already in
  place (the test fails without the wait); on FreeBSD 15.1, the phase 1
  exit test.
- **Memory limits that count shared memory once** (DESIGN.md §6.3).
  rctl's `memoryuse` sums a jail's processes, so Postgres's shared
  buffers counted once per backend and a limit killed it (the phase 4
  exit test ran without one). Now the agent counts each limited
  container's memory itself, every page once (`legion_sys::JailMemory`,
  from `kern.proc.vmmap` and `vm.swap_objects`, following copy-on-write
  chains), every second, and kills the process with the most memory of
  its own in a container over its limit; the task's message says so when
  the container exits. rctl's rule is now per process, a backstop that
  never kills what the count spares. Metrics and the autoscaler use the
  same count. Tested: the counting (shared segments, forks, SysV and
  POSIX segments), the agent sparing a container rctl counts at 2 GiB and
  killing the largest process of one over its limit; on FreeBSD 15.1, a
  jail with a 64 MiB segment in five processes counted at 101 MiB (rctl:
  375 MiB), and `tests/memory.rs`: Postgres under 256 MiB with 128 MiB of
  buffers left alone by sixteen sessions reading them (rctl: 1276 MiB),
  losing a backend to eight sessions of 50 MiB (counted: 701 MiB) and
  to one of 300 MiB, and recovering each time without a restart. The
  lab's cleanup now also removes the rctl rules of the pods it removes
  (rctl keeps rules of jails that are gone; twenty had piled up). Also
  fixed, found as a flaky `restart_policy`: an exit event arriving after
  the resync had restarted the container, and the new run had ended,
  gave the task the earlier run's exit code; the code is now the
  runtime's.

- **Packaging** (README.md, Install; `packaging/freebsd`). An rc.d
  script runs the daemon under daemon(8), which restarts it 5 s after a
  crash and writes its output to `/var/log/legion.log` (reopened on
  SIGHUP, for the newsyslog entry beside it); its flags are
  `legion_args`, since rc.subr hands `legion_flags` to daemon(8), and it
  warns when racct is off. `release.sh` builds, on a FreeBSD host, a
  tarball laid out as `/usr/local` (the binary, the rc.d script, the
  newsyslog entry, the volume plugin directory, the docs), owned by root,
  with its SHA-256. The daemon now listens on every address by default
  (every call but `Join` needs a cluster certificate, and `Unlock` only
  answers loopback); a manager advertises `--listen` when it names an
  address and otherwise its address on the default route, a worker the
  address it reaches its managers from (its WireGuard endpoint, which it
  had none of before), and `legion join` sends a manager's from the
  address it reaches `--manager` from. Tested: the defaults; on FreeBSD
  15.1, the tarball built and unpacked, and its rc.d script starting the
  daemon (the CLI reaching it), daemon(8) restarting it after a kill,
  the log reopened after a rotation, and a clean stop. Not built: a
  FreeBSD port, for when the flags have settled.

### To do, in order

Nothing left in the hardening agreed with the user. Candidates from the
phases' follow-ups: refusing to run pods unlimited on a host without
racct, egress network policy, a faster down signal for lost nodes, and a
FreeBSD port.

