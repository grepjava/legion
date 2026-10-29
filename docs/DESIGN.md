# Legion — Design Plan

Legion is a container orchestrator for FreeBSD. It takes the operating model of
Docker Swarm (one binary, `init`/`join`, embedded Raft, automatic mTLS, compose
stacks) and adds the Kubernetes capabilities that matter most in practice:
pods, real probes, namespaces, stateful workloads, network policy. Workloads run
as jails through Podman and the `ocijail` runtime.

Status: draft v0.4, 2026-10-26. All phase-0 spikes (S1–S6) have been run on
FreeBSD 15.1; results are in section 10 and are reflected throughout. Phase 1
has started: the workspace has `legion-api`, `legion-store`, `legion-proto`,
`legion-manager` and the `legion` binary (sections 3.7 and 3.8). Other crates
are added as their phase starts.

---

## 1. Decisions

| # | Decision | Choice |
|---|----------|--------|
| D1 | Implementation | Fresh implementation in Rust. SwarmKit is a reference, not a dependency. |
| D2 | API surface | Own gRPC API and `legion` CLI. Compose files accepted via `legion stack deploy`. No Docker Engine/Swarm API compatibility. |
| D3 | Networking | Legion owns jail networking (VNET, epair, bridge, routing, pf). Podman is used for image and process lifecycle only. |
| D4 | Linux images | Nice-to-have, late phase, behind a node capability flag. |
| D5 | Target OS | FreeBSD 15.x only, amd64 and arm64. |
| D6 | Kubernetes scope | Selected features, phased (section 4). No Kubernetes API compatibility. |
| D7 | Storage | A ZFS pool is required on every node. `legion join` refuses a node without one. |

**Why not Docker API compatibility (D2).** The Swarm API's object model is
exactly what Legion extends: multi-container pods, separate probes, namespaces
and stateful services do not fit into it. A compatibility shim would either hide
those features or fake them. Compose is the part of the Docker ecosystem people
actually depend on, so Legion accepts compose files directly and offers an
`x-legion` extension for the extra features.

**Why own the network (D3).** Podman's FreeBSD networking is built for single
hosts. Cluster-wide addressing, service VIPs, routing mesh and policy all need
one owner that knows the whole cluster. Spike S1 also showed that Podman's own
pod and shared-network modes do not give a private network stack in the way
Legion needs (section 6).

---

## 2. Goals and non-goals

### Goals

- A 3-node cluster running in under 10 minutes with no external dependencies
  (no etcd, no separate CA, no external load balancer).
- Every task is a jail with its own VNET stack, resource limits and ZFS-backed
  storage.
- Declarative: users `apply` desired state, controllers reconcile it forever.
- Existing compose v3 files deploy with few or no changes.
- Use FreeBSD primitives directly: ZFS, VNET, pf, rctl, WireGuard, CARP.
- Coexist with containers the operator runs by hand on the same node.

### Non-goals

- Kubernetes API, CRI, kubelet or Helm compatibility.
- Custom resource definitions, operators, admission webhooks.
- Running on Linux or older FreeBSD releases.
- Layer-2 features between containers (multicast, broadcast) across nodes.
- Clusters beyond roughly 500 nodes / 20,000 tasks in the first versions.

---

## 3. Architecture

### 3.1 Process model

One binary, `legion`, with two roles:

- `legion daemon` runs on every node as an rc.d service (`legion_enable="YES"`).
  It always runs the **agent**. On manager nodes it also runs the **manager**.
  The agent follows its work through the dispatcher in either case, on a
  manager node through its own manager first.
- `legion <command>` is the CLI. It talks gRPC over mTLS to any manager.

```
                 ┌──────────────── manager nodes (3 or 5) ────────────────┐
  legion CLI ──► │  API server ─ store (Raft) ─ controllers ─ dispatcher  │
                 └─────────────────────────────┬──────────────────────────┘
                                               │ gRPC/mTLS stream
                       ┌───────────────────────┼───────────────────────┐
                       ▼                       ▼                       ▼
                 ┌──────────┐            ┌──────────┐            ┌──────────┐
                 │  agent   │            │  agent   │            │  agent   │
                 │ executor │◄─ libpod ─►│ podman + │            │   ...    │
                 │ net/dns  │   REST     │ ocijail  │            │          │
                 │ probes   │            │  (jails) │            │          │
                 └──────────┘            └──────────┘            └──────────┘
                       ▲───────── WireGuard mesh (wg0) between all nodes ──────▲
```

### 3.2 Manager components

| Component | Responsibility |
|-----------|----------------|
| **Store** | Replicated state machine on `openraft`, with `redb` for the log, the objects and snapshots, in one file per manager. All objects live here. Every write gets a Raft index, which doubles as the resource version. See 3.7. |
| **API server** | gRPC (`tonic`). CRUD plus `Watch(kind, fromVersion)` streams. Authenticates by client certificate, authorises by RBAC. Runs on every manager; writes are forwarded to the leader. |
| **Controllers** | Run on the leader only. Each watches the store and writes desired changes back. See 3.3. |
| **Scheduler** | Assigns pending tasks to nodes: filter, then score. See 3.5. |
| **Allocator** | Assigns task IPs, service VIPs, published ports, node pod subnets. |
| **Dispatcher** | On every manager. Streams each agent its node's assignments and every service's endpoints from the local replica, and serves the secrets, configs and volume claims of the node's own tasks; status reports, registration and heartbeats are forwarded to the leader, which alone tracks liveness. Agents fail over between managers and learn the manager list from heartbeat replies. It also carries sessions (`logs`, `exec`, `port-forward`): see 3.8. |
| **CA** | Issues and rotates node and user certificates. Root key stored in Raft, optionally sealed with an autolock key. |

### 3.3 Controllers

| Controller | Watches | Produces |
|------------|---------|----------|
| Service | Service, Task, Node | Creates and removes Tasks to match replicas / one per eligible node (global). |
| Stateful | Service (`mode: stateful`) | Tasks with ordinals from 0, each `<svc>-<n>` to its pod (hostname) and DNS. A pod volume with `claim: { size }` mounts a Volume of the task's own, `<claim>-<svc>-<n>`, which the controller makes for every wanted ordinal (resized with the claim) and keeps when the service shrinks or goes. Ordinal `n` starts only once every lower one has a ready task; the service shrinks from its highest ordinal, one task at a time, and updates from the highest down, always stopping a task before its replacement starts (`start-first` is refused). Quotas count the Volumes claims will make. |
| Update | Service spec generation | Rolling replacement honouring parallelism, delay, failure action, `maxUnavailable`; automatic rollback. |
| Job / CronJob | Job, CronJob | Run-to-completion tasks; CronJob creates Jobs on schedule with concurrency policy and history limit. |
| Node health | Heartbeats | Marks nodes `down` after three missed heartbeats (5 s each; a new leader allows three from taking over) and `ready` when heard from again; marks their tasks orphaned so the Service controller replaces them. |
| Endpoints | Task readiness | Per-service list of ready task IPs, pushed to every agent for load balancing and DNS. |
| Autoscaler | Task use from heartbeats | A replicated service's `autoscale: {min, max, cpu, memory, downDelay}` sets its replica count (`status.autoscale.replicas`; `spec.replicas` is only where it starts) so its ready tasks' average use comes to the targets, percentages of the containers' requests. Part of the Service controller's pass. |
| Cert rotation | Node certs | Renews certificates before expiry; rotates root CA on demand. |

The Service controller and the scheduler are built (`legion-manager`,
`controller/plan.rs`) as one pure function from Services, Tasks, Nodes and
the nodes holding Volumes to the writes that converge them; a loop on the leader runs it on every store
change, on planned wake-ups and every 5 seconds, and writes with version
preconditions. Rules:

- **Slots.** A replicated service has slots `1..replicas`; a global service
  one per eligible node. Each slot runs at most one current task, named
  `<service>-<slot>-<n>`. Tasks carry the label `legion.service`.
- **Current** means the task runs the service's pod spec as it is now. Other
  spec changes (replicas, placement, update settings) bump the generation
  but replace nothing.
- **Replacement.** A slot whose task is gone gets a new one: after
  `shutdown` or `orphaned` at once, whatever the restart condition; after a failure (unless the restart condition is
  `none`) with backoff, the restart delay (at least 1 s) doubled per failure
  in a row, at most 60 s; after `complete` only with condition `any`.
  Container restarts within a task are the agent's job; a task fails when
  its restart policy is exhausted.
- **History.** The last 5 terminal tasks of a slot are kept; slots no longer
  wanted keep none. Tasks of deleted services are stopped, then deleted.
- **Updates** replace outdated tasks `parallelism` slots at a time, in
  `stop-first` or `start-first` order (the old task stops once the new one
  is ready). A slot counts against `parallelism` until its new task is
  ready and has run for `delay`. An update starts when the pod in the spec
  changes; the status keeps the spec seen last (`status.spec`) and the
  update (`status.update`: state, message, the spec it came from). A new
  task failing before it has run for `monitor` (5 s) triggers
  `failureAction`: `pause` stops replacing tasks until the spec changes
  again, `continue` ignores it, `rollback` writes the previous spec back.
  The update completes once every slot runs a new task only, ready and
  past `monitor`. Changing the spec back to the one the update came from
  is a rollback (`legion rollback <service>` does that), with states
  `rolling-back`, `rolled-back`, and `rollback-paused` when a task fails
  during it. A slot whose last task failed running the pod the rollback
  left is refilled whatever the restart policy. Swarm's `maxFailureRatio`
  and a separate rollback config are not built: any failure counts, and a
  rollback uses the restored spec's own update settings. A stateful
  service may snapshot its Volumes for an update (4.3).
- **Nodes.** A drained node's tasks stop and move elsewhere; a paused node
  keeps its tasks but gets no new ones (`legion node drain|pause|activate
  <node>` sets it). The tasks of a node that is `down`
  are marked `orphaned` and told to stop, in one write: they leave the
  endpoints, their slots run elsewhere at once (a global service's slot
  goes with the node, and keeps its task number for when the node is
  back), and a node that comes back removes their pods. Tasks do not move
  back by themselves.
- **Disruption budget** (phase 3). `maxUnavailable` on a Service bounds
  the voluntary disruptions, updates and drains: they take a slot's ready
  task away only while fewer than `maxUnavailable` of the service's slots
  lack one, failures included. Past that, the replacement starts first,
  and the old task stops once the new one is ready, so `maxUnavailable: 0`
  surges instead of stalling. A start-first service drains that way too.
  Unset, there is no bound beyond the update's `parallelism`. A
  `NoExecute` taint evicts at once, and a global service's task goes
  with its drained node.
- **Taints** on a node (`spec.taints: [{key, value, effect}]`) apply unless
  the service tolerates them (`placement.tolerations`, matching the key,
  and the value and effect when given): `NoSchedule` keeps new tasks off,
  `NoExecute` also moves running ones away, `PreferNoSchedule` makes the
  node a last choice.
- **Scheduling** filters nodes (ready, active, taints, constraints, the node
  holding any claimed Volume the pod mounts, requests within allocatable)
  and prefers nodes without an untolerated `PreferNoSchedule` taint, then
  the fewest tasks of the same service, then the least requested CPU and
  memory. An unplaceable task stays `pending` with
  the reasons in its message, e.g. `no node can run it: 1 constraints`.
  A global task names its node from the start but is not sent there
  until placed.
- **Namespace quotas** (`Namespace.spec.quota: {cpu, memory, tasks,
  volumes, storage}`, set by the cluster admin only, as Namespaces are
  cluster-scoped): the scheduler places a task only if its namespace's
  placed tasks, with it, stay within the CPU and memory requests and the
  task count; otherwise it stays `pending` with, e.g., `over the quota of
  namespace shop: cpu 1200m of 1`. Admission refuses a Service or Volume
  whose namespace would then want more than the quota in all (replicas
  times requests; a global service counts one task per node), and, under
  a CPU, memory or storage quota, one that does not say what it needs.
  Only growth is refused: under a lowered quota, nothing is stopped and
  shrinking still goes through. Volumes are held to the quota at
  admission only, so the leader admits and writes Services and Volumes one
  at a time. A start-first update needs room for one more task.
- **Jobs** (phase 3, `controller/plan/jobs.rs`, part of the same pure
  function) own tasks as Services do, so scheduling, quotas, draining and
  lost nodes treat them alike; their tasks carry `job: true` and the label
  `legion.job`, and are no Service's endpoints. A replicated Job has
  completion slots `1..completions`, `parallelism` (1) running at once; a
  global one a slot per eligible node, plus those it completed on. A slot
  is done when a task in it completes. A failed task is retried in its
  slot as a new task, after 10 s doubling per failure in a row (at most
  60 s); the Job fails once more than `backoffLimit` (6) tasks failed,
  with the last failure in its message, or when it runs past
  `activeDeadline`, and its running tasks are stopped. Tasks stopped by a
  drain or lost with their node are retried without counting. The pod's
  restart condition defaults to `none` for Jobs and may not be `any`. A
  finished Job keeps its tasks until it is deleted, which removes them.
  Status: `state` (running, complete, failed), `started`, `finished`,
  `active`, `succeeded`, `failed`, `message`. Admission does not check
  Jobs against quotas: tasks beyond the quota wait `pending`.
- **CronJobs** make a Job, `<cronjob>-<minutes since the epoch>` labelled
  `legion.cronjob`, for the latest run due since `status.lastSchedule`
  (five-field cron, UTC; set to the time the controller first sees the
  CronJob, so earlier runs are not owed, and a controller that was away
  owes the latest run only). A run is skipped, with the reason in the
  status message, while `suspend` is set, when it is more than
  `startingDeadline` late, or under `concurrencyPolicy: forbid` while an
  earlier Job runs; `replace` deletes the running Jobs first, `allow` (the
  default) runs alongside. The newest finished Jobs are kept per outcome
  (`successfulJobsHistoryLimit` 3, `failedJobsHistoryLimit` 1), the rest
  deleted; deleting a CronJob deletes the Jobs labelled with it. A
  CronJob's name is at most 52 characters, leaving room for the suffix.

### 3.4 Agent components

| Component | Responsibility |
|-----------|----------------|
| **Session** | Connects to the leader, receives the assignment set, reports task state. Persists the last assignment set in a local `redb` so running tasks survive a manager outage and agent restarts. |
| **Executor** | Drives a **Legion-owned Podman instance** through the libpod REST API (section 6). Builds each task as an infra jail plus child container jails, pulls images, runs init containers in order, starts containers, streams logs and events. Every Podman object carries `legion.task.id` and `legion.node.id` labels, so the agent can always reconcile Podman's view against assignments. |
| **Network** | Creates epairs, attaches them to the node bridge, moves the jail end into the pod's VNET, programs routes, WireGuard peers and pf anchors. See section 5. |
| **Volumes** | Creates, mounts and snapshots ZFS datasets; mounts them into containers (nullfs). A `Volume` is `<pool>/legion/volumes/<ns>/<name>`, created on first use with its size as quota and bound to that node in its status. Pod `tmpfs` volumes live and die with the pod. |
| **Secrets** | Writes secrets and configs to a per-task tmpfs (`<run dir>/tasks/<task uid>`) and bind-mounts each file read-only into the container that names it. Files never touch disk. Values used as environment variables are passed to Podman, which keeps them in the container's configuration under its root; files are the safer form. Values are read when the pod starts. |
| **Resources** | Applies rctl rules to **each container's own jail** (memory, CPU percentage, process and open-file limits) and `cpuset` pinning. Removes the rules when the task ends; rctl keys rules by jail name and keeps them after the jail is gone. Reads usage with `rctl -u` for metrics and autoscaling. See 6.3. |
| **Sessions** | Runs the `logs`, `exec` and `port-forward` sessions managers offer it: Podman's log stream, an exec attached through libpod's upgraded connection (stdin, a terminal and its size), or a TCP connection to the pod's address. See 3.8. |
| **Probes** | Startup, liveness and readiness probes. TCP, HTTP and gRPC probes run from the host, which can reach every local pod IP on `legion0`; exec probes run in the container. One prober per running container reports only transitions to the task's worker: a failed startup or liveness probe stops the container and the restart policy decides what follows; readiness sets the Task's `ready`. HTTP passes on 2xx and 3xx; gRPC uses `grpc.health.v1.Health/Check`. |
| **DNS** | Resolver on each node's bridge gateway address (`hickory-dns`). Answers cluster names, forwards everything else to the host's resolvers. |

### 3.5 Scheduling

Filter, then score, per task:

1. **Filters**: node is `ready` and `active` (not drained or paused); platform
   matches the image (os/arch, and `linux` capability for Linux images);
   resource **requests** fit in allocatable capacity; constraints match node
   labels; the task tolerates every taint on the node; required anti-affinity
   holds; host-mode published ports are free; stateful tasks go to the node
   holding their volume unless migration is allowed.
2. **Scores**: placement preferences (spread across a label such as `zone`);
   fewest tasks of the same service on the node; least requested resources;
   preferred affinity.

The scheduler places by **requests**; rctl enforces **limits**. A task with no
requests is scheduled as zero-cost but is the first to be evicted when a node is
under memory pressure (a simple form of Kubernetes QoS classes).

### 3.6 Rust workspace

```
crates/
  legion-proto      protobuf definitions, generated tonic/prost code
  legion-api        object model, validation, defaults, YAML/compose conversion
  legion-store      openraft state machine, redb storage, watch streams
  legion-manager    API server, controllers, scheduler, allocator, dispatcher, CA
  legion-agent      session, executor, probes, volumes, secrets, resources
  legion-net        FreeBSD networking: interfaces, routes, WireGuard, pf, DNS
  legion-podman     typed libpod REST client
  legion-sys        thin FreeBSD bindings (ioctl, rctl, jail, ZFS via libzfs_core or zfs(8))
  legion            the binary: daemon and CLI (clap)
```

Main dependencies: `tokio`, `tonic`/`prost`, `openraft`, `redb`, `rustls`,
`rcgen`, `hyper`, `hickory-dns`, `clap`, `serde`, `tracing`, `nix`.

Pinned choices so far: `openraft` 0.9.25 (the stable line; 0.10 is still alpha)
with its v2 storage API, `redb` 4.4, `tonic`/`prost` 0.14 with `protox` (a
Rust protobuf compiler, so building needs no `protoc`), `serde_yaml_ng` for
manifests, Rust edition 2024.

System calls start as a trait with two implementations: one that shells out to
`ifconfig`, `route`, `pfctl`, `wg`, `rctl`, `jail` and `zfs` (fast to build,
easy to debug), and native ioctl / netlink / `libpfctl` versions that replace it
per operation once each path is stable. Tests use a fake implementation. Every
command used in the shell implementation has been exercised in the phase-0 lab.

`legion-sys` implements this as the `Sys` trait (synchronous, so the agent can
hold it as `Arc<dyn Sys>` and call it on the blocking pool), with `Shell` and
`FakeSys`. Its live tests run on FreeBSD 15.1: network and pf inside a
throwaway VNET jail, rctl on the host against lab jails, ZFS on a file-backed
pool. Details they pinned down:

- A pod's interface is configured from the host with `ifconfig -j <jail>` and
  `route -j <jail>`; nothing needs to exist inside the image.
- `jls --libxo=json` gives every value as a string; a child jail is named
  `<parent>.<child>`.
- `rctl -r jail:<name>` fails with "No such process" when the jail has no
  rules (treated as success); `rctl -u` for an unknown jail reports zeros.
  Memory amounts come back in bytes.
- `pfctl -T replace` with no addresses empties a table; `-T show` on a missing
  table fails with "Table does not exist" (treated as empty).
- `zfs create -p` succeeds on an existing dataset, so creation is idempotent.
- **Kernel modules**: after a reboot `if_bridge` and `if_epair` are not loaded,
  and a jail cannot load them on demand (`SIOCIFCREATE2: Invalid argument`).
  The agent loads `if_bridge`, `if_epair` and `pf` at startup.

### 3.7 Store internals

Built and tested in spike S4 (`crates/legion-store`).

- **One redb file** (`store.redb`) holds the Raft log, vote, committed id, the
  objects and the current snapshot. Every log append and every apply batch is
  one fsynced transaction.
- **Persistent state machine.** An apply batch writes its objects and the last
  applied log id in the same transaction, so a restart needs no log replay and
  reads are served as soon as the store opens.
- **Commands carry no replica-specific data.** `Put` and `Delete` take an
  optional precondition (`MustNotExist`, or `Version(n)` for optimistic
  concurrency). The state machine sets `version` to the log index, derives
  `uid` from the creating index, and bumps `generation` only when the spec
  changes, so every replica computes identical objects.
- **Keys** are `Kind/namespace/name`, so listing a kind, or a kind within a
  namespace, is a range scan.
- **Watches.** The last 4,096 applied events stay in memory. `list` returns
  objects together with the version they were read at; `watch(version)` then
  replays the held backlog and continues live, with no gap and no duplicates.
  A version older than the held history fails with `Compacted`, and a snapshot
  install ends watches with `Reset`; in both cases the client lists again
  (the Kubernetes list-then-watch pattern). History starts empty after a
  restart.
- **Snapshots** are the full object set as JSON; meta (last log id,
  membership) travels beside it. Encoding is JSON throughout for now, which
  keeps the file inspectable; a binary encoding is a later optimisation.
- **Shutdown.** `Raft::shutdown` returns before openraft's state machine worker,
  snapshot builder and replication tasks drop their database handles, so
  `Store::shutdown` waits for the file to close before returning. Without this,
  reopening the same directory in-process fails intermittently with
  `DatabaseAlreadyOpen`.
- **Blocking work.** Transactions that commit (and so fsync) run on tokio's
  blocking pool, so the API server's async workers never wait on the disk.
  Reads run inline. The committed log id is not persisted (openraft's
  default): the state machine already records what it applied, and saving it
  cost one more fsync per write on every node.
- **Timing.** Heartbeats every second, elections after 5 to 10 s of
  silence (Swarm elects after about 10 s). openraft also uses the
  heartbeat interval as the timeout of each replication call, so it must
  exceed the slowest round trip between managers: at 250 ms, managers 225
  ms apart never kept a leader (the phase 2 exit test). Writes and
  linearizable reads give up after 30 s without a quorum, with an error
  saying the write may still apply, rather than hang.
- **Backup and restore** (phase 4). `legion backup -o FILE` (cluster
  admin) has the leader read every object at one linearizable version,
  with the managers' ids and addresses, and streams it as JSON (file mode
  0600: it holds Secrets and join tokens). The managers' keys are not in
  it; `<data-dir>/pki` is backed up beside it. `legion restore [FILE]`
  runs offline on a stopped manager's data directory, its `pki` and
  `node-id` in place: it sets the old store aside (`store.redb.<time>`,
  refused while a daemon holds it) and writes a new one in which the
  backup's objects are applied at the backup's version, the log is purged
  up to it, a snapshot of them is kept for joiners, and this manager is
  the only voter, at the address the backup records for it or
  `--advertise`. Without FILE it reads the manager's own store: a cluster
  that lost its quorum restarts from a survivor. The daemon then leads a
  cluster of one; versions and uids carry on above the backup's, and
  managers join again from empty data directories, with the backed-up
  tokens. Agents keep their certificates (the CA is the same) and
  reconcile: tasks created after the backup are stopped, and the
  controllers replace what is missing. Raft calls from managers outside
  the current membership are refused (unless the manager knows no
  members yet, as when joining), so the old cluster's managers cannot
  take the restored one back: openraft would otherwise accept their log
  over its own (in debug builds it panics). The other managers should
  still be stopped before restoring, as their agents would keep following
  them.

---

### 3.8 API server and daemon

Built in phase 1 (`crates/legion-manager`, `crates/legion`).

- **One gRPC port per manager** (default `7470`) serves three services:
  `Objects` (Apply, Get, List, Delete, Watch), `Cluster` (Init, Status) and
  `Raft` (the transport between managers). It listens on `127.0.0.1` by
  default. Since phase 2 every connection is mutual TLS with certificates
  from the cluster CA (§9); the API takes admins and managers, Raft only
  managers.
- **Wire format.** The object envelope is a protobuf message; spec and status
  travel as JSON bytes until each kind gets a typed message. Raft messages are
  openraft's own types as JSON inside one bytes field, so the proto file does
  not have to track openraft's structures. Snapshot chunks are 1 MiB and the
  message limit is 64 MiB, which leaves room for JSON's expansion of binary
  data.
- **Server-side apply.** Apply creates the object or replaces its labels and
  spec, keeps its status, and reports `created`, `updated` or `unchanged`
  (an unchanged apply writes nothing). Last applier wins: a lost race is
  retried on top of the newer write, unless the caller passed
  `expected_version`, which turns the race into `ABORTED`. `Task` objects are
  rejected. A namespaced object with no namespace goes to `default`.
- **Any manager answers.** Writes and Get/List go to the leader; a follower
  forwards the request once, marked so it is never forwarded twice. Get and
  List call `ensure_linearizable` first. Watches are served from the local
  replica. During an election the API returns `UNAVAILABLE` and clients retry:
  the CLI asks again for up to 30 s (single calls only, not streams).
- **Shutdown.** SIGTERM (rc.d) or Ctrl-C stops accepting requests, ends open
  watch streams with `UNAVAILABLE` (otherwise graceful shutdown would wait on
  them forever), then closes the store.
- **Identity.** A manager generates a random 64-bit node id on first start and
  keeps it in `<data-dir>/node-id`. A new daemon belongs to no cluster;
  `legion init` makes it the first member.
- **Sessions** (phase 3). `logs`, `exec` and `port-forward` go through the
  `Pods` service of any manager, which checks RBAC (logs take `view` in the
  task's namespace, exec and port-forward `edit`), resolves the task (or a
  running task of the service named) and its container, and offers the
  session to the agent of the task's node. Every agent keeps a `Sessions`
  stream open with every manager, so any manager reaches any node without
  forwarding between managers and without a port on the node: nodes behind
  NAT work too. The agent answers an offer with `Attach` to that manager,
  which then relays frames both ways: input, input closed and terminal
  size from the user; stdout, stderr and the exit code back, or an error
  that ends the session. A user who goes away ends the agent's side, which
  drops the process's streams; the process itself may run on, as with
  Docker. Port-forward is one session per TCP connection, from the agent
  to the pod's address. A worker may attach only to its own node's
  sessions.
- **Output of finished tasks** (phase 3). Before it removes a pod, the
  agent saves each container's output, the last 1 MiB, under
  `<data-dir>/logs/<task uid>`, and keeps it until the task is no longer
  assigned to the node, which is when the task is deleted. `logs` on a
  task whose pod is gone reads the saved copy (following it ends at its
  end); on a Service or Job with no running task it picks the task that
  last ended on a node. A restarted container's earlier runs are not
  kept.
- **Metrics** (phase 3). With `--metrics-listen <addr>` the daemon serves
  Prometheus's text format at `/metrics` over plain HTTP, without
  authentication: the endpoint names every namespace's services and
  tasks, so it belongs on an admin network. Every manager reports its
  Raft state (leader, term, log and applied indexes, voters, and on the
  leader each follower's lag). Only the leader reports the cluster:
  objects by kind and namespace, tasks by state, restarts of tasks not
  ended, services' replicas and ready tasks, nodes, and each quota's
  limits beside what the namespace uses of them. A sum across managers
  then counts each thing once. The agent reports each running
  container's usage from rctl on the container's own jail: CPU time and
  percent, resident and virtual memory, swap, processes, threads, open
  files and storage I/O (the infra jail, which counts its containers
  again, is left out). The kernel refreshes memory once a second, so a
  container started within the last second may show none. Without
  racct the usage is missing and `legion_agent_usage_unread` counts the
  containers concerned. The manager's metrics read the store through a
  slot emptied before the store shuts down, so a scraper holding a
  connection open never keeps the store open.
- **CLI.** `legion init | status | apply -f | get | list | delete | watch`,
  with kubectl-style kind names (`service`, `services`, `svc`), `-n`, `-A`,
  `-l key=value` and `-o table|yaml|json`. `apply` reads multi-document YAML
  or JSON, or stdin with `-f -`.

## 4. Feature set

### 4.1 From Swarm (kept)

- `legion init`, `legion join <token>`, join tokens that embed the CA digest.
- Automatic PKI: node certificates, rotation, optional manager autolock.
- Raft managers embedded in the daemon; promote and demote nodes.
- Replicated and global services; replicated and global jobs.
- Rolling updates and rollback with Swarm's knobs (parallelism, delay, order
  `start-first`/`stop-first`, failure action, monitor window).
- Placement constraints and preferences; node labels; drain and pause.
- Secrets and configs.
- Service VIPs, DNS round-robin mode, routing mesh with ingress and host
  publishing modes.
- Compose v3 `stack deploy`.

### 4.2 From Kubernetes (adopted)

| Feature | Legion form | Phase |
|---------|-------------|-------|
| Pods | A task is a pod: one infra jail owning the VNET, one child jail per container. Sidecars are just more containers. | 1 |
| Init containers | Run in order to completion before main containers start. | 3 |
| Probes | Separate `startup`, `liveness`, `readiness`. Readiness controls load-balancer and DNS membership. Liveness failure restarts the container. | 1 (liveness, readiness), 3 (startup) |
| Requests and limits | Requests for scheduling, rctl limits for enforcement. | 2 |
| Taints and tolerations | `NoSchedule` and `NoExecute` effects. | 2 |
| Namespaces | Scope names, DNS, quotas, RBAC and default network isolation. | 3 |
| RBAC | Built-in roles `admin`, `edit`, `view`, bound per namespace to certificate identities. No custom roles at first. | 3 |
| Labels and selectors | On every object; used by constraints, policy and the CLI. | 1 |
| CronJobs | Cron schedule, concurrency policy, history limits. | 3 |
| Network policy | Ingress and egress rules by namespace, label and port; compiled to pf. | 3 |
| Disruption budgets | `maxUnavailable` on the service, honoured by drain and updates. | 3 |
| Stateful services | `mode: stateful`: stable names `<svc>-<n>`, per-ordinal ZFS volumes from claims, ordered rollout (3.3). Data moves between nodes with `zfs send/recv`. | 4 |
| Ingress | `Route` objects (host, path prefix, service, port) served by an HTTP proxy in every agent that enables it (5.8). | 4 |
| Autoscaling | CPU and memory targets read from rctl; no separate metrics server. Every 15 s an agent's heartbeat carries what each running task's app containers use together (rctl `pcpu` and `memoryuse`); the leader keeps the reports in memory for a minute, believing each only from the task's own node. Each pass, a service with `autoscale` wants `ceil(measured × use / target)` replicas over its ready tasks with a report, for the target asking most, between `min` and `max`, and keeps its count while use is within a tenth of every target; ready tasks without a report count as idle for growing and on target for shrinking. Passes are far more frequent than reports, so the count follows from the reports alone. It grows at once and shrinks only to the most recommended over the last `downDelay` (5 min); a new leader waits that long before shrinking. Admission needs a target, `1 ≤ min ≤ max`, and every container requesting what a target measures; quotas count `max` tasks, so the service fits whatever it scales to. | 4 |
| `apply` / `diff` | Server-side apply with field ownership kept simple: last applier wins per object. | 1 |
| `logs`, `exec`, `port-forward` | Streamed through the manager to the agent, under RBAC (3.8). | 3 |

### 4.3 FreeBSD-specific

| Feature | Notes | Phase |
|---------|-------|-------|
| ZFS image and volume storage | Podman's ZFS storage driver for images (one dataset per layer); one dataset per volume under `<pool>/legion/volumes/<ns>/<name>`. | 1 |
| Snapshots before updates | A stateful service's `update.snapshot: true`: each task an update starts snapshots its claimed Volumes (`@legion-update-<generation>`, named in `status.update.snapshot`) before it runs, unless taken already, dropping older updates' snapshots; its old task has stopped by then, as stateful updates are stop-first. Each task the update's rollback starts, automatic or by `legion rollback` (even after the update completed, which discards what was written since), rolls its Volumes back to the snapshot and drops it, so a restart keeps its data. An ordinal the update never reached has no snapshot and keeps its data; so does a Volume migrated since, as migration sends no other snapshots. | 4 |
| Stateful migration | Draining a node moves each wanted stateful ordinal's Volumes to a node its service may run on, the one with fewest of its tasks: the Service controller records a migration in the Volume's status (`copying`); the leader's migrator has the old node's agent snapshot and `zfs send` a full copy while the task runs, and the new node's `zfs receive -u` it, joining the two agents' sessions (3.8), so nodes need no port. Once `copied`, the drain stops the task; with nothing on a node using the Volume it is `finishing`: a second snapshot's increment goes, the Volume becomes the new node's (mounted, the migration's snapshots gone) and the old node drops its copy. The slot's next task waits (`volume moving`) and then runs there. A migration whose new node goes away, or whose old node stops draining before the last part, is dropped and started again if still needed; a failed attempt is noted in it and retried. A stale copy on the receiving node is replaced. A Volume's `spec.node` (`legion volume move VOLUME NODE`) asks for its data on a node: unset, the first task to use the Volume chooses; set before then, that task goes there; set to another node than the data's, a migration to it starts once the node is ready and active, and the tasks using the Volume on the old node stop once it is copied, as off a draining node, their next ones waiting until it has moved (`volume moving` holds new placements throughout a migration). A drain still moves a Volume asked to be on the draining node; the data returns once the node is active again. A migration lasts while a drain or the Volume's `node` calls for it. | 4 |
| CARP manager VIP | Optional floating address for the API: `legion daemon --vip ADDR --vip-interface IF --vip-password-file F [--vip-vhid N]` on managers sharing a link puts ADDR on IF as a CARP virtual host (`/32`, loading `carp`). Any manager answers the API, so the address need only be on a running one: the leader advertises skew 0 and forces itself master when it is not, followers advertise 100 and keep the address only until a better advertiser takes it, so it never goes without a holder (a follower holds it while a manager without `--vip` leads). Set again every 5 s and on every leadership change; released on shutdown, so a clean stop hands it on at once and a crash after CARP's timeout (3 s). The API listens on `0.0.0.0` to answer on it; certificates name the fixed server name, so no change is needed for clients using it. | 4 |
| Security profiles | Per-service jail parameters (e.g. `allow.raw_sockets`, securelevel, enforce_statfs), with optional MAC policy. | 3 |
| Linux images | Via `linux64` on nodes labelled with the `linux` capability. | 5 |
| bhyve tasks | `runtime: bhyve` for workloads that need their own kernel. | 5 (exploratory) |

---

## 5. Networking

### 5.1 Model: routed, not overlaid

Legion uses a **flat routed pod network**, not a VXLAN layer-2 overlay. Every
task gets one IP from the cluster pod CIDR. Every node owns a slice of it. Nodes
route to each other over WireGuard. Network separation (compose networks,
namespaces, policy) is enforced by pf, not by separate layer-2 segments.

Reasons:

- FreeBSD's `if_vxlan` is built around one remote or a multicast group. A
  full unicast mesh would need one VXLAN interface per peer per network, plus
  split-horizon handling in the bridge. That is fragile and grows as nodes × networks.
- WireGuard is in the kernel, encrypts everything, and its per-peer allowed IPs
  give us routing for free.
- One IP per task makes DNS, load balancing, policy and debugging much simpler.

Trade-off: no multicast or broadcast between containers across nodes, and
networks cannot have overlapping CIDRs. Both are acceptable for the target
workloads.

### 5.2 Addressing

| Range | Default | Purpose |
|-------|---------|---------|
| Underlay | `100.64.0.0/16` | One `wg0` address per node |
| Pod CIDR | `10.200.0.0/16` | One `/24` per node, one IP per task |
| Service CIDR | `10.201.0.0/16` | One VIP per service |

All three are set at `legion init` and are fixed for the life of the cluster.
`init` checks that they do not overlap networks already on the hosts (Podman's
default `10.88.0.0/16`, for example).

### 5.3 Per-node layout

```
            pod infra jail                   host
      ┌──────────────────┐     ┌───────────────────────────────────────────┐
      │ epairNb          │     │ epairNa ─┐                                │
      │ 10.200.3.17/24   │◄───►│          ├── legion0 bridge 10.200.3.1/24 │
      │ mtu 1420         │     │ epairMa ─┘    (DNS on :53)                │
      │ gw 10.200.3.1    │     │                                           │
      │  └ container jails     │ wg0 100.64.0.3  mtu 1420                  │
      │    (children, share    │   peer node2: allowed 100.64.0.2/32,      │
      │     this VNET)   │     │               10.200.2.0/24               │
      └──────────────────┘     │ route 10.200.2.0/24 -> wg0, ...           │
                               │ pf: legion/svc, legion/ingress,           │
                               │     legion/policy anchors                 │
                               └───────────────────────────────────────────┘
```

- The agent creates `legion0` and assigns the node's gateway address.
- Per task: create an epair, add the `a` end to `legion0`, move the `b` end into
  the pod's infra jail (`ifconfig epairNb vnet <infra>`), set address, MTU,
  default route and `resolv.conf`. When the infra jail is destroyed, the `b` end
  returns to the host and the agent destroys the pair.
- **MTU**: pod interfaces get the `wg0` MTU (1420). Path-MTU discovery does
  work over WireGuard (S3), but Legion should not rely on it; pf also clamps
  MSS (`scrub ... max-mss`) for traffic that leaves via `wg0`.
- The dispatcher sends each agent the full peer list (public key, endpoint,
  pod subnet). The agent converges `wg0` peers to it incrementally with
  `wg set`; S6 showed 100 peers added and removed in under a second with no
  loss on existing peers.
- **Built** (`legion-net`, phase 1): a node is given the lowest free /24 of
  the pod CIDR when it first registers, kept in `Node.status.podCidr`. The
  agent allocates pod addresses from it (the lowest free one, recorded in
  `Task.status.ip`, kept across agent restarts), and renames each pod's host
  end `lp<last 12 hex digits of the task uid>`, which finds it again after a
  restart. Teardown destroys the host end first, which removes the pair
  even while the other end is still in the jail.
- **Built** (phase 2): the mesh. Each node makes a WireGuard key on first
  start (`<data-dir>/wireguard.key`, mode 0600) and registers its public
  key and endpoint (its advertised address, port 51820) in
  `Node.status.wireguard`. Its `wg0` address is derived from its pod
  subnet (`10.200.<n>.0/24` has `100.64.<n>.1/16`), so nothing more is
  allocated. One static route sends the whole pod CIDR into `wg0`; the
  node's own /24 on `legion0` is a longer match, and WireGuard picks the
  peer by allowed IPs (the peer's /24 and underlay /32), so routes never
  change as nodes come and go. Peers have a 25-second keepalive; a node
  without an endpoint is reached once it has reached the other side. The
  agent applies the peer list only once it has one, so a restarting agent
  does not cut traffic. Down nodes stay peers until their Node is removed.
- **Reachability**: managers must reach each other's advertised addresses
  (Raft, both ways), every agent must reach a manager, and nodes must reach
  each other's WireGuard endpoints. A node behind NAT cannot be one today:
  its advertised address is unreachable from outside, and the endpoint it
  advertises replaces the one its peers' WireGuard learned from its
  keepalives. Such a node joins through a site tunnel or VPN and
  advertises its address there (the phase 2 exit test does this for a
  remote server); traversing NAT without one is later work.
- **Bridge sysctls**: `net.link.bridge.pfil_*` are per-VNET, and Legion runs
  in the host VNET, so they apply to every bridge on the host, Podman's own
  included. Load balancing needs none of them (the hairpin NAT rule in 5.4
  works either way), but network policy (5.6) does: FreeBSD 15 defaults
  `pfil_member` to 0, so pf never sees traffic bridged between pods, and
  without `pfil_local_phys` a pod's frames for the host are filtered only
  on the bridge, after a VIP's `rdr pass` has translated them. The agent
  sets both to 1 at start (decided with the user, phase 3). Other bridges'
  members are then filtered too, which a pass-by-default main ruleset does
  not notice; Legion nodes are expected to be dedicated.

### 5.4 Services and load balancing

pf does the load balancing, programmed by the agent on **every** node. All of
this was verified in a 3-node lab (S3).

- **Legion's anchors, not the main ruleset.** The host's pf.conf belongs to
  the operator (and to Podman's CNI, on a host that runs other containers);
  Legion loads only the anchors under `legion/` that pf.conf calls with
  `nat-anchor`, `rdr-anchor` and `anchor "legion/*"`. Translation rules use
  `pass`, so they create states whatever the host's filter policy, and
  killing a dead backend's states makes the client's next packet fail fast.
  (A default-block policy for pod traffic belongs with network policy,
  §5.6.)
- One pf table per service holding its **ready** endpoints, cluster-wide.
- East-west traffic (task to VIP):
  `rdr on legion0 proto tcp to <VIP> port 80 tag LSVC -> <svc_web> port 8080 round-robin`.
  Replies come back through the same node, so pf's floating states translate
  them back. Round-robin distributes evenly across local and remote backends.
- **Same-node hairpin**: when the chosen backend is on the same bridge as the
  client, the reply bypasses the host's IP layer. Legion always installs
  `nat on legion0 from <podnet_local> to <podnet_local> tagged LSVC -> (legion0)`.
  (Without it, the hairpin case times out unless `pfil_bridge=1`.)
- **Routing mesh** (`publish: ingress`): `rdr on $ext_if` from the published
  port to the service table, plus `nat on wg0 from ! <podcidr> to <podcidr> -> (wg0)`
  so replies from remote backends return through the ingress node (built:
  one rule for every service, since pod traffic is never matched). Without
  that NAT rule, every remote backend times out. As in Swarm, the backend sees
  the ingress node's `wg0` address, not the client's. `publish: host` keeps the
  client address and forwards only to local tasks.
- **Readiness changes only update the table.** Existing connections keep their
  state and finish (soft drain, verified: a live stream completed after its
  backend was removed from the table).
- **Draining a stopping task.** A task told to stop leaves the endpoints at
  once, but every other node learns it through its own stream and then
  reloads its table, a few hundred milliseconds later across a WAN; until
  then it still sends the task new connections. The task's agent therefore
  keeps a ready Service task running for a drain (2 s) before stopping it,
  as Kubernetes users do with a preStop sleep. Found as the odd request
  timing out during the phase 2 exit test's rolling update. Job tasks and
  tasks never ready stop at once.
- **Dead backends**: the agent lists states and kills those whose translation
  targets the dead backend, by state key or ID (`pfctl -k key -k '<key>'`, or
  `pfctl_kill_states` via `libpfctl`). Killing by address does not work for
  this: `pfctl -k 0/0 -k <backend>` matches nothing because the inbound state
  is keyed by the VIP.
- `endpoint: dnsrr` skips the VIP: DNS returns task IPs directly.
- Host processes cannot reach VIPs or published ports via the host's own address
  (pf `rdr` does not apply to locally generated traffic). They use DNS
  round-robin names instead.

### 5.5 DNS

The node resolver answers:

| Name | Answer |
|------|--------|
| `<svc>` (same namespace) / `<svc>.<ns>` | VIP, or ready task IPs for dnsrr |
| `<svc>-<n>.<svc>.<ns>` | Stateful replica `n` |
| `tasks.<svc>.<ns>` | All ready task IPs |
| anything else | Forwarded to the host's resolvers |

Data comes from the endpoints stream, so answers are local and fast.

Built (phase 1): names live under `legion.` (`web.shop.legion`); pods get
`search <ns>.legion legion` and `ndots:2`, so `web` and `web.shop` work as
well. Cluster answers carry a 5-second TTL and the authoritative flag;
unknown names under `legion.` are NXDOMAIN and never leave the node.
Answers too large for UDP set the truncation flag and the client retries
over TCP.

### 5.6 Network policy

- Compose networks and namespaces become pf tables of member IPs.
- Default: a task can reach tasks in the same namespace and the node DNS; the
  namespace can be marked open or locked down.
- `NetworkPolicy` objects add allow rules by namespace, labels and ports, for
  ingress and egress.
- Rules are enforced on the **destination** node. With `pfil_member=1`
  (5.3), pf sees traffic between tasks on one node on the task's own
  epair (`out on lpX`, toward the jail). It does not see there what the
  host itself sends onto the bridge, which includes everything routed in
  from other nodes, so traffic from other nodes' tasks is checked where it
  arrives, `in on wg0`, against the same tables. A misbehaving node cannot
  bypass rules enforced on another.
- Each policy compiles to rules in the `legion/policy` anchor that reference
  tables. Membership changes update tables only; rules are reloaded only when
  policies change.
- **Built** (phase 3; `legion_api::policy`, `legion-agent::policing`,
  `legion-net::policy`): `Namespace.spec.isolation` is `namespace` (the
  default), `open` or `locked` (not even the namespace's own tasks).
  `NetworkPolicy` selects services of its namespace by labels
  (`services`) and lists `ingress` rules: `from` peers (namespace,
  default its own; service labels, default all; none listed: every task)
  and `ports` (the tasks' ports, default all). The dispatcher streams the
  non-default isolations and every policy to the agents (`Policies`), and
  the endpoints now carry each service's labels. On each pod's host end
  the agent allows in: the nodes (this gateway and the mesh range: probes,
  other hosts, published ports reached through another node), traffic a
  published port's or VIP's `rdr` translated here (`tagged LSVC`), and a
  table per allowed peer set, on its ports; and drops the rest. A pod may
  send only from its own address and never IPv6; what it starts is kept as
  `if-bound` state, so replies pass its own guard (a floating state would
  match the same packet again on the bridge before the VIP's translation).
  On the mesh, connections from other nodes' tasks are let in from the
  same tables and the rest from the pod range dropped; what a pod starts
  toward other nodes is kept as (floating) state on `wg0` too, so the
  replies are not taken for new connections. Found by the phase 3 exit
  test: the guards on the host ends had never been evaluated for traffic
  from other nodes, so a task reached any other namespace's tasks there.
  VIP traffic to a task on the same node is NATed to the gateway by the
  hairpin rule, losing its source, so VIPs are checked at the client pod
  instead (`in on lpX`, still to the VIP; this is what needs
  `pfil_local_phys`): each pod may reach the VIP ports whose tasks would
  let it in. Egress policies are not built yet.

### 5.7 IPv6

Dual-stack is planned for phase 5. The model (per-node prefix, WireGuard
routing, pf tables) carries over unchanged.

### 5.8 Ingress

HTTP routing by host and path. Earlier drafts planned a system global
service running haproxy from rendered routes; the agent proxies itself
instead, with the hyper it already has, so no image has to reach every
node and routes change without reloads.

- **`Route`** (namespaced): `host` (lower case; `*.example.org` takes any
  one label in front), `path` (a prefix of whole segments, `/` by
  default; `/shop` takes `/shop/cart`, not `/shopping`), `service` (of the
  route's namespace) and `port` (the service's port, needed when it has
  more than one TCP port).
- **Every node that enables it** (`legion daemon --ingress-listen
  0.0.0.0:80`) runs the proxy. The dispatcher streams every route to the
  agents (`Routes`), beside the endpoints they already follow. A request
  goes to the route matching its `Host` (port dropped, case ignored) with
  the longest path, an exact host before a wildcard; then to the next of
  the service's ready tasks in turn, at the task's address and target
  port, which the node reaches directly (nodes may reach every pod, 5.6).
  So, unlike the routing mesh, there is no NAT: the backend sees the
  node, and the client in `X-Forwarded-For` (with `X-Forwarded-Host` and
  `-Proto`). Upgrades such as WebSocket pass through. No route: 404; no
  ready task: 503; a task that does not answer: 502 (504 after 5 s).
- **HTTPS** (`--ingress-tls-listen 0.0.0.0:443`): a route with `tls: {
  secret }` names a Secret of its namespace holding the certificate chain
  and its private key in PEM. Every node may read such a Secret (the
  dispatcher allows a Secret that a route names as well as those its
  tasks use), as any node may serve the route. The proxy chooses the
  certificate by SNI, an exact host before a `*.` one, and reads the
  Secrets again when routes change and every minute, so a renewed
  certificate takes over; one that cannot be read or parsed keeps the
  last good one. A host without a certificate fails the handshake. The
  backend gets `X-Forwarded-Proto: https`. Plain HTTP still serves the
  route; redirecting it is left to the backend.
- **Not yet:** HTTP/2, connection reuse to backends, retries on another
  task, certificates from ACME.

---

## 6. Runtime: Podman and ocijail

### 6.1 A Legion-owned Podman instance

The agent runs its own `podman system service` with **its own `--root`,
`--runroot` and `--tmpdir`** on a Legion ZFS dataset, and talks to it over a
private socket. This keeps Legion's containers apart from containers the
operator runs with the system Podman.

All three paths must be separate. Podman's temp directory (default
`/var/run/libpod`) is shared even when `--root` and `--runroot` differ, and
holds the "alive" marker Podman uses to detect reboots. Phase-0 testing showed
that resetting a second store with only `--root`/`--runroot` changed wiped that
directory, and the system Podman then marked a running container as exited
while its process kept running.

The storage driver comes from the host's `containers/storage.conf`. The
FreeBSD package sets `zfs`; on a UFS host Podman then refuses to start
("could not open /dev/zfs") until the driver is set to `vfs`.

Through the service's API, each exec session's conmon stays for Podman's
`exit_command_delay` after the command ends, so the client can still
read the exit code: five minutes by default. With an exec probe every
second that is some 300 conmon pairs per probed container. Legion reads
the code as soon as the output ends, so its Podman gets five seconds,
from a containers.conf override (`<run-dir>/podman.conf`, passed as
`CONTAINERS_CONF_OVERRIDE`) that leaves the system's configuration
alone.

### 6.2 Pod construction

Podman pods and `--network container:` do **not** give a private shared VNET on
FreeBSD (S1): with `--network none` the jails get `vnet=inherit`, which is the
**host's** network stack. Legion therefore builds pods itself:

1. **Infra container**: `--network none` plus the annotation
   `org.freebsd.jail.vnet=new`, running a trivial long-lived process. This
   gives a jail with an empty private VNET. Without the annotation, `none`
   means host networking, so the executor treats a missing `vnet=new` as a bug
   and refuses to start the task.
2. The agent sets `children.max` on the infra jail and attaches the epair
   (section 5.3).
3. **Each container**: `--network none` plus `org.freebsd.parentJail=<infra id>`.
   It becomes a child jail sharing the infra jail's VNET; it sees only `lo0`
   and the pod's epair.

Verified behaviour: containers in a pod share the interface; a container
restart keeps the pod's network; stopping the infra container kills all child
containers, so a pod lives and dies as one unit. All of this works through the
libpod REST API (annotations and labels in `containers/create`).

### 6.3 Resource limits

- `podman run --memory` / `--cpus` are **silently ignored** on FreeBSD (no rctl
  rules are created), so Legion applies limits itself.
- rctl rules on the infra jail **account** for all child processes but are
  **not enforced** on them. Limits are therefore set on each container's own
  jail (`jail:<infra>.<ctr>:...`), where they are enforced: `maxproc` refuses
  forks, `pcpu` throttles by pausing the process (usage is bursty rather
  than smooth), and the memory limit is `memoryuse:sigkill`: a process that
  takes the container's resident memory past it is killed, as Linux's OOM
  killer does in a container, which is what a compose `memory:` means.
  Legion denied on `vmemoryuse` at first, but that sums the address space
  of every process in the container: a small shell-and-inetd app with
  `memory: 128M` failed to fork under light load while using a few MB.
  `memoryuse:deny` is no better: it pages the jail out rather than
  refusing, and rctl(8) warns it can thrash the machine.
- rctl needs the loader tunable `kern.racct.enable=1`. Without it, setting
  limits fails and the agent only warns, so pods run unlimited.
- Pod-level totals are read from the infra jail's accounting and used for
  scheduling feedback, metrics and eviction decisions.
- The rule set is removed explicitly when a task ends.
- The agent sets its rules **after** each container start and restart, not
  before. ocijail with rctl support (0.7.0-dev, "next") enforces the OCI
  `linux.resources` limits itself and *replaces* a jail's rules when it
  creates the jail, also when the container has no limits, so rules set
  beforehand are lost. Re-adding a rule replaces it, so reapplying is
  harmless; adopted containers get their rules reapplied too.
- Open: with runtimes that enforce OCI limits, Legion could pass limits
  through Podman instead, so they apply from the first instruction. ocijail
  also kills on `memoryuse`, so the memory semantics now match.

### 6.4 Other runtime details

- Images: pulled by Legion's Podman with the ZFS storage driver
  (`zfs.fsname=<pool>/legion/podman`), one dataset per layer. Registry
  credentials are Legion secrets passed per pull. Container start on ZFS was
  slightly faster than on vfs in testing (about 0.19 s vs 0.24 s).
- Mounts: volumes, secrets and configs are bind (nullfs) mounts into containers.
- Restart: the agent restarts containers itself according to the task's restart
  policy.
- Metrics: libpod `stats` returns zeros on FreeBSD; usage comes from `rctl -u`.
- Events: the libpod event stream works and drives fast state updates; the
  agent also reconciles periodically in case events are missed. A container's
  `died` event carries its exit code (`containerExitCode`).
- libpod details the client (`legion-podman`) handles, from Podman 5.8.9 with
  ocijail 0.7.0-dev:
  `start` and `stop` answer 304 when the container is already in that state;
  exec in a stopped container is HTTP 500 "container state improper", not
  409; exec and log output use Docker's multiplexed framing; `wait` returns a
  bare exit code. Under the API service, each container's conmon lingers for
  `exit_command_delay` (300 s) after the container exits before it runs the
  cleanup command; this is expected and not a leak.
- FreeBSD base images contain files with the `schg` flag, so removing image
  storage by hand needs `chflags -R 0` first.
- Base images: the official FreeBSD OCI images (`freebsd-runtime` and related).
  These are minimal (no `grep`, `tail`, `true`), which is why network probes run
  from the host rather than in the container.

---

### 6.5 Native jail runtime (phase 5)

Podman gives Legion three things: pulling images, a root filesystem per
container, and supervising its processes. Everything FreeBSD-specific
(VNET, epairs, pf, rctl on each container's own jail, ZFS volumes)
Legion does itself, and most of the runtime's trouble came from the
layer in between: libpod's stats are zeros on FreeBSD, conmons that
outlive exec sessions, a `podman system service` per node that leaked
when killed, the shared tmpdir, vfs copying whole images, and an infra
container that exists only so a process holds the pod.

A native runtime keeps OCI as the image format and runs it on jails
directly: a registry client (auth, manifests and indexes) pulls layers
into ZFS, one snapshot per layer, applied with OCI whiteouts; a
container's root filesystem is a clone of its image's top snapshot; a
pod is a `persist` VNET jail with no processes, and each container a
child jail running the image's entrypoint as its user, supervised by the
agent (output to the per-task log files, exit codes, stop signals and
the grace period) in a way that survives agent restarts. Exec and
images no longer used complete it.

It is a second implementation of the agent's `Runtime` trait, chosen
per node, so the existing tests apply to it and Podman stays as the
fallback. It does not change rctl: memory limits for workloads with
large shared memory need their own answer either way.

It starts with a spike on a FreeBSD host, as phase 0 did: pull an image
into ZFS clones, run its entrypoint in a child jail of a `persist` VNET
pod jail under the agent's supervision, and compare start time and disk
use with Podman. If it holds, it becomes the default.

## 7. Object model

Every object has the same envelope:

```yaml
apiVersion: legion/v1
kind: Service
metadata:
  name: web
  namespace: shop
  labels: { app: web, tier: front }
  # set by the server:
  uid: 01J...
  version: 18342       # Raft index of last write
  generation: 7        # bumps on spec changes only
spec: { ... }
status: { ... }        # written by controllers and agents only
```

Kinds: `Namespace`, `Node`, `Service`, `Job`, `CronJob`, `Task` (internal, read
only), `Secret`, `Config`, `Volume`, `NetworkPolicy`, `Route` (ingress),
`RoleBinding`.

### Example service

```yaml
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  mode: replicated            # replicated | global | stateful
  replicas: 3
  pod:
    initContainers:
      - name: migrate
        image: registry.example/shop/web:1.4
        command: ["/app/migrate"]
    containers:
      - name: web
        image: registry.example/shop/web:1.4
        ports: [{ name: http, containerPort: 8080 }]
        env: [{ name: DB_URL, secret: db-url }]
        resources:
          requests: { cpu: 500m, memory: 256Mi }
          limits:   { cpu: 1, memory: 512Mi, maxproc: 256 }
        probes:
          readiness: { http: { path: /ready, port: http }, period: 5s }
          liveness:  { http: { path: /live,  port: http }, failureThreshold: 3 }
        volumeMounts: [{ name: uploads, path: /var/uploads }]
      - name: log-shipper
        image: registry.example/tools/shipper:2
    volumes:
      - name: uploads
        volume: { name: uploads }
    restartPolicy: { condition: on-failure, maxAttempts: 5, delay: 5s }
  placement:
    constraints: ["node.labels.disk == ssd"]
    preferences: [{ spread: node.labels.zone }]
    tolerations: [{ key: dedicated, value: shop, effect: NoSchedule }]
  endpoint:
    mode: vip
    ports: [{ name: http, port: 80, targetPort: http, publish: ingress, publishedPort: 8080 }]
  update:
    parallelism: 1
    delay: 10s
    order: start-first
    failureAction: rollback
  maxUnavailable: 1
```

### Task states

```
pending → assigned → accepted → preparing → starting → running → complete
                                    │            │          │
                                    └────────────┴──────────┴──► failed / rejected
                       (any state) → shutdown          node lost → orphaned
```

`ready` is a separate condition on a running task, set by its readiness probe.

### Admission

Every apply runs the object through admission (`legion_api::admit`) before it
is stored:

- The spec is parsed into the kind's Rust type. Unknown fields are errors, and
  errors name the field (`spec.pod.containers[0].image: missing field`).
- Cross-field checks: probe and service ports resolve to a container port;
  port names and numbers are unique across the pod, since its containers share
  one network stack; mounts name a pod volume and no target is mounted twice;
  requests do not exceed limits; placement constraints parse
  (`node.labels.<key>`, `node.name`, `node.hostname`, `node.role`,
  `node.platform.os`, `node.platform.arch`, with `==` or `!=`).
- The spec is stored **normalised**: every default written out and quantities
  in canonical form (`0.5` → `500m`, `1073741824` → `1Gi`). Controllers read
  specs without applying defaults, and two manifests that mean the same thing
  store the same spec, so re-applying either is a no-op.
- `Task` objects are written only by Legion. Kinds whose controllers do not
  exist yet are rejected rather than stored unchecked.

A `Secret` or `Config` holds one value, as in Swarm, given as `data` (base64)
or `text` and stored as `data`; at most 500 KiB. Containers get it as a file
(by default `/run/secrets/<name>` or `/run/configs/<name>`) or as an
environment variable.

`legion diff -f` sends a dry-run apply and prints a unified diff of each
object's labels and normalised spec against the stored one.

---

## 8. Compose support

`legion stack deploy -c compose.yaml <stack> [-n namespace]`:

- Each compose service becomes a Legion `Service` labelled `legion.stack=<stack>`.
- `deploy:` keys map one-to-one to Legion's (replicas, mode, placement,
  resources, update_config, rollback_config, restart_policy).
- `healthcheck:` becomes a liveness probe and, unless overridden, the readiness probe.
- Compose networks become policy segments (section 5.6); top-level volumes,
  secrets and configs map directly.
- Unsupported keys (`privileged`, `cap_add`, `devices`, `network_mode: host`, ...)
  produce warnings, or errors with `--strict`.
- An `x-legion:` extension on a service exposes features compose cannot express:
  probes, init containers, sidecars, stateful mode, tolerations, security profile.

`legion stack convert` writes the native YAML, for teams that want to leave
compose behind.

Built (phase 2, `legion_api::compose`, `legion stack deploy|convert|rm`):

- The namespace defaults to the stack's name and services keep their
  compose names, so `db` resolves as it would on a compose network. Names
  become DNS labels (`db_data` is `db-data`, with a warning).
- `${VAR}`, `${VAR:-default}`, `${VAR-default}`, `${VAR:?error}`, `$VAR`
  and `$$` are interpolated from the CLI's environment, as `docker stack
  deploy` does (no `.env` file); YAML merge keys work.
- Mapped: `image`, `command` (args) and `entrypoint` (command), `environment`
  and `env_file`, `working_dir`, `user`, `ports` (VIP port = target; short
  and long forms, `mode: host`), `expose` (container ports only),
  `volumes` (named volumes and `tmpfs`), `tmpfs`, `secrets` and `configs`
  (Swarm's default targets `/run/secrets/<source>` and `/<source>`),
  `healthcheck` (an exec probe used for both liveness and readiness, with
  Docker's 30 s defaults), `stop_grace_period`, and under `deploy`: `mode`,
  `replicas`, `endpoint_mode`, `labels` (on the Service), `placement`
  (`constraints`, `preferences`), `resources` (Docker sizes are binary),
  `restart_policy`, `update_config`.
- Top-level `volumes` become Volumes (`x-legion: { size }` sets a quota),
  `secrets` and `configs` with `file`, `environment` or `content` become
  Secrets and Configs; `external` ones are referred to, not created.
- Warned and ignored: bind mounts and anonymous volumes, `depends_on` and
  `restart` (as `docker stack deploy` does), `rollback_config` (a rollback
  uses the restored spec's update settings), networks (not separated
  until §5.6), and every key Legion does not map. `--strict` refuses them.
- `x-legion` on a service is merged into the generated spec (mappings key by
  key, lists replaced): probes, init containers, tolerations and so on.
- `deploy --prune` deletes the stack's Services the file no longer has;
  `stack rm` deletes its Services, Secrets and Configs and keeps Volumes.

---

## 9. Security

- **Cluster PKI**: a root CA is generated at `init` (or an external one is
  imported). Node certificates carry the node ID and role; default lifetime 90
  days, renewed automatically at two-thirds of their life.
- **Join tokens**: `LGN-1-<ca-digest>-<secret>`, with separate tokens for workers and
  managers, and rotation on demand. The digest lets a joining node verify the
  cluster it reaches before sending anything. Built: the joining node reads
  the CA from the manager's TLS chain and checks it against the digest,
  then sends a certificate request for its own key; managers also receive
  the CA key (every manager can certify nodes) and join Raft as learners,
  promoted to voters once caught up. Token secrets live in the store under
  the reserved namespace `legion-system`, which the API never exposes.
- **Users**: `legion user create alice --role edit -n shop` issues a client
  certificate bundle. RBAC binds the certificate identity to roles per namespace.
  Built (phase 3): user certificates carry the role `user`; the CLI makes
  the key and sends the manager only a certificate request (`IssueUser`,
  cluster admin only, any manager signs). A `RoleBinding` (`spec: {user,
  role}`) gives `view` (read all but Secrets), `edit` (also Secrets, and
  apply and delete) or `admin` (also the namespace's RoleBindings). Users
  never act on cluster-scoped objects, `init` or join tokens; lists and
  watches across namespaces show only what they may read. The manager a
  request reaches first decides, from its own replica, before forwarding,
  so a binding deleted takes effect there as soon as it replicates. The
  cluster admin (`admin.pem`) and managers keep every right.
- **Encryption at rest**: Raft log and snapshots are encrypted with a data key.
  With autolock, the data key is wrapped by a key held by the operator and
  managers need `legion unlock` after restart.
- **Secrets in transit and on nodes**: delivered only to nodes running tasks
  that use them; held in tmpfs; removed when the last task using them stops.
- **Data plane**: all node-to-node traffic goes over WireGuard. WireGuard keys
  are generated on the node; only public keys go through the managers.
- **Jail defaults**: no raw sockets, no mounting, `enforce_statfs=2`,
  `securelevel=2` where possible; profiles loosen these explicitly.
  Built (phase 3): every container's jail has no raw sockets, no
  `chflags`, no mlock, no SysV IPC and no mounting, with
  `enforce_statfs=2` and `securelevel=2`. A container's `security`
  (`allow: [rawSockets, chflags, mlock, sysvipc]`, `securelevel` -1 to 3,
  `enforceStatfs` 1 or 2) loosens that, and admission refuses a Service,
  Job or CronJob that loosens more than its Namespace's
  `spec.security.allow` lists (the same names, plus `securelevel` for a
  level below 2 and `enforceStatfs` for 1). Namespaces are cluster-scoped,
  so only the cluster admin decides. Compose's `cap_add: [NET_RAW]` asks
  for raw sockets. How it is applied: through Podman, ocijail gives every
  jail raw sockets, `chflags` and `enforce_statfs=1`, and annotations can
  only add to that. The kernel refuses a child jail more than its parent
  has, so a locked-down infra jail would only stop ocijail making the
  containers. Legion's Podman therefore runs a `createRuntime` hook
  (`--hooks-dir`), which ocijail calls once a container's jail exists and
  before its process enters it. The hook sets the parameters from the
  container's `legion.jail` annotation with `jail -m`, and a failing hook
  fails the start. What a child may have only if its parent does (mlock,
  SysV IPC) is asked of ocijail for the infra jail and the container. A
  pod's containers that allow SysV IPC share the infra jail's namespace.
  After each start the agent reads the jail's parameters back and refuses
  the pod if the hook did not run. A pod adopted from an agent older than
  the hook has its running jails tightened in place, which the host may
  do, rather than being restarted. The infra jail keeps ocijail's
  defaults; it runs only `sleep`, and no session reaches it.
- **No host networking by accident**: the executor verifies every task's infra
  jail has `vnet=new` before starting containers (see 6.2).

---

## 10. Phase-0 spike results

Run on 2026-10-26 on FreeBSD 15.1-RELEASE amd64, Podman 5.8.9, ocijail 0.6.x.
The multi-node lab used three VNET jails as nodes, each with its own pf,
WireGuard and bridge, with tasks as VNET jails attached to the node bridges.

| ID | Question | Result |
|----|----------|--------|
| S1 | How does Legion provide networking to a Podman/ocijail pod? | **Solved.** Podman pods and `--network none` / `container:` share the host VNET. Working pattern: infra container with `org.freebsd.jail.vnet=new`, containers as child jails via `org.freebsd.parentJail`, epair moved into the infra jail. Shared network, survives container restart, pod-wide teardown (6.2). |
| S2 | Do rctl rules on the pod jail apply to all containers? | **No, design changed.** Parent-jail rules account for children but do not enforce. Per-container-jail rules enforce `maxproc`, `vmemoryuse`, `pcpu`. Podman's `--memory`/`--cpus` are ignored (6.3). |
| S3 | pf as the service load balancer. | **Works, with required rules.** Round-robin VIPs local and cross-node, routing mesh, soft drain on table update. Needs default-block ruleset, hairpin NAT, ingress NAT on `wg0`, and kill-by-state-key for dead backends. MTU 1420 for pods (5.3, 5.4). |
| S4 | openraft + redb under failure. | **Passes.** openraft's storage conformance suite passes against the redb log and state machine. In-process 3-node tests pass 30 of 30 runs on FreeBSD: leader crash, re-election and rejoin from disk; an isolated leader cannot commit and its uncommitted entry is discarded after healing; full-cluster restart keeps state and versions keep increasing; a lagging node and a new fourth voter catch up by snapshot once the leader has purged its log. Throughput, 3 replicas on one UFS disk with fsync: about 500 sequential and 1,750 concurrent writes/s (3.7). |
| S5 | libpod REST API coverage. | **Sufficient.** create (with annotations and labels), start, stop, exec, logs, events, label filters, pull, delete all work. `stats` returns zeros; use rctl. |
| S6 | WireGuard mesh churn. | **Passes.** 100 peers added and removed in under a second each; 0 of 3,000 pings lost on an existing peer. |
| — | ZFS storage and volumes. | **Works.** Podman ZFS driver, nullfs volume mounts, snapshot, full + incremental send/recv, rollback. |

---

## 11. Roadmap

| Phase | Theme | Contents | Exit criteria |
|-------|-------|----------|---------------|
| 0 | Spikes | ~~S1–S6, workspace, CI on FreeBSD 15 amd64 and arm64~~ done | ~~S4 answered~~ met |
| 1 | Single node | Store (one member), API, CLI, `apply`/`diff`, replicated and global services, pods, executor, node-local networking, DNS, VIPs, liveness and readiness probes, restart policy, secrets, configs, ZFS volumes, labels | ~~A multi-service app with probes and volumes runs and self-heals on one node~~ met |
| 2 | Cluster (**MVP**) | `init`/`join`, PKI, 3–5 managers, dispatcher, WireGuard mesh, routed pod network, scheduler (constraints, requests, taints), node failure handling, routing mesh, rolling updates and rollback, drain, rctl limits, compose `stack deploy` | ~~A compose app deploys on 3 nodes, survives losing any one node, and updates with zero downtime~~ met |
| 3 | K8s-lite | Namespaces, RBAC, init containers, startup probes, jobs and cronjobs, network policy, disruption budgets, `logs`/`exec`/`port-forward`, security profiles, Prometheus metrics endpoint | Two teams share a cluster with isolation enforced |
| 4 | Stateful and operations | Stateful services, ZFS snapshot and migration, ingress routes, autoscaling, CARP VIP, autolock, backup and restore of the store | Postgres with replicas runs as a stateful service and migrates between nodes |
| 5 | Extensions | Native jail runtime (6.5), Linux images, IPv6 dual-stack, volume plugin interface (NFS, iSCSI), bhyve tasks (exploratory) | Per feature |

---

## 12. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Legion depends on ocijail annotations (`org.freebsd.jail.vnet`, `org.freebsd.parentJail`) and Podman passing them through | A change upstream breaks pods | Pin tested versions; integration test for the pod pattern in CI; ocijail-direct fallback |
| Podman semantics differ from Linux (`--network none` = host stack, ignored resource flags, empty stats) | Silent security or limit gaps | Executor verifies jail parameters after create; limits and metrics via rctl, never Podman flags |
| Shared Podman temp directory | Interference with the operator's containers | Legion's Podman uses its own root, runroot **and** tmpdir (6.1) |
| pf is not a full load balancer (no weights, no health checks) | Uneven load, rough failover | Table-only updates for soft drain; source-hash option; kill-by-key for dead backends; userspace proxy fallback per service if needed |
| `pcpu` throttling is bursty | Latency spikes for CPU-limited tasks | Document; prefer `cpuset` for latency-sensitive services |
| Small FreeBSD OCI image ecosystem | Adoption | Good base images and build docs; Linux images in phase 5 |
| Rust rewrite effort | Schedule | Phase 1 is deliberately single-node; reuse SwarmKit's designs and tests as references |
| Double-NAT on routing mesh hides client IP | Logging and access control | `publish: host` and PROXY protocol support in ingress |
| Cluster-wide pf tables at scale | Update cost at thousands of endpoints | Incremental table updates; per-node tables limited to services that local tasks use (phase 4 optimisation) |

---

## 13. Open questions

1. Do we want a built-in image registry (a system service backed by ZFS) for air-gapped clusters?
2. Package distribution: FreeBSD ports/pkg from the start, or release tarballs until phase 2?
3. ~~Licence~~ Decided: BSD-2-Clause, which fits the FreeBSD ecosystem.
4. Should Legion also manage tasks for the system Podman (adopt existing containers), or stay strictly separate? This draft says separate.
