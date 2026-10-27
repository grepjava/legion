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
| **Dispatcher** | On every manager. Streams each agent its node's assignments and every service's endpoints from the local replica, and serves the secrets, configs and volume claims of the node's own tasks; status reports, registration and heartbeats are forwarded to the leader, which alone tracks liveness. Agents fail over between managers and learn the manager list from heartbeat replies. |
| **CA** | Issues and rotates node and user certificates. Root key stored in Raft, optionally sealed with an autolock key. |

### 3.3 Controllers

| Controller | Watches | Produces |
|------------|---------|----------|
| Service | Service, Task, Node | Creates and removes Tasks to match replicas / one per eligible node (global). |
| Stateful | Service (`mode: stateful`) | Tasks with stable ordinals and per-ordinal volumes; ordered start and stop. |
| Update | Service spec generation | Rolling replacement honouring parallelism, delay, failure action, `maxUnavailable`; automatic rollback. |
| Job / CronJob | Job, CronJob | Run-to-completion tasks; CronJob creates Jobs on schedule with concurrency policy and history limit. |
| Node health | Heartbeats | Marks nodes `down` after three missed heartbeats (5 s each; a new leader allows three from taking over) and `ready` when heard from again; marks their tasks orphaned so the Service controller replaces them. |
| Endpoints | Task readiness | Per-service list of ready task IPs, pushed to every agent for load balancing and DNS. |
| Autoscaler (phase 4) | Task metrics | Adjusts replicas between min and max to hit a CPU or memory target. |
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
  `stop-first` or `start-first` order. Monitoring, failure actions and
  rollback are phase 2.
- **Nodes.** A drained node's tasks stop and move elsewhere; a paused node
  keeps its tasks but gets no new ones. The tasks of a node that is `down`
  are marked `orphaned` and told to stop, in one write: they leave the
  endpoints, their slots run elsewhere at once (a global service's slot
  goes with the node, and keeps its task number for when the node is
  back), and a node that comes back removes their pods. Tasks do not move
  back by themselves.
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

### 3.4 Agent components

| Component | Responsibility |
|-----------|----------------|
| **Session** | Connects to the leader, receives the assignment set, reports task state. Persists the last assignment set in a local `redb` so running tasks survive a manager outage and agent restarts. |
| **Executor** | Drives a **Legion-owned Podman instance** through the libpod REST API (section 6). Builds each task as an infra jail plus child container jails, pulls images, runs init containers in order, starts containers, streams logs and events. Every Podman object carries `legion.task.id` and `legion.node.id` labels, so the agent can always reconcile Podman's view against assignments. |
| **Network** | Creates epairs, attaches them to the node bridge, moves the jail end into the pod's VNET, programs routes, WireGuard peers and pf anchors. See section 5. |
| **Volumes** | Creates, mounts and snapshots ZFS datasets; mounts them into containers (nullfs). A `Volume` is `<pool>/legion/volumes/<ns>/<name>`, created on first use with its size as quota and bound to that node in its status. Pod `tmpfs` volumes live and die with the pod. |
| **Secrets** | Writes secrets and configs to a per-task tmpfs (`<run dir>/tasks/<task uid>`) and bind-mounts each file read-only into the container that names it. Files never touch disk. Values used as environment variables are passed to Podman, which keeps them in the container's configuration under its root; files are the safer form. Values are read when the pod starts. |
| **Resources** | Applies rctl rules to **each container's own jail** (memory, CPU percentage, process and open-file limits) and `cpuset` pinning. Removes the rules when the task ends; rctl keys rules by jail name and keeps them after the jail is gone. Reads usage with `rctl -u` for metrics and autoscaling. See 6.3. |
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
  replica. During an election the API returns `UNAVAILABLE` and clients retry.
- **Shutdown.** SIGTERM (rc.d) or Ctrl-C stops accepting requests, ends open
  watch streams with `UNAVAILABLE` (otherwise graceful shutdown would wait on
  them forever), then closes the store.
- **Identity.** A manager generates a random 64-bit node id on first start and
  keeps it in `<data-dir>/node-id`. A new daemon belongs to no cluster;
  `legion init` makes it the first member.
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
| Stateful services | `mode: stateful`: stable names `<svc>-<n>`, per-ordinal ZFS volumes, ordered rollout. Data moves between nodes with `zfs send/recv`. | 4 |
| Ingress | A system global service running haproxy, configured from routes Legion renders from service labels. | 4 |
| Autoscaling | CPU and memory targets read from rctl; no separate metrics server. | 4 |
| `apply` / `diff` | Server-side apply with field ownership kept simple: last applier wins per object. | 1 |
| `logs`, `exec`, `port-forward` | Streamed through the manager to the agent. | 3 |

### 4.3 FreeBSD-specific

| Feature | Notes | Phase |
|---------|-------|-------|
| ZFS image and volume storage | Podman's ZFS storage driver for images (one dataset per layer); one dataset per volume under `<pool>/legion/volumes/<ns>/<name>`. | 1 |
| Snapshots before updates | Optional per service: snapshot volumes before rollout; rollback restores data along with the image. | 4 |
| Stateful migration | Drain a node and move a stateful replica's dataset with a full then incremental `zfs send/recv`, then reschedule it. | 4 |
| CARP manager VIP | Optional floating address for the API. | 4 |
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
- **Bridge sysctls**: `net.link.bridge.pfil_*` are per-VNET, but Legion runs in
  the host VNET, so changing them would affect every bridge on the host,
  including Podman's own. Legion leaves them at their defaults and uses the
  hairpin NAT rule in 5.4, which works either way.

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
- Rules are enforced on the **destination** node, on the task's own epair
  (`out on epairNa`, toward the jail). With the host's default
  `pfil_member=1`, pf sees same-node bridged traffic there, so policy also
  covers traffic between tasks on one node. A misbehaving node cannot bypass
  rules enforced on another.
- Each policy compiles to rules in the `legion/policy` anchor that reference
  tables. Membership changes update tables only; rules are reloaded only when
  policies change.

### 5.7 IPv6

Dual-stack is planned for phase 5. The model (per-node prefix, WireGuard
routing, pf tables) carries over unchanged.

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
  forks, `vmemoryuse` refuses allocations, `pcpu` throttles by pausing the
  process (usage is bursty rather than smooth).
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
  through Podman instead, so they apply from the first instruction. Note the
  semantics differ: ocijail kills on `memoryuse` (like the Linux OOM killer),
  where Legion denies on `vmemoryuse`.

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
- **Encryption at rest**: Raft log and snapshots are encrypted with a data key.
  With autolock, the data key is wrapped by a key held by the operator and
  managers need `legion unlock` after restart.
- **Secrets in transit and on nodes**: delivered only to nodes running tasks
  that use them; held in tmpfs; removed when the last task using them stops.
- **Data plane**: all node-to-node traffic goes over WireGuard. WireGuard keys
  are generated on the node; only public keys go through the managers.
- **Jail defaults**: no raw sockets, no mounting, `enforce_statfs=2`,
  `securelevel=2` where possible; profiles loosen these explicitly.
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
| 1 | Single node | Store (one member), API, CLI, `apply`/`diff`, replicated and global services, pods, executor, node-local networking, DNS, VIPs, liveness and readiness probes, restart policy, secrets, configs, ZFS volumes, labels | A multi-service app with probes and volumes runs and self-heals on one node |
| 2 | Cluster (**MVP**) | `init`/`join`, PKI, 3–5 managers, dispatcher, WireGuard mesh, routed pod network, scheduler (constraints, requests, taints), node failure handling, routing mesh, rolling updates and rollback, drain, rctl limits, compose `stack deploy` | A compose app deploys on 3 nodes, survives losing any one node, and updates with zero downtime |
| 3 | K8s-lite | Namespaces, RBAC, init containers, startup probes, jobs and cronjobs, network policy, disruption budgets, `logs`/`exec`/`port-forward`, security profiles, Prometheus metrics endpoint | Two teams share a cluster with isolation enforced |
| 4 | Stateful and operations | Stateful services, ZFS snapshot and migration, ingress routes, autoscaling, CARP VIP, autolock, backup and restore of the store | Postgres with replicas runs as a stateful service and migrates between nodes |
| 5 | Extensions | Linux images, IPv6 dual-stack, volume plugin interface (NFS, iSCSI), bhyve tasks (exploratory) | Per feature |

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
