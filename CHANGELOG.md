# Changelog

What changed in each version of Legion, newest first. Each release's notes on
GitHub ([releases](https://github.com/grepjava/legion/releases)) also say how it
was qualified and how to install it. Versions tagged without a GitHub release
are listed, with the release whose notes cover them.

New work goes under **Unreleased** as it lands on `main`.

## Unreleased

Since 0.2.0. Agents and managers still speak protocol 3: no upgrade order
beyond the usual (managers first, one at a time, then workers).

### Volumes

- **Replica failover.** A Volume's replica (a copy kept on another node,
  every 10 s to an hour) takes over when its node is lost: by hand with
  `legion volume promote` (the name typed, or `--yes`), or on its own with
  `failover: automatic`, which needs `onPartition: stop` so the old tasks
  have stopped before the copy carries on. A replica says its state
  (pending, ready, stale, failed), the copy under way, its last ten copies
  and why it would not fail over now; a stale one raises `replica-stale`.
- **Snapshots on a schedule.** `snapshots: [{every, keep}]` has the leader
  take and prune them; one late raises `snapshot-late`. `legion volume
  clone VOLUME@SNAPSHOT NAME` copies one into a new Volume; a rollback says
  what it destroys first. Names starting `legion-` are Legion's.
- **Backups to a target.** A `BackupTarget` (a ZFS host over SSH, its key in
  a Secret and its host key pinned, or a directory on a node) and a
  Volume's `backup: {target, every, keep}` send incremental backups on a
  schedule; `backup-late` says when one fails or is overdue. `legion volume
  backups` lists them and `legion volume restore --from TARGET` restores one.

### High availability

- **`legion ha`** ([docs/HA.md](docs/HA.md)) lists the cluster's single
  points of failure, each with how to put it right: too few managers or
  only one that may lead, one node running every task, a service with one
  replica or all on one node, published ports without a load balancer
  address, a Volume's data on one node or failing over only by hand. Exits
  nonzero on a risk. The console has an Availability page and an Overview
  tile for it.
- **Witness managers.** `legion join --witness` adds a manager that votes
  but runs no tasks and never leads, so a small machine makes two managers
  three. `legion status` and the console mark witnesses.
  A witness may sit where it reaches no manager (managers connect to it),
  but then can't serve clients. A failover takes about 16 s to elect a new
  leader, and about 32 s before the lost leader's node's tasks move;
  measured in the lab and on a real cluster ([docs/HA.md](docs/HA.md)).
- **Forms instead of YAML for it.** A Volume's Protection tab sets its
  replica (every 10 s to an hour, failover by hand or on its own, the node
  to keep it on), snapshot schedules, backups and size; Backup targets have
  a page of their own with a form; each finding on the Availability page
  has a Fix: a Service's replicas, keeping its tasks on different nodes, a
  load balancer address, a Volume's protection, or the commands for a
  witness. Every change is shown as a diff and saved only over what was
  read.
- **Leadership handed on** with nothing brought down: `legion node lead
  NODE`, or Lead on a manager's page. About 10 s with nothing written; no
  node marked down, running tasks carry on.
- **Demoting or removing a manager is checked first**, in the console and
  the CLI (`legion node demote --dry-run`): an election if it leads, fewer
  than three voters, one manager left that may lead; demoting the last
  manager that may lead is refused.
- **A manager's shutdown is bounded**: its connections get 10 s to end,
  then are closed, and each task after them 10 s; a stream passed on from
  the leader ends with its manager. A witness across a NAT had hung on a
  half-open connection.
- **Self-fencing.** A pod with `onPartition: stop` now stops on its node
  even when the daemon is dead: the agent renews a lease as heartbeats are
  answered, and each container's supervisor stops it once the lease runs
  out. A Volume's automatic failover is then safe in that case too.

### Upgrading

- **VM pods now need their namespace to allow them**: `spec.security.allow:
  [vm]` on the Namespace, which only the cluster admin sets. A bhyve VM's
  hypervisor runs on the host outside any jail, so no namespace runs one
  unless allowed. Running VM tasks carry on; a Service with a VM container
  is refused (saying so) until its namespace allows `vm`.

### Nodes and hosts

- A Node's status holds its host (FreeBSD release, kernel, boot method, up
  since) and its ZFS pools (state, space, errors, last scrub). Alerts
  `pool-degraded`, `pool-errors` and `pool-nearly-full` (80 %). `legion node
  pools`, `legion node errors-seen` and `legion node scrub` (a scrub
  schedule the agent runs).
- Agents report the host's load averages and the pools' I/O.
- Nodes say which build they run (a digest of the daemon), so
  `version-skew` tells builds of one version apart.
- `legion doctor --fix pf` (and `legion local doctor --fix pf`) adds the
  lines that call Legion's anchors to `/etc/pf.conf`, after showing them
  and asking.

### Operations

- **Alerts sent.** The `AlertReceiver` kind sends chosen alerts by webhook,
  ntfy or mail (SMTP), once as each is raised and once as it clears, from
  whichever manager leads. `legion alerts test RECEIVER`.
- **Long operations.** Drains, Volume moves, restores and replica copies,
  with their stage, bytes sent, who asked and how they ended: `legion
  activity` (`-w` to follow), a tab in `legion top`, the console's bottom
  bar.
- **Checks before disruption.** `legion node drain`, `volume move` and
  deleting a node or a Service ask the leader what they would do first and
  refuse what would not complete, saying why, unless `--force`;
  `--dry-run` prints it.
- **Usage kept 30 days** on every manager (a sample a minute for a day,
  every 15 minutes beyond), so a new leader has it: `legion usage --range`
  (e.g. `24h`, `7d`, `30d`).
- **Moved tags.** A Service's status says which of its tags name another
  image now than its tasks run; `legion list services` marks them.
- **Linux images pointed to native recipes.** A Service running a Linux
  image says so, with the FreeBSD-native recipe in `examples/` that runs the
  same.
- **Ready-to-deploy apps** from the daemonless catalog (`legion app`, the
  console's Apps page), read when asked, never bundled.
- Pods get an `/etc/hosts` naming their containers at the pod's address, as
  Docker and Kubernetes give them.

### Examples

- Legion's native base (FreeBSD's runtime image with pkg) and recipes on it
  for PostgreSQL 17, Redis, Node.js 24 and Python 3, each a console
  template; the static site's nginx is built on the base.

### Metrics

- `legion_task_info` and `legion_task_start_time_seconds`; each node's ZFS
  space and each Volume's used, available and size. The Grafana dashboard
  has a Tasks and storage row.
- `--metrics-listen` on an address not up yet (a pod gateway before its
  bridge) is bound once it is, rather than stopping the daemon.

### `legion top`

- ISPF-style panels over a menu of lists, a `Command ===>` line that runs
  any `legion` command, row marks acting on many at once, and btop-style
  coloured graphs (memory and ZFS against their whole, CPU and network
  against their peak).
- Each node's load and pools; scrubs, errors seen and Volume promotion from
  their keys.

### Web console

- **Phases 7 to 12** ([CONSOLE.md](CONSOLE.md)): replicas in view and
  promoted from a Volume's page, nodes' hosts and pools, alert receivers,
  long operations in the bottom bar, what a drain, move or delete would do
  before it is confirmed, usage charts over up to 30 days with PNG and CSV,
  and notes on any object.
- **Its own look.** Icons, its own confirmation dialog (the name typed for
  what can't come back), a toast answering every click, a health overview,
  a side-bar tree by namespace or node, one table kit (sorted, searched and
  paged alike), the namespace and filters kept in the address. A second
  round made it calmer and more modern: softer indigo, the Inter font, the
  namespace picker in the top bar, state pills.
- **Streams.** Everything that changes reaches the page as server-sent
  events on one stream per tab (`GET /api/stream`, what it follows changed
  with `POST /api/stream/ID`), so a browser on HTTP/1.1 is not held up by
  six connections. Nothing is asked again on a timer: watches, alerts,
  events, operations, logs, usage and its history, the cluster's status,
  load balancers, connections and the CA are sent again when they change
  (`Cluster.WatchUsage`, `WatchUsageHistory`, `WatchStatus`,
  `WatchLoadBalancers`, `WatchConnections`, `WatchCa`). Exec stays a
  WebSocket. Streams end when the manager closes.
- **Topology right-click** (or the menu key, Shift+F10) on a Service,
  task, node or Volume: start, stop, pause, restart, scale, move to another
  node with its data (and every Service sharing it, checked first as one
  change), duplicate onto a node with a copy of its data, protect its
  data, roll back, logs, console, events, edit, delete; a node paused,
  drained or made the leader. **Drag** a Service or Volume onto a node to
  move it there. A Service whose data is a Volume is never offered a
  second replica: two Postgres over one data directory, each in its own
  jail, don't see each other's lock.
- Topology's published ports and Routes have a right-click menu too: a
  port opened in the browser or its address copied, its Service, its ports
  edited, or no longer published (asked first); a Route opened, copied,
  edited or deleted.
- Topology's links are named for what they show (WCAG 2.5.3); a sign-in
  link pasted into the sign-in tab is read.
- Unit tests of its helpers (`npm test`, Vitest), run by CI with the type
  check.

### Fixed

- Usage charts dipped when nodes were sampled in different steps: each
  series now counts once per step, its latest sample held.
- A replica whose attempts kept failing turned back from stale to failed;
  it stays stale while its last copy is too old.
- A note's text was read from a Config's text, which only a Config not yet
  admitted has; it is read from its data.
- An app's default given as an `.env` value kept its quotes.

## 0.2.0 (2026-11-18)

A web console that runs the cluster, image builds on the cluster, jailer
merged in, the connections the nodes see, and `legion top` level with the
console. [Release notes](https://github.com/grepjava/legion/releases/tag/v0.2.0).

- **Web console** ([CONSOLE.md](CONSOLE.md), phases 2 to 7). Edit and deploy
  from YAML with the change shown first, forms for a service, Compose import
  and templates; alerts, usage history with charts, quotas, and log search;
  a Topology page that draws each namespace's services, tasks, routes and
  volumes, grouped by node; console settings and a banner; single sign-on
  with OIDC (users and groups in RoleBindings); several clusters in one
  console; plugins (pages from a Config, sandboxed); translations;
  accessibility checked with axe and the accessibility tree.
- **Builds.** Nodes labelled `legion.builder=true` build images from a Git
  repository (`Build`, `legion build-logs`, the console's Build form), push
  them, and remove them afterwards.
- **jailer merged in.** Building, running, testing and pushing OCI images of
  FreeBSD jails is now `legion build` and `legion local`, on any FreeBSD host
  with or without a cluster, with signing, an SPDX SBOM and provenance.
  State moves from jailer's directory on first use. jailer's repository is
  archived.
- **Day-to-day operations.** `legion restart`, `legion cronjob run`, stop and
  start (`stopped`), pause and resume (`paused`), events per object (`legion
  events`), the load balancer's holder (`legion lb`), image details, scans
  (`pkg audit`) and what uses them, disk usage and pruning, `legion
  diagnose` and support bundles (no Secrets), files and `legion cp`, and
  stacks.
- **Connections seen.** `legion connections` lists the connections the
  nodes' pf states hold between a namespace's services and from outside,
  each counted once across nodes; the console's Topology draws them as arcs,
  and `legion top` lists them for a service.
- **`legion top`** does what the console does: `:` runs any `legion` command
  below the table, `E` edits an object in `$EDITOR` and applies it over the
  version read, and keys open diagnose, reach, connections, topology, audit,
  usage, images, snapshots, backup, files and build logs. `legion topology`
  prints a namespace as the console's Topology draws it.
- **Upgrading:** agents speak protocol 3 (pause); upgrade managers first, one
  at a time, then workers. The upgrade from 0.1.19 is tested.

## 0.1.19 (2026-11-13)

Measured performance, a CA that rotates in place, nodes that change roles,
load-balancer addresses, VM pods, and a web console.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.19).

- **Measured.** Reconciliation, watches, endpoint propagation, pf as services
  grow, image pulls and loads, and the mesh as nodes join are each measured
  by a test that prints its table ([docs/MEASURE.md](docs/MEASURE.md)). The
  planner handles 5,000 tasks in 78 ms (from 383), and adding a service at
  1,000 services takes 0.9 s (from 4.2).
- **CA rotation.** `legion ca rotate` makes a new cluster CA, cross-signed by
  the old one; every manager and node moves to it by itself. `legion ca
  status` shows each holder.
- **Roles in place.** `legion node promote|demote`.
- **Placement.** Affinity and anti-affinity between services, and priorities
  with preemption.
- **Networking.** Mesh relays for nodes behind NAT (`--mesh-nat`,
  `--mesh-relay`). A service's load-balancer address
  (`endpoint.loadBalancer`), held with CARP by one ready node and moved when
  it fails.
- **Storage.** Local volumes lost with their node are marked lost, and
  `legion volume restore --replace` refills them from a backup on another
  node.
- **VM pods.** A container with `vm` boots with bhyve on the pod's network.
- **Multi-arch images.** Each node runs its own platform's image from a
  multi-arch index.
- **Web console** (phases 1 to 3): `--console-listen`, `legion console
  login`; works as the signed-in user with the API's role checks and audit.
- **`legion reach`.** What may reach what among a namespace's services.
- **Fixed:** `Cluster.Me` could answer from a replica that had not yet seen
  a role just given; an agent kept a connection made with its old
  certificate after a CA rotation; a bare `--advertise` had no port; a lost
  Volume could not be deleted while an unplaced task named it; a VM's
  disk-only image needed a command it doesn't have.

## 0.1.18 (2026-11-11)

Revocation, an audit trail, and jailer's images from build to recovery.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.18).

- **Revocation.** Deleting a user's RoleBinding ends their sessions and
  watches within 5 s. A manager removed with `legion delete node` leaves the
  Raft membership, and its certificate is refused.
- **Audit.** Changes, sessions and refusals are recorded in the replicated
  store, names only, never values: `legion audit`. Kept 30 days and at most
  100 000 events by default.
- **jailer to Legion, end to end**, tested in one run.
- **Compose and Linux images** documented as tested
  ([docs/COMPOSE.md](docs/COMPOSE.md), [docs/LINUX.md](docs/LINUX.md)).
- **Examples:** [examples/](examples), run as their READMEs say on every
  release.
- **Fixed:** static Linux programs ran nowhere (`--linux` now sets
  `kern.elf64.fallback_brand` when unset); a Volume moving to a node only
  briefly unresponsive started its copy again elsewhere.

## 0.1.17 (2026-11-10)

Image signatures.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.17).

- **Image policies.** An `ImagePolicy` lets a namespace run the images it
  covers only with a good cosign key-pair signature by one of its keys,
  enforced or only warning, checked on the digest the deployment runs.
- **Shared with jailer:** one signing module, `legion_image::cosign`.
- **Fixed:** an image covered by a policy and named by digest, or loaded
  onto the node, could start before its check.

## 0.1.16 (2026-11-10)

Recovery: data that moves between nodes survives interruption, and a whole
cluster can be rebuilt from backups.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.16).

- **Volume moves cut short lose nothing**, at any stage, by a daemon killed
  on either side or a full disk on the receiving node.
- **A cluster rebuilt from its backups:** the control state (`legion
  backup`), the manager's keys, and a Volume (`legion volume backup`).

### 0.1.15 (2026-11-09), 0.1.14 (2026-11-08), 0.1.13 (2026-11-07)

Tagged, not released; in 0.1.16's notes.

- `legion doctor` checks whether a host can run Legion (0.1.13).
- A deployment runs the digests its tags named when it was made, on every
  node, and a rollback runs the old ones (0.1.14).
- Partitions: `onPartition: stop`, a leader cut off from the majority stops
  answering heartbeats, and Volumes stay with tasks cut off until their node
  says they stopped ([docs/PARTITIONS.md](docs/PARTITIONS.md)); `legion node
  drain --wait`; builds that can't work together refuse each other; `legion
  volume backup` and `restore` (0.1.15).

## 0.1.12 (2026-11-06)

A security fix and an ingress fix.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.12).

- **Security:** an environment value that couldn't be passed on (a Secret
  with a NUL byte) was printed in the start error, which namespace viewers
  can read; errors now name the variable, never its value.
- **Certificates after a namespace move:** a host moved to another namespace
  kept serving the old namespace's certificate.
- New crate functions for tools built on Legion (registry login checks,
  path resolution inside a root, layer compression).

## 0.1.11 (2026-11-06)

Fixes from several rounds of code review since 0.1.7, many for security.
0.1.8 (2026-11-01), 0.1.9 (2026-11-01) and 0.1.10 (2026-11-05) were tagged,
not released; their fixes are here.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.11),
which list every fix.

- **Before upgrading:** the data directory is `0700` and container records
  `0600`; pods wait until bridge filtering is set; a Namespace's current
  `security` applies to what is made from specs admitted earlier; a second
  Volume naming storage one node at a time may use is refused; an exec keeps
  the first 1 MiB of each output.
- **Security:** Secrets readable on disk under a `022` umask; network policy
  bypassed when bridge filtering failed; users' rights checked on a lagging
  follower; removed workers registering again; links and special files
  followed by builds and loads; registry and ACME clients hardened; key
  files written world-readable; limits on objects, handshakes and usage
  reports; autolock reseals made atomic.
- **Fixes** to containers with loosened profiles, Volume sizes, agent
  restarts, rollouts, Volume moves and drains, CronJobs, ACME, CARP, exec,
  `legion join`, images, Compose, mixed amd64/arm64 clusters, Jobs under
  network policy, `logs -f` across rotation, IPv6 addresses and long service
  names.

## 0.1.7 (2026-11-01)

HTTPS certificates from Let's Encrypt for ingress routes, HTTP/2 from
clients, API metrics, the WireGuard key under autolock, and an image store
fix. [Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.7).

- **Certificates from ACME** for routes (HTTP-01 on port 80, renewed after
  two thirds of their life).
- **Ingress HTTP/2** from clients; connections to backends kept open.
- **API metrics:** `legion_api_requests_total`,
  `legion_api_request_duration_seconds`.
- **Autolock** seals the WireGuard key too.
- **Fixed:** two builds of one Containerfile with other layer compression
  were taken for one image; a node that joined another cluster kept its old
  IPv6 bridge address (from 0.1.6).

### 0.1.6 (2026-10-31)

Tagged with the port, not released; in 0.1.7's notes.

## 0.1.5 (2026-10-31)

Fixes since 0.1.0, and the jail runtime and image store shared with jailer.
0.1.1 to 0.1.4 (2026-10-31) were tagged, not released.
[Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.5).

- **Fixed:** managers recorded as workers; removing a container right after
  it served a connection; mounts left by a failed start; a pod's first IPv6
  connection waiting 30 s; mount targets with `..`; image store temporary
  files and concurrent removal.
- Crates for tools built on Legion (jailer): `legion-runtime`, `legion-jail`,
  `legion-image`, `legion-net`.

## 0.1.0 (2026-10-31)

The first release: a container orchestrator for FreeBSD built on jails,
ZFS, pf and WireGuard, in one binary that runs as a manager, a worker or
both. [Release notes](https://github.com/grepjava/legion/releases/tag/v0.1.0).

- **Clusters:** managers with a replicated store; workers that join with a
  token; a WireGuard mesh between pods' nodes.
- **Workloads:** services with rolling updates and rollback, Jobs, cron
  jobs, compose files as stacks; FreeBSD and Linux images from OCI
  registries, run by Legion's own jail runtime.
- **Networking:** a virtual IP and DNS name per service, published ports,
  IPv6, network policy, in pf anchors of Legion's own.
- **Storage and state:** ZFS volumes that follow their pods, network volumes
  through plugins, secrets and configs.
- **Operations:** namespaces, RBAC, quotas, security profiles, Prometheus
  metrics with a Grafana dashboard, `legion top`, node drain and pause.
