# Changelog

What changed in each version of Legion, newest first. Each release's notes on
GitHub ([releases](https://github.com/grepjava/legion/releases)) also say how it
was qualified and how to install it. Versions tagged without a GitHub release
are listed, with the release whose notes cover them.

New work goes under **Unreleased** as it lands on `main`.

## Unreleased

Since 0.2.0. Agents and managers speak protocol 4 (a task's spec may make
it a member of a replicated database); the usual upgrade order holds
(managers first, one at a time, then workers), and a replicated Service's
members are told their role only on nodes that read it.

### Replicated databases

- **A pluggable replication interface (SPI v1, docs/REPLICATION.md).** A
  stateful Service's `replication` has its members replicate a database
  among themselves: Legion names the primary (Raft), a plugin anyone can
  write (an executable, JSON on standard input and output, run in the
  database's own jail) does the database's part: `describe`, `prepare`,
  `status`, `promote`, `configure`.
- **PostgreSQL, built in** (`libexec/legion/replication/postgres`): the
  primary made with checksums and a replication role, replicas copied with
  `pg_basebackup`, an old primary brought back with `pg_rewind`, a slot per
  member, synchronous replicas with `synchronous: N`.
- **Failover**: automatic by default (`failoverAfter`, `maximumLag`), the
  old primary fenced first (stopped, or cut off with `onPartition: stop`
  past its grace period), the replica furthest ahead promoted, the others
  pointed at it; `failover: manual` reports and waits.
- **Routing**: the Service's address goes to the healthy primary alone (to
  none during a change), and with `read: true`, `<service>-read` to the
  healthy replicas.
- **`legion replication status|switchover|failover`.**
- **Log archiving** (`replication.archive: { target, keep }`): each
  member's finished WAL files shipped to an S3 BackupTarget as they are
  written (at least every minute), from every member so a failover leaves
  no gap, pruned to `keep` (7 days by default); `status.archive`,
  `legion replication status` and the console say how far it got, and
  `archive-late` fires after three failed runs.
- **A restore to a point in time** (`legion replication restore NEW --from
  SVC --to TIME`, or `replication.restore` on a Service): a new Service
  with SVC's database as it was at TIME, from the newest backup of its
  members' Volumes taken before and the log it archived, replayed by the
  plugin (a new SPI capability, `restore`; the PostgreSQL plugin has it).
  SVC goes on as it is; `legion replication status` and the console's
  Replication tab say how far the restore got.
- **`legion replication check -f FILE`**: the conformance check, run on
  the cluster. It drives a plugin through every step of the contract,
  including a primary stopped dead and brought back, with markers written
  and read through a new optional `probe` operation (the PostgreSQL plugin
  has it).
- **The console's Replication tab** on a replicated Service: the named
  primary and epoch, the phase and why, each member's role, position, lag
  and when it was last heard from, a change under way and the last one,
  and a switchover or failover asked for through the usual review.
- **Alerts and metrics**: `database-degraded` (warning), `database-split`
  and `database-stalled` (critical: a change of primary under way for over
  3 minutes); `legion_replication_lag_bytes` per replica and
  `legion_replication_phase`.
- **A primary is handed over, not stopped**: a drain of its node (or its
  data moving), a rolling update (which now replaces the replicas first and
  the primary last) and a scale-down past it switch over to a healthy
  replica first, waiting for one still catching up; the drain shows it as
  `switchover` meanwhile. With no other member running, it stops as
  before.

### Networking

- **Init containers reach their namespace's services under isolation.** A
  task still starting, its pod's address known, now counts as a source of
  its namespace for network policy and namespace isolation, so an init
  container (a migration, a database member's `prepare`) is no longer
  dropped on its way to a service of its own namespace. It is still no
  endpoint until it runs and is ready.

### Volumes

- **Replica failover.** A Volume's replica (a copy kept on another node,
  every 10 s to an hour) takes over when its node is lost: by hand with
  `legion volume promote` (the name typed, or `--yes`), or on its own with
  `failover: automatic`, which needs `onPartition: stop` so the old tasks
  have stopped before the copy carries on. A replica says its state
  (pending, ready, stale, failed), the copy under way, its last ten copies
  and why it would not fail over now; a stale one raises `replica-stale`.
- **Copies go on where they stopped: replicas, moves and backups over
  SSH.** A copy cut short (the link dropped, a daemon restarted, leadership
  moved) is kept by the node or host receiving it (`zfs receive -s`), and
  the next attempt sends only the rest (`zfs send -t`), so a large Volume
  over a slow link gets there however often daemons restart.
  `status.replica.partial` says which replica copy is under way. Backups to
  a directory are files, and are written again.
- **Backups to S3-compatible storage.** A `BackupTarget` can be a bucket
  (`s3: {endpoint, region, bucket, prefix, accessKeyId, secretKeySecret}`,
  `virtualHosted` for services that take no path-style requests): AWS S3,
  MinIO, Garage, Ceph's RGW, R2, B2 and the like. Each backup is an object,
  whole, at `<prefix>/<namespace>.<volume>/<snapshot>.zfs`, pruned to
  `keep`, listed and restored as the other targets' are. It goes up a part
  at a time, so the object appears only once complete; a part failing on
  the way is sent again, and an upload left unfinished is aborted at the
  next run. The console's Backup targets page adds and changes them.
- **Sends are compressed.** Every `zfs send` (replicas, moves, backups)
  sends blocks as the pool stores them (`-L -c -e`): a lz4 database at
  2x sends half what it did.
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
- **Managers far apart keep up**: a leader sends a manager what of the
  log fits the time a call has (the heartbeat interval), sized from the
  link as measured (its round trip and rate), and openraft goes on from
  there at once. Managers' connections to each other use FreeBSD's RACK
  TCP stack where `tcp_rack` is loaded, which resends a lost last segment
  within a fraction of a round trip. A witness across the internet caught
  up after a leader restart in a flood of `payload too large` errors (each
  batch halved down to one entry, then grown back and halved again); now
  none with RACK loaded on the full managers, and a few without it on a
  lossy uplink (docs/HA.md, A witness far off).

### Upgrading

- Queries summing a cluster metric (`legion_tasks`, `legion_nodes`,
  `legion_service_*`, `legion_volume_*`...) across several managers count
  it once per manager now that every manager reports it: take it once
  first with `max without (instance)`.
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

- Every manager reports the cluster's metrics, not only the leader: its
  objects, tasks, nodes and quotas from its own replica, and the agents'
  pool and Volume space as the leader last sent them. A scraper of one
  manager no longer goes blank while another leads. Upgrading: a query
  summing a cluster metric across several managers now counts it once
  per manager; take it once first (`max without (instance)`), as the
  Grafana dashboard now does.
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

- **Security: restoring from a backup target.** `Volumes.RestoreFrom`
  took a backup's full name as given: on an SSH target it reached the
  remote shell unquoted (`zfs send <name>`), so anyone who may write
  Volumes in a namespace could run commands as the target's account; on a
  directory target a path through `..` restored another namespace's
  backup, and `Volumes.Backups` listed another Volume's for a name with
  `/` in it (found by an audit). A restore now takes only a backup the
  target lists for that Volume (by its snapshot, or exactly as listed),
  both calls check the Volume's name, and agents take only names of the
  Volume's own chains (in ZFS's characters, every word quoted for the
  remote shell), files right in its own directory, and a Volume of the
  session's namespace.
- **Security: a Build's repository could reach the builder node's files.**
  `source.git.path` was checked for `..` only, so a symbolic link in the
  repository (`path` -> `/`) made the node's root the build context, and
  a `Containerfile` or `.dockerignore` linked out was read from the host
  (found by an audit): anyone who may create Builds could copy node files
  into an image and push it. The context, and the files a build reads
  straight from it, must now resolve inside the clone; links within the
  repository still work.
- Demoting the manager that leads stranded it: out of the membership it
  stepped down before it could write the change down, its manager
  certificate already revoked and its worker certificate never handed
  back, so it could only join again by hand. It now hands leadership to
  another manager first, and no manager demotes itself. Deleting the
  leader's Node is refused with the same advice (`legion node lead`),
  and the last manager that may lead can no longer be deleted while only
  witnesses would remain (they never stand: the cluster would have no
  leader for good); removals go one at a time, so two cannot each leave
  the other's the last (found by an audit).
- `legion exec -t` (and the console's shell) could leave its shim stuck
  for good when the client went away after output: the 5 s limit on
  waiting for the terminal's output to be read never fired, as FreeBSD's
  `signal(3)` restarts the wait the alarm breaks off; the session and its
  terminal stayed with it. Terminals opened at once for several sessions
  no longer share one buffer for their names (`ptsname`). Both found by
  an audit.
- **Security:** inspecting a registry's image (`legion image inspect`,
  the console's Images page) needed no rights: anyone with a certificate
  had the manager reach any registry named and learned what a namespace's
  ImagePolicy makes of the image. It now takes reading the namespace's
  Services (`default` unless `-n`). Found by an audit.
- **Security, and an upgrade step:** a Volume the host mounts (`driver:
  nfs`, `iscsi` or `plugin`) now takes its namespace's leave, as a VM
  does: `spec.security.allow` on the Namespace lists `nfsVolumes`,
  `iscsiVolumes` or `pluginVolumes`. Before, anyone who may edit a
  namespace could have the host mount an NFS server, or read a UFS
  filesystem from a LUN, of their choosing (found by an audit). NFS and
  iSCSI Volumes are mounted `nosuid`, and `suid` is refused as an option.
  Volumes made before keep working; to change one, or make another, the
  cluster admin allows the driver first. A stack's namespace allows the
  drivers its Volumes declare.
- A replicated database's member not running (an old primary being
  rewound to rejoin) had its spool shipped only once it ran again, while
  the PostgreSQL plugin now waits for it to be shipped first: Legion
  ships it from the spool the other members report.
- Usage charts dipped when nodes were sampled in different steps: each
  series now counts once per step, its latest sample held.
- A replica whose attempts kept failing turned back from stale to failed;
  it stays stale while its last copy is too old.
- A note's text was read from a Config's text, which only a Config not yet
  admitted has; it is read from its data.
- An app's default given as an `.env` value kept its quotes.
- A leader that stopped just after a replica's copy went in, before it
  wrote that down, made the next leader drop the replica and copy the
  Volume whole (seen on a 36 GB Volume, hours over Wi-Fi, with no usable
  replica meanwhile): the next leader now asks both nodes for the
  snapshot's GUID first and, the same, takes the copy as done. A move's
  full copy is checked the same way. Agents from before answer nothing,
  and the copy goes on as before.
- A restore to a point in time could stop short after failovers: an old
  primary rewound to rejoin lost the WAL it had archived but Legion had
  not shipped yet, as `pg_rewind` removes what the new primary lacks (seen
  in the lab with PostgreSQL 18). Before a rewind or a new copy, the
  PostgreSQL plugin now archives what PostgreSQL finished but had not
  archived, and waits until Legion has shipped the spool; only that
  replica waits, the primary taking writes meanwhile.
- A copy cut short by a leader restart could lose what it had kept: the
  receive the old leader fed held the dataset for over 20 s after its
  sender was gone, the new leader's attempts to go on were refused as busy
  meanwhile, and three of them in a row gave the part up (19 GB of a
  36 GB replica, copied again from nothing). A refusal while the attempt
  before is still being taken in no longer counts, for a replica's copy or
  a move's full copy.
- A failed copy, move, scheduled snapshot or backup said when the next
  attempt starts as "the next in 5s", wrong as soon as it was read later
  and all through a long attempt after it; it gives the time instead
  ("the next after 2026-11-25T12:05:54Z").

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
