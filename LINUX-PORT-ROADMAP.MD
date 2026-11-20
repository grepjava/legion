# Linux port roadmap

Written 20 November 2026, at main 2ce337b. A plan, not a commitment: it
says what a Linux node of Legion would take, in what order, and how each
step is known to be done. Nothing here is built yet.

## Goal

A Legion node that runs on Linux: the same daemon, the same API, CLI and
console, the same objects (Services, Jobs, Volumes, Routes, policies),
joining the same clusters as FreeBSD nodes. On Linux a pod's containers
run under **crun**, the OCI runtime; isolation is Linux namespaces, limits
are cgroups v2, the pod network is a bridge, veth pairs and nftables, and
storage is OpenZFS, as on FreeBSD.

What does not change: Legion is FreeBSD-first. The FreeBSD node keeps its
own jail runtime, pf and rctl; nothing in it is reshaped to suit Linux
beyond moving it behind the interfaces below. Linux images on FreeBSD
nodes keep running through the Linux ABI (`--linux`, docs/LINUX.md).

## Why crun

crun and runc implement the same OCI runtime spec and command line
(`create`, `start`, `state`, `kill`, `delete`, `exec`), so Legion writes
to that interface and either works. crun is the default:

- Small and fast to start, written in C, with no runtime of its own to
  carry: it suits a single-binary orchestrator.
- cgroups v2 first, which is what Legion's limits use.
- Also a library (`libcrun`), should Legion later call it in-process
  rather than run it per operation.
- The default runtime of Podman on Fedora and RHEL: well proven.

`--oci-runtime <PATH>` picks another; the Linux lab runs the suite under
runc too, so neither becomes a dependency by accident. youki (Rust) is
worth watching as a library Legion could link, once it is as proven.

## Where Linux goes in

Most of Legion already builds and runs off FreeBSD: the manager, the
Raft store, the API, the CLI, the console, the scheduler, `legion-image`'s
registry and OCI handling, `legion-net`'s DNS server and address plans.
The FreeBSD-specific part is about 16k lines:

| Today (FreeBSD) | Where | On Linux |
|---|---|---|
| Jails, the shim supervising them | `legion-jail` | crun, under Legion's own supervisor |
| `Sys`: about 100 host calls (jail, epair, ifconfig, CARP, rctl, pf, zfs, mounts) | `legion-sys` | a Linux host layer (netlink, cgroupfs, nft, zfs, mount) |
| Bridge, epair per pod, pf anchors (Service addresses, policy, NAT), WireGuard via ifconfig | `legion-net` | bridge, veth per pod, nftables tables, WireGuard via netlink and `wg` |
| The agent counting each container's memory (`kern.proc.vmmap`) and killing over it | `legion-sys` memory.rs | `memory.max`: the kernel does it, counting shared pages once |
| CARP for `--vip` | `legion-manager` vip | the leader announcing the address (gratuitous ARP and unsolicited NA) |
| bhyve tasks | `legion-jail` vm.rs | not ported (KVM later, if a workload asks) |
| `legion local`, `legion build` | `legion-local`, `legion-build` | not ported at first: Linux nodes run images built anywhere |

The seam for the runtime exists: `legion_runtime::Runtime` (`pull`,
`create`, `start`, `stop`, `pause`, `resume`, `remove`, `inspect`, `wait`,
`exec`…, `logs`, `list`, `events`) was written for a second
implementation, and Podman was one until phase 5. The host and network
side has no such seam yet: `Sys` names FreeBSD's tools, and the agent and
`legion-net` call them directly. Phase 1 makes that seam.

## Phases

Each phase ends with a **Done when**, checked in the lab, as Legion's
other phases are.

### Phase 0: a spike (a few days)

Build the daemon for `x86_64-unknown-linux-gnu`, run it manager-only
(`--no-agent`) on a Linux VM, join it to a FreeBSD cluster, and list what
breaks. Audit every `cfg(target_os)` and `cfg(unix)` (about 20 files) and
every place a FreeBSD tool is assumed.

**Done when** a Linux manager holds a Raft seat in a cluster whose
FreeBSD worker runs a Service, serves the console, and survives a change
of leader, with the list of breaks written down here.

### Phase 1: a platform seam (2 to 3 weeks)

Split what the agent asks of the host into interfaces at the level of
what it means, not which tool does it: a pod network (attach a pod, guard
it, publish a Service, NAT out), a mesh (WireGuard peers), volumes
(create, snapshot, send, receive, mount into a pod), limits (set, read
usage), and the node's facts (platform, capacity, what `doctor` finds).
The FreeBSD implementations are today's code, moved; `Sys` stays as
FreeBSD's tool layer under them. `FakeSys` tests keep passing unchanged.

`Platform.os` is already reported by nodes and used by the scheduler to
match images (`Platform::runs`); a Linux node reports `linux`, and only
Linux images are placed on it.

**Done when** the FreeBSD node runs through the new interfaces with no
change in behaviour: the full qualification suite passes on FreeBSD, and
the agent no longer names a FreeBSD tool outside the FreeBSD
implementations.

### Phase 2: one Linux node runs a Service (3 to 4 weeks)

- **Runtime** (`legion-crun`, implementing `Runtime`). A pod is a
  network namespace held by a bind mount (`/run/netns/legion-<uid>`), so
  no pause process is needed; each container is a crun container whose
  OCI spec joins that namespace and has its own mount, PID and UTS
  namespaces, and IPC shared within the pod. The rootfs is a ZFS clone of
  the image's top layer, as on FreeBSD. Secrets and Configs are bind
  mounts from the task's tmpfs.
- **Supervision.** Legion's supervisor stays the containers' parent, as
  the jail shim is now and Podman's conmon was: it holds stdout and
  stderr, keeps the last 1 MiB at teardown, records the exit code, and
  survives a daemon restart, so the agent adopts running pods as it does
  jails. `exec` and terminals go through `crun exec` with a console
  socket.
- **Limits.** The OCI spec's `linux.resources`: `memory.max`, `cpu.max`,
  `pids.max`. Usage for `legion usage`, autoscaling and metrics comes
  from `memory.current` and `cpu.stat` of the pod's cgroup. No memory
  killer of Legion's own on Linux.
- **Pod network.** Bridge `legion0`, a veth pair per pod (host end
  `lp<uid12>`, as now), addresses from the node's /24 and its IPv6 twin,
  Legion's DNS on the gateway. Outbound NAT in an nftables table of
  Legion's own (`table inet legion`), never touching the host's other
  tables, as Legion owns only `legion/*` anchors in pf.
- **Storage.** OpenZFS required, as `--zfs-root` is on FreeBSD; image
  layers, Volumes and snapshots work as they do there.
- **Host checks.** `legion doctor` on Linux: cgroups v2 unified, crun
  present and recent enough, nft, the wireguard and zfs modules,
  forwarding, and br_netfilter's settings.

**Done when** the lab's first test runs on a Linux node: a Service of a
Linux image (Postgres 17 and Node 22, which don't run under FreeBSD's
Linux ABI, and nginx with its defaults, which needs a workaround there)
is deployed, probed, scaled, updated and rolled
back, its logs and exec work, a memory limit holds, and the agent
restarted adopts its tasks.

### Phase 3: Linux in the cluster (3 to 4 weeks)

- **Mesh.** WireGuard by netlink and `wg`, the same keys, addresses and
  NAT relay rules as `legion-net::mesh`; Linux and FreeBSD nodes peer
  with each other.
- **Services.** Service addresses and published ports as nftables DNAT,
  spread across ready endpoints (`numgen random` or hash maps), with the
  same tables updated in place as endpoints change.
- **Network policy.** Per-pod sets and rules in Legion's table: ingress
  allowed by namespace and label, egress limits, DNS always allowed, the
  pod guarded before it starts (as phase "guard new pods" does with pf).
- **Ingress.** The agent's Route proxy is Rust already; it needs only
  the node to be marked `legion.ingress`.
- **Volumes across nodes.** Moves, migration on drain, backups and
  restores over the agents' sessions, as now (zfs send and receive are the
  same on Linux).

**Done when** the cluster test passes on a mixed cluster (a FreeBSD
manager, a Linux worker and a FreeBSD worker): pods reach each other
across the mesh both ways, a Service on the Linux node answers at its
address from the other two, a policy refuses what it should, and a
stateful Postgres moves from a Linux node to another Linux node on drain.

### Phase 4: the rest, and packaging (3 to 4 weeks)

- NFS and iSCSI Volumes (`mount.nfs`, open-iscsi) and volume plugins.
- `--vip`: the leader takes the address and announces it; a new leader
  takes it over within the election.
- Stateful updates' snapshots, init containers, Jobs and CronJobs, image
  pins and pruning, the console's Linux pages (what `doctor` finds).
- Packages: a systemd unit, `.deb` and `.rpm` naming crun, nftables,
  wireguard-tools and OpenZFS; a release tarball like FreeBSD's.
- Docs: a Linux section in DESIGN.md, the install guide, and
  COMPARISON.md saying what a Linux node is and isn't.

**Done when** the qualification suite, less the FreeBSD-only tests (jails
by name, bhyve, the Linux ABI, `legion local`, builds), passes on a
three-node Linux lab and on a mixed lab, under crun and again under runc,
with no leftovers.

## Not in this plan

- bhyve tasks on Linux (KVM, Firecracker): only when a workload asks.
- `legion local` and `legion build` on Linux: Linux nodes run images
  built anywhere; buildah exists for building.
- A Kubernetes API or CRI: as ROADMAP.md says, not Legion's direction.
- Storage without ZFS (LVM, btrfs, plain directories): a backend of its
  own, decided once a user asks for one.
- Changing the FreeBSD node to suit Linux, beyond the seam of phase 1.

## Effort

About 3 to 4 months to the end of phase 4, most of it phases 2 and 3; a
Linux worker that runs Services (phase 2) in about 6 to 8 weeks from the
start. The estimates assume the lab gains Linux VMs early (Debian 13 and
Ubuntu 24.04, amd64 first, arm64 after) and that the spike finds no
surprise in the manager.

## Decisions to make before phase 2

1. **Distributions** supported at first: Debian 13 and Ubuntu 24.04 LTS
   are proposed; Fedora or RHEL 10 if users ask.
2. **OpenZFS required** at first, or a simpler storage backend from the
   start (which delays phase 2).
3. **crun's oldest version** taken, and whether a package ships its own
   build of crun or names the distribution's.
4. **Linux-only clusters** supported, or always a FreeBSD manager.
