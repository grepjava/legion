# Measurements

What Legion's own work costs, measured (PLAN.md phase 9), with the test
that measures each, so the numbers can be taken again. Each test is
ignored by default and prints what it found:

```sh
cargo test -p legion-manager --release --lib reconciliation_time -- --ignored --nocapture
cargo test -p legion-manager --release --test api watch_fan_out -- --ignored --nocapture
cargo test -p legion-agent --release --test cluster endpoint_propagation -- --ignored --nocapture
```

Those on FreeBSD itself are below, with how they run.

Hardware: an Intel Core i9-12900KF (24 threads), 64 GiB, Windows 11; the
agent's measurement under WSL2 on it, as its tests need Unix. Fake hosts
stand in for FreeBSD where a measurement is of Legion's work alone: what
the kernel takes (pf, ZFS, jails) is measured on the lab, below.

## Reconciliation

One pass of the Service and Job controllers' planner over the whole
cluster, as the leader runs it on every change and every resync
(`reconciliation_time_by_cluster_size`, `crates/legion-manager/src/controller/plan_tests.rs`).
Tasks ask 10m of CPU and 16 MiB each; the pass after an update plans the
update's first step.

| Nodes | Services | Tasks | Settled pass | After an update |
|---|---|---|---|---|
| 10 | 50 | 150 | 1.9 ms | 4.4 ms |
| 50 | 200 | 600 | 10 ms | 17 ms |
| 100 | 500 | 2,000 | 34 ms | 37 ms |
| 200 | 1,000 | 5,000 | 78 ms | 85 ms |

Measuring found each owner reading every task, and every task checked
against every owner: a pass grew with services times tasks, 383 ms at
5,000 tasks. Tasks are now gathered by owner once a pass and owners found
by uid (after Legion 0.1.18), and a pass grows about as the tasks do.

## Watches

A Service applied, and the time until each of `n` watches on Services,
each on its own connection, has it: commit and fan-out together
(`watch_fan_out_by_watchers`, `crates/legion-manager/tests/api.rs`).

| Watchers | To the first | To half | To all |
|---|---|---|---|
| 1 | 5.2 ms | 5.2 ms | 5.2 ms |
| 10 | 2.9 ms | 2.9 ms | 14 ms |
| 100 | 3.0 ms | 4.3 ms | 8.8 ms |
| 500 | 4.4 ms | 14 ms | 33 ms |

## Endpoint propagation

A Service published on every node scaled from one task to two: the time
from the apply until the new task runs, and until every node's pf table
for the Service holds its address (`endpoint_propagation_by_nodes`,
`crates/legion-agent/tests/cluster.rs`; agents heartbeat every 200 ms
there, a tenth of the default).

| Nodes | New task running | In every node's table |
|---|---|---|
| 3 | 299 ms | 299 ms |
| 10 | 290 ms | 290 ms |
| 30 | 398 ms | 398 ms |

Every node has the address by the time the task is seen running: the
endpoints go out on the same change. The time is the scheduling and the
start, not the propagation.

## On FreeBSD

On a lab VM: FreeBSD 15.1 in KVM, 2 vCPUs and 4 GiB, on the hardware
above. Each test is in `scripts/legion.qualify`, so every record takes its
numbers again, in the test's log.

### Load balancing in pf

A node's pf work as services grow, each with a VIP, a port and three ready
tasks, in a throwaway VNET jail (`pf_updates_by_services`,
`crates/legion-net/tests/live.rs`): the first pass, a service added (the
rules change), and a task becoming ready (one table).

| Services | First pass | A service added | A task ready |
|---|---|---|---|
| 10 | 4.5 ms | 2.9 ms | 1.2 ms |
| 100 | 23 ms | 26 ms | 4.3 ms |
| 500 | 182 ms | 279 ms | 16 ms |
| 1,000 | 707 ms | 927 ms | 33 ms |
| 2,000 | 2.6 s | 4.2 s | 63 ms |

Measuring found a change of rules setting every table with a pfctl run of
its own: 4.2 s for a service added at 1,000 services, 16.5 s at 2,000, the
agent doing nothing else meanwhile. The rules now come with every table and
its addresses in one load (after Legion 0.1.18). What is left grows faster
than the services, in pf's own parsing; a cluster of thousands of services
is not what Legion is for.

### Images

Images pulled from Docker Hub, then exported, removed with their layers and
loaded from the archive, then loaded again under another name, their layers
there (`image_pulls_and_loads_by_size`, `crates/legion-image/tests/live.rs`).
A pull's time is mostly the network's, the lab's own connection; a load's is
Legion's: checking each layer and unpacking it onto ZFS.

| Image | Layers | Archive | Pull | Load | Load, layers there |
|---|---|---|---|---|---|
| `alpine:3.20` | 1 | 3 MiB | 10.3 s | 0.6 s | 0.0 s |
| `freebsd/freebsd-runtime:15.1` | 1 | 12 MiB | 3.9 s | 2.0 s | 0.0 s |
| `python:3.12-slim` | 4 | 41 MiB | 10.5 s | 7.4 s | 0.1 s |
| `postgres:17-alpine` | 10 | 112 MiB | 23.2 s | 18.0 s | 0.2 s |

Unpacking is most of a larger image's pull. A layer already there costs
nothing, so images that share their base pull and load only what differs.

### The mesh

Nodes join a WireGuard mesh one at a time, each a VNET jail on one switch
(`mesh_growth_by_nodes`, `crates/legion-net/tests/mesh.rs`): the new node
setting a peer for each other node, each other node adding it, and the
time until the new node has reached every other node.

| Nodes | New node's peers | Others add it (each, slowest) | Every node reached |
|---|---|---|---|
| 2 | 7 ms | 7 ms, 7 ms | 11 ms |
| 4 | 8 ms | 4 ms, 5 ms | 14 ms |
| 8 | 16 ms | 5 ms, 6 ms | 33 ms |
| 16 | 34 ms | 5 ms, 8 ms | 90 ms |
| 24 | 50 ms | 6 ms, 11 ms | 108 ms |

A join costs each node one peer, whatever the mesh's size; the new node
sets them all, about 2 ms each.
