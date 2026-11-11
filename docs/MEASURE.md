# Measurements

What Legion's own work costs, measured (PLAN.md phase 9), with the test
that measures each, so the numbers can be taken again. Each test is
ignored by default and prints what it found:

```sh
cargo test -p legion-manager --release --lib reconciliation_time -- --ignored --nocapture
cargo test -p legion-manager --release --test api watch_fan_out -- --ignored --nocapture
cargo test -p legion-agent --release --test cluster endpoint_propagation -- --ignored --nocapture
```

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

To come, on the lab: pf updates as services grow, image pulls and loads,
and the mesh as nodes join.
