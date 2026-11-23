# High availability

What it takes for a cluster to lose any one machine, a manager or a
worker, and carry on: every workload still serving, no data lost. Legion
does most of it on its own; the rest is how a cluster is laid out, which
`legion ha` checks.

```sh
legion ha            # every namespace you may read, and the cluster
legion ha -n shop    # one namespace
legion ha --json
```

It lists each single point of failure, with how to put it right, and exits
nonzero if any is a **risk** (losing one machine stops something, or loses
data). A **warning** carries on, but slowly, by hand, or only for some
clients. The console shows the same on its Availability page, and the
Overview has a tile for it. Any manager answers, from what it holds; a user
sees their namespaces' findings, and the cluster's own if they may read
nodes.

## The managers

Managers keep the cluster's state with Raft: a change is made once a
majority has it, so a cluster survives losing a minority of its managers.

| Managers | May lose |
|---|---|
| 1 | none: while it is gone nothing is placed, moved or changed (running tasks carry on) |
| 2 | none: one of two is not a majority |
| 3 | 1 |
| 5 | 2 |

An even number survives no more than the odd number below it. `legion ha`
says `managers` for 1 or 2, and warns of an even number.

### Witnesses

A third manager doesn't have to be a third server. A **witness** is a
manager that votes, so the quorum counts it, but runs no tasks and never
stands for leader, so it needs no ZFS, no pf and little memory: any small
FreeBSD machine (or VM) that both managers reach. Two managers and a
witness survive losing either manager.

```sh
# on a manager
legion join-token manager
# on the witness, then start its daemon as usual
legion join --witness --manager m1.example.org:7470 LGN-1-...
service legion start
```

The witness's data directory says it is one (a `witness` file), so its
daemon starts as a witness without being told (`legion daemon --witness`
does the same for a manager already joined). It never runs an agent, and
its Raft election timer is off: it votes for a full manager, and once a
quorum is up a full manager always leads. A witness can't start a cluster
(`legion init` refuses). `legion status`, the console's Overview and
`legion ha` mark it.

So a cluster needs two managers that may lead: three voters of which only
one is a full manager survive losing a witness, but not that manager
(`legion ha` says `leaders`).

## The nodes that run tasks

A node silent for 6 s is `suspect` and its tasks leave the load balancing;
silent for 16 s it is `down` and its tasks are replaced elsewhere
([PARTITIONS.md](PARTITIONS.md)). So with two or more nodes running tasks:

- **Stateless services** with two replicas or more lose nothing that
  clients notice beyond their open connections. One replica (`legion ha`:
  `one-replica`) is down until it starts on another node: 16 s at least,
  longer to pull its image. Replicas all on one node (`one-node`) go
  together: `placement.antiAffinity: [{service: NAME}]` keeps one per
  node.
- **Global services** run on every node, and need nothing more.
- **How clients reach a service.** A published port is open on every node,
  so a client that knows one node's address loses the service with that
  node (`published`). A load balancer address (`endpoint.loadBalancer`) is
  held by one ready node at a time and moved by CARP when it is lost;
  point clients and DNS at that.

With one node running every task (`nodes`), nothing else here helps.

## Data

A ZFS Volume's data is on the node that holds it. If that node is lost:

- **No replica** (`no-replica`, a risk): the data is lost with the node,
  and what uses it waits. Backups to a target (`backup:`) make it a
  warning: restored by hand, losing what changed since the last backup.
- **A replica** (`replica:`): a copy on another node, sent every 10 s to an
  hour (`everySeconds`), the most that can be lost. By hand
  (`failover: manual`, the default, a warning: `manual-failover`) the
  operator promotes it (`legion volume promote`); with `failover:
  automatic` the leader does, once the node has been down long enough
  that its tasks have stopped. Automatic needs its tasks to be
  `onPartition: stop`, so the old copy can't take writes after the new one
  does.
- **A database** is better replicated by itself: a primary and a replica,
  each a stateful service with its own Volume on its own node, with no
  copy lost.

NFS and iSCSI Volumes live off the nodes, as available as what serves
them.

## Fencing

A machine the managers can't reach may still be running. Legion has no
power switch over it, so a pod that must not run twice
(`onPartition: stop`) stops itself: its node's agent stops it once the
managers haven't answered for as long as they take to replace it, and if
the daemon itself is gone, each container's supervisor does, from a lease
the agent renewed ([PARTITIONS.md](PARTITIONS.md), self-fencing). What
remains is a node that is cut off and keeps serving clients on its side of
the split, for pods that may run twice; and a node removed by hand while
still running (removal is not fencing).

## Tested

`legion_api::ha`'s rules (`managers_are_counted_for_a_quorum_and_a_leader`,
`services_and_volumes_on_one_node_are_found`); a witness that votes, never
leads and lets the other full manager take over when the leader is lost,
with `Cluster.Availability` as an admin and a user
(`a_witness_votes_but_never_leads`); the gateway's `/api/availability`;
self-fencing as above.
