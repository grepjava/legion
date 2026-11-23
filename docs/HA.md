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

**Where a witness may be.** The managers open the connections to a
witness (to replicate, and to ask for its vote); it opens none of its own.
So a witness may sit where it reaches no manager, such as a VPS on the
internet for two managers behind a home router, as long as the managers
reach it: only its join needs a way to a manager (an SSH tunnel will do;
the join checks the cluster's CA, not the address). Such a witness can't
serve clients: a request sent to it is passed on to the leader, which it
can't reach ("connecting to ...: timed out", seen on 2026-11-23). Point
the CLI, the console and agents at the full managers.

**How long a failover takes.** A manager lost, the others elect another
once they stop hearing it: a follower waits a random 5 to 10 s
(`election_timeout_min/max`), and refuses to vote for anyone while it
still trusts the old leader's lease (10 s, `election_timeout_max`). So an
election comes about 16 s after the leader went (16.2 s in the lab, 16 s
on a real cluster), and nothing is decided meanwhile; running tasks carry
on. A new leader counts nodes' silence only from when it took over, so it
replaces the old leader's node's tasks no sooner than 16 s after that:
about 32 s after the leader's machine was lost, against 16 s for any other
node. A manager back within those 16 s (a restart, an upgrade) has nothing
replaced, and one back within about 10 s causes no election at all.

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

## Tested, and what it showed

What was run on 2026-11-23, at the commits given, and what was seen. Lab
runs are on three FreeBSD 15.1 VMs (lab base 5, 16 cores shared); the
real cluster is a Ryzen (manager, most workloads), weft (manager since
that day) and a witness on a Vultr VPS, managers on a home LAN and the
witness across the internet.

### Unit and in-process tests

- `legion_api::ha`: `managers_are_counted_for_a_quorum_and_a_leader`,
  `services_and_volumes_on_one_node_are_found`,
  `one_node_running_tasks_is_a_risk_and_says_so`.
- Witness and `Cluster.Availability` (`legion-manager`,
  `a_witness_votes_but_never_leads`): two managers are a `managers` risk;
  a user sees her namespace's findings, not the cluster's, and is told how
  many were left out; a witness joins, says so in its status, is known as
  one by the leader, can't initialise a cluster; the leader stopped, the
  other full manager leads, never the witness, and writes go on through
  the witness. Run three times, no failure.
- Streams of status and the rest (`a_tab_follows_everything_on_one_stream`),
  the gateway's `/api/availability`
  (`the_console_runs_the_cluster_for_an_admin`).
- Self-fencing: the supervisor's rule
  (`a_fenced_container_stops_once_its_lease_runs_out_then_is_killed`), the
  lease's file and clock (`legion_runtime::fence`), and in a real cluster
  of agents cut off by a silent link, the containers that carry the lease
  and the lease run out by the time the pod is replaced
  (`a_pod_that_must_not_run_twice_stops_before_it_is_replaced`, run twice).

### Lab

| Test | Commit | Result |
|---|---|---|
| `a_pod_that_must_not_run_twice_stops_when_the_daemon_dies` | c32c33c | passed: the daemon killed with SIGKILL, the `onPartition: stop` pod still ran 8 s later and was stopped by its supervisor between 12 and 45 s, its log saying why; the other pod ran on, the same process, and the next daemon adopted it |
| `pods_outlive_the_daemon` | c32c33c | passed: the supervisor's change leaves adoption as it was |
| `the_console_runs_the_cluster` | c32c33c, 1b741b7, 292fdc0 | passed: axe (WCAG 2.2 A and AA) on the Availability page, Backup targets and a Volume's Protection tab; the Availability page's Fix keeps a Service's tasks on different nodes and gives one a load balancer address, both saved as shown. The run at 3d7b945 failed on the test itself: an anti-affinity is stored with `required: false` written out, which it compared against the short form |
| `a_witness_keeps_the_quorum_and_a_full_manager_takes_over` | 3d7b945 | passed: two full managers and a witness; `legion ha` reports no cluster risk; the leader's daemon killed, the other full manager leads 16.2 s later, never the witness; writes through the witness (all three reach one another here); the lost node's task moves to the node left; the old leader back as a follower |

### The real cluster

- **Before and after.** `legion ha` on one manager: a `managers` risk, and
  27 risks in the workloads (services with one replica, Volumes on one
  node). weft promoted (`legion node promote weft`, no task stopped) and
  the witness joined (through an SSH tunnel, its pf open to the managers'
  address only): `3 voting managers (1 witness), 2 nodes running tasks`,
  no cluster risk. The workloads' risks are their owners' to fix and were
  left.
- **A restart of the leader's daemon** (`service legion restart`, a second
  or two): no election, the same leader and term; every jail kept.
- **The leader's daemon stopped 11 s:** no election. The followers were
  still within the old leader's lease, then waiting their timeout.
- **The leader's daemon stopped 22 s:** weft elected at 16 s (term 2); the
  Ryzen started at 22 s and rejoined as a follower; its node was never
  marked suspect or down, its 18 jails were the same before and after, its
  services answered throughout (an HTTPS health check: 200), and nothing
  was replaced, as the new leader had led 6 s when the Ryzen was back.
  The Ryzen's services were placed on it by constraint and their Volumes
  are on it, so nothing could have moved anyway; a node's other tasks
  would have, after 16 s of the new leader.
- **A write through the witness while weft led:** refused after a 36 s
  wait, the witness unable to reach weft's LAN address; the same write
  through a full manager went through. Hence "Where a witness may be"
  above.

### The forms

Driven in a browser against a throwaway manager (no agent), the stored
objects read back after each: a backup target added; a Volume's Protection
tab opened from a fix, filled in for it, the Service using the Volume set
to `onPartition: stop` from it, then a replica, two snapshot schedules and
backups saved; a Service's replicas raised from the Availability page.
That found three faults, fixed before they shipped: an object's page read
itself again, losing the form, when a tab came or went with it (a Volume
given a replica gains a Replica tab); a Volume with no node yet was said
to be on "node ?"; and the warning about Services that would run on cut
off stayed after they were fixed, its list read once.

### Not tested

- A manager's whole machine lost (power or network) on the real cluster,
  as against its daemon stopped: the lab test kills the daemon, which is
  the same to the others.
- The two LAN managers cut off from each other with the witness reaching
  both: the side with the witness keeps the quorum, as Raft says, but no
  test splits them.
- The rules of `legion ha` against every kind of Service and Volume a
  cluster may hold: the tests cover each rule once.
