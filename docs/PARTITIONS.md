# When a cluster splits

What Legion does when its nodes can't all reach each other, and what it
promises. This is the contract of [PLAN.md](../PLAN.md) phase 4. It starts
from the code as it is: each statement says whether a test shows it, and
where none does yet, it says so. The design behind it is in
[DESIGN.md](DESIGN.md) §3.2, §3.3, §3.7 and §6.7.

Legion fails open. A workload keeps running when the control plane can't
reach it, because surviving a control-plane outage is a stated goal. The
cost is that a workload can run twice for a while, once on each side of a
split. The cases are listed under [Running twice](#running-twice).

## A worker cut off from the managers

**What the managers see.** Agents heartbeat every 2 s. Only the leader
judges them, and only from heartbeats it has heard itself or that other
managers forwarded to it:

| Silent for | Node state | What happens |
|---|---|---|
| 6 s (3 heartbeats) | `suspect` | Its tasks leave the services' endpoints, so other nodes stop sending them connections. It gets no new tasks. Nothing is replaced. |
| 16 s (8 heartbeats) | `down` | Its tasks are marked `orphaned` and told to stop, in one write, and their slots are filled elsewhere in the same controller pass. |

The leader checks once a second, so each time can be up to a second late. A
new leader marks nothing `down` until it has led for 16 s, and nothing
`suspect` before it has heard from the node.

Tested: `tasks_of_a_lost_node_move`, `a_global_task_stays_on_a_node_taking_no_new_ones`
and `lost_tasks_are_replaced_whatever_the_restart_policy` (the planner);
`tasks_survive_losing_a_node` and
`a_silent_node_loses_its_connections_before_its_tasks` (agents in a real
cluster, with the agent process stopped); and
`a_node_cut_off_runs_on_until_it_is_back` (an agent kept running, cut off
by a proxy that goes silent, as a split network does).

**What happens to its tasks, by kind:**

- **Replicated service:** each task is replaced on another node, with no
  delay beyond the 16 s and whatever the restart policy says.
- **Global service:** the task stays while the node is `suspect`. Once the
  node is `down`, its slot goes with it, and comes back with the node.
- **Job:** the task is retried elsewhere, and isn't counted as a failure.
- **A task bound to its volume** (a ZFS volume, an iSCSI one, or any other
  used by one node at a time): the replacement waits, `Pending` with "volume
  on another node", until the node comes back or is removed. The volume
  stays with the node, as only the node can say the device is free.
- **A task on a shared volume (NFS)** isn't bound, and is replaced elsewhere
  like any other.

**What the agent does.** Nothing, on its own account. It keeps running every
task it was last assigned, keeps its pf rules and WireGuard peers, and
retries a manager every 500 ms. No lease or timeout makes it stop: tested
by `a_node_cut_off_runs_on_until_it_is_back`, whose old pod runs on beside
its replacement. An agent
restarted while cut off starts reconciling only once it has a full set of
assignments from a manager, so its pods keep running meanwhile.

**When it's back.** The manager sends the agent its full set of assignments
on every connection, the orphaned tasks included, now with their desired
state `shutdown`. The agent stops those, and tears down any container that
no task of its own accounts for. A task is identified by its uid, and a
replacement is a new task with a new uid, so the old one is never started
again: an orphaned task is terminal, and the agent starts only tasks that
aren't. Tested: `tasks_survive_losing_a_node`.

Until it has stopped the old pod, the node reports the task as it is,
running, over `orphaned`, and the reason it was orphaned leaves its status.
The task stays told to stop, so it gets no connections, as endpoints take
only tasks told to run, and the service counts its replacement, not it.
Tested: `an_orphan_reported_running_by_its_returning_node_displaces_nothing`.

## Managers without a majority

A cluster keeps working while a majority of its managers can reach each
other. Without one, nothing can be written: no API, no scheduling, no
replacing tasks on down nodes, no autoscaling and no certificate renewals.
Writes give up after 30 s with an error saying the write may still apply.
Agents keep running their pods and retry. Tested: `minority_cannot_commit`
and `leader_loss_and_rejoin` (the store), and
`a_follower_cut_off_from_the_leader_opens_no_watch` (the API).

Not tested: a leader cut off from the majority doesn't step down until it
hears of a newer term. Until then it keeps running the controller, the image
resolver, ACME, the volume migrator and the floating address. Their writes
fail, after 30 s each, and nothing is known to go wrong from that, but no
test shows it.

## Removing a node

`legion delete node NAME` is the operator's word that the node
has stopped. Legion has no fencing: it can't stop a node it can't reach, so
the word is taken on trust.

- Certificates the node was issued up to the removal are refused from then
  on, on calls and every 5 s on open streams. Tested:
  `a_removed_worker_stays_out_until_it_joins_again` and the two tests
  beside it.
- Its tasks are orphaned with "was removed", and replaced.
- A volume bound to it, used by one node at a time, is let go, and its
  tasks can start elsewhere. A ZFS move that had copied the volume to
  another node hands the volume to that node. Tested:
  `a_move_whose_old_node_is_removed_keeps_the_copy`.
- **The node's agent, if it is still running, keeps running its pods.** It
  takes the refusal as a lost connection and retries forever.

The removal is recorded before the Node is deleted, so the node can't come
back between the two writes. If the record's write fails without saying
whether it went through, Legion reads it back once the store is current: a
record that was written counts, so a removal that fails undoes it rather
than leave a node refused that is still in the cluster. A join's record of
a name is settled the same way. Tested:
`a_record_whose_reply_was_lost_counts_as_written`, with the reply lost on
purpose. Still open: a write that times out may commit after the read, and
nothing then undoes it.

## Running twice

Where an old copy of a workload and its replacement can run at once today:

1. **Any task of a node cut off, but still running:** a replicated or
   global service's, or a Job's, from 16 s after the split until the node
   is back. Both copies serve whoever can reach them. Tested:
   `a_node_cut_off_runs_on_until_it_is_back`.
2. **A task on a shared (NFS) volume**, the same way, with both copies
   writing the same files.
3. **A removed node that is in fact still running**, for as long as it
   runs. Removal is not fencing.
4. **A node whose agent reaches only a cut-off manager.** Assignments are
   served from each manager's own copy of the store, so that agent keeps
   what that copy says, and can't learn that its tasks were replaced.
   Not tested.

A task bound to a volume used by one node at a time doesn't run twice
during a split: its replacement waits for the volume.

## A volume an orphaned task used

An orphaned task's pod may still be running on its node, so a volume used
one node at a time that the task used stays held by it until the node says
the task stopped, or is removed. Until then no other task using the volume
is placed (`volume held by an orphaned task`), and a ZFS move of it doesn't
take its last step, as the pod may still be writing. Without this, a node
coming back could start the next task on the volume before it had stopped
the old pod, and a move whose task was cut off before it was seen to stop
would send its last increment without the pod's latest writes.

A node coming back says so either way. If it still has a worker for the
task, the worker stops the pod and reports `shutdown`. If it has none, for
example after its agent restarted, it removes whatever is left and reports
`shutdown` with "not running on its node once back". An agent from before
v0.1.15 doesn't do the second, so a volume it held waits until the node is
removed. Tested: `tasks_go_where_their_volume_is` and
`a_move_waits_for_a_task_orphaned_before_it_was_seen_to_stop` (the
planner), and `tasks_survive_losing_a_node` (agents in a real cluster).

## Not yet known

Nothing found in the code is unaccounted for. No test yet leaves an agent
with only a cut-off manager (the fourth way of running twice): that needs
several managers and a link between them that can be cut.

## What comes next

PLAN.md phase 4 turns this into tests and decisions:

- Fault injection for asymmetric partitions and stale assignments, for the
  cases above that no test shows yet.
- Decision 3, recommended: fail-open stays the default, with an opt-in
  fail-closed mode for workloads that must never run twice. Such a
  workload's agent stops it once cut off for longer than the managers take
  to replace it, and the managers wait that long before they do.
- Exclusive storage moves only on a confirmed release, or on removal, which
  the operator states means the node has stopped.
