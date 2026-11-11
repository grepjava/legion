# Legion implementation plan

Updated: 9 November 2026. Baseline: v0.1.15. Turns [ROADMAP.MD](ROADMAP.MD)
into an order of work; what each milestone means stays there.

The order: make test results trustworthy first, then settle what happens
when a cluster splits, then build the features that depend on those
answers. Each phase ends in a tagged release with recorded evidence.

## What the code review changed

The roadmap's direction holds. Checking it against the code changes the
scope or order of these items:

| Finding | Effect on the plan |
|---|---|
| Only 12 of the 23 privileged tests are in the current full run. Six of the 11 others are the multi-host application tests (`cluster`, `exit`, `memory`, `stateful`, `storage`, `teams`). | P0.1 starts with coverage, not new tests. Most tests already exist. |
| Live tests print "skipping" and **pass** when their environment is unset, so `linux_images` reports success without running. | A skip must be a visible result. This comes first: everything else relies on it. |
| Fake runtimes hid real bugs. The infra jail bug passed every unit test because the fake didn't model the kernel's parent rule. | New work in P0.1, not in the roadmap: check that fakes behave like FreeBSD. |
| Exclusive volumes already stay with their node until it releases them or is removed (`plan/migrate.rs`). | P0.2 is smaller for storage: write the contract down and test it, rather than build it. |
| A down node's tasks are replaced while a partitioned agent keeps running them. | This is P0.2's real gap: stateless and singleton workloads can run twice. |
| `pfil_member` and `pfil_local_phys` are now hard requirements for pods. | Add them to `legion doctor`; the roadmap's list predates this. |
| There is no version handshake between managers and agents. | P1.4 can't test "unsupported version combinations" without one. Add it. |
| No pull policy or recorded image digest exists, and nothing resembling an audit log. | P1.3 and P2.1 are new builds, not extensions. |
| jailer's image handoff and shared qualification wait on P1.3. | Move P1.3 earlier: it doesn't depend on P0.2 and it unblocks jailer. |

Not in the roadmap, and added here: a release checklist covering the port
and arm64, the fake checks, the version handshake, and two known gaps (an
ACME route change between recheck and write, and a supersede whose write
outcome is unknown).

## Phases

| Phase | Work | Roadmap | Waits on | jailer |
|---|---|---|---|---|
| 1. Honest results | Qualification runner, no silent skips, all live tests | P0.1, single host | Test hosts | Shares the harness |
| 2. Doctor | Read-only host checks in a shared crate | P1.1 | Nothing; alongside phase 1 | Its P2.3 reuses them |
| 3. Image identity | Pull policy, recorded digests, reproducible rollback | P1.3 | Phase 1 | Unblocks its P1.4 |
| 4. Multi-host and partitions | Cluster qualification, partition contract, fault injection | P0.1, multi-host; P0.2 | Phase 1; three or more hosts | Needed by its phase 5 |
| 5. Drain barrier | Drain status, wait mode, completion evidence | P1.2 | Phase 4 | Needed by its phase 5 |
| 6. Recovery and upgrade | Version handshake, restore drills, migration interruption | P1.4 | Phase 4 | Needed by its phase 5 |
| 7. Trust and audit | Signature policy, revocation coverage, audit events | P2.1 | Phase 3 | Coordinate with its P2.1 |
| 8. Joint qualification | Build to Recover across both projects; examples and matrices | P2.2 | Phases 3 to 7 | Its phase 5 |
| 9. Measure | Benchmarks; extensions only on a concrete need | P3 | Phase 8 | |

Phases 2 and 3 can run beside phase 1 and phase 4 respectively. Everything
else is in dependency order.

### Phase 1: Honest results

Goal: a pass means the test ran, on a named host, and left nothing behind.

1. Move the current test runner into the repository as a qualification
   script. It checks its preconditions and fails if they don't hold.
2. Make required live tests fail rather than skip when an environment
   variable is missing. Report each test as passed, failed or skipped, with
   the reason.
3. List all 23 tests in a manifest with what each needs. Bring in the
   single-host ones not yet run: `pods_on_the_bridge`, `policy_on_the_bridge`,
   `linux_images`, `network_storage`, and the bhyve guest test if it stays.
4. Record the OS, architecture, Legion revision and each result in an
   archived file. Compare jails, mounts, datasets, interfaces, pf state and
   rctl rules before and after, and fail on a difference the test owns.
5. Add daemon crash and adoption coverage: kill the daemon under running
   workloads and check they're adopted.
6. Run shared assertions against both the fakes and real FreeBSD for what
   fakes model: jail parameter rules, pf loads, ZFS properties.
7. Write the release checklist: versions, port `distinfo`, `poudriere
   testport`, arm64, notes, tag. State which steps are automated.

Exit: a release candidate has a recorded run with every test accounted for,
on each supported platform, which for now means amd64 (see Decisions). Agree
the harness with jailer before step 1, so one serves both.

### Phase 2: Doctor

Goal: `legion doctor` says whether a host can run Legion and how to fix it.

1. Put the checks in a shared crate, most likely `legion-sys`. Each is
   callable alone, takes its anchor name as a parameter, returns failure,
   warning, unsupported or not applicable with remedy text, and doesn't
   depend on the daemon or its config.
2. Cover RACCT, pf and its anchor hooks, forwarding, the bridge filtering
   sysctls, required kernel modules, ZFS root access and the Linux ABI.
3. Add the command with text and JSON output and a nonzero exit on a missing
   requirement. It changes nothing on the host.

Exit: tests cover each failure through the fakes without root, and one
checked run on a real host matches. Scheduling on readiness stays a later
change. Move the agent's existing startup checks onto the same code.

### Phase 3: Image identity

Goal: what was deployed can be named exactly, and rollback restores it.

1. Decide where tags are resolved (see Decisions) and write it down.
2. Add explicit pull policies.
3. Record the resolved digest for each deployment, and the platform manifest
   for each task. Handle mixed-architecture clusters through the index.
4. Make rollback use the recorded digest, not the tag.

The shape:

- **Resolving.** A task on the leader, beside the controller's pass, which
  does no network I/O itself. For each Service or Job generation it asks the
  registry what each reference in the pod names now, with the pod's
  `imagePullSecrets`. It uses `Client::document`, so the digest is the one the
  tag names: the manifest, or for a multi-architecture image the index, never
  the config. The parsing of pull secrets moves from the agent to `legion-api`,
  so both use one copy.
- **Recording.** `ServiceStatus.images` holds, for a generation, each
  reference as written and the `repository@sha256:...` it resolved to.
  `UpdateStatus` keeps the previous generation's beside `previous`, so a
  rollback returns to those digests and asks no registry.
- **Tasks.** `TaskSpec.images` carries that map, and the agent looks each
  image up there when it pulls and builds a container. `TaskSpec.pod` stays as
  written: the controller compares it with the Service's to find outdated
  tasks, so rewriting it would replace every task. A task from before this has
  an empty map and runs as it did.
- **Waiting.** The plan makes no task for a generation until it is resolved.
  One that can't be says why in the Service's status, and retries; what runs
  keeps running. A reference with a digest needs no registry, and a local one
  stays as named.
- **Pull policy.** Per container: `resolve`, the default, or `never`, which
  uses what the node holds and asks no registry. A digest pinned at deployment
  means a node pulls only what it lacks, so there is no `always`. A tag moved
  since deployment changes nothing until the Service is deployed again.
- **Per node.** `TaskStatus` reports the platform manifest each image ran as,
  so a mixed cluster shows one index digest and its manifests. Status and logs
  name references and digests, never credentials.

Exit: a tag moved between two nodes' pulls can't produce an unnoticed mix,
rollback still works after a tag changes, and private registry details stay
out of status and logs. Tag a release so jailer can start its image handoff.

State, 8 November 2026: steps 1 to 4 are in, on main once qualified. For the
exit, `a_deployment_runs_what_its_tags_named_then` (legion-manager's API
tests) runs the resolver against a registry with a login: scaling keeps the
digest after the tag moves, an update takes the new one, a rollback the old
one, and the password appears in no status or task. The plan's tests cover
the rest: no task before its pod is resolved, and old tasks running while an
update waits. Not shown on real hosts yet: a cluster of two architectures,
as the lab is amd64 only. The live tests whose daemons reach Docker Hub
(storage, exit, teams) now run their images by digest; the adopt test's
daemon has no route out and uses `pullPolicy: never`. Released in
v0.1.14, tagged on 8 November 2026 after a clean run of all 36 steps on
`scripts/lab`.

### Phase 4: Multi-host and partitions

Goal: a written contract for a split cluster, with tests showing it's true.

1. Bring the six multi-host application tests and the mesh into the
   qualification run, on disposable hosts.
2. Write the contract: quorum loss, agent disconnection, node removal,
   reconnection, and reassigning exclusive storage. Record the current
   behaviour first, including what already holds for exclusive volumes.
3. Add fault injection for asymmetric partitions and stale assignments.
4. Decide fail-open or fail-closed for each workload class (see Decisions),
   and implement the result.
5. Require fencing or a stated operator procedure before exclusive storage
   moves without a confirmed release. Say plainly that node removal is the
   operator's word that the node has stopped.
6. Close the known gap on a supersede whose write outcome is unknown.

Exit: tests show whether old and replacement workloads can run at once, the
contract matches them, and reconnecting resurrects nothing obsolete.

State, 8 November 2026: item 1 is done; the six multi-host tests and the
mesh are in every full run. Item 2's record of the current behaviour is
[docs/PARTITIONS.md](docs/PARTITIONS.md): what each side does, which tests
show it, the four ways a workload can run twice today, and three cases only
the code suggests, which need tests. Item 6 is closed: a removal's or a
join's record whose write outcome is unknown is read back once the store is
current (`a_record_whose_reply_was_lost_counts_as_written`, through a
lost-reply fault in the `test-hooks` feature), except a write that commits
after the read. Decision 3 is taken as recommended, and item 4 is
built: `onPartition: stop` (docs/PARTITIONS.md). A volume used one node at
a time stays held by an orphaned task until its node says the task
stopped, and a node cut off with its agent running is tested through a
link that goes silent. A leader cut off from the majority answers no
heartbeats, so agents attached to it know they are cut off. Item 5 is a
stated procedure: what to check before removing a node that holds data.
Released in v0.1.15, tagged on 9 November 2026 at 27ee1c1 after a clean
run of all 39 steps on `scripts/lab`.

### Phase 5: Drain barrier

Goal: a tool can ask "is this node safe to stop?" and trust the answer.

State, 8 November 2026: the drain status and `legion node drain --wait`
are built (docs/DESIGN.md, Nodes). Its phases are `draining`, `blocked`
and `completed`; there is no `failed`, as no condition found so far can't
clear by itself. A task still running says why: `data`, `budget` or
`replacement`, as the planner decided that pass. The exit's tests are in:
stateless and stateful evacuation (planner tests, and the postgres test on
the lab through `drain --wait`), a budget holding a stateful task
(`a_drain_says_a_task_waits_for_its_budget`), a transfer cut short and a
last part lost on its way in, each tried again with the failure in the
drain status, and a source cut off mid-copy, blocking the drain until it
is back (cluster tests, through fault hooks in the fake host), cancellation
(`a_drain_is_waited_for`), and a waiter riding out the leader's loss among
three managers (`a_drain_waiter_survives_the_leaders_loss`, which fails
without the waiter's retries). A waiter whose own manager goes needs the
managers' floating address, and that is not tested. Not yet run on the lab: the faults are
injected on the fake host, as the lab has no way to cut a transfer. The
slow lab found a drain reported `completed` while a stateful Volume's move,
called off as its target went silent, had left the data behind: such a
Volume now holds the drain (`waiting`) until it has moved
(`a_drain_waits_for_data_whose_move_was_called_off`). Released in v0.1.15.

1. Add a persisted drain operation with `Draining`, `Blocked`, `Completed`
   and `Failed`, stable reason codes and per-workload progress. Reuse the
   existing controller; there's no second evacuation controller.
2. Define completion from phase 4's ownership evidence: unknown source state
   never counts as released.
3. Add `legion node drain --wait` with JSON output and a meaningful exit.

Exit: waiters survive manager failover, and tests cover stateless and
stateful evacuation, blocked budgets, interrupted transfers, a disconnected
source and cancellation. Manager quorum stays a separate report.

### Phase 6: Recovery and upgrade

Goal: recovery and upgrade are procedures that have been drilled.

1. Add a version handshake, so unsupported mixes are refused with a reason.
   Then close the ACME gap, a route changed between the recheck and the write:
   it needs a write conditional on several objects, a new Raft command that
   nodes of two versions would apply differently without the handshake.
2. Document and test backup of control state, keys and volumes separately.
3. Interrupt a ZFS migration at each handoff stage, and test a full disk.
4. Restore a representative application onto replacement hosts, and record
   measured recovery times.

State, 8 November 2026: item 1's handshake is built (docs/DESIGN.md,
3.8, Versions): registrations, joins and Raft calls between builds that
don't work together are refused, saying which to upgrade first, each
shown by a test that fails without the check. Builds speak
protocol 2 now: the managers' log takes guarded writes once every
manager speaks it, recorded as a floor that only rises
(`the_floor_rises_once_every_manager_speaks_it`). The ACME gap is closed
with one: a certificate is written only while the routes calling for it
are unchanged (`a_route_changed_as_its_certificate_is_written_gets_none`,
which shows the gap still open at protocol 1). The upgrade drill
(`the_cluster_upgrades_one_node_at_a_time`) is in every full run: three
managers on v0.1.14 serve an app while each node is restarted on the new
build, the floor rising once the last is; then every node is restarted
again, and no request may fail. Its first runs found three gaps, each
fixed: the node resolver died with the daemon (now a process of its own,
so lookups fail only once, while a node leaves v0.1.14); a restarted agent
loaded its services' pf rules before it knew the endpoints, dropping
published ports; and setting wg0's and the bridge's addresses again at
each start dropped the routes through them for a moment. Released in
v0.1.15.

Item 2 is built (docs/DESIGN.md, What to back up): the control state's
backup holds no volume's data, and says so; a manager is rebuilt on an
empty disk from it and its keys alone, `pki`, `node-id` and `admin.pem`
(`a_manager_is_rebuilt_on_a_new_disk_from_its_backup_and_keys`); and
`legion volume backup` and `restore` take a Volume's data apart, as a
crash-consistent snapshot, labelled so in the file and by the CLI, restored
onto a node without the data, the Volume made only once all of it is there
(`a_volume_backup_restores_onto_another_node`). Tested on the fake host;
on the lab's ZFS since item 4's drill.

Item 3 is done, 9 November 2026 (docs/DESIGN.md, Stateful migration):
a move cut short at each stage loses nothing. On the fake host, a full
disk holds the full copy, then the last part, until there is room or the
full node is drained, which sends the Volume to a third node and drops
the full node's copy (`a_full_disk_holds_a_move_until_there_is_room`);
an old node cut off as the move finishes keeps the task from starting
anywhere else; a new node gone silent sends the Volume elsewhere; and a
manager lost as the last part lands has it sent again onto the full copy.
On the lab's ZFS, in the manifest run,
`a_move_cut_short_or_held_by_a_full_disk_loses_nothing` moves a Postgres
primary held first by a quota (the drain status says `space quota
exceeded`), then cut short by killing the receiving node's daemon during
the full copy and its `zfs receive` during the last part, and ends with
every row on the new node and nothing left of what was cut. Its first
runs found a gap in the test proxy (a cut link passed data already read)
and no Legion faults.

Item 4 is done, 10 November 2026 (docs/DESIGN.md, What to back up, with
the order of a restore): `postgres_is_restored_onto_replacement_hosts`,
in the manifest run, loses every host of a cluster running Postgres (a
primary and a replica on two workers): its daemons are killed and its
data directories and pools destroyed. It is rebuilt on the same
machines with new disks from the three backups kept apart: the control
state, the manager's keys and the primary's Volume. This is also the
volume restore's first run on real ZFS. The primary comes back with every
row up to its backup and none written after, recovering as from a crash,
and the replica copies it anew. Measured on the lab's 2-CPU VMs, from
the restore's start (run 78caae5): the manager serving in 0.7 s; the two
new hosts joined and given the Postgres image in 263 s; the 173 MiB
Volume restored in 8.9 s (its backup took 12.3 s); the primary serving
16 s later and the replica in step 223 s after that; 511 s in all. The
image load and the replica's copy take most of it. The first run failed
before any backup: the test forced a checkpoint as the replica caught up
with a large write, and the replica, with no WAL kept for it, most likely
never could. The test no longer does so, and it dumps Postgres's state
if a wait times out.

All four items are done.

Exit: a supported upgrade path is tested. Snapshots are labelled
crash-consistent, and a control-state backup isn't described as a backup of
volume contents.

### Phase 7: Trust and audit

Goal: an explicit policy on which images may run, and who did what.

1. Add signature verification using an established format and tooling,
   agreed with jailer, which signs. Define the behaviour when verification
   services are down.
2. Test revocation of users, workers and managers across follower requests,
   renewal and open streams.
3. Add audit events naming actor, action, target and result, never secret
   contents, with a retention rule.

State, 10 November 2026: item 1 is built (docs/DESIGN.md, Signatures),
the shape agreed with jailer: cosign key-based signatures, as jailer
makes them, checked by `ImagePolicy` as the leader resolves a pod, in
`enforce` or `warn` mode; a registry that cannot be read holds an
update, its old tasks running, and is tried again every 30 s. The code
making and checking signatures is one module, `legion_image::cosign`,
for both; jailer moves its signing onto it at the next tag. Tested on
the fake registries of the unit and API tests; jailer's handoff test
(signed, by digest, unsigned then signed, another key, warn, registry
down, a local image) runs it on the lab.

Item 2, in progress (docs/DESIGN.md, Revocation). A survey of every
principal across follower requests, renewal and open streams found
three holes, each fixed and tested, the test failing without the fix:
- a user's session (exec, logs, port-forward) outlived their RoleBinding:
  it now ends within 5 s
  (`a_users_session_ends_when_their_binding_goes`);
- a user's watch went quiet on a namespace they lost, the client unable to
  tell: it now ends saying why (`users_act_within_their_role_bindings`);
- a manager could not be removed at all, though DESIGN.md said it was:
  deleting its Node now takes it out of the Raft membership (never the
  last voter) and refuses its certificate on the API and the dispatcher
  (`a_removed_manager_leaves_the_membership_and_is_refused`). It held the
  CA key, so only rotating the CA makes that sure; decided with the user
  as a later item.
And a gap: a worker's streams on a follower were checked against its own
replica only. They now open once it has caught up with the leader
(`a_follower_cut_off_from_the_leader_opens_no_worker_stream`). Through a
real follower, a revoked user and a removed worker are refused, for what
it forwards (reads, heartbeats, renewals) and what it serves (the user's
watch and the worker's assignments, peers, policies and routes, each
ending), and a user's certificate renews nothing
(`revocation_holds_through_a_follower`). Item 2 is done but for CA
rotation, its own item to come.

Item 3 is built (docs/DESIGN.md, Audit), as decided with the user: kept
in the replicated store, for changes, sessions and refusals, not reads.
An event is recorded where the request first arrives, a follower handing
its own to the leader; the leader keeps them 30 days and at most 100 000,
both settable. `legion audit` lists them, narrowed. Tested: the admin's
Secret written through a follower is recorded with the admin as actor
and the follower as where it was decided, alice's refusal with its
message, and nothing of what the Secret held
(`changes_and_refusals_are_audited_without_their_contents`); the oldest
go past the cap (`the_audit_trail_keeps_only_what_its_retention_allows`);
the CLI lists and narrows (`the_audit_trail_lists_changes_and_narrows`).
Each test fails without what it covers.

Exit: invalid or missing required signatures are refused under the policy.

### Phase 8: Joint qualification

Goal: Build, Sign, Push, Deploy, Drain, Migrate and Recover proved end to end
with jailer, recording both versions and their shared crate revisions.
Alongside it, add tested FreeBSD application examples, the Linuxulator and
Compose support matrices, and measured resource-limit behaviour.

Built, agreed with jailer: one chain in Legion's handoff test, jailer
adding no test of its own for it
(`a_jailer_image_goes_from_build_to_recovery`). jailer builds a writer,
signs it with a key it made and pushes it; Legion runs it under an
ImagePolicy enforcing that key, drains its node, moves it back with the
move's last part cut short, backs its Volume up, loses the host and
restores it onto another. At each stage the task runs the digest jailer
recorded and the policy verified, and its data is the same byte for byte;
a drain or move keeps the verification, a restore checks the signature
again. The record names both versions: `jailer --version` gives the
Legion crates jailer was built with (jailer 1dfeaa6). It passed on the lab
in 874 s, its first run.

Alongside:
- Compose: docs/COMPOSE.md, every key with the test that converts it and
  the live one that runs it; the keys no test converted gained one.
- Linux images: docs/LINUX.md, from `linux_application_images`, which
  keeps the known failures failing as described. It found static Linux
  programs refused, fixed (`--linux` sets the fallback ELF brand,
  `legion_jail::enable_linux`, shared with jailer); nginx's workers
  exiting on the ABI's refusal of EPOLLEXCLUSIVE (jailer found why),
  served with `accept_mutex on`; Postgres needing signalfd and Node 22
  spinning, which the ABI does not run.
- Examples: `examples/`, a static site on nginx and a counter on Redis,
  built from FreeBSD's packages with jailer and run on three nodes by
  `the_examples_run_as_their_readmes_say`.
- Limits: `limits_hold_what_wants_more` measures cpu (500m gave 0.47 of
  a CPU) and maxproc (20 let 19 run); memory was measured on Postgres
  (`postgres_under_a_memory_limit`). `openfiles` is not measured: rctl
  counts descriptor tables, so a fork takes one whole (DESIGN.md 6.3).

The lab found two more on the way: a move whose target was only suspect
was dropped and its full copy sent again elsewhere (now kept), and a test
stopped at its limit left its daemon beside the next test's (the test lab
now kills it).

Exit: the chain passes on the lab with both versions recorded, and the
matrices, examples and limits run in the qualification.

### Phase 9: Measure

Benchmark reconciliation, watches, endpoint propagation, pf updates, image
operations and mesh growth, publishing sizes and hardware. Optimize what
measurement shows. Affinity, external load balancers, manager promotion,
bhyve and NAT options wait for a concrete user need.

Measured (docs/MEASURE.md, after 0.1.18), each by an ignored test that
prints its table; those on FreeBSD run in every qualification. Two were
optimized for what they showed:

- Reconciliation: each owner read every task, and every task was checked
  against every owner. Tasks are now gathered by owner once a pass: 5,000
  tasks plan in 78 ms, from 383.
- pf: a change of rules set each service's table with a pfctl run of its
  own. The rules now come with every table in one load: a service added
  at 1,000 services takes 0.9 s, from 4.2.

Watches (500 within 33 ms), endpoint propagation (in every table by the
time the task runs), image loads (unpacking is most of a large pull) and
mesh joins (one peer per node, 108 ms to reach 23 others) showed nothing
to fix.

## Decisions needed

1. **Test hosts.** Decided 6 November 2026: QEMU with KVM on the Windows PC,
   under WSL2, through `scripts/lab`, shared with jailer. Three VMs on one
   read-only base, reset for every run. Real hardware stays untested, and
   records say the host was a VM.
2. **arm64.** Decided 6 November 2026: amd64 is finished first. Until an
   arm64 host joins qualification, arm64 isn't called supported. It stays in
   the port and in CI's emulated unit tests, and was last built at v0.1.7.
3. **Partitions.** Recommended default: fail-open, as today, since workloads
   surviving a control-plane outage is a stated goal. Add an opt-in fail-closed
   mode for workloads that must never run twice.
4. **Image resolution.** Decided 7 November 2026: the leader resolves a tag
   once at deployment, using the namespace's pull secret, so every node gets
   one digest. The cost is that managers need registry access. A local image
   (`localhost/`) has no registry to ask: it stays as named, and is loaded
   onto the nodes that run it.
5. **Port.** Submit the port once `testport` passes on the current release,
   or hold it until phase 1 qualifies a release.
