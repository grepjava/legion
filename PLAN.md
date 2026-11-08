# Legion implementation plan

Updated: 8 November 2026. Baseline: v0.1.14. Turns [ROADMAP.MD](ROADMAP.MD)
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
link that goes silent. Left: an agent with only a cut-off manager, and
item 5's statement for exclusive storage.

### Phase 5: Drain barrier

Goal: a tool can ask "is this node safe to stop?" and trust the answer.

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

Exit: invalid or missing required signatures are refused under the policy.

### Phase 8: Joint qualification

Goal: Build, Sign, Push, Deploy, Drain, Migrate and Recover proved end to end
with jailer, recording both versions and their shared crate revisions.
Alongside it, add tested FreeBSD application examples, the Linuxulator and
Compose support matrices, and measured resource-limit behaviour.

### Phase 9: Measure

Benchmark reconciliation, watches, endpoint propagation, pf updates, image
operations and mesh growth, publishing sizes and hardware. Optimize what
measurement shows. Affinity, external load balancers, manager promotion,
bhyve and NAT options wait for a concrete user need.

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
