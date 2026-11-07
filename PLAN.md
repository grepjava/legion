# Legion implementation plan

Updated: 7 November 2026. Baseline: v0.1.13. Turns [ROADMAP.MD](ROADMAP.MD)
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

Exit: a tag moved between two nodes' pulls can't produce an unnoticed mix,
rollback still works after a tag changes, and private registry details stay
out of status and logs. Tag a release so jailer can start its image handoff.

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
4. **Image resolution.** Recommended: the leader resolves a tag once at
   deployment, using the namespace's pull secret, so every node gets one
   digest. The cost is that managers need registry access. Agree an offline
   rule for local images.
5. **Port.** Submit the port once `testport` passes on the current release,
   or hold it until phase 1 qualifies a release.
