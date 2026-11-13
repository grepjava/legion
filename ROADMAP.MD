# Legion development roadmap

Updated: 8 November 2026. Baseline: workspace version 0.1.15.

This roadmap proposes future work based on a source review. It does not
certify runtime behavior or replace the historical implementation tracker
in [PHASE.md](PHASE.md). Priorities are recommendations, not release or
delivery commitments.

## Direction

Make Legion a dependable FreeBSD-native application orchestrator, using
jails, VNET, PF, RCTL and ZFS. Preserve a straightforward single-node
experience while strengthening the existing multi-host implementation.

The closest operating-model comparison is Docker Swarm. Kubernetes-style
extensibility is outside the current direction: a Kubernetes API, custom
resources, operators and admission webhooks are not roadmap requirements.

OCI image interoperability and Linux binary compatibility are separate.
Linuxulator supports selected Linux workloads; Legion does not promise
arbitrary Linux kernel behavior or generic Linux-container equivalence.

## Existing foundation

The following have concrete implementations and should be extended rather
than rebuilt:

- Native jail lifecycle and supervision, including VNET pod jails and child
  container jails (`legion-jail`).
- OCI registry authentication, platform selection, digest checks, image
  import/export/push, ZFS layer storage and garbage collection (`legion-image`).
- Typed declarative objects, admission, reconciliation, scheduling,
  rolling updates, rollback, probes, Jobs and CronJobs.
- OpenRaft/redb control state, manager/worker membership, mTLS, dispatcher
  streams and node failure detection.
- VNET/epair networking, DNS, PF service routing, WireGuard mesh,
  namespace isolation and network policy.
- RCTL resource rules, userspace aggregate-memory enforcement, quotas,
  metrics, logs, exec, port forwarding and the terminal dashboard.
- ZFS volumes, stateful snapshots, volume migration, NFS, iSCSI and
  executable volume plugins.
- RBAC, namespace jail allowances, encrypted-store support, removed-worker
  rejection, control-state backup/restore, ingress and ACME certificates.

Implementation does not imply production qualification. Privileged live
tests exist, but ordinary `cargo test --workspace` skips ignored tests.
Historical Podman references in early phase entries do not describe the
current native runtime.

## Prioritized milestones

### P0.1 — Repeatable FreeBSD runtime qualification

Scope: single-host foundation, followed by multi-host qualification.

Add a dedicated integration lane that explicitly invokes selected ignored
tests on disposable FreeBSD hosts or VMs. Retain the existing fast CI.

Deliverables:

- A documented harness for temporary ZFS pools and network resources.
- Jail lifecycle, PF routing/policy, ZFS storage and daemon crash/adoption
  coverage, followed by cluster failover and stateful migration coverage.
- Archived test output identifying the OS, architecture and Legion version.
- A release qualification checklist distinguishing automated results from
  manually recorded results.

Acceptance:

- Failure and cancellation leave no unexpected jails, mounts, datasets,
  interfaces or PF state owned by the test.
- Existing workloads survive daemon restart and are adopted correctly.
- A release candidate has recorded privileged test results on supported
  platforms; skipped coverage is explicit.

Dependencies: disposable FreeBSD infrastructure with root, ZFS and network
access. Privileged jobs must run trusted code in isolated environments.

Starting points: `.github/workflows/ci.yml`,
`crates/legion-jail/tests/live.rs`, `crates/legion-net/tests/live.rs`,
`crates/legion/tests/cluster.rs`, `crates/legion/tests/stateful.rs`.

### P0.2 — Explicit partition and storage-ownership guarantees

Scope: multi-host safety.

Define what happens when an agent cannot reach managers but can still
reach clients or storage. Failure detection and replacement scheduling do
not prove that the old workload stopped.

Deliverables:

- A documented contract for quorum loss, agent disconnection, node
  removal, reconnect and exclusive-volume reassignment.
- Fault-injection tests for asymmetric partitions and stale assignments.
- A deliberate fail-open or fail-closed policy for each relevant workload
  class. Do not introduce universal lease expiry without considering
  workloads intended to survive control-plane outages.
- A fencing requirement or explicit operator procedure before an
  exclusive storage claim can be reassigned without confirmed release.

Acceptance:

- Tests expose whether replacement and disconnected workloads can run
  concurrently; the documented contract matches observed behavior.
- Exclusive storage is not handed to another writer solely because a
  heartbeat stopped.
- Node removal documents the operator's responsibility to stop or fence
  the old node; credential rejection alone is not described as fencing.
- Reconnection converges without resurrecting obsolete assignments.

Dependencies: P0.1 harness for runtime evidence; planner and protocol tests
can begin independently. Preserve current conservative volume behavior
until a stronger mechanism is demonstrated.

Starting points: `crates/legion-manager/src/controller/plan.rs`,
`crates/legion-manager/src/controller/plan/migrate.rs`,
`crates/legion-manager/src/dispatcher.rs`,
`crates/legion-agent/src/control.rs`.

### P1.1 — Host diagnostics and actionable readiness

Scope: single-host deployment first; reuse results across the cluster.

Deliver a read-only `legion doctor` command before changing scheduling.
Reuse the existing system abstraction and startup checks.

Initial checks:

- RACCT availability and `kern.racct.enable`.
- PF availability and the required main-ruleset anchor hooks.
- IPv4/IPv6 forwarding, reported according to enabled features.
- Configured ZFS-root existence and accessibility.
- Required interfaces and conflicting host configuration where detectable.

Acceptance:

- Text and JSON output identify checks, outcomes and concrete remedies.
- Missing required prerequisites produce a nonzero exit status.
- Checks distinguish failure, warning, unsupported and not applicable.
- The command does not rewrite PF, sysctls, datasets or interfaces.
- Tests cover missing RACCT, missing PF hooks, inaccessible storage and
  command failures without requiring root.

Follow-up: expose persistent readiness conditions and gate affected
scheduling behavior. Do not mark an otherwise healthy node unusable for
an optional feature failure without an explicit policy.

Starting points: `crates/legion/src/cli.rs`,
`crates/legion-agent/src/lib.rs`, `crates/legion-sys/src/lib.rs`,
`crates/legion-net/src/lb.rs`.

### P1.2 — Drain completion barrier for automated maintenance

Scope: a machine-readable completion contract for node evacuation.

Setting a node's availability to `drain` starts asynchronous reconciliation;
successful submission does not establish that the node is safe to stop.
Expose an operation status that distinguishes `Draining`, `Blocked`,
`Completed` and `Failed`, with stable reason codes and progress details.

Deliverables:

- A persisted, inspectable drain operation identity and status, with
  text/JSON output and a CLI wait mode with meaningful exit status.
- Per-workload and per-volume progress, including destination, outstanding
  verification and actionable blockers such as placement constraints,
  capacity, disruption budgets, unavailable agents or transfer failures.
- An explicit completion predicate: source tasks have stopped and released
  required resources; required replacements are ready at eligible
  destinations; moved volumes have completed their final transfer and
  ownership handoff; exclusive storage has confirmed source release.
- Defined exceptions for workloads that need no replacement, such as a
  global instance removed from this node, and explicit handling of pending
  Jobs, ordinary services pinned to local volumes and retained source data.

Acceptance:

- `Completed` requires current, authoritative evidence for every required
  workload and volume; an empty endpoint list or requested shutdown alone
  is insufficient. Unknown source state cannot count as verified release.
- `Blocked` identifies a condition that prevents progress but may resolve;
  `Failed` identifies a terminal operation error. Retryable transfer errors
  and a CLI wait timeout do not silently become terminal drain failures.
- Define how later desired-state changes, node reactivation/cancellation,
  retries and stale reports affect the operation. Persisted terminal status
  describes the operation's verified completion point, not a perpetual
  guarantee that the node remains empty.
- Waiters survive manager failover and client reconnect without losing the
  operation identity or mistaking stale status for completion.
- Tests cover stateless and stateful evacuation, exclusive-volume release,
  blocked placement/budgets, transfer interruption, source disconnection,
  leader change and cancellation.

Dependencies: P0.2 defines the ownership and partition-safety evidence;
P0.1 supplies live qualification. Reuse controller/task/volume status and
version preconditions rather than creating a second evacuation controller.

The barrier verifies workload evacuation. It does not by itself establish
that shutting down a manager preserves Raft quorum, or fence an unreachable
machine; report those maintenance preconditions separately.

Starting points: `crates/legion/src/cli.rs`,
`crates/legion-api/src/node.rs`,
`crates/legion-manager/src/controller/plan.rs`,
`crates/legion-manager/src/controller/plan/migrate.rs`,
`crates/legion-agent/src/volumes.rs`.

### P1.3 — Reproducible image deployment

Scope: both single-host and cluster deployment correctness.

Digest addressing and verification already exist. Strengthen the contract
between a declared image, the deployed content and rollback.

Deliverables:

- Inspect current cache/pull behavior and specify explicit pull policies.
- Record and expose resolved image identities for each deployment.
- Support immutable index identities for mixed-architecture deployments,
  retaining the selected platform-manifest identity per task.
- Make rollback use the intended immutable content rather than silently
  reinterpreting a mutable tag.

Acceptance:

- A tag changed between node pulls cannot silently produce an
  undocumented mixed-content deployment.
- Rollback behavior remains reproducible after a registry tag changes.
- Tests cover registry unavailability, private credentials, local images,
  architecture mismatch and content no longer available locally.
- Resolution preserves namespace credential boundaries and does not leak
  private registry data into public status or logs.

Dependencies: decide where resolution belongs before adding manager-side
registry access. Reuse `legion-image` rather than adding a parallel client.

Starting points: `crates/legion-image/src/registry.rs`,
`crates/legion-image/src/store.rs`, `crates/legion-api/src/task.rs`,
`crates/legion-agent/src/task.rs`.

### P1.4 — Recovery and upgrade qualification

Scope: control state, credentials and workload data as separate concerns.

Deliverables:

- Tested backup/restore procedures covering control state, required keys
  and application volumes independently.
- Replacement-host recovery and supported version-upgrade procedures.
- Explicit migration retry/abort behavior and storage capacity errors.
- Documented recovery point and recovery time objectives, measured by
  drills rather than inferred from feature presence.

Acceptance:

- Restore a representative application onto replacement hosts.
- Interrupt a ZFS migration at each handoff stage and verify safe retry.
- Test disk-full conditions and unsupported version combinations.
- Label snapshots as crash-consistent unless application quiescing or
  another mechanism establishes application consistency.
- Protect backup keys and secret-bearing artifacts; a control-state
  backup is not presented as a backup of volume contents.

Dependencies: P0.1; P0.2 for ownership during recovery.

Starting points: `crates/legion-store/src/backup.rs`,
`crates/legion-agent/src/transfer.rs`, `crates/legion-manager/src/migrate.rs`,
`packaging/freebsd/`.

### P2.1 — Image trust and security lifecycle

Scope: deployment admission and administrative accountability.

Deliverables:

- Image signature/provenance verification policy using established tools
  and formats; distinguish verification from digest integrity checks.
- A defined policy for SBOM/provenance presence and inspection where useful.
- Documented credential revocation coverage for users, workers, managers
  and active sessions, extending existing removed-worker rejection.
- Attributable audit events for administrative mutations and sensitive
  sessions, with retention and secret-redaction rules.

Acceptance:

- Invalid or missing required signatures are rejected under an explicit
  policy, including when verification services are unavailable.
- Revocation tests include follower requests, renewal and active streams.
- Audit output identifies actor, action, target and result without storing
  secret contents or private keys.

Dependencies: P1.3 for stable image identities. Keep verification policy
separate from image building; do not invent cryptographic formats.

### P2.2 — Compatibility and adoption

Scope: predictable application delivery.

Deliverables:

- Tested native FreeBSD application examples and companion jailer workflows.
- A versioned Linuxulator compatibility matrix for real applications.
- A versioned Compose support matrix, including strict-mode behavior.
- Measured resource-limit behavior, including CPU throttling and memory
  overshoot while aggregate enforcement depends on the running agent.
- Clear host firewall requirements and coexistence documentation.

Acceptance:

- Examples deploy, update and recover on supported architectures.
- Unsupported Compose options produce actionable warnings or strict errors.
- Documentation separates OCI format support, native execution and Linux
  ABI compatibility; it does not claim cgroups-equivalent enforcement.

Dependencies: P0.1 qualification infrastructure. Preserve the companion
builder boundary instead of creating another build subsystem in Legion.

### P3 — Measured scaling and optional extensions

Benchmark reconciliation, API/watch behavior, endpoint propagation, PF
updates, image operations and WireGuard full-mesh growth before expanding
the feature set. Publish workload sizes, hardware and latency/resource
budgets; optimize demonstrated bottlenecks.

**Parked: scaling past the measured sizes.** Taken up only when a user
reports pain at scale, as the sizes in `docs/MEASURE.md` cover the small
clusters Legion is for. What is known, so the work starts quickly then:

- **Benchmark first.** A whole cluster at the reported size on the fake
  runtime (20,000 tasks: say 200 nodes of 100), timing a pass with its load,
  Raft writes per second as tasks change, the store's memory and snapshot
  size, endpoints sent to every node, a deploy and a node lost end to end,
  and the console's lists (not paginated). It decides which of the below
  matter, and may add to them.
- **pf: one anchor per VIP block.** Every service's rdr rules sit in one
  flat `legion/svc` anchor, reloaded whole when a service is added (4.2 s
  at 2,000 services). Split by the VIP's /24 (`rdr-anchor "vip-<b>" on
  legion0 inet to 10.201.<b>.0/24`, and its IPv6 twin
  `fd00:10:201::<b>:0/112`), a change reloads only its block: VIPs are
  allocated lowest free first, so at most 254 services, roughly 100 ms by
  the current figures. Costs: a change across blocks is no longer one
  atomic load, the parent's block lines must follow the blocks in use and
  be rebuilt after a crash, moving to it on upgrade leaves a moment without
  rules, tables move with their rules, rules are read across anchors when
  debugging, and `legion-net` is shared with jailer.
- **The planner's per-pass work** (78 ms at 5,000 tasks and 200 nodes,
  the planner alone):
  - `controller/mod.rs` lists and parses every Task, Node and Volume on
    every pass, which no figure covers. A cache kept from the store's watch
    removes it, but is the riskiest change: drifting from the store means
    acting on a false cluster, so it must start its watch at the revision
    it listed and rebuild on compaction and on every new leader, and it
    holds the objects twice.
  - `orphan()` clones every task even when no node is down or removed;
    borrow them when none is.
  - Each service's status is serialized to JSON to find whether it changed;
    a typed comparison must mean the same, fields not serialized and those
    others write included.
  - Tasks on a node are found by scanning every node for each task; index
    the nodes by name once a pass.
  - Pods are compared in full for every task. A cryptographic digest of the
    pod, kept on the task, may stand for "equal"; a different digest must
    fall back to the full comparison, or a new defaulted field in a later
    version rolls every task on upgrade.

Consider the following only with a concrete user requirement:

- Scheduling affinity/anti-affinity, priorities and preemption.
- External load-balancer and additional storage integrations.
- In-place manager promotion/demotion.
- VM workloads through bhyve as a separate optional backend.
- Additional connectivity options for nodes behind different NATs.

These are product-scope decisions, not prerequisites for a useful local
jail manager or small cluster.

## Smallest useful next implementation

Implement the read-only `legion doctor` slice from P1.1: RACCT, PF anchor
hooks, forwarding and configured ZFS-root checks, with text/JSON output
and meaningful exit status. Reuse existing checks and test through fakes.
Leave automated host remediation and scheduler changes for a later change.

P0 qualification and partition-safety work remain the highest-risk
priorities; the diagnostic command is the smallest independently useful
implementation, not a substitute for those milestones.

## Documentation maintenance

- Correct `docs/COMPARISON.md`: ACME is implemented, and certificate
  revocation claims must account for removed-worker rejection.
- Keep historical phase entries clearly historical, particularly the
  former Podman runtime architecture.
- Record evidence for completed milestones, including privileged test
  runs and known limitations. Do not equate source presence with a
  production guarantee.

## Comparison references

These describe comparison targets, not Legion's implemented behavior:

- [Docker Swarm](https://docs.docker.com/engine/swarm/): declarative
  services, discovery, load balancing and rolling updates.
- [Podman Quadlet](https://docs.podman.io/en/latest/markdown/podman-systemd.unit.5.html):
  declarative single-host service integration.
- [Docker rootless mode](https://docs.docker.com/engine/security/rootless/):
  an architectural security comparison, not a promised Legion feature.
- [FreeBSD OCI containers](https://docs.freebsd.org/en/books/handbook/containers/)
  and [Linux compatibility](https://docs.freebsd.org/en/books/handbook/linuxemu/):
  image interoperability and execution limits.
