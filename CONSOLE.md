# Web console plan

A web console for Legion, built in phases. Each phase ships on its own and
leaves the console usable. The end state is a console on a par with
OpenShift's for what Legion does: one place to see and run a cluster,
its workloads, storage, network and access, for operators and for the
teams that deploy to it.

The console never becomes a second API. Every page is a view of the
gRPC API (`legion.v1`: `Objects`, `Cluster`, `Volumes`, `Pods`), and
every action is a call to it, checked by the same RBAC (`RoleBinding`:
`view`, `edit`, `admin` per namespace; admins cluster-wide). What the
CLI cannot do, the console does not do either: a feature reaches the
API first, then the CLI and the console.

## Principles

- **One binary.** The console is served by the daemon (`--console-listen`)
  from assets embedded at build time: a Svelte app, on Svelte's latest
  release. No Node at run time, no separate server, and nothing new for
  the port to package at run time.
- **Off by default.** Nothing listens until a flag says so, as with
  `--metrics-listen` and the ingress.
- **The API is the boundary.** A gateway in the manager turns browser
  requests into the same calls the CLI makes, as the signed-in user.
  Watches, logs and exec go over WebSockets; nothing is polled.
- **Generic first.** The 13 kinds share one list, detail, YAML and
  delete view, driven by the API's own kind list. Hand-built pages are
  added where a generic view is not enough (nodes, tasks, volumes).
- **Read before write.** A phase that adds writes adds their audit
  events, confirmation for destructive actions and lab tests in the same
  change.
- **Tested like the rest.** Gateway tests run with the manager's API
  tests; a browser test per phase runs on the lab against a real cluster
  (`scripts/legion.qualify`, `cluster` profile).

## Phase 0: design notes

Decide the two things that are hard to change later, in DESIGN.md:

1. **Sign-in.** Users today are certificates (`legion user create`).
   Browsers handle client certificates badly, so the console signs users
   in and keeps a session:
   - A user signs in with their existing credentials file (the PEM the
     CLI uses, uploaded once and checked by the gateway) or with a
     one-time code from `legion console login`, which prints a URL for
     the CLI's current identity.
   - The session is an HTTP-only, `SameSite=Strict` cookie naming a
     server-side session in the replicated store, bound to the user's
     identity and expiring with their certificate or after 12 h idle.
   - Revoking a user's RoleBindings ends their console sessions within
     5 s, as it ends their gRPC sessions and watches (phase 7, item 2).
   - CSRF: every write carries a per-session token; the gateway refuses
     cross-origin requests.
   - OIDC comes later (phase 6), mapped onto the same identities.
2. **Gateway.** In the manager, on axum (the hyper/tonic stack the
   manager already has): JSON over HTTP for unary calls, WebSockets for
   `Watch`, logs and `Pods.Session`. It calls the local manager's gRPC
   services in-process as the user, so leader forwarding, consistency
   and RBAC behave as for the CLI.

The frontend is Svelte, kept on its latest release: SvelteKit with its
static adapter, built as a single-page app in TypeScript, the output
embedded in the daemon with `include_bytes!` (Node is needed to build,
never to run). Also decided here: the URL scheme, and the browsers
supported (current Firefox, Chrome, Safari).

**Done when** both notes are in DESIGN.md and reviewed.

## Phase 1: read-only console

The cluster as it is, live.

- Gateway: sign-in, sessions, `List`/`Get`/`Watch` for every kind,
  `Cluster.Status`, task logs (follow and tail).
- Pages: overview (managers, leader, Raft state, node count by state,
  services by health, recent events); generic list and detail for every
  kind, filtered by namespace and label, updating as the store changes;
  YAML view of any object.
- Hand-built: Nodes (platform, state, availability, allocatable and
  used, tasks on it); Tasks (state, node, IP, restarts, message, logs).
- The namespace picker shows only namespaces the user can read.

**Done when** a `view` user signs in, sees only their namespaces, and a
change made with the CLI shows in an open page within a second, on the
lab; and no page offers an action.

## Phase 2: operating workloads

What a team does day to day.

- Apply YAML (edit in place, or paste new), with the server's validation
  errors shown at the field; diff against the stored object before
  applying.
- Services: scale, roll out a new image, watch the rollout's progress,
  restart (a new generation); Jobs and CronJobs: run now, suspend,
  history.
- Delete with confirmation, naming what goes with it (a Service's tasks;
  a Volume that is in use is refused, with the reason the API gives).
- Exec into a task's container: a terminal (xterm.js) over
  `Pods.Session`.
- Events per object: why a task is pending ("no node can run it: …"),
  rejected, restarted.

**Done when** an `edit` user deploys, scales, rolls out and rolls back a
Service, execs into a task and reads its logs, all from the console, on
the lab; every action shows in `legion audit`; a `view` user sees the
buttons disabled with the reason.

## Phase 3: cluster operations

What an admin does.

- Nodes: drain, pause, activate, promote and demote (`legion node
  promote|demote`), remove; labels and taints.
- Storage: Volumes with their node, size, used, migrations in progress,
  lost Volumes and `restore --replace`; backups (download), restore
  (upload).
- Network: NetworkPolicies as a per-namespace matrix of who reaches
  whom; Routes with their hosts, TLS and backends; load balancer
  addresses and which node holds each.
- Access: RoleBindings per namespace, users, `legion user create` (the
  credentials file offered once as a download); image policies and
  their keys.
- Cluster: join tokens (show, rotate), CA rotation (start, the holders'
  progress), autolock, backups of the store, the audit log with filters.

**Done when** an admin can take a node out for maintenance and back,
rotate the CA and watch every node move to it, and restore a lost Volume
from the console, on the lab.

## Phase 4: observability

- Metrics: the manager's and agents' Prometheus metrics, kept for a
  short window in the manager (or read from a Prometheus the user names),
  shown as charts per node, service and task: CPU, memory, restarts,
  network.
- Usage against requests and limits; namespaces' quotas and what is
  left.
- Alerts in the console for what an operator acts on: nodes down or
  suspect, tasks crash-looping, Volumes lost or nearly full, a
  certificate or the CA near expiry, Raft without quorum.
- Log search across a service's tasks for a time range.

**Done when** a crash-looping task and a node going down each raise an
alert in an open console within 30 s, with the way to the cause, on the
lab.

## Phase 5: developer experience

What makes OpenShift's console a place to deploy from.

- Deploy from an image: a form (image, ports, env, volumes, replicas,
  probes) that writes the YAML, shown alongside.
- Topology view: a namespace's services, the routes and policies between
  them and the volumes they use, as a graph, with health on each.
- Templates: a catalog of the examples in `examples/` and of the user's
  own saved manifests, with parameters.
- Compose import: a `docker-compose.yml` turned into Legion objects as
  `docs/COMPOSE.md` describes, shown before applying.
- Builds: images built with jailer from a Git repository or a
  Containerfile, pushed to the cluster's registry, then rolled out;
  build logs live. (With jailer: the console calls jailer's API rather
  than building anything itself.)

**Done when** a user who has never used the CLI deploys an example from
the catalog, changes its image through a jailer build and sees the
rollout, on the lab.

## Phase 6: enterprise

- Single sign-on: OIDC, groups mapped to RoleBindings; certificates stay
  for the CLI and nodes.
- Several clusters in one console, each signed into separately.
- Accessibility to WCAG 2.2 AA, keyboard use throughout; translations.
- Console settings per cluster: banner, links, default namespace,
  session length.
- Plugins: pages added by an external bundle declared in a Config, for
  views of things Legion does not know about.

**Done when** a team signs in with their company's identity provider and
sees two clusters, with the access each group has there.

## Out of scope

- Anything the API does not offer. A console-only feature would be a
  second source of truth.
- Running workloads in the console's own process, or the console
  changing nodes directly: it calls the managers like any client.
- OpenShift features Legion does not have (operators, a service mesh,
  the OperatorHub): they come to the console when they come to Legion.

## Sizes

| Phase | Effort |
|---|---|
| 0 Design notes | days |
| 1 Read-only | 2–3 weeks |
| 2 Operating workloads | 3–4 weeks |
| 3 Cluster operations | 4–6 weeks |
| 4 Observability | 4–6 weeks |
| 5 Developer experience | 6–8 weeks |
| 6 Enterprise | 6–8 weeks |

Phases 1 to 3 make a console operators can use for everything the CLI
does; 4 to 6 make it one teams deploy from.
