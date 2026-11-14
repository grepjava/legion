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

Decide the two things that are hard to change later, in DESIGN.md (§3.9, written):

1. **Sign-in.** Users today are certificates (`legion user create`).
   Browsers handle client certificates badly, so the console signs users
   in and keeps a session:
   - A user signs in with a one-time code from `legion console login`,
     which the CLI asks for with the user's certificate and prints as a
     URL. No private key leaves the user's machine. The gateway then
     calls the API as that user, over mutual TLS, with a short-lived
     certificate it issues for their identity and keeps in memory, so
     RBAC, revocation and audit apply as for the CLI.
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

**Built so far (v0.1.19):** sign-in (`legion console login`, `Cluster.Me`,
`ConsoleCode`/`ConsoleSession`/`ConsoleEnd`); the gateway (`--console-listen`)
with list, get, watch and status as the user; pages for the overview, a
live list of every kind with columns for nodes, tasks, services, volumes,
routes and bindings, and each object's spec and status (JSON for now).
Tested over HTTPS against a manager (`crates/legion-manager/tests/console.rs`).

**Since v0.1.19:** sign-in over HTTP/2, as browsers speak it (the origin
check took the host from a `Host` header HTTP/2 doesn't send; the test
now signs in over HTTP/2); task logs (`/api/logs`, a WebSocket of the
`Pods.Session` the CLI's `logs` opens, tail and follow); what tasks use
now (`/api/usage`); label filters in every list (`app=web`, `app!=web`,
`!canary`, beside words for names); a YAML view of any object; a node's
page with its capacity, use and tasks; a task's page with its logs and
links to its service and node. Its browser test passes on the lab (see
below).

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

**Built so far:** writes carry a per-session token (`x-legion-csrf`, from
`/api/me`; a WebSocket sends it in its query) besides the origin check.
Apply YAML pasted or edited from the stored manifest (`/api/manifest`),
an edit applied only over the version read; "Show changes" is `legion
diff` on the server (a dry run, a unified diff of the manifests). Delete
with confirmation naming what goes with it. Services: scale, a new image
per container over the version shown, the rollout's state and counts,
roll back. CronJobs: suspend and resume. A task's terminal (xterm.js,
loaded only when opened) over `Pods.Session`. Labels added and removed on
any object. Buttons disabled with the reason for a user who may only
look; the API decides in the end, and audits each write as the user. The
CLI and the console share the code for these (`legion_manager::actions`:
`set_spec`, `set_labels`, `rollback`, `create_user`; `legion_api::manifest`).
Tested over HTTPS (`crates/legion-manager/tests/console.rs`). Not yet:
restart and run-now, which need the API first (an applied spec that is
unchanged makes no new generation; there is no call to start a CronJob's
run), events per object (there is no event kind; a task's status message
says why it waits). Its browser test passes on the lab (see below).

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

**Built so far:** nodes drained, paused and activated, promoted and
demoted, removed, labelled; a Volume moved to a node, its backup
downloaded, a backup uploaded onto a node, a lost Volume restored in
place; an Operations page (the CA and each holder's, rotation started
and followed; join tokens shown and rotated; autolock; the control
state's backup downloaded; users' credentials made and offered once as a
file); the audit log with `legion audit`'s filters and a time window (the
API leaves out the newest when more match than the limit); a node's
taints added and removed over the version shown; a Service's load
balancer address and vhid in its list; NetworkPolicies and image policies
summarised in theirs; the Network page, a namespace's matrix of what
reaches what and on which ports, from `Cluster.Reach` (`legion reach`),
which evaluates the same rules the nodes' guards do (`legion_api::policy`:
`may_reach`, `may_send`, moved there from the agent). Not yet: which node
holds each load balancer address (no node reports it; the lab test asks
the nodes' interfaces).

**Lab test, phases 1 to 3** (`crates/legion/tests/console.rs`, in every
record from lab base 5, which has Chromium): a headless Chromium, driven
over the DevTools protocol, signs in on three nodes as a viewer, an editor
and the admin with the link `legion console login` prints, and does each
phase's "done when" through the pages: the viewer offered only their
namespace, a change made with the CLI shown within a second (about 0.1 s
on the lab), every button disabled for them saying why; the editor
deploying from pasted YAML after its changes, scaling, rolling out an image
and back, reading a task's logs and typing in its terminal, each in `legion
audit`; the admin draining a node and making it active again, downloading a
Volume's backup, removing a lost node and restoring its Volume onto another
from that file, filtering the audit log, and rotating the CA until every
holder is on it. First passed on 14133c5.

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

**Built so far:** alerts (`legion_api::alert`, `Cluster.WatchAlerts`,
`legion alerts -w`): a task restarted 3 times in 5 minutes, a node suspect
or down, a Volume lost with its node, the CA within 30 days of expiry, no
leader or a leader cut off from a majority. Any manager works them out from
its own replica and the restarts it has seen, so a manager cut off from the
leader still answers; a user gets the alerts of the namespaces they may
read. The console follows them while signed in, counts them in the side
bar, lists them with a link to each cause and shows the first on the
overview. What tasks used over the last hour (`Cluster.UsageHistory`,
`legion usage`), a sample every 15 s kept by the leader, charted per task,
service and node with what the tasks request and their limits; the charts
fetch the history again as each sample is due, the console's one poll, as
the history is no store object to watch. Namespaces' quotas and what each
uses now (`Cluster.Quotas`, `legion quota`), worked out by the same code as
the metrics, as bars with their numbers on the overview and a Namespace's
page. A Service's or Job's logs searched across its tasks for a time
range (`legion logs-search`, a Logs tab): agents keep each line's time
(their saved output and jails' logs carry it), a logs session takes a range
and gives each line with its time, and the manager's `logsearch` opens one
per task and container, 8 at once, and merges what holds the text in time
order, the newest 1000 by default; a node whose agent keeps no times is
named as not searched (its lab step passed on 5b762be). The lab test (`the_console_runs_the_cluster`) checks
the phase's "done when": on f7efd71 a crash loop was shown as its third
restart was seen and a node's alert 4.2 s after its daemon stopped, each
leading to the cause. Not yet: CPU,
restarts and network charts beyond what heartbeats carry (agents report
CPU and memory only); alerts for nearly full Volumes, which need agents to
report Volume use.

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

**Built so far:** deploy from an image (`/deploy`): a form (name,
namespace, image, replicas, command, port and whether it is published on
every node, a readiness probe, environment, a Volume mounted, made with a
size or one there is, memory and CPU) writing the Service's YAML alongside
as it is filled, shown as changes and applied as `legion apply` does.
Compose import (`/import`, the gateway's `/api/compose`): a compose file
converted by the same code as `legion stack convert`, nothing written,
shown with what Legion cannot honour and its namespace first when it is
missing; the variables it reads (`legion_api::compose::Stack::variables`,
with their defaults) become fields, and files it names are not read (their
content goes in it). Templates (`/templates`): the examples in `examples/`,
built into the gateway, and compose files saved from the import as Configs
labelled `legion.template`, each opened in the import. Topology
(`/topology`): a namespace's Services, green, amber or red by how many of
their tasks are ready, the Routes to them, what may reach what among them
as `legion reach` gives it, and the Volumes they use, followed live, each
box leading to its object. The lab test deploys from the form and from a
saved template with its variable set, and sees both in the topology
(passed on 5b762be). Not
yet: builds. jailer is a command, with no API for the console to call; a
build needs either jailer serving one, or a Legion Job running `jailer
build` in a jail allowed to make jails, pushing to the cluster's registry.
That choice is jailer's as much as Legion's, so the "done when" waits on it.

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

**Built so far:** the console's settings (`legion_api::console::Settings`,
`Cluster.ConsoleSettings` and `SetConsoleSettings`, `legion console
settings` and `legion console configure -f`): a banner above every page,
the sign-in page included, side-bar links, the namespace a user starts in
if they may read it, and how long a session lasts (5m to 168h, 12h by
default, never past the certificate it was signed in with). Before signing
in the gateway serves the banner alone; the admin sets them on a page of
their own too.

**Single sign-on** (`legion console oidc -f`, built as designed below and
tested over the gateway against a provider the test plays,
`the_console_signs_in_with_an_identity_provider`). The certificate stays
the one way to call the API; OIDC is one more way to start a console
session.

- *Who.* An OIDC user is named `oidc:` and the claim configured
  (`email` by default), so no name from a provider can be a certificate
  user's, which are DNS labels. Their groups are the configured claim's
  (`groups`).
- *Rights stay in the bindings.* A RoleBinding names a `user` or a
  `group`. At sign-in the leader records the user's groups in the reserved
  namespace, until their newest session ends; RBAC gives a user what their
  own bindings give and their groups' do, so deleting a binding still takes
  its rights at once. Groups change at the next sign-in.
- *The flow.* The gateway does the authorization code flow with PKCE,
  state and nonce: `/api/oidc/start` sends the browser to the provider,
  `/api/oidc/callback` exchanges the code, checks the ID token (signature by
  the provider's published keys, RS256 or ES256 with ring; issuer, audience,
  expiry, nonce), and asks the leader for a session for that name and those
  groups (`Cluster.ConsoleOidcSession`, managers only). From there it is a
  session like any other: a certificate per session for its calls.
- *Configured* by the admin (`legion console oidc -f`): issuer, client ID,
  the console's address for the redirect, claims and scopes; the client
  secret kept as a Secret in the reserved namespace, never shown again.
- *Tested* against a provider played by the test: discovery, keys, a token
  endpoint, a group mapped to a role.

**Several clusters** (built as below; tested over two gateways,
`the_consoles_of_listed_clusters_see_who_is_signed_in`). Each cluster keeps
its own console, served by
its own managers; a console never holds another cluster's sessions or
calls another cluster's API for a user. "One console" is these consoles
knowing each other:

- *Named and listed.* The settings gain `name` (this cluster, shown at the
  top of the side bar and in the page title) and `clusters`, the other
  consoles (`name` and an `https://` address). The side bar switches to one
  at the same page, so a list of Services on one opens the list of Services
  on the next.
- *Signed into separately.* Each console has its own session, by its own
  sign-in: a code from that cluster's CLI, or its own single sign-on, whose
  provider remembers the user, so the second sign-in is one click. Rights
  are each cluster's bindings; nothing is carried across.
- *Seen together.* A Clusters page asks each listed console who the user
  is there (`/api/me`, read across origins), showing the role and
  namespaces in each, or that the user is not signed in there. A console
  answers that question from another's page only if its own settings list
  that console back, and answers nothing else across origins.
- *Where the cookie goes.* The session cookie stays `SameSite=Strict`, so
  the Clusters page sees a session only where the consoles are one site
  (`a.console.example` and `b.console.example`); elsewhere it shows a link
  to sign in. Browsers send cookies by host, not port: consoles of two
  clusters on one host would receive each other's, so each needs its own
  host name.

**Accessibility** (a first pass against WCAG 2.2 AA, by reading the code;
not yet checked by a tool or a screen reader):

- *Contrast.* Every text colour is 4.5:1 or more on the page, the panels
  and the side bar, in both themes; amber was 4.1:1 on the side bar and is
  darker. Control borders and the topology's boxes are 3:1 (they were
  1.3:1). The primary button's text is dark on the dark theme's accent,
  where white was 3.1:1.
- *Keyboard.* A focus ring on everything focusable; a link past the side
  bar; an object's tabs one stop, moved between with the arrows, Home and
  End, each controlling its panel; the terminal, which takes every key, is
  left with Shift+Esc (Esc alone still reaches the command).
- *Screen readers.* Each page has its own title (`Services · prod-east ·
  Legion`), announced on navigation; the side bar is a named navigation
  with the page shown marked current; errors are alerts and confirmations
  status messages, read as they appear; the topology is a group of links,
  each naming its health (`Service web: 2/3 ready`), not an image hiding
  them; the list filter has a label.
- *Reflow and targets.* Below 48rem (a phone, or 400% zoom) the side bar
  goes above the page and wide tables scroll on their own; small buttons
  (a label's remove) are 24 by 24 px at least.
- *Still to do.* An automated check (axe) on every page in the lab's
  browser test, a screen reader pass, and the charts' values as a table.

**Translations** (the mechanism; no language but English yet). The console
is written in English and every string a person reads goes through `t()`,
or `tn()` where it counts (`lib/i18n.svelte.ts`). A language is one file,
`lib/locales/<code>.json`, mapping each English string to its translation,
with a form per plural category for counts; it is picked up when the
console is built, and a string it lacks stays English. The browser's
languages choose one, and a menu in the side bar, shown once there is a
second language, changes it for that browser. `npm run messages` writes
every string to `messages.json` for translators; `npm run check` fails if
that file is stale or a string is built where the script cannot read it.
Strings are whole sentences with `{name}` for what changes, so a
translation orders words its own way. Names from the API (kinds, fields,
the states it reports) and what users typed stay as they are.

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
