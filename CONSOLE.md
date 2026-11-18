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

**Since v0.1.19 (released in v0.2.0):** sign-in over HTTP/2, as browsers speak it (the origin
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
Tested over HTTPS (`crates/legion-manager/tests/console.rs`). Restart
and run now came after (2026-11-15), each as the CLI does it (`legion
restart`, `legion cronjob run`; `actions::restart`, `actions::run_now`): a
restart gives the pod a new `restartedAt`, a new pod that the update
config rolls out and a rollback undoes; a run now makes the Job the
schedule would make this minute, created only (an apply over expected
version 0), so a second within the minute is refused and a run due that
minute counts it as its own. The lab test restarts hello from its page
and sees every task replaced on the same image (passed on c41302a).
Events per object came last (2026-11-15; chosen: kept in memory, not
written): each manager works them out from the changes its replica sees,
an object's summary (a task's state, message, node, restarts and
readiness; a Service's generation and rollout; a Job's or Build's state; a
Node's state and availability; a Volume's node and loss) compared with the
one before, and keeps an hour of them, at most 1000 a namespace
(`legion_api::event`, `legion_manager::events`). `Cluster.Events` gives a
namespace's or an object's (a Service's or Job's with its tasks'),
followed with `follow`; `legion events [kind/name] [-w]`; an Events tab on
those kinds' pages, followed live. A manager knows only what happened
since it started, and what was there when it first looked is no event. The
lab test opens hello's Events tab after its restart and sees the change,
its new tasks made and running (passed on 07e7fe7).
Its browser test passes on the lab (see below).

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
`may_reach`, `may_send`, moved there from the agent). Which node holds each
load balancer address came after (2026-11-15): nodes report their CARP
states in their heartbeats, the leader keeps them, and the Network page
lists each address with the node holding it and the state of each that
may (`Cluster.LoadBalancers`, `legion lb`), read again every 10 s; the lab
load balancer test checks `legion lb` against the nodes' interfaces before
and after a drain moves the address (passed on 6927113).

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
- Builds: images built with `legion local` from a Git repository or a
  Containerfile, pushed to the cluster's registry, then rolled out;
  build logs live. (`legion local` builds, on builder nodes, as the agent
  tells it; the console builds nothing itself.)

**Done when** a user who has never used the CLI deploys an example from
the catalog, changes its image through a build and sees the
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
(`/topology`): a namespace's Services, amber or red when not all of
their tasks are ready, the Routes to them, what may reach what among them
as `legion reach` gives it, and the Volumes they use, followed live, each
box leading to its object. The lab test deploys from the form and from a
saved template with its variable set, and sees both in the topology
(passed on 5b762be). Since 2026-11-17 its arcs show either what may
connect or the connections the nodes saw (`Cluster.Connections`, `legion
connections`, asked every 10 s): each ready node's agent lists the states
its pf keeps with a pod's address at an end, open or closed in about the
last minute and a half, and the leader names the ends from the tasks and
nodes it knows (a service or Job of the namespace, another namespace, a
node, or outside), counts each connection once though several states and
nodes saw it, and says how it was sent (to a service's address, a
published port, or the task's own). An arc grows wider with more
connections; the panel lists them by port with their counts, and the
TUI's details of a service list them too. A node that cannot say is named
with why; its connections are missing.

**Builds** (design; chosen 2026-11-14: on builder nodes, no pod may make
jails). The builder (`legion local`, once the separate tool jailer) is a
command with no API; a node the operator chose runs it on the host, as its
agent is told, the way a Volume's data is sent.

- *The object.* A `Build` in a namespace: a source (a Git repository, a
  ref and a directory in it; or a Containerfile written out, with nothing
  else), the image to push (`registry/repo:tag`), the Secret holding the
  registry's login (a Docker config, as `imagePullSecrets`), and,
  optionally, the Service and container to roll out once pushed, and a
  Secret holding a signing key (`signingKeySecret`): the image is pushed
  signed, with SLSA provenance and an SPDX SBOM attested (`legion local push
  --sign-key`, `legion local attest`), for an ImagePolicy requiring them. Its
  status: `Pending`, `Running` on a node, `Succeeded` with the pushed
  digest and the reference pinned to it, or `Failed` saying why; the last
  of its log is kept with it. A Build runs once; another build is another
  Build. Who may write Services in a namespace may write Builds there.
- *Where it runs.* Nodes labelled `legion.builder=true`, ready, one build
  at a time each; a Build waits while none is free. The node needs the
  builder (the agent's `--builder`, by default its own binary as `legion
  local`, so nothing else to install) and, for Git sources, git. Its `RUN`
  steps run in build jails (`legion_build_*`) on that host,
  as a workload's would in Legion's: who may run workloads in a namespace
  may build there.
- *How.* The leader offers the node's agent a session with a `build`
  action, as it does a Volume's export: the agent fetches the source
  (`git clone --depth 1`), writes the login where the builder reads one
  (`REGISTRY_AUTH_FILE`), runs `legion local build` and `legion local push` with a
  release record, and sends their output as it comes, then what it
  pushed. The leader keeps the log and sets the status; with a rollout
  named, it sets that container's image to the pinned reference, and the
  rollout is the Service's own. The session ending early, the node lost or
  the leader changing fail the Build, saying so.
- *Its log, live.* `Cluster.BuildLogs` (to the leader), `legion build logs
  -f`, and the console's Build page, following it as it is written.
- *In the console.* Builds are listed as any kind; a Service's page offers
  "Build a new image" (a Git repository or a Containerfile, the image to
  push, the login), which makes a Build rolling the Service out, and opens
  its log.
- *Done when*, on the lab: a user who has never used the CLI deploys an
  example from the catalog, builds a new image for it with `legion local` from the
  console, and sees the build's log and the rollout.

**Builds built** as designed: the `Build` kind, the leader's builds
controller, the agent's `--builder` (and `--builder-arg`; `--jailer` and
`--jailer-arg` before jailer was merged into Legion), `legion
build-logs [-f]`, the console's Builds, a Build's Log tab (followed while
it runs, saying how it ended) and a Service's "Build a new image". The lab
found three things on the way: the test labelling its node before it had
joined, the Log tab never saying the build had ended, and the test asking
the published port from the host itself, which pf does not redirect. After
each build, pushed or not, the agent runs `legion local rmi` on its image, as
the registry has it: its base images and the build cache stay for the
next build (the cache is bounded by `legion build --cache-max`, and `rmi`
keeps what the cache uses). The lab builds test checks the image gone and
its base kept (passed on e79a058).

**Done when: met** (`the_console_builds_an_example_with_jailer`, passed on
66b8f8f; now `the_console_builds_an_example`, with no jailer: the builder is
`legion local`). One node, labelled a builder, with jailer and a registry the test
runs. ed, an `edit` user in shop who signs in with a certificate and never
uses the CLI, deploys static-site from the catalog with an image not yet
built, then from its Service's page builds one from its Containerfile
pasted in. The Build's log shows jailer pulling FreeBSD's base, running the
`RUN` step and pushing, then says it succeeded; the Service rolls out to
the reference pinned to the pushed digest, both tasks ready, green in the
topology, and the site answers on its published port.

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

The console's own pages name the listed consoles in their
`Content-Security-Policy` (`connect-src`), so the Clusters page may ask
them; no other address.

**Plugins** (built; tested over the gateway,
`the_console_serves_plugins_fenced_in`). A plugin is a page for something
Legion does not
know about, shown inside the console: a Config holding one self-contained
HTML page (scripts and styles inline; 500 KiB, a Config's limit), labelled
`legion.console-plugin: page`. Its name is the menu's.

- *Where it shows.* The side bar lists the plugins in the namespaces the
  user may read; each opens at `/plugins/<namespace>/<name>`. Who may add
  one is who may write Configs in that namespace; nothing new to grant.
- *Fenced in.* The page is served by the gateway (`/api/plugin/…`, read as
  the user) with a policy of its own: `sandbox allow-scripts`, so it runs
  in an origin of its own, with no cookie, no session and none of the
  console's storage; `connect-src 'none'`, so its scripts reach no
  address; framed only by the console, in an `iframe` sandboxed the same
  way.
- *What it may read.* It asks the console in messages (`postMessage`):
  list a kind, get an object, who the user is. The console answers as the
  user, for the plugin's own namespace alone, and reads only: no writes,
  no cluster-wide kinds. A plugin is written by someone who may write in
  that namespace, so it learns nothing they could not read already, from
  any user who opens it; it acts for no one.
- *The messages.* The console first sends `{legion: 1, op: "hello",
  namespace, language}`. The plugin asks `{legion: 1, id, op: "list",
  kind}`, `{…, op: "get", kind, name}` or `{…, op: "me"}`, and the answer
  is `{legion: 1, id, ok: true, result}` or `{…, ok: false, error}`.
- *An example*, `examples/console-plugin.yaml`: a page of the namespace's
  Services and their ready tasks.

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
- *Checked by axe.* The lab's browser test runs axe-core (pinned as a
  devDependency) on sixteen pages against WCAG 2.2 A and AA; its first run
  found links in text told apart by colour alone, now underlined. It
  passed on 4373b5f.
- *Screen readers* (2026-11-15). Each chart's samples as a table behind a
  disclosure, scrolled in a focusable region; changes said as they happen
  where they were only shown: the alert count (a polite live region in the
  side bar) and a rollout's state; the network matrix's row headers marked
  as such. The lab test reads Chromium's accessibility tree, what screen
  readers are given, on every page axe checks and a Service's usage and
  events: every control named, one level-1 heading, the main landmark.
  Passed on d988dd3. *Still to do:* a pass with a real screen reader
  (NVDA, VoiceOver), which the lab cannot run.

**In the lab** (`the_console_runs_the_cluster`, phase 6, passed on
4373b5f): the admin names the cluster and sets a banner, which the
editor's pages show, titled by page and cluster; the keyboard reaches the
skip link first and moves along an object's tabs; a plugin in team-a is
listed, sandboxed, and reads team-a's Services through the console as the
editor; axe on every page. The settings form taking input before it had
read the settings, which then replaced it, was found there.

**Done when: met** (`the_console_spans_two_clusters_by_single_sign_on`,
passed on d38c10d). Two clusters of one node each, east and west, each its
own console, known to the browser by name on one site; an identity
provider the test plays as a company's would be to a browser (a sign-in
page with a session of its own, the code flow with PKCE, ES256 ID tokens),
with a client for each console; the group shop-team bound to edit in east
and view in west. alice signs in to east through the provider, typing her
name there; east's Clusters page shows west not signed into; she signs in
to west with one click, the provider remembering her; then each console's
Clusters page shows both clusters with her access in each. The provider is
the test's own, not a product (Dex, Keycloak), which would be one more
package on the lab's image.

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

## After the evaluation

A review of the console and `legion top` (2026-11-15) found both could
show data that had stopped moving as if it were current, and `top`
behind the console. Fixed:

- *Live views say when they are not current.* The console's followed
  streams (the watch of a kind, alerts, events) share one helper that
  reconnects with a growing wait (1 s to 30 s), stops once the session is
  gone (sending the user to sign in), and while any is broken a banner
  says the page is not current. Before, a dropped stream left the last
  list showing, error frames were ignored, and a failed listing was never
  tried again. `legion top` says "not current" in its header when the
  cluster's status or use can't be read, not only when its watch breaks.
- *No inline script runs but the page's own.* The page policy allows
  scripts from the console and, inline, only those in the built
  `index.html`, by their SHA-256 (worked out once from the embedded page),
  in place of `'unsafe-inline'`. The lab test slips a script into a page
  and sees it not run.
- *`legion top` caught up:* `R` restarts a service (asked), `n` runs a
  cron job now, `e` follows the selected row's events in the pane, `A`
  the alerts; Builds are listed with the workloads and ImagePolicies with
  the config.
- *`legion top` level with the console* (2026-11-18): `:` runs any
  `legion` command with the connection `top` has, its output in the pane
  (Up recalls; what needs the terminal or acts on a host, `exec`,
  `port-forward`, `daemon` and such, is refused with where to do it). Keys
  stand for the console's pages: `H` diagnoses every node, `W` and `C`
  show the namespace's reach and the connections seen, `T` its topology
  (`legion topology`, new: each service with its ports, Routes, tasks,
  Volumes, what may reach it and what was seen), `U` the audit log of a
  day, `K` what tasks use; on a row, `E` edits the object in `$EDITOR`
  and applies it over the version read (as the console's editor does; a
  failed apply keeps the edit in its file), `I` a node's images, `N` and
  `B` a volume's snapshots and its backup to a file, `f` a task's files,
  `l` a build's log. The deploy form, compose import and templates stay
  the console's: in a terminal they are a manifest and `legion apply`.
- *Restoring a backup:* the Operations page gives the steps beside the
  backup's download. A restore replaces a stopped manager's store on its
  host, so the console, served by a manager that is up and out of reach
  when the cluster is down, can't do it itself.

## What a desktop container tool offers

Set against what Docker Desktop gives one machine, the console lacked a
handful of things a cluster wants too (2026-11-15). Each is in the API and
the CLI first, as everything is:

- *A task's files* (`legion files`, `legion cp`, a task's Files tab):
  listed, downloaded and uploaded in its container's root, its Volumes'
  mounts included. It takes what a shell in the task takes. The agent,
  root on the host, opens each component of a path from the root with
  `O_NOFOLLOW`, so a link the container's processes put on the way is
  refused, never followed out; an upload goes to a new file renamed into
  place, whole or not at all.
- *Image details*: an image's build history (each step and the layer it
  added), how it runs (entrypoint, user, ports, labels), and which
  services, Jobs and cron jobs name it, of those the caller may read;
  from the registry, or from a node's cached copy (`legion image inspect
  --node`) for what no registry serves.
- *Vulnerabilities* (`legion image scan`, Scan on a cached image): the
  image's FreeBSD packages, read from its files by `pkg query`, checked by
  the node's own `pkg audit` against FreeBSD's VuXML (fetched again when
  over an hour old), with every package listed as its bill of materials.
  An image without a package database (a Linux one) has nothing to audit,
  and says so.
- *Opening a published port*: a service's page links its ingress ports
  through the node the console is served from (the routing mesh answers
  on every node), its host ports on the nodes running it, its load
  balancer's address and its Routes.
- *Stopping a service* (`legion stop`, `legion start`, Stop on its page,
  `o` in `legion top`): `stopped` in its spec, so its tasks go and its
  replica count stays for the start. A paused node keeps no global task
  of a stopped service.
- *Stacks* (the Stacks page, `legion stack ls|stop|start`): what a compose
  file became, by its `legion.stack` label, shown, stopped, started and
  removed together; removing one keeps its Volumes, as `legion stack rm`
  does.
- *Disk use* (the Disk usage page): each node's space under Legion's ZFS
  root by what holds it (images, tasks' roots, Volumes, the rest) and
  the builder's store (`legion local`'s) beside it, from the agents'
  heartbeats, with unused images and the build cache (`legion local
  prune`, `legion image prune
  --builds`) reclaimed from there.
- *Diagnostics* (`legion diagnose`, the Diagnostics page): `legion
  doctor`'s checks run by every node's agent, and a support bundle
  (`legion support-bundle`): the cluster's objects without its Secrets,
  environment values and what else could be secret masked, its alerts,
  the last hour's events and the diagnosis. Read it before sending it.
- *Host readiness*: the Nodes list's Host column and each Node's page
  say what its host lacks, from the checks its agent runs every minute;
  one it requires keeps new tasks off the node (`legion node require`
  adds advised ones), as `legion list nodes` and `legion top` show.

- *Pausing a service* (`legion pause`, `legion resume`, Pause on its
  page, `P` in `legion top`): `paused` in its spec, which the controller
  copies to its tasks on nodes whose agent can pause (protocol 3). The
  agent stops every process of each container's jail, or a VM's `bhyve`
  (its virtual CPUs), holds the probes and reports the task not ready,
  so it leaves the endpoints; no container restarts meanwhile. Resumed,
  the probes start over. A stop lets a paused container go first, so it
  gets its stop signal and grace; an agent restarted meanwhile pauses it
  again rather than probe it.

## Getting around

Added 2026-11-16, in the console's own look:

- *Pinned pages*: Pin in the top bar puts the page shown (with its
  filter or tab) at the head of the side bar. Pins are dragged into the
  order wanted, or moved with Alt and an arrow key, which says where the
  page went; × unpins. The palette (Ctrl+K) lists them first, then the
  pages visited last, and matches every word typed.
- *Keys*: `?` lists them. `/` goes to the page's filter or search, `[`
  hides or shows the side bar, and `g` then a letter goes to a page
  (`g b` the Builder, `g s` Stacks, `g d` Deploy, `g n` Network, `g c`
  Operations for an admin, besides the earlier ones).
- *The side bar* remembers which sections were closed and whether it was
  hidden. On a narrow window it is put away, opened by ☰ and closed again
  on going to a page.
- *Dropping files*: a backup, an image archive or a file to upload goes
  on its field; a Containerfile, compose file, YAML or environment file
  on its editor. A compose file dropped anywhere else opens in Import
  compose, other YAML or JSON in Apply YAML; anything else is refused
  with a word, not opened by the browser in the console's place.
- *Workbench tabs* are dragged, or moved with Alt and an arrow, into the
  order wanted.
- *Lists* sort by any column, say they are loading until the watch has
  answered (they said "None." until then), and say when the filter, not
  the cluster, left nothing.
- *The topology* pans by dragging its canvas and zooms with Ctrl and
  the wheel. Its pods can be grouped by node: a band per node, saying its
  role and state (red when down), each service's lines fanning out to the
  nodes running its tasks, and a band leading to its node. Grouped by
  service, each pod and Volume carries its node's colour, keyed above the
  canvas. A line from a pod to a ZFS Volume whose data is on another node
  is drawn red and dotted. Published ports join the Routes on the left
  (`ingress`: on every node; `host`: on the nodes running it), and "may
  reach every other" stands for the arcs whenever every service may reach
  every other, two of them included.
- *A service's circle shows what it runs*: the product's logo (nginx,
  PostgreSQL, Node, Redis and some thirty others, from Simple Icons,
  CC0) in its brand colour, darkened or lightened to stand 4.5:1 in either
  theme, matched on its name and image; a VM shows a monitor, an unknown
  database a cylinder, anything else a jail's cube. Apps on Node (Express,
  Koa, Fastify, Nest, Next and the like) show Node's logo, called Node.
- *A new page starts at the top*, and Back returns to where the page
  was left: the page scrolls in its own column, which SvelteKit's reset
  did not reach. Pages cross-fade briefly unless the system asks for
  less motion.

## Phases 7 to 12: what an operator watches

Planned 2026-11-19, after a look at Sylve's host panel
(docs/COMPARISON.md): the state a small cluster's operator needs before
something breaks, and word of it when nobody has the console open. Each
phase is API first, then the CLI, `legion top` and the console in its own
look, then a lab test for its "done when".

### Phase 7: replication in view

- A Volume's replica says its state (`ready`, `syncing`, `stale`,
  `failed`), the age of its last copy and the bytes of the last copy and
  the one under way; each copy and each failover is an event, the
  Volume's page listing them as its copies.
- A `replica-stale` alert once the last copy is older than three
  intervals.
- Whether it can fail over now, and if not why: a pod using it that is
  not `onPartition: stop`, no copy yet, the replica's node not ready.
- The deploy form's claim takes a replica and its failover, warning of
  `automatic` without `onPartition: stop`; promoting by hand is an API
  action, the console asking for the Volume's name typed and saying how
  long ago the copy it carries on from was taken.

**Done when** the replica's node stopping raises `replica-stale` with the
way to the Volume, and the cut-off test sees "can fail over" before the
cut, on the lab.

**Built so far** (2026-11-19): the replica's `state`, `copyingMs`, its
last ten `copies` (when, how long, bytes, whole or not, why it failed)
and `blocked`, written by the replicator as they change; the
`replica-stale` alert; events as the replica's state changes
(`ReplicaStale`, `ReplicaFailed`, `ReplicaReady`) and as a Volume fails
over (`FailedOver`). The console's Volume page has a Replica tab (its
state, where it is kept, the last copy's age against when it goes stale,
what was last sent, whether it would fail over now or what blocks it, its
copies, and Promote while its node is down); the Volumes list and `legion
top`'s storage tab say stale, copying and failover blocked; the deploy
form keeps a replica of the Volume it makes, failing over by hand or on
its own (then with `onPartition: stop`). `Volumes.Promote` and `legion
volume promote` hand over by hand. The lab tests check the stale alert
(the host-lost test) and "can fail over" before the cut (the cut-off
test); their records are to come.

### Phase 8: pools and hosts

- Agents report each pool's state, read, write and checksum errors, last
  scrub or resilver and how full it is; each Volume's use; the host's
  FreeBSD release, kernel, uptime, load and boot method.
- Alerts: `pool-degraded`, `pool-errors` (the counts seen can be taken as
  the baseline), `scrub-overdue`, `pool-nearly-full` at 80% and 90%,
  `volume-nearly-full`, and `version-skew` while nodes run different
  builds. A scrub schedule is a field of the Node, run by its agent.
- A node's page shows its host and pools; the overview a health card:
  quorum in words ("3 managers, quorum 2, can lose 1"), nodes up and
  down, and the cluster's CPU, memory and disk.

**Done when** one side of a mirrored pool taken offline raises
`pool-degraded` within 30 s, leading to the node.

### Phase 9: alerts sent

- An `AlertReceiver` kind: a webhook, ntfy or SMTP, its credentials in a
  Secret, chosen by severity, namespace and alert kind.
- The leader sends each alert once as it is raised and once as it
  clears, across a change of leader (what was sent is in the store),
  trying again with a growing wait; a receiver says when it last
  delivered and its last error.
- `legion alerts test RECEIVER` and the console's Send test.

**Done when** a node going down reaches a webhook within 60 s, once,
with the leader killed in between.

### Phase 10: long operations and activity

- Moves, drains, restores and a replica's first copy say their phase
  (preparing, full copy, final copy, switch-over, cleanup) and bytes
  done; cancelling is refused by the API once the switch-over has begun.
- An activity panel at the console's foot, from the audit log and the
  events: what was done, by whom, when it began and ended, how it ended,
  the full output with Copy; `legion top` lists the same.

**Done when** the drain test shows each phase in turn, and a cancel past
the switch-over is refused.

### Phase 11: checks before disruption

- A dry run of a drain, a Volume's move, a node's removal and deleting a
  service with Volumes: what could not be placed, what Volumes would move
  and their sizes, what `maxUnavailable` holds back, as blocking or
  warning. `--dry-run` on the CLI; the console's confirmation shows it
  and does not confirm while anything blocks.

**Done when** a drain that would strand a Volume is refused in the
dialog and from the CLI alike.

### Phase 12: history and the rest

- What tasks used kept 30 days (a sample a minute for a day, then one
  every 15 minutes) in a file on each manager, outside Raft, so a new
  leader keeps it; charts with ranges, a strip to drag one, PNG and CSV.
- Notes: a Config labelled `legion.note`, shown as Markdown on what it
  names.
- "Tag moved since the rollout" on services not pinned to a digest.

**Done when** a week's history outlives a change of leader in the
console test.

### Alongside: the console's feel

From going through Sylve's live sandbox (2026-11-19): what makes a
console feel finished is a frame that never moves, colour kept for
state, an icon on everything, actions in one place, and an answer to
every click. Taken in Legion's own look (Midnight Indigo and crimson,
meters rather than rings, tabs rather than a menu column), each built
where the phases above first need it:

1. *Icons*: a small line set of Legion's own, drawn for its kinds
   (namespace, service, task as a jail or a VM, node, volume, job, route
   and the rest) and its controls, in the side bar, lists, headers, the
   palette and the workbench; muted, the text colour on hover, never
   crimson.
2. *An object strip* on every object's page: its icon with a state dot,
   name, namespace and what runs it on the left, its usual actions as one
   group and the rest in a menu on the right, the tabs under it, so
   changing tab never moves the actions.
3. *A confirmation dialog of the console's own* in place of the browser's
   `confirm()`: what will happen, a button naming the action, the name
   typed for what can't be undone, and room for a dry run's answer
   (phase 11).
4. *An answer to every click*: a button busy while its call runs, a
   toast for what was done (an error one stays until closed), skeleton
   rows in place of "Loading…", and an empty list saying what to do.
5. *The overview as a health page*: one card for the cluster's health in
   words, then tiles with a label, a value, a line on what it means and
   what to do, a meter; when it was sampled beside the title.
6. *A resource tree* as a mode of the side bar: by namespace (service,
   its tasks and Volumes) or by node (its tasks and Volumes), each with
   its icon and state dot, fed by the watches the console holds, filtered
   by the side bar's filter, arrow keys and a context menu on each.
7. *Tables*: one toolbar (filter, count, what the selected rows can do),
   sort marks, compact rows, a row opening on what it holds (a task's
   events, a Volume's snapshots).
8. *Charts*: the card's header with the range, CSV and PNG; a line under
   it saying range and resolution; two series on one chart where they
   belong together (received solid, sent dashed).
9. *Forms*: names and namespaces filled in, a reset in the title bar, a
   draft kept when the form is left (a dot on its entry).
10. *The workbench as a thin bar always there*: the last thing done and
    how it ended, what runs, opening into the activity of phase 10.
11. *A lighter top bar*: theme and language in the user's menu, quorum
    as a dot and a word, leaving search, alerts and pins.
12. *Motion*: the side bar's sections opening and closing smoothly,
    nothing moving when the system asks for less.

Not taken: a near-black palette with one accent, ring gauges, a pager on
live lists, a search hidden behind an icon, menus that close one section
on opening another, a column of menus per object.

Not taken, by design: host networking, switches, DHCP and firewall rules
in order, a root terminal or file explorer on the host, partitioning
disks and making pools, host package updates, Samba and iSCSI shares,
and "force" buttons (see Out of scope).

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
