# Legion: next enhancements

Written 22 November 2026, after comparing Legion with
[Sylve](docs/COMPARISON.md#sylve) where the two overlap. Each item says
what Legion does today (checked in the code and docs), what is missing,
a proposed shape, and when it is done. The shapes are proposals, to be
settled in [docs/DESIGN.md](docs/DESIGN.md) before building, as every
phase has been.

In order of priority:

1. [Scheduled Volume snapshots and backups](#1-scheduled-volume-snapshots-and-backups): built
2. [Moving a Volume while it runs](#2-moving-a-volume-while-it-runs): checked, already built
3. [Setting up pf for Legion](#3-setting-up-pf-for-legion): built
4. [Console sign-in with host accounts and passkeys](#4-console-sign-in-with-host-accounts-and-passkeys): not now (decided 22 November 2026: the certificate and one-time link suit, as Kubernetes' do)
5. [Ready-to-deploy applications](#5-ready-to-deploy-applications): built
6. [Console polish](#6-console-polish): built

## 1. Scheduled Volume snapshots and backups

**Today.**
- Snapshots are taken by hand (`legion volume snapshot`, `snapshots`,
  `rollback`, `rm-snapshot`), and by a stateful service's update when it
  asks (`update.snapshot: true`, dropped by the next update).
- A backup is `legion volume backup VOLUME -o FILE`, run by hand, one
  Volume at a time, written where the CLI runs.
- A Volume's `replica` keeps one copy on another node, every
  `everySeconds`. That is for failover, not a backup: it keeps only the
  last snapshot, so a deletion or a corruption reaches the replica at the
  next copy.

Nothing takes snapshots on a schedule, keeps a history of them, or sends
backups off the cluster on its own. Sylve does all three (periodic
snapshot jobs with retention; scheduled backup jobs to a remote ZFS host
over SSH, with retention and restore).

**Proposed.**
- *Snapshot schedule* on a Volume (and a stateful claim):
  `snapshots: { every: 1h, keep: 24 }`, or several, for example hourly
  kept for a day and daily kept for a month. The holding node's agent
  takes them (`@legion-auto-<time>`) and prunes those past `keep`.
  `legion volume snapshots`, the console's Volume page and `legion top`
  list them; restoring is the existing `rollback`, or a new Volume cloned
  from one (`legion volume clone VOLUME@SNAPSHOT NAME`), which needs no
  rollback of the original.
- *Backup targets*: a cluster-scoped `BackupTarget` kind. First a remote
  ZFS host over SSH (host, user, dataset, the key in a Secret, its host
  key pinned), then a directory on a node (the existing file format).
  S3-compatible storage later; Sylve has none, so it would go beyond it.
- *Backup schedule* on a Volume: `backup: { target, every, keep }`.
  Incremental `zfs send` from the last snapshot the target holds, whole
  the first time or when the base has gone; pruned on the target to
  `keep`. Each run is an Activity (bytes, time, outcome), as a
  replica's whole copy is now.
- *Restore* from a target: `legion volume restore --from TARGET VOLUME@TIME
  --node NODE`, through the same checks as a restore from a file.
- *Alerts*: a schedule that fails, or a backup older than twice its
  interval, raises an alert (`AlertReceiver`s), as a stale replica does.

**To settle first.**
- Snapshot names the migrator, the replicator and backups each rely on
  must stay distinct, and pruning must never take a snapshot another one
  needs as its incremental base.
- An update's rollback (`zfs rollback -r`) destroys later snapshots,
  scheduled ones too: say so, or roll back by clone and promote.
- Snapshots use the pool's space: count them against the Volume's quota
  or the namespace's, and say which.

**Done when** a Volume with both schedules keeps its snapshots and
backups pruned to `keep` across a manager failover and a node drain, a
backup restores to another node with the data as at that time, and the
lab covers it.

**Built** (22 November 2026), as proposed, settled in DESIGN.md §4.3 (Volume snapshot schedules, Volume backups):
- `snapshots: [{every, keep}]` on a Volume or a stateful claim, taken by the leader's snapshotter as `legion-auto-<every>-<time>`, pruned per schedule. Its status is in the Volume, so a new leader carries on. `snapshot-late` is raised.
- `legion-` names are Legion's: a person's may not start so, and of Legion's own only a schedule's are listed, rolled back to or deleted. `rollback` says which later snapshots it destroys and asks first. `legion volume clone VOLUME@SNAPSHOT NAME` copies a snapshot into a new Volume.
- `BackupTarget` (cluster-scoped): a ZFS host over SSH (key from a Secret, host key pinned, incremental after the first, each whole backup in a dataset of its own there) or a directory on the node (a whole stream file each).
- `backup: {target, every, keep}` runs each as an Activity, prunes the target and keeps only the next base on the node. `legion volume backups` lists, `restore --from` restores onto a node. `backup-late` is raised.
- Console: the Volume's Data tab shows its schedules, its backups and a restore, and copies a snapshot into a new Volume.
- Checked on the Ryzen: snapshots and directory backups pruned to `keep`; restore and clone hold the data as at their time.
- Lab test: `volumes_keep_scheduled_snapshots_and_backups_that_restore`, n3 as the SSH host.
- Left: S3-compatible targets; the failover and drain cases of "done when" are covered by the status carrying over moves and promotions, but not yet run in the lab.

## 2. Moving a Volume while it runs

**Checked, already built; nothing to do.** Sylve's staged migration
copies a jail or VM while it runs, then stops it for the last changes.
Legion moves Volumes the same way, on a drain and with `legion volume
move`: the full copy is sent while the task runs (`copying`), then the
task stops, the last increment goes (`finishing`), and the next task
starts on the new node (DESIGN.md, "Stateful migration";
`a_volume_moves_where_it_is_asked_to_be_with_its_task`).

What Sylve moves that Legion does not: whole VMs. Legion's VM tasks have
no volumes yet (DESIGN.md §6.8), so there is nothing of theirs to move.

## 3. Setting up pf for Legion

**Today.** The host's `/etc/pf.conf` must call Legion's anchors
(`nat-anchor`, `rdr-anchor` and `anchor "legion/*"`) and pass the
cluster's traffic, edited by hand before the daemon starts. `legion
doctor` checks it and changes nothing; until it is right the node takes
no new tasks (`ready, lacks pf.anchors`). It is the first thing a new
user trips on. Sylve writes the whole ruleset itself.

**Proposed.**
- `legion doctor --fix pf` (and an offer from `legion init` and `legion
  join`, asked first): adds the three anchor lines and the pass rules for
  WireGuard and Legion's interfaces to `/etc/pf.conf`, keeps a dated copy
  of the old file, checks the new one with `pfctl -nf` before loading it,
  enables pf (`sysrc pf_enable=YES`) and loads it. Run again, it changes
  nothing.
- When another tool owns `pf.conf` (Sylve's `# --- sylve:` sections,
  others by their markers), it changes nothing and prints the lines to add
  in that tool's own place for custom rules.
- The same for `legion local`'s anchors (`legion-local/*`).
- The port's `pkg-message` points to it.

**Done when** a fresh FreeBSD 15 host goes from `pkg install legion` to a
node that takes tasks without editing a file by hand, and a second run
changes nothing (lab).

**Built** (22 November 2026), as proposed, recorded in DESIGN.md §5.4:
- `legion doctor --fix pf` and `legion local doctor --fix pf`; `legion join` and `legion init` on the manager's host offer it.
- A file Sylve (or another tool) writes is refused with the lines to add. A default-block ruleset gets a warning, never a changed policy.
- The console's Diagnostics page shows each node's plan for review: the file as it would be, the copy kept, the way back, the command to run.
- Lab test `doctor_puts_pf_right_asks_first_and_changes_nothing_twice`.
- Left: the port's `pkg-message`, at its next update (it lives in the ports tree).

## 4. Console sign-in with host accounts and passkeys

**Today.** A user is a certificate (`legion user create`). The console
signs them in with a one-time code from `legion console login`, or
through an identity provider (OIDC, `legion console oidc -f`). Sylve
also signs in the host's own accounts (PAM) and passkeys.

**Proposed.**
- *Host accounts*: `legion console pam -f` turns on a `pam:` realm on
  the managers. A user signs in with their FreeBSD account and is named
  `pam:<user>`, their Unix groups `pam-group:<group>`, which RoleBindings
  name as they name OIDC users and groups. Sessions are the console's
  existing ones. Attempts are rate-limited per user and address and
  audited.
- Accounts differ from host to host: a PAM sign-in is checked on the
  manager that answers. Say so, or recommend a directory (LDAP, AD)
  behind PAM for clusters with several managers.
- *Passkeys* (WebAuthn): a signed-in user registers one or more; they
  then sign in with it alone, kept in the replicated store with their
  identity. Removing a user removes their passkeys.

**Done when** a FreeBSD account and a passkey each sign in to the
console with exactly the access their RoleBindings give, a wrong
password is refused and counted, and the lab covers both.

## 5. Ready-to-deploy applications

**Today.** `examples/` has a native FreeBSD base and recipes for
PostgreSQL, Redis, nginx (static site), Node.js and Python, built into
images by `legion local`. Sylve offers 117 apps from the daemonless
catalog (`catalog.daemonless.io`): FreeBSD OCI images on
`ghcr.io/daemonless/*` with compose-style manifests, among them Immich,
Paperless-ngx and Actual Budget.

**Proposed.**
- *Try the daemonless images under Legion first.* They are FreeBSD OCI
  images, which Legion already pulls and runs as jails. Check a sample
  (a service, a database and a multi-container stack) for what they
  assume: their init, `PUID`/`PGID`, `network_mode: host` (Legion pods
  have their own address), and the variables their manifests fill in.
- *If they run*, read their compose manifests as Legion stacks
  (`legion stack deploy`), filling the variables from flags or a file,
  rather than copying the catalog: fetch it when asked, as Sylve does.
  Check the catalog's and the images' licences before using or bundling
  anything.
- *An Apps page* in the console and `legion app list|show|deploy`:
  browse, fill in the variables (a Volume where a manifest asks for a ZFS
  dataset), deploy as a stack in a namespace.
- *If they do not run*, add recipes of our own for the most asked-for
  applications, starting from the native base.

**Done when** ten widely used applications deploy from a catalog with one
command or a few clicks, and the lab deploys and reaches three of them.

**Built** (22 November 2026), settled in DESIGN.md §4.3 (Ready-to-deploy apps):
- The daemonless images run under Legion as they are. The catalog (BSD-2-Clause) is read when asked, never bundled.
- `legion app list|show|deploy` and the console's Apps page (132 apps, opening on Import compose with the variables filled in).
- A manifest's variables are typed: storage is a Volume of the stack, a secret not set is made up and printed once, an empty default is left to the app.
- Podman's FreeBSD jail annotations become `security.allow`, and a stack's namespace allows what its containers ask (for `stack deploy`, `app deploy` and the console alike).
- Checked on the Ryzen with twelve apps deployed by `legion app deploy` and reached: PostgreSQL, Redis, Uptime Kuma, Homepage, Vaultwarden, Syncthing, Actual Budget, nginx, File Browser, Navidrome, Gitea, Mealie.
- Two Legion bugs found and fixed on the way: pods now have an `/etc/hosts` naming their containers (Homepage), and a manifest's quoted defaults are read as `.env` files read them (Navidrome).
- Lab test: `apps_of_the_catalog_deploy_and_answer` (Vaultwarden, PostgreSQL, Redis).
- Not honoured: `network_mode: host` (Actual Budget answers on its pod's address, not a node port).

## 6. Console polish

**Today.** Compared screen by screen with Sylve's console on
22 November 2026 (Sylve on weft, ours on the Ryzen): ours is stronger at
showing what a cluster does (Topology, the Overview's health, alerts,
rollouts, the command palette, YAML beside every form), Sylve's is the
more finished management UI. Fixed the same day: the namespace, an
object's tab, a list's filter and the Images tab now stay in the address
(`?ns=`, `?tab=`, `?filter=`) across a reload or a shared link; the
Overview says no tasks ran in a namespace instead of drawing empty CPU
and memory charts; sizes no longer break between number and unit; the
"Dock …" buttons say "Keep … open".

**Done** (22 November 2026):
- *One component kit*.
  - Every list is sorted, searched and paged the same way: `lib/table.svelte.ts` with SortTh, TableBar and Pager, on about 20 lists (alerts, audit, events, log search, long operations, tasks, Volumes, snapshots, images, builds, CA holders, virtual IPs, files).
  - Fixed per-node and per-setting tables are left as they are.
  - One button look with hover states, and one segmented control for chart ranges and Topology's toggles.
  - One Tabs component on Deploy, object pages and Images.
  - Figures in the text font, with monospace kept for addresses and names.
  - Card titles are headings.
- *The sidebar as a tree*: Observe, Applications, Create, Storage and images, Network, Configuration and access, then the Cluster's own.
- *Guided Deploy*:
  - tabs (Basic, Network, Environment, Storage, Resources) with Back and Next, and a mark on a tab filled in;
  - empty port fields instead of `0`;
  - the YAML beside it.
- *Dashboards*:
  - Status cards (lib/Tile.svelte) on the Overview (nodes, managers, workloads, CPU, memory, storage, network, each node's meters), on Volumes (pool health, capacity, last scrub, Volumes) and on a node's Host tab.
  - A range picker on every usage chart.
- *Plainer names*: "Volumes" and "Images".

**Left.**
- *Live pool I/O*: built. The agent reads ZFS's per-dataset counters (`kstat.zfs.<pool>.dataset`) as rates. They are charted (Pool I/O) and shown on the Overview's and Volumes' cards, with a history of 30 days.
- *A review before changing a host*: built with item 3 (Diagnostics).
- *Topology's service links*: built. They are named for what they show (its name, its port, how many are ready), and the ready count is no longer said twice (WCAG 2.5.3).

**Second round** (22 November 2026, "ours needs to be more user friendly and modern"; the Midnight Indigo kept, calmer, as chosen):
- *Colour*: softer navy surfaces; indigo for the main button, the current page and tab, the focus ring and meters; crimson only for the brand's mark and danger (the red rule along the top gone).
- *Type*: Inter in place of Plus Jakarta Sans, a size smaller; tabular figures only where numbers stand in columns (Inter's widen the hyphens in names).
- *Shell*: the namespace picker in the top bar; quieter side bar entries, the current one tinted indigo.
- *Lists*: headers in sentence case on a faint band, rounded tables, names in the text colour, images and other detail muted, an empty cell a dash; a Service's Ready count a pill in its state's colour (`Column.tone`); a cell of buttons stays a cell, so the row's rule runs straight.
- *Cards*: rounder; at most four status cards a row; the Overview's alerts inside its health card, not in a second one; an object's details in a card.
- *Charts*: CSV and PNG as quiet icon buttons, full strength on hover or focus.
- *Images*: a row's less used actions (Scan, Download, Push, Remove) behind a More menu (lib/RowMenu.svelte); names in the text font. Apps' Deploy buttons plain, not a wall of indigo.
- *Topology*: each link's two lines one text, its name starting with what it shows (axe's 2.5.3 rule; the route's and the published port's names did not hold their visible text).
- Checked through the dev server against the Ryzen, dark and light: axe's WCAG 2.2 AA rules pass on Overview, Services, a Service, Volumes, a node, Deploy, Apps, Alerts, Images (its menu open), Topology and Diagnostics.

**Done when** the lab's console tests and an axe-core pass hold after the
change, and the same screens, side by side with Sylve's, read as one
consistent product.
