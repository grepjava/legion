# Legion: next enhancements

Written 22 November 2026, after comparing Legion with
[Sylve](docs/COMPARISON.md#sylve) where the two overlap. Each item says
what Legion does today (checked in the code and docs), what is missing,
a proposed shape, and when it is done. The shapes are proposals, to be
settled in [docs/DESIGN.md](docs/DESIGN.md) before building, as every
phase has been.

In order of priority:

1. [Scheduled Volume snapshots and backups](#1-scheduled-volume-snapshots-and-backups)
2. [Moving a Volume while it runs](#2-moving-a-volume-while-it-runs): checked, already built
3. [Setting up pf for Legion](#3-setting-up-pf-for-legion)
4. [Console sign-in with host accounts and passkeys](#4-console-sign-in-with-host-accounts-and-passkeys)
5. [Ready-to-deploy applications](#5-ready-to-deploy-applications)

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
