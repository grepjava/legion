# Database replication plugins (SPI v1)

**Status: SPI v1, built** (2026-11-24). The Legion side is in DESIGN.md
6.9.

A stateful Service can have its members replicate a database among
themselves: one primary, the others replicas streaming from it. Legion
decides which member is primary. A **replication plugin** does what
that takes for one database: it makes a data directory ready, reports
where the member stands, and promotes and re-points it. This document is
the contract between the two. Anyone can write a plugin for any database
without changing Legion. Legion's own PostgreSQL plugin (at the end) is
written to this contract and nothing more.

## Principles

1. **Legion decides, the plugin executes.** Which member is primary is
   recorded in Legion's store (Raft) and nowhere else. A plugin never
   elects, never promotes on its own, and never reaches another member
   except to replicate from the primary Legion named. Fencing (making
   sure an old primary has stopped) is also Legion's work.
2. **Every operation may be repeated.** An agent restarts, a leader
   changes, a connection drops after the plugin finished: Legion calls
   again with the same request. Each operation must leave the same state
   when called twice. `promote` on a primary succeeds and does nothing.
3. **Small, versioned, extended by capabilities.** v1 has five
   operations, and `probe` for the conformance check where a plugin
   declares it. Additions come as capabilities a plugin declares in
   `describe`, never as changed meanings.

## Running a plugin

A plugin is an executable. Legion finds it in one of two places, set on
the Service (DESIGN.md 6.9):

- `plugin: NAME`: `<dir>/NAME` on the node (`legion daemon
  --replication-plugins DIR`, by default
  `/usr/local/libexec/legion/replication`), mounted read-only into the
  container at `/.legion/replication/NAME`. Plugins shipped with Legion
  come this way, so an image needs no change beyond the database.
- `command: [PATH, ARGS...]`: an executable in the image. Third parties
  can ship their plugin inside their database image this way.

The agent runs it **inside the replicated container's jail**, as the
container's user unless the Service says otherwise, with the container's
environment, mounts and secrets. The database's own tools and socket are
therefore at hand.

Each call is:

```
<plugin> <operation>        request JSON on standard input
                            response JSON on standard output
                            log lines on standard error (into the task's log)
```

- Exit 0 means success, and standard output holds the response.
- Any other exit is a failure. Standard output may then hold
  `{"error": {"code": "...", "message": "..."}}`. Without it, the last
  lines of standard error are the message.
- Every request carries `deadline` (Unix milliseconds). A call still
  running then is killed (SIGTERM, then SIGKILL 10 s later) and counts
  as failed. A plugin should finish, or stop safely, before it.
- Unknown fields in a request must be ignored, and Legion ignores
  unknown fields in a response. This is how minor additions stay
  compatible.

Error codes Legion acts on:

| Code | Meaning | Legion |
|------|---------|--------|
| `retry` | Transient: the primary is not up yet, a lock is held | Calls again with backoff |
| `restart` | Cannot be done while the database runs (a primary told to follow) | Stops the task. Its next start runs `prepare` in the new role |
| `diverged` | Data cannot follow this primary and cannot be repaired here | Marks the member `diverged`. With `reclone: allow`, it empties the data directory and restarts the task. Otherwise it waits for the operator |
| anything else | Failed | Recorded, retried with backoff, shown on the Service |

## The request

Every request has these fields. Operations add their own.

```json
{
  "spiVersion": "1",
  "operation": "status",
  "deadline": 1764000000000,
  "service": { "namespace": "data", "name": "pg" },
  "epoch": 4,
  "self": {
    "member": "pg-1",
    "ordinal": 1,
    "address": "10.200.1.17",
    "dns": "pg-1.pg.data.legion"
  },
  "topology": {
    "primary": { "member": "pg-0", "address": "10.200.0.9", "dns": "pg-0.pg.data.legion" },
    "members": [
      { "member": "pg-0", "address": "10.200.0.9", "dns": "pg-0.pg.data.legion" },
      { "member": "pg-1", "address": "10.200.1.17", "dns": "pg-1.pg.data.legion" },
      { "member": "pg-2", "address": "10.200.2.4", "dns": "pg-2.pg.data" }
    ],
    "synchronous": { "count": 1, "members": ["pg-1", "pg-2"] }
  },
  "options": { }
}
```

- `epoch` goes up by one each time Legion names a new primary. A plugin
  may store it with the data. Legion never sends a request with an
  epoch lower than one it already sent to that member.
- `topology` is Legion's current view. `primary` is absent while there
  is none (before the first start, or during a failover).
- `role` (`prepare` only) is the role to start in. `reclone` (`prepare`
  of a replica only) is true when the member answered `diverged` before
  and the Service allows it to be copied anew: discard the data and copy
  the primary's.
- `archive` (capability `archive`) is true when the Service archives
  its log (`replication.archive`): the plugin keeps each finished piece of
  the database's log in a spool directory and says where in `status`
  (`spool`). Legion ships what is there to the target and removes it.
  Files ending `.part` are being written and are left alone.
- `restore` (`prepare` of the primary only, capability `restore`) is
  `{"id", "log", "to"}` when the data directory holds a restored backup
  of another database to replay to a point in time (below, Restoring to a
  point in time). `log` is a directory, as the container sees it, holding
  the log files as the database archived them. `to` is an RFC 3339 time
  in UTC; absent, all the log there is.
- `options` is the Service's `replication.options`, passed as given.
  Secrets go in as files the Service mounts (`/run/secrets/...`), named
  in options. Legion never sends a secret's value in a request.

## Operations

### `describe`

Called once per plugin and image, before anything else.

```json
{ "spiVersions": ["1"],
  "database": "postgresql",
  "capabilities": {
    "synchronous": true,
    "readableReplicas": true,
    "rewind": true,
    "seedFromSnapshot": false,
    "probe": true,
    "archive": true,
    "restore": true
  } }
```

Legion uses the highest version both support, and refuses a Service
whose settings need a capability the plugin lacks: `synchronous` above
0 needs `synchronous`, a read endpoint needs `readableReplicas`,
`archive` needs `archive`, and `restore` needs `restore`.

### `prepare`

Run before the container's main process starts, each time the task
starts. The data directory is mounted, and the database is not running.
It adds `"role": "primary" | "replica"`.

It makes the data directory ready to start in that role:

- **Primary, empty data directory:** create the database, configured to
  be replicated (users, access rules, settings), so the first start
  serves.
- **Primary, existing data:** make sure the replication configuration is
  in place, and nothing more.
- **Primary, told to `restore`:** set the data directory up to replay the
  files in `log` up to `to`, then serve as the primary on a history of its
  own. Only once per `id`: told the same again (a restart while it
  replays, or once it is done), it carries on as it is. Until it serves,
  `status` says `role: primary`, `healthy: false` and
  `detail.restoring: true`, and `configure` and `promote` answer `retry`.
  Data that is no database is refused, never replaced by a new one.
- **Replica:** whatever it takes to start as a replica of
  `topology.primary`. That means copying it when the directory is empty,
  rewinding when this member's history diverged (an old primary coming
  back), and doing nothing when it already follows. Data of another
  database (a different system identifier) is `diverged`.

A replica's `prepare` may need the primary running. If it is not, the
code is `retry`. Response: `{}`, or `{"action": "cloned" | "rewound" |
"none"}` for the record.

### `status`

Run every 5 s on every running member, like a probe. It must be quick
(the deadline is 4 s) and must not change anything.

```json
{ "role": "replica",
  "healthy": true,
  "position": { "term": 7, "offset": "50331744" },
  "upstream": "pg-0",
  "streaming": true,
  "synchronous": true,
  "lag": { "bytes": "8192", "seconds": 0.4 },
  "detail": { } }
```

- `role` is what the database says it is: `primary`, `replica`, or
  `unknown`.
- `position` is how far this member's data goes, comparable across
  members. Positions are ordered by `term`, then `offset`, an unsigned
  64-bit integer written as a decimal string. A replica reports what it
  has **received**, which promotion would keep, not only what it has
  applied. Legion compares positions only and never parses `detail`.
- `lag` is optional. A replica reports it from its own view. A primary
  may leave it out.
- `detail` is free-form, shown to operators as given.
- `spool`, asked to `archive`: the directory, as the container sees it,
  holding finished pieces of the log for Legion to ship. It must be on one
  of the pod's claimed Volumes, so the node can reach it.

Legion routes writes to a member only while Legion names it primary
**and** its `status` says `primary` and healthy. A member Legion calls a
replica that reports `primary` is reported as split, and Legion stops it.

### `probe` (capability `probe`)

Only for the conformance check below, never called by Legion's agents.
The request adds `"probe": {"write": KEY}` or `{"read": KEY}`, where KEY
is letters, digits, `-` and `_`, up to 64. A write, on the primary,
stores a marker under KEY, committed as any write is (so synchronous
replicas confirm it). A read, on any member, says whether it is there.
Response: `{"found": true | false}`. The deadline is 30 s. A plugin
without it is still checked, except that data is not seen to travel.

### `promote`

Make this replica the primary, with the request's `topology` (it is
named primary there, and `epoch` is the new one). Legion calls it only
once the old primary is fenced (stopped, or out of reach and certain to
have stopped). It returns once the database accepts writes. On a member
already primary in this epoch, it succeeds.

Response: `{"position": {...}}`, where the new history starts.

### `configure`

Apply a changed topology. It is called on every running member after a
promotion, when members join or leave, and when the synchronous settings
change.

- **On the primary:** accept and keep data for the listed members (for
  example, retention or slots per member) and apply `synchronous`: wait
  for `count` of its `members` to confirm each commit, or for none when it
  is absent. Legion lists only the replicas healthy and streaming, so a
  lost one never stops the primary taking writes.
- **On a replica:** follow `topology.primary`, without a restart where
  the database allows. A member that is still primary in an older epoch
  answers `restart`.

Response: `{}`.

## What Legion guarantees a plugin

- Two members never get `promote` for the same epoch.
- `promote` comes only after the previous primary is fenced. Its task
  is stopped, confirmed by its agent, or its node is cut off and every
  task of the Service there has `onPartition: stop` and has passed its
  stop grace period plus 5 s. Such a task's supervisor stops it on its
  own by then, even with the node's daemon gone (self-fencing,
  docs/PARTITIONS.md).
- `prepare` runs with the database stopped. `status`, `promote` and
  `configure` run with it running and ready. Calls for one member never
  overlap.
- Writes go to the primary through the Service's own address, and reads
  through `<svc>-read` (DESIGN.md 6.9). A plugin does not touch routing.

## Conformance

`legion replication check -f FILE [-n NAMESPACE] [--keep]` runs a plugin
through the contract on a cluster, in real jails, with Legion driving it
as it drives any database. FILE holds a replicated Service with
`replicas: 3` and automatic failover, and what it needs (Secrets, a
Namespace); one node is enough. The check applies it, then:

1. Calls `describe` in member 0's container, and refuses a plugin that
   does not speak SPI 1, or lacks `synchronous` the Service asks for.
2. Waits for Legion to form it: three members `prepare`d, started and
   healthy, a primary named.
3. Calls `status` twice on each member, directly, as the agent does: the
   same both times, the role the member was told, replicas no further
   than the primary.
4. With `probe`, writes a marker on the primary and reads it on both
   replicas.
5. Calls `configure` twice on every member and `promote` twice on the
   primary: the same answers, still healthy.
6. Writes a marker, then stops the primary dead (`kill -STOP -1` in its
   container). Legion must fence it, promote a replica and raise the
   epoch. With `synchronous`, the marker must be on the new primary; a
   marker written there must reach the third member, `configure`d to
   follow it.
7. Waits for the old primary to come back, `prepare`d as a replica
   (rewound or cloned), holding what was written since.

It prints each step as it passes, and removes what it applied and the
members' Volumes, unless `--keep`. A step that fails stops it and leaves
everything for a look. A plugin that passes is one Legion can drive.
Legion's lab runs it on the PostgreSQL plugin.

## Legion's PostgreSQL plugin (`postgres`)

A static Rust binary, shipped with Legion in
`/usr/local/libexec/legion/replication/postgres`. It drives the image's
own `initdb`, `postgres --single`, `pg_basebackup`, `pg_rewind` and
`psql`, so it follows the image's PostgreSQL version, 13 or later.

Options:

| Option | Default | |
|--------|---------|--|
| `pgdata` | `$PGDATA` | The data directory |
| `port` | 5432 | |
| `superuserPasswordFile` | | For `initdb`, as `examples/postgres` uses |
| `replicationUser` | `replicator` | Created at `prepare` on the primary |
| `replicationPasswordFile` | required | A Secret mounted in the pod |
| `maxSlotWalKeepSize` | `4GB` | Bounds what a stopped replica's slot holds on the primary |
| `walKeepSize` | `512MB` | WAL every member keeps, so a primary cut off and back can be rewound |
| `initdbArgs` | `[]` | Added to `initdb` |

Asked to `archive`, every member sets `archive_mode = always` and
`archive_command` to the plugin itself, `<plugin> archive-wal %p %f
<spool>`, with `archive_timeout = 60` (a quiet database still ships a
file a minute). It copies each finished WAL file, timeline history files
included, into the spool, synced before it says so, and fails once 1024
files wait there, so PostgreSQL keeps its WAL rather than fill the Volume
while Legion cannot ship. The spool is `<pgdata>/legion_wal`, and what
replaces the data directory would take it with it: `pg_rewind` removes
what the new primary lacks, and a new copy starts from an empty
directory. So before either, the plugin archives what PostgreSQL
finished but had not archived yet (a primary stopped at once leaves
some) and waits (`retry`) until Legion has shipped the spool. Only that
replica waits: Legion counts only replicas streaming as synchronous, so
the primary goes on taking writes; with the target unreachable it waits
until the target is back. Every member archives, not
only the primary: a replica promoted then has what the old primary wrote
before, and the target takes each name once. `archive_mode` takes effect
at a start, so archiving turned on for a running Service starts at each
member's next restart (`legion restart`).

- **`prepare`, primary, empty data directory:** `initdb
  --data-checksums` (pg_rewind needs checksums or `wal_log_hints`). It
  then writes `legion-replication.conf`, included from
  `postgresql.conf`, which holds `wal_level=replica`, `hot_standby=on`,
  `max_wal_senders`, `max_replication_slots` and
  `max_slot_wal_keep_size` and `wal_keep_size`. It adds `pg_hba.conf` lines for replication
  from the pod network, and creates the replication role in single-user
  mode (`postgres --single`), since nothing is running yet.
- **`prepare`, replica:**
  - With an empty directory, `pg_basebackup -X stream` from the primary,
    using this member's slot.
  - With existing data, it compares system identifiers
    (`pg_controldata` with `IDENTIFY_SYSTEM`). A different one is
    `diverged`. A timeline that forked from the primary's is repaired by
    `pg_rewind`; a failed rewind is `diverged`.
  - It then writes `standby.signal`, and `primary_conninfo` with
    `application_name` set to the member's name, used for synchronous
    standbys.
- **`status`:**
  - `pg_is_in_recovery()`.
  - The term is the timeline: `pg_control_checkpoint()` on a primary,
    the receiver's `received_tli` on a replica.
  - The offset is `pg_current_wal_lsn()` on a primary, or
    `pg_last_wal_receive_lsn()` on a replica.
  - Lag comes from `pg_last_xact_replay_timestamp()`.
  - `streaming` and `synchronous` come from `pg_stat_wal_receiver`.
- **`promote`:** `pg_promote(wait => true)`, then a physical slot for
  each other member.
- **`configure`:**
  - On the primary, slots are kept for exactly the listed members, and
    `synchronous_standby_names = 'ANY n (members)'` is set with
    `ALTER SYSTEM` and a reload.
  - On a replica, `primary_conninfo` is rewritten and the configuration
    reloaded (PostgreSQL 13 and later need no restart).
  - A primary of an older epoch (`pg_is_in_recovery()` false) answers
    `restart`.

- **`prepare` told to `restore`:** it removes the backup's
  `postmaster.pid` and `standby.signal` (a replica's backup is as good as
  a primary's), writes `recovery.signal` and `legion_restore.id`, and adds
  to `legion-replication.conf` `restore_command` (a copy from `log`, else
  from the member's own spool), `recovery_target_time`,
  `recovery_target_timeline = 'latest'` (so it follows the failovers the
  log went through) and `recovery_target_action = 'promote'`. PostgreSQL
  removes `recovery.signal` once promoted, and the next `prepare` leaves
  the settings out.

## Restoring to a point in time

With a Service's log archived (`replication.archive`) and its members'
Volumes backed up to a BackupTarget (the claim's `backup`), Legion
restores the database as it was at a time, as a new Service:

```
legion replication restore pg-then --from pg --to 2026-11-25T10:00:00Z [--replicas N]
```

makes Service `pg-then` from `pg`'s spec with:

```yaml
replication:
  restore:
    from: pg                      # whose backups and log; it may be gone
    to: 2026-11-25T10:00:00Z      # UTC; unset, all the log there is
    target: offsite               # the log's target; unset, from's archive.target, else this Service's
    backups: vault                # the Volume backups' target; unset, the claim's backup.target, else target
    backup: pg-pg-1@legion-bk-vault-20261125T090000Z   # unset, the newest of from's members' before `to`
    claim: pg                     # unset, the pod's only claim
    node: n2                      # unset, a ready node taking new tasks
```

Written by hand, it restores from a Service that is gone. Before any
member of the new Service runs, the leader restores the backup as its
first member's Volume (`<claim>-<service>-0`) on the node, and fetches
the files under `<prefix>/<namespace>.<from>/wal/` into its
`legion_restore` directory. The first member's `prepare` is then told to
`restore`; the other members wait (they are told no primary) until it
serves as the primary, then copy it as any replica does. The fetched
log is then removed. `status.restore` says where it stands (`restoring`,
`replaying`, `done`, or `skipped` when the first member's Volume was
there already: nothing is restored over data), as `legion replication
status` and the console's Replication tab do. A step that fails is tried
again, after a backoff, and said there.

The original goes on as it was; the restored database has a history of
its own from `to` on, and with `archive` its log goes under its own name.

Not in v1: seeding a replica from a ZFS snapshot (`seedFromSnapshot`);
cascading replicas; logical replication.
