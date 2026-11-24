# Database replication plugins (SPI v1)

**Status: draft for review, nothing built.** The Legion side is in
DESIGN.md 6.9.

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
   operations. Additions come as capabilities a plugin declares in
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
    "dns": "pg-1.pg.data"
  },
  "topology": {
    "primary": { "member": "pg-0", "address": "10.200.0.9", "dns": "pg-0.pg.data" },
    "members": [
      { "member": "pg-0", "address": "10.200.0.9", "dns": "pg-0.pg.data" },
      { "member": "pg-1", "address": "10.200.1.17", "dns": "pg-1.pg.data" },
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
    "seedFromSnapshot": false
  } }
```

Legion uses the highest version both support, and refuses a Service
whose settings need a capability the plugin lacks: `synchronous` above
0 needs `synchronous`, and a read endpoint needs `readableReplicas`.

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

Legion routes writes to a member only while Legion names it primary
**and** its `status` says `primary` and healthy. A member Legion calls a
replica that reports `primary` is reported as split, and Legion stops it.

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
  example, retention or slots per member) and apply `synchronous`.
- **On a replica:** follow `topology.primary`, without a restart where
  the database allows. A member that is still primary in an older epoch
  answers `restart`.

Response: `{}`.

## What Legion guarantees a plugin

- Two members never get `promote` for the same epoch.
- `promote` comes only after the previous primary is fenced. Its task
  is stopped, confirmed by its agent, or its node is cut off and every
  task of the Service there has `onPartition: stop` and has passed its
  stop grace period plus 5 s.
- `prepare` runs with the database stopped. `status`, `promote` and
  `configure` run with it running and ready. Calls for one member never
  overlap.
- Writes go to the primary through the Service's own address, and reads
  through `<svc>-read` (DESIGN.md 6.9). A plugin does not touch routing.

## Conformance

`legion replication check PLUGIN --image IMAGE` (to be built with v1)
runs a plugin through the contract on one node with real jails:

1. `describe`.
2. Three members `prepare`d and started.
3. Writes on the primary seen on both replicas, and `status` positions
   ordered.
4. The primary killed, the furthest replica promoted, and the third
   member `configure`d to follow it.
5. The old primary `prepare`d as a replica (rewound or cloned) and
   following.
6. Every operation called twice with the same request, with the same
   result.
7. With `synchronous`, nothing acknowledged before the kill is lost.

A plugin that passes is one Legion can drive. The PostgreSQL plugin is
held to the same check in Legion's lab.

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
| `initdbArgs` | `[]` | Added to `initdb` |

- **`prepare`, primary, empty data directory:** `initdb
  --data-checksums` (pg_rewind needs checksums or `wal_log_hints`). It
  then writes `legion-replication.conf`, included from
  `postgresql.conf`, which holds `wal_level=replica`, `hot_standby=on`,
  `max_wal_senders`, `max_replication_slots` and
  `max_slot_wal_keep_size`. It adds `pg_hba.conf` lines for replication
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

Not in v1: archiving WAL to S3 for point-in-time recovery, which comes
next with the S3 backup targets; seeding a replica from a ZFS snapshot
(`seedFromSnapshot`); cascading replicas; logical replication.
