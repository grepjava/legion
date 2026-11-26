# Legion CLI Reference

`legion` is the single binary for the Legion daemon, cluster CLI, OCI image builder (`legion build`), and standalone local jail/VM runner (`legion local`).

## Global Options

| Flag | Environment | Default | Description |
|---|---|---|---|
| `--server <HOST:PORT>` | `LEGION_SERVER` | `127.0.0.1:7470` | Manager gRPC endpoint |
| `--identity <PATH>` | `LEGION_IDENTITY` | `/var/db/legion/admin.pem` | Client certificate, key, and cluster CA PEM |
| `-h`, `--help` | — | — | Print help |
| `-V`, `--version` | — | — | Print version |

---

## Cluster & Node Lifecycle

### `legion daemon [OPTIONS]`
Run the node daemon (manager and, on FreeBSD, worker agent). Key flags:
- `--data-dir <DIR>` (default `/var/db/legion`), `--run-dir <DIR>` (default `/var/run/legion`)
- `--listen <ADDR>` (default `0.0.0.0:7470`), `--advertise <ADDR>`
- `--no-agent` (manager only), `--node-name <NAME>`
- `--zfs-root <DATASET>`, `--infra-image <IMAGE>`, `--linux`
- `--manager <HOST:PORT>` (repeatable), `--insecure-registry <HOST[:PORT]>` (repeatable)
- `--metrics-listen <ADDR>`, `--console-listen <ADDR>`, `--console-cert <PEM>`, `--console-key <PEM>`
- `--ingress-listen <ADDR>`, `--ingress-tls-listen <ADDR>`
- `--builder <BIN>` (default: this binary, as `legion local`), `--builder-arg <ARG>` (repeatable): what a builder node runs Builds with
- `--iscsi-targets <FILE>`, `--iscsi-portal-group <NAME>` (default `legion`), `--iscsi-portal <HOST[:PORT]>`: iSCSI Volumes this node provides

### `legion init`
Initialize a new cluster with the reached manager as its first member.

### `legion join-token <manager|worker> [--rotate]`
Print (or rotate) the join token for a manager or worker node.

### `legion join <TOKEN> --manager <HOST:PORT> [OPTIONS]`
Join an existing cluster before starting the daemon (`--data-dir`, `--advertise`, `--node-name`).

### `legion status`
Show the reached manager's view of the cluster (managers, Raft leader, nodes).

### `legion doctor [--zfs-root <DATASET>] [--json]`
Check whether this host satisfies Legion's prerequisites without changing anything (run as `root` to inspect `pf` rules). Exits nonzero if a required check fails or cannot be read.

### `legion doctor --fix pf [--yes] [--pf-conf <FILE>]`
Put pf right for Legion: add the lines calling its anchors to `/etc/pf.conf` (translation hooks before the first filter rule, the filter hook at the end; no other line changed), after showing them and asking. pf checks the new file before it is written (`pfctl -nf`); the old one is kept as `/etc/pf.conf.legion-<time>`; then pf is loaded, `pf_enable=YES` set and the rules loaded, and the way back is printed. A file another tool writes (Sylve's) is left alone, with the lines to add said instead. Run again, it changes nothing. `legion init` (on the manager's host) and `legion join` offer it when pf is not set up; the console's Diagnostics page shows the same plan for each node. `legion local doctor --fix pf` does the same for `legion local`'s anchors.

### `legion diagnose [--node <NAME>] [--json]`
Run `doctor` checks across all cluster nodes (or a single `--node`). Cluster admin only.

### `legion support-bundle [-o <FILE>]`
Export a masked cluster diagnostics bundle (objects without Secrets, masked env values, alerts, recent events, node diagnoses). Cluster admin only.

### `legion backup -o <FILE>` / `legion restore [FILE]`
Save a consistent snapshot of all cluster objects, or restore a stopped manager's store from a backup (or from its own store when other managers are lost).

### `legion autolock <enable|disable|rotate|show>`
Manage manager key sealing. When enabled, restarted managers wait for `legion unlock [--key-file <PATH>]`.

### `legion ca <rotate|status>`
Rotate the cluster CA across all managers and nodes, or inspect CA rotation status.

### `legion console <login|settings|configure|oidc>`
- `login [--url <URL>]`: Print a one-time browser sign-in URL (valid 60 s) for the web console.
- `settings` / `configure -f <FILE>`: Print or update console settings (banner, sidebar links, default namespace, session lifetime).
- `oidc [-f <FILE>] [--remove]`: View, configure, or disable OIDC single sign-on. With `usernameClaim: email` (the default), the ID token must carry `email_verified: true`. Providers that omit it, such as Microsoft Entra ID, need another claim (`oid`, `preferred_username` or `sub`); see CONSOLE.md.

### `legion user create <NAME> [--role <view|edit|admin> -n <NS>] [-o <FILE>] [--days <N>]`
Generate a client certificate PEM for a user and optionally create a `RoleBinding`.

### `legion node <SUBCOMMAND>`
- `drain <NODE> [--wait [--timeout <SECONDS>] [-o <table|json>]] [--dry-run|--force]`, `pause <NODE>`, `activate <NODE>`
- `promote <NODE>`, `demote <NODE>`
- `label <NODE> <key=value|key->...`
- `require <NODE> [CHECK...]` (gate new tasks on `doctor` advised checks)

---

## Workloads, Objects & Stacks

### `legion apply -f <FILE|->`
Create or update objects from YAML or JSON (`-` reads stdin).

### `legion diff -f <FILE|->`
Preview what `apply` would change. Exits `1` when differences exist.

### `legion get <KIND> <NAME> [-n <NS>] [-o <yaml|json>]`
Show a single object.

### `legion list <KIND> [-n <NS>|-A] [-l <SELECTOR>] [-o <table|yaml|json>]`
List objects of a kind.

### `legion delete <KIND> <NAME> [-n <NS>] [--dry-run|--force]`
Delete an object.

#### Checks before disruption
`legion node drain`, `legion volume move`, and `legion delete` of a node or
a Service ask the leader first what the change would do. The leader runs
the controller over a copy of the cluster, once as it is and once with the
change, until each settles, and reports the difference:

- the Volumes that move, from where to where, and how much data each holds;
- what blocks it: work no node would take (`unplaced`), a Volume left where
  it is (`stranded`), a Volume's move waiting on tasks that may not stop
  (`held`), a task still on a drained node (`stays`), a node whose
  pools lack room for the data moving there (`no-room`), data lost with a
  node that is still up (`lost`: drain it first), a cancel of a move past its
  switch-over (`switch-over`);
- what to know first: tasks a disruption budget holds back until the
  others run again (`budget`), a Volume carrying on from an older replica
  (`replica`), the Volumes a deleted Service leaves (`kept`), a manager
  leaving Raft (`manager`), a drained node that is not ready, whose drain
  completes once it is back (`unreachable`), a node not in the cluster yet
  (`absent`).

With `--dry-run` it prints this, changes nothing, and exits 1 if anything
blocks. Without it, a change that would block is refused (exit 1, with the
findings) unless `--force` is given; warnings are printed and the change
goes ahead. The console's confirmation shows the same check and keeps its
button disabled while anything blocks.

### `legion watch [KIND] [--from <VERSION>]`
Stream live `PUT`/`DELETE` object events, from now on or after a version.

### `legion stack <SUBCOMMAND>`
- `deploy <STACK> -c <COMPOSE_FILE> [-n <NS>] [--prune]`
- `convert -c <COMPOSE_FILE>` (print Legion YAML)
- `ls [-n <NS>]`, `stop <STACK>`, `start <STACK>`, `rm <STACK>` (keeps Volumes)

### `legion app <SUBCOMMAND>`
Ready-to-deploy applications from the daemonless catalog (FreeBSD images, read from `catalog.daemonless.io` when asked; `--catalog URL` for another):
- `list [--category C]`, `show <APP>` (its variables, kinds and defaults, and its image's variants)
- `deploy <APP> [--name STACK] [-n NS] [--set NAME=VALUE]... [--variant TAG] [--dry-run]`: deployed as a stack, in a namespace made if missing allowing what its containers ask for (SysV IPC, raw sockets); a storage variable names a Volume, a secret not set is made up and printed once.

### `legion stop <SERVICE>` / `legion start <SERVICE>`
Stop a service's tasks while preserving its spec and replica count, or start it again.

### `legion pause <SERVICE>` / `legion resume <SERVICE>`
Freeze a service's tasks where they run, or let them go on. A paused task keeps its node, address and memory: a jail's processes are stopped (`SIGSTOP`), a VM's `bhyve` is, holding its virtual CPUs. It leaves the service's endpoints and its probes are held, so it is neither sent traffic nor restarted; resumed, its probes start over. A stop or an update while paused lets the task go first, so its stop signal is taken as usual. Tasks on nodes whose agent is older than protocol 3 run on; `legion list services` shows `paused N/M`, N of the M running tasks paused, and `legion list tasks` each task's state. `legion list services` also marks an image `(tag moved)` when its tag names another image now, and `(Linux)` when its tasks run it through FreeBSD's Linux ABI, with `; native: <recipe>` when a recipe in `examples/` runs the same from FreeBSD's packages.

### `legion restart <SERVICE>` / `legion rollback <SERVICE>`
Trigger a rolling restart (`restartedAt`) or roll back to the previous spec.

### `legion cronjob run <CRONJOB> [-n <NS>]`
Trigger a CronJob immediately outside its schedule.

### `legion replication <status|switchover|failover> <SERVICE> [-n <NS>]`
A replicated database's members (DESIGN.md 6.9, docs/REPLICATION.md).
`status` shows each member's role, health, position and lag, the primary
Legion names, any change under way and the last one. `switchover [--to
MEMBER] [--force]` stops the primary cleanly and promotes MEMBER (or the
replica furthest ahead) once it holds all the old primary wrote.
`failover --to MEMBER [--force]` promotes MEMBER when the primary is lost
under `failover: manual`; `--force` passes over the lag and synchronous
checks. Each is a request in the Service's spec, acted on once; a refused
one is said in `status`.

### `legion replication restore <NEW> --from <SERVICE> [--to <TIME>] [--replicas <N>] [-n <NS>]`
A new Service NEW with SERVICE's database as it was at TIME (RFC 3339 in
UTC; without it, all the log there is): SERVICE's spec, `--replicas`
members (1 by default, `synchronous` lowered to fit), its first member's Volume restored from the newest
backup of SERVICE's members taken before TIME, and the log SERVICE
archived replayed up to TIME (docs/REPLICATION.md, Restoring to a point in
time). `--backup VOLUME@NAME`, `--target TARGET` and `--node NODE` choose
for it. SERVICE goes on as it is; `legion replication status NEW` says how
far it got.

### `legion replication check -f <FILE> [-n <NS>] [--keep]`
The conformance check (docs/REPLICATION.md): applies the replicated
Service in FILE (`replicas: 3`, automatic failover), then drives its
plugin through the contract on the cluster, each operation twice, a
primary stopped dead and brought back, data seen to travel with the
plugin's `probe`. Prints each step as it passes; removes what it applied
unless `--keep` or a step failed.

---

## Observability, Storage, Network & Sessions

- `legion top [-n <NS>] [--once]`: Live terminal dashboard (nodes with their load and ZFS pools, workloads with their Linux images and moved tags marked, tasks, resource graphs over the polls or up to 30 days (`w`), logs, shell, alerts, long operations), one panel at a time as ISPF shows them: Enter opens a list, a row or a node's tasks, F3 goes back, 0–8 go to the graphs or a list, F10 pulls down the action bar, Space marks rows for one letter to act on all; the command line (`:` or `=`) takes `=N`, `find`, `sort`, `range` and others, else runs any `legion` command; `E` edits the selected object, F1 or `?` lists the keys.
- `legion logs <TASK|SERVICE> [-c <CONTAINER>] [-f] [--tail <N>]`
- `legion build-logs <BUILD> [-n <NS>] [-f]`
- `legion logs-search <SERVICE|JOB> [--since <DUR>] [--until <DUR>] [TEXT]`
- `legion exec <TASK|SERVICE> [-c <CONTAINER>] [-i] [-t] -- <CMD...>`
- `legion files <TASK|SERVICE> [PATH]` / `legion cp <SRC> <DEST>`
- `legion port-forward <TASK|SERVICE> <[LOCAL:]REMOTE>...`
- `legion events [KIND/NAME] [-n <NS>] [-w]`
- `legion activity [ID|NAME] [-w] [-o <table|yaml|json>]` (drains, Volume moves, restores and replicas' whole copies: stage, bytes sent, how long, who asked, how it ended; with an ID, its stages and full output)
- `legion alerts [-n <NS>] [-w] [-o <table|yaml|json>]`
- `legion usage [TASK] [-n <NS>] [--range <DUR>] [-o <table|yaml|json>]` (the last hour as the leader sampled it, every 15 s; with `--range`, up to `30d`, from what every manager keeps in its data directory: a sample a minute within a day, every 15 minutes beyond, kept across a change of leader)
- `legion quota [-n <NS>]`
- `legion audit [FILTERS]` (cluster admin only)
- `legion reach [-n <NS>]` (evaluate NetworkPolicy reachability matrix)
- `legion connections [-n <NS>]` (the connections the nodes' pf saw, by service and port)
- `legion topology [-n <NS>]` (each service with its ports, Routes, tasks, Volumes, what may reach it and what was seen)
- `legion load-balancers [-n <NS>]` (CARP holders and states)
- `legion volume <move|backup|restore|backups|snapshots|snapshot|rollback|rm-snapshot|clone>` (`move <VOLUME> <NODE> [--dry-run|--force]` is checked first, see Checks before disruption)
  - `snapshots` lists a person's and the schedules' (`legion-auto-<every>-<time>`), not the bases of moves, replicas and backups; `legion-` names are Legion's, so a person's may not start so.
  - `rollback <VOLUME> <SNAPSHOT> [--yes]` says which later snapshots it destroys and asks first.
  - `clone <VOLUME>@<SNAPSHOT> <NAME>` copies a snapshot's data into a new Volume on the same node, leaving the Volume as it is.
  - `backups <VOLUME> [--from TARGET]` lists what its BackupTarget holds of it, oldest first; `restore --from TARGET <VOLUME>@<BACKUP> --node NODE [--name NAME]` restores one of them onto NODE as a new Volume.
  - A Volume's `snapshots: [{every, keep}]` and `backup: {target, every, keep}` take them on a schedule (DESIGN.md §4.3); a `BackupTarget` is a ZFS host over SSH (`ssh: {host, port, user, dataset, keySecret, hostKey}`) a directory on the node (`directory: {path}`), or an S3-compatible bucket (`s3: {endpoint, region, bucket, prefix, accessKeyId, secretKeySecret, virtualHosted}`).

---

## Cluster Image Commands (`legion image`)

- `legion image load <FILE> [--name <NAME>] [--zfs-root <DATASET> [--data-dir <DIR>] | --node <NODE>]`
- `legion image ls [--node <NODE>] [-o <table|yaml|json>]`
- `legion image pull <REF> [--node <NODE>] [-n <NS> --secret <SECRET>]`
- `legion image push <REF> <TARGET> --node <NODE> [-n <NS> --secret <SECRET>]`
- `legion image rm <REF> [--node <NODE>]`
- `legion image prune [--node <NODE>] [--builds]`
- `legion image scan <REF> --node <NODE> [--json]` (FreeBSD `pkg audit` vulnerability check)
- `legion image export <REF> --node <NODE> -o <FILE> [--tag <NAME>]`
- `legion image search [QUERY] [--limit <N>] [-n <NS> --secret <SECRET>]`: the FreeBSD catalog and Docker Hub. `ghcr.io/OWNER/REPO` is read from ghcr.io anonymously. With `--secret` (a Secret in the namespace holding a Docker config with a ghcr.io login, a token with `read:packages`), GitHub container packages are listed too: the token's own, or `ghcr.io/OWNER`'s. Only the first 100 packages or tags are read. The OS column says `freebsd` or `linux` where the search knows it (the catalog's images, Docker's own), else `?`: `legion image inspect` reads it from the image. A Linux image runs only on nodes started with `--linux`.
- `legion image sbom <REF> [-n <NS> --secret <SECRET>] [--json]`
- `legion image inspect <REF> [--node <NODE>] [-n <NS> --secret <SECRET>]`

---

## Building Images (`legion build`)

### Jail OCI Images: `legion build [OPTIONS] [CONTEXT]` (or `legion build jail ...`)
Builds a FreeBSD OCI image from a `Containerfile` using native FreeBSD jails (or `--backend buildah`):
- `-t, --tag <NAME[:TAG]>`, `-f, --file <PATH>`, `--build-arg <KEY=VALUE>`
- `--target <STAGE>`, `--jobs <N>`, `--strict`, `--no-cache`, `--squash`
- `--cache-max <SIZE>`, `--cache-from <REF>`, `--cache-to <REF>`
- `--secret <id=ID,src=FILE>`, `--ssh <ID[=SOCKET]>`, `--param <KEY=VALUE>`, `--sysv <msg,sem,shm>`
- `-m, --memory <SIZE>`, `--cpus <N>`, `--pids-limit <N>`, `--cpuset-cpus <LIST>`
- `--label <KEY=VALUE>`, `--annotation <KEY=VALUE>`, `--timestamp <SECONDS>`, `--compression <gzip|zstd>`
- `--state-dir <DIR>`, `--zfs-root <DATASET>`, `--no-zfs`, `--plain-http <HOST[:PORT]>`

### bhyve VM OCI Images: `legion build vm [OPTIONS]`
Packages a `bhyve` VM disk image as `/disk.img` inside a Legion OCI image:
- `-t, --tag <NAME[:TAG]>` (default `localhost/vm:latest`)
- `--disk <PATH>`: Package an existing raw disk image as `/disk.img`
- `--rootfs <DIR>` + optional `--size <SIZE>`: Create UFS `/disk.img` from a FreeBSD rootfs via `makefs`
- `--compression <gzip|zstd>`, `--label <KEY=VALUE>`, `--annotation <KEY=VALUE>`, `--timestamp <SECONDS>`
- Store flags, before `vm` or after it: `--state-dir <DIR>`, `--zfs-root <DATASET>`, `--no-zfs`. A jail build's own flags go without `vm`.

---

## Local Standalone Mode (`legion local`)

All standalone local image, build, signing, and container commands live under `legion local`. It was the separate tool jailer, merged into Legion:

| Command | Purpose |
|---|---|
| `init` | Scaffold a FreeBSD `Containerfile` and `.containerignore` |
| `build` | Build a FreeBSD jail OCI image from a `Containerfile` |
| `run` | Run an image locally in a FreeBSD jail (`--vnet`, `-p`, `-v`, `-e`, `--linux`) or as a `bhyve` VM (`--vm`, `--vm-cpus`, `--vm-memory`, `--vm-boot <bhyveload\|uefi>`) |
| `ps`, `logs`, `exec`, `stop`, `rm` | Manage local standalone containers |
| `pull`, `load`, `push`, `export`, `images`, `rmi`, `prune` | Manage the local OCI image store and registries |
| `publish`, `manifest` | Build multi-arch images across hosts and push OCI indexes |
| `sign`, `verify`, `attest`, `sbom`, `key` | Cosign-compatible image signing, SLSA/SPDX attestations, and key generation |
| `check`, `doctor`, `login` | Compatibility checks, local host readiness, and registry login |

Local global options: `--state-dir <DIR>` (`LEGION_LOCAL_STATE_DIR`; default `/var/db/legion-local` for root, `~/.local/share/legion-local` otherwise), `--zfs-root <DATASET>` (`LEGION_LOCAL_ZFS_ROOT`; default `zroot/legion-local` for root when `zroot` exists), `--no-zfs` (`LEGION_LOCAL_NO_ZFS`), `--pod-subnet <CIDR>` (`LEGION_LOCAL_POD_SUBNET`; default `10.77.0.0/16`), `--plain-http <HOST[:PORT]>`.

Pods (`run --vnet`, `-p`) are on the bridge `legionlocal0`; their ports are pf rules in anchors under `legion-local/`, which `/etc/pf.conf` must call before its filter rules:

```
nat-anchor "legion-local/*"
rdr-anchor "legion-local/*"
anchor "legion-local/*"
```

`publish --remote-legion <PATH>` names the legion on the build hosts (default `legion`).

jailer's state moves on its own: with neither `--state-dir` nor `--zfs-root` given, the first `legion local` command moves `/var/db/jailer` (or `~/.local/share/jailer`) and `zroot/jailer` to the new defaults, unless the old state still holds containers; remove those first, or keep using it with `--state-dir`.
