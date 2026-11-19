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
- `oidc [-f <FILE>] [--remove]`: View, configure, or disable OIDC single sign-on.

### `legion user create <NAME> [--role <view|edit|admin> -n <NS>] [-o <FILE>] [--days <N>]`
Generate a client certificate PEM for a user and optionally create a `RoleBinding`.

### `legion node <SUBCOMMAND>`
- `drain <NODE> [--wait [--timeout <SECONDS>] [-o <table|json>]]`, `pause <NODE>`, `activate <NODE>`
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

### `legion delete <KIND> <NAME> [-n <NS>]`
Delete an object.

### `legion watch [KIND] [--from <VERSION>]`
Stream live `PUT`/`DELETE` object events, from now on or after a version.

### `legion stack <SUBCOMMAND>`
- `deploy <STACK> -c <COMPOSE_FILE> [-n <NS>] [--prune]`
- `convert -c <COMPOSE_FILE>` (print Legion YAML)
- `ls [-n <NS>]`, `stop <STACK>`, `start <STACK>`, `rm <STACK>` (keeps Volumes)

### `legion stop <SERVICE>` / `legion start <SERVICE>`
Stop a service's tasks while preserving its spec and replica count, or start it again.

### `legion pause <SERVICE>` / `legion resume <SERVICE>`
Freeze a service's tasks where they run, or let them go on. A paused task keeps its node, address and memory: a jail's processes are stopped (`SIGSTOP`), a VM's `bhyve` is, holding its virtual CPUs. It leaves the service's endpoints and its probes are held, so it is neither sent traffic nor restarted; resumed, its probes start over. A stop or an update while paused lets the task go first, so its stop signal is taken as usual. Tasks on nodes whose agent is older than protocol 3 run on; `legion list services` shows `paused N/M`, N of the M running tasks paused, and `legion list tasks` each task's state.

### `legion restart <SERVICE>` / `legion rollback <SERVICE>`
Trigger a rolling restart (`restartedAt`) or roll back to the previous spec.

### `legion cronjob run <CRONJOB> [-n <NS>]`
Trigger a CronJob immediately outside its schedule.

---

## Observability, Storage, Network & Sessions

- `legion top [-n <NS>] [--once]`: Live terminal dashboard (nodes, workloads, tasks, resource graphs, logs, shell, alerts, long operations); `:` runs any `legion` command below it, `E` edits the selected object, `?` lists the keys.
- `legion logs <TASK|SERVICE> [-c <CONTAINER>] [-f] [--tail <N>]`
- `legion build-logs <BUILD> [-n <NS>] [-f]`
- `legion logs-search <SERVICE|JOB> [--since <DUR>] [--until <DUR>] [TEXT]`
- `legion exec <TASK|SERVICE> [-c <CONTAINER>] [-i] [-t] -- <CMD...>`
- `legion files <TASK|SERVICE> [PATH]` / `legion cp <SRC> <DEST>`
- `legion port-forward <TASK|SERVICE> <[LOCAL:]REMOTE>...`
- `legion events [KIND/NAME] [-n <NS>] [-w]`
- `legion activity [ID|NAME] [-w] [-o <table|yaml|json>]` (drains, Volume moves, restores and replicas' whole copies: stage, bytes sent, how long, who asked, how it ended; with an ID, its stages and full output)
- `legion alerts [-n <NS>] [-w] [-o <table|yaml|json>]`
- `legion usage [-n <NS>] [-o <table|yaml|json>]`
- `legion quota [-n <NS>]`
- `legion audit [FILTERS]` (cluster admin only)
- `legion reach [-n <NS>]` (evaluate NetworkPolicy reachability matrix)
- `legion connections [-n <NS>]` (the connections the nodes' pf saw, by service and port)
- `legion topology [-n <NS>]` (each service with its ports, Routes, tasks, Volumes, what may reach it and what was seen)
- `legion load-balancers [-n <NS>]` (CARP holders and states)
- `legion volume <move|backup|restore|snapshots|snapshot|rollback|rm-snapshot>`

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
