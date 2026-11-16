# Legion CLI Reference

`legion` is the single binary for the Legion daemon, cluster CLI, OCI image builder (`legion build`), and standalone local jail/VM runner (`legion local`, also invokable via a `jailer` symlink).

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
- `--jailer <BIN>` (defaults to `<current_exe> local`), `--jailer-arg <ARG>`

### `legion init`
Initialize a new cluster with the reached manager as its first member.

### `legion join-token <manager|worker> [--rotate]`
Print (or rotate) the join token for a manager or worker node.

### `legion join <TOKEN> <MANAGER> [OPTIONS]`
Join an existing cluster before starting the daemon (`--data-dir`, `--advertise`, `--node-name`).

### `legion status`
Show the reached manager's view of the cluster (managers, Raft leader, nodes).

### `legion doctor [--zfs-root <DATASET>] [--json]`
Check whether this host satisfies Legion's prerequisites without changing anything (run as `root` to inspect `pf` rules). Exits nonzero if a required check fails or cannot be read.

### `legion diagnose [--node <NAME>] [--json]`
Run `doctor` checks across all cluster nodes (or a single `--node`). Cluster admin only.

### `legion support-bundle [-o <FILE>]`
Export a masked cluster diagnostics bundle (objects without Secrets, masked env values, alerts, recent events, node diagnoses). Cluster admin only.

### `legion backup <FILE>` / `legion restore [FILE]`
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
- `drain <NODE>`, `pause <NODE>`, `activate <NODE>`
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

### `legion watch [KIND] [-n <NS>]`
Stream live `PUT`/`DELETE` object events.

### `legion stack <SUBCOMMAND>`
- `deploy <STACK> -c <COMPOSE_FILE> [-n <NS>]`
- `convert -c <COMPOSE_FILE>` (print Legion YAML)
- `ls [-n <NS>]`, `stop <STACK>`, `start <STACK>`, `rm <STACK>` (keeps Volumes)

### `legion stop <SERVICE>` / `legion start <SERVICE>`
Stop a service's tasks while preserving its spec and replica count, or start it again.

### `legion restart <SERVICE>` / `legion rollback <SERVICE>`
Trigger a rolling restart (`restartedAt`) or roll back to the previous spec.

### `legion cronjob run <CRONJOB> [-n <NS>]`
Trigger a CronJob immediately outside its schedule.

---

## Observability, Storage, Network & Sessions

- `legion top [--once]`: Live terminal dashboard (nodes, workloads, tasks, resource graphs, logs, shell, alerts).
- `legion logs <TASK|SERVICE> [-c <CONTAINER>] [-f] [--tail <N>]`
- `legion build-logs <BUILD> [-n <NS>] [-f]`
- `legion logs-search <SERVICE|JOB> [--since <DUR>] [--until <DUR>] [TEXT]`
- `legion exec <TASK|SERVICE> [-c <CONTAINER>] [-i] [-t] -- <CMD...>`
- `legion files <TASK|SERVICE> [PATH]` / `legion cp <SRC> <DEST>`
- `legion port-forward <TASK|SERVICE> <[LOCAL:]REMOTE>...`
- `legion events [KIND/NAME] [-n <NS>] [-w]`
- `legion alerts [--watch]`
- `legion usage [-n <NS>] [-o <table|yaml|json>]`
- `legion quota [-n <NS>]`
- `legion audit [FILTERS]` (cluster admin only)
- `legion reach [-n <NS>]` (evaluate NetworkPolicy reachability matrix)
- `legion load-balancers [-n <NS>]` (CARP holders and states)
- `legion volume <move|backup|restore|snapshots|snapshot|rollback|rm-snapshot>`

---

## Cluster Image Commands (`legion image`)

- `legion image load <FILE> [--zfs-root <DATASET> | --node <NODE>]`
- `legion image ls [--node <NODE>]`
- `legion image pull <REF> [--node <NODE>]`
- `legion image push <REF> --node <NODE> <TARGET>`
- `legion image rm <REF> [--node <NODE>]`
- `legion image prune [--node <NODE>] [--builds]`
- `legion image scan <REF> --node <NODE>` (FreeBSD `pkg audit` vulnerability check)
- `legion image export <REF> --node <NODE> <FILE>`
- `legion image search <QUERY>`
- `legion image sbom <REF>` / `legion image inspect <REF>`

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
- Global store flags: `--state-dir <DIR>`, `--zfs-root <DATASET>`, `--no-zfs`

---

## Local Standalone Mode (`legion local`, alias `jailer`)

All standalone local image, build, signing, and container commands live under `legion local` (also reachable as `legion jailer` or by invoking a `jailer` symlink to `legion`):

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

Local global options: `--state-dir <DIR>` (`JAILER_STATE_DIR`), `--zfs-root <DATASET>` (`JAILER_ZFS_ROOT`), `--no-zfs` (`JAILER_NO_ZFS`), `--pod-subnet <CIDR>` (`JAILER_POD_SUBNET`), `--plain-http <HOST[:PORT]>`.
