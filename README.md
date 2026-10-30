# Legion

A container orchestrator for FreeBSD: Docker Swarm's operating model plus
selected Kubernetes features, running workloads as jails from OCI images,
which it pulls into ZFS and runs itself. The plan and its rationale are in
[docs/DESIGN.md](docs/DESIGN.md); how it compares with Docker Swarm and
Kubernetes is in [docs/COMPARISON.md](docs/COMPARISON.md), and how to build
and ship images for it in [docs/IMAGES.md](docs/IMAGES.md).

Status: phases 1 (single node), 2 (cluster), 3 (K8s-lite: namespaces, RBAC,
network policy, quotas, jobs, security profiles, metrics), 4 (stateful
services and operations) and 5 (Legion's own jail runtime, Linux images,
IPv6, network volumes) are done; it is being hardened for real use.
[PHASE.md](PHASE.md) tracks what is built and what is next.

## Install

On FreeBSD 15, from a release tarball (or one made with
`packaging/freebsd/release.sh`; a port is in `packaging/freebsd/port`), as
root:

```sh
tar -xf legion-<version>-freebsd15-amd64.tar.xz --strip-components 1 -C /usr/local
sysrc -f /boot/loader.conf kern.racct.enable=1   # rctl limits; reboot once
zfs create zroot/legion       # images, containers and volumes; must be mounted
sysrc legion_enable=YES legion_args="--zfs-root zroot/legion"
service legion start
legion init                   # on the first manager; other nodes join (below)
```

`/etc/pf.conf` must call Legion's anchors (see below). The daemon logs to
`/var/log/legion.log`, which newsyslog rotates; `service legion stop`
leaves the pods running, and the next start takes them over.

## Build

Rust 1.90 or newer. No `protoc` is needed.

```sh
cargo build --release
cargo test --workspace
```

## Try it

```sh
legion daemon --data-dir /tmp/legion --no-agent &
export LEGION_IDENTITY=/tmp/legion/admin.pem   # written by the daemon
legion init
cat <<'EOF' | legion apply -f -
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop, labels: { app: web } }
spec:
  replicas: 3
  pod:
    containers: [{ name: web, image: nginx:1.27 }]
EOF
legion list svc -A
legion get svc web -n shop
legion watch Service
legion top
```

`legion top` shows the cluster live in the terminal: nodes, workloads,
tasks, volumes, routes and policies, namespaces and their quotas, Secrets
and Configs, with graphs of what tasks use, each object's details and
YAML, and a log of what changes. Keys drain, pause or label nodes, scale
or roll back services, suspend cron jobs, move volumes, delete objects,
follow logs in a pane under the table, and open a shell in a task; `/` filters and `?` lists the
keys (`legion top --once` prints it as text). For dashboards, Prometheus can scrape each daemon's
`--metrics-listen` endpoint; [packaging/grafana](packaging/grafana) has a
Grafana dashboard for it.

The daemon serves the API on port 7470 of every address, with mutual TLS:
every call but joining needs a certificate of the cluster's, as
`admin.pem` holds. Point the CLI at another manager with `--server` or
`LEGION_SERVER`. Without `--no-agent`, on
FreeBSD and as root, it also runs the agent, so applied services run as
pods of jails on the node (`legion list tasks -n shop` shows each pod's
address on the node's `legion0` bridge). The agent runs them itself: it
pulls OCI images into ZFS and runs each container as a jail on a clone of
its image, so it needs `--zfs-root <pool>/legion`, where images,
containers and volumes live (`legion image load` adds images no registry
serves). Pods reach each other and the host, and find services by
name (`web`, `web.shop` or `web.shop.legion`) through the node's resolver
on the bridge. Service VIPs,
published ports and NAT for pods need the host's `/etc/pf.conf` to call
Legion's anchors:

```
nat-anchor "legion/*"
rdr-anchor "legion/*"
anchor "legion/*"
```

More nodes join with a token. A manager advertises the address of its
default route to the others (or `--listen`'s, when that names one), and a
worker the address it reaches its managers from; `--advertise` chooses
another. A worker runs only the agent, which follows its work through any
manager and fails over to the others. Pods on different nodes reach each
other over a WireGuard mesh (UDP port 51820, which the hosts must let
through), sent to each node's advertised address:

```sh
legion join-token manager            # or worker; on a member
legion join <token> --manager 10.0.0.1:7470    # on the new node, then
service legion start                           # or legion daemon
```

Compose files deploy as stacks. Each stack gets a namespace of its name, in
which services find each other by their compose names; what Legion cannot
honour (bind mounts, `depends_on`, `privileged`, ...) is reported as a
warning, or refused with `--strict`:

```sh
legion stack deploy shop -c compose.yaml
legion stack convert shop -c compose.yaml   # the Legion YAML it becomes
legion rollback web -n shop                 # undo the latest update
legion stack rm shop                        # volumes and their data stay
```

A namespace's tasks are reached only by its own tasks, the nodes and
published ports, unless its `isolation` is `open` (or `locked`, not even
its own); `NetworkPolicy` objects let more in by namespace, service labels
and ports. With `egress`, a policy also limits where the tasks it
selects may send (DNS always works; `egress: []` allows nothing else):

```yaml
kind: NetworkPolicy
metadata: { name: api-out, namespace: shop }
spec:
  services: { app: api }
  egress:
    - to: [{ services: { app: db } }]
      ports: [{ port: 5432 }]
    - to: [{ cidr: 192.0.2.0/24 }]   # a payment provider
      ports: [{ port: 443 }]
```

A namespace's `quota`, which only the cluster admin sets, caps
what its tasks request, how many run, and its volumes; tasks beyond it
wait as `pending`:

```yaml
kind: Namespace
metadata: { name: shop }
spec:
  quota: { cpu: 4, memory: 8Gi, tasks: 20, volumes: 5, storage: 100Gi }
```

Containers' jails get no raw sockets, `chflags`, mlock, SysV IPC or
mounts, at `securelevel` 2 and `enforce_statfs` 2. A container's `security`
loosens that only as far as its namespace allows, which again only the
cluster admin sets:

```yaml
kind: Namespace
metadata: { name: shop }
spec:
  security: { allow: [rawSockets] }   # also chflags, mlock, sysvipc,
                                      # securelevel, enforceStatfs
---
# in a Service's pod
containers:
  - name: probe
    image: "registry.example/shop/netcheck:1"
    security: { allow: [rawSockets] }   # ping works here
```

Jobs run pods to completion, retrying failures; CronJobs make Jobs on a
schedule (UTC):

```yaml
kind: CronJob
metadata: { name: backup, namespace: shop }
spec:
  schedule: "30 2 * * *"
  concurrencyPolicy: forbid
  job:
    completions: 1
    backoffLimit: 3
    activeDeadline: 1h
    pod:
      containers: [{ name: dump, image: "registry.example/shop/tools:1" }]
```

Into the tasks themselves, through any manager; a service name picks one
of its running tasks (a Job's name works too). A finished task's output is
kept until the task is deleted, so `logs` works on a completed Job:

```sh
legion logs web -n shop -f --tail 20
legion exec -it web -n shop -- /bin/sh       # exits with the command's code
legion port-forward web -n shop 8080:80      # localhost:8080 to the pod's 80
```

`admin.pem` is the cluster admin's. People get their own credentials and a
role (`view`, `edit` or `admin`) in a namespace; the key is made where the
command runs:

```sh
legion user create alice --role edit -n shop   # writes alice.pem; hand it over
                                               # (view may read logs; exec needs edit)
legion --identity alice.pem list services -n shop
legion delete RoleBinding alice -n shop        # takes her rights away
```

Nodes started with `--ingress-listen 0.0.0.0:80` (and
`--ingress-tls-listen 0.0.0.0:443`) serve `Route` objects: HTTP by host
and path to a service's ready tasks, HTTPS with a certificate and key
from a Secret the route names. Any node may read those Secrets until
one node is labelled as serving HTTPS; then only labelled nodes may, so
label every node that serves it:

```sh
legion node label n1 legion.ingress=true    # key- removes a label
```

`--metrics-listen 0.0.0.0:7472` on the daemon serves Prometheus metrics
at `/metrics`: Raft, objects, tasks, services, nodes and quotas from the
leading manager, and each container's CPU, memory and I/O from rctl on
every node. It is plain HTTP without authentication, and shows every
namespace's names, so keep it on an admin network.

## Layout

| Crate | Role |
|-------|------|
| `legion-api` | Object model: envelope, typed specs, admission |
| `legion-store` | openraft state machine on redb, watches |
| `legion-proto` | gRPC definitions (compiled with protox) |
| `legion-manager` | API server, Raft transport |
| `legion-image` | OCI images: registry client, ZFS layer store |
| `legion-jail` | The jail runtime: containers as jails on ZFS clones, each under a supervisor |
| `legion-sys` | FreeBSD operations: jails, interfaces, routes, WireGuard, rctl, pf, ZFS |
| `legion-agent` | Registers the node; runs assigned tasks as pod jails |
| `legion-net` | Pod bridge and interfaces, the WireGuard mesh, the node resolver, pf load balancing |
| `legion-pki` | Cluster CA, node and user certificates, identities |
| `legion` | Daemon and CLI |

## Licence

BSD-2-Clause; see [LICENSE](LICENSE).
