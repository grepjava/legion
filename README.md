# Legion

A container orchestrator for FreeBSD: Docker Swarm's operating model plus
selected Kubernetes features, running workloads as jails through Podman and
`ocijail`. The plan and its rationale are in [docs/DESIGN.md](docs/DESIGN.md).

Status: phases 1 (single node) and 2 (cluster) are done; phase 3 (K8s-lite)
is in progress. [PHASE.md](PHASE.md) tracks what is built and what is next.

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
```

The daemon listens on `127.0.0.1:7470` without TLS; set `--listen`, or point the
CLI elsewhere with `--server` or `LEGION_SERVER`. Without `--no-agent`, on
FreeBSD and as root, it also runs the agent with its own Podman service, so
applied services run as pods of jails on the node (`legion list tasks -n
shop` shows each pod's address on the node's `legion0` bridge). Pods reach
each other and the host, and find services by name (`web`, `web.shop` or
`web.shop.legion`) through the node's resolver on the bridge. Give it
`--zfs-root <pool>/legion` to run tasks that use volumes. Service VIPs,
published ports and NAT for pods need the host's `/etc/pf.conf` to call
Legion's anchors:

```
nat-anchor "legion/*"
rdr-anchor "legion/*"
anchor "legion/*"
```

More nodes join with a token. Managers must listen where the others can
reach them, for example `--listen 0.0.0.0:7470 --advertise 10.0.0.1:7470`.
A worker runs only the agent, which follows its work through any manager
and fails over to the others. Pods on different nodes reach each other over
a WireGuard mesh (UDP port 51820, which the hosts must let through); each
node's `--advertise` address is where the others send to:

```sh
legion join-token manager            # or worker; on a member
legion join <token> --manager 10.0.0.1:7470 --advertise 10.0.0.2:7470
legion daemon --listen 0.0.0.0:7470 --advertise 10.0.0.2:7470

legion join <worker-token> --manager 10.0.0.1:7470    # on a worker
legion daemon --advertise 10.0.0.3
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
and ports. A namespace's `quota`, which only the cluster admin sets, caps
what its tasks request, how many run, and its volumes; tasks beyond it
wait as `pending`:

```yaml
kind: Namespace
metadata: { name: shop }
spec:
  quota: { cpu: 4, memory: 8Gi, tasks: 20, volumes: 5, storage: 100Gi }
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

## Layout

| Crate | Role |
|-------|------|
| `legion-api` | Object model: envelope, typed specs, admission |
| `legion-store` | openraft state machine on redb, watches |
| `legion-proto` | gRPC definitions (compiled with protox) |
| `legion-manager` | API server, Raft transport |
| `legion-podman` | libpod REST client; supervises Legion's own Podman service |
| `legion-sys` | FreeBSD operations: jails, interfaces, routes, WireGuard, rctl, pf, ZFS |
| `legion-agent` | Registers the node; runs assigned tasks as pod jails |
| `legion-net` | Pod bridge and interfaces, the WireGuard mesh, the node resolver, pf load balancing |
| `legion-pki` | Cluster CA, node and user certificates, identities |
| `legion` | Daemon and CLI |

## Licence

BSD-2-Clause; see [LICENSE](LICENSE).
