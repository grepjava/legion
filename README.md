# Legion

A container orchestrator for FreeBSD: Docker Swarm's operating model plus
selected Kubernetes features, running workloads as jails through Podman and
`ocijail`. The plan and its rationale are in [docs/DESIGN.md](docs/DESIGN.md).

Status: phase 1 in progress. The replicated store, the API server and the CLI
work; nothing schedules or runs containers yet.

## Build

Rust 1.90 or newer. No `protoc` is needed.

```sh
cargo build --release
cargo test --workspace
```

## Try it

```sh
legion daemon --data-dir /tmp/legion --no-agent &
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
`web.shop.legion`) through the node's resolver on the bridge; there is no
outbound NAT yet. Give it
`--zfs-root <pool>/legion` to run tasks that use volumes. Service VIPs,
published ports and NAT for pods need the host's `/etc/pf.conf` to call
Legion's anchors:

```
nat-anchor "legion/*"
rdr-anchor "legion/*"
anchor "legion/*"
```

## Layout

| Crate | Role |
|-------|------|
| `legion-api` | Object model: envelope, typed specs, admission |
| `legion-store` | openraft state machine on redb, watches |
| `legion-proto` | gRPC definitions (compiled with protox) |
| `legion-manager` | API server, Raft transport |
| `legion-podman` | libpod REST client; supervises Legion's own Podman service |
| `legion-sys` | FreeBSD operations: jails, interfaces, routes, rctl, pf, ZFS |
| `legion-agent` | Registers the node; runs assigned tasks as pod jails |
| `legion-net` | Pod bridge, pod interfaces and addresses |
| `legion` | Daemon and CLI |

## Licence

BSD-2-Clause; see [LICENSE](LICENSE).
