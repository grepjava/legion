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
legion daemon --data-dir /tmp/legion &
legion init
cat <<'EOF' | legion apply -f -
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop, labels: { app: web } }
spec: { replicas: 3 }
EOF
legion list svc -A
legion get svc web -n shop
legion watch Service
```

The daemon listens on `127.0.0.1:7470` without TLS; set `--listen`, or point the
CLI elsewhere with `--server` or `LEGION_SERVER`.

## Layout

| Crate | Role |
|-------|------|
| `legion-api` | Object envelope, keys, validation |
| `legion-store` | openraft state machine on redb, watches |
| `legion-proto` | gRPC definitions (compiled with protox) |
| `legion-manager` | API server, Raft transport |
| `legion` | Daemon and CLI |
