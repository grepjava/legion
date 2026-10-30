# Legion, Docker Swarm and Kubernetes

How Legion's features compare with Docker's cluster mode (Swarm) and with
Kubernetes. Legion takes Swarm's way of running a cluster (one binary,
built-in Raft and PKI, `init` and `join`, compose files) and adds the parts
of Kubernetes that teams sharing a cluster need (namespaces, RBAC, network
policy, quotas, jobs, stateful services, autoscaling). It runs only on
FreeBSD, with jails instead of Linux containers. [DESIGN.md](DESIGN.md)
has the design and [PHASE.md](../PHASE.md) what is built.

In the tables, **Add-on** means Kubernetes defines the feature but a
cluster needs a separate component to provide it (a CNI plugin, an
ingress controller, metrics-server and so on). Legion's column describes
what is built today, not what is planned.

## In short

| | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Hosts | FreeBSD 15 and newer (amd64, aarch64) | Linux, Windows | Linux (Windows workers) |
| Workloads | Jails, run by Legion itself (no Docker, containerd or Podman); their root file systems come from OCI images pulled from any registry into ZFS. Linux images run in jails through FreeBSD's Linux compatibility layer | Linux and Windows containers | Linux and Windows containers |
| What you install | One binary on every node | Docker Engine on every node | Control plane (API server, etcd, scheduler, controllers), kubelet, a container runtime, a CNI plugin, then add-ons |
| Configuration | YAML objects, or compose files as stacks | Compose files, `docker service` flags | YAML objects, Helm charts, operators |
| Extensible API | No (fixed kinds) | No | Yes (CRDs, operators, webhooks) |
| Scale it is built for | Up to 256 nodes (one /24 of the pod network each) and about 20,000 tasks | Hundreds of nodes | Thousands of nodes |
| Maturity | New; not yet released or used in production | Mature, in maintenance | Mature, very widely used |

Choose Legion to run services on FreeBSD hosts with ZFS, pf and jails,
without assembling a stack of components. Choose Swarm or Kubernetes for
Linux hosts, an ecosystem of tools and charts, or proven production use.

## Cluster and control plane

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Set-up | `legion init`; `legion join <token>` | `docker swarm init`; `docker swarm join` | kubeadm, a distribution, or a managed service |
| Cluster store | Raft (openraft) built into the daemon, on redb | Raft built into the engine | etcd, a separate process |
| Join tokens | Separate worker and manager tokens; the token holds the CA's digest | Same | Bootstrap tokens (kubeadm) |
| Cluster CA and node certificates | Built in; renewed automatically | Built in; rotated automatically | kubeadm creates them; kubelet client certificates rotate |
| Revoking a node's certificate | No (valid until it expires) | No | No (certificates expire; remove RBAC rights) |
| Store encrypted at rest | Always | Always (Raft log) | Optional (`EncryptionConfiguration`, per resource type) |
| Locking the managers' keys | `legion autolock`; `legion unlock` after a restart | `docker swarm --autolock`; `unlock` | No |
| Backup and restore | `legion backup`; offline `legion restore` | Copy `/var/lib/docker/swarm` offline | etcd snapshot and restore |
| Floating API address | Built in (CARP, `--vip`) | No (external load balancer) | No (external load balancer or keepalived) |
| Promoting and demoting managers | No (the role comes from the join token) | `docker node promote`, `demote` | Not applicable (control-plane nodes are set up as such) |
| Node health | Heartbeats every 2 s; `suspect` after 6 s takes its tasks out of load balancing; `down` after 16 s replaces them | Heartbeats; down after missed heartbeats | Node leases; tasks evicted after a timeout (5 min by default) |
| Drain and pause | `legion node drain`, `pause`, `activate` | `docker node update --availability` | `kubectl drain`, `cordon`, `uncordon` |

## Workloads

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Unit of scheduling | Pod: containers sharing a network stack, each its own jail | Task: one container | Pod |
| Sidecars | Yes (more containers in the pod) | No | Yes |
| Init containers | Yes | No | Yes |
| Replicated services | Yes | Yes | Deployment |
| One task per node | `mode: global` | `--mode global` | DaemonSet |
| Stateful services | `mode: stateful`: names `<svc>-<n>`, a volume per ordinal, ordered start and updates | No | StatefulSet |
| Jobs | Replicated and global, with completions, parallelism, backoff and deadline | `replicated-job`, `global-job` | Job (no global form) |
| Scheduled jobs | CronJob (UTC), with concurrency policy | No | CronJob |
| Rolling updates | Parallelism, delay, `start-first` or `stop-first`, failure action, monitor window | Same | maxSurge, maxUnavailable |
| Rollback | Automatic on failure, or `legion rollback` | Automatic, or `docker service rollback` | `kubectl rollout undo` |
| Disruption budgets | `maxUnavailable` on the service, honoured by drains and updates | No | PodDisruptionBudget |
| Autoscaling | Built in: CPU and memory targets from rctl, no metrics server | No | HorizontalPodAutoscaler, needs metrics-server (add-on) |
| Probes | Startup, liveness and readiness: exec, TCP, HTTP, gRPC | One healthcheck (exec) | Startup, liveness and readiness: exec, TCP, HTTP, gRPC |
| Restart policy | `any`, `on-failure`, `none`, with delay and attempts | Same | Always, OnFailure, Never |
| Private registries | `imagePullSecrets` | `--with-registry-auth` | `imagePullSecrets` |
| Building images | No: buildah or Podman on FreeBSD ([IMAGES.md](IMAGES.md)) | `docker build` | No (external tools) |
| Linux images | Run through FreeBSD's Linux layer (`linux64`) on nodes started with `--linux`, not on a Linux kernel | Native | Native |

## Scheduling

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Requests (for placement) and limits (enforced) | Yes; limits through rctl | Reservations and limits | Requests and limits |
| Nodes without enforceable limits | Pods with limits are not placed on them (no `kern.racct`) | Not applicable | Not applicable |
| Constraints on node labels and attributes | Yes | Yes | nodeSelector, node affinity |
| Spreading over a label (such as zone) | `spread` preferences, nested | `--placement-pref spread` | topologySpreadConstraints |
| Affinity and anti-affinity between pods | No | No | Yes |
| Taints and tolerations | `NoSchedule`, `PreferNoSchedule`, `NoExecute` | No | Yes |
| Most replicas per node | No (spread one per node by default) | `--replicas-max-per-node` | Through spread constraints or anti-affinity |
| Priority and preemption | No | No | PriorityClass |

## Networking

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Pod network | Routed: one subnet per node, a WireGuard mesh between nodes | VXLAN overlay networks | Add-on (a CNI plugin: routed, overlay or eBPF) |
| Encryption between nodes | Always (WireGuard) | Optional per network (IPsec) | Depends on the CNI plugin |
| IPv6 | Dual-stack: every address has an IPv6 twin | Optional per network | Dual-stack |
| Service virtual IPs | Yes, balanced by pf | Yes, by IPVS | Yes, by kube-proxy or the CNI plugin |
| DNS round robin instead of a VIP | `endpoint: { mode: dnsrr }` | `--endpoint-mode dnsrr` | Headless Services |
| Service DNS | `<svc>`, `<svc>.<ns>`, `<svc>.<ns>.legion`, AAAA too | Service names on shared networks | `<svc>.<ns>.svc.cluster.local` |
| Publishing a port on every node | `publish: ingress` (routing mesh) | Ingress routing mesh | NodePort |
| Publishing on the node running the task | `publish: host` | `mode=host` | hostPort |
| External load balancer integration | No | No | LoadBalancer Services (cloud or MetalLB) |
| HTTP routing by host and path | `Route` objects, served by a proxy in the agent; HTTPS by SNI from Secrets | No | Ingress or Gateway API, with a controller (add-on) |
| HTTP/2 to backends, automatic certificates (ACME) | No | Not applicable | Depends on the controller |
| Namespace isolation by default | Yes (a namespace's tasks reach only each other, unless `open`) | Through separate networks | No (everything reaches everything) |
| Network policy | Ingress and egress by namespace, labels, ports and CIDR; enforced by pf | No | NetworkPolicy, enforced by the CNI plugin |
| Nodes behind NAT | Workers, reached through their keepalives; not two nodes behind different NATs | Needs reachable addresses | Depends on the CNI plugin |

## Storage

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Local volumes | ZFS datasets, a size as quota | Local driver | hostPath, local PersistentVolumes |
| Network volumes | NFS, iSCSI (UFS on the LUN), plugins as executables | Volume plugins | CSI drivers (add-on) |
| Per-replica volumes | From claims in stateful services | No | volumeClaimTemplates |
| Moving a volume between nodes | `zfs send` when a node drains, or `legion volume move` | No | Only with network storage |
| Snapshots | Before each update of a stateful service; rolled back with it | No | VolumeSnapshot, with a CSI driver |
| Scratch space in memory | `tmpfs` volumes | `--mount type=tmpfs` | `emptyDir: { medium: Memory }` |

## Configuration and secrets

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Secrets | Files on a tmpfs per task, or environment variables | Files on a tmpfs | Files or environment variables |
| Configs | Yes | Yes | ConfigMaps |
| Compose files | `legion stack deploy`; `stack convert` writes native YAML | `docker stack deploy` | No (Kompose converts) |
| Server-side diff | `legion diff` | No | `kubectl diff` |

## Multi-tenancy and security

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Namespaces | Yes; scope names, DNS, quotas, RBAC and isolation | No | Yes |
| Users and roles | Certificates per user; `view`, `edit`, `admin` per namespace | No (one admin) | RBAC with custom roles; many authentication methods |
| Custom roles | No | No | Yes |
| Quotas | CPU, memory, tasks, volumes and storage per namespace | No | ResourceQuota, LimitRange |
| Workload isolation | A jail per container, with its own network stack | Linux namespaces and cgroups | Linux namespaces and cgroups |
| Hardened by default | No raw sockets, chflags, mlock, SysV IPC or mounts; securelevel 2 | Default capabilities, seccomp profile | Depends on Pod Security Admission |
| Loosening per workload | `security.allow`, only as far as the namespace permits | `--cap-add` and similar | securityContext, limited by Pod Security levels |
| Admission webhooks | No | No | Yes |

## Operations

| Feature | Legion | Docker Swarm | Kubernetes |
|---|---|---|---|
| Logs | `legion logs`, through any manager; a finished task's output is kept | `docker service logs` | `kubectl logs` |
| Starting a workload from the command line | No: workloads are objects sent with `legion apply` or `legion stack deploy` | `docker service create`, and `docker run` on one node | `kubectl run`, `kubectl create deployment` |
| Exec into a task | `legion exec`, through any manager | Only on the node (`docker exec`) | `kubectl exec` |
| Port forwarding | `legion port-forward` | No | `kubectl port-forward` |
| Watching objects | `legion watch` | `docker events` | `kubectl get --watch` |
| Live view in the terminal | `legion top`: nodes, services and Jobs, tasks with their CPU and memory, and a log of changes | `docker stats` (one node), `docker service ps` | `kubectl top`; k9s and others, separately |
| Web UI | No; a Grafana dashboard for its metrics | No; Portainer and others, separately | The Kubernetes Dashboard, an add-on |
| Metrics | Prometheus endpoint: cluster state, and each container's CPU, memory and I/O; a Grafana dashboard | Engine metrics endpoint | metrics-server, kube-state-metrics, cAdvisor in kubelet |
| Packaging | rc.d script under daemon(8), newsyslog entry, release tarballs built by a script, a FreeBSD port (not yet in the ports tree); no published release yet | Distribution packages | Distribution packages, many installers |
| Running containers by hand beside the cluster | Yes; Legion touches only its own jails and pf anchors | Yes | Not recommended |

## Not in Legion

Things Swarm or Kubernetes have that Legion does not, and is not
planning soon:

- Linux or Windows hosts, and any Linux kernel feature that FreeBSD's
  Linux layer does not provide.
- Custom resources, operators, admission webhooks, Helm or the Kubernetes
  API (non-goals; see [DESIGN.md](DESIGN.md) §2).
- Custom roles, pod affinity and anti-affinity, priorities and preemption.
- Integration with external load balancers, and certificates from ACME.
- Revoking certificates, and promoting or demoting managers after they
  join.
- A relay for nodes that are both behind NAT, and IPv6 NAT to the outside.
- Virtual machines as tasks: bhyve was tried but is not a runtime yet.
