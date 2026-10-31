# Images for Legion

Legion runs containers from OCI images, the same format Docker and
Kubernetes use, but it does not build them. You build images with the
usual tools, push them to a registry or copy them to the nodes, and
Legion pulls or loads them. This guide covers where to get FreeBSD
images, how to build your own, how to get them onto a cluster, and what
to expect from Linux images.

## What Legion runs

- **FreeBSD images** run natively: each container is a jail whose root
  file system is the image (DESIGN.md §6.1).
- **Linux images** run on nodes started with `legion daemon --linux`,
  through FreeBSD's Linux compatibility layer. That is not a Linux kernel;
  see [Linux images](#linux-images) below.

When an image is published for several systems, Legion takes the
`freebsd/<arch>` entry first, then `linux/<arch>` on a node started with
`--linux`. Images for any other system are refused.

## Base images

FreeBSD publishes official base images on Docker Hub. The ones Legion's
own tests use:

| Image | What is in it | Use it for |
|---|---|---|
| `docker.io/freebsd/freebsd-runtime:15.1` | A minimal userland: shared libraries, `sh` and basic tools | Small images for programs you copy in |
| `docker.io/freebsd/freebsd-notoolchain:15.1` | The base system without compilers, with `pkg` | Installing packages with `pkg install` |

Match the base image's release to the nodes' FreeBSD release or an older
one: a jail shares the host's kernel, and programs built for a newer
release may not run on an older kernel.

## Building a FreeBSD image

Build on a FreeBSD machine with buildah or Podman, both in the ports tree
(`pkg install buildah` or `pkg install podman`). Building does not need a
Legion node, and is best done on a build machine or in CI rather than on
a node.

A Containerfile is written as for Docker. This one, from Legion's
stateful-service test (`crates/legion/tests/stateful/Containerfile`),
installs PostgreSQL and removes what the server does not need at run
time:

```dockerfile
FROM docker.io/freebsd/freebsd-notoolchain:15.1
RUN env ASSUME_ALWAYS_YES=yes pkg bootstrap -f >/dev/null \
 && env ASSUME_ALWAYS_YES=yes pkg install -y postgresql17-server >/dev/null \
 && pkg delete -fy llvm19 python312 perl5 >/dev/null \
 && rm -f /usr/local/lib/postgresql/llvmjit* \
 && pkg clean -ay >/dev/null && rm -rf /var/db/pkg/repos /var/cache/pkg/*
USER postgres
```

Build it:

```sh
podman build --network host -t registry.example/shop/pg:17 .
```

`--network host` gives the `RUN` steps the host's network, which `pkg`
needs to fetch packages.

### With jailer

jailer builds the same Containerfiles without buildah or Podman, and
checks the result against what Legion runs: that the `USER` is in the
image's `/etc/passwd`, that the entrypoint exists and is executable, and
that the stop signal is one Legion knows. It warns about `pkg` caches
left in the image and Containerfile settings Legion ignores. It can also
run the image in a jail set up as Legion sets one up, push it, and save
it for `legion image load`. As root, since `RUN` steps and test runs are
jails:

```sh
doas jailer build --strict -t pg:17 .
doas jailer run pg:17                               # try it in a jail
doas jailer push pg:17 registry.example/shop/pg:17
doas jailer export pg:17 pg.tar                     # or save it for loading
```

`--strict` fails the build when a check fails. A rebuild reuses the
`RUN` steps whose inputs did not change.

Some tips:

- **Keep images small.** Remove what the program does not need at run
  time (compilers, scripting languages pulled in as build dependencies,
  `pkg`'s caches), in the same `RUN` step that added it, as above.
  Smaller images pull faster to every node.
- **Set a user.** `USER` works as in Docker. Legion reads the user and
  its groups from the image's `/etc/passwd` and `/etc/group`.
- **`CMD` and `ENTRYPOINT`** work as in Docker. A Legion pod's `command`
  and `args` replace them, as in Kubernetes.
- **Health checks** in the Containerfile are not used. Give the pod
  probes instead (or, from a compose file, `healthcheck:`, which Legion
  turns into probes).

### Several architectures

Legion runs on amd64 and aarch64 nodes, and picks the entry for its own
architecture from an image index. Build the image on (or for) each
architecture and publish both under one name, for example with
`buildah manifest create` and `buildah manifest push --all`. A
single-architecture image runs only on nodes of that architecture.

## Getting an image to the cluster

### Through a registry

Push the image to any registry that speaks the OCI distribution API:
Docker Hub, GitHub's or GitLab's container registry, Harbor, a cloud
provider's registry, or one you run yourself.

```sh
podman push registry.example/shop/pg:17
```

Then name it in a pod. Each node pulls it the first time it needs it:

```yaml
pod:
  containers:
    - name: db
      image: registry.example/shop/pg:17
```

An image name without a registry means Docker Hub, as in Docker
(`nginx:1.27` is `docker.io/library/nginx:1.27`).

### From a private registry

Put a Docker login in a Secret of the pod's namespace, in the format
`docker login` writes to `~/.docker/config.json`, and name it in the pod's
`imagePullSecrets`:

```yaml
apiVersion: legion/v1
kind: Secret
metadata: { name: registry-login, namespace: shop }
spec:
  text: |
    {"auths": {"registry.example": {"username": "ci", "password": "..."}}}
---
# in the Service's pod
pod:
  imagePullSecrets: [registry-login]
  containers:
    - name: db
      image: registry.example/shop/pg:17
```

`auth` (base64 of `user:password`) works in place of `username` and
`password`; credential helpers do not. An image pulled with a login is
kept private to the namespaces that pulled it: another namespace pulls it
again, with its own login, and the registry decides.

### Without a registry

Save the image as an OCI archive and load it on each node that may run
it, as root:

```sh
podman save --format oci-archive -o pg.tar localhost/pg:17
legion image load pg.tar --zfs-root zroot/legion   # the daemon's --zfs-root
```

Name such images `localhost/...`: Legion never tries to pull those, so a
node without the image fails the task instead of asking a registry.
(`localhost:5000/...`, with a port, is a registry on that port, and is
pulled as any other.)
Load the image on every node that may run the service, or keep the
service to those nodes with a placement constraint:

```yaml
placement:
  constraints: ["node.labels.images == local"]
```

`legion node label n1 images=local` sets that label. `legion image load`
works beside a running daemon, and `--name` loads the image under
another name than the archive gives.

## Linux images

A node started with `legion daemon --linux` also runs Linux images, such
as `alpine` and `debian` from Docker Hub, through FreeBSD's Linux
compatibility layer (linux(4)). Both musl and glibc programs run; Legion
gives such containers `/proc`, `/sys`, `/dev/fd` and `/dev/shm` as Linux
programs expect them.

In a cluster where only some nodes run Linux images, ask for them:

```yaml
placement:
  constraints: ["node.platform.linux == true"]
```

Many Linux images run unchanged. These do not:

- **Images that need Linux kernel features FreeBSD's layer does not
  provide:** namespaces, cgroups, seccomp, io_uring, eBPF. Images that
  run containers or sandboxes of their own (Docker-in-Docker, some
  browsers, some build tools) are among them.
- **Programs that read Linux-only files under `/proc` or `/sys`**, for
  example some monitoring agents, may fail or report wrong values.

If a program is in FreeBSD's ports, a FreeBSD image is the better choice:
it runs natively, without the compatibility layer.

## Removing images

Nodes remove images nothing has used for an hour, checked every ten
minutes, then any layers no image uses. There is nothing to clean up by
hand.
