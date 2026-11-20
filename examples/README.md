# Examples

FreeBSD applications on Legion, each a compose file and the Containerfile
of its image, built from FreeBSD's packages with `legion build`. The
qualification runs every one of them on three nodes as written here
(`the_examples_run_as_their_readmes_say` in
`crates/legion/tests/examples.rs`).

The recipes build on [base](base/Containerfile), Legion's native base
image: FreeBSD's runtime image with `pkg` added (35 MiB on disk). Build
it first, once; rebuilding it, then the recipes, patches them all.

To try one, build the base and its image, load the image on every node
(or push it to a registry and set `IMAGE`), and deploy it:

```sh
legion build -t examples/base:15.1 examples/base
legion build -t examples/nginx:1 examples/static-site
legion local export examples/nginx:1 /var/tmp/nginx.oci.tar
legion image load /var/tmp/nginx.oci.tar     # on each node
legion stack deploy site -c examples/static-site/compose.yaml
```

Each is also a template in the web console: Templates, then Use.

| Example | What it shows | Check |
|---|---|---|
| [postgres](postgres/compose.yaml) | PostgreSQL 17, its data on a Volume made the server's by an init container, the superuser's password from a Secret made of `POSTGRES_PASSWORD`; the namespace must allow `sysvipc` | `legion exec postgres -n <ns> -- env PGPASSWORD=... psql -h 127.0.0.1 -U postgres` |
| [redis](redis/compose.yaml) | Redis, its data on a Volume (append-only), found as `redis` | `legion exec redis -n <ns> -- redis-cli ping` |
| [static-site](static-site/compose.yaml) | nginx from packages, two tasks behind a VIP, its page and configuration from Configs, a health check, memory and process limits | `fetch -qo - http://<any node>:30081/` |
| [node](node/compose.yaml) | Node.js 24, two tasks of a server whose code is a Config; an app's own image is built FROM `examples/node:24` | `fetch -qo - http://<any node>:30083/` |
| [python](python/compose.yaml) | Python 3, two tasks of a server whose code is a Config; an app's own image is built FROM `examples/python:3` | `fetch -qo - http://<any node>:30084/` |
| [counter](counter/compose.yaml) | Redis keeping its data on a Volume, found by name through its VIP from a web tier of FreeBSD's base tools; the data moves with `legion volume move`. Built on FreeBSD's notoolchain image, for `inetd` | `fetch -qo - http://<any node>:30082/` answers `hits <n>`, one more each time |

[console-plugin](console-plugin/plugin.yaml) is a page for the web console
rather than an application: a Config the console shows in its side bar,
listing the namespace's Services and their ready tasks
(`legion apply -f examples/console-plugin/plugin.yaml`).

A stateful database with replicas, backups and a restore onto new hosts
is in `crates/legion/tests/stateful.rs` (Postgres, its image built from
`crates/legion/tests/stateful/Containerfile`).
