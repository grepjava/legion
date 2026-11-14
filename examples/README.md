# Examples

FreeBSD applications on Legion, each a compose file and the Containerfile
of its image, built from FreeBSD's packages with
[jailer](https://github.com/grepjava/jailer). The qualification runs every
one of them on three nodes as written here
(`the_examples_run_as_their_readmes_say` in
`crates/legion/tests/examples.rs`).

To try one, build its image, load it on every node (or push it to a
registry and set `IMAGE`), and deploy it:

```sh
jailer build -t examples/nginx:1 examples/static-site
jailer export examples/nginx:1 /var/tmp/nginx.oci.tar
legion image load /var/tmp/nginx.oci.tar     # on each node
legion stack deploy site -c examples/static-site/compose.yaml
```

| Example | What it shows | Check |
|---|---|---|
| [static-site](static-site/compose.yaml) | nginx from packages, two tasks behind a VIP, its page and configuration from Configs, a health check, memory and process limits | `fetch -qo - http://<any node>:30081/` |
| [counter](counter/compose.yaml) | Redis keeping its data on a Volume, found by name through its VIP from a web tier of FreeBSD's base tools; the data moves with `legion volume move` | `fetch -qo - http://<any node>:30082/` answers `hits <n>`, one more each time |

[console-plugin](console-plugin/plugin.yaml) is a page for the web console
rather than an application: a Config the console shows in its side bar,
listing the namespace's Services and their ready tasks
(`legion apply -f examples/console-plugin/plugin.yaml`).

A stateful database with replicas, backups and a restore onto new hosts
is in `crates/legion/tests/stateful.rs` (Postgres, its image built from
`crates/legion/tests/stateful/Containerfile`).
