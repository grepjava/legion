# Volume plugins

Volume plugins for Legion, to copy or link into a node's plugin directory
(`legion daemon --volume-plugins DIR`, by default
`/usr/local/libexec/legion/volume`). A Volume names one by its file name:

```yaml
apiVersion: legion/v1
kind: Volume
metadata: { name: cache, namespace: shop }
spec:
  driver: plugin
  plugin: { name: hostpath, options: { path: /srv/cache } }
```

Each is a script for `flua`, which FreeBSD's base system has, run by the
agent as `<plugin> mount DIR` and `<plugin> unmount DIR` with
`{"namespace", "volume", "options"}` as JSON on its standard input
(DESIGN.md §6.7). They are installed as examples: none runs until it is in
the plugin directory.

| Plugin | Mounts | Needs | Options |
|---|---|---|---|
| `hostpath` | A directory of the node, with nullfs. Its data is the node's: leave the Volume unshared, so one node uses it at a time. | Nothing | `path` (absolute, existing), `readOnly: "true"` |
| `sshfs` | A directory of another host over SSH. | The `fusefs-sshfs` package; the host's key in root's `known_hosts` | `remote` (`[user@]host:/path`), `identity` (a private key on the node), `port` |
| `rclone` | An rclone remote: S3, B2, WebDAV and the rest. | The `rclone` package; a config naming the remote | `remote` (`remote:path`), `config` (its absolute path on the node, root's only), `cache` (rclone's `--vfs-cache-mode`: `off`, `minimal`, `writes` by default, `full`) |

The FUSE ones load the `fusefs` kernel module themselves.

Tested on FreeBSD 15.1: `hostpath` mounting read-only and read-write,
unmounting, refusing a relative or missing path; `sshfs` and `rclone`
with stand-ins for their programs, checking the commands they run and their
refusals. They have not been run against a real SSH server or object store.
