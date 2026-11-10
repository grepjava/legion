# Compose support matrix

What `legion stack deploy` does with each part of a compose v3 file, and
which test shows it (PLAN.md phase 8). How the conversion works is in
DESIGN.md section 8; this page is the table.

Two kinds of evidence:

- **Converted**: a unit test turns the key into Legion's objects and checks
  them: `converts_a_stack` and `the_rest_of_the_matrix`, with the narrower
  ones beside them, in `crates/legion-api/src/compose_tests.rs`; and
  `compose_stacks` in `crates/legion/tests/cli.rs` for the CLI, against a
  daemon.
- **Run**: the qualification's live test deploys a compose file using the
  key, on three FreeBSD hosts, and the app works through an update and the
  loss of a node: `app_updates_and_survives_losing_a_node`
  (`crates/legion/tests/cluster.rs`, its file
  `crates/legion/tests/cluster/compose.yaml`).

A key not marked Run is converted and checked, but no live test runs a
compose file with it; its Legion equivalent may still be run by other
tests in native YAML.

## Services

| Key | Legion | Converted | Run |
|---|---|---|---|
| `image` | container image | yes | yes |
| `command` | container `args`; a string is split as a shell would | yes | yes |
| `entrypoint` | container `command` | yes | |
| `environment` (list or map), `env_file` | container `env` | yes | yes |
| `working_dir`, `user` | container fields | yes | |
| `ports`, short and long, `mode: host` | endpoint ports; the VIP port is the target, published ports on every node | yes | yes |
| `ports` with a host address | the address is ignored, with a warning | yes | |
| `ports` with a range | refused | yes | |
| `expose` | container ports only | yes | |
| `volumes`, named | a Volume mount | yes | |
| `volumes`, bind mount or anonymous | ignored, with a warning; `--strict` refuses | yes | |
| `tmpfs`, and `volumes` of type tmpfs | a tmpfs volume, read-only kept | yes | |
| `secrets` | Secret mounts, at `/run/secrets/<source>` unless given | yes | |
| `configs` | Config mounts, at `/<source>` unless given | yes | yes |
| `healthcheck` (`CMD`, `CMD-SHELL`, a string) | an exec probe for liveness and readiness, Docker's 30 s defaults | yes | yes |
| `stop_grace_period` | the pod's grace period | yes | |
| `cap_add: [NET_RAW]` | `security.allow: [rawSockets]` | yes | |
| `cap_add`, other capabilities, `privileged`, `devices`, `network_mode: host` | ignored, with a warning; `--strict` refuses | yes | |
| `depends_on`, `restart` | ignored, with a warning, as `docker stack deploy` does | yes | |
| `build` without `image` | refused: build with jailer and push | yes | |
| `x-legion` | merged into the Service spec: probes, init containers, sidecars, stateful mode, tolerations, endpoints, pull secrets | yes | yes |

## `deploy`

| Key | Legion | Converted | Run |
|---|---|---|---|
| `mode` (`replicated`, `global`), `replicas` | Service mode and replicas | yes | yes |
| `endpoint_mode` | VIP or DNS round robin | yes | |
| `labels` | the Service's labels | yes | |
| `placement.constraints`, `preferences` | placement; constraints Legion cannot read are refused | yes | |
| `resources.limits` (`cpus`, `memory`, `pids`) | limits; Docker sizes are binary | yes | memory, pids |
| `resources.reservations` | requests | yes | |
| `restart_policy` | the pod's restart policy | yes | |
| `update_config` (`parallelism`, `delay`, `order`, `failure_action`, `monitor`) | the Service's update settings | yes | yes |
| `rollback_config` | ignored, with a warning: a rollback uses the restored spec's update settings | yes | |

## Top level

| Key | Legion | Converted | Run |
|---|---|---|---|
| `volumes` | Volumes; `x-legion` sets a quota or another driver | yes | |
| `volumes` with `external: true` | referred to, not created | yes | |
| `secrets` (`file`, `environment`, `content`) | Secrets | yes | |
| `configs` (`file`, `environment`, `content`) | Configs | yes | yes (`content`) |
| `secrets`, `configs` with `external: true` | referred to, not created | yes | |
| `networks` | ignored, with a warning: not separated yet (DESIGN.md 5.6) | yes | |
| `x-*` fields and YAML merge keys | merged before conversion | yes | |
| `${VAR}`, `${VAR:-d}`, `${VAR-d}`, `${VAR:?e}`, `$VAR`, `$$` | interpolated from the CLI's environment, no `.env` file | yes | `${VAR:-d}`, `$$` |

## Stacks

| Command | Converted | Run |
|---|---|---|
| `stack deploy`, again unchanged, `--strict`, `--prune` | `compose_stacks` | `deploy` |
| `stack convert` | `compose_stacks` | |
| `stack rm`: Services, Secrets, Configs go, Volumes stay | `compose_stacks` | |
| A user with a role in the namespace deploys a stack | `users_get_their_roles` (cli.rs) | |
