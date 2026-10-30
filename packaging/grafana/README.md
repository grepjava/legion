# Grafana dashboard

`legion.json` shows a Legion cluster in Grafana: whether the managers
have a leader, nodes and tasks by state, services short of ready tasks,
container restarts, CPU, memory and storage I/O by task and by node,
quota use, and each node's runtime.

It reads what the daemons serve with `--metrics-listen` (DESIGN.md,
Metrics). Have Prometheus scrape every node, managers and workers:

```yaml
scrape_configs:
  - job_name: legion
    static_configs:
      - targets: ["n1.lan:9470", "n2.lan:9470", "n3.lan:9470"]
```

with each daemon started with, for example,
`--metrics-listen 0.0.0.0:9470`. The endpoint has no authentication and
names every namespace's services and tasks, so keep it on an admin
network.

Only the leading manager reports the cluster (nodes, services, tasks,
quotas), so sums across managers count each thing once. Container usage
comes from each node's agent and is labelled with the node Prometheus
scraped (`instance`).

To add it: in Grafana, Dashboards → New → Import, upload `legion.json`,
and pick the Prometheus data source. The Namespace menu narrows the task,
service, resource and quota panels.
