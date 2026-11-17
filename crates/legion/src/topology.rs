//! `legion topology`: a namespace as the console's Topology page draws it,
//! in text. Each Service with its tasks, what reaches it from outside (its
//! Routes and published ports), the Volumes its pods use, what may reach it
//! (`Cluster.Reach`) and the connections the nodes saw (`Cluster.Connections`).

use legion_api::Object;
use legion_proto::v1 as pb;
use serde_json::Value;

/// What the namespace holds, as read.
pub struct Namespace<'a> {
    pub services: &'a [Object],
    pub tasks: &'a [Object],
    pub routes: &'a [Object],
    pub volumes: &'a [Object],
    pub reach: &'a [pb::ReachEdge],
    /// `None` when the managers could not say (too old, or refused).
    pub seen: Option<&'a [pb::ConnectionEdge]>,
}

fn str_of<'v>(v: &'v Value, path: &[&str]) -> &'v str {
    path.iter()
        .fold(v, |v, p| &v[*p])
        .as_str()
        .unwrap_or_default()
}

/// The lines `legion topology` prints, a block for each service.
pub fn lines(ns: &Namespace) -> Vec<String> {
    let mut services: Vec<&Object> = ns.services.iter().collect();
    services.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    let mut out = Vec::new();
    for s in services {
        let name = s.metadata.name.as_str();
        let (spec, status) = (&s.spec, &s.status);
        let ready = format!(
            "{}/{} ready",
            status["ready"].as_u64().unwrap_or(0),
            status["desired"].as_u64().unwrap_or(0)
        );
        let mut head = format!("{name}  {ready}");
        if spec["stopped"].as_bool() == Some(true) {
            head.push_str(", stopped");
        }
        if spec["paused"].as_bool() == Some(true) {
            head.push_str(", paused");
        }
        out.push(head);
        let mut field = |label: &str, items: Vec<String>| {
            if !items.is_empty() {
                out.push(format!("  {label:<13} {}", items.join("; ")));
            }
        };

        let mut ports = Vec::new();
        let got = status["publishedPorts"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for p in spec["endpoint"]["ports"].as_array().into_iter().flatten() {
            let port = p["port"].as_u64().unwrap_or(0);
            let protocol = p["protocol"].as_str().unwrap_or("tcp");
            let mut what = format!("{port}/{protocol}");
            if let Some(publish @ ("ingress" | "host")) = p["publish"].as_str() {
                let at = got.iter().find(|g| {
                    g["port"].as_u64() == Some(port)
                        && g["protocol"].as_str().unwrap_or("tcp") == protocol
                });
                let on = if publish == "host" {
                    "on nodes running it"
                } else {
                    "on every node"
                };
                match at.and_then(|g| g["publishedPort"].as_u64()) {
                    Some(at) => what.push_str(&format!(", published :{at} {on}")),
                    None => what.push_str(", not published yet"),
                }
            }
            ports.push(what);
        }
        field("ports", ports);

        let mut routes: Vec<String> = ns
            .routes
            .iter()
            .filter(|r| str_of(&r.spec, &["service"]) == name)
            .map(|r| {
                let path = str_of(&r.spec, &["path"]);
                format!(
                    "{}{} (Route {})",
                    str_of(&r.spec, &["host"]),
                    if path.is_empty() { "/" } else { path },
                    r.metadata.name
                )
            })
            .collect();
        routes.sort();
        field("routes", routes);

        let mut tasks: Vec<String> = ns
            .tasks
            .iter()
            .filter(|t| str_of(&t.spec, &["service"]) == name)
            .filter(|t| str_of(&t.spec, &["desiredState"]) != "shutdown")
            .map(|t| {
                let mut what = format!(
                    "{} {}",
                    t.metadata.name,
                    str_of(&t.status, &["state"]).to_owned()
                );
                let node = str_of(&t.spec, &["node"]);
                if !node.is_empty() {
                    what.push_str(&format!(" on {node}"));
                }
                let ip = str_of(&t.status, &["ip"]);
                if !ip.is_empty() {
                    what.push_str(&format!(" {ip}"));
                }
                what
            })
            .collect();
        tasks.sort();
        field("tasks", tasks);

        let volumes: Vec<String> = spec["pod"]["volumes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v["volume"]["name"].as_str())
            .map(|v| match ns.volumes.iter().find(|o| o.metadata.name == v) {
                None => format!("{v} (missing)"),
                Some(o) if o.status["lost"].as_bool() == Some(true) => format!("{v} (lost)"),
                Some(o) => match str_of(&o.status, &["node"]) {
                    "" => v.to_owned(),
                    node => format!("{v} on {node}"),
                },
            })
            .collect();
        field("volumes", volumes);

        let allowed: Vec<String> = ns
            .reach
            .iter()
            .filter(|e| e.to == name && e.from != name)
            .map(|e| {
                let from = if e.from.is_empty() {
                    "(other namespaces)"
                } else {
                    e.from.as_str()
                };
                let ports = if e.all_ports {
                    "all ports".to_owned()
                } else {
                    e.ports
                        .iter()
                        .map(|p| format!("{}/{}", p.port, p.protocol))
                        .collect::<Vec<_>>()
                        .join(",")
                };
                format!("{from} {ports}")
            })
            .collect();
        field("may reach it", allowed);

        if let Some(seen) = ns.seen {
            let is_it = |e: &Option<pb::ConnectionEnd>| {
                e.as_ref()
                    .is_some_and(|e| e.kind == "service" && e.name == name)
            };
            let said = |e: &pb::ConnectionEdge, other: Option<&pb::ConnectionEnd>| {
                let via = match e.via.as_str() {
                    "" => String::new(),
                    via => format!(" via {via}"),
                };
                format!(
                    "{} {}/{}{via} x{}",
                    crate::cli::connection_end(other),
                    e.port,
                    e.protocol,
                    e.connections
                )
            };
            let into: Vec<String> = seen
                .iter()
                .filter(|e| is_it(&e.to))
                .map(|e| said(e, e.from.as_ref()))
                .collect();
            let from: Vec<String> = seen
                .iter()
                .filter(|e| is_it(&e.from) && !is_it(&e.to))
                .map(|e| said(e, e.to.as_ref()))
                .collect();
            field("seen in", into);
            field("seen out", from);
        }
        out.push(String::new());
    }
    // Volumes no service uses, so none above names them.
    let used: std::collections::BTreeSet<&str> = ns
        .services
        .iter()
        .flat_map(|s| s.spec["pod"]["volumes"].as_array().into_iter().flatten())
        .filter_map(|v| v["volume"]["name"].as_str())
        .collect();
    let mut unused: Vec<&str> = ns
        .volumes
        .iter()
        .map(|v| v.metadata.name.as_str())
        .filter(|v| !used.contains(v))
        .collect();
    unused.sort();
    if !unused.is_empty() {
        out.push(format!("volumes no service uses: {}", unused.join(", ")));
    }
    while out.last().is_some_and(String::is_empty) {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use legion_api::Kind;

    use super::*;

    fn object(kind: Kind, name: &str, spec: Value, status: Value) -> Object {
        let mut o = Object::new(kind, "shop", name, spec);
        o.status = status;
        o
    }

    #[test]
    fn a_namespace_reads_as_its_services_with_what_reaches_them() {
        let services = [
            object(
                Kind::Service,
                "web",
                serde_json::json!({
                    "endpoint": { "ports": [{ "port": 80, "publish": "ingress" }] },
                    "pod": { "volumes": [{ "name": "d", "volume": { "name": "data" } }] }
                }),
                serde_json::json!({
                    "ready": 2, "desired": 2,
                    "publishedPorts": [{ "port": 80, "publishedPort": 30080 }]
                }),
            ),
            object(
                Kind::Service,
                "db",
                serde_json::json!({ "endpoint": { "ports": [{ "port": 5432 }] }, "paused": true }),
                serde_json::json!({ "ready": 0, "desired": 1 }),
            ),
        ];
        let tasks = [
            object(
                Kind::Task,
                "web-1-1",
                serde_json::json!({ "service": "web", "node": "n1", "desiredState": "running" }),
                serde_json::json!({ "state": "running", "ip": "10.200.0.3" }),
            ),
            object(
                Kind::Task,
                "web-1-0",
                serde_json::json!({ "service": "web", "node": "n1", "desiredState": "shutdown" }),
                serde_json::json!({ "state": "shutdown" }),
            ),
        ];
        let routes = [object(
            Kind::Route,
            "site",
            serde_json::json!({ "host": "shop.example.org", "service": "web" }),
            Value::Null,
        )];
        let volumes = [
            object(
                Kind::Volume,
                "data",
                Value::Null,
                serde_json::json!({ "node": "n1" }),
            ),
            object(Kind::Volume, "spare", Value::Null, Value::Null),
        ];
        let reach = [pb::ReachEdge {
            from: "web".into(),
            to: "db".into(),
            all_ports: false,
            ports: vec![pb::ReachPort {
                protocol: "tcp".into(),
                port: 5432,
            }],
        }];
        let end = |kind: &str, name: &str| {
            Some(pb::ConnectionEnd {
                kind: kind.into(),
                name: name.into(),
            })
        };
        let seen = [pb::ConnectionEdge {
            from: end("outside", ""),
            to: end("service", "web"),
            protocol: "tcp".into(),
            port: 80,
            via: "published".into(),
            connections: 3,
            clients: 2,
        }];
        let ns = Namespace {
            services: &services,
            tasks: &tasks,
            routes: &routes,
            volumes: &volumes,
            reach: &reach,
            seen: Some(&seen),
        };
        assert_eq!(
            lines(&ns),
            [
                "db  0/1 ready, paused",
                "  ports         5432/tcp",
                "  may reach it  web 5432/tcp",
                "",
                "web  2/2 ready",
                "  ports         80/tcp, published :30080 on every node",
                "  routes        shop.example.org/ (Route site)",
                "  tasks         web-1-1 running on n1 10.200.0.3",
                "  volumes       data on n1",
                "  seen in       (outside) 80/tcp via published x3",
                "",
                "volumes no service uses: spare",
            ]
        );
    }
}
