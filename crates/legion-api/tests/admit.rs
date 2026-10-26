use legion_api::service::{PortRef, ServiceSpec};
use legion_api::{Error, Kind, Object, admit, parse};

fn object(yaml: &str) -> Object {
    serde_yaml_ng::from_str(yaml).unwrap()
}

fn admitted(yaml: &str) -> Object {
    let mut o = object(yaml);
    admit(&mut o).unwrap_or_else(|e| panic!("{e}"));
    o
}

fn rejected(yaml: &str) -> String {
    let mut o = object(yaml);
    admit(&mut o).expect_err("admitted").to_string()
}

fn service(spec: &str) -> String {
    format!(
        "apiVersion: legion/v1\nkind: Service\nmetadata: {{ name: web, namespace: shop }}\nspec:\n{spec}"
    )
}

/// The example from DESIGN.md §7.
const DESIGN_EXAMPLE: &str = r#"
apiVersion: legion/v1
kind: Service
metadata: { name: web, namespace: shop }
spec:
  mode: replicated            # replicated | global | stateful
  replicas: 3
  pod:
    initContainers:
      - name: migrate
        image: registry.example/shop/web:1.4
        command: ["/app/migrate"]
    containers:
      - name: web
        image: registry.example/shop/web:1.4
        ports: [{ name: http, containerPort: 8080 }]
        env: [{ name: DB_URL, secret: db-url }]
        resources:
          requests: { cpu: 500m, memory: 256Mi }
          limits:   { cpu: 1, memory: 512Mi, maxproc: 256 }
        probes:
          readiness: { http: { path: /ready, port: http }, period: 5s }
          liveness:  { http: { path: /live,  port: http }, failureThreshold: 3 }
        volumeMounts: [{ name: uploads, path: /var/uploads }]
      - name: log-shipper
        image: registry.example/tools/shipper:2
    volumes:
      - name: uploads
        volume: { name: uploads }
    restartPolicy: { condition: on-failure, maxAttempts: 5, delay: 5s }
  placement:
    constraints: ["node.labels.disk == ssd"]
    preferences: [{ spread: node.labels.zone }]
    tolerations: [{ key: dedicated, value: shop, effect: NoSchedule }]
  endpoint:
    mode: vip
    ports: [{ name: http, port: 80, targetPort: http, publish: ingress, publishedPort: 8080 }]
  update:
    parallelism: 1
    delay: 10s
    order: start-first
    failureAction: rollback
    maxUnavailable: 1
"#;

#[test]
fn design_example_is_admitted() {
    let o = admitted(DESIGN_EXAMPLE);
    let spec: ServiceSpec = parse(&o.spec).unwrap();
    assert_eq!(spec.replicas, Some(3));
    let web = &spec.pod.containers[0];
    assert_eq!(web.resources.requests.cpu.unwrap().millis(), 500);
    assert_eq!(web.resources.limits.memory.unwrap().0, 512 << 20);
    let readiness = web.probes.readiness.as_ref().unwrap();
    assert_eq!(readiness.period.millis(), 5000);
    // Defaults are written out.
    assert_eq!(readiness.timeout.millis(), 1000);
    assert_eq!(o.spec["pod"]["stopGracePeriod"], "10s");
    assert_eq!(
        spec.pod.resolve_port(&PortRef::Name("http".into())),
        Some(8080)
    );
    assert_eq!(spec.pod.requests().memory.0, 256 << 20);
}

#[test]
fn admission_is_idempotent() {
    let once = admitted(DESIGN_EXAMPLE);
    let mut twice = once.clone();
    admit(&mut twice).unwrap();
    assert_eq!(once, twice);
}

#[test]
fn equivalent_manifests_store_the_same_spec() {
    let a = admitted(&service(
        "  pod:\n    containers: [{ name: web, image: web:1, resources: { limits: { cpu: 0.5, memory: 1073741824 } } }]\n",
    ));
    let b = admitted(&service(
        "  replicas: 1\n  pod:\n    containers: [{ name: web, image: web:1, resources: { limits: { cpu: 500m, memory: 1Gi } } }]\n    restartPolicy: { condition: any }\n",
    ));
    assert_eq!(a.spec, b.spec);
    assert_eq!(
        a.spec["pod"]["containers"][0]["resources"]["limits"]["memory"],
        "1Gi"
    );
}

#[test]
fn errors_name_the_field() {
    let cases = [
        (
            "  pod:\n    containers: [{ name: web }]\n",
            "spec.pod.containers[0]: missing field `image`",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, imagee: x }]\n",
            "spec.pod.containers[0].imagee: unknown field `imagee`",
        ),
        (
            "  pod:\n    containers: [{ name: Web, image: web:1 }]\n",
            "spec.pod.containers[0].name: invalid name",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, resources: { limits: { memory: lots } } }]\n",
            "spec.pod.containers[0].resources.limits.memory: invalid byte quantity",
        ),
        (
            "  pod:\n    containers:\n      - { name: a, image: a:1, ports: [{ containerPort: 80 }] }\n      - { name: b, image: b:1, ports: [{ containerPort: 80 }] }\n",
            "spec.pod.containers[1].ports[0].containerPort: port 80/tcp is already used in the pod",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, probes: { readiness: { http: { port: http } } } }]\n",
            "spec.pod.containers[0].probes.readiness.http.port: no container port named `http`",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, probes: { liveness: { period: 5s } } }]\n",
            "spec.pod.containers[0].probes.liveness: set exactly one of",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, volumeMounts: [{ name: data, path: /data }] }]\n",
            "spec.pod.containers[0].volumeMounts[0].name: no pod volume named \"data\"",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, env: [{ name: A, value: x, secret: s }] }]\n",
            "spec.pod.containers[0].env[0]: set exactly one of",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1, resources: { requests: { cpu: 2 }, limits: { cpu: 1 } } }]\n",
            "spec.pod.containers[0].resources.requests.cpu: request 2 is above the limit 1",
        ),
        (
            "  mode: global\n  replicas: 2\n  pod:\n    containers: [{ name: web, image: web:1 }]\n",
            "spec.replicas: a global service",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1 }]\n  placement: { constraints: [\"node.labels.disk = ssd\"] }\n",
            "spec.placement.constraints[0]:",
        ),
        (
            "  pod:\n    containers: [{ name: web, image: web:1 }]\n  endpoint: { ports: [{ port: 80, publishedPort: 8080 }] }\n",
            "spec.endpoint.ports[0].publishedPort: set `publish: ingress`",
        ),
        (
            "  pod:\n    containers: []\n",
            "spec.pod.containers: a pod needs",
        ),
    ];
    for (spec, want) in cases {
        let got = rejected(&service(spec));
        assert!(
            got.starts_with(&format!("invalid Service: {want}")),
            "for\n{spec}\ngot:  {got}\nwant: invalid Service: {want}"
        );
    }
}

#[test]
fn secret_text_is_stored_as_base64() {
    let o = admitted(
        "apiVersion: legion/v1\nkind: Secret\nmetadata: { name: db-url, namespace: shop }\nspec: { text: postgres://db }\n",
    );
    assert_eq!(
        o.spec,
        serde_json::json!({ "data": "cG9zdGdyZXM6Ly9kYg==" })
    );
    let spec: legion_api::data::SecretSpec = parse(&o.spec).unwrap();
    assert_eq!(spec.bytes().unwrap(), b"postgres://db");

    assert!(rejected("apiVersion: legion/v1\nkind: Secret\nmetadata: { name: s, namespace: shop }\nspec: { data: \"not base64!\" }\n")
        .contains("spec.data: not valid base64"));
}

#[test]
fn empty_specs_get_defaults() {
    let ns = admitted("apiVersion: legion/v1\nkind: Namespace\nmetadata: { name: shop }\n");
    assert_eq!(ns.spec, serde_json::json!({}));
    let node = admitted("apiVersion: legion/v1\nkind: Node\nmetadata: { name: n1 }\n");
    assert_eq!(
        node.spec,
        serde_json::json!({ "availability": "active", "role": "worker" })
    );
    let vol = admitted(
        "apiVersion: legion/v1\nkind: Volume\nmetadata: { name: data, namespace: shop }\nspec: { size: 10Gi }\n",
    );
    assert_eq!(
        vol.spec,
        serde_json::json!({ "driver": "zfs", "size": "10Gi" })
    );
}

#[test]
fn kinds_not_written_through_admission() {
    let mut task = Object::new(Kind::Task, "shop", "web-1", serde_json::json!({}));
    assert_eq!(admit(&mut task), Err(Error::Managed(Kind::Task)));
    let mut job = Object::new(Kind::Job, "shop", "backup", serde_json::json!({}));
    assert_eq!(admit(&mut job), Err(Error::Unsupported(Kind::Job)));
}
