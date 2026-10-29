use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::json;

use super::*;
use crate::Kind;
use crate::service::ServiceSpec;

const APP: &str = r#"
version: "3.8"
x-common: &common
  restart: always
services:
  web:
    <<: *common
    image: "nginx:${TAG:-1.27}"
    command: nginx -g "daemon off;"
    environment:
      - DB_HOST=db
      - HOME_DIR=$$HOME
    ports: ["8080:80", "127.0.0.1:5353:53/udp"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost"]
      interval: 10s
      retries: 2
    secrets: [db_password]
    configs: [{ source: nginx_conf, target: /etc/nginx/nginx.conf }]
    depends_on: [db]
    cap_add: [NET_RAW, SYS_ADMIN]
    cap_drop: [ALL]
    deploy:
      replicas: 3
      update_config: { parallelism: 1, delay: 10s, order: start-first, failure_action: rollback }
      restart_policy: { condition: on-failure, max_attempts: 3 }
      resources: { limits: { cpus: "0.5", memory: 512M }, reservations: { memory: 128M } }
      placement: { constraints: ["node.role == worker"] }
      labels: { tier: front }
  db:
    image: postgres:17
    volumes: ["db_data:/var/lib/postgresql/data", "./init.sql:/init.sql:ro"]
    tmpfs: /run
    env_file: db.env
    environment: { POSTGRES_PASSWORD_FILE: /run/secrets/db_password }
    secrets: [{ source: db_password, target: pw }]
    healthcheck: { test: pg_isready }
    x-legion: { placement: { tolerations: [{ key: dedicated }] }, pod: { imagePullSecrets: [registry-login] } }
volumes:
  db_data: {}
  shared: { external: true }
secrets:
  db_password: { file: ./db_password.txt }
configs:
  nginx_conf: { file: ./nginx.conf }
networks:
  front: {}
"#;

fn convert_with(yaml: &str, env: &[(&str, &str)]) -> Result<Stack, String> {
    let env: BTreeMap<String, String> = env
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect();
    let lookup = move |k: &str| env.get(k).cloned();
    let read = |path: &str| -> Result<Vec<u8>, String> {
        match path {
            "./db_password.txt" => Ok(b"s3cret\n".to_vec()),
            "./nginx.conf" => Ok(b"events {}\n".to_vec()),
            "db.env" => Ok(b"# comment\nPGDATA=/var/lib/postgresql/data\n".to_vec()),
            p => Err(format!("{p}: no such file")),
        }
    };
    convert(
        yaml,
        "shop",
        "shop",
        &Inputs {
            env: &lookup,
            read: &read,
        },
    )
}

fn spec(stack: &Stack, name: &str) -> ServiceSpec {
    let o = stack
        .objects
        .iter()
        .find(|o| o.kind == Kind::Service && o.metadata.name == name)
        .unwrap();
    crate::parse(&o.spec).unwrap()
}

#[test]
fn converts_a_stack() {
    let stack = convert_with(APP, &[]).unwrap();
    let keys: Vec<String> = stack.objects.iter().map(|o| o.key().to_string()).collect();
    assert_eq!(
        keys,
        [
            "Volume/shop/db-data",
            "Secret/shop/db-password",
            "Config/shop/nginx-conf",
            "Service/shop/db",
            "Service/shop/web",
        ]
    );
    assert!(
        stack
            .objects
            .iter()
            .all(|o| o.metadata.labels[LABEL_STACK] == "shop")
    );
    assert_eq!(stack.objects[1].spec["data"], STANDARD.encode("s3cret\n"));

    let web = spec(&stack, "web");
    assert_eq!(web.replicas, Some(3));
    let c = &web.pod.containers[0];
    assert_eq!(c.image, "nginx:1.27");
    assert_eq!(c.args, ["nginx", "-g", "daemon off;"]);
    let env: Vec<(&str, &str)> = c
        .env
        .iter()
        .map(|e| (e.name.as_str(), e.value.as_deref().unwrap()))
        .collect();
    assert_eq!(env, [("DB_HOST", "db"), ("HOME_DIR", "$HOME")]);
    let ports = &web.endpoint.as_ref().unwrap().ports;
    assert_eq!(ports.len(), 2);
    assert_eq!((ports[0].port, ports[0].published_port), (80, Some(8080)));
    assert_eq!((ports[1].port, ports[1].published_port), (53, Some(5353)));
    assert_eq!(ports[1].protocol, crate::service::Protocol::Udp);
    assert_eq!(c.secrets[0].name, "db-password");
    assert_eq!(
        c.secrets[0].target.as_deref(),
        Some("/run/secrets/db_password")
    );
    assert_eq!(
        c.configs[0].target.as_deref(),
        Some("/etc/nginx/nginx.conf")
    );
    let probe = c.probes.liveness.as_ref().unwrap();
    assert_eq!(
        probe.exec.as_ref().unwrap().command,
        ["curl", "-f", "http://localhost"]
    );
    assert_eq!((probe.period.0, probe.failure_threshold), (10_000, 2));
    assert_eq!(c.probes.readiness, c.probes.liveness);
    assert_eq!(c.resources.limits.cpu.unwrap().0, 500);
    assert_eq!(c.resources.limits.memory.unwrap().0, 512 << 20);
    assert_eq!(c.resources.requests.memory.unwrap().0, 128 << 20);
    assert_eq!(web.update.order, crate::service::UpdateOrder::StartFirst);
    assert_eq!(
        web.update.failure_action,
        crate::service::FailureAction::Rollback
    );
    assert_eq!(web.pod.restart_policy.max_attempts, 3);
    assert_eq!(web.placement.constraints, ["node.role == worker"]);
    assert_eq!(
        c.security.allow,
        [crate::security::Allow::RawSockets].into()
    );
    assert_eq!(stack.objects[4].metadata.labels["tier"], "front");

    let db = spec(&stack, "db");
    let c = &db.pod.containers[0];
    let mounts: Vec<(&str, &str)> = c
        .volume_mounts
        .iter()
        .map(|m| (m.name.as_str(), m.path.as_str()))
        .collect();
    assert_eq!(
        mounts,
        [("tmpfs-0", "/run"), ("db-data", "/var/lib/postgresql/data")]
    );
    assert!(db.pod.volumes[0].tmpfs.is_some());
    assert_eq!(db.pod.volumes[1].volume.as_ref().unwrap().name, "db-data");
    assert!(c.env.iter().any(|e| e.name == "PGDATA"));
    assert_eq!(c.secrets[0].target.as_deref(), Some("/run/secrets/pw"));
    assert_eq!(
        c.probes
            .liveness
            .as_ref()
            .unwrap()
            .exec
            .as_ref()
            .unwrap()
            .command,
        ["/bin/sh", "-c", "pg_isready"]
    );
    assert_eq!(db.placement.tolerations[0].key, "dedicated");
    assert_eq!(db.pod.image_pull_secrets, ["registry-login"]);
    assert_eq!(db.pod.containers.len(), 1, "merged into the pod");

    let warnings = stack.warnings.join("\n");
    for expected in [
        "volumes.db_data: named db-data in Legion",
        "services.web.restart: ignored",
        "services.web.depends_on: ignored",
        "services.web.cap_add: SYS_ADMIN: not supported, ignored",
        "services.web.ports[1]: the host address is ignored",
        "services.db.volumes[1]: bind mounts of host paths are not supported",
        "networks: not separated yet",
    ] {
        assert!(warnings.contains(expected), "{expected}\n{warnings}");
    }
}

#[test]
fn interpolation() {
    let stack = convert_with(APP, &[("TAG", "1.28")]).unwrap();
    assert_eq!(spec(&stack, "web").pod.containers[0].image, "nginx:1.28");
    let stack = convert_with("services: { a: { image: \"a:${MISSING}x\" } }", &[]).unwrap();
    assert_eq!(spec(&stack, "a").pod.containers[0].image, "a:x");
    assert!(stack.warnings[0].contains("$MISSING is not set"));
    let err = convert_with("services: { a: { image: \"${IMG:?set IMG}\" } }", &[]);
    assert_eq!(err.unwrap_err(), "services.a.image: IMG: set IMG");
    let stack = convert_with(
        "services: { a: { image: \"b${E-x}:${E:-c}\" } }",
        &[("E", "")],
    )
    .unwrap();
    assert_eq!(spec(&stack, "a").pod.containers[0].image, "b:c");
}

#[test]
fn errors_name_the_place() {
    for (yaml, error) in [
        (
            "services: { a: { build: . } }",
            "services.a.image: required",
        ),
        (
            "services: { a: { image: a, volumes: [\"data:/d\"] } }",
            "services.a.volumes[0]: volume data is not declared under volumes",
        ),
        (
            "services: { a: { image: a, ports: [\"8000-8001:80\"] } }",
            "port ranges are not supported",
        ),
        (
            "services: { a: { image: a, deploy: { placement: { constraints: [\"node.id == x\"] } } } }",
            "Service/shop/a:",
        ),
        ("volumes: {}", "services: the compose file has none"),
    ] {
        let err = convert_with(yaml, &[]).unwrap_err();
        assert!(err.contains(error), "{yaml}: {err}");
    }
}

#[test]
fn helpers() {
    assert_eq!(
        words(&json!("sh -c 'echo \"hi there\"' a\\ b"), "").unwrap(),
        ["sh", "-c", "echo \"hi there\"", "a b"]
    );
    assert_eq!(
        docker_bytes(&json!("1g"), "").unwrap(),
        (1u64 << 30).to_string()
    );
    assert_eq!(docker_bytes(&json!("100kb"), "").unwrap(), "102400");
    assert_eq!(docker_bytes(&json!(2048), "").unwrap(), "2048");
    assert!(docker_bytes(&json!("1x"), "").is_err());
}
