//! The image resolver (PLAN.md, phase 3). On the leader, it asks the
//! registry what each image reference of a Service's or Job's pod names, once
//! for each pod deployed, and records the digest on the owner's status. Its
//! tasks run that digest wherever they land, so a tag moved between two
//! nodes' pulls can't make a mix.
//!
//! The digest is the one the reference names: the manifest, or for an image
//! of several platforms the index, from which each node takes its own.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use legion_api::image::{ImageStatus, PullPolicy};
use legion_api::job::JobSpec;
use legion_api::login::{Login, docker_config};
use legion_api::service::{PodSpec, ServiceSpec};
use legion_api::{Kind, Object, ObjectKey};
use legion_image::{Credentials, Reference};
use legion_store::{NodeId, Store};
use sha2::Digest;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::controller::plan::Action;

/// How long a pod that couldn't be resolved waits before it is tried again.
const RETRY: Duration = Duration::from_secs(30);
const DEBOUNCE: Duration = Duration::from_millis(50);

/// What asks registries what a reference names: a registry client, or a
/// test's answers.
pub trait Registry: Send + Sync {
    /// The digest `reference` names, as `sha256:<hex>`, asked with the
    /// first of `logins` for its registry.
    fn digest(
        &self,
        reference: &Reference,
        logins: &[Login],
    ) -> impl Future<Output = Result<String, String>> + Send;
}

impl Registry for legion_image::Client {
    async fn digest(&self, reference: &Reference, logins: &[Login]) -> Result<String, String> {
        let creds = Credentials::for_registry(logins, &reference.registry);
        self.document(reference, reference.target(), creds.as_ref())
            .await
            .map(|(digest, _)| digest)
            .map_err(|e| e.to_string())
    }
}

/// Resolves pods whenever this manager leads, until `closing` turns true.
pub async fn run(store: Store, id: NodeId, closing: watch::Receiver<bool>) {
    let client = match legion_image::Client::new() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("image resolver: no registry client: {e}");
            return;
        }
    };
    crate::controller::while_leading(&store, id, closing, "image resolver", || {
        lead(&store, &client)
    })
    .await
}

async fn lead<R: Registry>(store: &Store, registry: &R) {
    let mut failed = BTreeMap::new();
    loop {
        let Ok(from) = store.applied_version() else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        pass(store, registry, &mut failed).await;
        let Ok(mut watcher) = store.watch(from, None) else {
            continue;
        };
        tokio::select! {
            ev = watcher.next() => if ev.is_ok() {
                tokio::time::sleep(DEBOUNCE).await;
            },
            _ = tokio::time::sleep(RETRY) => {}
        }
    }
}

/// A hash of `pod`, which [`ImageStatus::pod`] holds.
pub fn pod_hash(pod: &PodSpec) -> String {
    let json = serde_json::to_vec(pod).expect("a pod serialises");
    let digest = sha2::Sha256::digest(&json);
    digest[..16].iter().map(|b| format!("{b:02x}")).collect()
}

/// The references of `pod` the leader resolves: by tag, from a registry,
/// under `resolve`. One by digest already names its image, a local one has no
/// registry to ask, and `never` asks none.
pub fn to_resolve(pod: &PodSpec) -> Result<BTreeSet<String>, String> {
    let mut out = BTreeSet::new();
    for c in pod.containers() {
        if c.pull_policy == PullPolicy::Never {
            continue;
        }
        let r = Reference::parse(&c.image).map_err(|e| e.to_string())?;
        if r.digest.is_none() && !r.is_local() {
            out.insert(c.image.clone());
        }
    }
    Ok(out)
}

/// What each of `pod`'s references names now, by digest.
pub async fn resolve<R: Registry>(
    registry: &R,
    pod: &PodSpec,
    logins: &[Login],
) -> Result<BTreeMap<String, String>, String> {
    let mut resolved = BTreeMap::new();
    for image in to_resolve(pod)? {
        let r = Reference::parse(&image).map_err(|e| e.to_string())?;
        let digest = registry
            .digest(&r, logins)
            .await
            .map_err(|e| format!("resolving {image}: {e}"))?;
        let pinned = Reference {
            tag: None,
            digest: Some(digest),
            ..r
        };
        resolved.insert(image, pinned.to_string());
    }
    Ok(resolved)
}

/// The logins in `pod`'s pull secrets, in `namespace`.
fn logins(store: &Store, namespace: &str, pod: &PodSpec) -> Result<Vec<Login>, String> {
    let mut out = Vec::new();
    for name in &pod.image_pull_secrets {
        let key = ObjectKey::new(Kind::Secret, namespace, name);
        let value = crate::acme::read_data(store, &key)?
            .ok_or_else(|| format!("secret {name} not found"))?;
        out.extend(docker_config(&value).map_err(|e| format!("secret {name}: {e}"))?);
    }
    Ok(out)
}

/// The pod of a Service or Job.
fn pod_of(kind: Kind, o: &Object) -> Option<PodSpec> {
    match kind {
        Kind::Service => legion_api::parse::<ServiceSpec>(&o.spec)
            .ok()
            .map(|s| s.pod),
        Kind::Job => legion_api::parse::<JobSpec>(&o.spec).ok().map(|s| s.pod),
        _ => None,
    }
}

/// Resolves each Service's and Job's pod that has no images for it yet. A
/// pod that failed is tried again after [`RETRY`]; `failed` holds when, by
/// owner and pod.
pub async fn pass<R: Registry>(
    store: &Store,
    registry: &R,
    failed: &mut BTreeMap<(ObjectKey, String), Instant>,
) {
    let mut owners = Vec::new();
    for kind in [Kind::Service, Kind::Job] {
        match store.list(kind, None) {
            Ok(list) => owners.extend(list.objects.into_iter().map(|o| (kind, o))),
            Err(e) => {
                tracing::warn!("image resolver: reading the store: {e}");
                return;
            }
        }
    }
    let now = Instant::now();
    failed.retain(|_, at| now.duration_since(*at) < RETRY);
    for (kind, o) in owners {
        let Some(pod) = pod_of(kind, &o) else {
            continue;
        };
        let hash = pod_hash(&pod);
        let current: Option<ImageStatus> = o
            .status
            .get("images")
            .and_then(|v| serde_json::from_value(v.clone()).ok());
        if current.as_ref().is_some_and(|c| c.ready_for(&hash)) {
            continue;
        }
        let key = o.key();
        if failed.contains_key(&(key.clone(), hash.clone())) {
            continue;
        }
        let namespace = o.metadata.namespace.clone();
        let outcome = match logins(store, &namespace, &pod) {
            Ok(logins) => resolve(registry, &pod, &logins).await,
            Err(e) => Err(e),
        };
        let status = match outcome {
            Ok(resolved) => ImageStatus {
                pod: hash.clone(),
                resolved,
                message: String::new(),
            },
            Err(message) => {
                tracing::info!(owner = %key, "{message}");
                failed.insert((key.clone(), hash.clone()), now);
                ImageStatus {
                    pod: hash.clone(),
                    resolved: BTreeMap::new(),
                    message,
                }
            }
        };
        if current.as_ref() == Some(&status) {
            continue;
        }
        let mut written = o.clone();
        if !written.status.is_object() {
            written.status = serde_json::json!({});
        }
        written.status["images"] = serde_json::to_value(&status).expect("status serialises");
        let version = o.metadata.version;
        if let Err(e) = crate::controller::apply(store, Action::Update(written, version)).await {
            // Changed meanwhile: the next pass looks again.
            tracing::debug!(owner = %key, "image resolver write: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Answers(BTreeMap<String, Result<String, String>>);

    impl Registry for Answers {
        async fn digest(&self, r: &Reference, logins: &[Login]) -> Result<String, String> {
            let key = format!("{}/{}:{}", r.registry, r.repository, r.target());
            let private = key.starts_with("ghcr.io/");
            if private && !logins.iter().any(|l| l.registry == "ghcr.io") {
                return Err("denied".into());
            }
            self.0.get(&key).cloned().unwrap_or(Err("not found".into()))
        }
    }

    fn pod(yaml: &str) -> PodSpec {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[tokio::test]
    async fn references_by_tag_are_resolved_and_the_rest_left() {
        let p = pod(r#"
initContainers: [{ name: i, image: "busybox" }]
containers:
  - { name: a, image: "nginx:1" }
  - { name: b, image: "nginx@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
  - { name: c, image: "localhost/app" }
  - { name: d, image: "held:1", pullPolicy: never }
"#);
        assert_eq!(
            to_resolve(&p).unwrap(),
            BTreeSet::from(["busybox".to_owned(), "nginx:1".to_owned()])
        );
        let registry = Answers(BTreeMap::from([
            (
                "docker.io/library/nginx:1".to_owned(),
                Ok("sha256:aa".to_owned()),
            ),
            (
                "docker.io/library/busybox:latest".to_owned(),
                Ok("sha256:cc".to_owned()),
            ),
        ]));
        let resolved = resolve(&registry, &p, &[]).await.unwrap();
        assert_eq!(
            resolved,
            BTreeMap::from([
                (
                    "busybox".to_owned(),
                    "docker.io/library/busybox@sha256:cc".to_owned()
                ),
                (
                    "nginx:1".to_owned(),
                    "docker.io/library/nginx@sha256:aa".to_owned()
                ),
            ])
        );
    }

    #[tokio::test]
    async fn a_reference_that_cant_be_resolved_says_which() {
        let p = pod(r#"containers: [{ name: a, image: "ghcr.io/me/app:2" }]"#);
        let registry = Answers(BTreeMap::from([(
            "ghcr.io/me/app:2".to_owned(),
            Ok("sha256:dd".to_owned()),
        )]));
        let err = resolve(&registry, &p, &[]).await.unwrap_err();
        assert_eq!(err, "resolving ghcr.io/me/app:2: denied");
        let login = Login {
            registry: "ghcr.io".into(),
            username: "bot".into(),
            password: "pw".into(),
        };
        let resolved = resolve(&registry, &p, &[login]).await.unwrap();
        assert_eq!(resolved["ghcr.io/me/app:2"], "ghcr.io/me/app@sha256:dd");
    }

    #[test]
    fn a_pod_hash_changes_with_the_pod_only() {
        let a = pod(r#"containers: [{ name: a, image: "nginx:1" }]"#);
        let b = pod(r#"containers: [{ name: a, image: "nginx:2" }]"#);
        assert_eq!(pod_hash(&a), pod_hash(&a.clone()));
        assert_ne!(pod_hash(&a), pod_hash(&b));
    }
}
