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

use legion_api::image::{ImagePolicySpec, ImageStatus, PolicyMode, PullPolicy, Verification};
use legion_api::job::JobSpec;
use legion_api::login::{Login, docker_config};
use legion_api::service::{PodSpec, ServiceSpec};
use legion_api::{Kind, Object, ObjectKey};
use legion_image::cosign::{self, PublicKey, Verified, VerifyError};
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

    /// Whether the image `digest` in `reference`'s repository carries a
    /// good signature by one of `keys` (phase 7).
    fn verify(
        &self,
        reference: &Reference,
        digest: &str,
        keys: &[PublicKey],
        logins: &[Login],
    ) -> impl Future<Output = Result<Verified, VerifyError>> + Send;
}

/// Registries, over HTTPS; with `loopback`, those on this host over plain
/// HTTP, as tests run them.
struct Live {
    client: legion_image::Client,
    loopback: bool,
}

impl Live {
    /// A plain-HTTP client for `reference`'s registry if it is on this host
    /// under `loopback`; none for the shared client.
    fn local_client(&self, reference: &Reference) -> Result<Option<legion_image::Client>, String> {
        let host = reference.host();
        let local =
            self.loopback && (host.starts_with("127.0.0.1:") || host.starts_with("localhost:"));
        if !local {
            return Ok(None);
        }
        Ok(Some(
            legion_image::Client::new()
                .map_err(|e| e.to_string())?
                .plain_http(&reference.registry),
        ))
    }
}

impl Registry for Live {
    async fn digest(&self, reference: &Reference, logins: &[Login]) -> Result<String, String> {
        let creds = Credentials::for_registry(logins, &reference.registry);
        let local = self.local_client(reference)?;
        local
            .as_ref()
            .unwrap_or(&self.client)
            .document(reference, reference.target(), creds.as_ref())
            .await
            .map(|(digest, _)| digest)
            .map_err(|e| e.to_string())
    }

    async fn verify(
        &self,
        reference: &Reference,
        digest: &str,
        keys: &[PublicKey],
        logins: &[Login],
    ) -> Result<Verified, VerifyError> {
        let creds = Credentials::for_registry(logins, &reference.registry);
        let local = self
            .local_client(reference)
            .map_err(VerifyError::CannotFetch)?;
        let client = local.as_ref().unwrap_or(&self.client);
        cosign::verify(client, reference, digest, keys, creds.as_ref()).await
    }
}

/// An `ImagePolicy`, its keys read.
#[derive(Debug, Clone)]
pub struct Policy {
    pub name: String,
    pub images: Vec<String>,
    pub keys: Vec<PublicKey>,
    pub mode: PolicyMode,
}

/// The cluster's `ImagePolicy` objects, by name. Keys are checked as a
/// policy is applied; one that does not read here is left out, so a
/// policy left with none passes nothing.
fn policies(store: &Store) -> Result<Vec<Policy>, String> {
    let list = store
        .list(Kind::ImagePolicy, None)
        .map_err(|e| e.to_string())?;
    Ok(list
        .objects
        .iter()
        .filter_map(|o| {
            let spec: ImagePolicySpec = legion_api::parse(&o.spec).ok()?;
            Some(Policy {
                name: o.metadata.name.clone(),
                images: spec.images,
                keys: spec
                    .keys
                    .iter()
                    .filter_map(|k| PublicKey::from_pem(k).ok())
                    .collect(),
                mode: spec.mode,
            })
        })
        .collect())
}

/// Checks each of `pod`'s images that a policy covers, as it is to run
/// (`resolved`, or as written), against every policy covering it. Returns
/// the good signatures, the failures under policies that enforce (which
/// hold the pod), and those under policies that warn.
pub async fn check_signatures<R: Registry>(
    registry: &R,
    pod: &PodSpec,
    resolved: &BTreeMap<String, String>,
    policies: &[Policy],
    logins: &[Login],
) -> (BTreeMap<String, Verification>, Vec<String>, Vec<String>) {
    let (mut verified, mut held, mut warned) = (BTreeMap::new(), Vec::new(), Vec::new());
    // Each image once; `never` if any container pulls it so.
    let mut images: BTreeMap<&str, PullPolicy> = BTreeMap::new();
    for c in pod.containers() {
        let pull = images.entry(c.image.as_str()).or_insert(c.pull_policy);
        if c.pull_policy == PullPolicy::Never {
            *pull = PullPolicy::Never;
        }
    }
    for (image, pull) in images {
        let Ok(written) = Reference::parse(image) else {
            continue;
        };
        let name = format!("{}/{}", written.registry, written.repository);
        let covering: Vec<&Policy> = policies
            .iter()
            .filter(|p| {
                p.images
                    .iter()
                    .any(|pat| legion_api::image::matches(pat, &name))
            })
            .collect();
        if covering.is_empty() {
            continue;
        }
        let runs = Reference::parse(legion_api::image::pinned(resolved, image)).ok();
        let digest = runs.as_ref().and_then(|r| r.digest.clone());
        let mut good = None;
        for policy in covering {
            let failure = if written.is_local() {
                Some(format!("{image} is local and has no signature"))
            } else if let Some(digest) = &digest {
                match registry
                    .verify(&written, digest, &policy.keys, logins)
                    .await
                {
                    Ok(v) => {
                        good.get_or_insert(Verification {
                            digest: digest.clone(),
                            key: v.fingerprint,
                        });
                        None
                    }
                    Err(VerifyError::Unsigned) => Some(format!("{image} is unsigned")),
                    Err(VerifyError::CannotFetch(e)) => {
                        Some(format!("cannot check the signature of {image}: {e}"))
                    }
                    Err(VerifyError::Bad(e)) => Some(format!("bad signature on {image}: {e}")),
                }
            } else if pull == PullPolicy::Never {
                Some(format!(
                    "{image} runs whatever each node holds by that name (pullPolicy never), \
                     which no signature covers: name it by digest"
                ))
            } else {
                Some(format!("{image} was not resolved to a digest"))
            };
            if let Some(f) = failure {
                let f = format!("{f} (policy {})", policy.name);
                match policy.mode {
                    PolicyMode::Enforce => held.push(f),
                    PolicyMode::Warn => warned.push(f),
                }
                good = None;
                break;
            }
        }
        if let Some(v) = good {
            verified.insert(image.to_owned(), v);
        }
    }
    (verified, held, warned)
}

/// Resolves pods whenever this manager leads, until `closing` turns true.
/// With `loopback`, registries on this host are asked over plain HTTP, as
/// are the `insecure` ones (`host[:port]`).
pub async fn run(
    store: Store,
    id: NodeId,
    loopback: bool,
    insecure: Vec<String>,
    closing: watch::Receiver<bool>,
) {
    let client = match legion_image::Client::new() {
        Ok(c) => insecure.iter().fold(c, |c, r| c.plain_http(r)),
        Err(e) => {
            tracing::error!("image resolver: no registry client: {e}");
            return;
        }
    };
    let live = Live { client, loopback };
    crate::controller::while_leading(&store, id, closing, "image resolver", || {
        lead(&store, &live)
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
        resolved.insert(image, r.by_digest(&digest).to_string());
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

/// The images a Service ran before its update, when its pod is that one's
/// again (a rollback): it runs them again, whatever its tags name now.
pub(crate) fn rolled_back_to(o: &Object, hash: &str) -> Option<BTreeMap<String, String>> {
    let update: legion_api::service::UpdateStatus =
        serde_json::from_value(o.status.get("update")?.clone()).ok()?;
    (!update.previous_images.is_empty() && pod_hash(&update.previous.pod) == hash)
        .then_some(update.previous_images)
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
    // Pins are used once every node runs them; until then nothing is asked.
    match store.list(Kind::Node, None) {
        Ok(list) => {
            let pins =
                |o: &Object| o.status.get("imagePins").and_then(|v| v.as_bool()) == Some(true);
            if list.objects.is_empty() || !list.objects.iter().all(pins) {
                return;
            }
        }
        Err(e) => {
            tracing::warn!("image resolver: reading the store: {e}");
            return;
        }
    }
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
    let policies = match policies(store) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("image resolver: reading the store: {e}");
            return;
        }
    };
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
        let logins = logins(store, &namespace, &pod);
        let outcome = match (rolled_back_to(&o, &hash), &logins) {
            (Some(before), _) => Ok(before),
            (None, Ok(logins)) => resolve(registry, &pod, logins).await,
            (None, Err(e)) => Err(e.clone()),
        };
        // What runs is checked against the policies, a rollback's digests
        // too: a policy made since applies to them.
        let outcome = match outcome {
            Ok(resolved) => {
                let logins = logins.as_deref().unwrap_or_default();
                let (verified, held, warned) =
                    check_signatures(registry, &pod, &resolved, &policies, logins).await;
                if held.is_empty() {
                    Ok((resolved, verified, warned.join("; ")))
                } else {
                    Err(held.join("; "))
                }
            }
            Err(e) => Err(e),
        };
        let status = match outcome {
            Ok((resolved, verified, warning)) => ImageStatus {
                pod: hash.clone(),
                resolved,
                message: String::new(),
                verified,
                warning,
            },
            Err(message) => {
                tracing::info!(owner = %key, "{message}");
                failed.insert((key.clone(), hash.clone()), now);
                ImageStatus {
                    pod: hash.clone(),
                    message,
                    ..ImageStatus::default()
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

        async fn verify(
            &self,
            _: &Reference,
            _: &str,
            _: &[PublicKey],
            _: &[Login],
        ) -> Result<Verified, VerifyError> {
            Err(VerifyError::Unsigned)
        }
    }

    /// Signatures by digest: the fingerprint of the key that made each, or
    /// a registry that cannot be read.
    struct Signed {
        by: BTreeMap<String, String>,
        down: bool,
    }

    impl Registry for Signed {
        async fn digest(&self, _: &Reference, _: &[Login]) -> Result<String, String> {
            Ok(format!("sha256:{}", "a".repeat(64)))
        }

        async fn verify(
            &self,
            _: &Reference,
            digest: &str,
            keys: &[PublicKey],
            _: &[Login],
        ) -> Result<Verified, VerifyError> {
            if self.down {
                return Err(VerifyError::CannotFetch("503".into()));
            }
            match self.by.get(digest) {
                None => Err(VerifyError::Unsigned),
                Some(f) if keys.iter().any(|k| k.fingerprint() == f) => Ok(Verified {
                    fingerprint: f.clone(),
                }),
                Some(_) => Err(VerifyError::Bad("made by none of the keys given".into())),
            }
        }
    }

    fn policy(name: &str, images: &[&str], keys: &[&PublicKey], mode: PolicyMode) -> Policy {
        Policy {
            name: name.into(),
            images: images.iter().map(|s| s.to_string()).collect(),
            keys: keys.iter().map(|k| (*k).clone()).collect(),
            mode,
        }
    }

    #[tokio::test]
    async fn images_a_policy_covers_run_only_with_a_good_signature() {
        let digest = format!("sha256:{}", "a".repeat(64));
        let good = cosign::PrivateKey::generate().public();
        let other = cosign::PrivateKey::generate().public();
        let p = pod(r#"
containers:
  - { name: a, image: "ghcr.io/me/app:1" }
  - { name: b, image: "nginx:1" }
"#);
        let resolved = BTreeMap::from([
            (
                "ghcr.io/me/app:1".to_owned(),
                format!("ghcr.io/me/app@{digest}"),
            ),
            (
                "nginx:1".to_owned(),
                format!("docker.io/library/nginx@{digest}"),
            ),
        ]);
        let signed = Signed {
            by: BTreeMap::from([(digest.clone(), good.fingerprint().to_owned())]),
            down: false,
        };
        let enforce = [policy(
            "me",
            &["ghcr.io/me/*"],
            &[&good],
            PolicyMode::Enforce,
        )];
        let (verified, held, warned) =
            check_signatures(&signed, &p, &resolved, &enforce, &[]).await;
        assert!(held.is_empty() && warned.is_empty(), "{held:?} {warned:?}");
        // nginx is no policy's: it runs as before, with no entry.
        assert_eq!(
            verified,
            BTreeMap::from([(
                "ghcr.io/me/app:1".to_owned(),
                Verification {
                    digest: digest.clone(),
                    key: good.fingerprint().to_owned(),
                }
            )])
        );

        // Another key's policy: held, saying so.
        let wrong = [policy(
            "me",
            &["ghcr.io/me/*"],
            &[&other],
            PolicyMode::Enforce,
        )];
        let (verified, held, _) = check_signatures(&signed, &p, &resolved, &wrong, &[]).await;
        assert!(verified.is_empty());
        assert_eq!(
            held,
            ["bad signature on ghcr.io/me/app:1: made by none of the keys given (policy me)"]
        );
        // Every policy covering an image must be met.
        let both = [
            policy("me", &["ghcr.io/me/*"], &[&good], PolicyMode::Enforce),
            policy("all", &["ghcr.io/**"], &[&other], PolicyMode::Enforce),
        ];
        let (_, held, _) = check_signatures(&signed, &p, &resolved, &both, &[]).await;
        assert_eq!(held.len(), 1, "{held:?}");

        // Unsigned, and a registry that cannot be read.
        let none = Signed {
            by: BTreeMap::new(),
            down: false,
        };
        let (_, held, _) = check_signatures(&none, &p, &resolved, &enforce, &[]).await;
        assert_eq!(held, ["ghcr.io/me/app:1 is unsigned (policy me)"]);
        let down = Signed {
            by: signed.by.clone(),
            down: true,
        };
        let (_, held, _) = check_signatures(&down, &p, &resolved, &enforce, &[]).await;
        assert_eq!(
            held,
            ["cannot check the signature of ghcr.io/me/app:1: 503 (policy me)"]
        );

        // Under warn: it runs, and the status says why it would not.
        let warn = [policy("me", &["ghcr.io/me/*"], &[&good], PolicyMode::Warn)];
        let (verified, held, warned) = check_signatures(&none, &p, &resolved, &warn, &[]).await;
        assert!(verified.is_empty() && held.is_empty());
        assert_eq!(warned, ["ghcr.io/me/app:1 is unsigned (policy me)"]);
    }

    #[tokio::test]
    async fn a_covered_image_by_digest_is_checked_and_a_local_one_refused() {
        let digest = format!("sha256:{}", "a".repeat(64));
        let good = cosign::PrivateKey::generate().public();
        let signed = Signed {
            by: BTreeMap::from([(digest.clone(), good.fingerprint().to_owned())]),
            down: false,
        };
        let policies = [policy(
            "lab",
            &["ghcr.io/me/**", "localhost/**"],
            &[&good],
            PolicyMode::Enforce,
        )];
        let by_digest = format!("ghcr.io/me/app@{digest}");
        let p = pod(&format!(
            r#"containers: [{{ name: a, image: "{by_digest}" }}]"#
        ));
        let (verified, held, _) =
            check_signatures(&signed, &p, &BTreeMap::new(), &policies, &[]).await;
        assert!(held.is_empty(), "{held:?}");
        assert_eq!(verified[&by_digest].digest, digest);

        let p = pod(r#"containers: [{ name: a, image: "localhost/app:1" }]"#);
        let (_, held, _) = check_signatures(&signed, &p, &BTreeMap::new(), &policies, &[]).await;
        assert_eq!(
            held,
            ["localhost/app:1 is local and has no signature (policy lab)"]
        );

        // `never` by tag runs what each node holds: no signature binds it.
        let p = pod(r#"containers: [{ name: a, image: "ghcr.io/me/app:1", pullPolicy: never }]"#);
        let (_, held, _) = check_signatures(&signed, &p, &BTreeMap::new(), &policies, &[]).await;
        assert!(held[0].contains("pullPolicy never"), "{held:?}");
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

    /// A Service whose pod is the one an update replaced runs that one's
    /// images again; any other pod is resolved afresh.
    #[test]
    fn a_rollback_runs_the_digests_from_before_its_update() {
        let spec = |image: &str| -> ServiceSpec {
            serde_json::from_value(serde_json::json!({
                "pod": { "containers": [{ "name": "a", "image": image }] },
            }))
            .unwrap()
        };
        let before = BTreeMap::from([(
            "nginx:1".to_owned(),
            "docker.io/library/nginx@sha256:aa".to_owned(),
        )]);
        let mut o = Object::new(Kind::Service, "shop", "web", serde_json::json!({}));
        o.status = serde_json::json!({
            "update": {
                "state": "rolling-back",
                "generation": 2,
                "previous": spec("nginx:1"),
                "previousImages": before,
            },
        });
        let old = pod_hash(&spec("nginx:1").pod);
        let new = pod_hash(&spec("nginx:2").pod);
        assert_eq!(rolled_back_to(&o, &old), Some(before));
        assert_eq!(rolled_back_to(&o, &new), None);
        // An update from before pins has none to run again.
        o.status["update"]
            .as_object_mut()
            .unwrap()
            .remove("previousImages");
        assert_eq!(rolled_back_to(&o, &old), None);
    }

    #[test]
    fn a_pod_hash_changes_with_the_pod_only() {
        let a = pod(r#"containers: [{ name: a, image: "nginx:1" }]"#);
        let b = pod(r#"containers: [{ name: a, image: "nginx:2" }]"#);
        assert_eq!(pod_hash(&a), pod_hash(&a.clone()));
        assert_ne!(pod_hash(&a), pod_hash(&b));
    }
}
