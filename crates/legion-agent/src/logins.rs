//! Registry logins from a pod's `imagePullSecrets` (DESIGN.md §6.1). Each
//! Secret is a Docker config, as `docker login` writes
//! `~/.docker/config.json` and Kubernetes' `dockerconfigjson` Secrets hold:
//! `{"auths": {"<registry>": {"auth": "<base64 of user:password>"}}}`, or
//! `username` and `password` in place of `auth`.

use legion_api::Kind;
use legion_api::login::docker_config;

use crate::control::{Assignment, Control};
use crate::runtime::Login;

/// The logins in the pod's pull secrets, in their order.
pub(crate) async fn read<C: Control>(control: &C, a: &Assignment) -> Result<Vec<Login>, String> {
    let mut logins = Vec::new();
    for name in &a.spec.pod.image_pull_secrets {
        let value = match control.data(&a.key.namespace, Kind::Secret, name).await {
            Ok(Some(v)) => v,
            Ok(None) => return Err(format!("secret {name} not found")),
            Err(e) => return Err(format!("reading secret {name}: {e}")),
        };
        logins.extend(docker_config(&value).map_err(|e| format!("secret {name}: {e}"))?);
    }
    Ok(logins)
}
