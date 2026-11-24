//! `legion app`: ready-to-deploy applications from the daemonless catalog
//! (TODO.MD item 5, legion_api::app): listed and shown without a cluster,
//! deployed as a stack of its own.

use std::collections::BTreeMap;

use anyhow::{Context, bail};
use legion_api::app::{Catalog, CatalogApp, Manifest};

/// The catalog at `base`.
pub async fn catalog(base: &str) -> anyhow::Result<Catalog> {
    let url = format!("{}/catalog.json", base.trim_end_matches('/'));
    let body = legion_image::fetch::get(&url)
        .await
        .map_err(anyhow::Error::msg)?;
    serde_json::from_slice(&body).with_context(|| format!("reading {url}"))
}

/// `id` in the catalog, and its manifest.
pub async fn app(base: &str, id: &str) -> anyhow::Result<(CatalogApp, Manifest)> {
    let c = catalog(base).await?;
    let Some(found) = c.apps.into_iter().find(|a| a.id == id) else {
        bail!("no app {id} in the catalog: `legion app list` lists them");
    };
    if found.manifest_url.contains("..") || found.manifest_url.starts_with('/') {
        bail!("{id}: its manifest is outside the catalog");
    }
    let url = if found.manifest_url.starts_with("https://") {
        found.manifest_url.clone()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), found.manifest_url)
    };
    let body = legion_image::fetch::get(&url)
        .await
        .map_err(anyhow::Error::msg)?;
    let text = String::from_utf8(body).with_context(|| format!("reading {url}"))?;
    let m = legion_api::app::manifest(&text).map_err(anyhow::Error::msg)?;
    Ok((found, m))
}

pub async fn list(base: &str, category: Option<&str>) -> anyhow::Result<()> {
    let c = catalog(base).await?;
    let mut rows: Vec<[String; 4]> = c
        .apps
        .iter()
        .filter(|a| category.is_none_or(|k| a.category.eq_ignore_ascii_case(k)))
        .map(|a| {
            let mut about = a.description.clone();
            if about.chars().count() > 70 {
                about = about.chars().take(69).collect::<String>() + "…";
            }
            [a.id.clone(), a.category.clone(), a.version.clone(), about]
        })
        .collect();
    rows.sort();
    crate::cli::print_table(["APP", "CATEGORY", "VERSION", "WHAT IT IS"], &rows);
    Ok(())
}

pub async fn show(base: &str, id: &str) -> anyhow::Result<()> {
    let (a, m) = app(base, id).await?;
    println!("{} ({})", m.name, a.id);
    if !m.description.is_empty() {
        println!("{}", m.description);
    }
    if !a.web_url.is_empty() {
        println!("{}", a.web_url);
    }
    println!("image: {} {}", a.image, a.version);
    if !m.variants.is_empty() {
        let v: Vec<_> = m
            .variants
            .iter()
            .map(|v| format!("{}{}", v.id, if v.default { " (default)" } else { "" }))
            .collect();
        println!("variants (--variant): {}", v.join(", "));
    }
    let rows: Vec<[String; 4]> = m
        .variables
        .iter()
        .map(|v| {
            let default = match (
                v.kind.as_str(),
                v.default.as_deref().filter(|d| !d.is_empty()),
            ) {
                ("secret", None) => "(made up)".to_owned(),
                (_, Some(d)) => d.to_owned(),
                (_, None) if v.is_storage() => v.name.to_lowercase().replace('_', "-"),
                (_, None) => "(empty: the app's own)".to_owned(),
            };
            let kind = if v.is_storage() {
                "volume".to_owned()
            } else {
                v.kind.clone()
            };
            [v.name.clone(), kind, default, v.label.clone()]
        })
        .collect();
    crate::cli::print_table(["VARIABLE (--set)", "KIND", "DEFAULT", "WHAT"], &rows);
    Ok(())
}

/// The compose file and variables to deploy `id` with, from `--set`s.
pub async fn rendered(
    base: &str,
    id: &str,
    set: &[String],
    variant: Option<&str>,
) -> anyhow::Result<(String, BTreeMap<String, String>)> {
    let (a, m) = app(base, id).await?;
    let mut values = BTreeMap::new();
    for kv in set {
        let (k, v) = kv
            .split_once('=')
            .with_context(|| format!("--set {kv}: write NAME=value"))?;
        values.insert(k.to_owned(), v.to_owned());
    }
    let variant = variant
        .map(str::to_owned)
        .or_else(|| m.variants.iter().find(|v| v.default).map(|v| v.id.clone()))
        .or_else(|| a.variants.iter().find(|v| v.default).map(|v| v.id.clone()));
    if let Some(v) = &variant
        && !m.variants.is_empty()
        && !m.variants.iter().any(|x| &x.id == v)
    {
        bail!(
            "{id} has no variant {v}: {}",
            m.variants
                .iter()
                .map(|x| x.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    legion_api::app::render(
        &m,
        &a.image,
        variant.as_deref(),
        &values,
        &legion_api::app::random_secret,
    )
    .map_err(anyhow::Error::msg)
}

/// The stack `id` deploys as, `stack` in `namespace`: its objects, what
/// Legion does not honour of it said on stderr, and the variables' values.
pub async fn stack(
    base: &str,
    id: &str,
    set: &[String],
    variant: Option<&str>,
    stack: &str,
    namespace: &str,
) -> anyhow::Result<(legion_api::compose::Stack, BTreeMap<String, String>)> {
    let (compose, env) = rendered(base, id, set, variant).await?;
    let get = |k: &str| env.get(k).cloned();
    let read = |p: &str| -> Result<Vec<u8>, String> { Err(format!("{p}: an app names no files")) };
    let converted = legion_api::compose::convert(
        &compose,
        stack,
        namespace,
        &legion_api::compose::Inputs {
            env: &get,
            read: &read,
        },
    )
    .map_err(|e| anyhow::anyhow!("{id}: {e}"))?;
    for w in &converted.warnings {
        eprintln!("warning: {w}");
    }
    Ok((converted, env))
}

/// `legion app deploy --dry-run`: the objects it would make, printed as
/// `legion stack convert` prints them.
pub async fn dry_run(
    base: &str,
    id: &str,
    set: &[String],
    variant: Option<&str>,
    name: Option<String>,
    namespace: Option<String>,
) -> anyhow::Result<()> {
    let name = name.unwrap_or_else(|| id.to_owned());
    let namespace = namespace.unwrap_or_else(|| name.clone());
    let (converted, _) = stack(base, id, set, variant, &name, &namespace).await?;
    for o in &converted.objects {
        print!(
            "---\n{}",
            serde_yaml_ng::to_string(&serde_json::json!({
                "apiVersion": o.api_version,
                "kind": o.kind,
                "metadata": {
                    "name": o.metadata.name,
                    "namespace": o.metadata.namespace,
                    "labels": o.metadata.labels,
                },
                "spec": o.spec,
            }))?
        );
    }
    Ok(())
}
