//! Manifests: objects as users write and read them in YAML, for
//! `legion apply` and `diff` and the console's editor.

use crate::{API_VERSION, Object};

/// The objects of a stream of YAML documents, in order. An empty document
/// (a trailing `---`) is skipped. Errors name the document, counting from 1.
pub fn parse(text: &str) -> Result<Vec<Object>, String> {
    let mut objects = Vec::new();
    for (n, doc) in serde_yaml_ng::Deserializer::from_str(text).enumerate() {
        let at = |e: &dyn std::fmt::Display| format!("document {}: {e}", n + 1);
        let value: serde_json::Value = serde::Deserialize::deserialize(doc).map_err(|e| at(&e))?;
        if value.is_null() {
            continue;
        }
        let object: Object = serde_json::from_value(value).map_err(|e| at(&e))?;
        if object.api_version != API_VERSION {
            return Err(at(&format!("apiVersion must be {API_VERSION}")));
        }
        objects.push(object);
    }
    Ok(objects)
}

/// The parts of `o` a manifest sets, as YAML: no UID, versions or status,
/// so that what the store holds compares with what a user applies.
pub fn yaml(o: &Object) -> String {
    let mut metadata = serde_json::json!({ "name": o.metadata.name });
    if !o.metadata.namespace.is_empty() {
        metadata["namespace"] = o.metadata.namespace.clone().into();
    }
    if !o.metadata.labels.is_empty() {
        metadata["labels"] = serde_json::to_value(&o.metadata.labels).expect("strings");
    }
    serde_yaml_ng::to_string(&serde_json::json!({
        "apiVersion": o.api_version,
        "kind": o.kind,
        "metadata": metadata,
        "spec": o.spec,
    }))
    .expect("JSON values serialise")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_are_read_in_order_and_named_in_errors() {
        let two = "apiVersion: legion/v1\nkind: Config\nmetadata: {name: a, namespace: shop}\n\
                   spec: {data: {}}\n---\n---\napiVersion: legion/v1\nkind: Config\n\
                   metadata: {name: b}\nspec: {data: {}}\n";
        let objects = parse(two).unwrap();
        let names: Vec<_> = objects.iter().map(|o| o.metadata.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let wrong = "apiVersion: legion/v1\nkind: Config\nmetadata: {name: a}\nspec: {}\n---\n\
                     apiVersion: v2\nkind: Config\nmetadata: {name: b}\nspec: {}\n";
        assert_eq!(
            parse(wrong).unwrap_err(),
            "document 2: apiVersion must be legion/v1"
        );
        assert!(parse("kind: [").unwrap_err().starts_with("document 1: "));
    }

    #[test]
    fn a_manifest_leaves_out_what_the_server_sets() {
        let mut o = parse(
            "apiVersion: legion/v1\nkind: Config\nmetadata: {name: a, namespace: shop}\n\
             spec: {data: {}}\n",
        )
        .unwrap()
        .remove(0);
        o.metadata.uid = "u-1".into();
        o.metadata.version = 7;
        o.status = serde_json::json!({ "x": 1 });
        let y = yaml(&o);
        assert!(y.contains("namespace: shop"), "{y}");
        assert!(
            !y.contains("u-1") && !y.contains("version") && !y.contains("status"),
            "{y}"
        );
        assert_eq!(parse(&y).unwrap()[0].spec, o.spec, "it reads back");
    }
}
