//! Generated gRPC code for Legion's API and Raft transport, plus conversions
//! between the wire envelope and [`legion_api::Object`], and the TLS
//! transport ([`tls`]).

pub mod tls;

pub mod v1 {
    tonic::include_proto!("legion.v1");
}

use legion_api::{API_VERSION, Kind, ObjectKey, ObjectMeta};

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error(transparent)]
    Api(#[from] legion_api::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
}

fn json_bytes(value: &serde_json::Value) -> Vec<u8> {
    if value.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(value).expect("a Value always serializes")
    }
}

fn json_value(bytes: &[u8]) -> Result<serde_json::Value, serde_json::Error> {
    if bytes.is_empty() {
        Ok(serde_json::Value::Null)
    } else {
        serde_json::from_slice(bytes)
    }
}

impl From<&legion_api::Object> for v1::Object {
    fn from(o: &legion_api::Object) -> Self {
        v1::Object {
            kind: o.kind.to_string(),
            namespace: o.metadata.namespace.clone(),
            name: o.metadata.name.clone(),
            labels: o.metadata.labels.clone().into_iter().collect(),
            uid: o.metadata.uid.clone(),
            version: o.metadata.version,
            generation: o.metadata.generation,
            spec_json: json_bytes(&o.spec),
            status_json: json_bytes(&o.status),
        }
    }
}

impl TryFrom<v1::Object> for legion_api::Object {
    type Error = ConvertError;

    fn try_from(o: v1::Object) -> Result<Self, ConvertError> {
        Ok(legion_api::Object {
            api_version: API_VERSION.to_owned(),
            kind: o.kind.parse()?,
            metadata: ObjectMeta {
                name: o.name,
                namespace: o.namespace,
                labels: o.labels.into_iter().collect(),
                uid: o.uid,
                version: o.version,
                generation: o.generation,
            },
            spec: json_value(&o.spec_json)?,
            status: json_value(&o.status_json)?,
        })
    }
}

impl From<&ObjectKey> for v1::ObjectRef {
    fn from(k: &ObjectKey) -> Self {
        v1::ObjectRef {
            kind: k.kind.to_string(),
            namespace: k.namespace.clone(),
            name: k.name.clone(),
        }
    }
}

impl TryFrom<v1::ObjectRef> for ObjectKey {
    type Error = ConvertError;

    fn try_from(r: v1::ObjectRef) -> Result<Self, ConvertError> {
        let kind: Kind = r.kind.parse()?;
        Ok(ObjectKey::new(kind, &r.namespace, &r.name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_round_trip() {
        let mut obj = legion_api::Object::new(
            Kind::Service,
            "shop",
            "web",
            serde_json::json!({ "replicas": 3 }),
        );
        obj.metadata.labels.insert("app".into(), "web".into());
        obj.metadata.version = 7;
        let wire = v1::Object::from(&obj);
        assert!(wire.status_json.is_empty());
        assert_eq!(legion_api::Object::try_from(wire).unwrap(), obj);
    }

    #[test]
    fn bad_kind() {
        let wire = v1::Object {
            kind: "Pod".into(),
            ..Default::default()
        };
        assert!(legion_api::Object::try_from(wire).is_err());
    }
}
