//! Legion's OCI hook (DESIGN.md §9). Through Podman, ocijail gives every
//! jail raw sockets, chflags and `enforce_statfs=1`, and annotations can
//! only add to that. A child jail may not have what its parent lacks, so
//! locking down the infra jail would only stop ocijail making the others.
//! Instead, Legion's Podman runs this `createRuntime` hook, which ocijail
//! calls once a container's jail exists and before its process enters it:
//! it sets the parameters in the container's `legion.jail` annotation with
//! `jail -m`. If it fails, so does the container's start.
//!
//! The hook reads the container state ocijail gives it on stdin, in which
//! ocijail puts the jail's id as the annotation `org.freebsd.jail.jid`. The
//! agent wrote the parameters, and the hook takes only the characters they
//! are made of.

use std::path::Path;

const NAME: &str = "legion-jail";

const SCRIPT: &str = r#"#!/bin/sh
# Legion's createRuntime hook: sets the jail parameters in the container's
# legion.jail annotation before its process starts.
state=$(cat)
jid=$(printf '%s\n' "$state" | sed -n 's/.*"org\.freebsd\.jail\.jid":"\([0-9][0-9]*\)".*/\1/p')
params=$(printf '%s\n' "$state" | sed -n 's/.*"legion\.jail":"\([a-z0-9_.= -]*\)".*/\1/p')
if [ -z "$jid" ] || [ -z "$params" ]; then
    echo "legion jail hook: no jail id or parameters in the container state" >&2
    exit 1
fi
exec /usr/sbin/jail -m jid="$jid" $params
"#;

/// Writes the hook into `dir`, Podman's `--hooks-dir`.
pub fn install(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let script = dir.join(format!("{NAME}.sh"));
    std::fs::write(&script, SCRIPT)?;
    let hook = serde_json::json!({
        "version": "1.0.0",
        "hook": { "path": "/bin/sh", "args": ["sh", script] },
        "when": { "annotations": { "^legion\\.jail$": ".+" } },
        "stages": ["createRuntime"],
    });
    std::fs::write(
        dir.join(format!("{NAME}.json")),
        serde_json::to_vec_pretty(&hook).expect("serialises"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installs_the_hook() {
        let dir = tempfile::tempdir().unwrap();
        install(&dir.path().join("hooks")).unwrap();
        let hook: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("hooks/legion-jail.json")).unwrap(),
        )
        .unwrap();
        let script = dir.path().join("hooks").join("legion-jail.sh");
        assert_eq!(hook["hook"]["args"][1], script.to_str().unwrap());
        assert_eq!(hook["when"]["annotations"]["^legion\\.jail$"], ".+");
        assert!(std::fs::read_to_string(script).unwrap().contains("jail -m"));
    }
}
