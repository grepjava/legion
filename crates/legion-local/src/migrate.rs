//! The state the standalone jailer kept, moved to where `legion local`
//! keeps its own: jailer is part of Legion now. The move is the state
//! directory, and the ZFS dataset under it if there is one. Images and the
//! build cache go across as they are; containers do not, as their jails,
//! clones and pod addresses name where they were made, so the move waits
//! until there are none.

use std::path::Path;

use legion_sys::Sys;

use crate::Error;

/// Where the standalone jailer kept its state by default: `/var/db/jailer`
/// for root, `~/.local/share/jailer` for anyone else.
pub const OLD_DIR_NAME: &str = "jailer";
/// The dataset jailer made under the pool it found, `zroot`.
pub const OLD_DATASET: &str = "zroot/jailer";

/// Moves jailer's state at `old` to `new`, and its dataset `datasets.0` to
/// `datasets.1` when given, if `new` is not there yet and `old` is. Says
/// what it moved; `None` when there was nothing to move.
pub fn jailer_state(
    sys: &dyn Sys,
    old: &Path,
    new: &Path,
    datasets: Option<(&str, &str)>,
) -> Result<Option<String>, Error> {
    if new.exists() || !old.is_dir() {
        return Ok(None);
    }
    let containers = old.join("containers");
    let held = match std::fs::read_dir(&containers) {
        Ok(entries) => entries.count(),
        Err(_) => 0,
    };
    if held > 0 {
        return Err(Error::Runtime(format!(
            "{}: jailer's state, with {held} container(s) in it, which cannot move: \
             remove them with jailer (`jailer rm -f`), or keep that state with \
             --state-dir {}",
            old.display(),
            old.display()
        )));
    }
    let mut said = vec![format!("{} to {}", old.display(), new.display())];
    if let Some((from, to)) = datasets {
        let there = |d: &str| sys.zfs_list(d).map(|l| !l.is_empty());
        if there(from)? && !there(to)? {
            sys.zfs_rename(from, to)?;
            said.push(format!("{from} to {to}"));
        }
    }
    if let Some(parent) = new.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::rename(old, new).map_err(|e| {
        Error::Runtime(format!(
            "moving jailer's state {} to {}: {e}",
            old.display(),
            new.display()
        ))
    })?;
    Ok(Some(format!("moved jailer's state: {}", said.join(", "))))
}

#[cfg(test)]
mod tests {
    use legion_sys::FakeSys;

    use super::*;

    #[test]
    fn jailers_state_moves_once_with_its_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        let (old, new) = (
            tmp.path().join("jailer"),
            tmp.path().join("db/legion-local"),
        );
        std::fs::create_dir_all(old.join("images/layers")).unwrap();
        std::fs::write(old.join("images/index.json"), "{}").unwrap();
        let sys = FakeSys::new();
        sys.zfs_create("zroot/jailer/images", &[]).unwrap();
        sys.zfs_snapshot("zroot/jailer/images@x").unwrap();

        let said = jailer_state(
            &sys,
            &old,
            &new,
            Some(("zroot/jailer", "zroot/legion-local")),
        )
        .unwrap()
        .unwrap();
        assert!(
            said.contains("zroot/jailer to zroot/legion-local"),
            "{said}"
        );
        assert!(!old.exists());
        assert_eq!(
            std::fs::read_to_string(new.join("images/index.json")).unwrap(),
            "{}"
        );
        assert!(sys.zfs_list("zroot/jailer").unwrap().is_empty());
        let names: Vec<String> = sys
            .zfs_list("zroot/legion-local")
            .unwrap()
            .into_iter()
            .map(|d| d.name)
            .collect();
        assert_eq!(names, ["zroot/legion-local", "zroot/legion-local/images"]);
        assert_eq!(
            sys.zfs_snapshots("zroot/legion-local/images").unwrap(),
            ["x"]
        );

        // Once moved, nothing more: the new state is what is used.
        std::fs::create_dir_all(&old).unwrap();
        assert_eq!(jailer_state(&sys, &old, &new, None).unwrap(), None);
    }

    #[test]
    fn nothing_moves_without_jailers_state_or_while_it_holds_containers() {
        let tmp = tempfile::tempdir().unwrap();
        let (old, new) = (tmp.path().join("jailer"), tmp.path().join("legion-local"));
        let sys = FakeSys::new();
        assert_eq!(jailer_state(&sys, &old, &new, None).unwrap(), None);
        assert!(!new.exists());

        std::fs::create_dir_all(old.join("containers/web")).unwrap();
        let err = jailer_state(&sys, &old, &new, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 container(s)"), "{err}");
        assert!(err.contains("--state-dir"), "{err}");
        assert!(old.exists() && !new.exists());
    }
}
