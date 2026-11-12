//! The web console's pages (DESIGN.md §3.9): the Svelte app's build,
//! embedded at compile time, for the manager's gateway to serve. Node is
//! needed only to make `dist/`, which is kept in the repository, so a
//! build of Legion needs no Node.

include!(concat!(env!("OUT_DIR"), "/assets.rs"));

/// The file at `path` (relative, no leading `/`) and its media type.
pub fn asset(path: &str) -> Option<(&'static [u8], &'static str)> {
    ASSETS
        .iter()
        .find(|(p, _, _)| *p == path)
        .map(|(_, bytes, media)| (*bytes, *media))
}

/// Whether the build had the app's pages.
pub fn built() -> bool {
    asset("index.html").is_some()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_path_outside_the_build_is_not_found() {
        assert!(super::asset("../Cargo.toml").is_none());
        assert!(super::asset("").is_none());
    }
}
