//! For privileged tests run through `scripts/qualify`, Legion's and jailer's.

/// Whether a test lacking what it needs must fail instead of skipping.
const REQUIRE: &str = "QUALIFY_REQUIRE_LIVE";

/// Reports a test that cannot run here, which then returns: a pass that
/// tested nothing must not pass for one. Panics when a run requires the
/// test; otherwise prints the line `scripts/qualify` records it by.
pub fn skip(why: &str) {
    assert!(
        std::env::var_os(REQUIRE).is_none(),
        "required, but cannot run: {why}"
    );
    println!("QUALIFY-SKIP: {why}");
}
