//! Putting a host's `/etc/pf.conf` right for Legion (`legion doctor --fix
//! pf`, `legion local doctor --fix pf`): what to add, worked out from the
//! file as it is, so the same plan can be shown before anything changes
//! (the CLI asks; the console shows it on a node's diagnostics) and
//! applied after. Planning reads nothing and changes nothing; [`apply`]
//! changes the host.
//!
//! The file stays the operator's. Only the hook lines are added: the
//! translation hooks (`nat-anchor`, `rdr-anchor`) just before the first
//! filter rule, as pf wants translation before filtering, and the filter
//! hook (`anchor`) at the end. A file another tool writes (Sylve's says
//! so at its top) is left alone, with the lines to add said instead. A
//! ruleset that blocks by default is not changed either: Legion needs the
//! host to pass what its anchors don't refuse (DESIGN.md §5.4), which is a
//! policy for the operator to choose.

use serde::Serialize;

use crate::Sys;
use crate::check::missing_hooks_of;

/// The anchors one program needs called: its name pattern and the three
/// lines calling it, as [`crate::check::pf_anchors`] takes them.
#[derive(Debug, Clone, Copy)]
pub struct Hooks<'a> {
    pub anchor: &'a str,
    pub lines: [&'a str; 3],
}

/// What [`plan`] would do to the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum Plan {
    /// The file already calls every anchor.
    Nothing,
    /// The file as it would be written, and the lines added to it.
    Change {
        new: String,
        added: Vec<String>,
        /// What it does not change and the operator should know.
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    /// The file is not Legion's to change: why, and the lines to add in
    /// the place its owner keeps for them.
    Refused { why: String, lines: Vec<String> },
}

/// A file with nothing in it: Legion's hooks, the loopback without state
/// (local churn reuses ports faster than states expire) and everything
/// else passed, as the README has a dedicated node's file.
fn fresh(sets: &[Hooks]) -> String {
    let mut out = String::from(
        "# Written by legion doctor --fix pf: hooks for Legion's anchors;\n\
         # everything else passes.\n",
    );
    for h in sets {
        out += h.lines[0];
        out += "\n";
        out += h.lines[1];
        out += "\n";
    }
    out += "# Loopback: no state. Heavy local connection churn reuses ports faster\n\
            # than old states expire, and keep state then drops the new SYNs.\n\
            pass quick on lo0 all no state\n\
            pass all\n";
    for h in sets {
        out += h.lines[2];
        out += "\n";
    }
    out
}

/// The lines that carry rules: comments and blank lines dropped, and a
/// line continued with `\` joined to the next, each with the number of the
/// line it starts on.
fn statements(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut open: Option<(usize, String)> = None;
    for (i, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim_end();
        let (start, mut joined) = open.take().unwrap_or((i, String::new()));
        if let Some(head) = line.strip_suffix('\\') {
            joined += head;
            joined += " ";
            open = Some((start, joined));
            continue;
        }
        joined += line;
        let joined = joined.split_whitespace().collect::<Vec<_>>().join(" ");
        if !joined.is_empty() {
            out.push((start, joined));
        }
    }
    out
}

fn first_word(statement: &str) -> &str {
    statement.split_whitespace().next().unwrap_or("")
}

/// Whether a statement is a filter rule, before which translation must go.
fn is_filter(statement: &str) -> bool {
    matches!(first_word(statement), "pass" | "block" | "anchor" | "antispoof" | "match")
}

/// Which tool says it writes the file, from its comments: a file it
/// writes again would lose lines added by hand.
fn owner(text: &str) -> Option<&'static str> {
    let head: String = text.lines().take(10).collect::<Vec<_>>().join("\n").to_lowercase();
    if head.contains("sylve") && (head.contains("generated") || head.contains("don't edit")) {
        Some("Sylve")
    } else if head.contains("automatically generated")
        || head.contains("do not edit")
        || head.contains("don't edit")
    {
        Some("another tool")
    } else {
        None
    }
}

/// Whether the ruleset blocks by default: a `block` with no `pass all`
/// after it. pf takes the last rule that matches, so a later `pass all`
/// passes what the block would have dropped.
fn blocks_by_default(rules: &[(usize, String)]) -> bool {
    let mut blocking = false;
    for (_, r) in rules {
        let words: Vec<_> = r.split_whitespace().collect();
        match words.first() {
            Some(&"block") if !words.contains(&"quick") => blocking = true,
            Some(&"pass") if words.get(1) == Some(&"all") => blocking = false,
            _ => {}
        }
    }
    blocking
}

/// What to do to `current` (the file's text; `None` when there is none)
/// so it calls every one of `sets`.
pub fn plan(current: Option<&str>, sets: &[Hooks]) -> Plan {
    let text = current.unwrap_or("");
    let rules = statements(text);
    if rules.is_empty() {
        let new = fresh(sets);
        let added = new
            .lines()
            .filter(|l| !l.starts_with('#'))
            .map(str::to_owned)
            .collect();
        return Plan::Change { new, added, note: None };
    }
    let mut translation: Vec<&str> = Vec::new();
    let mut filter: Vec<&str> = Vec::new();
    for h in sets {
        for line in missing_hooks_of(text, h.anchor, h.lines) {
            if line.starts_with("anchor") {
                filter.push(line);
            } else {
                translation.push(line);
            }
        }
    }
    if translation.is_empty() && filter.is_empty() {
        return Plan::Nothing;
    }
    let added: Vec<String> = translation.iter().chain(&filter).map(|l| (*l).to_owned()).collect();
    if let Some(who) = owner(text) {
        return Plan::Refused {
            why: format!(
                "{who} writes /etc/pf.conf and would drop lines added to it; add these where {who} keeps rules of your own, or run Legion on a host {who} does not manage"
            ),
            lines: added,
        };
    }
    let lines: Vec<&str> = text.lines().collect();
    // Translation goes just before the first filter rule, else at the end.
    let at = rules
        .iter()
        .find(|(_, r)| is_filter(r))
        .map(|(i, _)| *i)
        .unwrap_or(lines.len());
    let mut out = String::new();
    for (i, line) in lines.iter().enumerate() {
        if i == at && !translation.is_empty() {
            out += "# Added by legion doctor --fix pf: translation for Legion's anchors.\n";
            for l in &translation {
                out += l;
                out += "\n";
            }
        }
        out += line;
        out += "\n";
    }
    if at == lines.len() && !translation.is_empty() {
        out += "# Added by legion doctor --fix pf: translation for Legion's anchors.\n";
        for l in &translation {
            out += l;
            out += "\n";
        }
    }
    if !filter.is_empty() {
        out += "# Added by legion doctor --fix pf: Legion's filter anchors.\n";
        for l in &filter {
            out += l;
            out += "\n";
        }
    }
    let note = blocks_by_default(&rules).then(|| {
        "this ruleset blocks by default, which Legion does not support: its anchors refuse only what network policy names, so pass the rest (a final `pass all`), or pods' traffic between nodes is dropped".to_owned()
    });
    Plan::Change { new: out, added, note }
}

/// The plan as lines a person reads before saying yes: what is added, and
/// where.
pub fn describe(plan: &Plan, path: &str) -> String {
    match plan {
        Plan::Nothing => format!("{path} already calls Legion's anchors.\n"),
        Plan::Refused { why, lines } => {
            let mut s = format!("{path} is left as it is: {why}:\n");
            for l in lines {
                s += &format!("    {l}\n");
            }
            s
        }
        Plan::Change { added, note, .. } => {
            let mut s = format!("Adds to {path}:\n");
            for l in added {
                s += &format!("  + {l}\n");
            }
            if let Some(n) = note {
                s += &format!("Note: {n}.\n");
            }
            s
        }
    }
}

/// What [`apply`] did, for the person to read.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Where the old file was copied, if there was one.
    pub backup: Option<String>,
    pub steps: Vec<String>,
}

/// Writes `plan`'s file at `path` (keeping the old one at `backup`, as
/// `path.legion-<stamp>`), after pf has checked it; then loads pf, enables
/// it at boot and loads the rules. Nothing is written if pf refuses the
/// file. `files` reads and writes the file, so tests need no root.
pub fn apply(
    sys: &dyn Sys,
    files: &dyn Files,
    path: &str,
    plan: &Plan,
    stamp: &str,
) -> Result<Applied, String> {
    let mut done = Applied::default();
    if let Plan::Change { new, .. } = plan {
        sys.run_program("pfctl", &["-nf", "-"], new)
            .map_err(|e| format!("pf refuses the new file, so nothing was changed: {e}"))?;
        if let Some(old) = files.read(path)? {
            let backup = format!("{path}.legion-{stamp}");
            files.write(&backup, &old)?;
            done.steps.push(format!("kept the old file as {backup}"));
            done.backup = Some(backup);
        }
        files.write(path, new)?;
        done.steps.push(format!("wrote {path}"));
    } else if let Plan::Refused { why, .. } = plan {
        return Err(why.clone());
    }
    if sys
        .run_program("sysrc", &["-n", "pf_enable"], "")
        .map(|v| v.trim() != "YES")
        .unwrap_or(true)
    {
        sys.run_program("sysrc", &["pf_enable=YES"], "")
            .map_err(|e| format!("setting pf_enable=YES: {e}"))?;
        done.steps.push("set pf_enable=YES, so pf starts at boot".into());
    }
    if !matches!(sys.kld_state("pf"), Ok(crate::KldState::Loaded)) {
        sys.kld_load("pf").map_err(|e| format!("loading pf: {e}"))?;
        done.steps.push("loaded pf".into());
    }
    sys.run_program("pfctl", &["-f", path], "")
        .map_err(|e| format!("loading {path}: {e}"))?;
    done.steps.push(format!("loaded {path}"));
    // Enabled already, pfctl says so and fails; that is what is wanted.
    match sys.run_program("pfctl", &["-e"], "") {
        Ok(_) => done.steps.push("enabled pf".into()),
        Err(e) if e.to_string().contains("already enabled") => {}
        Err(e) => return Err(format!("enabling pf: {e}")),
    }
    Ok(done)
}

/// `20261122-143005`: a backup's name, in UTC, from `t`.
pub fn stamp(t: std::time::SystemTime) -> String {
    let secs = t.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}-{:02}{:02}{:02}",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// How a way back is said: the copy kept, put back and loaded.
pub fn undo(path: &str, backup: &str) -> String {
    format!("cp {backup} {path} && pfctl -f {path}")
}

/// Reading and writing the file, apart from [`Sys`] so tests can hold it.
pub trait Files {
    fn read(&self, path: &str) -> Result<Option<String>, String>;
    fn write(&self, path: &str, text: &str) -> Result<(), String>;
}

/// The host's own files.
pub struct HostFiles;

impl Files for HostFiles {
    fn read(&self, path: &str) -> Result<Option<String>, String> {
        match std::fs::read_to_string(path) {
            Ok(t) => Ok(Some(t)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("reading {path}: {e}")),
        }
    }
    fn write(&self, path: &str, text: &str) -> Result<(), String> {
        // Written beside it and renamed, so a crash leaves one file or the other.
        let tmp = format!("{path}.legion-new");
        std::fs::write(&tmp, text).map_err(|e| format!("writing {tmp}: {e}"))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("renaming {tmp} to {path}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use super::*;
    use crate::FakeSys;

    const LEGION: Hooks = Hooks {
        anchor: "legion/*",
        lines: ["nat-anchor \"legion/*\"", "rdr-anchor \"legion/*\"", "anchor \"legion/*\""],
    };
    const LOCAL: Hooks = Hooks {
        anchor: "legion-local/*",
        lines: [
            "nat-anchor \"legion-local/*\"",
            "rdr-anchor \"legion-local/*\"",
            "anchor \"legion-local/*\"",
        ],
    };

    fn new_of(p: &Plan) -> &str {
        match p {
            Plan::Change { new, .. } => new,
            other => panic!("not a change: {other:?}"),
        }
    }

    #[test]
    fn an_empty_or_missing_file_gets_a_dedicated_nodes_ruleset() {
        for current in [None, Some(""), Some("# nothing yet\n\n")] {
            let p = plan(current, &[LEGION]);
            let new = new_of(&p);
            let rules: Vec<_> = statements(new).into_iter().map(|(_, r)| r).collect();
            assert_eq!(
                rules,
                [
                    "nat-anchor \"legion/*\"",
                    "rdr-anchor \"legion/*\"",
                    "pass quick on lo0 all no state",
                    "pass all",
                    "anchor \"legion/*\""
                ]
            );
            // Run again on what it wrote, it changes nothing.
            assert_eq!(plan(Some(new), &[LEGION]), Plan::Nothing);
        }
    }

    #[test]
    fn translation_goes_before_the_first_filter_rule_and_the_anchor_last() {
        let current = "ext_if = \"em0\"\nset skip on lo0\nscrub in all\n\
                       nat on $ext_if from 10.0.0.0/8 to any -> ($ext_if)\n\
                       # Filtering.\npass in all\npass out all\n";
        let p = plan(Some(current), &[LEGION]);
        let new = new_of(&p);
        let rules: Vec<_> = statements(new).into_iter().map(|(_, r)| r).collect();
        assert_eq!(
            rules,
            [
                "ext_if = \"em0\"",
                "set skip on lo0",
                "scrub in all",
                "nat on $ext_if from 10.0.0.0/8 to any -> ($ext_if)",
                "nat-anchor \"legion/*\"",
                "rdr-anchor \"legion/*\"",
                "pass in all",
                "pass out all",
                "anchor \"legion/*\""
            ]
        );
        assert!(new.contains("# Filtering.\n"), "the operator's comments stay: {new}");
        assert_eq!(plan(Some(new), &[LEGION]), Plan::Nothing);
    }

    #[test]
    fn only_what_is_missing_is_added() {
        let current = "nat-anchor \"legion/*\"\npass all\n";
        match plan(Some(current), &[LEGION]) {
            Plan::Change { added, .. } => {
                assert_eq!(added, ["rdr-anchor \"legion/*\"", "anchor \"legion/*\""])
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn several_programs_hooks_are_added_together() {
        let p = plan(Some("pass all\n"), &[LEGION, LOCAL]);
        let new = new_of(&p);
        assert_eq!(plan(Some(new), &[LEGION, LOCAL]), Plan::Nothing);
        assert_eq!(plan(Some(new), &[LOCAL]), Plan::Nothing);
    }

    #[test]
    fn a_file_another_tool_writes_is_left_alone() {
        let sylve = "# === This file is automatically generated by Sylve, don't edit! ===\n\n\
                     table <cni-nat> persist\nnat-anchor \"sylve/nat-rules\" all\n";
        match plan(Some(sylve), &[LEGION]) {
            Plan::Refused { why, lines } => {
                assert!(why.starts_with("Sylve writes /etc/pf.conf"), "{why}");
                assert_eq!(lines.len(), 3);
            }
            other => panic!("{other:?}"),
        }
        let other = "# Automatically generated by somebody\npass all\n";
        assert!(matches!(plan(Some(other), &[LEGION]), Plan::Refused { .. }));
    }

    #[test]
    fn a_default_block_ruleset_is_changed_only_with_a_warning() {
        let current = "block in all\npass out all keep state\npass in proto tcp to port 22\n";
        match plan(Some(current), &[LEGION]) {
            Plan::Change { note: Some(n), .. } => assert!(n.contains("blocks by default"), "{n}"),
            other => panic!("{other:?}"),
        }
        let open = "block in all\npass all\n";
        assert!(matches!(plan(Some(open), &[LEGION]), Plan::Change { note: None, .. }));
    }

    #[test]
    fn continued_lines_and_comments_are_read_as_pf_reads_them() {
        let current = "pass in proto tcp \\\n  to port { 22 80 }  # ssh, web\n";
        let rules = statements(current);
        assert_eq!(rules, [(0, "pass in proto tcp to port { 22 80 }".to_owned())]);
        let new = new_of(&plan(Some(current), &[LEGION])).to_owned();
        assert!(
            new.starts_with("# Added by legion doctor --fix pf: translation for Legion's anchors.\n"),
            "{new}"
        );
    }

    struct MemFiles(RefCell<BTreeMap<String, String>>);
    impl Files for MemFiles {
        fn read(&self, path: &str) -> Result<Option<String>, String> {
            Ok(self.0.borrow().get(path).cloned())
        }
        fn write(&self, path: &str, text: &str) -> Result<(), String> {
            self.0.borrow_mut().insert(path.into(), text.into());
            Ok(())
        }
    }

    #[test]
    fn applying_keeps_the_old_file_checks_the_new_and_loads_it() {
        let sys = FakeSys::new();
        let files = MemFiles(RefCell::new(BTreeMap::from([(
            "/etc/pf.conf".to_owned(),
            "pass all\n".to_owned(),
        )])));
        let p = plan(Some("pass all\n"), &[LEGION]);
        let done = apply(&sys, &files, "/etc/pf.conf", &p, "20261122-1200").unwrap();
        assert_eq!(done.backup.as_deref(), Some("/etc/pf.conf.legion-20261122-1200"));
        let all = files.0.borrow();
        assert_eq!(all["/etc/pf.conf.legion-20261122-1200"], "pass all\n");
        assert_eq!(all["/etc/pf.conf"], new_of(&p));
        let ran = sys.programs_run();
        let at = |c: &str| ran.iter().position(|r| r == c).unwrap_or_else(|| panic!("{c} not run: {ran:?}"));
        assert!(at("pfctl -nf -") < at("pfctl -f /etc/pf.conf"), "checked before loaded");
        assert!(ran.contains(&"sysrc pf_enable=YES".to_owned()), "{ran:?}");
        assert_eq!(undo("/etc/pf.conf", done.backup.as_deref().unwrap()),
            "cp /etc/pf.conf.legion-20261122-1200 /etc/pf.conf && pfctl -f /etc/pf.conf");
    }

    #[test]
    fn a_file_pf_refuses_is_not_written() {
        let sys = FakeSys::new();
        sys.fail_next("pfctl -nf -");
        let files = MemFiles(RefCell::new(BTreeMap::from([(
            "/etc/pf.conf".to_owned(),
            "pass all\n".to_owned(),
        )])));
        let p = plan(Some("pass all\n"), &[LEGION]);
        let e = apply(&sys, &files, "/etc/pf.conf", &p, "x").unwrap_err();
        assert!(e.contains("nothing was changed"), "{e}");
        assert_eq!(files.0.borrow()["/etc/pf.conf"], "pass all\n");
        assert_eq!(files.0.borrow().len(), 1, "no copy either");
    }

    #[test]
    fn stamps_are_utc_dates() {
        let at = |s: u64| stamp(std::time::UNIX_EPOCH + std::time::Duration::from_secs(s));
        assert_eq!(at(0), "19700101-000000");
        // 2026-11-22 14:30:05 UTC.
        assert_eq!(at(1_795_357_805), "20261122-143005");
        assert_eq!(at(951_782_400), "20000229-000000");
    }

    #[test]
    fn a_refused_plan_changes_nothing() {
        let sys = FakeSys::new();
        let files = MemFiles(RefCell::new(BTreeMap::new()));
        let p = Plan::Refused { why: "Sylve writes it".into(), lines: vec![] };
        assert!(apply(&sys, &files, "/etc/pf.conf", &p, "x").is_err());
        assert!(sys.programs_run().is_empty());
    }
}
