//! Cron schedules for `CronJob`: the five fields of crontab(5) (minute,
//! hour, day of month, month, day of week), with `*`, ranges, steps, lists,
//! month and day names, and the `@hourly` ... `@yearly` shorthands. Times
//! are UTC.
//!
//! As in Vixie cron, when both day fields are restricted a day matches if
//! either does; when one is `*`, both must.

use std::fmt;
use std::str::FromStr;

use crate::clock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    minutes: u64,
    hours: u32,
    /// Bit `d` for day `d`, 1-31.
    days: u32,
    /// Bit `m` for month `m`, 1-12.
    months: u16,
    /// Bit 0 for Sunday.
    weekdays: u8,
    any_day: bool,
    any_weekday: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const WEEKDAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// How far ahead [`Schedule::next_after`] looks: past any leap-day cycle.
const HORIZON_DAYS: i64 = 366 * 8;

impl FromStr for Schedule {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let expanded = match s.trim() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            other if other.starts_with('@') => {
                return Err(Error(format!("unknown schedule {other:?}")));
            }
            other => other,
        };
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        let [minute, hour, day, month, weekday] = fields[..] else {
            return Err(Error(format!(
                "a schedule has five fields (minute hour day month weekday), not {}",
                fields.len()
            )));
        };
        let weekdays = field(weekday, "day of week", 0, 7, &WEEKDAYS)?;
        // 7 is Sunday too.
        let weekdays = (weekdays | (weekdays >> 7)) & 0x7f;
        let schedule = Schedule {
            minutes: field(minute, "minute", 0, 59, &[])?,
            hours: field(hour, "hour", 0, 23, &[])? as u32,
            days: field(day, "day of month", 1, 31, &[])? as u32,
            months: field(month, "month", 1, 12, &MONTHS)? as u16,
            weekdays: weekdays as u8,
            any_day: day.starts_with('*'),
            any_weekday: weekday.starts_with('*'),
        };
        if schedule.next_after(0).is_none() {
            return Err(Error(format!("{s:?} never matches a date")));
        }
        Ok(schedule)
    }
}

/// A field as a bit set over `lo..=hi`. `names` spell values from `lo`.
fn field(s: &str, what: &str, lo: u32, hi: u32, names: &[&str]) -> Result<u64, Error> {
    let bad = |why: String| Error(format!("{what} {s:?}: {why}"));
    let value = |v: &str| -> Result<u32, Error> {
        if let Some(i) = names.iter().position(|n| n.eq_ignore_ascii_case(v)) {
            return Ok(lo + i as u32);
        }
        let n: u32 = v
            .parse()
            .map_err(|_| bad(format!("{v:?} is not a number")))?;
        if n < lo || n > hi {
            return Err(bad(format!("{n} is outside {lo}-{hi}")));
        }
        Ok(n)
    };
    let mut bits = 0u64;
    for item in s.split(',') {
        let (range, step) = match item.split_once('/') {
            Some((r, step)) => {
                let step: u32 = step
                    .parse()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| bad(format!("step {step:?} must be a positive number")))?;
                (r, Some(step))
            }
            None => (item, None),
        };
        let (from, to) = match range {
            "*" => (lo, hi),
            r => match r.split_once('-') {
                Some((a, b)) => (value(a)?, value(b)?),
                // `5/15`: from 5 to the end, every 15.
                None if step.is_some() => (value(r)?, hi),
                None => {
                    let v = value(r)?;
                    (v, v)
                }
            },
        };
        if from > to {
            return Err(bad(format!("range {from}-{to} runs backwards")));
        }
        let mut v = from;
        while v <= to {
            bits |= 1 << v;
            v += step.unwrap_or(1);
        }
    }
    Ok(bits)
}

impl Schedule {
    fn day_matches(&self, days: i64) -> bool {
        let (_, month, day) = clock::civil_from_days(days);
        if self.months & (1 << month) == 0 {
            return false;
        }
        let by_day = self.days & (1 << day) != 0;
        let by_weekday = self.weekdays & (1 << clock::weekday(days)) != 0;
        if self.any_day || self.any_weekday {
            by_day && by_weekday
        } else {
            by_day || by_weekday
        }
    }

    /// The first time the schedule is due strictly after `t`, in seconds
    /// since the epoch, on a whole minute.
    pub fn next_after(&self, t: i64) -> Option<i64> {
        let start = (t.div_euclid(60) + 1) * 60;
        let first = start.div_euclid(86_400);
        for days in first..first + HORIZON_DAYS {
            if !self.day_matches(days) {
                continue;
            }
            for hour in (0..24).filter(|h| self.hours & (1 << h) != 0) {
                for minute in (0..60).filter(|m| self.minutes & (1 << m) != 0) {
                    let at = days * 86_400 + hour * 3600 + minute * 60;
                    if at >= start {
                        return Some(at);
                    }
                }
            }
        }
        None
    }

    /// The last time the schedule was due at or before `t`, if it was after
    /// `since`: the run that is owed, when several were missed.
    pub fn latest_in(&self, since: i64, t: i64) -> Option<i64> {
        let last = t.div_euclid(86_400);
        let first = since.div_euclid(86_400).max(last - HORIZON_DAYS);
        for days in (first..=last).rev() {
            if !self.day_matches(days) {
                continue;
            }
            for hour in (0..24).rev().filter(|h| self.hours & (1 << h) != 0) {
                for minute in (0..60).rev().filter(|m| self.minutes & (1 << m) != 0) {
                    let at = days * 86_400 + hour * 3600 + minute * 60;
                    if at <= since {
                        return None;
                    }
                    if at <= t {
                        return Some(at);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> i64 {
        clock::parse(s).unwrap()
    }

    fn next(schedule: &str, after: &str) -> String {
        let s: Schedule = schedule.parse().unwrap();
        clock::format(s.next_after(at(after)).unwrap())
    }

    #[test]
    fn next_times() {
        let wed = "2026-10-28T09:30:00Z";
        assert_eq!(next("* * * * *", wed), "2026-10-28T09:31:00Z");
        assert_eq!(next("*/15 * * * *", wed), "2026-10-28T09:45:00Z");
        assert_eq!(next("5/15 * * * *", wed), "2026-10-28T09:35:00Z");
        assert_eq!(next("0 9-17/4 * * *", wed), "2026-10-28T13:00:00Z");
        assert_eq!(next("@daily", wed), "2026-10-29T00:00:00Z");
        assert_eq!(next("@hourly", wed), "2026-10-28T10:00:00Z");
        assert_eq!(next("@weekly", wed), "2026-11-01T00:00:00Z");
        assert_eq!(next("@monthly", wed), "2026-11-01T00:00:00Z");
        assert_eq!(next("@yearly", wed), "2027-01-01T00:00:00Z");
        assert_eq!(next("30 2 * * mon-fri", wed), "2026-10-29T02:30:00Z");
        assert_eq!(next("0 0 * * SAT,sun", wed), "2026-10-31T00:00:00Z");
        assert_eq!(next("0 0 * * 7", wed), "2026-11-01T00:00:00Z");
        assert_eq!(next("0 0 1 feb *", wed), "2027-02-01T00:00:00Z");
        // Leap day: four years at most.
        assert_eq!(next("0 0 29 2 *", wed), "2028-02-29T00:00:00Z");
        // Both day fields restricted: either matches (the 1st, or a Monday).
        assert_eq!(next("0 0 1 * mon", wed), "2026-11-01T00:00:00Z");
        assert_eq!(
            next("0 0 13 * fri", "2026-11-01T00:00:00Z"),
            "2026-11-06T00:00:00Z"
        );
        // One of them `*`: both must, and `*` is every day.
        assert_eq!(next("0 0 13 * *", wed), "2026-11-13T00:00:00Z");
        assert_eq!(next("0 0 */1 * fri", wed), "2026-10-30T00:00:00Z");
        // Strictly after, on the minute.
        assert_eq!(next("30 9 * * *", wed), "2026-10-29T09:30:00Z");
        assert_eq!(
            next("31 9 * * *", "2026-10-28T09:30:59Z"),
            "2026-10-28T09:31:00Z"
        );
    }

    #[test]
    fn owed_runs() {
        let s: Schedule = "0 * * * *".parse().unwrap();
        let since = at("2026-10-28T09:30:00Z");
        assert_eq!(s.latest_in(since, at("2026-10-28T09:59:00Z")), None);
        assert_eq!(
            s.latest_in(since, at("2026-10-28T12:10:00Z"))
                .map(clock::format),
            Some("2026-10-28T12:00:00Z".into()),
            "of three missed, only the latest is owed"
        );
        assert_eq!(
            s.latest_in(since, at("2026-10-28T10:00:00Z"))
                .map(clock::format),
            Some("2026-10-28T10:00:00Z".into())
        );
    }

    #[test]
    fn errors() {
        for (bad, why) in [
            ("* * * *", "five fields"),
            ("60 * * * *", "minute \"60\": 60 is outside 0-59"),
            ("* * 0 * *", "day of month"),
            ("*/0 * * * *", "step"),
            ("5-1 * * * *", "runs backwards"),
            ("* * * foo *", "\"foo\" is not a number"),
            ("0 0 31 2 *", "never matches"),
            ("@often", "unknown schedule"),
        ] {
            let e = bad.parse::<Schedule>().unwrap_err().to_string();
            assert!(e.contains(why), "{bad}: {e}");
        }
    }
}
