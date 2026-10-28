//! Wall-clock times as Legion stores them: seconds since the Unix epoch, in
//! UTC, written in status as RFC 3339 (`2026-10-28T09:30:00Z`).

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's
/// `days_from_civil`).
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The date of a day since 1970-01-01: (year, month 1-12, day 1-31).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Day of the week of a day since 1970-01-01, 0 for Sunday.
pub fn weekday(days: i64) -> u32 {
    // 1970-01-01 was a Thursday.
    (days + 4).rem_euclid(7) as u32
}

/// `2026-10-28T09:30:00Z`.
pub fn format(unix: i64) -> String {
    let (days, secs) = (unix.div_euclid(86_400), unix.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// Reads what [`format`] writes.
pub fn parse(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[19] != b'Z' {
        return None;
    }
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let s = s.get(r)?;
        s.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| s.parse().ok())?
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo as u32).into() {
        return None;
    }
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    Some(days_from_civil(y, mo as u32, d as u32) * 86_400 + h * 3600 + mi * 60 + sec)
}

pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Now, in seconds since the epoch.
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        assert_eq!(format(0), "1970-01-01T00:00:00Z");
        assert_eq!(parse("1970-01-01T00:00:00Z"), Some(0));
        // A leap day, and the 400-year rule.
        let t = parse("2024-02-29T23:59:59Z").unwrap();
        assert_eq!(format(t), "2024-02-29T23:59:59Z");
        assert_eq!(format(t + 1), "2024-03-01T00:00:00Z");
        assert_eq!(
            format(parse("2000-02-29T12:00:00Z").unwrap()),
            "2000-02-29T12:00:00Z"
        );
        assert_eq!(parse("2100-02-29T00:00:00Z"), None);
        assert_eq!(parse("2026-10-28T09:30:00Z"), Some(1_793_179_800));
        for bad in [
            "2026-10-28 09:30:00Z",
            "2026-13-01T00:00:00Z",
            "2026-10-28T24:00:00Z",
            "",
        ] {
            assert_eq!(parse(bad), None, "{bad}");
        }
        for days in [-1000, -1, 0, 1, 10_957, 20_000, 100_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }

    #[test]
    fn weekdays() {
        assert_eq!(weekday(0), 4, "a Thursday");
        let wed = parse("2026-10-28T00:00:00Z").unwrap() / 86_400;
        assert_eq!(weekday(wed), 3);
    }
}
