//! CPU, byte and duration quantities as manifests write them: `500m`,
//! `256Mi`, `1m30s`. Each parses from a string or a plain number and
//! serialises in one canonical form, so applying `0.5` or `500m` stores the
//! same spec.

use std::fmt;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// CPU in millicores: `500m`, `1`, `0.5`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Cpu(pub u64);

impl Cpu {
    pub fn millis(self) -> u64 {
        self.0
    }
}

impl FromStr for Cpu {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || {
            format!(
                "invalid CPU quantity {s:?}: expected cores (`1`, `0.5`) or millicores (`500m`)"
            )
        };
        if let Some(m) = s.strip_suffix('m') {
            return m.parse().map(Cpu).map_err(|_| bad());
        }
        let cores: f64 = s.parse().map_err(|_| bad())?;
        cpu_from_cores(cores).ok_or_else(bad)
    }
}

fn cpu_from_cores(cores: f64) -> Option<Cpu> {
    let millis = cores * 1000.0;
    (cores.is_finite() && cores >= 0.0 && millis.fract().abs() < 1e-6 && millis < u64::MAX as f64)
        .then(|| Cpu(millis.round() as u64))
}

impl fmt::Display for Cpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_multiple_of(1000) {
            write!(f, "{}", self.0 / 1000)
        } else {
            write!(f, "{}m", self.0)
        }
    }
}

/// A byte count: `256Mi`, `1G`, `4096`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Bytes(pub u64);

const BINARY: [(&str, u64); 5] = [
    ("Pi", 1 << 50),
    ("Ti", 1 << 40),
    ("Gi", 1 << 30),
    ("Mi", 1 << 20),
    ("Ki", 1 << 10),
];
const DECIMAL: [(&str, u64); 5] = [
    ("P", 1_000_000_000_000_000),
    ("T", 1_000_000_000_000),
    ("G", 1_000_000_000),
    ("M", 1_000_000),
    ("k", 1_000),
];

impl FromStr for Bytes {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || {
            format!(
                "invalid byte quantity {s:?}: expected a number with an optional suffix (Ki, Mi, Gi, Ti, k, M, G, T)"
            )
        };
        let (digits, unit) = BINARY
            .iter()
            .chain(&DECIMAL)
            .find_map(|&(suffix, unit)| s.strip_suffix(suffix).map(|d| (d, unit)))
            .unwrap_or((s, 1));
        let n: u64 = digits.parse().map_err(|_| bad())?;
        n.checked_mul(unit).map(Bytes).ok_or_else(bad)
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.0;
        if n > 0 {
            for (suffix, unit) in BINARY.iter().chain(&DECIMAL) {
                if n.is_multiple_of(*unit) {
                    return write!(f, "{}{suffix}", n / unit);
                }
            }
        }
        write!(f, "{n}")
    }
}

/// A duration with millisecond precision: `500ms`, `5s`, `1m30s`, `2h`.
/// A plain number is seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(pub u64);

impl Duration {
    pub const fn from_secs(s: u64) -> Self {
        Duration(s * 1000)
    }

    pub fn millis(self) -> u64 {
        self.0
    }

    pub fn as_std(self) -> std::time::Duration {
        std::time::Duration::from_millis(self.0)
    }
}

impl FromStr for Duration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("invalid duration {s:?}: expected for example `500ms`, `5s`, `1m30s`");
        if let Ok(secs) = s.parse::<u64>() {
            return secs.checked_mul(1000).map(Duration).ok_or_else(bad);
        }
        let mut rest = s;
        let mut total: u64 = 0;
        if rest.is_empty() {
            return Err(bad());
        }
        while !rest.is_empty() {
            let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
            if digits == 0 {
                return Err(bad());
            }
            let n: u64 = rest[..digits].parse().map_err(|_| bad())?;
            rest = &rest[digits..];
            let (unit, len) = if rest.starts_with("ms") {
                (1, 2)
            } else if rest.starts_with('s') {
                (1000, 1)
            } else if rest.starts_with('m') {
                (60_000, 1)
            } else if rest.starts_with('h') {
                (3_600_000, 1)
            } else {
                return Err(bad());
            };
            rest = &rest[len..];
            total = n
                .checked_mul(unit)
                .and_then(|v| total.checked_add(v))
                .ok_or_else(bad)?;
        }
        Ok(Duration(total))
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ms = self.0;
        if ms == 0 {
            return f.write_str("0s");
        }
        for (suffix, unit) in [("h", 3_600_000), ("m", 60_000), ("s", 1000), ("ms", 1)] {
            if ms >= unit {
                write!(f, "{}{suffix}", ms / unit)?;
                ms %= unit;
            }
        }
        Ok(())
    }
}

/// Serde for the three types: a string in canonical form out; a string or a
/// number in.
macro_rules! quantity_serde {
    ($ty:ident, $expecting:literal, |$n:ident| $from_u64:expr, |$x:ident| $from_f64:expr) => {
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl Visitor<'_> for V {
                    type Value = $ty;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str($expecting)
                    }
                    fn visit_str<E: de::Error>(self, s: &str) -> Result<$ty, E> {
                        s.parse().map_err(E::custom)
                    }
                    fn visit_u64<E: de::Error>(self, $n: u64) -> Result<$ty, E> {
                        $from_u64.ok_or_else(|| {
                            E::custom(concat!("out of range: expected ", $expecting))
                        })
                    }
                    fn visit_i64<E: de::Error>(self, n: i64) -> Result<$ty, E> {
                        u64::try_from(n)
                            .map_err(|_| E::custom(concat!("negative: expected ", $expecting)))
                            .and_then(|n| self.visit_u64(n))
                    }
                    fn visit_f64<E: de::Error>(self, $x: f64) -> Result<$ty, E> {
                        $from_f64.ok_or_else(|| E::custom(concat!("expected ", $expecting)))
                    }
                }
                d.deserialize_any(V)
            }
        }
    };
}

quantity_serde!(
    Cpu,
    "a CPU quantity such as `500m` or `1`",
    |n| n.checked_mul(1000).map(Cpu),
    |x| cpu_from_cores(x)
);
quantity_serde!(
    Bytes,
    "a byte quantity such as `256Mi`",
    |n| Some(Bytes(n)),
    |x| (x.fract() == 0.0 && x >= 0.0 && x < u64::MAX as f64).then_some(Bytes(x as u64))
);
quantity_serde!(
    Duration,
    "a duration such as `5s`",
    |n| n.checked_mul(1000).map(Duration),
    |x| (x.is_finite() && x >= 0.0 && (x * 1000.0).fract().abs() < 1e-6)
        .then(|| Duration((x * 1000.0).round() as u64))
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu() {
        assert_eq!("500m".parse::<Cpu>().unwrap(), Cpu(500));
        assert_eq!("0.5".parse::<Cpu>().unwrap(), Cpu(500));
        assert_eq!("2".parse::<Cpu>().unwrap(), Cpu(2000));
        assert!("0.0005".parse::<Cpu>().is_err());
        assert!("-1".parse::<Cpu>().is_err());
        assert_eq!(Cpu(500).to_string(), "500m");
        assert_eq!(Cpu(2000).to_string(), "2");
        assert_eq!(serde_json::from_str::<Cpu>("0.25").unwrap(), Cpu(250));
        assert_eq!(serde_json::from_str::<Cpu>("1").unwrap(), Cpu(1000));
    }

    #[test]
    fn bytes() {
        assert_eq!("256Mi".parse::<Bytes>().unwrap(), Bytes(256 << 20));
        assert_eq!("1G".parse::<Bytes>().unwrap(), Bytes(1_000_000_000));
        assert_eq!("4096".parse::<Bytes>().unwrap(), Bytes(4096));
        assert!("1.5Gi".parse::<Bytes>().is_err());
        assert!("lots".parse::<Bytes>().is_err());
        assert_eq!(Bytes(512 << 20).to_string(), "512Mi");
        assert_eq!(Bytes(1_000_000).to_string(), "1M");
        assert_eq!(Bytes(1000 << 20).to_string(), "1000Mi");
        assert_eq!(Bytes(1234).to_string(), "1234");
        assert_eq!(serde_json::from_str::<Bytes>("1024").unwrap(), Bytes(1024));
    }

    #[test]
    fn durations() {
        assert_eq!("5s".parse::<Duration>().unwrap(), Duration(5000));
        assert_eq!("1m30s".parse::<Duration>().unwrap(), Duration(90_000));
        assert_eq!("250ms".parse::<Duration>().unwrap(), Duration(250));
        assert_eq!("10".parse::<Duration>().unwrap(), Duration(10_000));
        assert!("".parse::<Duration>().is_err());
        assert!("5x".parse::<Duration>().is_err());
        assert!("s".parse::<Duration>().is_err());
        assert_eq!(Duration(90_500).to_string(), "1m30s500ms");
        assert_eq!(Duration(0).to_string(), "0s");
        assert_eq!(Duration::from_secs(7200).to_string(), "2h");
        assert_eq!(
            serde_json::from_str::<Duration>("1.5").unwrap(),
            Duration(1500)
        );
    }
}
