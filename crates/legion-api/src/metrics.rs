//! The Prometheus text format (version 0.0.4), which managers and agents
//! write their metrics in. Each family is written whole: its help, its
//! type and then its samples.

use std::fmt::Write;

/// The content type of [`Metrics::finish`]'s text.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Metrics being written.
#[derive(Debug, Default)]
pub struct Metrics {
    out: String,
}

/// A family whose samples are being written.
pub struct Family<'a> {
    out: &'a mut String,
    name: &'a str,
}

impl Metrics {
    /// Starts a family of gauges: values that go up and down.
    pub fn gauge<'a>(&'a mut self, name: &'a str, help: &str) -> Family<'a> {
        self.family(name, "gauge", help)
    }

    /// Starts a family of counters: totals that only go up (until the
    /// process or the thing counted starts over). Their names end in
    /// `_total`, `_seconds_total` and the like.
    pub fn counter<'a>(&'a mut self, name: &'a str, help: &str) -> Family<'a> {
        self.family(name, "counter", help)
    }

    fn family<'a>(&'a mut self, name: &'a str, kind: &str, help: &str) -> Family<'a> {
        let help = help.replace('\\', "\\\\").replace('\n', "\\n");
        let _ = writeln!(self.out, "# HELP {name} {help}");
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
        Family {
            out: &mut self.out,
            name,
        }
    }

    /// Appends metrics written elsewhere.
    pub fn extend(&mut self, other: Metrics) {
        self.out.push_str(&other.out);
    }

    pub fn finish(self) -> String {
        self.out
    }
}

impl Family<'_> {
    /// Adds a sample with these labels.
    pub fn sample(&mut self, labels: &[(&str, &str)], value: f64) -> &mut Self {
        self.out.push_str(self.name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"");
                for c in v.chars() {
                    match c {
                        '\\' => self.out.push_str("\\\\"),
                        '"' => self.out.push_str("\\\""),
                        '\n' => self.out.push_str("\\n"),
                        c => self.out.push(c),
                    }
                }
                self.out.push('"');
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {}", Value(value));
        self
    }
}

struct Value(f64);

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            v if v.is_nan() => f.write_str("NaN"),
            v if v == f64::INFINITY => f.write_str("+Inf"),
            v if v == f64::NEG_INFINITY => f.write_str("-Inf"),
            v => write!(f, "{v}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_format() {
        let mut m = Metrics::default();
        m.gauge("legion_up", "Whether it is up.").sample(&[], 1.0);
        m.counter("legion_things_total", "Things,\nsplit.")
            .sample(&[("ns", "a\"b\\c"), ("x", "1")], 3.5)
            .sample(&[("ns", "d")], 1e12);
        assert_eq!(
            m.finish(),
            "# HELP legion_up Whether it is up.\n\
             # TYPE legion_up gauge\n\
             legion_up 1\n\
             # HELP legion_things_total Things,\\nsplit.\n\
             # TYPE legion_things_total counter\n\
             legion_things_total{ns=\"a\\\"b\\\\c\",x=\"1\"} 3.5\n\
             legion_things_total{ns=\"d\"} 1000000000000\n"
        );
    }
}
