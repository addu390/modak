//! The tier-key column's native type and its order-preserving codec onto the
//! canonical i64 axis. Comparisons against the native column render native
//! literals, values written to canonical columns render an encoding expression.

use crate::{Result, TierDBError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TierKeyType {
    #[default]
    Bigint,
    Timestamptz,
    Timestamp,
    Date,
}

impl TierKeyType {
    pub fn from_name(name: &str) -> Result<TierKeyType> {
        match name {
            "bigint" => Ok(TierKeyType::Bigint),
            "timestamptz" => Ok(TierKeyType::Timestamptz),
            "timestamp" => Ok(TierKeyType::Timestamp),
            "date" => Ok(TierKeyType::Date),
            other => Err(TierDBError::Catalog(format!(
                "unsupported tier key type: {other}"
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            TierKeyType::Bigint => "bigint",
            TierKeyType::Timestamptz => "timestamptz",
            TierKeyType::Timestamp => "timestamp",
            TierKeyType::Date => "date",
        }
    }

    /// A native SQL literal for comparing against the tier-key column.
    pub fn pg_literal(&self, canonical: i64) -> String {
        match self {
            TierKeyType::Bigint => canonical.to_string(),
            TierKeyType::Timestamptz => {
                format!("TIMESTAMPTZ '{}+00'", timestamp_text(canonical))
            }
            TierKeyType::Timestamp => format!("TIMESTAMP '{}'", timestamp_text(canonical)),
            TierKeyType::Date => format!("DATE '{}'", date_text(canonical)),
        }
    }

    /// A SQL expression encoding a native tier-key value to the canonical i64.
    pub fn canonical_expr(&self, expr: &str) -> String {
        match self {
            TierKeyType::Bigint => expr.to_string(),
            TierKeyType::Timestamptz | TierKeyType::Timestamp => {
                format!("(extract(epoch from {expr}) * 1000000)::bigint")
            }
            TierKeyType::Date => format!("(({expr}) - DATE '1970-01-01')::bigint"),
        }
    }

    /// Parses a native value in text form (JSON or Postgres output) to canonical.
    pub fn encode_text(&self, text: &str) -> Result<i64> {
        let bad =
            || TierDBError::Planning(format!("'{text}' is not a valid {} tier key", self.name()));
        match self {
            TierKeyType::Bigint => text.trim().parse::<i64>().map_err(|_| bad()),
            TierKeyType::Timestamptz | TierKeyType::Timestamp => {
                parse_timestamp_micros(text.trim()).ok_or_else(bad)
            }
            TierKeyType::Date => parse_date_days(text.trim()).ok_or_else(bad),
        }
    }
}

fn parse_date_days(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i64 = text[0..4].parse().ok()?;
    let m: u32 = text[5..7].parse().ok()?;
    let d: u32 = text[8..10].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

fn parse_timestamp_micros(text: &str) -> Option<i64> {
    let (date_part, rest) = if text.len() > 10 {
        (&text[..10], &text[10..])
    } else {
        (text, "")
    };
    let days = parse_date_days(date_part)?;
    let mut micros = days.checked_mul(86_400_000_000)?;
    if rest.is_empty() {
        return Some(micros);
    }

    let rest = rest.strip_prefix(['T', ' '])?;
    let b = rest.as_bytes();
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let h: i64 = rest[0..2].parse().ok()?;
    let mi: i64 = rest[3..5].parse().ok()?;
    let s: i64 = rest[6..8].parse().ok()?;
    if h > 23 || mi > 59 || s > 60 {
        return None;
    }
    micros += (h * 3600 + mi * 60 + s) * 1_000_000;

    let mut tail = &rest[8..];
    if let Some(frac) = tail.strip_prefix('.') {
        let n = frac
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(frac.len());
        let digits = &frac[..n.min(6)];
        let mut v: i64 = digits.parse().ok()?;
        for _ in digits.len()..6 {
            v *= 10;
        }
        micros += v;
        tail = &frac[n..];
    }

    match tail {
        "" | "Z" | "z" => Some(micros),
        _ => {
            let (sign, off) = match tail.as_bytes()[0] {
                b'+' => (1, &tail[1..]),
                b'-' => (-1, &tail[1..]),
                _ => return None,
            };
            let mut parts = off.split(':');
            let oh: i64 = parts.next()?.parse().ok()?;
            let om: i64 = parts.next().map_or(Some(0), |p| p.parse().ok())?;
            let os: i64 = parts.next().map_or(Some(0), |p| p.parse().ok())?;
            Some(micros - sign * (oh * 3600 + om * 60 + os) * 1_000_000)
        }
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn timestamp_text(micros: i64) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let in_day = micros.rem_euclid(86_400_000_000);
    let (y, m, d) = civil_from_days(days);
    let secs = in_day / 1_000_000;
    let frac = in_day % 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

fn date_text(epoch_days: i64) -> String {
    let (y, m, d) = civil_from_days(epoch_days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_stays_a_plain_number() {
        assert_eq!(TierKeyType::Bigint.pg_literal(100), "100");
        assert_eq!(TierKeyType::Bigint.pg_literal(-5), "-5");
        assert_eq!(TierKeyType::Bigint.canonical_expr("m.\"ts\""), "m.\"ts\"");
    }

    #[test]
    fn timestamptz_renders_utc_micros() {
        assert_eq!(
            TierKeyType::Timestamptz.pg_literal(1_782_864_000_000_000),
            "TIMESTAMPTZ '2026-07-01 00:00:00.000000+00'"
        );
        assert_eq!(
            TierKeyType::Timestamptz.pg_literal(0),
            "TIMESTAMPTZ '1970-01-01 00:00:00.000000+00'"
        );
        assert_eq!(
            TierKeyType::Timestamptz.pg_literal(-1),
            "TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'"
        );
    }

    #[test]
    fn timestamp_drops_the_offset() {
        assert_eq!(
            TierKeyType::Timestamp.pg_literal(1_782_909_045_123_456),
            "TIMESTAMP '2026-07-01 12:30:45.123456'"
        );
    }

    #[test]
    fn date_counts_epoch_days() {
        assert_eq!(TierKeyType::Date.pg_literal(0), "DATE '1970-01-01'");
        assert_eq!(TierKeyType::Date.pg_literal(20_635), "DATE '2026-07-01'");
        assert_eq!(TierKeyType::Date.pg_literal(-1), "DATE '1969-12-31'");
    }

    #[test]
    fn canonical_exprs_encode_natives_to_i64() {
        assert_eq!(
            TierKeyType::Timestamptz.canonical_expr("m.\"ts\""),
            "(extract(epoch from m.\"ts\") * 1000000)::bigint"
        );
        assert_eq!(
            TierKeyType::Date.canonical_expr("m.\"d\""),
            "((m.\"d\") - DATE '1970-01-01')::bigint"
        );
    }

    #[test]
    fn text_encoding_matches_the_literals() {
        for micros in [0i64, 1_782_864_000_000_000, -1, 1_782_909_045_123_456] {
            let lit = TierKeyType::Timestamptz.pg_literal(micros);
            let text = lit
                .trim_start_matches("TIMESTAMPTZ '")
                .trim_end_matches('\'');
            assert_eq!(TierKeyType::Timestamptz.encode_text(text).unwrap(), micros);
        }
        for days in [0i64, 20_635, -1] {
            let lit = TierKeyType::Date.pg_literal(days);
            let text = lit.trim_start_matches("DATE '").trim_end_matches('\'');
            assert_eq!(TierKeyType::Date.encode_text(text).unwrap(), days);
        }
    }

    #[test]
    fn text_encoding_accepts_json_and_pg_forms() {
        let t = TierKeyType::Timestamptz;
        assert_eq!(
            t.encode_text("2026-07-01T00:00:00+00:00").unwrap(),
            1_782_864_000_000_000
        );
        assert_eq!(
            t.encode_text("2026-07-01 00:00:00Z").unwrap(),
            1_782_864_000_000_000
        );
        assert_eq!(
            t.encode_text("2026-06-30 20:00:00-04").unwrap(),
            1_782_864_000_000_000
        );
        assert_eq!(
            t.encode_text("2026-07-01 00:00:00.5+00").unwrap(),
            1_782_864_000_500_000
        );
        assert_eq!(t.encode_text("2026-07-01").unwrap(), 1_782_864_000_000_000);
        assert!(t.encode_text("not a time").is_err());
        assert_eq!(TierKeyType::Bigint.encode_text(" 42 ").unwrap(), 42);
        assert!(TierKeyType::Date.encode_text("2026-13-01").is_err());
    }

    #[test]
    fn names_round_trip() {
        for t in [
            TierKeyType::Bigint,
            TierKeyType::Timestamptz,
            TierKeyType::Timestamp,
            TierKeyType::Date,
        ] {
            assert_eq!(TierKeyType::from_name(t.name()).unwrap(), t);
        }
        assert!(TierKeyType::from_name("uuid").is_err());
    }
}
