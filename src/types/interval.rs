use crate::errors::{BigQueryCodecErrorKind, BigQueryError};
use crate::types::error::CodecError;
use serde::{Deserialize, Serialize};
use std::fmt::Write;

pub(crate) const TAG_INTERVAL: &str = "BigQueryInterval";

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// An INTERVAL value as its three independent parts, each with its own sign, the way BigQuery
/// and Arrow's `MonthDayNano` hold it.
///
/// `jiff::Span` cannot hold an interval such as `-1 month +3 days`, which needs two signs, so
/// it converts with `TryFrom` both ways instead. In other serde formats this is a struct of
/// three integers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BigQueryInterval {
    /// The years and months, in months.
    pub months: i32,
    /// The days.
    pub days: i32,
    /// The time part in nanoseconds. BigQuery keeps microseconds, so the write path refuses a
    /// value that is not a whole number of microseconds.
    pub nanos: i64,
}

impl BigQueryInterval {
    /// BigQuery's canonical text, `[-]Y-M [-]D [-]H:M:S[.ffffff]`, the one form Storage Write
    /// was seen to accept. Sub-microsecond nanoseconds are not printed.
    pub(crate) fn write_bq(&self, out: &mut String) {
        let ms = if self.months < 0 { "-" } else { "" };
        let m = self.months.unsigned_abs();
        let _ = write!(out, "{ms}{}-{} {} ", m / 12, m % 12, self.days);
        let ts = if self.nanos < 0 { "-" } else { "" };
        let us = self.nanos.unsigned_abs() / 1000;
        let secs = us / 1_000_000;
        let _ = write!(out, "{ts}{}:{}:{}", secs / 3600, secs / 60 % 60, secs % 60);
        if !us.is_multiple_of(1_000_000) {
            let _ = write!(out, ".{:06}", us % 1_000_000);
        }
    }

    /// Parses the canonical text. A time part that does not fit `i64` nanoseconds is
    /// `OutOfRange`: Storage Read could not send it back.
    pub(crate) fn parse_bq(s: &str) -> Result<BigQueryInterval, CodecError> {
        let bad = || {
            CodecError::new(
                BigQueryCodecErrorKind::InvalidText,
                format!("invalid INTERVAL `{s}`, expected [-]Y-M [-]D [-]H:M:S[.ffffff]"),
            )
        };
        let mut parts = s.split_ascii_whitespace();
        let (ym, d, hms) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(a), Some(b), Some(c), None) => (a, b, c),
            _ => return Err(bad()),
        };
        let unsigned = |p: &str| -> Result<i64, CodecError> {
            if p.is_empty() || !p.bytes().all(|c| c.is_ascii_digit()) {
                return Err(bad());
            }
            p.parse::<i64>()
                .map_err(|_| CodecError::out_of_range(format!("INTERVAL `{s}` is out of range")))
        };
        let (year_sign, ym) = split_sign(ym);
        let (y, mo) = ym.split_once('-').ok_or_else(bad)?;
        let months = unsigned(y)?
            .checked_mul(12)
            .and_then(|m| m.checked_add(unsigned(mo).ok()?))
            .map(|m| year_sign * m);
        let (day_sign, d) = split_sign(d);
        let days = day_sign * unsigned(d)?;
        let (time_sign, hms) = split_sign(hms);
        let mut it = hms.splitn(3, ':');
        let (h, mi, sec) = match (it.next(), it.next(), it.next()) {
            (Some(h), Some(m), Some(s)) => (h, m, s),
            _ => return Err(bad()),
        };
        let (sec, frac) = sec.split_once('.').unwrap_or((sec, ""));
        if frac.len() > 6 || (!frac.is_empty() && !frac.bytes().all(|c| c.is_ascii_digit())) {
            return Err(bad());
        }
        let frac_micros = if frac.is_empty() {
            0
        } else {
            unsigned(frac)? * 10i64.pow(6 - frac.len() as u32)
        };
        let (h, mi, sec) = (unsigned(h)?, unsigned(mi)?, unsigned(sec)?);
        let too_long = || {
            CodecError::out_of_range(format!(
                "INTERVAL `{s}`: the time part does not fit in i64 nanoseconds"
            ))
        };
        let nanos = h
            .checked_mul(3600)
            .and_then(|x| x.checked_add(mi.checked_mul(60)?))
            .and_then(|x| x.checked_add(sec))
            .and_then(|x| x.checked_mul(1_000_000))
            .and_then(|us| us.checked_add(frac_micros))
            .and_then(|us| us.checked_mul(1000))
            .ok_or_else(too_long)?;
        let narrow = |v: Option<i64>| {
            v.and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| CodecError::out_of_range(format!("INTERVAL `{s}` is out of range")))
        };
        Ok(BigQueryInterval {
            months: narrow(months)?,
            days: narrow(Some(days))?,
            nanos: time_sign * nanos,
        })
    }
}

fn split_sign(part: &str) -> (i64, &str) {
    match part.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, part),
    }
}

/// Fails with `OutOfRange` when the parts have different signs, since a span has one sign for
/// all of its units, or when a part is beyond jiff's limits.
impl TryFrom<BigQueryInterval> for jiff::Span {
    type Error = BigQueryError;

    fn try_from(iv: BigQueryInterval) -> Result<Self, Self::Error> {
        let signs = [
            i64::from(iv.months).signum(),
            i64::from(iv.days).signum(),
            iv.nanos.signum(),
        ];
        if signs.contains(&1) && signs.contains(&-1) {
            return Err(CodecError::out_of_range(format!(
                "{iv:?} has parts of both signs, which a jiff::Span cannot hold"
            ))
            .into_deserialize());
        }
        let beyond = |err: jiff::Error| {
            CodecError::out_of_range(format!("{iv:?} does not fit a jiff::Span: {err}"))
                .into_deserialize()
        };
        jiff::Span::new()
            .try_months(iv.months)
            .and_then(|span| span.try_days(iv.days))
            .and_then(|span| span.try_seconds(iv.nanos / NANOS_PER_SECOND))
            .and_then(|span| span.try_nanoseconds(iv.nanos % NANOS_PER_SECOND))
            .map_err(beyond)
    }
}

/// Years and months become months, weeks and days become days, and every unit from hours down
/// becomes nanoseconds. Fails with `OutOfRange` when a part does not fit.
impl TryFrom<jiff::Span> for BigQueryInterval {
    type Error = BigQueryError;

    fn try_from(span: jiff::Span) -> Result<Self, Self::Error> {
        let beyond = || {
            CodecError::out_of_range(format!("{span} does not fit a BigQuery INTERVAL"))
                .into_serialize()
        };
        let months = i64::from(span.get_years()) * 12 + i64::from(span.get_months());
        let days = i64::from(span.get_weeks()) * 7 + i64::from(span.get_days());
        let nanos = i128::from(span.get_hours()) * 3_600 * i128::from(NANOS_PER_SECOND)
            + i128::from(span.get_minutes()) * 60 * i128::from(NANOS_PER_SECOND)
            + i128::from(span.get_seconds()) * i128::from(NANOS_PER_SECOND)
            + i128::from(span.get_milliseconds()) * 1_000_000
            + i128::from(span.get_microseconds()) * 1_000
            + i128::from(span.get_nanoseconds());
        Ok(BigQueryInterval {
            months: i32::try_from(months).map_err(|_| beyond())?,
            days: i32::try_from(days).map_err(|_| beyond())?,
            nanos: i64::try_from(nanos).map_err(|_| beyond())?,
        })
    }
}

#[cfg(test)]
mod tests;
