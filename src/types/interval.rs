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
        let month_sign = if self.months < 0 { "-" } else { "" };
        let months = self.months.unsigned_abs();
        let _ = write!(
            out,
            "{month_sign}{}-{} {} ",
            months / 12,
            months % 12,
            self.days
        );
        let time_sign = if self.nanos < 0 { "-" } else { "" };
        let micros = self.nanos.unsigned_abs() / 1000;
        let seconds = micros / 1_000_000;
        let _ = write!(
            out,
            "{time_sign}{}:{}:{}",
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60
        );
        if !micros.is_multiple_of(1_000_000) {
            let _ = write!(out, ".{:06}", micros % 1_000_000);
        }
    }

    /// Parses the canonical text. A time part that does not fit `i64` nanoseconds is
    /// `OutOfRange`: Storage Read could not send it back.
    pub(crate) fn parse_bq(text: &str) -> Result<BigQueryInterval, CodecError> {
        let invalid = || {
            CodecError::new(
                BigQueryCodecErrorKind::InvalidText,
                format!("invalid INTERVAL `{text}`, expected [-]Y-M [-]D [-]H:M:S[.ffffff]"),
            )
        };
        let mut parts = text.split_ascii_whitespace();
        let (year_month, day_part, time_part) =
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(year_month), Some(day_part), Some(time_part), None) => {
                    (year_month, day_part, time_part)
                }
                _ => return Err(invalid()),
            };
        let unsigned = |part: &str| -> Result<i64, CodecError> {
            if part.is_empty() || !part.bytes().all(|digit| digit.is_ascii_digit()) {
                return Err(invalid());
            }
            part.parse::<i64>()
                .map_err(|_| CodecError::out_of_range(format!("INTERVAL `{text}` is out of range")))
        };
        let (year_sign, year_month) = split_sign(year_month);
        let (years, month_part) = year_month.split_once('-').ok_or_else(invalid)?;
        let months = unsigned(years)?
            .checked_mul(12)
            .and_then(|months| months.checked_add(unsigned(month_part).ok()?))
            .map(|months| year_sign * months);
        let (day_sign, day_part) = split_sign(day_part);
        let days = day_sign * unsigned(day_part)?;
        let (time_sign, time_part) = split_sign(time_part);
        let mut pieces = time_part.splitn(3, ':');
        let (hours, minutes, seconds) = match (pieces.next(), pieces.next(), pieces.next()) {
            (Some(hours), Some(minutes), Some(seconds)) => (hours, minutes, seconds),
            _ => return Err(invalid()),
        };
        let (seconds, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
        if fraction.len() > 6
            || (!fraction.is_empty() && !fraction.bytes().all(|digit| digit.is_ascii_digit()))
        {
            return Err(invalid());
        }
        let fraction_micros = if fraction.is_empty() {
            0
        } else {
            unsigned(fraction)? * 10i64.pow(6 - fraction.len() as u32)
        };
        let (hours, minutes, seconds) = (unsigned(hours)?, unsigned(minutes)?, unsigned(seconds)?);
        let too_long = || {
            CodecError::out_of_range(format!(
                "INTERVAL `{text}`: the time part does not fit in i64 nanoseconds"
            ))
        };
        let nanos = hours
            .checked_mul(3600)
            .and_then(|total| total.checked_add(minutes.checked_mul(60)?))
            .and_then(|total| total.checked_add(seconds))
            .and_then(|total| total.checked_mul(1_000_000))
            .and_then(|micros| micros.checked_add(fraction_micros))
            .and_then(|micros| micros.checked_mul(1000))
            .ok_or_else(too_long)?;
        let narrow = |value: Option<i64>| {
            value
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| {
                    CodecError::out_of_range(format!("INTERVAL `{text}` is out of range"))
                })
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

    fn try_from(interval: BigQueryInterval) -> Result<Self, Self::Error> {
        let signs = [
            i64::from(interval.months).signum(),
            i64::from(interval.days).signum(),
            interval.nanos.signum(),
        ];
        if signs.contains(&1) && signs.contains(&-1) {
            return Err(CodecError::out_of_range(format!(
                "{interval:?} has parts of both signs, which a jiff::Span cannot hold"
            ))
            .into_deserialize());
        }
        let beyond = |err: jiff::Error| {
            CodecError::out_of_range(format!("{interval:?} does not fit a jiff::Span: {err}"))
                .into_deserialize()
        };
        jiff::Span::new()
            .try_months(interval.months)
            .and_then(|span| span.try_days(interval.days))
            .and_then(|span| span.try_seconds(interval.nanos / NANOS_PER_SECOND))
            .and_then(|span| span.try_nanoseconds(interval.nanos % NANOS_PER_SECOND))
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
mod tests {
    use super::*;
    use crate::types::testkit::{error_kind, written};

    #[test]
    fn interval_canonical_string_round_trips() {
        let interval = BigQueryInterval {
            months: 14,
            days: 3,
            nanos: (4 * 3600 + 5 * 60 + 6) * 1_000_000_000 + 789_000,
        };
        assert_eq!(
            written(|text| interval.write_bq(text)),
            "1-2 3 4:5:6.000789"
        );
        assert_eq!(
            BigQueryInterval::parse_bq("1-2 3 4:5:6.000789").ok(),
            Some(interval)
        );
        let mixed = BigQueryInterval {
            months: -14,
            days: 3,
            nanos: -6_000_000_000,
        };
        assert_eq!(written(|text| mixed.write_bq(text)), "-1-2 3 -0:0:6");
        assert_eq!(
            BigQueryInterval::parse_bq("-1-2 3 -0:0:6").ok(),
            Some(mixed)
        );
        assert_eq!(
            written(|text| BigQueryInterval::default().write_bq(text)),
            "0-0 0 0:0:0"
        );
        assert_eq!(
            BigQueryInterval::parse_bq("0-0 0 0:0:0").ok(),
            Some(BigQueryInterval::default())
        );
        assert_eq!(
            error_kind(BigQueryInterval::parse_bq("1-2 3")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(
            error_kind(BigQueryInterval::parse_bq("1-2 3 4:5:6.0000001")),
            BigQueryCodecErrorKind::InvalidText
        );
    }

    #[test]
    fn interval_time_part_beyond_i64_nanos_is_an_error() {
        assert_eq!(
            error_kind(BigQueryInterval::parse_bq("0-0 0 87840000:0:0")),
            BigQueryCodecErrorKind::OutOfRange
        );
        let max = BigQueryInterval {
            months: 0,
            days: 0,
            nanos: 9_223_372_036_854_775_000,
        };
        assert_eq!(
            BigQueryInterval::parse_bq(&written(|text| max.write_bq(text))).ok(),
            Some(max)
        );
    }

    #[test]
    fn interval_converts_to_span_only_with_one_sign() {
        let interval = BigQueryInterval {
            months: 14,
            days: 3,
            nanos: 3_600_000_001_000,
        };
        let span = jiff::Span::try_from(interval).expect("valid test input");
        assert_eq!(span.get_months(), 14);
        assert_eq!(span.get_days(), 3);
        assert_eq!(BigQueryInterval::try_from(span).ok(), Some(interval));

        let negative = BigQueryInterval {
            months: -1,
            days: 0,
            nanos: -5,
        };
        let span = jiff::Span::try_from(negative).expect("valid test input");
        assert_eq!(span.signum(), -1);
        assert_eq!(BigQueryInterval::try_from(span).ok(), Some(negative));

        let mixed = BigQueryInterval {
            months: -1,
            days: 3,
            nanos: 0,
        };
        assert!(
            matches!(
                jiff::Span::try_from(mixed),
                Err(BigQueryError::DeserializeError(ref err)) if err.kind == BigQueryCodecErrorKind::OutOfRange
            ),
            "mixed signs cannot be a span"
        );

        let span = jiff::Span::new()
            .years(1)
            .weeks(2)
            .hours(3)
            .minutes(4)
            .seconds(5)
            .milliseconds(6);
        assert_eq!(
            BigQueryInterval::try_from(span).ok(),
            Some(BigQueryInterval {
                months: 12,
                days: 14,
                nanos: ((3 * 3600 + 4 * 60 + 5) * 1000 + 6) * 1_000_000,
            })
        );
        let huge = jiff::Span::new().hours(175_307_616);
        assert!(
            matches!(
                BigQueryInterval::try_from(huge),
                Err(BigQueryError::SerializeError(ref err)) if err.kind == BigQueryCodecErrorKind::OutOfRange
            ),
            "a time part beyond i64 nanoseconds"
        );
    }

    #[test]
    fn interval_serde_form_is_a_struct_of_three_integers() {
        let interval = BigQueryInterval {
            months: 1,
            days: -2,
            nanos: 3,
        };
        let json = serde_json::to_string(&interval).expect("valid test input");
        assert_eq!(json, r#"{"months":1,"days":-2,"nanos":3}"#);
        assert_eq!(
            serde_json::from_str::<BigQueryInterval>(&json).expect("valid test input"),
            interval
        );
    }
}
