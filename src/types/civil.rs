//! BigQuery's civil and absolute time ranges, their text forms and their integer encodings.
//!
//! All integers here are BigQuery's own units: DATE in days since 1970-01-01, TIME in
//! microseconds since midnight, DATETIME in civil microseconds since 1970-01-01T00:00:00 with no
//! zone, and TIMESTAMP in microseconds since the Unix epoch. The text parsers cover BigQuery's
//! whole range, which is wider than `jiff::Timestamp`'s at the top.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;
use jiff::fmt::temporal::DateTimePrinter;
use jiff::SignedDuration;

pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;
/// `0001-01-01`, BigQuery's smallest DATE.
pub(crate) const DATE_MIN_DAYS: i32 = -719_162;
/// `9999-12-31`, BigQuery's largest DATE.
pub(crate) const DATE_MAX_DAYS: i32 = 2_932_896;
/// `0001-01-01T00:00:00Z`, BigQuery's smallest TIMESTAMP, and the smallest DATETIME as civil
/// microseconds.
pub(crate) const TIMESTAMP_MIN_MICROS: i64 = -62_135_596_800_000_000;
/// `9999-12-31T23:59:59.999999Z`, BigQuery's largest TIMESTAMP, and the largest DATETIME as
/// civil microseconds.
pub(crate) const TIMESTAMP_MAX_MICROS: i64 = 253_402_300_799_999_999;

const MICROS_PER_SECOND: i64 = 1_000_000;

const UNIX_EPOCH: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

/// The date `days` after 1970-01-01; `OutOfRange` outside jiff's years -9999 to 9999, which
/// hold BigQuery's.
fn date_from_days(days: i32) -> Result<jiff::civil::Date, CodecError> {
    UNIX_EPOCH
        .checked_add(SignedDuration::from_hours(i64::from(days) * 24))
        .map_err(|err| CodecError::out_of_range(format!("DATE of {days} days: {err}")))
}

/// The days since 1970-01-01 of a jiff date, without BigQuery's range check: the integer form
/// a temporal wrapper writes in a non-human-readable format.
pub(crate) fn raw_date_days(date: jiff::civil::Date) -> i32 {
    // jiff's dates span under 7.4 million days on either side of the epoch.
    (date.duration_since(UNIX_EPOCH).as_hours() / 24) as i32
}

/// Writes a time with no fraction.
const WHOLE_SECONDS: DateTimePrinter = DateTimePrinter::new().precision(Some(0));
/// Writes a time with BigQuery's six fractional digits.
const MICROSECONDS: DateTimePrinter = DateTimePrinter::new().precision(Some(6));

/// The time `micros` microseconds after midnight, wrapped into one day.
fn time_of_day(micros: i64) -> jiff::civil::Time {
    jiff::civil::Time::midnight().wrapping_add(SignedDuration::from_micros(micros))
}

/// A jiff printer's result; printing into a `String` fails only if jiff itself does.
fn printed(result: Result<(), jiff::Error>) -> Result<(), CodecError> {
    result.map_err(|err| CodecError::new(BigQueryCodecErrorKind::Custom, err.to_string()))
}

/// `YYYY-MM-DD`, with a `-` before a year below zero; `OutOfRange` beyond jiff's years.
pub(crate) fn fmt_date(days: i32, out: &mut String) -> Result<(), CodecError> {
    let date = date_from_days(days)?;
    if date.year() < 0 {
        // jiff writes a year below zero in its six-digit form, `-000001`.
        let (year, month, day) = (-date.year(), date.month(), date.day());
        out.push_str(&format!("-{year:04}-{month:02}-{day:02}"));
        return Ok(());
    }
    printed(WHOLE_SECONDS.print_date(&date, out))
}

/// `HH:MM:SS[.ffffff]` for microseconds since midnight; the fraction only when non-zero.
pub(crate) fn fmt_time(micros: i64, out: &mut String) -> Result<(), CodecError> {
    let time = time_of_day(micros);
    let printer = if time.subsec_nanosecond() == 0 {
        &WHOLE_SECONDS
    } else {
        &MICROSECONDS
    };
    printed(printer.print_time(&time, out))
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]` for civil microseconds since 1970-01-01T00:00:00.
pub(crate) fn fmt_datetime(micros: i64, out: &mut String) -> Result<(), CodecError> {
    // Any i64 of microseconds is within ±107 million days, well inside i32.
    fmt_date(micros.div_euclid(MICROS_PER_DAY) as i32, out)?;
    out.push('T');
    fmt_time(micros, out)
}

/// RFC 3339 in UTC, always with `Z`, for microseconds since the epoch.
pub(crate) fn fmt_timestamp(micros: i64, out: &mut String) -> Result<(), CodecError> {
    fmt_datetime(micros, out)?;
    out.push('Z');
    Ok(())
}

/// `s` as jiff parses it, `InvalidText` when it does not, or when it writes a leap second:
/// jiff reads `23:59:60` as `23:59:59`, a value one second off what the caller wrote.
fn parsed<'s, T>(
    s: &'s str,
    name: &str,
    parse: impl FnOnce(&'s str) -> Result<T, jiff::Error>,
) -> Result<T, CodecError> {
    let invalid = |reason: &dyn std::fmt::Display| {
        CodecError::invalid_text(format!("invalid {name} `{s}`: {reason}"))
    };
    if s.contains(":60") {
        return Err(invalid(&"BigQuery has no leap seconds"));
    }
    parse(s).map_err(|err| invalid(&err))
}

/// DATE text as days. A year below 1 is `OutOfRange`, since BigQuery starts at year 1.
pub(crate) fn parse_date(s: &str) -> Result<i32, CodecError> {
    let date: jiff::civil::Date = parsed(s, "DATE", str::parse)?;
    if date.year() < 1 {
        return Err(CodecError::out_of_range(format!(
            "DATE `{s}` is before BigQuery's 0001-01-01"
        )));
    }
    Ok(raw_date_days(date))
}

/// TIME text as microseconds since midnight, sub-microsecond digits dropped.
pub(crate) fn parse_time(s: &str) -> Result<i64, CodecError> {
    parsed(s, "TIME", str::parse).map(time_micros)
}

/// DATETIME text, `T` or a space between date and time and no zone, as civil microseconds.
pub(crate) fn parse_datetime(s: &str) -> Result<i64, CodecError> {
    let micros = raw_datetime_micros(parsed(s, "DATETIME", str::parse)?);
    if micros < TIMESTAMP_MIN_MICROS {
        return Err(CodecError::out_of_range(format!(
            "DATETIME `{s}` is before BigQuery's 0001-01-01T00:00:00"
        )));
    }
    Ok(micros)
}

/// TIMESTAMP text with an offset, `T` or a space between date and time, as UTC microseconds,
/// sub-microsecond digits floored toward the past. It is parsed as its civil parts and offset
/// rather than as a `jiff::Timestamp`, which ends 25 hours before BigQuery's range does.
pub(crate) fn parse_timestamp(s: &str) -> Result<i64, CodecError> {
    let pieces = parsed(s, "TIMESTAMP", jiff::fmt::temporal::Pieces::parse)?;
    let (Some(time), Some(offset)) = (pieces.time(), pieces.to_numeric_offset()) else {
        return Err(CodecError::invalid_text(format!(
            "invalid TIMESTAMP `{s}`: it needs a time and an offset"
        )));
    };
    // A non-negative time of day floors when its nanoseconds are dropped, and the civil part
    // carries the sign, so the instant floors too.
    let civil = raw_datetime_micros(pieces.date().to_datetime(time));
    let utc = civil - i64::from(offset.seconds()) * MICROS_PER_SECOND;
    if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&utc) {
        return Err(CodecError::out_of_range(format!(
            "TIMESTAMP `{s}` is outside BigQuery's 0001-01-01 to 9999-12-31 UTC"
        )));
    }
    Ok(utc)
}

/// TIME as Google's `CivilTimeEncoder.encodePacked64TimeMicros`, the form Storage Write takes.
pub(crate) fn pack_time(micros: i64) -> i64 {
    let secs = micros / MICROS_PER_SECOND;
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    (((h << 12) | (m << 6) | s) << 20) | (micros % MICROS_PER_SECOND)
}

/// DATETIME as Google's `CivilTimeEncoder.encodePacked64DatetimeMicros`. `micros` is inside
/// BigQuery's DATETIME range.
pub(crate) fn pack_datetime(micros: i64) -> i64 {
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let date = UNIX_EPOCH.saturating_add(SignedDuration::from_hours(
        micros.div_euclid(MICROS_PER_DAY) * 24,
    ));
    let (y, mo, d) = (date.year(), date.month(), date.day());
    let secs = tod / MICROS_PER_SECOND;
    let (h, mi, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    let packed = (i64::from(y) << 26)
        | (i64::from(mo) << 22)
        | (i64::from(d) << 17)
        | (h << 12)
        | (mi << 6)
        | s;
    (packed << 20) | (tod % MICROS_PER_SECOND)
}

/// The inverse of [`pack_time`].
#[cfg(test)]
pub(crate) fn unpack_time(packed: i64) -> i64 {
    let micros = packed & 0xF_FFFF;
    let secs = packed >> 20;
    let (h, m, s) = ((secs >> 12) & 0x1F, (secs >> 6) & 0x3F, secs & 0x3F);
    (h * 3600 + m * 60 + s) * MICROS_PER_SECOND + micros
}

/// The inverse of [`pack_datetime`].
#[cfg(test)]
pub(crate) fn unpack_datetime(packed: i64) -> i64 {
    let micros = packed & 0xF_FFFF;
    let fields = packed >> 20;
    let (h, mi, s) = ((fields >> 12) & 0x1F, (fields >> 6) & 0x3F, fields & 0x3F);
    let date = jiff::civil::date(
        (fields >> 26) as i16,
        ((fields >> 22) & 0xF) as i8,
        ((fields >> 17) & 0x1F) as i8,
    );
    i64::from(raw_date_days(date)) * MICROS_PER_DAY
        + (h * 3600 + mi * 60 + s) * MICROS_PER_SECOND
        + micros
}

/// A jiff date for BigQuery days; `OutOfRange` outside BigQuery's DATE range.
pub(crate) fn jiff_date(days: i32) -> Result<jiff::civil::Date, CodecError> {
    if !(DATE_MIN_DAYS..=DATE_MAX_DAYS).contains(&days) {
        return Err(CodecError::out_of_range(format!(
            "DATE of {days} days since 1970-01-01 is outside BigQuery's range"
        )));
    }
    date_from_days(days)
}

/// A jiff time for microseconds since midnight; `OutOfRange` outside one day.
pub(crate) fn jiff_time(micros: i64) -> Result<jiff::civil::Time, CodecError> {
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return Err(CodecError::out_of_range(format!(
            "TIME of {micros} microseconds is outside 0 to 86400000000"
        )));
    }
    let secs = micros / MICROS_PER_SECOND;
    // Each part is bounded by the range check above.
    jiff::civil::Time::new(
        (secs / 3600) as i8,
        (secs / 60 % 60) as i8,
        (secs % 60) as i8,
        ((micros % MICROS_PER_SECOND) * 1000) as i32,
    )
    .map_err(|err| CodecError::out_of_range(format!("TIME of {micros} microseconds: {err}")))
}

/// A jiff datetime for civil microseconds; `OutOfRange` outside BigQuery's DATETIME range.
pub(crate) fn jiff_datetime(micros: i64) -> Result<jiff::civil::DateTime, CodecError> {
    if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&micros) {
        return Err(CodecError::out_of_range(format!(
            "DATETIME of {micros} civil microseconds is outside BigQuery's range"
        )));
    }
    let date = jiff_date(micros.div_euclid(MICROS_PER_DAY) as i32)?;
    let time = jiff_time(micros.rem_euclid(MICROS_PER_DAY))?;
    Ok(jiff::civil::DateTime::from_parts(date, time))
}

/// A jiff timestamp for microseconds since the epoch; `OutOfRange` outside BigQuery's range,
/// or above jiff's own maximum, which ends 25 hours before BigQuery's.
pub(crate) fn jiff_timestamp(micros: i64) -> Result<jiff::Timestamp, CodecError> {
    if !(TIMESTAMP_MIN_MICROS..=TIMESTAMP_MAX_MICROS).contains(&micros) {
        return Err(CodecError::out_of_range(format!(
            "TIMESTAMP of {micros} microseconds is outside BigQuery's range"
        )));
    }
    jiff::Timestamp::from_microsecond(micros).map_err(|err| {
        CodecError::out_of_range(format!(
            "TIMESTAMP of {micros} microseconds does not fit jiff::Timestamp: {err}; \
             read it into a String or an i64"
        ))
    })
}

/// The civil microseconds of a jiff datetime, without BigQuery's range check.
pub(crate) fn raw_datetime_micros(dt: jiff::civil::DateTime) -> i64 {
    i64::from(raw_date_days(dt.date())) * MICROS_PER_DAY + time_micros(dt.time())
}

/// The microseconds of a jiff timestamp, floored, without BigQuery's range check.
pub(crate) fn raw_timestamp_micros(ts: jiff::Timestamp) -> i64 {
    ts.as_second() * MICROS_PER_SECOND + i64::from(ts.subsec_nanosecond()).div_euclid(1000)
}

/// BigQuery days for a jiff date; `OutOfRange` for a year below 1.
#[cfg(test)]
pub(crate) fn date_days(d: jiff::civil::Date) -> Result<i32, CodecError> {
    let days = raw_date_days(d);
    if days < DATE_MIN_DAYS {
        return Err(CodecError::out_of_range(format!(
            "DATE {d} is before BigQuery's 0001-01-01"
        )));
    }
    Ok(days)
}

/// Microseconds since midnight for a jiff time, sub-microsecond digits dropped.
pub(crate) fn time_micros(t: jiff::civil::Time) -> i64 {
    i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * MICROS_PER_SECOND
        + i64::from(t.subsec_nanosecond() / 1000)
}

/// Civil microseconds for a jiff datetime; `OutOfRange` for a year below 1.
#[cfg(test)]
pub(crate) fn datetime_micros(dt: jiff::civil::DateTime) -> Result<i64, CodecError> {
    let micros = raw_datetime_micros(dt);
    if micros < TIMESTAMP_MIN_MICROS {
        return Err(CodecError::out_of_range(format!(
            "DATETIME {dt} is before BigQuery's 0001-01-01T00:00:00"
        )));
    }
    Ok(micros)
}

/// Microseconds since the epoch for a jiff timestamp, floored to the microsecond;
/// `OutOfRange` before BigQuery's 0001-01-01T00:00:00Z.
#[cfg(test)]
pub(crate) fn timestamp_micros(ts: jiff::Timestamp) -> Result<i64, CodecError> {
    let micros = raw_timestamp_micros(ts);
    if micros < TIMESTAMP_MIN_MICROS {
        return Err(CodecError::out_of_range(format!(
            "TIMESTAMP {ts} is before BigQuery's 0001-01-01T00:00:00Z"
        )));
    }
    Ok(micros)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn written<R, F: Fn(&mut String) -> R>(f: F) -> String {
        let mut out = String::new();
        f(&mut out);
        out
    }

    fn kind<T: std::fmt::Debug>(result: Result<T, CodecError>) -> BigQueryCodecErrorKind {
        match result.map_err(CodecError::into_serialize) {
            Err(crate::errors::BigQueryError::SerializeError(err)) => err.kind,
            other => panic!("expected a codec error, got {other:?}"),
        }
    }

    #[test]
    fn civil_days_round_trip_over_bigquery_range() {
        use jiff::civil::date;
        assert_eq!(raw_date_days(date(1970, 1, 1)), 0);
        assert_eq!(raw_date_days(date(1969, 12, 31)), -1);
        assert_eq!(raw_date_days(date(2024, 2, 29)), 19782);
        assert_eq!(raw_date_days(date(1, 1, 1)), DATE_MIN_DAYS);
        assert_eq!(raw_date_days(date(9999, 12, 31)), DATE_MAX_DAYS);
        for d in (DATE_MIN_DAYS..=DATE_MAX_DAYS).step_by(997) {
            let civil = date_from_days(d).expect("inside jiff's range");
            assert_eq!(raw_date_days(civil), d, "day {d}");
        }
        assert_eq!(
            TIMESTAMP_MIN_MICROS,
            i64::from(DATE_MIN_DAYS) * MICROS_PER_DAY
        );
        assert_eq!(
            TIMESTAMP_MAX_MICROS,
            (i64::from(DATE_MAX_DAYS) + 1) * MICROS_PER_DAY - 1
        );
    }

    #[test]
    fn temporal_strings_format_and_parse() {
        assert_eq!(written(|o| fmt_date(19782, o)), "2024-02-29");
        assert_eq!(written(|o| fmt_date(DATE_MIN_DAYS, o)), "0001-01-01");
        assert_eq!(written(|o| fmt_time(0, o)), "00:00:00");
        assert_eq!(written(|o| fmt_time(86_399_999_999, o)), "23:59:59.999999");
        assert_eq!(written(|o| fmt_time(45_296_000_100, o)), "12:34:56.000100");
        let dt = 19782 * MICROS_PER_DAY + 45_296_789_012;
        assert_eq!(
            written(|o| fmt_datetime(dt, o)),
            "2024-02-29T12:34:56.789012"
        );
        assert_eq!(
            written(|o| fmt_timestamp(dt, o)),
            "2024-02-29T12:34:56.789012Z"
        );
        assert_eq!(
            written(|o| fmt_timestamp(-1, o)),
            "1969-12-31T23:59:59.999999Z"
        );
        assert_eq!(
            written(|o| fmt_timestamp(TIMESTAMP_MAX_MICROS, o)),
            "9999-12-31T23:59:59.999999Z"
        );

        assert_eq!(parse_date("2024-02-29").ok(), Some(19782));
        assert_eq!(
            kind(parse_date("2023-02-29")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(
            kind(parse_date("0000-01-01")),
            BigQueryCodecErrorKind::OutOfRange
        );
        assert_eq!(parse_time("12:34:56.000100").ok(), Some(45_296_000_100));
        assert_eq!(parse_time("12:34:56").ok(), Some(45_296_000_000));
        assert_eq!(parse_time("12:34:56.5").ok(), Some(45_296_500_000));
        assert_eq!(
            kind(parse_time("24:00:00")),
            BigQueryCodecErrorKind::InvalidText
        );
        assert_eq!(
            kind(parse_time("23:59:60")),
            BigQueryCodecErrorKind::InvalidText,
            "a leap second is not read as 23:59:59"
        );
        assert_eq!(parse_datetime("2024-02-29T12:34:56.789012").ok(), Some(dt));
        assert_eq!(parse_datetime("2024-02-29 12:34:56.789012").ok(), Some(dt));
        assert_eq!(
            parse_timestamp("2024-02-29T12:34:56.789012Z").ok(),
            Some(dt)
        );
        assert_eq!(
            parse_timestamp("2024-02-29 12:34:56.789012Z").ok(),
            Some(dt)
        );
        assert_eq!(
            parse_timestamp("2024-02-29T14:34:56.789012+02:00").ok(),
            Some(dt)
        );
        assert_eq!(
            parse_timestamp("9999-12-31T23:59:59.999999Z").ok(),
            Some(TIMESTAMP_MAX_MICROS)
        );
        assert_eq!(
            parse_timestamp("1969-12-31T23:59:59.999999Z").ok(),
            Some(-1)
        );
        // jiff prints nanoseconds; BigQuery keeps microseconds, floored toward the past.
        assert_eq!(
            parse_timestamp("1970-01-01T00:00:00.000001999Z").ok(),
            Some(1)
        );
        assert_eq!(
            parse_timestamp("1969-12-31T23:59:59.9999999Z").ok(),
            Some(-1)
        );
        assert_eq!(
            parse_timestamp("2024-02-29T12:34:56+00").ok(),
            Some(19782 * MICROS_PER_DAY + 45_296_000_000)
        );
        for bad in [
            "2024-02-29T12:34:56",
            "2024-02-29 12:34:56 UTC",
            "2016-12-31T23:59:60Z",
        ] {
            assert_eq!(
                kind(parse_timestamp(bad)),
                BigQueryCodecErrorKind::InvalidText,
                "{bad}"
            );
        }
        assert_eq!(
            kind(parse_timestamp("0001-01-01T00:00:00+01:00")),
            BigQueryCodecErrorKind::OutOfRange
        );
    }

    #[test]
    fn jiff_display_forms_parse_to_the_same_value() {
        let d: jiff::civil::Date = "2024-02-29".parse().expect("valid test input");
        assert_eq!(parse_date(&d.to_string()).ok(), Some(19782));
        let t: jiff::Timestamp = "2024-02-29T12:34:56.789012Z"
            .parse()
            .expect("valid test input");
        assert_eq!(
            parse_timestamp(&t.to_string()).ok(),
            Some(19782 * MICROS_PER_DAY + 45_296_789_012)
        );
        let tm: jiff::civil::Time = "23:59:59.999999".parse().expect("valid test input");
        assert_eq!(parse_time(&tm.to_string()).ok(), Some(86_399_999_999));
        let dt: jiff::civil::DateTime = "2024-02-29T12:34:56.789012"
            .parse()
            .expect("valid test input");
        assert_eq!(
            parse_datetime(&dt.to_string()).ok(),
            Some(19782 * MICROS_PER_DAY + 45_296_789_012)
        );

        assert_eq!(date_days(d).ok(), Some(19782));
        assert_eq!(jiff_date(19782).ok(), Some(d));
        assert_eq!(time_micros(tm), 86_399_999_999);
        assert_eq!(jiff_time(86_399_999_999).ok(), Some(tm));
        assert_eq!(
            datetime_micros(dt).ok(),
            Some(19782 * MICROS_PER_DAY + 45_296_789_012)
        );
        assert_eq!(
            jiff_datetime(19782 * MICROS_PER_DAY + 45_296_789_012).ok(),
            Some(dt)
        );
        assert_eq!(
            timestamp_micros(t).ok(),
            Some(19782 * MICROS_PER_DAY + 45_296_789_012)
        );
        assert_eq!(
            jiff_timestamp(19782 * MICROS_PER_DAY + 45_296_789_012).ok(),
            Some(t)
        );

        let before_epoch: jiff::Timestamp = "1969-12-31T23:59:59.9999999Z"
            .parse()
            .expect("valid test input");
        assert_eq!(timestamp_micros(before_epoch).ok(), Some(-1), "floored");
        let year_zero = jiff::civil::date(0, 12, 31);
        assert_eq!(
            kind(date_days(year_zero)),
            BigQueryCodecErrorKind::OutOfRange
        );
        assert_eq!(
            kind(jiff_timestamp(TIMESTAMP_MAX_MICROS)),
            BigQueryCodecErrorKind::OutOfRange,
            "above jiff's maximum"
        );
        assert_eq!(
            kind(jiff_time(MICROS_PER_DAY)),
            BigQueryCodecErrorKind::OutOfRange
        );
    }

    #[test]
    fn civil_time_packing_matches_google_encoder() {
        // CivilTimeEncoder.encodePacked64TimeMicros(12:34:56.123456)
        assert_eq!(
            pack_time(45_296_123_456),
            (((12 << 12) | (34 << 6) | 56) << 20) | 123456
        );
        let dt = i64::from(raw_date_days(jiff::civil::date(2026, 10, 4))) * MICROS_PER_DAY
            + 45_296_123_456;
        let secs = (2026i64 << 26) | (10 << 22) | (4 << 17) | (12 << 12) | (34 << 6) | 56;
        assert_eq!(pack_datetime(dt), (secs << 20) | 123456);
        assert_eq!(unpack_time(pack_time(45_296_123_456)), 45_296_123_456);
        assert_eq!(unpack_datetime(pack_datetime(dt)), dt);
        let min = i64::from(DATE_MIN_DAYS) * MICROS_PER_DAY;
        assert_eq!(unpack_datetime(pack_datetime(min)), min);
    }
}
