//! BigQuery's civil and absolute time ranges, their text forms and their integer encodings.
//!
//! All integers here are BigQuery's own units: DATE in days since 1970-01-01, TIME in
//! microseconds since midnight, DATETIME in civil microseconds since 1970-01-01T00:00:00 with no
//! zone, and TIMESTAMP in microseconds since the Unix epoch. The text parsers cover BigQuery's
//! whole range, which is wider than `jiff::Timestamp`'s at the top.

use crate::errors::BigQueryCodecErrorKind;
use crate::types::error::CodecError;

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

/// Days since 1970-01-01 for a proleptic Gregorian date. Total over `i32` years that fit the
/// result, so it is also the integer form of jiff dates outside BigQuery's range.
pub(crate) fn days_from_civil(y: i32, m: u8, d: u8) -> i32 {
    let (m, d) = (i32::from(m), i32::from(d));
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`]: `(year, month, day)`.
pub(crate) fn civil_from_days(days: i32) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i32::from(m <= 2);
    // `m` is 1..=12 and `d` 1..=31 by construction.
    (y, m as u8, d as u8)
}

fn push_digits(out: &mut String, v: u64, width: usize) {
    let mut buf = [b'0'; 20];
    let mut v = v;
    let mut i = buf.len();
    while v > 0 || buf.len() - i < width {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    for &c in &buf[i..] {
        out.push(char::from(c));
    }
}

/// `YYYY-MM-DD`, with a `-` before a year below zero.
pub(crate) fn fmt_date(days: i32, out: &mut String) {
    let (y, m, d) = civil_from_days(days);
    if y < 0 {
        out.push('-');
    }
    push_digits(out, u64::from(y.unsigned_abs()), 4);
    out.push('-');
    push_digits(out, u64::from(m), 2);
    out.push('-');
    push_digits(out, u64::from(d), 2);
}

/// `HH:MM:SS[.ffffff]` for microseconds since midnight; the fraction only when non-zero.
pub(crate) fn fmt_time(micros: i64, out: &mut String) {
    let micros = micros.rem_euclid(MICROS_PER_DAY).unsigned_abs();
    let secs = micros / 1_000_000;
    push_digits(out, secs / 3600, 2);
    out.push(':');
    push_digits(out, secs / 60 % 60, 2);
    out.push(':');
    push_digits(out, secs % 60, 2);
    let frac = micros % 1_000_000;
    if frac != 0 {
        out.push('.');
        push_digits(out, frac, 6);
    }
}

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]` for civil microseconds since 1970-01-01T00:00:00.
pub(crate) fn fmt_datetime(micros: i64, out: &mut String) {
    // Any i64 of microseconds is within ±107 million days, well inside i32.
    fmt_date(micros.div_euclid(MICROS_PER_DAY) as i32, out);
    out.push('T');
    fmt_time(micros.rem_euclid(MICROS_PER_DAY), out);
}

/// RFC 3339 in UTC, always with `Z`, for microseconds since the epoch.
pub(crate) fn fmt_timestamp(micros: i64, out: &mut String) {
    fmt_datetime(micros, out);
    out.push('Z');
}

fn digits(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 9 || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(b.iter().fold(0u32, |a, &c| a * 10 + u32::from(c - b'0')))
}

fn days_in_month(y: i32, m: u8) -> u8 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `YYYY-MM-DD` with a four-digit year, as days. Year 0 is `OutOfRange`, since BigQuery starts
/// at year 1.
pub(crate) fn parse_date(s: &str) -> Result<i32, CodecError> {
    let b = s.as_bytes();
    let bad = || CodecError::invalid_text(format!("invalid DATE `{s}`, expected YYYY-MM-DD"));
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(bad());
    }
    let (Some(y), Some(m), Some(d)) = (digits(&b[..4]), digits(&b[5..7]), digits(&b[8..])) else {
        return Err(bad());
    };
    // Four digits, two digits and two digits always fit.
    let (y, m, d) = (y as i32, m as u8, d as u8);
    if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        return Err(bad());
    }
    if y == 0 {
        return Err(CodecError::out_of_range(format!(
            "DATE `{s}` is before BigQuery's 0001-01-01"
        )));
    }
    Ok(days_from_civil(y, m, d))
}

/// `HH:MM:SS[.fraction]` with up to 9 fractional digits, as microseconds since midnight with
/// sub-microsecond digits dropped.
pub(crate) fn parse_time(s: &str) -> Result<i64, CodecError> {
    let b = s.as_bytes();
    let bad =
        || CodecError::invalid_text(format!("invalid TIME `{s}`, expected HH:MM:SS[.ffffff]"));
    if b.len() < 8 || b[2] != b':' || b[5] != b':' {
        return Err(bad());
    }
    let (Some(h), Some(m), Some(sec)) = (digits(&b[..2]), digits(&b[3..5]), digits(&b[6..8]))
    else {
        return Err(bad());
    };
    if h > 23 || m > 59 || sec > 59 {
        return Err(bad());
    }
    let mut micros = 0i64;
    if b.len() > 8 {
        let f = &b[9..];
        if b[8] != b'.' || f.is_empty() || f.len() > 9 || !f.iter().all(u8::is_ascii_digit) {
            return Err(bad());
        }
        for i in 0..6 {
            micros = micros * 10 + f.get(i).map_or(0, |c| i64::from(c - b'0'));
        }
    }
    Ok(i64::from(h * 3600 + m * 60 + sec) * MICROS_PER_SECOND + micros)
}

/// `YYYY-MM-DD[T ]HH:MM:SS[.fraction]` with no zone, as civil microseconds.
pub(crate) fn parse_datetime(s: &str) -> Result<i64, CodecError> {
    let b = s.as_bytes();
    if b.len() < 19 || !matches!(b[10], b'T' | b't' | b' ') || !s.is_char_boundary(10) {
        return Err(CodecError::invalid_text(format!(
            "invalid DATETIME `{s}`, expected YYYY-MM-DDTHH:MM:SS[.ffffff]"
        )));
    }
    let days = parse_date(&s[..10])?;
    let t = parse_time(&s[11..])?;
    Ok(i64::from(days) * MICROS_PER_DAY + t)
}

/// RFC 3339 with `Z` or a numeric `±HH:MM` offset, and `T` or a space between date and time, as
/// UTC microseconds, sub-microsecond digits floored toward the past.
pub(crate) fn parse_timestamp(s: &str) -> Result<i64, CodecError> {
    let bad = || {
        CodecError::invalid_text(format!(
            "invalid TIMESTAMP `{s}`, expected YYYY-MM-DDTHH:MM:SS[.f](Z|±HH:MM)"
        ))
    };
    let b = s.as_bytes();
    let (civil, offset_secs) = if let Some(c) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z'))
    {
        (c, 0i64)
    } else if b.len() >= 25 && matches!(b[b.len() - 6], b'+' | b'-') && b[b.len() - 3] == b':' {
        let o = &b[b.len() - 5..];
        let (Some(h), Some(m)) = (digits(&o[..2]), digits(&o[3..])) else {
            return Err(bad());
        };
        if h > 23 || m > 59 {
            return Err(bad());
        }
        let sign = if b[b.len() - 6] == b'-' { -1 } else { 1 };
        (&s[..s.len() - 6], sign * i64::from(h * 3600 + m * 60))
    } else {
        return Err(bad());
    };
    let micros = parse_datetime(civil).map_err(|err| match err.kind() {
        BigQueryCodecErrorKind::OutOfRange => err,
        _ => bad(),
    })?;
    // The sub-microsecond digits were dropped by `parse_time`, which floors a non-negative
    // time of day; a negative instant needs nothing more, since the civil part carries the sign.
    let utc = micros - offset_secs * MICROS_PER_SECOND;
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

/// DATETIME as Google's `CivilTimeEncoder.encodePacked64DatetimeMicros`.
pub(crate) fn pack_datetime(micros: i64) -> i64 {
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let (y, mo, d) = civil_from_days(micros.div_euclid(MICROS_PER_DAY) as i32);
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
    let (y, mo, d) = (
        (fields >> 26) as i32,
        ((fields >> 22) & 0xF) as u8,
        ((fields >> 17) & 0x1F) as u8,
    );
    i64::from(days_from_civil(y, mo, d)) * MICROS_PER_DAY
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
    let (y, m, d) = civil_from_days(days);
    // Years 1 to 9999 fit in i16 and a date from `civil_from_days` is always valid.
    jiff::civil::Date::new(y as i16, m as i8, d as i8)
        .map_err(|err| CodecError::out_of_range(format!("DATE of {days} days: {err}")))
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

/// The days of a jiff date, without BigQuery's range check: the integer form a temporal
/// wrapper writes in a non-human-readable format.
pub(crate) fn raw_date_days(d: jiff::civil::Date) -> i32 {
    days_from_civil(i32::from(d.year()), d.month() as u8, d.day() as u8)
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
mod tests;
