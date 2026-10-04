use super::*;

fn s<F: Fn(&mut String)>(f: F) -> String {
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
    assert_eq!(days_from_civil(1970, 1, 1), 0);
    assert_eq!(days_from_civil(1969, 12, 31), -1);
    assert_eq!(days_from_civil(2024, 2, 29), 19782);
    assert_eq!(days_from_civil(1, 1, 1), DATE_MIN_DAYS);
    assert_eq!(days_from_civil(9999, 12, 31), DATE_MAX_DAYS);
    for d in (DATE_MIN_DAYS..=DATE_MAX_DAYS).step_by(997) {
        let (y, m, dd) = civil_from_days(d);
        assert_eq!(days_from_civil(y, m, dd), d, "day {d}");
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
    assert_eq!(s(|o| fmt_date(19782, o)), "2024-02-29");
    assert_eq!(s(|o| fmt_date(DATE_MIN_DAYS, o)), "0001-01-01");
    assert_eq!(s(|o| fmt_time(0, o)), "00:00:00");
    assert_eq!(s(|o| fmt_time(86_399_999_999, o)), "23:59:59.999999");
    assert_eq!(s(|o| fmt_time(45_296_000_100, o)), "12:34:56.000100");
    let dt = 19782 * MICROS_PER_DAY + 45_296_789_012;
    assert_eq!(s(|o| fmt_datetime(dt, o)), "2024-02-29T12:34:56.789012");
    assert_eq!(s(|o| fmt_timestamp(dt, o)), "2024-02-29T12:34:56.789012Z");
    assert_eq!(s(|o| fmt_timestamp(-1, o)), "1969-12-31T23:59:59.999999Z");
    assert_eq!(
        s(|o| fmt_timestamp(TIMESTAMP_MAX_MICROS, o)),
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
    for bad in [
        "2024-02-29T12:34:56",
        "2024-02-29T12:34:56+00",
        "2024-02-29 12:34:56 UTC",
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
    let dt = i64::from(days_from_civil(2026, 10, 4)) * MICROS_PER_DAY + 45_296_123_456;
    let secs = (2026i64 << 26) | (10 << 22) | (4 << 17) | (12 << 12) | (34 << 6) | 56;
    assert_eq!(pack_datetime(dt), (secs << 20) | 123456);
    assert_eq!(unpack_time(pack_time(45_296_123_456)), 45_296_123_456);
    assert_eq!(unpack_datetime(pack_datetime(dt)), dt);
    let min = i64::from(DATE_MIN_DAYS) * MICROS_PER_DAY;
    assert_eq!(unpack_datetime(pack_datetime(min)), min);
}
