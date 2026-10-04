use super::*;

fn s(iv: &BigQueryInterval) -> String {
    let mut out = String::new();
    iv.write_bq(&mut out);
    out
}

fn kind<T: std::fmt::Debug>(result: Result<T, CodecError>) -> BigQueryCodecErrorKind {
    match result.map_err(CodecError::into_serialize) {
        Err(BigQueryError::SerializeError(err)) => err.kind,
        other => panic!("expected a codec error, got {other:?}"),
    }
}

#[test]
fn interval_canonical_string_round_trips() {
    let iv = BigQueryInterval {
        months: 14,
        days: 3,
        nanos: (4 * 3600 + 5 * 60 + 6) * 1_000_000_000 + 789_000,
    };
    assert_eq!(s(&iv), "1-2 3 4:5:6.000789");
    assert_eq!(
        BigQueryInterval::parse_bq("1-2 3 4:5:6.000789").ok(),
        Some(iv)
    );
    let mixed = BigQueryInterval {
        months: -14,
        days: 3,
        nanos: -6_000_000_000,
    };
    assert_eq!(s(&mixed), "-1-2 3 -0:0:6");
    assert_eq!(
        BigQueryInterval::parse_bq("-1-2 3 -0:0:6").ok(),
        Some(mixed)
    );
    assert_eq!(s(&BigQueryInterval::default()), "0-0 0 0:0:0");
    assert_eq!(
        BigQueryInterval::parse_bq("0-0 0 0:0:0").ok(),
        Some(BigQueryInterval::default())
    );
    assert_eq!(
        kind(BigQueryInterval::parse_bq("1-2 3")),
        BigQueryCodecErrorKind::InvalidText
    );
    assert_eq!(
        kind(BigQueryInterval::parse_bq("1-2 3 4:5:6.0000001")),
        BigQueryCodecErrorKind::InvalidText
    );
}

#[test]
fn interval_time_part_beyond_i64_nanos_is_an_error() {
    assert_eq!(
        kind(BigQueryInterval::parse_bq("0-0 0 87840000:0:0")),
        BigQueryCodecErrorKind::OutOfRange
    );
    let max = BigQueryInterval {
        months: 0,
        days: 0,
        nanos: 9_223_372_036_854_775_000,
    };
    assert_eq!(BigQueryInterval::parse_bq(&s(&max)).ok(), Some(max));
}

#[test]
fn interval_converts_to_span_only_with_one_sign() {
    let iv = BigQueryInterval {
        months: 14,
        days: 3,
        nanos: 3_600_000_001_000,
    };
    let span = jiff::Span::try_from(iv).expect("valid test input");
    assert_eq!(span.get_months(), 14);
    assert_eq!(span.get_days(), 3);
    assert_eq!(BigQueryInterval::try_from(span).ok(), Some(iv));

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
    let iv = BigQueryInterval {
        months: 1,
        days: -2,
        nanos: 3,
    };
    let json = serde_json::to_string(&iv).expect("valid test input");
    assert_eq!(json, r#"{"months":1,"days":-2,"nanos":3}"#);
    assert_eq!(
        serde_json::from_str::<BigQueryInterval>(&json).expect("valid test input"),
        iv
    );
}
