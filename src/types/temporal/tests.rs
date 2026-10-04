use super::*;
use crate::types::civil::MICROS_PER_DAY;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Plain {
    ts: jiff::Timestamp,
    d: jiff::civil::Date,
    t: jiff::civil::Time,
    dt: jiff::civil::DateTime,
    ots: Option<jiff::Timestamp>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Wrapped {
    ts: BigQueryTimestamp,
    d: BigQueryDate,
    t: BigQueryTime,
    dt: BigQueryDateTime,
    ots: Option<BigQueryTimestamp>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct WithModules {
    #[serde(with = "serialize_as_timestamp")]
    ts: jiff::Timestamp,
    #[serde(with = "serialize_as_date")]
    d: jiff::civil::Date,
    #[serde(with = "serialize_as_time")]
    t: jiff::civil::Time,
    #[serde(with = "serialize_as_datetime")]
    dt: jiff::civil::DateTime,
    #[serde(with = "serialize_as_optional_timestamp")]
    ots: Option<jiff::Timestamp>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct OptionalModules {
    #[serde(with = "serialize_as_optional_date")]
    d: Option<jiff::civil::Date>,
    #[serde(with = "serialize_as_optional_time")]
    t: Option<jiff::civil::Time>,
    #[serde(with = "serialize_as_optional_datetime")]
    dt: Option<jiff::civil::DateTime>,
}

fn sample() -> (
    jiff::Timestamp,
    jiff::civil::Date,
    jiff::civil::Time,
    jiff::civil::DateTime,
) {
    (
        "2024-02-29T12:34:56.789012Z"
            .parse()
            .expect("valid test input"),
        jiff::civil::date(2024, 2, 29),
        "23:59:59.999999".parse().expect("valid test input"),
        "2024-02-29T12:34:56.789012"
            .parse()
            .expect("valid test input"),
    )
}

fn plain() -> Plain {
    let (ts, d, t, dt) = sample();
    Plain {
        ts,
        d,
        t,
        dt,
        ots: Some(ts),
    }
}

fn wrapped() -> Wrapped {
    let (ts, d, t, dt) = sample();
    Wrapped {
        ts: BigQueryTimestamp(ts),
        d: BigQueryDate(d),
        t: BigQueryTime(t),
        dt: BigQueryDateTime(dt),
        ots: Some(BigQueryTimestamp(ts)),
    }
}

#[test]
fn temporal_wrappers_serialize_to_json_like_plain_jiff() {
    let plain = serde_json::to_string(&plain()).expect("valid test input");
    assert_eq!(
        serde_json::to_string(&wrapped()).expect("valid test input"),
        plain
    );
    assert_eq!(
        serde_json::to_string(&Wrapped {
            ots: None,
            ..wrapped()
        })
        .expect("valid test input"),
        serde_json::to_string(&Plain {
            ots: None,
            ..self::plain()
        })
        .expect("valid test input")
    );
}

#[test]
fn temporal_wrappers_round_trip_through_json() {
    let json = serde_json::to_string(&wrapped()).expect("valid test input");
    assert_eq!(
        serde_json::from_str::<Wrapped>(&json).expect("valid test input"),
        wrapped()
    );
    let from_plain = serde_json::to_string(&plain()).expect("valid test input");
    assert_eq!(
        serde_json::from_str::<Wrapped>(&from_plain).expect("valid test input"),
        wrapped()
    );
    assert!(serde_json::from_str::<BigQueryDate>("\"2024-13-01\"").is_err());
    assert!(
        serde_json::from_str::<BigQueryDate>("19782").is_err(),
        "JSON carries the text form"
    );
}

#[test]
fn with_modules_match_the_wrappers() {
    let (ts, d, t, dt) = sample();
    let modules = WithModules {
        ts,
        d,
        t,
        dt,
        ots: Some(ts),
    };
    let json = serde_json::to_string(&modules).expect("valid test input");
    assert_eq!(
        json,
        serde_json::to_string(&wrapped()).expect("valid test input")
    );
    assert_eq!(
        serde_json::from_str::<WithModules>(&json).expect("valid test input"),
        modules
    );

    let none = WithModules {
        ots: None,
        ..modules
    };
    let json = serde_json::to_string(&none).expect("valid test input");
    assert_eq!(
        json,
        serde_json::to_string(&Wrapped {
            ots: None,
            ..wrapped()
        })
        .expect("valid test input")
    );
    assert_eq!(
        serde_json::from_str::<WithModules>(&json).expect("valid test input"),
        none
    );

    let optional = OptionalModules {
        d: Some(d),
        t: None,
        dt: Some(dt),
    };
    let json = serde_json::to_string(&optional).expect("valid test input");
    assert_eq!(json, format!(r#"{{"d":"{d}","t":null,"dt":"{dt}"}}"#));
    assert_eq!(
        serde_json::from_str::<OptionalModules>(&json).expect("valid test input"),
        optional
    );

    let (ts_int, d_int, t_int, dt_int) = (
        capture_int(&ModuleField(&ts)).expect("valid test input"),
        capture_int(&BigQueryDate(d)).expect("valid test input"),
        capture_int(&BigQueryTime(t)).expect("valid test input"),
        capture_int(&BigQueryDateTime(dt)).expect("valid test input"),
    );
    assert_eq!(ts_int, 19782 * MICROS_PER_DAY + 45_296_789_012);
    assert_eq!(d_int, 19782);
    assert_eq!(t_int, 86_399_999_999);
    assert_eq!(dt_int, 19782 * MICROS_PER_DAY + 45_296_789_012);
}

/// A field serialized through the `with` module alone, the way a derived struct hands it to a
/// serializer.
struct ModuleField<'a>(&'a jiff::Timestamp);

impl Serialize for ModuleField<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serialize_as_timestamp::serialize(self.0, s)
    }
}

#[test]
fn integer_capture_sees_the_wrapper_name_and_not_plain_jiff() {
    let (ts, d, _, _) = sample();
    assert_eq!(temporal_tag_kind(TAG_TIMESTAMP), Some(BqKind::Timestamp));
    assert_eq!(temporal_tag_kind(TAG_DATE), Some(BqKind::Date));
    assert_eq!(temporal_tag_kind(TAG_TIME), Some(BqKind::Time));
    assert_eq!(temporal_tag_kind(TAG_DATETIME), Some(BqKind::DateTime));
    assert_eq!(temporal_tag_kind("BigQueryJson"), None);
    assert_eq!(
        capture_int(&BigQueryTimestamp(ts)).ok(),
        Some(19782 * MICROS_PER_DAY + 45_296_789_012)
    );
    assert_eq!(capture_int(&7i32).ok(), Some(7));
    let err = capture_int(&d).map_err(CodecError::into_serialize);
    assert!(
        matches!(err, Err(crate::errors::BigQueryError::SerializeError(ref e)) if e.kind == BigQueryCodecErrorKind::TypeMismatch),
        "plain jiff speaks text: {err:?}"
    );
    let below_year_one = BigQueryDate(jiff::civil::date(-5, 1, 1));
    assert!(capture_int(&below_year_one)
        .is_ok_and(|days| days < crate::types::civil::DATE_MIN_DAYS.into()));
}
