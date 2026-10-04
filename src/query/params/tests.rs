use super::*;
use crate::errors::BigQueryError;
use crate::{
    BigQueryDate, BigQueryDecimal, BigQueryFieldSchema, BigQueryJson, BigQueryRange,
    BigQueryTimestamp,
};
use serde::Serialize;

fn param_type(name: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: name.into(),
        ..Default::default()
    }
}

fn array_param_type(element: QueryParameterType) -> QueryParameterType {
    QueryParameterType {
        r#type: "ARRAY".into(),
        array_type: Some(Box::new(element)),
        ..Default::default()
    }
}

fn param_value(text: &str) -> QueryParameterValue {
    QueryParameterValue {
        value: Some(text.into()),
        ..Default::default()
    }
}

fn param(name: &str, t: QueryParameterType, v: QueryParameterValue) -> QueryParameter {
    QueryParameter {
        name: name.into(),
        parameter_type: Some(t),
        parameter_value: Some(v),
    }
}

fn infer<V: Serialize + ?Sized>(value: &V) -> BigQueryResult<QueryParameter> {
    infer_param(ParamLabel::Named("p"), value)
}

fn typed<V: Serialize + ?Sized>(
    t: impl Into<BigQueryParamType>,
    value: &V,
) -> BigQueryResult<QueryParameter> {
    typed_param(ParamLabel::Named("p"), &t.into(), value)
}

#[derive(Serialize)]
enum Colour {
    Red,
}

#[test]
fn scalars_infer_their_bigquery_types() -> BigQueryResult<()> {
    assert_eq!(
        infer(&41i32)?,
        param("p", param_type("INT64"), param_value("41"))
    );
    assert_eq!(
        infer(&(u64::MAX >> 1))?,
        param("p", param_type("INT64"), param_value("9223372036854775807"))
    );
    assert_eq!(
        infer(&1.5f64)?,
        param("p", param_type("FLOAT64"), param_value("1.5"))
    );
    assert_eq!(
        infer(&true)?,
        param("p", param_type("BOOL"), param_value("true"))
    );
    assert_eq!(
        infer("Åsa")?,
        param("p", param_type("STRING"), param_value("Åsa"))
    );
    assert_eq!(
        infer(&'x')?,
        param("p", param_type("STRING"), param_value("x"))
    );
    assert_eq!(
        infer(&Colour::Red)?,
        param("p", param_type("STRING"), param_value("Red"))
    );
    assert_eq!(
        infer(&serde_bytes::ByteBuf::from(vec![0u8, 255]))?,
        param("p", param_type("BYTES"), param_value("AP8="))
    );
    assert_eq!(
        infer(&Some(7i64))?,
        param("p", param_type("INT64"), param_value("7"))
    );
    Ok(())
}

#[test]
fn integer_above_int64_is_out_of_range() {
    match infer(&u64::MAX) {
        Err(BigQueryError::SerializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange);
            assert_eq!(err.path, "p");
            assert_eq!(err.row, None);
        }
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

#[test]
fn float_text_round_trips_and_names_special_values() -> BigQueryResult<()> {
    for x in [0.1, -0.0, 5e-324, 1e300, f64::MAX, 123_456.789] {
        let text = infer(&x)?
            .parameter_value
            .and_then(|v| v.value)
            .unwrap_or_default();
        let back: f64 = text.parse().expect("FLOAT64 text parses");
        assert_eq!(back.to_bits(), x.to_bits(), "{x} as {text}");
    }
    assert_eq!(
        infer(&f64::NAN)?,
        param("p", param_type("FLOAT64"), param_value("NaN"))
    );
    assert_eq!(
        infer(&f64::INFINITY)?,
        param("p", param_type("FLOAT64"), param_value("Infinity"))
    );
    assert_eq!(
        infer(&f64::NEG_INFINITY)?,
        param("p", param_type("FLOAT64"), param_value("-Infinity"))
    );
    Ok(())
}

#[test]
fn wrappers_are_recognised_by_their_serde_names() -> BigQueryResult<()> {
    let ts: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse().expect("valid");
    let date = jiff::civil::date(2024, 2, 29);
    assert_eq!(
        infer(&BigQueryTimestamp(ts))?,
        param(
            "p",
            param_type("TIMESTAMP"),
            param_value("2026-10-04 12:34:56.123456+00:00")
        )
    );
    assert_eq!(
        infer(&BigQueryDate(date))?,
        param("p", param_type("DATE"), param_value("2024-02-29"))
    );
    assert_eq!(
        infer(&crate::BigQueryTime(jiff::civil::time(4, 5, 6, 0)))?,
        param("p", param_type("TIME"), param_value("04:05:06"))
    );
    assert_eq!(
        infer(&crate::BigQueryDateTime(date.at(23, 59, 59, 999_999_000)))?,
        param(
            "p",
            param_type("DATETIME"),
            param_value("2024-02-29 23:59:59.999999")
        )
    );
    assert_eq!(
        infer(&BigQueryJson(serde_json::json!({"stad": "Malmö"})))?,
        param("p", param_type("JSON"), param_value(r#"{"stad":"Malmö"}"#))
    );
    assert_eq!(
        infer(&BigQueryDecimal("123.450"))?,
        param("p", param_type("NUMERIC"), param_value("123.45"))
    );
    assert_eq!(
        infer(&BigQueryDecimal("0.00000000000000000000000000000000000001"))?,
        param(
            "p",
            param_type("BIGNUMERIC"),
            param_value("0.00000000000000000000000000000000000001")
        )
    );
    assert_eq!(
        infer(&crate::BigQueryInterval {
            months: 14,
            days: -3,
            nanos: 3_723_500_000_000,
        })?,
        param(
            "p",
            param_type("INTERVAL"),
            param_value("1-2 -3 1:2:3.500000")
        )
    );
    let range = BigQueryRange {
        start: Some(BigQueryDate(date)),
        end: None,
    };
    assert_eq!(
        infer(&range)?,
        param(
            "p",
            QueryParameterType {
                r#type: "RANGE".into(),
                range_element_type: Some(Box::new(param_type("DATE"))),
                ..Default::default()
            },
            QueryParameterValue {
                range_value: Some(Box::new(RangeValue {
                    start: Some(Box::new(param_value("2024-02-29"))),
                    end: None,
                })),
                ..Default::default()
            }
        )
    );
    Ok(())
}

#[test]
fn plain_jiff_value_infers_string() -> BigQueryResult<()> {
    let ts: jiff::Timestamp = "2026-10-04T12:34:56Z".parse().expect("valid");
    assert_eq!(
        infer(&ts)?,
        param(
            "p",
            param_type("STRING"),
            param_value("2026-10-04T12:34:56Z")
        )
    );
    Ok(())
}

#[derive(Serialize)]
struct Pair {
    b: i64,
    a: String,
}

#[test]
fn sequences_and_structs_infer_array_and_struct_in_field_order() -> BigQueryResult<()> {
    assert_eq!(
        infer(&vec![1i64, 2, 3])?,
        param(
            "p",
            array_param_type(param_type("INT64")),
            QueryParameterValue {
                array_values: vec![param_value("1"), param_value("2"), param_value("3")],
                ..Default::default()
            }
        )
    );
    let struct_ty = QueryParameterType {
        r#type: "STRUCT".into(),
        struct_types: vec![
            QueryParameterStructType {
                name: "b".into(),
                r#type: Some(param_type("INT64")),
                ..Default::default()
            },
            QueryParameterStructType {
                name: "a".into(),
                r#type: Some(param_type("STRING")),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let struct_val = QueryParameterValue {
        struct_values: [
            ("b".to_string(), param_value("7")),
            ("a".to_string(), param_value("z")),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    assert_eq!(
        infer(&Pair {
            b: 7,
            a: "z".into()
        })?,
        param("p", struct_ty.clone(), struct_val.clone())
    );
    assert_eq!(
        infer(&OrderedMap(vec![("b", 7.into()), ("a", "z".into())]))?,
        param("p", struct_ty, struct_val)
    );
    Ok(())
}

/// A map that serializes its entries in the given order.
struct OrderedMap(Vec<(&'static str, serde_json::Value)>);

impl Serialize for OrderedMap {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

fn assert_points_at_param_as(result: BigQueryResult<QueryParameter>, what: &str) {
    match result {
        Err(BigQueryError::InvalidParametersError(err)) => {
            assert_eq!(err.public.field, "p", "{what}");
        }
        other => panic!("{what}: expected InvalidParametersError, got {other:?}"),
    }
}

#[test]
fn values_without_a_type_point_at_param_as() {
    assert_points_at_param_as(infer(&None::<i64>), "None");
    assert_points_at_param_as(infer(&Vec::<i64>::new()), "an empty array");
    assert_points_at_param_as(
        infer(&vec![serde_json::json!(1), serde_json::json!("a")]),
        "mixed elements",
    );
    assert_points_at_param_as(
        infer(&serde_json::json!({"a": null})),
        "a NULL struct field",
    );
}

#[test]
fn param_as_takes_the_write_forms_of_the_declared_type() -> BigQueryResult<()> {
    let ts: jiff::Timestamp = "2026-10-04T12:34:56.123456Z".parse().expect("valid");
    let expected = param(
        "p",
        param_type("TIMESTAMP"),
        param_value("2026-10-04 12:34:56.123456+00:00"),
    );
    assert_eq!(typed(BigQueryFieldType::Timestamp, &ts)?, expected);
    assert_eq!(
        typed(BigQueryFieldType::Timestamp, &1_791_117_296_123_456i64)?.parameter_type,
        expected.parameter_type
    );
    assert_eq!(
        typed(
            BigQueryFieldType::Timestamp,
            "2026-10-04T14:34:56.123456+02:00"
        )?,
        expected
    );
    assert_eq!(
        typed(BigQueryFieldType::Date, "2024-02-29")?,
        param("p", param_type("DATE"), param_value("2024-02-29"))
    );
    assert_eq!(
        typed(BigQueryFieldType::Numeric(None), &1.25f64)?,
        param("p", param_type("NUMERIC"), param_value("1.25"))
    );
    assert_eq!(
        typed(BigQueryFieldType::Bytes { max_length: None }, &vec![1u8, 2])?,
        param("p", param_type("BYTES"), param_value("AQI="))
    );
    assert_eq!(
        typed(BigQueryFieldType::Int64, &None::<i64>)?,
        param("p", param_type("INT64"), QueryParameterValue::default())
    );
    assert_eq!(
        typed(
            BigQueryParamType::array_of(BigQueryFieldType::String { max_length: None }),
            &["a", "b"]
        )?,
        param(
            "p",
            array_param_type(param_type("STRING")),
            QueryParameterValue {
                array_values: vec![param_value("a"), param_value("b")],
                ..Default::default()
            }
        )
    );
    Ok(())
}

#[test]
fn param_as_struct_follows_the_declared_fields() -> BigQueryResult<()> {
    let field = |name: &str, field_type| BigQueryFieldSchema {
        name: name.into(),
        field_type,
        mode: crate::BigQueryFieldMode::Nullable,
        description: None,
        default_value_expression: None,
    };
    let declared = BigQueryFieldType::Struct(vec![
        field("a", BigQueryFieldType::String { max_length: None }),
        field("b", BigQueryFieldType::Int64),
    ]);
    let encoded = typed(
        declared.clone(),
        &Pair {
            b: 7,
            a: "z".into(),
        },
    )?;
    let types: Vec<_> = encoded
        .parameter_type
        .map(|t| t.struct_types.into_iter().map(|s| s.name).collect())
        .unwrap_or_default();
    assert_eq!(types, ["a", "b"]);
    match typed(declared, &serde_json::json!({"a": "z", "c": 1})) {
        Err(BigQueryError::SerializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::UnknownField);
            assert_eq!(err.path, "p.c");
        }
        other => panic!("expected UnknownField, got {other:?}"),
    }
    Ok(())
}

#[test]
fn param_as_rejects_forms_outside_the_declared_type() {
    let date = jiff::civil::date(2024, 2, 29);
    for (what, result) in [
        (
            "an integer as FLOAT64",
            typed(BigQueryFieldType::Float64, &1i64),
        ),
        (
            "a DATE wrapper as TIMESTAMP",
            typed(BigQueryFieldType::Timestamp, &BigQueryDate(date)),
        ),
        (
            "a string as BYTES",
            typed(BigQueryFieldType::Bytes { max_length: None }, "AQI="),
        ),
    ] {
        match result {
            Err(BigQueryError::SerializeError(err)) => {
                assert_eq!(err.kind, BigQueryCodecErrorKind::TypeMismatch, "{what}");
                assert_eq!(err.path, "p", "{what}");
            }
            other => panic!("{what}: expected TypeMismatch, got {other:?}"),
        }
    }
    match typed(BigQueryFieldType::Date, "0000-01-01") {
        Err(BigQueryError::SerializeError(err)) => {
            assert_eq!(err.kind, BigQueryCodecErrorKind::OutOfRange)
        }
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

#[derive(Serialize)]
struct Filter {
    min: i64,
    name: &'static str,
}

#[test]
fn struct_params_send_each_top_level_field() -> BigQueryResult<()> {
    let params = struct_params(&Filter { min: 10, name: "x" })?;
    assert_eq!(
        params,
        [
            param("min", param_type("INT64"), param_value("10")),
            param("name", param_type("STRING"), param_value("x"))
        ]
    );
    match struct_params(&5i64) {
        Err(BigQueryError::InvalidParametersError(err)) => assert_eq!(err.public.field, "params"),
        other => panic!("expected InvalidParametersError, got {other:?}"),
    }
    Ok(())
}

#[test]
fn positional_parameter_errors_name_its_position() {
    match infer_param(ParamLabel::Positional(1), &None::<i64>) {
        Err(BigQueryError::InvalidParametersError(err)) => {
            assert_eq!(err.public.field, "positional parameter 2")
        }
        other => panic!("expected InvalidParametersError, got {other:?}"),
    }
}

#[test]
fn named_and_positional_parameters_cannot_mix() -> BigQueryResult<()> {
    let named = param("a", param_type("INT64"), param_value("1"));
    let positional = param("", param_type("INT64"), param_value("1"));
    assert_eq!(parameter_mode(&[])?, "");
    assert_eq!(parameter_mode(std::slice::from_ref(&named))?, "NAMED");
    assert_eq!(
        parameter_mode(std::slice::from_ref(&positional))?,
        "POSITIONAL"
    );
    assert!(matches!(
        parameter_mode(&[named, positional]),
        Err(BigQueryError::InvalidParametersError(_))
    ));
    Ok(())
}

#[test]
fn parameter_names_must_be_googlesql_identifiers() {
    for name in [
        "", "a b", "a;b", "a--", "--", "`a`", "a'b", "a\"b", "1a", "@a", "a-b", "a.b", "ä", "a\nb",
        "a\0",
    ] {
        for result in [
            infer_param(ParamLabel::Named(name), &1i64),
            typed_param(
                ParamLabel::Named(name),
                &BigQueryFieldType::Int64.into(),
                &1i64,
            ),
        ] {
            match result {
                Err(BigQueryError::InvalidParametersError(_)) => {}
                other => panic!("{name:?}: expected InvalidParametersError, got {other:?}"),
            }
        }
    }
    for name in ["a", "_a1", "A_b", "_"] {
        assert!(
            infer_param(ParamLabel::Named(name), &1i64).is_ok(),
            "{name:?}"
        );
    }
    let map: std::collections::BTreeMap<&str, i64> = [("ok", 1), ("bad name", 2)].into();
    assert!(matches!(
        struct_params(&map),
        Err(BigQueryError::InvalidParametersError(_))
    ));
}

/// A value that serializes as one of the shapes a parameter error describes.
enum Malformed {
    JsonWrapper,
    DecimalWrapper,
    IntegerMapKey,
    IntervalPart,
}

impl Serialize for Malformed {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeStruct};
        match self {
            Malformed::JsonWrapper => serializer.serialize_newtype_struct(TAG_JSON, &271828i64),
            Malformed::DecimalWrapper => {
                serializer.serialize_newtype_struct(TAG_DECIMAL, &271828i64)
            }
            Malformed::IntegerMapKey => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(&271828i64, "v")?;
                map.end()
            }
            Malformed::IntervalPart => {
                let mut interval = serializer.serialize_struct(TAG_INTERVAL, 3)?;
                interval.serialize_field("months", "s3cr3t")?;
                interval.serialize_field("days", &0i64)?;
                interval.serialize_field("nanos", &0i64)?;
                interval.end()
            }
        }
    }
}

#[test]
fn a_parameter_error_leaves_the_value_out() {
    for (value, kind, text) in [
        (Malformed::JsonWrapper, "integer", "271828"),
        (Malformed::DecimalWrapper, "integer", "271828"),
        (Malformed::IntegerMapKey, "integer", "271828"),
        (Malformed::IntervalPart, "string", "s3cr3t"),
    ] {
        let message = match infer(&value) {
            Err(err) => err.to_string(),
            Ok(param) => panic!("{kind}: expected an error, got {param:?}"),
        };
        assert!(!message.contains(text), "{kind}: {message}");
    }
}
