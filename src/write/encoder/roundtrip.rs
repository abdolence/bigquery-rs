//! Every type and mode of the type matrix, from the canonical value through each Rust form
//! the contract lists, encoded, decoded against the plan's own descriptor and compared.

use super::tests::{field, message_descriptor};
use super::*;
use crate::types::civil;
use crate::types::decimal;
use crate::types::testkit::Canonical;
use crate::{
    BigQueryDate, BigQueryDateTime, BigQueryDecimal, BigQueryFieldMode, BigQueryFieldType,
    BigQueryInterval, BigQueryJson, BigQueryRange, BigQueryRangeElementType, BigQueryTableSchema,
    BigQueryTime, BigQueryTimestamp,
};
use arrow_buffer::i256;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use prost_reflect::{DynamicMessage, MessageDescriptor, Value};
use serde::Serialize;
use std::collections::BTreeMap;

/// One Rust form of a value. Untagged, so each variant serializes exactly as its inner type.
#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
enum Target {
    I64(i64),
    I32(i32),
    U64(u64),
    F64(f64),
    F32(f32),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
    ByteBuf(serde_bytes::ByteBuf),
    Date(jiff::civil::Date),
    BqDate(BigQueryDate),
    Time(jiff::civil::Time),
    BqTime(BigQueryTime),
    DateTime(jiff::civil::DateTime),
    BqDateTime(BigQueryDateTime),
    Timestamp(jiff::Timestamp),
    BqTimestamp(BigQueryTimestamp),
    Decimal(BigQueryDecimal<String>),
    Json(BigQueryJson<serde_json::Value>),
    Value(serde_json::Value),
    Interval(BigQueryInterval),
    RangeDate(BigQueryRange<jiff::civil::Date>),
    RangeBqDate(BigQueryRange<BigQueryDate>),
    RangeDays(BigQueryRange<i32>),
    RangeDateTime(BigQueryRange<jiff::civil::DateTime>),
    RangeBqDateTime(BigQueryRange<BigQueryDateTime>),
    RangeTimestamp(BigQueryRange<jiff::Timestamp>),
    RangeBqTimestamp(BigQueryRange<BigQueryTimestamp>),
    RangeMicros(BigQueryRange<i64>),
    Struct(StructValue),
    Map(BTreeMap<String, i64>),
}

#[derive(Serialize, Clone, Debug)]
struct StructValue {
    x: i64,
}

#[derive(Serialize)]
struct Row<V> {
    v: V,
}

fn text(write: impl FnOnce(&mut String)) -> String {
    let mut out = String::new();
    write(&mut out);
    out
}

fn bound<T>(b: Option<i64>, convert: impl Fn(i64) -> Option<T>) -> Option<Option<T>> {
    match b {
        None => Some(None),
        Some(v) => convert(v).map(Some),
    }
}

fn range<T>(
    start: Option<i64>,
    end: Option<i64>,
    convert: impl Fn(i64) -> Option<T>,
) -> Option<BigQueryRange<T>> {
    Some(BigQueryRange {
        start: bound(start, &convert)?,
        end: bound(end, &convert)?,
    })
}

impl Canonical {
    /// Every Rust form the contract lists for `value`'s type, which can hold `value`.
    fn targets(&self) -> Vec<Target> {
        match self.clone() {
            Canonical::Int64(x) => {
                let mut out = vec![Target::I64(x)];
                out.extend(i32::try_from(x).ok().map(Target::I32));
                out.extend(u64::try_from(x).ok().map(Target::U64));
                out
            }
            Canonical::Float64(bits) => {
                let x = f64::from_bits(bits);
                let mut out = vec![Target::F64(x)];
                let narrow = x as f32;
                if !x.is_nan() && f64::from(narrow).to_bits() == bits {
                    out.push(Target::F32(narrow));
                }
                out
            }
            Canonical::Bool(b) => vec![Target::Bool(b)],
            Canonical::String(s) => vec![Target::Str(s)],
            Canonical::Bytes(b) => vec![
                Target::ByteBuf(serde_bytes::ByteBuf::from(b.clone())),
                Target::Bytes(b),
            ],
            Canonical::Date(days) => {
                let d = civil::jiff_date(days).expect("BigQuery's DATE range is inside jiff's");
                vec![
                    Target::Date(d),
                    Target::BqDate(BigQueryDate(d)),
                    Target::I32(days),
                    Target::Str(text(|o| civil::fmt_date(days, o))),
                ]
            }
            Canonical::Time(us) => {
                let t = civil::jiff_time(us).expect("a time of day");
                vec![
                    Target::Time(t),
                    Target::BqTime(BigQueryTime(t)),
                    Target::I64(us),
                    Target::Str(text(|o| civil::fmt_time(us, o))),
                ]
            }
            Canonical::DateTime(us) => {
                let dt =
                    civil::jiff_datetime(us).expect("BigQuery's DATETIME range is inside jiff's");
                vec![
                    Target::DateTime(dt),
                    Target::BqDateTime(BigQueryDateTime(dt)),
                    Target::I64(us),
                    Target::Str(text(|o| civil::fmt_datetime(us, o))),
                ]
            }
            Canonical::Timestamp(us) => {
                let mut out = vec![
                    Target::I64(us),
                    Target::Str(text(|o| civil::fmt_timestamp(us, o))),
                ];
                // jiff ends below BigQuery's maximum; the integer and text forms cover the rest.
                if let Ok(ts) = civil::jiff_timestamp(us) {
                    out.push(Target::Timestamp(ts));
                    out.push(Target::BqTimestamp(BigQueryTimestamp(ts)));
                }
                out
            }
            Canonical::Numeric(v) => {
                let s = text(|o| decimal::fmt_decimal_i128(v, decimal::NUMERIC_SCALE, o));
                let mut out = vec![Target::Decimal(BigQueryDecimal(s.clone())), Target::Str(s)];
                let scale = 10i128.pow(decimal::NUMERIC_SCALE);
                if v % scale == 0 {
                    out.extend(i64::try_from(v / scale).ok().map(Target::I64));
                }
                out
            }
            Canonical::BigNumeric(v) => {
                let s = text(|o| decimal::fmt_decimal_i256(v, decimal::BIGNUMERIC_SCALE, o));
                vec![Target::Decimal(BigQueryDecimal(s.clone())), Target::Str(s)]
            }
            Canonical::Geography(s) => vec![Target::Str(s)],
            Canonical::Json(v) => {
                let mut out = vec![Target::Str(v.to_string())];
                // A top-level `Value::String` serializes as a string, which a JSON column takes
                // as the JSON text itself.
                if !v.is_string() {
                    out.push(Target::Value(v.clone()));
                }
                out.push(Target::Json(BigQueryJson(v)));
                out
            }
            Canonical::Interval(iv) => {
                vec![Target::Str(text(|o| iv.write_bq(o))), Target::Interval(iv)]
            }
            Canonical::Range {
                element,
                start,
                end,
            } => {
                let mut out = Vec::new();
                match element {
                    BigQueryRangeElementType::Date => {
                        let date = |v: i64| civil::jiff_date(i32::try_from(v).ok()?).ok();
                        out.extend(range(start, end, date).map(Target::RangeDate));
                        out.extend(
                            range(start, end, |v| date(v).map(BigQueryDate))
                                .map(Target::RangeBqDate),
                        );
                        out.extend(
                            range(start, end, |v| i32::try_from(v).ok()).map(Target::RangeDays),
                        );
                    }
                    BigQueryRangeElementType::DateTime => {
                        let dt = |v: i64| civil::jiff_datetime(v).ok();
                        out.extend(range(start, end, dt).map(Target::RangeDateTime));
                        out.extend(
                            range(start, end, |v| dt(v).map(BigQueryDateTime))
                                .map(Target::RangeBqDateTime),
                        );
                        out.extend(range(start, end, Some).map(Target::RangeMicros));
                    }
                    BigQueryRangeElementType::Timestamp => {
                        let ts = |v: i64| civil::jiff_timestamp(v).ok();
                        out.extend(range(start, end, ts).map(Target::RangeTimestamp));
                        out.extend(
                            range(start, end, |v| ts(v).map(BigQueryTimestamp))
                                .map(Target::RangeBqTimestamp),
                        );
                        out.extend(range(start, end, Some).map(Target::RangeMicros));
                    }
                }
                out
            }
        }
    }

    /// A canonical value read back from one decoded field of `field_type`.
    fn decoded(field_type: &BigQueryFieldType, value: &Value) -> Canonical {
        use BigQueryFieldType as FieldType;
        match field_type {
            FieldType::Int64 => Canonical::Int64(value.as_i64().expect("an int64 field")),
            FieldType::Float64 => {
                Canonical::Float64(value.as_f64().expect("a double field").to_bits())
            }
            FieldType::Bool => Canonical::Bool(value.as_bool().expect("a bool field")),
            FieldType::String { .. } => {
                Canonical::String(value.as_str().expect("a string field").to_string())
            }
            FieldType::Bytes { .. } => {
                Canonical::Bytes(value.as_bytes().expect("a bytes field").to_vec())
            }
            FieldType::Date => Canonical::Date(value.as_i32().expect("an int32 field")),
            FieldType::Time => {
                Canonical::Time(civil::unpack_time(value.as_i64().expect("an int64 field")))
            }
            FieldType::DateTime => Canonical::DateTime(civil::unpack_datetime(
                value.as_i64().expect("an int64 field"),
            )),
            FieldType::Timestamp => Canonical::Timestamp(value.as_i64().expect("an int64 field")),
            FieldType::Numeric(_) => Canonical::Numeric(
                decimal::decimal_from_le_bytes(value.as_bytes().expect("a bytes field"))
                    .to_i128()
                    .expect("a NUMERIC fits i128"),
            ),
            FieldType::BigNumeric(_) => Canonical::BigNumeric(decimal::decimal_from_le_bytes(
                value.as_bytes().expect("a bytes field"),
            )),
            FieldType::Geography => {
                Canonical::Geography(value.as_str().expect("a string field").to_string())
            }
            FieldType::Json => Canonical::Json(
                serde_json::from_str(value.as_str().expect("a string field")).expect("JSON text"),
            ),
            FieldType::Interval => Canonical::Interval(
                BigQueryInterval::parse_bq(value.as_str().expect("a string field"))
                    .expect("canonical INTERVAL text"),
            ),
            FieldType::Range(element) => {
                let message = value.as_message().expect("a RANGE message");
                let element_type = match element {
                    BigQueryRangeElementType::Date => FieldType::Date,
                    BigQueryRangeElementType::DateTime => FieldType::DateTime,
                    BigQueryRangeElementType::Timestamp => FieldType::Timestamp,
                };
                let side = |name: &str| {
                    message
                        .has_field_by_name(name)
                        .then(|| message.get_field_by_name(name))
                        .flatten()
                        .map(|v| match Canonical::decoded(&element_type, &v) {
                            Canonical::Date(d) => i64::from(d),
                            Canonical::DateTime(v) | Canonical::Timestamp(v) => v,
                            other => panic!("not a RANGE element: {other:?}"),
                        })
                };
                Canonical::Range {
                    element: *element,
                    start: side("start"),
                    end: side("end"),
                }
            }
            FieldType::Struct(_) => {
                let message = value.as_message().expect("a STRUCT message");
                let x = message.get_field_by_name("x").expect("x is declared");
                Canonical::decoded(&FieldType::Int64, &x)
            }
        }
    }

    fn struct_targets(&self) -> Vec<Target> {
        match self {
            Canonical::Int64(x) => vec![
                Target::Struct(StructValue { x: *x }),
                Target::Map([("x".to_string(), *x)].into()),
            ],
            other => panic!("a STRUCT case carries its field's INT64, got {other:?}"),
        }
    }
}

struct Case {
    field_type: BigQueryFieldType,
    values: BoxedStrategy<Canonical>,
}

fn cases() -> Vec<Case> {
    use BigQueryFieldType as FieldType;
    let scalar = |field_type: FieldType, kind: FieldKind| Case {
        field_type,
        values: Canonical::strategy(kind),
    };
    let range = |element: BigQueryRangeElementType| Case {
        field_type: FieldType::Range(element),
        values: Canonical::range_strategy(element),
    };
    vec![
        scalar(FieldType::Int64, FieldKind::Int64),
        scalar(FieldType::Float64, FieldKind::Float64),
        scalar(FieldType::Numeric(None), FieldKind::Numeric),
        scalar(FieldType::BigNumeric(None), FieldKind::BigNumeric),
        scalar(FieldType::Bool, FieldKind::Bool),
        scalar(FieldType::String { max_length: None }, FieldKind::String),
        scalar(FieldType::Bytes { max_length: None }, FieldKind::Bytes),
        scalar(FieldType::Date, FieldKind::Date),
        scalar(FieldType::Time, FieldKind::Time),
        scalar(FieldType::DateTime, FieldKind::DateTime),
        scalar(FieldType::Timestamp, FieldKind::Timestamp),
        scalar(FieldType::Geography, FieldKind::Geography),
        scalar(FieldType::Json, FieldKind::Json),
        scalar(FieldType::Interval, FieldKind::Interval),
        range(BigQueryRangeElementType::Date),
        range(BigQueryRangeElementType::DateTime),
        range(BigQueryRangeElementType::Timestamp),
        Case {
            field_type: FieldType::Struct(vec![field(
                "x",
                FieldType::Int64,
                BigQueryFieldMode::Nullable,
            )]),
            values: Canonical::strategy(FieldKind::Int64),
        },
    ]
}

struct Column {
    field_type: BigQueryFieldType,
    plan: Arc<WritePlan>,
    descriptor: MessageDescriptor,
}

impl Column {
    fn new(field_type: &BigQueryFieldType, mode: BigQueryFieldMode) -> Self {
        let schema = BigQueryTableSchema {
            fields: vec![field("v", field_type.clone(), mode)],
        };
        let plan = Arc::new(WritePlan::new(&schema, false));
        let descriptor = message_descriptor(&plan);
        Column {
            field_type: field_type.clone(),
            plan,
            descriptor,
        }
    }

    fn decode<V: Serialize>(&self, v: &V) -> Result<Option<Value>, TestCaseError> {
        let mut out = Vec::new();
        Encoder::new(self.plan.clone())
            .encode(&Row { v }, &mut out)
            .map_err(|e| {
                TestCaseError::fail(format!("{} does not encode: {e}", self.field_type))
            })?;
        let message =
            DynamicMessage::decode(self.descriptor.clone(), out.as_slice()).map_err(|e| {
                TestCaseError::fail(format!("{} does not decode: {e}", self.field_type))
            })?;
        Ok(message
            .has_field_by_name("v")
            .then(|| message.get_field_by_name("v").map(|v| v.into_owned()))
            .flatten())
    }

    fn canonical(&self, value: &Value) -> Canonical {
        Canonical::decoded(&self.field_type, value)
    }
}

fn check(case: &Case, values: &[Canonical]) -> Result<(), TestCaseError> {
    let targets_of = |value: &Canonical| match case.field_type {
        BigQueryFieldType::Struct(_) => value.struct_targets(),
        _ => value.targets(),
    };
    let first = &values[0];
    let first_targets = targets_of(first);
    prop_assert!(!first_targets.is_empty(), "no Rust form for {first:?}");

    let nullable = Column::new(&case.field_type, BigQueryFieldMode::Nullable);
    let required = Column::new(&case.field_type, BigQueryFieldMode::Required);
    for target in &first_targets {
        for (column, got) in [
            (&nullable, nullable.decode(&Some(target))?),
            (&required, required.decode(target)?),
        ] {
            let got = got.map(|v| column.canonical(&v));
            prop_assert_eq!(
                got.as_ref(),
                Some(first),
                "{:?} as {:?}",
                case.field_type,
                target
            );
        }
    }
    prop_assert_eq!(nullable.decode(&None::<Target>)?, None);

    let repeated = Column::new(&case.field_type, BigQueryFieldMode::Repeated);
    let per_value: Vec<Vec<Target>> = values.iter().map(targets_of).collect();
    let widest = per_value.iter().map(Vec::len).max().unwrap_or(0);
    for j in 0..widest {
        let elements: Vec<&Target> = per_value.iter().map(|t| &t[j % t.len()]).collect();
        let got = repeated.decode(&elements)?;
        let got: Vec<Canonical> = got
            .and_then(|v| v.as_list().map(<[Value]>::to_vec))
            .unwrap_or_default()
            .iter()
            .map(|v| repeated.canonical(v))
            .collect();
        prop_assert_eq!(
            &got,
            &values.to_vec(),
            "{:?} as {:?}",
            case.field_type,
            elements
        );
    }
    prop_assert_eq!(repeated.decode(&Vec::<Target>::new())?, None);
    Ok(())
}

#[test]
fn every_type_and_mode_encodes_its_canonical_wire_form() {
    for case in cases() {
        let mut runner = TestRunner::new(Config {
            cases: 128,
            failure_persistence: None,
            ..Config::default()
        });
        let values = proptest::collection::vec(case.values.clone(), 1..4);
        if let Err(err) = runner.run(&values, |values| check(&case, &values)) {
            panic!("{}: {err}", case.field_type);
        }
    }
}

#[test]
fn bignumeric_extremes_round_trip() {
    let case = &cases()[3];
    assert_eq!(case.field_type, BigQueryFieldType::BigNumeric(None));
    let extremes = [
        Canonical::BigNumeric(i256::MAX),
        Canonical::BigNumeric(i256::MIN),
        Canonical::BigNumeric(i256::ZERO),
    ];
    if let Err(err) = check(case, &extremes) {
        panic!("{err}");
    }
}
