//! Every type and mode of the type matrix, from the canonical value through each Rust form
//! the contract lists, encoded, decoded against the plan's own descriptor and compared.

use super::tests::{field, message_descriptor};
use super::*;
use crate::types::civil;
use crate::types::decimal;
use crate::types::testkit::{canonical, canonical_range, Canonical};
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

/// Every Rust form the contract lists for `value`'s type, which can hold `value`.
fn targets(value: &Canonical) -> Vec<Target> {
    use Target as T;
    match value.clone() {
        Canonical::Int64(x) => {
            let mut out = vec![T::I64(x)];
            out.extend(i32::try_from(x).ok().map(T::I32));
            out.extend(u64::try_from(x).ok().map(T::U64));
            out
        }
        Canonical::Float64(bits) => {
            let x = f64::from_bits(bits);
            let mut out = vec![T::F64(x)];
            let narrow = x as f32;
            if !x.is_nan() && f64::from(narrow).to_bits() == bits {
                out.push(T::F32(narrow));
            }
            out
        }
        Canonical::Bool(b) => vec![T::Bool(b)],
        Canonical::String(s) => vec![T::Str(s)],
        Canonical::Bytes(b) => vec![
            T::ByteBuf(serde_bytes::ByteBuf::from(b.clone())),
            T::Bytes(b),
        ],
        Canonical::Date(days) => {
            let d = civil::jiff_date(days).expect("BigQuery's DATE range is inside jiff's");
            vec![
                T::Date(d),
                T::BqDate(BigQueryDate(d)),
                T::I32(days),
                T::Str(text(|o| civil::fmt_date(days, o))),
            ]
        }
        Canonical::Time(us) => {
            let t = civil::jiff_time(us).expect("a time of day");
            vec![
                T::Time(t),
                T::BqTime(BigQueryTime(t)),
                T::I64(us),
                T::Str(text(|o| civil::fmt_time(us, o))),
            ]
        }
        Canonical::DateTime(us) => {
            let dt = civil::jiff_datetime(us).expect("BigQuery's DATETIME range is inside jiff's");
            vec![
                T::DateTime(dt),
                T::BqDateTime(BigQueryDateTime(dt)),
                T::I64(us),
                T::Str(text(|o| civil::fmt_datetime(us, o))),
            ]
        }
        Canonical::Timestamp(us) => {
            let mut out = vec![T::I64(us), T::Str(text(|o| civil::fmt_timestamp(us, o)))];
            // jiff ends below BigQuery's maximum; the integer and text forms cover the rest.
            if let Ok(ts) = civil::jiff_timestamp(us) {
                out.push(T::Timestamp(ts));
                out.push(T::BqTimestamp(BigQueryTimestamp(ts)));
            }
            out
        }
        Canonical::Numeric(v) => {
            let s = text(|o| decimal::fmt_decimal_i128(v, decimal::NUMERIC_SCALE, o));
            let mut out = vec![T::Decimal(BigQueryDecimal(s.clone())), T::Str(s)];
            let scale = 10i128.pow(decimal::NUMERIC_SCALE);
            if v % scale == 0 {
                out.extend(i64::try_from(v / scale).ok().map(T::I64));
            }
            out
        }
        Canonical::BigNumeric(v) => {
            let s = text(|o| decimal::fmt_decimal_i256(v, decimal::BIGNUMERIC_SCALE, o));
            vec![T::Decimal(BigQueryDecimal(s.clone())), T::Str(s)]
        }
        Canonical::Geography(s) => vec![T::Str(s)],
        Canonical::Json(v) => vec![T::Str(v.to_string()), T::Json(BigQueryJson(v))],
        Canonical::Interval(iv) => vec![T::Str(text(|o| iv.write_bq(o))), T::Interval(iv)],
        Canonical::Range {
            element,
            start,
            end,
        } => {
            let mut out = Vec::new();
            match element {
                BigQueryRangeElementType::Date => {
                    let date = |v: i64| civil::jiff_date(i32::try_from(v).ok()?).ok();
                    out.extend(range(start, end, date).map(T::RangeDate));
                    out.extend(
                        range(start, end, |v| date(v).map(BigQueryDate)).map(T::RangeBqDate),
                    );
                    out.extend(range(start, end, |v| i32::try_from(v).ok()).map(T::RangeDays));
                }
                BigQueryRangeElementType::DateTime => {
                    let dt = |v: i64| civil::jiff_datetime(v).ok();
                    out.extend(range(start, end, dt).map(T::RangeDateTime));
                    out.extend(
                        range(start, end, |v| dt(v).map(BigQueryDateTime)).map(T::RangeBqDateTime),
                    );
                    out.extend(range(start, end, Some).map(T::RangeMicros));
                }
                BigQueryRangeElementType::Timestamp => {
                    let ts = |v: i64| civil::jiff_timestamp(v).ok();
                    out.extend(range(start, end, ts).map(T::RangeTimestamp));
                    out.extend(
                        range(start, end, |v| ts(v).map(BigQueryTimestamp))
                            .map(T::RangeBqTimestamp),
                    );
                    out.extend(range(start, end, Some).map(T::RangeMicros));
                }
            }
            out
        }
    }
}

/// A canonical value read back from one decoded field of `field_type`.
fn canonical_of(field_type: &BigQueryFieldType, value: &Value) -> Canonical {
    use BigQueryFieldType as F;
    let int = || value.as_i64().expect("an int64 field");
    let bytes = || value.as_bytes().expect("a bytes field").to_vec();
    let string = || value.as_str().expect("a string field").to_string();
    match field_type {
        F::Int64 => Canonical::Int64(int()),
        F::Float64 => Canonical::Float64(value.as_f64().expect("a double field").to_bits()),
        F::Bool => Canonical::Bool(value.as_bool().expect("a bool field")),
        F::String { .. } => Canonical::String(string()),
        F::Bytes { .. } => Canonical::Bytes(bytes()),
        F::Date => Canonical::Date(value.as_i32().expect("an int32 field")),
        F::Time => Canonical::Time(civil::unpack_time(int())),
        F::DateTime => Canonical::DateTime(civil::unpack_datetime(int())),
        F::Timestamp => Canonical::Timestamp(int()),
        F::Numeric(_) => Canonical::Numeric(
            decimal::decimal_from_le_bytes(&bytes())
                .to_i128()
                .expect("a NUMERIC fits i128"),
        ),
        F::BigNumeric(_) => Canonical::BigNumeric(decimal::decimal_from_le_bytes(&bytes())),
        F::Geography => Canonical::Geography(string()),
        F::Json => Canonical::Json(serde_json::from_str(&string()).expect("JSON text")),
        F::Interval => Canonical::Interval(
            BigQueryInterval::parse_bq(&string()).expect("canonical INTERVAL text"),
        ),
        F::Range(element) => {
            let message = value.as_message().expect("a RANGE message");
            let element_type = match element {
                BigQueryRangeElementType::Date => F::Date,
                BigQueryRangeElementType::DateTime => F::DateTime,
                BigQueryRangeElementType::Timestamp => F::Timestamp,
            };
            let side = |name: &str| {
                message
                    .has_field_by_name(name)
                    .then(|| message.get_field_by_name(name))
                    .flatten()
                    .map(|v| match canonical_of(&element_type, &v) {
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
        F::Struct(_) => {
            let message = value.as_message().expect("a STRUCT message");
            let x = message.get_field_by_name("x").expect("x is declared");
            canonical_of(&F::Int64, &x)
        }
    }
}

fn targets_of_struct(value: &Canonical) -> Vec<Target> {
    match value {
        Canonical::Int64(x) => vec![
            Target::Struct(StructValue { x: *x }),
            Target::Map([("x".to_string(), *x)].into()),
        ],
        other => panic!("a STRUCT case carries its field's INT64, got {other:?}"),
    }
}

struct Case {
    field_type: BigQueryFieldType,
    values: BoxedStrategy<Canonical>,
}

fn cases() -> Vec<Case> {
    use BigQueryFieldType as F;
    use BqKind as K;
    let scalar = |field_type: F, kind: K| Case {
        field_type,
        values: canonical(kind),
    };
    let range = |element: BigQueryRangeElementType| Case {
        field_type: F::Range(element),
        values: canonical_range(element),
    };
    vec![
        scalar(F::Int64, K::Int64),
        scalar(F::Float64, K::Float64),
        scalar(F::Numeric(None), K::Numeric),
        scalar(F::BigNumeric(None), K::BigNumeric),
        scalar(F::Bool, K::Bool),
        scalar(F::String { max_length: None }, K::String),
        scalar(F::Bytes { max_length: None }, K::Bytes),
        scalar(F::Date, K::Date),
        scalar(F::Time, K::Time),
        scalar(F::DateTime, K::DateTime),
        scalar(F::Timestamp, K::Timestamp),
        scalar(F::Geography, K::Geography),
        scalar(F::Json, K::Json),
        scalar(F::Interval, K::Interval),
        range(BigQueryRangeElementType::Date),
        range(BigQueryRangeElementType::DateTime),
        range(BigQueryRangeElementType::Timestamp),
        Case {
            field_type: F::Struct(vec![field("x", F::Int64, BigQueryFieldMode::Nullable)]),
            values: canonical(K::Int64),
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
        canonical_of(&self.field_type, value)
    }
}

fn check(case: &Case, values: &[Canonical]) -> Result<(), TestCaseError> {
    let targets_of = |value: &Canonical| match case.field_type {
        BigQueryFieldType::Struct(_) => targets_of_struct(value),
        _ => targets(value),
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
