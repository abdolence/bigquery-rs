//! Every BigQuery type and mode, from the canonical value through each Rust form the encoder
//! accepts for it (`Canonical::targets`), encoded, decoded against the plan's own descriptor and
//! compared.

use super::tests::message_descriptor;
use super::*;
use crate::types::civil;
use crate::types::decimal;
use crate::types::testkit::{field, written, Canonical};
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
    Text(String),
    Bytes(Vec<u8>),
    ByteBuf(serde_bytes::ByteBuf),
    Date(jiff::civil::Date),
    BigQueryDate(BigQueryDate),
    Time(jiff::civil::Time),
    BigQueryTime(BigQueryTime),
    DateTime(jiff::civil::DateTime),
    BigQueryDateTime(BigQueryDateTime),
    Timestamp(jiff::Timestamp),
    BigQueryTimestamp(BigQueryTimestamp),
    Decimal(BigQueryDecimal<String>),
    Json(BigQueryJson<serde_json::Value>),
    Value(serde_json::Value),
    Interval(BigQueryInterval),
    RangeDate(BigQueryRange<jiff::civil::Date>),
    RangeBigQueryDate(BigQueryRange<BigQueryDate>),
    RangeDays(BigQueryRange<i32>),
    RangeDateTime(BigQueryRange<jiff::civil::DateTime>),
    RangeBigQueryDateTime(BigQueryRange<BigQueryDateTime>),
    RangeTimestamp(BigQueryRange<jiff::Timestamp>),
    RangeBigQueryTimestamp(BigQueryRange<BigQueryTimestamp>),
    RangeMicros(BigQueryRange<i64>),
    Struct(StructValue),
    Map(BTreeMap<String, i64>),
}

#[derive(Serialize, Clone, Debug)]
struct StructValue {
    quantity: i64,
}

#[derive(Serialize)]
struct Row<V> {
    value: V,
}

fn bound<T>(value: Option<i64>, convert: impl Fn(i64) -> Option<T>) -> Option<Option<T>> {
    match value {
        None => Some(None),
        Some(value) => convert(value).map(Some),
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
            Canonical::Int64(integer) => {
                let mut out = vec![Target::I64(integer)];
                out.extend(i32::try_from(integer).ok().map(Target::I32));
                out.extend(u64::try_from(integer).ok().map(Target::U64));
                out
            }
            Canonical::Float64(bits) => {
                let float = f64::from_bits(bits);
                let mut out = vec![Target::F64(float)];
                let narrow = float as f32;
                if !float.is_nan() && f64::from(narrow).to_bits() == bits {
                    out.push(Target::F32(narrow));
                }
                out
            }
            Canonical::Bool(flag) => vec![Target::Bool(flag)],
            Canonical::String(text) => vec![Target::Text(text)],
            Canonical::Bytes(bytes) => vec![
                Target::ByteBuf(serde_bytes::ByteBuf::from(bytes.clone())),
                Target::Bytes(bytes),
            ],
            Canonical::Date(days) => {
                let date = civil::jiff_date(days).expect("BigQuery's DATE range is inside jiff's");
                vec![
                    Target::Date(date),
                    Target::BigQueryDate(BigQueryDate(date)),
                    Target::I32(days),
                    Target::Text(written(|out| {
                        civil::fmt_date(days, out).expect("inside BigQuery's DATE range")
                    })),
                ]
            }
            Canonical::Time(micros) => {
                let time = civil::jiff_time(micros).expect("a time of day");
                vec![
                    Target::Time(time),
                    Target::BigQueryTime(BigQueryTime(time)),
                    Target::I64(micros),
                    Target::Text(written(|out| {
                        civil::fmt_time(micros, out).expect("a time of day")
                    })),
                ]
            }
            Canonical::DateTime(micros) => {
                let datetime = civil::jiff_datetime(micros)
                    .expect("BigQuery's DATETIME range is inside jiff's");
                vec![
                    Target::DateTime(datetime),
                    Target::BigQueryDateTime(BigQueryDateTime(datetime)),
                    Target::I64(micros),
                    Target::Text(written(|out| {
                        civil::fmt_datetime(micros, out).expect("inside BigQuery's DATETIME range")
                    })),
                ]
            }
            Canonical::Timestamp(micros) => {
                let mut out = vec![
                    Target::I64(micros),
                    Target::Text(written(|out| {
                        civil::fmt_timestamp(micros, out)
                            .expect("inside BigQuery's TIMESTAMP range")
                    })),
                ];
                // jiff ends below BigQuery's maximum; the integer and text forms cover the rest.
                if let Ok(timestamp) = civil::jiff_timestamp(micros) {
                    out.push(Target::Timestamp(timestamp));
                    out.push(Target::BigQueryTimestamp(BigQueryTimestamp(timestamp)));
                }
                out
            }
            Canonical::Numeric(unscaled) => {
                let text =
                    written(|out| decimal::fmt_decimal_i128(unscaled, decimal::NUMERIC_SCALE, out));
                let mut out = vec![
                    Target::Decimal(BigQueryDecimal(text.clone())),
                    Target::Text(text),
                ];
                let scale = 10i128.pow(decimal::NUMERIC_SCALE);
                if unscaled % scale == 0 {
                    out.extend(i64::try_from(unscaled / scale).ok().map(Target::I64));
                }
                out
            }
            Canonical::BigNumeric(unscaled) => {
                let text = written(|out| {
                    decimal::fmt_decimal_i256(unscaled, decimal::BIGNUMERIC_SCALE, out)
                });
                vec![
                    Target::Decimal(BigQueryDecimal(text.clone())),
                    Target::Text(text),
                ]
            }
            Canonical::Geography(text) => vec![Target::Text(text)],
            Canonical::Json(json) => {
                let mut out = vec![Target::Text(json.to_string())];
                // A top-level `Value::String` serializes as a string, which a JSON column takes
                // as the JSON text itself.
                if !json.is_string() {
                    out.push(Target::Value(json.clone()));
                }
                out.push(Target::Json(BigQueryJson(json)));
                out
            }
            Canonical::Interval(interval) => {
                vec![
                    Target::Text(written(|out| interval.write_bq(out))),
                    Target::Interval(interval),
                ]
            }
            Canonical::Range {
                element,
                start,
                end,
            } => {
                let mut out = Vec::new();
                match element {
                    BigQueryRangeElementType::Date => {
                        let date = |days: i64| civil::jiff_date(i32::try_from(days).ok()?).ok();
                        out.extend(range(start, end, date).map(Target::RangeDate));
                        out.extend(
                            range(start, end, |days| date(days).map(BigQueryDate))
                                .map(Target::RangeBigQueryDate),
                        );
                        out.extend(
                            range(start, end, |days| i32::try_from(days).ok())
                                .map(Target::RangeDays),
                        );
                    }
                    BigQueryRangeElementType::DateTime => {
                        let datetime = |micros: i64| civil::jiff_datetime(micros).ok();
                        out.extend(range(start, end, datetime).map(Target::RangeDateTime));
                        out.extend(
                            range(start, end, |micros| datetime(micros).map(BigQueryDateTime))
                                .map(Target::RangeBigQueryDateTime),
                        );
                        out.extend(range(start, end, Some).map(Target::RangeMicros));
                    }
                    BigQueryRangeElementType::Timestamp => {
                        let timestamp = |micros: i64| civil::jiff_timestamp(micros).ok();
                        out.extend(range(start, end, timestamp).map(Target::RangeTimestamp));
                        out.extend(
                            range(start, end, |micros| {
                                timestamp(micros).map(BigQueryTimestamp)
                            })
                            .map(Target::RangeBigQueryTimestamp),
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
                        .map(|side| match Canonical::decoded(&element_type, &side) {
                            Canonical::Date(days) => i64::from(days),
                            Canonical::DateTime(micros) | Canonical::Timestamp(micros) => micros,
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
                let quantity = message
                    .get_field_by_name("quantity")
                    .expect("quantity is declared");
                Canonical::decoded(&FieldType::Int64, &quantity)
            }
        }
    }

    fn struct_targets(&self) -> Vec<Target> {
        match self {
            Canonical::Int64(quantity) => vec![
                Target::Struct(StructValue {
                    quantity: *quantity,
                }),
                Target::Map([("quantity".to_string(), *quantity)].into()),
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
                "quantity",
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
            fields: vec![field("value", field_type.clone(), mode)],
        };
        let plan = Arc::new(WritePlan::new(&schema, false));
        let descriptor = message_descriptor(&plan);
        Column {
            field_type: field_type.clone(),
            plan,
            descriptor,
        }
    }

    fn decode<V: Serialize>(&self, value: &V) -> Result<Option<Value>, TestCaseError> {
        let mut out = Vec::new();
        Encoder::new(self.plan.clone())
            .encode(&Row { value }, &mut out)
            .map_err(|error| {
                TestCaseError::fail(format!("{} does not encode: {error}", self.field_type))
            })?;
        let message =
            DynamicMessage::decode(self.descriptor.clone(), out.as_slice()).map_err(|error| {
                TestCaseError::fail(format!("{} does not decode: {error}", self.field_type))
            })?;
        Ok(message
            .has_field_by_name("value")
            .then(|| {
                message
                    .get_field_by_name("value")
                    .map(|value| value.into_owned())
            })
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
            let got = got.map(|value| column.canonical(&value));
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
    for form in 0..widest {
        let elements: Vec<&Target> = per_value
            .iter()
            .map(|targets| &targets[form % targets.len()])
            .collect();
        let got = repeated.decode(&elements)?;
        let got: Vec<Canonical> = got
            .and_then(|list| list.as_list().map(<[Value]>::to_vec))
            .unwrap_or_default()
            .iter()
            .map(|element| repeated.canonical(element))
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
