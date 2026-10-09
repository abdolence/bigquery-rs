use super::*;
use crate::errors::{BigQueryError, BigQuerySerializationError};
use crate::types::civil;
use crate::types::testkit::Canonical;
use crate::{
    BigQueryDate, BigQueryDateTime, BigQueryDecimal, BigQueryJson, BigQueryRange,
    BigQueryRangeElementType, BigQueryTime, BigQueryTimestamp,
};
use arrow_array::types::IntervalMonthDayNano;
use arrow_array::*;
use arrow_buffer::{i256, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use proptest::prelude::*;
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::sync::Arc;

fn batch(columns: Vec<(Field, ArrayRef)>) -> RecordBatch {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays)
        .expect("the test columns form a batch")
}

fn column<A: Array + 'static>(name: &str, array: A) -> (Field, ArrayRef) {
    (
        Field::new(name, array.data_type().clone(), true),
        Arc::new(array),
    )
}

fn required<A: Array + 'static>(name: &str, array: A) -> (Field, ArrayRef) {
    (
        Field::new(name, array.data_type().clone(), false),
        Arc::new(array),
    )
}

fn extension_metadata(name: &str) -> HashMap<String, String> {
    HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
}

fn range_metadata() -> HashMap<String, String> {
    HashMap::from([("google:sqlType".to_string(), "range".to_string())])
}

/// BigQuery's ARRAY shape: a non-null `List` whose item field is `item`, with the element's
/// metadata on the list field.
fn list_with(
    name: &str,
    offsets: Vec<i32>,
    values: ArrayRef,
    metadata: HashMap<String, String>,
) -> (Field, ArrayRef) {
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let array = ListArray::new(item, OffsetBuffer::new(offsets.into()), values, None);
    (
        Field::new(name, array.data_type().clone(), false).with_metadata(metadata),
        Arc::new(array),
    )
}

fn list(name: &str, offsets: Vec<i32>, values: ArrayRef) -> (Field, ArrayRef) {
    list_with(name, offsets, values, HashMap::new())
}

fn struct_column(
    name: &str,
    children: Vec<(Field, ArrayRef)>,
    nulls: Option<Vec<bool>>,
) -> (Field, ArrayRef) {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = children.into_iter().unzip();
    let array = StructArray::new(Fields::from(fields), arrays, nulls.map(NullBuffer::from));
    (
        Field::new(name, array.data_type().clone(), true),
        Arc::new(array),
    )
}

fn timestamp_micros(text: &str) -> i64 {
    civil::parse_timestamp(text).expect("a valid test timestamp")
}

fn rows<T: DeserializeOwned>(batch: &RecordBatch) -> Vec<T> {
    decode_rows::<T>(batch, 0)
        .into_iter()
        .collect::<BigQueryResult<Vec<T>>>()
        .unwrap_or_else(|error| panic!("every row decodes: {error}"))
}

fn row<T: DeserializeOwned>(
    batch: &RecordBatch,
    index: usize,
) -> Result<T, BigQuerySerializationError> {
    match decode_rows::<T>(batch, 0).swap_remove(index) {
        Ok(value) => Ok(value),
        Err(BigQueryError::DeserializeError(details)) => Err(details),
        Err(other) => panic!("expected a deserialize error, got {other:?}"),
    }
}

fn row_error<T: DeserializeOwned>(batch: &RecordBatch, index: usize) -> BigQuerySerializationError {
    match row::<T>(batch, index) {
        Ok(_) => panic!("row {index} decoded, and it must fail"),
        Err(error) => error,
    }
}

fn all_scalars() -> RecordBatch {
    batch(vec![
        column("quantity", Int64Array::from(vec![i64::MIN])),
        column("price", Float64Array::from(vec![f64::NEG_INFINITY])),
        column("in_stock", BooleanArray::from(vec![true])),
        column("name", StringArray::from(vec!["héllo 世界 🦀"])),
        column("payload", BinaryArray::from_vec(vec![&[0u8, 255, 16][..]])),
        column("order_date", Date32Array::from(vec![-719162])),
        column(
            "pickup_time",
            Time64MicrosecondArray::from(vec![86_399_999_999]),
        ),
        {
            let (field, array) = column(
                "placed_local",
                TimestampMicrosecondArray::from(vec![timestamp_micros(
                    "2024-02-29T12:34:56.789012Z",
                )]),
            );
            (
                field.with_metadata(extension_metadata("google:sqlType:datetime")),
                array,
            )
        },
        column(
            "placed_at",
            TimestampMicrosecondArray::from(vec![timestamp_micros("1969-07-20T20:17:40Z")])
                .with_timezone("UTC"),
        ),
        column(
            "total",
            Decimal128Array::from(vec![10i128.pow(38) - 1])
                .with_precision_and_scale(38, 9)
                .expect("NUMERIC's Arrow type"),
        ),
        column(
            "big_total",
            Decimal256Array::from(vec![i256::MIN])
                .with_precision_and_scale(76, 38)
                .expect("BIGNUMERIC's Arrow type"),
        ),
        {
            let (field, array) = column(
                "attributes",
                StringArray::from(vec![r#"{"quantity":1,"in_stock":[true,null]}"#]),
            );
            (
                field.with_metadata(extension_metadata("google:sqlType:json")),
                array,
            )
        },
        {
            let (field, array) = column(
                "lead_time",
                IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(
                    14,
                    -3,
                    14_706_000_789_000,
                )]),
            );
            (
                field.with_metadata(extension_metadata("google:sqlType:interval")),
                array,
            )
        },
    ])
}

#[derive(Deserialize, Debug, PartialEq)]
struct Scalars {
    quantity: i64,
    price: f64,
    in_stock: bool,
    name: String,
    payload: Vec<u8>,
    order_date: jiff::civil::Date,
    pickup_time: jiff::civil::Time,
    placed_local: jiff::civil::DateTime,
    placed_at: jiff::Timestamp,
    total: String,
    big_total: String,
    attributes: BigQueryJson<serde_json::Value>,
    lead_time: crate::BigQueryInterval,
}

#[test]
fn scalars_decode_into_their_rust_forms() {
    let got: Vec<Scalars> = rows(&all_scalars());
    assert_eq!(
        got,
        vec![Scalars {
            quantity: i64::MIN,
            price: f64::NEG_INFINITY,
            in_stock: true,
            name: "héllo 世界 🦀".into(),
            payload: vec![0, 255, 16],
            order_date: jiff::civil::date(1, 1, 1),
            pickup_time: "23:59:59.999999".parse().expect("valid"),
            placed_local: "2024-02-29T12:34:56.789012".parse().expect("valid"),
            placed_at: "1969-07-20T20:17:40Z".parse().expect("valid"),
            total: "99999999999999999999999999999.999999999".into(),
            big_total:
                "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
                    .into(),
            attributes: BigQueryJson(serde_json::json!({"quantity": 1, "in_stock": [true, null]})),
            lead_time: crate::BigQueryInterval {
                months: 14,
                days: -3,
                nanos: 14_706_000_789_000,
            },
        }]
    );
}

#[test]
fn batch_rows_yield_every_row_past_a_failing_one() {
    let batch = batch(vec![column(
        "quantity",
        Int64Array::from(vec![Some(1), None, Some(3)]),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Order {
        quantity: i64,
    }
    let decoded: Vec<_> = BigQueryBatchRows::<Order>::new(&batch).collect();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0].as_ref().ok(), Some(&Order { quantity: 1 }));
    let Err(BigQueryError::DeserializeError(error)) = &decoded[1] else {
        panic!("the NULL row must fail, got {:?}", decoded[1]);
    };
    assert_eq!(error.row, Some(1));
    assert_eq!(decoded[2].as_ref().ok(), Some(&Order { quantity: 3 }));
}

#[test]
fn null_cells_need_option_and_errors_carry_row_and_path() {
    let batch = batch(vec![column(
        "quantity",
        Int64Array::from(vec![Some(1), None]),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct OptionalOrder {
        quantity: Option<i64>,
    }
    #[derive(Deserialize, Debug)]
    struct Order {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        quantity: i64,
    }
    assert_eq!(
        rows::<OptionalOrder>(&batch),
        vec![
            OptionalOrder { quantity: Some(1) },
            OptionalOrder { quantity: None }
        ]
    );
    let decoded = decode_rows::<Order>(&batch, 40);
    assert!(decoded[0].is_ok());
    let Err(BigQueryError::DeserializeError(error)) = &decoded[1] else {
        panic!("the NULL row must fail, got {:?}", decoded[1]);
    };
    assert_eq!(error.kind, BigQueryCodecErrorKind::NullForNonOption);
    assert_eq!(error.row, Some(41));
    assert_eq!(error.path, "quantity");
}

fn nested() -> RecordBatch {
    let inner = struct_column(
        "inner",
        vec![column(
            "delivered_on",
            Date32Array::from(vec![Some(19782), None, None]),
        )],
        Some(vec![true, false, true]),
    );
    let tags = list(
        "tags",
        vec![0, 2, 2, 3],
        Arc::new(StringArray::from(vec!["gift", "fragile", "express"])),
    );
    let line_item = struct_column(
        "line_item",
        vec![
            column("quantity", Int64Array::from(vec![Some(7), None, None])),
            tags,
            inner,
        ],
        Some(vec![true, false, true]),
    );
    let measurement_values = StructArray::new(
        Fields::from(vec![
            Field::new("unit", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]),
        vec![
            Arc::new(StringArray::from(vec![Some("kg"), Some("cm"), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None])),
        ],
        None,
    );
    let measurements = list(
        "measurements",
        vec![0, 2, 2, 3],
        Arc::new(measurement_values),
    );
    batch(vec![
        column("id", Int64Array::from(vec![0, 1, 2])),
        line_item,
        measurements,
    ])
}

#[derive(Deserialize, Debug, PartialEq)]
struct Inner {
    delivered_on: Option<jiff::civil::Date>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct LineItem {
    quantity: Option<i64>,
    tags: Vec<String>,
    inner: Option<Inner>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Measurement {
    unit: Option<String>,
    amount: Option<f64>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct NestedRow {
    id: i64,
    line_item: Option<LineItem>,
    measurements: Vec<Measurement>,
}

#[test]
fn nested_struct_list_and_list_of_struct() {
    assert_eq!(
        rows::<NestedRow>(&nested()),
        vec![
            NestedRow {
                id: 0,
                line_item: Some(LineItem {
                    quantity: Some(7),
                    tags: vec!["gift".into(), "fragile".into()],
                    inner: Some(Inner {
                        delivered_on: Some(jiff::civil::date(2024, 2, 29))
                    }),
                }),
                measurements: vec![
                    Measurement {
                        unit: Some("kg".into()),
                        amount: Some(1.0)
                    },
                    Measurement {
                        unit: Some("cm".into()),
                        amount: Some(2.0)
                    }
                ],
            },
            NestedRow {
                id: 1,
                line_item: None,
                measurements: vec![]
            },
            NestedRow {
                id: 2,
                line_item: Some(LineItem {
                    quantity: None,
                    tags: vec!["express".into()],
                    inner: Some(Inner { delivered_on: None }),
                }),
                measurements: vec![Measurement {
                    unit: None,
                    amount: None
                }],
            },
        ]
    );
}

#[test]
fn null_struct_into_bare_struct_is_an_error() {
    #[derive(Deserialize, Debug)]
    struct Bare {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        line_item: LineItem,
    }
    let error = row_error::<Bare>(&nested(), 1);
    assert_eq!(
        (error.row, error.path.as_str(), error.kind),
        (
            Some(1),
            "line_item",
            BigQueryCodecErrorKind::NullForNonOption
        )
    );
}

#[test]
fn error_path_names_the_nested_list_element() {
    #[derive(Deserialize, Debug)]
    struct BadMeasurement {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        amount: Option<String>,
    }
    #[derive(Deserialize, Debug)]
    struct Bad {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        measurements: Vec<BadMeasurement>,
    }
    let error = row_error::<Bad>(&nested(), 0);
    assert_eq!(
        (error.row, error.path.as_str(), error.kind),
        (
            Some(0),
            "measurements[0].amount",
            BigQueryCodecErrorKind::TypeMismatch
        )
    );

    #[derive(Deserialize, Debug)]
    struct LineItemBadTag {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        tags: Vec<i64>,
    }
    #[derive(Deserialize, Debug)]
    struct BadTag {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        line_item: Option<LineItemBadTag>,
    }
    let error = row_error::<BadTag>(&nested(), 2);
    assert_eq!(
        (error.row, error.path.as_str()),
        (Some(2), "line_item.tags[0]")
    );
}

#[test]
fn unnamed_columns_are_never_visited() {
    let batch = batch(vec![
        column("id", Int64Array::from(vec![1, 2])),
        column("odd", Float32Array::from(vec![1.0, 2.0])),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Id {
        id: i64,
    }
    assert_eq!(rows::<Id>(&batch), vec![Id { id: 1 }, Id { id: 2 }]);
}

#[test]
fn unknown_arrow_type_fails_only_rows_that_name_it() {
    let line_item = struct_column(
        "line_item",
        vec![column("odd", Float32Array::from(vec![1.0, 2.0]))],
        Some(vec![false, true]),
    );
    let batch = batch(vec![column("id", Int64Array::from(vec![1, 2])), line_item]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Odd {
        odd: f32,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Order {
        id: i64,
        line_item: Option<Odd>,
    }
    assert_eq!(
        row::<Order>(&batch, 0),
        Ok(Order {
            id: 1,
            line_item: None
        })
    );
    let error = row_error::<Order>(&batch, 1);
    assert_eq!(error.kind, BigQueryCodecErrorKind::UnsupportedType);
    assert_eq!(error.path, "line_item.odd");
}

#[test]
fn the_field_plan_is_built_once_per_batch() {
    let batch = nested();
    let decoder = BatchDecoder::new(&batch);
    for index in 0..3 {
        decoder
            .row::<NestedRow>(index)
            .unwrap_or_else(|error| panic!("row {index}: {error}"));
    }
    // One plan for the row, one each for line_item, line_item.inner and the measurements element.
    assert_eq!(decoder.plans_built(), 4);
}

#[test]
fn bignumeric_numeric_and_interval_alternate_targets() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Alt {
        total: f64,
        big_total: f64,
        lead_time: String,
        total_decimal: BigQueryDecimal<String>,
    }
    let scalars = all_scalars();
    let mut columns: Vec<(Field, ArrayRef)> = scalars
        .schema()
        .fields()
        .iter()
        .zip(scalars.columns())
        .map(|(field, array)| (field.as_ref().clone(), array.clone()))
        .collect();
    columns.push(column(
        "total_decimal",
        Decimal128Array::from(vec![1_500_000_000i128])
            .with_precision_and_scale(38, 9)
            .expect("NUMERIC's Arrow type"),
    ));
    let got: Alt = rows(&batch(columns)).remove(0);
    assert_eq!(got.total, 1e29);
    assert_eq!(
        got.big_total,
        "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"
            .parse::<f64>()
            .expect("valid")
    );
    assert_eq!(got.lead_time, "1-2 -3 4:5:6.000789");
    assert_eq!(got.total_decimal, BigQueryDecimal("1.5".to_string()));
}

#[test]
fn timestamp_forms_cover_the_full_bigquery_range() {
    let max = civil::TIMESTAMP_MAX_MICROS;
    let batch = batch(vec![column(
        "placed_at",
        TimestampMicrosecondArray::from(vec![max, -1]).with_timezone("UTC"),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct TextTimestamp {
        placed_at: String,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct IntegerTimestamp {
        placed_at: i64,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct JiffTimestamp {
        placed_at: jiff::Timestamp,
    }
    assert_eq!(
        row::<TextTimestamp>(&batch, 0).map(|row| row.placed_at),
        Ok("9999-12-31T23:59:59.999999Z".to_string())
    );
    assert_eq!(
        row::<IntegerTimestamp>(&batch, 0).map(|row| row.placed_at),
        Ok(max)
    );
    assert_eq!(
        row::<JiffTimestamp>(&batch, 1).map(|row| row.placed_at),
        Ok("1969-12-31T23:59:59.999999Z".parse().expect("valid"))
    );
}

#[test]
fn timestamp_above_jiff_max_fails_only_that_row() {
    let batch = batch(vec![column(
        "placed_at",
        TimestampMicrosecondArray::from(vec![civil::TIMESTAMP_MAX_MICROS, 0]).with_timezone("UTC"),
    )]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct JiffTimestamp {
        placed_at: jiff::Timestamp,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct WrappedTimestamp {
        placed_at: BigQueryTimestamp,
    }
    let error = row_error::<JiffTimestamp>(&batch, 0);
    assert_eq!(
        (error.row, error.path.as_str(), error.kind),
        (Some(0), "placed_at", BigQueryCodecErrorKind::OutOfRange)
    );
    assert_eq!(
        row::<JiffTimestamp>(&batch, 1),
        Ok(JiffTimestamp {
            placed_at: jiff::Timestamp::UNIX_EPOCH
        })
    );
    assert_eq!(
        row_error::<WrappedTimestamp>(&batch, 0).kind,
        BigQueryCodecErrorKind::OutOfRange
    );
    assert_eq!(
        row::<WrappedTimestamp>(&batch, 1),
        Ok(WrappedTimestamp {
            placed_at: BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH)
        })
    );
}

#[test]
fn borrowed_str_and_bytes_live_as_long_as_the_batch() {
    let batch = batch(vec![
        column("name", StringArray::from(vec!["abc"])),
        column("payload", BinaryArray::from_vec(vec![&b"\x01\x02"[..]])),
    ]);
    #[derive(Deserialize)]
    struct Borrowed<'a> {
        name: &'a str,
        payload: &'a [u8],
    }
    let decoder = BatchDecoder::new(&batch);
    let borrowed: Borrowed = decoder.row(0).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!((borrowed.name, borrowed.payload), ("abc", &[1u8, 2][..]));
}

#[test]
fn dynamic_rows_through_deserialize_any() {
    let batch = nested();
    assert_eq!(
        row::<serde_json::Value>(&batch, 1),
        Ok(serde_json::json!({"id": 1, "line_item": null, "measurements": []}))
    );
    let value = row::<serde_json::Value>(&batch, 0).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(value["line_item"]["inner"]["delivered_on"], "2024-02-29");
    assert_eq!(value["measurements"][1]["amount"], 2.0);
}

#[test]
fn temporal_wrappers_decode_from_integers() {
    let day = 19_782; // 2024-02-29
    let at = timestamp_micros("2024-02-29T12:34:56.789012Z");
    let (local_field, local) = column("local", TimestampMicrosecondArray::from(vec![at]));
    let batch = batch(vec![
        column("day", Date32Array::from(vec![day])),
        column(
            "time_of_day",
            Time64MicrosecondArray::from(vec![45_296_000_001]),
        ),
        (
            local_field.with_metadata(extension_metadata("google:sqlType:datetime")),
            local,
        ),
        column(
            "at",
            TimestampMicrosecondArray::from(vec![at]).with_timezone("UTC"),
        ),
        column("no_day", Date32Array::from(vec![None::<i32>])),
        list(
            "seen_at",
            vec![0, 2],
            Arc::new(TimestampMicrosecondArray::from(vec![0, at]).with_timezone("UTC")),
        ),
        column("day_with", Date32Array::from(vec![day])),
        column(
            "maybe_at",
            TimestampMicrosecondArray::from(vec![at]).with_timezone("UTC"),
        ),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct TemporalWrappers {
        day: BigQueryDate,
        time_of_day: BigQueryTime,
        local: BigQueryDateTime,
        at: BigQueryTimestamp,
        no_day: Option<BigQueryDate>,
        seen_at: Vec<BigQueryTimestamp>,
        #[serde(with = "crate::serialize_as_date")]
        day_with: jiff::civil::Date,
        #[serde(with = "crate::serialize_as_optional_timestamp")]
        maybe_at: Option<jiff::Timestamp>,
    }
    let timestamp: jiff::Timestamp = "2024-02-29T12:34:56.789012Z".parse().expect("valid");
    assert_eq!(
        rows::<TemporalWrappers>(&batch),
        vec![TemporalWrappers {
            day: BigQueryDate(jiff::civil::date(2024, 2, 29)),
            time_of_day: BigQueryTime(jiff::civil::time(12, 34, 56, 1_000)),
            local: BigQueryDateTime("2024-02-29T12:34:56.789012".parse().expect("valid")),
            at: BigQueryTimestamp(timestamp),
            no_day: None,
            seen_at: vec![
                BigQueryTimestamp(jiff::Timestamp::UNIX_EPOCH),
                BigQueryTimestamp(timestamp)
            ],
            day_with: jiff::civil::date(2024, 2, 29),
            maybe_at: Some(timestamp),
        }]
    );
}

#[test]
fn temporal_wrapper_on_another_temporal_column_is_type_mismatch() {
    let (local_field, local_array) = column("local", TimestampMicrosecondArray::from(vec![0]));
    let batch = batch(vec![
        column(
            "at",
            TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC"),
        ),
        (
            local_field.with_metadata(extension_metadata("google:sqlType:datetime")),
            local_array,
        ),
    ]);
    #[derive(Deserialize, Debug)]
    struct DateOnTimestamp {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        at: BigQueryDate,
    }
    #[derive(Deserialize, Debug)]
    struct TimestampOnDateTime {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        local: BigQueryTimestamp,
    }
    let error = row_error::<DateOnTimestamp>(&batch, 0);
    assert_eq!(
        (error.path.as_str(), error.kind),
        ("at", BigQueryCodecErrorKind::TypeMismatch)
    );
    let error = row_error::<TimestampOnDateTime>(&batch, 0);
    assert_eq!(
        (error.path.as_str(), error.kind),
        ("local", BigQueryCodecErrorKind::TypeMismatch)
    );
}

#[test]
fn numeric_reads_into_integers_only_when_whole() {
    let numeric = Decimal128Array::from(vec![5_000_000_000i128, 1_500_000_000, -2_000_000_000])
        .with_precision_and_scale(38, 9)
        .expect("NUMERIC's Arrow type");
    let ten_pow_38 = i256::from_i128(10).wrapping_pow(38);
    let big = Decimal256Array::from(vec![
        i256::from_i128(i128::MAX).wrapping_mul(ten_pow_38),
        ten_pow_38,
        ten_pow_38,
    ])
    .with_precision_and_scale(76, 38)
    .expect("BIGNUMERIC's Arrow type");
    let batch = batch(vec![column("total", numeric), column("big_total", big)]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct WholeNumeric {
        total: i64,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct WholeBigNumeric {
        big_total: i128,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Narrow {
        total: u8,
    }
    assert_eq!(
        row::<WholeNumeric>(&batch, 0),
        Ok(WholeNumeric { total: 5 })
    );
    let error = row_error::<WholeNumeric>(&batch, 1);
    assert_eq!(
        (error.path.as_str(), error.kind),
        ("total", BigQueryCodecErrorKind::OutOfRange)
    );
    assert_eq!(
        row::<WholeNumeric>(&batch, 2),
        Ok(WholeNumeric { total: -2 })
    );
    assert_eq!(
        row::<WholeBigNumeric>(&batch, 0),
        Ok(WholeBigNumeric {
            big_total: i128::MAX
        })
    );
    assert_eq!(
        row::<WholeBigNumeric>(&batch, 1),
        Ok(WholeBigNumeric { big_total: 1 })
    );
    assert_eq!(
        row_error::<Narrow>(&batch, 2).kind,
        BigQueryCodecErrorKind::OutOfRange
    );
}

/// BigQuery sends an unbounded bound of a REQUIRED RANGE as the epoch, with no validity for it.
/// A pair that breaks `start < end` can only hold that sentinel, so the epoch side is unbounded,
/// for every element type and column mode.
#[test]
fn range_epoch_bound_that_breaks_start_before_end_is_unbounded() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Booking {
        booked: BigQueryRange<i64>,
        history: Vec<BigQueryRange<i64>>,
    }
    let unbounded_end = BigQueryRange {
        start: Some(10),
        end: None,
    };
    let unbounded_start = BigQueryRange {
        start: None,
        end: Some(-10),
    };
    let unbounded = BigQueryRange {
        start: None,
        end: None,
    };
    for element in [
        BigQueryRangeElementType::Date,
        BigQueryRangeElementType::DateTime,
        BigQueryRangeElementType::Timestamp,
    ] {
        let ranges = || {
            StructArray::new(
                Fields::from(vec![
                    Field::new("start", range_child_type(element), true),
                    Field::new("end", range_child_type(element), true),
                ]),
                vec![
                    range_child(element, vec![Some(10), Some(0), Some(0)]),
                    range_child(element, vec![Some(0), Some(-10), Some(0)]),
                ],
                None,
            )
        };
        let required = ranges();
        let batch = batch(vec![
            (
                Field::new("booked", required.data_type().clone(), false)
                    .with_metadata(range_metadata()),
                Arc::new(required) as ArrayRef,
            ),
            list_with(
                "history",
                vec![0, 3, 3, 3],
                Arc::new(ranges()),
                range_metadata(),
            ),
        ]);
        let booked: Vec<_> = (0..3)
            .map(|index| row::<Booking>(&batch, index).map(|booking| booking.booked))
            .collect();
        assert_eq!(
            booked,
            [Ok(unbounded_end), Ok(unbounded_start), Ok(unbounded)],
            "{element:?}"
        );
        assert_eq!(
            row::<Booking>(&batch, 0).map(|booking| booking.history),
            Ok(vec![unbounded_end, unbounded_start, unbounded]),
            "{element:?}"
        );
    }
}

/// An unbounded start before a real end after the epoch keeps `start < end`, so the sentinel
/// cannot be told from a real epoch start and stays one. A NULLABLE RANGE carries validity for
/// its ends and reads an unbounded start as `None`.
#[test]
fn range_epoch_start_before_a_later_end_stays_the_epoch() {
    let children = |nulls: Option<Vec<bool>>| {
        let start = Date32Array::new(vec![0, 10].into(), nulls.clone().map(NullBuffer::from));
        let end = Date32Array::new(vec![5, 0].into(), None);
        StructArray::new(
            Fields::from(vec![
                Field::new("start", DataType::Date32, true),
                Field::new("end", DataType::Date32, true),
            ]),
            vec![Arc::new(start) as ArrayRef, Arc::new(end)],
            None,
        )
    };
    let required = children(None);
    let nullable = children(Some(vec![false, true]));
    let batch = batch(vec![
        (
            Field::new("booked", required.data_type().clone(), false)
                .with_metadata(range_metadata()),
            Arc::new(required) as ArrayRef,
        ),
        (
            Field::new("tentative", nullable.data_type().clone(), true)
                .with_metadata(range_metadata()),
            Arc::new(nullable),
        ),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Booking {
        booked: BigQueryRange<jiff::civil::Date>,
        tentative: Option<BigQueryRange<jiff::civil::Date>>,
    }
    let epoch = jiff::civil::date(1970, 1, 1);
    assert_eq!(
        row::<Booking>(&batch, 0),
        Ok(Booking {
            booked: BigQueryRange {
                start: Some(epoch),
                end: Some(jiff::civil::date(1970, 1, 6))
            },
            tentative: Some(BigQueryRange {
                start: None,
                end: Some(jiff::civil::date(1970, 1, 6))
            }),
        })
    );
}

#[test]
fn json_column_parses_into_any_shape_but_a_string() {
    let (field, array) = column(
        "attributes",
        StringArray::from(vec![
            Some(r#"{"quantity":1,"tags":["gift"]}"#),
            Some("null"),
            None,
        ]),
    );
    let (list_field, list_array) = list_with(
        "attribute_list",
        vec![0, 2, 2, 2],
        Arc::new(StringArray::from(vec![
            r#"{"quantity":2,"tags":[]}"#,
            "[1,2]",
        ])),
        extension_metadata("google:sqlType:json"),
    );
    let batch = batch(vec![
        (
            field.with_metadata(extension_metadata("google:sqlType:json")),
            array,
        ),
        (list_field, list_array),
    ]);

    #[derive(Deserialize, Debug, PartialEq)]
    struct Doc {
        quantity: i64,
        tags: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Typed {
        attributes: Option<Doc>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Values {
        attributes: Option<serde_json::Value>,
        attribute_list: Vec<serde_json::Value>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Raw {
        attributes: Option<String>,
        attribute_list: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Maps {
        attributes: HashMap<String, serde_json::Value>,
    }

    assert_eq!(
        row::<Typed>(&batch, 0),
        Ok(Typed {
            attributes: Some(Doc {
                quantity: 1,
                tags: vec!["gift".into()]
            })
        })
    );
    assert_eq!(
        row::<Values>(&batch, 0),
        Ok(Values {
            attributes: Some(serde_json::json!({"quantity": 1, "tags": ["gift"]})),
            attribute_list: vec![
                serde_json::json!({"quantity": 2, "tags": []}),
                serde_json::json!([1, 2])
            ],
        })
    );
    assert_eq!(
        row::<Values>(&batch, 1),
        Ok(Values {
            attributes: Some(serde_json::Value::Null),
            attribute_list: vec![]
        }),
        "JSON null stays apart from SQL NULL"
    );
    assert_eq!(
        row::<Values>(&batch, 2),
        Ok(Values {
            attributes: None,
            attribute_list: vec![]
        })
    );
    assert_eq!(
        row::<Raw>(&batch, 0),
        Ok(Raw {
            attributes: Some(r#"{"quantity":1,"tags":["gift"]}"#.into()),
            attribute_list: vec![r#"{"quantity":2,"tags":[]}"#.into(), "[1,2]".into()],
        }),
        "a String gets the JSON text as it is"
    );
    assert_eq!(
        row::<Maps>(&batch, 0).map(|maps| maps.attributes.len()),
        Ok(2)
    );
}

#[derive(Deserialize, Debug, PartialEq)]
struct NullDoc {
    quantity: i64,
}

/// A JSON column `attributes` holding `{"quantity":1}`, JSON `null` and SQL NULL, in that order.
fn json_nulls() -> RecordBatch {
    let (field, array) = column(
        "attributes",
        StringArray::from(vec![Some(r#"{"quantity":1}"#), Some("null"), None]),
    );
    batch(vec![(
        field.with_metadata(extension_metadata("google:sqlType:json")),
        array,
    )])
}

#[test]
fn json_null_into_an_option_of_a_typed_target_is_none() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Typed {
        attributes: Option<NullDoc>,
    }
    let batch = json_nulls();
    assert_eq!(
        row::<Typed>(&batch, 0),
        Ok(Typed {
            attributes: Some(NullDoc { quantity: 1 })
        })
    );
    assert_eq!(row::<Typed>(&batch, 1), Ok(Typed { attributes: None }));
    assert_eq!(row::<Typed>(&batch, 2), Ok(Typed { attributes: None }));
    #[derive(Deserialize, Debug, PartialEq)]
    struct Int {
        attributes: Option<i64>,
    }
    assert_eq!(row::<Int>(&batch, 1), Ok(Int { attributes: None }));
}

#[test]
fn json_null_into_an_option_of_a_value_stays_apart_from_sql_null() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Values {
        attributes: Option<serde_json::Value>,
    }
    let batch = json_nulls();
    assert_eq!(
        row::<Values>(&batch, 1),
        Ok(Values {
            attributes: Some(serde_json::Value::Null)
        })
    );
    assert_eq!(row::<Values>(&batch, 2), Ok(Values { attributes: None }));
    #[derive(Deserialize, Debug, PartialEq)]
    struct Raw {
        attributes: Option<String>,
    }
    assert_eq!(
        row::<Raw>(&batch, 1),
        Ok(Raw {
            attributes: Some("null".into())
        }),
        "a String is the JSON text, and `null` is text"
    );
}

#[test]
fn json_null_into_a_bare_typed_target_is_an_error() {
    #[derive(Deserialize, Debug)]
    struct Bare {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        attributes: NullDoc,
    }
    let error = row_error::<Bare>(&json_nulls(), 1);
    assert_eq!(
        (error.path.as_str(), error.kind),
        ("attributes", BigQueryCodecErrorKind::Custom)
    );
}

#[test]
fn json_null_into_an_option_of_the_wrapper_follows_its_inner_type() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Wrapped {
        attributes: Option<BigQueryJson<serde_json::Value>>,
        #[serde(rename = "typed_attributes")]
        typed: Option<BigQueryJson<NullDoc>>,
        #[serde(rename = "attributes_with", with = "crate::serialize_as_optional_json")]
        with: Option<NullDoc>,
    }
    let (json_field, json_array) =
        column("attributes", StringArray::from(vec![Some("null"), None]));
    let (typed_field, typed_array) = column(
        "typed_attributes",
        StringArray::from(vec![Some("null"), None]),
    );
    let (with_field, with_array) = column(
        "attributes_with",
        StringArray::from(vec![Some("null"), None]),
    );
    let batch = batch(vec![
        (
            json_field.with_metadata(extension_metadata("google:sqlType:json")),
            json_array,
        ),
        (
            typed_field.with_metadata(extension_metadata("google:sqlType:json")),
            typed_array,
        ),
        (
            with_field.with_metadata(extension_metadata("google:sqlType:json")),
            with_array,
        ),
    ]);
    assert_eq!(
        row::<Wrapped>(&batch, 0),
        Ok(Wrapped {
            attributes: Some(BigQueryJson(serde_json::Value::Null)),
            typed: None,
            with: None,
        })
    );
    assert_eq!(
        row::<Wrapped>(&batch, 1),
        Ok(Wrapped {
            attributes: None,
            typed: None,
            with: None
        })
    );
}

#[test]
fn json_null_elements_of_an_array_into_options_are_none() {
    let (list_field, list_array) = list_with(
        "attribute_list",
        vec![0, 3],
        Arc::new(StringArray::from(vec!["null", r#"{"quantity":2}"#, "null"])),
        extension_metadata("google:sqlType:json"),
    );
    #[derive(Deserialize, Debug, PartialEq)]
    struct Docs {
        attribute_list: Vec<Option<NullDoc>>,
    }
    assert_eq!(
        row::<Docs>(&batch(vec![(list_field, list_array)]), 0),
        Ok(Docs {
            attribute_list: vec![None, Some(NullDoc { quantity: 2 }), None]
        })
    );
}

#[test]
fn bytes_column_into_json_value_is_an_error() {
    let batch = batch(vec![column(
        "payload",
        BinaryArray::from_vec(vec![&b"\x01"[..]]),
    )]);
    #[derive(Deserialize, Debug)]
    struct JsonPayload {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        payload: serde_json::Value,
    }
    let error = row_error::<JsonPayload>(&batch, 0);
    assert_eq!(
        (error.path.as_str(), error.kind),
        ("payload", BigQueryCodecErrorKind::TypeMismatch)
    );
}

// Field keys: one test per row of the serde attribute table, under the default strategy, on
// a batch of `id, user_name, camelField, extra`.

fn full_batch() -> RecordBatch {
    batch(vec![
        required("id", Int64Array::from(vec![1, 2])),
        required("user_name", StringArray::from(vec!["ann", "bob"])),
        required("camelField", Int64Array::from(vec![10, 20])),
        required("extra", StringArray::from(vec!["e1", "e2"])),
    ])
}

#[test]
fn rename_matches_the_renamed_column() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Rename {
        id: i64,
        #[serde(rename = "user_name")]
        name: String,
    }
    assert_eq!(
        rows::<Rename>(&full_batch()),
        [
            Rename {
                id: 1,
                name: "ann".to_string()
            },
            Rename {
                id: 2,
                name: "bob".to_string()
            }
        ]
    );
}

#[test]
fn rename_all_matches_camel_case_columns() {
    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct RenameAll {
        id: i64,
        camel_field: i64,
    }
    assert_eq!(
        rows::<RenameAll>(&full_batch()),
        [
            RenameAll {
                id: 1,
                camel_field: 10
            },
            RenameAll {
                id: 2,
                camel_field: 20
            }
        ]
    );
}

#[test]
fn alias_matches_the_alias_column() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Alias {
        id: i64,
        #[serde(alias = "user_name")]
        name: String,
    }
    assert_eq!(
        rows::<Alias>(&full_batch()),
        [
            Alias {
                id: 1,
                name: "ann".to_string()
            },
            Alias {
                id: 2,
                name: "bob".to_string()
            }
        ]
    );
}

#[test]
fn skipped_field_is_left_at_its_default() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Skip {
        id: i64,
        #[serde(skip)]
        cache: Option<String>,
        extra: String,
    }
    assert_eq!(
        rows::<Skip>(&full_batch()),
        [
            Skip {
                id: 1,
                cache: None,
                extra: "e1".to_string()
            },
            Skip {
                id: 2,
                cache: None,
                extra: "e2".to_string()
            }
        ]
    );
}

#[test]
fn default_field_without_column_takes_its_default() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct WithDefault {
        id: i64,
        #[serde(default)]
        missing: i64,
        #[serde(default)]
        tags: Vec<String>,
        extra: String,
    }
    assert_eq!(
        rows::<WithDefault>(&full_batch()),
        [
            WithDefault {
                id: 1,
                missing: 0,
                tags: vec![],
                extra: "e1".to_string()
            },
            WithDefault {
                id: 2,
                missing: 0,
                tags: vec![],
                extra: "e2".to_string()
            }
        ]
    );
}

#[test]
fn option_field_without_column_is_none() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct OptionMissing {
        id: i64,
        not_a_column: Option<i64>,
    }
    assert_eq!(
        rows::<OptionMissing>(&full_batch()),
        [
            OptionMissing {
                id: 1,
                not_a_column: None
            },
            OptionMissing {
                id: 2,
                not_a_column: None
            }
        ]
    );
}

#[test]
fn required_field_without_column_is_a_missing_field_error() {
    #[derive(Deserialize, Debug)]
    struct RequiredMissing {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        id: i64,
        #[allow(dead_code, reason = "decoded only to see it fail")]
        not_a_column: i64,
    }
    let error = row_error::<RequiredMissing>(&full_batch(), 0);
    assert_eq!(error.kind, BigQueryCodecErrorKind::Custom);
    assert!(
        error.message.contains("missing field `not_a_column`"),
        "{}",
        error.message
    );
}

#[test]
fn flatten_collects_the_remaining_columns() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct FlatRest {
        user_name: String,
        extra: String,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Flat {
        id: i64,
        #[serde(flatten)]
        rest: FlatRest,
    }
    assert_eq!(
        rows::<Flat>(&full_batch()),
        [
            Flat {
                id: 1,
                rest: FlatRest {
                    user_name: "ann".to_string(),
                    extra: "e1".to_string()
                }
            },
            Flat {
                id: 2,
                rest: FlatRest {
                    user_name: "bob".to_string(),
                    extra: "e2".to_string()
                }
            }
        ]
    );
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
struct Deny {
    id: i64,
}

#[test]
fn deny_unknown_fields_rejects_extra_columns_in_every_row() {
    let batch = full_batch();
    for index in 0..2 {
        let error = row_error::<Deny>(&batch, index);
        assert!(
            error.message.contains("unknown field"),
            "row {index}: {}",
            error.message
        );
    }
}

#[test]
fn deny_unknown_fields_accepts_exact_columns() {
    let batch = batch(vec![required("id", Int64Array::from(vec![1, 2]))]);
    assert_eq!(rows::<Deny>(&batch), [Deny { id: 1 }, Deny { id: 2 }]);
}

#[test]
fn fields_in_another_order_than_columns_decode() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Reordered {
        extra: String,
        id: i64,
    }
    assert_eq!(
        rows::<Reordered>(&full_batch()),
        [
            Reordered {
                extra: "e1".to_string(),
                id: 1
            },
            Reordered {
                extra: "e2".to_string(),
                id: 2
            }
        ]
    );
}

// The alias probe.

/// `id, name, user_name`: a struct aliasing `name` as `user_name` finds both columns.
fn alias_batch() -> RecordBatch {
    batch(vec![
        required("id", Int64Array::from(vec![1, 2])),
        required("name", StringArray::from(vec!["ann", "bob"])),
        required("user_name", StringArray::from(vec!["ANN", "BOB"])),
    ])
}

#[test]
fn alias_with_both_columns_is_a_duplicate_field_error() {
    #[derive(Deserialize, Debug)]
    struct Alias {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        id: i64,
        #[allow(dead_code, reason = "decoded only to see it fail")]
        #[serde(alias = "user_name")]
        name: String,
    }
    let batch = alias_batch();
    for index in 0..2 {
        let error = row_error::<Alias>(&batch, index);
        assert!(
            error.message.contains("duplicate field"),
            "row {index}: {}",
            error.message
        );
    }
}

#[test]
fn alias_with_deny_unknown_fields_is_a_duplicate_field_error() {
    #[derive(Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    struct Alias {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        id: i64,
        #[allow(dead_code, reason = "decoded only to see it fail")]
        #[serde(alias = "user_name")]
        name: String,
    }
    let error = row_error::<Alias>(&alias_batch(), 0);
    assert!(
        error.message.contains("duplicate field"),
        "{}",
        error.message
    );
}

#[test]
fn alias_on_the_first_field_with_every_name_a_column_is_an_error() {
    let batch = batch(vec![
        required("quantity", Int64Array::from(vec![1])),
        required("amount", Int64Array::from(vec![2])),
        required("total", Int64Array::from(vec![3])),
    ]);
    #[derive(Deserialize, Debug)]
    struct First {
        #[allow(dead_code, reason = "decoded only to see it fail")]
        #[serde(alias = "amount")]
        quantity: i64,
        #[allow(dead_code, reason = "decoded only to see it fail")]
        total: i64,
    }
    let error = row_error::<First>(&batch, 0);
    assert!(
        error.message.contains("duplicate field"),
        "{}",
        error.message
    );
}

#[test]
fn alias_with_only_the_alias_column_decodes() {
    let batch = batch(vec![
        required("id", Int64Array::from(vec![1])),
        required("user_name", StringArray::from(vec!["ann"])),
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Alias {
        id: i64,
        #[serde(alias = "user_name")]
        name: String,
    }
    assert_eq!(
        rows::<Alias>(&batch),
        [Alias {
            id: 1,
            name: "ann".to_string()
        }]
    );
}

#[test]
fn alias_and_default_decode_by_name() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct AliasDefault {
        #[serde(alias = "user_name", default)]
        customer: String,
        extra: String,
        #[serde(default)]
        other: String,
    }
    assert_eq!(
        rows::<AliasDefault>(&full_batch()),
        [
            AliasDefault {
                customer: "ann".to_string(),
                extra: "e1".to_string(),
                other: "".to_string()
            },
            AliasDefault {
                customer: "bob".to_string(),
                extra: "e2".to_string(),
                other: "".to_string()
            }
        ]
    );
}

#[test]
fn plain_struct_keeps_index_keys() {
    let batch = all_scalars();
    let decoder = BatchDecoder::new(&batch);
    decoder
        .row::<Scalars>(0)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(decoder.name_key_plans(), 0);
}

#[test]
fn trailing_skip_is_not_an_alias() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct TrailingSkip {
        id: i64,
        extra: String,
        #[serde(skip)]
        cache: Option<String>,
    }
    let batch = batch(vec![
        required("id", Int64Array::from(vec![1])),
        required("extra", StringArray::from(vec!["e1"])),
    ]);
    let decoder = BatchDecoder::new(&batch);
    assert_eq!(
        decoder
            .row::<TrailingSkip>(0)
            .map_err(|error| error.to_string()),
        Ok(TrailingSkip {
            id: 1,
            extra: "e1".to_string(),
            cache: None
        })
    );
    assert_eq!(decoder.name_key_plans(), 0);
}

#[derive(Deserialize, Debug, PartialEq)]
struct Tail {
    id: i64,
    rest: IgnoredAny,
}

fn tail_batch() -> RecordBatch {
    batch(vec![
        required("id", Int64Array::from(vec![7])),
        required("rest", StringArray::from(vec!["ignored"])),
    ])
}

#[test]
fn ignored_any_field_falls_back_to_string_keys() {
    let batch = tail_batch();
    let decoder = BatchDecoder::new(&batch);
    assert_eq!(
        decoder.row::<Tail>(0).map_err(|error| error.to_string()),
        Ok(Tail {
            id: 7,
            rest: IgnoredAny
        })
    );
    assert_eq!(decoder.name_key_plans(), 1);
}

#[test]
fn swallowed_probe_error_still_redoes_the_row() {
    fn lenient<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Tail>, D::Error> {
        Ok(Option::<Tail>::deserialize(deserializer).unwrap_or(None))
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Outer {
        #[serde(deserialize_with = "lenient")]
        line_item: Option<Tail>,
    }
    let tail = tail_batch();
    let line_item = StructArray::from(tail);
    let batch = batch(vec![column("line_item", line_item)]);
    assert_eq!(
        row::<Outer>(&batch, 0),
        Ok(Outer {
            line_item: Some(Tail {
                id: 7,
                rest: IgnoredAny
            })
        })
    );
}

// The round trip of every type and mode: canonical values built into the Arrow arrays
// BigQuery sends, decoded into each Rust target of the type, and mapped back.

fn optional_values<T: Copy>(
    values: &[Option<Canonical>],
    convert: impl Fn(&Canonical) -> T,
) -> Vec<Option<T>> {
    values
        .iter()
        .map(|value| value.as_ref().map(&convert))
        .collect()
}

fn range_child_type(element: BigQueryRangeElementType) -> DataType {
    match element {
        BigQueryRangeElementType::Date => DataType::Date32,
        BigQueryRangeElementType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
        BigQueryRangeElementType::Timestamp => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
    }
}

fn range_child(element: BigQueryRangeElementType, values: Vec<Option<i64>>) -> ArrayRef {
    match element {
        BigQueryRangeElementType::Date => Arc::new(Date32Array::from(
            values
                .into_iter()
                .map(|value| value.map(|days| i32::try_from(days).expect("DATE days fit i32")))
                .collect::<Vec<_>>(),
        )),
        BigQueryRangeElementType::DateTime => Arc::new(TimestampMicrosecondArray::from(values)),
        BigQueryRangeElementType::Timestamp => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
        }
    }
}

/// The values of one column of `kind`, as BigQuery sends them: the Arrow array and the field
/// metadata of the type.
fn arrow_values(
    kind: FieldKind,
    values: &[Option<Canonical>],
) -> (ArrayRef, HashMap<String, String>) {
    let none = HashMap::new();
    let unexpected = |value: &Canonical| -> ! { panic!("{kind:?} column got {value:?}") };
    match kind {
        FieldKind::Int64 => (
            Arc::new(Int64Array::from(optional_values(
                values,
                |value| match value {
                    Canonical::Int64(inner) => *inner,
                    value => unexpected(value),
                },
            ))),
            none,
        ),
        FieldKind::Float64 => (
            Arc::new(Float64Array::from(optional_values(
                values,
                |value| match value {
                    Canonical::Float64(bits) => f64::from_bits(*bits),
                    value => unexpected(value),
                },
            ))),
            none,
        ),
        FieldKind::Bool => (
            Arc::new(BooleanArray::from(optional_values(
                values,
                |value| match value {
                    Canonical::Bool(inner) => *inner,
                    value => unexpected(value),
                },
            ))),
            none,
        ),
        FieldKind::String | FieldKind::Geography | FieldKind::Json => {
            let text: Vec<Option<String>> = values
                .iter()
                .map(|value| {
                    value.as_ref().map(|value| match value {
                        Canonical::String(text) | Canonical::Geography(text) => text.clone(),
                        Canonical::Json(json) => json.to_string(),
                        value => unexpected(value),
                    })
                })
                .collect();
            let field_metadata = match kind {
                FieldKind::Json => extension_metadata("google:sqlType:json"),
                FieldKind::Geography => {
                    let mut metadata = extension_metadata("google:sqlType:geography");
                    metadata.insert(
                        "ARROW:extension:metadata".into(),
                        r#"{"encoding": "WKT"}"#.into(),
                    );
                    metadata
                }
                _ => none,
            };
            (Arc::new(StringArray::from(text)), field_metadata)
        }
        FieldKind::Bytes => {
            let bytes: Vec<Option<&[u8]>> = values
                .iter()
                .map(|value| {
                    value.as_ref().map(|value| match value {
                        Canonical::Bytes(bytes) => bytes.as_slice(),
                        value => unexpected(value),
                    })
                })
                .collect();
            (Arc::new(BinaryArray::from_opt_vec(bytes)), none)
        }
        FieldKind::Date => (
            Arc::new(Date32Array::from(optional_values(
                values,
                |value| match value {
                    Canonical::Date(days) => *days,
                    value => unexpected(value),
                },
            ))),
            none,
        ),
        FieldKind::Time => (
            Arc::new(Time64MicrosecondArray::from(optional_values(
                values,
                |value| match value {
                    Canonical::Time(micros) => *micros,
                    value => unexpected(value),
                },
            ))),
            none,
        ),
        FieldKind::DateTime => (
            Arc::new(TimestampMicrosecondArray::from(optional_values(
                values,
                |value| match value {
                    Canonical::DateTime(micros) => *micros,
                    value => unexpected(value),
                },
            ))),
            extension_metadata("google:sqlType:datetime"),
        ),
        FieldKind::Timestamp => (
            Arc::new(
                TimestampMicrosecondArray::from(optional_values(values, |value| match value {
                    Canonical::Timestamp(micros) => *micros,
                    value => unexpected(value),
                }))
                .with_timezone("UTC"),
            ),
            none,
        ),
        FieldKind::Numeric => (
            Arc::new(
                Decimal128Array::from(optional_values(values, |value| match value {
                    Canonical::Numeric(inner) => *inner,
                    value => unexpected(value),
                }))
                .with_precision_and_scale(38, 9)
                .expect("NUMERIC's Arrow type"),
            ),
            none,
        ),
        FieldKind::BigNumeric => (
            Arc::new(
                Decimal256Array::from(optional_values(values, |value| match value {
                    Canonical::BigNumeric(inner) => *inner,
                    value => unexpected(value),
                }))
                .with_precision_and_scale(76, 38)
                .expect("BIGNUMERIC's Arrow type"),
            ),
            none,
        ),
        FieldKind::Interval => (
            Arc::new(IntervalMonthDayNanoArray::from(optional_values(
                values,
                |value| match value {
                    Canonical::Interval(interval) => {
                        IntervalMonthDayNano::new(interval.months, interval.days, interval.nanos)
                    }
                    value => unexpected(value),
                },
            ))),
            extension_metadata("google:sqlType:interval"),
        ),
        FieldKind::Range => {
            let element = values
                .iter()
                .flatten()
                .find_map(|value| match value {
                    Canonical::Range { element, .. } => Some(*element),
                    _ => None,
                })
                .unwrap_or(BigQueryRangeElementType::Date);
            let bound = |pick: fn(&Canonical) -> Option<i64>| -> Vec<Option<i64>> {
                values
                    .iter()
                    .map(|value| value.as_ref().and_then(pick))
                    .collect()
            };
            let start = bound(|value| match value {
                Canonical::Range { start, .. } => *start,
                _ => None,
            });
            let end = bound(|value| match value {
                Canonical::Range { end, .. } => *end,
                _ => None,
            });
            let fields = Fields::from(vec![
                Field::new("start", range_child_type(element), true),
                Field::new("end", range_child_type(element), true),
            ]);
            let nulls = NullBuffer::from(values.iter().map(Option::is_some).collect::<Vec<_>>());
            (
                Arc::new(StructArray::new(
                    fields,
                    vec![range_child(element, start), range_child(element, end)],
                    Some(nulls),
                )),
                range_metadata(),
            )
        }
        FieldKind::Struct => panic!("STRUCT columns are covered by the nested tests"),
    }
}

/// A batch of `nullable` (NULLABLE, every third row NULL), `required` (REQUIRED) and `repeated`
/// (REPEATED, up to three values from the row on) over `values`.
fn mode_batch(kind: FieldKind, values: &[Canonical]) -> RecordBatch {
    let nullable: Vec<Option<Canonical>> = values
        .iter()
        .enumerate()
        .map(|(index, value)| (index % 3 != 2).then(|| value.clone()))
        .collect();
    let all: Vec<Option<Canonical>> = values.iter().cloned().map(Some).collect();
    let mut offsets = vec![0i32];
    let mut flat = Vec::new();
    for index in 0..values.len() {
        flat.extend(
            values[index..(index + 3).min(values.len())]
                .iter()
                .cloned()
                .map(Some),
        );
        offsets.push(i32::try_from(flat.len()).expect("a small test list"));
    }
    let (nullable_array, nullable_metadata) = arrow_values(kind, &nullable);
    let (required_array, required_metadata) = arrow_values(kind, &all);
    let (repeated_array, repeated_metadata) = arrow_values(kind, &flat);
    batch(vec![
        (
            Field::new("nullable", nullable_array.data_type().clone(), true)
                .with_metadata(nullable_metadata),
            nullable_array,
        ),
        (
            Field::new("required", required_array.data_type().clone(), false)
                .with_metadata(required_metadata),
            required_array,
        ),
        list_with("repeated", offsets, repeated_array, repeated_metadata),
    ])
}

#[derive(Deserialize)]
struct ModeRow<X> {
    nullable: Option<X>,
    required: X,
    repeated: Vec<X>,
}

/// Decodes `values` in every mode into `X` and checks that `back` gives each value again.
fn check_target<X: DeserializeOwned>(
    kind: FieldKind,
    target: &str,
    values: &[Canonical],
    back: impl Fn(X) -> Canonical,
) -> Result<(), TestCaseError> {
    if values.is_empty() {
        return Ok(());
    }
    let batch = mode_batch(kind, values);
    for (index, decoded) in decode_rows::<ModeRow<X>>(&batch, 0).into_iter().enumerate() {
        let decoded = decoded.map_err(|error| {
            TestCaseError::fail(format!("{kind:?} into {target}, row {index}: {error}"))
        })?;
        let expected_nullable = (index % 3 != 2).then(|| values[index].clone());
        prop_assert_eq!(
            decoded.nullable.map(&back),
            expected_nullable,
            "{:?} nullable into {}",
            kind,
            target
        );
        prop_assert_eq!(
            back(decoded.required),
            values[index].clone(),
            "{:?} required into {}",
            kind,
            target
        );
        let repeated: Vec<Canonical> = decoded.repeated.into_iter().map(&back).collect();
        prop_assert_eq!(
            repeated,
            values[index..(index + 3).min(values.len())].to_vec(),
            "{:?} repeated into {}",
            kind,
            target
        );
    }
    Ok(())
}

fn below_jiff_max(values: &[Canonical]) -> Vec<Canonical> {
    let max = jiff::Timestamp::MAX.as_microsecond();
    values
        .iter()
        .filter(|value| match value {
            Canonical::Timestamp(micros) => *micros <= max,
            Canonical::Range { start, end, .. } => {
                start.is_none_or(|start| start <= max) && end.is_none_or(|end| end <= max)
            }
            _ => true,
        })
        .cloned()
        .collect()
}

fn checks(kind: FieldKind, values: &[Canonical]) -> Result<(), TestCaseError> {
    let ok = |result: Result<i64, CodecError>| result.unwrap_or_else(|error| panic!("{error}"));
    match kind {
        FieldKind::Int64 => {
            check_target(kind, "i64", values, Canonical::Int64)?;
            check_target(kind, "i128", values, |decoded: i128| {
                Canonical::Int64(i64::try_from(decoded).expect("an INT64 value"))
            })?;
        }
        FieldKind::Float64 => check_target(kind, "f64", values, |decoded: f64| {
            Canonical::Float64(decoded.to_bits())
        })?,
        FieldKind::Bool => check_target(kind, "bool", values, Canonical::Bool)?,
        FieldKind::String => {
            check_target(kind, "String", values, Canonical::String)?;
            check_target(kind, "Box<str>", values, |decoded: Box<str>| {
                Canonical::String(decoded.into())
            })?;
        }
        FieldKind::Bytes => {
            check_target(kind, "Vec<u8>", values, Canonical::Bytes)?;
            check_target(kind, "ByteBuf", values, |decoded: serde_bytes::ByteBuf| {
                Canonical::Bytes(decoded.into_vec())
            })?;
        }
        FieldKind::Date => {
            check_target(kind, "jiff Date", values, |date: jiff::civil::Date| {
                Canonical::Date(civil::date_days(date).expect("in range"))
            })?;
            check_target(kind, "BigQueryDate", values, |date: BigQueryDate| {
                Canonical::Date(civil::date_days(date.0).expect("in range"))
            })?;
            check_target(kind, "String", values, |text: String| {
                Canonical::Date(civil::parse_date(&text).expect("DATE text"))
            })?;
            check_target(kind, "i32", values, Canonical::Date)?;
        }
        FieldKind::Time => {
            check_target(kind, "jiff Time", values, |time: jiff::civil::Time| {
                Canonical::Time(civil::time_micros(time))
            })?;
            check_target(kind, "BigQueryTime", values, |time: BigQueryTime| {
                Canonical::Time(civil::time_micros(time.0))
            })?;
            check_target(kind, "String", values, |text: String| {
                Canonical::Time(ok(civil::parse_time(&text)))
            })?;
            check_target(kind, "i64", values, Canonical::Time)?;
        }
        FieldKind::DateTime => {
            check_target(
                kind,
                "jiff DateTime",
                values,
                |time: jiff::civil::DateTime| Canonical::DateTime(ok(civil::datetime_micros(time))),
            )?;
            check_target(
                kind,
                "BigQueryDateTime",
                values,
                |time: BigQueryDateTime| Canonical::DateTime(ok(civil::datetime_micros(time.0))),
            )?;
            check_target(kind, "String", values, |text: String| {
                Canonical::DateTime(ok(civil::parse_datetime(&text)))
            })?;
            check_target(kind, "i64", values, Canonical::DateTime)?;
        }
        FieldKind::Timestamp => {
            let safe = below_jiff_max(values);
            check_target(kind, "jiff Timestamp", &safe, |time: jiff::Timestamp| {
                Canonical::Timestamp(ok(civil::timestamp_micros(time)))
            })?;
            check_target(
                kind,
                "BigQueryTimestamp",
                &safe,
                |time: BigQueryTimestamp| Canonical::Timestamp(ok(civil::timestamp_micros(time.0))),
            )?;
            check_target(kind, "String", values, |text: String| {
                Canonical::Timestamp(ok(civil::parse_timestamp(&text)))
            })?;
            check_target(kind, "i64", values, Canonical::Timestamp)?;
        }
        FieldKind::Numeric => {
            let numeric = |text: &str| {
                Canonical::Numeric(
                    decimal::parse_numeric(text)
                        .expect("NUMERIC text")
                        .to_i128()
                        .expect("NUMERIC fits i128"),
                )
            };
            check_target(kind, "String", values, |text: String| numeric(&text))?;
            check_target(
                kind,
                "BigQueryDecimal<String>",
                values,
                |text: BigQueryDecimal<String>| numeric(&text.0),
            )?;
        }
        FieldKind::BigNumeric => {
            let big =
                |text: &str| Canonical::BigNumeric(decimal::parse_bignumeric(text).expect("text"));
            check_target(kind, "String", values, |text: String| big(&text))?;
            check_target(
                kind,
                "BigQueryDecimal<String>",
                values,
                |text: BigQueryDecimal<String>| big(&text.0),
            )?;
        }
        FieldKind::Geography => check_target(kind, "String", values, Canonical::Geography)?,
        FieldKind::Json => {
            check_target(kind, "String", values, |text: String| {
                Canonical::Json(serde_json::from_str(&text).expect("JSON text"))
            })?;
            check_target(
                kind,
                "BigQueryJson<Value>",
                values,
                |json: BigQueryJson<serde_json::Value>| Canonical::Json(json.0),
            )?;
            check_target(kind, "Value", values, Canonical::Json)?;
        }
        FieldKind::Interval => {
            check_target(kind, "BigQueryInterval", values, Canonical::Interval)?;
            check_target(kind, "String", values, |text: String| {
                Canonical::Interval(
                    crate::BigQueryInterval::parse_bq(&text).expect("INTERVAL text"),
                )
            })?;
        }
        FieldKind::Range => range_checks(values)?,
        FieldKind::Struct => {}
    }
    Ok(())
}

fn range_checks(values: &[Canonical]) -> Result<(), TestCaseError> {
    let Some(Canonical::Range { element, .. }) = values.first() else {
        return Ok(());
    };
    let element = *element;
    let kind = FieldKind::Range;
    let back = move |start: Option<i64>, end: Option<i64>| Canonical::Range {
        element,
        start,
        end,
    };
    let ok = |result: Result<i64, CodecError>| result.unwrap_or_else(|error| panic!("{error}"));
    match element {
        BigQueryRangeElementType::Date => {
            let days =
                |date: jiff::civil::Date| i64::from(civil::date_days(date).expect("in range"));
            check_target(
                kind,
                "BigQueryRange<Date>",
                values,
                |range: BigQueryRange<jiff::civil::Date>| {
                    back(range.start.map(days), range.end.map(days))
                },
            )?;
            check_target(
                kind,
                "BigQueryRange<BigQueryDate>",
                values,
                |range: BigQueryRange<BigQueryDate>| {
                    back(
                        range.start.map(|date| days(date.0)),
                        range.end.map(|date| days(date.0)),
                    )
                },
            )?;
            check_target(
                kind,
                "BigQueryRange<i32>",
                values,
                |range: BigQueryRange<i32>| {
                    back(range.start.map(i64::from), range.end.map(i64::from))
                },
            )?;
        }
        BigQueryRangeElementType::DateTime => {
            let micros = |date: jiff::civil::DateTime| ok(civil::datetime_micros(date));
            check_target(
                kind,
                "BigQueryRange<DateTime>",
                values,
                |range: BigQueryRange<jiff::civil::DateTime>| {
                    back(range.start.map(micros), range.end.map(micros))
                },
            )?;
            check_target(
                kind,
                "BigQueryRange<i64>",
                values,
                |range: BigQueryRange<i64>| back(range.start, range.end),
            )?;
        }
        BigQueryRangeElementType::Timestamp => {
            let micros = |timestamp: BigQueryTimestamp| ok(civil::timestamp_micros(timestamp.0));
            check_target(
                kind,
                "BigQueryRange<BigQueryTimestamp>",
                &below_jiff_max(values),
                |range: BigQueryRange<BigQueryTimestamp>| {
                    back(range.start.map(micros), range.end.map(micros))
                },
            )?;
            check_target(
                kind,
                "BigQueryRange<String>",
                values,
                |range: BigQueryRange<String>| {
                    let parse = |text: String| ok(civil::parse_timestamp(&text));
                    back(range.start.map(parse), range.end.map(parse))
                },
            )?;
            check_target(
                kind,
                "BigQueryRange<i64>",
                values,
                |range: BigQueryRange<i64>| back(range.start, range.end),
            )?;
        }
    }
    Ok(())
}

#[test]
fn every_type_and_mode_decodes_its_canonical_arrow_form() {
    let kinds = [
        FieldKind::Int64,
        FieldKind::Float64,
        FieldKind::Bool,
        FieldKind::String,
        FieldKind::Bytes,
        FieldKind::Date,
        FieldKind::Time,
        FieldKind::DateTime,
        FieldKind::Timestamp,
        FieldKind::Numeric,
        FieldKind::BigNumeric,
        FieldKind::Geography,
        FieldKind::Json,
        FieldKind::Interval,
    ];
    let config = proptest::test_runner::Config::with_cases(64);
    for kind in kinds {
        let mut runner = proptest::test_runner::TestRunner::new(config.clone());
        let strategy = proptest::collection::vec(Canonical::strategy(kind), 1..8);
        runner
            .run(&strategy, |values| checks(kind, &values))
            .unwrap_or_else(|error| panic!("{kind:?}: {error}"));
    }
    for element in [
        BigQueryRangeElementType::Date,
        BigQueryRangeElementType::DateTime,
        BigQueryRangeElementType::Timestamp,
    ] {
        let mut runner = proptest::test_runner::TestRunner::new(config.clone());
        let strategy = proptest::collection::vec(Canonical::range_strategy(element), 1..8);
        runner
            .run(&strategy, |values| checks(FieldKind::Range, &values))
            .unwrap_or_else(|error| panic!("RANGE<{element:?}>: {error}"));
    }
}

#[test]
fn parameterised_decimals_read_at_their_own_scale() {
    let numeric = Decimal128Array::from(vec![1_234_567_891i128, 500])
        .with_precision_and_scale(10, 2)
        .expect("NUMERIC(10, 2)'s Arrow type");
    let big = Decimal256Array::from(vec![i256::from_i128(-15), i256::from_i128(2000)])
        .with_precision_and_scale(40, 3)
        .expect("BIGNUMERIC(40, 3)'s Arrow type");
    let tags = list(
        "tags",
        vec![0, 1, 1],
        Arc::new(
            Decimal128Array::from(vec![7i128])
                .with_precision_and_scale(5, 1)
                .expect("NUMERIC(5, 1)'s Arrow type"),
        ),
    );
    let batch = batch(vec![
        column("price", numeric),
        column("big_total", big),
        tags,
    ]);
    #[derive(Deserialize, Debug, PartialEq)]
    struct Prices {
        price: String,
        big_total: String,
        tags: Vec<String>,
    }
    #[derive(Deserialize, Debug, PartialEq)]
    struct Whole {
        price: i64,
        big_total: i64,
    }
    assert_eq!(
        rows::<Prices>(&batch),
        [
            Prices {
                price: "12345678.91".to_string(),
                big_total: "-0.015".to_string(),
                tags: vec!["0.7".to_string()]
            },
            Prices {
                price: "5".to_string(),
                big_total: "2".to_string(),
                tags: vec![]
            }
        ]
    );
    assert_eq!(
        row::<Whole>(&batch, 1),
        Ok(Whole {
            price: 5,
            big_total: 2
        })
    );
    assert_eq!(
        row_error::<Whole>(&batch, 0).kind,
        BigQueryCodecErrorKind::OutOfRange
    );
}
